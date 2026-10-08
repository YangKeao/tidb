// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Private, mandatory projection consumer for the checked numeric-batch entry.
//! Public EvaluatorSuite::run still uses the native consumer. No filter, grouping,
//! row fallback, D4 diagnostic conversion or control/result-lineage composition.
//!
//! Native source metadata may have alias-backed mutable slices/atomic collation
//! state. Owners must keep it stable from preflight through checks/publication.
//! Input values/column owners must also be alias-stable throughout the borrowed
//! invocation: native integer leaf phases and singleton reads need not hold the
//! same lock interval. These are owner preconditions, NOT atomic snapshots.
//! Logical source byte policies and actual retained value capacities are separate
//! from allocator peaks, immutable program heap and externally owned Chunk buffers.

use std::mem::size_of;
use std::sync::Arc;

use protobuf::ProtobufEnum;
use tidb_chunk::column::get_fixed_len;
use tidb_datatype::tikv_compat::value::{
    from_scalar, project_field_type, snapshot_field_type, to_scalar, BridgeError, ValueMetadata,
};
use tidb_datatype::{Datum, DatumKind, FieldType, FieldTypeCode, FieldTypeFlags};
use tidb_query_datatype::codec::data_type::{ChunkRef, ChunkedVec, ScalarValue, VectorValue};
use tidb_query_datatype::expr::EvalContext;
use tidb_query_datatype::EvalType;
use tidb_query_expr::local::{
    compile_numeric_batch, CallMetadata, CompileLimits, ExecutionLimits, FunctionRef, InputRow,
    LiteralKind, LocalCompileContext, LocalError, LocalExpr, LocalFailureSite,
    LocalNumericBatchProgram, LocalResult, LocalRuntimeServices, NumericBatchFacts,
    OrdinaryCallSite, OrdinarySourceId, ReportedLocalFailure,
};

use crate::expr_collation::{Coercibility, CollationInfo, Repertoire};
use crate::pushdown_catalog::CATALOG;
use crate::scalar_function::ScalarFunction;

use super::{
    Chunk, Columns, EvalError, EvaluatorError, EvaluatorProgram, EvaluatorSuite, Expression,
    NumericBatchConsumer, NumericBatchInvocation,
};

#[derive(Clone, Copy, Debug)]
pub(crate) struct NumericSourceLimits {
    pub(crate) tree: CompileLimits,
    // Logical snapshot payload/copy-group and flat-scaffolding policy. Int/NULL
    // literal payload is fixed-size and included in the bounded node records.
    pub(crate) max_metadata_bytes: usize,
}

#[derive(Debug)]
enum FailureKind {
    Admission(&'static str),
    Bridge(BridgeError),
    Local(LocalError),
    Native(EvaluatorError),
    Reported {
        failure: ReportedLocalFailure,
        source: Arc<NumericSource>,
    },
}

/// Opaque failure ownership; no public `(report, foreign table)` constructor.
/// Raw strips the same report without rerunning. No SQL message/severity view.
#[derive(Debug)]
pub(crate) struct NumericBatchFailure {
    kind: FailureKind,
}
impl NumericBatchFailure {
    fn admission(message: &'static str) -> Self {
        Self {
            kind: FailureKind::Admission(message),
        }
    }
    fn reported(failure: ReportedLocalFailure, source: Arc<NumericSource>) -> Self {
        Self {
            kind: FailureKind::Reported { failure, source },
        }
    }
    pub(crate) fn local_error(&self) -> Option<&LocalError> {
        match &self.kind {
            FailureKind::Local(error) => Some(error),
            FailureKind::Reported { failure, .. } => Some(failure.error()),
            _ => None,
        }
    }
    pub(crate) fn site(&self) -> Option<&LocalFailureSite> {
        match &self.kind {
            FailureKind::Reported { failure, .. } => failure.site(),
            _ => None,
        }
    }
    pub(crate) fn source_ordinal(&self) -> Option<usize> {
        let FailureKind::Reported { failure, source } = &self.kind else {
            return None;
        };
        match failure.site()? {
            LocalFailureSite::Kernel { call, .. } => source
                .facts
                .call_sites()
                .iter()
                .find(|own| *own == call)
                .map(OrdinaryCallSite::ordinal),
            LocalFailureSite::InputSlot { slot, .. } => source.bindings.get(*slot).copied(),
        }
    }
    pub(crate) fn into_raw(self) -> Self {
        match self.kind {
            FailureKind::Reported { failure, .. } => failure.into_error().into(),
            kind => Self { kind },
        }
    }
}
impl From<LocalError> for NumericBatchFailure {
    fn from(error: LocalError) -> Self {
        Self {
            kind: FailureKind::Local(error),
        }
    }
}
impl From<BridgeError> for NumericBatchFailure {
    fn from(error: BridgeError) -> Self {
        Self {
            kind: FailureKind::Bridge(error),
        }
    }
}
impl From<EvaluatorError> for NumericBatchFailure {
    fn from(error: EvaluatorError) -> Self {
        Self {
            kind: FailureKind::Native(error),
        }
    }
}
impl From<EvalError> for NumericBatchFailure {
    fn from(error: EvalError) -> Self {
        EvaluatorError::Eval(error).into()
    }
}
type Result<T> = std::result::Result<T, NumericBatchFailure>;

fn resource(message: &'static str) -> LocalError {
    LocalError::ResourceLimit(message.into())
}
fn binding(message: &'static str) -> LocalError {
    LocalError::BindingContract(message.into())
}
fn array_bytes<T>(count: usize) -> LocalResult<usize> {
    count
        .checked_mul(size_of::<T>())
        .ok_or_else(|| resource("numeric caller size overflow"))
}
fn reserve<T>(values: &mut Vec<T>, count: usize) -> LocalResult<()> {
    values
        .try_reserve_exact(count)
        .map_err(|_| resource("numeric caller reservation failed"))
}

#[derive(Debug, PartialEq, Eq)]
struct CollationRecord {
    coercibility: Coercibility,
    initialized: bool,
    repertoire: Repertoire,
    charset: String,
    collation: String,
    explicit_charset: bool,
}
impl CollationRecord {
    fn capture(source: &CollationInfo) -> Self {
        let (charset, collation) = source.charset_and_collation();
        Self {
            coercibility: source.coercibility(),
            initialized: source.has_coercibility(),
            repertoire: source.repertoire(),
            charset: charset.into(),
            collation: collation.into(),
            explicit_charset: source.is_explicit_charset(),
        }
    }
    fn matches(&self, source: &CollationInfo) -> bool {
        let (charset, collation) = source.charset_and_collation();
        self.coercibility == source.coercibility()
            && self.initialized == source.has_coercibility()
            && self.repertoire == source.repertoire()
            && self.charset == charset
            && self.collation == collation
            && self.explicit_charset == source.is_explicit_charset()
    }
}

#[derive(Debug)]
enum SourceKind {
    Constant {
        value: Option<i64>,
        subquery: i64,
    },
    Column {
        index: usize,
        id: i64,
        unique_id: i64,
        name: String,
        hidden: bool,
        prefix: bool,
        in_operand: bool,
    },
    Plus {
        display_name: tidb_ast::CiString,
    },
}
impl SourceKind {
    fn capture(node: &Expression) -> Result<Self> {
        Ok(match node {
            Expression::Constant(value) => {
                Self::Constant {
                    value: strict_literal(value.literal_value().ok_or_else(|| {
                        NumericBatchFailure::admission("nonliteral numeric source")
                    })?)?,
                    subquery: value.subquery_ref_id,
                }
            }
            Expression::Column(column) => Self::Column {
                index: column.index as usize,
                id: column.id,
                unique_id: column.unique_id,
                name: column.orig_name.clone(),
                hidden: column.is_hidden,
                prefix: column.is_prefix,
                in_operand: column.in_operand,
            },
            Expression::ScalarFunction(function) => Self::Plus {
                display_name: function.func_name.clone(),
            },
            Expression::CorrelatedColumn(_) => {
                return Err(NumericBatchFailure::admission("correlated source"))
            }
        })
    }
    fn matches(&self, node: &Expression) -> bool {
        match (self, node) {
            (Self::Constant { value, subquery }, Expression::Constant(constant)) => {
                let current = match constant.literal_value() {
                    Some(Datum::Null) => None,
                    Some(Datum::Int(value)) => Some(*value),
                    _ => return false,
                };
                *value == current && *subquery == constant.subquery_ref_id
            }
            (
                Self::Column {
                    index,
                    id,
                    unique_id,
                    name,
                    hidden,
                    prefix,
                    in_operand,
                },
                Expression::Column(column),
            ) => {
                *index == column.index as usize
                    && *id == column.id
                    && *unique_id == column.unique_id
                    && *name == column.orig_name
                    && *hidden == column.is_hidden
                    && *prefix == column.is_prefix
                    && *in_operand == column.in_operand
            }
            (Self::Plus { display_name }, Expression::ScalarFunction(function)) => {
                display_name.original() == function.func_name.original()
                    && display_name.lowercase() == function.func_name.lowercase()
            }
            _ => false,
        }
    }
}

#[derive(Debug)]
struct NodeRecord {
    sql: FieldType,
    collation: CollationRecord,
    kind: SourceKind,
}

struct NumericSource {
    // The actual native program owner, not another lowered row/D4 plan. Native
    // evaluation is never called through it by the mandatory consumer.
    owner: Arc<EvaluatorProgram>,
    calculated_slot: usize,
    output_index: usize,
    unit: u64,
    nodes: Box<[NodeRecord]>,
    bindings: Box<[usize]>, // slot -> all-node source ordinal
    row_schema: Box<[FieldType]>,
    expr: LocalExpr,
    schema: Box<[tipb::FieldType]>,
    facts: NumericBatchFacts,
    limits: NumericSourceLimits,
}
impl std::fmt::Debug for NumericSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NumericSource")
            .field("unit", &self.unit)
            .field("nodes", &self.nodes.len())
            .finish()
    }
}

struct CheckedNode<'a> {
    node: &'a Expression,
    sql: &'a FieldType,
    collation: &'a CollationInfo,
}
struct Budget {
    used: usize,
    max: usize,
}
impl Budget {
    fn add(&mut self, bytes: usize) -> LocalResult<()> {
        self.used = self
            .used
            .checked_add(bytes)
            .filter(|sum| *sum <= self.max)
            .ok_or_else(|| resource("numeric caller source metadata policy exceeded"))?;
        Ok(())
    }
    fn array<T>(&mut self, count: usize) -> LocalResult<()> {
        self.add(array_bytes::<T>(count)?)
    }
    fn field(&mut self, field: &FieldType, copies: usize) -> LocalResult<()> {
        self.add(
            field
                .checked_snapshot_payload_bytes()
                .and_then(|size| size.checked_mul(copies))
                .ok_or_else(|| resource("numeric caller FieldType payload overflow"))?,
        )
    }
}

fn check_type(sql: &FieldType) -> Result<()> {
    const DOCUMENTED: u64 = (1 << 25) - 1;
    let excluded = u64::from(
        FieldTypeFlags::UNSIGNED
            | FieldTypeFlags::ENUM
            | FieldTypeFlags::SET
            | FieldTypeFlags::PARSE_TO_JSON
            | FieldTypeFlags::ENUM_SET_AS_INT,
    );
    if sql.code() != FieldTypeCode::LongLong
        || sql.is_array()
        || sql.raw_flags() & (excluded | !DOCUMENTED) != 0
    {
        return Err(NumericBatchFailure::admission(
            "numeric source requires actual closed signed LongLong",
        ));
    }
    Ok(())
}
fn strict_literal(value: &Datum) -> Result<Option<i64>> {
    match value {
        Datum::Null => Ok(None),
        Datum::Int(value) => Ok(Some(*value)),
        _ => Err(NumericBatchFailure::admission(
            "numeric source literal is not strict Int/NULL",
        )),
    }
}
fn parts(node: &Expression) -> Result<(&FieldType, &CollationInfo)> {
    let (field, collation, pb) = match node {
        Expression::Constant(value) => (
            value.ret_type.as_ref(),
            &value.collation,
            value.pb_origin().is_some(),
        ),
        Expression::Column(value) => (
            value.ret_type.as_ref(),
            &value.collation,
            value.pb_origin().is_some(),
        ),
        Expression::ScalarFunction(value) => (
            value.ret_type.as_ref(),
            &value.collation,
            value.pb_origin().is_some(),
        ),
        Expression::CorrelatedColumn(_) => {
            return Err(NumericBatchFailure::admission("correlated numeric source"))
        }
    };
    if pb {
        return Err(NumericBatchFailure::admission(
            "PB state inside numeric source",
        ));
    }
    Ok((
        field.ok_or_else(|| NumericBatchFailure::admission("missing numeric declaration"))?,
        collation,
    ))
}
fn check_plus(function: &ScalarFunction) -> Result<()> {
    if function.func_name.lowercase() != "plus"
        || function.args.len() != 2
        || function.pb_signature().is_some()
        || function.pb_origin().is_some()
        || function.has_values_offset()
        || function.has_grouping_metadata()
    {
        return Err(NumericBatchFailure::admission(
            "outside native numeric PLUS closure",
        ));
    }
    Ok(())
}
fn plus_fact() -> Result<FunctionRef> {
    // Existing SQL signed/signed signature facts, never a new selector/kernel
    // table or a PB Expr round trip. All node declarations are checked separately.
    let row = CATALOG
        .iter()
        .find(|row| {
            row.name == "plus"
                && row.selector.len() == 2
                && row.selector.iter().all(|arg| {
                    arg.eval == Some(tidb_datatype::EvalType::Int)
                        && arg.unsigned == Some(false)
                        && arg.binary_string.is_none()
                })
                && row.arg_types.len() == 2
                && row
                    .arg_types
                    .iter()
                    .all(|ty| *ty == tidb_datatype::EvalType::Int)
                && row.ret == tidb_datatype::EvalType::Int
        })
        .ok_or_else(|| NumericBatchFailure::admission("missing signed PLUS catalog fact"))?;
    let signature = tipb::ScalarFuncSig::from_i32(row.sig as i32)
        .filter(|signature| *signature == tipb::ScalarFuncSig::PlusInt)
        .ok_or_else(|| NumericBatchFailure::admission("numeric catalog is not exact203"))?;
    Ok(FunctionRef::TiPb(signature))
}
fn projected(sql: &FieldType, new_collation: bool) -> Result<tipb::FieldType> {
    let collation = tidb_datatype::get_collation_by_name(sql.collation_name())
        .map_err(|_| NumericBatchFailure::admission("unknown numeric SQL collation name"))?;
    if collation.id <= 0 {
        return Err(NumericBatchFailure::admission(
            "nonpositive SQL collation ID",
        ));
    }
    Ok(project_field_type(
        sql,
        if new_collation {
            -collation.id
        } else {
            collation.id
        },
    )?)
}

// Complete source and incoming schema byte gates precede EVERY equality and
// detached/projected/C snapshot. Borrowed temporary nodes have no executable
// children and are not a second interpreter. Stable owner interval is required.
fn preflight<'a>(
    root: &'a Expression,
    row_schema: &[FieldType],
    limits: NumericSourceLimits,
) -> Result<Vec<CheckedNode<'a>>> {
    if limits.tree.max_nodes == 0 || limits.tree.max_depth == 0 {
        return Err(resource("numeric source node/depth policy is zero").into());
    }
    if !matches!(root, Expression::ScalarFunction(_)) {
        return Err(NumericBatchFailure::admission(
            "native numeric entry does not admit a leaf root",
        ));
    }
    let mut budget = Budget {
        used: 0,
        max: limits.max_metadata_bytes,
    };
    for field in row_schema {
        budget.field(field, 1)?;
    }
    let mut pending = Vec::new();
    let mut checked = Vec::new();
    budget.array::<(&Expression, usize)>(1)?;
    reserve(&mut pending, 1)?;
    pending.push((root, 1usize));
    let mut scheduled = 1usize;
    while let Some((node, depth)) = pending.pop() {
        let (sql, collation) = parts(node)?;
        check_type(sql)?;
        budget.field(sql, 3)?; // native source, projected declaration, C snapshot
        budget.array::<CheckedNode<'_>>(1)?;
        budget.array::<NodeRecord>(1)?;
        budget.array::<BuildNode>(1)?;
        budget.array::<LocalExpr>(1)?;
        let (charset, collate) = collation.charset_and_collation();
        budget.add(charset.len())?;
        budget.add(collate.len())?;
        match node {
            Expression::Constant(value) => {
                strict_literal(value.literal_value().ok_or_else(|| {
                    NumericBatchFailure::admission("parameter/deferred numeric source")
                })?)?;
            }
            Expression::Column(column) => {
                if column.virtual_expr.is_some() || column.correlated_col_unique_id != 0 {
                    return Err(NumericBatchFailure::admission(
                        "virtual/correlated numeric column",
                    ));
                }
                let index = usize::try_from(column.index)
                    .map_err(|_| NumericBatchFailure::admission("negative numeric column index"))?;
                if index >= row_schema.len() {
                    return Err(NumericBatchFailure::admission(
                        "numeric column outside row schema",
                    ));
                }
                budget.add(column.orig_name.len())?;
                budget.array::<usize>(1)?;
                budget.field(sql, 1)?; // projected binding declaration copy
            }
            Expression::ScalarFunction(function) => {
                check_plus(function)?;
                budget.add(function.func_name.original().len())?;
                budget.add(function.func_name.lowercase().len())?;
                budget.array::<OrdinaryCallSite>(1)?;
                scheduled = scheduled
                    .checked_add(2)
                    .filter(|nodes| *nodes <= limits.tree.max_nodes)
                    .ok_or_else(|| resource("numeric source node policy exceeded"))?;
                let child_depth = depth
                    .checked_add(1)
                    .filter(|depth| *depth <= limits.tree.max_depth)
                    .ok_or_else(|| resource("numeric source depth policy exceeded"))?;
                budget.array::<(&Expression, usize)>(2)?;
                reserve(&mut pending, 2)?;
                for child in function.args.iter().rev() {
                    pending.push((child, child_depth));
                }
            }
            Expression::CorrelatedColumn(_) => unreachable!(),
        }
        reserve(&mut checked, 1)?;
        checked.push(CheckedNode {
            node,
            sql,
            collation,
        });
    }
    for item in &checked {
        if let Expression::Column(column) = item.node {
            let field = &row_schema[column.index as usize];
            if field.collation() != item.sql.collation() || field != item.sql {
                return Err(NumericBatchFailure::admission(
                    "numeric binding differs from complete row declaration",
                ));
            }
        }
    }
    Ok(checked)
}

fn suite_shape(suite: &EvaluatorSuite) -> Result<&Expression> {
    let program = &suite.program;
    if program.calculated.len() != 1
        || program.calculated_output_indexes.len() != 1
        || program.calculated_output_indexes.first() != Some(&0)
        || !program.column_mapping.is_empty()
        || suite.column_swap_helper.is_some()
    {
        return Err(NumericBatchFailure::admission(
            "private numeric suite requires one calculation and no owner swap",
        ));
    }
    Ok(&program.calculated[0])
}
enum BuildKind {
    Literal(Option<i64>),
    Input(usize),
    Plus,
}
struct BuildNode {
    field: tipb::FieldType,
    kind: BuildKind,
}

fn lower(
    suite: &EvaluatorSuite,
    row_schema: &[FieldType],
    new_collation: bool,
    unit: u64,
    limits: NumericSourceLimits,
) -> Result<Arc<NumericSource>> {
    let root = suite_shape(suite)?;
    let checked = preflight(root, row_schema, limits)?;
    let selected = plus_fact()?;
    let mut nodes = Vec::new();
    let mut bindings = Vec::new();
    let mut schema = Vec::new();
    let mut sites = Vec::new();
    let mut build = Vec::new();
    reserve(&mut nodes, checked.len())?;
    reserve(&mut build, checked.len())?;
    for (ordinal, item) in checked.into_iter().enumerate() {
        let field = projected(item.sql, new_collation)?;
        let kind = SourceKind::capture(item.node)?;
        let construction = match &kind {
            SourceKind::Constant { value, .. } => BuildKind::Literal(*value),
            SourceKind::Column { .. } => {
                let slot = bindings.len();
                reserve(&mut bindings, 1)?;
                reserve(&mut schema, 1)?;
                bindings.push(ordinal);
                schema.push(field.clone());
                BuildKind::Input(slot)
            }
            SourceKind::Plus { .. } => {
                reserve(&mut sites, 1)?;
                let node =
                    u64::try_from(ordinal).map_err(|_| resource("numeric source ID overflow"))?;
                sites.push(OrdinaryCallSite::sql_native_numeric_batch(
                    ordinal,
                    OrdinarySourceId::new(unit, node),
                ));
                BuildKind::Plus
            }
        };
        build.push(BuildNode {
            field,
            kind: construction,
        });
        nodes.push(NodeRecord {
            sql: snapshot_field_type(item.sql),
            collation: CollationRecord::capture(item.collation),
            kind,
        });
    }
    let mut stack = Vec::new();
    reserve(&mut stack, nodes.len())?;
    for node in build.into_iter().rev() {
        let expr =
            match node.kind {
                BuildKind::Literal(value) => LocalExpr::Constant {
                    value: ScalarValue::Int(value),
                    field_type: node.field,
                    literal_kind: LiteralKind::Typed,
                },
                BuildKind::Input(slot) => LocalExpr::InputSlot {
                    slot,
                    field_type: node.field,
                },
                BuildKind::Plus => {
                    let mut args = Vec::new();
                    reserve(&mut args, 2)?;
                    args.push(stack.pop().ok_or_else(|| {
                        NumericBatchFailure::admission("missing numeric left child")
                    })?);
                    args.push(stack.pop().ok_or_else(|| {
                        NumericBatchFailure::admission("missing numeric right child")
                    })?);
                    LocalExpr::Call {
                        function: selected,
                        args: args.into_boxed_slice(),
                        return_type: node.field,
                        metadata: CallMetadata::None,
                    }
                }
            };
        stack.push(expr);
    }
    if stack.len() != 1 {
        return Err(NumericBatchFailure::admission(
            "numeric source is not one tree",
        ));
    }
    let expr = stack
        .pop()
        .ok_or_else(|| NumericBatchFailure::admission("missing numeric root"))?;
    let facts = NumericBatchFacts::sql_native_numeric_batch(&expr, &schema, sites, limits.tree)?;
    let mut detached = Vec::new();
    reserve(&mut detached, row_schema.len())?;
    detached.extend(row_schema.iter().map(snapshot_field_type));
    Ok(Arc::new(NumericSource {
        owner: Arc::clone(&suite.program),
        calculated_slot: 0,
        output_index: 0,
        unit,
        nodes: nodes.into_boxed_slice(),
        bindings: bindings.into_boxed_slice(),
        row_schema: detached.into_boxed_slice(),
        expr,
        schema: schema.into_boxed_slice(),
        facts,
        limits,
    }))
}

/// Independent compiled numeric-batch owner. No raw evaluation method accepts
/// arbitrary rows; every invocation goes through the real suite dispatch seal.
pub(crate) struct PreparedNumericBatch {
    source: Arc<NumericSource>,
    program: LocalNumericBatchProgram,
    limits: ExecutionLimits,
    max_materialization_retained_bytes: usize,
}
impl PreparedNumericBatch {
    pub(crate) fn compile(
        suite: &EvaluatorSuite,
        row_schema: &[FieldType],
        new_collation: bool,
        unit: u64,
        source_limits: NumericSourceLimits,
        execution: ExecutionLimits,
        max_materialization_retained_bytes: usize,
    ) -> Result<Self> {
        let source = lower(suite, row_schema, new_collation, unit, source_limits)?;
        let program = compile_numeric_batch(
            &source.expr,
            &source.schema,
            LocalCompileContext {
                limits: source.limits.tree,
            },
            &source.facts,
        )?;
        Ok(Self {
            source,
            program,
            limits: execution,
            max_materialization_retained_bytes,
        })
    }
    pub(crate) fn declared_type_snapshot(&self) -> FieldType {
        snapshot_field_type(&self.source.nodes[0].sql)
    }

    fn validate_invocation(
        &self,
        suite: &EvaluatorSuite,
        input: &Chunk,
        output: &Chunk,
        row_schema: &[FieldType],
    ) -> Result<Vec<usize>> {
        if !Arc::ptr_eq(&suite.program, &self.source.owner) {
            return Err(NumericBatchFailure::admission(
                "numeric worker belongs to another native program",
            ));
        }
        let root = suite_shape(suite)?;
        // All incoming source/schema bytes BEFORE Eq or the earlier Decimal
        // worker. This also bounds the native eligibility walk that follows.
        let current = preflight(root, row_schema, self.source.limits)?;
        if current.len() != self.source.nodes.len() {
            return Err(NumericBatchFailure::admission(
                "numeric source shape changed",
            ));
        }
        for (node, old) in current.iter().zip(self.source.nodes.iter()) {
            if node.sql.collation() != old.sql.collation()
                || node.sql != &old.sql
                || !old.collation.matches(node.collation)
                || !old.kind.matches(node.node)
            {
                return Err(NumericBatchFailure::admission(
                    "numeric source metadata changed since preparation",
                ));
            }
        }
        if row_schema.len() != self.source.row_schema.len()
            || row_schema
                .iter()
                .zip(self.source.row_schema.iter())
                .any(|(a, b)| a.collation() != b.collation() || a != b)
            || input.num_cols() != row_schema.len()
        {
            return Err(LocalError::InvalidBatch(
                "numeric incoming schema differs from preparation".into(),
            )
            .into());
        }
        let physical = input.physical_rows();
        for (index, field) in row_schema.iter().enumerate() {
            let column = input.column(index);
            if column.rows() != physical || column.type_size() != get_fixed_len(field) {
                return Err(LocalError::InvalidBatch(
                    "numeric native column layout/length differs".into(),
                )
                .into());
            }
        }
        if output.num_cols() != 1
            || output.column(0).type_size() != get_fixed_len(&self.source.nodes[0].sql)
            || output.sel().is_some()
            || output.is_incomplete_chunk()
        {
            return Err(LocalError::InvalidBatch(
                "numeric destination layout is not one ordinary Int column".into(),
            )
            .into());
        }
        let count = input.num_rows();
        if count > 1024 {
            return Err(resource("numeric caller exceeds1024 selected occurrences").into());
        }
        let mut selection = Vec::new();
        reserve(&mut selection, count)?;
        if let Some(selected) = input.sel() {
            if selected.iter().any(|row| *row >= physical) {
                return Err(LocalError::InvalidBatch(
                    "numeric selection exceeds physical rows".into(),
                )
                .into());
            }
            selection.extend_from_slice(selected);
        } else {
            selection.extend(0..physical);
        }
        self.accept_retained(array_bytes::<usize>(selection.capacity())?)?;
        Ok(selection)
    }
    fn accept_retained(&self, bytes: usize) -> LocalResult<()> {
        if bytes > self.max_materialization_retained_bytes {
            Err(resource(
                "numeric caller retained materialization policy exceeded",
            ))
        } else {
            Ok(())
        }
    }
    fn materialize(&self, values: VectorValue, selection: &Vec<usize>) -> Result<Vec<Datum>> {
        if values.eval_type() != EvalType::Int || values.len() != selection.len() {
            return Err(binding("numeric result shape differs from computed boundary").into());
        }
        let VectorValue::Int(ints) = &values else {
            unreachable!()
        };
        let source_bytes = array_bytes::<i64>(ints.capacity())?
            .checked_add(
                ints.get_bit_vec()
                    .retained_heap_bytes()
                    .ok_or_else(|| resource("numeric bitmap size overflow"))?,
            )
            .and_then(|size| size.checked_add(array_bytes::<usize>(selection.capacity()).ok()?))
            .ok_or_else(|| resource("numeric source/selection retained size overflow"))?;
        let minimum = source_bytes
            .checked_add(array_bytes::<Datum>(values.len())?)
            .ok_or_else(|| resource("numeric native output size overflow"))?;
        self.accept_retained(minimum)?;
        let mut output = Vec::new();
        reserve(&mut output, values.len())?;
        let actual = source_bytes
            .checked_add(array_bytes::<Datum>(output.capacity())?)
            .ok_or_else(|| resource("numeric native output capacity overflow"))?;
        self.accept_retained(actual)?;
        let computed = ValueMetadata {
            kind: DatumKind::Int,
            string_collation: None,
            decimal_declared_shape: None,
        };
        for index in 0..values.len() {
            output.push(from_scalar(
                values.get_scalar_ref(index),
                EvalType::Int,
                &computed,
            )?);
        }
        // Each admitted Datum is fixed-size Int/NULL. Source and selection stay
        // charged through conversion; external output Chunk allocation is not a
        // callback/allocator peak guarantee of this private buffer policy.
        drop(values);
        Ok(output)
    }
}

// Private native leaf seam; only tests can substitute an adversarial provider,
// and they still enter through the suite/token and genuine Chunk preflight.
trait NativeDatumSource {
    fn schema(&self) -> &[tipb::FieldType];
    fn read(
        &mut self,
        ctx: &mut EvalContext,
        slot: usize,
        row: InputRow,
        expected: &tipb::FieldType,
    ) -> LocalResult<Datum>;
}
struct NativeChunk<'a> {
    source: &'a NumericSource,
    input: &'a Chunk,
    selection: &'a [usize],
}
impl NativeDatumSource for NativeChunk<'_> {
    fn schema(&self) -> &[tipb::FieldType] {
        &self.source.schema
    }
    fn read(
        &mut self,
        _ctx: &mut EvalContext,
        slot: usize,
        row: InputRow,
        expected: &tipb::FieldType,
    ) -> LocalResult<Datum> {
        let ordinal = *self
            .source
            .bindings
            .get(slot)
            .ok_or_else(|| binding("unknown numeric slot"))?;
        if self.source.schema.get(slot) != Some(expected)
            || row.input_row >= self.input.physical_rows()
            || self.selection.get(row.occurrence) != Some(&row.input_row)
        {
            return Err(binding("numeric demanded occurrence/type differs"));
        }
        let node = &self.source.nodes[ordinal];
        let SourceKind::Column { index, .. } = &node.kind else {
            return Err(binding("numeric slot has no native column"));
        };
        // Physical already: get_row would apply the actual Chunk Sel twice.
        Ok(self
            .input
            .physical_row(row.input_row)
            .get_datum(*index, &node.sql))
    }
}
struct CheckedInputs<'a> {
    source: &'a mut dyn NativeDatumSource,
}
impl LocalRuntimeServices for CheckedInputs<'_> {
    fn binding_schema(&self) -> &[tipb::FieldType] {
        self.source.schema()
    }
    fn read_input(
        &mut self,
        ctx: &mut EvalContext,
        slot: usize,
        row: InputRow,
        expected: &tipb::FieldType,
    ) -> LocalResult<VectorValue> {
        let value = self.source.read(ctx, slot, row, expected)?;
        if !matches!(value, Datum::Int(_) | Datum::Null) {
            return Err(binding(
                "numeric demanded Datum is not strict Int/NULL before erasure",
            ));
        }
        let (value, _) = to_scalar(&value, EvalType::Int)
            .map_err(|error| LocalError::BindingContract(error.to_string()))?;
        Ok(VectorValue::from_scalar(&value, 1))
    }
}

struct Consumer<'a> {
    worker: &'a mut PreparedNumericBatch,
    ctx: &'a mut EvalContext,
    row_schema: &'a [FieldType],
    selection: Vec<usize>,
    #[cfg(test)]
    override_source: Option<&'a mut dyn NativeDatumSource>,
}
impl NumericBatchConsumer for Consumer<'_> {
    type Error = NumericBatchFailure;
    fn before_run(&mut self, suite: &EvaluatorSuite, input: &Chunk, output: &Chunk) -> Result<()> {
        self.selection = self
            .worker
            .validate_invocation(suite, input, output, self.row_schema)?;
        Ok(())
    }
    fn consume(
        &mut self,
        invocation: NumericBatchInvocation<'_>,
        _native_ctx: &dyn Columns,
    ) -> Result<Vec<Datum>> {
        let source = Arc::clone(&self.worker.source);
        if !Arc::ptr_eq(invocation.program, &source.owner)
            || invocation.calculated_slot != source.calculated_slot
            || invocation.output_index != source.output_index
            || !std::ptr::eq(
                invocation.candidate.expression(),
                &source.owner.calculated[source.calculated_slot],
            )
            || invocation.candidate.target() != tidb_datatype::EvalType::Int
            || invocation.input.num_rows() != self.selection.len()
        {
            return Err(NumericBatchFailure::admission(
                "numeric invocation does not join its own source/program",
            ));
        }
        // Consume the native eligibility evidence without calling its value
        // worker. The actual suite/global decision minted it for THIS call.
        drop(invocation.candidate);
        let mut native = NativeChunk {
            source: &source,
            input: invocation.input,
            selection: &self.selection,
        };
        #[cfg(test)]
        let provider: &mut dyn NativeDatumSource = match self.override_source.as_mut() {
            Some(source) => &mut **source,
            None => &mut native,
        };
        #[cfg(not(test))]
        let provider: &mut dyn NativeDatumSource = &mut native;
        let mut checked = CheckedInputs { source: provider };
        let values = self
            .worker
            .program
            .eval_with_bindings_reported(
                self.worker.limits,
                self.ctx,
                invocation.input.physical_rows(),
                &self.selection,
                &mut checked,
            )
            .map_err(|failure| NumericBatchFailure::reported(failure, Arc::clone(&source)))?;
        self.worker.materialize(values, &self.selection)
    }
    fn row_route(&mut self) -> Result<()> {
        Err(NumericBatchFailure::admission(
            "mandatory numeric consumer did not reach the native batch entry",
        ))
    }
}

impl EvaluatorSuite {
    pub(crate) fn run_numeric_batch_reported<C: Columns>(
        &self,
        native_ctx: &C,
        ctx: &mut EvalContext,
        worker: &mut PreparedNumericBatch,
        input: &mut Chunk,
        output: &mut Chunk,
        row_schema: &[FieldType],
    ) -> Result<()> {
        self.run_with_consumer(
            native_ctx,
            input,
            output,
            &mut Consumer {
                worker,
                ctx,
                row_schema,
                selection: Vec::new(),
                #[cfg(test)]
                override_source: None,
            },
        )
    }
    pub(crate) fn run_numeric_batch_raw<C: Columns>(
        &self,
        native_ctx: &C,
        ctx: &mut EvalContext,
        worker: &mut PreparedNumericBatch,
        input: &mut Chunk,
        output: &mut Chunk,
        row_schema: &[FieldType],
    ) -> Result<()> {
        self.run_numeric_batch_reported(native_ctx, ctx, worker, input, output, row_schema)
            .map_err(NumericBatchFailure::into_raw)
    }
}

#[cfg(test)]
#[path = "numeric_batch_tests.rs"]
mod tests;
