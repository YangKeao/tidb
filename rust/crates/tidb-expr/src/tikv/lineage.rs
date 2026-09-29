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

//! Private SQL TypedRow Datum-returning control closure, not EvalInt/EvalString,
//! AST, PB, numeric batch, PLUS composition or a production evaluator hook.
//!
//! Every source node is proved before transport. The one executable LocalExpr
//! and C's checked control facade stay paired with this caller's immutable ID
//! table; equal numeric IDs from another table do not authenticate a join.
//! No native expression, deferred callback or second executable graph is saved.
//!
//! Source FieldTypes may share mutable GoSharedSlice backing through aliases.
//! Their owner MUST keep metadata stable through preflight, equality, detached
//! copies and C fact construction; incoming row_schema requires the same stable
//! interval through invocation preflight. An observer is not an atomic snapshot.
//! Prepared metadata stays private; outward type copies are deeply detached.

use std::mem::size_of;
use std::sync::Arc;

use tidb_chunk::chunk::Chunk;
use tidb_datatype::tikv_compat::value::{
    from_scalar, snapshot_field_type, to_scalar, ValueMetadata,
};
use tidb_datatype::{Datum, DatumKind, FieldType, FieldTypeCode, FieldTypeFlags, StringDatum};
use tidb_query_datatype::{
    codec::data_type::{
        ChunkRef, ChunkedVec, ChunkedVecBytes, ScalarValue, ScalarValueRef, VectorValue,
    },
    expr::EvalContext,
    EvalType,
};
use tidb_query_expr::local::{
    compile_control_with_lineage, CallMetadata, CompileLimits, ControlLineageFacts,
    ControlProducerFact, ControlProducerRole, ExecutionLimits, FunctionRef, InputRow,
    LineageCarrier, LiteralKind, LocalCompileContext, LocalControlProgram, LocalError,
    LocalEvalState, LocalExpr, LocalResult, LocalRuntimeServices, ResultMetaId,
};

use crate::expr_collation::CollationInfo;
use crate::expression::Expression;

use super::context::NativeInputs;
use super::lower::{self, ColumnBinding, LoweredSpec, NodeMetadata, SourceNode};
use super::{catalog, SeedError, SeedResult};

/// Explicit logical source/snapshot-work policy, not an allocator/peak bound.
/// Literals charge the transport and flat-fact payload copies. Metadata charges
/// flat scaffolding and known native/projected/fact declaration copy groups.
/// Immutable compiler copies and allocator overhead are not totalled as heap.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ControlSourceLimits {
    pub(crate) tree: CompileLimits,
    pub(crate) max_literal_bytes: usize,
    pub(crate) max_metadata_bytes: usize,
}

struct ProducerRecord {
    id: ResultMetaId,
    carrier: LineageCarrier,
    role: ControlProducerRole,
    identity: ValueMetadata,
    // This is a static permission, not a chosen origin or an evaluator.
    may_escape_root: bool,
    display_name: Option<tidb_ast::CiString>,
}

/// Opaque ownership of metadata/facts and the one executable source graph.
/// No shallow FieldType getter exposes the detached mutable-slice backing.
pub(crate) struct LoweredControlLineage {
    core: LoweredSpec,
    facts: ControlLineageFacts,
    records: Box<[ProducerRecord]>,
    // Sparse record numbers never index allocations. Values are source ordinals.
    id_index: Box<[(ResultMetaId, usize)]>,
    binding_nodes: Box<[usize]>,
    source_limits: ControlSourceLimits,
}

impl std::fmt::Debug for LoweredControlLineage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LoweredControlLineage")
            .field("namespace", &self.facts.namespace())
            .field("nodes", &self.records.len())
            .finish()
    }
}

impl LoweredControlLineage {
    fn record(&self, id: ResultMetaId) -> LocalResult<(usize, &ProducerRecord)> {
        if id.unit() != self.facts.namespace() {
            return Err(contract("control result belongs to another namespace"));
        }
        let index = self
            .id_index
            .binary_search_by_key(&id, |entry| entry.0)
            .map_err(|_| contract("unknown control result metadata ID"))?;
        let ordinal = self.id_index[index].1;
        let record = self
            .records
            .get(ordinal)
            .ok_or_else(|| contract("invalid control producer ordinal"))?;
        if record.id != id {
            return Err(contract("control producer index differs from its record"));
        }
        Ok((ordinal, record))
    }
}

#[derive(Clone, Copy)]
enum Shape {
    If,
    IfNull,
    Case,
    Coalesce,
    Boolean,
}

impl Shape {
    fn from_name(name: &str) -> SeedResult<Self> {
        match name {
            "if" => Ok(Self::If),
            "ifnull" => Ok(Self::IfNull),
            "case" => Ok(Self::Case),
            "coalesce" => Ok(Self::Coalesce),
            "and" | "or" => Ok(Self::Boolean),
            _ => Err(SeedError::Admission("outside native Datum control path")),
        }
    }

    fn predicate_child(self, index: usize, arity: usize) -> bool {
        match self {
            Self::If => index == 0,
            Self::Case => index % 2 == 0 && index + 1 < arity,
            Self::Boolean => true,
            Self::IfNull | Self::Coalesce => false,
        }
    }

    fn generates_null(self, arity: usize) -> bool {
        matches!(self, Self::Coalesce) || (matches!(self, Self::Case) && arity % 2 == 0)
    }
}

#[derive(Clone, Copy)]
enum Demand {
    Value(Option<LineageCarrier>),
    PredicateInt,
}

struct Visit<'a> {
    node: &'a Expression,
    depth: usize,
    demand: Demand,
    escapes: bool,
}

// Temporary borrowed all-node preorder facts, no executable children. Dropped
// after lowering. PredicateInt propagates down every possible selected producer,
// including dead alternatives: a signed intermediate flag cannot hide UInt.
struct CheckedSource<'a> {
    node: &'a Expression,
    sql: &'a FieldType,
    collation: &'a CollationInfo,
    carrier: LineageCarrier,
    role: ControlProducerRole,
    identity: ValueMetadata,
    selected: Option<FunctionRef>,
    escapes: bool,
}

struct SourceBudget {
    metadata: usize,
    literal: usize,
    limits: ControlSourceLimits,
}

impl SourceBudget {
    fn metadata(&mut self, bytes: usize) -> LocalResult<()> {
        self.metadata = self
            .metadata
            .checked_add(bytes)
            .filter(|total| *total <= self.limits.max_metadata_bytes)
            .ok_or_else(|| resource("control source metadata byte policy exceeded"))?;
        Ok(())
    }

    fn array<T>(&mut self, count: usize) -> LocalResult<()> {
        self.metadata(array_bytes::<T>(count)?)
    }

    fn field(&mut self, field: &FieldType, copies: usize) -> LocalResult<()> {
        let bytes = field
            .checked_snapshot_payload_bytes()
            .and_then(|bytes| bytes.checked_mul(copies))
            .ok_or_else(|| resource("control FieldType payload size overflow"))?;
        self.metadata(bytes)
    }

    fn literal(&mut self, bytes: usize) -> LocalResult<()> {
        self.literal = bytes
            .checked_mul(2)
            .and_then(|bytes| self.literal.checked_add(bytes))
            .filter(|total| *total <= self.limits.max_literal_bytes)
            .ok_or_else(|| resource("control literal byte policy exceeded"))?;
        Ok(())
    }
}

fn resource(message: &'static str) -> LocalError {
    LocalError::ResourceLimit(message.into())
}
fn contract(message: &'static str) -> LocalError {
    LocalError::BindingContract(message.into())
}
fn array_bytes<T>(count: usize) -> LocalResult<usize> {
    count
        .checked_mul(size_of::<T>())
        .ok_or_else(|| resource("control buffer byte size overflow"))
}
fn reserve<T>(values: &mut Vec<T>, additional: usize) -> LocalResult<()> {
    values
        .try_reserve_exact(additional)
        .map_err(|_| resource("control buffer reservation failed"))
}

fn eval_type(carrier: LineageCarrier) -> EvalType {
    match carrier {
        LineageCarrier::Int => EvalType::Int,
        LineageCarrier::Bytes => EvalType::Bytes,
    }
}

fn identity(kind: DatumKind) -> ValueMetadata {
    ValueMetadata {
        kind,
        string_collation: None,
        decimal_declared_shape: None,
    }
}

// Same closed documented SQL flags as the checked C3b facts. Never project by
// truncating unknown flags. ENUM/SET tails and JSON conversion are not proved.
fn check_type(sql: &FieldType, demand: Demand) -> SeedResult<LineageCarrier> {
    const DOCUMENTED: u64 = (1 << 25) - 1;
    let excluded = u64::from(
        FieldTypeFlags::ENUM
            | FieldTypeFlags::SET
            | FieldTypeFlags::ENUM_SET_AS_INT
            | FieldTypeFlags::PARSE_TO_JSON,
    );
    if sql.is_array() || sql.raw_flags() & (excluded | !DOCUMENTED) != 0 {
        return Err(SeedError::Admission(
            "control ARRAY/hybrid/conversion/unknown flags",
        ));
    }
    let carrier = match sql.code() {
        FieldTypeCode::LongLong => LineageCarrier::Int,
        FieldTypeCode::Varchar
        | FieldTypeCode::VarString
        | FieldTypeCode::String
        | FieldTypeCode::TinyBlob
        | FieldTypeCode::MediumBlob
        | FieldTypeCode::LongBlob
        | FieldTypeCode::Blob => LineageCarrier::Bytes,
        _ => {
            return Err(SeedError::Admission(
                "actual SQL type is outside control lineage",
            ))
        }
    };
    match demand {
        Demand::PredicateInt if carrier != LineageCarrier::Int || sql.is_unsigned() => Err(
            SeedError::Admission("PredicateInt requires signed LongLong selected producers"),
        ),
        Demand::Value(Some(expected)) if carrier != expected => Err(SeedError::Admission(
            "unproved cross-family native control coercion",
        )),
        _ => Ok(carrier),
    }
}

fn literal_identity(
    value: &Datum,
    sql: &FieldType,
    carrier: LineageCarrier,
) -> SeedResult<ValueMetadata> {
    let result = match (value, carrier) {
        (Datum::Null, _) => identity(DatumKind::Null),
        (Datum::Int(_), LineageCarrier::Int) if !sql.is_unsigned() => identity(DatumKind::Int),
        (Datum::UInt(_), LineageCarrier::Int) if sql.is_unsigned() => identity(DatumKind::UInt),
        (Datum::String(value), LineageCarrier::Bytes) => ValueMetadata {
            kind: DatumKind::String,
            string_collation: Some(value.collation()),
            decimal_declared_shape: None,
        },
        (Datum::Bytes(_), LineageCarrier::Bytes) => identity(DatumKind::Bytes),
        _ => {
            return Err(SeedError::Admission(
                "native control literal kind/declaration mismatch",
            ))
        }
    };
    Ok(result)
}

fn input_identity(sql: &FieldType, carrier: LineageCarrier) -> ValueMetadata {
    match carrier {
        LineageCarrier::Int => identity(if sql.is_unsigned() {
            DatumKind::UInt
        } else {
            DatumKind::Int
        }),
        // DatumCell::datum_with_buffer uses set_string even for binary/blob.
        LineageCarrier::Bytes => ValueMetadata {
            kind: DatumKind::String,
            string_collation: Some(sql.collation()),
            decimal_declared_shape: None,
        },
    }
}

fn sql_and_collation(node: &Expression) -> SeedResult<(&FieldType, &CollationInfo)> {
    let (sql, collation) = match node {
        Expression::Constant(node) => (node.ret_type.as_ref(), &node.collation),
        Expression::Column(node) => (node.ret_type.as_ref(), &node.collation),
        Expression::ScalarFunction(node) => (node.ret_type.as_ref(), &node.collation),
        Expression::CorrelatedColumn(_) => {
            return Err(SeedError::Admission("correlated control source"))
        }
    };
    Ok((
        sql.ok_or(SeedError::Admission("missing native control declaration"))?,
        collation,
    ))
}

fn preflight<'a>(
    root: &'a Expression,
    row_schema: &[FieldType],
    ids: &[ResultMetaId],
    limits: ControlSourceLimits,
) -> SeedResult<(Vec<CheckedSource<'a>>, Vec<(ResultMetaId, usize)>)> {
    if limits.tree.max_nodes == 0 || limits.tree.max_depth == 0 || ids.len() > limits.tree.max_nodes
    {
        return Err(resource("control source node/depth policy exceeded").into());
    }
    if !matches!(root, Expression::ScalarFunction(_)) {
        return Err(SeedError::Admission(
            "control lineage requires a control root",
        ));
    }
    let unit = ids
        .first()
        .ok_or(SeedError::Admission("missing control producer IDs"))?
        .unit();
    let mut budget = SourceBudget {
        metadata: 0,
        literal: 0,
        limits,
    };
    // Charge flat source/index/output scaffolding BEFORE allocating it. This is
    // a logical policy, not a promise about Vec allocator capacity or peak.
    budget.array::<CheckedSource<'_>>(ids.len())?;
    budget.array::<Visit<'_>>(ids.len())?;
    budget.array::<BuildNode>(ids.len())?;
    budget.array::<LocalExpr>(ids.len())?;
    budget.array::<NodeMetadata>(ids.len())?;
    budget.array::<ProducerRecord>(ids.len())?;
    budget.array::<ControlProducerFact>(ids.len())?;
    budget.array::<(ResultMetaId, usize)>(ids.len())?;
    for field in row_schema {
        budget.field(field, 1)?;
    }
    let mut index = Vec::new();
    reserve(&mut index, ids.len())?;
    for (ordinal, id) in ids.iter().copied().enumerate() {
        if id.unit() != unit {
            return Err(SeedError::Admission("control IDs span multiple namespaces"));
        }
        index.push((id, ordinal));
    }
    index.sort_unstable_by_key(|entry| entry.0);
    if index.windows(2).any(|pair| pair[0].0 == pair[1].0) {
        return Err(SeedError::Admission("duplicate control producer ID"));
    }
    let mut pending = Vec::new();
    let mut checked = Vec::new();
    reserve(&mut pending, 1)?;
    reserve(&mut checked, ids.len())?;
    pending.push(Visit {
        node: root,
        depth: 1,
        demand: Demand::Value(None),
        escapes: true,
    });
    let mut scheduled = 1usize;
    while let Some(Visit {
        node,
        depth,
        demand,
        escapes,
    }) = pending.pop()
    {
        if checked.len() >= ids.len() {
            return Err(SeedError::Admission("missing all-node control producer ID"));
        }
        if lower::origin(node).is_some() {
            return Err(SeedError::Admission(
                "PB state inside SQL Datum control closure",
            ));
        }
        let (sql, collation) = sql_and_collation(node)?;
        let carrier = check_type(sql, demand)?;
        // Native node metadata, projected source declaration, immutable C fact.
        budget.field(sql, 3)?;
        if checked.is_empty() {
            budget.field(sql, 1)?; // detached root declaration
        }
        let (charset, collate) = collation.charset_and_collation();
        budget.metadata(charset.len())?;
        budget.metadata(collate.len())?;
        let mut selected = None;
        let (role, record, may_escape) = match node {
            Expression::Constant(constant) => {
                let value = constant
                    .literal_value()
                    .ok_or(SeedError::Admission("parameter/deferred control constant"))?;
                let record = literal_identity(value, sql, carrier)?;
                budget.literal(match value {
                    Datum::String(value) => value.bytes().len(),
                    Datum::Bytes(value) => value.len(),
                    _ => 0,
                })?;
                (ControlProducerRole::Constant, record, escapes)
            }
            Expression::Column(column) => {
                if column.virtual_expr.is_some() || column.correlated_col_unique_id != 0 {
                    return Err(SeedError::Admission("virtual/correlated control column"));
                }
                let index = usize::try_from(column.index)
                    .map_err(|_| SeedError::Admission("negative control column index"))?;
                if index >= row_schema.len() {
                    return Err(SeedError::Admission("control column outside row schema"));
                }
                // Equality is deliberately deferred until EVERY source payload
                // has passed the static policy, including unused schema types.
                budget.field(sql, 1)?;
                budget.array::<ColumnBinding>(1)?;
                budget.array::<usize>(1)?;
                budget.metadata(column.orig_name.len())?;
                (
                    ControlProducerRole::InputSlot,
                    input_identity(sql, carrier),
                    escapes,
                )
            }
            Expression::ScalarFunction(function) => {
                selected = Some(catalog::datum_control(function)?);
                let shape = Shape::from_name(function.func_name.lowercase())?;
                budget.metadata(function.func_name.original().len())?;
                budget.metadata(function.func_name.lowercase().len())?;
                let arity = function.args.len();
                scheduled = scheduled
                    .checked_add(arity)
                    .filter(|count| *count <= limits.tree.max_nodes && *count <= ids.len())
                    .ok_or_else(|| resource("control scheduled node policy exceeded"))?;
                let child_depth = depth
                    .checked_add(1)
                    .filter(|depth| *depth <= limits.tree.max_depth)
                    .ok_or_else(|| resource("control source depth policy exceeded"))?;
                reserve(&mut pending, arity)?;
                for (position, child) in function.args.iter().enumerate().rev() {
                    let predicate = shape.predicate_child(position, arity);
                    pending.push(Visit {
                        node: child,
                        depth: child_depth,
                        demand: if predicate || matches!(demand, Demand::PredicateInt) {
                            Demand::PredicateInt
                        } else {
                            Demand::Value(Some(carrier))
                        },
                        escapes: escapes && !predicate,
                    });
                }
                if matches!(shape, Shape::Boolean) {
                    check_type(sql, Demand::PredicateInt)?;
                    (
                        ControlProducerRole::ComputedBoolean,
                        identity(DatumKind::Int),
                        escapes,
                    )
                } else {
                    (
                        ControlProducerRole::SelectedControl,
                        identity(DatumKind::Null),
                        escapes && shape.generates_null(arity),
                    )
                }
            }
            Expression::CorrelatedColumn(_) => unreachable!(),
        };
        checked.push(CheckedSource {
            node,
            sql,
            collation,
            carrier,
            role,
            identity: record,
            selected,
            escapes: may_escape,
        });
    }
    if checked.len() != ids.len() {
        return Err(SeedError::Admission("extra all-node control producer IDs"));
    }
    // No FieldType equality (which snapshots slices) is performed before the
    // complete byte gate. Cached native collation is checked separately from Eq.
    for source in &checked {
        if let Expression::Column(column) = source.node {
            let declared = &row_schema[column.index as usize];
            if declared.collation() != source.sql.collation() || declared != source.sql {
                return Err(SeedError::Admission(
                    "control column differs from complete row declaration",
                ));
            }
        }
    }
    Ok((checked, index))
}

// Temporary flat construction records, not a second executable tree. Values and
// FieldTypes are moved into LocalExpr once, without recursive Clone/Drop.
enum BuildKind {
    Constant(ScalarValue, LiteralKind),
    Input(usize),
    Call(FunctionRef, usize),
}
struct BuildNode {
    field_type: tipb::FieldType,
    kind: BuildKind,
}

/// Bind the actual native Datum path at every node, with no evaluator probe or
/// SQL builder. Earlier SqlBuild effects are not undone. The caller supplies
/// unique producer IDs in all-node source preorder and a metadata-stable owner
/// interval, not cryptographically authenticated origin or atomic snapshots.
pub(crate) fn lower_typed_control_lineage(
    root: &Expression,
    row_schema: &[FieldType],
    new_collation: bool,
    producer_ids: &[ResultMetaId],
    limits: ControlSourceLimits,
) -> SeedResult<Arc<LoweredControlLineage>> {
    let (checked, id_index) = preflight(root, row_schema, producer_ids, limits)?;
    let count = checked.len();
    let mut build = Vec::new();
    let mut nodes = Vec::new();
    let mut records = Vec::new();
    let mut producers = Vec::new();
    let mut schema = Vec::new();
    let mut bindings = Vec::new();
    let mut binding_nodes = Vec::new();
    reserve(&mut build, count)?;
    reserve(&mut nodes, count)?;
    reserve(&mut records, count)?;
    reserve(&mut producers, count)?;
    for (ordinal, source) in checked.into_iter().enumerate() {
        let id = producer_ids[ordinal];
        let field_type = lower::projected_type(source.sql, None, new_collation)?;
        let mut display_name = None;
        let (kind, native_source, fact) = match source.node {
            Expression::Constant(constant) => {
                let value = constant.literal_value().ok_or(SeedError::Admission(
                    "control source changed during stable interval",
                ))?;
                let (value, actual) = to_scalar(value, eval_type(source.carrier))?;
                if actual != source.identity {
                    return Err(SeedError::Admission(
                        "control literal identity changed before transport",
                    ));
                }
                let tag = if source.identity.kind == DatumKind::String {
                    LiteralKind::Text
                } else {
                    LiteralKind::Typed
                };
                (
                    BuildKind::Constant(value, tag),
                    SourceNode::Literal {
                        kind: source.identity.kind,
                        subquery_ref_id: constant.subquery_ref_id,
                    },
                    ControlProducerFact::constant(ordinal, id, source.carrier),
                )
            }
            Expression::Column(column) => {
                let slot = bindings.len();
                reserve(&mut bindings, 1)?;
                reserve(&mut binding_nodes, 1)?;
                reserve(&mut schema, 1)?;
                bindings.push(ColumnBinding {
                    index: column.index as usize,
                    sql_type: snapshot_field_type(source.sql),
                });
                binding_nodes.push(ordinal);
                schema.push(field_type.clone());
                (
                    BuildKind::Input(slot),
                    SourceNode::Column {
                        index: column.index as usize,
                        id: column.id,
                        unique_id: column.unique_id,
                        original_name: column.orig_name.clone(),
                        hidden: column.is_hidden,
                        prefix: column.is_prefix,
                        in_operand: column.in_operand,
                        correlated_unique_id: column.correlated_col_unique_id,
                    },
                    ControlProducerFact::input_slot(ordinal, id, source.carrier),
                )
            }
            Expression::ScalarFunction(function) => {
                let selected = source
                    .selected
                    .ok_or(SeedError::Admission("missing checked control signature"))?;
                display_name = Some(function.func_name.clone());
                let fact = if source.role == ControlProducerRole::ComputedBoolean {
                    ControlProducerFact::computed_boolean(ordinal, id)
                } else {
                    ControlProducerFact::selected_control(ordinal, id, source.carrier)
                };
                (
                    BuildKind::Call(selected, function.args.len()),
                    SourceNode::Call { selected },
                    fact,
                )
            }
            Expression::CorrelatedColumn(_) => unreachable!(),
        };
        build.push(BuildNode { field_type, kind });
        producers.push(fact);
        records.push(ProducerRecord {
            id,
            carrier: source.carrier,
            role: source.role,
            identity: source.identity,
            may_escape_root: source.escapes,
            display_name,
        });
        nodes.push(NodeMetadata {
            sql_type: snapshot_field_type(source.sql),
            wire_origin: None,
            collation: source.collation.into(),
            source: native_source,
        });
    }
    let mut stack = Vec::new();
    reserve(&mut stack, count)?;
    for node in build.into_iter().rev() {
        let expr = match node.kind {
            BuildKind::Constant(value, literal_kind) => LocalExpr::Constant {
                value,
                field_type: node.field_type,
                literal_kind,
            },
            BuildKind::Input(slot) => LocalExpr::InputSlot {
                slot,
                field_type: node.field_type,
            },
            BuildKind::Call(function, arity) => {
                let mut args = Vec::new();
                reserve(&mut args, arity)?;
                for _ in 0..arity {
                    args.push(
                        stack
                            .pop()
                            .ok_or(SeedError::Admission("invalid control build stack"))?,
                    );
                }
                LocalExpr::Call {
                    function,
                    args: args.into_boxed_slice(),
                    return_type: node.field_type,
                    metadata: CallMetadata::None,
                }
            }
        };
        stack.push(expr);
    }
    if stack.len() != 1 {
        return Err(SeedError::Admission(
            "control lowering did not produce one root",
        ));
    }
    let expr = stack
        .pop()
        .ok_or(SeedError::Admission("missing control root"))?;
    let facts = ControlLineageFacts::sql_typed_row(&expr, &schema, producers, limits.tree)?;
    let mut detached_schema = Vec::new();
    reserve(&mut detached_schema, row_schema.len())?;
    detached_schema.extend(row_schema.iter().map(snapshot_field_type));
    let output_type = snapshot_field_type(&nodes[0].sql_type);
    Ok(Arc::new(LoweredControlLineage {
        core: LoweredSpec {
            expr,
            schema: schema.into_boxed_slice(),
            bindings: bindings.into_boxed_slice(),
            row_schema: detached_schema.into_boxed_slice(),
            nodes: nodes.into_boxed_slice(),
            output_type,
            new_collation,
            limits: limits.tree,
        },
        facts,
        records: records.into_boxed_slice(),
        id_index: id_index.into_boxed_slice(),
        binding_nodes: binding_nodes.into_boxed_slice(),
        source_limits: limits,
    }))
}

// This private leaf seam exists for hostile raw-Datum tests. Only NativeInputs
// supplies production values. It is not a native expression or dynamic-kind
// provider API; the same returned Datum is checked before B erases identity.
trait NativeDatumSource {
    fn schema(&self) -> &[tipb::FieldType];
    fn read_datum(
        &mut self,
        ctx: &mut EvalContext,
        slot: usize,
        row: InputRow,
        expected: &tipb::FieldType,
    ) -> LocalResult<Datum>;
}
impl NativeDatumSource for NativeInputs<'_> {
    fn schema(&self) -> &[tipb::FieldType] {
        self.binding_schema()
    }
    fn read_datum(
        &mut self,
        _ctx: &mut EvalContext,
        slot: usize,
        row: InputRow,
        expected: &tipb::FieldType,
    ) -> LocalResult<Datum> {
        self.read_native_datum(slot, row, expected)
    }
}

struct ControlInputs<'a, T: NativeDatumSource + ?Sized> {
    source: &'a mut T,
    spec: &'a LoweredControlLineage,
}
impl<T: NativeDatumSource + ?Sized> LocalRuntimeServices for ControlInputs<'_, T> {
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
        let ordinal = *self
            .spec
            .binding_nodes
            .get(slot)
            .ok_or_else(|| contract("unknown control input slot"))?;
        let record = &self.spec.records[ordinal];
        let value = self.source.read_datum(ctx, slot, row, expected)?;
        if !matches!(value, Datum::Null) {
            if value.kind() != record.identity.kind {
                return Err(contract(
                    "control demanded native kind differs before erasure",
                ));
            }
            if let Datum::String(value) = &value {
                if Some(value.collation()) != record.identity.string_collation {
                    return Err(contract(
                        "control demanded String collation differs before erasure",
                    ));
                }
            }
        }
        // NULL has no collation: its fixed producer contract is not retagged.
        let (scalar, _) = to_scalar(&value, eval_type(record.carrier))
            .map_err(|error| LocalError::BindingContract(error.to_string()))?;
        match scalar {
            ScalarValue::Int(_) => Ok(VectorValue::from_scalar(&scalar, 1)),
            ScalarValue::Bytes(bytes) => {
                let mut values =
                    ChunkedVecBytes::try_with_capacities(1, bytes.as_ref().map_or(0, Vec::len))
                        .map_err(|_| resource("control singleton Bytes reservation failed"))?;
                values.push_ref(bytes.as_deref());
                // C measures this actual owner before its next effect. Native
                // reading/B's copy already happened; no callback peak is claimed.
                Ok(VectorValue::Bytes(values))
            }
            _ => Err(contract("unexpected control transport carrier")),
        }
    }
}

/// Datums and their exact returned producer IDs remain tied to this own table.
/// Only detached FieldType snapshots may escape the immutable spec.
pub(crate) struct NativeControlBatch {
    values: Vec<Datum>,
    result_metadata: Vec<ResultMetaId>,
    spec: Arc<LoweredControlLineage>,
}
impl std::fmt::Debug for NativeControlBatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeControlBatch")
            .field("values", &self.values)
            .field("result_metadata", &self.result_metadata)
            .finish()
    }
}
impl NativeControlBatch {
    pub(crate) fn values(&self) -> &[Datum] {
        &self.values
    }
    pub(crate) fn result_metadata(&self) -> &[ResultMetaId] {
        &self.result_metadata
    }
    /// A separate metadata copy, outside the retained value-buffer policy.
    pub(crate) fn declared_type_snapshot(&self) -> FieldType {
        snapshot_field_type(&self.spec.core.output_type)
    }
    pub(crate) fn producer_type_snapshot(&self, occurrence: usize) -> Option<FieldType> {
        let (ordinal, _) = self
            .spec
            .record(*self.result_metadata.get(occurrence)?)
            .ok()?;
        Some(snapshot_field_type(&self.spec.core.nodes[ordinal].sql_type))
    }
}

pub(crate) struct PreparedControlLineage {
    spec: Arc<LoweredControlLineage>,
    program: LocalControlProgram,
    state: LocalEvalState,
    max_materialization_retained_bytes: usize,
}
impl PreparedControlLineage {
    pub(crate) fn compile(
        spec: Arc<LoweredControlLineage>,
        execution: ExecutionLimits,
        max_materialization_retained_bytes: usize,
    ) -> SeedResult<Self> {
        let program = compile_control_with_lineage(
            &spec.core.expr,
            &spec.core.schema,
            LocalCompileContext {
                limits: spec.core.limits,
            },
            &spec.facts,
        )?;
        Ok(Self {
            spec,
            program,
            state: LocalEvalState::with_limits(execution),
            max_materialization_retained_bytes,
        })
    }

    pub(crate) fn eval_selected(
        &mut self,
        ctx: &mut EvalContext,
        chunk: &Chunk,
        row_schema: &[FieldType],
        selection: &[usize],
    ) -> SeedResult<NativeControlBatch> {
        self.check_incoming_schema(row_schema)?;
        let mut native = NativeInputs::new(&self.spec.core, chunk, row_schema, selection)?;
        let mut inputs = ControlInputs {
            source: &mut native,
            spec: &self.spec,
        };
        let output = self.program.eval_with_bindings(
            &mut self.state,
            ctx,
            chunk.physical_rows(),
            selection,
            &mut inputs,
        )?;
        let (values, ids) = output.into_parts();
        self.materialize_parts(values, ids, selection.len())
    }

    fn check_incoming_schema(&self, row_schema: &[FieldType]) -> LocalResult<()> {
        // Existing NativeInputs::new calls FieldType Eq (slice snapshots).
        // Bound the ENTIRE incoming payload before that equality, not after it.
        let mut budget = SourceBudget {
            metadata: 0,
            literal: 0,
            limits: self.spec.source_limits,
        };
        for field in row_schema {
            budget.field(field, 1)?;
        }
        if row_schema.len() != self.spec.core.row_schema.len()
            || row_schema
                .iter()
                .zip(self.spec.core.row_schema.iter())
                .any(|(incoming, old)| incoming.collation() != old.collation())
        {
            return Err(LocalError::InvalidBatch(
                "native control cached collation/schema mismatch".into(),
            ));
        }
        Ok(())
    }

    fn accept_materialization(&self, bytes: usize) -> LocalResult<()> {
        if bytes > self.max_materialization_retained_bytes {
            Err(resource(
                "native control materialization retained-byte policy exceeded",
            ))
        } else {
            Ok(())
        }
    }

    // Private parts seam used only after this own program returns, plus hostile
    // validation tests. There is no public foreign-batch/table join operation.
    fn materialize_parts(
        &self,
        values: VectorValue,
        ids: Vec<ResultMetaId>,
        count: usize,
    ) -> SeedResult<NativeControlBatch> {
        let expected = eval_type(self.spec.records[0].carrier);
        if values.len() != count || ids.len() != count || values.eval_type() != expected {
            return Err(contract("control result values/IDs shape mismatch").into());
        }
        for (index, id) in ids.iter().copied().enumerate() {
            let (_, record) = self.spec.record(id)?;
            if !record.may_escape_root || eval_type(record.carrier) != expected {
                return Err(contract("control result producer cannot escape this root").into());
            }
            if record.identity.kind == DatumKind::Null
                && !scalar_is_null(values.get_scalar_ref(index))
            {
                return Err(contract("NULL producer ID attached to non-NULL control value").into());
            }
        }
        let source_bytes = vector_heap_bytes(&values)?;
        let id_bytes = array_bytes::<ResultMetaId>(ids.capacity())?;
        let sources = source_bytes
            .checked_add(id_bytes)
            .ok_or_else(|| resource("control source/ID size overflow"))?;
        let minimum = sources
            .checked_add(array_bytes::<Datum>(count)?)
            .ok_or_else(|| resource("control native output size overflow"))?;
        self.accept_materialization(minimum)?;
        let mut output = Vec::new();
        reserve(&mut output, count)?;
        let output_bytes = array_bytes::<Datum>(output.capacity())?;
        let fixed = sources
            .checked_add(output_bytes)
            .ok_or_else(|| resource("control materialization capacity overflow"))?;
        self.accept_materialization(fixed)?;
        let mut native_heap = 0usize;
        for (index, id) in ids.iter().copied().enumerate() {
            let (_, record) = self.spec.record(id)?;
            let scalar = values.get_scalar_ref(index);
            let next_minimum = match scalar {
                ScalarValueRef::Bytes(Some(bytes)) => bytes.len(),
                _ => 0,
            };
            let before = fixed
                .checked_add(native_heap)
                .and_then(|total| total.checked_add(next_minimum))
                .ok_or_else(|| resource("control native payload size overflow"))?;
            self.accept_materialization(before)?;
            // The ID, not equal bits/bytes/NULL or the root FT, selects metadata.
            let value = from_scalar(scalar, expected, &record.identity)?;
            let (value, heap) = measured_native_datum(value)?;
            native_heap = native_heap
                .checked_add(heap)
                .ok_or_else(|| resource("control native retained payload overflow"))?;
            self.accept_materialization(
                fixed
                    .checked_add(native_heap)
                    .ok_or_else(|| resource("control native coexistence overflow"))?,
            )?;
            output.push(value); // pre-reserved; no further container growth
        }
        // C's source buffer remains charged until this actual drop. There is no
        // all-owner/allocator peak promise during B copies or Vec reservations.
        drop(values);
        let published = id_bytes
            .checked_add(output_bytes)
            .and_then(|sum| sum.checked_add(native_heap))
            .ok_or_else(|| resource("control publication retained size overflow"))?;
        self.accept_materialization(published)?;
        Ok(NativeControlBatch {
            values: output,
            result_metadata: ids,
            spec: Arc::clone(&self.spec),
        })
    }

    #[cfg(test)]
    fn eval_test_native(
        &mut self,
        ctx: &mut EvalContext,
        physical_rows: usize,
        selection: &[usize],
        source: &mut impl NativeDatumSource,
    ) -> SeedResult<NativeControlBatch> {
        let mut inputs = ControlInputs {
            source,
            spec: &self.spec,
        };
        let output = self.program.eval_with_bindings(
            &mut self.state,
            ctx,
            physical_rows,
            selection,
            &mut inputs,
        )?;
        let (values, ids) = output.into_parts();
        self.materialize_parts(values, ids, selection.len())
    }
}

fn scalar_is_null(value: ScalarValueRef<'_>) -> bool {
    match value {
        ScalarValueRef::Int(value) => value.is_none(),
        ScalarValueRef::Bytes(value) => value.is_none(),
        _ => false,
    }
}

fn vector_heap_bytes(value: &VectorValue) -> LocalResult<usize> {
    let bytes = match value {
        VectorValue::Int(values) => values
            .capacity()
            .checked_mul(size_of::<i64>())
            .and_then(|bytes| bytes.checked_add(values.get_bit_vec().retained_heap_bytes()?)),
        VectorValue::Bytes(values) => values.retained_heap_bytes(),
        _ => return Err(contract("unadmitted retained control carrier")),
    };
    bytes.ok_or_else(|| resource("control vector retained size overflow"))
}

fn measured_native_datum(value: Datum) -> LocalResult<(Datum, usize)> {
    match value {
        Datum::String(value) => {
            let collation = value.collation();
            let bytes = value.into_bytes();
            let capacity = bytes.capacity();
            // Move the SAME Vec back: no decoder, to_vec, metadata guess or
            // second from_scalar. Spare capacity is measured, not inferred.
            Ok((Datum::String(StringDatum::new(bytes, collation)), capacity))
        }
        Datum::Bytes(bytes) => {
            let capacity = bytes.capacity();
            Ok((Datum::Bytes(bytes), capacity))
        }
        value @ (Datum::Null | Datum::Int(_) | Datum::UInt(_)) => Ok((value, 0)),
        _ => Err(contract("unadmitted native control materialization kind")),
    }
}

#[cfg(test)]
#[path = "lineage_tests.rs"]
mod tests;
