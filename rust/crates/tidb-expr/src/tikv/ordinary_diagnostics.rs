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

//! Private PLUS diagnostic views, not an error replacement or SQL activation.
//! Only this caller's fresh reported result is adapted against its own spec.
//! Owner-supplied source IDs provide consistency, not authentication.

use std::fmt::{self, Write as _};
use std::sync::Arc;

use tidb_chunk::chunk::Chunk;
use tidb_datatype::{Datum, DatumKind, FieldType, FieldTypeCode};
use tidb_query_datatype::{codec::data_type::VectorValue, expr::EvalContext};
use tidb_query_expr::local::{
    FunctionRef, InputRow, LocalError, LocalFailureSite, LocalFailureStage, OrdinaryCallSite,
    OrdinaryProfile, ReportedLocalFailure,
};

use crate::scalar_function::{arithmetic_symbol, binary_op_for_name};
use crate::EvalError;

use super::{
    LoweredIntPlusRow, NativeInputs, PlusInputs, PreparedIntPlusRow, SeedError, SeedResult,
    SourceNode, SourceShape,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct WarningEndpoint {
    pub(crate) warning_cnt: usize,
    pub(crate) stored_len: usize,
}
impl WarningEndpoint {
    fn capture(ctx: &EvalContext) -> Self {
        Self {
            warning_cnt: ctx.warnings.warning_cnt,
            stored_len: ctx.warnings.warnings.len(),
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct WarningEndpoints {
    pub(crate) before: WarningEndpoint,
    pub(crate) after: WarningEndpoint,
}

#[derive(Debug)]
pub(crate) enum JoinedPlusSite {
    Kernel {
        call: OrdinaryCallSite,
        row: InputRow,
    },
    Input {
        slot: usize,
        node_ordinal: usize,
        row: InputRow,
    },
}
impl JoinedPlusSite {
    pub(crate) fn node_ordinal(&self) -> usize {
        match self {
            Self::Kernel { call, .. } => call.ordinal(),
            Self::Input { node_ordinal, .. } => *node_ordinal,
        }
    }
    pub(crate) fn row(&self) -> InputRow {
        match self {
            Self::Kernel { row, .. } | Self::Input { row, .. } => *row,
        }
    }
    pub(crate) fn input_slot(&self) -> Option<usize> {
        match self {
            Self::Input { slot, .. } => Some(*slot),
            _ => None,
        }
    }
    pub(crate) fn call(&self) -> Option<&OrdinaryCallSite> {
        match self {
            Self::Kernel { call, .. } => Some(call),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CallerFailureStage {
    Preflight,
    Materialization,
}
#[derive(Debug)]
enum Cause {
    Caller {
        stage: CallerFailureStage,
        error: SeedError,
    },
    Runtime(ReportedLocalFailure),
}
#[derive(Debug)]
pub(crate) struct PlusFailure {
    cause: Cause,
    joined: Option<JoinedPlusSite>,
    native: Option<EvalError>,
}
impl PlusFailure {
    fn caller(stage: CallerFailureStage, error: SeedError) -> Self {
        Self {
            cause: Cause::Caller { stage, error },
            joined: None,
            native: None,
        }
    }
    pub(crate) fn reported(&self) -> Option<&ReportedLocalFailure> {
        match &self.cause {
            Cause::Runtime(report) => Some(report),
            _ => None,
        }
    }
    pub(crate) fn caller_error(&self) -> Option<&SeedError> {
        match &self.cause {
            Cause::Caller { error, .. } => Some(error),
            _ => None,
        }
    }
    pub(crate) fn caller_stage(&self) -> Option<CallerFailureStage> {
        match &self.cause {
            Cause::Caller { stage, .. } => Some(*stage),
            _ => None,
        }
    }
    pub(crate) fn joined_site(&self) -> Option<&JoinedPlusSite> {
        self.joined.as_ref()
    }
    pub(crate) fn native_error(&self) -> Option<&EvalError> {
        self.native.as_ref()
    }
    pub(crate) fn into_raw(self) -> SeedError {
        match self.cause {
            Cause::Caller { error, .. } => error,
            Cause::Runtime(report) => SeedError::Local(report.into_error()),
        }
    }
}

pub(crate) struct PlusEvaluation {
    outcome: Result<Vec<Datum>, PlusFailure>,
    warnings: WarningEndpoints,
}
impl PlusEvaluation {
    fn finish(
        before: WarningEndpoint,
        ctx: &EvalContext,
        outcome: Result<Vec<Datum>, PlusFailure>,
    ) -> Self {
        Self {
            outcome,
            warnings: WarningEndpoints {
                before,
                after: WarningEndpoint::capture(ctx),
            },
        }
    }
    pub(crate) fn warnings(&self) -> WarningEndpoints {
        self.warnings
    }
    pub(crate) fn outcome(&self) -> Result<&[Datum], &PlusFailure> {
        self.outcome.as_ref().map(Vec::as_slice)
    }
    pub(crate) fn into_parts(self) -> (Result<Vec<Datum>, PlusFailure>, WarningEndpoints) {
        (self.outcome, self.warnings)
    }
}

#[derive(Clone, Copy, Default)]
struct RenderFact {
    // None is native non-renderability, never a budget/allocator failure.
    operand_bytes: Option<usize>,
    overflow_bytes: Option<usize>,
    depth: usize,
}

pub(crate) struct PlusDiagnosticPlan {
    spec: Arc<LoweredIntPlusRow>,
    facts: Box<[RenderFact]>,
    max_rendered_bytes: usize,
}

fn resource() -> SeedError {
    LocalError::ResourceLimit("PLUS diagnostic plan budget/allocation exceeded".into()).into()
}
fn invalid() -> SeedError {
    SeedError::Admission("inconsistent PLUS diagnostic source facts")
}
fn signed(field: &FieldType) -> bool {
    field.code() == FieldTypeCode::LongLong && !field.is_unsigned() && !field.is_array()
}
fn integer_len(value: i64) -> usize {
    let mut n = value.unsigned_abs();
    let mut len = usize::from(value < 0) + 1;
    while n >= 10 {
        n /= 10;
        len += 1;
    }
    len
}
fn combined(
    left: Option<usize>,
    right: Option<usize>,
    op: &str,
    max: usize,
) -> SeedResult<Option<usize>> {
    let (Some(left), Some(right)) = (left, right) else {
        return Ok(None);
    };
    let len = left
        .checked_add(right)
        .and_then(|n| n.checked_add(op.len()))
        .and_then(|n| n.checked_add(4))
        .filter(|len| *len <= max)
        .ok_or_else(resource)?;
    Ok(Some(len))
}
fn display_symbol(shape: &SourceShape) -> Option<&'static str> {
    let SourceShape::Call { display_name, .. } = shape else {
        return None;
    };
    binary_op_for_name(display_name.lowercase()).and_then(arithmetic_symbol)
}

fn call_domain(spec: &LoweredIntPlusRow, ordinal: usize, call: &OrdinaryCallSite) -> bool {
    let Some(node) = spec.core.nodes.get(ordinal) else {
        return false;
    };
    let Some(SourceShape::Call { children, .. }) = spec.shapes.get(ordinal) else {
        return false;
    };
    let Some(Some(output)) = spec.outputs.get(ordinal) else {
        return false;
    };
    if !matches!(
        &node.source,
        SourceNode::Call {
            selected: FunctionRef::TiPb(tipb::ScalarFuncSig::PlusInt)
        }
    ) || !signed(&node.sql_type)
        || !signed(&output.sql_type)
        || output.sql_type != node.sql_type
        || output.identity.kind != DatumKind::Int
        || output.identity.string_collation.is_some()
        || output.identity.decimal_declared_shape.is_some()
        || children.iter().any(|child| {
            !spec
                .core
                .nodes
                .get(*child)
                .is_some_and(|node| signed(&node.sql_type))
        })
        || call.ordinal() != ordinal
        || u64::try_from(ordinal).ok() != Some(call.source().node())
        || call.profile() != spec.profiles.consumer()
    {
        return false;
    }
    match call.profile() {
        OrdinaryProfile::TypedRow => {
            node.wire_origin.is_none() && call.original_pb_signature().is_none()
        }
        OrdinaryProfile::PbRow => {
            call.original_pb_signature() == Some(203)
                && node.wire_origin.as_ref().is_some_and(|origin| {
                    origin.signature == Some(203) && origin.matches_effective_type(&node.sql_type)
                })
        }
        _ => false,
    }
}

impl PlusDiagnosticPlan {
    fn new(
        spec: Arc<LoweredIntPlusRow>,
        max_nodes: usize,
        max_depth: usize,
        max_bytes: usize,
    ) -> SeedResult<Self> {
        let count = spec.shapes.len();
        if count == 0 || count > max_nodes || max_depth == 0 {
            return Err(resource());
        }
        if count != spec.core.nodes.len()
            || count != spec.outputs.len()
            || count != spec.profiles.node_count()
            || !matches!(spec.shapes.first(), Some(SourceShape::Call { .. }))
        {
            return Err(invalid());
        }
        let mut facts = Vec::new();
        facts.try_reserve_exact(count).map_err(|_| resource())?;
        facts.resize(count, RenderFact::default());
        let mut parents = Vec::new();
        parents.try_reserve_exact(count).map_err(|_| resource())?;
        parents.resize(count, false);
        let sites = spec.profiles.call_sites();
        let mut call_index = sites.len();
        // The retained all-node preorder makes reverse order a postorder for
        // length facts. Verify unique forward edges instead of recursing.
        for ordinal in (0..count).rev() {
            let node = &spec.core.nodes[ordinal];
            if !signed(&node.sql_type) {
                return Err(invalid());
            }
            let mut fact = RenderFact {
                depth: 1,
                ..RenderFact::default()
            };
            match (&spec.shapes[ordinal], &node.source) {
                (SourceShape::Literal(value), SourceNode::Literal { kind, .. }) => {
                    if !matches!(
                        (value, kind),
                        (Some(_), DatumKind::Int) | (None, DatumKind::Null)
                    ) {
                        return Err(invalid());
                    }
                    fact.operand_bytes = Some(value.map_or(4, integer_len));
                }
                (
                    SourceShape::Column,
                    SourceNode::Column {
                        original_name,
                        unique_id,
                        ..
                    },
                ) => {
                    fact.operand_bytes = Some(if original_name.is_empty() {
                        7 + integer_len(*unique_id)
                    } else {
                        original_name.len()
                    });
                }
                (SourceShape::Call { children, .. }, SourceNode::Call { .. }) => {
                    call_index = call_index.checked_sub(1).ok_or_else(invalid)?;
                    if !call_domain(&spec, ordinal, &sites[call_index]) {
                        return Err(invalid());
                    }
                    for child in children {
                        if *child <= ordinal || *child >= count || parents[*child] {
                            return Err(invalid());
                        }
                        parents[*child] = true;
                    }
                    let [left, right] = children.map(|child| facts[child]);
                    fact.depth = left
                        .depth
                        .max(right.depth)
                        .checked_add(1)
                        .ok_or_else(resource)?;
                    fact.overflow_bytes =
                        combined(left.operand_bytes, right.operand_bytes, "+", max_bytes)?;
                    fact.operand_bytes = match display_symbol(&spec.shapes[ordinal]) {
                        Some(symbol) => {
                            combined(left.operand_bytes, right.operand_bytes, symbol, max_bytes)?
                        }
                        None => None,
                    };
                }
                _ => return Err(invalid()),
            }
            if fact.depth > max_depth || fact.operand_bytes.is_some_and(|len| len > max_bytes) {
                return Err(resource());
            }
            facts[ordinal] = fact;
        }
        if call_index != 0 || parents[0] || parents.iter().skip(1).any(|seen| !seen) {
            return Err(invalid());
        }
        if spec.binding_nodes.len() != spec.core.bindings.len()
            || spec.binding_nodes.len() != spec.core.schema.len()
        {
            return Err(invalid());
        }
        parents.fill(false);
        for (slot, ordinal) in spec.binding_nodes.iter().copied().enumerate() {
            let Some(node) = spec.core.nodes.get(ordinal) else {
                return Err(invalid());
            };
            let SourceNode::Column { index, .. } = &node.source else {
                return Err(invalid());
            };
            let binding = &spec.core.bindings[slot];
            if !matches!(spec.shapes[ordinal], SourceShape::Column)
                || parents[ordinal]
                || *index != binding.index
                || binding.sql_type != node.sql_type
                || spec.core.row_schema.get(*index) != Some(&binding.sql_type)
            {
                return Err(invalid());
            }
            parents[ordinal] = true;
        }
        if spec
            .shapes
            .iter()
            .enumerate()
            .any(|(ordinal, shape)| matches!(shape, SourceShape::Column) && !parents[ordinal])
        {
            return Err(invalid());
        }
        Ok(Self {
            spec,
            facts: facts.into_boxed_slice(),
            max_rendered_bytes: max_bytes,
        })
    }

    fn join(
        &self,
        report: &ReportedLocalFailure,
        physical_rows: usize,
        selection: &[usize],
    ) -> Option<JoinedPlusSite> {
        let row_valid = |row: &InputRow| {
            row.input_row < physical_rows && selection.get(row.occurrence) == Some(&row.input_row)
        };
        match (report.stage(), report.site()?) {
            (LocalFailureStage::Kernel, LocalFailureSite::Kernel { call, row })
                if row_valid(row) =>
            {
                let sites = self.spec.profiles.call_sites();
                let index = sites
                    .binary_search_by_key(&call.ordinal(), OrdinaryCallSite::ordinal)
                    .ok()?;
                if sites.get(index)? != call || !call_domain(&self.spec, call.ordinal(), call) {
                    return None;
                }
                Some(JoinedPlusSite::Kernel {
                    call: call.clone(),
                    row: *row,
                })
            }
            (LocalFailureStage::Input, LocalFailureSite::InputSlot { slot, row })
                if row_valid(row) =>
            {
                let ordinal = *self.spec.binding_nodes.get(*slot)?;
                let binding = self.spec.core.bindings.get(*slot)?;
                let node = self.spec.core.nodes.get(ordinal)?;
                let SourceNode::Column { index, .. } = &node.source else {
                    return None;
                };
                if !matches!(self.spec.shapes.get(ordinal), Some(SourceShape::Column))
                    || *index != binding.index
                    || binding.sql_type != node.sql_type
                {
                    return None;
                }
                Some(JoinedPlusSite::Input {
                    slot: *slot,
                    node_ordinal: ordinal,
                    row: *row,
                })
            }
            _ => None,
        }
    }

    // Private and called only on the own program's just-returned result. A
    // matching source ID alone cannot authenticate an arbitrary foreign report.
    fn failure(
        &self,
        report: ReportedLocalFailure,
        physical_rows: usize,
        selection: &[usize],
    ) -> PlusFailure {
        let joined = self.join(&report, physical_rows, selection);
        let native = match &joined {
            Some(JoinedPlusSite::Kernel { call, .. }) => {
                // Code is consulted only after exact call/row/domain verification.
                if report.sql_error_code() == Some(1690) {
                    self.render_overflow(call.ordinal())
                } else {
                    None
                }
            }
            _ => None,
        };
        PlusFailure {
            cause: Cause::Runtime(report),
            joined,
            native,
        }
    }

    fn render_overflow(&self, ordinal: usize) -> Option<EvalError> {
        let fact = self.facts.get(ordinal)?;
        let Some(expected) = fact.overflow_bytes else {
            return Some(EvalError::IntOverflow);
        };
        if expected > self.max_rendered_bytes {
            return None;
        }
        let mut output = BoundedText {
            value: String::new(),
            limit: expected,
        };
        output.value.try_reserve_exact(expected).ok()?;
        let mut work = Vec::new();
        work.try_reserve_exact(self.facts.len().checked_mul(5)?)
            .ok()?;
        work.push(Token::Node(ordinal, true));
        let mut visited = 0usize;
        while let Some(token) = work.pop() {
            match token {
                Token::Text(text) => output.write_str(text).ok()?,
                Token::Int(value) => write!(&mut output, "{value}").ok()?,
                Token::Node(node, own_error) => {
                    visited = visited.checked_add(1)?;
                    if visited > self.facts.len() {
                        return None;
                    }
                    match (&self.spec.shapes[node], &self.spec.core.nodes[node].source) {
                        (SourceShape::Literal(Some(value)), _) => work.push(Token::Int(*value)),
                        (SourceShape::Literal(None), _) => work.push(Token::Text("NULL")),
                        (
                            SourceShape::Column,
                            SourceNode::Column {
                                original_name,
                                unique_id,
                                ..
                            },
                        ) => {
                            if original_name.is_empty() {
                                work.push(Token::Int(*unique_id));
                                work.push(Token::Text("Column#"));
                            } else {
                                work.push(Token::Text(original_name));
                            }
                        }
                        (shape @ SourceShape::Call { children, .. }, _) => {
                            let symbol = if own_error {
                                "+"
                            } else {
                                display_symbol(shape)?
                            };
                            work.push(Token::Text(")"));
                            work.push(Token::Node(children[1], false));
                            work.push(Token::Text(" "));
                            work.push(Token::Text(symbol));
                            work.push(Token::Text(" "));
                            work.push(Token::Node(children[0], false));
                            work.push(Token::Text("("));
                        }
                        _ => return None,
                    }
                }
            }
        }
        if output.value.len() != expected {
            return None;
        }
        Some(EvalError::DataOutOfRange {
            value: "BIGINT",
            expression: output.value,
        })
    }
}

enum Token<'a> {
    Node(usize, bool),
    Text(&'a str),
    Int(i64),
}
struct BoundedText {
    value: String,
    limit: usize,
}
impl fmt::Write for BoundedText {
    fn write_str(&mut self, text: &str) -> fmt::Result {
        if self
            .value
            .len()
            .checked_add(text.len())
            .is_none_or(|len| len > self.limit)
        {
            return Err(fmt::Error);
        }
        self.value.push_str(text);
        Ok(())
    }
}

impl PreparedIntPlusRow {
    pub(crate) fn prepare_diagnostics(
        &self,
        max_nodes: usize,
        max_depth: usize,
        max_rendered_bytes: usize,
    ) -> SeedResult<PlusDiagnosticPlan> {
        PlusDiagnosticPlan::new(
            Arc::clone(&self.spec),
            max_nodes,
            max_depth,
            max_rendered_bytes,
        )
    }

    pub(crate) fn eval_selected_reported(
        &mut self,
        ctx: &mut EvalContext,
        chunk: &Chunk,
        row_schema: &[FieldType],
        selection: &[usize],
        diagnostics: &PlusDiagnosticPlan,
    ) -> PlusEvaluation {
        // Before ALL preflight; no outer early return can bypass the after sample.
        let before = WarningEndpoint::capture(ctx);
        let outcome = self.run_reported(ctx, chunk, row_schema, selection, diagnostics);
        PlusEvaluation::finish(before, ctx, outcome)
    }

    fn check_diagnostics(&self, diagnostics: &PlusDiagnosticPlan) -> Result<(), PlusFailure> {
        if !Arc::ptr_eq(&self.spec, &diagnostics.spec) {
            return Err(PlusFailure::caller(
                CallerFailureStage::Preflight,
                SeedError::Admission("PLUS diagnostic plan belongs to another prepared spec"),
            ));
        }
        Ok(())
    }

    fn run_reported(
        &mut self,
        ctx: &mut EvalContext,
        chunk: &Chunk,
        row_schema: &[FieldType],
        selection: &[usize],
        diagnostics: &PlusDiagnosticPlan,
    ) -> Result<Vec<Datum>, PlusFailure> {
        self.check_diagnostics(diagnostics)?;
        let mut native = NativeInputs::new(&self.spec.core, chunk, row_schema, selection)
            .map_err(|error| PlusFailure::caller(CallerFailureStage::Preflight, error.into()))?;
        let mut inputs = PlusInputs(&mut native);
        let result = self.program.eval_with_bindings_reported(
            &mut self.state,
            ctx,
            chunk.physical_rows(),
            selection,
            &mut inputs,
        );
        self.finish_reported(diagnostics, chunk.physical_rows(), selection, result)
    }

    fn finish_reported(
        &self,
        diagnostics: &PlusDiagnosticPlan,
        physical_rows: usize,
        selection: &[usize],
        result: Result<VectorValue, ReportedLocalFailure>,
    ) -> Result<Vec<Datum>, PlusFailure> {
        match result {
            Ok(output) => self
                .materialize(output, selection.len())
                .map_err(|error| PlusFailure::caller(CallerFailureStage::Materialization, error)),
            Err(report) => Err(diagnostics.failure(report, physical_rows, selection)),
        }
    }

    #[cfg(test)]
    fn eval_test_reported_native(
        &mut self,
        ctx: &mut EvalContext,
        physical_rows: usize,
        selection: &[usize],
        diagnostics: &PlusDiagnosticPlan,
        source: &mut impl super::NativeDatumSource,
    ) -> PlusEvaluation {
        let before = WarningEndpoint::capture(ctx);
        let outcome =
            self.run_test_reported_native(ctx, physical_rows, selection, diagnostics, source);
        PlusEvaluation::finish(before, ctx, outcome)
    }
    #[cfg(test)]
    fn run_test_reported_native(
        &mut self,
        ctx: &mut EvalContext,
        physical_rows: usize,
        selection: &[usize],
        diagnostics: &PlusDiagnosticPlan,
        source: &mut impl super::NativeDatumSource,
    ) -> Result<Vec<Datum>, PlusFailure> {
        self.check_diagnostics(diagnostics)?;
        let mut inputs = PlusInputs(source);
        let result = self.program.eval_with_bindings_reported(
            &mut self.state,
            ctx,
            physical_rows,
            selection,
            &mut inputs,
        );
        self.finish_reported(diagnostics, physical_rows, selection, result)
    }
}

#[cfg(test)]
#[path = "ordinary_diagnostics_tests.rs"]
mod tests;
