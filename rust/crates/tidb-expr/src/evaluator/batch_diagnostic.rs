// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Explicit fallible diagnostic completions in the existing host protocol.
use super::*;
use crate::scalar_function::dynamic_diagnostic::{
    DiagnosticState, DiagnosticStep, DynamicDiagnostic,
};
#[derive(Debug)]
pub(in super::super) struct CompiledDiagnosticParent {
    pub slot: usize,
    pub main_arity: usize,
    pub arguments: Box<[usize]>,
    pub plan: DynamicDiagnostic,
}
#[derive(Clone, Copy)]
enum MathOverflow {
    Double,
    BigInt,
}
impl MathOverflow {
    fn raw(self) -> EvalError {
        match self {
            Self::Double => EvalError::FloatOverflow,
            Self::BigInt => EvalError::IntOverflow,
        }
    }
    fn value(self) -> &'static str {
        match self {
            Self::Double => "DOUBLE",
            Self::BigInt => "BIGINT",
        }
    }
}
#[derive(Clone, Copy)]
struct WindowDiagnostic {
    position: usize,
    row: InputRow,
}
pub(super) struct DiagnosticTask {
    window: Option<WindowDiagnostic>,
    state: Option<DiagnosticState>,
    overflow: MathOverflow,
    base: usize,
}
impl DiagnosticTask {
    pub(super) fn next_argument(&self) -> LocalResult<Option<usize>> {
        self.state
            .as_ref()
            .and_then(DiagnosticState::pending_constant)
            .map(|i| {
                self.base
                    .checked_add(i)
                    .ok_or_else(|| resource("diagnostic argument index overflow"))
            })
            .transpose()
    }
    #[cfg(test)]
    pub(super) fn retained_bytes(&self) -> LocalResult<usize> {
        std::mem::size_of::<Self>()
            .checked_add(match &self.state {
                Some(s) => s
                    .retained_bytes()
                    .map_err(|_| resource("diagnostic state size overflow"))?,
                None => 0,
            })
            .ok_or_else(|| resource("diagnostic task size overflow"))
    }
}

impl NativeHostMetadata {
    fn native_output(&self) -> Option<(usize, NativeValueRole)> {
        match self {
            Self::Arguments(meta) => meta.output_role.map(|role| (meta.output_node, role)),
            Self::TypedContext(meta) => Some((meta.output_node, meta.output_role)),
            Self::ContextValue {
                output_node,
                output_role,
                ..
            }
            | Self::EagerValues {
                output_node,
                output_role,
                ..
            }
            | Self::Like {
                output_node,
                output_role,
                ..
            }
            | Self::RawValues {
                output_node,
                output_role,
                ..
            }
            | Self::UnaryWindow {
                output_node,
                output_role,
                ..
            }
            | Self::Selection {
                output_node,
                output_role,
                ..
            }
            | Self::Comparison {
                output_node,
                output_role,
                ..
            }
            | Self::Arithmetic {
                output_node,
                output_role,
                ..
            }
            | Self::Math {
                output_node,
                output_role,
                ..
            }
            | Self::ValueCast {
                output_node,
                output_role,
                ..
            } => Some((*output_node, *output_role)),
            Self::In(v) => Some((v.output_node, v.output_role)),
            Self::Interval(v) => Some((v.output_node, v.output_role)),
            Self::Extremum(v) => Some((v.output_node, v.output_role)),
            Self::Charset(v) => v.output_role.map(|role| (v.output_node, role)),
            _ => None,
        }
    }
    fn native_argument_node(&self, index: usize) -> Option<usize> {
        let single = |node: &usize| (index == 0).then_some(*node);
        match self {
            Self::Arguments(meta) => meta.arguments.get(index).copied(),
            Self::TypedContext(meta) => meta.argument.filter(|_| index == 0),
            Self::ContextValue { argument, .. } => argument.filter(|_| index == 0),
            Self::EagerValues { arguments, .. } => arguments.get(index).copied(),
            Self::Math { argument_node, .. }
            | Self::ValueCast { argument_node, .. }
            | Self::UnaryWindow { argument_node, .. } => single(argument_node),
            Self::Like { arguments, .. } => arguments.get(index).copied(),
            Self::Selection { argument_nodes, .. } | Self::DecimalFast { argument_nodes, .. } => {
                argument_nodes.get(index).copied()
            }
            Self::Comparison { argument_nodes, .. } | Self::Arithmetic { argument_nodes, .. } => {
                argument_nodes.get(index).copied()
            }
            Self::RawValues {
                left_node,
                right_node,
                ..
            } => match index {
                0 => Some(*left_node),
                1 => *right_node,
                _ => None,
            },
            Self::In(v) => v.arguments.get(index).map(|a| a.node),
            Self::Interval(v) => v.arguments.get(index).map(|a| a.node),
            Self::Extremum(v) => v.arguments.get(index).copied(),
            Self::Charset(v) => v.arguments.get(index).copied(),
            _ => None,
        }
    }
}
impl<'a> NativeBatchHosts<'a> {
    pub(in super::super) fn validate_diagnostic_slots(
        catalog: &HostCatalog,
        metadata: &[NativeHostMetadata],
        numerics: &[CompiledNumericParent],
        diagnostics: &[CompiledDiagnosticParent],
        slots: &[usize],
    ) -> LocalResult<()> {
        if slots.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(binding("diagnostic slots must be strictly increasing"));
        }
        for &index in slots {
            let slot = catalog.slot(index)?;
            if catalog.sql_batch_operation(slot)?.is_windowed() {
                return Err(binding("diagnostic subtree must use row demand"));
            }
            let meta = metadata
                .get(index)
                .ok_or_else(|| binding("diagnostic metadata absent"))?;
            if !matches!(meta.native_output(), Some((_, NativeValueRole::CharInput))) {
                return Err(binding("diagnostic producer lacks its native input role"));
            }
            if matches!(
                meta,
                NativeHostMetadata::TypedContext(NativeTypedContext {
                    cache_slot: Some(_),
                    ..
                })
            ) {
                return Err(binding(
                    "diagnostic typed reads cannot use invocation broadcasts",
                ));
            }
            let signature = catalog.signature(slot)?;
            for argument in 0..signature.arg_types.len() {
                if Self::diagnostic_argument_node_from(
                    metadata,
                    numerics,
                    diagnostics,
                    slot,
                    argument,
                )
                .is_none()
                {
                    return Err(binding("diagnostic argument provenance absent"));
                }
            }
        }
        Ok(())
    }

    pub(in super::super) fn with_diagnostic_slots(mut self, slots: &'a [usize]) -> Self {
        self.diagnostic_slots = slots;
        self
    }
    fn diagnostic_output(&self, slot: HostSlot) -> LocalResult<Option<usize>> {
        if self.diagnostic_slots.binary_search(&slot.index()).is_err() {
            return Ok(None);
        }
        match self
            .metadata
            .get(slot.index())
            .and_then(NativeHostMetadata::native_output)
        {
            Some((node, NativeValueRole::CharInput)) => Ok(Some(node)),
            _ => Err(binding("diagnostic producer metadata changed")),
        }
    }
    pub(super) fn emit_diagnostic_result(
        &mut self,
        text: Option<String>,
        node: usize,
        row: InputRow,
    ) -> LocalResult<VectorValue> {
        let store = self
            .native_values
            .ok_or_else(|| binding("diagnostic store absent"))?;
        let origin = NativeValueOrigin { node, row };
        let token = if let Some(text) = text {
            store.borrow_mut().insert_diagnostic_text(text, origin)?
        } else {
            store.borrow_mut().insert_diagnostic_unavailable(origin)?
        };
        let mut values = ChunkedVecBytes::try_with_capacities(1, token.len())
            .map_err(|e| LocalError::ResourceLimit(e.to_string()))?;
        values.push_ref(Some(&token));
        Ok(VectorValue::Bytes(values))
    }
    fn emit_diagnostic_unavailable(
        &mut self,
        node: usize,
        row: InputRow,
    ) -> LocalResult<VectorValue> {
        self.emit_diagnostic_result(None, node, row)
    }
    fn capture_diagnostic_failure(
        &mut self,
        error: LocalError,
        id: Option<&HostTaskId>,
        node: usize,
        row: InputRow,
    ) -> LocalResult<VectorValue> {
        let Some(native) = self.native_error.as_ref() else {
            return Err(error);
        };
        // Runtime resource/contract failures cannot be swallowed by a SQL
        // diagnostic's ordinary .ok() boundary.
        if matches!(native, EvalError::ExpressionRuntimeFailure(_)) {
            return Err(error);
        }
        self.native_error.take();
        if let Some(id) = id {
            self.cancel(id);
        }
        self.emit_diagnostic_unavailable(node, row)
    }
    pub(super) fn diagnostic_start(
        &mut self,
        ctx: &mut EvalContext,
        invocation: HostInvocation<'_>,
    ) -> LocalResult<HostStart> {
        let output = self.diagnostic_output(invocation.slot)?;
        if output.is_some() && self.native_error.is_some() {
            return Err(binding("diagnostic provider retained a terminal error"));
        }
        match self.start_sql(ctx, invocation) {
            Err(error) if output.is_some() => self
                .capture_diagnostic_failure(error, None, output.unwrap(), invocation.row)
                .map(HostStart::Ready),
            result => result,
        }
    }
    pub(super) fn diagnostic_resume(
        &mut self,
        ctx: &mut EvalContext,
        id: &HostTaskId,
        reply: HostArgReply<'_>,
    ) -> LocalResult<HostStep> {
        let task = self.task(id)?;
        let (slot, row) = (task.slot, task.row);
        let is_diagnostic_task = matches!(task.demand, SqlDemand::Diagnostic(_));
        let output = self.diagnostic_output(slot)?;
        if is_diagnostic_task || matches!(task.demand, SqlDemand::Numeric(_)) {
            let result = if is_diagnostic_task {
                self.resume_math_diagnostic(id, reply)
            } else {
                self.resume_numeric_parent(id, reply)
            };
            return match result {
                Err(error) if output.is_some() => self
                    .capture_diagnostic_failure(error, Some(id), output.unwrap(), row)
                    .map(HostStep::Ready),
                result => result,
            };
        }
        if let Some(node) = output {
            if self.native_error.is_some() {
                return Err(binding("diagnostic provider retained a terminal error"));
            }
            self.validate_row_reply(id, reply)?;
            let ScalarValueRef::Bytes(Some(token)) = reply.values.get_scalar_ref(0) else {
                return Err(binding("diagnostic operand cannot be wire NULL"));
            };
            let argument = self
                .diagnostic_argument_node(slot, reply.index)
                .ok_or_else(|| binding("diagnostic argument source absent"))?;
            let unavailable = self
                .native_values
                .ok_or_else(|| binding("diagnostic store absent"))?
                .borrow()
                .diagnostic_unavailable(
                    token,
                    NativeValueOrigin {
                        node: argument,
                        row,
                    },
                )?;
            if unavailable {
                // Authenticate and consume this real failure before publishing
                // the enclosing failure; no skipped sibling/body may execute.
                self.cancel(id);
                return self
                    .emit_diagnostic_unavailable(node, row)
                    .map(HostStep::Ready);
            }
        }
        match self.resume_sql(ctx, id, reply) {
            Err(error) if output.is_some() => self
                .capture_diagnostic_failure(error, Some(id), output.unwrap(), row)
                .map(HostStep::Ready),
            result => result,
        }
    }
}

impl<'a> NativeBatchHosts<'a> {
    pub(in super::super) fn validate_diagnostic_parents(
        catalog: &HostCatalog,
        metadata: &[NativeHostMetadata],
        parents: &[CompiledDiagnosticParent],
    ) -> LocalResult<()> {
        if parents.windows(2).any(|p| p[0].slot >= p[1].slot) {
            return Err(binding("diagnostic parents must be strictly increasing"));
        }
        for p in parents {
            let slot = catalog.slot(p.slot)?;
            let (main_arity, valid_operation) = match metadata.get(p.slot) {
                Some(NativeHostMetadata::EagerValues {
                    arguments, kernel, ..
                }) => (
                    arguments.len(),
                    matches!(kernel.name(), "cot" | "exp" | "pow" | "power" | "abs")
                        && catalog.sql_batch_operation(slot)?
                            == SqlBatchHostOperation::EagerValuesNative,
                ),
                Some(NativeHostMetadata::Math { name, .. }) => (
                    1,
                    name.as_ref() == "abs"
                        && catalog.sql_batch_operation(slot)?
                            == SqlBatchHostOperation::EagerValuesNative,
                ),
                Some(NativeHostMetadata::UnaryWindow {
                    operation: SqlBatchHostOperation::AbsIntWindowNative,
                    ..
                }) => (
                    1,
                    catalog.sql_batch_operation(slot)?
                        == SqlBatchHostOperation::EagerValuesWindowNative,
                ),
                _ => {
                    return Err(binding(
                        "diagnostic parent requires its original numeric source",
                    ));
                }
            };
            if !valid_operation
                || main_arity != p.main_arity
                || main_arity.checked_add(p.arguments.len())
                    != Some(catalog.signature(slot)?.arg_types.len())
            {
                return Err(binding("diagnostic parent graph differs"));
            }
        }
        Ok(())
    }

    pub(in super::super) fn with_diagnostic_parents(
        mut self,
        parents: &'a [CompiledDiagnosticParent],
    ) -> Self {
        self.diagnostic_parents = parents;
        self
    }
    pub(super) fn diagnostic_parent(&self, slot: HostSlot) -> Option<&'a CompiledDiagnosticParent> {
        self.diagnostic_parents
            .binary_search_by_key(&slot.index(), |p| p.slot)
            .ok()
            .map(|i| &self.diagnostic_parents[i])
    }
    fn diagnostic_argument_node_from(
        metadata: &[NativeHostMetadata],
        numerics: &[CompiledNumericParent],
        diagnostics: &[CompiledDiagnosticParent],
        slot: HostSlot,
        index: usize,
    ) -> Option<usize> {
        metadata
            .get(slot.index())
            .and_then(|m| m.native_argument_node(index))
            .or_else(|| {
                numerics
                    .binary_search_by_key(&slot.index(), |p| p.slot)
                    .ok()
                    .and_then(|i| numerics[i].arguments.get(index).copied())
            })
            .or_else(|| {
                diagnostics
                    .binary_search_by_key(&slot.index(), |p| p.slot)
                    .ok()
                    .and_then(|i| {
                        index
                            .checked_sub(diagnostics[i].main_arity)
                            .and_then(|arg| diagnostics[i].arguments.get(arg).copied())
                    })
            })
    }

    fn diagnostic_argument_node(&self, slot: HostSlot, index: usize) -> Option<usize> {
        Self::diagnostic_argument_node_from(
            self.metadata,
            self.numeric_parents,
            self.diagnostic_parents,
            slot,
            index,
        )
    }
    pub(super) fn begin_math_diagnostic(
        &mut self,
        id: &HostTaskId,
        error: EvalError,
    ) -> LocalResult<HostStep> {
        self.begin_math_diagnostic_scope(id, error, None)
    }
    pub(super) fn begin_window_math_diagnostic(
        &mut self,
        id: &HostTaskId,
        position: usize,
        error: EvalError,
    ) -> LocalResult<HostStep> {
        let task = self.task(id)?;
        let SqlDemand::WindowUnary { rows } = &task.demand else {
            return Err(binding("window math diagnostic lost its source phase"));
        };
        let input_row = *rows
            .get(position)
            .ok_or_else(|| binding("window diagnostic position outside source"))?;
        let occurrence = task
            .row
            .occurrence
            .checked_add(position)
            .ok_or_else(|| resource("window diagnostic occurrence overflow"))?;
        self.begin_math_diagnostic_scope(
            id,
            error,
            Some(WindowDiagnostic {
                position,
                row: InputRow {
                    occurrence,
                    input_row,
                },
            }),
        )
    }
    fn begin_math_diagnostic_scope(
        &mut self,
        id: &HostTaskId,
        error: EvalError,
        window: Option<WindowDiagnostic>,
    ) -> LocalResult<HostStep> {
        let task = self.task(id)?;
        let Some(parent) = self.diagnostic_parent(task.slot) else {
            return self.native_failure(error);
        };
        let name = match self.metadata.get(task.slot.index()) {
            Some(NativeHostMetadata::EagerValues { kernel, .. }) => kernel.name(),
            Some(NativeHostMetadata::Math { name, .. }) => name.as_ref(),
            Some(NativeHostMetadata::UnaryWindow {
                operation: SqlBatchHostOperation::AbsIntWindowNative,
                ..
            }) => "abs",
            _ => return Err(binding("diagnostic parent metadata absent")),
        };
        let overflow = match (name, &error) {
            ("cot" | "exp" | "pow" | "power", EvalError::FloatOverflow) => MathOverflow::Double,
            ("abs", EvalError::IntOverflow) => MathOverflow::BigInt,
            _ => return self.native_failure(error),
        };

        let state = parent.plan.state();
        self.slots[id.slot]
            .task
            .as_mut()
            .expect("authenticated diagnostic task")
            .demand = SqlDemand::Diagnostic(Box::new(DiagnosticTask {
            state: Some(state),
            window,
            overflow,
            base: parent.main_arity,
        }));
        self.drive_math_diagnostic(id, None)
    }
    fn resume_math_diagnostic(
        &mut self,
        id: &HostTaskId,
        reply: HostArgReply<'_>,
    ) -> LocalResult<HostStep> {
        let task = self.task(id)?;
        if matches!(&task.demand,SqlDemand::Diagnostic(state) if state.window.is_some()) {
            return Err(binding(
                "window diagnostic requires its scoped window reply",
            ));
        }
        self.validate_row_reply(id, reply)?;
        let row = task.row;
        self.resume_math_diagnostic_value(id, reply, row)
    }
    pub(super) fn resume_window_math_diagnostic(
        &mut self,
        id: &HostTaskId,
        reply: HostWindowArgReply<'_>,
    ) -> LocalResult<HostStep> {
        let task = self.task(id)?;
        let SqlDemand::Diagnostic(state) = &task.demand else {
            return Err(binding("window diagnostic demand differs"));
        };
        let window = state
            .window
            .ok_or_else(|| binding("row diagnostic cannot use a window reply"))?;
        if reply.occurrence_start != window.row.occurrence
            || reply.physical_rows != [window.row.input_row]
        {
            return Err(binding("window diagnostic occurrence differs"));
        }
        let row_reply = HostArgReply {
            index: reply.index,
            field_type: reply.field_type,
            values: reply.values,
        };
        self.validate_row_reply(id, row_reply)?;
        self.resume_math_diagnostic_value(id, row_reply, window.row)
    }
    fn resume_math_diagnostic_value(
        &mut self,
        id: &HostTaskId,
        reply: HostArgReply<'_>,
        row: InputRow,
    ) -> LocalResult<HostStep> {
        let slot = self.task(id)?.slot;
        let ScalarValueRef::Bytes(Some(token)) = reply.values.get_scalar_ref(0) else {
            return Err(binding("diagnostic reply cannot be wire NULL"));
        };
        let origin = NativeValueOrigin {
            node: self
                .diagnostic_argument_node(slot, reply.index)
                .ok_or_else(|| binding("diagnostic reply source absent"))?,
            row,
        };
        let store = self
            .native_values
            .ok_or_else(|| binding("diagnostic store absent"))?;
        store.borrow().diagnostic_text(token, origin)?;

        // No source capability or callback is replayed. The diagnostic state
        // is consumed by drive before a formatter can unwind.
        let text = store
            .borrow()
            .diagnostic_text(token, origin)?
            .map(str::to_owned);
        let result = self.drive_math_diagnostic(id, Some(text.as_deref()));
        store.borrow().diagnostic_text(token, origin)?;
        result
    }
    fn drive_math_diagnostic(
        &mut self,
        id: &HostTaskId,
        text: Option<Option<&str>>,
    ) -> LocalResult<HostStep> {
        let task = self.task(id)?;
        let parent = self
            .diagnostic_parent(task.slot)
            .ok_or_else(|| binding("diagnostic parent absent"))?;

        let SqlDemand::Diagnostic(task) = &mut self.slots[id.slot]
            .task
            .as_mut()
            .expect("authenticated diagnostic task")
            .demand
        else {
            return Err(binding("diagnostic demand differs"));
        };
        let Some(mut state) = task.state.take() else {
            return Err(binding("diagnostic state already consumed"));
        };
        let overflow = task.overflow;
        let window = task.window;
        let result = (|| {
            if let Some(text) = text {
                let index = state
                    .pending_constant()
                    .ok_or(EvalError::Unsupported("diagnostic read absent"))?;
                parent.plan.resume_text(&mut state, index, text)
            } else {
                parent.plan.step(&mut state)
            }
        })();
        let result = match result {
            Ok(step) => step,
            Err(error) => return self.native_failure(error),
        };
        match result {
            DiagnosticStep::NeedConstant(index) => {
                if index >= parent.arguments.len() {
                    return Err(binding("diagnostic demand outside graph"));
                }
                let SqlDemand::Diagnostic(task) = &mut self.slots[id.slot]
                    .task
                    .as_mut()
                    .expect("authenticated diagnostic task")
                    .demand
                else {
                    unreachable!()
                };
                task.state = Some(state);

                Ok(if let Some(window) = window {
                    HostStep::NeedArgRow {
                        index: parent.main_arity + index,
                        position: window.position,
                    }
                } else {
                    HostStep::NeedArg(HostArgRequest {
                        index: parent.main_arity + index,
                        mode: ArgMode::Fresh,
                    })
                })
            }
            DiagnosticStep::Done(text) => self.native_failure(match text {
                Some(expression) => EvalError::DataOutOfRange {
                    value: overflow.value(),
                    expression,
                },
                None => overflow.raw(),
            }),
        }
    }
}
