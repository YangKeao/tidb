// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Owned arithmetic continuations. Each source and diagnostic demand stays in
//! the original driver; only detached values and declarations reach this body.
use super::*;
#[path = "batch_integer_division.rs"]
mod integer_division;
#[path = "batch_numeric_window.rs"]
pub(super) mod window;
use crate::scalar_function::{
    dynamic_diagnostic::{ArgumentDiagnostic, DiagnosticState, DiagnosticStep, DynamicDiagnostic},
    NativeArithmeticInputMode as Mode, NumericOperandPending, NumericOperandStep,
};
use tidb_ast::BinaryOp;
use tidb_query_datatype::codec::native_eval_type::{
    native_render_binary_expression, NativeArithmeticOperator,
};

pub(in super::super) enum NumericPlanBody {
    Subject(DynamicDiagnostic),
    Argument(ArgumentDiagnostic),
}
pub(in super::super) struct NumericPlan {
    pub base: usize,
    pub bindings: usize,
    pub body: NumericPlanBody,
}
pub(in super::super) struct CompiledNumericParent {
    pub slot: usize,
    /// Actual original producers, including alternate domains and mirrors.
    pub arguments: Box<[usize]>,
    /// Initial probe and fresh general retry, each in original operand order.
    pub requests: [[usize; 2]; 2],
    pub preparation: [[NativeValuePreparation; 2]; 2],
    /// Subjects left/right, integer overflow left/right, real overflow left/right.
    pub plans: Box<[NumericPlan]>,
}
#[derive(Clone, Copy)]
enum Overflow {
    Int,
    Real,
    Decimal,
}
impl Overflow {
    fn raw(self) -> EvalError {
        match self {
            Self::Int => EvalError::IntOverflow,
            Self::Real => EvalError::FloatOverflow,
            Self::Decimal => EvalError::DecimalOverflow,
        }
    }
    fn class(self, unsigned: bool) -> &'static str {
        match self {
            Self::Int if unsigned => "BIGINT UNSIGNED",
            Self::Int => "BIGINT",
            Self::Real => "DOUBLE",
            Self::Decimal => "DECIMAL",
        }
    }
}
enum DiagnosticReason {
    Subject(NumericOperandPending),
    IntegerDivision {
        pending: NumericOperandPending,
        continuation: crate::ops::quotient::IntegerDivisionContinuation,
    },
    DecimalDivision {
        values: [tidb_datatype::Decimal; 2],
        left: Option<String>,
        unsigned: bool,
    },
    Overflow {
        kind: Overflow,
        left: Option<String>,
    },
}
struct NumericDiagnostic {
    plan: usize,
    state: DiagnosticState,
    reason: DiagnosticReason,
}
pub(super) struct NumericTask {
    typed: bool,
    index: usize,
    next: usize,
    left: Option<Datum>,
    diagnostic: Option<NumericDiagnostic>,
}
impl NumericTask {
    pub(super) fn next_argument(&self) -> usize {
        self.next
    }
    pub(super) fn retained_bytes(&self) -> LocalResult<usize> {
        let mut n = std::mem::size_of::<Self>();
        if let Some(value) = &self.left {
            n = n
                .checked_add(
                    native_payload_bytes(value)?
                        .checked_mul(2)
                        .ok_or_else(|| resource("numeric retained value overflow"))?,
                )
                .ok_or_else(|| resource("numeric retained extent overflow"))?;
        }
        if let Some(diag) = &self.diagnostic {
            n = n
                .checked_add(
                    diag.state
                        .retained_bytes()
                        .map_err(|_| resource("numeric diagnostic state overflow"))?,
                )
                .ok_or_else(|| resource("numeric diagnostic retained extent overflow"))?;
            if let DiagnosticReason::Overflow {
                left: Some(left), ..
            } = &diag.reason
            {
                n = n
                    .checked_add(left.capacity())
                    .ok_or_else(|| resource("numeric text extent overflow"))?;
            }
            let payload = match &diag.reason {
                DiagnosticReason::IntegerDivision { continuation, .. } => match continuation {
                    crate::ops::quotient::IntegerDivisionContinuation::Left { right, .. } => {
                        native_payload_bytes(right)?
                    }
                    crate::ops::quotient::IntegerDivisionContinuation::Right { left, .. } => {
                        left.coefficient_bytes().len()
                    }
                },
                DiagnosticReason::DecimalDivision { values, left, .. } => {
                    n = n
                        .checked_add(left.as_ref().map_or(0, String::capacity))
                        .ok_or_else(|| resource("DIV diagnostic text extent overflow"))?;
                    values
                        .iter()
                        .try_fold(0usize, |n, value| {
                            n.checked_add(value.coefficient_bytes().len())
                        })
                        .ok_or_else(|| resource("DIV actual-value extent overflow"))?
                }
                _ => 0,
            };
            n = n
                .checked_add(
                    payload
                        .checked_mul(2)
                        .ok_or_else(|| resource("DIV continuation payload overflow"))?,
                )
                .ok_or_else(|| resource("DIV continuation extent overflow"))?;
            // Subject is constructed only from a parsed, fixed MyDecimal
            // overflow state, never from the SDK's owned Ready input variant.
        }
        Ok(n)
    }
}
/// A fixed capability check, with no SQL or context callback. Full window
/// replies are authenticated before copying; one token checks generation at
/// intermediate owner boundaries without rescanning a whole vector per row.
pub(super) struct NumericAuthentication<'a> {
    pub store: &'a RefCell<NativeValueStore>,
    pub token: &'a [u8],
    pub origin: NativeValueOrigin,
    pub diagnostic: bool,
}
impl NumericAuthentication<'_> {
    pub(super) fn check(&self) -> LocalResult<()> {
        if self.diagnostic {
            self.store
                .borrow()
                .diagnostic_text(self.token, self.origin)?;
        } else {
            self.store
                .borrow()
                .get(self.token, NativeValueRole::CharInput, self.origin)?;
        }
        Ok(())
    }
}
enum DiagnosticTransition {
    Need(Box<NumericTask>),
    Operand(Box<NumericTask>, Datum),
    Failure(EvalError),
    IntegerDivision(Box<NumericTask>, crate::ops::quotient::IntegerDivisionStep),
}
enum Transition {
    Need(Box<NumericTask>),
    Value(Datum),
    Failure(EvalError),
}
impl<'a> NativeBatchHosts<'a> {
    pub(in super::super) fn with_numeric_parents(
        mut self,
        parents: &'a [CompiledNumericParent],
    ) -> LocalResult<Self> {
        if parents.windows(2).any(|p| p[0].slot >= p[1].slot) {
            return Err(binding("numeric parents must be strictly increasing"));
        }
        for p in parents {
            let slot = self.catalog.slot(p.slot)?;
            let Some(NativeHostMetadata::Arithmetic {
                batch,
                operation,
                name,
                source_types,
                binary_literal,
                generic_mode,
                argument_nodes,
                ..
            }) = self.metadata.get(p.slot)
            else {
                return Err(binding(
                    "numeric parent requires its original row arithmetic",
                ));
            };
            let window = batch.is_some();
            let valid_operation = if window {
                matches!(
                    operation,
                    SqlBatchHostOperation::AddWindowNative
                        | SqlBatchHostOperation::SubtractWindowNative
                        | SqlBatchHostOperation::MultiplyWindowNative
                        | SqlBatchHostOperation::ModuloWindowNative
                        | SqlBatchHostOperation::DivideWindowNative
                        | SqlBatchHostOperation::IntegerDivideWindowNative
                )
            } else {
                matches!(
                    operation,
                    SqlBatchHostOperation::AddNative
                        | SqlBatchHostOperation::SubtractNative
                        | SqlBatchHostOperation::MultiplyNative
                        | SqlBatchHostOperation::ModuloNative
                        | SqlBatchHostOperation::DivideNative
                        | SqlBatchHostOperation::IntegerDivideNative
                )
            };
            let expected_name = match operation {
                SqlBatchHostOperation::AddNative | SqlBatchHostOperation::AddWindowNative => "plus",
                SqlBatchHostOperation::SubtractNative
                | SqlBatchHostOperation::SubtractWindowNative => "minus",
                SqlBatchHostOperation::MultiplyNative
                | SqlBatchHostOperation::MultiplyWindowNative => "mul",
                SqlBatchHostOperation::ModuloNative | SqlBatchHostOperation::ModuloWindowNative => {
                    "mod"
                }
                SqlBatchHostOperation::DivideNative | SqlBatchHostOperation::DivideWindowNative => {
                    "div"
                }
                SqlBatchHostOperation::IntegerDivideNative
                | SqlBatchHostOperation::IntegerDivideWindowNative => "intdiv",
                _ => return Err(binding("numeric parent operation differs")),
            };
            let expected = if window {
                SqlBatchHostOperation::EagerValuesWindowNative
            } else {
                SqlBatchHostOperation::EagerValuesNative
            };
            if !valid_operation
                || name.as_ref() != expected_name
                || self.catalog.sql_batch_operation(slot)? != expected
                || p.arguments.len() != self.catalog.signature(slot)?.arg_types.len()
                || p.arguments.get(..2) != Some(argument_nodes.as_slice())
                || p.requests[0] != [0, 1]
                || p.requests.iter().flatten().any(|&i| i >= p.arguments.len())
                || p.plans.len() != 6
            {
                return Err(binding("numeric parent graph differs"));
            }
            if let Some(batch) = batch {
                if (name.as_ref() == "intdiv" && batch.domain == NativeEvalType::Real)
                    || p.requests[1] != [0, 1]
                    || !matches!(
                        batch.domain,
                        NativeEvalType::Int | NativeEvalType::Real | NativeEvalType::Decimal
                    )
                    || *generic_mode != Mode::Numeric(batch.domain)
                    || (batch.integer
                        && (batch.domain != NativeEvalType::Int
                            || !batch.output_integer
                            || [0, 1].into_iter().any(|i| {
                                crate::scalar_function::native_numeric_batch_source(
                                    &source_types[i],
                                    binary_literal[i],
                                    batch.domain,
                                ) != NativeEvalType::Int
                            })))
                {
                    return Err(binding("numeric window source declarations differ"));
                }
            }
            let mut end = 2usize;
            for (logical, &physical) in p.requests[1].iter().enumerate() {
                if physical != logical {
                    if physical != end {
                        return Err(binding("numeric alternate producer order differs"));
                    }
                    end += 1;
                }
            }
            for (i, plan) in p.plans.iter().enumerate() {
                if plan.base != end
                    || plan
                        .base
                        .checked_add(plan.bindings)
                        .is_none_or(|end| end > p.arguments.len())
                    || matches!(
                        (&plan.body, i < 2),
                        (NumericPlanBody::Subject(_), false) | (NumericPlanBody::Argument(_), true)
                    )
                {
                    return Err(binding("numeric diagnostic graph differs"));
                }
                end = plan.base + plan.bindings;
            }
            if end != p.arguments.len() {
                return Err(binding("numeric graph contains unused producers"));
            }
        }
        self.numeric_parents = parents;
        Ok(self)
    }
    pub(super) fn numeric_parent(&self, slot: HostSlot) -> Option<&'a CompiledNumericParent> {
        self.numeric_parents
            .binary_search_by_key(&slot.index(), |p| p.slot)
            .ok()
            .map(|i| &self.numeric_parents[i])
    }
    pub(super) fn start_numeric_parent(
        &mut self,
        invocation: HostInvocation<'_>,
    ) -> LocalResult<HostStart> {
        if self.native_values.is_none() {
            return Err(binding("numeric store absent"));
        }
        let parent = self
            .numeric_parent(invocation.slot)
            .ok_or_else(|| binding("numeric parent absent"))?;
        let Some(NativeHostMetadata::Arithmetic {
            mode, batch: None, ..
        }) = self.metadata.get(invocation.slot.index())
        else {
            return Err(binding("numeric metadata absent"));
        };
        self.install_task(
            invocation.slot,
            invocation.row,
            parent.arguments.len(),
            SqlDemand::Numeric(Box::new(NumericTask {
                typed: *mode == Mode::TypedInteger,
                index: 0,
                next: 0,
                left: None,
                diagnostic: None,
            })),
        )
    }
    pub(super) fn resume_numeric_parent(
        &mut self,
        id: &HostTaskId,
        reply: HostArgReply<'_>,
    ) -> LocalResult<HostStep> {
        self.validate_row_reply(id, reply)?;
        let task = self.task(id)?;
        let (slot, row) = (task.slot, task.row);
        let SqlDemand::Numeric(state) = &task.demand else {
            return Err(binding("numeric continuation absent"));
        };
        let diagnostic = state.diagnostic.is_some();
        let parent = self
            .numeric_parent(slot)
            .ok_or_else(|| binding("numeric parent absent"))?;
        let ScalarValueRef::Bytes(Some(token)) = reply.values.get_scalar_ref(0) else {
            return Err(binding("numeric reply cannot be wire NULL"));
        };
        let origin = NativeValueOrigin {
            node: parent.arguments[reply.index],
            row,
        };
        let store = self
            .native_values
            .ok_or_else(|| binding("numeric store absent"))?;
        let payload = if diagnostic {
            store
                .borrow()
                .diagnostic_text(token, origin)?
                .map_or(0, str::len)
        } else {
            native_payload_bytes(
                store
                    .borrow()
                    .get(token, NativeValueRole::CharInput, origin)?,
            )?
        };
        let base = self
            .retained_bytes()?
            .checked_add(
                payload
                    .checked_mul(3)
                    .ok_or_else(|| resource("numeric input extent overflow"))?,
            )
            .ok_or_else(|| resource("numeric live allowance overflow"))?;
        self.check_bytes(base)?;
        let (value, text) = if diagnostic {
            (
                None,
                store
                    .borrow()
                    .diagnostic_text(token, origin)?
                    .map(str::to_owned),
            )
        } else {
            (
                Some(
                    store
                        .borrow()
                        .clone_for_kernel(token, NativeValueRole::CharInput, origin)?,
                ),
                None,
            )
        };
        // Consume before every context or formatter callback. A panic, error or
        // invalidated source capability can never re-enter a later phase.
        let demand = std::mem::replace(
            &mut self.slots[id.slot]
                .task
                .as_mut()
                .expect("authenticated numeric task")
                .demand,
            SqlDemand::Consumed,
        );
        let SqlDemand::Numeric(state) = demand else {
            unreachable!()
        };
        let allowance = self
            .max_retained_bytes
            .checked_sub(base)
            .ok_or_else(|| resource("numeric conversion allowance exhausted"))?;
        let auth = NumericAuthentication {
            store,
            token,
            origin,
            diagnostic,
        };
        let transition = tidb_query_expr::local::with_value_kernel_work_limit(allowance, || {
            let result =
                self.drive_numeric_parent(slot, state, value, text.as_deref(), base, &auth);
            tidb_query_expr::local::value_kernel_work_status()?;
            result
        });
        if diagnostic {
            store.borrow().diagnostic_text(token, origin)?;
        } else {
            store
                .borrow()
                .get(token, NativeValueRole::CharInput, origin)?;
        }
        match transition? {
            Transition::Need(state) => {
                let next = state.next;
                self.check_bytes(
                    self.retained_bytes()?
                        .checked_add(state.retained_bytes()?)
                        .ok_or_else(|| resource("numeric continuation extent overflow"))?,
                )?;
                self.slots[id.slot]
                    .task
                    .as_mut()
                    .expect("authenticated numeric task")
                    .demand = SqlDemand::Numeric(state);
                Ok(HostStep::NeedArg(HostArgRequest {
                    index: next,
                    mode: ArgMode::Fresh,
                }))
            }
            Transition::Failure(error) => self.native_failure(error),
            Transition::Value(value) => {
                let Some(NativeHostMetadata::Arithmetic {
                    name,
                    result_type,
                    output_node,
                    output_role,
                    ..
                }) = self.metadata.get(slot.index())
                else {
                    unreachable!()
                };
                let value = match crate::scalar_function::coerce_function_result(
                    name,
                    Some(result_type),
                    value,
                ) {
                    Ok(value) => value,
                    Err(error) => return self.native_failure(error),
                };
                self.finish_native_value(id, value, *output_node, *output_role, row, base)
                    .map(HostStep::Ready)
            }
        }
    }
    fn drive_numeric_parent(
        &self,
        slot: HostSlot,
        mut task: Box<NumericTask>,
        value: Option<Datum>,
        text: Option<&str>,
        base: usize,
        auth: &NumericAuthentication<'_>,
    ) -> LocalResult<Transition> {
        let parent = self
            .numeric_parent(slot)
            .ok_or_else(|| binding("numeric parent absent"))?;
        let Some(NativeHostMetadata::Arithmetic {
            operation,
            source_types,
            binary_literal,
            generic_mode,
            ..
        }) = self.metadata.get(slot.index())
        else {
            return Err(binding("numeric metadata absent"));
        };
        let op = match operation {
            SqlBatchHostOperation::AddNative => BinaryOp::Plus,
            SqlBatchHostOperation::SubtractNative => BinaryOp::Minus,
            SqlBatchHostOperation::MultiplyNative => BinaryOp::Mul,
            SqlBatchHostOperation::ModuloNative => BinaryOp::Mod,
            SqlBatchHostOperation::DivideNative => BinaryOp::Div,
            SqlBatchHostOperation::IntegerDivideNative => BinaryOp::IntDiv,
            _ => return Err(binding("numeric operation differs")),
        };
        let available = self
            .max_retained_bytes
            .checked_sub(base)
            .ok_or_else(|| resource("numeric kernel allowance exhausted"))?;
        let normalized;
        if let Some(mut diag) = task.diagnostic.take() {
            let plan = &parent.plans[diag.plan];
            let index = diag
                .state
                .pending_constant()
                .ok_or_else(|| binding("numeric diagnostic pending input absent"))?;
            let result = tidb_query_expr::local::with_value_kernel_work_limit(available, || {
                match &plan.body {
                    NumericPlanBody::Subject(plan) => {
                        plan.resume_text(&mut diag.state, index, text)
                    }
                    NumericPlanBody::Argument(plan) => {
                        let actual = match &diag.reason {
                            DiagnosticReason::DecimalDivision { values, .. } => {
                                Some(&values[diag.plan - 2])
                            }
                            _ => None,
                        };
                        plan.resume_text(&mut diag.state, index, text, actual)
                    }
                }
            });
            match result {
                Ok(step) => {
                    match self.numeric_diagnostic_step(task, diag, step, slot, available, auth)? {
                        DiagnosticTransition::Need(state) => return Ok(Transition::Need(state)),
                        DiagnosticTransition::Failure(error) => {
                            return Ok(Transition::Failure(error))
                        }
                        DiagnosticTransition::IntegerDivision(state, step) => {
                            return self.drive_integer_division(slot, state, step, available, auth)
                        }
                        DiagnosticTransition::Operand(state, value) => {
                            task = state;
                            normalized = value;
                        }
                    }
                }
                Err(error) => return Ok(Transition::Failure(error)),
            }
        } else {
            let mode = if task.typed {
                Mode::TypedInteger
            } else {
                *generic_mode
            };
            let prep = parent.preparation[usize::from(!task.typed)][task.index];
            let result = crate::scalar_function::prepare_native_numeric_operand(
                &source_types[task.index],
                matches!(prep, NativeValuePreparation::Literal),
                matches!(prep, NativeValuePreparation::TypedReady),
                binary_literal[task.index],
                value.ok_or_else(|| binding("numeric operand absent"))?,
                mode,
                self.context,
            );
            auth.check()?;
            match result {
                Ok(NumericOperandStep::Ready(value)) => normalized = value,
                Ok(NumericOperandStep::Subject(pending)) => {
                    let plan_index = task.index;
                    let NumericPlanBody::Subject(plan) = &parent.plans[plan_index].body else {
                        unreachable!()
                    };
                    let mut diag = NumericDiagnostic {
                        plan: plan_index,
                        state: plan.state(),
                        reason: DiagnosticReason::Subject(pending),
                    };
                    let step =
                        tidb_query_expr::local::with_value_kernel_work_limit(available, || {
                            plan.step(&mut diag.state)
                        });
                    match step {
                        Ok(step) => {
                            match self
                                .numeric_diagnostic_step(task, diag, step, slot, available, auth)?
                            {
                                DiagnosticTransition::Need(state) => {
                                    return Ok(Transition::Need(state))
                                }
                                DiagnosticTransition::Failure(error) => {
                                    return Ok(Transition::Failure(error))
                                }
                                DiagnosticTransition::IntegerDivision(_, _) => {
                                    return Err(binding(
                                        "ordinary numeric cast resumed DIV promotion",
                                    ))
                                }
                                DiagnosticTransition::Operand(state, value) => {
                                    task = state;
                                    normalized = value;
                                }
                            }
                        }
                        Err(error) => return Ok(Transition::Failure(error)),
                    }
                }
                Err(error) => return Ok(Transition::Failure(error)),
            }
        }
        let value = normalized;
        let mode = if task.typed {
            Mode::TypedInteger
        } else {
            *generic_mode
        };
        let prepared = if task.index == 0 {
            if value.is_null()
                && (task.typed
                    || matches!(mode, Mode::Numeric(domain) if !(domain == NativeEvalType::Real && op == BinaryOp::Plus) && !(op == BinaryOp::Mod && domain != NativeEvalType::Decimal)))
            {
                crate::ops::arithmetic::PreparedArithmetic::Null
            } else {
                task.left = Some(value);
                task.index = 1;
                task.next = parent.requests[usize::from(!task.typed)][1];
                return Ok(Transition::Need(task));
            }
        } else {
            let left = task
                .left
                .take()
                .ok_or_else(|| binding("numeric left operand absent"))?;
            let unsigned = source_types.each_ref().map(FieldType::is_unsigned);
            if op == BinaryOp::IntDiv {
                return self.begin_integer_division_row(slot, task, left, value, available, auth);
            }
            if matches!(op, BinaryOp::Mod | BinaryOp::Div) {
                let result =
                    self.compute_quotient(&self.metadata[slot.index()], left, value, available)?;
                auth.check()?;
                return self.numeric_result(slot, task, result, available, auth);
            }
            if task.typed {
                let prepared = crate::ops::prepare_native_typed_arithmetic(
                    op,
                    left,
                    value,
                    unsigned,
                    self.context,
                );
                auth.check()?;
                match prepared {
                    Ok(Some(prepared)) => prepared,
                    Ok(None) => {
                        task.typed = false;
                        task.index = 0;
                        task.next = parent.requests[1][0];
                        return Ok(Transition::Need(task));
                    }
                    Err(error) => return Ok(Transition::Failure(error)),
                }
            } else {
                let (left, right) = crate::binary_literal::cast_signed_literal_operands(
                    op,
                    left,
                    value,
                    unsigned.map(|u| !u),
                );
                let _ = self.context.div_precision_increment();
                auth.check()?;
                let prepared =
                    crate::ops::arithmetic::prepare(op, left, right, unsigned, self.context);
                auth.check()?;
                match prepared {
                    Ok(prepared) => prepared,
                    Err(error) => return Ok(Transition::Failure(error)),
                }
            }
        };
        self.numeric_result(slot, task, prepared.compute(available), available, auth)
    }
    fn numeric_result(
        &self,
        slot: HostSlot,
        task: Box<NumericTask>,
        result: std::result::Result<Datum, EvalError>,
        available: usize,
        auth: &NumericAuthentication<'_>,
    ) -> LocalResult<Transition> {
        let parent = self
            .numeric_parent(slot)
            .ok_or_else(|| binding("numeric result parent absent"))?;
        match result {
            Ok(value) => Ok(Transition::Value(value)),
            Err(error) => {
                let kind = match error {
                    EvalError::IntOverflow => Overflow::Int,
                    EvalError::FloatOverflow => Overflow::Real,
                    EvalError::DecimalOverflow => Overflow::Decimal,
                    other => return Ok(Transition::Failure(other)),
                };
                let plan_index = if matches!(kind, Overflow::Real) { 4 } else { 2 };
                let NumericPlanBody::Argument(plan) = &parent.plans[plan_index].body else {
                    unreachable!()
                };
                let mut diag = NumericDiagnostic {
                    plan: plan_index,
                    state: plan.state(),
                    reason: DiagnosticReason::Overflow { kind, left: None },
                };
                let step = tidb_query_expr::local::with_value_kernel_work_limit(available, || {
                    plan.step(&mut diag.state, None)
                });
                match step {
                    Ok(step) => {
                        match self
                            .numeric_diagnostic_step(task, diag, step, slot, available, auth)?
                        {
                            DiagnosticTransition::Need(state) => Ok(Transition::Need(state)),
                            DiagnosticTransition::Failure(error) => Ok(Transition::Failure(error)),
                            DiagnosticTransition::Operand(_, _)
                            | DiagnosticTransition::IntegerDivision(_, _) => {
                                Err(binding("overflow diagnostic returned an operand"))
                            }
                        }
                    }
                    Err(error) => Ok(Transition::Failure(error)),
                }
            }
        }
    }
    fn numeric_diagnostic_step(
        &self,
        mut task: Box<NumericTask>,
        mut diag: NumericDiagnostic,
        mut step: DiagnosticStep,
        slot: HostSlot,
        available: usize,
        auth: &NumericAuthentication<'_>,
    ) -> LocalResult<DiagnosticTransition> {
        let parent = self
            .numeric_parent(slot)
            .ok_or_else(|| binding("numeric parent absent"))?;
        loop {
            match step {
                DiagnosticStep::NeedConstant(index) => {
                    let plan = &parent.plans[diag.plan];
                    if index >= plan.bindings {
                        return Err(binding("numeric diagnostic demand outside graph"));
                    }
                    task.next = plan.base + index;
                    task.diagnostic = Some(diag);
                    return Ok(DiagnosticTransition::Need(task));
                }
                DiagnosticStep::Done(text) => match diag.reason {
                    DiagnosticReason::Subject(pending) => {
                        let Some(NativeHostMetadata::Arithmetic { source_types, .. }) =
                            self.metadata.get(slot.index())
                        else {
                            unreachable!()
                        };
                        let value = pending.finish(
                            &source_types[task.index],
                            text.as_deref(),
                            self.context,
                        );
                        auth.check()?;
                        match value {
                            Ok(value) => return Ok(DiagnosticTransition::Operand(task, value)),
                            Err(error) => return Ok(DiagnosticTransition::Failure(error)),
                        }
                    }
                    DiagnosticReason::IntegerDivision {
                        pending,
                        continuation,
                    } => {
                        let Some(NativeHostMetadata::Arithmetic { source_types, .. }) =
                            self.metadata.get(slot.index())
                        else {
                            unreachable!()
                        };
                        let result = pending.finish(
                            &source_types[task.index],
                            text.as_deref(),
                            self.context,
                        );
                        auth.check()?;
                        match result {
                            Ok(Datum::Decimal(value)) => {
                                return Ok(DiagnosticTransition::IntegerDivision(
                                    task,
                                    continuation.finish(value),
                                ))
                            }
                            Ok(_) => {
                                return Ok(DiagnosticTransition::Failure(EvalError::Unsupported(
                                    "DIV decimal argument domain",
                                )))
                            }
                            Err(error) => return Ok(DiagnosticTransition::Failure(error)),
                        }
                    }
                    DiagnosticReason::DecimalDivision {
                        values,
                        left,
                        unsigned,
                    } => {
                        if diag.plan == 2 {
                            diag.plan = 3;
                            let NumericPlanBody::Argument(plan) = &parent.plans[3].body else {
                                unreachable!()
                            };
                            diag.state = plan.state();
                            let result = tidb_query_expr::local::with_value_kernel_work_limit(
                                available,
                                || plan.step(&mut diag.state, Some(&values[1])),
                            );
                            // Original Option::zip evaluates the right formatter
                            // even when the left formatter was unavailable.
                            diag.reason = DiagnosticReason::DecimalDivision {
                                values,
                                left: text,
                                unsigned,
                            };
                            step = match result {
                                Ok(step) => step,
                                Err(error) => return Ok(DiagnosticTransition::Failure(error)),
                            };
                        } else {
                            let result = match left.zip(text) {
                                Some((left, right)) => {
                                    let extent = left
                                        .len()
                                        .checked_add(right.len())
                                        .and_then(|n| n.checked_mul(4))
                                        .and_then(|n| n.checked_add(256))
                                        .ok_or_else(|| {
                                            resource("DIV diagnostic output extent overflow")
                                        })?;
                                    if extent > available {
                                        return Err(resource(
                                            "DIV diagnostic output exceeds allowance",
                                        ));
                                    }
                                    EvalError::DataOutOfRange {
                                        value: if unsigned {
                                            "BIGINT UNSIGNED"
                                        } else {
                                            "BIGINT"
                                        },
                                        expression: native_render_binary_expression(
                                            &left,
                                            NativeArithmeticOperator::IntegerDivide,
                                            &right,
                                        ),
                                    }
                                }
                                None => EvalError::IntOverflow,
                            };
                            return Ok(DiagnosticTransition::Failure(result));
                        }
                    }
                    DiagnosticReason::Overflow { kind, left } => {
                        let Some(text) = text else {
                            return Ok(DiagnosticTransition::Failure(kind.raw()));
                        };
                        if let Some(left) = left {
                            let Some(NativeHostMetadata::Arithmetic {
                                operation,
                                result_type,
                                ..
                            }) = self.metadata.get(slot.index())
                            else {
                                unreachable!()
                            };
                            let op = match operation {
                                SqlBatchHostOperation::AddNative
                                | SqlBatchHostOperation::AddWindowNative => {
                                    NativeArithmeticOperator::Plus
                                }
                                SqlBatchHostOperation::SubtractNative
                                | SqlBatchHostOperation::SubtractWindowNative => {
                                    NativeArithmeticOperator::Minus
                                }
                                SqlBatchHostOperation::MultiplyNative
                                | SqlBatchHostOperation::MultiplyWindowNative => {
                                    NativeArithmeticOperator::Multiply
                                }
                                SqlBatchHostOperation::ModuloNative
                                | SqlBatchHostOperation::ModuloWindowNative => {
                                    NativeArithmeticOperator::Modulo
                                }
                                SqlBatchHostOperation::DivideNative
                                | SqlBatchHostOperation::DivideWindowNative => {
                                    NativeArithmeticOperator::Divide
                                }
                                SqlBatchHostOperation::IntegerDivideNative
                                | SqlBatchHostOperation::IntegerDivideWindowNative => {
                                    NativeArithmeticOperator::IntegerDivide
                                }
                                _ => unreachable!(),
                            };
                            let extent = left
                                .len()
                                .checked_add(text.len())
                                .and_then(|n| n.checked_mul(4))
                                .and_then(|n| n.checked_add(256))
                                .ok_or_else(|| resource("numeric overflow text extent overflow"))?;
                            if extent > available {
                                return Err(resource("numeric overflow text allowance exceeded"));
                            }
                            return Ok(DiagnosticTransition::Failure(EvalError::DataOutOfRange {
                                value: kind.class(result_type.is_unsigned()),
                                expression: native_render_binary_expression(&left, op, &text),
                            }));
                        }
                        diag.plan += 1;
                        let NumericPlanBody::Argument(plan) = &parent.plans[diag.plan].body else {
                            unreachable!()
                        };
                        diag.state = plan.state();
                        diag.reason = DiagnosticReason::Overflow {
                            kind,
                            left: Some(text),
                        };
                        step = match tidb_query_expr::local::with_value_kernel_work_limit(
                            available,
                            || plan.step(&mut diag.state, None),
                        ) {
                            Ok(step) => step,
                            Err(error) => return Ok(DiagnosticTransition::Failure(error)),
                        };
                    }
                },
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{constant::Constant, expression::Expression};
    use std::cell::{Cell, RefCell};
    use tidb_ast::CiString;
    use tidb_datatype::FieldTypeCode;
    use tidb_query_expr::local::HostSignature;
    struct Fixture {
        catalog: HostCatalog,
        metadata: [NativeHostMetadata; 1],
        parents: [CompiledNumericParent; 1],
        field: tipb::FieldType,
    }
    fn fixture(name: &str, source: FieldType, result: FieldType, alternate: bool) -> Fixture {
        let mut args: Vec<_> = (0..2)
            .map(|i| {
                let mut c = Constant::new(
                    if alternate && i == 0 {
                        Datum::BinaryLiteral(tidb_datatype::BinaryLiteral::from(vec![3]))
                    } else {
                        Datum::Int(0)
                    },
                    source.clone(),
                );
                c.param_marker = Some(crate::constant::ParamMarker { order: i });
                Expression::Constant(c)
            })
            .collect();
        let function = crate::scalar_function::ScalarFunction::new(
            CiString::new(name),
            result.clone(),
            std::mem::take(&mut args),
        );
        let mut arguments = vec![1, 2];
        if alternate {
            arguments.push(3);
        }
        let mut plans = vec![];
        for arg in &function.args {
            let (plan, bindings) =
                DynamicDiagnostic::capture_numeric(arg, true, 128, 1 << 20).unwrap();
            let base = arguments.len();
            for _ in &bindings {
                arguments.push(arguments.len() + 1);
            }
            plans.push(NumericPlan {
                base,
                bindings: bindings.len(),
                body: NumericPlanBody::Subject(plan),
            });
        }
        for float in [false, true] {
            for arg in &function.args {
                let (plan, bindings) =
                    ArgumentDiagnostic::survey(&function, arg, float, 128, 1 << 20)
                        .unwrap()
                        .freeze()
                        .unwrap();
                let base = arguments.len();
                for _ in &bindings {
                    arguments.push(arguments.len() + 1);
                }
                plans.push(NumericPlan {
                    base,
                    bindings: bindings.len(),
                    body: NumericPlanBody::Argument(plan),
                });
            }
        }
        let field = projected(&native_bytes_type(), true).unwrap();
        let catalog = HostCatalog::for_sql_batch(vec![(
            SqlBatchHostOperation::EagerValuesNative,
            HostSignature {
                arg_types: vec![field.clone(); arguments.len()].into(),
                return_type: field.clone(),
            },
        )])
        .unwrap();
        let operation = match name {
            "plus" => SqlBatchHostOperation::AddNative,
            "minus" => SqlBatchHostOperation::SubtractNative,
            "mul" => SqlBatchHostOperation::MultiplyNative,
            "mod" => SqlBatchHostOperation::ModuloNative,
            "div" => SqlBatchHostOperation::DivideNative,
            "intdiv" => SqlBatchHostOperation::IntegerDivideNative,
            _ => unreachable!(),
        };
        Fixture {
            catalog,
            field,
            metadata: [NativeHostMetadata::Arithmetic {
                batch: None,
                operation,
                name: name.into(),
                result_type: result,
                argument_nodes: [1, 2],
                source_types: [source.clone(), source],
                literal: [true; 2],
                binary_literal: [alternate, false],
                mode: function.native_arithmetic_mode(),
                generic_mode: function.native_arithmetic_generic_mode(),
                subjects: [None, None],
                subject_format_failures: [None; 2],
                overflow_expressions: [None, None],
                overflow_format_failure: None,
                intdiv: None,
                output_node: 0,
                output_role: NativeValueRole::Output,
            }],
            parents: [CompiledNumericParent {
                slot: 0,
                arguments: arguments.into(),
                requests: [[0, 1], if alternate { [2, 1] } else { [0, 1] }],
                preparation: [[NativeValuePreparation::TypedReady; 2]; 2],
                plans: plans.into(),
            }],
        }
    }
    fn start(
        provider: &mut NativeBatchHosts<'_>,
        eval: &mut EvalContext,
        fixture: &Fixture,
        row: InputRow,
    ) -> HostTaskId {
        let args = vec![fixture.field.clone(); fixture.parents[0].arguments.len()];
        let HostStart::Pending { task, request } = provider
            .start(
                eval,
                HostInvocation {
                    slot: provider.catalog.slot(0).unwrap(),
                    row,
                    arg_types: &args,
                    return_type: &fixture.field,
                },
            )
            .unwrap()
        else {
            panic!()
        };
        assert_eq!(request.index, 0);
        task
    }
    fn sql(
        store: &RefCell<NativeValueStore>,
        node: usize,
        row: InputRow,
        value: Datum,
    ) -> VectorValue {
        let token = store
            .borrow_mut()
            .insert_clone(
                &value,
                NativeValueRole::CharInput,
                NativeValueOrigin { node, row },
            )
            .unwrap();
        VectorValue::from_scalar(&ScalarValue::Bytes(Some(token)), 1)
    }
    fn reply<'a>(
        field: &'a tipb::FieldType,
        index: usize,
        values: &'a VectorValue,
    ) -> HostArgReply<'a> {
        HostArgReply {
            field_type: field,
            index,
            values,
        }
    }
    #[test]
    fn numeric_parent_original_overflow_reloads_text_in_order_and_authenticates_it() {
        for name in ["plus", "minus", "mul"] {
            for text in [Some("9"), None] {
                let int = FieldType::new(FieldTypeCode::LongLong);
                let f = fixture(name, int.clone(), int, false);
                let store = RefCell::new(NativeValueStore::new(1 << 20).unwrap());
                store.borrow_mut().begin_window().unwrap();
                let mut hosts = NativeBatchHosts::new(
                    f.catalog.clone(),
                    &f.metadata,
                    &crate::NoColumns,
                    1 << 20,
                )
                .with_native_values(Some(&store))
                .with_numeric_parents(&f.parents)
                .unwrap();
                let row = InputRow {
                    occurrence: 11,
                    input_row: 7,
                };
                let mut eval = EvalContext::default();
                let task = start(&mut hosts, &mut eval, &f, row);
                let left = sql(&store, 1, row, Datum::Int(i64::MAX));
                let right = sql(
                    &store,
                    2,
                    row,
                    Datum::Int(if name == "minus" { -1 } else { 2 }),
                );
                assert!(matches!(
                    hosts
                        .resume(&mut eval, &task, reply(&f.field, 0, &left))
                        .unwrap(),
                    HostStep::NeedArg(HostArgRequest {
                        index: 1,
                        mode: ArgMode::Fresh
                    })
                ));
                let left_diag = f.parents[0].plans[2].base;
                assert!(
                    matches!(hosts.resume(&mut eval, &task, reply(&f.field, 1, &right)).unwrap(), HostStep::NeedArg(r) if r.index == left_diag)
                );
                assert!(hosts.take_native_error().is_none());
                let node = f.parents[0].arguments[left_diag];
                for (badnode, badrow) in [
                    (node + 100, row),
                    (
                        node,
                        InputRow {
                            occurrence: 12,
                            ..row
                        },
                    ),
                    (
                        node,
                        InputRow {
                            input_row: 8,
                            ..row
                        },
                    ),
                ] {
                    let token = store
                        .borrow_mut()
                        .insert_diagnostic_text(
                            "9".into(),
                            NativeValueOrigin {
                                node: badnode,
                                row: badrow,
                            },
                        )
                        .unwrap();
                    let bad = VectorValue::from_scalar(&ScalarValue::Bytes(Some(token)), 1);
                    assert!(hosts
                        .resume(&mut eval, &task, reply(&f.field, left_diag, &bad))
                        .is_err());
                }
                let null = sql(&store, node, row, Datum::Null);
                assert!(hosts
                    .resume(&mut eval, &task, reply(&f.field, left_diag, &null))
                    .is_err());
                let origin = NativeValueOrigin { node, row };
                let token = match text {
                    Some(text) => store
                        .borrow_mut()
                        .insert_diagnostic_text(text.into(), origin),
                    None => store.borrow_mut().insert_diagnostic_unavailable(origin),
                }
                .unwrap();
                let values = VectorValue::from_scalar(&ScalarValue::Bytes(Some(token)), 1);
                let result = hosts.resume(&mut eval, &task, reply(&f.field, left_diag, &values));
                if text.is_some() {
                    let right_diag = f.parents[0].plans[3].base;
                    assert!(
                        matches!(result.unwrap(), HostStep::NeedArg(r) if r.index == right_diag)
                    );
                    let token = store
                        .borrow_mut()
                        .insert_diagnostic_text(
                            "3".into(),
                            NativeValueOrigin {
                                node: f.parents[0].arguments[right_diag],
                                row,
                            },
                        )
                        .unwrap();
                    let values = VectorValue::from_scalar(&ScalarValue::Bytes(Some(token)), 1);
                    assert!(hosts
                        .resume(&mut eval, &task, reply(&f.field, right_diag, &values))
                        .is_err());
                    let error = hosts.take_native_error().unwrap();
                    assert!(
                        matches!(error, EvalError::DataOutOfRange { value: "BIGINT", expression } if expression == format!("(9 {} 3)", if name == "plus" { "+" } else if name == "minus" { "-" } else { "*" }))
                    );
                    assert!(hosts
                        .resume(&mut eval, &task, reply(&f.field, right_diag, &values))
                        .is_err());
                } else {
                    assert!(result.is_err());
                    assert!(matches!(
                        hosts.take_native_error(),
                        Some(EvalError::IntOverflow)
                    ));
                    assert!(hosts
                        .resume(&mut eval, &task, reply(&f.field, left_diag, &values))
                        .is_err());
                }
                hosts.cancel(&task);
            }
        }
    }
    #[test]
    fn numeric_parent_failed_integer_probe_requests_the_distinct_original_general_source() {
        let int = FieldType::new(FieldTypeCode::LongLong);
        let f = fixture("plus", int, FieldType::new(FieldTypeCode::Double), true);
        let store = RefCell::new(NativeValueStore::new(1 << 20).unwrap());
        store.borrow_mut().begin_window().unwrap();
        let mut hosts =
            NativeBatchHosts::new(f.catalog.clone(), &f.metadata, &crate::NoColumns, 1 << 20)
                .with_native_values(Some(&store))
                .with_numeric_parents(&f.parents)
                .unwrap();
        let row = InputRow {
            occurrence: 0,
            input_row: 0,
        };
        let mut eval = EvalContext::default();
        let task = start(&mut hosts, &mut eval, &f, row);
        let left = sql(&store, 1, row, Datum::Int(7));
        let vector = sql(
            &store,
            2,
            row,
            Datum::VectorFloat32(tidb_datatype::VectorFloat32::init(2)),
        );
        assert!(
            matches!(hosts.resume(&mut eval, &task, reply(&f.field, 0, &left)).unwrap(), HostStep::NeedArg(r) if r.index == 1)
        );
        assert!(
            matches!(hosts.resume(&mut eval, &task, reply(&f.field, 1, &vector)).unwrap(), HostStep::NeedArg(r) if r.index == 2 && r.mode == ArgMode::Fresh)
        );
        // The old Int producer's capability cannot satisfy the Real request.
        assert!(hosts
            .resume(&mut eval, &task, reply(&f.field, 2, &left))
            .is_err());
        let real = sql(&store, 3, row, Datum::Real(7.0));
        assert!(
            matches!(hosts.resume(&mut eval, &task, reply(&f.field, 2, &real)).unwrap(), HostStep::NeedArg(r) if r.index == 1 && r.mode == ArgMode::Fresh)
        );
        hosts.cancel(&task);
    }
    struct Callback<'a> {
        store: &'a RefCell<NativeValueStore>,
        mode: Cell<usize>,
        calls: Cell<usize>,
    }
    impl Columns for Callback<'_> {
        fn get(&self, _: &[String]) -> Option<Datum> {
            panic!("no expression callback")
        }
        fn div_precision_increment(&self) -> u32 {
            self.calls.set(self.calls.get() + 1);
            let mut store = self.store.borrow_mut();
            match self.mode.get() {
                1 => panic!("numeric callback"),
                2 => {
                    store.begin_window().unwrap();
                }
                _ => (),
            }
            4
        }
    }
    #[test]
    fn numeric_parent_consumes_before_callbacks_and_reauthenticates_before_future_demands_or_output(
    ) {
        let real = FieldType::new(FieldTypeCode::Double);
        let f = fixture("plus", real.clone(), real, false);
        let store = RefCell::new(NativeValueStore::new(1 << 20).unwrap());
        let ctx = Callback {
            store: &store,
            mode: Cell::new(0),
            calls: Cell::new(0),
        };
        let mut hosts = NativeBatchHosts::new(f.catalog.clone(), &f.metadata, &ctx, 1 << 20)
            .with_native_values(Some(&store))
            .with_numeric_parents(&f.parents)
            .unwrap();
        let row = InputRow {
            occurrence: 0,
            input_row: 0,
        };
        let mut eval = EvalContext::default();
        for mode in [1, 2, 0] {
            ctx.mode.set(mode);
            store.borrow_mut().begin_window().unwrap();
            let task = start(&mut hosts, &mut eval, &f, row);
            let left = sql(&store, 1, row, Datum::Real(1.0));
            let right = sql(&store, 2, row, Datum::Real(2.0));
            assert!(matches!(
                hosts
                    .resume(&mut eval, &task, reply(&f.field, 0, &left))
                    .unwrap(),
                HostStep::NeedArg(_)
            ));
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                hosts.resume(&mut eval, &task, reply(&f.field, 1, &right))
            }));
            match mode {
                1 => assert!(result.is_err()),
                2 => assert!(result.unwrap().is_err()),
                _ => assert!(matches!(result.unwrap().unwrap(), HostStep::Ready(_))),
            }
            let calls = ctx.calls.get();
            assert!(hosts
                .resume(&mut eval, &task, reply(&f.field, 1, &right))
                .is_err());
            assert_eq!(ctx.calls.get(), calls);
            hosts.cancel(&task);
        }
    }
    struct Warning<'a> {
        store: &'a RefCell<NativeValueStore>,
        events: RefCell<Vec<String>>,
        mode: Cell<usize>,
    }
    impl Columns for Warning<'_> {
        fn div_precision_increment(&self) -> u32 {
            self.events.borrow_mut().push("precision".into());
            4
        }
        fn get(&self, _: &[String]) -> Option<Datum> {
            panic!("no expression callback")
        }
        fn truncate_level(&self) -> crate::ErrorLevel {
            self.events.borrow_mut().push("level".into());
            crate::ErrorLevel::Warn
        }
        fn handle_truncate(&self, message: &str) -> std::result::Result<(), EvalError> {
            self.events.borrow_mut().push(format!("warning:{message}"));
            let mut store = self.store.borrow_mut();
            match self.mode.get() {
                1 => panic!("numeric subject warning"),
                2 => {
                    store.begin_window().unwrap();
                }
                _ => (),
            }
            Ok(())
        }
    }
    #[test]
    fn numeric_parent_subject_pause_precedes_warning_and_keeps_right_operand_left_value_and_consumption(
    ) {
        let real = FieldType::new(FieldTypeCode::Double);
        let f = fixture(
            "minus",
            real,
            FieldType::new(FieldTypeCode::NewDecimal),
            false,
        );
        let store = RefCell::new(NativeValueStore::new(1 << 20).unwrap());
        let ctx = Warning {
            store: &store,
            events: RefCell::new(vec![]),
            mode: Cell::new(0),
        };
        let mut hosts = NativeBatchHosts::new(f.catalog.clone(), &f.metadata, &ctx, 1 << 20)
            .with_native_values(Some(&store))
            .with_numeric_parents(&f.parents)
            .unwrap();
        let row = InputRow {
            occurrence: 3,
            input_row: 7,
        };
        let mut eval = EvalContext::default();
        for mode in [1, 2, 0] {
            store.borrow_mut().begin_window().unwrap();
            ctx.mode.set(mode);
            ctx.events.borrow_mut().clear();
            let task = start(&mut hosts, &mut eval, &f, row);
            let left = sql(&store, 1, row, Datum::Real(1.25));
            assert!(
                matches!(hosts.resume(&mut eval, &task, reply(&f.field, 0, &left)).unwrap(), HostStep::NeedArg(r) if r.index == 1)
            );
            let right = sql(&store, 2, row, Datum::Real(1e100));
            let diag = f.parents[0].plans[1].base;
            assert!(
                matches!(hosts.resume(&mut eval, &task, reply(&f.field, 1, &right)).unwrap(), HostStep::NeedArg(r) if r.index == diag)
            );
            assert_eq!(*ctx.events.borrow(), ["level"]);
            let token = store
                .borrow_mut()
                .insert_diagnostic_text(
                    "actual_source".into(),
                    NativeValueOrigin {
                        node: f.parents[0].arguments[diag],
                        row,
                    },
                )
                .unwrap();
            let values = VectorValue::from_scalar(&ScalarValue::Bytes(Some(token)), 1);
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                hosts.resume(&mut eval, &task, reply(&f.field, diag, &values))
            }));
            match mode {
                1 => assert!(result.is_err()),
                2 => assert!(result.unwrap().is_err()),
                _ => {
                    let HostStep::Ready(values) = result.unwrap().unwrap() else {
                        panic!("subject completion must finish original arithmetic")
                    };
                    let ScalarValueRef::Bytes(Some(token)) = values.get_scalar_ref(0) else {
                        panic!()
                    };
                    let got = store
                        .borrow()
                        .get(
                            token,
                            NativeValueRole::Output,
                            NativeValueOrigin { node: 0, row },
                        )
                        .unwrap()
                        .clone();
                    assert!(matches!(got, Datum::Decimal(_)));
                }
            }
            assert!(ctx.events.borrow()[1].contains("actual_source"));
            if mode == 2 {
                assert!(
                    !ctx.events.borrow().iter().any(|event| event == "precision"),
                    "invalidated conversion cannot reach the kernel"
                );
            }
            let count = ctx.events.borrow().len();
            assert!(hosts
                .resume(&mut eval, &task, reply(&f.field, diag, &values))
                .is_err());
            assert_eq!(ctx.events.borrow().len(), count);
            hosts.cancel(&task);
            assert!(tidb_query_expr::local::value_kernel_work_remaining().is_none());
        }
    }
    fn fixture_window(name: &str, source: FieldType, result: FieldType) -> Fixture {
        let mut f = fixture(name, source, result, false);
        f.catalog = HostCatalog::for_sql_batch(vec![(
            SqlBatchHostOperation::EagerValuesWindowNative,
            HostSignature {
                arg_types: vec![f.field.clone(); f.parents[0].arguments.len()].into(),
                return_type: f.field.clone(),
            },
        )])
        .unwrap();
        let NativeHostMetadata::Arithmetic {
            batch,
            operation,
            generic_mode,
            source_types,
            result_type,
            literal,
            ..
        } = &mut f.metadata[0]
        else {
            unreachable!()
        };
        *operation = match name {
            "plus" => SqlBatchHostOperation::AddWindowNative,
            "minus" => SqlBatchHostOperation::SubtractWindowNative,
            "mul" => SqlBatchHostOperation::MultiplyWindowNative,
            "mod" => SqlBatchHostOperation::ModuloWindowNative,
            "div" => SqlBatchHostOperation::DivideWindowNative,
            "intdiv" => SqlBatchHostOperation::IntegerDivideWindowNative,
            _ => unreachable!(),
        };
        let Mode::Numeric(domain) = *generic_mode else {
            panic!()
        };
        *batch = Some(WindowArithmeticMetadata {
            domain,
            integer: domain == NativeEvalType::Int
                && source_types[0].eval_type() == NativeEvalType::Int
                && result_type.eval_type() == NativeEvalType::Int,
            output_integer: result_type.eval_type() == NativeEvalType::Int,
            sources: [WindowArithmeticSource::TypedReady; 2],
        });
        *literal = [true; 2];
        f
    }
    fn window_values(
        store: &RefCell<NativeValueStore>,
        node: usize,
        start: usize,
        rows: &[usize],
        values: Vec<Datum>,
    ) -> VectorValue {
        assert_eq!(rows.len(), values.len());
        let mut output = ChunkedVecBytes::try_with_capacities(
            rows.len(),
            rows.len() * native_values::HANDLE_BYTES,
        )
        .unwrap();
        for (p, (&input_row, value)) in rows.iter().zip(values).enumerate() {
            let token = store
                .borrow_mut()
                .insert_clone(
                    &value,
                    NativeValueRole::CharInput,
                    NativeValueOrigin {
                        node,
                        row: InputRow {
                            occurrence: start + p,
                            input_row,
                        },
                    },
                )
                .unwrap();
            output.push_ref(Some(&token));
        }
        VectorValue::Bytes(output)
    }
    fn window_reply<'a>(
        f: &'a Fixture,
        index: usize,
        start: usize,
        rows: &'a [usize],
        values: &'a VectorValue,
    ) -> HostWindowArgReply<'a> {
        HostWindowArgReply {
            index,
            occurrence_start: start,
            physical_rows: rows,
            field_type: &f.field,
            values,
        }
    }
    #[test]
    fn numeric_window_subject_finishes_whole_left_before_whole_right_and_authenticates_scoped_resume(
    ) {
        let f = fixture_window(
            "minus",
            FieldType::new(FieldTypeCode::Double),
            FieldType::new(FieldTypeCode::NewDecimal),
        );
        let store = RefCell::new(NativeValueStore::new(1 << 20).unwrap());
        let ctx = Warning {
            store: &store,
            events: RefCell::new(vec![]),
            mode: Cell::new(0),
        };
        let mut hosts = NativeBatchHosts::new(f.catalog.clone(), &f.metadata, &ctx, 1 << 20)
            .with_native_values(Some(&store))
            .with_numeric_parents(&f.parents)
            .unwrap();
        let rows = [2, 0, 2];
        let start = 11;
        let mut eval = EvalContext::default();
        for mode in [1, 2, 0] {
            store.borrow_mut().begin_window().unwrap();
            ctx.mode.set(mode);
            ctx.events.borrow_mut().clear();
            let args = vec![f.field.clone(); f.parents[0].arguments.len()];
            let HostStart::Pending { task, request } = hosts
                .start_window(
                    &mut eval,
                    HostWindowInvocation {
                        slot: hosts.catalog.slot(0).unwrap(),
                        occurrence_start: start,
                        physical_rows: &rows,
                        arg_types: &args,
                        return_type: &f.field,
                    },
                )
                .unwrap()
            else {
                panic!()
            };
            assert_eq!(request.index, 0);
            let values = window_values(
                &store,
                1,
                start,
                &rows,
                vec![Datum::Real(1.25), Datum::Real(1e100), Datum::Real(2.5)],
            );
            let diag = f.parents[0].plans[0].base;
            assert!(
                matches!(hosts.resume_window(&mut eval, &task, window_reply(&f, 0, start, &rows, &values)).unwrap(), HostStep::NeedArgRow { index, position: 1 } if index == diag)
            );
            assert_eq!(*ctx.events.borrow(), ["level"]);
            let row = InputRow {
                occurrence: start + 1,
                input_row: 0,
            };
            let token = store
                .borrow_mut()
                .insert_diagnostic_text(
                    "whole_left".into(),
                    NativeValueOrigin {
                        node: f.parents[0].arguments[diag],
                        row,
                    },
                )
                .unwrap();
            let text = VectorValue::from_scalar(&ScalarValue::Bytes(Some(token)), 1);
            assert!(hosts
                .resume(&mut eval, &task, reply(&f.field, diag, &text))
                .is_err());
            assert!(hosts
                .resume_window(&mut eval, &task, window_reply(&f, diag, start, &[0], &text))
                .is_err());
            assert!(hosts
                .resume_window(
                    &mut eval,
                    &task,
                    window_reply(&f, diag, start + 1, &[2], &text)
                )
                .is_err());
            let null = sql(&store, f.parents[0].arguments[diag], row, Datum::Null);
            assert!(hosts
                .resume_window(
                    &mut eval,
                    &task,
                    window_reply(&f, diag, start + 1, &[0], &null)
                )
                .is_err());
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                hosts.resume_window(
                    &mut eval,
                    &task,
                    window_reply(&f, diag, start + 1, &[0], &text),
                )
            }));
            match mode {
                1 => assert!(result.is_err()),
                2 => assert!(result.unwrap().is_err()),
                _ => {
                    assert!(
                        matches!(result.unwrap().unwrap(), HostStep::NeedArg(r) if r.index == 1 && r.mode == ArgMode::Fresh)
                    );
                    let right = window_values(&store, 2, start, &rows, vec![Datum::Real(0.0); 3]);
                    let HostStep::Ready(output) = hosts
                        .resume_window(&mut eval, &task, window_reply(&f, 1, start, &rows, &right))
                        .unwrap()
                    else {
                        panic!()
                    };
                    assert_eq!(output.len(), 3);
                    let ScalarValueRef::Bytes(Some(token)) = output.get_scalar_ref(0) else {
                        panic!()
                    };
                    let expected = tidb_datatype::Decimal::from_literal("1.25");
                    assert!(
                        matches!(store.borrow().get(token, NativeValueRole::Output, NativeValueOrigin { node: 0, row: InputRow { occurrence: start, input_row: rows[0] } }).unwrap(), Datum::Decimal(value) if value == &expected)
                    );
                }
            }
            let n = ctx.events.borrow().len();
            assert!(hosts
                .resume_window(
                    &mut eval,
                    &task,
                    window_reply(&f, diag, start + 1, &[0], &text)
                )
                .is_err());
            assert_eq!(ctx.events.borrow().len(), n);
            hosts.cancel(&task);
            assert!(tidb_query_expr::local::value_kernel_work_remaining().is_none());
        }
    }
    #[test]
    fn numeric_window_overflow_requests_original_row_only_after_both_whole_sources() {
        for name in ["plus", "minus", "mul"] {
            for unavailable in [false, true] {
                let int = FieldType::new(FieldTypeCode::LongLong);
                let f = fixture_window(name, int.clone(), int);
                let store = RefCell::new(NativeValueStore::new(1 << 20).unwrap());
                store.borrow_mut().begin_window().unwrap();
                let mut hosts = NativeBatchHosts::new(
                    f.catalog.clone(),
                    &f.metadata,
                    &crate::NoColumns,
                    1 << 20,
                )
                .with_native_values(Some(&store))
                .with_numeric_parents(&f.parents)
                .unwrap();
                let rows = [2, 0, 2];
                let start = 11;
                let mut eval = EvalContext::default();
                let args = vec![f.field.clone(); f.parents[0].arguments.len()];
                let HostStart::Pending { task, .. } = hosts
                    .start_window(
                        &mut eval,
                        HostWindowInvocation {
                            slot: hosts.catalog.slot(0).unwrap(),
                            occurrence_start: start,
                            physical_rows: &rows,
                            arg_types: &args,
                            return_type: &f.field,
                        },
                    )
                    .unwrap()
                else {
                    panic!()
                };
                let values = window_values(
                    &store,
                    1,
                    start,
                    &rows,
                    vec![Datum::Int(1), Datum::Int(i64::MAX), Datum::Int(1)],
                );
                assert!(
                    matches!(hosts.resume_window(&mut eval, &task, window_reply(&f, 0, start, &rows, &values)).unwrap(), HostStep::NeedArg(r) if r.index == 1)
                );
                let values = window_values(
                    &store,
                    2,
                    start,
                    &rows,
                    vec![Datum::Int(if name == "minus" { -1 } else { 2 }); 3],
                );
                let left = f.parents[0].plans[2].base;
                assert!(
                    matches!(hosts.resume_window(&mut eval, &task, window_reply(&f, 1, start, &rows, &values)).unwrap(), HostStep::NeedArgRow { index, position: 1 } if index == left)
                );
                let row = InputRow {
                    occurrence: start + 1,
                    input_row: 0,
                };
                let origin = NativeValueOrigin {
                    node: f.parents[0].arguments[left],
                    row,
                };
                let token = if unavailable {
                    store.borrow_mut().insert_diagnostic_unavailable(origin)
                } else {
                    store
                        .borrow_mut()
                        .insert_diagnostic_text("7".into(), origin)
                }
                .unwrap();
                let text = VectorValue::from_scalar(&ScalarValue::Bytes(Some(token)), 1);
                let result = hosts.resume_window(
                    &mut eval,
                    &task,
                    window_reply(&f, left, start + 1, &[0], &text),
                );
                if unavailable {
                    assert!(result.is_err());
                    assert!(matches!(
                        hosts.take_native_error(),
                        Some(EvalError::IntOverflow)
                    ));
                } else {
                    let right = f.parents[0].plans[3].base;
                    assert!(
                        matches!(result.unwrap(), HostStep::NeedArgRow { index, position: 1 } if index == right)
                    );
                    let token = store
                        .borrow_mut()
                        .insert_diagnostic_text(
                            "9".into(),
                            NativeValueOrigin {
                                node: f.parents[0].arguments[right],
                                row,
                            },
                        )
                        .unwrap();
                    let text = VectorValue::from_scalar(&ScalarValue::Bytes(Some(token)), 1);
                    assert!(hosts
                        .resume_window(
                            &mut eval,
                            &task,
                            window_reply(&f, right, start + 1, &[0], &text)
                        )
                        .is_err());
                    assert!(
                        matches!(hosts.take_native_error(), Some(EvalError::DataOutOfRange { value: "BIGINT", expression }) if expression == format!("(7 {} 9)", if name == "plus" { "+" } else if name == "minus" { "-" } else { "*" }))
                    );
                }
                assert!(hosts
                    .resume_window(
                        &mut eval,
                        &task,
                        window_reply(&f, left, start + 1, &[0], &text)
                    )
                    .is_err());
                hosts.cancel(&task);
            }
        }
    }
    #[test]
    fn numeric_window_consumes_callbacks_and_stops_before_next_row_after_generation_changes() {
        let real = FieldType::new(FieldTypeCode::Double);
        let f = fixture_window("plus", real.clone(), real);
        let store = RefCell::new(NativeValueStore::new(1 << 20).unwrap());
        let ctx = Callback {
            store: &store,
            mode: Cell::new(0),
            calls: Cell::new(0),
        };
        let mut hosts = NativeBatchHosts::new(f.catalog.clone(), &f.metadata, &ctx, 1 << 20)
            .with_native_values(Some(&store))
            .with_numeric_parents(&f.parents)
            .unwrap();
        let rows = [2, 0, 2];
        let start = 11;
        let mut eval = EvalContext::default();
        for mode in [1, 2, 0] {
            store.borrow_mut().begin_window().unwrap();
            ctx.mode.set(mode);
            let before = ctx.calls.get();
            let args = vec![f.field.clone(); f.parents[0].arguments.len()];
            let HostStart::Pending { task, .. } = hosts
                .start_window(
                    &mut eval,
                    HostWindowInvocation {
                        slot: hosts.catalog.slot(0).unwrap(),
                        occurrence_start: start,
                        physical_rows: &rows,
                        arg_types: &args,
                        return_type: &f.field,
                    },
                )
                .unwrap()
            else {
                panic!()
            };
            let left = window_values(&store, 1, start, &rows, vec![Datum::Real(1.0); 3]);
            assert!(matches!(
                hosts
                    .resume_window(&mut eval, &task, window_reply(&f, 0, start, &rows, &left))
                    .unwrap(),
                HostStep::NeedArg(_)
            ));
            let right = window_values(&store, 2, start, &rows, vec![Datum::Real(2.0); 3]);
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                hosts.resume_window(&mut eval, &task, window_reply(&f, 1, start, &rows, &right))
            }));
            match mode {
                1 => assert!(result.is_err()),
                2 => assert!(result.unwrap().is_err()),
                _ => assert!(matches!(result.unwrap().unwrap(), HostStep::Ready(_))),
            }
            assert_eq!(
                ctx.calls.get() - before,
                if mode == 0 { 3 } else { 1 },
                "invalidated input must stop before another row callback"
            );
            let calls = ctx.calls.get();
            assert!(hosts
                .resume_window(&mut eval, &task, window_reply(&f, 1, start, &rows, &right))
                .is_err());
            assert_eq!(ctx.calls.get(), calls);
            hosts.cancel(&task);
        }
    }
    struct Zero<'a> {
        store: &'a RefCell<NativeValueStore>,
        mode: Cell<usize>,
        calls: Cell<usize>,
    }
    impl Columns for Zero<'_> {
        fn get(&self, _: &[String]) -> Option<Datum> {
            panic!("no expression callback")
        }
        fn handle_division_by_zero(&self) -> std::result::Result<(), EvalError> {
            self.calls.set(self.calls.get() + 1);
            let mut store = self.store.borrow_mut();
            match self.mode.get() {
                1 => panic!("zero callback"),
                2 => {
                    store.begin_window().unwrap();
                }
                3 => return Err(EvalError::DivisionByZero),
                _ => (),
            }
            Ok(())
        }
    }
    #[test]
    fn paired_quotient_zero_callbacks_are_consuming_and_stop_before_next_row_or_publication() {
        for name in ["mod", "div", "intdiv"] {
            for window in [false, true] {
                let real = FieldType::new(if name == "intdiv" {
                    FieldTypeCode::LongLong
                } else {
                    FieldTypeCode::Double
                });
                let f = if window {
                    fixture_window(name, real.clone(), real)
                } else {
                    fixture(name, real.clone(), real, false)
                };
                let store = RefCell::new(NativeValueStore::new(1 << 20).unwrap());
                let ctx = Zero {
                    store: &store,
                    mode: Cell::new(0),
                    calls: Cell::new(0),
                };
                let mut hosts =
                    NativeBatchHosts::new(f.catalog.clone(), &f.metadata, &ctx, 1 << 20)
                        .with_native_values(Some(&store))
                        .with_numeric_parents(&f.parents)
                        .unwrap();
                let rows = [2, 0, 2];
                let occurrence = 11;
                let mut eval = EvalContext::default();
                for mode in [1, 2, 3, 0] {
                    store.borrow_mut().begin_window().unwrap();
                    ctx.mode.set(mode);
                    let before = ctx.calls.get();
                    let args = vec![f.field.clone(); f.parents[0].arguments.len()];
                    let task = if window {
                        let HostStart::Pending { task, .. } = hosts
                            .start_window(
                                &mut eval,
                                HostWindowInvocation {
                                    slot: hosts.catalog.slot(0).unwrap(),
                                    occurrence_start: occurrence,
                                    physical_rows: &rows,
                                    arg_types: &args,
                                    return_type: &f.field,
                                },
                            )
                            .unwrap()
                        else {
                            panic!()
                        };
                        task
                    } else {
                        start(
                            &mut hosts,
                            &mut eval,
                            &f,
                            InputRow {
                                occurrence: occurrence,
                                input_row: rows[0],
                            },
                        )
                    };
                    let count = if window { 3 } else { 1 };
                    let left = window_values(
                        &store,
                        1,
                        occurrence,
                        &rows[..count],
                        vec![
                            if name == "intdiv" {
                                Datum::Int(7)
                            } else {
                                Datum::Real(7.0)
                            };
                            count
                        ],
                    );
                    let right = window_values(
                        &store,
                        2,
                        occurrence,
                        &rows[..count],
                        vec![
                            if name == "intdiv" {
                                Datum::Int(0)
                            } else {
                                Datum::Real(0.0)
                            };
                            count
                        ],
                    );
                    let first = if window {
                        hosts.resume_window(
                            &mut eval,
                            &task,
                            window_reply(&f, 0, occurrence, &rows, &left),
                        )
                    } else {
                        hosts.resume(&mut eval, &task, reply(&f.field, 0, &left))
                    };
                    assert!(matches!(first.unwrap(), HostStep::NeedArg(r) if r.index == 1));
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        if window {
                            hosts.resume_window(
                                &mut eval,
                                &task,
                                window_reply(&f, 1, occurrence, &rows, &right),
                            )
                        } else {
                            hosts.resume(&mut eval, &task, reply(&f.field, 1, &right))
                        }
                    }));
                    match mode {
                        1 => assert!(result.is_err()),
                        2 | 3 => assert!(result.unwrap().is_err()),
                        _ => {
                            let HostStep::Ready(value) = result.unwrap().unwrap() else {
                                panic!()
                            };
                            assert_eq!(value.len(), count);
                        }
                    }
                    assert_eq!(ctx.calls.get() - before, if mode == 0 { count } else { 1 });
                    if mode == 3 {
                        assert!(matches!(
                            hosts.take_native_error(),
                            Some(EvalError::DivisionByZero)
                        ));
                    } else {
                        assert!(hosts.take_native_error().is_none());
                    }
                    let calls = ctx.calls.get();
                    let replay = if window {
                        hosts.resume_window(
                            &mut eval,
                            &task,
                            window_reply(&f, 1, occurrence, &rows, &right),
                        )
                    } else {
                        hosts.resume(&mut eval, &task, reply(&f.field, 1, &right))
                    };
                    assert!(replay.is_err());
                    assert_eq!(ctx.calls.get(), calls);
                    hosts.cancel(&task);
                    assert!(tidb_query_expr::local::value_kernel_work_remaining().is_none());
                }
            }
        }
    }
    #[test]
    fn integer_division_context_promotion_pauses_before_warning_and_converts_left_before_right_null(
    ) {
        for right_null in [true, false] {
            let source = FieldType::new(FieldTypeCode::Double);
            let f = fixture(
                "intdiv",
                source.clone(),
                FieldType::new(FieldTypeCode::VarString),
                false,
            );
            let store = RefCell::new(NativeValueStore::new(1 << 20).unwrap());
            let ctx = Warning {
                store: &store,
                events: RefCell::new(vec![]),
                mode: Cell::new(0),
            };
            let mut hosts = NativeBatchHosts::new(f.catalog.clone(), &f.metadata, &ctx, 1 << 20)
                .with_native_values(Some(&store))
                .with_numeric_parents(&f.parents)
                .unwrap();
            let row = InputRow {
                occurrence: 11,
                input_row: 7,
            };
            let mut eval = EvalContext::default();
            for mode in [1, 2, 0] {
                store.borrow_mut().begin_window().unwrap();
                ctx.mode.set(mode);
                ctx.events.borrow_mut().clear();
                let task = start(&mut hosts, &mut eval, &f, row);
                let left_datum = Datum::Real(if right_null { 1e100 } else { 1.25 });
                let right_datum = if right_null {
                    Datum::Null
                } else {
                    Datum::Real(1e100)
                };
                let left = sql(&store, 1, row, left_datum.clone());
                let right = sql(&store, 2, row, right_datum.clone());
                assert!(
                    matches!(hosts.resume(&mut eval, &task, reply(&f.field, 0, &left)).unwrap(), HostStep::NeedArg(r) if r.index == 1)
                );
                let argument = usize::from(!right_null);
                let diag = f.parents[0].plans[argument].base;
                assert!(
                    matches!(hosts.resume(&mut eval, &task, reply(&f.field, 1, &right)).unwrap(), HostStep::NeedArg(r) if r.index == diag)
                );
                assert_eq!(*ctx.events.borrow(), ["precision", "level"]);
                let subject = if right_null {
                    "left_before_null"
                } else {
                    "right_after_left"
                };
                let origin = NativeValueOrigin {
                    node: f.parents[0].arguments[diag],
                    row,
                };
                let null = sql(&store, origin.node, row, Datum::Null);
                assert!(hosts
                    .resume(&mut eval, &task, reply(&f.field, diag, &null))
                    .is_err());
                let token = store
                    .borrow_mut()
                    .insert_diagnostic_text(subject.into(), origin)
                    .unwrap();
                let text = VectorValue::from_scalar(&ScalarValue::Bytes(Some(token)), 1);
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    hosts.resume(&mut eval, &task, reply(&f.field, diag, &text))
                }));
                match mode {
                    1 => assert!(result.is_err()),
                    2 => {
                        assert!(result.unwrap().is_err());
                        assert_eq!(
                            ctx.events
                                .borrow()
                                .iter()
                                .filter(|e| e.as_str() == "precision")
                                .count(),
                            1
                        );
                    }
                    _ => {
                        let HostStep::Ready(values) = result.unwrap().unwrap() else {
                            panic!("DIV must finish the owned continuation")
                        };
                        let ScalarValueRef::Bytes(Some(token)) = values.get_scalar_ref(0) else {
                            panic!()
                        };
                        let got = store
                            .borrow()
                            .get(
                                token,
                                NativeValueRole::Output,
                                NativeValueOrigin { node: 0, row },
                            )
                            .unwrap()
                            .clone();
                        let oldstore = RefCell::new(NativeValueStore::new(4096).unwrap());
                        let old = Warning {
                            store: &oldstore,
                            events: RefCell::new(vec![]),
                            mode: Cell::new(0),
                        };
                        let _ = old.div_precision_increment();
                        let prepared = crate::ops::quotient::prepare_integer_division(
                            left_datum,
                            right_datum,
                            [false; 2],
                            &old,
                            &mut |_, value| {
                                crate::ops::decimal_div_operand_value(
                                    value,
                                    Some(&source),
                                    &old,
                                    || Some(subject.into()),
                                )
                            },
                        )
                        .unwrap();
                        let value = prepared.compute(1 << 20, &old).unwrap();
                        let expected = crate::scalar_function::coerce_function_result(
                            "intdiv",
                            Some(&FieldType::new(FieldTypeCode::VarString)),
                            value,
                        )
                        .unwrap();
                        assert_eq!(got, expected);
                        assert_eq!(*ctx.events.borrow(), *old.events.borrow());
                    }
                }
                let count = ctx.events.borrow().len();
                assert!(hosts
                    .resume(&mut eval, &task, reply(&f.field, diag, &text))
                    .is_err());
                assert_eq!(ctx.events.borrow().len(), count);
                hosts.cancel(&task);
                assert!(tidb_query_expr::local::value_kernel_work_remaining().is_none());
            }
        }
    }
    #[test]
    fn integer_division_context_decimal_overflow_reads_both_reports_even_when_left_unavailable() {
        for unsigned in [false, true] {
            for unavailable_left in [false, true] {
                for unavailable_right in [false, true] {
                    let source = FieldType::new(FieldTypeCode::NewDecimal).with_unsigned(unsigned);
                    let f = fixture(
                        "intdiv",
                        source,
                        FieldType::new(FieldTypeCode::LongLong),
                        false,
                    );
                    let store = RefCell::new(NativeValueStore::new(1 << 20).unwrap());
                    store.borrow_mut().begin_window().unwrap();
                    let mut hosts = NativeBatchHosts::new(
                        f.catalog.clone(),
                        &f.metadata,
                        &crate::NoColumns,
                        1 << 20,
                    )
                    .with_native_values(Some(&store))
                    .with_numeric_parents(&f.parents)
                    .unwrap();
                    let row = InputRow {
                        occurrence: 11,
                        input_row: 7,
                    };
                    let mut eval = EvalContext::default();
                    let task = start(&mut hosts, &mut eval, &f, row);
                    let left = sql(
                        &store,
                        1,
                        row,
                        Datum::Decimal(tidb_datatype::Decimal::from_literal(
                            "18446744073709551616",
                        )),
                    );
                    let right = sql(
                        &store,
                        2,
                        row,
                        Datum::Decimal(tidb_datatype::Decimal::from_int(1)),
                    );
                    assert!(
                        matches!(hosts.resume(&mut eval, &task, reply(&f.field, 0, &left)).unwrap(), HostStep::NeedArg(r) if r.index == 1)
                    );
                    let l = f.parents[0].plans[2].base;
                    let r = f.parents[0].plans[3].base;
                    assert!(
                        matches!(hosts.resume(&mut eval, &task, reply(&f.field, 1, &right)).unwrap(), HostStep::NeedArg(request) if request.index == l)
                    );
                    let origin = NativeValueOrigin {
                        node: f.parents[0].arguments[l],
                        row,
                    };
                    let token = if unavailable_left {
                        store.borrow_mut().insert_diagnostic_unavailable(origin)
                    } else {
                        store
                            .borrow_mut()
                            .insert_diagnostic_text("7".into(), origin)
                    }
                    .unwrap();
                    let left_text = VectorValue::from_scalar(&ScalarValue::Bytes(Some(token)), 1);
                    assert!(
                        matches!(hosts.resume(&mut eval, &task, reply(&f.field, l, &left_text)).unwrap(), HostStep::NeedArg(request) if request.index == r && request.mode == ArgMode::Fresh)
                    );
                    let null = sql(&store, f.parents[0].arguments[r], row, Datum::Null);
                    assert!(hosts
                        .resume(&mut eval, &task, reply(&f.field, r, &null))
                        .is_err());
                    let origin = NativeValueOrigin {
                        node: f.parents[0].arguments[r],
                        row,
                    };
                    let token = if unavailable_right {
                        store.borrow_mut().insert_diagnostic_unavailable(origin)
                    } else {
                        store
                            .borrow_mut()
                            .insert_diagnostic_text("9".into(), origin)
                    }
                    .unwrap();
                    let right_text = VectorValue::from_scalar(&ScalarValue::Bytes(Some(token)), 1);
                    assert!(hosts
                        .resume(&mut eval, &task, reply(&f.field, r, &right_text))
                        .is_err());
                    let error = hosts.take_native_error().unwrap();
                    if unavailable_left || unavailable_right {
                        assert!(matches!(error, EvalError::IntOverflow));
                    } else {
                        assert!(
                            matches!(error, EvalError::DataOutOfRange { value, expression } if value == if unsigned { "BIGINT UNSIGNED" } else { "BIGINT" } && expression == "(7 DIV 9)")
                        );
                    }
                    assert!(hosts
                        .resume(&mut eval, &task, reply(&f.field, r, &right_text))
                        .is_err());
                    hosts.cancel(&task);
                }
            }
        }
    }
}
