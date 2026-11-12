// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Owned DIV promotion and its original early Decimal diagnostic phase.
use super::*;
use crate::ops::quotient::{IntegerDivisionStep, PreparedIntegerDivision};
impl NativeBatchHosts<'_> {
    pub(super) fn begin_integer_division_row(
        &self,
        slot: HostSlot,
        task: Box<NumericTask>,
        left: Datum,
        right: Datum,
        available: usize,
        auth: &NumericAuthentication<'_>,
    ) -> LocalResult<Transition> {
        let Some(NativeHostMetadata::Arithmetic { source_types, .. }) =
            self.metadata.get(slot.index())
        else {
            return Err(binding("DIV source metadata absent"));
        };
        let unsigned = arithmetic_unsigned(source_types);
        let (left, right) = crate::binary_literal::cast_signed_literal_operands(
            BinaryOp::IntDiv,
            left,
            right,
            arithmetic_signed(source_types),
        );
        // This route precedes the generic precision read, and retains the
        // actual converted Decimals for the original cast-width refinement.
        let (left, right) = match (left, right) {
            (Datum::Decimal(left), Datum::Decimal(right)) => {
                let unsigned = unsigned[0] || unsigned[1];
                let result = crate::ops::quotient::decimal_integer(
                    &left,
                    &right,
                    unsigned,
                    available,
                    self.context,
                );
                auth.check()?;
                if matches!(result, Err(EvalError::IntOverflow)) {
                    return self.begin_decimal_division_diagnostic(
                        slot,
                        task,
                        [left, right],
                        unsigned,
                        auth,
                    );
                }
                return self.numeric_result(slot, task, result, auth);
            }
            pair => pair,
        };
        let _ = self.context.div_precision_increment();
        auth.check()?;
        let step =
            crate::ops::quotient::begin_integer_division(left, right, unsigned, self.context);
        auth.check()?;
        match step {
            Ok(step) => self.drive_integer_division(slot, task, step, available, auth),
            Err(error) => Ok(Transition::Failure(error)),
        }
    }
    pub(super) fn drive_integer_division(
        &self,
        slot: HostSlot,
        mut task: Box<NumericTask>,
        mut step: IntegerDivisionStep,
        available: usize,
        auth: &NumericAuthentication<'_>,
    ) -> LocalResult<Transition> {
        let parent = self
            .numeric_parent(slot)
            .ok_or_else(|| binding("DIV parent absent"))?;
        let Some(NativeHostMetadata::Arithmetic {
            source_types,
            generic_mode,
            ..
        }) = self.metadata.get(slot.index())
        else {
            return Err(binding("DIV metadata absent"));
        };
        loop {
            match step {
                IntegerDivisionStep::Ready(prepared) => {
                    let mut result = prepared.compute(available, self.context);
                    auth.check()?;
                    if *generic_mode == Mode::Numeric(NativeEvalType::Int)
                        && matches!(result, Err(EvalError::IntOverflow))
                    {
                        if let PreparedIntegerDivision::Integer { left, right } = prepared {
                            result = Err(crate::scalar_function::integer_division_overflow(
                                left, right,
                            ));
                        }
                    }
                    return self.numeric_result(slot, task, result, auth);
                }
                IntegerDivisionStep::Operand {
                    index,
                    value,
                    continuation,
                } => {
                    let result = crate::scalar_function::prepare_native_integer_division_operand(
                        source_types[index].as_ref(),
                        value,
                        self.context,
                    );
                    auth.check()?;
                    match result {
                        Ok(NumericOperandStep::Ready(Datum::Decimal(value))) => {
                            step = continuation.finish(value)
                        }
                        Ok(NumericOperandStep::Ready(_)) => {
                            return Ok(Transition::Failure(EvalError::Unsupported(
                                "DIV decimal argument domain",
                            )))
                        }
                        Ok(NumericOperandStep::Subject(pending)) => {
                            task.index = index;
                            let NumericPlanBody::Subject(plan) = &parent.plans[index].body else {
                                unreachable!()
                            };
                            let mut diag = NumericDiagnostic {
                                plan: index,
                                state: plan.state(),
                                reason: DiagnosticReason::IntegerDivision {
                                    pending,
                                    continuation,
                                },
                            };
                            let first = match plan.step(&mut diag.state) {
                                Ok(step) => step,
                                Err(error) => return Ok(Transition::Failure(error)),
                            };
                            match self.numeric_diagnostic_step(task, diag, first, slot, auth)? {
                                DiagnosticTransition::Need(task) => {
                                    return Ok(Transition::Need(task))
                                }
                                DiagnosticTransition::Failure(error) => {
                                    return Ok(Transition::Failure(error))
                                }
                                DiagnosticTransition::IntegerDivision(next_task, next_step) => {
                                    task = next_task;
                                    step = next_step;
                                }
                                DiagnosticTransition::Operand(_, _) => {
                                    return Err(binding(
                                        "DIV promotion resumed an ordinary operand",
                                    ))
                                }
                            }
                        }
                        Err(error) => return Ok(Transition::Failure(error)),
                    }
                }
            }
        }
    }
    fn begin_decimal_division_diagnostic(
        &self,
        slot: HostSlot,
        task: Box<NumericTask>,
        values: [tidb_datatype::Decimal; 2],
        unsigned: bool,
        auth: &NumericAuthentication<'_>,
    ) -> LocalResult<Transition> {
        let parent = self
            .numeric_parent(slot)
            .ok_or_else(|| binding("DIV diagnostic parent absent"))?;
        let NumericPlanBody::Argument(plan) = &parent.plans[2].body else {
            unreachable!()
        };
        let mut state = plan.state();
        let step = plan.step(&mut state, Some(&values[0]));
        let step = match step {
            Ok(step) => step,
            Err(error) => return Ok(Transition::Failure(error)),
        };
        let diag = NumericDiagnostic {
            plan: 2,
            state,
            reason: DiagnosticReason::DecimalDivision {
                values,
                left: None,
                unsigned,
            },
        };
        match self.numeric_diagnostic_step(task, diag, step, slot, auth)? {
            DiagnosticTransition::Need(task) => Ok(Transition::Need(task)),
            DiagnosticTransition::Failure(error) => Ok(Transition::Failure(error)),
            DiagnosticTransition::Operand(_, _) | DiagnosticTransition::IntegerDivision(_, _) => {
                Err(binding("DIV overflow diagnostic returned a value"))
            }
        }
    }
}
