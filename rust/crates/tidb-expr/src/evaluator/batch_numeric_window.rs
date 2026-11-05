// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Complete typed source phases with scoped diagnostic continuations in the
//! same SDK window frame. Values are owned before any callback can run.
use super::*;

pub(in super::super) struct NumericWindowTask {
    rows: Vec<usize>,
    index: usize,
    position: usize,
    computing: bool,
    presenting: bool,
    values: [Vec<Datum>; 2],
    pending: Option<Box<NumericTask>>,
    output: Option<ChunkedVecBytes>,
}
impl NumericWindowTask {
    pub(in super::super) fn next_argument(&self) -> usize {
        self.pending.as_ref().map_or(self.index, |s| s.next)
    }
    #[cfg(test)]
    pub(in super::super) fn retained_bytes(&self) -> LocalResult<usize> {
        std::mem::size_of::<Self>()
            .checked_add(array_bytes::<usize>(self.rows.capacity())?)
            .and_then(|n| {
                self.values.iter().try_fold(n, |n, values| {
                    values.iter().try_fold(
                        n.checked_add(array_bytes::<Datum>(values.capacity()).ok()?)?,
                        |n, value| n.checked_add(native_payload_bytes(value).ok()?.checked_mul(2)?),
                    )
                })
            })
            .and_then(|n| {
                n.checked_add(
                    self.pending
                        .as_ref()
                        .map_or(Ok(0), |p| p.retained_bytes())
                        .ok()?,
                )
            })
            .and_then(|n| {
                n.checked_add(
                    self.output
                        .as_ref()
                        .map_or(Some(0), ChunkedVecBytes::retained_heap_bytes)?,
                )
            })
            .ok_or_else(|| resource("numeric window retained extent overflow"))
    }
}
enum WindowTransition {
    Need(Box<NumericWindowTask>),
    Ready(VectorValue),
    Failure(EvalError),
}
impl NativeBatchHosts<'_> {
    pub(in super::super) fn start_numeric_window(
        &mut self,
        invocation: HostWindowInvocation<'_>,
    ) -> LocalResult<HostStart> {
        let parent = self
            .numeric_parent(invocation.slot)
            .ok_or_else(|| binding("numeric window parent absent"))?;
        let signature = self.catalog.signature(invocation.slot)?;
        if invocation.physical_rows.is_empty()
            || signature.arg_types.as_ref() != invocation.arg_types
            || &signature.return_type != invocation.return_type
            || invocation
                .physical_rows
                .len()
                .checked_sub(1)
                .and_then(|n| invocation.occurrence_start.checked_add(n))
                .is_none()
            || self.native_values.is_none()
            || !matches!(
                self.metadata.get(invocation.slot.index()),
                Some(NativeHostMetadata::Arithmetic {
                    batch: Some(_),
                    result_type: Some(_),
                    source_types: [Some(_), Some(_)],
                    ..
                })
            )
        {
            return Err(binding("numeric window invocation differs"));
        }

        let mut rows = Vec::new();
        rows.try_reserve_exact(invocation.physical_rows.len())
            .map_err(|e| LocalError::ResourceLimit(e.to_string()))?;
        rows.extend_from_slice(invocation.physical_rows);
        let row = InputRow {
            occurrence: invocation.occurrence_start,
            input_row: rows[0],
        };
        self.install_task(
            invocation.slot,
            row,
            parent.arguments.len(),
            SqlDemand::NumericWindow(Box::new(NumericWindowTask {
                rows,
                index: 0,
                position: 0,
                computing: false,
                presenting: false,
                values: [Vec::new(), Vec::new()],
                pending: None,
                output: None,
            })),
        )
    }
    pub(in super::super) fn resume_numeric_window(
        &mut self,
        id: &HostTaskId,
        reply: HostWindowArgReply<'_>,
    ) -> LocalResult<HostStep> {
        let task = self.task(id)?;
        let (slot, start) = (task.slot, task.row.occurrence);
        let SqlDemand::NumericWindow(state) = &task.demand else {
            return Err(binding("numeric window continuation absent"));
        };
        let scoped = state.pending.is_some();
        let parent = self
            .numeric_parent(slot)
            .ok_or_else(|| binding("numeric window parent absent"))?;
        let position = state.position;
        let expected_start = if scoped { start + position } else { start };
        let expected_rows = if scoped {
            &state.rows[position..position + 1]
        } else {
            &state.rows[..]
        };
        if reply.index != state.next_argument()
            || reply.occurrence_start != expected_start
            || reply.physical_rows != expected_rows
            || reply.values.len() != expected_rows.len()
            || reply.values.eval_type() != TikvEvalType::Bytes
            || self.catalog.signature(slot)?.arg_types.get(reply.index) != Some(reply.field_type)
        {
            return Err(binding("numeric window reply differs from original demand"));
        }
        let node = parent.arguments[reply.index];
        let store = self
            .native_values
            .ok_or_else(|| binding("numeric window store absent"))?;
        let authenticate = || -> LocalResult<()> {
            for p in 0..reply.values.len() {
                let ScalarValueRef::Bytes(Some(token)) = reply.values.get_scalar_ref(p) else {
                    return Err(binding("numeric window reply cannot be wire NULL"));
                };
                let origin = NativeValueOrigin {
                    node,
                    row: InputRow {
                        occurrence: expected_start + p,
                        input_row: reply.physical_rows[p],
                    },
                };
                if scoped {
                    store.borrow().diagnostic_text(token, origin)?;
                } else {
                    store
                        .borrow()
                        .get(token, NativeValueRole::CharInput, origin)?;
                }
            }
            Ok(())
        };
        authenticate()?;
        let ScalarValueRef::Bytes(Some(first_token)) = reply.values.get_scalar_ref(0) else {
            unreachable!()
        };
        let auth = NumericAuthentication {
            store,
            token: first_token,
            origin: NativeValueOrigin {
                node,
                row: InputRow {
                    occurrence: expected_start,
                    input_row: reply.physical_rows[0],
                },
            },
            diagnostic: scoped,
        };
        let text = if scoped {
            let ScalarValueRef::Bytes(Some(token)) = reply.values.get_scalar_ref(0) else {
                unreachable!()
            };
            let origin = NativeValueOrigin {
                node,
                row: InputRow {
                    occurrence: expected_start,
                    input_row: reply.physical_rows[0],
                },
            };
            store.borrow().diagnostic_text(token, origin)?;

            store
                .borrow()
                .diagnostic_text(token, origin)?
                .map(str::to_owned)
        } else {
            None
        };
        let demand = std::mem::replace(
            &mut self.slots[id.slot]
                .task
                .as_mut()
                .expect("authenticated numeric window")
                .demand,
            SqlDemand::Consumed,
        );
        let SqlDemand::NumericWindow(mut state) = demand else {
            unreachable!()
        };
        if !scoped {
            let Some(NativeHostMetadata::Arithmetic {
                batch: Some(batch),
                source_types,
                binary_literal,
                ..
            }) = self.metadata.get(slot.index())
            else {
                unreachable!()
            };
            let field = arithmetic_typed_source(source_types, state.index)?;
            let target = crate::scalar_function::native_numeric_batch_source(
                field,
                binary_literal[state.index],
                batch.domain,
            );
            let values = self.collect_window_source_charged(
                &reply,
                WindowSourceFacts {
                    field,
                    owner: batch.sources[state.index],
                    node,
                    target,
                },
                Some(&auth),
            );
            if let Err(error) = values {
                let native = self.native_error.take();
                authenticate()?;
                return match native {
                    Some(native) => self.native_failure(native),
                    None => Err(error),
                };
            }
            let values = values?;
            state.values[state.index] = values;
        }

        let transition = (|| {
            let result = self.drive_numeric_window(slot, start, state, text.as_deref(), &auth);
            result
        })();
        authenticate()?;
        match transition? {
            WindowTransition::Need(state) => {
                let next = state.next_argument();
                let scoped = state.pending.is_some();
                let position = state.position;

                self.slots[id.slot]
                    .task
                    .as_mut()
                    .expect("authenticated numeric window")
                    .demand = SqlDemand::NumericWindow(state);
                Ok(if scoped {
                    HostStep::NeedArgRow {
                        index: next,
                        position,
                    }
                } else {
                    HostStep::NeedArg(HostArgRequest {
                        index: next,
                        mode: ArgMode::Fresh,
                    })
                })
            }
            WindowTransition::Failure(error) => self.native_failure(error),
            WindowTransition::Ready(value) => {
                self.slots[id.slot].task = None;
                Ok(HostStep::Ready(value))
            }
        }
    }

    fn drive_numeric_window(
        &self,
        slot: HostSlot,
        start: usize,
        mut task: Box<NumericWindowTask>,
        text: Option<&str>,
        auth: &NumericAuthentication<'_>,
    ) -> LocalResult<WindowTransition> {
        let parent = self
            .numeric_parent(slot)
            .ok_or_else(|| binding("numeric window parent absent"))?;
        let Some(NativeHostMetadata::Arithmetic {
            operation,
            batch: Some(batch),
            name,
            source_types,
            binary_literal,
            result_type,
            output_node,
            output_role,
            ..
        }) = self.metadata.get(slot.index())
        else {
            return Err(binding("numeric window metadata absent"));
        };
        let result_type = result_type
            .as_ref()
            .ok_or_else(|| binding("numeric window result declaration missing"))?;
        let op = match operation {
            SqlBatchHostOperation::AddWindowNative => BinaryOp::Plus,
            SqlBatchHostOperation::SubtractWindowNative => BinaryOp::Minus,
            SqlBatchHostOperation::MultiplyWindowNative => BinaryOp::Mul,
            SqlBatchHostOperation::ModuloWindowNative => BinaryOp::Mod,
            SqlBatchHostOperation::DivideWindowNative => BinaryOp::Div,
            SqlBatchHostOperation::IntegerDivideWindowNative => BinaryOp::IntDiv,
            _ => return Err(binding("numeric window operation differs")),
        };
        if let Some(mut pending) = task.pending.take() {
            let Some(mut diag) = pending.diagnostic.take() else {
                return Err(binding("numeric window diagnostic absent"));
            };
            let plan = &parent.plans[diag.plan];
            let index = diag
                .state
                .pending_constant()
                .ok_or_else(|| binding("numeric window diagnostic pending input absent"))?;
            let step = match &plan.body {
                NumericPlanBody::Subject(plan) => plan.resume_text(&mut diag.state, index, text),
                NumericPlanBody::Argument(plan) => {
                    plan.resume_text(&mut diag.state, index, text, None)
                }
            };
            let step = match step {
                Ok(step) => step,
                Err(error) => return Ok(WindowTransition::Failure(error)),
            };
            match self.numeric_diagnostic_step(pending, diag, step, slot, auth)? {
                DiagnosticTransition::Need(pending) => {
                    task.pending = Some(pending);
                    return Ok(WindowTransition::Need(task));
                }
                DiagnosticTransition::Failure(error) => {
                    return Ok(WindowTransition::Failure(error))
                }
                DiagnosticTransition::IntegerDivision(_, _) => {
                    return Err(binding("window cannot resume raw row DIV promotion"))
                }
                DiagnosticTransition::Operand(_, value) => {
                    if task.computing {
                        return Err(binding("window overflow produced an operand"));
                    }
                    Self::numeric_window_cast_value(&mut task, value)?;
                }
            }
        }
        loop {
            let available = usize::MAX;
            if !task.computing {
                if task.position == task.rows.len() {
                    if task.index == 0 {
                        task.index = 1;
                        task.position = 0;
                        return Ok(WindowTransition::Need(task));
                    }
                    let count = task.rows.len();

                    task.output = Some(
                        ChunkedVecBytes::try_with_capacities(
                            count,
                            count * native_values::HANDLE_BYTES,
                        )
                        .map_err(|e| LocalError::ResourceLimit(e.to_string()))?,
                    );
                    task.computing = true;
                    task.position = 0;
                    continue;
                }
                let index = task.index;
                let source = crate::scalar_function::native_numeric_batch_source(
                    arithmetic_typed_source(source_types, index)?,
                    binary_literal[index],
                    batch.domain,
                );
                let value = std::mem::replace(&mut task.values[index][task.position], Datum::Null);

                let step = crate::scalar_function::prepare_native_numeric_batch_cast(
                    arithmetic_typed_source(source_types, index)?,
                    source,
                    batch.domain,
                    value,
                    self.context,
                );
                auth.check()?;
                match step {
                    Ok(NumericOperandStep::Ready(value)) => {
                        Self::numeric_window_cast_value(&mut task, value)?;
                    }
                    Ok(NumericOperandStep::Subject(pending)) => {
                        let NumericPlanBody::Subject(plan) = &parent.plans[index].body else {
                            unreachable!()
                        };
                        let mut diag = NumericDiagnostic {
                            plan: index,
                            state: plan.state(),
                            reason: DiagnosticReason::Subject(pending),
                        };
                        let step = match plan.step(&mut diag.state) {
                            Ok(step) => step,
                            Err(error) => return Ok(WindowTransition::Failure(error)),
                        };
                        let row_task = Box::new(NumericTask {
                            typed: false,
                            index,
                            next: 0,
                            left: None,
                            diagnostic: None,
                        });
                        match self.numeric_diagnostic_step(row_task, diag, step, slot, auth)? {
                            DiagnosticTransition::Need(pending) => {
                                task.pending = Some(pending);
                                return Ok(WindowTransition::Need(task));
                            }
                            DiagnosticTransition::Failure(error) => {
                                return Ok(WindowTransition::Failure(error))
                            }
                            DiagnosticTransition::IntegerDivision(_, _) => {
                                return Err(binding("window cannot resume raw row DIV promotion"))
                            }
                            DiagnosticTransition::Operand(_, value) => {
                                Self::numeric_window_cast_value(&mut task, value)?
                            }
                        }
                    }
                    Err(error) => return Ok(WindowTransition::Failure(error)),
                }
                continue;
            }
            if task.position == task.rows.len() {
                if batch.output_integer && !batch.integer && !task.presenting {
                    task.presenting = true;
                    task.position = 0;
                    continue;
                }
                return Ok(WindowTransition::Ready(VectorValue::Bytes(
                    task.output
                        .take()
                        .ok_or_else(|| binding("numeric window output absent"))?,
                )));
            }
            let position = task.position;
            if task.presenting {
                let value = std::mem::replace(&mut task.values[0][position], Datum::Null);

                let bits = match crate::scalar_function::native_batch_integer_bits(value) {
                    Ok(bits) => bits,
                    Err(error) => return Ok(WindowTransition::Failure(error)),
                };
                let value = crate::scalar_function::native_batch_integer_value(
                    bits,
                    result_type.is_unsigned(),
                );

                let store = self
                    .native_values
                    .ok_or_else(|| binding("numeric window store absent"))?;
                let token = store.borrow_mut().insert_owned(
                    value,
                    *output_role,
                    NativeValueOrigin {
                        node: *output_node,
                        row: InputRow {
                            occurrence: start + position,
                            input_row: task.rows[position],
                        },
                    },
                )?;
                task.output
                    .as_mut()
                    .ok_or_else(|| binding("numeric window output absent"))?
                    .push_ref(Some(&token));
                task.position += 1;
                continue;
            }
            let lhs = std::mem::replace(&mut task.values[0][position], Datum::Null);
            let rhs = std::mem::replace(&mut task.values[1][position], Datum::Null);
            let unsigned = arithmetic_unsigned(source_types);
            let computed = if matches!(op, BinaryOp::Mod | BinaryOp::Div | BinaryOp::IntDiv) {
                self.compute_quotient(&self.metadata[slot.index()], lhs, rhs, available)?
            } else {
                let prepared = if batch.integer {
                    let prepared = crate::ops::prepare_native_typed_arithmetic(
                        op,
                        lhs,
                        rhs,
                        unsigned,
                        self.context,
                    );
                    auth.check()?;
                    match prepared {
                        Ok(Some(prepared)) => prepared,
                        Ok(None) => {
                            return Err(binding("numeric integer window lost its strict values"))
                        }
                        Err(error) => return Ok(WindowTransition::Failure(error)),
                    }
                } else {
                    let (lhs, rhs) = crate::binary_literal::cast_signed_literal_operands(
                        op,
                        lhs,
                        rhs,
                        unsigned.map(|u| !u),
                    );
                    let _ = self.context.div_precision_increment();
                    auth.check()?;
                    let prepared =
                        crate::ops::arithmetic::prepare(op, lhs, rhs, unsigned, self.context);
                    auth.check()?;
                    match prepared {
                        Ok(prepared) => prepared,
                        Err(error) => return Ok(WindowTransition::Failure(error)),
                    }
                };
                prepared.compute(available)
            };
            auth.check()?;
            let value = match computed {
                Ok(value) => value,
                Err(error) => {
                    let kind = match error {
                        EvalError::IntOverflow => Overflow::Int,
                        EvalError::FloatOverflow => Overflow::Real,
                        EvalError::DecimalOverflow => Overflow::Decimal,
                        error => return Ok(WindowTransition::Failure(error)),
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
                    let step = match plan.step(&mut diag.state, None) {
                        Ok(step) => step,
                        Err(error) => return Ok(WindowTransition::Failure(error)),
                    };
                    // Nothing from an unsuccessful batch may reach publication.
                    task.values = [Vec::new(), Vec::new()];
                    task.output = None;
                    let row_task = Box::new(NumericTask {
                        typed: false,
                        index: 0,
                        next: 0,
                        left: None,
                        diagnostic: None,
                    });
                    return match self.numeric_diagnostic_step(row_task, diag, step, slot, auth)? {
                        DiagnosticTransition::Need(pending) => {
                            task.pending = Some(pending);
                            Ok(WindowTransition::Need(task))
                        }
                        DiagnosticTransition::Failure(error) => {
                            Ok(WindowTransition::Failure(error))
                        }
                        DiagnosticTransition::IntegerDivision(_, _)
                        | DiagnosticTransition::Operand(_, _) => {
                            Err(binding("numeric window overflow produced a value"))
                        }
                    };
                }
            };
            let value = if batch.integer {
                value
            } else {
                match crate::scalar_function::coerce_function_result(name, Some(result_type), value)
                {
                    Ok(value) => value,
                    Err(error) => return Ok(WindowTransition::Failure(error)),
                }
            };
            if batch.output_integer && !batch.integer {
                task.values[0][position] = value;
                task.position += 1;
                continue;
            }
            let value = if batch.output_integer {
                match crate::scalar_function::native_batch_integer_bits(value) {
                    Ok(bits) => crate::scalar_function::native_batch_integer_value(
                        bits,
                        result_type.is_unsigned(),
                    ),
                    Err(error) => return Ok(WindowTransition::Failure(error)),
                }
            } else {
                value
            };

            let store = self
                .native_values
                .ok_or_else(|| binding("numeric window store absent"))?;
            let token = store.borrow_mut().insert_owned(
                value,
                *output_role,
                NativeValueOrigin {
                    node: *output_node,
                    row: InputRow {
                        occurrence: start + position,
                        input_row: task.rows[position],
                    },
                },
            )?;
            task.output
                .as_mut()
                .ok_or_else(|| binding("numeric window output absent"))?
                .push_ref(Some(&token));
            task.position += 1;
        }
    }
    fn numeric_window_cast_value(task: &mut NumericWindowTask, value: Datum) -> LocalResult<()> {
        task.values[task.index][task.position] = value;
        task.position += 1;
        Ok(())
    }
}
