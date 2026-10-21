// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Eager actual operands and immutable original value-body policy in the SDK frame.
use super::*;
impl NativeBatchHosts<'_> {
    pub(super) fn resume_eager_values(
        &mut self,
        id: &HostTaskId,
        token: &[u8],
    ) -> LocalResult<HostStep> {
        let task = self.task(id)?;
        let SqlDemand::EagerValues { next, tokens } = &task.demand else {
            return Err(binding("eager task differs"));
        };
        let (next, arity, row) = (*next, task.arity, task.row);
        if next >= arity || tokens.len() != next {
            return Err(binding("eager stage consumed"));
        }
        let Some(NativeHostMetadata::EagerValues {
            arguments, kernel, ..
        }) = self.metadata.get(task.slot.index())
        else {
            return Err(binding("eager metadata absent"));
        };
        let demanded = kernel.demanded_arguments();
        if demanded > arity || next >= demanded {
            return Err(binding("eager demanded arity differs"));
        }
        let arity = demanded;
        let origin = NativeValueOrigin {
            node: arguments[next],
            row,
        };
        let store = self
            .native_values
            .ok_or_else(|| binding("eager store absent"))?;
        store
            .borrow()
            .get(token, NativeValueRole::CharInput, origin)?;
        let owned: [u8; native_values::HANDLE_BYTES] = token
            .try_into()
            .map_err(|_| binding("eager capability width"))?;
        self.check_bytes(
            self.retained_bytes()?
                .checked_add(native_values::HANDLE_BYTES)
                .ok_or_else(|| resource("eager token extent overflow"))?,
        )?;
        {
            let SqlDemand::EagerValues {
                next: stage,
                tokens,
            } = &mut self.slots[id.slot]
                .task
                .as_mut()
                .expect("validated eager task")
                .demand
            else {
                unreachable!()
            };
            *stage = next + 1;
            tokens
                .try_reserve_exact(1)
                .map_err(|e| LocalError::ResourceLimit(e.to_string()))?;
            tokens.push(owned);
        }
        self.check_bytes(self.retained_bytes()?)?;
        if next + 1 < arity {
            return Ok(HostStep::NeedArg(HostArgRequest {
                index: next + 1,
                mode: ArgMode::Fresh,
            }));
        }
        self.finish_eager_values(id)
    }
    pub(super) fn finish_eager_values(&mut self, id: &HostTaskId) -> LocalResult<HostStep> {
        let task = self.task(id)?;
        let SqlDemand::EagerValues { next, tokens } = &task.demand else {
            return Err(binding("eager task differs"));
        };
        let (graph_arity, row, slot) = (task.arity, task.row, task.slot);
        let Some(NativeHostMetadata::EagerValues { kernel, .. }) = self.metadata.get(slot.index())
        else {
            return Err(binding("eager demand descriptor absent"));
        };
        let arity = kernel.demanded_arguments();
        if arity > graph_arity {
            return Err(binding("eager demanded arity exceeds graph"));
        }
        if *next != arity || tokens.len() != arity {
            return Err(binding("eager body incomplete or consumed"));
        }
        let base = self.retained_bytes()?;
        let tokens = {
            let SqlDemand::EagerValues { next, tokens } = &mut self.slots[id.slot]
                .task
                .as_mut()
                .expect("validated eager task")
                .demand
            else {
                unreachable!()
            };
            *next = arity
                .checked_add(1)
                .ok_or_else(|| resource("eager demand overflow"))?;
            std::mem::take(tokens)
        };
        let Some(NativeHostMetadata::EagerValues {
            arguments,
            kernel,
            result_type,
            output_node,
            output_role,
            ..
        }) = self.metadata.get(slot.index())
        else {
            return Err(binding("eager metadata absent"));
        };
        let store = self
            .native_values
            .ok_or_else(|| binding("eager store absent"))?;
        let origin = |i| NativeValueOrigin {
            node: arguments[i],
            row,
        };
        let copied = {
            let values = store.borrow();
            tokens.iter().enumerate().try_fold(0usize, |sum, (i, t)| {
                let value = values.get(t, NativeValueRole::CharInput, origin(i))?;
                sum.checked_add(
                    native_payload_bytes(value)?
                        .checked_mul(2)
                        .ok_or_else(|| resource("eager input copy extent overflow"))?,
                )
                .ok_or_else(|| resource("eager input extent overflow"))
            })?
        };
        let mut live = base
            .checked_add(copied)
            .and_then(|n| n.checked_add(array_bytes::<Datum>(arity).ok()?))
            .ok_or_else(|| resource("eager argument layout overflow"))?;
        self.check_bytes(live)?;
        let mut ready = Vec::new();
        ready
            .try_reserve_exact(arity)
            .map_err(|e| LocalError::ResourceLimit(e.to_string()))?;
        live = base
            .checked_add(copied)
            .and_then(|n| n.checked_add(array_bytes::<Datum>(ready.capacity()).ok()?))
            .ok_or_else(|| resource("eager argument capacity overflow"))?;
        self.check_bytes(live)?;
        {
            let values = store.borrow();
            for (i, token) in tokens.iter().enumerate() {
                ready.push(values.clone_for_kernel(
                    token,
                    NativeValueRole::CharInput,
                    origin(i),
                )?);
            }
        }
        let maximum = self
            .max_retained_bytes
            .checked_sub(live)
            .ok_or_else(|| resource("eager work allowance exhausted"))?;
        let result = tidb_query_expr::local::with_value_kernel_work_limit(maximum, || {
            crate::scalar_function::eager_values::audit_work_result(kernel.eval_in(
                self.context,
                ready,
                |error| kernel.source_math_error(error),
            ))
        });
        // Every original callback ran without a storage Ref. It may have changed
        // the store, so none of the consumed capabilities can be trusted again.
        {
            let values = store.borrow();
            for (i, token) in tokens.iter().enumerate() {
                values.get(token, NativeValueRole::CharInput, origin(i))?;
            }
        }
        drop(tokens);
        let result = match result {
            Ok(v) => v,
            Err(e) => return self.begin_math_diagnostic(id, e),
        };
        let held = base
            .checked_add(
                native_payload_bytes(&result)?
                    .checked_mul(2)
                    .ok_or_else(|| resource("eager result extent overflow"))?,
            )
            .ok_or_else(|| resource("eager result storage overflow"))?;
        self.check_bytes(held)?;
        let result = match crate::tikv::coerce_result_bounded(
            kernel.name(),
            result_type,
            result,
            self.max_retained_bytes - held,
        ) {
            Ok(v) => v,
            Err(e) => return self.native_failure(e),
        };
        self.finish_native_value(id, result, *output_node, *output_role, row, base)
            .map(HostStep::Ready)
    }
}

impl NativeBatchHosts<'_> {
    /// A stored-name read has no argument demand. A stored-name assignment
    /// demands only argument 1. Neither path enters an expression evaluator.
    pub(super) fn finish_user_variable(
        &mut self,
        id: &HostTaskId,
        token: Option<&[u8]>,
    ) -> LocalResult<HostStep> {
        let task = self.task(id)?;
        let (row, slot) = (task.row, task.slot);
        let write = match (&task.demand, token) {
            (
                SqlDemand::UserVariableRead {
                    consumed: false, ..
                },
                None,
            ) => false,
            (
                SqlDemand::UserVariableWrite {
                    consumed: false, ..
                },
                Some(_),
            ) => true,
            _ => return Err(binding("user variable demand consumed or differs")),
        };
        let Some(NativeHostMetadata::EagerValues {
            arguments,
            kernel,
            result_type,
            output_node,
            output_role,
            ..
        }) = self.metadata.get(slot.index())
        else {
            return Err(binding("user variable metadata absent"));
        };
        let store = self
            .native_values
            .ok_or_else(|| binding("user variable store absent"))?;
        let origin = NativeValueOrigin {
            node: if write { arguments[1] } else { *output_node },
            row,
        };
        let base = self.retained_bytes()?;
        let copied = if let Some(token) = token {
            let values = store.borrow();
            native_payload_bytes(values.get(token, NativeValueRole::CharInput, origin)?)?
                .checked_mul(3)
                .and_then(|n| n.checked_add(std::mem::size_of::<Datum>()))
                .ok_or_else(|| resource("user variable argument extent overflow"))?
        } else {
            0
        };
        let live = base
            .checked_add(copied)
            .ok_or_else(|| resource("user variable live extent overflow"))?;
        self.check_bytes(live)?;
        // All external credentials are authenticated before consuming the task.
        // Mark it consumed before any context callback, conversion or unwind.
        let name = match &mut self.slots[id.slot]
            .task
            .as_mut()
            .expect("validated variable task")
            .demand
        {
            SqlDemand::UserVariableRead { name, consumed }
            | SqlDemand::UserVariableWrite { name, consumed } => {
                *consumed = true;
                std::mem::take(name)
            }
            _ => unreachable!(),
        };
        let maximum = self.max_retained_bytes - live;
        let action = kernel
            .user_variable()
            .ok_or_else(|| binding("user variable action absent"))?;
        let value = if let Some(token) = token {
            // Release the store borrow BEFORE the setter callback. A callback
            // can mutably borrow or invalidate the storage without a Ref panic.
            let value =
                store
                    .borrow()
                    .clone_for_kernel(token, NativeValueRole::CharInput, origin)?;
            tidb_query_expr::local::with_value_kernel_work_limit(maximum, || {
                crate::scalar_function::eager_values::audit_work_result(action.write(
                    self.context,
                    &name,
                    value,
                ))
            })
        } else {
            tidb_query_expr::local::with_value_kernel_work_limit(maximum, || {
                crate::scalar_function::eager_values::audit_work_result(
                    action.read(self.context, &name),
                )
            })
        };
        if let Some(token) = token {
            store
                .borrow()
                .get(token, NativeValueRole::CharInput, origin)?;
        }
        let value = match value {
            Ok(value) => value,
            Err(error) => return self.native_failure(error),
        };
        let held = native_payload_bytes(&value)?
            .checked_mul(2)
            .and_then(|n| n.checked_add(base))
            .ok_or_else(|| resource("user variable result extent overflow"))?;
        self.check_bytes(held)?;
        // The setter already stored the raw value. Return conversion can fail
        // or panic afterward; neither outcome rolls back that original effect.
        let value = match crate::tikv::identity_actual::coerce_result(
            kernel.name(),
            result_type,
            value,
            self.max_retained_bytes - held,
        ) {
            Ok(value) => value,
            Err(error) => return self.native_failure(error),
        };
        self.finish_native_value(id, value, *output_node, *output_role, row, base)
            .map(HostStep::Ready)
    }
}
