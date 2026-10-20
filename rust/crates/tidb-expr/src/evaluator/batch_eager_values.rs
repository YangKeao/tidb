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
        let Some(NativeHostMetadata::EagerValues { arguments, .. }) =
            self.metadata.get(task.slot.index())
        else {
            return Err(binding("eager metadata absent"));
        };
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
        let (arity, row, slot) = (task.arity, task.row, task.slot);
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
        let result = match result {
            Ok(v) => v,
            Err(e) => return self.native_failure(e),
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
