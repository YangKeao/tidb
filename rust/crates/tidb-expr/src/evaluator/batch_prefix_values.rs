// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Fresh unary prefix demands in the existing EagerValuesNative SDK frame.
use super::*;
use crate::scalar_function::prefix_value::{PrefixPhase, PrefixStep};
impl NativeBatchHosts<'_> {
    pub(super) fn resume_prefix_value(
        &mut self,
        id: &HostTaskId,
        token: &[u8],
    ) -> LocalResult<HostStep> {
        let task = self.task(id)?;
        let SqlDemand::PrefixValues { phase } = task.demand else {
            return Err(binding("prefix task differs"));
        };
        let (slot, row) = (task.slot, task.row);
        let Some(NativeHostMetadata::EagerValues {
            arguments,
            kernel,
            result_type,
            output_node,
            output_role,
            ..
        }) = self.metadata.get(slot.index())
        else {
            return Err(binding("prefix descriptor absent"));
        };
        if arguments.len() != 1 || !kernel.accepts_prefix_phase(phase) {
            return Err(binding("prefix stage differs"));
        }
        let origin = NativeValueOrigin {
            node: arguments[0],
            row,
        };
        let store = self
            .native_values
            .ok_or_else(|| binding("prefix value store absent"))?;
        let base = self.retained_bytes()?;
        let payload = native_payload_bytes(store.borrow().get(
            token,
            NativeValueRole::CharInput,
            origin,
        )?)?;
        let live = base
            .checked_add(
                payload
                    .checked_mul(2)
                    .ok_or_else(|| resource("prefix clone allowance overflow"))?,
            )
            .ok_or_else(|| resource("prefix live extent overflow"))?;
        self.check_bytes(live)?;
        let value = store
            .borrow()
            .clone_for_kernel(token, NativeValueRole::CharInput, origin)?;
        // Consume this phase before any original warning or conversion callback.
        let SqlDemand::PrefixValues { phase: stage } = &mut self.slots[id.slot]
            .task
            .as_mut()
            .expect("validated prefix task")
            .demand
        else {
            unreachable!()
        };
        *stage = PrefixPhase::Consumed;
        let result = tidb_query_expr::local::with_value_kernel_work_limit(
            self.max_retained_bytes - live,
            || {
                crate::scalar_function::eager_values::audit_work_result(kernel.eval_prefix(
                    phase,
                    value,
                    self.context,
                ))
            },
        );
        // No Ref survives an original callback. Authenticate the actual reply
        // again before a fresh child request or output can cross this boundary.
        store
            .borrow()
            .get(token, NativeValueRole::CharInput, origin)?;
        let result = match result {
            Ok(result) => result,
            Err(error) => return self.native_failure(error),
        };
        match result {
            PrefixStep::Need(next) => {
                if !kernel.accepts_prefix_phase(next) {
                    return Err(binding("prefix next stage differs"));
                }
                let SqlDemand::PrefixValues { phase } = &mut self.slots[id.slot]
                    .task
                    .as_mut()
                    .expect("validated prefix task")
                    .demand
                else {
                    unreachable!()
                };
                *phase = next;
                Ok(HostStep::NeedArg(HostArgRequest {
                    index: 0,
                    mode: ArgMode::Fresh,
                }))
            }
            PrefixStep::Value(value) => {
                let held = base
                    .checked_add(
                        native_payload_bytes(&value)?
                            .checked_mul(2)
                            .ok_or_else(|| resource("prefix result allowance overflow"))?,
                    )
                    .ok_or_else(|| resource("prefix result extent overflow"))?;
                self.check_bytes(held)?;
                let value = match coerce_optional_value_result(
                    kernel.name(),
                    result_type.as_ref(),
                    value,
                    self.max_retained_bytes - held,
                ) {
                    Ok(value) => value,
                    Err(error) => return self.native_failure(error),
                };
                store
                    .borrow()
                    .get(token, NativeValueRole::CharInput, origin)?;
                self.finish_native_value(id, value, *output_node, *output_role, row, base)
                    .map(HostStep::Ready)
            }
        }
    }
}
