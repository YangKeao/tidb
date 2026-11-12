// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Authenticated lazy operands; no store borrow spans escape/context callbacks.
use super::*;
use crate::tikv::wildcard_value::Step;

impl NativeBatchHosts<'_> {
    pub(super) fn resume_wildcard(
        &mut self,
        id: &HostTaskId,
        token: &[u8],
    ) -> LocalResult<HostStep> {
        let (slot, row, index) = {
            let task = self.task(id)?;
            let SqlDemand::Like {
                pending: Some(pending),
            } = &task.demand
            else {
                return Err(binding("LIKE invocation already consumed"));
            };
            (task.slot, task.row, pending.argument())
        };
        let Some(NativeHostMetadata::Like {
            arguments,
            escape_type,
            invocation,
            result_type,
            output_node,
            output_role,
        }) = self.metadata.get(slot.index())
        else {
            return Err(binding("LIKE metadata missing"));
        };
        let store = self
            .native_values
            .ok_or_else(|| binding("LIKE native store absent"))?;
        let origin = NativeValueOrigin {
            node: arguments[index],
            row,
        };
        store
            .borrow()
            .get(token, NativeValueRole::CharInput, origin)?;
        let SqlDemand::Like { pending } = &mut self.slots[id.slot]
            .task
            .as_mut()
            .expect("validated LIKE task")
            .demand
        else {
            unreachable!()
        };
        // Take before any conversion, error or callback can unwind. Only a
        // successful Need step installs a new state. NULL/error cannot replay.
        let pending = pending.take().expect("validated pending LIKE state");

        let maximum = usize::MAX;
        let step = if index < 2 {
            // SQL byte conversion has no Columns callbacks. Retain only its
            // bounded actual output before demanding the following child.
            let values = store.borrow();
            let value = values.get(token, NativeValueRole::CharInput, origin)?;
            pending.advance(value, None, self.context, maximum)
        } else {
            // Escape casting can call timezone/truncation/warning services.
            // String and hybrid names use immutable shared GoString headers;
            // cloning these actual values does not copy unused huge names.
            let owned = {
                let values = store.borrow();
                let value = values.get(token, NativeValueRole::CharInput, origin)?;
                if matches!(
                    value,
                    Datum::VectorFloat32(_) | Datum::MinNotNull | Datum::MaxValue
                ) {
                    // Original cast rejects these kinds before any callback.
                    // Do not clone a huge unsupported vector to return that error.
                    let result =
                        pending.advance(value, escape_type.as_ref(), self.context, maximum);
                    return match result {
                        Err(e) => self.native_failure(e),
                        Ok(_) => Err(binding("unsupported LIKE escape unexpectedly accepted")),
                    };
                }
                value.clone()
            };
            // The copied actual input remains owned while the callbacks run.
            pending.advance(&owned, escape_type.as_ref(), self.context, maximum)
        };
        let result = match step {
            Err(e) => return self.native_failure(e),
            Ok(Step::Need(next)) => {
                let index = next.argument();
                let SqlDemand::Like { pending } = &mut self.slots[id.slot]
                    .task
                    .as_mut()
                    .expect("live LIKE task")
                    .demand
                else {
                    unreachable!()
                };
                *pending = Some(next);
                return Ok(HostStep::NeedArg(HostArgRequest {
                    index,
                    mode: ArgMode::Fresh,
                }));
            }
            Ok(Step::Null) => Datum::Null,
            Ok(Step::Match(values)) => {
                let invocation = if invocation.cache_enabled() {
                    invocation
                        .clone()
                        .with_context_id(self.context.context_id())
                } else {
                    invocation.clone()
                };
                match values.evaluate(&invocation, maximum) {
                    Ok(v) => v,
                    Err(e) => return self.native_failure(e),
                }
            }
        };

        let result = if let Some(result_type) = result_type {
            match coerce_function_result("like", Some(result_type), result) {
                Ok(v) => v,
                Err(e) => return self.native_failure(e),
            }
        } else {
            result
        };
        self.finish_native_value(id, result, *output_node, *output_role, row)
            .map(HostStep::Ready)
    }
}
