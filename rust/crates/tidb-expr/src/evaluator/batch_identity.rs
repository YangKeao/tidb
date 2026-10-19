// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Eager actual identity selection. NAME_CONST never coerces its ignored name.
use super::*;
impl NativeBatchHosts<'_> {
    pub(super) fn resume_identity(
        &mut self,
        id: &HostTaskId,
        token: &[u8],
    ) -> LocalResult<HostStep> {
        let task = self.task(id)?;
        let SqlDemand::Identity { next } = task.demand else {
            return Err(binding("identity demand differs"));
        };
        let row = task.row;
        let Some(NativeHostMetadata::Identity {
            arguments,
            name_const,
            result_type,
            output_node,
            output_role,
            ..
        }) = self.metadata.get(task.slot.index())
        else {
            return Err(binding("identity metadata absent"));
        };
        if next >= arguments.len() {
            return Err(binding("identity task consumed"));
        }
        let store = self
            .native_values
            .ok_or_else(|| binding("identity store absent"))?;
        let origin = NativeValueOrigin {
            node: arguments[next],
            row,
        };
        store
            .borrow()
            .get(token, NativeValueRole::CharInput, origin)?;
        let SqlDemand::Identity { next: saved } = &mut self.slots[id.slot]
            .task
            .as_mut()
            .expect("validated identity task")
            .demand
        else {
            unreachable!()
        };
        // Consume BEFORE copy, result conversion, publication or panic.
        *saved = next + 1;
        if next + 1 < arguments.len() {
            return Ok(HostStep::NeedArg(HostArgRequest {
                index: next + 1,
                mode: ArgMode::Fresh,
            }));
        }
        let base = self.retained_bytes()?;
        let maximum = self
            .max_retained_bytes
            .checked_sub(base)
            .ok_or_else(|| resource("identity work allowance exhausted"))?;
        let result = {
            let values = store.borrow();
            crate::tikv::identity_actual::copy_bounded(
                values.get(token, NativeValueRole::CharInput, origin)?,
                maximum,
            )
        };
        let result = match result {
            Ok(v) => v,
            Err(e) => return self.native_failure(e),
        };
        let held = native_payload_bytes(&result)?
            .checked_mul(2)
            .and_then(|n| base.checked_add(n))
            .ok_or_else(|| resource("identity output extent overflow"))?;
        self.check_bytes(held)?;
        let result = match crate::tikv::identity_actual::coerce_result(
            if *name_const {
                "name_const"
            } else {
                "any_value"
            },
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
