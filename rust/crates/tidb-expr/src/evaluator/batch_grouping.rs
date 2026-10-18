// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Strict actual grouping id precedes demand for immutable planner metadata.
use super::*;
impl NativeBatchHosts<'_> {
    pub(super) fn resume_grouping(
        &mut self,
        id: &HostTaskId,
        token: &[u8],
    ) -> LocalResult<HostStep> {
        let task = self.task(id)?;
        let SqlDemand::Grouping { consumed } = task.demand else {
            return Err(binding("GROUPING demand differs"));
        };
        if consumed {
            return Err(binding("GROUPING task already consumed"));
        }
        let Some(NativeHostMetadata::Grouping {
            argument,
            metadata,
            result_type,
            output_node,
            output_role,
            ..
        }) = self.metadata.get(task.slot.index())
        else {
            return Err(binding("GROUPING metadata absent"));
        };
        let row = task.row;
        let store = self
            .native_values
            .ok_or_else(|| binding("GROUPING native store absent"))?;
        store.borrow().get(
            token,
            NativeValueRole::CharInput,
            NativeValueOrigin {
                node: *argument,
                row,
            },
        )?;
        let SqlDemand::Grouping { consumed } = &mut self.slots[id.slot]
            .task
            .as_mut()
            .expect("validated GROUPING task")
            .demand
        else {
            unreachable!()
        };
        *consumed = true;
        let base = self.retained_bytes()?;
        let maximum = self
            .max_retained_bytes
            .checked_sub(base)
            .ok_or_else(|| resource("GROUPING work allowance exhausted"))?;
        let result = {
            let values = store.borrow();
            let value = values.get(
                token,
                NativeValueRole::CharInput,
                NativeValueOrigin {
                    node: *argument,
                    row,
                },
            )?;
            crate::tikv::grouping_value::eval(value, metadata.as_deref(), maximum)
        };
        let result = match result {
            Ok(v) => v,
            Err(e) => return self.native_failure(e),
        };
        // The signature returns UInt bits, then ScalarFunction::eval applies
        // its ordinary return-family boundary once, outside that signature.
        let held = native_payload_bytes(&result)?
            .checked_mul(2)
            .and_then(|n| base.checked_add(n))
            .ok_or_else(|| resource("GROUPING result extent overflow"))?;
        self.check_bytes(held)?;
        let result = match crate::tikv::coerce_result_bounded(
            "grouping",
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
