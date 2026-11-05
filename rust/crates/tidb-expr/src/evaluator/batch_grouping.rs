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

        let maximum = usize::MAX;
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

        let result = if let Some(result_type) = result_type {
            match crate::tikv::coerce_result_bounded("grouping", result_type, result) {
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
