// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Consuming single-argument byte-unary native demand.
use super::*;
impl NativeBatchHosts<'_> {
    pub(super) fn resume_compression(
        &mut self,
        id: &HostTaskId,
        token: &[u8],
    ) -> LocalResult<HostStep> {
        let task = self.task(id)?;
        let SqlDemand::Compression { consumed: false } = task.demand else {
            return Err(binding("compression task consumed"));
        };
        let row = task.row;
        let Some(NativeHostMetadata::Compression {
            argument,
            operation,
            result_type,
            output_node,
            output_role,
            ..
        }) = self.metadata.get(task.slot.index())
        else {
            return Err(binding("compression metadata absent"));
        };
        let store = self
            .native_values
            .ok_or_else(|| binding("compression store absent"))?;
        let origin = NativeValueOrigin {
            node: *argument,
            row,
        };
        store
            .borrow()
            .get(token, NativeValueRole::CharInput, origin)?;
        let SqlDemand::Compression { consumed } = &mut self.slots[id.slot]
            .task
            .as_mut()
            .expect("validated compression task")
            .demand
        else {
            unreachable!()
        };
        // Consume before all fallible body work, result conversion and publication.
        *consumed = true;

        let maximum = usize::MAX;
        let report = {
            let values = store.borrow();
            crate::tikv::compression_value::body(
                *operation,
                values.get(token, NativeValueRole::CharInput, origin)?,
                maximum,
            )
        };
        let report = match report {
            Ok(v) => v,
            Err(e) => return self.native_failure(e),
        };
        // The actual completed warning runs without a Ref and cannot replay
        // this consumed body. It may mutate the store, so authenticate again.
        report.warn(self.context);
        store
            .borrow()
            .get(token, NativeValueRole::CharInput, origin)?;
        let result = match report.value {
            Ok(v) => v,
            Err(e) => return self.native_failure(e),
        };

        let result = match crate::tikv::coerce_result_bounded(operation.name(), result_type, result)
        {
            Ok(v) => v,
            Err(e) => return self.native_failure(e),
        };
        self.finish_native_value(id, result, *output_node, *output_role, row)
            .map(HostStep::Ready)
    }
}
