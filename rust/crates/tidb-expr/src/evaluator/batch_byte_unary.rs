// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Consuming single-argument byte-unary native demand.
use super::*;
impl NativeBatchHosts<'_> {
    pub(super) fn resume_byte_unary(
        &mut self,
        id: &HostTaskId,
        token: &[u8],
    ) -> LocalResult<HostStep> {
        let task = self.task(id)?;
        let SqlDemand::ByteUnary { consumed: false } = task.demand else {
            return Err(binding("byte unary task consumed"));
        };
        let row = task.row;
        let Some(NativeHostMetadata::ByteUnary {
            argument,
            operation,
            result_type,
            output_node,
            output_role,
            ..
        }) = self.metadata.get(task.slot.index())
        else {
            return Err(binding("byte unary metadata absent"));
        };
        let store = self
            .native_values
            .ok_or_else(|| binding("byte unary store absent"))?;
        let origin = NativeValueOrigin {
            node: *argument,
            row,
        };
        store
            .borrow()
            .get(token, NativeValueRole::CharInput, origin)?;
        let SqlDemand::ByteUnary { consumed } = &mut self.slots[id.slot]
            .task
            .as_mut()
            .expect("validated byte unary task")
            .demand
        else {
            unreachable!()
        };
        // Consume before all fallible body work, result conversion and publication.
        *consumed = true;
        let base = self.retained_bytes()?;
        let maximum = self
            .max_retained_bytes
            .checked_sub(base)
            .ok_or_else(|| resource("byte unary work allowance exhausted"))?;
        let result = {
            let values = store.borrow();
            crate::tikv::byte_unary_value::scalar_body(
                *operation,
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
            .ok_or_else(|| resource("byte unary result extent overflow"))?;
        self.check_bytes(held)?;
        let result = match crate::tikv::coerce_result_bounded(
            operation.name(),
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
