// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Raw CONCAT operands retain the original packet-limit demand state.
use super::*;
impl NativeBatchHosts<'_> {
    pub(super) fn resume_concat_value(
        &mut self,
        id: &HostTaskId,
        token: &[u8],
    ) -> LocalResult<HostStep> {
        let task = self.task(id)?;
        let index = task
            .next_argument()?
            .ok_or_else(|| binding("CONCAT value task consumed"))?;
        let slot = task.slot;
        let row = task.row;
        let Some(NativeHostMetadata::ConcatValues {
            arguments, kind, ..
        }) = self.metadata.get(slot.index())
        else {
            return Err(binding("CONCAT value metadata absent"));
        };
        let kind = *kind;
        let node = *arguments
            .get(index)
            .ok_or_else(|| binding("CONCAT argument absent"))?;
        let store = self
            .native_values
            .ok_or_else(|| binding("CONCAT value store absent"))?;
        let origin = NativeValueOrigin { node, row };
        store
            .borrow()
            .get(token, NativeValueRole::CharInput, origin)?;
        // Consume before conversion or callbacks. An unwind cannot replay a prefix.
        let SqlDemand::ConcatValues { pending } = &mut self.slots[id.slot]
            .task
            .as_mut()
            .expect("validated CONCAT task")
            .demand
        else {
            unreachable!()
        };
        let mut demand = pending
            .take()
            .ok_or_else(|| binding("CONCAT demand consumed"))?;

        let converted = {
            let values = store.borrow();
            let value = values.get(token, NativeValueRole::CharInput, origin)?;
            (|| crate::coerce::coerce_str_bytes(value))()
        };
        let converted = match converted {
            Ok(value) => value,
            Err(error) => return self.native_failure(error),
        };

        let length = converted.as_ref().map(Vec::len);
        let old = &self.slots[id.slot]
            .task
            .as_ref()
            .expect("validated CONCAT task")
            .values;
        let target = old
            .len()
            .checked_add(1)
            .ok_or_else(|| resource("CONCAT prefix count overflow"))?;

        let task = self.slots[id.slot]
            .task
            .as_mut()
            .expect("validated CONCAT task");
        if target > task.values.capacity() {
            task.values
                .try_reserve_exact(target - task.values.len())
                .map_err(|_| resource("CONCAT prefix reservation failed"))?;
        }
        task.values.push(converted);

        let action = demand.observe(length)?;
        // No NativeValueStore borrow spans either original callback.
        if action == ConcatDemandAction::ReadPacketLimit {
            let limit = self.context.max_allowed_packet();
            store
                .borrow()
                .get(token, NativeValueRole::CharInput, origin)?;
            if demand.accept_packet_limit(limit)? {
                let name = match kind {
                    ConcatKind::Concat => "concat",
                    ConcatKind::ConcatWs => "concat_ws",
                };
                let result = self.context.handle_allowed_packet_overflowed(name);
                store
                    .borrow()
                    .get(token, NativeValueRole::CharInput, origin)?;
                if let Err(error) = result {
                    return self.native_failure(error);
                }
            }
        }
        if let Some(index) = demand.next_argument()? {
            let SqlDemand::ConcatValues { pending } = &mut self.slots[id.slot]
                .task
                .as_mut()
                .expect("validated CONCAT task")
                .demand
            else {
                unreachable!()
            };
            *pending = Some(demand);
            return Ok(HostStep::NeedArg(HostArgRequest {
                index,
                mode: ArgMode::Fresh,
            }));
        }
        self.finish_concat_value(id, kind, demand)
    }
    fn finish_concat_value(
        &mut self,
        id: &HostTaskId,
        kind: ConcatKind,
        demand: ConcatDemand,
    ) -> LocalResult<HostStep> {
        let task = self.task(id)?;
        let slot = task.slot;
        let row = task.row;
        let arity = task.arity;
        let Some(NativeHostMetadata::ConcatValues {
            result_type,
            output_node,
            output_role,
            ..
        }) = self.metadata.get(slot.index())
        else {
            return Err(binding("CONCAT output metadata absent"));
        };
        // Original CONCAT enters its operation scope only after the demanded
        // prefix and packet callbacks. Keep poison/reentry outcomes at that point.
        if let Some(guard) = &self.slots[id.slot].ready_preparation {
            if let Err(error) = guard.cache().original_scope_status() {
                return self.native_failure(error);
            }
        }
        let terminal = demand.finish_terminal()?;

        let encode_cap = usize::MAX;
        let values = std::mem::take(
            &mut self.slots[id.slot]
                .task
                .as_mut()
                .expect("validated CONCAT task")
                .values,
        );
        // This is the existing pure SDK codec and kernel, without a Ready frame.
        let prepared =
            tidb_query_expr::local::prepare_concat_args(kind, arity, values, terminal, encode_cap)?;
        let output_cap = usize::MAX;
        let bytes = prepared.execute(output_cap)?;
        let value = bytes.map_or(Datum::Null, Datum::new_string);

        let name = match kind {
            ConcatKind::Concat => "concat",
            ConcatKind::ConcatWs => "concat_ws",
        };
        let value = match coerce_optional_value_result(name, result_type.as_ref(), value) {
            Ok(value) => value,
            Err(error) => return self.native_failure(error),
        };
        self.finish_native_value(id, value, *output_node, *output_role, row)
            .map(HostStep::Ready)
    }
}
