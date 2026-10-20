// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Context reads and deferred conversion in the existing SDK host frame.
use super::*;
impl NativeBatchHosts<'_> {
    pub(super) fn finish_context_value(
        &mut self,
        id: &HostTaskId,
        token: Option<&[u8]>,
    ) -> LocalResult<HostStep> {
        let task = self.task(id)?;
        let SqlDemand::ContextValue { consumed: false } = task.demand else {
            return Err(binding("context task consumed or differs"));
        };
        let (row, slot) = (task.row, task.slot);
        let Some(NativeHostMetadata::ContextValue {
            descriptor,
            argument,
            output_node,
            output_role,
        }) = self.metadata.get(slot.index())
        else {
            return Err(binding("context descriptor absent"));
        };
        if argument.is_some() != token.is_some() {
            return Err(binding("context operand demand differs"));
        }
        let store = self
            .native_values
            .ok_or_else(|| binding("context store absent"))?;
        let origin = argument.map(|node| NativeValueOrigin { node, row });
        let base = self.retained_bytes()?;
        let copied = if let (Some(token), Some(origin)) = (token, origin) {
            native_payload_bytes(
                store
                    .borrow()
                    .get(token, NativeValueRole::CharInput, origin)?,
            )?
            .checked_mul(2)
            .ok_or_else(|| resource("context operand extent overflow"))?
        } else {
            0
        };
        let live = base
            .checked_add(copied)
            .ok_or_else(|| resource("context live extent overflow"))?;
        self.check_bytes(live)?;
        let SqlDemand::ContextValue { consumed } = &mut self.slots[id.slot]
            .task
            .as_mut()
            .expect("validated context task")
            .demand
        else {
            unreachable!()
        };
        *consumed = true;
        let maximum = self.max_retained_bytes - live;
        let shortcut =
            self.constant_root_shortcut && *output_node == 0 && descriptor.root_broadcast;
        let value = if shortcut && self.root_value.is_some() {
            crate::tikv::identity_actual::copy_bounded(self.root_value.as_ref().unwrap(), maximum)
        } else if let (Some(token), Some(origin)) = (token, origin) {
            Ok(store
                .borrow()
                .clone_for_kernel(token, NativeValueRole::CharInput, origin)?)
        } else if let crate::context_value::ContextRead::Correlated(binding) = &descriptor.read {
            if let Some(binding) = binding {
                let raw = binding
                    .read()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if let Some(integer) =
                    crate::expression::enum_set_integer_view(&raw, descriptor.target.as_ref())
                {
                    Ok(Datum::UInt(integer))
                } else {
                    crate::tikv::identity_actual::copy_bounded(&raw, maximum)
                }
            } else {
                Ok(Datum::Null)
            }
        } else {
            descriptor.read_context(self.context)
        };
        let value = match value {
            Ok(value) => value,
            Err(error) => return self.native_failure(error),
        };
        let value = if shortcut {
            Ok(value)
        } else {
            tidb_query_expr::local::with_value_kernel_work_limit(maximum, || {
                crate::scalar_function::eager_values::audit_work_result(
                    descriptor
                        .finish_expression(value, self.context)
                        .map_err(|(_, error)| error),
                )
            })
        };
        if let (Some(token), Some(origin)) = (token, origin) {
            store
                .borrow()
                .get(token, NativeValueRole::CharInput, origin)?;
        }
        let value = match value {
            Ok(value) => value,
            Err(error) => return self.native_failure(error),
        };
        let held = base
            .checked_add(
                native_payload_bytes(&value)?
                    .checked_mul(3)
                    .ok_or_else(|| resource("context result extent overflow"))?,
            )
            .ok_or_else(|| resource("context result storage overflow"))?;
        self.check_bytes(held)?;
        if shortcut && self.root_value.is_none() {
            self.root_value = Some(value.clone());
            self.check_bytes(self.retained_bytes()?)?;
        }
        self.finish_native_value(id, value, *output_node, *output_role, row, base)
            .map(HostStep::Ready)
    }
}
