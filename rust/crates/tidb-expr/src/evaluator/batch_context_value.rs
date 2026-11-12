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
            diagnostic,
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

        if let (Some(token), Some(origin)) = (token, origin) {
            store
                .borrow()
                .get(token, NativeValueRole::CharInput, origin)?;
        }

        let SqlDemand::ContextValue { consumed } = &mut self.slots[id.slot]
            .task
            .as_mut()
            .expect("validated context task")
            .demand
        else {
            unreachable!()
        };
        *consumed = true;
        let shortcut = diagnostic.is_none()
            && self.constant_root_shortcut
            && *output_node == 0
            && descriptor.root_broadcast;
        let value = if shortcut && self.root_value.is_some() {
            crate::tikv::identity_actual::copy(self.root_value.as_ref().unwrap())
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
                    crate::tikv::identity_actual::copy(&raw)
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
        if let Some(policy) = diagnostic {
            let result = (|| {
                let value = descriptor
                    .finish_raw(value, self.context)
                    .map_err(|(_, error)| error)?;
                policy.render(&value)
            })();
            if let (Some(token), Some(origin)) = (token, origin) {
                store
                    .borrow()
                    .get(token, NativeValueRole::CharInput, origin)?;
            }
            let text = match result {
                Ok(text) => text,
                Err(error) => return self.native_failure(error),
            };
            let node = *output_node;
            self.cancel(id);
            return self
                .emit_diagnostic_result(text, node, row)
                .map(HostStep::Ready);
        }
        let capture_error_value = self.capture_root_error_value;
        let mut error_value = None;
        let value = if shortcut {
            Ok(value)
        } else {
            descriptor
                .finish_expression(value, self.context)
                .map_err(|(value, error)| {
                    if capture_error_value {
                        error_value = Some(value);
                    }
                    error
                })
        };
        if let (Some(token), Some(origin)) = (token, origin) {
            store
                .borrow()
                .get(token, NativeValueRole::CharInput, origin)?;
        }
        let value = match value {
            Ok(value) => value,
            Err(error) => {
                if let Some(value) = error_value {
                    self.retain_root_error_value(id, value)?;
                }
                return self.native_failure(error);
            }
        };

        if shortcut && self.root_value.is_none() {
            self.root_value = Some(value.clone());
        }
        self.finish_native_value(id, value, *output_node, *output_role, row)
            .map(HostStep::Ready)
    }
}
