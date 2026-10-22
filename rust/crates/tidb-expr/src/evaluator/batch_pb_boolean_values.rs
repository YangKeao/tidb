// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Actual PB Boolean owners: raw NULL observation or original truth conversion,
//! with authenticated short circuit demands in the common SDK driver.
use super::*;
pub(in super::super) struct NativePbBoolean {
    pub kernel: crate::scalar_function::PbBooleanValues,
    pub arguments: Box<[usize]>,
    pub logical_arity: usize,
    pub name: Box<str>,
    pub result_type: Option<FieldType>,
    pub integer_batch_result: bool,
    pub output_node: usize,
    pub output_role: NativeValueRole,
}
pub(super) struct PbBooleanTask {
    pub tokens: Vec<[u8; native_values::HANDLE_BYTES]>,
    left: Option<Option<bool>>,
}
impl PbBooleanTask {
    pub(super) fn new() -> Self {
        Self {
            tokens: Vec::new(),
            left: None,
        }
    }
    pub(super) fn retained_bytes(&self) -> LocalResult<usize> {
        std::mem::size_of::<Self>()
            .checked_add(array_bytes::<[u8; native_values::HANDLE_BYTES]>(
                self.tokens.capacity(),
            )?)
            .ok_or_else(|| resource("PB Boolean retained extent overflow"))
    }
}
impl NativeBatchHosts<'_> {
    pub(super) fn resume_pb_boolean(
        &mut self,
        id: &HostTaskId,
        token: Option<&[u8]>,
    ) -> LocalResult<HostStep> {
        let task = self.task(id)?;
        let SqlDemand::PbBoolean(state) = &task.demand else {
            return Err(binding("PB Boolean phase differs"));
        };
        let (slot, row, next) = (task.slot, task.row, state.tokens.len());
        let Some(NativeHostMetadata::PbBoolean(meta)) = self.metadata.get(slot.index()) else {
            return Err(binding("PB Boolean metadata absent"));
        };
        if meta.arguments.len() != meta.kernel.data_arity(meta.logical_arity) || next >= 2 {
            return Err(binding("PB Boolean declaration differs"));
        }
        let Some(token) = token else {
            if meta.logical_arity != 0 {
                return Err(binding("PB Boolean reply absent"));
            }
            return self
                .native_failure(crate::scalar_function::PbBinaryValues::missing_argument_error());
        };
        if next >= meta.arguments.len() {
            return Err(binding("PB Boolean demand exceeds actual arguments"));
        }
        let store = self
            .native_values
            .ok_or_else(|| binding("PB Boolean store absent"))?;
        let origin = |index| NativeValueOrigin {
            node: meta.arguments[index],
            row,
        };
        let (is_null, copied) = {
            let values = store.borrow();
            for (index, held) in state.tokens.iter().enumerate() {
                values.get(held, NativeValueRole::CharInput, origin(index))?;
            }
            let value = values.get(token, NativeValueRole::CharInput, origin(next))?;
            (
                value.is_null(),
                if meta.kernel.observes_null_only() {
                    0
                } else {
                    native_payload_bytes(value)?
                        .checked_mul(2)
                        .ok_or_else(|| resource("PB Boolean copy extent overflow"))?
                },
            )
        };
        let base = self.retained_bytes()?;
        let live = base
            .checked_add(copied)
            .and_then(|n| n.checked_add(native_values::HANDLE_BYTES))
            .ok_or_else(|| resource("PB Boolean live extent overflow"))?;
        self.check_bytes(live)?;
        // Presence checks never clone or format an unused payload.
        let value = if meta.kernel.observes_null_only() {
            None
        } else {
            Some(store.borrow().clone_for_kernel(
                token,
                NativeValueRole::CharInput,
                origin(next),
            )?)
        };
        let SqlDemand::PbBoolean(mut state) = std::mem::replace(
            &mut self.slots[id.slot]
                .task
                .as_mut()
                .expect("authenticated PB Boolean task")
                .demand,
            SqlDemand::Consumed,
        ) else {
            unreachable!()
        };
        let old_capacity = state.tokens.capacity();
        state
            .tokens
            .try_reserve_exact(1)
            .map_err(|error| LocalError::ResourceLimit(error.to_string()))?;
        state.tokens.push(
            token
                .try_into()
                .map_err(|_| binding("PB Boolean capability width differs"))?,
        );
        let live = base
            .checked_add(copied)
            .and_then(|n| {
                n.checked_add(
                    (state.tokens.capacity() - old_capacity) * native_values::HANDLE_BYTES,
                )
            })
            .ok_or_else(|| resource("PB Boolean prefix capacity overflow"))?;
        self.check_bytes(live)?;
        let check = || -> LocalResult<()> {
            let values = store.borrow();
            for (index, token) in state.tokens.iter().enumerate() {
                values.get(token, NativeValueRole::CharInput, origin(index))?;
            }
            Ok(())
        };
        let result = tidb_query_expr::local::with_value_kernel_work_limit(
            self.max_retained_bytes - live,
            || {
                crate::scalar_function::eager_values::audit_work_result(
                    if meta.kernel.observes_null_only() {
                        Ok(meta.kernel.nullness(is_null))
                    } else {
                        meta.kernel
                            .truth(value.as_ref().expect("actual truth input"), self.context)
                    },
                )
            },
        );
        drop(value);
        check()?;
        let truth = match result {
            Ok(value) => value,
            Err(error) => return self.native_failure(error),
        };
        if next == 0 && meta.kernel.needs_right(truth) {
            if meta.logical_arity < 2 {
                return self.native_failure(
                    crate::scalar_function::PbBinaryValues::missing_argument_error(),
                );
            }
            state.left = Some(truth);
            self.slots[id.slot]
                .task
                .as_mut()
                .expect("live PB Boolean task")
                .demand = SqlDemand::PbBoolean(state);
            self.check_bytes(self.retained_bytes()?)?;
            return Ok(HostStep::NeedArg(HostArgRequest {
                index: 1,
                mode: ArgMode::Fresh,
            }));
        }
        let result = tidb_query_expr::local::with_value_kernel_work_limit(
            self.max_retained_bytes - live,
            || {
                crate::scalar_function::eager_values::audit_work_result(
                    if meta.kernel.logical_function().is_some() {
                        let arguments = if next == 0 {
                            crate::LogicalArgs::UndemandedRight { left: truth }
                        } else {
                            crate::LogicalArgs::Both(
                                state.left.expect("actual consumed logical left"),
                                truth,
                            )
                        };
                        meta.kernel.logical_value(arguments, self.context)
                    } else {
                        meta.kernel.unary_value(truth, self.context)
                    },
                )
            },
        );
        check()?;
        let value = match result {
            Ok(value) => value,
            Err(error) => return self.native_failure(error),
        };
        let held = base
            .checked_add(
                native_payload_bytes(&value)?
                    .checked_mul(2)
                    .ok_or_else(|| resource("PB Boolean result allowance overflow"))?,
            )
            .ok_or_else(|| resource("PB Boolean result extent overflow"))?;
        self.check_bytes(held)?;
        let value =
            crate::scalar_function::interpret_integer_result(value, meta.result_type.as_ref());
        let result = coerce_optional_value_result(
            &meta.name,
            meta.result_type.as_ref(),
            value,
            self.max_retained_bytes - held,
        );
        check()?;
        let value = match result {
            Ok(value) => value,
            Err(error) => return self.native_failure(error),
        };
        let result = if meta.integer_batch_result {
            present_integer_batch_value(
                value,
                meta.result_type.as_ref(),
                self.context,
                self.max_retained_bytes - held,
            )
        } else {
            Ok(value)
        };
        check()?;
        let value = match result {
            Ok(value) => value,
            Err(error) => return self.native_failure(error),
        };
        self.finish_native_value(id, value, meta.output_node, meta.output_role, row, base)
            .map(HostStep::Ready)
    }
}
