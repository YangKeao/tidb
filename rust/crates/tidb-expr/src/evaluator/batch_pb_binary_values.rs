// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Actual selected PB binary facts. Typed operands share numeric continuations;
//! the four baked MOD signatures own their original raw, two-demand prefix.
use super::*;

pub(in super::super) struct NativePbBinary {
    pub kernel: crate::scalar_function::PbBinaryValues,
    pub arguments: Box<[usize]>,
    pub logical_arity: usize,
    pub source_types: [Option<FieldType>; 2],
    pub binary_literal: [bool; 2],
    pub operands: Option<[crate::ops::OperandFacts; 2]>,
    pub name: Box<str>,
    pub collation: tidb_datatype::Collation,
    pub result_type: Option<FieldType>,
    pub integer_batch_result: bool,
    pub output_node: usize,
    pub output_role: NativeValueRole,
}
pub(super) struct PbModTask {
    pub tokens: Vec<[u8; native_values::HANDLE_BYTES]>,
    left: Option<Datum>,
}
impl PbModTask {
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
            .ok_or_else(|| resource("PB MOD retained extent overflow"))
    }
}
pub(super) fn present_pb_binary_value(
    meta: &NativePbBinary,
    value: Datum,
    context: &dyn Columns,
    maximum: usize,
) -> std::result::Result<Datum, EvalError> {
    let value = crate::scalar_function::interpret_integer_result(value, meta.result_type.as_ref());
    let value =
        coerce_optional_value_result(&meta.name, meta.result_type.as_ref(), value, maximum)?;
    if meta.integer_batch_result {
        present_integer_batch_value(value, meta.result_type.as_ref(), context, maximum)
    } else {
        Ok(value)
    }
}
impl NativeBatchHosts<'_> {
    pub(super) fn resume_pb_integer_mod(
        &mut self,
        id: &HostTaskId,
        token: Option<&[u8]>,
    ) -> LocalResult<HostStep> {
        let task = self.task(id)?;
        let SqlDemand::PbIntegerMod(state) = &task.demand else {
            return Err(binding("PB MOD phase differs"));
        };
        let (slot, row, next) = (task.slot, task.row, state.tokens.len());
        let Some(NativeHostMetadata::PbBinary(meta)) = self.metadata.get(slot.index()) else {
            return Err(binding("PB MOD metadata absent"));
        };
        if meta.kernel.domain().is_some()
            || meta.arguments.len() != meta.logical_arity.min(2)
            || next >= 2
        {
            return Err(binding("PB MOD declaration differs"));
        }
        let Some(token) = token else {
            if meta.logical_arity != 0 {
                return Err(binding("PB MOD reply absent"));
            }
            return self
                .native_failure(crate::scalar_function::PbBinaryValues::missing_argument_error());
        };
        if next >= meta.arguments.len() {
            return Err(binding("PB MOD demand exceeds actual arguments"));
        }
        let store = self
            .native_values
            .ok_or_else(|| binding("PB MOD store absent"))?;
        let origin = |index| NativeValueOrigin {
            node: meta.arguments[index],
            row,
        };
        let copied = {
            let values = store.borrow();
            for (index, held) in state.tokens.iter().enumerate() {
                values.get(held, NativeValueRole::CharInput, origin(index))?;
            }
            native_payload_bytes(values.get(token, NativeValueRole::CharInput, origin(next))?)?
                .checked_mul(2)
                .ok_or_else(|| resource("PB MOD copy extent overflow"))?
        };
        let base = self.retained_bytes()?;
        let live = base
            .checked_add(copied)
            .and_then(|n| n.checked_add(native_values::HANDLE_BYTES))
            .ok_or_else(|| resource("PB MOD live extent overflow"))?;
        self.check_bytes(live)?;
        let value =
            store
                .borrow()
                .clone_for_kernel(token, NativeValueRole::CharInput, origin(next))?;
        let SqlDemand::PbIntegerMod(mut state) = std::mem::replace(
            &mut self.slots[id.slot]
                .task
                .as_mut()
                .expect("authenticated PB MOD task")
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
                .map_err(|_| binding("PB MOD capability width differs"))?,
        );
        let live = base
            .checked_add(copied)
            .and_then(|n| {
                n.checked_add(
                    (state.tokens.capacity() - old_capacity) * native_values::HANDLE_BYTES,
                )
            })
            .ok_or_else(|| resource("PB MOD prefix capacity overflow"))?;
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
                    meta.kernel.integer_mod_operand(next, value),
                )
            },
        );
        check()?;
        let value = match result {
            Ok(value) => value,
            Err(error) => return self.native_failure(error),
        };
        if next == 0 {
            if meta.logical_arity < 2 {
                return self.native_failure(
                    crate::scalar_function::PbBinaryValues::missing_argument_error(),
                );
            }
            state.left = Some(value);
            self.slots[id.slot]
                .task
                .as_mut()
                .expect("live PB MOD task")
                .demand = SqlDemand::PbIntegerMod(state);
            self.check_bytes(self.retained_bytes()?)?;
            return Ok(HostStep::NeedArg(HostArgRequest {
                index: 1,
                mode: ArgMode::Fresh,
            }));
        }
        let left = state
            .left
            .take()
            .ok_or_else(|| binding("PB MOD left value absent"))?;
        let result = tidb_query_expr::local::with_value_kernel_work_limit(
            self.max_retained_bytes - live,
            || {
                crate::scalar_function::eager_values::audit_work_result(
                    meta.kernel.integer_mod_values(
                        left,
                        value,
                        self.context.div_precision_increment(),
                        meta.collation,
                        self.context,
                    ),
                )
            },
        );
        // Both actual consumed capabilities survive every late context callback.
        let check = || -> LocalResult<()> {
            let values = store.borrow();
            for (index, token) in state.tokens.iter().enumerate() {
                values.get(token, NativeValueRole::CharInput, origin(index))?;
            }
            Ok(())
        };
        check()?;
        let value = match result {
            Ok(value) => value,
            Err(error) => return self.native_failure(error),
        };
        let held = base
            .checked_add(
                native_payload_bytes(&value)?
                    .checked_mul(2)
                    .ok_or_else(|| resource("PB MOD result allowance overflow"))?,
            )
            .ok_or_else(|| resource("PB MOD result extent overflow"))?;
        self.check_bytes(held)?;
        let result =
            present_pb_binary_value(meta, value, self.context, self.max_retained_bytes - held);
        check()?;
        let value = match result {
            Ok(value) => value,
            Err(error) => return self.native_failure(error),
        };
        self.finish_native_value(id, value, meta.output_node, meta.output_role, row, base)
            .map(HostStep::Ready)
    }
}
