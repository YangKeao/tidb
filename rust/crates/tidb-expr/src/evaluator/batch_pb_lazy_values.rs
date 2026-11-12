// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Selected PB lazy values share the SDK frame and the original value boundaries.
use super::*;
use crate::scalar_function::PbLazyPhase;
pub(in super::super) struct NativePbLazy {
    pub kernel: crate::scalar_function::PbLazyValues,
    pub arguments: Box<[usize]>,
    pub logical_arity: usize,
    pub name: Box<str>,
    pub result_type: Option<FieldType>,
    pub integer_batch_result: bool,
    pub output_node: usize,
    pub output_role: NativeValueRole,
}
struct ConsumedArgument {
    index: usize,
    token: [u8; native_values::HANDLE_BYTES],
}
pub(super) struct PbLazyTask {
    pub(super) phase: PbLazyPhase,
    tokens: Vec<ConsumedArgument>,
}
impl PbLazyTask {
    pub(super) fn new(phase: PbLazyPhase) -> Self {
        Self {
            phase,
            tokens: Vec::new(),
        }
    }
    #[cfg(test)]
    pub(super) fn retained_bytes(&self) -> LocalResult<usize> {
        std::mem::size_of::<Self>()
            .checked_add(array_bytes::<ConsumedArgument>(self.tokens.capacity())?)
            .ok_or_else(|| resource("PB lazy retained extent overflow"))
    }
}
impl NativeBatchHosts<'_> {
    pub(super) fn resume_pb_lazy(
        &mut self,
        id: &HostTaskId,
        token: Option<&[u8]>,
    ) -> LocalResult<HostStep> {
        let task = self.task(id)?;
        let SqlDemand::PbLazy(state) = &task.demand else {
            return Err(binding("PB lazy phase differs"));
        };
        let (slot, row, phase) = (task.slot, task.row, state.phase);
        let Some(NativeHostMetadata::PbLazy(meta)) = self.metadata.get(slot.index()) else {
            return Err(binding("PB lazy metadata absent"));
        };
        if meta.arguments.len() != meta.kernel.data_arity(meta.logical_arity)
            || state.tokens.len() > meta.arguments.len()
        {
            return Err(binding("PB lazy declaration differs"));
        }
        let store = self
            .native_values
            .ok_or_else(|| binding("PB lazy store absent"))?;
        let origin = |index| NativeValueOrigin {
            node: meta.arguments[index],
            row,
        };
        let index = phase.argument();
        if index.is_some() != token.is_some() || index.is_some_and(|i| i >= meta.arguments.len()) {
            return Err(binding("PB lazy reply differs from demanded argument"));
        }
        {
            let values = store.borrow();
            for held in &state.tokens {
                values.get(&held.token, NativeValueRole::CharInput, origin(held.index))?;
            }
            if let (Some(index), Some(token)) = (index, token) {
                values.get(token, NativeValueRole::CharInput, origin(index))?;
            }
        }

        let value = match (index, token) {
            (Some(index), Some(token)) => Some(store.borrow().clone_for_kernel(
                token,
                NativeValueRole::CharInput,
                origin(index),
            )?),
            _ => None,
        };
        // Consumption precedes truth conversion, warnings and any value formatter.
        let SqlDemand::PbLazy(mut state) = std::mem::replace(
            &mut self.slots[id.slot]
                .task
                .as_mut()
                .expect("authenticated PB lazy task")
                .demand,
            SqlDemand::Consumed,
        ) else {
            unreachable!()
        };

        if let (Some(index), Some(token)) = (index, token) {
            state
                .tokens
                .try_reserve_exact(1)
                .map_err(|e| LocalError::ResourceLimit(e.to_string()))?;
            state.tokens.push(ConsumedArgument {
                index,
                token: token
                    .try_into()
                    .map_err(|_| binding("PB lazy capability width differs"))?,
            });
        }

        let check = || -> LocalResult<()> {
            let values = store.borrow();
            for held in &state.tokens {
                values.get(&held.token, NativeValueRole::CharInput, origin(held.index))?;
            }
            Ok(())
        };
        let value = match phase {
            PbLazyPhase::Condition(index) => {
                let result = (|| {
                    meta.kernel
                        .truth(value.as_ref().expect("actual condition"), self.context)
                })();
                drop(value);
                check()?;
                let truth = match result {
                    Ok(v) => v,
                    Err(e) => return self.native_failure(e),
                };
                state.phase = meta.kernel.after_truth(meta.logical_arity, index, truth);
                match state.phase {
                    PbLazyPhase::Missing => {
                        return self.native_failure(
                            crate::scalar_function::PbBinaryValues::missing_argument_error(),
                        )
                    }
                    PbLazyPhase::Exhausted => None,
                    _ => {
                        let next = state
                            .phase
                            .argument()
                            .ok_or_else(|| binding("PB lazy next argument absent"))?;
                        self.slots[id.slot]
                            .task
                            .as_mut()
                            .expect("live PB lazy task")
                            .demand = SqlDemand::PbLazy(state);

                        return Ok(HostStep::NeedArg(HostArgRequest {
                            index: next,
                            mode: ArgMode::Fresh,
                        }));
                    }
                }
            }
            PbLazyPhase::NullableFirst => {
                let value = value.expect("actual IFNULL first value");
                if meta.kernel.needs_second(value.is_null()) {
                    drop(value);
                    if meta.logical_arity < 2 {
                        return self.native_failure(
                            crate::scalar_function::PbBinaryValues::missing_argument_error(),
                        );
                    }
                    state.phase = PbLazyPhase::Value(1);
                    self.slots[id.slot]
                        .task
                        .as_mut()
                        .expect("live PB lazy task")
                        .demand = SqlDemand::PbLazy(state);

                    return Ok(HostStep::NeedArg(HostArgRequest {
                        index: 1,
                        mode: ArgMode::Fresh,
                    }));
                }
                Some(value)
            }
            PbLazyPhase::Value(_) => value,
            PbLazyPhase::Exhausted => None,
            PbLazyPhase::Missing => {
                return self.native_failure(
                    crate::scalar_function::PbBinaryValues::missing_argument_error(),
                )
            }
        };
        let value = value.unwrap_or(Datum::Null);
        check()?;

        let value =
            crate::scalar_function::interpret_integer_result(value, meta.result_type.as_ref());
        let result = coerce_function_result(&meta.name, meta.result_type.as_ref(), value);
        check()?;
        let value = match result {
            Ok(v) => v,
            Err(e) => return self.native_failure(e),
        };
        let result = if meta.integer_batch_result {
            present_integer_batch_value(value, meta.result_type.as_ref(), self.context)
        } else {
            Ok(value)
        };
        check()?;
        let value = match result {
            Ok(v) => v,
            Err(e) => return self.native_failure(e),
        };
        self.finish_native_value(id, value, meta.output_node, meta.output_role, row)
            .map(HostStep::Ready)
    }
}
