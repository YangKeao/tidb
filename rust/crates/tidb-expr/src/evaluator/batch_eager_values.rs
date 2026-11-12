// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Eager actual operands and immutable original value-body policy in the SDK frame.
use super::*;
impl NativeBatchHosts<'_> {
    pub(super) fn resume_eager_values(
        &mut self,
        id: &HostTaskId,
        token: &[u8],
    ) -> LocalResult<HostStep> {
        let task = self.task(id)?;
        let SqlDemand::EagerValues { next, tokens } = &task.demand else {
            return Err(binding("eager task differs"));
        };
        let (next, arity, row) = (*next, task.arity, task.row);
        if next >= arity || tokens.len() != next {
            return Err(binding("eager stage consumed"));
        }
        let Some(NativeHostMetadata::EagerValues {
            arguments, kernel, ..
        }) = self.metadata.get(task.slot.index())
        else {
            return Err(binding("eager metadata absent"));
        };
        let demanded = kernel.demanded_arguments();
        if demanded > arity || next >= demanded {
            return Err(binding("eager demanded arity differs"));
        }
        let arity = demanded;
        let origin = NativeValueOrigin {
            node: arguments[next],
            row,
        };
        let store = self
            .native_values
            .ok_or_else(|| binding("eager store absent"))?;
        store
            .borrow()
            .get(token, NativeValueRole::CharInput, origin)?;
        let owned: [u8; native_values::HANDLE_BYTES] = token
            .try_into()
            .map_err(|_| binding("eager capability width"))?;

        {
            let SqlDemand::EagerValues {
                next: stage,
                tokens,
            } = &mut self.slots[id.slot]
                .task
                .as_mut()
                .expect("validated eager task")
                .demand
            else {
                unreachable!()
            };
            *stage = next + 1;
            tokens
                .try_reserve_exact(1)
                .map_err(|e| LocalError::ResourceLimit(e.to_string()))?;
            tokens.push(owned);
        }

        if next + 1 < arity {
            return Ok(HostStep::NeedArg(HostArgRequest {
                index: next + 1,
                mode: ArgMode::Fresh,
            }));
        }
        self.finish_eager_values(id)
    }
    pub(super) fn finish_eager_values(&mut self, id: &HostTaskId) -> LocalResult<HostStep> {
        let task = self.task(id)?;
        let SqlDemand::EagerValues { next, tokens } = &task.demand else {
            return Err(binding("eager task differs"));
        };
        let (graph_arity, row, slot) = (task.arity, task.row, task.slot);
        let Some(NativeHostMetadata::EagerValues { kernel, .. }) = self.metadata.get(slot.index())
        else {
            return Err(binding("eager demand descriptor absent"));
        };
        let arity = kernel.demanded_arguments();
        if arity > graph_arity {
            return Err(binding("eager demanded arity exceeds graph"));
        }
        if *next != arity || tokens.len() != arity {
            return Err(binding("eager body incomplete or consumed"));
        }

        let tokens = {
            let SqlDemand::EagerValues { next, tokens } = &mut self.slots[id.slot]
                .task
                .as_mut()
                .expect("validated eager task")
                .demand
            else {
                unreachable!()
            };
            *next = arity
                .checked_add(1)
                .ok_or_else(|| resource("eager demand overflow"))?;
            std::mem::take(tokens)
        };
        let Some(NativeHostMetadata::EagerValues {
            arguments,
            kernel,
            result_type,
            output_node,
            output_role,
            ..
        }) = self.metadata.get(slot.index())
        else {
            return Err(binding("eager metadata absent"));
        };
        let store = self
            .native_values
            .ok_or_else(|| binding("eager store absent"))?;
        let origin = |i| NativeValueOrigin {
            node: arguments[i],
            row,
        };
        {
            let values = store.borrow();
            for (index, token) in tokens.iter().enumerate() {
                values.get(token, NativeValueRole::CharInput, origin(index))?;
            }
        }

        let mut ready = Vec::new();
        ready
            .try_reserve_exact(arity)
            .map_err(|e| LocalError::ResourceLimit(e.to_string()))?;

        {
            let values = store.borrow();
            for (i, token) in tokens.iter().enumerate() {
                ready.push(values.clone_for_kernel(
                    token,
                    NativeValueRole::CharInput,
                    origin(i),
                )?);
            }
        }

        let result = kernel.eval_in(self.context, ready, |error| kernel.source_math_error(error));
        // Every original callback ran without a storage Ref. It may have changed
        // the store, so none of the consumed capabilities can be trusted again.
        {
            let values = store.borrow();
            for (i, token) in tokens.iter().enumerate() {
                values.get(token, NativeValueRole::CharInput, origin(i))?;
            }
        }
        drop(tokens);
        let result = match result {
            Ok(v) => v,
            Err(e) => return self.begin_math_diagnostic(id, e),
        };

        let converted = coerce_function_result(kernel.name(), result_type.as_ref(), result);
        let result = match converted {
            Ok(v) => v,
            Err(e) => return self.native_failure(e),
        };
        self.finish_native_value(id, result, *output_node, *output_role, row)
            .map(HostStep::Ready)
    }
}

impl NativeBatchHosts<'_> {
    /// A stored-name read has no argument demand. A stored-name assignment
    /// demands only argument 1. Neither path enters an expression evaluator.
    pub(super) fn finish_user_variable(
        &mut self,
        id: &HostTaskId,
        token: Option<&[u8]>,
    ) -> LocalResult<HostStep> {
        let task = self.task(id)?;
        let (row, slot) = (task.row, task.slot);
        let write = match (&task.demand, token) {
            (
                SqlDemand::UserVariableRead {
                    consumed: false, ..
                },
                None,
            ) => false,
            (
                SqlDemand::UserVariableWrite {
                    consumed: false, ..
                },
                Some(_),
            ) => true,
            _ => return Err(binding("user variable demand consumed or differs")),
        };
        let Some(NativeHostMetadata::EagerValues {
            arguments,
            kernel,
            result_type,
            output_node,
            output_role,
            ..
        }) = self.metadata.get(slot.index())
        else {
            return Err(binding("user variable metadata absent"));
        };
        let store = self
            .native_values
            .ok_or_else(|| binding("user variable store absent"))?;
        let origin = NativeValueOrigin {
            node: if write { arguments[1] } else { *output_node },
            row,
        };

        if let Some(token) = token {
            store
                .borrow()
                .get(token, NativeValueRole::CharInput, origin)?;
        }

        // All external credentials are authenticated before consuming the task.
        // Mark it consumed before any context callback, conversion or unwind.
        let name = match &mut self.slots[id.slot]
            .task
            .as_mut()
            .expect("validated variable task")
            .demand
        {
            SqlDemand::UserVariableRead { name, consumed }
            | SqlDemand::UserVariableWrite { name, consumed } => {
                *consumed = true;
                std::mem::take(name)
            }
            _ => unreachable!(),
        };

        let action = kernel
            .user_variable()
            .ok_or_else(|| binding("user variable action absent"))?;
        let value = if let Some(token) = token {
            // Release the store borrow BEFORE the setter callback. A callback
            // can mutably borrow or invalidate the storage without a Ref panic.
            let value =
                store
                    .borrow()
                    .clone_for_kernel(token, NativeValueRole::CharInput, origin)?;
            action.write(self.context, &name, value)
        } else {
            action.read(self.context, &name)
        };
        if let Some(token) = token {
            store
                .borrow()
                .get(token, NativeValueRole::CharInput, origin)?;
        }
        let value = match value {
            Ok(value) => value,
            Err(error) => return self.native_failure(error),
        };

        // The setter already stored the raw value. Return conversion can fail
        // or panic afterward; neither outcome rolls back that original effect.
        let converted = coerce_function_result(kernel.name(), result_type.as_ref(), value);
        let value = match converted {
            Ok(value) => value,
            Err(error) => return self.native_failure(error),
        };
        self.finish_native_value(id, value, *output_node, *output_role, row)
            .map(HostStep::Ready)
    }
}
