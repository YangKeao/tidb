// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Eager authenticated vector sources over borrowed actual values.
use super::*;
impl NativeBatchHosts<'_> {
    pub(super) fn resume_vector(&mut self, id: &HostTaskId, token: &[u8]) -> LocalResult<HostStep> {
        let (slot, row, next, mut tokens) = {
            let task = self.task(id)?;
            let SqlDemand::Vector { next, tokens } = &task.demand else {
                return Err(binding("vector task differs"));
            };
            (task.slot, task.row, *next, *tokens)
        };
        let Some(NativeHostMetadata::Vector {
            arguments,
            operation,
            result_type,
            output_node,
            output_role,
        }) = self.metadata.get(slot.index())
        else {
            return Err(binding("vector metadata missing"));
        };
        let arity = operation.arity();
        if next >= arity {
            return Err(binding("vector task already consumed all arguments"));
        }
        let store = self
            .native_values
            .ok_or_else(|| binding("vector store missing"))?;
        store.borrow().get(
            token,
            NativeValueRole::CharInput,
            NativeValueOrigin {
                node: arguments[next],
                row,
            },
        )?;
        tokens[next] = Some(
            token
                .try_into()
                .map_err(|_| binding("vector capability width differs"))?,
        );
        // Close demand before any fallible conversion or panic. A caught panic
        // cannot replay a partially converted invocation into a NULL success.
        let SqlDemand::Vector {
            next: saved_next,
            tokens: saved_tokens,
        } = &mut self.slots[id.slot]
            .task
            .as_mut()
            .expect("validated vector task")
            .demand
        else {
            unreachable!()
        };
        *saved_next = next + 1;
        *saved_tokens = tokens;
        if next + 1 < arity {
            return Ok(HostStep::NeedArg(HostArgRequest {
                index: next + 1,
                mode: ArgMode::Fresh,
            }));
        }
        let token_for = |index: usize| {
            tokens[index]
                .as_ref()
                .map(|v| v.as_slice())
                .ok_or_else(|| binding("vector capability lost"))
        };
        {
            let source = store.borrow();
            for index in 0..arity {
                source.get(
                    token_for(index)?,
                    NativeValueRole::CharInput,
                    NativeValueOrigin {
                        node: arguments[index],
                        row,
                    },
                )?;
            }
        }

        let maximum = usize::MAX;
        let result = {
            let source = store.borrow();
            let mut values = Vec::new();
            reserve(&mut values, arity)?;
            for i in 0..arity {
                values.push(source.get(
                    token_for(i)?,
                    NativeValueRole::CharInput,
                    NativeValueOrigin {
                        node: arguments[i],
                        row,
                    },
                )?);
            }
            crate::tikv::vector_value::eval_source(*operation, &values, maximum)
        };
        let result = match result {
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
