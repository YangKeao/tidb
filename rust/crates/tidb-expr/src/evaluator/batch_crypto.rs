// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Eager crypto children; consuming warning/body phases over actual values.
use super::*;
impl NativeBatchHosts<'_> {
    pub(super) fn resume_crypto(&mut self, id: &HostTaskId, token: &[u8]) -> LocalResult<HostStep> {
        let task = self.task(id)?;
        let SqlDemand::Crypto { next, mut tokens } = task.demand else {
            return Err(binding("crypto demand differs"));
        };
        let row = task.row;
        let Some(NativeHostMetadata::Crypto {
            arguments,
            operation,
            result_type,
            output_node,
            output_role,
            ..
        }) = self.metadata.get(task.slot.index())
        else {
            return Err(binding("crypto metadata absent"));
        };
        let arity = arguments.len();
        if next >= arity {
            return Err(binding("crypto task consumed"));
        }
        let store = self
            .native_values
            .ok_or_else(|| binding("crypto store absent"))?;
        let origin = |i: usize| NativeValueOrigin {
            node: arguments[i],
            row,
        };
        store
            .borrow()
            .get(token, NativeValueRole::CharInput, origin(next))?;
        tokens[next] = Some(
            token
                .try_into()
                .map_err(|_| binding("crypto capability width differs"))?,
        );
        let SqlDemand::Crypto {
            next: saved,
            tokens: saved_tokens,
        } = &mut self.slots[id.slot]
            .task
            .as_mut()
            .expect("validated crypto task")
            .demand
        else {
            unreachable!()
        };
        // Consume each stage before further demand, and the body BEFORE warnings/panic.
        *saved = next + 1;
        *saved_tokens = tokens;
        if next + 1 < arity {
            return Ok(HostStep::NeedArg(HostArgRequest {
                index: next + 1,
                mode: ArgMode::Fresh,
            }));
        }
        let token_for = |i: usize| {
            tokens[i]
                .as_ref()
                .map(|v| v.as_slice())
                .ok_or_else(|| binding("crypto capability lost"))
        };
        {
            let values = store.borrow();
            for i in 0..arity {
                values.get(token_for(i)?, NativeValueRole::CharInput, origin(i))?;
            }
        }
        // The genuine PASSWORD warning precedes ALL body coercion and NULL handling.
        // No store Ref survives this callback; reauthenticate the actual values below.
        crate::tikv::crypto_value::before_body(*operation, self.context);

        let maximum = usize::MAX;
        let result = {
            let values = store.borrow();
            let first = values.get(token_for(0)?, NativeValueRole::CharInput, origin(0))?;
            if arity == 1 {
                crate::tikv::crypto_value::body(*operation, &[first], maximum)
            } else {
                let second = values.get(token_for(1)?, NativeValueRole::CharInput, origin(1))?;
                crate::tikv::crypto_value::body(*operation, &[first, second], maximum)
            }
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
