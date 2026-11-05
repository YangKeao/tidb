// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Authenticated clock arguments: no payload copy and no borrow over callbacks.
use super::*;
impl NativeBatchHosts<'_> {
    pub(super) fn resume_clock(
        &mut self,
        id: &HostTaskId,
        token: Option<&[u8]>,
    ) -> LocalResult<HostStep> {
        let task = self.task(id)?;
        if !matches!(task.demand, SqlDemand::Clock) {
            return Err(binding("clock task differs"));
        }
        let Some(NativeHostMetadata::Clock {
            argument,
            operation,
            result_type,
            output_node,
            output_role,
        }) = self.metadata.get(task.slot.index())
        else {
            return Err(binding("clock metadata missing"));
        };
        if argument.is_some() != token.is_some() {
            return Err(binding("clock source presence differs"));
        }
        let row = task.row;

        let maximum = usize::MAX;
        let store = self
            .native_values
            .ok_or_else(|| binding("clock store missing"))?;
        let origin = argument.map(|node| NativeValueOrigin { node, row });
        if let Some(origin) = origin {
            store.borrow().get(
                token.ok_or_else(|| binding("clock source lost"))?,
                NativeValueRole::CharInput,
                origin,
            )?;
        }
        // SYSDATE's flag precedes precision validation. No borrow spans it.
        let resolved = crate::tikv::clock_value::resolve(*operation, self.context);
        let prepared = {
            let values = store.borrow();
            let input = match origin {
                Some(origin) => std::slice::from_ref(values.get(
                    token.ok_or_else(|| binding("clock source lost"))?,
                    NativeValueRole::CharInput,
                    origin,
                )?),
                None => &[],
            };
            crate::tikv::clock_value::prepare(resolved, input, maximum)
        };
        let prepared = match prepared {
            Ok(v) => v,
            Err(e) => return self.native_failure(e),
        };
        let result = match crate::tikv::clock_value::finish(prepared, self.context, maximum) {
            Ok(v) => v,
            Err(e) => return self.native_failure(e),
        };

        let result = match crate::tikv::clock_value::finish_source(
            *operation,
            result,
            result_type,
            self.context,
            usize::MAX,
        ) {
            Ok(v) => v,
            Err(e) => return self.native_failure(e),
        };
        self.finish_native_value(id, result, *output_node, *output_role, row)
            .map(HostStep::Ready)
    }
}
