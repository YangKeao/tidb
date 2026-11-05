// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Transaction timestamps over authenticated actual-value capabilities.
use super::*;
impl NativeBatchHosts<'_> {
    pub(super) fn resume_tso(
        &mut self,
        id: &HostTaskId,
        token: Option<&[u8]>,
    ) -> LocalResult<HostStep> {
        let task = self.task(id)?;
        if !matches!(task.demand, SqlDemand::Tso) {
            return Err(binding("TSO task differs"));
        }
        let Some(NativeHostMetadata::Tso {
            argument,
            argument_type,
            operation,
            result_type,
            output_node,
            output_role,
        }) = self.metadata.get(task.slot.index())
        else {
            return Err(binding("TSO metadata missing"));
        };
        if argument.is_some() != token.is_some() {
            return Err(binding("TSO source presence differs"));
        }
        let row = task.row;

        let store = self
            .native_values
            .ok_or_else(|| binding("TSO store missing"))?;
        let owned = if let Some(node) = argument {
            let token = token.ok_or_else(|| binding("TSO source lost"))?;
            let origin = NativeValueOrigin { node: *node, row };
            let values = store.borrow();
            values.get(token, NativeValueRole::CharInput, origin)?;

            Some(values.clone_for_kernel(token, NativeValueRole::CharInput, origin)?)
        } else {
            None
        };

        let remaining = usize::MAX;
        // The clone, not a store borrow, crosses the original zone/transaction
        // getter. These callbacks may exclusively access the frame-owned store.
        let result = crate::tikv::tso_value::eval_source_bounded(
            *operation,
            owned.as_ref().map_or(&[] as &[Datum], std::slice::from_ref),
            argument_type.as_ref(),
            self.context,
            remaining,
        );
        drop(owned);
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
