// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Actual INTERVAL source probes in the same SDK frame, without Expr callbacks.
use super::*;
use tidb_query_expr::{NativeIntervalSearch, NativeIntervalValueStep as Step};

pub(in super::super) struct NativeIntervalArgument {
    pub node: usize,
    pub field: FieldType,
}
pub(in super::super) struct NativeIntervalMetadata {
    pub arguments: Box<[NativeIntervalArgument]>,
    pub search: NativeIntervalSearch,
    pub result_type: FieldType,
    pub output_node: usize,
    pub output_role: NativeValueRole,
}
impl NativeBatchHosts<'_> {
    pub(super) fn start_interval(&self, invocation: &HostInvocation<'_>) -> LocalResult<SqlDemand> {
        let Some(NativeHostMetadata::Interval(metadata)) =
            self.metadata.get(invocation.slot.index())
        else {
            return Err(binding("INTERVAL descriptor missing"));
        };
        if self.native_values.is_none()
            || metadata.arguments.len() != invocation.arg_types.len()
            || metadata.arguments.len() < 2
            || metadata.search.argument_count() != metadata.arguments.len()
            || !matches!(metadata.search.step(), Step::Probe { index: 0, .. })
        {
            return Err(binding("INTERVAL source descriptor differs"));
        }
        Ok(SqlDemand::Interval {
            search: metadata.search,
        })
    }
    pub(super) fn resume_interval(
        &mut self,
        id: &HostTaskId,
        token: &[u8],
    ) -> LocalResult<HostStep> {
        let task = self.task(id)?;
        let SqlDemand::Interval { mut search } = task.demand else {
            return Err(binding("INTERVAL has another task kind"));
        };
        let row = task.row;
        let Some(NativeHostMetadata::Interval(metadata)) = self.metadata.get(task.slot.index())
        else {
            return Err(binding("INTERVAL metadata vanished"));
        };
        let Step::Probe { index, cast } = search.step() else {
            return Err(binding("INTERVAL completed task cannot resume"));
        };
        let argument = metadata
            .arguments
            .get(index)
            .ok_or_else(|| binding("INTERVAL probe outside source descriptor"))?;
        let origin = NativeValueOrigin {
            node: argument.node,
            row,
        };
        let store = self
            .native_values
            .ok_or_else(|| binding("INTERVAL value store absent"))?;
        let payload = native_payload_bytes(store.borrow().get(
            token,
            NativeValueRole::CharInput,
            origin,
        )?)?;
        let base = self
            .retained_bytes()?
            .checked_add(
                payload
                    .checked_mul(2)
                    .ok_or_else(|| resource("INTERVAL source clone allowance overflow"))?,
            )
            .ok_or_else(|| resource("INTERVAL source live bytes overflow"))?;
        self.check_bytes(base)?;
        let value = store
            .borrow()
            .clone_for_kernel(token, NativeValueRole::CharInput, origin)?;
        // The source owner and borrow are independent of context effects. Only
        // this demanded argument is cast; skipped sources are never inspected.
        let number = match crate::tikv::prepare_interval_number_in(
            self.context,
            &value,
            Some(&argument.field),
            cast,
            self.max_retained_bytes - base,
        ) {
            Ok(value) => value,
            Err(error) => return self.native_failure(error),
        };
        let step = match search.accept(number) {
            Ok(step) => step,
            Err(error) => return self.native_failure(crate::tikv::interval_value_error(error)),
        };
        match step {
            Step::Probe { index, .. } => {
                let SqlDemand::Interval { search: state } = &mut self.slots[id.slot]
                    .task
                    .as_mut()
                    .expect("validated INTERVAL task")
                    .demand
                else {
                    unreachable!()
                };
                *state = search;
                Ok(HostStep::NeedArg(HostArgRequest {
                    index,
                    mode: ArgMode::Fresh,
                }))
            }
            Step::Index(index) => {
                let value = match crate::scalar_function::coerce_function_result(
                    "interval",
                    Some(&metadata.result_type),
                    Datum::Int(index),
                ) {
                    Ok(value) => value,
                    Err(error) => return self.native_failure(error),
                };
                self.finish_native_value(
                    id,
                    value,
                    metadata.output_node,
                    metadata.output_role,
                    row,
                    base,
                )
                .map(HostStep::Ready)
            }
        }
    }
}
