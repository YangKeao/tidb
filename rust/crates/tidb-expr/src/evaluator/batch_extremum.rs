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

//! Eager GREATEST/LEAST sources in the same SDK frame, then actual value kernels.
use super::*;
pub(in super::super) struct NativeExtremumMetadata {
    pub arguments: Box<[usize]>,
    pub signature: Option<crate::builtin_ext::GlSignature>,
    pub decimals: Box<[i64]>,
    pub all_constant: bool,
    pub collation: tidb_datatype::Collation,
    pub greatest: bool,
    pub result_type: Option<FieldType>,
    pub output_node: usize,
    pub output_role: NativeValueRole,
}
impl NativeBatchHosts<'_> {
    pub(super) fn start_extremum(&self, invocation: &HostInvocation<'_>) -> LocalResult<SqlDemand> {
        let Some(NativeHostMetadata::Extremum(metadata)) =
            self.metadata.get(invocation.slot.index())
        else {
            return Err(binding("extremum source descriptor missing"));
        };
        if self.native_values.is_none()
            || metadata.arguments.is_empty()
            || metadata.arguments.len() != invocation.arg_types.len()
            || metadata.decimals.len() != metadata.arguments.len()
        {
            return Err(binding("extremum source descriptor differs"));
        }
        Ok(SqlDemand::Extremum {
            next: 0,
            tokens: Vec::new(),
        })
    }
    pub(super) fn resume_extremum(
        &mut self,
        id: &HostTaskId,
        token: &[u8],
    ) -> LocalResult<HostStep> {
        let task = self.task(id)?;
        let SqlDemand::Extremum { next, .. } = task.demand else {
            return Err(binding("extremum has another task kind"));
        };
        let row = task.row;
        let Some(NativeHostMetadata::Extremum(metadata)) = self.metadata.get(task.slot.index())
        else {
            return Err(binding("extremum metadata vanished"));
        };
        let store = self
            .native_values
            .ok_or_else(|| binding("extremum value store missing"))?;
        let node = *metadata
            .arguments
            .get(next)
            .ok_or_else(|| binding("extremum argument outside descriptor"))?;
        store.borrow().get(
            token,
            NativeValueRole::CharInput,
            NativeValueOrigin { node, row },
        )?;

        let handle = token
            .try_into()
            .map_err(|_| binding("extremum capability width differs"))?;
        let SqlDemand::Extremum { tokens, .. } = &mut self.slots[id.slot]
            .task
            .as_mut()
            .expect("validated extremum task")
            .demand
        else {
            unreachable!()
        };
        tokens
            .try_reserve_exact(1)
            .map_err(|error| LocalError::ResourceLimit(error.to_string()))?;
        tokens.push(handle);

        if next + 1 < metadata.arguments.len() {
            let SqlDemand::Extremum { next: index, .. } = &mut self.slots[id.slot]
                .task
                .as_mut()
                .expect("validated extremum task")
                .demand
            else {
                unreachable!()
            };
            *index = next + 1;
            return Ok(HostStep::NeedArg(HostArgRequest {
                index: next + 1,
                mode: ArgMode::Fresh,
            }));
        }
        // All original sources have completed, even after an actual NULL.
        // Authenticate the complete retained set before cloning or context work.
        let task = self.task(id)?;
        let SqlDemand::Extremum { tokens, .. } = &task.demand else {
            unreachable!()
        };

        {
            let values = store.borrow();
            for (token, &node) in tokens.iter().zip(metadata.arguments.iter()) {
                values.get(
                    token,
                    NativeValueRole::CharInput,
                    NativeValueOrigin { node, row },
                )?;
            }
        }

        let mut values = Vec::new();
        values
            .try_reserve_exact(tokens.len())
            .map_err(|error| LocalError::ResourceLimit(error.to_string()))?;

        for (token, &node) in tokens.iter().zip(metadata.arguments.iter()) {
            values.push(store.borrow().clone_for_kernel(
                token,
                NativeValueRole::CharInput,
                NativeValueOrigin { node, row },
            )?);
        }
        let want = if metadata.greatest {
            std::cmp::Ordering::Greater
        } else {
            std::cmp::Ordering::Less
        };
        let value = match crate::tikv::eval_extremum_values_bounded(
            self.context,
            &values,
            want,
            metadata.signature,
            &metadata.decimals,
            metadata.all_constant,
            metadata.collation,
            usize::MAX,
        ) {
            Ok(value) => value,
            Err(error) => return self.native_failure(error),
        };
        let name = if metadata.greatest {
            "greatest"
        } else {
            "least"
        };
        let value = match crate::scalar_function::coerce_function_result(
            name,
            metadata.result_type.as_ref(),
            value,
        ) {
            Ok(value) => value,
            Err(error) => return self.native_failure(error),
        };
        self.finish_native_value(id, value, metadata.output_node, metadata.output_role, row)
            .map(HostStep::Ready)
    }
}
