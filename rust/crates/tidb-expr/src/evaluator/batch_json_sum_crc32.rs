// Copyright 2026 PingCAP, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Actual JSON checksum input in the same SDK source frame.
use super::*;
impl NativeBatchHosts<'_> {
    pub(super) fn resume_json_sum_crc32(
        &mut self,
        id: &HostTaskId,
        token: &[u8],
    ) -> LocalResult<HostStep> {
        let task = self.task(id)?;
        let SqlDemand::JsonSumCrc32 = task.demand else {
            return Err(binding("JSON checksum task kind differs"));
        };
        let Some(NativeHostMetadata::JsonSumCrc32 {
            argument_node,
            result_type,
            output_node,
            output_role,
        }) = self.metadata.get(task.slot.index())
        else {
            return Err(binding("JSON checksum metadata missing"));
        };
        let row = task.row;
        let store = self
            .native_values
            .ok_or_else(|| binding("JSON checksum store absent"))?;
        let base = self.retained_bytes()?;
        let value = {
            let values = store.borrow();
            let value = values.get(
                token,
                NativeValueRole::CharInput,
                NativeValueOrigin {
                    node: *argument_node,
                    row,
                },
            )?;
            crate::tikv::json_sum_crc32_bounded(value, self.max_retained_bytes - base)
        };
        let value = match value {
            Ok(value) => value,
            Err(error) => return self.native_failure(error),
        };
        let value = match crate::scalar_function::coerce_function_result(
            "json_sum_crc32",
            Some(result_type),
            value,
        ) {
            Ok(value) => value,
            Err(error) => return self.native_failure(error),
        };
        if let Some(role) = output_role {
            return self
                .finish_native_value(id, value, *output_node, *role, row, base)
                .map(HostStep::Ready);
        }
        let value = crate::expression::apply_enum_set_integer_view(value, Some(result_type));
        let bits = match crate::arg_eval_type::eval_int(&value) {
            Ok(value) => value.map(|value| value as i64),
            Err(error) => return self.native_failure(error),
        };
        self.check_bytes(
            base.checked_add(128)
                .ok_or_else(|| resource("JSON checksum typed publication extent overflow"))?,
        )?;
        let output = VectorValue::from_scalar(&ScalarValue::Int(bits), 1);
        self.slots[id.slot].task = None;
        Ok(HostStep::Ready(output))
    }
}
