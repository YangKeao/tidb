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

//! Both actual date sources, then owned preparation before statement callbacks.
use super::*;
use tidb_query_expr::local::SqlTemporalOutput as Output;
impl NativeBatchHosts<'_> {
    pub(super) fn resume_str_to_date(
        &mut self,
        id: &HostTaskId,
        token: &[u8],
    ) -> LocalResult<HostStep> {
        let task = self.task(id)?;
        let SqlDemand::StrToDate { first } = task.demand else {
            return Err(binding("STR_TO_DATE task kind differs"));
        };
        let Some(NativeHostMetadata::StrToDate {
            arguments,
            result_type,
            output_node,
            output_role,
            output,
        }) = self.metadata.get(task.slot.index())
        else {
            return Err(binding("STR_TO_DATE metadata missing"));
        };
        let row = task.row;
        let store = self
            .native_values
            .ok_or_else(|| binding("STR_TO_DATE store missing"))?;
        if first.is_none() {
            store.borrow().get(
                token,
                NativeValueRole::CharInput,
                NativeValueOrigin {
                    node: arguments[0],
                    row,
                },
            )?;
            let first = token
                .try_into()
                .map_err(|_| binding("STR_TO_DATE capability width differs"))?;
            let SqlDemand::StrToDate { first: saved } = &mut self.slots[id.slot]
                .task
                .as_mut()
                .expect("validated date task")
                .demand
            else {
                unreachable!()
            };
            *saved = Some(first);
            // BOTH original sources are eager even after an actual NULL.
            return Ok(HostStep::NeedArg(HostArgRequest {
                index: 1,
                mode: ArgMode::Fresh,
            }));
        }

        let remaining = usize::MAX;
        let prepared = {
            let values = store.borrow();
            let first = values.get(
                first.as_ref().expect("retained source"),
                NativeValueRole::CharInput,
                NativeValueOrigin {
                    node: arguments[0],
                    row,
                },
            )?;
            let second = values.get(
                token,
                NativeValueRole::CharInput,
                NativeValueOrigin {
                    node: arguments[1],
                    row,
                },
            )?;
            crate::tikv::prepare_str_to_date_value(first, second, remaining)
        };
        let prepared = match prepared {
            Ok(value) => value,
            Err(error) => return self.native_failure(error),
        };
        // Owned input/format strings survive callbacks, never the store borrow.
        let value = crate::tikv::compute_str_to_date_value(
            self.context,
            prepared.as_ref(),
            Some(result_type.code()),
            remaining,
        );
        drop(prepared);
        let value = match value {
            Ok(value) => value,
            Err(error) => return self.native_failure(error),
        };
        let value = match crate::scalar_function::finish_native_temporal_result(
            Some(result_type),
            value,
            self.context,
        ) {
            Ok(value) => value,
            Err(error) => return self.native_failure(error),
        };
        let value = match crate::scalar_function::coerce_function_result(
            "str_to_date",
            Some(result_type),
            value,
        ) {
            Ok(value) => value,
            Err(error) => return self.native_failure(error),
        };
        if let Some(role) = output_role {
            return self
                .finish_native_value(id, value, *output_node, *role, row)
                .map(HostStep::Ready);
        }
        let value = crate::expression::apply_enum_set_integer_view(value, Some(result_type));
        let output = match output {
            Output::Value => return Err(binding("STR_TO_DATE native carrier lost output role")),
            Output::Bytes => {
                let bytes = match crate::arg_eval_type::eval_string_ref(&value) {
                    Ok(bytes) => bytes,
                    Err(error) => return self.native_failure(error),
                };
                let length = bytes.map_or(0, <[u8]>::len);

                let mut output = ChunkedVecBytes::try_with_capacities(1, length)
                    .map_err(|error| LocalError::ResourceLimit(error.to_string()))?;
                output.push_ref(bytes);

                VectorValue::Bytes(output)
            }
            Output::DateTime => {
                let value = match value {
                    Datum::Null => None,
                    Datum::Time(value) => Some(TikvDateTime::from_go_raw_like_go(value.go_raw())),
                    _ => {
                        return Err(binding(
                            "STR_TO_DATE temporal result differs from its carrier",
                        ))
                    }
                };
                VectorValue::from_scalar(&ScalarValue::DateTime(value), 1)
            }
            Output::Duration => {
                let value = match value {
                    Datum::Null => None,
                    Datum::Duration(value) => Some(
                        TikvDuration::from_native_raw_parts(
                            value.nanoseconds(),
                            i8::try_from(value.fsp())
                                .map_err(|_| binding("date duration FSP is not representable"))?,
                        )
                        .map_err(|_| binding("date duration is not representable"))?,
                    ),
                    _ => {
                        return Err(binding(
                            "STR_TO_DATE duration result differs from its carrier",
                        ))
                    }
                };
                VectorValue::from_scalar(&ScalarValue::Duration(value), 1)
            }
        };
        self.slots[id.slot].task = None;
        Ok(HostStep::Ready(output))
    }
}
