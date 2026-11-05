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

//! Actual eager date operands and the shared scalar temporal result policy.
use super::*;
impl NativeBatchHosts<'_> {
    pub(super) fn resume_date_arithmetic(
        &mut self,
        id: &HostTaskId,
        token: &[u8],
    ) -> LocalResult<HostStep> {
        let task = self.task(id)?;
        let SqlDemand::DateArithmetic { first } = task.demand else {
            return Err(binding("date arithmetic task differs"));
        };
        let Some(NativeHostMetadata::DateArithmetic {
            arguments,
            argument_types,
            unit,
            sign,
            result_fsp,
            output,
            result_type,
            output_node,
            output_role,
        }) = self.metadata.get(task.slot.index())
        else {
            return Err(binding("date arithmetic metadata missing"));
        };
        let row = task.row;
        let store = self
            .native_values
            .ok_or_else(|| binding("date arithmetic value store missing"))?;
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
                .map_err(|_| binding("date arithmetic capability width differs"))?;
            let SqlDemand::DateArithmetic { first: saved } = &mut self.slots[id.slot]
                .task
                .as_mut()
                .expect("validated date arithmetic task")
                .demand
            else {
                unreachable!()
            };
            *saved = Some(first);
            return Ok(HostStep::NeedArg(HostArgRequest {
                index: 1,
                mode: ArgMode::Fresh,
            }));
        }
        let tokens = [
            first
                .as_ref()
                .expect("retained date arithmetic source")
                .as_slice(),
            token,
        ];

        {
            let values = store.borrow();
            for i in 0..2 {
                values.get(
                    tokens[i],
                    NativeValueRole::CharInput,
                    NativeValueOrigin {
                        node: arguments[i],
                        row,
                    },
                )?;
            }
        }

        let mut owned = {
            let values = store.borrow();
            let get = |i: usize| {
                values.clone_for_kernel(
                    tokens[i],
                    NativeValueRole::CharInput,
                    NativeValueOrigin {
                        node: arguments[i],
                        row,
                    },
                )
            };
            [get(0)?, get(1)?]
        };
        // Bounded payload ownership is detached before any original cast, warning,
        // overflow-policy or result callback can access the value store.
        let remaining = usize::MAX;
        let value = crate::tikv::date_add_scalar_values_bounded(
            self.context,
            unit,
            &mut owned,
            argument_types.each_ref().map(Option::as_ref),
            result_type.as_ref(),
            *sign,
            *result_fsp,
            remaining,
        );
        drop(owned);
        let value = match value {
            Ok(value) => value,
            Err(error) => return self.native_failure(error),
        };

        let value = match coerce_optional_value_result(
            if *sign < 0 { "date_sub" } else { "date_add" },
            result_type.as_ref(),
            value,
        ) {
            Ok(v) => v,
            Err(e) => return self.native_failure(e),
        };
        if let Some(role) = output_role {
            return self
                .finish_native_value(id, value, *output_node, *role, row)
                .map(HostStep::Ready);
        }
        let value = crate::expression::apply_enum_set_integer_view(value, result_type.as_ref());
        let output = match output {
            tidb_query_expr::local::SqlTemporalOutput::Value => {
                return Err(binding("date arithmetic native carrier lost output role"))
            }
            tidb_query_expr::local::SqlTemporalOutput::Bytes => {
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
            tidb_query_expr::local::SqlTemporalOutput::DateTime => {
                let value = match value {
                    Datum::Null => None,
                    Datum::Time(value) => Some(TikvDateTime::from_go_raw_like_go(value.go_raw())),
                    _ => {
                        return Err(binding(
                            "date arithmetic temporal result differs from its carrier",
                        ))
                    }
                };
                VectorValue::from_scalar(&ScalarValue::DateTime(value), 1)
            }
            tidb_query_expr::local::SqlTemporalOutput::Duration => {
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
                            "date arithmetic duration result differs from its carrier",
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
