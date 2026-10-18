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

//! Actual TIMESTAMPDIFF sources and authenticated retained prefixes.
use super::*;
impl NativeBatchHosts<'_> {
    pub(super) fn resume_timestamp_diff(
        &mut self,
        id: &HostTaskId,
        token: &[u8],
    ) -> LocalResult<HostStep> {
        let task = self.task(id)?;
        let SqlDemand::TimestampDiff { next, tokens } = &task.demand else {
            return Err(binding("timestamp diff task kind differs"));
        };
        let next = *next;
        let mut tokens = *tokens;
        let Some(NativeHostMetadata::TimestampDiff {
            arguments,
            argument_types,
            stop_on_null,
            result_type,
            output_node,
            output_role,
        }) = self.metadata.get(task.slot.index())
        else {
            return Err(binding("timestamp diff metadata absent"));
        };
        let row = task.row;
        let store = self
            .native_values
            .ok_or_else(|| binding("timestamp diff store absent"))?;
        tokens[next] = Some(
            token
                .try_into()
                .map_err(|_| binding("timestamp diff capability width differs"))?,
        );
        let base = self.retained_bytes()?;
        let observed_null = {
            let values = store.borrow();
            for i in 0..=next {
                values.get(
                    tokens[i]
                        .as_ref()
                        .ok_or_else(|| binding("timestamp diff prefix absent"))?,
                    NativeValueRole::CharInput,
                    NativeValueOrigin {
                        node: arguments[i],
                        row,
                    },
                )?;
            }
            values
                .get(
                    token,
                    NativeValueRole::CharInput,
                    NativeValueOrigin {
                        node: arguments[next],
                        row,
                    },
                )?
                .is_null()
        };
        let value =
            if *stop_on_null && observed_null {
                Some(Ok(Datum::Null))
            } else if next == 2 {
                let mut live = base;
                {
                    let values = store.borrow();
                    for i in 0..3 {
                        let value = values.get(
                            tokens[i].as_ref().expect("complete prefix"),
                            NativeValueRole::CharInput,
                            NativeValueOrigin {
                                node: arguments[i],
                                row,
                            },
                        )?;
                        live = live
                            .checked_add(native_payload_bytes(value)?.checked_mul(2).ok_or_else(
                                || resource("timestamp diff clone allowance overflow"),
                            )?)
                            .ok_or_else(|| resource("timestamp diff source extent overflow"))?;
                    }
                }
                self.check_bytes(live)?;
                let mut owned = {
                    let values = store.borrow();
                    let get = |i: usize| {
                        values.clone_for_kernel(
                            tokens[i].as_ref().expect("complete prefix"),
                            NativeValueRole::CharInput,
                            NativeValueOrigin {
                                node: arguments[i],
                                row,
                            },
                        )
                    };
                    [get(0)?, get(1)?, get(2)?]
                };
                // Original generic endpoint casts may read modes/zone/clock and
                // warn. All source ownership is detached before those callbacks.
                let cast = if *stop_on_null {
                    Ok(())
                } else {
                    crate::tikv::timestamp_diff_cast_arguments_bounded(
                        self.context,
                        &mut owned,
                        argument_types,
                        self.max_retained_bytes - live,
                    )
                };
                Some(cast.and_then(|()| {
                    crate::tikv::timestamp_diff_values_bounded(
                        &owned[0],
                        &owned[1],
                        &owned[2],
                        self.max_retained_bytes - live,
                    )
                }))
            } else {
                None
            };
        let Some(value) = value else {
            let SqlDemand::TimestampDiff {
                next: saved,
                tokens: saved_tokens,
            } = &mut self.slots[id.slot]
                .task
                .as_mut()
                .expect("validated timestamp task")
                .demand
            else {
                unreachable!()
            };
            *saved = next + 1;
            *saved_tokens = tokens;
            return Ok(HostStep::NeedArg(HostArgRequest {
                index: next + 1,
                mode: ArgMode::Fresh,
            }));
        };
        let value = match value {
            Ok(value) => value,
            Err(error) => return self.native_failure(error),
        };
        // PB evalInt interprets bits through unsigned metadata before generic RT.
        let value = match value {
            Datum::Int(value) if *stop_on_null && result_type.is_unsigned() => {
                Datum::UInt(value as u64)
            }
            value => value,
        };
        let value = match crate::scalar_function::coerce_function_result(
            "timestampdiff",
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
            Ok(value) => value.map(|v| v as i64),
            Err(error) => return self.native_failure(error),
        };
        self.check_bytes(
            base.checked_add(128)
                .ok_or_else(|| resource("timestamp diff publication extent overflow"))?,
        )?;
        let output = VectorValue::from_scalar(&ScalarValue::Int(bits), 1);
        self.slots[id.slot].task = None;
        Ok(HostStep::Ready(output))
    }
}
