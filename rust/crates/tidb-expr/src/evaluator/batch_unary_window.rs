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

//! Whole-child native ABS/CAST phases. Values and metadata only; the single
//! SDK frame owns argument scheduling and cancellation.
use super::*;
fn result_domain(operation: SqlBatchHostOperation) -> LocalResult<NativeEvalType> {
    match operation {
        SqlBatchHostOperation::AbsIntWindowNative => Ok(NativeEvalType::Int),
        SqlBatchHostOperation::CastDecimalWindowNative => Ok(NativeEvalType::Decimal),
        SqlBatchHostOperation::CastJsonWindowNative => Ok(NativeEvalType::Json),
        _ => Err(binding("unary window descriptor has a different operation")),
    }
}
impl NativeBatchHosts<'_> {
    pub(super) fn start_unary_window(
        &mut self,
        invocation: HostWindowInvocation<'_>,
    ) -> LocalResult<HostStart> {
        let actual = self.catalog.sql_batch_operation(invocation.slot)?;
        let signature = self.catalog.signature(invocation.slot)?;
        let Some(NativeHostMetadata::UnaryWindow {
            operation,
            result_type,
            source_type,
            source,
            output_role,
            ..
        }) = self.metadata.get(invocation.slot.index())
        else {
            return Err(binding("unary window descriptor missing"));
        };
        if *operation != actual
            || signature.arg_types.as_ref() != invocation.arg_types
            || &signature.return_type != invocation.return_type
            || invocation.arg_types.len() != 1
            || invocation.physical_rows.is_empty()
            || invocation
                .occurrence_start
                .checked_add(invocation.physical_rows.len() - 1)
                .is_none()
            || result_type.eval_type() != result_domain(actual)?
            || self.native_values.is_none()
            || !matches!(
                output_role,
                NativeValueRole::CharInput | NativeValueRole::Output
            )
            || (actual != SqlBatchHostOperation::CastJsonWindowNative
                && source_type.eval_type() != NativeEvalType::Int)
            || (matches!(source, WindowArithmeticSource::IntegerReady)
                && source_type.eval_type() != NativeEvalType::Int)
        {
            return Err(binding("unary window invocation differs from descriptor"));
        }
        if actual == SqlBatchHostOperation::CastJsonWindowNative {
            if let Err(error) =
                crate::builtin_ext::json::validate_json_cast_source(Some(source_type))
            {
                return self.native_failure(error);
            }
        }
        self.check_bytes(
            self.retained_bytes()?
                .checked_add(array_bytes::<usize>(invocation.physical_rows.len())?)
                .ok_or_else(|| resource("unary window row map allowance overflow"))?,
        )?;
        let mut rows = Vec::new();
        rows.try_reserve_exact(invocation.physical_rows.len())
            .map_err(|error| LocalError::ResourceLimit(error.to_string()))?;
        rows.extend_from_slice(invocation.physical_rows);
        self.install_task(
            invocation.slot,
            InputRow {
                occurrence: invocation.occurrence_start,
                input_row: rows[0],
            },
            1,
            SqlDemand::WindowUnary { rows },
        )
    }
    pub(super) fn resume_unary_window(
        &mut self,
        id: &HostTaskId,
        reply: HostWindowArgReply<'_>,
    ) -> LocalResult<HostStep> {
        let metadata = self.metadata;
        let task = self.task(id)?;
        let SqlDemand::WindowUnary { rows } = &task.demand else {
            return Err(binding("unary window reply reached a different task"));
        };
        let slot = task.slot;
        let start = task.row.occurrence;
        if reply.index != 0
            || reply.occurrence_start != start
            || reply.physical_rows != rows
            || reply.values.len() != rows.len()
            || reply.values.eval_type() != TikvEvalType::Bytes
            || self.catalog.signature(slot)?.arg_types.first() != Some(reply.field_type)
        {
            return Err(binding("unary window reply differs from demand"));
        }
        let Some(NativeHostMetadata::UnaryWindow {
            operation,
            result_type,
            source_type,
            source,
            argument_node,
            output_node,
            output_role,
            overflow_expression,
        }) = metadata.get(slot.index())
        else {
            return Err(binding("unary window result descriptor missing"));
        };
        let target = if *operation == SqlBatchHostOperation::CastJsonWindowNative {
            source_type.eval_type()
        } else {
            NativeEvalType::Int
        };
        let WindowSourceValues {
            mut values,
            retained_bytes,
        } = self.collect_window_source(
            &reply,
            WindowSourceFacts {
                field: source_type,
                owner: *source,
                node: *argument_node,
                target,
            },
        )?;
        let count = reply.physical_rows.len();
        let base = self
            .retained_bytes()?
            .checked_add(retained_bytes)
            .ok_or_else(|| resource("unary window argument size overflow"))?;
        let payload = count
            .checked_mul(native_values::HANDLE_BYTES)
            .ok_or_else(|| resource("unary window result payload overflow"))?;
        self.check_bytes(
            base.checked_add(array_bytes::<usize>(
                count
                    .checked_add(1)
                    .ok_or_else(|| resource("unary window result offsets overflow"))?,
            )?)
            .and_then(|bytes| bytes.checked_add(payload))
            .and_then(|bytes| bytes.checked_add(count))
            .ok_or_else(|| resource("unary window result allowance overflow"))?,
        )?;
        let mut output = ChunkedVecBytes::try_with_capacities(count, payload)
            .map_err(|error| LocalError::ResourceLimit(error.to_string()))?;
        let store = self
            .native_values
            .ok_or_else(|| binding("unary window store missing"))?;
        for (position, &input_row) in reply.physical_rows.iter().enumerate() {
            let current = base
                .checked_add(
                    output
                        .retained_heap_bytes()
                        .ok_or_else(|| resource("unary window output capacity overflow"))?,
                )
                .ok_or_else(|| resource("unary window compute allowance overflow"))?;
            self.check_bytes(current)?;
            let value = std::mem::replace(&mut values[position], Datum::Null);
            // Bound known formatting expansion before entering an opaque
            // native conversion, including a small Decimal with wide padding.
            let conversion = window_values::conversion_work_bound(
                &value,
                if *operation == SqlBatchHostOperation::CastJsonWindowNative {
                    128
                } else {
                    2
                },
            )?;
            self.check_bytes(
                current
                    .checked_add(conversion)
                    .ok_or_else(|| resource("unary window conversion allowance overflow"))?,
            )?;
            let result = match operation {
                SqlBatchHostOperation::AbsIntWindowNative => {
                    let bits = match crate::scalar_function::native_batch_integer_bits(value) {
                        Ok(bits) => bits,
                        Err(error) => return self.native_failure(error),
                    };
                    let result = match tidb_query_expr::impl_math::native_abs_int_value(bits) {
                        Ok(value) => value,
                        Err(_) => {
                            let error = if let Some(expression) = overflow_expression {
                                self.check_bytes(
                                    current
                                        .checked_add(expression.len().checked_mul(2).ok_or_else(
                                            || resource("unary ABS diagnostic copy overflow"),
                                        )?)
                                        .ok_or_else(|| {
                                            resource("unary ABS diagnostic size overflow")
                                        })?,
                                )?;
                                EvalError::DataOutOfRange {
                                    value: "BIGINT",
                                    expression: expression.to_string(),
                                }
                            } else {
                                EvalError::IntOverflow
                            };
                            return self.native_failure(error);
                        }
                    };
                    crate::scalar_function::native_batch_integer_value(
                        result,
                        result_type.is_unsigned(),
                    )
                }
                SqlBatchHostOperation::CastDecimalWindowNative => {
                    let bits = match crate::scalar_function::native_batch_integer_bits(value) {
                        Ok(bits) => bits,
                        Err(error) => return self.native_failure(error),
                    };
                    match crate::scalar_function::native_integer_batch_decimal_value(
                        bits,
                        source_type.is_unsigned(),
                        result_type,
                        self.context,
                    ) {
                        Ok(value) => value,
                        Err(error) => return self.native_failure(error),
                    }
                }
                SqlBatchHostOperation::CastJsonWindowNative => {
                    match crate::builtin_ext::json::cast_json_prepared(
                        &value,
                        Some(source_type),
                        Some(result_type),
                    ) {
                        Ok(value) => value,
                        Err(error) => return self.native_failure(error),
                    }
                }
                _ => return Err(binding("unary window operation changed after demand")),
            };
            self.check_bytes(
                current
                    .checked_add(
                        native_payload_bytes(&result)?
                            .checked_mul(2)
                            .ok_or_else(|| resource("unary window result copy overflow"))?,
                    )
                    .ok_or_else(|| resource("unary window result overlap overflow"))?,
            )?;
            let token = store.borrow_mut().insert_clone(
                &result,
                *output_role,
                NativeValueOrigin {
                    node: *output_node,
                    row: InputRow {
                        occurrence: start + position,
                        input_row,
                    },
                },
            )?;
            output.push_ref(Some(&token));
        }
        self.slots[id.slot].task = None;
        Ok(HostStep::Ready(VectorValue::Bytes(output)))
    }
}

#[cfg(test)]
#[path = "batch_unary_window_tests.rs"]
mod tests;
