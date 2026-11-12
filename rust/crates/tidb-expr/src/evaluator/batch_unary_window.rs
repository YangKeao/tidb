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
        let actual = if actual == SqlBatchHostOperation::EagerValuesWindowNative
            && self.diagnostic_parent(invocation.slot).is_some()
            && *operation == SqlBatchHostOperation::AbsIntWindowNative
        {
            *operation
        } else {
            actual
        };
        let arity = self
            .diagnostic_parent(invocation.slot)
            .map_or(1, |p| p.main_arity + p.arguments.len());
        if *operation != actual
            || signature.arg_types.as_ref() != invocation.arg_types
            || &signature.return_type != invocation.return_type
            || invocation.arg_types.len() != arity
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
            arity,
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
            protobuf_owner,
        }) = metadata.get(slot.index())
        else {
            return Err(binding("unary window result descriptor missing"));
        };
        let target = if *operation == SqlBatchHostOperation::CastJsonWindowNative {
            source_type.eval_type()
        } else {
            NativeEvalType::Int
        };
        let mut values = self.collect_window_source(
            &reply,
            WindowSourceFacts {
                field: source_type,
                owner: *source,
                node: *argument_node,
                target,
            },
            None,
        )?;
        let count = reply.physical_rows.len();

        let payload = count
            .checked_mul(native_values::HANDLE_BYTES)
            .ok_or_else(|| resource("unary window result payload overflow"))?;

        let mut output = ChunkedVecBytes::try_with_capacities(count, payload)
            .map_err(|error| LocalError::ResourceLimit(error.to_string()))?;
        let store = self
            .native_values
            .ok_or_else(|| binding("unary window store missing"))?;
        for (position, &input_row) in reply.physical_rows.iter().enumerate() {
            let value = std::mem::replace(&mut values[position], Datum::Null);
            // Bound known formatting expansion before entering an opaque
            // native conversion, including a small Decimal with wide padding.

            let result = match operation {
                SqlBatchHostOperation::AbsIntWindowNative => {
                    let bits = match crate::scalar_function::native_batch_integer_bits(value) {
                        Ok(bits) => bits,
                        Err(error) => return self.native_failure(error),
                    };
                    // The original ABS skips its Ready operation for NULL, but
                    // observes an earlier unwind/reentry before a real value.
                    if *protobuf_owner && bits.is_some() {
                        let cache = self.context.ready_value_cache();
                        // The getter is a user callback. Authenticate the real
                        // source capability again before using detached data.
                        let ScalarValueRef::Bytes(Some(token)) =
                            reply.values.get_scalar_ref(position)
                        else {
                            return Err(binding("unary scope source is not a native capability"));
                        };
                        store.borrow().get(
                            token,
                            NativeValueRole::CharInput,
                            NativeValueOrigin {
                                node: *argument_node,
                                row: InputRow {
                                    occurrence: start + position,
                                    input_row,
                                },
                            },
                        )?;
                        if let Some(cache) = cache {
                            if let Err(error) = cache.original_scope_status() {
                                return self.native_failure(error);
                            }
                        }
                    }
                    let result = match tidb_query_expr::impl_math::native_abs_int_value(bits) {
                        Ok(value) => value,
                        Err(_) => {
                            if self.diagnostic_parent(slot).is_some() {
                                return self.begin_window_math_diagnostic(
                                    id,
                                    position,
                                    EvalError::IntOverflow,
                                );
                            }
                            let error = if let Some(expression) = overflow_expression {
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
