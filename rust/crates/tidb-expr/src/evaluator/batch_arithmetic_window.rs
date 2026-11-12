// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Value-only window arithmetic. Scheduling and argument programs remain in
//! the SDK's single frame driver; this provider owns only demanded values.
use super::*;

fn row_operation(operation: SqlBatchHostOperation) -> Option<SqlBatchHostOperation> {
    Some(match operation {
        SqlBatchHostOperation::AddWindowNative => SqlBatchHostOperation::AddNative,
        SqlBatchHostOperation::SubtractWindowNative => SqlBatchHostOperation::SubtractNative,
        SqlBatchHostOperation::MultiplyWindowNative => SqlBatchHostOperation::MultiplyNative,
        SqlBatchHostOperation::ModuloWindowNative => SqlBatchHostOperation::ModuloNative,
        SqlBatchHostOperation::DivideWindowNative => SqlBatchHostOperation::DivideNative,
        SqlBatchHostOperation::IntegerDivideWindowNative => {
            SqlBatchHostOperation::IntegerDivideNative
        }
        _ => return None,
    })
}
impl NativeBatchHosts<'_> {
    pub(super) fn start_arithmetic_window(
        &mut self,
        invocation: HostWindowInvocation<'_>,
    ) -> LocalResult<HostStart> {
        let signature = self.catalog.signature(invocation.slot)?;
        let operation = self.catalog.sql_batch_operation(invocation.slot)?;
        let Some(NativeHostMetadata::Arithmetic {
            operation: actual,
            batch: Some(batch),
            name,
            result_type,
            source_types,
            binary_literal,
            literal,
            generic_mode,
            intdiv,
            ..
        }) = self.metadata.get(invocation.slot.index())
        else {
            return Err(binding("window arithmetic descriptor missing"));
        };
        if source_types.iter().any(Option::is_none)
            || result_type.is_none()
            || signature.arg_types.as_ref() != invocation.arg_types
            || &signature.return_type != invocation.return_type
            || invocation.arg_types.len() != 2
            || invocation.physical_rows.is_empty()
            || invocation
                .occurrence_start
                .checked_add(invocation.physical_rows.len() - 1)
                .is_none()
            || self.native_values.is_none()
            || *actual != operation
            || row_operation(operation).is_none()
            || row_operation(operation) != native_arithmetic_operation(name)
            || (name.as_ref() == "intdiv") != intdiv.is_some()
            || !matches!(
                batch.domain,
                NativeEvalType::Int | NativeEvalType::Real | NativeEvalType::Decimal
            )
            || *generic_mode
                != crate::scalar_function::NativeArithmeticInputMode::Numeric(batch.domain)
            || [0, 1].into_iter().any(|index| {
                literal[index] != matches!(batch.sources[index], WindowArithmeticSource::Literal)
            })
            || (batch.integer
                && (batch.domain != NativeEvalType::Int
                    || !batch.output_integer
                    || [0, 1].into_iter().any(|index| {
                        crate::scalar_function::native_numeric_batch_source(
                            source_types[index]
                                .as_ref()
                                .expect("checked arithmetic window source"),
                            binary_literal[index],
                            batch.domain,
                        ) != NativeEvalType::Int
                    })))
        {
            return Err(binding("window arithmetic descriptor differs"));
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
            2,
            SqlDemand::WindowArithmetic {
                next: 0,
                rows,
                left: Vec::new(),
            },
        )
    }
    pub(super) fn resume_arithmetic_window(
        &mut self,
        id: &HostTaskId,
        reply: HostWindowArgReply<'_>,
    ) -> LocalResult<HostStep> {
        let metadata = self.metadata;
        let task = self.task(id)?;
        let SqlDemand::WindowArithmetic { next, rows, .. } = &task.demand else {
            return Err(binding("window reply reached a row task"));
        };
        let (slot, start, index) = (task.slot, task.row.occurrence, *next);
        if reply.index != index
            || reply.physical_rows != rows
            || reply.occurrence_start != start
            || reply.values.len() != rows.len()
            || reply.values.eval_type() != TikvEvalType::Bytes
            || self.catalog.signature(slot)?.arg_types.get(index) != Some(reply.field_type)
        {
            return Err(binding("window arithmetic reply differs from demand"));
        }
        let Some(NativeHostMetadata::Arithmetic {
            batch: Some(batch),
            source_types,
            binary_literal,
            argument_nodes,
            subjects,
            subject_format_failures,
            ..
        }) = metadata.get(slot.index())
        else {
            return Err(binding("window arithmetic metadata disappeared"));
        };
        let field = arithmetic_typed_source(source_types, index)?;
        let source = crate::scalar_function::native_numeric_batch_source(
            field,
            binary_literal[index],
            batch.domain,
        );
        if batch.integer && source != NativeEvalType::Int {
            return Err(binding("integer window source domain differs"));
        }
        let count = rows.len();

        let mut values = self.collect_window_source(
            &reply,
            WindowSourceFacts {
                field,
                owner: batch.sources[index],
                node: argument_nodes[index],
                target: source,
            },
            None,
        )?;
        // The complete typed source phase precedes every numeric argument cast.

        for position in 0..count {
            let value = std::mem::replace(&mut values[position], Datum::Null);
            let value = crate::scalar_function::native_numeric_batch_cast(
                field,
                source,
                batch.domain,
                value,
                self.context,
                subjects[index].as_deref(),
                subject_format_failures[index],
            );
            values[position] = match value {
                Ok(value) => value,
                Err(error) => return self.native_failure(error),
            };
        }
        if index == 0 {
            let SqlDemand::WindowArithmetic { next, left, .. } = &mut self.slots[id.slot]
                .task
                .as_mut()
                .expect("validated window task")
                .demand
            else {
                unreachable!()
            };
            *left = values;
            // Keep this conservative charge until the task completes; nested
            // row callbacks must not rescan an ancestor's complete vector.

            *next = 1;
            return Ok(HostStep::NeedArg(HostArgRequest {
                index: 1,
                mode: ArgMode::Fresh,
            }));
        }
        self.finish_arithmetic_window(id, values)
            .map(HostStep::Ready)
    }
    fn finish_arithmetic_window(
        &mut self,
        id: &HostTaskId,
        mut right: Vec<Datum>,
    ) -> LocalResult<VectorValue> {
        let task = self.task(id)?;
        let slot = task.slot;
        let start = task.row.occurrence;
        let Some(NativeHostMetadata::Arithmetic {
            operation,
            batch: Some(batch),
            name,
            result_type,
            source_types,
            overflow_expressions,
            overflow_format_failure,
            output_node,
            output_role,
            ..
        }) = self.metadata.get(slot.index())
        else {
            return Err(binding("window result descriptor missing"));
        };
        let result_type = result_type
            .as_ref()
            .ok_or_else(|| binding("window result declaration missing"))?;
        let op = match operation {
            SqlBatchHostOperation::AddWindowNative => tidb_ast::BinaryOp::Plus,
            SqlBatchHostOperation::SubtractWindowNative => tidb_ast::BinaryOp::Minus,
            SqlBatchHostOperation::MultiplyWindowNative => tidb_ast::BinaryOp::Mul,
            SqlBatchHostOperation::ModuloWindowNative => tidb_ast::BinaryOp::Mod,
            SqlBatchHostOperation::DivideWindowNative => tidb_ast::BinaryOp::Div,
            SqlBatchHostOperation::IntegerDivideWindowNative => tidb_ast::BinaryOp::IntDiv,
            _ => return Err(binding("window result operation differs")),
        };

        let count = right.len();

        let mut output =
            ChunkedVecBytes::try_with_capacities(count, count * native_values::HANDLE_BYTES)
                .map_err(|error| LocalError::ResourceLimit(error.to_string()))?;
        let store = self
            .native_values
            .ok_or_else(|| binding("window result store missing"))?;
        let staged_integer = batch.output_integer && !batch.integer;

        for position in 0..count {
            let SqlDemand::WindowArithmetic { left, rows, .. } = &mut self.slots[id.slot]
                .task
                .as_mut()
                .expect("validated window task")
                .demand
            else {
                unreachable!()
            };
            let row = InputRow {
                occurrence: start + position,
                input_row: rows[position],
            };
            let lhs = std::mem::replace(&mut left[position], Datum::Null);
            let rhs = std::mem::replace(&mut right[position], Datum::Null);
            let unsigned = arithmetic_unsigned(source_types);
            let available = usize::MAX;
            let computed = if quotient::is_quotient(op) {
                self.compute_quotient(&self.metadata[slot.index()], lhs, rhs, available)?
            } else {
                let prepared = if batch.integer {
                    match crate::ops::prepare_native_typed_arithmetic(
                        op,
                        lhs,
                        rhs,
                        unsigned,
                        self.context,
                    ) {
                        Ok(Some(prepared)) => prepared,
                        Ok(None) => {
                            return Err(binding("integer window lost its strict typed values"))
                        }
                        Err(error) => return self.native_failure(error),
                    }
                } else {
                    let (lhs, rhs) = crate::binary_literal::cast_signed_literal_operands(
                        op,
                        lhs,
                        rhs,
                        unsigned.map(|value| !value),
                    );
                    let _ = self.context.div_precision_increment();
                    match crate::ops::arithmetic::prepare(op, lhs, rhs, unsigned, self.context) {
                        Ok(prepared) => prepared,
                        Err(error) => return self.native_failure(error),
                    }
                };
                prepared.compute(available)
            };
            let value = match computed {
                Ok(value) => value,
                Err(error) => {
                    let (class, text) = match error {
                        EvalError::IntOverflow => (
                            if result_type.is_unsigned() {
                                "BIGINT UNSIGNED"
                            } else {
                                "BIGINT"
                            },
                            overflow_expressions[0].as_deref(),
                        ),
                        EvalError::FloatOverflow => ("DOUBLE", overflow_expressions[1].as_deref()),
                        EvalError::DecimalOverflow => {
                            ("DECIMAL", overflow_expressions[0].as_deref())
                        }
                        other => return self.native_failure(other),
                    };
                    if let Some(bits) = overflow_format_failure {
                        let _ = crate::scalar_function::render_numeric_format_failure(*bits);
                    }
                    return self.native_failure(match text {
                        Some(text) => EvalError::DataOutOfRange {
                            value: class,
                            expression: text.to_owned(),
                        },
                        None => error,
                    });
                }
            };
            let value = if batch.integer {
                value
            } else {
                match crate::scalar_function::coerce_function_result(name, Some(result_type), value)
                {
                    Ok(value) => value,
                    Err(error) => return self.native_failure(error),
                }
            };
            // The generic integer consumer completes all arithmetic and
            // result coercions before it collects integer carriers.
            if staged_integer {
                right[position] = value;
                continue;
            }
            let value = if batch.output_integer {
                match crate::scalar_function::native_batch_integer_bits(value) {
                    Ok(bits) => crate::scalar_function::native_batch_integer_value(
                        bits,
                        result_type.is_unsigned(),
                    ),
                    Err(error) => return self.native_failure(error),
                }
            } else {
                value
            };

            let token = store.borrow_mut().insert_owned(
                value,
                *output_role,
                NativeValueOrigin {
                    node: *output_node,
                    row,
                },
            )?;
            output.push_ref(Some(&token));
        }
        if staged_integer {
            for (position, value) in right.iter_mut().enumerate() {
                let bits = match crate::scalar_function::native_batch_integer_bits(
                    std::mem::replace(value, Datum::Null),
                ) {
                    Ok(bits) => bits,
                    Err(error) => return self.native_failure(error),
                };
                let value = crate::scalar_function::native_batch_integer_value(
                    bits,
                    result_type.is_unsigned(),
                );

                let SqlDemand::WindowArithmetic { rows, .. } = &self.task(id)?.demand else {
                    unreachable!()
                };
                let token = store.borrow_mut().insert_owned(
                    value,
                    *output_role,
                    NativeValueOrigin {
                        node: *output_node,
                        row: InputRow {
                            occurrence: start + position,
                            input_row: rows[position],
                        },
                    },
                )?;
                output.push_ref(Some(&token));
            }
        }
        self.slots[id.slot].task = None;
        Ok(VectorValue::Bytes(output))
    }
}

#[cfg(test)]
#[path = "batch_arithmetic_window_tests.rs"]
mod tests;
