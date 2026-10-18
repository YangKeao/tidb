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

//! Native quotient values after the driver's original row/window demands.
//! Only detached declarations, text and fetched values reach this module.
use super::*;
use crate::scalar_function::NativeArithmeticInputMode as Mode;
use tidb_ast::BinaryOp;

pub(super) fn is_quotient(op: BinaryOp) -> bool {
    matches!(op, BinaryOp::Mod | BinaryOp::Div | BinaryOp::IntDiv)
}
impl NativeBatchHosts<'_> {
    pub(super) fn compute_quotient(
        &self,
        descriptor: &NativeHostMetadata,
        left: Datum,
        right: Datum,
        available: usize,
    ) -> LocalResult<std::result::Result<Datum, EvalError>> {
        let NativeHostMetadata::Arithmetic {
            operation,
            source_types,
            generic_mode,
            subjects,
            subject_format_failures,
            intdiv,
            batch,
            ..
        } = descriptor
        else {
            return Err(binding("quotient lost its declaration"));
        };
        let op = match operation {
            SqlBatchHostOperation::ModuloNative | SqlBatchHostOperation::ModuloWindowNative => {
                BinaryOp::Mod
            }
            SqlBatchHostOperation::DivideNative | SqlBatchHostOperation::DivideWindowNative => {
                BinaryOp::Div
            }
            SqlBatchHostOperation::IntegerDivideNative
            | SqlBatchHostOperation::IntegerDivideWindowNative => BinaryOp::IntDiv,
            _ => return Err(binding("quotient operation differs")),
        };
        let unsigned = source_types.each_ref().map(FieldType::is_unsigned);
        let (left, right) = crate::binary_literal::cast_signed_literal_operands(
            op,
            left,
            right,
            unsigned.map(|value| !value),
        );
        if batch.as_ref().is_some_and(|batch| batch.integer) {
            return Ok(
                match crate::ops::prepare_typed_integer_values(
                    &left,
                    &right,
                    unsigned.map(|value| !value),
                ) {
                    Ok(crate::ops::TypedIntegerValues::Null) => {
                        crate::ops::eval_binary_arithmetic_null_in(self.context)
                    }
                    Ok(crate::ops::TypedIntegerValues::Pair(a, b)) => {
                        crate::ops::quotient::integer(op, a, b, self.context).map_err(|error| {
                            if op == BinaryOp::IntDiv && matches!(error, EvalError::IntOverflow) {
                                crate::scalar_function::integer_division_overflow(a, b)
                            } else {
                                error
                            }
                        })
                    }
                    Ok(crate::ops::TypedIntegerValues::Outside) => {
                        return Err(binding("integer quotient window lost its typed values"))
                    }
                    Err(error) => Err(error),
                },
            );
        }
        // This early Decimal DIV route precedes the generic precision read.
        if op == BinaryOp::IntDiv {
            if let (Datum::Decimal(a), Datum::Decimal(b)) = (&left, &right) {
                let result = crate::ops::quotient::decimal_integer(
                    a,
                    b,
                    unsigned[0] || unsigned[1],
                    available,
                    self.context,
                );
                if matches!(result, Err(EvalError::IntOverflow)) {
                    let error = if batch
                        .as_ref()
                        .is_some_and(|batch| batch.domain == NativeEvalType::Decimal)
                    {
                        // The original vector owner reports actual converted
                        // decimals. Formatting is charged only on this error.
                        let bound = [a, b].into_iter().try_fold(1024usize, |bytes, value| {
                            value
                                .coefficient_bytes()
                                .len()
                                .checked_add(value.scale().max(value.storage_scale()) as usize)
                                .and_then(|n| n.checked_add(4))
                                .and_then(|n| n.checked_mul(4))
                                .and_then(|n| bytes.checked_add(n))
                                .ok_or_else(|| resource("DIV value diagnostic extent overflow"))
                        })?;
                        if bound > available {
                            return Err(resource("DIV value diagnostic exceeds allowance"));
                        }
                        EvalError::DataOutOfRange {
                            value: if unsigned[0] || unsigned[1] {
                                "BIGINT UNSIGNED"
                            } else {
                                "BIGINT"
                            },
                            expression: format!("({a} DIV {b})"),
                        }
                    } else {
                        let diagnostic = intdiv
                            .as_deref()
                            .ok_or_else(|| binding("row Decimal DIV lacks its diagnostic facts"))?;
                        let bound = diagnostic.arguments.iter().flatten().try_fold(
                            1024usize,
                            |bytes, text| {
                                text.len()
                                    .checked_mul(4)
                                    .and_then(|n| bytes.checked_add(n))
                                    .ok_or_else(|| {
                                        resource("DIV source diagnostic extent overflow")
                                    })
                            },
                        )?;
                        if bound > available {
                            return Err(resource("DIV source diagnostic exceeds allowance"));
                        }
                        diagnostic.decimal_overflow(
                            source_types,
                            [a, b],
                            unsigned[0] || unsigned[1],
                        )
                    };
                    return Ok(Err(error));
                }
                return Ok(result);
            }
        }
        let increment = self.context.div_precision_increment();
        if matches!(op, BinaryOp::Mod | BinaryOp::Div) {
            return Ok(crate::ops::quotient::prepare(
                op,
                left,
                right,
                unsigned,
                available,
                self.context,
            )
            .and_then(|value| value.compute(op, increment, available, self.context)));
        }
        let prepared = crate::ops::quotient::prepare_integer_division(
            left,
            right,
            unsigned,
            self.context,
            &mut |index, value| {
                crate::ops::decimal_div_operand_value(
                    value,
                    Some(&source_types[index]),
                    self.context,
                    || {
                        subject_format_failures[index]
                            .map(crate::scalar_function::render_numeric_format_failure)
                            .or_else(|| subjects[index].as_deref().map(str::to_owned))
                    },
                )
            },
        );
        let prepared = match prepared {
            Ok(value) => value,
            Err(error) => return Ok(Err(error)),
        };
        let result = prepared.compute(available, self.context);
        if *generic_mode == Mode::Numeric(NativeEvalType::Int)
            && matches!(result, Err(EvalError::IntOverflow))
        {
            if let crate::ops::quotient::PreparedIntegerDivision::Integer { left, right } = prepared
            {
                return Ok(Err(crate::scalar_function::integer_division_overflow(
                    left, right,
                )));
            }
        }
        Ok(result)
    }

    pub(super) fn finish_arithmetic_row_value(
        &mut self,
        id: &HostTaskId,
        result: std::result::Result<Datum, EvalError>,
        base: usize,
    ) -> LocalResult<HostStep> {
        let Some(NativeHostMetadata::Arithmetic {
            name,
            result_type,
            overflow_expressions,
            overflow_format_failure,
            output_node,
            output_role,
            ..
        }) = self.metadata.get(self.task(id)?.slot.index())
        else {
            return Err(binding("arithmetic publication lost its declaration"));
        };
        let value = match result {
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
                    EvalError::DecimalOverflow => ("DECIMAL", overflow_expressions[0].as_deref()),
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
        let value =
            match crate::scalar_function::coerce_function_result(name, Some(result_type), value) {
                Ok(value) => value,
                Err(error) => return self.native_failure(error),
            };
        let row = self.task(id)?.row;
        self.finish_native_value(id, value, *output_node, *output_role, row, base)
            .map(HostStep::Ready)
    }
}
