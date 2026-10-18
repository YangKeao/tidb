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
// See the License for the specific language governing permissions and
// limitations under the License.

//! Value-only quotient preparation and computed SQL outcome policy.

use super::*;
use tidb_query_datatype::codec::mysql::Decimal as SharedDecimal;
use tidb_query_expr::impl_arithmetic::{
    native_decimal_division_value, native_decimal_modulo_value, native_integer_divide_value,
    native_integer_modulo_value, native_real_quotient_value, NativeDecimalDivisionDisposition,
    NativeRealQuotientOperation,
};
use tidb_query_expr::local::LocalError;

pub(crate) enum PreparedQuotient {
    Null,
    Integer { left: Integer, right: Integer },
    Real(f64, f64),
    Decimal(SharedDecimal, SharedDecimal),
}

fn shared(value: Decimal, maximum: usize) -> Result<SharedDecimal, EvalError> {
    value
        .try_to_shared_math(maximum)
        .map_err(crate::tikv::math_decimal_bridge_error)
}

/// The original MOD and '/' domain ladder. Only actual value preparation
/// occurs here; this function has no expression, argument program or callback.
pub(crate) fn prepare(
    op: BinaryOp,
    left: Datum,
    right: Datum,
    unsigned: [bool; 2],
    maximum: usize,
    ctx: &dyn crate::Columns,
) -> Result<PreparedQuotient, EvalError> {
    debug_assert!(matches!(op, BinaryOp::Mod | BinaryOp::Div));
    if left.is_range_sentinel() || right.is_range_sentinel() {
        return Err(EvalError::Unsupported("range sentinel expression operand"));
    }
    let left = unsigned_operand_flag(left, unsigned[0]);
    let right = unsigned_operand_flag(right, unsigned[1]);
    if matches!(left, Datum::Json(_)) || matches!(right, Datum::Json(_)) {
        return Err(EvalError::Unsupported("JSON operand"));
    }
    if matches!(left, Datum::Raw(_) | Datum::VectorFloat32(_))
        || matches!(right, Datum::Raw(_) | Datum::VectorFloat32(_))
    {
        return Err(EvalError::UnsupportedOperandPair(left.kind(), right.kind()));
    }
    if matches!(left, Datum::Time(_) | Datum::Duration(_))
        || matches!(right, Datum::Time(_) | Datum::Duration(_))
    {
        return prepare(
            op,
            numeric_context_value(left),
            numeric_context_value(right),
            unsigned,
            maximum,
            ctx,
        );
    }
    if matches!(left, Datum::String(_) | Datum::Bytes(_))
        || matches!(right, Datum::String(_) | Datum::Bytes(_))
    {
        return prepare(
            op,
            cast_binary_string_operand(op, left, ctx)?,
            cast_binary_string_operand(op, right, ctx)?,
            unsigned,
            maximum,
            ctx,
        );
    }
    if matches!(left, Datum::Real(_) | Datum::Float32(_))
        || matches!(right, Datum::Real(_) | Datum::Float32(_))
    {
        if left.is_null() || right.is_null() {
            return Ok(PreparedQuotient::Null);
        }
        return Ok(PreparedQuotient::Real(to_f64(left), to_f64(right)));
    }
    if op == BinaryOp::Div
        || matches!(left, Datum::Decimal(_))
        || matches!(right, Datum::Decimal(_))
    {
        if left.is_null() || right.is_null() {
            return Ok(PreparedQuotient::Null);
        }
        let left = to_decimal(left);
        let right = to_decimal(right);
        return Ok(PreparedQuotient::Decimal(
            shared(left, maximum)?,
            shared(right, maximum)?,
        ));
    }
    if left.is_null() || right.is_null() {
        return Ok(PreparedQuotient::Null);
    }
    match (integer_of(&left)?, integer_of(&right)?) {
        (Some(left), Some(right)) => Ok(PreparedQuotient::Integer { left, right }),
        _ => Err(EvalError::UnsupportedOperandPair(left.kind(), right.kind())),
    }
}

impl PreparedQuotient {
    pub(crate) fn compute(
        self,
        op: BinaryOp,
        increment: u32,
        maximum: usize,
        ctx: &dyn crate::Columns,
    ) -> Result<Datum, EvalError> {
        if maximum == usize::MAX {
            return Err(crate::tikv::math_decimal_bridge_error(
                tidb_query_datatype::codec::mysql::decimal::NativeDecimalError::Resource(
                    "quotient requires a finite value allowance",
                ),
            ));
        }
        let input_null = matches!(self, Self::Null);
        let value = match self {
            Self::Null => {
                tidb_query_expr::impl_arithmetic::binary_arithmetic_null_native_value(None)
                    .map(|value| value.map_or(Datum::Null, Datum::Int))
                    .map_err(|error| {
                        super::arithmetic::kernel_error(LocalError::Evaluation(error))
                    })?
            }
            Self::Integer { left, right } => return integer(op, left, right, ctx),
            Self::Real(left, right) => native_real_quotient_value(
                left,
                right,
                match op {
                    BinaryOp::Div => NativeRealQuotientOperation::Divide,
                    BinaryOp::Mod => NativeRealQuotientOperation::Modulo,
                    _ => unreachable!("prepared real quotient operation"),
                },
            )
            .map(|value| value.map_or(Datum::Null, Datum::Real))
            .map_err(|error| super::arithmetic::kernel_error(LocalError::Evaluation(error)))?,
            Self::Decimal(left, right) if op == BinaryOp::Div => {
                let (value, disposition) = native_decimal_division_value(
                    &left,
                    &right,
                    effective_div_precision_increment(increment),
                    maximum,
                )
                .map_err(|error| super::arithmetic::kernel_error(LocalError::Evaluation(error)))?;
                match (value, disposition) {
                    (Some(value), NativeDecimalDivisionDisposition::Ok) => {
                        Datum::Decimal(crate::tikv::restore_math_decimal(&value, maximum)?)
                    }
                    (Some(value), NativeDecimalDivisionDisposition::Truncated) => {
                        let value = crate::tikv::restore_math_decimal(&value, maximum)?;
                        ctx.handle_truncate(&format!(
                            "Truncated incorrect DECIMAL value: '{value}'"
                        ))?;
                        Datum::Decimal(value)
                    }
                    (Some(_), NativeDecimalDivisionDisposition::Overflow) => {
                        return Err(EvalError::DecimalOverflow)
                    }
                    (None, NativeDecimalDivisionDisposition::ZeroDivisor) => Datum::Null,
                    _ => unreachable!("actual Decimal division outcome is internally consistent"),
                }
            }
            Self::Decimal(left, right) => native_decimal_modulo_value(&left, &right, maximum)
                .map_err(|error| super::arithmetic::kernel_error(LocalError::Evaluation(error)))?
                .map_or(Ok(Datum::Null), |value| {
                    crate::tikv::restore_math_decimal(&value, maximum).map(Datum::Decimal)
                })?,
        };
        finish_arithmetic_result(op, value, input_null, ctx)
    }
}

pub(crate) fn integer(
    op: BinaryOp,
    left: Integer,
    right: Integer,
    ctx: &dyn crate::Columns,
) -> Result<Datum, EvalError> {
    let lu = matches!(left, Integer::Unsigned(_));
    let ru = matches!(right, Integer::Unsigned(_));
    let left = integer_bits(left) as i64;
    let right = integer_bits(right) as i64;
    let (result, unsigned) = match op {
        BinaryOp::Mod => (native_integer_modulo_value(left, right, lu, ru), lu),
        BinaryOp::IntDiv => (native_integer_divide_value(left, right, lu, ru), lu || ru),
        _ => unreachable!("integer quotient operation"),
    };
    let value = result
        .map_err(|error| super::arithmetic::kernel_error(LocalError::Evaluation(error)))?
        .map_or(Datum::Null, |bits| {
            if unsigned {
                Datum::UInt(bits as u64)
            } else {
                Datum::Int(bits)
            }
        });
    finish_arithmetic_result(op, value, false, ctx)
}

fn decimal_view(value: &Decimal) -> tidb_query_expr::NativeIdentityRef<'_> {
    tidb_query_expr::NativeIdentityRef::Decimal {
        negative: value.is_negative(),
        scale: value.scale(),
        storage_scale: value.storage_scale(),
        declared_shape: value.declared_shape(),
        coefficient: value.coefficient_bytes(),
    }
}

pub(crate) fn decimal_integer(
    left: &Decimal,
    right: &Decimal,
    unsigned: bool,
    maximum: usize,
    ctx: &dyn crate::Columns,
) -> Result<Datum, EvalError> {
    let left = decimal_view(left);
    let right = decimal_view(right);
    let probe = tidb_query_expr::native_intdiv_needs_probe(left, right)
        .expect("actual Decimal identities")
        .then(|| i64::from(ctx.div_precision_increment()));
    let fallback = tidb_query_expr::native_intdiv_needs_fallback(left, right, probe)
        .expect("actual demanded precision")
        .then(|| i64::from(ctx.div_precision_increment()));
    let result = tidb_query_expr::native_decimal_integer_division_value(
        left, right, probe, fallback, unsigned, maximum,
    )
    .map_err(|error| super::arithmetic::kernel_error(LocalError::Evaluation(error)))?;
    if let Some(warning) = result.warning {
        ctx.handle_truncate(&warning)?;
    }
    match result.outcome {
        tidb_query_expr::NativeIntDivOutcome::ZeroDivisor => {
            ctx.handle_division_by_zero()?;
            Ok(Datum::Null)
        }
        tidb_query_expr::NativeIntDivOutcome::Value(bits) => Ok(if unsigned {
            Datum::UInt(bits as u64)
        } else {
            Datum::Int(bits)
        }),
        tidb_query_expr::NativeIntDivOutcome::IntOverflow => Err(EvalError::IntOverflow),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::{Cell, RefCell};

    struct Context {
        strict: bool,
        increments: [u32; 2],
        reads: Cell<usize>,
        observations: RefCell<Vec<String>>,
    }
    impl Context {
        fn new(strict: bool, increments: [u32; 2]) -> Self {
            Self {
                strict,
                increments,
                reads: Cell::new(0),
                observations: RefCell::default(),
            }
        }
    }
    impl crate::Columns for Context {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }
        fn div_precision_increment(&self) -> u32 {
            let read = self.reads.get();
            self.reads.set(read + 1);
            self.observations
                .borrow_mut()
                .push(format!("precision:{read}"));
            self.increments[read.min(1)]
        }
        fn handle_truncate(&self, message: &str) -> Result<(), EvalError> {
            self.observations
                .borrow_mut()
                .push(format!("truncate:{message}"));
            if self.strict {
                Err(EvalError::Unsupported("quotient strict truncation"))
            } else {
                Ok(())
            }
        }
        fn handle_division_by_zero(&self) -> Result<(), EvalError> {
            self.observations.borrow_mut().push("zero divisor".into());
            if self.strict {
                Err(EvalError::Unsupported("quotient strict division by zero"))
            } else {
                Ok(())
            }
        }
    }
    fn witness(hash: &mut u64, result: &Result<Datum, EvalError>, observations: &[String]) {
        // Stable FNV-1a over complete Debug results and actual context traces.
        // Constants were captured from independent still-active old adapters,
        // with every pair compared (including f64 bits/Decimal shape), before
        // retirement. See n6p-quotient-original-witness-fixed.log and receipt.
        // Do not regenerate expectations from this implementation.
        for byte in format!("{result:?}|{observations:?}\n").bytes() {
            *hash = (*hash ^ u64::from(byte)).wrapping_mul(0x100000001b3);
        }
    }
    #[test]
    fn plain_modulo_and_division_match_original_adapter_witness() {
        let mut hash = 0xcbf29ce484222325;
        let fixtures = super::super::arithmetic::tests::fixtures();
        let mut count = 0;
        for strict in [false, true] {
            for unsigned in [[false, false], [false, true], [true, false], [true, true]] {
                for op in [BinaryOp::Mod, BinaryOp::Div] {
                    for increment in [0, 4, 31] {
                        for left in &fixtures {
                            for right in &fixtures {
                                let context = Context::new(strict, [increment; 2]);
                                let actual = prepare(
                                    op,
                                    left.clone(),
                                    right.clone(),
                                    unsigned,
                                    64 << 20,
                                    &context,
                                )
                                .and_then(|value| value.compute(op, increment, 64 << 20, &context));
                                witness(&mut hash, &actual, &context.observations.borrow());
                                count += 1;
                            }
                        }
                    }
                }
            }
        }
        assert_eq!(count, 48 * fixtures.len() * fixtures.len());
        assert_eq!(
            hash, 0x3171bf8f5862af8f,
            "original MOD/div values and context observations"
        );
    }

    #[test]
    fn plain_integer_division_matches_original_adapter_witness() {
        let mut hash = 0xcbf29ce484222325;
        for strict in [false, true] {
            for lu in [false, true] {
                for ru in [false, true] {
                    for left in [0, 1, -1, 2, -13, i64::MIN, i64::MAX] {
                        for right in [0, 1, -1, 2, -13, i64::MIN, i64::MAX] {
                            let number = |value, unsigned| {
                                if unsigned {
                                    Integer::Unsigned(value as u64)
                                } else {
                                    Integer::Signed(value)
                                }
                            };
                            for op in [BinaryOp::Mod, BinaryOp::IntDiv] {
                                let context = Context::new(strict, [4; 2]);
                                let actual =
                                    integer(op, number(left, lu), number(right, ru), &context);
                                witness(&mut hash, &actual, &context.observations.borrow());
                            }
                        }
                    }
                }
            }
        }
        assert_eq!(
            hash, 0x481ede75774e35f9,
            "original integer quotient values and context observations"
        );
    }

    #[test]
    fn plain_decimal_integer_division_matches_original_adapter_witness() {
        let mut hash = 0xcbf29ce484222325;
        let values: Vec<_> = ["0", "1", "-1", "-7", "2", "3", "1.0000", "3.0000", "18446744073709551615", "9223372036854775808", "9999999999999999999999999999999999999999999999999999999999999999999999999999999999.0000"]
            .into_iter().map(Decimal::from_literal).collect();
        for strict in [false, true] {
            for unsigned in [false, true] {
                for increments in [[0, 4], [31, 0], [4, 100]] {
                    for left in &values {
                        for right in &values {
                            let context = Context::new(strict, increments);
                            let actual = decimal_integer(left, right, unsigned, 64 << 20, &context);
                            witness(&mut hash, &actual, &context.observations.borrow());
                        }
                    }
                }
            }
        }
        assert_eq!(
            hash, 0x4af311db65938339,
            "original Decimal DIV values and precision/warning observations"
        );
    }
    #[test]
    fn quotient_value_limits_preserve_terminal_failures_null_and_unperformed_work() {
        use crate::tikv::ExpressionRuntimeFailureClass;
        let resource = |result: Result<Datum, EvalError>| {
            let Err(EvalError::ExpressionRuntimeFailure(error)) = result else {
                panic!("expected terminal resource failure: {result:?}")
            };
            // NativeDecimalError is the retained cause inside the original SDK
            // Evaluation discriminant. The private boundary tests verify it.
            assert_eq!(error.class(), ExpressionRuntimeFailureClass::Evaluation);
        };
        for op in [BinaryOp::Mod, BinaryOp::Div] {
            let context = Context::new(true, [4; 2]);
            resource(
                prepare(
                    op,
                    Datum::Decimal(Decimal::from_literal("7.0000")),
                    Datum::Int(3),
                    [false; 2],
                    0,
                    &context,
                )
                .and_then(|value| value.compute(op, 4, 0, &context)),
            );
            resource(
                prepare(
                    op,
                    Datum::Real(7.0),
                    Datum::Real(3.0),
                    [false; 2],
                    4096,
                    &context,
                )
                .unwrap()
                .compute(op, 4, usize::MAX, &context),
            );
            assert!(context.observations.borrow().is_empty());
            assert_eq!(
                prepare(
                    op,
                    Datum::Null,
                    Datum::Real(f64::INFINITY),
                    [false; 2],
                    0,
                    &context
                )
                .unwrap()
                .compute(op, 4, 0, &context),
                Ok(Datum::Null)
            );
            assert!(context.observations.borrow().is_empty());
            assert_eq!(
                integer(
                    BinaryOp::Mod,
                    Integer::Signed(10),
                    Integer::Signed(3),
                    &context
                ),
                Ok(Datum::Int(1))
            );
        }
        let context = Context::new(false, [4; 2]);
        resource(decimal_integer(
            &Decimal::from_literal("1.0000"),
            &Decimal::from_literal("3.0000"),
            false,
            0,
            &context,
        ));
        assert_eq!(*context.observations.borrow(), ["precision:0"]);
        let context = Context::new(false, [4; 2]);
        let invalid = Decimal::from_raw_parts(true, vec![255], u32::MAX, u32::MAX);
        let zero = Decimal::from_raw_parts(true, vec![], 0, 0);
        assert_eq!(
            decimal_integer(&invalid, &zero, false, 0, &context),
            Ok(Datum::Null)
        );
        assert_eq!(*context.observations.borrow(), ["zero divisor"]);
        let context = Context::new(false, [4; 2]);
        // The original fast coefficient policy does not format visible padding
        // or allocate a report frame. Charge neither in this borrowed API.
        let wide = Decimal::from_raw_parts(false, vec![b'1'], 1_000_000, 0);
        assert_eq!(
            decimal_integer(&wide, &Decimal::from_int(2), false, 0, &context),
            Ok(Datum::Int(0))
        );
        assert_eq!(*context.observations.borrow(), ["precision:0"]);
    }
}
