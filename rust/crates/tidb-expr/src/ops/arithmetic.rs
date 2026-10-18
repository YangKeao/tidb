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

//! Add/subtract/multiply value preparation, independent of expression owners.

use super::*;
use tidb_query_datatype::codec::mysql::Decimal as SharedDecimal;
use tidb_query_expr::local::LocalError;
use tidb_query_expr::{
    BinaryArithmeticErrorKind, BinaryArithmeticOperation as Operation, NativeBinaryArithmeticError,
};

pub(crate) enum PreparedArithmetic {
    Null,
    Integer {
        operation: Operation,
        left: Integer,
        right: Integer,
        force_signed: bool,
    },
    Real(Operation, f64, f64),
    Decimal(Operation, SharedDecimal, SharedDecimal),
    Vector(
        Operation,
        tidb_datatype::VectorFloat32,
        tidb_datatype::VectorFloat32,
    ),
}

fn operation(op: BinaryOp) -> Operation {
    match op {
        BinaryOp::Plus => Operation::Add,
        BinaryOp::Minus => Operation::Subtract,
        BinaryOp::Mul => Operation::Multiply,
        _ => unreachable!("only additive arithmetic prepares here"),
    }
}

pub(crate) fn prepare_integer(
    op: BinaryOp,
    left: Integer,
    right: Integer,
    ctx: &dyn crate::Columns,
) -> PreparedArithmetic {
    let force_signed = op == BinaryOp::Minus && ctx.no_unsigned_subtraction();
    PreparedArithmetic::Integer {
        operation: operation(op),
        left,
        right,
        force_signed,
    }
}

pub(crate) fn prepare(
    op: BinaryOp,
    left: Datum,
    right: Datum,
    unsigned: [bool; 2],
    ctx: &dyn crate::Columns,
) -> Result<PreparedArithmetic, EvalError> {
    if left.is_range_sentinel() || right.is_range_sentinel() {
        return Err(EvalError::Unsupported("range sentinel expression operand"));
    }
    let left = unsigned_operand_flag(left, unsigned[0]);
    let right = unsigned_operand_flag(right, unsigned[1]);
    if matches!(left, Datum::VectorFloat32(_)) || matches!(right, Datum::VectorFloat32(_)) {
        if left.is_null() || right.is_null() {
            return Ok(PreparedArithmetic::Null);
        }
        return Ok(PreparedArithmetic::Vector(
            operation(op),
            binary_vector_operand(left)?,
            binary_vector_operand(right)?,
        ));
    }
    if matches!(left, Datum::Json(_)) || matches!(right, Datum::Json(_)) {
        return Err(EvalError::Unsupported("JSON operand"));
    }
    if matches!(left, Datum::Raw(_)) || matches!(right, Datum::Raw(_)) {
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
            ctx,
        );
    }
    if matches!(left, Datum::Real(_) | Datum::Float32(_))
        || matches!(right, Datum::Real(_) | Datum::Float32(_))
    {
        if left.is_null() || right.is_null() {
            return Ok(PreparedArithmetic::Null);
        }
        return Ok(PreparedArithmetic::Real(
            operation(op),
            to_f64(left),
            to_f64(right),
        ));
    }
    if matches!(left, Datum::Decimal(_)) || matches!(right, Datum::Decimal(_)) {
        if left.is_null() || right.is_null() {
            return Ok(PreparedArithmetic::Null);
        }
        let left = to_decimal(left);
        let right = to_decimal(right);
        return Ok(PreparedArithmetic::Decimal(
            operation(op),
            crate::tikv::prepare_math_decimal(&left)?,
            crate::tikv::prepare_math_decimal(&right)?,
        ));
    }
    if left.is_null() || right.is_null() {
        return Ok(PreparedArithmetic::Null);
    }
    let (a, b) = match (integer_of(&left)?, integer_of(&right)?) {
        (Some(a), Some(b)) => (a, b),
        _ => return Err(EvalError::UnsupportedOperandPair(left.kind(), right.kind())),
    };
    Ok(prepare_integer(op, a, b, ctx))
}

fn runtime_error(error: LocalError) -> EvalError {
    EvalError::ExpressionRuntimeFailure(crate::tikv::ExpressionRuntimeFailure::from_local_eval(
        error, None,
    ))
}
fn resource(message: &'static str) -> EvalError {
    runtime_error(LocalError::ResourceLimit(message.into()))
}
pub(super) fn kernel_error(error: LocalError) -> EvalError {
    // Follow the actual retained cause from this direct kernel call. This is
    // not a sealed EvaluatedArgs receipt and does not manufacture one.
    if let LocalError::Evaluation(evaluation) = &error {
        let mut cause: &(dyn std::error::Error + 'static) = evaluation;
        loop {
            if let Some(cause) = cause.downcast_ref::<NativeBinaryArithmeticError>() {
                return match cause.kind {
                    BinaryArithmeticErrorKind::IntOverflow => EvalError::IntOverflow,
                    BinaryArithmeticErrorKind::FloatOverflow => EvalError::FloatOverflow,
                    BinaryArithmeticErrorKind::DecimalOverflow => EvalError::DecimalOverflow,
                };
            }
            let Some(source) = cause.source() else {
                break;
            };
            cause = source;
        }
    }
    runtime_error(error)
}

impl PreparedArithmetic {
    /// Executes the same value helpers as the registered native kernels.
    pub(crate) fn compute(self, available: usize) -> Result<Datum, EvalError> {
        if available == usize::MAX {
            return Err(resource("arithmetic requires a finite value allowance"));
        }
        use tidb_query_expr::impl_arithmetic::{
            binary_arithmetic_null_native_value, native_decimal_binary_value,
            native_integer_binary_value, native_real_binary_value,
        };
        match self {
            Self::Null => binary_arithmetic_null_native_value(None)
                .map(|value| value.map_or(Datum::Null, Datum::Int))
                .map_err(|error| kernel_error(LocalError::Evaluation(error))),
            Self::Integer {
                operation,
                left,
                right,
                force_signed,
            } => {
                let unsigned = (matches!(left, Integer::Unsigned(_))
                    || matches!(right, Integer::Unsigned(_)))
                    && !force_signed;
                native_integer_binary_value(
                    integer_bits(left) as i64,
                    integer_bits(right) as i64,
                    operation,
                    matches!(left, Integer::Unsigned(_)),
                    matches!(right, Integer::Unsigned(_)),
                    force_signed,
                )
                .map(|value| {
                    value.map_or(Datum::Null, |value| {
                        if unsigned {
                            Datum::UInt(value as u64)
                        } else {
                            Datum::Int(value)
                        }
                    })
                })
                .map_err(|error| kernel_error(LocalError::Evaluation(error)))
            }
            Self::Real(op, left, right) => native_real_binary_value(Some(left), Some(right), op)
                .map(|value| value.map_or(Datum::Null, Datum::Real))
                .map_err(|error| kernel_error(LocalError::Evaluation(error))),
            Self::Decimal(op, left, right) => {
                let budget = u64::try_from(available)
                    .map_err(|_| resource("arithmetic decimal allowance overflow"))?
                    as i64;
                native_decimal_binary_value(&left, &right, &budget, op)
                    .map_err(|error| kernel_error(LocalError::Evaluation(error)))?
                    .map_or(Ok(Datum::Null), |value| {
                        crate::tikv::restore_math_decimal(&value, available).map(Datum::Decimal)
                    })
            }
            Self::Vector(op, left, right) => {
                // Both input owners and a full output owner can coexist. Check
                // their capacities before the original value method allocates.
                let bytes = left
                    .elements_capacity()
                    .checked_add(right.elements_capacity())
                    .and_then(|cells| cells.checked_add(left.len().max(right.len())))
                    .and_then(|cells| cells.checked_mul(std::mem::size_of::<f32>()))
                    .ok_or_else(|| resource("arithmetic vector allowance overflow"))?;
                if bytes > available {
                    return Err(resource("arithmetic vector exceeds value allowance"));
                }
                use tidb_query_expr::impl_vec::{
                    add_vector_native_value, mul_vector_native_value, sub_vector_native_value,
                };
                let result = match op {
                    Operation::Add => add_vector_native_value(&left, &right),
                    Operation::Subtract => sub_vector_native_value(&left, &right),
                    Operation::Multiply => mul_vector_native_value(&left, &right),
                    _ => unreachable!("prepared additive operation"),
                };
                result
                    .map(Datum::VectorFloat32)
                    .map_err(|cause| EvalError::Vector(cause.to_string()))
            }
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::cell::RefCell;
    use tidb_datatype::{
        BinaryJSON, BinaryLiteral, Collation, FieldType, FieldTypeCode, MySqlDuration, MysqlEnum,
        MysqlSet, VectorFloat32,
    };

    #[derive(Default)]
    struct Context {
        forced: bool,
        level: Option<crate::ErrorLevel>,
        observations: RefCell<Vec<String>>,
    }
    impl crate::Columns for Context {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }
        fn no_unsigned_subtraction(&self) -> bool {
            self.observations
                .borrow_mut()
                .push("no_unsigned_subtraction".into());
            self.forced
        }
        fn truncate_level(&self) -> crate::ErrorLevel {
            self.observations.borrow_mut().push("truncate_level".into());
            self.level.unwrap_or(crate::ErrorLevel::Warn)
        }
        fn append_warning(&self, code: u16, message: &str) {
            self.observations
                .borrow_mut()
                .push(format!("{code}:{message}"));
        }
    }
    pub(crate) fn fixtures() -> Vec<Datum> {
        let time = tidb_datatype::parse_datetime(
            "2026-10-18 12:34:56.123456",
            &chrono_tz::UTC,
            true,
            false,
        )
        .unwrap()
        .time;
        vec![
            Datum::Null,
            Datum::Int(0),
            Datum::Int(-1),
            Datum::Int(i64::MIN),
            Datum::Int(i64::MAX),
            Datum::UInt(0),
            Datum::UInt(1u64 << 63),
            Datum::UInt(u64::MAX),
            Datum::Real(-0.0),
            Datum::Real(1.25),
            Datum::Real(f64::MAX),
            Datum::Real(f64::from_bits(0x7ff8000000000042)),
            Datum::Real(f64::INFINITY),
            Datum::Float32(-0.10000000149011612),
            Datum::Decimal(Decimal::from_literal("-1.2300")),
            Datum::Decimal(Decimal::from_literal(
                "123456789012345678901234567890123456789012345678901234567890.001",
            )),
            Datum::new_string("2.5tail"),
            Datum::Bytes(b"-3.25tail".to_vec()),
            Datum::new_string("bad number"),
            Datum::BinaryLiteral(BinaryLiteral::from(vec![0xff; 8])),
            Datum::Bit(BinaryLiteral::from(vec![0xff; 8])),
            Datum::Enum(MysqlEnum::new("b", 2), Collation::GbkBin),
            Datum::Set(MysqlSet::new("a,b", 3), Collation::GbkBin),
            Datum::Time(time),
            Datum::Duration(MySqlDuration::from_nanoseconds(-123456789000, 6).unwrap()),
            Datum::Json(BinaryJSON::parse("[1,true]").unwrap()),
            Datum::Raw(vec![0xff, 0]),
            Datum::MinNotNull,
            Datum::MaxValue,
            Datum::VectorFloat32(VectorFloat32::must_create(vec![1.0, -0.0])),
            Datum::VectorFloat32(VectorFloat32::must_create(vec![2.0])),
            Datum::VectorFloat32(VectorFloat32::must_create(vec![])),
        ]
    }
    fn additive_witness(
        hash: &mut u64,
        result: &Result<Datum, EvalError>,
        observations: &[String],
    ) {
        for byte in format!("{result:?}|{observations:?}\n").bytes() {
            *hash = (*hash ^ u64::from(byte)).wrapping_mul(0x100000001b3);
        }
    }
    #[test]
    fn plain_additive_callers_match_original_recipe_witness() {
        let mut hash = 0xcbf29ce484222325;
        let fixtures = fixtures();
        let mut cases = 0;
        for level in [
            crate::ErrorLevel::Warn,
            crate::ErrorLevel::Error,
            crate::ErrorLevel::Ignore,
        ] {
            for forced in [false, true] {
                for unsigned in [[false, false], [false, true], [true, false], [true, true]] {
                    for op in [BinaryOp::Plus, BinaryOp::Minus, BinaryOp::Mul] {
                        for left in &fixtures {
                            for right in &fixtures {
                                let reference = Context {
                                    forced,
                                    level: Some(level),
                                    ..Context::default()
                                };
                                let context = Context {
                                    forced,
                                    level: Some(level),
                                    ..Context::default()
                                };
                                let lhs = crate::expression::Expression::Constant(
                                    crate::constant::Constant::new(
                                        left.clone(),
                                        FieldType::new(FieldTypeCode::LongLong)
                                            .with_unsigned(unsigned[0]),
                                    ),
                                );
                                let rhs = crate::expression::Expression::Constant(
                                    crate::constant::Constant::new(
                                        right.clone(),
                                        FieldType::new(FieldTypeCode::LongLong)
                                            .with_unsigned(unsigned[1]),
                                    ),
                                );
                                let expected = eval_binary_full(
                                    op,
                                    left.clone(),
                                    right.clone(),
                                    4,
                                    Collation::Binary,
                                    Operands::of(&lhs, &rhs),
                                    &reference,
                                );
                                additive_witness(
                                    &mut hash,
                                    &expected,
                                    &reference.observations.borrow(),
                                );
                                let actual =
                                    prepare(op, left.clone(), right.clone(), unsigned, &context)
                                        .and_then(|value| value.compute(16 << 20));
                                assert_eq!(
                                    actual, expected,
                                    "{op:?} {unsigned:?} forced={forced} {left:?} {right:?}"
                                );
                                if let (Ok(actual), Ok(expected)) = (&actual, &expected) {
                                    match (actual, expected) {
                                        (Datum::Real(actual), Datum::Real(expected)) => {
                                            assert_eq!(actual.to_bits(), expected.to_bits())
                                        }
                                        (Datum::Decimal(actual), Datum::Decimal(expected)) => {
                                            assert_eq!(
                                                actual.coefficient_bytes(),
                                                expected.coefficient_bytes()
                                            );
                                            assert_eq!(actual.scale(), expected.scale());
                                            assert_eq!(
                                                actual.storage_scale(),
                                                expected.storage_scale()
                                            );
                                        }
                                        (
                                            Datum::VectorFloat32(actual),
                                            Datum::VectorFloat32(expected),
                                        ) => assert_eq!(
                                            actual
                                                .elements()
                                                .iter()
                                                .map(|v| v.to_bits())
                                                .collect::<Vec<_>>(),
                                            expected
                                                .elements()
                                                .iter()
                                                .map(|v| v.to_bits())
                                                .collect::<Vec<_>>()
                                        ),
                                        _ => {}
                                    }
                                }
                                assert_eq!(
                                    *context.observations.borrow(),
                                    *reference.observations.borrow(),
                                    "{op:?} {left:?} {right:?}"
                                );
                                cases += 1;
                            }
                        }
                    }
                }
            }
        }
        assert_eq!(cases, 72 * fixtures.len() * fixtures.len());
        assert_eq!(
            hash, 0x7246d6c6e2263fe1,
            "original additive result/context witness"
        );
    }

    #[test]
    fn typed_integer_additive_values_match_original_recipe_witness() {
        let bits = [0, 1, -1, i64::MIN, i64::MAX, 2, -2];
        let mut hash = 0xcbf29ce484222325;
        let mut cases = 0;
        for forced in [false, true] {
            for lu in [false, true] {
                for ru in [false, true] {
                    for op in [BinaryOp::Plus, BinaryOp::Minus, BinaryOp::Mul] {
                        for left in bits {
                            for right in bits {
                                let a = if lu {
                                    Integer::Unsigned(left as u64)
                                } else {
                                    Integer::Signed(left)
                                };
                                let b = if ru {
                                    Integer::Unsigned(right as u64)
                                } else {
                                    Integer::Signed(right)
                                };
                                let reference = Context {
                                    forced,
                                    ..Context::default()
                                };
                                let context = Context {
                                    forced,
                                    ..Context::default()
                                };
                                let expected = integer_binary(op, a, b, &reference);
                                let actual = prepare_integer(op, a, b, &context).compute(16 << 20);
                                assert_eq!(actual, expected, "{op:?} {a:?} {b:?} forced={forced}");
                                assert_eq!(
                                    *context.observations.borrow(),
                                    *reference.observations.borrow()
                                );
                                additive_witness(
                                    &mut hash,
                                    &expected,
                                    &reference.observations.borrow(),
                                );
                                cases += 1;
                            }
                        }
                    }
                }
            }
        }
        assert_eq!(cases, 1176);
        assert_eq!(
            hash, 0x6e729cf695ab6ca5,
            "original typed additive result/context witness"
        );
    }

    #[test]
    fn additive_value_budgets_preserve_resource_failures_and_actual_null() {
        use crate::tikv::ExpressionRuntimeFailureClass;
        let context = Context::default();
        let vector = || Datum::VectorFloat32(VectorFloat32::must_create(vec![1.0, 2.0]));
        for available in [0, usize::MAX] {
            let result = prepare(BinaryOp::Plus, vector(), vector(), [false; 2], &context)
                .unwrap()
                .compute(available);
            let Err(EvalError::ExpressionRuntimeFailure(error)) = result else {
                panic!("vector budget must be terminal: {result:?}")
            };
            assert_eq!(error.class(), ExpressionRuntimeFailureClass::ResourceLimit);
        }
        // NULL has no invented partner and does not read subtraction mode.
        assert_eq!(
            prepare(BinaryOp::Minus, Datum::Null, vector(), [true; 2], &context)
                .unwrap()
                .compute(0),
            Ok(Datum::Null)
        );
        assert!(context.observations.borrow().is_empty());
    }
}
