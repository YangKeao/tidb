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

//! The `ETInt` half of operator evaluation, split out of `ops.rs` -- the
//! sibling of [`super::real_coerce`].
//!
//! Everything an operand does once MySQL's promotion hierarchy has decided the
//! pair is an INTEGER pair: the signedness rule that decides which of Go's
//! per-signedness signatures applies ([`unsigned_operand`]), the arithmetic and
//! bitwise operators themselves ([`integer_binary`] and the overflow checks it
//! delegates to), the shift width rule, and unary-minus recipe selection
//! ([`unary_minus_integer`]). The shared worker owns integer negation,
//! overflow, and the constant-only promotion to a decimal result.

use super::*;

/// Go's `uval := uint64(val)`: an operand whose FIELD TYPE carries
/// `UnsignedFlag` is read through that flag, whatever `Datum` kind its value
/// came back in. Only the integer kinds can be reinterpreted -- a `DOUBLE
/// UNSIGNED` stays a `Real`, and its signedness travels separately -- so this
/// touches `Datum::Int` alone.
pub(super) fn unsigned_operand(value: Datum, operand: Operand<'_>) -> Datum {
    match value {
        Datum::Int(bits) if operand.is_unsigned() => Datum::UInt(bits as u64),
        other => other,
    }
}

/// Select the signedness/constness recipe without inspecting overflow or
/// computing a negated value. Constant recipes provide their own checked Int
/// view of the computed Decimal; column recipes report the actual typed cause.
pub(super) fn unary_minus_integer(
    bits: u64,
    unsigned: bool,
    arg: Operand<'_>,
    ctx: &dyn crate::context::Columns,
) -> Result<Datum, EvalError> {
    use crate::tikv::{evaluate_args_in, EvaluatedArgs, EvaluatedBytesOp};

    let constant = arg.is_constant();
    let operation = match (unsigned, constant) {
        (false, false) => EvaluatedBytesOp::UnaryMinusIntNative,
        (true, false) => EvaluatedBytesOp::UnaryMinusUIntNative,
        (false, true) => EvaluatedBytesOp::UnaryMinusIntConstantNative,
        (true, true) => EvaluatedBytesOp::UnaryMinusUIntConstantNative,
    };
    evaluate_args_in(
        operation,
        ctx,
        || Ok(EvaluatedArgs::Int(Some(bits as i64))),
        |computed| {
            if constant {
                computed.into_decimal_or_int_datum()
            } else {
                computed.into_int_datum()
            }
        },
    )
}

/// Select a fixed integer recipe from original operand/profile metadata only.
/// Call inside preparation so the subtraction mode is read at its demand point.
pub(super) fn prepare_integer_arithmetic(
    op: BinaryOp,
    a: Integer,
    b: Integer,
    ctx: &dyn crate::context::Columns,
    unsigned_result: &std::cell::Cell<bool>,
) -> (crate::tikv::EvaluatedBytesOp, crate::tikv::EvaluatedArgs) {
    use crate::tikv::{EvaluatedArgs, EvaluatedBytesOp as Op};
    let left_unsigned = matches!(a, Integer::Unsigned(_));
    let right_unsigned = matches!(b, Integer::Unsigned(_));
    let left = integer_bits(a) as i64;
    let right = integer_bits(b) as i64;
    let force_signed = op == BinaryOp::Minus && ctx.no_unsigned_subtraction();
    let operation = match op {
        BinaryOp::Plus => match (left_unsigned, right_unsigned) {
            (false, false) => Op::AddIntSsNative,
            (false, true) => Op::AddIntSuNative,
            (true, false) => Op::AddIntUsNative,
            (true, true) => Op::AddIntUuNative,
        },
        BinaryOp::Minus => match (left_unsigned, right_unsigned, force_signed) {
            (false, false, _) => Op::SubIntSsNative,
            (false, true, false) => Op::SubIntSuNative,
            (true, false, false) => Op::SubIntUsNative,
            (true, true, false) => Op::SubIntUuNative,
            (false, true, true) => Op::SubIntSuForcedNative,
            (true, false, true) => Op::SubIntUsForcedNative,
            (true, true, true) => Op::SubIntUuForcedNative,
        },
        BinaryOp::Mul if left_unsigned || right_unsigned => Op::MulIntUnsignedNative,
        BinaryOp::Mul => Op::MulIntSignedNative,
        BinaryOp::Mod => match (left_unsigned, right_unsigned) {
            (false, false) => Op::ModIntSsNative,
            (false, true) => Op::ModIntSuNative,
            (true, false) => Op::ModIntUsNative,
            (true, true) => Op::ModIntUuNative,
        },
        _ => unreachable!("only worker arithmetic families prepare here"),
    };
    unsigned_result.set(if op == BinaryOp::Mod {
        left_unsigned
    } else {
        (left_unsigned || right_unsigned) && !force_signed
    });
    (operation, EvaluatedArgs::Int2(Some(left), Some(right)))
}

pub(crate) fn integer_binary(
    op: BinaryOp,
    a: Integer,
    b: Integer,
    ctx: &dyn crate::context::Columns,
) -> Result<Datum, EvalError> {
    use BinaryOp::*;
    if matches!(op, Plus | Minus | Mul | Mod) {
        let unsigned_result = std::cell::Cell::new(false);
        return crate::tikv::evaluate_prepared_args_in(
            ctx,
            || Ok(prepare_integer_arithmetic(op, a, b, ctx, &unsigned_result)),
            |computed| {
                let value = if unsigned_result.get() {
                    computed.into_uint_bits_datum()
                } else {
                    computed.into_int_datum()
                }?;
                finish_arithmetic_result(op, value, false, ctx)
            },
        );
    }
    let bits_a = integer_bits(a);
    let bits_b = integer_bits(b);
    Ok(match op {
        Plus | Minus | Mul => unreachable!("worker arithmetic dispatched above"),
        // `DIV`/`MOD` by zero yield NULL in MySQL. `DIV` truncates toward zero.
        IntDiv => {
            if bits_b == 0 {
                ctx.handle_division_by_zero()?;
                Datum::Null
            } else {
                // Go selects a different checked helper for every signedness
                // pair.  In particular, a mixed signed/unsigned quotient is
                // an unsigned result and rejects a negative quotient instead
                // of dividing the raw two's-complement bit patterns.
                let quotient = match (a, b) {
                    (Integer::Unsigned(lhs), Integer::Unsigned(rhs)) => lhs / rhs,
                    (Integer::Unsigned(lhs), Integer::Signed(rhs)) => {
                        div_uint_with_int(lhs, rhs).map_err(|_| EvalError::IntOverflow)?
                    }
                    (Integer::Signed(lhs), Integer::Unsigned(rhs)) => {
                        div_int_with_uint(lhs, rhs).map_err(|_| EvalError::IntOverflow)?
                    }
                    (Integer::Signed(lhs), Integer::Signed(rhs)) => {
                        return div_int64(lhs, rhs)
                            .map(Datum::Int)
                            .map_err(|_| EvalError::IntOverflow);
                    }
                };
                Datum::UInt(quotient)
            }
        }
        Mod => unreachable!("worker arithmetic dispatched above"),
        BitAnd | BitOr | BitXor | LeftShift | RightShift => {
            return eval_bitwise_binary_in(op, Some(bits_a as i64), Some(bits_b as i64), ctx);
        }
        Eq => bool_int(integer_cmp(a, b).is_eq()),
        Ge => bool_int(integer_cmp(a, b).is_ge()),
        Gt => bool_int(integer_cmp(a, b).is_gt()),
        Le => bool_int(integer_cmp(a, b).is_le()),
        Lt => bool_int(integer_cmp(a, b).is_lt()),
        Ne => bool_int(!integer_cmp(a, b).is_eq()),
        Div => unreachable!("handled above"),
        LogicAnd | LogicOr | LogicXor | NullEq => unreachable!("handled above"),
    })
}

#[cfg(test)]
mod source_tests {
    use super::*;
    use crate::column::Column;
    use crate::context::NoColumns;
    use crate::expression::Expression;
    use tidb_datatype::{FieldTypeBuilder, FieldTypeCode, FieldTypeFlags};

    fn integer_column(unsigned: bool) -> Expression {
        let mut field_type = FieldTypeBuilder::new()
            .with_code(FieldTypeCode::LongLong)
            .build();
        if unsigned {
            field_type.add_flags(FieldTypeFlags::UNSIGNED);
        }
        Expression::Column(Column::new(1, field_type))
    }

    struct NoUnsignedSubtraction;

    impl crate::Columns for NoUnsignedSubtraction {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }

        fn no_unsigned_subtraction(&self) -> bool {
            true
        }
    }

    #[test]
    fn no_unsigned_subtraction_forces_the_signed_value_domain() {
        assert_eq!(
            integer_binary(
                tidb_ast::BinaryOp::Minus,
                Integer::Unsigned(0),
                Integer::Signed(1),
                &NoUnsignedSubtraction,
            ),
            Ok(Datum::Int(-1))
        );
        assert_eq!(
            integer_binary(
                tidb_ast::BinaryOp::Minus,
                Integer::Unsigned(0),
                Integer::Signed(1),
                &NoColumns,
            ),
            Err(EvalError::IntOverflow)
        );
    }

    /// Exact scalar-semantic port of Go `TestBuiltinUnaryMinusIntSig` from
    /// `builtin_op_vec_test.go`. Rust has one evaluator rather than separate
    /// row/vector signatures, so the source's six value rows exercise that
    /// sole path: ordinary, overflow, and NULL for both signedness flags.
    #[test]
    fn test_builtin_unary_minus_int_sig() {
        let signed = integer_column(false);
        let signed_operand = Operand::Expr(&signed);
        assert!(!signed.static_type().unwrap().is_unsigned());
        assert_eq!(
            eval_unary(
                UnaryOp::Minus,
                Datum::Int(233_333),
                signed_operand,
                &NoColumns,
            ),
            Ok(Datum::Int(-233_333))
        );
        assert_eq!(
            eval_unary(
                UnaryOp::Minus,
                Datum::Int(i64::MIN),
                signed_operand,
                &NoColumns,
            ),
            // Go `builtin_op.go:1121` renders `GenWithStackByArgs("BIGINT",
            // fmt.Sprintf("-%v", val))` -- the format prefix plus the value's
            // own sign yields the double-minus.
            Err(EvalError::DataOutOfRange {
                value: "BIGINT",
                expression: "--9223372036854775808".to_string(),
            })
        );
        assert_eq!(
            eval_unary(UnaryOp::Minus, Datum::Null, signed_operand, &NoColumns,),
            Ok(Datum::Null)
        );

        let unsigned = integer_column(true);
        let unsigned_operand = Operand::Expr(&unsigned);
        assert!(unsigned.static_type().unwrap().is_unsigned());
        assert_eq!(
            eval_unary(
                UnaryOp::Minus,
                Datum::UInt(233_333),
                unsigned_operand,
                &NoColumns,
            ),
            Ok(Datum::Int(-233_333))
        );
        assert_eq!(
            eval_unary(
                UnaryOp::Minus,
                Datum::UInt((1_u64 << 63) + 1),
                unsigned_operand,
                &NoColumns,
            ),
            // Go `builtin_op.go:1116`: `"-%v"` over the raw uint64.
            Err(EvalError::DataOutOfRange {
                value: "BIGINT",
                expression: "-9223372036854775809".to_string(),
            })
        );
        assert_eq!(
            eval_unary(UnaryOp::Minus, Datum::Null, unsigned_operand, &NoColumns,),
            Ok(Datum::Null)
        );
    }
}
