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
//! delegates to), the shift width rule, and unary minus -- the one operator
//! whose Int signature can be replaced at BUILD time by a decimal one
//! ([`unary_minus_integer`]).

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

/// Go `builtinUnaryMinusIntSig.evalInt`'s two branches
/// (`pkg/expression/builtin_op.go:1106-1124`), plus the ONE promotion
/// `unaryMinusFunctionClass.typeInfer` performs ahead of them:
///
/// ```go
/// if arg, ok := argExpr.(*Constant); ok && tp == types.ETInt {
///     overflow = c.handleIntOverflow(ctx, arg)
///     if overflow { tp = types.ETDecimal }
/// }
/// ```
///
/// A CONSTANT whose negation leaves `BIGINT` is rebuilt on the DECIMAL
/// signature and keeps its magnitude; a COLUMN keeps the Int signature and
/// reports `ErrOverflow`. Captured over `b BIGINT` holding -9223372036854775808
/// and `u BIGINT UNSIGNED` holding 9223372036854775809: both `SELECT -b` and
/// `SELECT -u` are `[types:1690] BIGINT value is out of range`, where this
/// evaluator answered the CLAMPED 9223372036854775807 and -9223372036854775808.
pub(super) fn unary_minus_integer(
    bits: u64,
    unsigned: bool,
    arg: Operand<'_>,
) -> Result<Datum, EvalError> {
    if !unsigned {
        let value = bits as i64;
        if value != i64::MIN {
            return Ok(Datum::Int(-value));
        }
    } else {
        if bits <= i64::MAX as u64 {
            return Ok(Datum::Int(-(bits as i64)));
        }
        // `-2^63` is representable, so Go returns it rather than overflowing.
        if bits == 1_u64 << 63 {
            return Ok(Datum::Int(i64::MIN));
        }
    }
    if !arg.is_constant() {
        // Go `builtinUnaryMinusIntSig.evalInt` (`builtin_op.go:1116,1121`):
        // `types.ErrOverflow.GenWithStackByArgs("BIGINT", fmt.Sprintf("-%v", val))`
        // quotes the NEGATED value rather than the source expression, so the
        // wire text carries `in '-9223372036854775808'`.
        let expression = if unsigned {
            format!("-{bits}")
        } else {
            format!("-{}", i64::MIN)
        };
        return Err(EvalError::DataOutOfRange {
            value: "BIGINT",
            expression,
        });
    }
    Ok(Datum::Decimal(if unsigned {
        Decimal::from_uint(bits).negate()
    } else {
        Decimal::from_uint(1_u64 << 63)
    }))
}

pub(crate) fn integer_binary(
    op: BinaryOp,
    a: Integer,
    b: Integer,
    ctx: &dyn tidb_exprctx::context::Columns,
) -> Result<Datum, EvalError> {
    use BinaryOp::*;
    let lhs_unsigned = matches!(a, Integer::Unsigned(_));
    let rhs_unsigned = matches!(b, Integer::Unsigned(_));
    let unsigned = lhs_unsigned || rhs_unsigned;
    let bits_a = integer_bits(a);
    let bits_b = integer_bits(b);
    Ok(match op {
        Plus => return integer_add(a, b),
        // `-` reports `ErrOverflow` exactly where Go `builtinArithmeticMinusIntSig`
        // does — via [`minus_overflows`], a line-for-line port of Go's
        // `overflowCheck` (verified against goeval across every branch). A
        // non-overflowing result keeps its wrapped two's-complement value, typed
        // by `unsigned`.
        Minus => {
            let force_signed = ctx.no_unsigned_subtraction();
            if minus_overflows(
                lhs_unsigned,
                rhs_unsigned,
                force_signed,
                bits_a as i64,
                bits_b as i64,
            ) {
                return Err(EvalError::IntOverflow);
            }
            integer_result(unsigned && !force_signed, bits_a.wrapping_sub(bits_b))
        }
        Mul => return integer_multiply(bits_a as i64, bits_b as i64, unsigned),
        IntDiv | Mod => return integer_div_mod(op, bits_a as i64, bits_b as i64, [lhs_unsigned, rhs_unsigned], ctx),
        BitAnd | BitOr | BitXor | LeftShift | RightShift => {
            return integer_bitwise(op, bits_a as i64, bits_b as i64);
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

pub(super) fn integer_bitwise(op: BinaryOp, left: i64, right: i64) -> Result<Datum, EvalError> {
    use tidb_query_expr::impl_op::{bit_and, bit_or, bit_xor};
    let result = match op {
        BinaryOp::BitAnd => bit_and(Some(&left), Some(&right)),
        BinaryOp::BitOr => bit_or(Some(&left), Some(&right)),
        BinaryOp::BitXor => bit_xor(Some(&left), Some(&right)),
        BinaryOp::LeftShift => return Ok(Datum::UInt(shift_left(left as u64, right as u64))),
        BinaryOp::RightShift => return Ok(Datum::UInt(shift_right(left as u64, right as u64))),
        _ => unreachable!("only bitwise binary operators use the SDK bitwise kernel"),
    };
    sdk_integer_result(result, true)
}

pub(super) fn integer_bit_neg(value: i64) -> Result<Datum, EvalError> {
    sdk_integer_result(tidb_query_expr::impl_op::bit_neg(Some(&value)), true)
}

fn integer_div_mod(
    op: BinaryOp,
    left: i64,
    right: i64,
    unsigned: [bool; 2],
    ctx: &dyn tidb_exprctx::context::Columns,
) -> Result<Datum, EvalError> {
    use tidb_query_expr::impl_arithmetic::{
        ArithmeticOp, IntDivideInt, IntDivideUint, IntIntMod, IntUintMod, UintDivideInt,
        UintDivideUint, UintIntMod, UintUintMod,
    };
    if right == 0 {
        crate::expression_errors::handle_division_by_zero(ctx)?;
        return Ok(Datum::Null);
    }
    let result = if op == BinaryOp::IntDiv {
        match unsigned {
            [false, false] => IntDivideInt::calc(&left, &right),
            [false, true] => IntDivideUint::calc(&left, &right),
            [true, false] => UintDivideInt::calc(&left, &right),
            [true, true] => UintDivideUint::calc(&left, &right),
        }
    } else {
        match unsigned {
            // Rust remainder overflows for this pair, but Go returns 0.
            [false, false] if left == i64::MIN && right == -1 => Ok(Some(0)),
            [false, false] => IntIntMod::calc(&left, &right),
            [false, true] => IntUintMod::calc(&left, &right),
            [true, false] => UintIntMod::calc(&left, &right),
            [true, true] => UintUintMod::calc(&left, &right),
        }
    };
    let unsigned_result = unsigned[0] || (op == BinaryOp::IntDiv && unsigned[1]);
    sdk_integer_result(result, unsigned_result)
}

pub(super) fn integer_result(unsigned: bool, bits: u64) -> Datum {
    if unsigned {
        Datum::UInt(bits)
    } else {
        Datum::Int(bits as i64)
    }
}

/// Integer `+` with TiDB's overflow rule (`builtinArithmeticPlusIntSig`): the
/// result is `UNSIGNED` when either operand is, and any result past that type's
/// range is `ErrOverflow`, never a silent wrap. Go errors in every signedness
/// case rather than adding the raw two's-complement bits, so each case maps to a
/// checked operation: a mixed sum underflows past `0` when a negative addend
/// exceeds the unsigned operand, or overflows past `u64::MAX`.
pub(super) fn integer_add(a: Integer, b: Integer) -> Result<Datum, EvalError> {
    use tidb_query_expr::impl_arithmetic::{ArithmeticOp, IntIntPlus, IntUintPlus, UintUintPlus};

    let (result, unsigned) = match (a, b) {
        (Integer::Signed(x), Integer::Signed(y)) => (IntIntPlus::calc(&x, &y), false),
        (Integer::Unsigned(x), Integer::Unsigned(y)) => {
            (UintUintPlus::calc(&(x as i64), &(y as i64)), true)
        }
        (Integer::Unsigned(x), Integer::Signed(y)) | (Integer::Signed(y), Integer::Unsigned(x)) => {
            (IntUintPlus::calc(&y, &(x as i64)), true)
        }
    };
    sdk_integer_result(result, unsigned)
}

pub(crate) fn integer_multiply(left: i64, right: i64, unsigned: bool) -> Result<Datum, EvalError> {
    use tidb_query_expr::impl_arithmetic::{ArithmeticOp, IntIntMultiply, UintUintMultiply};

    let result = if unsigned {
        UintUintMultiply::calc(&left, &right)
    } else {
        IntIntMultiply::calc(&left, &right)
    };
    sdk_integer_result(result, unsigned)
}

fn sdk_integer_result(
    result: tidb_query_common::Result<Option<i64>>,
    unsigned: bool,
) -> Result<Datum, EvalError> {
    // The caller formats the overflow using the original SQL operands.
    result
        .map(|value| value.map_or(Datum::Null, |bits| integer_result(unsigned, bits as u64)))
        .map_err(|_| EvalError::IntOverflow)
}

/// A line-for-line port of Go `builtinArithmeticMinusIntSig.overflowCheck`:
/// `true` when `a - b` overflows the result type. `a`/`b` are the operands
/// reinterpreted as `i64` (Go passes the raw `int64` bits), and
/// `force_signed` is `NO_UNSIGNED_SUBTRACTION`.
pub(super) fn minus_overflows(
    lhs_unsigned: bool,
    rhs_unsigned: bool,
    force_signed: bool,
    a: i64,
    b: i64,
) -> bool {
    let signed = force_signed || (!lhs_unsigned && !rhs_unsigned);
    let res = a.wrapping_sub(b);
    let (ua, ub) = (a as u64, b as u64);
    let mut res_unsigned = false;
    if lhs_unsigned {
        if rhs_unsigned {
            if ua < ub {
                if res >= 0 {
                    return true;
                }
            } else {
                res_unsigned = true;
            }
        } else if b >= 0 {
            if ua > ub {
                res_unsigned = true;
            }
        } else if ua > u64::MAX - b.unsigned_abs() {
            // Go `testIfSumOverflowsUll(ua, uint64(-b))`.
            return true;
        } else {
            res_unsigned = true;
        }
    } else if rhs_unsigned {
        // Go `uint64(a - math.MinInt64) < ub`.
        if (a.wrapping_sub(i64::MIN) as u64) < ub {
            return true;
        }
    } else if a > 0 && b < 0 {
        res_unsigned = true;
    } else if a < 0 && b > 0 && res >= 0 {
        return true;
    }
    (!signed && !res_unsigned && res < 0)
        || (signed && res_unsigned && (res as u64) > i64::MAX as u64)
}

/// MySQL shifts operate on 64-bit unsigned values; a shift amount `>= 64`
/// yields 0.
fn shift_left(a: u64, b: u64) -> u64 {
    if b >= 64 {
        0
    } else {
        a << b
    }
}

fn shift_right(a: u64, b: u64) -> u64 {
    if b >= 64 {
        0
    } else {
        a >> b
    }
}

#[cfg(test)]
mod source_tests {
    use super::*;
    use crate::column::Column;
    use tidb_exprctx::context::NoColumns;
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
