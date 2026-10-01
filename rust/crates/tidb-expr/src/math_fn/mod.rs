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

//! Source-family implementation for the currently translated portion of
//! `pkg/expression/builtin_math.go`.
//!
//! One dispatch owns RAND's AST identity, ABS, SIGN, CEIL/FLOOR,
//! ROUND/TRUNCATE, CONV, CRC32, and the existing transcendental functions.
//! It intentionally does not claim the unimplemented remainder of the Go
//! source. Arguments are still evaluated by `crate::func::eval_func` before
//! this dispatch; RAND additionally receives the original argument AST so its
//! constant-versus-row-dependent generator identity remains unchanged.

mod go_trig;

use tidb_ast::{BinaryOp, Expr, UnaryOp};

use crate::coerce::{coerce_str, coerce_str_bytes};
use crate::ops::{finite_float, to_f64, to_f64_with_mysql_string};
use crate::tikv::{EvaluatedArgs, EvaluatedBytesOp};
use crate::{Columns, Datum, EvalError, MysqlRng};

/// Dispatches translated `builtin_math.go` functions, or returns `None` when
/// the name belongs to another source family.
pub(crate) fn dispatch(
    name: &str,
    args: &[Expr],
    vals: &[Datum],
    cols: &dyn Columns,
    function_key: Option<usize>,
) -> Option<Result<Datum, EvalError>> {
    // RAND is the one arm that needs the argument AST (constant-versus-row
    // generator identity), the session `Columns`, and the per-call
    // `function_key`; every other math builtin is a function of `vals` plus
    // the statement warning sink, and lives in [`dispatch_values`] so the
    // chunk-row bridge can reuse it.
    if name == "RAND" {
        return Some(eval_rand(args, vals, cols, function_key));
    }
    dispatch_values(name, vals, cols)
        .map(|result| result.map_err(|error| ast_math_overflow_error(name, args, error)))
}

/// Go's math signatures attach the source expression to their 1690 overflow.
/// The AST evaluator has the original nodes, so it can preserve that text
/// before the values-only implementation returns its datum-level carrier.
fn ast_math_overflow_error(name: &str, args: &[Expr], error: EvalError) -> EvalError {
    let value = match (name, &error) {
        ("ABS", EvalError::IntOverflow) => "BIGINT",
        ("COT" | "EXP" | "POW" | "POWER", EvalError::FloatOverflow) => "DOUBLE",
        _ => return error,
    };
    let args = args
        .iter()
        .map(render_ast_expression)
        .collect::<Option<Vec<_>>>();
    let Some(args) = args else {
        return error;
    };
    let expression = format!("{}({})", name.to_ascii_lowercase(), args.join(", "));
    EvalError::DataOutOfRange { value, expression }
}

/// Renders the expression text Go includes in a function-owned overflow.
/// The value-tier helper has no AST and therefore cannot use this boundary.
pub(crate) fn render_ast_expression(expression: &Expr) -> Option<String> {
    match expression {
        Expr::Int(value) => Some(value.clone()),
        Expr::Float(value) => Some(tidb_datatype::format_float_g_shortest(*value)),
        Expr::Decimal(value) => Some(value.clone()),
        Expr::Null => Some("NULL".to_owned()),
        Expr::Column(path) => Some(path.join(".")),
        Expr::Unary(UnaryOp::Plus, expression) => {
            Some(format!("+{}", render_ast_expression(expression)?))
        }
        Expr::Unary(UnaryOp::Minus, expression) => {
            Some(format!("-{}", render_ast_expression(expression)?))
        }
        Expr::Paren(expression) => Some(format!("({})", render_ast_expression(expression)?)),
        Expr::Binary(operator, left, right) => render_ast_binary_expression(*operator, left, right),
        Expr::Func { name, args, .. } => {
            let args = args
                .iter()
                .map(render_ast_expression)
                .collect::<Option<Vec<_>>>()?;
            Some(format!(
                "{}({})",
                name.to_ascii_lowercase(),
                args.join(", ")
            ))
        }
        _ => None,
    }
}

/// Renders a binary AST expression with the same parenthesized source shape
/// used by Go's arithmetic overflow signatures.
pub(crate) fn render_ast_binary_expression(
    operator: BinaryOp,
    left: &Expr,
    right: &Expr,
) -> Option<String> {
    let operator = match operator {
        BinaryOp::Plus => "+",
        BinaryOp::Minus => "-",
        BinaryOp::Mul => "*",
        BinaryOp::Div => "/",
        BinaryOp::Mod => "%",
        BinaryOp::IntDiv => "DIV",
        BinaryOp::BitOr => "|",
        BinaryOp::BitAnd => "&",
        BinaryOp::BitXor => "^",
        BinaryOp::LeftShift => "<<",
        BinaryOp::RightShift => ">>",
        BinaryOp::Eq => "=",
        BinaryOp::NullEq => "<=>",
        BinaryOp::Ge => ">=",
        BinaryOp::Gt => ">",
        BinaryOp::Le => "<=",
        BinaryOp::Lt => "<",
        BinaryOp::Ne => "!=",
        BinaryOp::LogicAnd => "AND",
        BinaryOp::LogicOr => "OR",
        BinaryOp::LogicXor => "XOR",
    };
    Some(format!(
        "({} {} {})",
        render_ast_expression(left)?,
        operator,
        render_ast_expression(right)?
    ))
}

/// The values-only subset of [`dispatch`]: every math builtin whose result is
/// a function of its already-evaluated arguments alone. Shared by the
/// AST-level `eval_func` path and `crate::func::eval_func_values` (the
/// `ScalarFunction`/chunk-row bridge).
///
/// `ctx` is NOT a second source of values -- it is the statement warning sink
/// (`crate::Columns::append_warning`). The RESULT still depends only on
/// `vals`; what the context adds is the 1292 truncation warning Go raises
/// while coercing a string argument into the ETReal domain, which is a side
/// effect this dispatch used to have no way to produce.
pub(crate) fn dispatch_values(
    name: &str,
    vals: &[Datum],
    ctx: &dyn Columns,
) -> Option<Result<Datum, EvalError>> {
    let result = match name {
        "ABS" => abs(vals, ctx),
        "SIGN" => sign(vals, ctx),
        "CEIL" | "CEILING" => ceil_floor(vals, true, ctx),
        "FLOOR" => ceil_floor(vals, false, ctx),
        "ROUND" => round_or_truncate(vals, true, ctx),
        "TRUNCATE" => round_or_truncate(vals, false, ctx),
        "SQRT" => sqrt(vals, ctx),
        "POW" | "POWER" => pow(vals, ctx),
        "EXP" => exp(vals, ctx),
        "LN" => ln(vals, ctx),
        "LOG" => log(vals, ctx),
        "LOG2" => log2(vals, ctx),
        "LOG10" => log10(vals, ctx),
        "PI" => pi(vals, ctx),
        "SIN" => sin(vals, ctx),
        "COS" => cos(vals, ctx),
        "TAN" => tan(vals, ctx),
        "ASIN" => asin(vals, ctx),
        "ACOS" => acos(vals, ctx),
        "ATAN" => atan(vals, ctx),
        "ATAN2" => atan2(vals, ctx),
        "COT" => cot(vals, ctx),
        "RADIANS" => radians(vals, ctx),
        "DEGREES" => degrees(vals, ctx),
        "CONV" if vals.len() == 3 => conv_in(vals, ctx),
        "CRC32" if vals.len() == 1 => crc32_in(vals, ctx),
        _ => return None,
    };
    Some(result)
}

/// `CONV(n, from_base, to_base)`: reinterprets `n`'s digits in `from_base`
/// and re-emits them (uppercase) in `to_base`. Ported from `builtinConvSig`
/// in `pkg/expression/builtin_math.go`: a NEGATIVE base means signed
/// (`from_base < 0` interprets the value as signed and clamps to
/// `i64` range; `to_base < 0` renders the result signed with a `-` sign);
/// the value is carried through an unsigned 64-bit two's-complement wrap.
/// Bases must be `2..=36` after taking their absolute value, else `NULL`.
/// A leading `+`/`-` sign is honored; an empty valid prefix yields `"0"`.
/// `NULL` if any argument is `NULL`.
pub(crate) fn conv_in(vals: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    use crate::tikv::{
        EvaluatedArgs, EvaluatedBytesOp, ReadyBytesArg,
        ReadyIntArg::{Undemanded, Value},
    };

    // This selects only the argument representation. Coercion and even the
    // sentinel/base-NULL checks stay inside the existing execution guard.
    let operation = if matches!(vals.first(), Some(Datum::BinaryLiteral(_))) {
        EvaluatedBytesOp::ConvBinaryLiteralNative
    } else {
        EvaluatedBytesOp::ConvNative
    };
    crate::tikv::evaluate_args_in(
        operation,
        ctx,
        || {
            if vals.iter().any(Datum::is_range_sentinel) {
                return Err(EvalError::Unsupported("range sentinel CONV argument"));
            }
            // Keep the helper's original indexing/short-circuit contract:
            // extra values participate only in the sentinel scan, and a NULL
            // first base does not demand even an index read of the second.
            if vals[1].is_null() {
                return Ok(EvaluatedArgs::ConvReady {
                    number: ReadyBytesArg::Undemanded,
                    from_base: Value(None),
                    to_base: Undemanded,
                });
            }
            if vals[2].is_null() {
                return Ok(EvaluatedArgs::ConvReady {
                    number: ReadyBytesArg::Undemanded,
                    from_base: Undemanded,
                    to_base: Value(None),
                });
            }
            // Original no-warning/UTC casts, including UInt's unchanged bits;
            // both precede the number's NULL or strict-text conversion.
            let (from_base, to_base) = (
                crate::cast::to_i64_signed(&vals[1]),
                crate::cast::to_i64_signed(&vals[2]),
            );
            let number = match &vals[0] {
                Datum::Null => None,
                // Do not narrow to u64 or build an intermediate bit string.
                // The shared kernel owns the complete 2 -> from -> to path.
                Datum::BinaryLiteral(literal) => Some(literal.as_bytes().to_vec()),
                value => coerce_str(value)?.map(String::into_bytes),
            };
            Ok(EvaluatedArgs::ConvReady {
                number: ReadyBytesArg::Value(number),
                from_base: Value(Some(from_base)),
                to_base: Value(Some(to_base)),
            })
        },
        |result| {
            Ok(match result.into_bytes()? {
                None => Datum::Null,
                Some(bytes) => Datum::new_string(
                    String::from_utf8(bytes).expect("shared CONV output is ASCII"),
                ),
            })
        },
    )
}

#[cfg(test)]
pub(crate) fn conv(vals: &[Datum]) -> Result<Datum, EvalError> {
    conv_in(vals, &crate::NoColumns)
}

/// The longest valid `CONV` prefix in `base` (a port of
/// `expression.getValidPrefix`): a leading `+`/`-` at position 0 is allowed
/// (a leading `+` is dropped), then valid base-`base` digits until the first
/// invalid character.
#[cfg(test)]
pub(crate) fn conv_valid_prefix(s: &str, base: u32) -> String {
    crate::tikv::conv_valid_prefix_native(s, base)
}

/// `CRC32(str)`: the IEEE CRC-32 checksum (polynomial `0xEDB88320`) as an
/// unsigned integer; `NULL` propagates.
#[cfg(test)]
pub(crate) fn crc32(vals: &[Datum]) -> Result<Datum, EvalError> {
    crc32_in(vals, &crate::NoColumns)
}

pub(crate) fn crc32_in(vals: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    // Go's `builtinCRC32Sig.evalInt` hashes the byte sequence returned by
    // `EvalString`; it does not require the bytes to be valid UTF-8.  This is
    // observable for a non-legacy connection charset: the rewriter's
    // `to_binary` wrapper hands CRC32 GBK bytes such as `D2 BB`, which must be
    // hashed directly rather than rejected by a UTF-8 conversion.
    crate::tikv::evaluate_bytes_in(
        crate::tikv::EvaluatedBytesOp::Crc32,
        ctx,
        || coerce_str_bytes(&vals[0]),
        |result| match result.into_int_datum()? {
            Datum::Null => Ok(Datum::Null),
            Datum::Int(value) => u32::try_from(value)
                .map(|checksum| Datum::UInt(u64::from(checksum)))
                .map_err(|_| EvalError::Unsupported("CRC32 result out of range")),
            _ => Err(EvalError::Unsupported("CRC32 result type")),
        },
    )
}

/// Pack only the computed integer's original SQL signedness.
fn math_integer_in(
    operation: EvaluatedBytesOp,
    arguments: EvaluatedArgs,
    unsigned: bool,
    ctx: &dyn Columns,
) -> Result<Datum, EvalError> {
    crate::tikv::evaluate_args_in(
        operation,
        ctx,
        || Ok(arguments),
        |computed| {
            if unsigned {
                computed.into_uint_bits_datum()
            } else {
                computed.into_int_datum()
            }
        },
    )
}

/// Raw IEEE results retain NaN/Inf and the frontend's Real/Float32 label.
fn math_real_in(
    operation: EvaluatedBytesOp,
    arguments: EvaluatedArgs,
    float32: bool,
    ctx: &dyn Columns,
) -> Result<Datum, EvalError> {
    crate::tikv::evaluate_args_in(
        operation,
        ctx,
        || Ok(arguments),
        |computed| {
            Ok(computed.into_ieee754_bits()?.map_or(Datum::Null, |bits| {
                let value = f64::from_bits(bits);
                if float32 {
                    Datum::Float32(value)
                } else {
                    Datum::Real(value)
                }
            }))
        },
    )
}

/// `ABS(x)`. `absFunctionClass.getFunction` picks the signature from the
/// argument's EVAL TYPE, not from a fixed list of kinds: `ETInt`, `ETDecimal`
/// and `ETReal` each get their own sig, and EVERY remaining kind — string,
/// enum, set, bit, temporal, json, `FLOAT` — evaluates as `ETReal` and lands
/// on `builtinAbsRealSig`. So the last arm is a genuine signature, not a
/// fallback: `ABS('12abc')` is 12, `ABS(<enum>)` its ordinal, and `ABS(f)`
/// for a `FLOAT` column is the WIDENED double (captured: `ABS(0.1e0::float)`
/// is `0.10000000149011612`, not `0.1`), which is why `Float32` produces
/// `Real` here rather than staying `Float32` the way `CEIL` does.
fn abs(vals: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    let [v] = vals else {
        return Err(EvalError::Unsupported("bad function arity"));
    };
    match v {
        Datum::Null => math_integer_in(
            EvaluatedBytesOp::AbsIntNative,
            EvaluatedArgs::Int(None),
            false,
            ctx,
        ),
        Datum::Int(value) => math_integer_in(
            EvaluatedBytesOp::AbsIntNative,
            EvaluatedArgs::Int(Some(*value)),
            false,
            ctx,
        ),
        Datum::UInt(value) => math_integer_in(
            EvaluatedBytesOp::AbsUIntNative,
            EvaluatedArgs::Int(Some(*value as i64)),
            true,
            ctx,
        ),
        Datum::Decimal(value) => crate::tikv::evaluate_args_in(
            EvaluatedBytesOp::AbsDecimalNative,
            ctx,
            || {
                Ok(EvaluatedArgs::Decimal(Some(
                    crate::tikv::prepare_math_decimal(value)?,
                )))
            },
            crate::tikv::EvaluatedBytesResult::into_decimal_datum,
        ),
        Datum::Real(value) => math_real_in(
            EvaluatedBytesOp::AbsRealNative,
            EvaluatedArgs::Ieee754Bits(Some(value.to_bits())),
            false,
            ctx,
        ),
        other => math_real_in(
            EvaluatedBytesOp::AbsRealNative,
            EvaluatedArgs::Ieee754Bits(numeric_arg(other, ctx)?.map(f64::to_bits)),
            false,
            ctx,
        ),
    }
}

/// `SIGN(x)`. Like [`abs`], `signFunctionClass` selects per eval type and
/// every kind outside `ETInt`/`ETDecimal` reaches `builtinSignSig`'s real
/// form, so the catch-all is the ETReal signature (captured: `SIGN(b'11')`
/// is 1, `SIGN(<enum>)` is 1).
fn sign(vals: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    let [v] = vals else {
        return Err(EvalError::Unsupported("bad function arity"));
    };
    crate::tikv::evaluate_args_in(
        crate::tikv::EvaluatedBytesOp::SignRaw,
        ctx,
        || {
            let ready = match v {
                Datum::Null => None,
                // Rounding can lose magnitude bits, but not the sign/zero
                // class: every nonzero magnitude is in [1, 2^64].
                Datum::Int(value) => Some(*value as f64),
                Datum::UInt(value) => Some(*value as f64),
                Datum::Decimal(value) => Some(sign_decimal_representative(value)),
                other => numeric_arg(other, ctx)?,
            };
            Ok(crate::tikv::EvaluatedArgs::Ieee754Bits(
                ready.map(f64::to_bits),
            ))
        },
        crate::tikv::EvaluatedBytesResult::into_int_datum,
    )
}

/// SIGN alone needs a sign/zero-preserving conversion, not an approximate
/// decimal value. `Decimal::to_f64` parses Display, which can round hidden
/// storage precision to zero; public Decimal constructors also allow scales
/// beyond the f64 exponent range. Do not infer an 81-digit bound here.
///
/// A prefix of at most 15 significant coefficient digits is either zero or
/// an integer in [1, 10^15), hence exact in f64 (10^15 < 2^50). Discarding
/// scale and trailing digits preserves SIGN's equivalence class for every
/// valid ASCII decimal coefficient. This is NOT a general decimal-to-real
/// conversion and does not compute SIGN's final -1/0/1 answer in native code.
fn sign_decimal_representative(value: &tidb_datatype::Decimal) -> f64 {
    let magnitude = value
        .coefficient_digits()
        .bytes()
        .skip_while(|byte| *byte == b'0')
        .take(15)
        .fold(0_u64, |prefix, byte| prefix * 10 + u64::from(byte - b'0'));
    let representative = magnitude as f64;
    if value.is_negative() {
        -representative
    } else {
        representative
    }
}

/// Coerces one function argument to `f64`: `NULL` propagates (the `Ok(None)`
/// case, for the caller to turn into `Datum::Null`). This is a port of the
/// `EvalReal` argument coercion used by the signatures in
/// `pkg/expression/builtin_math.go`: string arguments use MySQL's numeric
/// prefix rule (so `SQRT('4')` is 2 and `SIN('abc')` is 0). `ctx` carries the
/// statement warning sink that the string case raises 1292 on; every other
/// kind converts exactly and raises nothing.
pub(crate) fn numeric_arg(v: &Datum, ctx: &dyn Columns) -> Result<Option<f64>, EvalError> {
    match v {
        Datum::Null => Ok(None),
        Datum::String(_) | Datum::Bytes(_) => Ok(Some(to_f64_with_mysql_string(v, ctx)?)),
        Datum::Int(_) | Datum::UInt(_) | Datum::Decimal(_) | Datum::Real(_) => {
            Ok(Some(to_f64(v.clone())))
        }
        Datum::MinNotNull | Datum::MaxValue => {
            Err(EvalError::Unsupported("range sentinel numeric argument"))
        }
        other => other
            .to_f64()
            .map(|converted| Some(converted.value))
            .map_err(|_| EvalError::Unsupported("numeric argument conversion")),
    }
}

/// Keeps the original numeric conversion inside the sole C4 driver. The
/// private IEEE carrier retains every f64 bit pattern; each frontend owns
/// only its existing result policy, not the mathematical computation.
fn unary_raw_real(
    vals: &[Datum],
    ctx: &dyn Columns,
    operation: crate::tikv::EvaluatedBytesOp,
    pack: impl FnOnce(Option<f64>) -> Result<Datum, EvalError>,
) -> Result<Datum, EvalError> {
    let [v] = vals else {
        return Err(EvalError::Unsupported("bad function arity"));
    };
    crate::tikv::evaluate_args_in(
        operation,
        ctx,
        || {
            Ok(crate::tikv::EvaluatedArgs::Ieee754Bits(
                numeric_arg(v, ctx)?.map(f64::to_bits),
            ))
        },
        |computed| pack(computed.into_ieee754_bits()?.map(f64::from_bits)),
    )
}

fn sqrt(vals: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    unary_raw_real(vals, ctx, crate::tikv::EvaluatedBytesOp::SqrtRaw, |value| {
        // Unlike finite_float, SQRT retains the kernel's NaN, +Inf and -0.
        Ok(value.map_or(Datum::Null, Datum::Real))
    })
}

fn ln(vals: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    let [v] = vals else {
        return Err(EvalError::Unsupported("bad function arity"));
    };
    let value = numeric_arg(v, ctx)?;
    let invalid_domain = value.is_some_and(|x| x <= 0.0);
    if invalid_domain {
        // Keep the original 3020 after coercion, before entering C4. Domain
        // rejection is a result policy, never a replacement NULL argument.
        ctx.append_warning(3020, "Invalid argument for logarithm");
    }
    crate::tikv::evaluate_args_in(
        crate::tikv::EvaluatedBytesOp::LnNative,
        ctx,
        || {
            Ok(crate::tikv::EvaluatedArgs::Ieee754Bits(
                value.map(f64::to_bits),
            ))
        },
        |computed| {
            let value = computed.into_ieee754_bits()?.map(f64::from_bits);
            Ok(if invalid_domain {
                Datum::Null
            } else {
                value.map_or(Datum::Null, Datum::Real)
            })
        },
    )
}

/// `LOG(x)` (1 argument, natural log — identical to `LN`) or `LOG(base,
/// x)` (2 arguments, log base `base` of `x`).
fn log(vals: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    match vals {
        [_] => ln(vals, ctx),
        [b, x] => {
            // Both conversions remain demanded even when the base is NULL.
            let (base, value) = (numeric_arg(b, ctx)?, numeric_arg(x, ctx)?);
            let invalid_domain = matches!(
                (base, value),
                (Some(base), Some(x)) if base <= 0.0 || base == 1.0 || x <= 0.0
            );
            if invalid_domain {
                // The original domain warning requires two non-NULL values.
                ctx.append_warning(3020, "Invalid argument for logarithm");
            }
            crate::tikv::evaluate_args_in(
                crate::tikv::EvaluatedBytesOp::LogNative,
                ctx,
                || {
                    Ok(crate::tikv::EvaluatedArgs::Ieee754Bits2 {
                        left: crate::tikv::ReadyIeee754Arg::Value(base.map(f64::to_bits)),
                        right: crate::tikv::ReadyIeee754Arg::Value(value.map(f64::to_bits)),
                    })
                },
                |computed| {
                    let value = computed.into_ieee754_bits()?.map(f64::from_bits);
                    Ok(if invalid_domain {
                        Datum::Null
                    } else {
                        value.map_or(Datum::Null, Datum::Real)
                    })
                },
            )
        }
        _ => Err(EvalError::Unsupported("bad function arity")),
    }
}

fn log2(vals: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    let [v] = vals else {
        return Err(EvalError::Unsupported("bad function arity"));
    };
    let value = numeric_arg(v, ctx)?;
    let invalid_domain = value.is_some_and(|x| x <= 0.0);
    if invalid_domain {
        // Preserve numeric-prefix diagnostics before 3020 (log2('x') warns
        // 1292 then 3020), but send the actual non-positive bits into C4.
        ctx.append_warning(3020, "Invalid argument for logarithm");
    }
    crate::tikv::evaluate_args_in(
        crate::tikv::EvaluatedBytesOp::Log2Native,
        ctx,
        || {
            Ok(crate::tikv::EvaluatedArgs::Ieee754Bits(
                value.map(f64::to_bits),
            ))
        },
        |computed| {
            let value = computed.into_ieee754_bits()?.map(f64::from_bits);
            Ok(if invalid_domain {
                Datum::Null
            } else {
                value.map_or(Datum::Null, Datum::Real)
            })
        },
    )
}

fn log10(vals: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    let [v] = vals else {
        return Err(EvalError::Unsupported("bad function arity"));
    };
    let invalid_domain = std::cell::Cell::new(false);
    crate::tikv::evaluate_args_in(
        EvaluatedBytesOp::Log10GoNative,
        ctx,
        || {
            let value = numeric_arg(v, ctx)?;
            let invalid = value.is_some_and(|x| x <= 0.0);
            invalid_domain.set(invalid);
            if invalid {
                // Keep 3020 after coercion and before admission, but send the
                // actual argument to the shared kernel, not a substitute NULL.
                ctx.append_warning(3020, "Invalid argument for logarithm");
            }
            Ok(EvaluatedArgs::Ieee754Bits(value.map(f64::to_bits)))
        },
        |computed| {
            let value = computed.into_ieee754_bits()?.map(f64::from_bits);
            Ok(if invalid_domain.get() {
                Datum::Null
            } else {
                // Unlike EXP, native LOG10 retains NaN and positive infinity.
                value.map_or(Datum::Null, Datum::Real)
            })
        },
    )
}

pub(crate) fn pow(vals: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    let [base, exp] = vals else {
        return Err(EvalError::Unsupported("bad function arity"));
    };
    // Unlike PB's child loop, this tuple demands both numeric conversions
    // even when the left value is NULL; preserve their diagnostic order.
    let (base, exp) = (numeric_arg(base, ctx)?, numeric_arg(exp, ctx)?);
    pow_ready_in(
        crate::tikv::ReadyIeee754Arg::Value(base.map(f64::to_bits)),
        crate::tikv::ReadyIeee754Arg::Value(exp.map(f64::to_bits)),
        ctx,
    )
}

/// Shares POW's C4 call and result policy with PB's valid-arity NULL boundary.
/// Only that boundary supplies an Undemanded side opposite an actual NULL;
/// the backend validates that demand record before choosing its representative.
pub(crate) fn pow_ready_in(
    left: crate::tikv::ReadyIeee754Arg,
    right: crate::tikv::ReadyIeee754Arg,
    ctx: &dyn Columns,
) -> Result<Datum, EvalError> {
    crate::tikv::evaluate_args_in(
        crate::tikv::EvaluatedBytesOp::PowNative,
        ctx,
        || Ok(crate::tikv::EvaluatedArgs::Ieee754Bits2 { left, right }),
        |computed| {
            computed
                .into_ieee754_bits()?
                .map(f64::from_bits)
                .map_or(Ok(Datum::Null), finite_float)
        },
    )
}

fn exp(vals: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    let [v] = vals else {
        return Err(EvalError::Unsupported("bad function arity"));
    };
    // This invocation's input is retained only for diagnostic formatting;
    // neither coercion nor packing computes or predicts the EXP result.
    let coerced_input = std::cell::Cell::new(None);
    crate::tikv::evaluate_args_in(
        EvaluatedBytesOp::ExpGoNative,
        ctx,
        || {
            let value = numeric_arg(v, ctx)?;
            coerced_input.set(value);
            Ok(EvaluatedArgs::Ieee754Bits(value.map(f64::to_bits)))
        },
        |computed| match computed.into_ieee754_bits()?.map(f64::from_bits) {
            None => Ok(Datum::Null),
            Some(value) if value.is_finite() => Ok(Datum::Real(value)),
            Some(_) => {
                // Format the evaluated argument, not the source expression:
                // exp('2020-01-01') truncates to 2020 before exp(2020) errors.
                let x = coerced_input
                    .get()
                    .ok_or(EvalError::Unsupported("EXP result missing coerced input"))?;
                Err(EvalError::DataOutOfRange {
                    value: "DOUBLE",
                    expression: format!("exp({x})"),
                })
            }
        },
    )
}

/// `PI()`: a niladic function returning the constant.
pub(crate) fn pi(vals: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    match vals {
        [] => crate::eval_pi_in(ctx),
        _ => Err(EvalError::Unsupported("bad function arity")),
    }
}

pub(crate) fn sin(vals: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    unary_raw_real(vals, ctx, EvaluatedBytesOp::SinGoNative, |value| {
        value.map_or(Ok(Datum::Null), finite_float)
    })
}

pub(crate) fn cos(vals: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    unary_raw_real(vals, ctx, EvaluatedBytesOp::CosGoNative, |value| {
        value.map_or(Ok(Datum::Null), finite_float)
    })
}

fn tan(vals: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    unary_raw_real(vals, ctx, EvaluatedBytesOp::TanGoNative, |value| {
        value.map_or(Ok(Datum::Null), finite_float)
    })
}

pub(crate) fn cot(vals: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    unary_raw_real(vals, ctx, EvaluatedBytesOp::CotGoNative, |value| {
        value.map_or(Ok(Datum::Null), finite_float)
    })
}

fn radians(vals: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    unary_raw_real(
        vals,
        ctx,
        crate::tikv::EvaluatedBytesOp::RadiansRaw,
        |value| value.map_or(Ok(Datum::Null), finite_float),
    )
}

fn degrees(vals: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    unary_raw_real(
        vals,
        ctx,
        crate::tikv::EvaluatedBytesOp::DegreesRaw,
        |value| value.map_or(Ok(Datum::Null), finite_float),
    )
}

/// The raw inverse-trig primitive returns NaN exactly for values excluded
/// by the former [-1, 1].contains domain policy (including NaN and infinity).
/// Ordinary SQL callers expose NULL; the legacy raw-real seam intentionally
/// does not use this policy because its casts and total_cmp consume NaN.
fn pack_inverse_trig(value: Option<f64>) -> Result<Datum, EvalError> {
    Ok(value
        .filter(|value| !value.is_nan())
        .map_or(Datum::Null, Datum::Real))
}

pub(crate) fn asin(vals: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    unary_raw_real(
        vals,
        ctx,
        crate::tikv::EvaluatedBytesOp::AsinRaw,
        pack_inverse_trig,
    )
}

pub(crate) fn acos(vals: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    unary_raw_real(
        vals,
        ctx,
        crate::tikv::EvaluatedBytesOp::AcosRaw,
        pack_inverse_trig,
    )
}

/// `ATAN(x)` (1 argument) or `ATAN(y, x)` (2 arguments, exactly `ATAN2(y,
/// x)` — same argument order, confirmed via `goeval`, not assumed).
pub(crate) fn atan(vals: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    match vals {
        [_] => unary_raw_real(vals, ctx, EvaluatedBytesOp::AtanGoNative, |value| {
            value.map_or(Ok(Datum::Null), finite_float)
        }),
        [_, _] => atan2(vals, ctx),
        _ => Err(EvalError::Unsupported("bad function arity")),
    }
}

pub(crate) fn atan2(vals: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    let [y, x] = vals else {
        return Err(EvalError::Unsupported("bad function arity"));
    };
    crate::tikv::evaluate_args_in(
        EvaluatedBytesOp::Atan2GoNative,
        ctx,
        || {
            // Preserve the value path's demand: even a NULL y still coerces x.
            // PB's first-NULL child boundary is handled by its own witness.
            let left = numeric_arg(y, ctx)?.map(f64::to_bits);
            let right = numeric_arg(x, ctx)?.map(f64::to_bits);
            Ok(EvaluatedArgs::Ieee754Bits2 {
                left: crate::tikv::ReadyIeee754Arg::Value(left),
                right: crate::tikv::ReadyIeee754Arg::Value(right),
            })
        },
        |computed| {
            computed
                .into_ieee754_bits()?
                .map(f64::from_bits)
                .map_or(Ok(Datum::Null), finite_float)
        },
    )
}

/// `randFunctionClass` / `builtinRandSig` / `builtinRandWithSeedFirstGenSig`
/// from `pkg/expression/builtin_math.go`. Constant `RAND(N)` owns one
/// statement-scoped generator per AST occurrence; nonconstant inputs start a
/// fresh generator for every row evaluation.
fn eval_rand(
    args: &[Expr],
    vals: &[Datum],
    cols: &dyn Columns,
    function_key: Option<usize>,
) -> Result<Datum, EvalError> {
    match (args, vals) {
        ([], []) => eval_rand_values(&[], cols, function_key, false),
        ([arg], [value]) => eval_rand_values(
            std::slice::from_ref(value),
            cols,
            function_key,
            is_constant_expr(arg),
        ),
        _ => Err(EvalError::Unsupported("bad function arity")),
    }
}

/// The value-level half of [`eval_rand`], shared with the chunk-row bridge
/// (`ScalarFunction::eval`), which has no `tidb_ast::Expr` to classify --
/// its caller passes the constant-vs-row identity it already knows instead.
pub(crate) fn eval_rand_values(
    vals: &[Datum],
    cols: &dyn Columns,
    function_key: Option<usize>,
    arg_is_constant: bool,
) -> Result<Datum, EvalError> {
    match vals {
        [] => cols
            .rand_next()
            .map(Datum::Real)
            .ok_or(EvalError::Unsupported("RAND requires a session")),
        [value] => {
            let seed = rand_seed(value)?;
            if arg_is_constant {
                let key = function_key.ok_or(EvalError::Unsupported(
                    "RAND requires a stable function identity",
                ))?;
                Ok(Datum::Real(
                    cols.rand_seeded_next(key, seed)
                        .unwrap_or_else(|| MysqlRng::new_with_seed(seed).gen()),
                ))
            } else {
                Ok(Datum::Real(MysqlRng::new_with_seed(seed).gen()))
            }
        }
        _ => Err(EvalError::Unsupported("bad function arity")),
    }
}

fn rand_seed(value: &Datum) -> Result<i64, EvalError> {
    match value {
        Datum::Null => Ok(0),
        Datum::Int(value) => Ok(*value),
        Datum::UInt(value) => Ok(*value as i64),
        Datum::Decimal(value) => value.round_to_i64().ok_or(EvalError::IntOverflow),
        Datum::Real(value) => Ok(*value as i64),
        Datum::String(value) => Ok(value
            .as_utf8()
            .map_err(|_| EvalError::Unsupported("invalid UTF-8 string datum"))?
            .trim()
            .parse::<f64>()
            .unwrap_or(0.0) as i64),
        Datum::Bytes(value) => Ok(std::str::from_utf8(value)
            .map_err(|_| EvalError::Unsupported("invalid UTF-8 byte datum"))?
            .trim()
            .parse::<f64>()
            .unwrap_or(0.0) as i64),
        Datum::MinNotNull | Datum::MaxValue => {
            Err(EvalError::Unsupported("range sentinel RAND seed"))
        }
        other => other
            .to_i64()
            .map(|converted| converted.value)
            .map_err(|_| EvalError::Unsupported("RAND seed conversion")),
    }
}

/// This AST-only classifier mirrors the build-time distinction TiDB's
/// function builder makes between a `Constant` and a row-dependent
/// expression. The parser represents a constant arithmetic tree directly,
/// so recurse through its structural wrappers as well.
fn is_constant_expr(expr: &Expr) -> bool {
    match expr {
        Expr::Int(_)
        | Expr::Decimal(_)
        | Expr::Float(_)
        | Expr::Hex(_)
        | Expr::Bit(_)
        | Expr::String(_)
        | Expr::Null => true,
        Expr::Paren(expr) | Expr::Unary(_, expr) => is_constant_expr(expr),
        Expr::Binary(_, left, right) => is_constant_expr(left) && is_constant_expr(right),
        _ => false,
    }
}

/// CEIL/CEILING (`ceiling: true`) or FLOOR (`false`): `Int` is unchanged;
/// `Decimal` computes the EXACT ceiling/floor ([`Decimal::ceil_floor`]).
/// TiDB's builder keeps a decimal result when the argument's declared
/// integer width exceeds `mysql.MaxIntWidth - 2` (18 digits), even when the
/// exact rounded value happens to fit `i64`; this preserves the source
/// `getEvalTp4FloorAndCeil` type boundary rather than inferring the return
/// domain from the runtime magnitude. Narrower decimals collapse to `Int`.
/// `Float`
/// stays `Float` — the OPPOSITE convention from `Decimal`'s own
/// int-collapsing rule, also confirmed via `goeval`, not assumed.
fn ceil_floor(vals: &[Datum], ceiling: bool, ctx: &dyn Columns) -> Result<Datum, EvalError> {
    ceil_floor_with_result_domain(vals, ceiling, None, ctx)
}

/// Typed scalar evaluation for `CEIL`/`FLOOR`.
///
/// `decimal_result` is Go's build-time `retTp == ETDecimal` decision. The
/// values-only path can still recover a stored column's declared shape from
/// [`Decimal::declared_shape`], and uses the payload width only for an
/// unstamped literal.
pub(crate) fn ceil_floor_with_result_domain(
    vals: &[Datum],
    ceiling: bool,
    decimal_result: Option<bool>,
    ctx: &dyn Columns,
) -> Result<Datum, EvalError> {
    let [v] = vals else {
        return Err(EvalError::Unsupported("bad function arity"));
    };
    let integer_op = if ceiling {
        EvaluatedBytesOp::CeilIntNative
    } else {
        EvaluatedBytesOp::FloorIntNative
    };
    let real_op = if ceiling {
        EvaluatedBytesOp::CeilRealNative
    } else {
        EvaluatedBytesOp::FloorRealNative
    };
    match v {
        Datum::Null => math_integer_in(integer_op, EvaluatedArgs::Int(None), false, ctx),
        Datum::Int(i) => math_integer_in(integer_op, EvaluatedArgs::Int(Some(*i)), false, ctx),
        Datum::UInt(i) => {
            math_integer_in(integer_op, EvaluatedArgs::Int(Some(*i as i64)), true, ctx)
        }
        Datum::Decimal(d) => {
            let declared_decimal_result = d
                .declared_shape()
                .map(|(flen, decimal)| flen - decimal > 18);
            let payload_decimal_result = || {
                d.coefficient_digits()
                    .len()
                    .saturating_sub(d.storage_scale() as usize)
                    .max(1)
                    > 18
            };
            let keep_decimal = decimal_result
                .or(declared_decimal_result)
                .unwrap_or_else(payload_decimal_result);
            crate::tikv::evaluate_args_in(
                if ceiling {
                    EvaluatedBytesOp::CeilDecimalNative
                } else {
                    EvaluatedBytesOp::FloorDecimalNative
                },
                ctx,
                || {
                    Ok(EvaluatedArgs::Decimal(Some(
                        crate::tikv::prepare_math_decimal(d)?,
                    )))
                },
                |computed| {
                    if keep_decimal {
                        computed.into_decimal_datum()
                    } else {
                        computed.into_decimal_or_int_datum()
                    }
                },
            )
        }
        Datum::Real(f) => math_real_in(
            real_op,
            EvaluatedArgs::Ieee754Bits(Some(f.to_bits())),
            false,
            ctx,
        ),
        Datum::Float32(f) => math_real_in(
            real_op,
            EvaluatedArgs::Ieee754Bits(Some(f.to_bits())),
            true,
            ctx,
        ),
        // `ceilFunctionClass`/`floorFunctionClass` choose their real
        // signatures for strings. Preserve the resulting FLOAT type in
        // addition to the numeric-prefix coercion: CEIL('1.23') is 2.0,
        // unlike CEIL(Decimal('1.23')) which has a DECIMAL signature.
        Datum::String(_) | Datum::Bytes(_) => {
            let f = to_f64_with_mysql_string(v, ctx)?;
            math_real_in(
                real_op,
                EvaluatedArgs::Ieee754Bits(Some(f.to_bits())),
                false,
                ctx,
            )
        }
        Datum::MinNotNull | Datum::MaxValue => {
            return Err(EvalError::Unsupported("range sentinel numeric argument"));
        }
        other => {
            let f = other
                .to_f64()
                .map_err(|_| EvalError::Unsupported("numeric argument conversion"))?
                .value;
            math_real_in(
                real_op,
                EvaluatedArgs::Ieee754Bits(Some(f.to_bits())),
                false,
                ctx,
            )
        }
    }
}

/// `ROUND(x)`/`ROUND(x, d)` (`round: true`) or `TRUNCATE(x, d)` (`false`,
/// always 2 arguments — confirmed via `goeval`: unlike `ROUND`, `TRUNCATE`
/// has no 1-arg form). `NULL` if any argument is `NULL`. Per-type rule,
/// each confirmed via `goeval` then cross-checked against the real
/// `pkg/expression/builtin_math.go`/`pkg/types/helper.go` sources (not
/// assumed to match `CEIL`/`FLOOR`'s rule, which is different):
/// - Integer `ROUND` is identity with one argument; with two it always
///   round-trips through `f64`, including the signed-bit reading of UInt.
///   Integer `TRUNCATE` instead uses exact signed/unsigned division. These
///   distinctions belong to the selected shared kernel, not result packing.
/// - `Decimal` NEVER collapses to `Int` (unlike `CEIL`/`FLOOR` — confirmed
///   `ROUND(3.14159)` is `DEC:3`, not `INT:3`) and rounds ties AWAY from
///   zero (`ModeHalfUp`/`ModeTruncate`), clamped to MySQL's `DECIMAL` max
///   scale (30) for a positive `d`.
/// - `Float` rounds/truncates via Go's `types.Round`/`types.Truncate`
///   implemented bit-for-bit in the shared TiKV native math kernels:
///   `ROUND` ties TO EVEN — the OPPOSITE tie-breaking rule from `Decimal`
///   (matching the bitwise-conversion precedent), not a "more correct"
///   decimal-aware rounding, since the reference implementation is
///   deliberately this simple and occasionally imprecise.
fn round_or_truncate(vals: &[Datum], round: bool, ctx: &dyn Columns) -> Result<Datum, EvalError> {
    round_or_truncate_with_result_decimal(vals, round, None, ctx)
}

/// The typed scalar path for decimal `ROUND`/`TRUNCATE`.
///
/// Go's decimal signatures cap the row's requested scale by `b.tp.Decimal`,
/// which is fixed when the expression is built. The values-only evaluator has
/// no result `FieldType`, so it passes `None`; [`crate::ScalarFunction`] owns
/// that type and passes the declared scale here.
pub(crate) fn round_or_truncate_with_result_decimal(
    vals: &[Datum],
    round: bool,
    result_decimal: Option<i64>,
    ctx: &dyn Columns,
) -> Result<Datum, EvalError> {
    if vals.iter().any(Datum::is_range_sentinel) {
        return Err(EvalError::Unsupported(
            "range sentinel ROUND/TRUNCATE argument",
        ));
    }
    if vals.contains(&Datum::Null) {
        return math_integer_in(
            EvaluatedBytesOp::MathNullWitnessNative,
            EvaluatedArgs::NullWitness(None),
            false,
            ctx,
        );
    }
    // The integer TRUNCATE signatures inspect the scale FieldType before
    // evaluating its value.  An unsigned scale is therefore always
    // non-negative, even when its u64 bit pattern would become negative if
    // narrowed to i64 (for example CAST(18446744073709551615 AS UNSIGNED));
    // Go returns the integer input unchanged in that case.  Keep this type
    // boundary explicit instead of letting the value-only scale cast invent
    // a signed negative precision.
    let unsigned_integer_scale = !round && matches!(vals.get(1), Some(Datum::UInt(_)));
    // `builtinRoundIntSig.evalInt` is literally `return b.args[0].EvalInt(...)`:
    // the ONE-argument integer ROUND is the identity, not a round trip through
    // `f64`. The two-argument form is a different signature
    // (`builtinRoundWithFracIntSig`) that really does go through `f64`, so the
    // distinction is a signature boundary rather than an optimization.
    // It is observable past `f64`'s 53-bit exact range: CAPTURED from TiDB,
    // `ROUND(9223372036854775806)` is `9223372036854775806` while
    // `ROUND(9223372036854775806, 0)` is `9223372036854775807`.
    let int_round_is_identity = round && vals.len() == 1;
    let (v, d) = match vals {
        [v] if round => (v, 0i64),
        // The scale is Go's `types.ETInt` argument, cast by
        // `crate::arg_eval_type` before this body ever sees it, so
        // `ROUND(1.2345, '2')` arrives here as the integer `2` -- this
        // signature no longer has, or needs, an opinion about a string scale.
        [v, d] => (v, crate::arg_eval_type::eval_int(d)?.unwrap_or_default()),
        _ => return Err(EvalError::Unsupported("bad function arity")),
    };
    let integer = |bits: i64, unsigned: bool| {
        let operation = if int_round_is_identity {
            EvaluatedBytesOp::RoundIntNative
        } else if round {
            EvaluatedBytesOp::RoundIntWithScaleNative
        } else if unsigned_integer_scale {
            EvaluatedBytesOp::TruncateIntUnsignedScaleNative
        } else if unsigned {
            EvaluatedBytesOp::TruncateUIntNative
        } else {
            EvaluatedBytesOp::TruncateIntNative
        };
        let arguments = if int_round_is_identity {
            EvaluatedArgs::Int(Some(bits))
        } else {
            EvaluatedArgs::Int2(Some(bits), Some(d))
        };
        math_integer_in(operation, arguments, unsigned, ctx)
    };
    let real_op = if round {
        EvaluatedBytesOp::RoundRealNative
    } else {
        EvaluatedBytesOp::TruncateRealNative
    };
    match v {
        Datum::Int(i) => integer(*i, false),
        // ROUND's two-argument integer kernel reads these as signed bits;
        // unsigned TRUNCATE has its own exact-magnitude kernel.
        Datum::UInt(i) => integer(*i as i64, true),
        Datum::Decimal(dec) => {
            // MySQL clamps a positive scale to DECIMAL's max (30); a
            // negative scale is used as-is (confirmed via `goeval`:
            // `ROUND(12345, -2)` is `12300`, not clamped).
            let target_scale = crate::tikv::native_decimal_target_scale(d, result_decimal);
            crate::tikv::evaluate_args_in(
                if round {
                    EvaluatedBytesOp::RoundDecimalNative
                } else {
                    EvaluatedBytesOp::TruncateDecimalNative
                },
                ctx,
                || {
                    Ok(EvaluatedArgs::DecimalIntReady {
                        value: crate::tikv::ReadyDecimalArg::Value(Some(
                            crate::tikv::prepare_math_decimal(dec)?,
                        )),
                        scale: crate::tikv::ReadyIntArg::Value(Some(i64::from(target_scale))),
                    })
                },
                crate::tikv::EvaluatedBytesResult::into_decimal_datum,
            )
        }
        Datum::Real(f) => math_real_in(
            real_op,
            EvaluatedArgs::Ieee754BitsInt {
                value: Some(f.to_bits()),
                scale: Some(d),
            },
            false,
            ctx,
        ),
        Datum::Float32(f) => math_real_in(
            real_op,
            EvaluatedArgs::Ieee754BitsInt {
                value: Some(f.to_bits()),
                scale: Some(d),
            },
            true,
            ctx,
        ),
        Datum::Null | Datum::MinNotNull | Datum::MaxValue => unreachable!("guarded above"),
        // Every remaining kind — string, enum, set, bit, temporal, json,
        // `FLOAT`'s widened form — is `ETReal` to
        // `roundFunctionClass`/`truncateFunctionClass`, so it rounds as a
        // double and REPORTS as one (captured: `ROUND('12.6abc')` is
        // `FLOAT:13`, `TRUNCATE('12.68abc', 1)` is `FLOAT:12.6`). Strings
        // used to be refused here; they are an ordinary signature, reached
        // through the shared numeric-prefix coercion rather than a
        // ROUND-local parse.
        other => {
            let f = numeric_arg(other, ctx)?.expect("NULL guarded above");
            math_real_in(
                real_op,
                EvaluatedArgs::Ieee754BitsInt {
                    value: Some(f.to_bits()),
                    scale: Some(d),
                },
                false,
                ctx,
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use super::{cot, dispatch, log2, sin, sqrt};
    use crate::{Columns, Datum, EvalError};
    use tidb_ast::Expr;

    struct RandColumns {
        seeded: Cell<Option<(usize, i64)>>,
    }

    impl Columns for RandColumns {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }

        fn rand_seeded_next(&self, key: usize, seed: i64) -> Option<f64> {
            self.seeded.set(Some((key, seed)));
            Some(0.25)
        }
    }

    #[test]
    fn rand_dispatch_retains_constant_ast_and_function_identity() {
        let columns = RandColumns {
            seeded: Cell::new(None),
        };
        assert_eq!(
            dispatch(
                "RAND",
                &[Expr::Int("7".to_string())],
                &[Datum::Int(7)],
                &columns,
                Some(41),
            ),
            Some(Ok(Datum::Real(0.25)))
        );
        assert_eq!(columns.seeded.get(), Some((41, 7)));
    }

    #[test]
    fn rand_values_shares_the_ast_paths_identity_semantics() {
        use super::eval_rand_values;

        // The chunk bridge has no `Expr` to classify, so its caller passes
        // constant-vs-row as a plain bool; a constant argument still needs
        // the stable identity to reach the seeded generator.
        let columns = RandColumns {
            seeded: Cell::new(None),
        };
        assert_eq!(
            eval_rand_values(&[Datum::Int(7)], &columns, Some(41), true),
            Ok(Datum::Real(0.25))
        );
        assert_eq!(columns.seeded.get(), Some((41, 7)));

        // A nonconstant argument (or a missing identity) never touches the
        // seeded generator -- it always starts a fresh one.
        let columns = RandColumns {
            seeded: Cell::new(None),
        };
        assert!(matches!(
            eval_rand_values(&[Datum::Int(7)], &columns, None, false),
            Ok(Datum::Real(_))
        ));
        assert_eq!(columns.seeded.get(), None);

        // The zero-argument form reads the session's running generator.
        assert_eq!(
            eval_rand_values(&[], &columns, None, false),
            Err(EvalError::Unsupported("RAND requires a session"))
        );
    }

    #[test]
    fn real_math_signatures_coerce_mysql_string_prefixes() {
        assert_eq!(
            sqrt(&[Datum::new_string("4".to_owned())], &crate::NoColumns),
            Ok(Datum::Real(2.0))
        );
        assert_eq!(
            sin(
                &[Datum::new_string("not numeric".to_owned())],
                &crate::NoColumns
            ),
            Ok(Datum::Real(0.0))
        );
        assert_eq!(
            log2(&[Datum::new_string("4abc".to_owned())], &crate::NoColumns),
            Ok(Datum::Real(2.0))
        );
        assert_eq!(
            log2(&[Datum::new_string("abc".to_owned())], &crate::NoColumns),
            Ok(Datum::Null)
        );
    }

    #[test]
    fn cot_preserves_go_overflow_after_string_coercion() {
        assert_eq!(
            cot(&[Datum::new_string("tidb".to_owned())], &crate::NoColumns),
            Err(EvalError::FloatOverflow)
        );
    }
}
