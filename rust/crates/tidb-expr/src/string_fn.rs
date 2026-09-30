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

//! String builtin functions (`CONCAT`, `LENGTH`, `UPPER`/`LOWER`,
//! `LEFT`/`RIGHT`, `SUBSTRING`), dispatched from `crate::func::eval_func`,
//! plus `position`/`trim_value` below. `POSITION(a IN b)` and `TRIM(...)`
//! have dedicated AST grammars; the row evaluator handles those nodes
//! directly, while the production rewriter lowers them to scalar functions.

use crate::coerce::{coerce_str, coerce_str_bytes};
use crate::ops::to_f64_with_mysql_string;
use crate::{Datum, EvalError};
use tidb_datatype::{
    find_encoding, get_default_collation, Collation, FieldType, GoString, TransformOp,
};

/// CONCAT: `NULL` if any argument is `NULL`, else the concatenation.
pub(crate) fn concat(vals: &[Datum]) -> Result<Datum, EvalError> {
    concat_with_context(vals, &crate::context::NoColumns)
}

/// Value-evaluated CONCAT with the statement packet limit applied.
pub(crate) fn concat_with_context(
    vals: &[Datum],
    ctx: &dyn crate::context::Columns,
) -> Result<Datum, EvalError> {
    if vals.is_empty() {
        return Err(EvalError::Unsupported(
            "CONCAT requires at least one argument",
        ));
    }
    let mut out = Vec::new();
    for v in vals {
        match coerce_str_bytes(v)? {
            Some(s) => {
                if out.len().saturating_add(s.len()) as u64 > ctx.max_allowed_packet() {
                    ctx.handle_allowed_packet_overflowed("concat")?;
                    return Ok(Datum::Null);
                }
                out.extend_from_slice(&s);
            }
            None => return Ok(Datum::Null),
        }
    }
    Ok(Datum::new_string(out))
}

/// `LOWER`/`UPPER`: text signatures apply Unicode case mapping while binary
/// signatures return the original bytes unchanged (the source selects those
/// signatures from the argument FieldType before evaluation).  Numeric values
/// still pass through the ordinary ETString conversion.
#[cfg(test)]
pub(crate) fn case_convert(vals: &[Datum], upper: bool) -> Result<Datum, EvalError> {
    case_convert_in(vals, upper, &crate::NoColumns)
}

pub(crate) fn case_convert_in(
    vals: &[Datum],
    upper: bool,
    ctx: &dyn crate::Columns,
) -> Result<Datum, EvalError> {
    let [value] = vals else {
        return Err(EvalError::Unsupported("bad case-conversion arity"));
    };
    case_convert_signature_in(
        vals,
        upper,
        crate::string_signature::is_binary_str(value),
        ctx,
    )
}

/// The selected binary/UTF-8 signature must survive even a NULL PB argument.
/// Binary no-op and simple Unicode mapping both execute in C4. Only Go's
/// per-malformed-byte RuneError normalization remains on the native side.
pub(crate) fn case_convert_signature_in(
    vals: &[Datum],
    upper: bool,
    binary: bool,
    ctx: &dyn crate::Columns,
) -> Result<Datum, EvalError> {
    let [value] = vals else {
        return Err(EvalError::Unsupported("bad case-conversion arity"));
    };
    let operation = match (upper, binary) {
        (false, true) => crate::tikv::EvaluatedBytesOp::Lower,
        (true, true) => crate::tikv::EvaluatedBytesOp::Upper,
        (false, false) => crate::tikv::EvaluatedBytesOp::LowerUtf8Ready,
        (true, false) => crate::tikv::EvaluatedBytesOp::UpperUtf8Ready,
    };
    crate::tikv::evaluate_bytes_in(
        operation,
        ctx,
        || {
            Ok(coerce_str_bytes(value)?.map(|bytes| {
                if binary {
                    bytes
                } else {
                    GoString::from(bytes).to_utf8_lossy_go().into_bytes()
                }
            }))
        },
        |computed| {
            Ok(computed.into_bytes()?.map_or(Datum::Null, |bytes| {
                if binary {
                    Datum::new_bytes(bytes)
                } else {
                    Datum::new_string(bytes)
                }
            }))
        },
    )
}

#[cfg(test)]
mod case_convert_tests {
    use super::case_convert;
    use crate::Datum;
    use tidb_datatype::{Collation, MysqlEnum};

    #[test]
    fn enum_names_follow_the_selected_binary_or_unicode_signature() {
        let binary = Datum::new_enum(MysqlEnum::new([0xff], 1), Collation::Binary);
        assert_eq!(
            case_convert(&[binary], true),
            Ok(Datum::new_bytes(vec![0xff]))
        );

        let text = Datum::new_enum(MysqlEnum::new([0xe2, 0x82], 1), Collation::Utf8Mb4Bin);
        assert_eq!(
            case_convert(&[text], true),
            Ok(Datum::new_string("\u{fffd}\u{fffd}"))
        );
    }
}

/// `ASCII(s)` preserves native arity and byte coercion, then delegates every
/// successful value (including NULL) to the closed TiKV C4 worker.
pub(crate) fn ascii(vals: &[Datum], ctx: &dyn crate::Columns) -> Result<Datum, EvalError> {
    if vals.len() != 1 {
        return Err(EvalError::Unsupported("bad function arity"));
    }
    crate::tikv::evaluate_ascii_in(&vals[0], ctx)
}

/// `BIT_LENGTH(s)`: count evaluated bytes and multiply by eight. This follows
/// Go's `len(val)` contract for ordinary UTF-8 and binary values alike.
#[cfg(test)]
pub(crate) fn bit_length(vals: &[Datum]) -> Result<Datum, EvalError> {
    bit_length_in(vals, &crate::NoColumns)
}

pub(crate) fn bit_length_in(vals: &[Datum], ctx: &dyn crate::Columns) -> Result<Datum, EvalError> {
    if vals.len() != 1 {
        return Err(EvalError::Unsupported("bad function arity"));
    }
    crate::tikv::evaluate_bytes_in(
        crate::tikv::EvaluatedBytesOp::BitLength,
        ctx,
        || coerce_str_bytes(&vals[0]),
        crate::tikv::EvaluatedBytesResult::into_int_datum,
    )
}

/// `LEFT`/`RIGHT`: the first or last `n` units, where `builtinLeftSig` and
/// `builtinRightSig` count BYTES for a binary argument (preserving invalid
/// UTF-8) and their `...UTF8Sig` twins count CHARACTERS. `NULL` if either
/// argument is `NULL`.
#[cfg(test)]
pub(crate) fn str_take(vals: &[Datum], from_left: bool) -> Result<Datum, EvalError> {
    str_take_in(vals, from_left, &crate::NoColumns)
}

pub(crate) fn str_take_in(
    vals: &[Datum],
    from_left: bool,
    ctx: &dyn crate::Columns,
) -> Result<Datum, EvalError> {
    if vals.len() != 2 {
        return Err(EvalError::Unsupported("bad LEFT/RIGHT arguments"));
    }
    let binary = crate::string_signature::is_binary_str(&vals[0]);
    let operation = match (from_left, binary) {
        (true, true) => crate::tikv::EvaluatedBytesOp::Left,
        (true, false) => crate::tikv::EvaluatedBytesOp::LeftUtf8,
        (false, true) => crate::tikv::EvaluatedBytesOp::Right,
        (false, false) => crate::tikv::EvaluatedBytesOp::RightUtf8,
    };
    crate::tikv::evaluate_args_in(
        operation,
        ctx,
        || {
            let n = match &vals[1] {
                Datum::Null => return Ok(crate::tikv::EvaluatedArgs::BytesInt(None, None)),
                value => crate::cast::to_i64_signed(value).max(0),
            };
            let bytes = coerce_str_bytes(&vals[0])?.map(|bytes| {
                if binary {
                    bytes
                } else {
                    crate::string_signature::normalize_utf8_go(bytes)
                }
            });
            Ok(crate::tikv::EvaluatedArgs::BytesInt(bytes, Some(n)))
        },
        |result| {
            Ok(result.into_bytes()?.map_or(Datum::Null, |bytes| {
                if binary {
                    Datum::new_bytes(bytes)
                } else {
                    Datum::new_string(bytes)
                }
            }))
        },
    )
}

/// `SUBSTRING(s, pos[, len])`: 1-indexed, counting in the units of the
/// signature Go selected for the argument's charset — bytes for
/// `builtinSubstring2ArgsSig`/`builtinSubstring3ArgsSig`, characters for their
/// `...UTF8Sig` twins (`pkg/expression/builtin_string.go`). C4 owns slicing
/// in those units: negative positions count back from the end, while position
/// zero and out-of-range positions produce the empty string. The caller keeps
/// its original coercion and NULL-demand policy.
pub(crate) fn substring(
    vals: &[Datum],
    cols: &dyn crate::context::Columns,
) -> Result<Datum, EvalError> {
    substring_with_contexts(vals, cols, cols)
}

/// The two-argument value dispatcher historically uses NoColumns for casts.
/// Keep that complete policy separate from the actual C4 execution capability.
pub(crate) fn substring_with_contexts(
    vals: &[Datum],
    cast_cols: &dyn crate::Columns,
    execution_cols: &dyn crate::Columns,
) -> Result<Datum, EvalError> {
    use crate::tikv::{ReadyBytesArg, ReadyIntArg};
    if let Some(null_index) = vals.iter().position(Datum::is_null) {
        // The original NULL check precedes even arity validation. Only real
        // two/three-operand calls have a recipe; malformed calls keep early NULL.
        if !matches!(vals.len(), 2 | 3) {
            return Ok(Datum::Null);
        }
        let bytes = if null_index == 0 {
            ReadyBytesArg::Value(None)
        } else {
            ReadyBytesArg::Undemanded
        };
        let pos = if null_index == 1 {
            ReadyIntArg::Value(None)
        } else {
            ReadyIntArg::Undemanded
        };
        let len = (vals.len() == 3).then(|| {
            if null_index == 2 {
                ReadyIntArg::Value(None)
            } else {
                ReadyIntArg::Undemanded
            }
        });
        return substring_ready_in(
            bytes,
            pos,
            len,
            crate::string_signature::is_binary_str(&vals[0]),
            execution_cols,
        );
    }
    // Preserve pos -> optional length -> source coercion, even at positions
    // whose eventual result is empty. Two arguments never acquire a fake MAX.
    let (source, pos, len) = match vals {
        [source, pos] => (
            source,
            crate::cast::to_i64_signed_with_warnings(pos, cast_cols)?,
            None,
        ),
        [source, pos, len] => (
            source,
            crate::cast::to_i64_signed_with_warnings(pos, cast_cols)?,
            Some(crate::cast::to_i64_signed_with_warnings(len, cast_cols)?),
        ),
        _ => return Err(EvalError::Unsupported("bad SUBSTRING arguments")),
    };
    let bytes = coerce_str_bytes(source)?;
    let binary = crate::string_signature::is_binary_str(source);
    let bytes = bytes.map(|bytes| {
        if binary {
            bytes
        } else {
            crate::string_signature::normalize_utf8_go(bytes)
        }
    });
    substring_ready_in(
        ReadyBytesArg::Value(bytes),
        ReadyIntArg::Value(Some(pos)),
        len.map(|len| ReadyIntArg::Value(Some(len))),
        binary,
        execution_cols,
    )
}

/// Receives actual NULL/demand records from native and PB boundaries. An absent
/// length means the true two-argument signature, not a nullable third argument.
pub(crate) fn substring_ready_in(
    bytes: crate::tikv::ReadyBytesArg,
    pos: crate::tikv::ReadyIntArg,
    len: Option<crate::tikv::ReadyIntArg>,
    binary: bool,
    ctx: &dyn crate::Columns,
) -> Result<Datum, EvalError> {
    use crate::tikv::{EvaluatedArgs, EvaluatedBytesOp};
    let (operation, args) = match (binary, len) {
        (true, None) => (
            EvaluatedBytesOp::Substring2BytesNative,
            EvaluatedArgs::Substring2Ready { bytes, pos },
        ),
        (false, None) => (
            EvaluatedBytesOp::Substring2Utf8Native,
            EvaluatedArgs::Substring2Ready { bytes, pos },
        ),
        (true, Some(len)) => (
            EvaluatedBytesOp::Substring3BytesNative,
            EvaluatedArgs::Substring3Ready { bytes, pos, len },
        ),
        (false, Some(len)) => (
            EvaluatedBytesOp::Substring3Utf8Native,
            EvaluatedArgs::Substring3Ready { bytes, pos, len },
        ),
    };
    crate::tikv::evaluate_args_in(
        operation,
        ctx,
        || Ok(args),
        |computed| {
            Ok(computed.into_bytes()?.map_or(Datum::Null, |bytes| {
                if binary {
                    Datum::new_bytes(bytes)
                } else {
                    Datum::new_string(bytes)
                }
            }))
        },
    )
}

/// `REVERSE(s)`: the units of `s` in the opposite order. Go's
/// `reverseFunctionClass.getFunction` selects `builtinReverseSig`
/// (`reverseBytes`) for a binary or BIT argument and `builtinReverseUTF8Sig`
/// (`reverseRunes`) otherwise, so reversing a binary value must NOT permute
/// the bytes of a multi-byte character back into a valid one.
#[cfg(test)]
pub(crate) fn reverse(vals: &[Datum]) -> Result<Datum, EvalError> {
    reverse_in(vals, &crate::NoColumns)
}

pub(crate) fn reverse_in(vals: &[Datum], ctx: &dyn crate::Columns) -> Result<Datum, EvalError> {
    let [value] = vals else {
        return Err(EvalError::Unsupported("bad REVERSE arity"));
    };
    let binary = crate::string_signature::is_binary_str(value);
    let operation = if binary {
        crate::tikv::EvaluatedBytesOp::Reverse
    } else {
        crate::tikv::EvaluatedBytesOp::ReverseUtf8
    };
    crate::tikv::evaluate_bytes_in(
        operation,
        ctx,
        || {
            Ok(coerce_str_bytes(value)?.map(|bytes| {
                if binary {
                    bytes
                } else {
                    crate::string_signature::normalize_utf8_go(bytes)
                }
            }))
        },
        |result| {
            Ok(result.into_bytes()?.map_or(Datum::Null, |bytes| {
                if binary {
                    Datum::new_bytes(bytes)
                } else {
                    Datum::new_string(bytes)
                }
            }))
        },
    )
}

/// `POSITION(substr IN str)`: the 1-indexed, character-based position of
/// `substr`'s first occurrence in `str`; `0` if not found; an empty
/// `substr` always matches at position `1` (confirmed via `gorun`).
/// `NULL` if either operand is `NULL`.
#[cfg(test)]
pub(crate) fn position(substr: Option<String>, str: Option<String>) -> Datum {
    position_in(substr, str, &crate::NoColumns).expect("test POSITION worker")
}

pub(crate) fn position_in(
    substr: Option<String>,
    str: Option<String>,
    ctx: &dyn crate::Columns,
) -> Result<Datum, EvalError> {
    position_with_collation_in(substr, str, tidb_datatype::Collation::Utf8Mb4Bin, ctx)
}

/// `LOCATE(substr, str)` / `INSTR(str, substr)` / `POSITION(substr IN str)`
/// over the raw arguments, which is what selects the signature.
///
/// `locateFunctionClass.getFunction` picks `builtinLocate2ArgsSig` over
/// `builtinLocate2ArgsUTF8Sig` when the function's DERIVED collation is
/// `binary` (`bf.collation == charset.CollationBin`), and that signature is a
/// plain `strings.Index`: a 1-based BYTE offset, with no collation folding.
/// The UTF-8 signature reports a 1-based CHARACTER offset instead, so the two
/// disagree for any haystack with a multi-byte prefix.
///
/// `collation` is the derived collation where the caller has one; the AST
/// evaluator has no derivation pass and passes `binary` exactly when
/// [`crate::string_signature::is_binary_str`] holds for an argument, which is
/// the same condition that makes Go's aggregate `binary`.
#[cfg(test)]
pub(crate) fn locate(
    substr: &Datum,
    str: &Datum,
    collation: tidb_datatype::Collation,
) -> Result<Datum, EvalError> {
    locate_in(substr, str, collation, &crate::NoColumns)
}

pub(crate) fn locate_in(
    substr: &Datum,
    str: &Datum,
    collation: tidb_datatype::Collation,
    ctx: &dyn crate::Columns,
) -> Result<Datum, EvalError> {
    crate::tikv::evaluate_args_in(
        crate::tikv::EvaluatedBytesOp::Locate2Native,
        ctx,
        || {
            let (needle, haystack, policy) = if collation == Collation::Binary {
                (
                    coerce_str_bytes(substr)?,
                    coerce_str_bytes(str)?,
                    crate::tikv::NativeSearchPolicy::Bytes,
                )
            } else {
                (
                    coerce_str(substr)?.map(String::into_bytes),
                    coerce_str(str)?.map(String::into_bytes),
                    crate::tikv::NativeSearchPolicy::Utf8(collation.native_policy()),
                )
            };
            Ok(crate::tikv::EvaluatedArgs::SearchBytes2 {
                needle,
                haystack,
                policy,
            })
        },
        crate::tikv::EvaluatedBytesResult::into_int_datum,
    )
}

/// `LOCATE(substr, str, pos)` — the three-argument signatures
/// `builtinLocate3ArgsSig` (bytes, under a binary collation) and
/// `builtinLocate3ArgsUTF8Sig` (characters, under any other collation)
/// (`pkg/expression/builtin_string.go:1615/:1660`).
///
/// Go converts the 1-based `pos` to a 0-based index BEFORE the bounds check;
/// `pos < 1` (0-based negative) or `pos` beyond `len - needle_len` answers 0,
/// and an empty needle answers `pos` itself (reported 1-based). Under a
/// case-insensitive collation Go lowers BOTH strings with `strings.ToLower`
/// before the rune-count bounds, so the bounds apply to the lowered lengths.
/// Matching in the slice is by the function's collator — which the 2-arg
/// path already relies on — so a folding collation finds a case-folded
/// occurrence exactly as it does without a start position.
pub(crate) fn locate_with_position_in(
    vals: &[Datum],
    collation: tidb_datatype::Collation,
    ctx: &dyn crate::Columns,
) -> Result<Datum, EvalError> {
    let [substr, str, pos] = vals else {
        return Err(EvalError::Unsupported("bad LOCATE arity"));
    };
    crate::tikv::evaluate_args_in(
        crate::tikv::EvaluatedBytesOp::Locate3Native,
        ctx,
        || {
            // Preserve both string conversions before NULL prevents reading
            // the integer. Typed callers have already cast that child in ctx.
            let (needle, haystack, policy) = if collation == Collation::Binary {
                (
                    coerce_str_bytes(substr)?,
                    coerce_str_bytes(str)?,
                    crate::tikv::NativeSearchPolicy::Bytes,
                )
            } else {
                (
                    coerce_str(substr)?.map(String::into_bytes),
                    coerce_str(str)?.map(String::into_bytes),
                    crate::tikv::NativeSearchPolicy::Utf8(collation.native_policy()),
                )
            };
            let pos = if needle.is_some() && haystack.is_some() {
                crate::tikv::ReadyIntArg::Value(crate::arg_eval_type::eval_int(pos)?)
            } else {
                crate::tikv::ReadyIntArg::Undemanded
            };
            Ok(crate::tikv::EvaluatedArgs::SearchBytes2IntReady {
                needle,
                haystack,
                pos,
                policy,
            })
        },
        crate::tikv::EvaluatedBytesResult::into_int_datum,
    )
}

/// The collation `LOCATE`/`INSTR` derive when no derivation pass ran: Go's
/// aggregate is `binary` as soon as one string argument is binary.
pub(crate) fn locate_collation(substr: &Datum, str: &Datum) -> tidb_datatype::Collation {
    if crate::string_signature::is_binary_str(substr) || crate::string_signature::is_binary_str(str)
    {
        tidb_datatype::Collation::Binary
    } else {
        tidb_datatype::Collation::Utf8Mb4Bin
    }
}

/// `LOCATE`/`INSTR`/`POSITION` under an explicit collation.
///
/// Go's `builtinLocate2ArgsUTF8Sig`/`builtinInstrUTF8Sig` search with the
/// function's own collator, so a case-insensitive collation finds a
/// case-folded occurrence: captured from TiDB,
/// `INSTR('ABC' COLLATE utf8mb4_general_ci, 'b')` is 2 where the
/// `utf8mb4_bin` form is 0. The position reported is a 1-based CHARACTER
/// index into the haystack either way.
///
/// The window is compared by the collation rather than by bytes, which is
/// what makes a folding collation match; a collation whose folding changes
/// character COUNT (none this tier registers) would need a different scan.
///
/// Unlike [`locate_in`], this already-text helper always counts characters,
/// even if its explicit comparison collation is `binary`.
pub(crate) fn position_with_collation_in(
    substr: Option<String>,
    str: Option<String>,
    collation: tidb_datatype::Collation,
    ctx: &dyn crate::Columns,
) -> Result<Datum, EvalError> {
    crate::tikv::evaluate_args_in(
        crate::tikv::EvaluatedBytesOp::Locate2Native,
        ctx,
        || {
            Ok(crate::tikv::EvaluatedArgs::SearchBytes2 {
                needle: substr.map(String::into_bytes),
                haystack: str.map(String::into_bytes),
                // This helper always counts characters, even with Binary compare.
                policy: crate::tikv::NativeSearchPolicy::Utf8(collation.native_policy()),
            })
        },
        crate::tikv::EvaluatedBytesResult::into_int_datum,
    )
}

/// `REPLACE(str, from, to)`: every non-overlapping occurrence of `from` in
/// `str` replaced by `to`. An empty `from` leaves `str` unchanged (matching
/// MySQL, and avoiding a pathological empty-pattern replace). `NULL` if any
/// argument is `NULL`.
#[cfg(test)]
pub(crate) fn replace(vals: &[Datum]) -> Result<Datum, EvalError> {
    replace_in(vals, &crate::NoColumns)
}

pub(crate) fn replace_in(vals: &[Datum], ctx: &dyn crate::Columns) -> Result<Datum, EvalError> {
    let [value, from, to] = vals else {
        return Err(EvalError::Unsupported("bad REPLACE arity"));
    };
    crate::tikv::evaluate_args_in(
        crate::tikv::EvaluatedBytesOp::Replace,
        ctx,
        || {
            let (s, from, to) = (
                coerce_str_bytes(value)?,
                coerce_str_bytes(from)?,
                coerce_str_bytes(to)?,
            );
            Ok(crate::tikv::EvaluatedArgs::Bytes3([s, from, to]))
        },
        |result| {
            Ok(result
                .into_bytes()?
                .map_or(Datum::Null, |bytes| string_result(value, bytes)))
        },
    )
}

/// Preserve binary result semantics when the source string is binary; normal
/// SQL strings retain the ordinary String datum/collation boundary.
fn string_result(source: &Datum, bytes: Vec<u8>) -> Datum {
    if matches!(source, Datum::Bytes(_)) {
        Datum::new_bytes(bytes)
    } else {
        Datum::new_string(bytes)
    }
}

/// `STRCMP(a, b)`: `-1`/`0`/`1` under TiDB's default `utf8mb4_bin` PAD SPACE
/// collation; a binary operand switches the source signature to the raw
/// binary collation. `NULL` propagates from either operand.
#[cfg(test)]
pub(crate) fn strcmp(vals: &[Datum]) -> Result<Datum, EvalError> {
    strcmp_in(vals, &crate::NoColumns)
}

pub(crate) fn strcmp_in(vals: &[Datum], ctx: &dyn crate::Columns) -> Result<Datum, EvalError> {
    let [left, right] = vals else {
        return Err(EvalError::Unsupported("bad STRCMP arity"));
    };
    let collation = if matches!(left, Datum::Bytes(_)) || matches!(right, Datum::Bytes(_)) {
        tidb_datatype::Collation::Binary
    } else {
        left.collation()
            .or_else(|| right.collation())
            .unwrap_or(tidb_datatype::Collation::DEFAULT)
    };
    strcmp_with_collation_in(vals, collation, ctx)
}

/// `STRCMP` under the collation the expression derivation aggregated
/// (Go `builtinStrcmpSig`, which compares with `b.collator`). Captured from
/// TiDB: `STRCMP('a' COLLATE utf8mb4_general_ci, 'A' COLLATE
/// utf8mb4_general_ci)` is 0 where the `utf8mb4_bin` form is 1.
pub(crate) fn strcmp_with_collation_in(
    vals: &[Datum],
    collation: tidb_datatype::Collation,
    ctx: &dyn crate::Columns,
) -> Result<Datum, EvalError> {
    let [left, right] = vals else {
        return Err(EvalError::Unsupported("bad STRCMP arity"));
    };
    crate::tikv::evaluate_args_in(
        crate::tikv::EvaluatedBytesOp::StrcmpNative,
        ctx,
        || {
            Ok(crate::tikv::EvaluatedArgs::CollatedBytes2 {
                left: coerce_str_bytes(left)?,
                right: coerce_str_bytes(right)?,
                collation: collation.native_policy(),
            })
        },
        crate::tikv::EvaluatedBytesResult::into_int_datum,
    )
}

/// `HEX(x)`: renders a numeric argument's implicit-integer bits as uppercase
/// base-16 (`HEX(12.8)` = `D`, `HEX(-1)` = `FFFFFFFFFFFFFFFF`), or renders a
/// string/binary-literal argument's bytes (`HEX('abc')` = `616263`).
///
/// This is deliberately a type-directed split, rather than formatting every
/// value through `coerce_str`: TiDB's `TestHexFunc` proves that decimal and
/// real values use the integer signature while ETString values retain their
/// original bytes. Reusing `radix_integer_bits` keeps every numeric rounding
/// and two's-complement edge on one source-defined conversion path.
///
/// # A `BIT` COLUMN is an integer argument, a bit LITERAL is not
///
/// `hexFunctionClass.getFunction` switches on `args[0].GetType().EvalType()`,
/// and `mysql.TypeBit`'s eval type is `types.ETInt` -- so a `BIT(48)` column
/// is hexed through `builtinHexIntArgSig`, which formats the VALUE and
/// therefore drops the storage width's leading zero bytes. A bit LITERAL
/// (`b'01000001'`) is a different type: Go stores it as `KindBinaryLiteral`
/// typed `mysql.TypeVarString`, so it takes the string branch, and it only
/// LOOKS like the integer answer because a literal carries no padding.
/// Measured through `gorun` on `bit(48)` holding `0x00080A0D091A`:
///
/// ```text
/// hex(b)          -> 80A0D091A     (int branch: value, no leading zeros)
/// hex(concat(b))  -> 000000000041  (string branch on bit(48) x'0041')
/// hex(x'0041')    -> 0041          (hex literal keeps both bytes)
/// hex(b'01000001')-> 41            (bit literal is one byte to begin with)
/// ```
#[cfg(test)]
pub(crate) fn hex(vals: &[Datum]) -> Result<Datum, EvalError> {
    hex_in(vals, &crate::NoColumns)
}

pub(crate) fn hex_in(vals: &[Datum], ctx: &dyn crate::Columns) -> Result<Datum, EvalError> {
    match &vals[0] {
        Datum::Null => hex_string_value(&vals[0], ctx),
        Datum::Int(_)
        | Datum::UInt(_)
        | Datum::Decimal(_)
        | Datum::Real(_)
        | Datum::Float32(_)
        | Datum::Bit(_) => hex_int_value(&vals[0], ctx),
        Datum::String(_)
        | Datum::Bytes(_)
        | Datum::BinaryLiteral(_)
        | Datum::Duration(_)
        | Datum::Enum(_, _)
        | Datum::Set(_, _)
        | Datum::Time(_)
        | Datum::Json(_) => hex_string_value(&vals[0], ctx),
        Datum::Raw(value) => hex_bytes_in(ctx, || Ok(Some(value.clone()))),
        Datum::MinNotNull | Datum::MaxValue => {
            Err(EvalError::Unsupported("range sentinel HEX argument"))
        }
        other => hex_bytes_in(ctx, || {
            other
                .to_bytes()
                .map(Some)
                .map_err(|_| EvalError::Unsupported("HEX argument conversion"))
        }),
    }
}

fn hex_int_value(value: &Datum, ctx: &dyn crate::Columns) -> Result<Datum, EvalError> {
    crate::tikv::evaluate_args_in(
        crate::tikv::EvaluatedBytesOp::HexInt,
        ctx,
        || {
            Ok(crate::tikv::EvaluatedArgs::Int(
                radix_integer_bits(value)?.map(|bits| bits as i64),
            ))
        },
        |result| Ok(result.into_bytes()?.map_or(Datum::Null, Datum::new_string)),
    )
}

fn hex_string_value(value: &Datum, ctx: &dyn crate::Columns) -> Result<Datum, EvalError> {
    hex_bytes_in(ctx, || coerce_str_bytes(value))
}

fn hex_bytes_in(
    ctx: &dyn crate::Columns,
    coerce: impl FnOnce() -> Result<Option<Vec<u8>>, EvalError>,
) -> Result<Datum, EvalError> {
    crate::tikv::evaluate_args_in(
        crate::tikv::EvaluatedBytesOp::HexStr,
        ctx,
        || Ok(crate::tikv::EvaluatedArgs::Bytes(coerce()?)),
        |result| Ok(result.into_bytes()?.map_or(Datum::Null, Datum::new_string)),
    )
}

/// Go `hexFunctionClass.getFunction`: select the string or integer signature
/// from the argument's declared eval type, then apply the corresponding
/// build-time argument cast before evaluating the body.
pub(crate) fn hex_with_type(
    vals: &[Datum],
    arg_type: Option<&FieldType>,
    ctx: &dyn crate::Columns,
) -> Result<Datum, EvalError> {
    use tidb_datatype::EvalType;

    let Some(arg_type) = arg_type else {
        return hex_in(vals, ctx);
    };
    let [value] = vals else {
        return Err(EvalError::Unsupported("bad HEX arity"));
    };
    match arg_type.eval_type() {
        EvalType::String
        | EvalType::Datetime
        | EvalType::Timestamp
        | EvalType::Duration
        | EvalType::Json => {
            let value = crate::cast::cast_arg_as_string(value, Some(arg_type), ctx)?;
            hex_string_value(&value, ctx)
        }
        EvalType::Int | EvalType::Real | EvalType::Decimal => {
            let value = crate::cast::cast_arg_as_int(value, Some(arg_type), ctx)?;
            hex_int_value(&value, ctx)
        }
        _ => Err(EvalError::Unsupported("HEX argument eval type")),
    }
}

/// `UNHEX(s)`: the inverse of `HEX` for a string — each hex pair becomes one
/// byte in a binary datum (`UNHEX('4D7953514C')` carries `MySQL` bytes). An odd number of
/// digits gets an implicit leading zero (`UNHEX('126')` = bytes `01 26`), as
/// `builtinUnHexSig` does. `NULL` only when a digit is invalid; arbitrary
/// decoded octets remain representable. The source signature is `ETString`,
/// which is a byte-preserving Go string boundary, so malformed input bytes
/// also follow the normal invalid-hex `NULL` path rather than becoming a Rust
/// UTF-8 decoding error.
#[cfg(test)]
pub(crate) fn unhex(vals: &[Datum]) -> Result<Datum, EvalError> {
    unhex_in(vals, &crate::NoColumns)
}

pub(crate) fn unhex_in(vals: &[Datum], ctx: &dyn crate::Columns) -> Result<Datum, EvalError> {
    crate::tikv::evaluate_bytes_in(
        crate::tikv::EvaluatedBytesOp::UnHex,
        ctx,
        || coerce_str_bytes(&vals[0]),
        |result| Ok(result.into_bytes()?.map_or(Datum::Null, Datum::new_bytes)),
    )
}

/// `BIN(n)`: the base-2 form of an implicitly-cast integer as an unsigned
/// 64-bit value.  TiDB's `binFunctionClass` requests `ETInt`, so strings use
/// their leading integer run, decimals round half-up, and reals round
/// ties-to-even before their resulting two's-complement bits are formatted.
pub(crate) fn bin(vals: &[Datum], ctx: &dyn crate::Columns) -> Result<Datum, EvalError> {
    if vals.len() != 1 {
        return Err(EvalError::Unsupported("bad BIN arity"));
    }
    crate::tikv::evaluate_args_in(
        crate::tikv::EvaluatedBytesOp::Bin,
        ctx,
        || {
            let raw: Option<&[u8]> = match &vals[0] {
                Datum::String(text) => Some(text.bytes()),
                Datum::Bytes(text) => Some(text),
                _ => None,
            };
            if let Some(text) = raw {
                if radix_truncated(text) {
                    ctx.append_warning(
                        1292,
                        &format!(
                            "Truncated incorrect INTEGER value: '{}'",
                            String::from_utf8_lossy(text)
                        ),
                    );
                }
            }
            Ok(crate::tikv::EvaluatedArgs::Int(
                radix_integer_bits(&vals[0])?.map(|bits| bits as i64),
            ))
        },
        |result| Ok(result.into_bytes()?.map_or(Datum::Null, Datum::new_string)),
    )
}

/// `OCT(n)`: the base-8 form of an integer as an unsigned 64-bit value.
/// Go has two signatures here: the integer path renders the raw `u64` bits,
/// while every non-integer source first goes through `builtinOctStringSig`.
/// That second path is deliberately *not* the `BIN` integer cast: an empty
/// original string is `NULL`, a whitespace-only/invalid nonempty string is
/// zero, and a decimal-prefix overflow stays at `u64::MAX` even if it had a
/// leading minus sign.  Keeping that conversion local makes those cases the
/// normal signature split rather than test-specific exceptions.
///
/// The split is Go's own build-time test, and it is a SIGNATURE choice rather
/// than an argument cast, which is why `OCT` is not a member of
/// `crate::arg_eval_type`'s `types.ETInt` layer:
///
/// ```go
/// if IsBinaryLiteral(args[0]) || args[0].GetType(ctx.GetEvalCtx()).EvalType() == types.ETInt {
/// ```
///
/// (`builtin_string.go:3005`). `IsBinaryLiteral` is `con.Value.Kind() ==
/// types.KindBinaryLiteral`, this tier's [`Datum::BinaryLiteral`], and
/// `mysql.TypeBit`'s eval type is `types.ETInt`, this tier's [`Datum::Bit`];
/// both therefore take the INTEGER signature and render their big-endian
/// value. ENUM and SET do NOT -- their eval type is `types.ETString` until
/// something adds `EnumSetAsIntFlag`, and `OCT` never does. Captured from
/// real TiDB (`gorun`) over `enum('x','y','z')` holding `'y'`,
/// `set('a','b','c')` holding `'a,c'` and `bit(8)` holding `b'00000011'`:
/// `oct(e)` is `0` and `oct(s)` is `0` (the STRINGS parsed), while `oct(b)`
/// is `3` and `oct(b'01000001')` is `101`.
pub(crate) fn oct(vals: &[Datum]) -> Result<Datum, EvalError> {
    match &vals[0] {
        Datum::Null => Ok(Datum::Null),
        Datum::Int(value) => Ok(Datum::new_string(format!("{:o}", *value as u64))),
        Datum::UInt(value) => Ok(Datum::new_string(format!("{value:o}"))),
        Datum::Bit(bits) | Datum::BinaryLiteral(bits) => {
            Ok(Datum::new_string(format!("{:o}", bits.to_int().value())))
        }
        value => Ok(oct_string_bits(value)?
            .map_or(Datum::Null, |bits| Datum::new_string(format!("{bits:o}")))),
    }
}

/// The `ETString` conversion inside Go's `builtinOctStringSig`.
/// `getValidPrefix(..., 10)` retains only a leading sign and decimal digits;
/// `strconv.ParseUint` returns `MaxUint64` together with `ErrRange` on
/// overflow.  A negative in-range magnitude is negated in the `u64` domain,
/// but a negative overflow is intentionally left as `MaxUint64` by the Go
/// implementation.
fn oct_string_bits(value: &Datum) -> Result<Option<u64>, EvalError> {
    let Some(value) = coerce_str_bytes(value)? else {
        return Ok(None);
    };
    if value.is_empty() {
        return Ok(None);
    }
    // Go's `EvalString` and `strings.TrimSpace` operate on byte strings. A
    // valid UTF-8 value can use Rust's Unicode-equivalent `trim`; malformed
    // bytes cannot be decoded, so trim the ASCII whitespace that Go also
    // recognizes around such a payload and leave every other octet intact.
    let value = trim_go_space(&value);
    let prefix_len = value
        .iter()
        .enumerate()
        .take_while(|(index, byte)| {
            byte.is_ascii_digit() || (*index == 0 && matches!(byte, b'+' | b'-'))
        })
        .map(|(index, _)| index + 1)
        .last()
        .unwrap_or(0);
    let prefix = &value[..prefix_len];
    let prefix = prefix.strip_prefix(b"+").unwrap_or(prefix);
    let (negative, digits) = match prefix.strip_prefix(b"-") {
        Some(digits) => (true, digits),
        None => (false, prefix),
    };
    if digits.is_empty() {
        return Ok(Some(0));
    }
    let digits = std::str::from_utf8(digits).expect("OCT decimal prefix is ASCII");
    let (bits, overflow) = match digits.parse::<u64>() {
        Ok(bits) => (bits, false),
        Err(_) => (u64::MAX, true),
    };
    Ok(Some(if negative && !overflow {
        bits.wrapping_neg()
    } else {
        bits
    }))
}

/// Trims the same source-visible whitespace around an `ETString` value that
/// `strings.TrimSpace` sees. Keeping this byte-preserving matters for values
/// such as `OCT(UNHEX('FF'))`: Go treats the invalid leading byte as an empty
/// numeric prefix and returns zero, whereas a checked UTF-8 conversion would
/// incorrectly raise an evaluator error before `getValidPrefix` runs.
fn trim_go_space(bytes: &[u8]) -> &[u8] {
    if let Ok(text) = std::str::from_utf8(bytes) {
        return text.trim().as_bytes();
    }
    let mut start = 0;
    let mut end = bytes.len();
    while start < end && bytes[start].is_ascii_whitespace() {
        start += 1;
    }
    while end > start && bytes[end - 1].is_ascii_whitespace() {
        end -= 1;
    }
    &bytes[start..end]
}

/// `BIN`'s argument path.  This mirrors
/// `newBaseBuiltinFuncWithTp(..., ETInt)` in Go: a non-negative string is
/// parsed through `StrToUint` and its low 64 bits are retained, whereas a
/// negative string follows the signed path.  The seed has no warning surface,
/// so truncation/range warnings become the same clamped result TiDB produces
/// with `IgnoreTruncateErr` in `TestBin`.
fn radix_integer_bits(value: &Datum) -> Result<Option<u64>, EvalError> {
    match value {
        Datum::Null => Ok(None),
        Datum::Int(value) => Ok(Some(*value as u64)),
        Datum::UInt(value) => Ok(Some(*value)),
        Datum::Decimal(value) => value
            .round_to_i64()
            .map(|value| Some(value as u64))
            .ok_or(EvalError::IntOverflow),
        Datum::Real(value) => Ok(Some(round_float_to_i64_saturating(*value) as u64)),
        Datum::String(value) => Ok(Some(radix_string_bits_bytes(value.bytes()))),
        Datum::Bytes(value) => Ok(Some(radix_string_bits_bytes(value))),
        // A `BIT` column reaches the integer signatures through
        // `WrapWithCastAsInt` over `mysql.TypeBit`, which is `BinaryLiteral.
        // ToInt`: the big-endian payload read as an UNSIGNED 64-bit value.
        // Going through `to_i64` instead would refuse `bit(64)` values with
        // the high bit set, which Go answers as `FFFFFFFFFFFFFFFF`.
        Datum::Bit(value) => Ok(Some(value.to_int().value())),
        Datum::MinNotNull | Datum::MaxValue => {
            Err(EvalError::Unsupported("range sentinel integer argument"))
        }
        other => other
            .to_i64()
            .map(|converted| Some(converted.value as u64))
            .map_err(|_| EvalError::Unsupported("integer argument conversion")),
    }
}

/// `builtinCastStringAsIntSig` selects its signed path only when the trimmed
/// input has a `-` followed by at least one byte.  The other path accepts an
/// optional `+`, parses the leading ASCII digits, and preserves all `u64`
/// bits.  Invalid/trailing text only emits a warning in TiDB's test context,
/// so it leaves the valid prefix (or zero) intact.
/// go `StrToInt`/`StrToUint`: the cast warns `Truncated incorrect INTEGER
/// value` when the text carries anything the numeric parse could not
/// consume — a remainder after the digit run, or no digits at all.
fn radix_truncated(value: &[u8]) -> bool {
    let value = trim_go_space(value);
    let digits = if value.len() > 1 && value.starts_with(b"-") {
        &value[1..]
    } else {
        value.strip_prefix(b"+").unwrap_or(value)
    };
    let digit_len = digits
        .iter()
        .take_while(|byte| byte.is_ascii_digit())
        .count();
    digit_len < digits.len()
}

fn radix_string_bits_bytes(value: &[u8]) -> u64 {
    let value = trim_go_space(value);
    let negative = value.len() > 1 && value.starts_with(b"-");
    let digits = if negative {
        &value[1..]
    } else {
        value.strip_prefix(b"+").unwrap_or(value)
    };
    let digit_len = digits
        .iter()
        .take_while(|byte| byte.is_ascii_digit())
        .count();
    let digits = &digits[..digit_len];
    if digits.is_empty() {
        return 0;
    }
    let magnitude = digits.iter().try_fold(0_u64, |value, digit| {
        value
            .checked_mul(10)
            .and_then(|value| value.checked_add(u64::from(*digit - b'0')))
    });
    let magnitude = magnitude.unwrap_or(u64::MAX);
    if negative {
        if magnitude >= 1_u64 << 63 {
            i64::MIN as u64
        } else {
            (-(magnitude as i64)) as u64
        }
    } else {
        magnitude
    }
}

/// `FIELD(needle, a, b, c, ...)`: the 1-based index of the first argument
/// equal to `needle`, or `0` if none match. A `NULL` `needle` never matches
/// (returns `0`). The Go function class chooses ONE signature for the whole
/// argument list before evaluation: all-string arguments use the collator,
/// all-integer arguments use integer equality, and every mixed/decimal/real
/// list uses `EvalReal` for every argument. Selecting that mode once is
/// important: pairwise equality would compare a string/string pair as text
/// even when a numeric argument later forces the source's REAL signature.
///
/// The collation-free entry point, for the AST evaluator and any caller with
/// no derived collation to offer; see [`field_with_collation`].
pub(crate) fn field(vals: &[Datum], ctx: &dyn crate::Columns) -> Result<Datum, EvalError> {
    field_with_collation(vals, crate::ops::DERIVATION_FREE_COLLATION, ctx)
}

/// [`field`] under the collation the expression derivation aggregated over
/// ALL of `FIELD`'s arguments (Go `deriveCollation`'s `ast.Field` arm, taken
/// when the argument list is all-string).
///
/// Go's `builtinFieldStringSig.evalInt` tests `b.ctor.Compare(str, stri) == 0`
/// -- the function's own collator, not a fixed byte comparison -- so a
/// case-folding collation matches a differently-cased candidate. Captured from
/// TiDB: `FIELD('ABC' COLLATE utf8mb4_general_ci, 'abc')` is 1 where the
/// `utf8mb4_bin` form is 0.
pub(crate) fn field_with_collation(
    vals: &[Datum],
    collation: tidb_datatype::Collation,
    ctx: &dyn crate::Columns,
) -> Result<Datum, EvalError> {
    if vals[0] == Datum::Null {
        return Ok(Datum::Int(0));
    }
    // Go's two flags, verbatim (`builtin_string.go:2774-2776`):
    //
    // ```go
    // argTp := args[i].GetType(ctx.GetEvalCtx()).EvalType()
    // isAllString = isAllString && (argTp == types.ETString)
    // isAllNumber = isAllNumber && (argTp == types.ETInt)
    // ```
    //
    // so the membership question is `FieldType.EvalType()`, not the datum's
    // kind -- and that switch (`pkg/parser/types/field_type.go:417-441`) puts
    // ENUM and SET under `types.ETString` (they reach the signature as their
    // NAME) and `mysql.TypeBit` under `types.ETInt`. Captured from real TiDB
    // (`gorun`) over `enum('a','b','c')` holding `'b'` and `set('a','b')`
    // holding `'a,b'`: `field(e,'b')` and `field(s,'a,b')` are both `1`, and
    // `field(x'61','a')` is `1`; this tier answered `0` to all three while
    // the hybrids fell through to the REAL signature, which compares an
    // enum's ORDINAL. The mixed lists still do exactly that, and correctly:
    // `field(e,2)` is `1` and `field(e,b)` is `0` in both engines.
    //
    // `Datum::Null` stays a member of BOTH arms. Go's `mysql.TypeNull`
    // answers `types.ETString`, so a NULL LITERAL would be string-only there
    // -- but this tier cannot tell a NULL literal from a NULL-valued INT
    // column, and reading every NULL as string-typed would push
    // `field(int_col, null_int_col, ...)` onto the REAL signature, which
    // loses integers past 2^53. Neutral is the reading that is right whenever
    // the argument's own type is what Go looked at.
    let mode = if vals.iter().all(|value| {
        matches!(
            value,
            Datum::Null
                | Datum::String(_)
                | Datum::Bytes(_)
                | Datum::Enum(..)
                | Datum::Set(..)
                | Datum::BinaryLiteral(_)
        )
    }) {
        FieldComparisonMode::String
    } else if vals.iter().all(|value| {
        matches!(
            value,
            Datum::Null | Datum::Int(_) | Datum::UInt(_) | Datum::Bit(_)
        )
    }) {
        FieldComparisonMode::Integer
    } else {
        FieldComparisonMode::Real
    };
    // The real signature coerces the needle ONCE, before the scan: Go's
    // `builtinFieldRealSig.evalInt` evaluates `args[0]` a single time, so
    // `FIELD('12abc', 1, 2)` records exactly ONE 1292 (captured) no matter how
    // many candidates follow. Coercing it inside the loop repeated the
    // warning per candidate.
    let needle = match mode {
        FieldComparisonMode::Real => Some(Datum::Real(to_f64_with_mysql_string(&vals[0], ctx)?)),
        FieldComparisonMode::String | FieldComparisonMode::Integer => None,
    };
    for (i, v) in vals[1..].iter().enumerate() {
        if *v == Datum::Null {
            continue;
        }
        let equal = match mode {
            // Go compares the two evaluated strings through the signature's
            // own collator, which is where a `_ci` collation folds case and a
            // PAD SPACE one ignores trailing blanks.
            FieldComparisonMode::String => {
                // `builtinFieldStringSig.evalInt` is `b.args[i].EvalString`,
                // which is `crate::arg_eval_type::eval_string` -- the same
                // reader that gives an ENUM its NAME, and the reason this arm
                // admits the hybrids the mode test above just let in.
                let (Some(needle), Some(candidate)) = (
                    crate::arg_eval_type::eval_string(&vals[0])?,
                    crate::arg_eval_type::eval_string(v)?,
                ) else {
                    return Err(EvalError::Unsupported("non-string FIELD string operand"));
                };
                collation.compare(&needle, &candidate) == std::cmp::Ordering::Equal
            }
            FieldComparisonMode::Integer => {
                crate::eval_binary(tidb_ast::BinaryOp::Eq, vals[0].clone(), v.clone())?
                    == Datum::Int(1)
            }
            FieldComparisonMode::Real => {
                let needle = needle.clone().expect("coerced above for this mode");
                let candidate = Datum::Real(to_f64_with_mysql_string(v, ctx)?);
                crate::eval_binary(tidb_ast::BinaryOp::Eq, needle, candidate)? == Datum::Int(1)
            }
        };
        if equal {
            return Ok(Datum::Int(i as i64 + 1));
        }
    }
    Ok(Datum::Int(0))
}

#[derive(Clone, Copy)]
enum FieldComparisonMode {
    String,
    Integer,
    Real,
}

/// `ELT(n, a, b, c, ...)`: the `n`-th (1-based) following argument, or
/// `NULL` if `n` is out of range or `NULL`.
///
/// Go declares `argTps[0] = types.ETInt` and `types.ETString` for every
/// following position (`builtin_string.go:3305-3309`), so both readings below
/// are `crate::arg_eval_type`'s, not this body's -- `builtinEltSig.evalString`
/// is `b.args[0].EvalInt` then `b.args[idx].EvalString` and nothing else.
/// Reading the selected argument as BYTES is what the routing bought:
/// captured from real TiDB (`gorun`), `hex(elt(1,v))` over a `varbinary`
/// holding `0xFF` is `FF`, where the previous UTF-8 coercion here raised a
/// hard error.
///
/// The result charset is Go's `if types.IsBinaryStr(argType) {
/// types.SetBinChsClnFlag(bf.tp) }` over `args[1:]` (`:3314-3318`): ANY
/// binary candidate makes the whole function binary, not just the selected
/// one.
pub(crate) fn elt(vals: &[Datum]) -> Result<Datum, EvalError> {
    let Some(index) = crate::arg_eval_type::eval_int(&vals[0])? else {
        return Ok(Datum::Null);
    };
    if index < 1 || index as usize >= vals.len() {
        return Ok(Datum::Null);
    }
    let Some(selected) = crate::arg_eval_type::eval_string(&vals[index as usize])? else {
        return Ok(Datum::Null);
    };
    Ok(
        if vals[1..].iter().any(crate::string_signature::is_binary_str) {
            Datum::new_bytes(selected)
        } else {
            Datum::new_string(selected)
        },
    )
}

/// `CONCAT_WS(sep, a, b, ...)`: joins the non-`NULL` arguments with `sep`.
/// Unlike `CONCAT`, a `NULL` argument is SKIPPED (not propagated); only a
/// `NULL` separator yields `NULL` (confirmed via `gorun`).
#[cfg(test)]
pub(crate) fn concat_ws(vals: &[Datum]) -> Result<Datum, EvalError> {
    concat_ws_with_context(vals, &crate::context::NoColumns)
}

/// Value-evaluated CONCAT_WS with the statement packet limit applied.
pub(crate) fn concat_ws_with_context(
    vals: &[Datum],
    ctx: &dyn crate::context::Columns,
) -> Result<Datum, EvalError> {
    if vals.len() < 2 {
        return Err(EvalError::Unsupported(
            "CONCAT_WS requires at least two arguments",
        ));
    }
    let Some(sep) = coerce_str_bytes(&vals[0])? else {
        return Ok(Datum::Null);
    };
    let mut out = Vec::new();
    let mut have_part = false;
    let mut target_len = 0_u64;
    for (index, value) in vals[1..].iter().enumerate() {
        let Some(value) = coerce_str_bytes(value)? else {
            continue;
        };
        target_len = target_len.saturating_add(value.len() as u64);
        if index > 0 {
            target_len = target_len.saturating_add(sep.len() as u64);
        }
        if target_len > ctx.max_allowed_packet() {
            ctx.handle_allowed_packet_overflowed("concat_ws")?;
            return Ok(Datum::Null);
        }
        if have_part {
            out.extend_from_slice(&sep);
        }
        out.extend_from_slice(&value);
        have_part = true;
    }
    Ok(Datum::new_string(out))
}

/// `SUBSTRING_INDEX(str, delim, count)`: the substring before the `count`-th
/// occurrence of `delim` — from the left for `count > 0`, from the right for
/// `count < 0` (`SUBSTRING_INDEX('a.b.c.d', '.', -2)` = `c.d`). `count = 0`
/// yields the empty string; `|count|` past the number of parts yields the
/// whole string. `NULL` if any argument is `NULL`.
#[cfg(test)]
pub(crate) fn substring_index(vals: &[Datum]) -> Result<Datum, EvalError> {
    substring_index_in(vals, &crate::NoColumns)
}

pub(crate) fn substring_index_in(
    vals: &[Datum],
    ctx: &dyn crate::Columns,
) -> Result<Datum, EvalError> {
    let [value, delim_value, count_value] = vals else {
        return Err(EvalError::Unsupported("bad SUBSTRING_INDEX arity"));
    };
    let operation = if matches!(count_value, Datum::UInt(_)) {
        crate::tikv::EvaluatedBytesOp::SubstringIndexUnsignedNative
    } else {
        crate::tikv::EvaluatedBytesOp::SubstringIndexSignedNative
    };
    crate::tikv::evaluate_args_in(
        operation,
        ctx,
        || {
            // Tuple coercion still demands the delimiter when the source is
            // NULL, and still stops on a source coercion error.
            let (bytes, delimiter) = (coerce_str_bytes(value)?, coerce_str_bytes(delim_value)?);
            let count = if count_value == &Datum::Null {
                crate::tikv::ReadyIntArg::Value(None)
            } else if bytes.is_none()
                || delimiter.is_none()
                || delimiter.as_ref().is_some_and(Vec::is_empty)
            {
                crate::tikv::ReadyIntArg::Undemanded
            } else {
                // The unsigned operation retains the original UInt bits.
                crate::tikv::ReadyIntArg::Value(Some(crate::cast::to_i64_signed(count_value)))
            };
            Ok(crate::tikv::EvaluatedArgs::BytesBytesIntReady {
                bytes,
                delimiter,
                count,
            })
        },
        |computed| {
            Ok(computed
                .into_bytes()?
                .map_or(Datum::Null, |bytes| string_result(value, bytes)))
        },
    )
}

/// `INSERT(str, pos, len, newstr)`: replaces the `len` characters of `str`
/// starting at 1-based character `pos` with `newstr`. A `pos` outside
/// `1..=len(str)` leaves `str` unchanged (matching MySQL). `NULL` if any
/// argument is `NULL`.
///
/// A NEGATIVE `len` means "through the end of the string", exactly as an
/// oversized one does -- `builtinInsertUTF8Sig.evalString` clamps both with the
/// single condition `length > runeLength-pos+1 || length < 0`. Reading it as
/// zero instead would splice `newstr` in without removing anything.
pub(crate) fn str_insert(vals: &[Datum], ctx: &dyn crate::Columns) -> Result<Datum, EvalError> {
    // `insertFunctionClass.getFunction` selects `builtinInsertSig` (bytes) or
    // `builtinInsertUTF8Sig` (characters) from the RESULT type's charset,
    // which `addBinFlag` makes binary when either string argument is, so
    // `pos` and `len` count bytes as soon as the replacement is binary.
    let binary = crate::string_signature::is_binary_str(&vals[0])
        || crate::string_signature::is_binary_str(&vals[3]);
    // `pos` and `len` are Go's `types.ETInt` arguments, cast by
    // `crate::arg_eval_type`; each body below reads the `int64` carrier
    // `b.args[i].EvalInt` returns, so an UNSIGNED position keeps its bits
    // rather than turning the whole call NULL.
    let operation = if binary {
        crate::tikv::EvaluatedBytesOp::Insert
    } else {
        crate::tikv::EvaluatedBytesOp::InsertUtf8Native
    };
    crate::tikv::evaluate_args_in(
        operation,
        ctx,
        || {
            // Preserve the original tuple's full demand and error order.
            // Only source text gets Go's per-malformed-byte normalization;
            // the replacement is raw bytes even for a UTF-8 signature.
            let (bytes, pos, len, replacement) = (
                coerce_str_bytes(&vals[0])?.map(|bytes| {
                    if binary {
                        bytes
                    } else {
                        crate::string_signature::normalize_utf8_go(bytes)
                    }
                }),
                crate::arg_eval_type::eval_int(&vals[1])?,
                crate::arg_eval_type::eval_int(&vals[2])?,
                coerce_str_bytes(&vals[3])?,
            );
            Ok(crate::tikv::EvaluatedArgs::BytesIntIntBytes(
                bytes,
                pos,
                len,
                replacement,
            ))
        },
        |computed| {
            Ok(computed.into_bytes()?.map_or(Datum::Null, |bytes| {
                if binary {
                    Datum::new_bytes(bytes)
                } else {
                    Datum::new_string(bytes)
                }
            }))
        },
    )
}

/// `MAKE_SET(bits, a, b, c, ...)`: a comma-joined set of the arguments whose
/// 1-based position has the corresponding bit set in `bits` (arg `i` is
/// included when bit `i-1` of `bits` is `1`). `NULL` arguments are excluded
/// even when their bit is set (matching MySQL). `NULL` if `bits` is `NULL`.
pub(crate) fn make_set(vals: &[Datum]) -> Result<Datum, EvalError> {
    // `bits` is Go's `types.ETInt` argument, cast by `crate::arg_eval_type`
    // before this body runs, so a SET/ENUM/BIT operand reaches here as the
    // ordinal integer Go's `builtinCastIntAsIntSig` reads out of it -- and
    // `1|4`'s unsigned bitwise OR keeps its raw 64-bit pattern, the same way
    // `bit_count` above does.
    let Some(bits) = crate::arg_eval_type::eval_int(&vals[0])? else {
        return Ok(Datum::Null);
    };
    let bits = bits as u64;
    let mut parts = Vec::new();
    for (i, v) in vals[1..].iter().enumerate() {
        if bits & (1 << i) != 0 {
            if let Some(s) = coerce_str(v)? {
                parts.push(s);
            }
        }
    }
    Ok(Datum::new_string(parts.join(",")))
}
/// Go `EXPORT_SET(bits, on, off[, separator[, number_of_bits]])`
/// (`builtin_string.go:3403`/`:3434-3542`): bit 0 first, `separator`
/// between entries. The three-argument form uses `,` and 64 bits; the
/// four-argument form uses the given separator with 64 bits; the
/// five-argument form clamps a `number_of_bits` outside 0..=64 to 64. Any
/// `NULL` argument yields `NULL`.
pub(crate) fn export_set(vals: &[Datum]) -> Result<Datum, EvalError> {
    if !(3..=5).contains(&vals.len()) {
        return Err(EvalError::Unsupported("bad EXPORT_SET arguments"));
    }
    if vals.iter().any(|v| matches!(v, Datum::Null)) {
        return Ok(Datum::Null);
    }
    let bits = crate::arg_eval_type::eval_int(&vals[0])?.unwrap_or(0);
    let Some(on) = crate::arg_eval_type::eval_string(&vals[1])? else {
        return Ok(Datum::Null);
    };
    let Some(off) = crate::arg_eval_type::eval_string(&vals[2])? else {
        return Ok(Datum::Null);
    };
    let on = String::from_utf8_lossy(&on).into_owned();
    let off = String::from_utf8_lossy(&off).into_owned();
    let (separator, number_of_bits) = match vals.len() {
        3 => (",".to_owned(), 64),
        4 => {
            let Some(separator) = crate::arg_eval_type::eval_string(&vals[3])? else {
                return Ok(Datum::Null);
            };
            (String::from_utf8_lossy(&separator).into_owned(), 64)
        }
        _ => {
            let Some(separator) = crate::arg_eval_type::eval_string(&vals[3])? else {
                return Ok(Datum::Null);
            };
            let separator = String::from_utf8_lossy(&separator).into_owned();
            let number = crate::arg_eval_type::eval_int(&vals[4])?.unwrap_or(64);
            let number_of_bits = if !(0..=64).contains(&number) {
                64
            } else {
                number
            };
            (separator, number_of_bits)
        }
    };
    let mut result = String::new();
    for i in 0..number_of_bits {
        if bits & (1 << i) > 0 {
            result.push_str(&on);
        } else {
            result.push_str(&off);
        }
        if i < number_of_bits - 1 {
            result.push_str(&separator);
        }
    }
    Ok(Datum::new_string(result))
}

/// `FROM_BASE64(str)`: inverse of [`to_base64`], ported from
/// `builtinFromBase64Sig.evalString` in `pkg/expression/builtin_string.go`.
/// TiDB removes spaces/tabs before calling Go's `StdEncoding.DecodeString`,
/// whose decoder also ignores CR/LF.  The result is binary string data, so
/// invalid UTF-8 is preserved rather than replaced or turned into NULL.
#[cfg(test)]
pub(crate) fn from_base64(vals: &[Datum]) -> Result<Datum, EvalError> {
    from_base64_in(vals, &crate::NoColumns)
}

/// Value-only decoding retains the caller's execution capability without
/// consulting the statement packet limit or applying its raw-length guard.
pub(crate) fn from_base64_in(vals: &[Datum], ctx: &dyn crate::Columns) -> Result<Datum, EvalError> {
    if vals.len() != 1 {
        return Err(EvalError::Unsupported("FROM_BASE64 arity"));
    }
    crate::tikv::evaluate_args_in(
        crate::tikv::EvaluatedBytesOp::FromBase64ValueNative,
        ctx,
        || {
            Ok(crate::tikv::EvaluatedArgs::Bytes(coerce_str_bytes(
                &vals[0],
            )?))
        },
        |result| Ok(result.into_bytes()?.map_or(Datum::Null, Datum::new_bytes)),
    )
}

/// Evaluates `FROM_BASE64` with Go's signature-level packet bound. The Go
/// builtin checks the decoded-size estimate before stripping spaces/tabs and
/// invoking the decoder; keeping that check in the context-aware entry point
/// lets both the AST and chunk evaluators share the same warning/NULL policy
/// while the value-only helper remains useful for datatype-style tests.
pub(crate) fn from_base64_with_packet_limit(
    vals: &[Datum],
    ctx: &dyn crate::Columns,
) -> Result<Datum, EvalError> {
    if vals.len() != 1 {
        return Err(EvalError::Unsupported("FROM_BASE64 arity"));
    }
    crate::tikv::evaluate_args_in(
        crate::tikv::EvaluatedBytesOp::FromBase64Native,
        ctx,
        || {
            let value = coerce_str_bytes(&vals[0])?;
            let mut disposition = crate::tikv::OutputDisposition::Allow;
            if let Some(input) = value.as_ref() {
                // The oversized-input guard belongs to C4. Only skip the
                // packet getter here, retaining the original bytes and Allow.
                if input.len() <= (isize::MAX as usize) / 3
                    && (input.len() * 3 / 4) as u64 > ctx.max_allowed_packet()
                {
                    ctx.handle_allowed_packet_overflowed("from_base64")?;
                    disposition = crate::tikv::OutputDisposition::SuppressByPacket;
                }
            }
            Ok(crate::tikv::EvaluatedArgs::PacketBytes { value, disposition })
        },
        |result| Ok(result.into_bytes()?.map_or(Datum::Null, Datum::new_bytes)),
    )
}

#[cfg(test)]
mod from_base64_tests {
    use super::from_base64;
    use crate::Datum;

    fn s(value: &str) -> Datum {
        Datum::new_string(value.to_string())
    }

    #[test]
    fn source_vectors_and_binary_boundaries() {
        for (input, expected) in [
            ("", b"".as_slice()),
            ("YWJj", b"abc".as_slice()),
            ("YWIgYw==", b"ab c".as_slice()),
            ("YWIKYw==", b"ab\nc".as_slice()),
            ("YWIJYw==", b"ab\tc".as_slice()),
            ("cXdlcnR5MTIzNDU2", b"qwerty123456".as_slice()),
            (
                "QUJDREVGR0hJSktMTU5PUFFSU1RVVldYWVphYmNkZWZnaGlqa2xtbm9wcXJzdHV2d3h5ejAxMjM0\nNTY3ODkrL0FCQ0RFRkdISUpLTE1OT1BRUlNUVVZXWFlaYWJjZGVmZ2hpamtsbW5vcHFyc3R1dnd4\neXowMTIzNDU2Nzg5Ky9BQkNERUZHSElKS0xNTk9QUVJTVFVWV1hZWmFiY2RlZmdoaWprbG1ub3Bx\ncnN0dXZ3eHl6MDEyMzQ1Njc4OSsv",
                b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/".as_slice(),
            ),
            (
                "QUJDREVGR0hJSkt\tMTU5PUFFSU1RVVld\nYWVphYmNkZ\rWZnaGlqa2xt   bm9wcXJzdHV2d3h5ejAxMjM0NTY3ODkrLw==",
                b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/".as_slice(),
            ),
        ] {
            assert_eq!(
                from_base64(&[s(input)]).unwrap(),
                Datum::new_bytes(expected.to_vec())
            );
        }
        assert_eq!(
            from_base64(&[Datum::new_bytes(b"/wA=".to_vec())]).unwrap(),
            Datum::new_bytes(vec![0xff, 0x00])
        );
        assert_eq!(from_base64(&[Datum::Null]).unwrap(), Datum::Null);
    }

    #[test]
    fn malformed_padding_and_arity_return_null_or_error() {
        for input in ["asc", "YWJj=", "YWI", "YQ===", "YQ=A", "Y!Jj", "YR=="] {
            assert_eq!(from_base64(&[s(input)]).unwrap(), Datum::Null, "{input:?}");
        }
        assert_eq!(
            from_base64(&[s("YQ==")]).unwrap(),
            Datum::new_bytes(b"a".to_vec())
        );
        assert_eq!(
            from_base64(&[s("YWI=")]).unwrap(),
            Datum::new_bytes(b"ab".to_vec())
        );
        assert!(from_base64(&[]).is_err());
        assert!(from_base64(&[s("YWJj"), s("extra")]).is_err());
    }
}

/// `ORD(s)`: the character-code of the leftmost character — for a
/// single-byte (ASCII) char its byte value, for a multibyte char its UTF-8
/// bytes folded as a base-256 number (`ORD('A')` = 65, `ORD('é')` = 50089).
/// `0` for the empty string; `NULL` propagates.
#[cfg(test)]
pub(crate) fn ord(vals: &[Datum]) -> Result<Datum, EvalError> {
    ord_in(vals, &crate::NoColumns)
}

pub(crate) fn ord_in(vals: &[Datum], ctx: &dyn crate::Columns) -> Result<Datum, EvalError> {
    ord_with_type_in(vals, None, ctx)
}

/// Go `builtinOrdSig.evalInt` with the argument's declared charset. The
/// signature reads the first UTF-8 character from the in-memory value,
/// encodes it into that charset, then folds the first encoded character as a
/// base-256 integer. An unrepresentable character falls back to its first raw
/// byte.
#[cfg(test)]
pub(crate) fn ord_with_type(
    vals: &[Datum],
    arg_type: Option<&FieldType>,
) -> Result<Datum, EvalError> {
    ord_with_type_in(vals, arg_type, &crate::NoColumns)
}

pub(crate) fn ord_with_type_in(
    vals: &[Datum],
    arg_type: Option<&FieldType>,
    ctx: &dyn crate::Columns,
) -> Result<Datum, EvalError> {
    let [value] = vals else {
        return Err(EvalError::Unsupported("bad ORD arity"));
    };
    crate::tikv::evaluate_bytes_in(
        crate::tikv::EvaluatedBytesOp::OrdNative,
        ctx,
        || {
            let Some(bytes) = coerce_str_bytes(value)? else {
                return Ok(None);
            };
            let Some(&first_byte) = bytes.first() else {
                return Ok(Some(bytes));
            };
            // This is only the original charset preparation. The kernel owns
            // the base-256 fold, including empty and NULL answers.
            let charset = arg_type.map_or_else(
                || {
                    if crate::string_signature::is_binary_str(value) {
                        "binary"
                    } else {
                        "utf8mb4"
                    }
                },
                FieldType::charset_name,
            );
            let encoding = find_encoding(charset);
            let utf8_first = find_encoding("utf8mb4").peek(&bytes);
            let (encoded, error) = encoding
                .transform(utf8_first, TransformOp::ENCODE)
                .into_parts();
            let first = if error.is_some() {
                std::slice::from_ref(&first_byte)
            } else {
                encoding.peek(&encoded)
            };
            Ok(Some(first.to_vec()))
        },
        crate::tikv::EvaluatedBytesResult::into_int_datum,
    )
}

/// `QUOTE(s)`: `s` wrapped in single quotes with `'`, `\`, NUL and Ctrl-Z
/// backslash-escaped — a value safe to paste into SQL. A `NULL` argument
/// yields the four-character string `NULL` (NOT SQL `NULL`), matching MySQL.
///
/// The argument retains `crate::arg_eval_type`'s `types.ETString` cast
/// (`builtin_string.go:3180`) followed by Rust's `String::from_utf8_lossy`.
/// Its replacement grouping differs from Go's per-malformed-byte `[]rune`
/// conversion; preserve the existing frontend behavior before C4 escaping.
#[cfg(test)]
pub(crate) fn quote(vals: &[Datum]) -> Result<Datum, EvalError> {
    quote_in(vals, &crate::NoColumns)
}

pub(crate) fn quote_in(vals: &[Datum], ctx: &dyn crate::Columns) -> Result<Datum, EvalError> {
    crate::tikv::evaluate_bytes_in(
        crate::tikv::EvaluatedBytesOp::Quote,
        ctx,
        || {
            Ok(crate::arg_eval_type::eval_string(&vals[0])?
                .map(|bytes| String::from_utf8_lossy(&bytes).into_owned().into_bytes()))
        },
        |result| {
            // Go's SetBinFlagOrBinStr keeps binary arguments' results binary.
            Ok(result.into_bytes()?.map_or(Datum::Null, |bytes| {
                if crate::string_signature::is_binary_str(&vals[0]) {
                    Datum::new_bytes(bytes)
                } else {
                    Datum::new_string(bytes)
                }
            }))
        },
    )
}

/// `BIT_COUNT(n)`: the number of set bits in `n` (as an unsigned 64-bit
/// value); `NULL` propagates. This follows the function's ETInt input
/// signature: decimal/real/string values first take their MySQL integer
/// coercion, whose statement warnings are outside this value-only domain.
pub(crate) fn bit_count(vals: &[Datum], ctx: &dyn crate::Columns) -> Result<Datum, EvalError> {
    crate::tikv::evaluate_args_in(
        crate::tikv::EvaluatedBytesOp::BitCount,
        ctx,
        || {
            let raw: Option<&[u8]> = match &vals[0] {
                Datum::String(text) => Some(text.bytes()),
                Datum::Bytes(text) => Some(text),
                _ => None,
            };
            if let Some(text) = raw {
                if radix_truncated(text) {
                    ctx.append_warning(
                        1292,
                        &format!(
                            "Truncated incorrect INTEGER value: '{}'",
                            String::from_utf8_lossy(text)
                        ),
                    );
                }
            }
            let bits = match &vals[0] {
                Datum::Null => return Ok(crate::tikv::EvaluatedArgs::Int(None)),
                Datum::Int(n) => *n as u64,
                Datum::UInt(n) => *n,
                Datum::Decimal(n) => n.round_to_i64_saturating() as u64,
                Datum::Real(n) => round_float_to_i64_saturating(*n) as u64,
                // BIT_COUNT's ETInt argument is built through Go's
                // `builtinCastStringAsIntSig`, which parses non-negative strings as
                // UINT64 before reinterpreting the result as the signed ETInt carrier.
                // A plain signed saturation would therefore be wrong for values in
                // `2^63..=u64::MAX` (for example, `2^63` has one set bit, not 63).
                // Keep the scan byte-oriented too: Go strings/bytes may contain
                // malformed UTF-8 after an otherwise valid numeric prefix, and the
                // source conversion still consumes that prefix.
                Datum::String(n) => bit_count_string_bytes(n.bytes()),
                Datum::Bytes(n) => bit_count_string_bytes(n),
                Datum::MinNotNull | Datum::MaxValue => {
                    return Err(EvalError::Unsupported("range sentinel BIT_COUNT argument"));
                }
                other => {
                    other
                        .to_i64()
                        .map_err(|_| EvalError::Unsupported("BIT_COUNT argument conversion"))?
                        .value as u64
                }
            };
            Ok(crate::tikv::EvaluatedArgs::Int(Some(bits as i64)))
        },
        crate::tikv::EvaluatedBytesResult::into_int_datum,
    )
}

/// Coerces a raw string/bytes payload through the integer cast used by
/// `builtinBitCountSig`. The source's cast selects `StrToUint` for a
/// non-negative value, retaining all 64 bits, while a negative value uses
/// `StrToInt` and clamps below `i64::MIN` to that endpoint. A positive value
/// beyond `u64::MAX` likewise clamps to `u64::MAX` after the warning is
/// handled, so all three source boundaries are represented before the
/// population count runs.
fn bit_count_string_bytes(raw: &[u8]) -> u64 {
    // `strings.TrimSpace` runs before the source's sign check. Preserve its
    // Unicode behavior for valid strings, and retain a byte-safe ASCII trim
    // path for malformed Go string payloads.
    let raw = match std::str::from_utf8(raw) {
        Ok(s) => s.trim_start().as_bytes(),
        Err(_) => raw
            .iter()
            .position(|byte| !byte.is_ascii_whitespace())
            .map_or(&[][..], |start| &raw[start..]),
    };
    let negative = raw.len() > 1 && raw[0] == b'-';
    let digits = if negative || raw.first() == Some(&b'+') {
        &raw[1..]
    } else {
        raw
    };
    let digits = &digits[..digits
        .iter()
        .position(|byte| !byte.is_ascii_digit())
        .unwrap_or(digits.len())];
    if digits.is_empty() {
        return 0;
    }

    let mut magnitude = 0u64;
    let mut overflow = false;
    for &digit in digits {
        let next = magnitude
            .checked_mul(10)
            .and_then(|value| value.checked_add(u64::from(digit - b'0')));
        let Some(next) = next else {
            overflow = true;
            break;
        };
        magnitude = next;
    }

    if negative {
        if overflow || magnitude > i64::MAX as u64 {
            i64::MIN as u64
        } else {
            (-(magnitude as i64)) as u64
        }
    } else if overflow {
        u64::MAX
    } else {
        magnitude
    }
}

/// `FORMAT(x, d)`: the two-argument English-locale spelling.  See
/// [`format_num_locale`] for the shared port of TiDB's `FORMAT` evaluator.
pub(crate) fn format_num(vals: &[Datum], ctx: &dyn crate::Columns) -> Result<Datum, EvalError> {
    format_num_locale(vals, Some("en_US"), ctx)
}

/// Shared `FORMAT(x, d[, locale])` evaluator, ported from
/// `evalNumDecArgsForFormat` and the two `builtinFormat*Sig.evalString`
/// bodies in `pkg/expression/builtin_string.go`.
///
/// ```text
/// x, d, isNull, err := evalNumDecArgsForFormat(ctx, b, row)   // x already rounded
/// formatString, found, err := mysql.FormatByLocale(x, d, locale)
/// if !isNull && !found { tc.AppendWarning(errUnknownLocale.FastGenByArgs(locale)) }
/// ```
///
/// The grouping itself is `tidb_mysql::locale::format_by_locale`, the
/// complete port of `pkg/parser/mysql/locale_format.go` -- which is where
/// the `found` flag comes from, and why the unknown-locale warning is
/// reachable at all now. A second, less faithful locale table used to live
/// here beside it; it had no `found` flag and so could not raise 1649.
///
/// `locale` is `None` for the NULL a three-argument `FORMAT` evaluated to,
/// which Go warns about with the literal text `NULL` BEFORE it falls back to
/// `en_US`, and `Some("en_US")` for the two-argument form, which Go's
/// `builtinFormatSig` discards `found` for and never warns about. The two
/// are different states, not one default.
///
/// `ctx` is the statement warning sink, which `FORMAT`'s ETReal coercion of
/// a string argument raises 1292 on as well.
pub(crate) fn format_num_locale(
    vals: &[Datum],
    locale: Option<&str>,
    ctx: &dyn crate::Columns,
) -> Result<Datum, EvalError> {
    let [number, precision, ..] = vals else {
        return Err(EvalError::Unsupported("bad FORMAT arguments"));
    };
    let Some(number) = format_number_text(number, ctx)? else {
        return Ok(Datum::Null);
    };
    let Some(precision) = format_precision(precision, ctx)? else {
        return Ok(Datum::Null);
    };
    // `evalNumDecArgsForFormat`: `d` is clamped, the number is rounded to it,
    // and BOTH cross into `FormatByLocale` as decimal strings.
    let precision = precision.clamp(0, FORMAT_MAX_DECIMALS) as usize;
    let rounded = round_format_args(&number, precision);
    let (locale, is_null_locale) = match locale {
        Some(locale) => (locale, false),
        None => ("en_US", true),
    };
    if is_null_locale {
        append_unknown_locale_warning(ctx, "NULL");
    }
    let (formatted, found) =
        tidb_mysql::locale::format_by_locale(&rounded, &precision.to_string(), locale)
            .map_err(|_| EvalError::Unsupported("bad FORMAT arguments"))?;
    if !is_null_locale && !found {
        append_unknown_locale_warning(ctx, locale);
    }
    Ok(Datum::new_bytes(formatted))
}

/// Go `formatMaxDecimals` (`pkg/expression/builtin_string.go`).
const FORMAT_MAX_DECIMALS: i64 = 30;

/// Go `errUnknownLocale` (`ErrUnknownLocale`, 1649).
fn append_unknown_locale_warning(ctx: &dyn crate::Columns, locale: &str) {
    ctx.append_warning(1649, &format!("Unknown locale: '{locale}'"));
}

fn format_number_text(
    value: &Datum,
    ctx: &dyn crate::Columns,
) -> Result<Option<String>, EvalError> {
    Ok(match value {
        Datum::Null => None,
        Datum::Int(n) => Some(n.to_string()),
        Datum::UInt(n) => Some(n.to_string()),
        Datum::Decimal(n) => Some(n.to_string()),
        Datum::Real(n) => Some(n.to_string()),
        // `FORMAT` requests ETReal for everything else, and that is the
        // NUMERIC reading of the argument, never its text: captured from
        // TiDB, `FORMAT(d,2)` on `DATE'2021-01-01'` is `20,210,101.00`,
        // `FORMAT(t,2)` on `TIME'10:20:30'` is `102,030.00`, an `enum` gives
        // its ordinal and a `json` string its numeric prefix. Rendering
        // `sql_string()` here instead formatted `'2021-01-01'` -- the text --
        // and answered `2.00`.
        other => Some(crate::ops::to_f64_with_mysql_string(other, ctx)?.to_string()),
    })
}

fn format_precision(value: &Datum, ctx: &dyn crate::Columns) -> Result<Option<i64>, EvalError> {
    Ok(match value {
        Datum::Null => None,
        Datum::Int(n) => Some(*n),
        Datum::UInt(n) => Some(*n as i64),
        Datum::Decimal(n) => Some(n.round_to_i64_saturating()),
        Datum::Real(n) => Some(round_float_to_i64_saturating(*n)),
        Datum::String(_) | Datum::Bytes(_) => {
            Some(crate::cast::to_i64_signed_with_warnings(value, ctx)?)
        }
        Datum::MinNotNull | Datum::MaxValue => {
            return Err(EvalError::Unsupported("range sentinel FORMAT precision"));
        }
        other => Some(
            other
                .to_i64()
                .map_err(|_| EvalError::Unsupported("FORMAT precision conversion"))?
                .value,
        ),
    })
}

fn round_float_to_i64_saturating(value: f64) -> i64 {
    let rounded = value.round_ties_even();
    if rounded < i64::MIN as f64 {
        i64::MIN
    } else if rounded >= i64::MAX as f64 {
        i64::MAX
    } else {
        rounded as i64
    }
}

fn round_format_args(number: &str, precision: usize) -> String {
    let (negative, number) = number
        .strip_prefix('-')
        .map_or((false, number), |n| (true, n));
    let (mut integer, fraction) = number.split_once('.').unwrap_or((number, ""));
    if !integer.bytes().all(|digit| digit.is_ascii_digit())
        || !fraction.bytes().all(|digit| digit.is_ascii_digit())
    {
        integer = "0";
    }
    let mut fraction: Vec<u8> = fraction.bytes().take(precision).collect();
    while fraction.len() < precision {
        fraction.push(b'0');
    }
    let round_up = number
        .split_once('.')
        .and_then(|(_, f)| f.as_bytes().get(precision))
        .is_some_and(|d| *d >= b'5');
    if round_up {
        let mut carry = true;
        for digit in fraction.iter_mut().rev() {
            if *digit == b'9' {
                *digit = b'0';
            } else {
                *digit += 1;
                carry = false;
                break;
            }
        }
        if carry {
            let mut digits = integer.as_bytes().to_vec();
            for digit in digits.iter_mut().rev() {
                if *digit == b'9' {
                    *digit = b'0';
                } else {
                    *digit += 1;
                    carry = false;
                    break;
                }
            }
            if carry {
                return format_number_parts(
                    negative,
                    format!("1{}", "0".repeat(integer.len())),
                    fraction,
                );
            }
            return format_number_parts(negative, String::from_utf8(digits).unwrap(), fraction);
        }
    }
    format_number_parts(negative, integer.to_string(), fraction)
}

fn format_number_parts(negative: bool, integer: String, fraction: Vec<u8>) -> String {
    let mut out = String::new();
    if negative {
        out.push('-');
    }
    out.push_str(&integer);
    if !fraction.is_empty() {
        out.push('.');
        out.push_str(std::str::from_utf8(&fraction).unwrap());
    }
    out
}

/// `CHAR(n1, n2, ...)` (parser-renamed `CHAR_FUNC`) ported from
/// `builtinCharSig.convertToBytes` in `pkg/expression/builtin_string.go`.
/// No-`USING` CHAR returns `Datum::Bytes` exactly as TiDB's binary signature
/// does, including invalid UTF-8 and embedded NUL.
#[cfg(test)]
pub(crate) fn char_func(vals: &[Datum]) -> Result<Datum, EvalError> {
    char_func_with_context(vals, &crate::context::NoColumns)
}

pub(crate) fn char_func_with_context(
    vals: &[Datum],
    ctx: &dyn crate::context::Columns,
) -> Result<Datum, EvalError> {
    // The last argument is the charset sentinel appended by the parser.
    let Some((charset, nums)) = vals.split_last() else {
        return Err(EvalError::Unsupported("CHAR requires arguments"));
    };
    let mut bytes = Vec::new();
    for v in nums {
        match v {
            Datum::Null => {} // skipped, matching TiDB's EvalInt NULL path
            // Go's `WrapWithCastAsInt` over a JSON operand re-reads the
            // document's text as an integer (StrToInt), raising go's 1292
            // truncation warning when the text is not a clean integer —
            // captured: `CHAR(65, j)` over `{}` warns
            // "Truncated incorrect INTEGER value: '{}'".
            Datum::Json(value) => {
                let as_text = Datum::new_string(value.to_string());
                crate::cast::report_int_truncation(&as_text, ctx)?;
                append_char_integer(&mut bytes, crate::cast::to_i64_signed(&as_text));
            }
            // go `WrapWithCastAsInt` over a STRING source reads the text as
            // an integer and warns `Truncated incorrect INTEGER value`
            // (captured: CHAR('中文测试', 'ünïcödé') warns against '中文测试'
            // before the charset error, so the warning survives it).
            Datum::String(_) | Datum::Bytes(_) => {
                crate::cast::report_int_truncation(v, ctx)?;
                append_char_integer(&mut bytes, crate::cast::to_i64_signed(v));
            }
            _ => append_char_integer(&mut bytes, crate::cast::to_i64_signed(v)),
        }
    }
    if *charset == Datum::Null {
        return Ok(Datum::new_bytes(bytes));
    }

    // go evaluates the charset argument as a STRING (a numeric source
    // spells its text out: `-3.75` IS the charset name), then
    // `GetCharsetInfo` answers the bare 1105 `Unknown charset <name>`
    // BEFORE any encoding work happens.
    let charset_text = crate::coerce::coerce_str(charset)?
        .ok_or_else(|| EvalError::Unsupported("CHAR charset argument"))?;
    let charset = charset_text.to_ascii_lowercase();
    let collation_name = get_default_collation(&charset).map_err(|_| {
        EvalError::Unsupported(Box::leak(
            format!("Unknown charset {charset}").into_boxed_str(),
        ))
    })?;
    let (decoded, error) = find_encoding(&charset)
        .transform(&bytes, TransformOp::DECODE)
        .into_parts();
    if let Some(error) = error {
        ctx.append_warning(1300, &error.to_string());
        if ctx.strict_sql_mode() {
            return Ok(Datum::Null);
        }
    }
    let collation = Collation::from_name(&collation_name).ok_or_else(|| {
        EvalError::Unsupported(Box::leak(
            format!("Unknown charset {charset}").into_boxed_str(),
        ))
    })?;
    Ok(Datum::new_collation_string(decoded, collation))
}

fn append_char_integer(bytes: &mut Vec<u8>, mut value: i64) {
    let mut current = Vec::with_capacity(4);
    for _ in 0..4 {
        current.push((value & 0xff) as u8);
        value >>= 8;
        if value == 0 {
            break;
        }
    }
    current.reverse();
    bytes.extend(current);
}

/// `TRIM([{BOTH|LEADING|TRAILING} [remstr]] FROM str)` / `TRIM(str)` /
/// `TRIM(remstr FROM str)`: repeatedly strips WHOLE occurrences of
/// `remstr` (never per-character) from the requested end(s) of `str` —
/// confirmed via `gorun`: `TRIM('xx' FROM 'xxhixx')` is `'hi'` (the
/// 2-character `remstr` removed as a unit, not char-by-char). An empty
/// `remstr` is a no-op (confirmed via `gorun`), guarded in the shared kernel
/// since `str::trim_start_matches`/`trim_end_matches` would otherwise
/// loop forever matching a zero-length pattern at every position.
/// `direction` defaults to `Both` when omitted (bare `TRIM(remstr FROM
/// str)` with no direction keyword, or bare `TRIM(str)` with an
/// implicit single-space `remstr`) — the caller already resolves BOTH
/// of those defaults before calling this (see `tidb_ast::Expr::Trim`'s
/// own doc for the exact `None`/`Some` shape). `NULL` if either operand
/// is `NULL`.
#[cfg(test)]
pub(crate) fn trim_value(
    str: Option<Vec<u8>>,
    remstr: Option<Vec<u8>>,
    direction: tidb_ast::TrimDirection,
    binary: bool,
) -> Datum {
    trim_value_in(str, remstr, direction, binary, &crate::NoColumns)
        .expect("test-only TRIM evaluation")
}

/// Receives already-coerced operands, preserving the distinct AST and typed
/// evaluation orders. Every direction, NULL and empty-removal case enters C4.
pub(crate) fn trim_value_in(
    str: Option<Vec<u8>>,
    remstr: Option<Vec<u8>>,
    direction: tidb_ast::TrimDirection,
    binary: bool,
    ctx: &dyn crate::Columns,
) -> Result<Datum, EvalError> {
    let operation = match direction {
        tidb_ast::TrimDirection::Both => crate::tikv::EvaluatedBytesOp::TrimBothNative,
        tidb_ast::TrimDirection::Leading => crate::tikv::EvaluatedBytesOp::TrimLeadingNative,
        tidb_ast::TrimDirection::Trailing => crate::tikv::EvaluatedBytesOp::TrimTrailingNative,
    };
    crate::tikv::evaluate_args_in(
        operation,
        ctx,
        || Ok(crate::tikv::EvaluatedArgs::Bytes2(str, remstr)),
        |computed| {
            Ok(computed.into_bytes()?.map_or(Datum::Null, |bytes| {
                if binary {
                    Datum::new_bytes(bytes)
                } else {
                    Datum::new_string(bytes)
                }
            }))
        },
    )
}

#[cfg(test)]
mod bit_count_tests {
    use super::bit_count;
    use crate::{Datum, Decimal};

    /// Full value vector from `pkg/expression/builtin_other_test.go`'s
    /// `TestBitCount`, including its UInt64 regression case.
    #[test]
    fn bit_count_matches_go_source_vectors() {
        let cases = [
            (Datum::Int(8), 1),
            (Datum::Int(29), 4),
            (Datum::Int(0), 0),
            (Datum::Int(-1), 64),
            (Datum::Int(-11), 62),
            (Datum::Int(-1000), 56),
            (Datum::Real(1.1), 1),
            (Datum::Real(3.1), 2),
            (Datum::Real(-1.1), 64),
            (Datum::Real(-3.1), 63),
            (Datum::UInt(u64::MAX), 64),
            // The Go ETInt cast preserves the low 64 bits for a positive
            // string up to UINT64_MAX; it does not saturate at INT64_MAX.
            (Datum::new_string("9223372036854775808"), 1),
            (Datum::new_string("18446744073709551616"), 64),
            (Datum::new_string("xxx".to_string()), 0),
            (Datum::Decimal(Decimal::from_literal("3.1")), 2),
        ];
        for (input, want) in cases {
            assert_eq!(
                bit_count(&[input], &crate::context::NoColumns),
                Ok(Datum::Int(want))
            );
        }
        assert_eq!(
            bit_count(&[Datum::Null], &crate::context::NoColumns),
            Ok(Datum::Null)
        );
        // Go's string conversion is byte-prefix based, so malformed UTF-8
        // after an ASCII number must not erase the numeric prefix.
        assert_eq!(
            bit_count(
                &[Datum::new_bytes(vec![b'1', 0xff])],
                &crate::context::NoColumns
            ),
            Ok(Datum::Int(1))
        );
    }
}

#[cfg(test)]
mod concat_source_tests {
    use super::{concat, concat_ws};
    use crate::{Datum, Decimal, EvalError};

    fn string(value: &str) -> Datum {
        Datum::new_string(value.to_string())
    }

    /// Representable scalar rows from `TestConcat` in
    /// `pkg/expression/builtin_string_test.go`.  The Go table's typed
    /// datetime/duration values have already crossed their `EvalString`
    /// boundary here as their exact rendered text; injected Go errors and
    /// packet-warning/session metadata remain outside the value-only API.
    #[test]
    fn concat_matches_go_source_scalar_rows() {
        assert_eq!(concat(&[Datum::Null]), Ok(Datum::Null));
        assert_eq!(
            concat(&[
                string("a"),
                string("b"),
                Datum::Int(1),
                Datum::Int(2),
                Datum::Real(1.1),
                Datum::Real(1.2),
                Datum::Decimal(Decimal::from_literal("1.1")),
                string("2000-01-01 12:01:01"),
                string("12:01:01"),
            ]),
            Ok(string("ab121.11.21.12000-01-01 12:01:0112:01:01"))
        );
        assert_eq!(
            concat(&[string("a"), string("b"), Datum::Null, string("c")]),
            Ok(Datum::Null)
        );

        // Go's ETString is a byte boundary.  Invalid UTF-8 from a binary
        // argument must survive CONCAT rather than becoming an evaluator
        // error or replacement character.
        assert_eq!(
            concat(&[Datum::new_bytes(vec![0xff]), string("a")]),
            Ok(Datum::new_string(vec![0xff, b'a']))
        );
        assert_eq!(
            concat(&[]),
            Err(EvalError::Unsupported(
                "CONCAT requires at least one argument"
            ))
        );
    }

    /// Representable scalar rows from `TestConcatWS`: a NULL separator
    /// propagates, later NULL fields are skipped, and empty fields still
    /// contribute separators.  Numeric and already-rendered temporal values
    /// use the same byte-preserving coercion as CONCAT.
    #[test]
    fn concat_ws_matches_go_source_scalar_rows() {
        assert_eq!(concat_ws(&[Datum::Null, Datum::Null]), Ok(Datum::Null));
        assert_eq!(
            concat_ws(&[Datum::Null, string("a"), string("b")]),
            Ok(Datum::Null)
        );
        assert_eq!(
            concat_ws(&[
                string(","),
                string("a"),
                string("b"),
                string("hello"),
                string("$^%"),
            ]),
            Ok(string("a,b,hello,$^%"))
        );
        assert_eq!(
            concat_ws(&[
                string("|"),
                string("a"),
                Datum::Null,
                string("b"),
                string("c")
            ]),
            Ok(string("a|b|c"))
        );
        assert_eq!(
            concat_ws(&[
                string(","),
                string("a"),
                string(","),
                string("b"),
                string("c"),
            ]),
            Ok(string("a,,,b,c"))
        );
        assert_eq!(
            concat_ws(&[
                string(","),
                string("a"),
                string("b"),
                Datum::Int(1),
                Datum::Int(2),
                Datum::Real(1.1),
                Datum::Real(0.11),
                Datum::Decimal(Decimal::from_literal("1.1")),
                string("2000-01-01 12:01:01"),
                string("12:01:01"),
            ]),
            Ok(string("a,b,1,2,1.1,0.11,1.1,2000-01-01 12:01:01,12:01:01"))
        );
        assert_eq!(
            concat_ws(&[string(","), string("a"), string("")]),
            Ok(string("a,"))
        );
        assert_eq!(
            concat_ws(&[
                Datum::new_bytes(vec![b'|']),
                Datum::new_bytes(vec![0xff]),
                Datum::Null,
                string("b")
            ]),
            Ok(Datum::new_string(vec![0xff, b'|', b'b']))
        );
        assert_eq!(
            concat_ws(&[string(",")]),
            Err(EvalError::Unsupported(
                "CONCAT_WS requires at least two arguments"
            ))
        );
    }
}

#[cfg(test)]
mod format_tests {
    use super::format_num;
    use crate::{Datum, Decimal};

    fn fmt(number: &str, precision: i64) -> String {
        let n = Datum::Decimal(if let Some(mag) = number.strip_prefix('-') {
            Decimal::from_literal(mag).negate()
        } else {
            Decimal::from_literal(number)
        });
        // `FormatByLocale` counts and groups the integer part in BYTES, so
        // its result is bytes rather than a UTF-8 string; every row here is
        // ASCII, so reading them back as text is exact.
        match format_num(&[n, Datum::Int(precision)], &crate::NoColumns).unwrap() {
            Datum::Bytes(bytes) => String::from_utf8(bytes).unwrap(),
            other => panic!("expected bytes, got {other:?}"),
        }
    }

    /// FORMAT rounds HALF AWAY FROM ZERO (2.5 -> 3, -2.5 -> -3), groups the
    /// integer part with `,` every three digits, pads to `precision` decimals,
    /// and clamps a negative precision to 0. Authoritative goeval values.
    #[test]
    fn format_rounds_half_away_from_zero_and_groups_thousands() {
        assert_eq!(fmt("2.5", 0), "3");
        assert_eq!(fmt("2.4", 0), "2");
        assert_eq!(fmt("-2.5", 0), "-3");
        assert_eq!(fmt("1234567.891", 2), "1,234,567.89");
        assert_eq!(fmt("1234.5678", 2), "1,234.57");
        assert_eq!(fmt("-1234.5", 0), "-1,235");
        assert_eq!(fmt("1.9999", 2), "2.00");
        assert_eq!(fmt("123.456", -1), "123");
    }
}

#[cfg(test)]
mod hex_bit_column_tests {
    use super::hex;
    use crate::Datum;
    use tidb_datatype::BinaryLiteral;

    fn bit(bytes: &[u8]) -> Datum {
        Datum::Bit(BinaryLiteral::from(bytes.to_vec()))
    }

    /// `hexFunctionClass.getFunction` reads `args[0].GetType().EvalType()`,
    /// and `mysql.TypeBit` is `types.ETInt` -- so a stored `BIT` value is
    /// hexed as a NUMBER and its storage padding disappears, where a
    /// `BINARY`/`BLOB` of the same bytes keeps every octet.
    ///
    /// Captured from real TiDB through `gorun`, on `bit(48)`, `bit(64)` and
    /// `bit(1)` columns:
    ///
    /// ```text
    /// bit(48) = _binary '\0\b\n\r\t\Z'  -> 80A0D091A
    /// bit(48) = x'0041'                 -> 41
    /// bit(64) = all ones                -> FFFFFFFFFFFFFFFF
    /// bit(1)  = b'1'                    -> 1
    /// bit(48) = 0                       -> 0
    /// blob    = unhex('00080A0D091A')   -> 00080A0D091A
    /// ```
    #[test]
    fn a_bit_value_is_hexed_as_a_number_and_a_blob_is_hexed_as_bytes() {
        let payload = [0x00, 0x08, 0x0A, 0x0D, 0x09, 0x1A];
        assert_eq!(
            hex(&[bit(&payload)]).unwrap(),
            Datum::new_string("80A0D091A".to_owned())
        );
        // The same six bytes as a blob keep their leading zero byte: this is
        // the pair the recording compares, and the whole difference is which
        // signature the argument's TYPE selects.
        assert_eq!(
            hex(&[Datum::new_bytes(payload.to_vec())]).unwrap(),
            Datum::new_string("00080A0D091A".to_owned())
        );
        assert_eq!(
            hex(&[bit(&[0x00, 0x00, 0x00, 0x00, 0x00, 0x41])]).unwrap(),
            Datum::new_string("41".to_owned())
        );
        // A `bit(64)` with the high bit set is why this reads the payload as
        // UNSIGNED: the signed conversion would refuse it outright.
        assert_eq!(
            hex(&[bit(&[0xFF; 8])]).unwrap(),
            Datum::new_string("FFFFFFFFFFFFFFFF".to_owned())
        );
        assert_eq!(
            hex(&[bit(&[0x01])]).unwrap(),
            Datum::new_string("1".to_owned())
        );
        assert_eq!(
            hex(&[bit(&[0x00; 6])]).unwrap(),
            Datum::new_string("0".to_owned())
        );
    }

    /// A hex LITERAL is not a `BIT` value: Go types `x'0041'` as
    /// `mysql.TypeVarString`, so it takes the string signature and keeps its
    /// leading zero byte. `hex(x'0041')` is `0041` in TiDB, and this is the
    /// datum that carries it.
    #[test]
    fn a_binary_literal_keeps_the_string_signature() {
        assert_eq!(
            hex(&[Datum::BinaryLiteral(BinaryLiteral::from(vec![0x00, 0x41]))]).unwrap(),
            Datum::new_string("0041".to_owned())
        );
    }
}

#[cfg(test)]
mod export_set_tests {
    use super::*;

    fn d(s: &str) -> Datum {
        Datum::new_string(s.to_string())
    }

    #[test]
    fn export_set_matches_go_bit_order_and_arity_forms() {
        // MySQL doc example: bits 0 and 2 set within 4 bits → Y,N,Y,N.
        assert_eq!(
            export_set(&[Datum::Int(5), d("Y"), d("N"), d(","), Datum::Int(4)]).unwrap(),
            d("Y,N,Y,N")
        );
        // Bit 0 first: 6 = 0b110 within 10 bits.
        assert_eq!(
            export_set(&[Datum::Int(6), d("1"), d("0"), d(","), Datum::Int(10)]).unwrap(),
            d("0,1,1,0,0,0,0,0,0,0")
        );
        // Three-argument form: `,` separator, 64 bits (bits 0 and 3 set).
        let three = export_set(&[Datum::Int(9), d("Y"), d("N")]).unwrap();
        eprintln!("PROBE three: {:?}", three);
        assert_eq!(three, d(&format!("Y,N,N,Y{}", ",N".repeat(60))));
        // Four-argument form: the custom separator with 64 bits.
        let expected_four = format!("Y@N@N@Y{}", "@N".repeat(60));
        assert_eq!(
            export_set(&[Datum::Int(9), d("Y"), d("N"), d("@")]).unwrap(),
            d(&expected_four)
        );
        // Five-argument form: a negative or oversized number_of_bits clamps
        // to 64; zero yields the empty string.
        assert_eq!(
            export_set(&[Datum::Int(5), d("Y"), d("N"), d(","), Datum::Int(100)]).unwrap(),
            d(&format!("Y,N,Y,N{}", ",N".repeat(60)))
        );
        assert_eq!(
            export_set(&[Datum::Int(5), d("Y"), d("N"), d(","), Datum::Int(0)]).unwrap(),
            d("")
        );
    }

    #[test]
    fn export_set_null_arguments_propagate_like_go() {
        for args in [
            vec![Datum::Null, d("Y"), d("N")],
            vec![Datum::Int(5), Datum::Null, d("N")],
            vec![Datum::Int(5), d("Y"), Datum::Null],
            vec![Datum::Int(5), d("Y"), d("N"), Datum::Null],
            vec![Datum::Int(5), d("Y"), d("N"), d(","), Datum::Null],
        ] {
            assert_eq!(
                export_set(&args).unwrap(),
                Datum::Null,
                "any NULL argument yields NULL"
            );
        }
    }

    #[test]
    fn export_set_is_reachable_through_the_values_dispatch() {
        let result = crate::func::eval_func_values(
            "EXPORT_SET",
            &[Datum::Int(5), d("Y"), d("N"), d(","), Datum::Int(4)],
            &crate::context::NoColumns,
        );
        assert!(result.is_some(), "the dispatch must own EXPORT_SET");
        assert_eq!(result.unwrap().unwrap(), d("Y,N,Y,N"));
    }
}

#[cfg(test)]
mod cast_in_union_tests {
    use super::*;

    /// Go `castAsIntSig.evalInt`'s in-union arm
    /// (`builtin_cast.go:998`): a negative result clamps to 0 for an
    /// unsigned target instead of the unsigned wrap.
    #[test]
    fn cast_unsigned_in_union_clamps_negatives_to_zero() {
        let ctx = crate::context::NoColumns;
        let result =
            crate::func::eval_func_values("cast_unsigned_in_union", &[Datum::Int(-1)], &ctx);
        assert_eq!(result.unwrap().unwrap(), Datum::UInt(0));
    }

    #[test]
    fn cast_unsigned_in_union_keeps_non_negatives() {
        let ctx = crate::context::NoColumns;
        let result =
            crate::func::eval_func_values("cast_unsigned_in_union", &[Datum::Int(7)], &ctx);
        assert_eq!(result.unwrap().unwrap(), Datum::UInt(7));
    }
}
