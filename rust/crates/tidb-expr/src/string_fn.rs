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

//! Remaining native string builtin helpers. Case conversion, LEFT/RIGHT,
//! REVERSE, REPLACE, STRCMP, ASCII, BIT_LENGTH, HEX/UNHEX, BIN/OCT, ORD,
//! BIT_COUNT, SUBSTRING_INDEX, and QUOTE were physically removed; retained
//! lowerable shapes execute only through TiKV. LOCATE/INSTR/POSITION and `TRIM(...)` retain native helpers
//! for later semantic-gap work.

use crate::coerce::{coerce_str, coerce_str_bytes};
use crate::ops::to_f64_with_mysql_string;
use crate::{Datum, EvalError};
use tidb_datatype::{find_encoding, get_default_collation, Collation, TransformOp};

/// `POSITION(substr IN str)`: the 1-indexed, character-based position of
/// `substr`'s first occurrence in `str`; `0` if not found; an empty
/// `substr` always matches at position `1`. `NULL` propagates.
pub(crate) fn position(substr: Option<String>, str: Option<String>) -> Datum {
    position_with_collation(substr, str, tidb_datatype::Collation::Utf8Mb4Bin)
}

/// `LOCATE(substr, str)` / `INSTR(str, substr)` / `POSITION(substr IN str)`
/// over the raw arguments, selecting byte offsets only for binary collation.
pub(crate) fn locate(
    substr: &Datum,
    str: &Datum,
    collation: tidb_datatype::Collation,
) -> Result<Datum, EvalError> {
    if collation != tidb_datatype::Collation::Binary {
        return Ok(position_with_collation(
            coerce_str(substr)?,
            coerce_str(str)?,
            collation,
        ));
    }
    let (Some(needle), Some(haystack)) = (coerce_str_bytes(substr)?, coerce_str_bytes(str)?) else {
        return Ok(Datum::Null);
    };
    if needle.is_empty() {
        return Ok(Datum::Int(1));
    }
    let found = haystack
        .windows(needle.len())
        .position(|window| window == needle.as_slice());
    Ok(Datum::Int(found.map_or(0, |index| index as i64 + 1)))
}

/// `LOCATE(substr, str, pos)` under the selected byte or character collation.
pub(crate) fn locate_with_position(
    vals: &[Datum],
    collation: tidb_datatype::Collation,
) -> Result<Datum, EvalError> {
    let [substr, str, pos] = vals else {
        return Err(EvalError::Unsupported("bad LOCATE arity"));
    };
    let binary = collation == tidb_datatype::Collation::Binary;
    let needle_opt = if binary {
        coerce_str_bytes(substr)?.map(|bytes| bytes.to_vec())
    } else {
        coerce_str(substr)?.map(|text| text.into_bytes())
    };
    let hay_opt = if binary {
        coerce_str_bytes(str)?.map(|bytes| bytes.to_vec())
    } else {
        coerce_str(str)?.map(|text| text.into_bytes())
    };
    let (Some(needle), Some(hay)) = (needle_opt, hay_opt) else {
        return Ok(Datum::Null);
    };
    let Some(position) = crate::arg_eval_type::eval_int(pos)? else {
        return Ok(Datum::Null);
    };
    let start = position - 1;

    if binary {
        if start < 0 || start > hay.len() as i64 - needle.len() as i64 {
            return Ok(Datum::Int(0));
        }
        if needle.is_empty() {
            return Ok(Datum::Int(start + 1));
        }
        let found = hay[start as usize..]
            .windows(needle.len())
            .position(|window| window == needle.as_slice());
        return Ok(Datum::Int(
            found.map_or(0, |index| start + index as i64 + 1),
        ));
    }

    let lower = tidb_datatype::is_ci_collation(collation.name());
    let (needle, hay) = if lower {
        (
            tidb_mysql::to_lowercase(&String::from_utf8_lossy(&needle)).into_bytes(),
            tidb_mysql::to_lowercase(&String::from_utf8_lossy(&hay)).into_bytes(),
        )
    } else {
        (needle, hay)
    };
    let needle = String::from_utf8(needle)
        .map_err(|_| EvalError::Unsupported("invalid UTF-8 LOCATE needle"))?;
    let hay = String::from_utf8(hay)
        .map_err(|_| EvalError::Unsupported("invalid UTF-8 LOCATE haystack"))?;
    let needle: Vec<char> = needle.chars().collect();
    let hay: Vec<char> = hay.chars().collect();
    if start < 0 || start > hay.len() as i64 - needle.len() as i64 {
        return Ok(Datum::Int(0));
    }
    if needle.is_empty() {
        return Ok(Datum::Int(start + 1));
    }
    let slice: String = hay[start as usize..].iter().collect();
    let window = needle.iter().collect::<String>();
    for offset in 0..=(slice.chars().count() - needle.len()) {
        let candidate: String = slice.chars().skip(offset).take(needle.len()).collect();
        if collation.compare(candidate.as_bytes(), window.as_bytes()) == std::cmp::Ordering::Equal {
            return Ok(Datum::Int(start + offset as i64 + 1));
        }
    }
    Ok(Datum::Int(0))
}

/// The collation `LOCATE`/`INSTR` derive when no derivation pass ran.
pub(crate) fn locate_collation(substr: &Datum, str: &Datum) -> tidb_datatype::Collation {
    if crate::string_signature::is_binary_str(substr) || crate::string_signature::is_binary_str(str)
    {
        tidb_datatype::Collation::Binary
    } else {
        tidb_datatype::Collation::Utf8Mb4Bin
    }
}

/// `LOCATE`/`INSTR`/`POSITION` under an explicit collation.
pub(crate) fn position_with_collation(
    substr: Option<String>,
    str: Option<String>,
    collation: tidb_datatype::Collation,
) -> Datum {
    let (Some(substr), Some(str)) = (substr, str) else {
        return Datum::Null;
    };
    let needle: Vec<char> = substr.chars().collect();
    let haystack: Vec<char> = str.chars().collect();
    if needle.is_empty() {
        return Datum::Int(1);
    }
    if needle.len() > haystack.len() {
        return Datum::Int(0);
    }
    let needle_bytes = substr.as_bytes();
    for start in 0..=(haystack.len() - needle.len()) {
        let window: String = haystack[start..start + needle.len()].iter().collect();
        if collation.compare(window.as_bytes(), needle_bytes) == std::cmp::Ordering::Equal {
            return Datum::Int(start as i64 + 1);
        }
    }
    Datum::Int(0)
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
            _ => append_char_integer(&mut bytes, crate::cast::to_i64_signed(v)),
        }
    }
    if *charset == Datum::Null {
        return Ok(Datum::new_bytes(bytes));
    }

    let charset = std::str::from_utf8(
        charset
            .as_raw_bytes()
            .ok_or(EvalError::Unsupported("CHAR charset argument"))?,
    )
    .map_err(|_| EvalError::Unsupported("CHAR charset argument"))?
    .to_ascii_lowercase();
    let (decoded, error) = find_encoding(&charset)
        .transform(&bytes, TransformOp::DECODE)
        .into_parts();
    if let Some(error) = error {
        ctx.append_warning(1300, &error.to_string());
        if ctx.strict_sql_mode() {
            return Ok(Datum::Null);
        }
    }
    let collation_name = get_default_collation(&charset)
        .map_err(|_| EvalError::Unsupported("CHAR charset argument"))?;
    let collation = Collation::from_name(&collation_name)
        .ok_or(EvalError::Unsupported("CHAR charset argument"))?;
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
/// `remstr` is a no-op (confirmed via `gorun`), guarded explicitly here
/// since `str::trim_start_matches`/`trim_end_matches` would otherwise
/// loop forever matching a zero-length pattern at every position.
/// `direction` defaults to `Both` when omitted (bare `TRIM(remstr FROM
/// str)` with no direction keyword, or bare `TRIM(str)` with an
/// implicit single-space `remstr`) — the caller already resolves BOTH
/// of those defaults before calling this (see `tidb_ast::Expr::Trim`'s
/// own doc for the exact `None`/`Some` shape). `NULL` if either operand
/// is `NULL`.
pub(crate) fn trim_value(
    str: Option<Vec<u8>>,
    remstr: Option<Vec<u8>>,
    direction: tidb_ast::TrimDirection,
    binary: bool,
) -> Datum {
    let (Some(mut str), Some(remstr)) = (str, remstr) else {
        return Datum::Null;
    };
    if remstr.is_empty() {
        return if binary {
            Datum::new_bytes(str)
        } else {
            Datum::new_string(str)
        };
    }
    use tidb_ast::TrimDirection::*;
    if matches!(direction, Leading | Both) {
        while str.starts_with(&remstr) {
            str.drain(..remstr.len());
        }
    }
    if matches!(direction, Trailing | Both) {
        while str.ends_with(&remstr) {
            let new_len = str.len() - remstr.len();
            str.truncate(new_len);
        }
    }
    if binary {
        Datum::new_bytes(str)
    } else {
        Datum::new_string(str)
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
