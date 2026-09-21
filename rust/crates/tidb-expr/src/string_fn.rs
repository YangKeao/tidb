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
//! BIT_COUNT, SUBSTRING_INDEX, QUOTE, CHAR_FUNC, FIELD, and ELT were physically
//! removed; retained lowerable shapes execute only through TiKV.
//! LOCATE/INSTR/POSITION and `TRIM(...)` retain native helpers for later work.

use crate::coerce::{coerce_str, coerce_str_bytes};
use crate::{Datum, EvalError};

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
