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

//! `CAST(expr AS type)` / `CONVERT(expr, type)` evaluation
//! ([`eval_cast`], dispatched from `crate::eval_in`'s `Expr::Cast` arm) and
//! `CONVERT(expr USING charset)` (a plain stringification passthrough,
//! handled directly in `crate::eval_in`'s own `Expr::ConvertUsing` arm —
//! this crate has no charset domain at all).
//!
//! JSON and DATE/DATETIME targets retain their native datum domains. Every
//! rule here (string-to-number prefix parsing width, rounding tie-breaking per
//! source type, `UNSIGNED`'s source-specific negative handling, `DECIMAL`'s
//! precision clamp, `BINARY`'s NUL-padding) was confirmed via `goeval`, not
//! assumed — see each function's own doc for the specific probe.

use crate::coerce::coerce_str;
#[cfg(test)]
use crate::Decimal;
use crate::{Datum, EvalError};
use tidb_ast::CastType;
#[cfg(test)]
use tidb_datatype::{FieldType, FieldTypeCode};

/// Internal marker used when a wrapper carries Go's `UnspecifiedLength`
/// decimal scale through the AST-facing `CastType::Decimal` (whose fields are
/// unsigned).  A wrapper cast with an unspecified scale must preserve the
/// source value; mapping `-1` to the ordinary `0` scale would round every
/// fractional value to an integer before Go's constant-refinement step.
pub(crate) const UNSPECIFIED_CAST_SCALE: u32 = u32::MAX;

/// Evaluates a [`CastType`] against an already-evaluated, non-`NULL`
/// operand (`NULL` is handled by the caller — every target type maps
/// `NULL` to `NULL`, so there's no per-type NULL case to write here).
///
/// `source` is the operand's static `FieldType` where the caller knows it, and
/// `None` where it does not. Go picks the cast SIGNATURE from that type
/// (`builtinCastIntAsTimeSig` vs `builtinCastStringAsTimeSig` vs ...), and the
/// datum kind is only a proxy for it — a proxy with exactly one hole, `YEAR`,
/// whose values are ordinary `Datum::Int`s that Go nonetheless converts by a
/// rule of their own. See [`cast_to_time`].
pub(crate) fn eval_cast(
    cast_type: &CastType,
    v: Datum,
    source: Option<&tidb_datatype::FieldType>,
    ctx: &dyn crate::Columns,
) -> Result<Datum, EvalError> {
    if v.is_range_sentinel() {
        return Err(EvalError::Unsupported("range sentinel cast operand"));
    }
    if matches!(v, Datum::VectorFloat32(_))
        && !matches!(
            cast_type,
            CastType::Char { .. } | CastType::Binary { .. } | CastType::Vector { .. }
        )
    {
        return Err(EvalError::Unsupported(
            "a vector can only be cast to string or vector",
        ));
    }
    match cast_type {
        CastType::Signed => crate::tikv::eval_cast_signed_in(ctx, &v).map(Datum::Int),
        CastType::Unsigned => crate::tikv::eval_cast_unsigned_in(ctx, &v).map(Datum::UInt),
        CastType::UnsignedInUnion => {
            crate::tikv::eval_cast_unsigned_union_in(ctx, &v, source).map(Datum::UInt)
        }
        CastType::Char { len, charset } => {
            crate::tikv::eval_cast_char_in(ctx, &v, source, *len, charset.as_deref())
        }
        CastType::Binary { len } => crate::tikv::eval_cast_binary_in(ctx, &v, source, *len),
        CastType::Decimal { flen, scale } => {
            crate::tikv::eval_cast_decimal_in(ctx, &v, *flen, *scale)
        }
        CastType::Date => cast_to_time(&v, source, ctx, tidb_datatype::TimeType::Date, 0),
        CastType::DateTime { fsp } => cast_to_time(
            &v,
            source,
            ctx,
            tidb_datatype::TimeType::DateTime,
            i64::from(fsp.unwrap_or(0)),
        ),
        CastType::Year => cast_to_year(&v, ctx),
        CastType::Double => crate::tikv::eval_cast_double_in(ctx, &v).map(Datum::Real),
        CastType::Float => crate::tikv::eval_cast_float_in(ctx, &v).map(Datum::Real),
        CastType::Vector { dimensions } => crate::tikv::eval_cast_vector(&v, source, *dimensions),
        CastType::Time { fsp } => cast_to_duration(&v, source, ctx, i64::from(fsp.unwrap_or(0))),
        CastType::Json => crate::builtin_ext::cast_as_json(&v),
    }
}

/// `CAST(expr AS TIME[(fsp)])` returns TiDB's elapsed-time datum, not a
/// calendar string. The source type selects the Go signature: numeric inputs
/// turn a truncation/overflow into `NULL`, while string inputs retain the
/// parser's best-effort duration after reporting the truncation event.
fn cast_to_duration(
    v: &Datum,
    source: Option<&tidb_datatype::FieldType>,
    ctx: &dyn crate::Columns,
    fsp: i64,
) -> Result<Datum, EvalError> {
    crate::tikv::eval_cast_duration_in(ctx, v, source, fsp)
}

/// Go `WrapWithCastAsDuration` applied to one builtin argument value.
///
/// A duration expression is already in the requested domain. Calendar values
/// preserve their declared fractional precision; every other source receives
/// Go's `MaxFsp` target before the cast signature parses it.
pub(crate) fn cast_arg_as_duration(
    value: &Datum,
    source: Option<&tidb_datatype::FieldType>,
    ctx: &dyn crate::Columns,
) -> Result<Datum, EvalError> {
    crate::tikv::eval_cast_arg_as_duration_in(ctx, value, source)
}

/// `SIGNED`'s own coercion: `Int` is unchanged; `Decimal`/`Float` round to
/// the nearest integer (ties away from zero for `Decimal`, ties to EVEN
/// for `Float` — a real asymmetry, matching the `~` bitwise operator's own
/// established rule, confirmed via `goeval`: `CAST(2.5e0 AS SIGNED)` is
/// `2`, `CAST(1.5 AS SIGNED)` — a `DECIMAL` literal — is also `2`, but
/// `CAST(0.5e0 AS SIGNED)` is `0`, the even neighbor); either CLAMPS
/// (never errors) on overflow past `i64`, confirmed via `goeval`:
/// `CAST(1e300 AS SIGNED)` is `9223372036854775807`. `Str` parses a
/// leading `[+-]?digits` prefix ONLY (no `.`, no exponent — confirmed via
/// `goeval`: `CAST('3.5abc' AS SIGNED)` sees just `3`, `CAST('.5' AS
/// SIGNED)` sees no digits at all), defaulting to `0` if no digit is
/// found. A nonnegative integer prefix is parsed through the full `u64`
/// domain and then converted to `i64`, preserving Go's negative-complement
/// result above `i64::MAX`; true `u64` overflow returns `-1`. Negative
/// overflow clamps to `i64::MIN`.
pub(crate) fn to_i64_signed(v: &Datum) -> i64 {
    to_i64_signed_in(v, &tidb_datatype::SessionTimeZone::utc())
}

/// [`to_i64_signed`] with the session's `time_zone`, which Go's
/// `toSignedInteger` hands to `Time.RoundFrac` -- load-bearing only when a
/// DATETIME's fractional carry lands on a DST transition instant.
pub(crate) fn to_i64_signed_in(v: &Datum, zone: &tidb_datatype::SessionTimeZone) -> i64 {
    crate::tikv::eval_cast_signed_value_in(v, zone)
}

/// Signed integer coercion plus the warnings produced by Go's cast signature.
///
/// Builtins whose arguments are wrapped with `WrapWithCastAsInt` must use this
/// boundary rather than the value-only helper so their statement warning list
/// remains identical to an explicit `CAST(... AS SIGNED)`.
pub(crate) fn to_i64_signed_with_warnings(
    v: &Datum,
    ctx: &dyn crate::Columns,
) -> Result<i64, EvalError> {
    crate::tikv::eval_cast_signed_in(ctx, v)
}

/// `UNSIGNED`'s own coercion. Integer and integer-string sources preserve
/// the low 64 bits, so `CAST(-5 AS UNSIGNED)` is the genuine
/// `18446744073709551611` UInt64 value.
///
/// The DECIMAL and the FLOAT source do NOT agree about a negative value, and
/// that disagreement is Go's, not an inconsistency to smooth over:
///
///  * `MyDecimal.ToUint` (`ConvertDecimalToUint`) returns 0 for a negative
///    value, plus a truncation event. Captured: `cast(-1.5 as unsigned)` is 0
///    with `1292 Truncated incorrect DECIMAL value: '-1.5'`.
///  * `ConvertFloatToUint` (`pkg/types/convert.go:169-183`) rounds first, and
///    for a negative result takes the `AllowNegativeToUnsigned` arm --
///    `return uint64(int64(val))`, the low 64 bits, exactly like the integer
///    source above -- beside an overflow event. Captured:
///    `cast(-1.5e0 as unsigned)` is 18446744073709551614 with `1690 constant
///    -2 overflows bigint`, `cast(-1e0 as unsigned)` is 18446744073709551615,
///    and `cast(-1e300 as unsigned)` is 9223372036854775808 (Go's
///    out-of-range `int64(...)` conversion lands on `i64::MIN`, which is what
///    Rust's saturating `as i64` gives too).
///
/// `-0.4` is the boundary the two share: it ROUNDS to `-0.0`, which is not
/// `< 0`, so both answer 0 with no event at all.
///
/// The result is [`Datum::UInt`], so downstream comparisons and arithmetic
/// retain the domain instead of silently reinterpreting it as signed display
/// text.
#[cfg(test)]
fn to_u64_unsigned_in(v: &Datum, ctx: &dyn crate::Columns) -> Result<u64, EvalError> {
    crate::tikv::eval_cast_unsigned_value_in(ctx, v)
}

// Preserve the original value-only test surface without a production fallback
// or swallowing failures from the real/Float32 worker.
#[cfg(test)]
fn to_u64_unsigned(v: &Datum, ctx: &dyn crate::Columns) -> u64 {
    to_u64_unsigned_in(v, ctx).expect("unsigned cast test evaluation")
}

/// Delegates string and structured-JSON integer input diagnostics to the SDK.
/// Numeric overflow diagnostics belong to [`to_i64_signed_with_warnings`].
pub(crate) fn report_int_truncation(v: &Datum, ctx: &dyn crate::Columns) -> Result<(), EvalError> {
    crate::tikv::report_cast_integer_input_in(ctx, v)
}

/// Maps `MyDecimal.FromString`'s non-overflow parse dispositions to the
/// warning emitted by Go's string-to-decimal cast signature. The parsed value
/// is still retained (including a valid prefix); only a completely invalid or
/// truncated suffix contributes this statement warning.
pub(crate) fn report_decimal_input_truncation(v: &Datum, ctx: &dyn crate::Columns) {
    crate::tikv::report_cast_decimal_input_in(ctx, v);
}

/// Uses the SDK's strict-UTF8, value-only profile, not the ordinary cast parser.
pub(crate) fn to_f64_for_cast(v: &Datum) -> f64 {
    crate::tikv::eval_cast_float_value(v)
}

/// `CAST(... AS DATE)` and `CAST(... AS DATETIME)`: Go
/// `builtinCastStringAsTimeSig.evalTime`.
///
/// The whole body is Go's, in order: `types.ParseTime` under the STATEMENT's
/// type flags, then `handleInvalidTimeError` on failure, then the separate
/// `NO_ZERO_DATE` rejection of an all-zero result, then the DATE truncation
/// of the clock fields.
///
/// # Why this does not use this crate's own date parser
///
/// It used to, and that was two sources of truth for one table. Go asks
/// `Time.Check` the zero-in-date and invalid-date questions with the
/// statement's flags; `time_fn::calendar::parse_date_ymd` asks NEITHER and
/// rejects a zero month unconditionally, so `CAST('2024-00-01' AS DATE)`
/// answered NULL where TiDB answers `2024-00-01`, and every failing cast
/// answered NULL with NO warning where TiDB warns 1292.
/// `tidb_datatype::parse_time` is the faithful port of Go's `ParseTime`,
/// flags included, and is the same parser the WRITE path converts through.
/// `parse_date_ymd` stays strict for its own callers, which Go does NOT
/// relax (see its doc).
///
/// # The flags are the READ path's, not the write path's
///
/// Go `ResetContextOfStmt`'s `*ast.SelectStmt` arm sets `IgnoreZeroInDate`
/// UNCONDITIONALLY -- a zero-in-date reads back intact even under the default
/// mode that refuses to STORE one -- and takes `IgnoreInvalidDateErr` from
/// `ALLOW_INVALID_DATES` alone. With `TruncateAsWarning` also set, a bad
/// value is a warning plus NULL, never a statement failure: READS NEVER FAIL,
/// in any sql_mode.
fn cast_to_time(
    v: &Datum,
    source: Option<&tidb_datatype::FieldType>,
    ctx: &dyn crate::Columns,
    kind: tidb_datatype::TimeType,
    fsp: i64,
) -> Result<Datum, EvalError> {
    let Some(time) = cast_to_time_value(v, source, ctx, kind, Some(fsp))? else {
        return Ok(Datum::Null);
    };
    Ok(Datum::Time(time))
}

/// Go's repeated DATE-target rule: preserve the calendar fields and clear the
/// clock before the typed value leaves the cast signature.
fn truncate_clock_for_date(
    mut time: tidb_datatype::Time,
    kind: tidb_datatype::TimeType,
) -> tidb_datatype::Time {
    if kind != tidb_datatype::TimeType::Date {
        return time;
    }
    let core = time.core_time();
    time.set_core_time(tidb_datatype::CoreTime::from_date(
        core.year() as u16,
        core.month(),
        core.day(),
        0,
        0,
        0,
        0,
    ));
    time
}

/// The `types.Time` Go's chosen `builtinCast*AsTimeSig` produces. `None` is
/// Go's NULL (any warning already raised). Both explicit CAST and the
/// argument-cast seam retain this native temporal value.
fn cast_to_time_value(
    v: &Datum,
    source: Option<&tidb_datatype::FieldType>,
    ctx: &dyn crate::Columns,
    kind: tidb_datatype::TimeType,
    fsp: Option<i64>,
) -> Result<Option<tidb_datatype::Time>, EvalError> {
    // A `YEAR` source is the one case the datum kind cannot speak for. Go
    // `builtinCastIntAsTimeSig.evalTime` (`builtin_cast.go:1127-1131`) asks the
    // ARGUMENT'S TYPE, not the integer's digits:
    //
    //   if b.args[0].GetType(ctx).GetType() == mysql.TypeYear {
    //       res, err = types.ParseTimeFromYear(val)
    //   } else {
    //       res, err = types.ParseTimeFromNum(typeCtx(ctx), val, ...)
    //   }
    //
    // and `types.ParseTimeFromYear` (`time.go:2072-2081`) INJECTS the value as
    // the year FIELD -- `FromDate(int(year), 0, 0, 0, 0, 0, 0)`, so `2018` is
    // `2018-00-00 00:00:00` -- with `0` mapping to the zero date typed
    // `mysql.TypeDate`. Routing that same `2018` through `ParseTimeFromNum`,
    // which reads an int as a packed `YYYYMMDD`, FAILS and yields NULL. Every
    // other INT source keeps `ParseTimeFromNum` below.
    if let Some(year) = year_source_value(v, source) {
        let time = tidb_datatype::parse_time_from_year(year)
            .map_err(|_| EvalError::Unsupported("a YEAR value outside the year range"))?;
        return Ok(Some(time));
    }
    // A DURATION source is the second kind whose text cannot speak for it. Go
    // `builtinCastDurationAsTimeSig.evalTime` (`builtin_cast.go:2275-2291`)
    // never parses `20:00:01` as a wall clock; it calls
    // `val.ConvertToTimeWithTimestamp(tc, b.tp.GetType(), ts)`, which takes
    // the CALENDAR DATE of the statement's own timestamp and mixes the
    // elapsed time into it (`types/time.go:1500-1507`). Routing the text
    // through `ParseTime` instead reads the `20` as a YEAR.
    //
    // Neither half of this is visible in the recorded corpus: every recorded
    // statement that reaches it has the OTHER argument winning, so any wrong
    // conversion still prints the recorded answer. Both are pinned by
    // `a_duration_beside_a_temporal_literal_lands_on_the_statement_date` in
    // `tidb-session`, which puts the duration on the winning side and then
    // moves the session zone across the date line.
    //
    // The two date-mode flags SURVIVED their own mutation (hardcoding both to
    // `false` moves nothing): `mixDateAndDuration` always starts from a real
    // calendar date, so no zero or invalid component can arise for them to
    // rule on. They are passed because Go passes its `ctx`, not because a
    // value distinguishes them.
    if let Datum::Duration(duration) = v {
        let modes = ctx.date_modes();
        let (utc_secs, nanos, tz_offset) = ctx
            .now()
            .ok_or(EvalError::Unsupported("no statement clock for a TIME cast"))?;
        // Go reads the calendar date of `ts.In(ctx.Location())`; `now`'s third
        // field is that location's offset AT that instant, so a fixed offset
        // names the same civil day without re-resolving the zone.
        let zone = chrono::FixedOffset::east_opt(tz_offset).ok_or(EvalError::Unsupported(
            "session time-zone offset out of range",
        ))?;
        let Some(stamp) = chrono::DateTime::from_timestamp(utc_secs, nanos) else {
            return Ok(None);
        };
        return Ok(duration
            .convert_to_time(
                stamp.with_timezone(&zone),
                kind,
                !modes.no_zero_in_date,
                modes.allow_invalid_dates,
            )
            .and_then(|time| match fsp {
                Some(fsp) => time.round_frac(fsp, &ctx.time_zone()),
                None => Ok(time),
            })
            .ok());
    }
    let Some(s) = coerce_str(v)? else {
        return Ok(None);
    };
    let modes = ctx.date_modes();
    // Go routes each source TYPE to its own parser, not its text: only the
    // STRING/BYTES signatures (`builtinCastStringAsTimeSig`) parse the wall-
    // clock text through `ParseTime`. An INT source takes `ParseTimeFromNum`,
    // and a REAL/DECIMAL source takes `ParseTimeFromFloatString`, both of which
    // read the value as TiDB's packed `YYYYMMDD[HHMMSS]` NUMBER -- not as a
    // free-form date string. Funnelling a decimal through the string parser is
    // what made `cast(121212.1111 as datetime)` absorb `.1111` as a clock
    // (`2012-12-12 11:11:00`) and `cast(111.1 as datetime)` fail outright,
    // where TiDB answers `2012-12-12 00:00:00` and `2000-01-11 00:00:00`
    // (`expression/cast`). The parser choice mirrors `Datum::convert_to_time`,
    // the faithful write-path port.
    let parsed = parse_time_by_source(
        v,
        &s,
        kind,
        fsp,
        modes.allow_invalid_dates,
        &ctx.time_zone(),
    );
    let Ok((time, truncated, dst_adjusted)) = parsed else {
        // go routes each source TYPE to its own parser and its own warning:
        // the STRING sources warn `Incorrect datetime value: '<text>'`; the
        // numeric sources parse the INT64/FLOAT reinterpretations and warn
        // `Incorrect time value: '<int64>'` (a u64 overflow reads -1).
        if matches!(v, Datum::String(_) | Datum::Bytes(_)) {
            invalid_time_warning(ctx, &s, fsp.unwrap_or(0));
        } else if matches!(v, Datum::Decimal(_) | Datum::Real(_) | Datum::Float32(_)) {
            // The DECIMAL/REAL sources read the value through
            // `ParseTimeFromFloatString` -- the same wall-clock TEXT parser
            // the string sources use -- so a failure names the value with
            // the DATETIME word and the full decimal text (`cast(2.5 as
            // datetime)` warns `Incorrect datetime value: '2.5'`, not a
            // truncated integer).
            invalid_time_warning(ctx, &s, fsp.unwrap_or(0));
        } else {
            // go ParseTimeFromNum consumes the number's INT64 WRAP: a u64
            // overflow wraps to -1 (not the saturating i64::MAX).
            let signed = match v {
                Datum::UInt(n) => format!("{}", *n as i64),
                _ => v
                    .to_i64()
                    .map(|converted| format!("{}", converted.value))
                    .unwrap_or_else(|_| s.clone()),
            };
            ctx.append_warning(1292, &format!("Incorrect time value: '{signed}'"));
        }
        return Ok(None);
    };
    if truncated {
        ctx.append_warning(
            1292,
            &format!(
                "Truncated incorrect datetime value: '{}'",
                tidb_datatype::warning_subject_byte_cap(&s)
            ),
        );
    }
    if dst_adjusted {
        ctx.append_warning(
            8179,
            &format!(
                "Timestamp is not valid, since it is in Daylight Saving Time transition '{}' for time zone '{:?}'",
                s,
                ctx.time_zone(),
            ),
        );
    }
    // Go's SECOND check is the STRING signature's ALONE
    // (`builtinCastStringAsTimeSig`: `res.IsZero() && HasNoZeroDateMode()`).
    // The INT/REAL/DECIMAL signatures have no such rejection -- a numeric zero
    // is the zero time, not NULL (Go `#11203`), so `cast(0 as datetime)` and a
    // `0`-valued double/decimal column read `0000-00-00 00:00:00`, matching
    // `expression/cast`. Gating this on the text sources keeps the numeric
    // sources on Go's own no-rejection path.
    if matches!(v, Datum::String(_) | Datum::Bytes(_)) && time.is_zero() && modes.no_zero_date {
        // The go warning text renders the PARSED value: the string sources
        // parse at go's MaxFsp (6), so the zero time renders with its full
        // `.000000` fraction (oracle-captured on g-fsp's DAYOFYEAR row).
        invalid_time_warning(ctx, &s, 6);
        return Ok(None);
    }
    Ok(Some(truncate_clock_for_date(time, kind)))
}

pub(crate) fn parse_computed_time(
    value: &Datum,
    ctx: &dyn crate::Columns,
    kind: tidb_datatype::TimeType,
    fsp: Option<i64>,
) -> Result<Datum, EvalError> {
    Ok(cast_to_time_value(value, None, ctx, kind, fsp)?.map_or(Datum::Null, Datum::Time))
}

pub(crate) fn parse_computed_duration(
    value: &Datum,
    ctx: &dyn crate::Columns,
) -> Result<Datum, EvalError> {
    crate::tikv::eval_parse_computed_duration_in(ctx, value)
}

/// Go's `WrapWithCastAsTime(ctx, expr, types.NewFieldType(mysql.TypeDatetime))`
/// (`pkg/expression/builtin_cast.go:2817`), applied to an argument's VALUE
/// because this tier has no build-time expression rewrite to hang the cast on.
///
/// Go's early return is the whole of the special-casing:
///
/// ```go
/// exprTp := expr.GetType(ctx.GetEvalCtx()).GetType()
/// if tp.GetType() == exprTp {
///     return expr
/// } else if (exprTp == mysql.TypeDate || exprTp == mysql.TypeTimestamp) && tp.GetType() == mysql.TypeDatetime {
///     return expr
/// }
/// ```
///
/// -- i.e. a DATE, DATETIME or TIMESTAMP expression is handed through
/// untouched. In this tier those three are exactly the expressions that
/// evaluate to a [`Datum::Time`], so ONE pass-through arm covers all of Go's
/// early return; everything else goes through the cast Go builds, which is
/// [`cast_to_time_value`] (the same function `CAST(x AS DATETIME)` uses,
/// YEAR hole and all).
pub(crate) fn cast_arg_as_datetime(
    v: &Datum,
    source: Option<&tidb_datatype::FieldType>,
    ctx: &dyn crate::Columns,
) -> Result<Datum, EvalError> {
    if matches!(v, Datum::Time(_) | Datum::Null) {
        return Ok(v.clone());
    }
    Ok(
        cast_to_time_value(v, source, ctx, tidb_datatype::TimeType::DateTime, None)?
            .map_or(Datum::Null, Datum::Time),
    )
}

/// Go `WrapWithCastAsInt(ctx, expr, nil)` (`builtin_cast.go:2666-2698`),
/// applied to an argument's VALUE for the same reason
/// [`cast_arg_as_datetime`] is.
///
/// Go's own body, in full:
///
/// ```go
/// if expr.GetType(ctx.GetEvalCtx()).GetType() == mysql.TypeEnum {
///     ... expr.GetType(ctx.GetEvalCtx()).AddFlag(mysql.EnumSetAsIntFlag)
/// }
/// if expr.GetType(ctx.GetEvalCtx()).EvalType() == types.ETInt {
///     return expr
/// }
/// tp := types.NewFieldType(mysql.TypeLonglong)
/// ...
/// if targetType == nil {
///     tp.AddFlag(expr.GetType(ctx.GetEvalCtx()).GetFlag() & mysql.UnsignedFlag)
/// }
/// return BuildCastFunction(ctx, expr, tp)
/// ```
///
/// Three of Go's rules land here, and every one of them is a KIND test, not a
/// per-builtin condition:
///
///  * **The early return.** `EvalType() == types.ETInt` covers the integer
///    types and, through `FieldType.EvalType`'s own switch
///    (`pkg/parser/types/field_type.go:417-441`), `mysql.TypeBit` and
///    `mysql.TypeYear` as well. In this tier those are exactly the arguments
///    that evaluate to an [`Datum::Int`]/[`Datum::UInt`] — a `YEAR` reaches
///    the signature as its integer either way, so unlike the ETDatetime rung
///    the static type buys NOTHING here.
///
///  * **The hybrid short-circuit.** `mysql.TypeEnum` gets
///    `EnumSetAsIntFlag`, which flips its `EvalType()` to `ETInt` and takes
///    the early return with the ORDINAL. `mysql.TypeSet` does NOT get the
///    flag, but the cast Go then builds routes it right back to the same
///    reading: `castAsIntFunctionClass.getFunction` opens with
///    `if args[0].GetType(ctx.GetEvalCtx()).Hybrid() || IsBinaryLiteral(args[0]) {
///    sig = &builtinCastIntAsIntSig{bf} }` (`builtin_cast.go:146-147`), whose
///    body is `b.args[0].EvalInt`. ENUM, SET, BIT and a bit/hex LITERAL
///    therefore all reach the signature as their ordinal or bit integer, and
///    [`Datum::to_i64_in`](tidb_datatype::Datum::to_i64_in) already reads all
///    four that way -- so the ordinary cast below is already Go's answer for
///    them and needs no arm of its own.
///
///  * **The unsigned inheritance.** `targetType` is `nil` at every
///    `newBaseBuiltinFuncWithTp` call site (`builtin.go:202`), so the built
///    cast is `UNSIGNED` exactly when the SOURCE type is, and that flag is
///    what `builtinTruncateIntSig` reads back out of
///    `b.args[1].GetType(ctx).GetFlag()` (`builtin_math.go:2166`). A tier
///    without the source type therefore answers SIGNED, which is Go's answer
///    for every argument that is not an unsigned non-integer.
///
/// Confirmed against real TiDB (`gorun`) over an `enum('x','y','z')` holding
/// `'y'`, a `set('a','b','c')` holding `'a,c'` and a `bit(8)` holding
/// `b'00000011'`: `make_set(e,'p','q','r')` is `q` (ordinal 2),
/// `make_set(s,'p','q','r')` is `p,r` (bits 5) and `make_set(b,'p','q','r')`
/// is `p,q` (bits 3).
pub(crate) fn cast_arg_as_int(
    v: &Datum,
    source: Option<&tidb_datatype::FieldType>,
    ctx: &dyn crate::Columns,
) -> Result<Datum, EvalError> {
    if matches!(v, Datum::Int(_) | Datum::UInt(_) | Datum::Null) {
        return Ok(v.clone());
    }
    // go's WrapWithCastAsInt over a JSON operand is `builtinCastJSONAsIntSig`:
    // the document's MarshalJSON text re-reads as an integer (StrToInt), with
    // go's 1292 truncation warning when the text is not a clean integer —
    // captured: `bitand(j, j)` over `{}` warns twice and answers 0, while
    // JSON `3` coerces silently.
    if let Datum::Json(value) = v {
        let as_text = Datum::new_string(value.to_string());
        report_int_truncation(&as_text, ctx)?;
        return Ok(Datum::Int(to_i64_signed(&as_text)));
    }
    let cast = if source.is_some_and(tidb_datatype::FieldType::is_unsigned) {
        CastType::Unsigned
    } else {
        CastType::Signed
    };
    eval_cast(&cast, v.clone(), source, ctx)
}

/// Go `WrapWithCastAsString(ctx, expr)` (`builtin_cast.go:2769-2813`), applied
/// to an argument's VALUE for the same reason [`cast_arg_as_datetime`] is.
///
/// Go's body is one early return and then a pile of RESULT-TYPE arithmetic:
///
/// ```go
/// exprTp := expr.GetType(ctx.GetEvalCtx())
/// if exprTp.EvalType() == types.ETString {
///     return expr
/// }
/// argLen := exprTp.GetFlen()
/// ... // argLen adjustments, then charset/collation on the built `tp`
/// return BuildCastFunction(ctx, expr, tp)
/// ```
///
/// Everything after the early return sets `tp`'s FLEN and CHARSET, which are
/// metadata: none of the `argLen` arms can truncate, because every one of them
/// is at least as wide as the rendering it describes (`mysql.MaxIntWidth` for
/// an integer, `GetFlen()+3` for a decimal, `-1` -- unspecified -- for a
/// float). So at the VALUE seam this cast is the early return plus "render
/// the value's text", and the two things worth transcribing are which values
/// take the early return and what BIT renders as.
///
///  * **The early return** is `EvalType() == types.ETString`, and
///    `FieldType.EvalType` (`pkg/parser/types/field_type.go:436-441`) puts
///    `mysql.TypeEnum` and `mysql.TypeSet` there unless they carry
///    `EnumSetAsIntFlag` -- a flag only `WrapWithCastAsInt` ever adds. An
///    ENUM or SET argument is therefore NOT wrapped, and the signature body
///    reads it with `EvalString`, which is its NAME. Captured from real TiDB
///    (`gorun`) over an `enum('{}','[1]','x')` holding `'{}'`: `quote(e)` is
///    `'{}'` and `ltrim(e)` is `{}` -- the name, never the ordinal `1`. This
///    is the exact OPPOSITE of [`cast_arg_as_int`]'s hybrid arm, where the
///    same column reaches the signature as its ordinal.
///
///  * **BIT is the one hybrid that is NOT string-typed**: `mysql.TypeBit` is
///    `ETInt`, so it does not take the early return. The cast Go then builds
///    lands on `castAsStringFunctionClass.getFunction`'s own hybrid arm
///    (`builtin_cast.go:315-321`), whose `castBitAsUnBinary` test is false
///    here because `WrapWithCastAsString` already set the target charset to
///    `charset.CharsetBin` for `TypeBit` (`:2801-2804`) -- so the signature
///    is `builtinCastStringAsStringSig` and the value is the bit's RAW BYTES,
///    not its decimal digits. Captured over a `bit(8)` holding `b'11111111'`:
///    `hex(ltrim(b))` is `FF`, and `hex(quote(b))` is `27EFBFBD27` -- one
///    0xFF byte that `Quote`'s own `[]rune` conversion then replaces.
///
/// `source` is unused: unlike [`cast_arg_as_datetime`]'s `YEAR` and
/// [`cast_arg_as_int`]'s `UNSIGNED`, nothing this cast produces depends on a
/// fact the datum does not already carry.
pub(crate) fn cast_arg_as_string(
    v: &Datum,
    _source: Option<&tidb_datatype::FieldType>,
    _ctx: &dyn crate::Columns,
) -> Result<Datum, EvalError> {
    match v {
        // Go's early return: every `types.ETString` eval type, which is every
        // string kind plus the two string-typed hybrids. Passing the datum
        // through UNCHANGED (rather than flattening it to bytes here) is what
        // keeps a `Datum::String`'s collation and a `Datum::Bytes`'s binary
        // signature readable by the body -- see `crate::string_signature`.
        Datum::Null
        | Datum::String(_)
        | Datum::Bytes(_)
        | Datum::Enum(..)
        | Datum::Set(..)
        | Datum::BinaryLiteral(_) => Ok(v.clone()),
        // The BIT arm above: raw bytes under the binary charset Go's `tp`
        // was given, which in this tier is `Datum::Bytes`.
        Datum::Bit(bits) => Ok(Datum::new_bytes(bits.as_bytes().to_vec())),
        // Everything else takes one of `castAsStringFunctionClass`'s
        // per-source signatures, all of which render the value's own text
        // under the connection charset -- which is exactly what
        // `crate::coerce::coerce_str_bytes` already is.
        _ => Ok(crate::coerce::coerce_str_bytes(v)?.map_or(Datum::Null, Datum::new_string)),
    }
}

/// The result type Go's `WrapWithCastAsString` assigns to `source`.
///
/// String-typed arguments take the source function's early return unchanged.
/// Every other type becomes `VAR_STRING`: an explicit collation survives,
/// BIT stays binary, and all remaining values use the connection charset.
/// The width is the same source-backed calculation used by CONCAT metadata.
pub(crate) fn cast_arg_as_string_type(
    source: &tidb_datatype::FieldType,
    explicit_collation: bool,
    connection: (&str, &str),
) -> tidb_datatype::FieldType {
    if source.eval_type() == tidb_datatype::EvalType::String {
        return source.clone();
    }
    let mut target = tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::VarString);
    if explicit_collation {
        target.set_charset_name(source.charset_name());
        target.set_collation_name(source.collation_name());
    } else if source.code() == tidb_datatype::FieldTypeCode::Bit {
        target.set_charset_name("binary");
        target.set_collation_name("binary");
    } else {
        let (charset, collation) = connection;
        target.set_charset_name(charset);
        target.set_collation_name(collation);
    }
    target.set_flen(crate::rewriter::result_type::string_cast_flen(source));
    target.set_decimal(tidb_datatype::UNSPECIFIED_LENGTH);
    target
}

/// The integer a `YEAR`-typed operand carries, or `None` when the operand is
/// not a `YEAR` at all.
///
/// The type test is Go's own (`b.args[0].GetType(ctx).GetType() ==
/// mysql.TypeYear`); the kind test is this tier's, because a `YEAR` expression
/// always evaluates to an integer and anything else under a `YEAR` field type
/// is a value this tier produced, not one Go's `EvalInt` could have returned.
fn year_source_value(v: &Datum, source: Option<&tidb_datatype::FieldType>) -> Option<i64> {
    if source?.code() != tidb_datatype::FieldTypeCode::Year {
        return None;
    }
    match v {
        Datum::Int(value) => Some(*value),
        Datum::UInt(value) => i64::try_from(*value).ok(),
        _ => None,
    }
}

/// Parses one cast operand into a `Time`, choosing the parser by SOURCE TYPE
/// the way Go's per-signature `builtinCast*AsTimeSig` split does (see
/// [`cast_to_time`]'s doc for why the text is not enough). The read-path flags
/// are the string signature's own: `allow_zero_in_date` is UNCONDITIONALLY
/// `true` (a SELECT reads a zero-in-date back intact), and `allow_invalid_date`
/// follows `ALLOW_INVALID_DATES`. `Err(())` is Go's parse failure, which the
/// caller turns into a 1292 warning plus NULL.
///
/// The parser routing mirrors `Datum::convert_to_time`, the faithful write-path
/// port: INT/UINT -> `parse_time_from_num`, DECIMAL -> `parse_time_from_decimal`,
/// REAL/FLOAT -> `parse_time_from_float64`. A UINT beyond `i64::MAX` cannot be a
/// packed datetime and is a parse failure. The float/decimal parsers classify
/// DATE-vs-DATETIME by digit count, so the target `kind` is re-imposed with
/// `set_kind` -- exactly what `convert_to_time` does after those two parsers.
fn parse_time_by_source(
    v: &Datum,
    text: &str,
    kind: tidb_datatype::TimeType,
    fsp: Option<i64>,
    allow_invalid: bool,
    zone: &tidb_datatype::SessionTimeZone,
) -> Result<(tidb_datatype::Time, bool, bool), ()> {
    match v {
        Datum::Int(value) => tidb_datatype::parse_time_from_num(
            *value,
            kind,
            fsp.unwrap_or(0),
            true,
            allow_invalid,
            true,
            zone,
        )
        .map(|parsed| (parsed.time, false, parsed.dst_adjusted))
        .map_err(|_| ()),
        Datum::UInt(value) => {
            let signed = i64::try_from(*value).map_err(|_| ())?;
            tidb_datatype::parse_time_from_num(
                signed,
                kind,
                fsp.unwrap_or(0),
                true,
                allow_invalid,
                true,
                zone,
            )
            .map(|parsed| (parsed.time, false, parsed.dst_adjusted))
            .map_err(|_| ())
        }
        Datum::Decimal(value) => {
            let mut time = tidb_datatype::parse_time_from_decimal(value, true, allow_invalid, zone)
                .map_err(|_| ())?;
            time.set_kind(kind);
            match fsp {
                Some(fsp) => time
                    .round_frac(fsp, zone)
                    .map(|time| (time, false, false))
                    .map_err(|_| ()),
                None => Ok((time, false, false)),
            }
        }
        Datum::Real(value) => real_to_time(*value, kind, fsp.unwrap_or(0), allow_invalid, zone)
            .map(|time| (time, false, false)),
        Datum::Float32(value) => real_to_time(*value, kind, fsp.unwrap_or(0), allow_invalid, zone)
            .map(|time| (time, false, false)),
        Datum::Time(value) => {
            let mut time = *value;
            time.set_kind(kind);
            match fsp {
                Some(fsp) => time
                    .round_frac(fsp, zone)
                    .map(|time| (time, false, false))
                    .map_err(|_| ()),
                None => Ok((time, false, false)),
            }
        }
        // STRING/BYTES and every other coercible source keep Go's
        // `builtinCastStringAsTimeSig` path: parse the wall-clock TEXT.
        //
        // The zone is the SESSION's, as Go's `builtinCastStringAsTimeSig` passes
        // `ctx.TypeCtx()`. It does more than TIMESTAMP's range check: a literal
        // whose fraction is wider than `fsp` ROUNDS, and Go applies that carry to
        // the INSTANT in `ctx.Location()`, so a carry landing on a DST transition
        // moves the wall clock by the offset change too. CAPTURED from real TiDB:
        // `cast('2011-03-13 01:59:59.9999999' as datetime)` is
        // `2011-03-13 02:00:00` under `time_zone='UTC'` and `03:00:00` under
        // `'America/Los_Angeles'` (02:00 does not exist there), and
        // `cast('2011-11-06 01:59:59.9999999' as datetime)` is `02:00:00` under
        // UTC and `01:00:00` there (the repeated hour). Hardcoding UTC returned
        // the UTC answer for every session.
        _ => tidb_datatype::parse_time(
            text,
            kind,
            fsp.unwrap_or_else(|| i64::from(tidb_datatype::get_fsp(text))),
            false,
            true,
            allow_invalid,
            zone,
        )
        .map(|parsed| (parsed.time, parsed.truncated, parsed.dst_adjusted))
        .map_err(|_| ()),
    }
}

/// REAL/FLOAT source shared by `Real` and `Float32`: Go
/// `builtinCastRealAsTimeSig` reads the float's packed-number form. `0.0` is
/// the zero time rather than a failure (Go's `#11203` guard); the float parser
/// already returns the zero time for a `0` integer part, so no special case is
/// needed here.
fn real_to_time(
    value: f64,
    kind: tidb_datatype::TimeType,
    fsp: i64,
    allow_invalid: bool,
    zone: &tidb_datatype::SessionTimeZone,
) -> Result<tidb_datatype::Time, ()> {
    let mut time =
        tidb_datatype::parse_time_from_float64(value, true, allow_invalid, zone).map_err(|_| ())?;
    time.set_kind(kind);
    time.round_frac(fsp, zone).map_err(|_| ())
}

/// Go `handleInvalidTimeError` on the read path: `ErrWrongValue` (1292)
/// becomes a warning and the cast yields NULL.
fn invalid_time_warning(ctx: &dyn crate::Columns, input: &str, fsp: i64) {
    // go splits the failure classes by SHAPE (oracle-captured):
    //  * the ZERO date renders through the parsed value with its fsp
    //    (`'0000-00-00 00:00:00.000000'` for fsp 6 -- g-fsp);
    //  * a VALID calendar date prefix followed by trailing characters raises
    //    the VALUE class 8034 with the raw text (`CAST('2020-01-01x' AS
    //    DATE)` -- m17);
    //  * a calendar-INVALID date shape renders through its parsed parts
    //    without zero padding under the truncation class 1292
    //    (`'2020-2-30'` -- m4);
    //  * everything else is 1292 with the raw text (`'abc'` -- m1).
    let trimmed = input.trim();
    let head: Vec<&str> = trimmed.splitn(3, '-').collect();
    if head.len() == 3
        && !head[0].is_empty()
        && !head[1].is_empty()
        && head[0].bytes().all(|b| b.is_ascii_digit())
        && head[1].bytes().all(|b| b.is_ascii_digit())
    {
        let digits = |part: &str| -> i64 {
            part.bytes()
                .take_while(|byte| byte.is_ascii_digit())
                .fold(0i64, |acc, byte| acc * 10 + i64::from(byte - b'0'))
        };
        let (year, month) = (digits(head[0]), digits(head[1]));
        let day_digits = head[2]
            .bytes()
            .take_while(|byte| byte.is_ascii_digit())
            .count();
        let day = digits(&head[2][..day_digits]);
        if year == 0 && month == 0 && day == 0 {
            let fraction = if fsp > 0 {
                format!(".{:0width$}", 0, width = fsp as usize)
            } else {
                String::new()
            };
            ctx.append_warning(
                1292,
                &format!("Incorrect datetime value: '0000-00-00 00:00:00{fraction}'"),
            );
            return;
        }
        if (1..=12).contains(&month) && (1..=31).contains(&day) {
            let days_in_month = match month {
                1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
                4 | 6 | 9 | 11 => 30,
                _ => {
                    let leap = (year % 4 == 0 && year % 100 != 0) || year % 400 == 0;
                    if leap {
                        29
                    } else {
                        28
                    }
                }
            };
            if day <= days_in_month {
                // A valid calendar date prefix plus trailing characters.
                ctx.append_warning(8034, &format!("Incorrect datetime value: '{input}'"));
                return;
            }
            if head[2].bytes().all(|byte| byte.is_ascii_digit()) {
                let rendered = format!("{year}-{month}-{day}");
                ctx.append_warning(1292, &format!("Incorrect datetime value: '{rendered}'"));
                return;
            }
        }
    }
    ctx.append_warning(1292, &format!("Incorrect datetime value: '{input}'"));
}

/// `CAST(... AS YEAR)`: the operand's calendar year if it parses as a
/// date-shaped string (confirmed via `goeval`: `CAST('2021-01-01' AS YEAR)`
/// is `2021`), else a plain `SIGNED`-style integer coercion (confirmed via
/// `goeval`: `CAST('99' AS YEAR)` is `99` — NOT the two-digit-year century
/// pivot the `YEAR` COLUMN TYPE applies at storage time, a genuinely separate
/// rule this scalar CAST does not share).
///
/// A DURATION operand is the one exception to the datum-kind fallback. Go's
/// `builtinCastDurationAsIntSig` calls `Duration.ConvertToYear`, which mixes
/// the elapsed time into the statement clock's local calendar date. The
/// previous Rust path treated the duration as its packed integer (`125959`),
/// so it never observed either `ctx.now()` or the session time zone.
fn cast_to_year(v: &Datum, ctx: &dyn crate::Columns) -> Result<Datum, EvalError> {
    crate::tikv::eval_cast_year_in(ctx, v)
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use super::*;
    use crate::context::NoColumns;

    struct WarningContext(RefCell<Vec<(u16, String)>>);

    impl crate::Columns for WarningContext {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }

        fn append_warning(&self, code: u16, message: &str) {
            self.0.borrow_mut().push((code, message.to_owned()));
        }
    }

    #[test]
    fn truncated_datetime_cast_keeps_the_value_and_warns() {
        for (input, expected) in [
            ("1701020304.111", "2017-01-02 03:04:11"),
            ("150101.a", "2015-01-01 00:00:00"),
            ("150101.1a", "2015-01-01 01:00:00"),
            ("150101.1a1", "2015-01-01 01:00:00"),
            ("1101010101.111", "2011-01-01 01:01:11"),
            ("1101010101.11aaaaa", "2011-01-01 01:01:11"),
            ("1101010101.a1aaaaa", "2011-01-01 01:01:00"),
            ("970101.111a1", "1997-01-01 11:01:00"),
        ] {
            let ctx = WarningContext(RefCell::new(Vec::new()));
            let got = eval_cast(
                &CastType::DateTime { fsp: Some(0) },
                Datum::new_string(input),
                None,
                &ctx,
            )
            .expect("a truncated datetime remains a successful read cast");
            assert_eq!(render_time(&got), expected, "{input}");
            assert_eq!(
                ctx.0.borrow().as_slice(),
                &[(
                    1292,
                    format!("Truncated incorrect datetime value: '{input}'")
                )],
                "{input}"
            );
        }
    }

    #[test]
    fn signed_string_overflow_keeps_go_complement_and_warning_semantics() {
        for (input, expected, warning) in [
            (
                "18446744073709551614",
                -2,
                (
                    8030,
                    "Cast to signed converted positive out-of-range integer to its negative complement",
                ),
            ),
            (
                "18446744073709551616",
                -1,
                (1292, "Truncated incorrect INTEGER value: '18446744073709551616'"),
            ),
            (
                "-9223372036854775809",
                i64::MIN,
                (1292, "Truncated incorrect INTEGER value: '-9223372036854775809'"),
            ),
        ] {
            let ctx = WarningContext(RefCell::new(Vec::new()));
            assert_eq!(
                eval_cast(
                    &CastType::Signed,
                    Datum::new_string(input),
                    None,
                    &ctx,
                )
                .unwrap(),
                Datum::Int(expected),
                "{input}",
            );
            assert_eq!(
                ctx.0.borrow().as_slice(),
                &[(warning.0, warning.1.to_owned())],
                "{input}",
            );
        }
    }

    /// The string-to-JSON literal parse routes a `{}` OBJECT document
    /// through `builtinCastJSONAsIntSig`'s StrToInt re-read when cast to an
    /// integer — the object text has no integer prefix, so the 1292 row is
    /// mandatory (`g-t3`: `CAST(j AS SIGNED)` over the object column).
    #[test]
    fn json_object_to_int_cast_warns_truncation_like_string() {
        let ctx = WarningContext(RefCell::new(Vec::new()));
        let json = Datum::new_json(tidb_datatype::BinaryJSON::parse("{}").expect("json"));
        let got = eval_cast(&CastType::Signed, json, None, &ctx)
            .expect("a truncated JSON int cast remains a successful read");
        assert_eq!(got, Datum::Int(0));
        assert_eq!(
            ctx.0.borrow().as_slice(),
            &[(1292, "Truncated incorrect INTEGER value: '{}'".to_owned())],
        );
    }

    /// Go's `ErrTruncatedWrongVal` template truncates the quoted value at
    /// 128 bytes (`"Truncated incorrect %-.64s value: '%-.128s'"`).
    #[test]
    fn double_cast_warning_value_truncates_at_128_bytes_like_go() {
        let ctx = WarningContext(RefCell::new(Vec::new()));
        let long = format!("x{margin}", margin = "9".repeat(300));
        let got = eval_cast(
            &CastType::Double,
            Datum::new_string(long.clone()),
            None,
            &ctx,
        )
        .expect("a truncated DOUBLE cast remains a successful read");
        assert_eq!(got, Datum::Real(0.0));
        let warnings = ctx.0.borrow();
        let (code, text) = &warnings[0];
        assert_eq!(*code, 1292);
        // The quoted subject is the first 128 bytes of the input, not all 300.
        assert!(
            text.contains(&format!("value: '{}'", &long[..128])),
            "{text}"
        );
        assert!(
            !text.contains(&format!("value: '{}'", &long[..129])),
            "{text}"
        );
    }

    #[test]
    fn double_cast_warning_subject_stops_at_nul_like_go() {
        let ctx = WarningContext(RefCell::new(Vec::new()));
        let got = eval_cast(&CastType::Double, Datum::new_string("\0 12"), None, &ctx)
            .expect("a truncated DOUBLE cast remains a successful read");
        assert_eq!(got, Datum::Real(0.0));
        assert_eq!(
            ctx.0.borrow().as_slice(),
            &[(1292, "Truncated incorrect DOUBLE value: ''".to_owned())]
        );
    }

    #[test]
    fn cast_decimal_as_unsigned_keeps_the_upper_half_of_unsigned_bigint() {
        // The wired bug: routing a decimal through the signed path saturated the
        // upper half of UNSIGNED BIGINT at i64::MAX (9223372036854775807). Go
        // rounds half-up then MyDecimal.ToUint, keeping the full u64 range.
        assert_eq!(
            to_u64_unsigned(
                &Datum::Decimal(Decimal::from_literal("10000000000000000000")),
                &NoColumns
            ),
            10_000_000_000_000_000_000,
            "one past i64::MAX is kept, not saturated"
        );
        assert_eq!(
            to_u64_unsigned(
                &Datum::Decimal(Decimal::from_literal("18446744073709551615")),
                &NoColumns
            ),
            u64::MAX
        );
        // Half-up rounding and the negative-to-zero rule are unchanged.
        assert_eq!(
            to_u64_unsigned(&Datum::Decimal(Decimal::from_literal("5.6")), &NoColumns),
            6
        );
        assert_eq!(
            to_u64_unsigned(
                &Datum::Decimal(Decimal::from_literal("5.6").negate()),
                &NoColumns
            ),
            0
        );
        // A signed-integer source still reinterprets its low 64 bits:
        // CAST(-5 AS UNSIGNED) stays 18446744073709551611, unaffected by the fix.
        assert_eq!(
            to_u64_unsigned(&Datum::Int(-5), &NoColumns),
            18_446_744_073_709_551_611
        );
    }

    #[test]
    fn cast_real_as_unsigned_keeps_the_upper_half_of_unsigned_bigint() {
        // The sibling wired bug: a real routed through the signed path saturated
        // the upper half of UNSIGNED BIGINT at i64::MAX (9223372036854775807).
        // Go rounds half-to-even (RoundFloat) then ConvertFloatToUint across the
        // full u64 range. 1e19 is exactly representable in f64.
        assert_eq!(
            to_u64_unsigned(&Datum::Real(1.0e19), &NoColumns),
            10_000_000_000_000_000_000,
            "a real past i64::MAX is kept, not saturated at i64::MAX"
        );
        // A magnitude past u64::MAX saturates to MaxUint64 (upperBound clamp).
        assert_eq!(to_u64_unsigned(&Datum::Real(1.0e30), &NoColumns), u64::MAX);
        // Half-to-even rounding (Go RoundFloat = math.RoundToEven), the same rule
        // the signed real path uses: 2.5 -> 2, 3.5 -> 4.
        assert_eq!(to_u64_unsigned(&Datum::Real(2.5), &NoColumns), 2);
        assert_eq!(to_u64_unsigned(&Datum::Real(3.5), &NoColumns), 4);
        // A negative real does NOT clamp to zero: Go's
        // `AllowNegativeToUnsigned` arm returns `uint64(int64(val))`, the same
        // low-64-bit reinterpretation the integer source above gets. This
        // assertion used to read `, 0)` and pinned the WRONG answer.
        // Captured (`goeval`): `cast(-1.5e0 as unsigned)` ->
        // 18446744073709551614, `cast(-1e0 as unsigned)` ->
        // 18446744073709551615, `cast(-1e300 as unsigned)` ->
        // 9223372036854775808.
        assert_eq!(
            to_u64_unsigned(&Datum::Real(-1.5), &NoColumns),
            18_446_744_073_709_551_614
        );
        assert_eq!(
            to_u64_unsigned(&Datum::Real(-1.0), &NoColumns),
            18_446_744_073_709_551_615
        );
        assert_eq!(
            to_u64_unsigned(&Datum::Real(-5.6), &NoColumns),
            18_446_744_073_709_551_610
        );
        assert_eq!(
            to_u64_unsigned(&Datum::Real(-1.0e300), &NoColumns),
            9_223_372_036_854_775_808
        );
        // -0.4 ROUNDS to -0.0, which is not `< 0`, so it is the one negative
        // input that really is 0 -- with no warning either.
        assert_eq!(to_u64_unsigned(&Datum::Real(-0.4), &NoColumns), 0);
        // The DECIMAL source keeps Go's own opposite rule: negative -> 0.
        assert_eq!(
            to_u64_unsigned(
                &Datum::Decimal(Decimal::from_literal("1.5").negate()),
                &NoColumns
            ),
            0
        );
    }

    #[test]
    fn float32_unsigned_cast_reports_negative_overflow() {
        let ctx = WarningContext(RefCell::new(Vec::new()));
        assert_eq!(
            eval_cast(&CastType::Unsigned, Datum::Float32(-1.5), None, &ctx).unwrap(),
            Datum::UInt(18_446_744_073_709_551_614)
        );
        assert_eq!(
            ctx.0.borrow().as_slice(),
            &[(1690, "constant -2 overflows bigint".to_owned())]
        );
    }

    #[test]
    fn real_unsigned_cast_reports_positive_overflow() {
        let ctx = WarningContext(RefCell::new(Vec::new()));
        assert_eq!(
            eval_cast(&CastType::Unsigned, Datum::Real(1.0e30), None, &ctx).unwrap(),
            Datum::UInt(u64::MAX)
        );
        assert_eq!(
            ctx.0.borrow().as_slice(),
            &[(1690, "constant 1e+30 overflows bigint".to_owned())]
        );
    }

    /// `CAST(str AS DATETIME)` rounds in the SESSION zone, not in UTC.
    ///
    /// Go's `builtinCastStringAsTimeSig` passes `ctx.TypeCtx()`, whose
    /// location the fractional-carry arm of `parseDatetime` applies the carry
    /// in. CAPTURED from real TiDB, both instants chosen so the carry lands
    /// exactly on a DST transition:
    ///
    /// ```text
    /// select cast('2011-03-13 01:59:59.9999999' as datetime)
    ///   time_zone='UTC'                 2011-03-13 02:00:00
    ///   time_zone='America/Los_Angeles' 2011-03-13 03:00:00
    /// select cast('2011-11-06 01:59:59.9999999' as datetime)
    ///   time_zone='UTC'                 2011-11-06 02:00:00
    ///   time_zone='America/Los_Angeles' 2011-11-06 01:00:00
    /// ```
    ///
    /// A four-zone probe over ordinary instants shows NO difference at all,
    /// which is why this pin uses the transition instants: an invariance
    /// probe here is a false negative.
    #[test]
    fn a_string_cast_to_datetime_rounds_in_the_session_zone() {
        use crate::Columns as _;
        struct Zoned(tidb_datatype::SessionTimeZone);
        impl crate::Columns for Zoned {
            fn get(&self, _: &[String]) -> Option<Datum> {
                None
            }
            fn time_zone(&self) -> tidb_datatype::SessionTimeZone {
                self.0.clone()
            }
        }
        let utc = Zoned(tidb_datatype::SessionTimeZone::utc());
        let la = Zoned(tidb_datatype::SessionTimeZone::Named(
            chrono_tz::America::Los_Angeles,
        ));
        for (input, in_utc, in_la) in [
            (
                "2011-03-13 01:59:59.9999999",
                "2011-03-13 02:00:00",
                "2011-03-13 03:00:00",
            ),
            (
                "2011-11-06 01:59:59.9999999",
                "2011-11-06 02:00:00",
                "2011-11-06 01:00:00",
            ),
        ] {
            for (ctx, expected) in [(&utc, in_utc), (&la, in_la)] {
                let got = cast_to_time(
                    &Datum::new_string(input.to_string()),
                    None,
                    ctx,
                    tidb_datatype::TimeType::DateTime,
                    0,
                )
                .unwrap_or_else(|error| panic!("{input}: {error:?}"));
                assert_eq!(
                    render_time(&got),
                    expected,
                    "{input} in {:?}",
                    ctx.time_zone()
                );
            }
        }
    }

    #[test]
    fn a_string_cast_to_timestamp_adjusts_dst_gap_and_warns() {
        struct ZonedWarnings {
            zone: tidb_datatype::SessionTimeZone,
            warnings: RefCell<Vec<(u16, String)>>,
        }
        impl crate::Columns for ZonedWarnings {
            fn get(&self, _: &[String]) -> Option<Datum> {
                None
            }
            fn time_zone(&self) -> tidb_datatype::SessionTimeZone {
                self.zone.clone()
            }
            fn append_warning(&self, code: u16, message: &str) {
                self.warnings.borrow_mut().push((code, message.to_owned()));
            }
        }

        let ctx = ZonedWarnings {
            zone: tidb_datatype::SessionTimeZone::Named(chrono_tz::America::Los_Angeles),
            warnings: RefCell::new(Vec::new()),
        };
        let got = cast_to_time(
            &Datum::new_string("2018-03-11 02:00:16".to_owned()),
            None,
            &ctx,
            tidb_datatype::TimeType::Timestamp,
            0,
        )
        .expect("DST-gap TIMESTAMP cast keeps Go's adjusted value");
        assert_eq!(render_time(&got), "2018-03-11 03:00:00");
        let warnings = ctx.warnings.borrow();
        assert_eq!(warnings.len(), 1);
        assert_eq!(warnings[0].0, 8179);
        assert!(warnings[0]
            .1
            .contains("Daylight Saving Time transition '2018-03-11 02:00:16'"));
    }

    fn render_time(v: &Datum) -> String {
        match v {
            Datum::Time(time) => time.to_string(),
            Datum::String(text) => crate::coerce::string_text(text)
                .expect("temporal CAST text is valid UTF-8")
                .to_owned(),
            Datum::Null => "NULL".to_owned(),
            other => panic!("a temporal cast produced an unexpected {other:?}"),
        }
    }

    fn datetime_fsp(v: Datum, fsp: i64) -> String {
        render_time(
            &cast_to_time(&v, None, &NoColumns, tidb_datatype::TimeType::DateTime, fsp)
                .expect("cast"),
        )
    }

    fn datetime(v: Datum) -> String {
        datetime_fsp(v, 0)
    }

    /// A DECIMAL source is read as TiDB's packed `YYYYMMDD[HHMMSS]` NUMBER
    /// (Go `builtinCastDecimalAsTimeSig` -> `ParseTimeFromFloatString`), NOT as
    /// wall-clock text. Funnelling `121212.1111` through the STRING parser made
    /// it absorb the `.1111` fraction as a clock (`2012-12-12 11:11:00`) where
    /// TiDB reads the whole-date number `121212` and answers midnight
    /// (`expression/cast`: `cast(d2 as datetime)` over `121212.1111`).
    #[test]
    fn a_decimal_source_reads_the_packed_number_not_the_wall_clock_text() {
        assert_eq!(
            datetime(Datum::Decimal(Decimal::from_literal("121212.1111"))),
            "2012-12-12 00:00:00",
        );
        // A number shorter than a full date is zero-padded YYMMDD, so `111`
        // is `00-01-11` -> `2000-01-11`; the string parser rejected it as NULL.
        assert_eq!(
            datetime(Datum::Decimal(Decimal::from_literal("111.1"))),
            "2000-01-11 00:00:00",
        );
        // A month of 13 is still an invalid date -> NULL, unchanged.
        assert_eq!(
            datetime(Datum::Decimal(Decimal::from_literal("1311.1"))),
            "NULL",
        );
    }

    /// A REAL/DOUBLE source takes the same packed-number reading
    /// (Go `builtinCastRealAsTimeSig`), so `1122.1` is `00-11-22`.
    #[test]
    fn a_real_source_reads_the_packed_number() {
        assert_eq!(datetime(Datum::Real(1122.1)), "2000-11-22 00:00:00",);
        assert_eq!(datetime(Datum::Float32(1122.1)), "2000-11-22 00:00:00",);
    }

    /// An INTEGER source is `ParseTimeFromNum` (Go `builtinCastIntAsTimeSig`):
    /// `20170118` is the packed date, no fractional text to misread.
    #[test]
    fn an_integer_source_reads_the_packed_number() {
        assert_eq!(datetime(Datum::Int(20_170_118)), "2017-01-18 00:00:00",);
    }

    /// Go's numeric cast signatures have NO `NO_ZERO_DATE` rejection (the
    /// `#11203` guard: a zero number is the zero time, never NULL), unlike the
    /// STRING signature. Under the default SQL mode -- which DOES carry
    /// `NO_ZERO_DATE` ([`NoColumns`] answers `TIDB_DEFAULT_SQL_MODE`) -- a zero
    /// INT/REAL/DECIMAL therefore reads `0000-00-00 00:00:00`, matching
    /// `expression/cast`'s `(0, 0, 0)` row, while a zero-date STRING is still
    /// rejected to NULL.
    #[test]
    fn a_numeric_zero_is_the_zero_time_not_null() {
        let zero = "0000-00-00 00:00:00";
        assert_eq!(datetime(Datum::Int(0)), zero, "int 0");
        assert_eq!(datetime(Datum::UInt(0)), zero, "uint 0");
        assert_eq!(datetime(Datum::Real(0.0)), zero, "real 0");
        assert_eq!(
            datetime(Datum::Decimal(Decimal::from_literal("0"))),
            zero,
            "decimal 0"
        );
        // The STRING signature keeps Go's zero-date rejection under NO_ZERO_DATE.
        assert_eq!(
            datetime(Datum::new_string("0000-00-00 00:00:00".to_string())),
            "NULL",
            "a zero-date STRING is still rejected"
        );
    }

    /// The STRING path is unchanged: a wall-clock literal still parses as text.
    #[test]
    fn a_string_source_is_unchanged() {
        assert_eq!(
            datetime(Datum::new_string("2017-01-18 12:34:56".to_string())),
            "2017-01-18 12:34:56",
        );
    }

    #[test]
    fn temporal_target_fsp_rounds_at_boundaries() {
        let text = |s: &str| Datum::new_string(s.to_owned());
        assert_eq!(
            datetime_fsp(text("2020-02-03 11:22:33.987654"), 3),
            "2020-02-03 11:22:33.988"
        );
        assert_eq!(
            datetime_fsp(text("2020-01-01 23:59:59.5"), 0),
            "2020-01-02 00:00:00"
        );
        assert_eq!(
            datetime_fsp(text("2020-01-01 23:59:59.5"), 1),
            "2020-01-01 23:59:59.5"
        );
        let through_dispatch = eval_cast(
            &CastType::DateTime { fsp: Some(3) },
            text("2020-02-03 11:22:33.987654"),
            None,
            &NoColumns,
        )
        .expect("CAST dispatch");
        assert_eq!(render_time(&through_dispatch), "2020-02-03 11:22:33.988");
    }

    #[test]
    fn temporal_cast_preserves_value_and_zero_date_fields() {
        let got = cast_to_time(
            &Datum::new_string("0000-01-02 03:04:05".to_owned()),
            None,
            &NoColumns,
            tidb_datatype::TimeType::DateTime,
            0,
        )
        .expect("cast");
        assert!(matches!(got, Datum::Time(_)));
        assert_eq!(render_time(&got), "0000-01-02 03:04:05");

        let date = cast_to_time(
            &Datum::new_string("2020-01-01 10:30:00".to_owned()),
            None,
            &NoColumns,
            tidb_datatype::TimeType::Date,
            0,
        )
        .expect("cast");
        assert_eq!(render_time(&date), "2020-01-01");
    }

    #[test]
    fn duration_to_date_keeps_go_visible_date() {
        struct AtMidnight;
        impl crate::Columns for AtMidnight {
            fn get(&self, _: &[String]) -> Option<Datum> {
                None
            }
            fn now(&self) -> Option<(i64, u32, i32)> {
                Some((1_785_974_400, 0, 0))
            }
        }
        let duration = Datum::Duration(
            tidb_datatype::MySqlDuration::new(12, 34, 56, 789_000, 3).expect("duration"),
        );
        let date = cast_to_time(
            &duration,
            None,
            &AtMidnight,
            tidb_datatype::TimeType::Date,
            0,
        )
        .expect("cast");
        assert_eq!(render_time(&date), "2026-08-06");
    }

    #[test]
    fn cast_time_dispatches_every_supported_source_domain() {
        use std::cell::RefCell;

        struct Warnings(RefCell<Vec<(u16, String)>>);
        impl crate::Columns for Warnings {
            fn get(&self, _: &[String]) -> Option<Datum> {
                None
            }

            fn append_warning(&self, code: u16, message: &str) {
                self.0.borrow_mut().push((code, message.to_owned()));
            }
        }

        let cast = CastType::Time { fsp: Some(3) };
        let int_type = FieldType::new(FieldTypeCode::LongLong);
        let decimal_type = FieldType::new(FieldTypeCode::NewDecimal);
        let string_type = FieldType::new(FieldTypeCode::VarString);
        let duration_type = FieldType::new(FieldTypeCode::Duration);
        let warnings = Warnings(RefCell::new(Vec::new()));

        for (value, source, expected) in [
            (Datum::Int(125_959), &int_type, "12:59:59.000"),
            (
                Datum::Decimal(Decimal::from_literal("125959")),
                &decimal_type,
                "12:59:59.000",
            ),
            (
                Datum::new_string("12:59:59".to_owned()),
                &string_type,
                "12:59:59.000",
            ),
            (
                Datum::new_duration(
                    tidb_datatype::MySqlDuration::new(12, 59, 59, 987_654, 6).expect("duration"),
                ),
                &duration_type,
                "12:59:59.988",
            ),
        ] {
            let got = eval_cast(&cast, value, Some(source), &warnings).expect("CAST AS TIME");
            assert_eq!(got.sql_string().expect("duration string"), expected);
        }

        // The Go string signature preserves the parser's best effort beside
        // a truncation warning, whereas the numeric signature returns NULL.
        let text = eval_cast(
            &CastType::Time { fsp: None },
            Datum::new_string("1x".to_owned()),
            Some(&string_type),
            &warnings,
        )
        .expect("string truncation is a warning");
        assert_eq!(text.sql_string().expect("duration string"), "00:00:01");
        let numeric = eval_cast(
            &CastType::Time { fsp: None },
            Datum::Int(126_060),
            Some(&int_type),
            &warnings,
        )
        .expect("numeric truncation is a warning");
        assert_eq!(numeric, Datum::Null);
        assert_eq!(warnings.0.borrow().len(), 2);

        // Go's JSON duration signature accepts only temporal/string JSON;
        // scalar numeric JSON is a NULL plus the same truncation warning.
        let json_type = FieldType::new(FieldTypeCode::Json);
        let json = eval_cast(
            &CastType::Time { fsp: None },
            Datum::new_json(tidb_datatype::BinaryJSON::parse("123").expect("json")),
            Some(&json_type),
            &warnings,
        )
        .expect("JSON mismatch is a warning");
        assert_eq!(json, Datum::Null);
        assert_eq!(warnings.0.borrow().len(), 3);
    }
}

#[cfg(test)]
#[test]
fn real_unsigned_worker_keeps_rounding_overflow_and_union_boundary() {
    use std::cell::{Cell, RefCell};
    struct Warnings {
        level: crate::ErrorLevel,
        policy_reads: Cell<usize>,
        values: RefCell<Vec<(u16, String)>>,
    }
    impl crate::Columns for Warnings {
        fn get(&self, _: &[String]) -> Option<Datum> {
            panic!("materialized real cast needs no provider")
        }
        fn truncate_level(&self) -> crate::ErrorLevel {
            self.policy_reads.set(self.policy_reads.get() + 1);
            self.level
        }
        fn append_warning(&self, code: u16, message: &str) {
            self.values.borrow_mut().push((code, message.to_owned()));
        }
        fn time_zone(&self) -> tidb_datatype::SessionTimeZone {
            panic!("real unsigned cast does not read a timezone")
        }
        fn date_modes(&self) -> tidb_datatype::DateModes {
            panic!("real unsigned cast does not read date modes")
        }
    }
    fn owner(slots: usize) -> crate::AsciiPoolOwner {
        crate::AsciiPoolOwner::new(
            crate::AsciiPoolPolicy::checked(
                slots,
                slots,
                16 * 1024 * 1024,
                4 * 1024 * 1024,
                4 * 1024 * 1024,
                64,
                8,
                4 * 1024 * 1024,
            )
            .unwrap(),
        )
        .unwrap()
    }
    // Expected values come from the removed native rule: ties-even, then
    // negative signed-bit wrap or upper/nonfinite clamp. The last flag pins
    // the separate, unchanged UNION early clamp on the unrounded source.
    let cases = [
        (2.5, 2, None, false),
        (3.5, 4, None, false),
        (f64::from_bits(2.5_f64.to_bits() + 1), 3, None, false),
        (-0.0, 0, None, false),
        (-0.4, 0, None, true),
        (-0.5, 0, None, true),
        (-1.5, u64::MAX - 1, Some("-2"), true),
        (9223372036854775808.0, 1u64 << 63, None, false),
        (
            f64::from_bits(0x43efffffffffffff),
            u64::MAX - 2047,
            None,
            false,
        ),
        (
            18446744073709551616.0,
            u64::MAX,
            Some("1.8446744073709552e+19"),
            false,
        ),
        (-1e300, 1u64 << 63, Some("-1e+300"), true),
        (f64::INFINITY, u64::MAX, Some("+Inf"), false),
        (f64::NEG_INFINITY, 1u64 << 63, Some("-Inf"), true),
        (
            f64::from_bits(0xfff8000012345678),
            u64::MAX,
            Some("NaN"),
            false,
        ),
    ];
    let pool = owner(1);
    let execution = pool.begin_execution().unwrap();
    for level in [crate::ErrorLevel::Warn, crate::ErrorLevel::Error] {
        let ctx = Warnings {
            level,
            policy_reads: Cell::new(0),
            values: RefCell::new(Vec::new()),
        };
        for (input, expected, overflow_text, negative_union) in cases {
            for float32 in [false, true] {
                let source = FieldType::new(if float32 {
                    FieldTypeCode::Float
                } else {
                    FieldTypeCode::Double
                });
                for union in [false, true] {
                    let value = if float32 {
                        Datum::Float32(input)
                    } else {
                        Datum::Real(input)
                    };
                    let target = if union {
                        CastType::UnsignedInUnion
                    } else {
                        CastType::Unsigned
                    };
                    let result = execution.scope().with_columns(&ctx, |columns| {
                        eval_cast(&target, value, Some(&source), columns)
                    });
                    let bypass = union && negative_union;
                    assert_eq!(
                        result.unwrap(),
                        Datum::UInt(if bypass { 0 } else { expected })
                    );
                    let warnings = if bypass {
                        vec![]
                    } else {
                        overflow_text
                            .map(|text| vec![(1690, format!("constant {text} overflows bigint"))])
                            .unwrap_or_default()
                    };
                    assert_eq!(ctx.values.take(), warnings);
                    // 1690 is append-only even in Error mode, never HandleTruncate.
                    assert_eq!(ctx.policy_reads.get(), 0);
                }
            }
        }
    }
    let denied_pool = owner(0);
    let denied = denied_pool.begin_execution().unwrap();
    let ctx = Warnings {
        level: crate::ErrorLevel::Error,
        policy_reads: Cell::new(0),
        values: RefCell::new(Vec::new()),
    };
    for float32 in [false, true] {
        let source = FieldType::new(if float32 {
            FieldTypeCode::Float
        } else {
            FieldTypeCode::Double
        });
        for (input, union) in [(2.5, false), (2.5, true), (-1.5, false), (-1.5, true)] {
            let value = if float32 {
                Datum::Float32(input)
            } else {
                Datum::Real(input)
            };
            let target = if union {
                CastType::UnsignedInUnion
            } else {
                CastType::Unsigned
            };
            let result = denied.scope().with_columns(&ctx, |columns| {
                eval_cast(&target, value, Some(&source), columns)
            });
            if input < 0.0 && union {
                // Explicitly outside this migration: UNION's earlier branch
                // still returns zero without entering the real-unsigned worker.
                assert_eq!(result.unwrap(), Datum::UInt(0));
            } else {
                assert!(
                    matches!(result, Err(EvalError::ExpressionAdapterFailure(failure))
                    if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource
                        && failure.origin() == crate::ExpressionAdapterFailureOrigin::Pool)
                );
            }
            assert!(ctx.values.take().is_empty());
            assert_eq!(ctx.policy_reads.get(), 0);
        }
    }
    assert_eq!(
        eval_cast(&CastType::Unsigned, Datum::Real(2.5), None, &ctx).unwrap(),
        Datum::UInt(2)
    );
    assert!(ctx.values.take().is_empty());
    assert_eq!(ctx.policy_reads.get(), 0);
}
