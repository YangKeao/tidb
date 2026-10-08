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

//! Calendar arithmetic shared by the source-owned time family and the
//! remaining generic date syntax in `crate::func`.

use crate::cast::to_i64_signed;
use crate::coerce::coerce_str;
use crate::{Columns, Datum, EvalError};
use tidb_datatype::{CoreTime, Time, TimeType};
use tidb_query_datatype::codec::mysql::Time as TikvTime;

/// The whole stored CoreTime supplied to a component date-part kernel.
/// For `YEAR`/`MONTH`/`DAYOFMONTH`/`QUARTER`, field access in Go is the whole of
/// the function body: `builtinYearSig`/`MonthSig`/`DayOfMonthSig`/`QuarterSig`
/// return `date.Year()` / `date.Month()` / `date.Day()` off the value
/// `EvalTime` handed them, with NO parsing and NO `InvalidZero` check -- so a
/// zero datetime yields the stored `0` (confirmed against real TiDB's
/// recorded `r/executor/executor.result` `TestZeroDateTimeCompatibility`:
/// `YEAR(v1)`/`MONTH(v1)`/`DAYOFMONTH(v1)`/`QUARTER(v1)` over a zero-datetime
/// column are all `0`).
///
/// Go can be that short because all four classes declare their argument
/// `types.ETDatetime` (`builtin_time.go:1116`, `:1284`, `:1620`, `:5833`),
/// which makes `newBaseBuiltinFuncWithTp` wrap it in `WrapWithCastAsTime`
/// before the signature ever runs. [`crate::arg_eval_type`] is that wrap, so
/// this function is now equally short: a temporal value or `NULL` is ALL that
/// can arrive, and a string is decided by the cast -- one rule, in one place,
/// instead of one per call site.
pub(crate) fn component_time_core(vals: &[Datum]) -> Result<Option<CoreTime>, EvalError> {
    let [value] = vals else {
        return Err(EvalError::Unsupported("bad function arity"));
    };
    match value {
        Datum::Time(time) => Ok(Some(time.core_time())),
        Datum::Null => Ok(None),
        // Unreachable through either evaluator: both apply the ETDatetime
        // argument cast first. Refusing loudly keeps a future caller that
        // forgets it from silently re-deriving a date of its own.
        _ => Err(EvalError::Unsupported(
            "a date-part argument reached the signature without its ETDatetime cast",
        )),
    }
}

/// Sends the complete, unvalidated stored core through the closed worker.
/// ETDatetime conversion belongs to the existing caller, not this adapter.
pub(crate) fn component_field_in(
    vals: &[Datum],
    operation: crate::tikv::EvaluatedBytesOp,
    ctx: &dyn Columns,
) -> Result<Datum, EvalError> {
    crate::tikv::evaluate_args_in(
        operation,
        ctx,
        || {
            Ok(crate::tikv::EvaluatedArgs::TimeCoreBits(
                component_time_core(vals)?.map(CoreTime::raw),
            ))
        },
        crate::tikv::EvaluatedBytesResult::into_int_datum,
    )
}

pub(crate) fn year_in(vals: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    component_field_in(vals, crate::tikv::EvaluatedBytesOp::YearCoreNative, ctx)
}

/// Test compatibility only: the original tuple uses the shared CoreTime
/// getters. Production date-part evaluation always enters the closed worker.
#[cfg(test)]
pub(crate) fn component_date(vals: &[Datum]) -> Result<Option<(i64, u32, u32)>, EvalError> {
    Ok(component_time_core(vals)?.map(|core| {
        (
            i64::from(core.year()),
            u32::from(core.month()),
            u32::from(core.day()),
        )
    }))
}

/// Preserves the old test-vector callback, not a production generic fallback.
#[cfg(test)]
pub(crate) fn date_part(
    vals: &[Datum],
    f: impl Fn((i64, u32, u32)) -> i64,
) -> Result<Datum, EvalError> {
    Ok(component_date(vals)?.map_or(Datum::Null, |ymd| Datum::Int(f(ymd))))
}

/// Parses a date or datetime string's calendar date into `(year, month,
/// day)`, calendar-validated (month 1-12, day valid for that specific
/// month/year, including leap years) — `None` if it doesn't look like one.
/// Mirrors MySQL's lenient separator handling (any run of non-digit
/// characters between the numeric components, confirmed via `goeval` to
/// accept `-`, `/`, and `.`) and leading/trailing whitespace tolerance. A
/// trailing time-of-day component (space-separated) is accepted but
/// ignored here — this function is deliberately scoped to the DATE part
/// only; see [`TikvTime::parse_native_hms`] for `HOUR`/`MINUTE`/`SECOND`
/// extraction, which needs a GENUINELY different algorithm (real TiDB's
/// behavior on a string with no time component is non-obvious — e.g.
/// `MINUTE('2021-01-01')` is NOT `0` — so it does not simply call this
/// function and default a missing time to midnight).
///
/// A bare, separator-less digit run of EXACTLY 6 or 8 digits (e.g.
/// `20240315` — including from an integer literal argument like
/// `YEAR(20240315)`, `Datum::Int` already coerces to this same decimal
/// string form) is a SEPARATE, positional `YYMMDD`/`YYYYMMDD` reading —
/// confirmed via `goeval`, not assumed, and NOT limited to `HOUR`/
/// `MINUTE`/`SECOND`'s own colon-less path, which this function does not
/// share (that path decodes an ELAPSED-time magnitude via modulo
/// arithmetic; this one slices fixed-width calendar fields). See
/// [`expand_year`] for the 2-digit-year century pivot this shares with
/// the separator-based path below.
pub(crate) fn parse_date_ymd(s: &str) -> Option<(i64, u32, u32)> {
    TikvTime::parse_native_date_ymd(s)
}

/// Preserves the original string coercion, including Duration Display/FSP,
/// without adding an ETDuration cast. Arity and UTF-8 errors precede admission;
/// parsing and clamping run in the worker, so zero-slot refusal now precedes
/// a malformed-text NULL result. SQL NULL also executes the worker.
fn hms_text_in(
    vals: &[Datum],
    operation: crate::tikv::EvaluatedBytesOp,
    ctx: &dyn Columns,
) -> Result<Datum, EvalError> {
    crate::tikv::evaluate_bytes_in(
        operation,
        ctx,
        || {
            let [value] = vals else {
                return Err(EvalError::Unsupported("bad function arity"));
            };
            Ok(coerce_str(value)?.map(String::into_bytes))
        },
        crate::tikv::EvaluatedBytesResult::into_int_datum,
    )
}

pub(crate) fn hour_in(vals: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    hms_text_in(vals, crate::tikv::EvaluatedBytesOp::HourTextNative, ctx)
}

pub(crate) fn minute_in(vals: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    hms_text_in(vals, crate::tikv::EvaluatedBytesOp::MinuteTextNative, ctx)
}

pub(crate) fn second_in(vals: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    hms_text_in(vals, crate::tikv::EvaluatedBytesOp::SecondTextNative, ctx)
}

/// Projects already-evaluated legacy nanoseconds, not native SQL text.
/// No field is precomputed and NULL is still a demanded worker input.
pub(crate) fn hms_nanos_in(
    field: crate::LegacyHmsField,
    nanos: Option<i64>,
    ctx: &dyn Columns,
) -> Result<Option<i64>, EvalError> {
    use crate::tikv::EvaluatedBytesOp;

    let operation = match field {
        crate::LegacyHmsField::Hour => EvaluatedBytesOp::HourNanosNative,
        crate::LegacyHmsField::Minute => EvaluatedBytesOp::MinuteNanosNative,
        crate::LegacyHmsField::Second => EvaluatedBytesOp::SecondNanosNative,
    };
    crate::tikv::evaluate_args_in(
        operation,
        ctx,
        || Ok(crate::tikv::EvaluatedArgs::Int(nanos)),
        |computed| match computed.into_int_datum()? {
            Datum::Null => Ok(None),
            Datum::Int(value) => Ok(Some(value)),
            _ => Err(EvalError::Unsupported("legacy HMS result kind mismatch")),
        },
    )
}

/// Parses `HOUR`/`MINUTE`/`SECOND`'s single argument into `(hour, minute,
/// second)`, following real TiDB's own two-path algorithm (confirmed via
/// `goeval`, not assumed) depending on whether the value contains a `:`:
///
/// - WITH a `:`: an optional `[DATE ]` prefix (validated the SAME way
///   [`parse_date_ymd`] validates a DATE, split on the FIRST whitespace —
///   `'junk 10:30:45'`'s `junk` prefix makes the WHOLE value invalid, not
///   just ignored) followed by a REQUIRED `H:M[:S]` time-of-day (`S`
///   defaults to `0` if omitted). `H` may be MULTI-DIGIT and exceed 23 —
///   TiDB's `TIME` domain is an ELAPSED-time range, not a wall-clock
///   hour, confirmed up to its real documented maximum `838:59:59` (a
///   larger `H`, even with `M`/`S` individually valid, clamps the WHOLE
///   value to exactly `838:59:59` — not just the hour component,
///   confirmed via `goeval`: `HOUR('900:30:15')` is `838` but
///   `MINUTE('900:30:15')` is `59`, not `30`). `M`/`S` must each be
///   `0..=59` or the WHOLE value is invalid (`NULL`), regardless of `H`'s
///   own magnitude.
/// - WITHOUT a `:` (including a bare `DATE`-only value — confirmed via
///   `goeval` this is NOT `(0, 0, 0)`, a genuinely surprising real TiDB
///   behavior, not a theoretical corner case this executor invented):
///   the value's OWN leading contiguous run of ASCII digits (after
///   trimming whitespace and an optional leading `-`, sign otherwise
///   irrelevant since `HOUR`/`MINUTE`/`SECOND` always return a
///   non-negative magnitude) is parsed as a plain integer `N` and
///   reinterpreted as a right-aligned `HHMMSS`-style number: `SECOND = N
///   % 100`, `MINUTE = (N / 100) % 100`, `HOUR = N / 10000` — the SAME
///   rule an integer-literal argument like `HOUR(103045)` already uses,
///   applied UNIFORMLY regardless of how many digits `N` has (so
///   `HOUR('2024-01-15')` takes ONLY the leading `'2024'` — stopping at
///   the first non-digit `-` — decoding to `HOUR=0, MINUTE=20,
///   SECOND=24`, NOT the calendar date's own values at all). The SAME
///   `0..=59`-for-`M`/`S`-or-invalid and clamp-to-`838:59:59` rules apply
///   identically to the decoded `N`.
#[cfg(test)]
fn parse_hms_extended(s: &str) -> Option<(u32, u32, u32)> {
    TikvTime::parse_native_hms(s)
}

/// Splits a string into the `(value, digit count)` of its maximal runs of
/// ASCII digits, treating every other character as a separator — `None`
/// if any component is empty (a leading, trailing, or doubled separator)
/// or if there are not exactly 3 components. The digit count (not just
/// the parsed value) is preserved for [`expand_year`]'s own century-pivot
/// rule, which depends on how many digits the year was actually WRITTEN
/// with, not on its numeric value alone.
fn split_numeric_components(input: &str) -> Option<Vec<(u32, usize)>> {
    TikvTime::native_split_date_components(input)
}

/// Splits numeric date components for the `TIMEDIFF` parser.
pub(crate) fn split_numeric_components_for_time_diff(input: &str) -> Option<Vec<(u32, usize)>> {
    split_numeric_components(input)
}

/// Expands a calendar-date component's own YEAR value per real MySQL/
/// TiDB's century-pivot rule, confirmed via `goeval` to depend on the
/// value's ORIGINAL WRITTEN digit count, not its numeric magnitude: a
/// 1- or 2-digit year (`'1-03-15'` and `'01-03-15'` are indistinguishable
/// once parsed to a plain integer, and both pivot identically) is
/// EXPANDED — `0..=69` becomes `2000..=2069`, `70..=99` becomes
/// `1970..=1999` — while a 3-or-more-digit year is taken LITERALLY, even
/// when its own value happens to be under 100 (`'099-03-15'` is year
/// `99`, NOT pivoted to `1999`/`2099` — confirmed via `goeval`, a real
/// asymmetry from the 2-digit case that could not be guessed from the
/// value alone).
fn expand_year(value: u32, digits: usize) -> i64 {
    TikvTime::native_expand_date_year(value, digits)
}

/// Expands a two-digit year using TiDB's date parsing window.
pub(crate) fn expand_year_for_time_diff(value: u32, digits: usize) -> i64 {
    expand_year(value, digits)
}

/// Computes TiDB's `calcDaynr` value for `TIMEDIFF` datetime arithmetic.
/// Unlike normal calendar parsing, the source permits zero month/day
/// components when `IgnoreZeroInDate` is enabled; mirroring `calcDaynr`
/// preserves those values instead of forcing them through Gregorian
/// month normalization.
pub(crate) fn time_diff_daynr(year: i64, month: u32, day: u32) -> i64 {
    TikvTime::native_time_diff_daynr(year, month, day)
}

fn is_leap_year(year: i64) -> bool {
    TikvTime::native_is_leap_year(year)
}

/// The number of days in `month` of `year` (Gregorian, leap-year aware);
/// `0` for an out-of-range month so a range check against it always fails.
fn days_in_month(year: i64, month: u32) -> u32 {
    TikvTime::native_days_in_month(year, month)
}

/// Returns the Gregorian month length for the `TIMEDIFF` parser.
pub(crate) fn days_in_month_for_time_diff(year: i64, month: u32) -> u32 {
    days_in_month(year, month)
}

/// TiDB's `types.CoreTime.Week` / `YearWeek` calculation, ported from
/// `pkg/types/core_time.go:calcDaynr`, `weekMode`, and `calcWeek`.
/// `mode` is masked to its low three bits exactly as the Go implementation
/// does.  `with_year` selects `YearWeek`'s always-year-numbered variant.
#[cfg(test)]
pub(crate) fn week_of_year(y: i64, m: u32, d: u32, mode: i64, with_year: bool) -> (i64, i64) {
    TikvTime::native_week_of_year(y, m, d, mode, with_year)
}

/// Gregorian days since 1970-01-01, delegated to the shared wide civil helper.
/// The fixed epoch is also used by weekday offsets, inverse conversion and
/// timestamp consumers; this is not MySQL's internal day-number domain.
pub(crate) fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    TikvTime::native_days_from_civil(y, m, d)
}

/// The inverse of [`days_from_civil`]: the Gregorian calendar date for a day
/// count `z` since the same 1970-01-01 epoch — Howard Hinnant's
/// `civil_from_days` algorithm, from the same public source. Used by
/// [`from_days_in`] (which converts through [`days_from_civil`]'s own epoch —
/// see `TO_DAYS`'s `719_528` offset, so the exact epoch choice is internal
/// and doesn't need to match MySQL's) and, unlike `from_days_in`, directly on
/// its OWN public epoch by `crate::time_fn`'s `NOW()`/`CURRENT_TIMESTAMP()`
/// (a true Unix timestamp's day count IS already `z` in this function's own
/// terms, since both anchor to 1970-01-01).
pub(crate) fn civil_from_days(z: i64) -> (i64, u32, u32) {
    TikvTime::native_civil_from_days(z)
}

/// `FROM_DAYS`: the inverse of `TO_DAYS` — an absolute day number back to a
/// `YYYY-MM-DD` date string. The source signature is `ETInt`, so strings use
/// TiDB's integer-prefix coercion (`"z550z"` becomes zero and `"6500z"`
/// becomes 6500), while decimal/float inputs round through the shared
/// `to_i64_signed` path. Outside the valid range (`366` to `3_652_424`,
/// i.e. year `0001` through `9999`), values normally return the literal
/// string `"0000-00-00"` (MySQL's "zero date"). Real TiDB also has a narrow,
/// clearly-anomalous `NULL` sub-band immediately ABOVE the valid range
/// (`3_652_425` to `3_652_499`), which is source-visible in `TestFromDays`
/// and therefore retained here before the zero-date fallback resumes beyond
/// it.
pub(crate) fn from_days_in(vals: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    crate::tikv::evaluate_args_in(
        crate::tikv::EvaluatedBytesOp::FromDaysNative,
        ctx,
        || {
            if vals.len() != 1 {
                return Err(EvalError::Unsupported("bad function arity"));
            }
            let value = match &vals[0] {
                Datum::Null => None,
                value => Some(to_i64_signed(value)),
            };
            Ok(crate::tikv::EvaluatedArgs::Int(value))
        },
        |computed| {
            Ok(match computed.into_bytes()? {
                None => Datum::Null,
                Some(bytes) if bytes.as_slice() == b"0000-00-00" => {
                    // Pack the provider's actual zero-date value as the original
                    // typed Time, so the outer cast does not parse it into NULL.
                    let zero = Time::new(CoreTime::default(), TimeType::Date, 0)
                        .expect("the zero date is a valid Time");
                    Datum::new_time(zero)
                }
                Some(bytes) => Datum::new_string(bytes),
            })
        },
    )
}

#[cfg(test)]
pub(crate) fn from_days(vals: &[Datum]) -> Result<Datum, EvalError> {
    from_days_in(vals, &crate::NoColumns)
}

/// `DATEDIFF` retains both original text conversions in left-to-right order,
/// even if the first produces NULL. Date parsing (which ignores time suffixes)
/// and subtraction run in the worker; original casts and warnings stay upstream.
pub(crate) fn date_diff_in(vals: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    crate::tikv::evaluate_args_in(
        crate::tikv::EvaluatedBytesOp::DateDiffTextNative,
        ctx,
        || {
            if vals.len() != 2 {
                return Err(EvalError::Unsupported("bad function arity"));
            }
            let (left, right) = (coerce_str(&vals[0])?, coerce_str(&vals[1])?);
            Ok(crate::tikv::EvaluatedArgs::Bytes2(
                left.map(String::into_bytes),
                right.map(String::into_bytes),
            ))
        },
        crate::tikv::EvaluatedBytesResult::into_int_datum,
    )
}

/// Legacy CoreTime inputs are not SQL text and must not acquire its date
/// validation. Preserve both actual nullable raw cores, including clock bits.
pub(crate) fn date_diff_core_in(
    left: Option<CoreTime>,
    right: Option<CoreTime>,
    ctx: &dyn Columns,
) -> Result<Option<i64>, EvalError> {
    crate::tikv::evaluate_args_in(
        crate::tikv::EvaluatedBytesOp::DateDiffCoreNative,
        ctx,
        || {
            Ok(crate::tikv::EvaluatedArgs::TimeCoreBits2(
                left.map(CoreTime::raw),
                right.map(CoreTime::raw),
            ))
        },
        |computed| match computed.into_int_datum()? {
            Datum::Null => Ok(None),
            Datum::Int(value) => Ok(Some(value)),
            _ => Err(EvalError::Unsupported(
                "legacy DATEDIFF result kind mismatch",
            )),
        },
    )
}

#[cfg(test)]
pub(crate) fn date_diff(vals: &[Datum]) -> Result<Datum, EvalError> {
    date_diff_in(vals, &crate::NoColumns)
}

/// `TIMESTAMPDIFF(unit, datetime_expr1, datetime_expr2)`, ported from
/// `builtinTimestampDiffSig.evalInt` and `types.TimestampDiff`.  The Rust
/// value boundary accepts scalar DATE/DATETIME strings and returns the exact
/// integer result; typed temporal conversion, warning state, and SQL-mode
/// handling remain at the caller boundary.
pub(crate) fn timestamp_diff_in(vals: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    crate::tikv::eval_timestamp_diff_in(ctx, vals)
}

#[cfg(test)]
pub(crate) fn timestamp_diff(vals: &[Datum]) -> Result<Datum, EvalError> {
    timestamp_diff_in(vals, &crate::NoColumns)
}

/// `TO_DAYS(date)`, implemented through the same zero-date day number used by
/// `types.TimestampDiff("DAY", types.ZeroDate, date)`.  This preserves the
/// source's year-zero `0000-01-01 -> 1` behavior while rejecting invalid
/// zero-date components and malformed time suffixes.
pub(crate) fn to_days_in(vals: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    crate::tikv::evaluate_bytes_in(
        crate::tikv::EvaluatedBytesOp::ToDaysTextNative,
        ctx,
        || super::single_temporal_text(vals),
        crate::tikv::EvaluatedBytesResult::into_int_datum,
    )
}

#[cfg(test)]
pub(crate) fn to_days(vals: &[Datum]) -> Result<Datum, EvalError> {
    to_days_in(vals, &crate::NoColumns)
}

/// `TO_SECONDS(date)`, implemented through the same zero-date timestamp
/// arithmetic as the Go builtin.  Fractional seconds are deliberately
/// ignored because the source's `SECOND` unit returns whole seconds.
pub(crate) fn to_seconds_in(vals: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    crate::tikv::evaluate_bytes_in(
        crate::tikv::EvaluatedBytesOp::ToSecondsTextNative,
        ctx,
        || super::single_temporal_text(vals),
        crate::tikv::EvaluatedBytesResult::into_int_datum,
    )
}

#[cfg(test)]
pub(crate) fn to_seconds(vals: &[Datum]) -> Result<Datum, EvalError> {
    to_seconds_in(vals, &crate::NoColumns)
}

/// `DATE_ADD`/`DATE_SUB` calendar arithmetic from current Go
/// `baseDateArithmetical` and `types.ParseDurationValue`.
///
/// Day/week units use exact civil-day arithmetic; month/quarter/year use
/// calendar-field arithmetic with a single final-month day clamp. Clock and
/// composite units use one microsecond timeline so carries and fractional
/// seconds follow the same path. Composite numeric groups are right-aligned
/// to their unit fields, and all computed years are limited to TiDB's
/// representable range.
pub(crate) fn date_add(
    unit: &str,
    date: &Datum,
    amount: &Datum,
    sign: i64,
) -> Result<Datum, EvalError> {
    date_add_with_result_fsp(unit, date, amount, sign, None, &crate::NoColumns)
}

/// Go determines a temporal `DATE_ADD` result's FSP from the argument
/// `FieldType`s while building the function. A string/numeric date result is
/// different: its string signature prints no fraction for a whole-second
/// answer and six digits otherwise, so it returns `None` here and lets the
/// value choose that final rendering.
pub(crate) fn date_add_result_fsp(
    unit: &str,
    date_type: Option<&tidb_datatype::FieldType>,
    amount_type: Option<&tidb_datatype::FieldType>,
) -> Option<u32> {
    use tidb_datatype::EvalType;
    use tidb_query_expr::{NativeDateArithmeticEvalType as Shared, NativeDateArithmeticFieldType};
    let metadata = |field: &tidb_datatype::FieldType| NativeDateArithmeticFieldType {
        eval_type: match field.eval_type() {
            EvalType::Int => Shared::Int,
            EvalType::Real => Shared::Real,
            EvalType::Decimal => Shared::Decimal,
            EvalType::String => Shared::String,
            EvalType::Datetime => Shared::Datetime,
            EvalType::Timestamp => Shared::Timestamp,
            EvalType::Duration => Shared::Duration,
            EvalType::Json => Shared::Json,
            EvalType::VectorFloat32 => Shared::VectorFloat32,
        },
        decimal: field.decimal(),
    };
    tidb_query_expr::native_date_arithmetic_result_fsp(
        unit,
        date_type.map(metadata),
        amount_type.map(metadata),
    )
}

pub(crate) fn date_add_with_result_fsp(
    unit: &str,
    date: &Datum,
    amount: &Datum,
    sign: i64,
    result_fsp: Option<u32>,
    ctx: &dyn Columns,
) -> Result<Datum, EvalError> {
    crate::tikv::eval_date_add_in(ctx, unit, date, amount, sign, result_fsp)
}

/// Preserve the old formatter test entry with the actual visible decimal text.
#[cfg(test)]
pub(super) fn format_decimal_composite_interval(unit: &str, decimal: &crate::Decimal) -> String {
    tidb_query_expr::native_format_decimal_composite_interval(unit, &decimal.to_string())
}

/// `EXTRACT(<composite unit> FROM value)`, ported from
/// `ExtractDatetimeNum`/`ExtractDurationNum` (`pkg/types/time.go`).  A
/// value that parses as a DATE/DATETIME uses `ExtractDatetimeNum`'s
/// formulas (the `DAY_*` variants include the actual day-of-month); a
/// value that is a bare, colon-separated TIME/duration literal (no `-`
/// date separators, confirmed via `pkg/executor` capture with
/// `'-01:02:03'`) uses `ExtractDurationNum`'s formulas instead, which drop
/// the day component entirely and apply the duration's own sign to the
/// WHOLE composite result rather than per-field. Fractional seconds on string
/// inputs are retained as six-digit microseconds, matching the source's
/// `ExtractDatetimeNum`/`ExtractDurationNum` behavior.
pub(crate) fn extract_composite(
    unit: &str,
    vals: &[Datum],
    cols: &dyn Columns,
) -> Result<Datum, EvalError> {
    crate::tikv::eval_extract_composite_in(cols, unit, vals)
}

/// Parses a `HH:MM:SS` time-of-day string into `(hour, minute, second)`,
/// each range-validated (`0..=23`/`0..=59`/`0..=59`) — `None` if malformed
/// or out of range. Strict about the `:` separator, unlike
/// [`parse_date_ymd`]'s lenient date-separator handling: every date/
/// datetime value this crate itself ever produces is `HH:MM:SS` exactly,
/// and every corpus/test input uses the same well-formed shape, so
/// leniency here has not been demonstrated as necessary the way the DATE
/// separator's was (confirmed via `goeval` to matter for real inputs).
pub(crate) fn parse_time_hms(s: &str) -> Option<(u32, u32, u32)> {
    TikvTime::parse_native_clock_hms(s)
}

/// `DATE_FORMAT`'s time-of-day parser extends [`parse_time_hms`] with the
/// optional fractional seconds which `%f` renders. The Go source delegates
/// this to `types.Time.DateFormat` (`pkg/types/time.go`); retaining the
/// written fraction here is enough for the evaluator's string-only domain.
pub(crate) fn parse_time_with_fraction(s: &str) -> Option<(u32, u32, u32, String)> {
    TikvTime::parse_native_clock_with_fraction(s)
}

/// `STR_TO_DATE(date, format)`, ported from `types.Time.StrToDate` and the
/// `builtinStrToDate*Sig` family in `pkg/expression/builtin_time.go`.
///
/// The surrounding evaluator has no typed `Time`/`Duration` datum yet, so a
/// successful parse is rendered as the canonical string representation that
/// the source's typed value exposes (`YYYY-MM-DD`, `YYYY-MM-DD HH:MM:SS`, or
/// `HH:MM:SS`).  The parser intentionally keeps the source's useful scalar
/// grammar: numeric date/time directives, `%r`/`%T`, fractional seconds,
/// case-insensitive AM/PM, and `%@`/`%#`/`%.` skip directives.
///
/// # The zero-component rule is the SQL mode's, not the parser's
///
/// A fully-parsed date with a zero YEAR, MONTH or DAY -- what a PARTIAL
/// format like `'%m'` alone produces -- is a VALUE in Go, not a parse
/// failure. `types.Time.StrToDate` ends at `t.Check(typeCtx)`, whose
/// `checkDateType` returns `nil` for an all-zero date outright and skips
/// the zero-month/zero-day rejection entirely when `allowZeroInDate` --
/// which `ResetContextOfStmt`'s `*ast.SelectStmt` arm sets UNCONDITIONALLY
/// (`WithIgnoreZeroInDate(true)`), in every SQL mode. The rejection that
/// does fire lives one level up, in the SIGNATURE, and reads a DIFFERENT
/// mode bit:
///
/// ```text
/// if sqlMode(ctx).HasNoZeroDateMode() && (t.Year() == 0 || t.Month() == 0 || t.Day() == 0) {
/// ```
///
/// (`builtinStrToDateDateSig.evalTime` and `builtinStrToDateDatetimeSig`,
/// `pkg/expression/builtin_time.go`.) So `NO_ZERO_DATE` -- NOT
/// `NO_ZERO_IN_DATE` -- decides, and it decides for the DATE and DATETIME
/// signatures ONLY: `builtinStrToDateDurationSig.evalDuration` carries no
/// such check at all (its source comment marks the omission as a TODO), so
/// a time-only format keeps its value in every mode. `expression/issues`
/// records both halves: under the default mode `str_to_date(1, '%m')` is
/// NULL, and with `NO_ZERO_DATE` dropped from `sql_mode` the SAME call is
/// `0000-01-00`, while `str_to_date(substr(dest,1,6),'%H%i%s')` is
/// `20:23:10` under a mode that has `NO_ZERO_DATE` set.
///
/// Rejecting a zero component unconditionally here was therefore wrong in
/// BOTH directions -- it answered NULL for every value TiDB returns under a
/// relaxed mode, and answered `0000-05-01` for `str_to_date('01,5','%d,%m')`
/// where the default mode's TiDB answers NULL.
pub(crate) fn str_to_date(vals: &[Datum], cols: &dyn crate::Columns) -> Result<Datum, EvalError> {
    crate::tikv::eval_str_to_date_in(cols, vals, None)
}

pub(crate) fn str_to_date_typed(
    vals: &[Datum],
    cols: &dyn crate::Columns,
    target: Option<tidb_datatype::FieldTypeCode>,
) -> Result<Datum, EvalError> {
    crate::tikv::eval_str_to_date_in(cols, vals, target)
}

/// `DATE_FORMAT(date, fmt)`: renders a date/datetime string per a MySQL
/// format string. The date argument is parsed as `Y-M-D[ H:M:S]` (a missing
/// time is midnight); `NULL` if either argument is `NULL` or the date
/// doesn't parse. Supported specifiers cover the common set (verified via
/// `gorun`): `%Y`/`%y` year, `%m`/`%c` month, `%d`/`%e` day, `%H`/`%k`/
/// `%h`/`%I`/`%l` hour, `%i` minute, `%S`/`%s` second, `%p` AM/PM, `%T`/`%r`
/// time, `%W`/`%a`/`%w` weekday renderings, `%M`/`%b` month name, `%j` day-of-year,
/// `%D` day-with-ordinal-suffix, and `%%` a literal `%`. An unknown `%X`
/// emits `X` verbatim (matching MySQL).
pub(crate) fn date_format_in(
    date: &Datum,
    fmt: &Datum,
    ctx: &dyn Columns,
) -> Result<Datum, EvalError> {
    crate::tikv::evaluate_args_in(
        crate::tikv::EvaluatedBytesOp::DateFormatTextNative,
        ctx,
        || {
            // Both original conversions run, even when the date is NULL.
            // Parsing and text-policy formatting belong entirely to the worker.
            let (date, fmt) = (coerce_str(date)?, coerce_str(fmt)?);
            Ok(crate::tikv::EvaluatedArgs::Bytes2(
                date.map(String::into_bytes),
                fmt.map(String::into_bytes),
            ))
        },
        |computed| {
            Ok(computed
                .into_bytes()?
                .map_or(Datum::Null, Datum::new_string))
        },
    )
}

#[cfg(test)]
pub(crate) fn date_format(date: &Datum, fmt: &Datum) -> Result<Datum, EvalError> {
    date_format_in(date, fmt, &crate::NoColumns)
}

#[cfg(test)]
mod clock_source_tests {
    use super::parse_hms_extended;

    /// Go `TestClock` gives HOUR, MINUTE and SECOND all three fractional
    /// source inputs. The fraction does not change those fields, but it must
    /// not make the ETDuration cast NULL; the double-dot row stays invalid.
    #[test]
    fn test_clock_time_parts() {
        for (input, expected) in [
            ("10:10:10.123456", (10, 10, 10)),
            ("11:11:11.11", (11, 11, 11)),
            ("2010-10-10 11:11:11.11", (11, 11, 11)),
        ] {
            assert_eq!(parse_hms_extended(input), Some(expected));
        }
        assert_eq!(parse_hms_extended("2011-11-11 10:10:10.11.12"), None);
    }
}

#[cfg(test)]
mod date_add_microsecond_tests {
    use super::{date_add, format_decimal_composite_interval};
    use tidb_datatype::{Datum, Decimal};

    /// Exact port of Go `TestGetIntervalFromDecimal` in
    /// `pkg/expression/builtin_time_test.go`. These are the intermediate
    /// strings produced before `ParseDurationValue`, not merely equivalent
    /// final dates that could conceal a misplaced decimal field.
    #[test]
    fn test_get_interval_from_decimal() {
        for (param, expected, unit) in [
            ("1.100", "1:100", "MINUTE_SECOND"),
            ("1.10000", "1-10000", "YEAR_MONTH"),
            ("1.10000", "1 10000", "DAY_HOUR"),
            ("11000", "0 00:00:11000", "DAY_MICROSECOND"),
            ("11000", "00:00:11000", "HOUR_MICROSECOND"),
            ("11.1000", "00:11:1000", "HOUR_SECOND"),
            ("1000", "00:1000", "MINUTE_MICROSECOND"),
        ] {
            let decimal = Decimal::from_literal(param);
            assert_eq!(
                format_decimal_composite_interval(unit, &decimal),
                expected,
                "{unit} {param}"
            );
        }
    }

    /// An `INTERVAL` amount too large for the arithmetic is `NULL`, not a
    /// panic. TiDB's own `expression/time` script asks for every one of these
    /// -- `select "1000-01-01 00:00:00" + INTERVAL 9223372036854775808 day`,
    /// the same with `YEAR`, `MINUTE`, `MICROSECOND`, and with
    /// `18446744073709551616` -- and
    /// `tests/integrationtest/r/expression/time.result` records `NULL` for
    /// all of them, because the computed date leaves `DATE`'s range. Before
    /// this the day/month/second arithmetic overflowed an `i64` and ABORTED
    /// the process, which is what made `expression/time` a crashing topic.
    ///
    /// A literal in the script is larger than `i64::MAX`, so the parse
    /// saturates to `i64::MAX` and the OVERFLOW is what has to be caught --
    /// the amount alone never looks out of range.
    #[test]
    fn an_interval_amount_too_large_to_compute_is_null_rather_than_an_overflow() {
        let date = Datum::new_string("1000-01-01 00:00:00");
        for unit in [
            "DAY",
            "WEEK",
            "MONTH",
            "YEAR",
            "QUARTER",
            "HOUR",
            "MINUTE",
            "SECOND",
            "MICROSECOND",
        ] {
            for sign in [1, -1] {
                for amount in [i64::MAX, i64::MIN] {
                    assert_eq!(
                        date_add(unit, &date, &Datum::Int(amount), sign),
                        Ok(Datum::Null),
                        "{unit} {amount} sign {sign}"
                    );
                }
            }
        }
        // The CONTROL: an ordinary amount still computes, so the guard above
        // did not simply turn the unit off.
        assert_eq!(
            date_add("DAY", &date, &Datum::Int(1), 1),
            Ok(Datum::new_string("1000-01-02 00:00:00"))
        );
        assert_eq!(
            date_add("YEAR", &date, &Datum::Int(1), 1),
            Ok(Datum::new_string("1001-01-01 00:00:00"))
        );
    }

    #[test]
    fn microsecond_preserves_six_digit_results() {
        let date = Datum::new_string("2026-07-25 10:00:00");
        let result = date_add("MICROSECOND", &date, &Datum::Int(1_500_000), 1).unwrap();
        assert_eq!(result, Datum::new_string("2026-07-25 10:00:01.500000"));

        let result = date_add("MICROSECOND", &date, &Datum::Int(499_999), 1).unwrap();
        assert_eq!(result, Datum::new_string("2026-07-25 10:00:00.499999"));

        let result = date_add("MICROSECOND", &date, &Datum::Int(1_500_000), -1).unwrap();
        assert_eq!(result, Datum::new_string("2026-07-25 09:59:58.500000"));

        let date = Datum::new_string("2026-07-25 10:00:00.250000");
        let result = date_add("MICROSECOND", &date, &Datum::Int(500_000), 1).unwrap();
        assert_eq!(result, Datum::new_string("2026-07-25 10:00:00.750000"));
    }
}

#[cfg(test)]
mod week_tests {
    use super::week_of_year;

    /// `week_of_year(y, m, d, mode, with_year) -> (week_year, week)` ports TiDB
    /// `core_time.go` calcWeek/weekMode, driving DATE_FORMAT's %U/%u/%V/%v/%X/%x.
    /// Vectors are authoritative goeval `DATE_FORMAT(d,'%U %u %V %v %X %x')` on
    /// boundary dates that stress week 0/52/53 and the week-year transition.
    #[test]
    fn week_of_year_matches_go_for_boundary_dates() {
        // y, m, d, %U, %u, %V, %v, %X, %x
        for &(y, m, d, uu, ul, vu, vl, xu, xl) in &[
            (
                2000i64, 1u32, 1u32, 0i64, 0i64, 52i64, 52i64, 1999i64, 1999i64,
            ),
            (2001, 1, 1, 0, 1, 53, 1, 2000, 2001),
            (1999, 12, 31, 52, 52, 52, 52, 1999, 1999),
            (2000, 12, 31, 53, 52, 53, 52, 2000, 2000),
            (2004, 1, 1, 0, 1, 52, 1, 2003, 2004),
            (2005, 1, 1, 0, 0, 52, 53, 2004, 2004),
            (2015, 12, 31, 52, 53, 52, 53, 2015, 2015),
            (2016, 1, 1, 0, 0, 52, 53, 2015, 2015),
        ] {
            assert_eq!(week_of_year(y, m, d, 0, false).1, uu, "%U {y}-{m}-{d}");
            assert_eq!(week_of_year(y, m, d, 1, false).1, ul, "%u {y}-{m}-{d}");
            assert_eq!(week_of_year(y, m, d, 2, false).1, vu, "%V {y}-{m}-{d}");
            assert_eq!(week_of_year(y, m, d, 3, false).1, vl, "%v {y}-{m}-{d}");
            assert_eq!(week_of_year(y, m, d, 2, true).0, xu, "%X {y}-{m}-{d}");
            assert_eq!(week_of_year(y, m, d, 3, true).0, xl, "%x {y}-{m}-{d}");
        }
    }
}

#[cfg(test)]
mod composite_extract_tests {
    use super::*;

    /// Go `ExtractDatetimeNum`/`ExtractDurationNum` per composite unit
    /// (`pkg/types/time.go`): the compound extraction concatenates the fields
    /// WITHOUT separators — the fractional-second compounds retain six-digit
    /// microseconds from string inputs, and a negative duration applies its sign
    /// to the WHOLE composite result rather than per-field.
    #[test]
    fn composite_extracts_concatenate_fields_like_go() {
        // DAY_MICROSECOND over a datetime: day 1 + 01:02:03.456700.
        let date = Datum::new_string("2023-03-14 01:02:03.4567".to_string());
        assert_eq!(
            extract_composite("DAY_MICROSECOND", &[date], &crate::context::NoColumns).unwrap(),
            Datum::Int(14_010_203_456_700)
        );
        // HOUR_MICROSECOND over a duration string.
        assert_eq!(
            extract_composite(
                "HOUR_MICROSECOND",
                &[Datum::new_string("01:02:03.4567".to_string())],
                &crate::context::NoColumns,
            )
            .unwrap(),
            Datum::Int(102_034_567_00)
        );
        // MINUTE_MICROSECOND over `02:03.4567`: Go's `ParseDurationValue`
        // distributes TWO groups onto (mi, sec), yielding 203456700
        // (`pkg/types/time.go`).
        // ALSO pinned: HOUR_MICROSECOND over the same two-group shape
        // (h=0 implied), and the seconds-only single-group form.
        assert_eq!(
            extract_composite(
                "MINUTE_MICROSECOND",
                &[Datum::new_string("02:03.4567".to_string())],
                &crate::context::NoColumns,
            )
            .unwrap(),
            Datum::Int(20_345_670_0)
        );
        // SECOND_MICROSECOND.
        assert_eq!(
            extract_composite(
                "SECOND_MICROSECOND",
                &[Datum::new_string("03.4567".to_string())],
                &crate::context::NoColumns,
            )
            .unwrap(),
            Datum::Int(3_456_700)
        );
        // The two-group shape also serves HOUR_MICROSECOND (h=0 implied).
        assert_eq!(
            extract_composite(
                "HOUR_MICROSECOND",
                &[Datum::new_string("02:03.4567".to_string())],
                &crate::context::NoColumns,
            )
            .unwrap(),
            Datum::Int(20_345_670_0)
        );
        // A negative duration applies its sign to the WHOLE composite result.
        let dur = Datum::new_string("-01:02:03.4567".to_string());
        assert_eq!(
            extract_composite("DAY_MICROSECOND", &[dur], &crate::context::NoColumns).unwrap(),
            // A Duration has no day field: DAY_MICROSECOND degenerates to the
            // h/mi/s/us composite, sign applied to the whole result.
            Datum::Int(-10_203_456_700)
        );
    }
}
