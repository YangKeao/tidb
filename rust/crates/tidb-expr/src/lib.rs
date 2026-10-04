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

//! A constant-expression evaluator over [`tidb_ast::Expr`] — the seed of the
//! design's `tidb-expr` crate and the first step from syntax into semantics.
//!
//! Scope: the integer/string/decimal/float/`NULL` domain of MySQL scalar
//! expressions — integer/boolean/string/decimal/float literals, `NULL`,
//! unary `+`/`-`/`~`/`NOT`/`!`, binary arithmetic (`+ - * / DIV MOD`),
//! bitwise (`& | ^ << >>`), comparison (`= <=> >= > <= < != <>`, with string
//! operands compared under `utf8mb4_bin` PAD SPACE and int/decimal/float
//! operands freely mixed), and logical (`AND OR XOR`) operators — with
//! MySQL's three-valued
//! logic — the `[NOT] IN (list)`, `[NOT] BETWEEN`, `IS [NOT] NULL/TRUE/FALSE`,
//! and `[NOT] LIKE` (case-sensitive `utf8mb4_bin`, `%`/`_` wildcards; a
//! non-string operand on EITHER side is implicitly stringified via
//! [`Datum::sql_string`], matching real MySQL's coercion — confirmed via
//! `gorun`, including that a `DECIMAL`'s declared scale is preserved, not
//! simplified) predicates, `CASE` (both the simple `CASE value WHEN cond THEN result
//! ... [ELSE result] END` form — `cond` compared via ordinary `=`, so a
//! `NULL` `value` never matches any `WHEN`, matching `=`'s own
//! propagation — and the searched `CASE WHEN cond THEN result ... [ELSE
//! result] END` form, `cond` truthiness-tested directly; the first
//! matching `WHEN` wins, evaluated LAZILY — only the taken branch's
//! expression ever runs, matching real MySQL's short-circuit CASE, a
//! load-bearing idiom for guarding against errors like division by zero;
//! real MySQL additionally infers CASE's overall result type from EVERY
//! branch statically, even ones never evaluated — confirmed via `goeval`:
//! `CASE WHEN 1=0 THEN 1/0 ELSE 5 END` is `DEC:5.0000`, not `INT:5`, even
//! though `1/0` is never evaluated — which cannot be replicated without a
//! genuine type-inference pass and is deliberately NOT attempted here;
//! the result is simply whichever branch was taken, in its own natural
//! type), plus builtin functions: numeric (`ABS`, `SIGN`, `LEAST`,
//! `GREATEST`, `COALESCE`, `IF`, `IFNULL`, `NULLIF`, `CEIL`/`CEILING`,
//! `FLOOR` — the last two return `Int` for an `Int`/`Decimal` argument
//! (`Decimal` computed EXACTLY, via [`Decimal::ceil_floor`], not
//! through `f64`) but `Float` for a `Float` one, confirmed via `goeval`,
//! not assumed; `ROUND`/`TRUNCATE` — a DIFFERENT type rule from
//! `CEIL`/`FLOOR`: `Decimal` NEVER collapses to `Int`, and rounds ties
//! away from zero via [`Decimal::round_to_scale`]/
//! [`Decimal::truncate_to_scale`], clamped to `DECIMAL`'s max
//! scale (30) for a positive scale argument, while `Float` rounds ties TO
//! EVEN via [`math_fn`]'s bit-for-bit port of Go's `types.Round`/
//! `types.Truncate` — including Go's own `math.Pow10` lookup table, which
//! is NOT the same as `f64::powi` for most exponents, confirmed by
//! diffing bit patterns, not assumed), transcendental ([`math_fn`]: `SQRT`,
//! `POW`/`POWER`, `EXP`, `LN`, `LOG`, `LOG2`, `LOG10`, `PI`, and the trigonometric
//! family — `SIN`, `COS`, `TAN`, `ASIN`, `ACOS`, `ATAN`/`ATAN2`, `COT`,
//! `RADIANS`, `DEGREES` — every one of these always returns `Float`), and
//! string (`CONCAT`, `LENGTH`, `CHAR_LENGTH`, `UPPER`, `LOWER`, `LEFT`,
//! `RIGHT`, `SUBSTRING`), all of which nest.
//!
//! Date-part extraction (`YEAR`, `MONTH`, `DAY`/`DAYOFMONTH`, `QUARTER`,
//! `DAYOFYEAR`, `DAYOFWEEK`, `WEEKDAY`, `TO_DAYS`, `TO_SECONDS`) and
//! `DATEDIFF` are also
//! covered: a `DATE`/`DATETIME` value has no dedicated value domain here, so
//! these parse a string argument's calendar components directly
//! (calendar-validated: month 1-12, day valid for that specific month/year
//! including leap years; lenient about separator characters and
//! zero-padding, matching MySQL's own leniency, confirmed via `goeval`).
//! `DATEDIFF` converts both dates to an absolute day number
//! ([`time_fn::calendar::days_from_civil`], a well-known algorithm) and subtracts,
//! ignoring any time-of-day component on either side; `DAYOFYEAR` is a
//! `days_from_civil` difference from that year's January 1st; `DAYOFWEEK`
//! (`1`=Sunday..`7`=Saturday) and `WEEKDAY` (`0`=Monday..`6`=Sunday) are
//! both `days_from_civil` read modulo 7 with a fixed offset; `TO_DAYS` and
//! `TO_SECONDS` use the source-compatible zero-date `calcDaynr` arithmetic
//! (including `TO_DAYS('0000-01-01') = 1`) and expose absolute day/second
//! numbers rather than differences. They reject malformed time suffixes and
//! zero-date components at the value boundary.
//! `FROM_DAYS` is `TO_DAYS`'s inverse ([`time_fn::calendar::civil_from_days`], the
//! complementary half of the same well-known algorithm as
//! `days_from_civil`), producing a `YYYY-MM-DD` string (still no dedicated
//! `DATE` value domain — a `goeval` `STR:` label was reused rather than
//! adding a new `DATE:` one, for direct comparability with how every other
//! date-shaped value in this crate is already represented); it returns
//! MySQL's "zero date" string outside the valid year `0001`-`9999` range,
//! except for a narrow, clearly-anomalous real-TiDB `NULL` sub-band just
//! above that range, deliberately not reproduced (documented on
//! [`time_fn::calendar::from_days`] itself).
//!
//! `DATE_ADD`/`DATE_SUB(date, INTERVAL amount unit)` are also covered, for
//! `DAY`, `WEEK`, `MONTH`, `QUARTER`, `YEAR`, `HOUR`, `MINUTE`, `SECOND`, and every
//! COMPOSITE unit (`YEAR_MONTH`, `DAY_HOUR`, `DAY_MINUTE`, `DAY_SECOND`,
//! `HOUR_MINUTE`, `HOUR_SECOND`, `MINUTE_SECOND`, and their
//! `*_MICROSECOND` variants — see [`time_fn::calendar::date_add`]'s own doc
//! for the composite split rules, ported from `parseTimeValue`
//! (`pkg/types/time.go`)).
//! `DAY` is exact day arithmetic via the same
//! `days_from_civil`/`civil_from_days` round-trip `TO_DAYS`/`FROM_DAYS`
//! use, so month/year rollover and leap days are handled correctly for
//! free (`2021-01-31 + 1 DAY` = `2021-02-01`, `2020-02-28 + 1 DAY` =
//! `2020-02-29`); `WEEK` is `DAY` with the (already-rounded) amount
//! pre-multiplied by 7. `MONTH`/`YEAR` are a genuinely DIFFERENT
//! algorithm — calendar-FIELD arithmetic ([`time_fn::calendar::add_months`]): the
//! year/month roll over via total-months arithmetic, and the day CLAMPS to
//! the target month's own length rather than overflowing into the next
//! month (`2021-01-31 + 1 MONTH` = `2021-02-28`, not `2021-03-03`), with
//! the clamp computed once against the FINAL target month, not iteratively
//! re-clamped one month at a time (`2021-01-31 + 2 MONTH` = `2021-03-31`,
//! the full 31 days, not `2021-03-28` from clamping through February
//! first — confirmed via `goeval`, not assumed); `YEAR` reuses the same
//! function with the amount pre-multiplied by 12. `DAY`/`WEEK`/`MONTH`/
//! `YEAR` all preserve an existing time-of-day suffix on the input
//! verbatim (or omit it if absent) — none of them touch it.
//!
//! `HOUR`/`MINUTE`/`SECOND` are a THIRD algorithm ([`time_fn::calendar::date_add_time`]):
//! unlike the units above, they always compute AND render a time-of-day
//! component — even for a `DATE`-only input, treated as midnight
//! (`2021-01-01 + 5 HOUR` = `2021-01-01 05:00:00`) — via absolute
//! seconds-since-epoch arithmetic, so overflow correctly carries into the
//! day and, through `civil_from_days`, into month/year
//! (`22:00:00 + 5 HOUR` = the next day's `03:00:00`). This is a
//! DIFFERENT, much simpler problem than the standalone `HOUR()`/
//! `MINUTE()`/`SECOND()` EXTRACTION functions below (see their own
//! paragraph): `DATE_ADD`'s interval unit is always explicit, so there is
//! no ambiguous string to reinterpret the way bare `MINUTE(...)` needs.
//!
//! `INTERVAL` itself ([`tidb_ast::Expr::Interval`]) is a general prefix
//! expression in the parser (matching real MySQL grammar, not
//! special-cased to `DATE_ADD`/`DATE_SUB`), but this evaluator only gives
//! it meaning as their second argument — an `Expr::Interval` there is
//! intercepted in [`func::eval_func`] BEFORE the uniform
//! eager-argument-evaluation every other function goes through (since its
//! `unit` is metadata, not a value `eval_in` can produce on its own).
//! `QUARTER` still parses but is `Unsupported` to evaluate. The interval
//! amount for a SINGLE unit accepts `Int` directly or `Decimal` (rounded to
//! the nearest whole unit via `Decimal::round_to_i64`, ties away from zero
//! — confirmed via `goeval` for both a positive and a negative half-unit,
//! and BEFORE any per-unit multiplication like `WEEK`'s `×7` or `YEAR`'s
//! `×12`); a `Str` amount is `Unsupported` there, needing MySQL's general
//! string-to-number coercion like `FROM_DAYS`'s argument. A COMPOSITE
//! unit's amount, by contrast, is always read as a string (an `Int`/
//! `Decimal` amount is formatted to its plain decimal string first,
//! matching Go's own `getIntervalFromInt`/`getIntervalFromReal`) and split
//! per [`time_fn::calendar::parse_composite_value`]'s doc. The result's
//! computed year is validated against
//! `DATE`'s real `0001`-`9999` range ([`time_fn::calendar::format_ymd_result`] /
//! [`time_fn::calendar::format_ymdhms_result`]): exactly `0` is MySQL's "zero date"
//! string (matching `FROM_DAYS`'s own convention — for `HOUR`/`MINUTE`/
//! `SECOND`, ONLY the date portion becomes the placeholder, the computed
//! time still shows through, e.g. `'0001-01-01 00:00:00' - 1 HOUR` =
//! `'0000-00-00 23:00:00'`), while any OTHER out-of-range year — negative,
//! or past `9999` — is `NULL` (a genuine asymmetry from `FROM_DAYS`'s
//! all-zero-date convention, confirmed via `goeval` for every unit alike).
//! This range check was MISSING entirely from an earlier increment's
//! `DAY`-only implementation — a real bug (`DATE_ADD('9999-12-31',
//! INTERVAL 1 DAY)` silently produced a malformed `10000-01-01` instead of
//! `NULL`), caught and fixed while probing `MONTH`/`YEAR`'s own boundary
//! behavior and confirming `DAY` obeys the identical rule.
//!
//! `HOUR`/`MINUTE`/`SECOND` EXTRACTION (the standalone functions, as
//! opposed to `DATE_ADD`'s interval arithmetic above) implements real
//! TiDB's own two-path algorithm ([`tidb_query_datatype::codec::mysql::Time::parse_native_hms`],
//! confirmed via `goeval`, not assumed), selected by whether the argument
//! contains a `:`: a colon-containing string parses as a structured
//! `[DATE ]H:M:S` (`S` defaults to `0`; `H` may be MULTI-DIGIT and exceed
//! 23, since `TIME` is an ELAPSED-time domain, not a wall-clock hour,
//! clamped to real TiDB's documented maximum `838:59:59` — an overflowing
//! `H` clamps the WHOLE value there, not just `H` alone, even when `M`/`S`
//! were individually valid; an out-of-range `M`/`S` invalidates the WHOLE
//! value regardless of `H`); a colon-LESS string (including a plain
//! `DATE`-only value, the common case for a `DATE` column) instead takes
//! ONLY its first digit run and reinterprets it as a right-aligned
//! `HHMMSS`-style number (so `MINUTE('2021-01-01')` is `20`, not `0`) —
//! the SAME rule an integer-literal argument like `HOUR(103045)` already
//! needs. This is unrelated to `DATE_ADD`'s `HOUR`/`MINUTE`/`SECOND`
//! interval handling above — that unit is always explicit, so there is no
//! ambiguous string to disambiguate the way bare `HOUR(...)` needs.
//!
//! `EXTRACT(unit FROM expr)` ([`tidb_ast::Expr::Extract`]) uses Go's separate
//! datetime, duration and mixed DAY_* string signatures through
//! `time_fn::extract`. Both AST and chunk evaluation reuse the datatype
//! extraction functions, preserving negative duration components.
//!
//! A genuinely unrelated gap surfaced while probing `EXTRACT`'s own
//! edge cases (deliberately deferred at the time to a dedicated later
//! increment, now closed): `time_fn::calendar::parse_date_ymd` did not handle a
//! bare, separator-less digit run at all (`YEAR(20240315)` gave `NULL`
//! instead of real TiDB's `2024`), the SAME class of gap `HOUR`/
//! `MINUTE`/`SECOND`'s own colon-less path already solved for `TIME`
//! values — `parse_date_ymd` simply never got the equivalent DATE-side
//! fix. Now fixed: a digit run of EXACTLY 6 or 8 digits is a separate
//! positional `YYMMDD`/`YYYYMMDD` reading (confirmed via `goeval`, not
//! limited to `EXTRACT`, since plain `YEAR(20240315)` diverged too).
//! Probing this surfaced a SECOND, related bug: the 2-digit year inside
//! that 6-digit form — and a separator-based date's own 1- or 2-digit
//! year — needs MySQL's real century-pivot rule (`00..=69` →
//! `2000..=2069`, `70..=99` → `1970..=1999`), which depends on the
//! year's ORIGINAL WRITTEN digit count, not its numeric value: a
//! 3-or-more-digit year is taken LITERALLY even when under 100
//! (`'099-03-15'` is year `99`, confirmed via `goeval`, not pivoted to
//! `1999`). Both fixes share one `expand_year` helper, applied uniformly
//! to the bare-digit-run path and the existing separator-based path
//! alike — `split_numeric_components` now returns each component's
//! digit count alongside its value specifically so the year component's
//! pivot decision has what it needs.
//!
//! `NOW()`/`CURRENT_TIMESTAMP()`/`CURDATE()`/`CURRENT_DATE()`/`CURTIME()`/
//! `CURRENT_TIME()`/`UTC_TIMESTAMP()`/`UTC_DATE()`/`UTC_TIME()` (each
//! `CURRENT_*`/`UTC_*` pair a true synonym of its non-`CURRENT_`/`UTC_`
//! sibling except `CURDATE`/`CURTIME`, which have no `UTC_` counterpart of
//! their own name; `CURRENT_TIMESTAMP`/`CURRENT_DATE`/`CURRENT_TIME`/
//! `UTC_DATE`/`UTC_TIME`/`UTC_TIMESTAMP` all also parse bare, with no `()`
//! at all — a genuine MySQL grammar rule `NOW`/`CURDATE`/`CURTIME` don't
//! share) all read [`Columns::now`] — the current statement's FIXED clock,
//! as `(utc_secs, nanos, tz_offset_seconds)`: the RAW Unix time, never
//! pre-adjusted, plus the session's `time_zone` offset to apply for
//! LOCAL rendering. `NOW`/`CURRENT_TIMESTAMP`/`CURDATE`/`CURTIME` apply the
//! offset; `UTC_TIMESTAMP`/`UTC_DATE`/`UTC_TIME` ignore it and render the
//! raw UTC value directly (confirmed via `gorun`: with a nonzero
//! `time_zone`, `UTC_TIMESTAMP()` only matches `NOW()` when the offset is
//! `+00:00`). `CURDATE`/`CURRENT_DATE`/`UTC_DATE` render `YYYY-MM-DD`
//! only; `CURTIME`/`CURRENT_TIME`/`UTC_TIME` render `HH:MM:SS[.ffffff]`
//! only (no argument at all for the `DATE` trio — confirmed via `godump
//! restore`: `CURDATE(1)` is a genuine parse error); the rest render the
//! full `YYYY-MM-DD HH:MM:SS[.ffffff]`. Rounding is genuinely
//! INCONSISTENT across this family — confirmed via `gorun` and by reading
//! `pkg/expression/builtin_time.go`, not assumed uniform: `NOW`/
//! `CURRENT_TIMESTAMP` always TRUNCATE the fraction; `UTC_TIMESTAMP`
//! always ROUNDS it (ties away from zero), for both its 0-arg and
//! explicit-arg forms alike; `CURTIME`/`CURRENT_TIME`/`UTC_TIME` instead
//! SPLIT — the 0-arg form truncates, but an EXPLICIT argument (even
//! literally `0`) rounds, matching Go's own two separate signatures for
//! each (`format` to no fractional digits at all vs. `format` to full
//! precision then reparse at the target scale). [`NoColumns`]
//! (constant-expression `eval`) has no session, so every function in this
//! family is always `Unsupported` there — this evaluator never falls back
//! to the live wall clock, which would be non-deterministic and
//! unverifiable against a static golden file; a caller establishes the
//! clock (via a `SET timestamp = ...`/`SET time_zone = ...` session, in
//! `tidb-exec`'s case) and threads the SAME value to every resolver used
//! while executing one top-level statement, so every clock-reading call
//! within it reads the identical value — matching real MySQL's "the clock
//! is fixed once per statement" semantics for free, with no dedicated
//! cache. `SYSDATE()` normally reads the live clock, while
//! `tidb_sysdate_is_now=ON` routes it through that same fixed statement clock.
//!
//! [`Decimal`] arithmetic (`+`/`-`/`*`) and comparison are exact — computed
//! digit-by-digit on the literal's own digit string, not through a binary
//! float — so they need no rounding and match MySQL's `DECIMAL` bit for bit.
//! `DIV`/`MOD` are exact too (unsigned long division on the same digit
//! strings, truncating toward zero — `DIV`'s quotient is an `Int`; `MOD`'s
//! remainder is a `Decimal` at `max(scale_a, scale_b)`, matching MySQL) and
//! decimal bitwise/shift ops round to the nearest `i64` first (ties away
//! from zero, MySQL's own decimal-to-integer conversion rule) before
//! applying the same integer operator. Bare `/` always promotes both
//! operands to `Decimal` (even two `Int` operands) and rounds to a result
//! scale of the DIVIDEND's own scale plus 4 (MySQL's `div_precision_increment`
//! — the same constant [`avg_of`] already uses, and the divisor's own scale
//! never affects it); `NULL` for division by zero.
//!
//! `FLOAT`/`DOUBLE` (`Datum::Real(f64)`) — the value domain for a
//! scientific-notation literal (`Expr::Float`, e.g. `1.5e2`) — uses
//! NATIVE `f64` arithmetic throughout: unlike `Decimal`, no custom
//! digit-string math is needed, since Rust's own `f64` Display was
//! confirmed (by direct comparison across a wide value range, including
//! subnormals and `f64::MAX`, not assumed) to produce byte-identical
//! output to Go's `strconv.FormatFloat(f, 'f', -1, 64)` — the parity risk
//! this domain was originally deferred over turned out not to exist. An
//! `Int` or `Decimal` operand promotes to `f64` — `Float` DOMINATES
//! `Decimal` in MySQL's promotion hierarchy, the OPPOSITE direction from
//! how `Decimal` dominates `Int` (confirmed via `goeval`: `1.5e2 + 3.14`
//! is `FLOAT:153.14`, not a `Decimal`) — so a `Float` operand is
//! intercepted before the `Decimal`/`Div` dispatch, not after. `DIV`
//! truncates its quotient toward zero to an `Int`, same as `Int`/
//! `Decimal`; `MOD` and `/` use native `f64` remainder/division, so a
//! fractional `MOD` result can carry the same floating-point rounding
//! noise real MySQL's own `f64` does; bitwise/shift operators round to
//! the nearest `i64` first, but TIES TO EVEN — the OPPOSITE tie-breaking
//! rule from `Decimal`'s own bitwise conversion (ties away from zero), a
//! real asymmetry confirmed via `goeval`, not assumed. A literal that
//! would overflow to infinity is rejected at PARSE time by the parser
//! itself (matching real TiDB, confirmed via `godump restore` — the
//! boundary is exactly `f64::MAX`), so every in-domain `Float` value here
//! is finite by construction; an ARITHMETIC result that overflows to
//! infinity is instead a genuine [`EvalError::FloatOverflow`] (confirmed
//! via `goeval`: MySQL raises a real evaluation error there, never
//! silently produces IEEE-754 infinity — underflow to zero, by contrast,
//! is fine and NOT an error). `ABS`/`SIGN`/`LEAST`/`GREATEST`/`NULLIF`
//! all cover `Float`, including MIXED Int/Decimal/Float argument lists
//! for `LEAST`/`GREATEST`/`NULLIF` (their comparison — and, for
//! `LEAST`/`GREATEST`, their RESULT type too — reuses the exact same
//! promotion `+`/`-` already implement, rather than a parallel hand-
//! rolled set of type-pair matches: a real bug where `LEAST`'s result
//! DIDN'T promote was caught by the differential corpus on the very
//! first attempt, not assumed correct); `SIGN(0.0)` is `0`, unlike
//! IEEE-754 `signum` (which is never `0`), confirmed via `goeval`.
//!
//! Anything else outside this domain (columns, other functions, subqueries —
//! resolved by the caller) returns [`EvalError::Unsupported`], so
//! results-ring coverage against the Go engine is measured, not assumed.
//!
//! `CAST(expr AS type)` / `CONVERT(...)` evaluation ([`cast::eval_cast`],
//! `tidb_ast::Expr::Cast`'s own arm here) covers `SIGNED`/`UNSIGNED`/
//! `CHAR`/`BINARY`/`DECIMAL`/`DATE`/`DATETIME`/`YEAR`/`DOUBLE`/`FLOAT`;
//! `TIME`/`JSON` are `Unsupported` (no value domain for either). `UNSIGNED`
//! evaluation is a first-class [`Datum::UInt`] domain: `CAST(-5 AS
//! UNSIGNED)` retains its UInt64 magnitude and comparisons/arithmetic do not
//! fall back to signed display bits.
//!
//! ## Module layout
//!
//! Split by concern so unrelated features can be extended without touching
//! the same file: [`Decimal`] (from the standalone `tidb-datatype` crate),
//! [`value`] (the [`Datum`] domain,
//! [`EvalError`], [`Columns`]), [`ops`] (unary/binary operator evaluation),
//! [`string_fn`] / [`date_fn`] / [`like`] / [`math_fn`] / [`cast`]
//! (builtin-function families and `CAST`/`CONVERT`), and [`func`] (the
//! builtin dispatch table + `IN` predicate) — all wired together by this
//! file's `eval_in`, the single recursive expression evaluator every other
//! module calls back into for its own subexpressions.

pub mod aggregation;
mod arg_eval_type;
mod binary_literal;
mod build;
pub mod builtin_arithmetic;
#[cfg(test)]
mod builtin_cast_semantics;
pub mod builtin_compare;
mod builtin_ext;
pub mod builtin_op;
pub mod builtin_registry;
mod cast;
mod coerce;
pub mod collation_derive;
pub mod column;
pub mod constant;
pub mod constant_fold;
pub mod constant_propagation;
mod go_flate;
pub use constant_fold::{
    derive_constant_null_flag, fold_constant_in_mode,
    fold_constant_in_mode_preserving_warning_casts, ConstantFoldMode,
};
mod context;
pub mod convert_charset;
pub mod evaluator;
pub mod expr_collation;
pub mod expr_util;
pub mod exprctx;
pub mod expression;
pub mod expropt;
pub mod exprstatic;
mod field_name;
pub mod fts;
mod func;
mod grouping;
pub mod infer_pushdown;
mod like;
mod math_fn;
pub mod memory_usage;
pub mod metabuild;
pub mod new_function;
pub use new_function::{
    new_function, new_function_base, new_function_impl, new_function_internal,
    new_function_try_fold, new_function_with_init, scalar_funcs_to_exprs, type_infer_for_null,
    ScalarFunctionCallBack,
};
pub mod distsql_builtin;
mod ops;
pub mod pb_predicate;
pub mod pushdown_catalog;
pub mod ranger_context;
mod regexp;
pub mod rewriter;
mod row;
pub mod scalar_function;
pub mod schema;
pub mod sessionexpr;
pub mod simple_expr;
mod string_fn;
mod string_packet;
mod string_signature;
mod tikv;
mod time_fn;
mod time_literal;
pub mod user_vars;

pub use field_name::{find_field_name, find_field_name_index_by_column, NonUniqueFieldName};

pub use build::{BuildContext, BuiltStringLength, StringLengthFunction, StringLengthSignature};
pub use coerce::truthy_of;
pub use context::{
    BlockEncryptionMode, Columns, CurrentTso, ErrorLevel, EvalError, JsonError, NoColumns,
    SequenceEvalError, SessionTimeZone, ZonedNoColumns,
};
pub use grouping::{GroupingFunction, GroupingMetadata, GroupingMetadataError, GroupingMode};
pub use like::{
    ilike_match, like_match_with_collation, like_match_with_collation_in, like_null_in,
};
pub use regexp::regexp_match_bin_collation;
pub use row::{compare_datums, compare_datums_with_collation};
pub(crate) use tidb_datatype::{Datum, Decimal};
pub use tidb_util::mathutil::MysqlRng;
pub use tikv::{
    eval_from_unixtime_legacy_scoped_in, eval_legacy_bytes_comparison_in, eval_legacy_date_in,
    eval_legacy_decimal_arithmetic_in, eval_legacy_decimal_comparison_in,
    eval_legacy_decimal_division_in, eval_legacy_decimal_integer_division_in,
    eval_legacy_integer_arithmetic_in, eval_legacy_integer_comparison_in,
    eval_legacy_json_array_append_step_in, eval_legacy_json_member_of_in,
    eval_legacy_json_merge_patch_in, eval_legacy_json_output_none_in, eval_legacy_json_replace_in,
    eval_legacy_like_in, eval_legacy_microsecond_in, eval_legacy_real_arithmetic_in,
    eval_legacy_real_comparison_in, eval_legacy_time_comparison_in, eval_regexp_legacy_ready_in,
    unix_timestamp_dec_legacy_in, unix_timestamp_int_legacy_in, AsciiExecution, AsciiOwnerError,
    AsciiPoolOwner, AsciiPoolPolicy, AsciiScope, BinaryArithmeticOperation, ComparisonOp,
    ExpressionAdapterFailure, ExpressionAdapterFailureClass, ExpressionAdapterFailureOrigin,
    ExpressionRuntimeFailure, ExpressionRuntimeFailureClass, ExpressionRuntimeFailurePhase,
    LegacyBinaryArgs, LegacyIntegerArithmetic, LegacyLikeArgs, RegexpLegacyInput,
    ScopedAsciiColumns,
};

use tidb_ast::{CastStyle, Expr, GetFormatSelector, IsTarget};

use binary_literal::{bit_literal_value, hex_literal_value};

/// Whether this AST node is a BIT literal, whose `types.DefaultTypeForValue`
/// arm is the one that does NOT add `mysql.UnsignedFlag` -- the AST tier's
/// stand-in for the `FieldType` the chunk tier reads instead.
fn is_signed_binary_literal(expr: &Expr) -> bool {
    match expr {
        Expr::Bit(_) => true,
        // Go drops unary plus while building the expression, so it cannot
        // change a BIT literal into the unsigned HEX-literal domain.
        Expr::Paren(inner) | Expr::Unary(tidb_ast::UnaryOp::Plus, inner) => {
            is_signed_binary_literal(inner)
        }
        _ => false,
    }
}

/// Go's arithmetic signatures include the source-shaped binary expression in
/// DOUBLE and DECIMAL overflow errors. The AST evaluator retains that syntax
/// until this boundary; the values-only operator helper intentionally keeps
/// returning its datum-level carrier.
fn ast_binary_overflow_error(
    operator: tidb_ast::BinaryOp,
    left: &Expr,
    right: &Expr,
    integer_unsigned: bool,
    error: EvalError,
) -> EvalError {
    let value = match error {
        EvalError::IntOverflow if integer_unsigned => "BIGINT UNSIGNED",
        EvalError::IntOverflow => "BIGINT",
        EvalError::FloatOverflow => "DOUBLE",
        EvalError::DecimalOverflow => "DECIMAL",
        _ => return error,
    };
    let Some(expression) = crate::math_fn::render_ast_binary_expression(operator, left, right)
    else {
        return error;
    };
    EvalError::DataOutOfRange { value, expression }
}
use coerce::{coerce_str, coerce_str_bytes};
use func::{eval_func, eval_in_list, negate_if};
use ops::{
    effective_div_precision_increment, eval_binary, eval_binary_with_div_precision, eval_unary,
    logic_and,
};
use row::row_compare_in;
use string_fn::{position_in, trim_value_in};

/// Evaluates a constant expression, or returns why it is out of scope.
pub fn eval(expr: &Expr) -> Result<Datum, EvalError> {
    eval_in(expr, &NoColumns)
}

/// Mirrors Go `expression.IsValidCurrentTimestampExpr` from
/// `pkg/expression/helper.go`.
///
/// The predicate is used while validating a temporal column's DEFAULT AST,
/// before the expression is lowered into an executable evaluator. Go accepts
/// only a `CURRENT_TIMESTAMP` function call: a bare call is valid when the
/// destination has no fractional-second precision, while an explicit first
/// integer argument is valid only when it exactly matches the destination
/// field type's decimal/FSP metadata. Additional arguments are intentionally
/// ignored here, matching Go's direct `Args[0]` read; malformed first
/// arguments simply fail the predicate.
#[must_use]
pub fn is_valid_current_timestamp_expr(
    expr: &Expr,
    field_type: Option<&tidb_datatype::FieldType>,
) -> bool {
    let Expr::Func { name, args, .. } = expr else {
        return false;
    };
    if !name.eq_ignore_ascii_case("CURRENT_TIMESTAMP") {
        return false;
    }

    match args.first() {
        None => field_type.is_none_or(|field_type| field_type.decimal() <= 0),
        Some(Expr::Int(digits)) => {
            let Some(field_type) = field_type else {
                return false;
            };
            let Ok(fsp) = digits.parse::<i64>() else {
                return false;
            };
            fsp == field_type.decimal()
        }
        Some(_) => false,
    }
}

/// The AST/value boundary for Go `expression.GetTimeValue`.
///
/// Go's helper is used while constructing temporal defaults, so it accepts
/// both raw sentinel strings (`CURRENT_TIMESTAMP`/`CURRENT_DATE`) and parser
/// value expressions. Rust represents the latter with [`Expr`]:
/// `String`/`Int`/`Null` stand in for `driver.ValueExpr`, `RawString` is the
/// untyped Go `string` case, `Func` is an AST function call, and `Unary` is
/// the small arithmetic form the source helper evaluates before parsing.
/// Unknown AST forms preserve Go's zero-value datum (`NULL`) rather than
/// pretending to evaluate a wider build-context surface.
pub fn get_time_value(
    cols: &dyn Columns,
    expr: &Expr,
    kind: tidb_datatype::TimeType,
    fsp: i64,
    explicit_timezone: Option<&tidb_datatype::SessionTimeZone>,
) -> Result<Datum, EvalError> {
    let parse_zone = explicit_timezone
        .cloned()
        .unwrap_or_else(|| cols.time_zone());
    let modes = cols.date_modes();

    let parse_text = |text: &str| {
        tidb_datatype::parse_time(
            text,
            kind,
            fsp,
            false,
            !modes.no_zero_in_date,
            modes.allow_invalid_dates,
            &parse_zone,
        )
        .map(|parsed| parsed.time)
        .map_err(|error| EvalError::TruncatedWrongValue(error.to_string()))
    };
    let parse_number = |number: i64| {
        tidb_datatype::parse_time_from_num(
            number,
            kind,
            fsp,
            !modes.no_zero_in_date,
            modes.allow_invalid_dates,
            number == 0 || !modes.no_zero_date,
            &parse_zone,
        )
        .map(|parsed| parsed.time)
        .map_err(|error| EvalError::TruncatedWrongValue(error.to_string()))
    };

    let time = match expr {
        // `GetTimeValue(ctx, string, ...)`: the two clock sentinels are
        // interpreted before ordinary text parsing.
        Expr::RawString(text) if text.eq_ignore_ascii_case("CURRENT_TIMESTAMP") => {
            current_time_value(cols, kind, fsp)?
        }
        Expr::RawString(text) if text.eq_ignore_ascii_case("CURRENT_DATE") => {
            current_date_value(cols, kind, fsp)?
        }
        Expr::RawString(text) if text == "0000-00-00 00:00:00" => {
            // Go logs (rather than returns) the zero-date parse error here;
            // the value remains the parser's zero temporal value.
            tidb_datatype::parse_time_from_num(
                0,
                kind,
                fsp,
                !modes.no_zero_in_date,
                modes.allow_invalid_dates,
                true,
                &parse_zone,
            )
            .map(|parsed| parsed.time)
            .map_err(|error| EvalError::TruncatedWrongValue(error.to_string()))?
        }
        Expr::RawString(text) => parse_text(text)?,

        // `*driver.ValueExpr` source cases.
        Expr::String(text) => parse_text(text)?,
        Expr::Int(digits) => {
            let number = digits.parse::<i64>().map_err(|_| EvalError::IntOverflow)?;
            parse_number(number)?
        }
        Expr::Null => return Ok(Datum::Null),

        // `*ast.FuncCallExpr` returns a string marker, not a parsed temporal
        // value; this is what DEFAULT-expression construction stores.
        Expr::Func { name, .. }
            if name.eq_ignore_ascii_case("CURRENT_TIMESTAMP")
                || name.eq_ignore_ascii_case("CURRENT_DATE") =>
        {
            return Ok(Datum::new_string(name.to_ascii_uppercase()));
        }
        Expr::Func { .. } => {
            return Err(EvalError::Unsupported("default value expression"));
        }

        // `*ast.UnaryOperationExpr`: evaluate the simple expression and then
        // feed its signed integer representation to ParseTimeFromNum.
        Expr::Unary(_, _) => {
            let value = eval_in(expr, cols)?;
            parse_number(crate::cast::to_i64_signed(&value))?
        }

        // Go's type switch returns the zero datum for every other `any` value.
        _ => return Ok(Datum::Null),
    };
    Ok(Datum::new_time(time))
}

fn current_time_value(
    cols: &dyn Columns,
    kind: tidb_datatype::TimeType,
    fsp: i64,
) -> Result<tidb_datatype::Time, EvalError> {
    let normalized_fsp = tidb_datatype::check_fsp(fsp)
        .map_err(|error| EvalError::TruncatedWrongValue(error.to_string()))?;
    let (seconds, nanos, _) = cols.now().ok_or(EvalError::Unsupported(
        "no statement clock for GetTimeValue",
    ))?;
    let instant = tidb_query_expr::native_typed_clock_utc(seconds, nanos)
        .ok_or(EvalError::Unsupported("statement clock is out of range"))?;
    let [year, month, day, hour, minute, second, microsecond] =
        tidb_query_expr::native_typed_clock_fields(instant, &cols.time_zone(), normalized_fsp);
    tidb_datatype::Time::from_date_checked(
        year,
        month,
        day,
        hour,
        minute,
        second,
        microsecond,
        kind,
        normalized_fsp,
    )
    .map_err(|error| EvalError::TruncatedWrongValue(error.to_string()))
}

fn current_date_value(
    cols: &dyn Columns,
    kind: tidb_datatype::TimeType,
    fsp: i64,
) -> Result<tidb_datatype::Time, EvalError> {
    let current = current_time_value(cols, kind, fsp)?;
    let [year, month, day, hour, minute, second, microsecond] =
        tidb_query_expr::native_typed_date_fields(current.core_time().raw());
    tidb_datatype::Time::from_date_checked(
        year,
        month,
        day,
        hour,
        minute,
        second,
        microsecond,
        kind,
        fsp,
    )
    .map_err(|error| EvalError::TruncatedWrongValue(error.to_string()))
}

/// Evaluates one already-built expression against the caller's statement
/// context and the single virtual row used for a column-free expression.
///
/// DDL constant folding lives in crates that should not depend directly on
/// `tidb-chunk`; keeping the virtual-row detail here also ensures every such
/// fold uses the same row shape instead of inventing a second evaluator.
pub fn eval_expression_once(
    expression: &expression::Expression,
    ctx: &dyn Columns,
) -> Result<Datum, EvalError> {
    let mut dual = tidb_chunk::chunk::Chunk::new_empty(&[]);
    dual.set_num_virtual_rows(1);
    expression.eval(ctx, dual.get_row(0))
}

/// Applies a binary operator to already-evaluated operands. Exposed so callers
/// that intercept some sub-expressions (e.g. aggregates during grouping) can
/// still reuse the operator semantics.
pub fn apply_binary(op: tidb_ast::BinaryOp, l: Datum, r: Datum) -> Result<Datum, EvalError> {
    eval_binary(op, l, r)
}

/// Moves a temporal value by `INTERVAL amount unit`, `sign` being `1` to add
/// and `-1` to subtract -- `DATE_ADD`/`DATE_SUB`'s own calendar arithmetic
/// applied to already-evaluated operands.
///
/// Exposed for the window executor's `RANGE BETWEEN INTERVAL n unit ...`
/// frame, whose boundary is the current row's `ORDER BY` key moved by the
/// interval; it must be the SAME arithmetic `DATE_ADD` performs, month-end
/// clamping and out-of-range `NULL` included.
pub fn date_add_interval(
    unit: &str,
    date: &Datum,
    amount: &Datum,
    sign: i64,
) -> Result<Datum, EvalError> {
    time_fn::calendar::date_add(unit, date, amount, sign)
}

/// Applies TiDB's byte-preserving `CONCAT` coercion to already-evaluated
/// values without round-tripping them through literal AST nodes.
pub fn concat_values(values: &[Datum]) -> Result<Datum, EvalError> {
    concat_values_in(values, &NoColumns)
}

/// Applies byte-preserving CONCAT using the caller's statement context.
pub fn concat_values_in(values: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    string_fn::concat_with_context(values, ctx)
}

/// Applies a binary operator with the current session's explicit
/// `div_precision_increment`. Every table-backed scalar, grouped, and window
/// division path calls this rather than relying on [`apply_binary`]'s
/// context-free default.
pub fn apply_binary_with_div_precision(
    op: tidb_ast::BinaryOp,
    l: Datum,
    r: Datum,
    div_precision_increment: u32,
    ctx: &dyn crate::context::Columns,
) -> Result<Datum, EvalError> {
    eval_binary_with_div_precision(op, l, r, div_precision_increment, ctx)
}

/// Applies a unary operator to an already-evaluated operand.
pub fn apply_unary(
    op: tidb_ast::UnaryOp,
    v: Datum,
    ctx: &dyn crate::context::Columns,
) -> Result<Datum, EvalError> {
    eval_unary(op, v, ops::Operand::Literal, ctx)
}

/// Closed boolean operations over a frontend-normalized truth/null marker.
/// These operations do not choose a SQL coercion policy or result FieldType.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BooleanFunction {
    /// Three-valued logical NOT.
    UnaryNot,
    /// Presence test; the value of a non-NULL marker is ignored.
    IsNull,
    /// IS TRUE, with NULL producing zero.
    IsTrue,
    /// IS FALSE, with NULL producing zero.
    IsFalse,
    /// Truth normalization that preserves NULL.
    IsTrueWithNull,
    /// NOT(IS NULL), evaluated as two official calls.
    IsNotNull,
    /// NOT(IS TRUE), not the NULL-distinct IS FALSE operation.
    IsNotTrue,
    /// NOT(IS FALSE), not the NULL-distinct IS TRUE operation.
    IsNotFalse,
}

/// Evaluates one boolean operation after the caller's original conversion.
///
/// `ready` is the already-evaluated truth value, or NULL. Presence-only
/// callers use `Some(false)` for every non-NULL datum without truth coercion.
/// The caller retains its own warning/error policy and passes the live context;
/// the shared driver supplies the answer, including for NULL inputs.
pub fn eval_boolean_ready_in(
    function: BooleanFunction,
    ready: Option<bool>,
    ctx: &dyn Columns,
) -> Result<Datum, EvalError> {
    let operation = match function {
        BooleanFunction::UnaryNot => tikv::EvaluatedBytesOp::UnaryNot,
        BooleanFunction::IsNull => tikv::EvaluatedBytesOp::IsNull,
        BooleanFunction::IsTrue => tikv::EvaluatedBytesOp::IsTrue,
        BooleanFunction::IsFalse => tikv::EvaluatedBytesOp::IsFalse,
        BooleanFunction::IsTrueWithNull => tikv::EvaluatedBytesOp::IsTrueWithNull,
        BooleanFunction::IsNotNull => tikv::EvaluatedBytesOp::IsNotNull,
        BooleanFunction::IsNotTrue => tikv::EvaluatedBytesOp::IsNotTrue,
        BooleanFunction::IsNotFalse => tikv::EvaluatedBytesOp::IsNotFalse,
    };
    tikv::evaluate_args_in(
        operation,
        ctx,
        || Ok(tikv::EvaluatedArgs::Int(ready.map(i64::from))),
        tikv::EvaluatedBytesResult::into_boolean_datum,
    )
}

/// Closed binary logical operations over frontend-normalized truth values.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LogicalFunction {
    /// Three-valued logical AND.
    And,
    /// Three-valued logical OR.
    Or,
    /// Three-valued logical XOR.
    Xor,
}

/// Records which logical arguments the original frontend actually demanded.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LogicalArgs {
    /// Both children were evaluated and converted, including genuine SQL NULLs.
    Both(Option<bool>, Option<bool>),
    /// Only the left child was demanded. Legal solely for false AND or true OR;
    /// the absent right child is not an evaluated SQL NULL or a supplied value.
    UndemandedRight {
        /// The evaluated and converted left child.
        left: Option<bool>,
    },
}

/// Evaluates a logical operation without choosing or repeating child demand.
///
/// Invalid demand markers fail the adapter contract before worker admission.
/// A legal undemanded right child uses an explicitly irrelevant representative
/// inside the closed kernel call; even short-circuit answers come from that call.
pub fn eval_logical_ready_in(
    function: LogicalFunction,
    arguments: LogicalArgs,
    ctx: &dyn Columns,
) -> Result<Datum, EvalError> {
    tikv::evaluate_logical_in(function, arguments, ctx)
}

/// Evaluates PI through the shared no-argument value driver. This seam has
/// no dummy operand and returns the kernel's owned, non-NULL real result.
/// Existing SQL constant folding is unchanged; a fold computes through the
/// same math entry, while direct callers retain their explicit context.
pub fn eval_pi_in(ctx: &dyn Columns) -> Result<Datum, EvalError> {
    tikv::evaluate_args_in(
        tikv::EvaluatedBytesOp::PiRaw,
        ctx,
        || Ok(tikv::EvaluatedArgs::NoArgs),
        tikv::EvaluatedBytesResult::into_nonnull_real_datum,
    )
}

/// Reads legacy MONTH from its already-evaluated raw CoreTime through the
/// shared worker, preserving NULL without a Time constructor or validation.
pub fn eval_legacy_month_in(
    value: Option<tidb_datatype::CoreTime>,
    ctx: &dyn Columns,
) -> Result<Option<i64>, EvalError> {
    time_fn::month_core_in(value, ctx)
}

/// Evaluates legacy DATEDIFF from both already-observed nullable raw cores.
/// No date reconstruction, clock clearing or SQL-text validation occurs here.
pub fn eval_legacy_date_diff_in(
    left: Option<tidb_datatype::CoreTime>,
    right: Option<tidb_datatype::CoreTime>,
    ctx: &dyn Columns,
) -> Result<Option<i64>, EvalError> {
    time_fn::calendar::date_diff_core_in(left, right, ctx)
}

/// Formats legacy DATE_FORMAT's already-observed raw core and ready layout.
/// `None` records an actual NULL time whose layout was never demanded. With a
/// core present, the optional layout is its actual nullable value after the
/// caller's original lossy UTF-8 conversion; this seam does not coerce it again.
pub fn eval_legacy_date_format_in(
    value: Option<(tidb_datatype::CoreTime, Option<&str>)>,
    ctx: &dyn Columns,
) -> Result<Option<Vec<u8>>, EvalError> {
    let operation = match value {
        None => tikv::EvaluatedBytesOp::DateFormatNullNative,
        Some(_) => tikv::EvaluatedBytesOp::DateFormatCoreNative,
    };
    tikv::evaluate_args_in(
        operation,
        ctx,
        || {
            Ok(match value {
                None => tikv::EvaluatedArgs::NullWitness(None),
                Some((core, layout)) => tikv::EvaluatedArgs::TimeCoreBitsBytes {
                    core: core.raw(),
                    bytes: layout.map(|layout| layout.as_bytes().to_vec()),
                },
            })
        },
        tikv::EvaluatedBytesResult::into_bytes,
    )
}

/// Carries an actual observed PB DATE_FORMAT NULL without demanding another
/// child or coercing an already-evaluated prefix. The owned worker result is
/// packed normally; it is not discarded in favor of a host-created NULL.
pub fn eval_date_format_null_in(ctx: &dyn Columns) -> Result<Datum, EvalError> {
    tikv::evaluate_args_in(
        tikv::EvaluatedBytesOp::DateFormatNullNative,
        ctx,
        || Ok(tikv::EvaluatedArgs::NullWitness(None)),
        |computed| {
            Ok(computed
                .into_bytes()?
                .map_or(Datum::Null, Datum::new_string))
        },
    )
}

/// Preserves legacy DATE_FORMAT's boolean path with genuinely no first child.
/// No dummy operand or SQL NULL is invented; the worker supplies the integer
/// answer, which this packer only widens to the legacy i128 result domain.
pub fn eval_legacy_date_format_missing_in(ctx: &dyn Columns) -> Result<Option<i128>, EvalError> {
    tikv::evaluate_args_in(
        tikv::EvaluatedBytesOp::DateFormatMissingNative,
        ctx,
        || Ok(tikv::EvaluatedArgs::NoArgs),
        |computed| match computed.into_int_datum()? {
            Datum::Null => Ok(None),
            Datum::Int(value) => Ok(Some(i128::from(value))),
            _ => Err(EvalError::Unsupported(
                "legacy DATE_FORMAT missing-child result kind mismatch",
            )),
        },
    )
}

/// Reads legacy WEEK's mode-zero projection from its actual nullable raw core.
/// No session default getter, text parsing or calendar validation is introduced.
pub fn eval_legacy_week_in(
    value: Option<tidb_datatype::CoreTime>,
    ctx: &dyn Columns,
) -> Result<Datum, EvalError> {
    time_fn::week_core_in(value, ctx)
}

/// Closed legacy duration projections, distinct from native SQL text parsing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LegacyHmsField {
    Hour,
    Minute,
    Second,
}

/// Projects an already-evaluated signed nanosecond count through the shared
/// worker. NULL also executes the worker; no Duration or FSP is reconstructed.
pub fn eval_legacy_hms_in(
    field: LegacyHmsField,
    nanos: Option<i64>,
    ctx: &dyn Columns,
) -> Result<Option<i64>, EvalError> {
    time_fn::calendar::hms_nanos_in(field, nanos, ctx)
}

/// Evaluates legacy integer ROUND's identity through the shared worker.
/// The full signed i128 domain, including NULL, crosses this closed bridge;
/// callers may convert the owned result to real only after computation.
pub fn eval_legacy_round_int_in(
    value: Option<i128>,
    ctx: &dyn Columns,
) -> Result<Option<i128>, EvalError> {
    tikv::evaluate_args_in(
        tikv::EvaluatedBytesOp::RoundInt128Legacy,
        ctx,
        || Ok(tikv::EvaluatedArgs::Int128(value)),
        tikv::EvaluatedBytesResult::into_int128,
    )
}

/// Rounds an already-evaluated legacy real to scale zero, with ties away
/// from zero (unlike native SQL ROUND's ties-even policy). Raw IEEE values
/// and NULL enter the shared worker; no finite-result policy is applied here.
pub fn eval_legacy_round_real_in(
    value: Option<f64>,
    ctx: &dyn Columns,
) -> Result<Option<f64>, EvalError> {
    tikv::evaluate_args_in(
        tikv::EvaluatedBytesOp::RoundRealLegacy,
        ctx,
        || Ok(tikv::EvaluatedArgs::Ieee754Bits(value.map(f64::to_bits))),
        |computed| Ok(computed.into_ieee754_bits()?.map(f64::from_bits)),
    )
}

/// Sends an unrounded exact decimal to legacy ROUND's shared worker.
/// The worker rounds to scale zero before converting to raw binary64; this
/// bridge only transports the wide coefficient and preserves NULL and errors.
pub fn eval_legacy_round_decimal_in(
    value: Option<&Decimal>,
    ctx: &dyn Columns,
) -> Result<Option<f64>, EvalError> {
    tikv::evaluate_args_in(
        tikv::EvaluatedBytesOp::RoundDecimalLegacy,
        ctx,
        || {
            Ok(tikv::EvaluatedArgs::Decimal(
                value.map(tikv::prepare_math_decimal).transpose()?,
            ))
        },
        |computed| Ok(computed.into_ieee754_bits()?.map(f64::from_bits)),
    )
}

/// Closed legacy libm operations, distinct from native SQL's Go-bit policy.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LegacyTrigFunction {
    Sin,
    Cos,
    Cot,
    Atan,
}

/// Computes a demanded legacy unary argument, preserving raw NaN and infinity.
/// NULL also enters the shared worker; no SQL finite-result policy is applied.
pub fn eval_legacy_trig_in(
    kind: LegacyTrigFunction,
    value: Option<f64>,
    ctx: &dyn Columns,
) -> Result<Option<f64>, EvalError> {
    let operation = match kind {
        LegacyTrigFunction::Sin => tikv::EvaluatedBytesOp::SinLibmLegacy,
        LegacyTrigFunction::Cos => tikv::EvaluatedBytesOp::CosLibmLegacy,
        LegacyTrigFunction::Cot => tikv::EvaluatedBytesOp::CotLibmLegacy,
        LegacyTrigFunction::Atan => tikv::EvaluatedBytesOp::AtanLibmLegacy,
    };
    tikv::evaluate_args_in(
        operation,
        ctx,
        || Ok(tikv::EvaluatedArgs::Ieee754Bits(value.map(f64::to_bits))),
        |computed| Ok(computed.into_ieee754_bits()?.map(f64::from_bits)),
    )
}

/// Computes legacy libm atan2(y, x) without changing operand demand.
/// `None` records a NULL y and an undemanded x; `Some((y, x))` carries the
/// non-NULL y and the actual demanded x. Raw nonfinite results are retained.
pub fn eval_legacy_atan2_in(
    arguments: Option<(f64, Option<f64>)>,
    ctx: &dyn Columns,
) -> Result<Option<f64>, EvalError> {
    tikv::evaluate_args_in(
        tikv::EvaluatedBytesOp::Atan2LibmLegacy,
        ctx,
        || {
            let (left, right) = match arguments {
                None => (
                    tikv::ReadyIeee754Arg::Value(None),
                    tikv::ReadyIeee754Arg::Undemanded,
                ),
                Some((y, x)) => (
                    tikv::ReadyIeee754Arg::Value(Some(y.to_bits())),
                    tikv::ReadyIeee754Arg::Value(x.map(f64::to_bits)),
                ),
            };
            Ok(tikv::EvaluatedArgs::Ieee754Bits2 { left, right })
        },
        |computed| Ok(computed.into_ieee754_bits()?.map(f64::from_bits)),
    )
}

/// Closed raw inverse-trigonometric operations for the legacy real channel.
/// This is not a new SQL or protobuf admission surface.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RawInverseTrigFunction {
    /// Raw arcsine, preserving NaN rather than applying SQL domain policy.
    Asin,
    /// Raw arccosine, preserving NaN rather than applying SQL domain policy.
    Acos,
}

/// Evaluates one already-converted raw inverse-trigonometric argument.
///
/// The result is `Datum::Real` (including NaN) or `Datum::Null`. Ordinary
/// ASIN/ACOS apply their NaN-to-NULL output policy separately. The legacy
/// real channel must retain NaN for its casts and `total_cmp` consumers.
/// NULL enters the same real C4 call; adapter/runtime errors are not NULL.
pub fn eval_raw_inverse_trig_ready_in(
    function: RawInverseTrigFunction,
    ready: Option<f64>,
    ctx: &dyn Columns,
) -> Result<Datum, EvalError> {
    let operation = match function {
        RawInverseTrigFunction::Asin => tikv::EvaluatedBytesOp::AsinRaw,
        RawInverseTrigFunction::Acos => tikv::EvaluatedBytesOp::AcosRaw,
    };
    tikv::evaluate_args_in(
        operation,
        ctx,
        || Ok(tikv::EvaluatedArgs::Ieee754Bits(ready.map(f64::to_bits))),
        |computed| {
            Ok(computed
                .into_ieee754_bits()?
                .map_or(Datum::Null, |bits| Datum::Real(f64::from_bits(bits))))
        },
    )
}

/// Already-demanded POW operands for the legacy raw-real channel.
/// This is not a SQL or protobuf admission surface.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum RawPowReadyArgs {
    /// The evaluated left operand was NULL; the right child was not demanded.
    LeftNull,
    /// A non-NULL left operand and the actual evaluated right operand.
    Values { base: f64, exponent: Option<f64> },
}

/// Computes raw POW without applying native SQL finite-result policy.
/// NaN and infinity remain available to legacy casts and comparisons; NULL
/// still enters C4, with an undemanded right child distinguished from SQL NULL.
/// Like the raw inverse-trig bridge, returns only Datum::Real or Datum::Null.
pub fn eval_raw_pow_ready_in(
    arguments: RawPowReadyArgs,
    ctx: &dyn Columns,
) -> Result<Datum, EvalError> {
    tikv::evaluate_args_in(
        tikv::EvaluatedBytesOp::PowNative,
        ctx,
        || {
            let (left, right) = match arguments {
                RawPowReadyArgs::LeftNull => (
                    tikv::ReadyIeee754Arg::Value(None),
                    tikv::ReadyIeee754Arg::Undemanded,
                ),
                RawPowReadyArgs::Values { base, exponent } => (
                    tikv::ReadyIeee754Arg::Value(Some(base.to_bits())),
                    tikv::ReadyIeee754Arg::Value(exponent.map(f64::to_bits)),
                ),
            };
            Ok(tikv::EvaluatedArgs::Ieee754Bits2 { left, right })
        },
        |computed| {
            Ok(computed
                .into_ieee754_bits()?
                .map_or(Datum::Null, |bits| Datum::Real(f64::from_bits(bits))))
        },
    )
}

/// Closed case-conversion operations for the legacy bytes channel.
/// The ASCII forms fold ASCII octets; unlike wire binary case conversion,
/// they are not no-ops. UTF-8 callers retain their own input normalization.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RawCaseFunction {
    LowerAscii,
    UpperAscii,
    LowerUtf8,
    UpperUtf8,
}

/// Converts already-prepared legacy bytes through the closed value driver.
/// Returns only Datum::Bytes or Datum::Null, including for UTF-8 operations;
/// callers own normalization, while the shared kernel owns all case mapping.
pub fn eval_raw_case_ready_in(
    function: RawCaseFunction,
    ready: Option<Vec<u8>>,
    ctx: &dyn Columns,
) -> Result<Datum, EvalError> {
    let operation = match function {
        RawCaseFunction::LowerAscii => tikv::EvaluatedBytesOp::LowerAsciiNative,
        RawCaseFunction::UpperAscii => tikv::EvaluatedBytesOp::UpperAsciiNative,
        RawCaseFunction::LowerUtf8 => tikv::EvaluatedBytesOp::LowerUtf8Ready,
        RawCaseFunction::UpperUtf8 => tikv::EvaluatedBytesOp::UpperUtf8Ready,
    };
    tikv::evaluate_args_in(
        operation,
        ctx,
        || Ok(tikv::EvaluatedArgs::Bytes(ready)),
        |computed| Ok(computed.into_bytes()?.map_or(Datum::Null, Datum::Bytes)),
    )
}

/// A legacy CONV base retains its full width and whether its child was demanded.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LegacyConvBase {
    Value(Option<i128>),
    Undemanded,
}

/// Computes legacy CONV from already-demanded bytes and integer bases.
/// NULL and out-of-i64 bases reach the same shared worker as ordinary values;
/// the kernel owns lossy text decoding, radix conversion and overflow-to-NULL.
pub fn eval_legacy_conv_in(
    number: Option<Vec<u8>>,
    from_base: LegacyConvBase,
    to_base: LegacyConvBase,
    ctx: &dyn Columns,
) -> Result<Option<Vec<u8>>, EvalError> {
    tikv::evaluate_args_in(
        tikv::EvaluatedBytesOp::ConvLegacy,
        ctx,
        || {
            let ready = |base| match base {
                LegacyConvBase::Value(value) => tikv::ReadyConvBaseArg::Value(value),
                LegacyConvBase::Undemanded => tikv::ReadyConvBaseArg::Undemanded,
            };
            Ok(tikv::EvaluatedArgs::ConvLegacyReady {
                number: tikv::ReadyBytesArg::Value(number),
                from_base: ready(from_base),
                to_base: ready(to_base),
            })
        },
        tikv::EvaluatedBytesResult::into_bytes,
    )
}

/// Legacy substring units, independent of ordinary SQL/wire substring policy.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RawSubstringFunction {
    Bytes,
    Utf8,
}

/// A legacy integer operand retains both its full width and its demand state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RawSubstringInt {
    Value(Option<i128>),
    Undemanded,
}

/// The two actual legacy substring arities; no synthetic maximum length.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RawSubstringReadyArgs {
    Two {
        bytes: Option<Vec<u8>>,
        pos: RawSubstringInt,
    },
    Three {
        bytes: Option<Vec<u8>>,
        pos: RawSubstringInt,
        len: RawSubstringInt,
    },
}

/// Asks the shared range implementation whether legacy substring needs its
/// third child. The final kernel independently checks the actual ready inputs.
pub fn raw_substring_needs_len(source: &[u8], position: i128, utf8: bool) -> bool {
    tikv::legacy_substring_needs_len(source, position, utf8)
}

/// Computes legacy substring from demanded values, returning Bytes or NULL.
/// The shared kernel owns width rejection, lossy UTF-8, ranges and slicing;
/// the frontend owns only evaluation order and legacy integer conversion.
pub fn eval_raw_substring_ready_in(
    function: RawSubstringFunction,
    arguments: RawSubstringReadyArgs,
    ctx: &dyn Columns,
) -> Result<Datum, EvalError> {
    let operation = match (&arguments, function) {
        (RawSubstringReadyArgs::Two { .. }, RawSubstringFunction::Bytes) => {
            tikv::EvaluatedBytesOp::Substring2BytesLegacy
        }
        (RawSubstringReadyArgs::Two { .. }, RawSubstringFunction::Utf8) => {
            tikv::EvaluatedBytesOp::Substring2Utf8Legacy
        }
        (RawSubstringReadyArgs::Three { .. }, RawSubstringFunction::Bytes) => {
            tikv::EvaluatedBytesOp::Substring3BytesLegacy
        }
        (RawSubstringReadyArgs::Three { .. }, RawSubstringFunction::Utf8) => {
            tikv::EvaluatedBytesOp::Substring3Utf8Legacy
        }
    };
    tikv::evaluate_args_in(
        operation,
        ctx,
        || {
            let ready = |value| match value {
                RawSubstringInt::Value(value) => tikv::ReadySubstringI128::Value(value),
                RawSubstringInt::Undemanded => tikv::ReadySubstringI128::Undemanded,
            };
            Ok(match arguments {
                RawSubstringReadyArgs::Two { bytes, pos } => {
                    tikv::EvaluatedArgs::LegacySubstring2Ready {
                        bytes,
                        pos: ready(pos),
                    }
                }
                RawSubstringReadyArgs::Three { bytes, pos, len } => {
                    tikv::EvaluatedArgs::LegacySubstring3Ready {
                        bytes,
                        pos: ready(pos),
                        len: ready(len),
                    }
                }
            })
        },
        |computed| Ok(computed.into_bytes()?.map_or(Datum::Null, Datum::Bytes)),
    )
}

/// `AVG`'s `SUM / COUNT`, exposed so `tidb-exec` can compute it without
/// reimplementing decimal division: an `Int` sum promotes to decimal (scale
/// 0, MySQL's implicit rule, same as every other decimal op); the result
/// scale grows by MySQL's `div_precision_increment` past the sum's own scale,
/// and is ROUNDED to that scale (ties away from zero) via true division — unlike `DIV`/`MOD`,
/// which truncate exactly and need no such growth. A `Float` sum instead
/// divides via plain native `f64` division — MySQL's `div_precision_increment`
/// scale growth is a `DECIMAL`-specific rule that doesn't apply to `AVG`
/// over a real `FLOAT`/`DOUBLE` column (confirmed via `gorun`: `AVG` there
/// is exactly `sum / count`, not assumed to match the `Decimal` rule).
/// `count` must be positive (an empty group is the caller's job to turn
/// into `NULL` before calling this, same as `SUM`).
pub fn avg_of(sum: Datum, count: i64) -> Result<Datum, EvalError> {
    avg_of_with_div_precision(sum, count, 4)
}

/// The session-aware form of [`avg_of`]. `AVG` uses the same
/// `div_precision_increment` as scalar `/`, so callers with a SQL session
/// must pass its current value explicitly.
pub fn avg_of_with_div_precision(
    sum: Datum,
    count: i64,
    div_precision_increment: u32,
) -> Result<Datum, EvalError> {
    let d = match sum {
        Datum::Real(f) => return Ok(Datum::Real(f / count as f64)),
        Datum::Float32(f) => return Ok(Datum::Float32(f / count as f64)),
        Datum::Decimal(d) => d,
        Datum::Int(i) => Decimal::from_int(i),
        Datum::UInt(i) => Decimal::from_uint(i),
        Datum::String(_) | Datum::Bytes(_) | Datum::Null | Datum::MinNotNull | Datum::MaxValue => {
            return Err(EvalError::Unsupported("AVG of non-numeric"));
        }
        other => {
            other
                .to_decimal()
                .map_err(|_| EvalError::Unsupported("AVG of non-numeric"))?
                .value
        }
    };
    let target_scale = d.scale() + effective_div_precision_increment(div_precision_increment);
    Ok(Datum::Decimal(d.div_round(count, target_scale)))
}

/// Fits a value into a `DECIMAL(precision, scale)` column for storage:
/// rounds a numeric value to `scale` and range-checks its integer part
/// (see [`Decimal::fit_precision_scale`]). Returns the rounded value, or
/// `None` when the integer part overflows (the caller turns that into a
/// column-out-of-range error). `NULL` and any non-numeric value pass
/// through unchanged — coercing those is outside this width-check's scope.
/// Used by `tidb_exec`'s `INSERT`/`UPDATE` column-width validation.
pub fn fit_decimal_column(value: Datum, precision: u32, scale: u32) -> Option<Datum> {
    match value {
        Datum::Decimal(d) => d.fit_precision_scale(precision, scale).map(Datum::Decimal),
        Datum::Int(i) => Decimal::from_int(i)
            .fit_precision_scale(precision, scale)
            .map(Datum::Decimal),
        Datum::UInt(i) => Decimal::from_uint(i)
            .fit_precision_scale(precision, scale)
            .map(Datum::Decimal),
        other => Some(other),
    }
}

/// Evaluates an expression, resolving column references via `cols`.
pub fn eval_in(expr: &Expr, cols: &dyn Columns) -> Result<Datum, EvalError> {
    match expr {
        Expr::Int(s) => s
            .parse::<u64>()
            .map(|i| match i64::try_from(i) {
                Ok(i) => Datum::Int(i),
                Err(_) => Datum::UInt(i),
            })
            .map_err(|_| EvalError::IntOverflow),
        Expr::Bool(b) => Ok(Datum::Int(i64::from(*b))),
        Expr::String(s) => Ok(Datum::new_string(s.clone())),
        Expr::CharsetString { charset, value } => {
            let collation_name = tidb_datatype::get_default_collation_legacy(charset)
                .map_err(|_| EvalError::Unsupported("unknown character introducer"))?;
            let collation = tidb_datatype::Collation::from_name(&collation_name)
                .ok_or(EvalError::Unsupported("unknown character introducer"))?;
            Ok(if collation == tidb_datatype::Collation::Binary {
                Datum::Bytes(value.as_bytes().to_vec())
            } else {
                Datum::new_collation_string(value.as_bytes().to_vec(), collation)
            })
        }
        Expr::Decimal(s) => Ok(Datum::Decimal(Decimal::from_literal(s))),
        // Always finite: the parser itself rejects a literal that would
        // overflow to infinity (confirmed via `godump restore` — real
        // TiDB rejects `1e400` at PARSE time, not eval time), so no
        // finiteness check is needed here.
        Expr::Float(f) => Ok(Datum::Real(*f)),
        Expr::Hex(digits) => hex_literal_value(digits),
        Expr::Bit(digits) => bit_literal_value(digits),
        // Go's charset introducer annotates the ValueExpr FieldType while the
        // Datum remains the same KindBinaryLiteral/KindMysqlBit payload. The
        // value-tier evaluator therefore delegates to the introduced leaf;
        // the chunk rewriter below retains the type annotation separately.
        Expr::CharsetBinary { value, .. } => eval_in(value, cols),
        Expr::Null => Ok(Datum::Null),
        Expr::Column(path) => cols
            .get(path)
            .ok_or(EvalError::Unsupported("unknown column")),
        // Reading an unset (or session-less) user variable is `NULL`,
        // never an error — the opposite convention from `Expr::SysVar`
        // just below, whose UNRECOGNIZED-name case is a genuine
        // `Unsupported` (see `Columns::get_uservar`'s own doc for why the
        // two differ). `SET @x = ...` (assignment) is a separate
        // top-level `tidb_ast::SessionStmt::SetUserVar` statement, not an
        // expression form reachable from here.
        Expr::UserVar(name) => Ok(cols.get_uservar(name).unwrap_or(Datum::Null)),
        // The inline `@x := expr` ASSIGNMENT EXPRESSION (usable
        // mid-`SELECT`, e.g. the classic MySQL running-total idiom
        // `SELECT @rn := @rn + 1 FROM t`, confirmed via `gorun`):
        // evaluates `value`, writes it through `Columns::set_uservar`
        // (a SIDE EFFECT — see that method's own doc for the interior-
        // mutability architecture this relies on), and evaluates to the
        // SAME assigned value, matching `gorun`'s own observed
        // `SELECT @i := 1` => `1`. This function's own CALLER
        // (`tidb_exec::aggregate::Database::project_row`'s row-wise
        // select-list loop, evaluated left to right per row) is what
        // gives a LATER select-list item visibility into an EARLIER
        // one's assignment within the same row, and one row's
        // assignment visibility into the next — this function itself
        // has no notion of "row" or "order," it just performs the one
        // write it's asked for.
        Expr::Assign { name, value } => {
            let v = eval_in(value, cols)?;
            // Go's scalar `SETVAR` signatures return NULL without touching
            // the existing variable when their RHS is NULL
            // (`builtin_other.go`'s `builtinSet*VarSig`). This is distinct
            // from top-level `SET @x = NULL`, whose executor semantics clear
            // the session value, so keep the boundary here rather than
            // teaching `Columns::set_uservar` a statement-kind flag.
            if v != Datum::Null {
                cols.set_uservar(name, v.clone());
            }
            Ok(v)
        }
        Expr::SysVar { scope, name } => cols
            .sysvar(*scope, name)
            .ok_or(EvalError::Unsupported("unknown system variable")),
        Expr::Paren(e) => eval_in(e, cols),
        // Go's parser does not build an operator node for unary plus: it
        // returns the operand unchanged. Delegating here preserves the
        // operand's exact Datum kind as well as its value (notably signed BIT
        // literals, which the generic numeric-unary path would promote to a
        // Decimal before surrounding arithmetic can inspect the type).
        Expr::Unary(tidb_ast::UnaryOp::Plus, e) => eval_in(e, cols),
        Expr::Unary(op, e) => eval_unary(*op, eval_in(e, cols)?, ops::Operand::Literal, cols),
        // `ROW(...) <op> ROW(...)` — see `crate::row`'s own doc for
        // why this is a special case rather than a new `Datum`
        // variant: real MySQL/TiDB restricts a bare `ROW(...)` to
        // ONLY appear as a comparison/`IN` operand, so `eval_in` never
        // needs to evaluate one standalone.
        Expr::Binary(op, l, r)
            if matches!((l.as_ref(), r.as_ref()), (Expr::Row(_), Expr::Row(_))) =>
        {
            let (Expr::Row(lv), Expr::Row(rv)) = (l.as_ref(), r.as_ref()) else {
                unreachable!("checked above")
            };
            let lv: Vec<Datum> = lv
                .iter()
                .map(|e| eval_in(e, cols))
                .collect::<Result<_, _>>()?;
            let rv: Vec<Datum> = rv
                .iter()
                .map(|e| eval_in(e, cols))
                .collect::<Result<_, _>>()?;
            row_compare_in(*op, &lv, &rv, cols)
        }
        Expr::Binary(op, l, r) => {
            // Go's `DefaultTypeForValue` gives a BIT literal a SIGNED field
            // type and a HEX one an unsigned one, and the arithmetic classes
            // are the only place that difference is reachable. This tier has
            // no `FieldType` at all, so the AST node itself is the type
            // fact -- see `binary_literal::cast_signed_literal_operands`.
            let signed = [is_signed_binary_literal(l), is_signed_binary_literal(r)];
            let (left, right) = binary_literal::cast_signed_literal_operands(
                *op,
                eval_in(l, cols)?,
                eval_in(r, cols)?,
                signed,
            );
            let unsigned_result =
                !matches!(*op, tidb_ast::BinaryOp::Minus) || !cols.no_unsigned_subtraction();
            let integer_unsigned = unsigned_result
                && (matches!(&left, Datum::UInt(_)) || matches!(&right, Datum::UInt(_)));
            eval_binary_with_div_precision(*op, left, right, cols.div_precision_increment(), cols)
                .map_err(|error| ast_binary_overflow_error(*op, l, r, integer_unsigned, error))
        }
        // A constant `RAND(N)` has state per function occurrence for the
        // whole statement. The function node's address is stable while this
        // parsed statement is evaluated; an argument-slice view is not,
        // because temporary views can be reused for siblings.
        Expr::Func { name, args, .. } => {
            eval_func(name, args, cols, Some(expr as *const Expr as usize))
        }
        // EXTRACT owns its datetime/duration signature and signed unit value.
        Expr::Extract { unit, value } => {
            time_fn::extract::extract(unit, &eval_in(value, cols)?, None, cols)
        }
        // `TIMESTAMPADD(unit, n, datetime)`'s unit is a dedicated AST field
        // rather than an argument expression (see
        // `tidb_ast::Expr::TimestampAdd`), and Go's
        // `builtinTimestampAddSig.evalString` reads it as its first VALUE --
        // so the same implementation the chunk tier reaches through the
        // rewriter runs here with the unit prepended as a string datum.
        Expr::TimestampAdd {
            unit,
            interval,
            expr,
        } => {
            let vals = vec![
                Datum::new_string(unit.clone()),
                eval_in(interval, cols)?,
                eval_in(expr, cols)?,
            ];
            // This arm builds its own argument list, so it must impose the
            // declared argument eval types itself: Go's
            // `timestampAddFunctionClass` declares
            // `types.ETString, types.ETString, types.ETReal, types.ETDatetime`
            // (`builtin_time.go:6551`), and the signature body is entitled to
            // a DATETIME third argument either way it is reached.
            let vals = arg_eval_type::wrap_datetime_args("TIMESTAMPADD", vals, &[], cols)?;
            time_fn::add_sub::timestamp_add(&vals, cols)
        }
        // `GET_FORMAT(<type>, location)` — the type is an AST selector (the
        // parser already collapsed `TIMESTAMP` into `Datetime`), so only the
        // location is evaluated; a NULL location yields NULL. Port of
        // `builtinGetFormatSig.evalString` + `getFormat`.
        Expr::GetFormat { selector, expr } => {
            let location = eval_in(expr, cols)?;
            let format_type = match selector {
                GetFormatSelector::Date => "DATE",
                GetFormatSelector::Time => "TIME",
                GetFormatSelector::Datetime => "DATETIME",
            };
            time_fn::get_format_ast_in(format_type, &location, cols)
        }
        // `CAST`/`CONVERT(expr, type)` share one evaluator (see
        // `tidb_ast::Expr::Cast`'s own doc for why they share one AST node);
        // `NULL` maps to `NULL` for every target type, so it's handled once
        // here rather than in each of `cast::eval_cast`'s own arms.
        //
        // The three ODBC-style typed-literal styles (`DATE`/`TIME`/
        // `TIMESTAMP 'literal'`) are checked FIRST and always
        // `Unsupported`, deliberately NOT falling through to
        // `cast::eval_cast` — confirmed via `goeval`/`gorun` that real
        // TiDB's own evaluation for these genuinely diverges from
        // `CAST(... AS DATE)`'s own (never-fails: a warning plus `NULL`,
        // or the value itself when the statement's flags admit it)
        // behavior: an invalid date string is a hard query ERROR for the
        // typed-literal form (`SELECT DATE '2007-10-00'` fails outright)
        // where the cast of that same text answers `2007-10-00` under
        // every mode — reusing `cast::eval_cast` here would silently
        // produce the WRONG value for exactly the invalid-date inputs
        // this syntax is most often used to test, not just an incomplete
        // one. See `tidb_ast::CastStyle::DateLiteral`'s own doc.
        Expr::Cast(cast)
            if matches!(
                cast.style,
                CastStyle::DateLiteral | CastStyle::TimeLiteral | CastStyle::TimestampLiteral
            ) =>
        {
            Err(EvalError::Unsupported("date/time/timestamp literal"))
        }
        // `AS type ARRAY` (a JSON multi-valued-index type modifier — see
        // `tidb_ast::CastExpr::array`'s own doc) is ALWAYS `Unsupported`,
        // unconditionally — this crate has no JSON value domain at all,
        // the SAME boundary `CastType::Json` already has. Covers
        // `CastStyle::JsonSumCrc32` too, which always sets `array: true`.
        Expr::Cast(cast) if cast.array => Err(EvalError::Unsupported("ARRAY cast type")),
        Expr::Cast(cast) => match eval_in(&cast.expr, cols)? {
            Datum::Null => Ok(Datum::Null),
            // The AST tier carries no static types, so no source type is
            // available; every `CAST` it evaluates routes by datum kind.
            v => cast::eval_cast(&cast.cast_type, v, None, cols),
        },
        // `CONVERT(expr USING charset)` evaluates through ETString, then
        // transcodes according to the argument's declared charset. Literal
        // introducers are part of that declared type: a plain hex/bit literal
        // is binary, while `_utf8 0x...` is UTF-8. Preserve that distinction
        // here instead of reconstructing every value as a UTF-8 string.
        Expr::ConvertUsing { expr, charset } => match eval_in(expr, cols)? {
            Datum::Null => Ok(Datum::Null),
            value => {
                let (string_value, source) = if let Some(source_charset) = literal_charset(expr) {
                    let source_charset = tidb_datatype::Charset::from_name(source_charset)
                        .ok_or(EvalError::Unsupported("unknown character introducer"))?;
                    let collation = source_charset.default_collation();
                    let mut source =
                        tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::VarString);
                    source.set_charset_name(source_charset.name());
                    source.set_collation_name(collation.name());
                    (
                        Datum::new_collation_string(value.go_bytes().to_vec(), collation),
                        source,
                    )
                } else {
                    let text = value
                        .sql_string()
                        .map_err(|_| EvalError::Unsupported("invalid UTF-8 string coercion"))?;
                    let collation = value
                        .collation()
                        .unwrap_or(tidb_datatype::Collation::DEFAULT);
                    (
                        Datum::new_collation_string(text.into_bytes(), collation),
                        tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::VarString)
                            .with_collation(collation),
                    )
                };
                convert_charset::convert_using(
                    &string_value,
                    &source,
                    &charset.to_ascii_lowercase(),
                )
            }
        },
        // `COLLATE` doesn't change the value at all (unlike `CONVERT ...
        // USING`, which stringifies) — it only affects comparison/sort
        // behavior, not modelled here (see `tidb_ast::Expr::Collate`'s own
        // doc). A NON-string operand is a genuine error in real TiDB
        // (confirmed via `goeval`: `1 COLLATE utf8mb4_bin` errors), but
        // that type restriction is a KNOWN, deliberately unmodelled
        // boundary here — every real-world use found in the corpus that
        // surfaced this feature collates a string, so this stays a plain
        // passthrough rather than adding a check nothing exercises.
        Expr::Collate { expr, .. } => eval_in(expr, cols),
        Expr::In { expr, list, not } => eval_in_list(expr, list, *not, cols),
        Expr::Between {
            expr,
            low,
            high,
            not,
        } => {
            // `x BETWEEN lo AND hi` is `x >= lo AND x <= hi`, in three-valued
            // logic; `NOT BETWEEN` negates the result (NULL stays NULL).
            let v = eval_in(expr, cols)?;
            let ge = crate::ops::eval_binary_in(
                tidb_ast::BinaryOp::Ge,
                v.clone(),
                eval_in(low, cols)?,
                cols,
            )?;
            let le =
                crate::ops::eval_binary_in(tidb_ast::BinaryOp::Le, v, eval_in(high, cols)?, cols)?;
            negate_if(logic_and(ge, le, cols)?, *not, cols)
        }
        Expr::Is { expr, target, not } => {
            // IS is always TRUE/FALSE (never NULL): it tests a definite property.
            let v = eval_in(expr, cols)?;
            let ready = match target {
                IsTarget::Null | IsTarget::Unknown => (!v.is_null()).then_some(false),
                IsTarget::True | IsTarget::False => truthy_of(&v)?,
            };
            let function = match (target, *not) {
                (IsTarget::Null | IsTarget::Unknown, false) => BooleanFunction::IsNull,
                (IsTarget::Null | IsTarget::Unknown, true) => BooleanFunction::IsNotNull,
                (IsTarget::True, false) => BooleanFunction::IsTrue,
                (IsTarget::True, true) => BooleanFunction::IsNotTrue,
                (IsTarget::False, false) => BooleanFunction::IsFalse,
                (IsTarget::False, true) => BooleanFunction::IsNotFalse,
            };
            eval_boolean_ready_in(function, ready, cols)
        }
        Expr::Like {
            expr,
            pattern,
            not,
            ilike,
            escape,
        } => {
            // Case-sensitive (utf8mb4_bin) LIKE; either NULL operand yields
            // NULL. A non-string operand (on EITHER side, confirmed via
            // `gorun`: `'2' LIKE 2` is TRUE) is implicitly stringified the
            // SAME way `Datum::sql_string` already renders it — including
            // a `DECIMAL`'s declared scale, confirmed via `gorun`: `12.50
            // LIKE '12.5'` is FALSE but `12.50 LIKE '12.50'` is TRUE,
            // matching how `Decimal`'s own `Display` already keeps
            // trailing zeros rather than simplifying them away. `escape`
            // passes straight through to the shared worker — see
            // `tidb_ast::Expr::Like::escape`'s own doc for its exact
            // `None`/`Some(0)`/`Some(byte)` meaning, confirmed via
            // `gorun` for a custom single-byte escape character.
            let arguments = (eval_in(expr, cols)?, eval_in(pattern, cols)?);
            let value = like::evaluate_like_in(*ilike, cols, || {
                let (v, p) = match &arguments {
                    (Datum::Null, _) | (_, Datum::Null) => {
                        return Ok(tikv::EvaluatedArgs::NullWitness(None));
                    }
                    (v, p) => (v, p),
                };
                let text = v
                    .sql_bytes()
                    .map_err(|_| EvalError::Unsupported("invalid LIKE operand scalar domain"))?;
                let pattern = p
                    .sql_bytes()
                    .map_err(|_| EvalError::Unsupported("invalid LIKE pattern scalar domain"))?;
                let collation = tidb_datatype::Collation::Utf8Mb4Bin.native_policy();
                let invocation = if *ilike {
                    tidb_query_expr::NativeLikeInvocation::ilike(collation, None)
                } else {
                    tidb_query_expr::NativeLikeInvocation::like(collation, None)
                };
                Ok(tikv::EvaluatedArgs::Like {
                    invocation,
                    text: Some(text),
                    pattern: Some(pattern),
                    escape: Some(i64::from(escape.unwrap_or(b'\\'))),
                })
            })?;
            negate_if(value, *not, cols)
        }
        // Case-sensitive (utf8mb4_bin) `[NOT] REGEXP`/`RLIKE`, the SAME
        // NULL-propagation and non-string-operand-coercion rules
        // `Expr::Like` just above already established (confirmed via
        // `gorun`: `5 REGEXP '5'` is `TRUE`) — see `crate::regexp::
        // regexp_match`'s own doc for the empty-pattern/malformed-
        // pattern error rules.
        Expr::Regexp { expr, pattern, not } => {
            // Preserve the original left-then-right child demand, including NULL.
            let arguments = (eval_in(expr, cols)?, eval_in(pattern, cols)?);
            let value = tikv::evaluate_regexp_in(tikv::RegexpFunction::Like, cols, || {
                let (v, p) = match &arguments {
                    (Datum::Null, _) | (_, Datum::Null) => {
                        return Ok(tikv::EvaluatedArgs::NullWitness(None));
                    }
                    (v, p) => (v, p),
                };
                let text = v
                    .sql_string()
                    .map_err(|_| EvalError::Unsupported("invalid UTF-8 REGEXP operand"))?;
                let pattern = p
                    .sql_string()
                    .map_err(|_| EvalError::Unsupported("invalid UTF-8 REGEXP pattern"))?;
                Ok(tikv::EvaluatedArgs::RegexpLike {
                    invocation: tidb_query_expr::NativeRegexpInvocation::new(
                        &builtin_ext::BuiltinFuncCache::default(),
                        &builtin_ext::BuiltinFuncCache::default(),
                        0,
                        false,
                        false,
                    ),
                    text: text.into_bytes(),
                    pattern: pattern.into_bytes(),
                    match_type: regexp::regexp_match_type_with_collation(
                        "",
                        ops::DERIVATION_FREE_COLLATION,
                    )
                    .into_bytes(),
                })
            })?;
            negate_if(value, *not, cols)
        }
        // Go `weightStringFunctionClass`: a NUMERIC argument builds
        // `builtinWeightStringNullSig`, which is always NULL. Go reads that
        // off the argument's FieldType while BUILDING; this tier has only the
        // evaluated value, whose kind is the same fact for every argument a
        // constant expression can produce. The collation likewise comes from
        // the VALUE here (`Datum::String` carries one) rather than from a
        // static type -- the chunk tier reads the real derived one.
        Expr::WeightString { expr, as_type } => {
            let value = eval_in(expr, cols)?;
            let numeric_type = match &value {
                Datum::Int(_) | Datum::UInt(_) => Some(tidb_datatype::FieldTypeCode::LongLong),
                Datum::Real(_) => Some(tidb_datatype::FieldTypeCode::Double),
                Datum::Float32(_) => Some(tidb_datatype::FieldTypeCode::Float),
                Datum::Decimal(_) => Some(tidb_datatype::FieldTypeCode::NewDecimal),
                _ => None,
            };
            if let Some(code) = numeric_type {
                return string_packet::weight_string_numeric_type(code, cols);
            }
            let collation = match &value {
                Datum::String(text) => text.collation(),
                Datum::Bytes(_) => tidb_datatype::Collation::Binary,
                _ => crate::ops::DERIVATION_FREE_COLLATION,
            };
            string_packet::weight_string(
                &value,
                as_type.map(|(kind, length)| {
                    (
                        kind == tidb_ast::WeightStringType::Binary,
                        i64::try_from(length).unwrap_or(i64::MAX),
                    )
                }),
                collation,
                cols,
            )
        }
        Expr::Position { substr, str } => position_in(
            coerce_str(&eval_in(substr, cols)?)?,
            coerce_str(&eval_in(str, cols)?)?,
            cols,
        ),
        Expr::Trim {
            expr,
            remstr,
            direction,
        } => {
            // A bare `TRIM(expr)` (no `remstr`, no `direction`) defaults
            // to stripping spaces from BOTH ends — see
            // `tidb_ast::Expr::Trim::remstr`'s own doc for why every
            // OTHER combination already has a real `remstr` (a `NULL`
            // remstr's own explicit `NULL` restores un-omitted, so it
            // still reaches here as a real evaluated expression, not a
            // magic `None`).
            let str_value = eval_in(expr, cols)?;
            let binary = matches!(str_value, Datum::Bytes(_));
            let str = coerce_str_bytes(&str_value)?;
            let remstr = match remstr {
                Some(r) => coerce_str_bytes(&eval_in(r, cols)?)?,
                None => Some(b" ".to_vec()),
            };
            trim_value_in(
                str,
                remstr,
                direction.unwrap_or(tidb_ast::TrimDirection::Both),
                binary,
                cols,
            )
        }
        Expr::Case {
            value,
            when_clauses,
            else_clause,
        } => {
            // LAZY: only the WHEN conditions up to (and including) the
            // first match, plus that one branch's own result, are ever
            // evaluated — matching real MySQL's short-circuit CASE, a
            // load-bearing idiom for guarding against errors (confirmed
            // via `gorun`: `CASE WHEN x != 0 THEN 1/x ELSE NULL END`
            // never raises division-by-zero for `x = 0`). Real MySQL
            // additionally infers CASE's overall result type from EVERY
            // branch statically (even ones never evaluated — confirmed
            // via `gorun`: the type promotes even when the promoting
            // branch is an unreached `1/0`), which cannot be replicated
            // without a genuine type-inference pass; deliberately NOT
            // attempted here — the result is simply whichever branch was
            // taken, in its own natural type, matching the common case
            // where every branch already shares one type.
            let taken = match value {
                // Simple form: `value = cond`, ordinary `=` (not `<=>`) —
                // a NULL `value` or `cond` never matches, matching `=`'s
                // own propagation (confirmed via `goeval`: `CASE NULL
                // WHEN NULL THEN 1 ELSE 2 END` is `2`, not `1`).
                Some(value_expr) => {
                    let v = eval_in(value_expr, cols)?;
                    let mut taken = None;
                    for (cond, result) in when_clauses {
                        let w = eval_in(cond, cols)?;
                        if crate::ops::eval_binary_in(tidb_ast::BinaryOp::Eq, v.clone(), w, cols)?
                            == Datum::Int(1)
                        {
                            taken = Some(result);
                            break;
                        }
                    }
                    taken
                }
                // Searched form: each `cond` is truthiness-tested
                // directly, the same three-valued logic `IF`/`WHERE`
                // already use.
                None => {
                    let mut taken = None;
                    for (cond, result) in when_clauses {
                        if truthy_of(&eval_in(cond, cols)?)? == Some(true) {
                            taken = Some(result);
                            break;
                        }
                    }
                    taken
                }
            };
            match taken {
                Some(result) => eval_in(result, cols),
                None => match else_clause {
                    Some(e) => eval_in(e, cols),
                    None => Ok(Datum::Null),
                },
            }
        }
        _ => Err(EvalError::Unsupported("unsupported expression")),
    }
}

/// Returns the parser-owned charset of a literal string value.
///
/// Go's `DefaultTypeForValue` makes bare hex/bit literals binary strings;
/// `parseCharsetIntroducer` overrides that type without changing the datum.
/// Parentheses preserve the type. Other expression types need resolver-owned
/// field metadata and therefore deliberately fall back to their evaluated
/// datum above.
fn literal_charset(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Hex(_) | Expr::Bit(_) => Some("binary"),
        Expr::String(_) => Some("utf8mb4"),
        Expr::CharsetString { charset, .. } | Expr::CharsetBinary { charset, .. } => Some(charset),
        Expr::Paren(inner) => literal_charset(inner),
        _ => None,
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod null_safe_composition_tests {
    use super::*;
    use crate::ops::{eval_binary_full, Operands};
    use std::cell::RefCell;
    use tidb_ast::BinaryOp;
    use tidb_datatype::{Collation, FieldType, FieldTypeCode, MySqlDuration, VectorFloat32};

    #[derive(Default)]
    struct Warnings(RefCell<Vec<(u16, String)>>);

    impl Columns for Warnings {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }
        fn append_warning(&self, code: u16, message: &str) {
            self.0.borrow_mut().push((code, message.to_owned()));
        }
    }

    fn owner(slots: usize) -> AsciiPoolOwner {
        AsciiPoolOwner::new(
            AsciiPoolPolicy::checked(slots, slots, 16 << 20, 1 << 20, 2 << 20, 64, 8, 1 << 16)
                .unwrap(),
        )
        .unwrap()
    }

    fn assert_admission(result: Result<Datum, EvalError>, expected: Datum, slots: usize) {
        if slots == 0 {
            assert!(
                matches!(result, Err(EvalError::ExpressionAdapterFailure(failure))
                if failure.class() == ExpressionAdapterFailureClass::PoolResource)
            );
        } else {
            assert_eq!(result, Ok(expected));
        }
    }

    #[test]
    fn null_safe_composition_actual_presence_domains_and_sentinel_precedence() {
        let decimal = |text| Datum::Decimal(Decimal::parse_mysql(text).0);
        let vector = |values| Datum::new_vector_float32(VectorFloat32::must_create(values));
        let json = |text| Datum::Json(tidb_datatype::BinaryJSON::parse(text).unwrap());
        let cases = vec![
            (Datum::Null, Datum::Null, Collation::Utf8Mb4Bin, 1),
            (Datum::Null, Datum::Int(0), Collation::Utf8Mb4Bin, 0),
            (Datum::Int(0), Datum::Null, Collation::Utf8Mb4Bin, 0),
            // Presence-only input must not be truth-coerced or kind-rejected.
            (
                Datum::Raw(vec![0xff]),
                Datum::Null,
                Collation::Utf8Mb4Bin,
                0,
            ),
            (
                Datum::Null,
                Datum::Raw(vec![0xff]),
                Collation::Utf8Mb4Bin,
                0,
            ),
            (
                Datum::Null,
                Datum::new_string("not-json"),
                Collation::Utf8Mb4Bin,
                0,
            ),
            (
                Datum::Int(-1),
                Datum::UInt(u64::MAX),
                Collation::Utf8Mb4Bin,
                0,
            ),
            (
                Datum::UInt(u64::MAX),
                Datum::UInt(u64::MAX),
                Collation::Utf8Mb4Bin,
                1,
            ),
            (
                Datum::Real(f64::NAN),
                Datum::Real(f64::NAN),
                Collation::Utf8Mb4Bin,
                0,
            ),
            (
                Datum::Real(-0.0),
                Datum::Real(0.0),
                Collation::Utf8Mb4Bin,
                1,
            ),
            (decimal("1.500"), decimal("1.50"), Collation::Utf8Mb4Bin, 1),
            (
                Datum::new_string("a "),
                Datum::new_string("a"),
                Collation::Utf8Mb4Bin,
                1,
            ),
            (
                Datum::new_bytes(b"a ".to_vec()),
                Datum::new_bytes(b"a".to_vec()),
                Collation::Binary,
                0,
            ),
            (
                json("{\"x\":1}"),
                Datum::new_string("{\"x\":1}"),
                Collation::Utf8Mb4Bin,
                1,
            ),
            (
                vector(vec![1.0, 2.0]),
                vector(vec![1.0, 3.0]),
                Collation::Utf8Mb4Bin,
                0,
            ),
        ];
        for slots in [0, 1] {
            let pool = owner(slots);
            let execution = pool.begin_execution().unwrap();
            let scope = execution.scope();
            let warnings = Warnings::default();
            scope.with_columns(&warnings, |ctx| {
                for (left, right, collation, expected) in &cases {
                    assert_admission(
                        eval_binary_full(
                            BinaryOp::NullEq,
                            left.clone(),
                            right.clone(),
                            4,
                            *collation,
                            Operands::LITERALS,
                            ctx,
                        ),
                        Datum::Int(*expected),
                        slots,
                    );
                }
                for sentinel in [Datum::MinNotNull, Datum::MaxValue] {
                    for (left, right) in [(sentinel.clone(), Datum::Null), (Datum::Null, sentinel)]
                    {
                        assert_eq!(
                            eval_binary_full(
                                BinaryOp::NullEq,
                                left,
                                right,
                                4,
                                Collation::Utf8Mb4Bin,
                                Operands::LITERALS,
                                ctx
                            ),
                            Err(EvalError::Unsupported("range sentinel expression operand")),
                        );
                    }
                }
            });
            assert!(warnings.0.borrow().is_empty());
            drop(scope);
            execution.close();
        }
    }

    #[test]
    fn null_safe_composition_preserves_duration_false_and_time_null() {
        use crate::column::Column;
        use crate::constant::Constant;
        use crate::expression::Expression;
        let duration_column = Expression::Column(Column::new(
            1,
            FieldType::new(FieldTypeCode::Duration).with_decimal(0),
        ));
        let duration =
            Datum::new_duration(MySqlDuration::from_nanoseconds(3_600_000_000_000, 0).unwrap());
        let time = Datum::new_time(
            tidb_datatype::parse_datetime("2026-08-14 12:00:00", &chrono_tz::UTC, true, false)
                .unwrap()
                .time,
        );
        for slots in [0, 1] {
            let pool = owner(slots);
            let execution = pool.begin_execution().unwrap();
            let scope = execution.scope();
            let warnings = Warnings::default();
            scope.with_columns(&warnings, |ctx| {
                for (text, expected, warning) in [("bad", 0, true), ("1:00:00", 1, false)] {
                    let text_value = Datum::new_string(text);
                    let constant = Expression::Constant(Constant::new(
                        text_value.clone(),
                        FieldType::new(FieldTypeCode::VarString),
                    ));
                    for reversed in [false, true] {
                        warnings.0.borrow_mut().clear();
                        let (left, right, operands) = if reversed {
                            (
                                text_value.clone(),
                                duration.clone(),
                                Operands::of(&constant, &duration_column),
                            )
                        } else {
                            (
                                duration.clone(),
                                text_value.clone(),
                                Operands::of(&duration_column, &constant),
                            )
                        };
                        assert_admission(
                            eval_binary_full(
                                BinaryOp::NullEq,
                                left,
                                right,
                                4,
                                Collation::Utf8Mb4Bin,
                                operands,
                                ctx,
                            ),
                            Datum::Int(expected),
                            slots,
                        );
                        let expected_warnings = if warning {
                            vec![(1292, "Incorrect time value: 'bad'".to_owned())]
                        } else {
                            vec![]
                        };
                        assert_eq!(*warnings.0.borrow(), expected_warnings);
                    }
                }
                warnings.0.borrow_mut().clear();
                // Without the duration-column/constant gate, formatted strings
                // still compare directly: no parse and no warning.
                assert_admission(
                    eval_binary_full(
                        BinaryOp::NullEq,
                        duration.clone(),
                        Datum::new_string("1:00:00"),
                        4,
                        Collation::Utf8Mb4Bin,
                        Operands::LITERALS,
                        ctx,
                    ),
                    Datum::Int(0),
                    slots,
                );
                assert!(warnings.0.borrow().is_empty());
                assert_admission(
                    eval_binary_full(
                        BinaryOp::NullEq,
                        time.clone(),
                        Datum::new_string("bad"),
                        4,
                        Collation::Utf8Mb4Bin,
                        Operands::LITERALS,
                        ctx,
                    ),
                    Datum::Null,
                    slots,
                );
                assert_eq!(
                    *warnings.0.borrow(),
                    vec![(1292, "Incorrect datetime value: 'bad'".to_owned())]
                );
            });
            drop(scope);
            execution.close();
        }
    }
}

#[cfg(test)]
mod between_composition_tests {
    use super::*;
    use crate::rewriter::{rewrite_expr_resolved, ColumnResolver};
    use std::cell::RefCell;
    use tidb_ast::{QueryStmt, SelectField, Stmt};
    use tidb_datatype::{FieldType, FieldTypeCode, FieldTypeFlags, MySqlDuration};

    struct Inputs {
        values: [Datum; 3],
        types: [FieldType; 3],
        assignments: RefCell<Vec<String>>,
    }

    impl Inputs {
        fn new(values: [Datum; 3], field: FieldType) -> Self {
            Self {
                values,
                types: [field.clone(), field.clone(), field],
                assignments: RefCell::new(Vec::new()),
            }
        }

        fn index(name: &str) -> Option<usize> {
            ["v", "l", "h"]
                .iter()
                .position(|candidate| *candidate == name)
        }

        fn assert_assignments(&self, expected: &[&str]) {
            let actual = self.assignments.borrow();
            assert_eq!(
                actual.iter().map(String::as_str).collect::<Vec<_>>(),
                expected
            );
        }
    }

    impl Columns for Inputs {
        fn get(&self, path: &[String]) -> Option<Datum> {
            Some(self.values[Self::index(path.last()?)?].clone())
        }

        fn set_uservar(&self, name: &str, _: Datum) {
            self.assignments.borrow_mut().push(name.to_owned());
        }

        fn truncate_level(&self) -> ErrorLevel {
            ErrorLevel::Error
        }
    }

    impl ColumnResolver for Inputs {
        fn resolve(&self, path: &[String]) -> Option<(usize, FieldType, i64)> {
            let index = Self::index(path.last()?)?;
            Some((index, self.types[index].clone(), index as i64 + 1))
        }

        fn time_zone(&self) -> tidb_datatype::SessionTimeZone {
            Columns::time_zone(self)
        }
    }

    fn parsed(sql: &str) -> Expr {
        let Stmt::Query(query) = tidb_parser::parse(&format!("select {sql}")).unwrap() else {
            panic!("expected query");
        };
        let QueryStmt::Select(select) = query.into_inner() else {
            panic!("expected select");
        };
        let SelectField::Expr { expr, .. } = &select.fields[0] else {
            panic!("expected expression");
        };
        expr.clone()
    }

    fn pool_owner(slots: usize) -> AsciiPoolOwner {
        // Explicit test ledger allowances, not physical allocation bounds.
        AsciiPoolOwner::new(
            AsciiPoolPolicy::checked(slots, slots, 16 << 20, 1 << 20, 2 << 20, 64, 8, 1 << 16)
                .unwrap(),
        )
        .unwrap()
    }

    #[test]
    fn between_composition_preserves_ast_and_rewritten_demand_and_nan() {
        let owner = pool_owner(1);
        let execution = owner.begin_execution().unwrap();
        let scope = execution.scope();
        // Fixed answers, not one frontend used as the other's oracle. SETVAR
        // witnesses real child demand; assigning NULL intentionally emits no write.
        for (values, code, not, ast_answer, rewritten_answer, ast_writes, rewritten_writes) in [
            (
                [Datum::Int(5), Datum::Int(6), Datum::Int(10)],
                FieldTypeCode::LongLong,
                false,
                Datum::Int(0),
                Datum::Int(0),
                vec!["v", "l", "h"],
                vec!["v", "l"],
            ),
            (
                [Datum::Int(5), Datum::Int(1), Datum::Int(10)],
                FieldTypeCode::LongLong,
                false,
                Datum::Int(1),
                Datum::Int(1),
                vec!["v", "l", "h"],
                vec!["v", "l", "v", "h"],
            ),
            (
                [Datum::Int(5), Datum::Null, Datum::Int(10)],
                FieldTypeCode::LongLong,
                false,
                Datum::Null,
                Datum::Null,
                vec!["v", "h"],
                vec!["v", "v", "h"],
            ),
            (
                [Datum::Int(5), Datum::Null, Datum::Int(4)],
                FieldTypeCode::LongLong,
                false,
                Datum::Int(0),
                Datum::Int(0),
                vec!["v", "h"],
                vec!["v", "v", "h"],
            ),
            (
                [Datum::Int(5), Datum::Int(6), Datum::Int(10)],
                FieldTypeCode::LongLong,
                true,
                Datum::Int(1),
                Datum::Int(1),
                vec!["v", "l", "h"],
                vec!["v", "l"],
            ),
            (
                [Datum::Int(5), Datum::Int(1), Datum::Int(10)],
                FieldTypeCode::LongLong,
                true,
                Datum::Int(0),
                Datum::Int(0),
                vec!["v", "l", "h"],
                vec!["v", "l", "v", "h"],
            ),
            (
                [Datum::Real(f64::NAN), Datum::Real(1.0), Datum::Real(10.0)],
                FieldTypeCode::Double,
                false,
                Datum::Int(0),
                Datum::Int(0),
                vec!["v", "l", "h"],
                vec!["v", "l"],
            ),
            // Preserve NOT(Ge AND Le) versus (Lt OR Gt), including their
            // pre-existing IEEE unordered difference. Do not normalize the AST.
            (
                [Datum::Real(f64::NAN), Datum::Real(1.0), Datum::Real(10.0)],
                FieldTypeCode::Double,
                true,
                Datum::Int(1),
                Datum::Int(0),
                vec!["v", "l", "h"],
                vec!["v", "l", "v", "h"],
            ),
        ] {
            let input = Inputs::new(values, FieldType::new(code));
            let ast = parsed(&format!(
                "(@v := v) {}between (@l := l) and (@h := h)",
                if not { "not " } else { "" }
            ));
            let rewritten = rewrite_expr_resolved(&ast, &input).unwrap();
            let row = tidb_chunk::mutrow::MutRow::from_datums(&input.values);
            input.assignments.borrow_mut().clear();
            scope.with_columns(&input, |ctx| {
                assert_eq!(eval_in(&ast, ctx).unwrap(), ast_answer);
                input.assert_assignments(&ast_writes);
                input.assignments.borrow_mut().clear();
                assert_eq!(rewritten.eval(ctx, row.to_row()).unwrap(), rewritten_answer);
                input.assert_assignments(&rewritten_writes);
            });
        }

        // A false lower comparison suppresses an upper conversion error only
        // in the rewritten tree; NULL does not short-circuit either frontend.
        for low in [Datum::Int(6), Datum::Null] {
            let null_low = low.is_null();
            let mut input = Inputs::new(
                [Datum::Int(5), low, Datum::new_string("bad")],
                FieldType::new(FieldTypeCode::LongLong),
            );
            input.types[2] = FieldType::new(FieldTypeCode::VarString);
            let ast = parsed("(@v := v) between (@l := l) and cast((@h := h) as signed)");
            let rewritten = rewrite_expr_resolved(&ast, &input).unwrap();
            let row = tidb_chunk::mutrow::MutRow::from_datums(&input.values);
            input.assignments.borrow_mut().clear();
            scope.with_columns(&input, |ctx| {
                assert!(matches!(
                    eval_in(&ast, ctx),
                    Err(EvalError::TruncatedWrongValue(_))
                ));
                input.assert_assignments(if null_low {
                    &["v", "h"]
                } else {
                    &["v", "l", "h"]
                });
                input.assignments.borrow_mut().clear();
                let result = rewritten.eval(ctx, row.to_row());
                if null_low {
                    assert!(matches!(result, Err(EvalError::TruncatedWrongValue(_))));
                    input.assert_assignments(&["v", "v", "h"]);
                } else {
                    assert_eq!(result, Ok(Datum::Int(0)));
                    input.assert_assignments(&["v", "l"]);
                }
            });
        }
        drop(scope);
        execution.close();
    }

    #[test]
    fn between_composition_domains_and_direct_column_scope_admission() {
        let integer = FieldType::new(FieldTypeCode::LongLong);
        let mut unsigned = integer.clone();
        unsigned.add_flags(FieldTypeFlags::UNSIGNED);
        let mut decimal = FieldType::new(FieldTypeCode::NewDecimal);
        decimal.set_flen(12);
        decimal.set_decimal(2);
        let mut padded = FieldType::new(FieldTypeCode::VarString);
        padded.set_charset_name("utf8mb4".to_owned());
        padded.set_collation_name("utf8mb4_bin");
        let mut binary = FieldType::new(FieldTypeCode::VarString);
        binary.set_charset_name("binary".to_owned());
        binary.set_collation_name("binary");
        let dec = |text| Datum::Decimal(Decimal::parse_mysql(text).0);
        let duration = |seconds: i64| {
            Datum::new_duration(
                MySqlDuration::from_nanoseconds(seconds * 1_000_000_000, 0).unwrap(),
            )
        };
        let time = |text| {
            Datum::new_time(
                tidb_datatype::parse_datetime(text, &chrono_tz::UTC, true, false)
                    .unwrap()
                    .time,
            )
        };
        let owner = pool_owner(1);
        let execution = owner.begin_execution().unwrap();
        let scope = execution.scope();
        for (field, values, ast_answer, rewritten_answer) in [
            (
                integer.clone(),
                [Datum::Int(5), Datum::Int(1), Datum::Int(10)],
                1,
                1,
            ),
            (
                unsigned,
                [
                    Datum::UInt(u64::MAX),
                    Datum::UInt(i64::MAX as u64),
                    Datum::UInt(u64::MAX),
                ],
                1,
                1,
            ),
            (
                FieldType::new(FieldTypeCode::Double),
                [Datum::Real(1.5), Datum::Real(1.0), Datum::Real(2.0)],
                1,
                1,
            ),
            (decimal, [dec("1.50"), dec("1.40"), dec("1.60")], 1, 1),
            (
                padded,
                [
                    Datum::new_string("a "),
                    Datum::new_string("a"),
                    Datum::new_string("a"),
                ],
                1,
                1,
            ),
            // The AST keeps its derivation-free PAD collation; rewritten
            // columns carry binary NO PAD. Both fixed policies remain intact.
            (
                binary,
                [
                    Datum::new_bytes(b"a ".to_vec()),
                    Datum::new_bytes(b"a".to_vec()),
                    Datum::new_bytes(b"a".to_vec()),
                ],
                1,
                0,
            ),
            (
                // Chunk duration cells store only nanoseconds; reads stamp
                // the declared FSP, which must match these FSP-0 fixtures.
                FieldType::new(FieldTypeCode::Duration).with_decimal(0),
                [duration(2), duration(1), duration(3)],
                1,
                1,
            ),
            (
                FieldType::new(FieldTypeCode::Datetime).with_decimal(0),
                [
                    time("2026-08-14 12:00:00"),
                    time("2026-08-14 11:00:00"),
                    time("2026-08-14 13:00:00"),
                ],
                1,
                1,
            ),
        ] {
            let input = Inputs::new(values, field);
            for (sql, expected_ast, expected_rewritten) in [
                ("v between l and h", ast_answer, rewritten_answer),
                (
                    "v not between l and h",
                    1 - ast_answer,
                    1 - rewritten_answer,
                ),
            ] {
                let ast = parsed(sql);
                let rewritten = rewrite_expr_resolved(&ast, &input).unwrap();
                let row = tidb_chunk::mutrow::MutRow::from_datums(&input.values);
                scope.with_columns(&input, |ctx| {
                    assert_eq!(eval_in(&ast, ctx), Ok(Datum::Int(expected_ast)), "{sql}");
                    assert_eq!(
                        rewritten.eval(ctx, row.to_row()),
                        Ok(Datum::Int(expected_rewritten)),
                        "{sql}"
                    );
                });
            }
        }
        drop(scope);
        execution.close();

        // Fresh zero-slot root, plain typed integer columns only: no CAST,
        // assignment, logical sibling or unrelated worker can refuse first.
        let denied_owner = pool_owner(0);
        let denied_execution = denied_owner.begin_execution().unwrap();
        let denied_scope = denied_execution.scope();
        for value in [Datum::Int(5), Datum::Null] {
            let input = Inputs::new([value, Datum::Int(1), Datum::Int(10)], integer.clone());
            let row = tidb_chunk::mutrow::MutRow::from_datums(&input.values);
            for sql in ["v between l and h", "v not between l and h"] {
                let ast = parsed(sql);
                let rewritten = rewrite_expr_resolved(&ast, &input).unwrap();
                denied_scope.with_columns(&input, |ctx| {
                    for result in [eval_in(&ast, ctx), rewritten.eval(ctx, row.to_row())] {
                        assert!(
                            matches!(result, Err(EvalError::ExpressionAdapterFailure(failure))
                            if failure.class() == ExpressionAdapterFailureClass::PoolResource)
                        );
                    }
                });
            }
        }
        drop(denied_scope);
        denied_execution.close();
    }
}
