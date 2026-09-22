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
//! type), plus non-math builtins such as `LEAST`, `GREATEST`, `COALESCE`,
//! `IF`, `IFNULL` and `NULLIF`, and string functions (`CONCAT`, `LENGTH`,
//! `CHAR_LENGTH`, `UPPER`, `LOWER`, `LEFT`,
//! `RIGHT`, `SUBSTRING`). The former native math-kernel module was physically
//! removed. With the TiKV adapter enabled, retained math families (`ABS`,
//! `SIGN`, `SQRT`, `EXP`, `LN`, `LOG`, `LOG2`, `LOG10`, `PI`, `CRC32`) execute
//! in TiKV. Unverified families (`CONV`, `POW`/`POWER`, `ROUND`/`TRUNCATE`,
//! `CEIL`/`CEILING`/`FLOOR`, `RAND`, and trigonometric functions) are explicit
//! `Unsupported` contractions rather than native fallbacks. The residual AST
//! and scalar-function evaluators reject every removed math name; they do not
//! implement those kernels. The former native crypto/encryption module is also
//! deleted; its hashes, AES/legacy ciphers, compression, password helpers and
//! RNG are explicit `Unsupported` contractions until engine charset,
//! collation, diagnostic and session semantics are verified. The native vector
//! SQL module is deleted: `VEC_DIMS`, the distance/norm family, and
//! `VEC_AS_TEXT` execute in TiKV, while `VEC_FROM_TEXT` is explicitly
//! unsupported because the pinned engine does not dispatch its signatures.
//! Vector datatype/bridge support remains. The former native JSON depth/storage
//! leaf module is also deleted: typed-column `JSON_DEPTH` executes in TiKV,
//! while text/other depth shapes and both storage-accounting functions fail
//! closed. Both native regexp modules and the residual operator/function
//! branches are deleted; REGEXP/RLIKE and REGEXP_LIKE/SUBSTR/INSTR/REPLACE
//! execute only in TiKV. Supported builtins may still nest.
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
//! `DATE_ADD`/`DATE_SUB(date, INTERVAL amount unit)` are also covered, for
//! `DAY`, `WEEK`, `MONTH`, `QUARTER`, `YEAR`, `HOUR`, `MINUTE`, `SECOND`, and every
//! COMPOSITE unit (`YEAR_MONTH`, `DAY_HOUR`, `DAY_MINUTE`, `DAY_SECOND`,
//! `HOUR_MINUTE`, `HOUR_SECOND`, `MINUTE_SECOND`, and their
//! `*_MICROSECOND` variants — see [`time_fn::calendar::date_add`]'s own doc
//! for the composite split rules, ported from `parseTimeValue`
//! (`pkg/types/time.go`)).
//! `DAY` is exact day arithmetic via the same
//! `days_from_civil`/`civil_from_days` round-trip, so month/year rollover
//! and leap days are handled correctly for
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
//! TiDB's own two-path algorithm ([`time_fn::calendar::parse_hms_extended`],
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
//! Native SQL evaluation of the statement-clock family is physically removed:
//! `NOW`/`CURRENT_TIMESTAMP`, `LOCALTIME`/`LOCALTIMESTAMP`, the `CUR*` and
//! `UTC_*` variants, and `SYSDATE` fail closed before child evaluation. Their
//! parser and result-type contracts remain, but TiKV currently lacks the host
//! clock needed to execute them. [`Columns::now`] remains only for statement-
//! owned temporal defaults, zero-argument `UNIX_TIMESTAMP`, and datatype/cast
//! bridges; those paths must not be treated as a native SQL-function fallback.
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
//! [`date_fn`] / [`like`] / [`math_fn`] / [`cast`]
//! (builtin-function families and `CAST`/`CONVERT`), and [`func`] (the
//! builtin dispatch table + `IN` predicate) — all wired together by this
//! file's `eval_in`, the single recursive expression evaluator every other
//! module calls back into for its own subexpressions.

pub mod aggregation;
mod arg_eval_type;
mod binary_literal;
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
mod grouping;
pub mod infer_pushdown;
mod like;
pub mod metabuild;
pub mod new_function;
pub use new_function::{
    new_function, new_function_base, new_function_impl, new_function_internal,
    new_function_try_fold, new_function_with_init, scalar_funcs_to_exprs, type_infer_for_null,
    ScalarFunctionCallBack,
};
mod ops;
pub mod pb_predicate;
pub mod pushdown_catalog;
pub mod ranger_context;
pub mod rewriter;
mod row;
pub mod scalar_function;
pub mod schema;
pub mod sessionexpr;
pub mod simple_expr;
mod string_signature;
#[cfg(feature = "tikv-expr")]
pub mod tikv;
mod time_fn;
mod time_literal;
pub mod user_vars;

pub use field_name::{find_field_name, find_field_name_index_by_column, NonUniqueFieldName};

pub use coerce::truthy_of;
pub use context::{
    BlockEncryptionMode, Columns, CurrentTso, ErrorLevel, EvalError, JsonError, NoColumns,
    SequenceEvalError, SessionTimeZone, ZonedNoColumns,
};
pub use grouping::{GroupingFunction, GroupingMetadata, GroupingMetadataError, GroupingMode};
pub use like::{ilike_match, like_match_with_collation};
pub use row::{compare_datums, compare_datums_with_collation};
pub(crate) use tidb_datatype::{Datum, Decimal};
pub use tidb_util::mathutil::MysqlRng;

use tidb_ast::{CastStyle, Expr, IsTarget};

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

fn render_ast_expression(expression: &Expr) -> Option<String> {
    match expression {
        Expr::Int(value) => Some(value.clone()),
        Expr::Float(value) => Some(tidb_datatype::format_float_g_shortest(*value)),
        Expr::Decimal(value) => Some(value.clone()),
        Expr::Null => Some("NULL".to_owned()),
        Expr::Column(path) => Some(path.join(".")),
        Expr::Unary(tidb_ast::UnaryOp::Plus, expression) => {
            Some(format!("+{}", render_ast_expression(expression)?))
        }
        Expr::Unary(tidb_ast::UnaryOp::Minus, expression) => {
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

fn render_ast_binary_expression(
    operator: tidb_ast::BinaryOp,
    left: &Expr,
    right: &Expr,
) -> Option<String> {
    use tidb_ast::BinaryOp;
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
    let Some(expression) = render_ast_binary_expression(operator, left, right) else {
        return error;
    };
    EvalError::DataOutOfRange { value, expression }
}

use coerce::{bool_int, coerce_str_bytes};
use like::like_match;
use ops::{
    effective_div_precision_increment, eval_binary, eval_binary_with_div_precision, eval_unary,
    logic_and,
};
use row::row_compare;

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
            let expression = crate::rewriter::rewrite_expr(expr)?;
            let value = eval_expression_once(&expression, cols)?;
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
    use chrono::{Datelike, Timelike};

    let normalized_fsp = tidb_datatype::check_fsp(fsp)
        .map_err(|error| EvalError::TruncatedWrongValue(error.to_string()))?;
    let (seconds, nanos, _) = cols.now().ok_or(EvalError::Unsupported(
        "no statement clock for GetTimeValue",
    ))?;
    let instant = chrono::DateTime::<chrono::Utc>::from_timestamp(seconds, nanos)
        .ok_or(EvalError::Unsupported("statement clock is out of range"))?
        .with_timezone(&cols.time_zone());
    let quantum = 10_u32.pow((9 - normalized_fsp) as u32);
    let nanos = (instant.nanosecond() / quantum) * quantum;
    tidb_datatype::Time::from_date_checked(
        instant.year(),
        instant.month() as i32,
        instant.day() as i32,
        instant.hour() as i32,
        instant.minute() as i32,
        instant.second() as i32,
        (nanos / 1_000) as i32,
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
    let core = current.core_time();
    tidb_datatype::Time::from_date_checked(
        core.year(),
        core.month() as i32,
        core.day() as i32,
        0,
        0,
        0,
        0,
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
    crate::evaluator::eval_constant_row(expression, ctx).map_err(crate::evaluator::into_eval_error)
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
