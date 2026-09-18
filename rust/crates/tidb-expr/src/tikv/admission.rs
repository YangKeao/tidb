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

//! The single explicit admission table for the local TiKV adapter.
//!
//! # Why a table
//!
//! Before this module the local adapter admitted an expression when its SQL
//! name was *not* in a hand-written deny list (the old `blocked_name`) and the
//! lowering functions happened to understand its shape. A name added to the
//! rewriter therefore fell back to the native evaluator silently. Milestone B
//! replaces that with an explicit, complete allow list: [`ADMISSION_ROWS`]
//! carries one row per SQL function name, and both directions of drift fail a
//! test.
//!
//! # Authoritative name universe
//!
//! The registry-derived half of the row set is exactly
//! [`crate::builtin_registry::FUNCTION_CLASSES`], the crate's 309-entry
//! transcription of Go's `pkg/expression/builtin.go` `funcs` map literal.
//! That Go map is keyed by `ast.*` identifiers declared in
//! `pkg/parser/ast/functions.go`, so the Rust table reaches the Go parser/AST
//! authority directly rather than by grepping string literals; the pin is the
//! compile-time `include_str!` proof in
//! `crate::expression::tests::test_null_reject_builtin_registry_snapshot`.
//!
//! The second half is [`SYNTHESIZED_NAMES`]: spellings the Rust rewriter and
//! the internal cast builders mint (`cast_*`, `date_add_*`, `getvar_*`, ...)
//! which are not SQL builtin names and so are absent from the Go registry.
//!
//! # Columns
//!
//! * [`AdmissionRow::name`] -- the lower-cased `func_name`.
//! * [`AdmissionRow::decision`] -- [`Decision::Admitted`] when at least one
//!   shape of this name lowers to the engine, [`Decision::Excluded`] otherwise.
//! * [`AdmissionRow::signature`] -- the lowering site and signature family, or
//!   [`Signature::None`] for an excluded row.
//! * [`AdmissionRow::required_eval_types`] -- argument eval types the lowering
//!   site fixes before selecting a signature; empty means the family selects
//!   and coerces per shape (see `lowering.rs` and `lowering/families.rs`).
//! * [`AdmissionRow::shape`] -- the shape constraint that must hold on top of
//!   the per-argument admission, for the lazy control nodes.
//! * [`AdmissionRow::exclusion_reason`] -- non-empty exactly for excluded
//!   rows.
//!
//! # Outcome-preservation contract
//!
//! This table was derived from the pre-table behaviour: a name is `Admitted`
//! if and only if the old `admitted()` returned true for some shape *and* one
//! of `local_call`, `families::lower` or `catalog_call` could lower it. Names
//! that the old code admitted but no lowering site understood were, and
//! remain, executed natively; they are recorded here as `Excluded` so that a
//! future name cannot join them by accident.

use tidb_datatype::EvalType;

use crate::expression::Expression;
use crate::scalar_function::ScalarFunction;

/// Whether a SQL name reaches the engine.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Decision {
    /// At least one shape lowers to a wire signature.
    Admitted,
    /// Always falls back to the native evaluator.
    Excluded,
}

/// The lowering site that selects signatures for an admitted name.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Family {
    Arithmetic,
    Comparison,
    Control,
    Math,
    String,
    /// Regexp spellings routed through the string family's extended arm.
    Regexp,
    Miscellaneous,
    Temporal,
    DateArithmetic,
    Json,
    Vector,
    /// The reused pushdown catalog's type selector.
    Catalog,
    /// No lowering site; only on excluded rows.
    None,
}

/// How the wire signature is named for inventory purposes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Signature {
    /// The named [`Family`] selects among its signatures per shape.
    Family(Family),
    /// Concrete wire signature names. Reserved for rows whose family is not a
    /// single lowering site; no current row needs it, so the variant is kept
    /// for the next such function rather than forcing a family.
    #[allow(dead_code)]
    Resolved(&'static [&'static str]),
    /// Excluded: no signature.
    None,
}

/// The extra shape constraint on top of per-argument admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Shape {
    /// Every admitted child is allowed.
    Any,
    /// `if`: exactly three children and both potentially-skipped arms are leaves.
    IfThree,
    /// `ifnull`/`coalesce`/`in`/`and`/`or`: non-empty and every child after the
    /// first is a leaf.
    LazyTail,
    /// `case`/`casewhen`/`elt`/`field`/`interval`/`greatest`/`least`: every
    /// child is a leaf.
    AllLeaves,
}

impl Shape {
    /// Whether `function`'s arguments satisfy this constraint.
    ///
    /// The lazy control nodes evaluate every child in TiKV's eager RPN, so a
    /// possibly-skipped branch may only be a leaf. Leaves are columns and
    /// constants: they cannot warn, fail, or perform a fallible implicit cast.
    #[must_use]
    pub(crate) fn permits(self, function: &ScalarFunction) -> bool {
        let leaf = |argument: &Expression| {
            matches!(argument, Expression::Column(_) | Expression::Constant(_))
        };
        match self {
            Shape::Any => true,
            Shape::IfThree => function.args.len() == 3 && function.args[1..].iter().all(leaf),
            Shape::LazyTail => !function.args.is_empty() && function.args[1..].iter().all(leaf),
            Shape::AllLeaves => function.args.iter().all(leaf),
        }
    }
}

/// One admission decision.
#[derive(Debug)]
pub(crate) struct AdmissionRow {
    pub name: &'static str,
    pub decision: Decision,
    pub signature: Signature,
    /// Read by the completeness test and the inventory generator, not by the
    /// hot path (the lowering family enforces the types itself).
    #[allow(dead_code)]
    pub required_eval_types: &'static [EvalType],
    pub shape: Shape,
    /// Read by the completeness test and the inventory generator; the hot path
    /// only needs `decision`.
    #[allow(dead_code)]
    pub exclusion_reason: &'static str,
}

const fn row(
    name: &'static str,
    decision: Decision,
    signature: Signature,
    required_eval_types: &'static [EvalType],
    shape: Shape,
    exclusion_reason: &'static str,
) -> AdmissionRow {
    AdmissionRow {
        name,
        decision,
        signature,
        required_eval_types,
        shape,
        exclusion_reason,
    }
}

/// Non-registry spellings the rewriter and the internal builders mint, so the
/// completeness test can tell a known internal name from a new one.
///
/// Read by the completeness test and by `scripts/tikv_expression_coverage.py`;
/// production code only ever looks a name up in [`ADMISSION_ROWS`].
#[allow(dead_code)]
#[rustfmt::skip]
pub(crate) const SYNTHESIZED_NAMES: &[&str] = &[
    "casewhen",
    "cast",
    "cast_binary",
    "cast_char",
    "cast_date",
    "cast_datetime",
    "cast_decimal",
    "cast_decimal_in_union",
    "cast_double",
    "cast_int_to_decimal_in_union",
    "cast_json",
    "cast_real_in_union",
    "cast_real_to_decimal_in_union",
    "cast_signed",
    "cast_string_to_decimal_in_union",
    "cast_time",
    "cast_unsigned",
    "cast_unsigned_in_union",
    "cast_vector",
    "cast_year",
    "convert_using",
    "date_add_day",
    "date_add_day_hour",
    "date_add_day_microsecond",
    "date_add_day_minute",
    "date_add_day_second",
    "date_add_hour",
    "date_add_hour_microsecond",
    "date_add_hour_minute",
    "date_add_hour_second",
    "date_add_microsecond",
    "date_add_minute",
    "date_add_minute_microsecond",
    "date_add_minute_second",
    "date_add_month",
    "date_add_quarter",
    "date_add_second",
    "date_add_second_microsecond",
    "date_add_week",
    "date_add_year",
    "date_add_year_month",
    "date_sub_day",
    "date_sub_day_hour",
    "date_sub_day_microsecond",
    "date_sub_day_minute",
    "date_sub_day_second",
    "date_sub_hour",
    "date_sub_hour_microsecond",
    "date_sub_hour_minute",
    "date_sub_hour_second",
    "date_sub_microsecond",
    "date_sub_minute",
    "date_sub_minute_microsecond",
    "date_sub_minute_second",
    "date_sub_month",
    "date_sub_quarter",
    "date_sub_second",
    "date_sub_second_microsecond",
    "date_sub_week",
    "date_sub_year",
    "date_sub_year_month",
    "from_binary",
    "getvar",
    "getvar_decimal",
    "getvar_int",
    "getvar_real",
    "getvar_string",
    "getvar_time",
    "getvar_uint",
    "isfalse_with_null",
    "json_member_of",
    "nullif",
    "rlike",
    "to_binary",
    "values",
];

/// One row per SQL function name, sorted by [`AdmissionRow::name`] for binary
/// search.
#[rustfmt::skip]
pub(crate) const ADMISSION_ROWS: &[AdmissionRow] = &[
    row("'tidb`.(dateliteral", Decision::Excluded, Signature::None, &[], Shape::Any, "Go internal literal-function name, not a rewriter function spelling"),
    row("'tidb`.(timeliteral", Decision::Excluded, Signature::None, &[], Shape::Any, "Go internal literal-function name, not a rewriter function spelling"),
    row("'tidb`.(timestampliteral", Decision::Excluded, Signature::None, &[], Shape::Any, "Go internal literal-function name, not a rewriter function spelling"),
    row("abs", Decision::Admitted, Signature::Family(Family::Math), &[], Shape::Any, ""),
    row("acos", Decision::Admitted, Signature::Family(Family::Math), &[], Shape::Any, ""),
    row("adddate", Decision::Excluded, Signature::None, &[], Shape::Any, "no local engine lowering: not selected by local_call, the temporal/JSON/vector families, or the reused pushdown catalog"),
    row("addtime", Decision::Admitted, Signature::Family(Family::Temporal), &[], Shape::Any, ""),
    row("aes_decrypt", Decision::Excluded, Signature::None, &[], Shape::Any, "no local engine lowering: not selected by local_call, the temporal/JSON/vector families, or the reused pushdown catalog"),
    row("aes_encrypt", Decision::Excluded, Signature::None, &[], Shape::Any, "no local engine lowering: not selected by local_call, the temporal/JSON/vector families, or the reused pushdown catalog"),
    row("and", Decision::Admitted, Signature::Family(Family::Control), &[], Shape::LazyTail, ""),
    row("any_value", Decision::Admitted, Signature::Family(Family::Miscellaneous), &[], Shape::Any, ""),
    row("ascii", Decision::Admitted, Signature::Family(Family::String), &[], Shape::Any, ""),
    row("asin", Decision::Admitted, Signature::Family(Family::Math), &[], Shape::Any, ""),
    row("atan", Decision::Admitted, Signature::Family(Family::Math), &[], Shape::Any, ""),
    row("atan2", Decision::Admitted, Signature::Family(Family::Math), &[], Shape::Any, ""),
    row("benchmark", Decision::Excluded, Signature::None, &[], Shape::Any, "session state, statement clock, RNG, user variables, sequences, or effects are absent from the embedded engine Context"),
    row("bin", Decision::Admitted, Signature::Family(Family::String), &[], Shape::Any, ""),
    row("bin_to_uuid", Decision::Excluded, Signature::None, &[], Shape::Any, "no local engine lowering: not selected by local_call, the temporal/JSON/vector families, or the reused pushdown catalog"),
    row("bit_count", Decision::Admitted, Signature::Family(Family::Miscellaneous), &[], Shape::Any, ""),
    row("bit_length", Decision::Admitted, Signature::Family(Family::String), &[], Shape::Any, ""),
    row("bitand", Decision::Admitted, Signature::Family(Family::Arithmetic), &[], Shape::Any, ""),
    row("bitneg", Decision::Admitted, Signature::Family(Family::Arithmetic), &[], Shape::Any, ""),
    row("bitor", Decision::Admitted, Signature::Family(Family::Arithmetic), &[], Shape::Any, ""),
    row("bitxor", Decision::Admitted, Signature::Family(Family::Arithmetic), &[], Shape::Any, ""),
    row("case", Decision::Admitted, Signature::Family(Family::Control), &[], Shape::AllLeaves, ""),
    row("casewhen", Decision::Admitted, Signature::Family(Family::Control), &[], Shape::AllLeaves, ""),
    row("cast", Decision::Admitted, Signature::Family(Family::Arithmetic), &[], Shape::Any, ""),
    row("cast_binary", Decision::Excluded, Signature::None, &[], Shape::Any, "no local engine lowering: not selected by local_call, the temporal/JSON/vector families, or the reused pushdown catalog"),
    row("cast_char", Decision::Excluded, Signature::None, &[], Shape::Any, "no local engine lowering: not selected by local_call, the temporal/JSON/vector families, or the reused pushdown catalog"),
    row("cast_date", Decision::Excluded, Signature::None, &[], Shape::Any, "no local engine lowering: not selected by local_call, the temporal/JSON/vector families, or the reused pushdown catalog"),
    row("cast_datetime", Decision::Excluded, Signature::None, &[], Shape::Any, "no local engine lowering: not selected by local_call, the temporal/JSON/vector families, or the reused pushdown catalog"),
    row("cast_decimal", Decision::Admitted, Signature::Family(Family::Catalog), &[], Shape::Any, ""),
    row("cast_decimal_in_union", Decision::Excluded, Signature::None, &[], Shape::Any, "no local engine lowering: not selected by local_call, the temporal/JSON/vector families, or the reused pushdown catalog"),
    row("cast_double", Decision::Admitted, Signature::Family(Family::Catalog), &[], Shape::Any, ""),
    row("cast_int_to_decimal_in_union", Decision::Excluded, Signature::None, &[], Shape::Any, "no local engine lowering: not selected by local_call, the temporal/JSON/vector families, or the reused pushdown catalog"),
    row("cast_json", Decision::Excluded, Signature::None, &[], Shape::Any, "no local engine lowering: not selected by local_call, the temporal/JSON/vector families, or the reused pushdown catalog"),
    row("cast_real_in_union", Decision::Excluded, Signature::None, &[], Shape::Any, "no local engine lowering: not selected by local_call, the temporal/JSON/vector families, or the reused pushdown catalog"),
    row("cast_real_to_decimal_in_union", Decision::Excluded, Signature::None, &[], Shape::Any, "no local engine lowering: not selected by local_call, the temporal/JSON/vector families, or the reused pushdown catalog"),
    row("cast_signed", Decision::Excluded, Signature::None, &[], Shape::Any, "no local engine lowering: not selected by local_call, the temporal/JSON/vector families, or the reused pushdown catalog"),
    row("cast_string_to_decimal_in_union", Decision::Excluded, Signature::None, &[], Shape::Any, "no local engine lowering: not selected by local_call, the temporal/JSON/vector families, or the reused pushdown catalog"),
    row("cast_time", Decision::Excluded, Signature::None, &[], Shape::Any, "no local engine lowering: not selected by local_call, the temporal/JSON/vector families, or the reused pushdown catalog"),
    row("cast_unsigned", Decision::Excluded, Signature::None, &[], Shape::Any, "no local engine lowering: not selected by local_call, the temporal/JSON/vector families, or the reused pushdown catalog"),
    row("cast_unsigned_in_union", Decision::Excluded, Signature::None, &[], Shape::Any, "no local engine lowering: not selected by local_call, the temporal/JSON/vector families, or the reused pushdown catalog"),
    row("cast_vector", Decision::Excluded, Signature::None, &[], Shape::Any, "no local engine lowering: not selected by local_call, the temporal/JSON/vector families, or the reused pushdown catalog"),
    row("cast_year", Decision::Excluded, Signature::None, &[], Shape::Any, "no local engine lowering: not selected by local_call, the temporal/JSON/vector families, or the reused pushdown catalog"),
    row("ceil", Decision::Admitted, Signature::Family(Family::Math), &[], Shape::Any, ""),
    row("ceiling", Decision::Admitted, Signature::Family(Family::Math), &[], Shape::Any, ""),
    row("char_func", Decision::Excluded, Signature::None, &[], Shape::Any, "no local engine lowering: not selected by local_call, the temporal/JSON/vector families, or the reused pushdown catalog"),
    row("char_length", Decision::Admitted, Signature::Family(Family::String), &[], Shape::Any, ""),
    row("character_length", Decision::Admitted, Signature::Family(Family::String), &[], Shape::Any, ""),
    row("charset", Decision::Excluded, Signature::None, &[], Shape::Any, "no local engine lowering: not selected by local_call, the temporal/JSON/vector families, or the reused pushdown catalog"),
    row("coalesce", Decision::Admitted, Signature::Family(Family::Control), &[], Shape::LazyTail, ""),
    row("coercibility", Decision::Excluded, Signature::None, &[], Shape::Any, "no local engine lowering: not selected by local_call, the temporal/JSON/vector families, or the reused pushdown catalog"),
    row("collation", Decision::Excluded, Signature::None, &[], Shape::Any, "no local engine lowering: not selected by local_call, the temporal/JSON/vector families, or the reused pushdown catalog"),
    row("compress", Decision::Admitted, Signature::Family(Family::Miscellaneous), &[], Shape::Any, ""),
    row("concat", Decision::Excluded, Signature::None, &[], Shape::Any, "native enforces max_allowed_packet before allocating; the pinned engine facade has no equivalent context setting"),
    row("concat_ws", Decision::Excluded, Signature::None, &[], Shape::Any, "native enforces max_allowed_packet before allocating; the pinned engine facade has no equivalent context setting"),
    row("connection_id", Decision::Excluded, Signature::None, &[], Shape::Any, "no local engine lowering: not selected by local_call, the temporal/JSON/vector families, or the reused pushdown catalog"),
    row("conv", Decision::Admitted, Signature::Family(Family::Math), &[], Shape::Any, ""),
    row("convert", Decision::Excluded, Signature::None, &[], Shape::Any, "no local engine lowering: not selected by local_call, the temporal/JSON/vector families, or the reused pushdown catalog"),
    row("convert_tz", Decision::Excluded, Signature::None, &[], Shape::Any, "no local engine lowering: not selected by local_call, the temporal/JSON/vector families, or the reused pushdown catalog"),
    row("convert_using", Decision::Excluded, Signature::None, &[], Shape::Any, "no local engine lowering: not selected by local_call, the temporal/JSON/vector families, or the reused pushdown catalog"),
    row("cos", Decision::Admitted, Signature::Family(Family::Math), &[], Shape::Any, ""),
    row("cot", Decision::Admitted, Signature::Family(Family::Math), &[], Shape::Any, ""),
    row("crc32", Decision::Admitted, Signature::Family(Family::Math), &[], Shape::Any, ""),
    row("curdate", Decision::Excluded, Signature::None, &[], Shape::Any, "session state, statement clock, RNG, user variables, sequences, or effects are absent from the embedded engine Context"),
    row("current_date", Decision::Excluded, Signature::None, &[], Shape::Any, "session state, statement clock, RNG, user variables, sequences, or effects are absent from the embedded engine Context"),
    row("current_resource_group", Decision::Excluded, Signature::None, &[], Shape::Any, "no local engine lowering: not selected by local_call, the temporal/JSON/vector families, or the reused pushdown catalog"),
    row("current_role", Decision::Excluded, Signature::None, &[], Shape::Any, "no local engine lowering: not selected by local_call, the temporal/JSON/vector families, or the reused pushdown catalog"),
    row("current_time", Decision::Excluded, Signature::None, &[], Shape::Any, "session state, statement clock, RNG, user variables, sequences, or effects are absent from the embedded engine Context"),
    row("current_timestamp", Decision::Excluded, Signature::None, &[], Shape::Any, "session state, statement clock, RNG, user variables, sequences, or effects are absent from the embedded engine Context"),
    row("current_user", Decision::Excluded, Signature::None, &[], Shape::Any, "no local engine lowering: not selected by local_call, the temporal/JSON/vector families, or the reused pushdown catalog"),
    row("curtime", Decision::Excluded, Signature::None, &[], Shape::Any, "session state, statement clock, RNG, user variables, sequences, or effects are absent from the embedded engine Context"),
    row("database", Decision::Excluded, Signature::None, &[], Shape::Any, "no local engine lowering: not selected by local_call, the temporal/JSON/vector families, or the reused pushdown catalog"),
    row("date", Decision::Admitted, Signature::Family(Family::Temporal), &[EvalType::Datetime], Shape::Any, ""),
    row("date_add", Decision::Excluded, Signature::None, &[], Shape::Any, "no local engine lowering: not selected by local_call, the temporal/JSON/vector families, or the reused pushdown catalog"),
    row("date_add_day", Decision::Admitted, Signature::Family(Family::DateArithmetic), &[], Shape::Any, ""),
    row("date_add_day_hour", Decision::Admitted, Signature::Family(Family::DateArithmetic), &[], Shape::Any, ""),
    row("date_add_day_microsecond", Decision::Admitted, Signature::Family(Family::DateArithmetic), &[], Shape::Any, ""),
    row("date_add_day_minute", Decision::Admitted, Signature::Family(Family::DateArithmetic), &[], Shape::Any, ""),
    row("date_add_day_second", Decision::Admitted, Signature::Family(Family::DateArithmetic), &[], Shape::Any, ""),
    row("date_add_hour", Decision::Admitted, Signature::Family(Family::DateArithmetic), &[], Shape::Any, ""),
    row("date_add_hour_microsecond", Decision::Admitted, Signature::Family(Family::DateArithmetic), &[], Shape::Any, ""),
    row("date_add_hour_minute", Decision::Admitted, Signature::Family(Family::DateArithmetic), &[], Shape::Any, ""),
    row("date_add_hour_second", Decision::Admitted, Signature::Family(Family::DateArithmetic), &[], Shape::Any, ""),
    row("date_add_microsecond", Decision::Admitted, Signature::Family(Family::DateArithmetic), &[], Shape::Any, ""),
    row("date_add_minute", Decision::Admitted, Signature::Family(Family::DateArithmetic), &[], Shape::Any, ""),
    row("date_add_minute_microsecond", Decision::Admitted, Signature::Family(Family::DateArithmetic), &[], Shape::Any, ""),
    row("date_add_minute_second", Decision::Admitted, Signature::Family(Family::DateArithmetic), &[], Shape::Any, ""),
    row("date_add_month", Decision::Admitted, Signature::Family(Family::DateArithmetic), &[], Shape::Any, ""),
    row("date_add_quarter", Decision::Admitted, Signature::Family(Family::DateArithmetic), &[], Shape::Any, ""),
    row("date_add_second", Decision::Admitted, Signature::Family(Family::DateArithmetic), &[], Shape::Any, ""),
    row("date_add_second_microsecond", Decision::Admitted, Signature::Family(Family::DateArithmetic), &[], Shape::Any, ""),
    row("date_add_week", Decision::Admitted, Signature::Family(Family::DateArithmetic), &[], Shape::Any, ""),
    row("date_add_year", Decision::Admitted, Signature::Family(Family::DateArithmetic), &[], Shape::Any, ""),
    row("date_add_year_month", Decision::Admitted, Signature::Family(Family::DateArithmetic), &[], Shape::Any, ""),
    row("date_format", Decision::Admitted, Signature::Family(Family::Temporal), &[EvalType::Datetime, EvalType::String], Shape::Any, ""),
    row("date_sub", Decision::Excluded, Signature::None, &[], Shape::Any, "no local engine lowering: not selected by local_call, the temporal/JSON/vector families, or the reused pushdown catalog"),
    row("date_sub_day", Decision::Admitted, Signature::Family(Family::DateArithmetic), &[], Shape::Any, ""),
    row("date_sub_day_hour", Decision::Admitted, Signature::Family(Family::DateArithmetic), &[], Shape::Any, ""),
    row("date_sub_day_microsecond", Decision::Admitted, Signature::Family(Family::DateArithmetic), &[], Shape::Any, ""),
    row("date_sub_day_minute", Decision::Admitted, Signature::Family(Family::DateArithmetic), &[], Shape::Any, ""),
    row("date_sub_day_second", Decision::Admitted, Signature::Family(Family::DateArithmetic), &[], Shape::Any, ""),
    row("date_sub_hour", Decision::Admitted, Signature::Family(Family::DateArithmetic), &[], Shape::Any, ""),
    row("date_sub_hour_microsecond", Decision::Admitted, Signature::Family(Family::DateArithmetic), &[], Shape::Any, ""),
    row("date_sub_hour_minute", Decision::Admitted, Signature::Family(Family::DateArithmetic), &[], Shape::Any, ""),
    row("date_sub_hour_second", Decision::Admitted, Signature::Family(Family::DateArithmetic), &[], Shape::Any, ""),
    row("date_sub_microsecond", Decision::Admitted, Signature::Family(Family::DateArithmetic), &[], Shape::Any, ""),
    row("date_sub_minute", Decision::Admitted, Signature::Family(Family::DateArithmetic), &[], Shape::Any, ""),
    row("date_sub_minute_microsecond", Decision::Admitted, Signature::Family(Family::DateArithmetic), &[], Shape::Any, ""),
    row("date_sub_minute_second", Decision::Admitted, Signature::Family(Family::DateArithmetic), &[], Shape::Any, ""),
    row("date_sub_month", Decision::Admitted, Signature::Family(Family::DateArithmetic), &[], Shape::Any, ""),
    row("date_sub_quarter", Decision::Admitted, Signature::Family(Family::DateArithmetic), &[], Shape::Any, ""),
    row("date_sub_second", Decision::Admitted, Signature::Family(Family::DateArithmetic), &[], Shape::Any, ""),
    row("date_sub_second_microsecond", Decision::Admitted, Signature::Family(Family::DateArithmetic), &[], Shape::Any, ""),
    row("date_sub_week", Decision::Admitted, Signature::Family(Family::DateArithmetic), &[], Shape::Any, ""),
    row("date_sub_year", Decision::Admitted, Signature::Family(Family::DateArithmetic), &[], Shape::Any, ""),
    row("date_sub_year_month", Decision::Admitted, Signature::Family(Family::DateArithmetic), &[], Shape::Any, ""),
    row("datediff", Decision::Admitted, Signature::Family(Family::Temporal), &[EvalType::Datetime, EvalType::Datetime], Shape::Any, ""),
    row("day", Decision::Admitted, Signature::Family(Family::Temporal), &[EvalType::Datetime], Shape::Any, ""),
    row("dayname", Decision::Admitted, Signature::Family(Family::Temporal), &[EvalType::Datetime], Shape::Any, ""),
    row("dayofmonth", Decision::Admitted, Signature::Family(Family::Temporal), &[EvalType::Datetime], Shape::Any, ""),
    row("dayofweek", Decision::Admitted, Signature::Family(Family::Temporal), &[EvalType::Datetime], Shape::Any, ""),
    row("dayofyear", Decision::Admitted, Signature::Family(Family::Temporal), &[EvalType::Datetime], Shape::Any, ""),
    row("decode", Decision::Excluded, Signature::None, &[], Shape::Any, "no local engine lowering: not selected by local_call, the temporal/JSON/vector families, or the reused pushdown catalog"),
    row("default_func", Decision::Excluded, Signature::None, &[], Shape::Any, "no local engine lowering: not selected by local_call, the temporal/JSON/vector families, or the reused pushdown catalog"),
    row("degrees", Decision::Admitted, Signature::Family(Family::Math), &[], Shape::Any, ""),
    row("div", Decision::Admitted, Signature::Family(Family::Arithmetic), &[], Shape::Any, ""),
    row("elt", Decision::Admitted, Signature::Family(Family::Control), &[], Shape::AllLeaves, ""),
    row("encode", Decision::Excluded, Signature::None, &[], Shape::Any, "no local engine lowering: not selected by local_call, the temporal/JSON/vector families, or the reused pushdown catalog"),
    row("eq", Decision::Admitted, Signature::Family(Family::Comparison), &[], Shape::Any, ""),
    row("exp", Decision::Admitted, Signature::Family(Family::Math), &[], Shape::Any, ""),
    row("export_set", Decision::Excluded, Signature::None, &[], Shape::Any, "no local engine lowering: not selected by local_call, the temporal/JSON/vector families, or the reused pushdown catalog"),
    row("extract", Decision::Excluded, Signature::None, &[], Shape::Any, "no local engine lowering: not selected by local_call, the temporal/JSON/vector families, or the reused pushdown catalog"),
    row("field", Decision::Admitted, Signature::Family(Family::Comparison), &[], Shape::AllLeaves, ""),
    row("find_in_set", Decision::Admitted, Signature::Family(Family::String), &[], Shape::Any, ""),
    row("floor", Decision::Admitted, Signature::Family(Family::Math), &[], Shape::Any, ""),
    row("format", Decision::Excluded, Signature::None, &[], Shape::Any, "no local engine lowering: not selected by local_call, the temporal/JSON/vector families, or the reused pushdown catalog"),
    row("format_bytes", Decision::Excluded, Signature::None, &[], Shape::Any, "no local engine lowering: not selected by local_call, the temporal/JSON/vector families, or the reused pushdown catalog"),
    row("format_nano_time", Decision::Excluded, Signature::None, &[], Shape::Any, "no local engine lowering: not selected by local_call, the temporal/JSON/vector families, or the reused pushdown catalog"),
    row("found_rows", Decision::Excluded, Signature::None, &[], Shape::Any, "no local engine lowering: not selected by local_call, the temporal/JSON/vector families, or the reused pushdown catalog"),
    row("from_base64", Decision::Excluded, Signature::None, &[], Shape::Any, "native enforces max_allowed_packet before allocating; the pinned engine facade has no equivalent context setting"),
    row("from_binary", Decision::Admitted, Signature::Family(Family::String), &[], Shape::Any, ""),
    row("from_days", Decision::Admitted, Signature::Family(Family::Temporal), &[EvalType::Int], Shape::Any, ""),
    row("from_unixtime", Decision::Admitted, Signature::Family(Family::Temporal), &[], Shape::Any, ""),
    row("fts_match_word", Decision::Excluded, Signature::None, &[], Shape::Any, "no local engine lowering: not selected by local_call, the temporal/JSON/vector families, or the reused pushdown catalog"),
    row("ge", Decision::Admitted, Signature::Family(Family::Comparison), &[], Shape::Any, ""),
    row("get_format", Decision::Excluded, Signature::None, &[], Shape::Any, "no local engine lowering: not selected by local_call, the temporal/JSON/vector families, or the reused pushdown catalog"),
    row("get_lock", Decision::Excluded, Signature::None, &[], Shape::Any, "session state, statement clock, RNG, user variables, sequences, or effects are absent from the embedded engine Context"),
    row("getparam", Decision::Excluded, Signature::None, &[], Shape::Any, "session state, statement clock, RNG, user variables, sequences, or effects are absent from the embedded engine Context"),
    row("getvar", Decision::Excluded, Signature::None, &[], Shape::Any, "session state, statement clock, RNG, user variables, sequences, or effects are absent from the embedded engine Context"),
    row("getvar_decimal", Decision::Excluded, Signature::None, &[], Shape::Any, "session state, statement clock, RNG, user variables, sequences, or effects are absent from the embedded engine Context"),
    row("getvar_int", Decision::Excluded, Signature::None, &[], Shape::Any, "session state, statement clock, RNG, user variables, sequences, or effects are absent from the embedded engine Context"),
    row("getvar_real", Decision::Excluded, Signature::None, &[], Shape::Any, "session state, statement clock, RNG, user variables, sequences, or effects are absent from the embedded engine Context"),
    row("getvar_string", Decision::Excluded, Signature::None, &[], Shape::Any, "session state, statement clock, RNG, user variables, sequences, or effects are absent from the embedded engine Context"),
    row("getvar_time", Decision::Excluded, Signature::None, &[], Shape::Any, "session state, statement clock, RNG, user variables, sequences, or effects are absent from the embedded engine Context"),
    row("getvar_uint", Decision::Excluded, Signature::None, &[], Shape::Any, "session state, statement clock, RNG, user variables, sequences, or effects are absent from the embedded engine Context"),
    row("greatest", Decision::Admitted, Signature::Family(Family::Comparison), &[], Shape::AllLeaves, ""),
    row("grouping", Decision::Excluded, Signature::None, &[], Shape::Any, "no local engine lowering: not selected by local_call, the temporal/JSON/vector families, or the reused pushdown catalog"),
    row("gt", Decision::Admitted, Signature::Family(Family::Comparison), &[], Shape::Any, ""),
    row("hex", Decision::Admitted, Signature::Family(Family::String), &[], Shape::Any, ""),
    row("hour", Decision::Admitted, Signature::Family(Family::Temporal), &[EvalType::Duration], Shape::Any, ""),
    row("if", Decision::Admitted, Signature::Family(Family::Control), &[], Shape::IfThree, ""),
    row("ifnull", Decision::Admitted, Signature::Family(Family::Control), &[], Shape::LazyTail, ""),
    row("ilike", Decision::Excluded, Signature::None, &[], Shape::Any, "no local engine lowering: not selected by local_call, the temporal/JSON/vector families, or the reused pushdown catalog"),
    row("in", Decision::Admitted, Signature::Family(Family::Comparison), &[], Shape::LazyTail, ""),
    row("inet6_aton", Decision::Admitted, Signature::Family(Family::Miscellaneous), &[], Shape::Any, ""),
    row("inet6_ntoa", Decision::Admitted, Signature::Family(Family::Miscellaneous), &[], Shape::Any, ""),
    row("inet_aton", Decision::Admitted, Signature::Family(Family::Miscellaneous), &[], Shape::Any, ""),
    row("inet_ntoa", Decision::Admitted, Signature::Family(Family::Miscellaneous), &[], Shape::Any, ""),
    row("insert_func", Decision::Excluded, Signature::None, &[], Shape::Any, "native enforces max_allowed_packet before allocating; the pinned engine facade has no equivalent context setting"),
    row("instr", Decision::Admitted, Signature::Family(Family::String), &[], Shape::Any, ""),
    row("intdiv", Decision::Admitted, Signature::Family(Family::Arithmetic), &[], Shape::Any, ""),
    row("interval", Decision::Admitted, Signature::Family(Family::Comparison), &[], Shape::AllLeaves, ""),
    row("is_free_lock", Decision::Excluded, Signature::None, &[], Shape::Any, "session state, statement clock, RNG, user variables, sequences, or effects are absent from the embedded engine Context"),
    row("is_ipv4", Decision::Admitted, Signature::Family(Family::Miscellaneous), &[], Shape::Any, ""),
    row("is_ipv4_compat", Decision::Admitted, Signature::Family(Family::Miscellaneous), &[], Shape::Any, ""),
    row("is_ipv4_mapped", Decision::Admitted, Signature::Family(Family::Miscellaneous), &[], Shape::Any, ""),
    row("is_ipv6", Decision::Admitted, Signature::Family(Family::Miscellaneous), &[], Shape::Any, ""),
    row("is_used_lock", Decision::Excluded, Signature::None, &[], Shape::Any, "session state, statement clock, RNG, user variables, sequences, or effects are absent from the embedded engine Context"),
    row("is_uuid", Decision::Excluded, Signature::None, &[], Shape::Any, "no local engine lowering: not selected by local_call, the temporal/JSON/vector families, or the reused pushdown catalog"),
    row("isfalse", Decision::Admitted, Signature::Family(Family::Comparison), &[], Shape::Any, ""),
    row("isfalse_with_null", Decision::Admitted, Signature::Family(Family::Comparison), &[], Shape::Any, ""),
    row("isnull", Decision::Admitted, Signature::Family(Family::Comparison), &[], Shape::Any, ""),
    row("istrue", Decision::Admitted, Signature::Family(Family::Comparison), &[], Shape::Any, ""),
    row("istrue_with_null", Decision::Admitted, Signature::Family(Family::Comparison), &[], Shape::Any, ""),
    row("json_array", Decision::Admitted, Signature::Family(Family::Json), &[], Shape::Any, ""),
    row("json_array_append", Decision::Admitted, Signature::Family(Family::Json), &[], Shape::Any, ""),
    row("json_array_insert", Decision::Excluded, Signature::None, &[], Shape::Any, "no local engine lowering: not selected by local_call, the temporal/JSON/vector families, or the reused pushdown catalog"),
    row("json_contains", Decision::Admitted, Signature::Family(Family::Json), &[], Shape::Any, ""),
    row("json_contains_path", Decision::Excluded, Signature::None, &[], Shape::Any, "no local engine lowering: not selected by local_call, the temporal/JSON/vector families, or the reused pushdown catalog"),
    row("json_depth", Decision::Admitted, Signature::Family(Family::Json), &[EvalType::Json], Shape::Any, ""),
    row("json_extract", Decision::Admitted, Signature::Family(Family::Json), &[], Shape::Any, ""),
    row("json_insert", Decision::Admitted, Signature::Family(Family::Json), &[], Shape::Any, ""),
    row("json_keys", Decision::Admitted, Signature::Family(Family::Json), &[], Shape::Any, ""),
    row("json_length", Decision::Admitted, Signature::Family(Family::Json), &[], Shape::Any, ""),
    row("json_member_of", Decision::Admitted, Signature::Family(Family::Json), &[EvalType::Json, EvalType::Json], Shape::Any, ""),
    row("json_memberof", Decision::Admitted, Signature::Family(Family::Json), &[EvalType::Json, EvalType::Json], Shape::Any, ""),
    row("json_merge", Decision::Excluded, Signature::None, &[], Shape::Any, "no local engine lowering: not selected by local_call, the temporal/JSON/vector families, or the reused pushdown catalog"),
    row("json_merge_patch", Decision::Admitted, Signature::Family(Family::Json), &[], Shape::Any, ""),
    row("json_merge_preserve", Decision::Admitted, Signature::Family(Family::Json), &[], Shape::Any, ""),
    row("json_object", Decision::Admitted, Signature::Family(Family::Json), &[], Shape::Any, ""),
    row("json_overlaps", Decision::Excluded, Signature::None, &[], Shape::Any, "no local engine lowering: not selected by local_call, the temporal/JSON/vector families, or the reused pushdown catalog"),
    row("json_pretty", Decision::Excluded, Signature::None, &[], Shape::Any, "no local engine lowering: not selected by local_call, the temporal/JSON/vector families, or the reused pushdown catalog"),
    row("json_quote", Decision::Admitted, Signature::Family(Family::Json), &[EvalType::String], Shape::Any, ""),
    row("json_remove", Decision::Admitted, Signature::Family(Family::Json), &[], Shape::Any, ""),
    row("json_replace", Decision::Admitted, Signature::Family(Family::Json), &[], Shape::Any, ""),
    row("json_schema_valid", Decision::Excluded, Signature::None, &[], Shape::Any, "no local engine lowering: not selected by local_call, the temporal/JSON/vector families, or the reused pushdown catalog"),
    row("json_search", Decision::Excluded, Signature::None, &[], Shape::Any, "no local engine lowering: not selected by local_call, the temporal/JSON/vector families, or the reused pushdown catalog"),
    row("json_set", Decision::Admitted, Signature::Family(Family::Json), &[], Shape::Any, ""),
    row("json_storage_free", Decision::Excluded, Signature::None, &[], Shape::Any, "no local engine lowering: not selected by local_call, the temporal/JSON/vector families, or the reused pushdown catalog"),
    row("json_storage_size", Decision::Excluded, Signature::None, &[], Shape::Any, "no local engine lowering: not selected by local_call, the temporal/JSON/vector families, or the reused pushdown catalog"),
    row("json_type", Decision::Admitted, Signature::Family(Family::Json), &[EvalType::Json], Shape::Any, ""),
    row("json_unquote", Decision::Admitted, Signature::Family(Family::Json), &[], Shape::Any, ""),
    row("json_valid", Decision::Admitted, Signature::Family(Family::Json), &[], Shape::Any, ""),
    row("last_day", Decision::Admitted, Signature::Family(Family::Temporal), &[EvalType::Datetime], Shape::Any, ""),
    row("last_insert_id", Decision::Excluded, Signature::None, &[], Shape::Any, "session state, statement clock, RNG, user variables, sequences, or effects are absent from the embedded engine Context"),
    row("lastval", Decision::Excluded, Signature::None, &[], Shape::Any, "session state, statement clock, RNG, user variables, sequences, or effects are absent from the embedded engine Context"),
    row("lcase", Decision::Admitted, Signature::Family(Family::String), &[], Shape::Any, ""),
    row("le", Decision::Admitted, Signature::Family(Family::Comparison), &[], Shape::Any, ""),
    row("least", Decision::Admitted, Signature::Family(Family::Comparison), &[], Shape::AllLeaves, ""),
    row("left", Decision::Admitted, Signature::Family(Family::String), &[], Shape::Any, ""),
    row("leftshift", Decision::Admitted, Signature::Family(Family::Arithmetic), &[], Shape::Any, ""),
    row("length", Decision::Admitted, Signature::Family(Family::String), &[], Shape::Any, ""),
    row("like", Decision::Admitted, Signature::Family(Family::String), &[], Shape::Any, ""),
    row("ln", Decision::Admitted, Signature::Family(Family::Math), &[], Shape::Any, ""),
    row("load_file", Decision::Excluded, Signature::None, &[], Shape::Any, "no local engine lowering: not selected by local_call, the temporal/JSON/vector families, or the reused pushdown catalog"),
    row("localtime", Decision::Excluded, Signature::None, &[], Shape::Any, "no local engine lowering: not selected by local_call, the temporal/JSON/vector families, or the reused pushdown catalog"),
    row("localtimestamp", Decision::Excluded, Signature::None, &[], Shape::Any, "no local engine lowering: not selected by local_call, the temporal/JSON/vector families, or the reused pushdown catalog"),
    row("locate", Decision::Admitted, Signature::Family(Family::String), &[], Shape::Any, ""),
    row("log", Decision::Admitted, Signature::Family(Family::Math), &[], Shape::Any, ""),
    row("log10", Decision::Admitted, Signature::Family(Family::Math), &[], Shape::Any, ""),
    row("log2", Decision::Admitted, Signature::Family(Family::Math), &[], Shape::Any, ""),
    row("lower", Decision::Admitted, Signature::Family(Family::String), &[], Shape::Any, ""),
    row("lpad", Decision::Excluded, Signature::None, &[], Shape::Any, "native enforces max_allowed_packet before allocating; the pinned engine facade has no equivalent context setting"),
    row("lt", Decision::Admitted, Signature::Family(Family::Comparison), &[], Shape::Any, ""),
    row("ltrim", Decision::Admitted, Signature::Family(Family::String), &[], Shape::Any, ""),
    row("make_set", Decision::Excluded, Signature::None, &[], Shape::Any, "native enforces max_allowed_packet before allocating; the pinned engine facade has no equivalent context setting"),
    row("makedate", Decision::Admitted, Signature::Family(Family::Temporal), &[EvalType::Int, EvalType::Int], Shape::Any, ""),
    row("maketime", Decision::Admitted, Signature::Family(Family::Temporal), &[EvalType::Int, EvalType::Int, EvalType::Real], Shape::Any, ""),
    row("match_against", Decision::Excluded, Signature::None, &[], Shape::Any, "no local engine lowering: not selected by local_call, the temporal/JSON/vector families, or the reused pushdown catalog"),
    row("md5", Decision::Admitted, Signature::Family(Family::Miscellaneous), &[], Shape::Any, ""),
    row("microsecond", Decision::Admitted, Signature::Family(Family::Temporal), &[EvalType::Duration], Shape::Any, ""),
    row("mid", Decision::Admitted, Signature::Family(Family::String), &[], Shape::Any, ""),
    row("minus", Decision::Admitted, Signature::Family(Family::Arithmetic), &[], Shape::Any, ""),
    row("minute", Decision::Admitted, Signature::Family(Family::Temporal), &[EvalType::Duration], Shape::Any, ""),
    row("mod", Decision::Admitted, Signature::Family(Family::Arithmetic), &[], Shape::Any, ""),
    row("month", Decision::Admitted, Signature::Family(Family::Temporal), &[EvalType::Datetime], Shape::Any, ""),
    row("monthname", Decision::Admitted, Signature::Family(Family::Temporal), &[EvalType::Datetime], Shape::Any, ""),
    row("mul", Decision::Admitted, Signature::Family(Family::Arithmetic), &[], Shape::Any, ""),
    row("name_const", Decision::Excluded, Signature::None, &[], Shape::Any, "no local engine lowering: not selected by local_call, the temporal/JSON/vector families, or the reused pushdown catalog"),
    row("ne", Decision::Admitted, Signature::Family(Family::Comparison), &[], Shape::Any, ""),
    row("nextval", Decision::Excluded, Signature::None, &[], Shape::Any, "session state, statement clock, RNG, user variables, sequences, or effects are absent from the embedded engine Context"),
    row("not", Decision::Admitted, Signature::Family(Family::Arithmetic), &[], Shape::Any, ""),
    row("now", Decision::Excluded, Signature::None, &[], Shape::Any, "session state, statement clock, RNG, user variables, sequences, or effects are absent from the embedded engine Context"),
    row("nulleq", Decision::Admitted, Signature::Family(Family::Comparison), &[], Shape::Any, ""),
    row("nullif", Decision::Excluded, Signature::None, &[], Shape::Any, "no local engine lowering: not selected by local_call, the temporal/JSON/vector families, or the reused pushdown catalog"),
    row("oct", Decision::Admitted, Signature::Family(Family::String), &[], Shape::Any, ""),
    row("octet_length", Decision::Admitted, Signature::Family(Family::String), &[], Shape::Any, ""),
    row("or", Decision::Admitted, Signature::Family(Family::Control), &[], Shape::LazyTail, ""),
    row("ord", Decision::Admitted, Signature::Family(Family::String), &[], Shape::Any, ""),
    row("password", Decision::Excluded, Signature::None, &[], Shape::Any, "no local engine lowering: not selected by local_call, the temporal/JSON/vector families, or the reused pushdown catalog"),
    row("period_add", Decision::Admitted, Signature::Family(Family::Temporal), &[EvalType::Int, EvalType::Int], Shape::Any, ""),
    row("period_diff", Decision::Admitted, Signature::Family(Family::Temporal), &[EvalType::Int, EvalType::Int], Shape::Any, ""),
    row("pi", Decision::Admitted, Signature::Family(Family::Math), &[], Shape::Any, ""),
    row("plus", Decision::Admitted, Signature::Family(Family::Arithmetic), &[], Shape::Any, ""),
    row("position", Decision::Admitted, Signature::Family(Family::String), &[], Shape::Any, ""),
    row("pow", Decision::Admitted, Signature::Family(Family::Math), &[], Shape::Any, ""),
    row("power", Decision::Admitted, Signature::Family(Family::Math), &[], Shape::Any, ""),
    row("quarter", Decision::Admitted, Signature::Family(Family::Temporal), &[EvalType::Datetime], Shape::Any, ""),
    row("quote", Decision::Admitted, Signature::Family(Family::String), &[], Shape::Any, ""),
    row("radians", Decision::Admitted, Signature::Family(Family::Math), &[], Shape::Any, ""),
    row("rand", Decision::Excluded, Signature::None, &[], Shape::Any, "session state, statement clock, RNG, user variables, sequences, or effects are absent from the embedded engine Context"),
    row("random_bytes", Decision::Excluded, Signature::None, &[], Shape::Any, "session state, statement clock, RNG, user variables, sequences, or effects are absent from the embedded engine Context"),
    row("regexp", Decision::Admitted, Signature::Family(Family::String), &[], Shape::Any, ""),
    row("regexp_instr", Decision::Admitted, Signature::Family(Family::Regexp), &[], Shape::Any, ""),
    row("regexp_like", Decision::Admitted, Signature::Family(Family::String), &[], Shape::Any, ""),
    row("regexp_replace", Decision::Admitted, Signature::Family(Family::Regexp), &[], Shape::Any, ""),
    row("regexp_substr", Decision::Admitted, Signature::Family(Family::Regexp), &[], Shape::Any, ""),
    row("release_all_locks", Decision::Excluded, Signature::None, &[], Shape::Any, "session state, statement clock, RNG, user variables, sequences, or effects are absent from the embedded engine Context"),
    row("release_lock", Decision::Excluded, Signature::None, &[], Shape::Any, "session state, statement clock, RNG, user variables, sequences, or effects are absent from the embedded engine Context"),
    row("repeat", Decision::Excluded, Signature::None, &[], Shape::Any, "native enforces max_allowed_packet before allocating; the pinned engine facade has no equivalent context setting"),
    row("replace", Decision::Admitted, Signature::Family(Family::String), &[], Shape::Any, ""),
    row("reverse", Decision::Admitted, Signature::Family(Family::String), &[], Shape::Any, ""),
    row("right", Decision::Admitted, Signature::Family(Family::String), &[], Shape::Any, ""),
    row("rightshift", Decision::Admitted, Signature::Family(Family::Arithmetic), &[], Shape::Any, ""),
    row("rlike", Decision::Admitted, Signature::Family(Family::String), &[], Shape::Any, ""),
    row("round", Decision::Admitted, Signature::Family(Family::Math), &[], Shape::Any, ""),
    row("row", Decision::Excluded, Signature::None, &[], Shape::Any, "no local engine lowering: not selected by local_call, the temporal/JSON/vector families, or the reused pushdown catalog"),
    row("row_count", Decision::Excluded, Signature::None, &[], Shape::Any, "no local engine lowering: not selected by local_call, the temporal/JSON/vector families, or the reused pushdown catalog"),
    row("rpad", Decision::Excluded, Signature::None, &[], Shape::Any, "native enforces max_allowed_packet before allocating; the pinned engine facade has no equivalent context setting"),
    row("rtrim", Decision::Admitted, Signature::Family(Family::String), &[], Shape::Any, ""),
    row("schema", Decision::Excluded, Signature::None, &[], Shape::Any, "no local engine lowering: not selected by local_call, the temporal/JSON/vector families, or the reused pushdown catalog"),
    row("sec_to_time", Decision::Excluded, Signature::None, &[], Shape::Any, "no local engine lowering: not selected by local_call, the temporal/JSON/vector families, or the reused pushdown catalog"),
    row("second", Decision::Admitted, Signature::Family(Family::Temporal), &[EvalType::Duration], Shape::Any, ""),
    row("session_user", Decision::Excluded, Signature::None, &[], Shape::Any, "no local engine lowering: not selected by local_call, the temporal/JSON/vector families, or the reused pushdown catalog"),
    row("setval", Decision::Excluded, Signature::None, &[], Shape::Any, "session state, statement clock, RNG, user variables, sequences, or effects are absent from the embedded engine Context"),
    row("setvar", Decision::Excluded, Signature::None, &[], Shape::Any, "session state, statement clock, RNG, user variables, sequences, or effects are absent from the embedded engine Context"),
    row("sha", Decision::Admitted, Signature::Family(Family::Miscellaneous), &[], Shape::Any, ""),
    row("sha1", Decision::Admitted, Signature::Family(Family::Miscellaneous), &[], Shape::Any, ""),
    row("sha2", Decision::Admitted, Signature::Family(Family::Miscellaneous), &[], Shape::Any, ""),
    row("sign", Decision::Admitted, Signature::Family(Family::Math), &[], Shape::Any, ""),
    row("sin", Decision::Admitted, Signature::Family(Family::Math), &[], Shape::Any, ""),
    row("sleep", Decision::Excluded, Signature::None, &[], Shape::Any, "session state, statement clock, RNG, user variables, sequences, or effects are absent from the embedded engine Context"),
    row("sm3", Decision::Excluded, Signature::None, &[], Shape::Any, "no local engine lowering: not selected by local_call, the temporal/JSON/vector families, or the reused pushdown catalog"),
    row("space", Decision::Excluded, Signature::None, &[], Shape::Any, "native enforces max_allowed_packet before allocating; the pinned engine facade has no equivalent context setting"),
    row("sqrt", Decision::Admitted, Signature::Family(Family::Math), &[], Shape::Any, ""),
    row("str_to_date", Decision::Admitted, Signature::Family(Family::Temporal), &[EvalType::String, EvalType::String], Shape::Any, ""),
    row("strcmp", Decision::Admitted, Signature::Family(Family::String), &[], Shape::Any, ""),
    row("subdate", Decision::Excluded, Signature::None, &[], Shape::Any, "no local engine lowering: not selected by local_call, the temporal/JSON/vector families, or the reused pushdown catalog"),
    row("substr", Decision::Admitted, Signature::Family(Family::String), &[], Shape::Any, ""),
    row("substring", Decision::Admitted, Signature::Family(Family::String), &[], Shape::Any, ""),
    row("substring_index", Decision::Admitted, Signature::Family(Family::String), &[], Shape::Any, ""),
    row("subtime", Decision::Admitted, Signature::Family(Family::Temporal), &[], Shape::Any, ""),
    row("sysdate", Decision::Excluded, Signature::None, &[], Shape::Any, "session state, statement clock, RNG, user variables, sequences, or effects are absent from the embedded engine Context"),
    row("system_user", Decision::Excluded, Signature::None, &[], Shape::Any, "no local engine lowering: not selected by local_call, the temporal/JSON/vector families, or the reused pushdown catalog"),
    row("tan", Decision::Admitted, Signature::Family(Family::Math), &[], Shape::Any, ""),
    row("tidb_bounded_staleness", Decision::Excluded, Signature::None, &[], Shape::Any, "no local engine lowering: not selected by local_call, the temporal/JSON/vector families, or the reused pushdown catalog"),
    row("tidb_current_tso", Decision::Excluded, Signature::None, &[], Shape::Any, "no local engine lowering: not selected by local_call, the temporal/JSON/vector families, or the reused pushdown catalog"),
    row("tidb_decode_binary_plan", Decision::Excluded, Signature::None, &[], Shape::Any, "no local engine lowering: not selected by local_call, the temporal/JSON/vector families, or the reused pushdown catalog"),
    row("tidb_decode_key", Decision::Excluded, Signature::None, &[], Shape::Any, "no local engine lowering: not selected by local_call, the temporal/JSON/vector families, or the reused pushdown catalog"),
    row("tidb_decode_plan", Decision::Excluded, Signature::None, &[], Shape::Any, "no local engine lowering: not selected by local_call, the temporal/JSON/vector families, or the reused pushdown catalog"),
    row("tidb_decode_sql_digests", Decision::Excluded, Signature::None, &[], Shape::Any, "no local engine lowering: not selected by local_call, the temporal/JSON/vector families, or the reused pushdown catalog"),
    row("tidb_encode_index_key", Decision::Excluded, Signature::None, &[], Shape::Any, "no local engine lowering: not selected by local_call, the temporal/JSON/vector families, or the reused pushdown catalog"),
    row("tidb_encode_record_key", Decision::Excluded, Signature::None, &[], Shape::Any, "no local engine lowering: not selected by local_call, the temporal/JSON/vector families, or the reused pushdown catalog"),
    row("tidb_encode_sql_digest", Decision::Excluded, Signature::None, &[], Shape::Any, "no local engine lowering: not selected by local_call, the temporal/JSON/vector families, or the reused pushdown catalog"),
    row("tidb_is_ddl_owner", Decision::Excluded, Signature::None, &[], Shape::Any, "no local engine lowering: not selected by local_call, the temporal/JSON/vector families, or the reused pushdown catalog"),
    row("tidb_mvcc_info", Decision::Excluded, Signature::None, &[], Shape::Any, "no local engine lowering: not selected by local_call, the temporal/JSON/vector families, or the reused pushdown catalog"),
    row("tidb_parse_tso", Decision::Excluded, Signature::None, &[], Shape::Any, "no local engine lowering: not selected by local_call, the temporal/JSON/vector families, or the reused pushdown catalog"),
    row("tidb_parse_tso_logical", Decision::Excluded, Signature::None, &[], Shape::Any, "no local engine lowering: not selected by local_call, the temporal/JSON/vector families, or the reused pushdown catalog"),
    row("tidb_row_checksum", Decision::Excluded, Signature::None, &[], Shape::Any, "no local engine lowering: not selected by local_call, the temporal/JSON/vector families, or the reused pushdown catalog"),
    row("tidb_shard", Decision::Excluded, Signature::None, &[], Shape::Any, "no local engine lowering: not selected by local_call, the temporal/JSON/vector families, or the reused pushdown catalog"),
    row("tidb_version", Decision::Excluded, Signature::None, &[], Shape::Any, "no local engine lowering: not selected by local_call, the temporal/JSON/vector families, or the reused pushdown catalog"),
    row("time", Decision::Excluded, Signature::None, &[], Shape::Any, "no local engine lowering: not selected by local_call, the temporal/JSON/vector families, or the reused pushdown catalog"),
    row("time_format", Decision::Excluded, Signature::None, &[], Shape::Any, "no local engine lowering: not selected by local_call, the temporal/JSON/vector families, or the reused pushdown catalog"),
    row("time_to_sec", Decision::Admitted, Signature::Family(Family::Temporal), &[EvalType::Duration], Shape::Any, ""),
    row("timediff", Decision::Admitted, Signature::Family(Family::Temporal), &[], Shape::Any, ""),
    row("timestamp", Decision::Excluded, Signature::None, &[], Shape::Any, "no local engine lowering: not selected by local_call, the temporal/JSON/vector families, or the reused pushdown catalog"),
    row("timestampadd", Decision::Excluded, Signature::None, &[], Shape::Any, "no local engine lowering: not selected by local_call, the temporal/JSON/vector families, or the reused pushdown catalog"),
    row("timestampdiff", Decision::Admitted, Signature::Family(Family::Temporal), &[EvalType::String, EvalType::Datetime, EvalType::Datetime], Shape::Any, ""),
    row("to_base64", Decision::Excluded, Signature::None, &[], Shape::Any, "native enforces max_allowed_packet before allocating; the pinned engine facade has no equivalent context setting"),
    row("to_binary", Decision::Admitted, Signature::Family(Family::String), &[], Shape::Any, ""),
    row("to_days", Decision::Admitted, Signature::Family(Family::Temporal), &[EvalType::Datetime], Shape::Any, ""),
    row("to_seconds", Decision::Admitted, Signature::Family(Family::Temporal), &[EvalType::Datetime], Shape::Any, ""),
    row("translate", Decision::Excluded, Signature::None, &[], Shape::Any, "no local engine lowering: not selected by local_call, the temporal/JSON/vector families, or the reused pushdown catalog"),
    row("trim", Decision::Admitted, Signature::Family(Family::String), &[], Shape::Any, ""),
    row("truncate", Decision::Admitted, Signature::Family(Family::Math), &[], Shape::Any, ""),
    row("ucase", Decision::Admitted, Signature::Family(Family::String), &[], Shape::Any, ""),
    row("unaryminus", Decision::Admitted, Signature::Family(Family::Arithmetic), &[], Shape::Any, ""),
    row("uncompress", Decision::Admitted, Signature::Family(Family::Miscellaneous), &[], Shape::Any, ""),
    row("uncompressed_length", Decision::Admitted, Signature::Family(Family::Miscellaneous), &[], Shape::Any, ""),
    row("unhex", Decision::Admitted, Signature::Family(Family::String), &[], Shape::Any, ""),
    row("unix_timestamp", Decision::Admitted, Signature::Family(Family::Temporal), &[], Shape::Any, ""),
    row("upper", Decision::Admitted, Signature::Family(Family::String), &[], Shape::Any, ""),
    row("user", Decision::Excluded, Signature::None, &[], Shape::Any, "no local engine lowering: not selected by local_call, the temporal/JSON/vector families, or the reused pushdown catalog"),
    row("utc_date", Decision::Excluded, Signature::None, &[], Shape::Any, "no local engine lowering: not selected by local_call, the temporal/JSON/vector families, or the reused pushdown catalog"),
    row("utc_time", Decision::Excluded, Signature::None, &[], Shape::Any, "no local engine lowering: not selected by local_call, the temporal/JSON/vector families, or the reused pushdown catalog"),
    row("utc_timestamp", Decision::Excluded, Signature::None, &[], Shape::Any, "no local engine lowering: not selected by local_call, the temporal/JSON/vector families, or the reused pushdown catalog"),
    row("uuid", Decision::Excluded, Signature::None, &[], Shape::Any, "session state, statement clock, RNG, user variables, sequences, or effects are absent from the embedded engine Context"),
    row("uuid_short", Decision::Excluded, Signature::None, &[], Shape::Any, "no local engine lowering: not selected by local_call, the temporal/JSON/vector families, or the reused pushdown catalog"),
    row("uuid_timestamp", Decision::Excluded, Signature::None, &[], Shape::Any, "TiKV UUID_VERSION/UUID_TIMESTAMP parse malformed input leniently where Go raises error 1411"),
    row("uuid_to_bin", Decision::Excluded, Signature::None, &[], Shape::Any, "no local engine lowering: not selected by local_call, the temporal/JSON/vector families, or the reused pushdown catalog"),
    row("uuid_v4", Decision::Excluded, Signature::None, &[], Shape::Any, "session state, statement clock, RNG, user variables, sequences, or effects are absent from the embedded engine Context"),
    row("uuid_v7", Decision::Excluded, Signature::None, &[], Shape::Any, "session state, statement clock, RNG, user variables, sequences, or effects are absent from the embedded engine Context"),
    row("uuid_version", Decision::Excluded, Signature::None, &[], Shape::Any, "TiKV UUID_VERSION/UUID_TIMESTAMP parse malformed input leniently where Go raises error 1411"),
    row("validate_password_strength", Decision::Excluded, Signature::None, &[], Shape::Any, "no local engine lowering: not selected by local_call, the temporal/JSON/vector families, or the reused pushdown catalog"),
    row("values", Decision::Excluded, Signature::None, &[], Shape::Any, "session state, statement clock, RNG, user variables, sequences, or effects are absent from the embedded engine Context"),
    row("vec_as_text", Decision::Admitted, Signature::Family(Family::Vector), &[EvalType::VectorFloat32], Shape::Any, ""),
    row("vec_cosine_distance", Decision::Admitted, Signature::Family(Family::Vector), &[EvalType::VectorFloat32, EvalType::VectorFloat32], Shape::Any, ""),
    row("vec_dims", Decision::Admitted, Signature::Family(Family::Vector), &[EvalType::VectorFloat32], Shape::Any, ""),
    row("vec_from_text", Decision::Excluded, Signature::None, &[], Shape::Any, "no local engine lowering: not selected by local_call, the temporal/JSON/vector families, or the reused pushdown catalog"),
    row("vec_l1_distance", Decision::Admitted, Signature::Family(Family::Vector), &[EvalType::VectorFloat32, EvalType::VectorFloat32], Shape::Any, ""),
    row("vec_l2_distance", Decision::Admitted, Signature::Family(Family::Vector), &[EvalType::VectorFloat32, EvalType::VectorFloat32], Shape::Any, ""),
    row("vec_l2_norm", Decision::Admitted, Signature::Family(Family::Vector), &[EvalType::VectorFloat32], Shape::Any, ""),
    row("vec_negative_inner_product", Decision::Admitted, Signature::Family(Family::Vector), &[EvalType::VectorFloat32, EvalType::VectorFloat32], Shape::Any, ""),
    row("version", Decision::Excluded, Signature::None, &[], Shape::Any, "no local engine lowering: not selected by local_call, the temporal/JSON/vector families, or the reused pushdown catalog"),
    row("vitess_hash", Decision::Excluded, Signature::None, &[], Shape::Any, "no local engine lowering: not selected by local_call, the temporal/JSON/vector families, or the reused pushdown catalog"),
    row("week", Decision::Admitted, Signature::Family(Family::Temporal), &[EvalType::Datetime, EvalType::Int], Shape::Any, ""),
    row("weekday", Decision::Admitted, Signature::Family(Family::Temporal), &[EvalType::Datetime], Shape::Any, ""),
    row("weekofyear", Decision::Admitted, Signature::Family(Family::Temporal), &[EvalType::Datetime], Shape::Any, ""),
    row("weight_string", Decision::Excluded, Signature::None, &[], Shape::Any, "no local engine lowering: not selected by local_call, the temporal/JSON/vector families, or the reused pushdown catalog"),
    row("xor", Decision::Admitted, Signature::Family(Family::Control), &[], Shape::Any, ""),
    row("year", Decision::Admitted, Signature::Family(Family::Temporal), &[EvalType::Datetime], Shape::Any, ""),
    row("yearweek", Decision::Admitted, Signature::Family(Family::Temporal), &[], Shape::Any, ""),
];

/// The admission row for a lower-cased `func_name`, if the name is known.
#[must_use]
pub(crate) fn admission(name: &str) -> Option<&'static AdmissionRow> {
    ADMISSION_ROWS
        .binary_search_by(|row| row.name.cmp(name))
        .ok()
        .map(|index| &ADMISSION_ROWS[index])
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use tidb_datatype::{Datum, FieldType, FieldTypeCode};

    use super::*;
    use crate::builtin_registry::FUNCTION_CLASSES;
    use crate::constant::Constant;

    /// Every name the completeness test accepts as authoritative: the Go
    /// builtin registry plus the Rust-only synthesized spellings.
    fn is_known_name(name: &str) -> bool {
        FUNCTION_CLASSES
            .iter()
            .any(|(candidate, _, _)| *candidate == name)
            || SYNTHESIZED_NAMES.contains(&name)
    }

    fn leaf() -> Expression {
        Expression::Constant(Constant::new(
            Datum::Int(1),
            FieldType::new(FieldTypeCode::LongLong),
        ))
    }

    fn nested() -> Expression {
        call("plus", vec![leaf(), leaf()])
    }

    fn call(name: &str, args: Vec<Expression>) -> Expression {
        Expression::ScalarFunction(ScalarFunction::new(
            tidb_ast::CiString::new(name),
            FieldType::new(FieldTypeCode::LongLong),
            args,
        ))
    }

    /// Step-1 completeness: every registered SQL builtin name has a row. A new
    /// entry in the Go registry fails this test until it is classified.
    #[test]
    fn every_registered_builtin_has_an_admission_row() {
        let missing = FUNCTION_CLASSES
            .iter()
            .map(|(name, _, _)| *name)
            .filter(|name| admission(*name).is_none())
            .collect::<Vec<_>>();
        assert!(
            missing.is_empty(),
            "registered builtins missing an admission row: {missing:?}"
        );
    }

    /// The second half of step 1: every rewriter/internal spelling has a row.
    #[test]
    fn every_synthesized_name_has_an_admission_row() {
        let missing = SYNTHESIZED_NAMES
            .iter()
            .copied()
            .filter(|name| admission(*name).is_none())
            .collect::<Vec<_>>();
        assert!(
            missing.is_empty(),
            "synthesized names missing an admission row: {missing:?}"
        );
    }

    /// Binary search requires strict byte order, and no row may invent a name
    /// outside the authoritative universe (a typo would silently un-admit a
    /// real function).
    #[test]
    fn admission_rows_are_sorted_unique_and_known() {
        for pair in ADMISSION_ROWS.windows(2) {
            assert!(
                pair[0].name < pair[1].name,
                "admission table is not sorted at {}",
                pair[1].name
            );
        }
        let unique: BTreeSet<&str> = ADMISSION_ROWS.iter().map(|row| row.name).collect();
        assert_eq!(
            unique.len(),
            ADMISSION_ROWS.len(),
            "duplicate admission row name"
        );
        for row in ADMISSION_ROWS {
            assert!(
                is_known_name(row.name),
                "admission row {} is neither registered nor synthesized",
                row.name
            );
            // Compare by value, not by address: `ADMISSION_ROWS` is a `const`,
            // so each use may be a distinct anonymous copy and pointer
            // identity across two uses is not meaningful.
            let found = admission(row.name).expect("row must resolve");
            assert_eq!(found.name, row.name, "lookup name disagrees");
            assert_eq!(found.decision, row.decision, "lookup decision disagrees");
            assert_eq!(found.signature, row.signature, "lookup signature disagrees");
            assert_eq!(found.shape, row.shape, "lookup shape disagrees");
            assert_eq!(
                found.exclusion_reason, row.exclusion_reason,
                "lookup exclusion reason disagrees"
            );
        }
    }

    #[test]
    fn excluded_rows_carry_a_reason_and_admitted_rows_do_not() {
        for row in ADMISSION_ROWS {
            match row.decision {
                Decision::Excluded => assert!(
                    !row.exclusion_reason.is_empty(),
                    "excluded row {} has no reason",
                    row.name
                ),
                Decision::Admitted => assert!(
                    row.exclusion_reason.is_empty(),
                    "admitted row {} carries an exclusion reason",
                    row.name
                ),
            }
        }
    }

    #[test]
    fn admitted_rows_name_a_lowering_site() {
        for row in ADMISSION_ROWS {
            match (row.decision, row.signature) {
                (Decision::Admitted, Signature::Family(family)) => {
                    assert_ne!(family, Family::None, "{} has no lowering family", row.name);
                }
                (Decision::Admitted, Signature::Resolved(signatures)) => assert!(
                    !signatures.is_empty(),
                    "{} is admitted without a signature",
                    row.name
                ),
                (Decision::Admitted, Signature::None) => {
                    panic!("{} is admitted without a signature", row.name)
                }
                (Decision::Excluded, signature) => assert_eq!(
                    signature,
                    Signature::None,
                    "excluded row {} names a signature",
                    row.name
                ),
            }
        }
    }

    /// Regression guard for the pre-table `blocked_name` deny list. Removing a
    /// row here would re-admit a name that must fall back to native.
    #[test]
    fn former_deny_list_names_stay_excluded() {
        const FORMERLY_BLOCKED: &[&str] = &[
            "rand",
            "random_bytes",
            "uuid",
            "uuid_v4",
            "uuid_v7",
            "sysdate",
            "now",
            "current_timestamp",
            "curdate",
            "current_date",
            "curtime",
            "current_time",
            "get_lock",
            "release_lock",
            "release_all_locks",
            "is_used_lock",
            "is_free_lock",
            "sleep",
            "benchmark",
            "getvar",
            "setvar",
            "getparam",
            "nextval",
            "lastval",
            "setval",
            "values",
            "last_insert_id",
            "uuid_version",
            "uuid_timestamp",
            "concat",
            "concat_ws",
            "repeat",
            "space",
            "lpad",
            "rpad",
            "insert_func",
            "to_base64",
            "from_base64",
            "make_set",
        ];
        for name in FORMERLY_BLOCKED {
            let row = admission(name).unwrap_or_else(|| panic!("{name} lost its admission row"));
            assert_eq!(
                row.decision,
                Decision::Excluded,
                "{name} must stay excluded"
            );
            assert!(
                !row.exclusion_reason.is_empty(),
                "{name} must keep an exclusion reason"
            );
        }
    }

    /// Every non-registry spelling the old `local_call` chain could match must
    /// still be admitted; routing now reads the table, so an `Excluded` row
    /// here would silently move a lowered expression back to native.
    #[test]
    fn local_call_spellings_are_admitted() {
        for name in [
            "cast",
            "to_binary",
            "from_binary",
            // Defensive aliases the parser normalizes away; the pre-table
            // `local_call` still dispatched them.
            "casewhen",
            "isfalse_with_null",
            "rlike",
        ] {
            let row = admission(name).unwrap_or_else(|| panic!("{name} lost its admission row"));
            assert_eq!(
                row.decision,
                Decision::Admitted,
                "{name} must stay admitted"
            );
        }
    }

    /// [`Shape::permits`] must reproduce the old `lazy_children_are_safe`
    /// truth table exactly.
    #[test]
    fn shape_permits_reproduces_the_lazy_rules() {
        fn scalar(name: &str, args: Vec<Expression>) -> ScalarFunction {
            match call(name, args) {
                Expression::ScalarFunction(function) => function,
                _ => unreachable!(),
            }
        }
        assert!(Shape::Any.permits(&scalar("plus", vec![nested(), nested()])));
        assert!(Shape::IfThree.permits(&scalar("if", vec![nested(), leaf(), leaf()])));
        assert!(!Shape::IfThree.permits(&scalar("if", vec![nested(), nested(), leaf()])));
        assert!(!Shape::IfThree.permits(&scalar("if", vec![leaf(), leaf()])));
        assert!(Shape::LazyTail.permits(&scalar("coalesce", vec![nested(), leaf()])));
        assert!(Shape::LazyTail.permits(&scalar("coalesce", vec![leaf()])));
        assert!(!Shape::LazyTail.permits(&scalar("coalesce", vec![])));
        assert!(!Shape::LazyTail.permits(&scalar("coalesce", vec![leaf(), nested()])));
        assert!(Shape::AllLeaves.permits(&scalar("greatest", vec![leaf(), leaf()])));
        assert!(!Shape::AllLeaves.permits(&scalar("greatest", vec![leaf(), nested()])));
    }
}
