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

//! Values-only builtin-family routing and original native value boundaries.
//! Recursive AST traversal is preserved only in the test oracle.

use tidb_ast::BinaryOp;

use crate::coerce::truthy_of;
use crate::string_fn::{
    ascii, bin, bit_count, bit_length_in, case_convert_in, char_func_with_context,
    concat_with_context, concat_ws_with_context, elt_in, export_set_in, field, format_num,
    from_base64_in, from_base64_with_packet_limit, hex_in, locate_collation, locate_in,
    locate_with_position_in, make_set_in, oct_in, ord_in, quote_in, replace_in, reverse_in,
    str_insert, str_take_in, strcmp_in, substring, substring_index_in, unhex_in,
};
use crate::string_packet::{pad, repeat, space, to_base64};
use crate::time_fn::calendar::{
    date_diff_in, date_format_in, from_days_in, hour_in, minute_in, second_in, year_in,
};
use crate::{Columns, Datum, EvalError};

// Direct legacy-function observers explicitly use the preserved test oracle.
#[cfg(test)]
pub(crate) use crate::original_ast::eval_func;

/// Go `builtinBenchmarkSig.evalInt`'s first-argument `EvalInt` boundary.
pub(crate) fn benchmark_loop_count(
    value: Datum,
    source: Option<&tidb_datatype::FieldType>,
    ctx: &dyn Columns,
) -> Result<Option<i64>, EvalError> {
    benchmark_loop_count_borrowed(&value, source, ctx)
}
pub(crate) fn benchmark_loop_count_borrowed(
    value: &Datum,
    source: Option<&tidb_datatype::FieldType>,
    ctx: &dyn Columns,
) -> Result<Option<i64>, EvalError> {
    match crate::cast::cast_arg_as_int(value, source, ctx)? {
        Datum::Null => Ok(None),
        Datum::Int(value) => Ok(Some(value)),
        Datum::UInt(value) => Ok(Some(value as i64)),
        _ => Err(EvalError::Unsupported("BENCHMARK loop count")),
    }
}

/// Go's `builtinBenchmarkSig` switch intentionally has no vector arm.
pub(crate) fn ensure_benchmark_eval_type(
    source: Option<&tidb_datatype::FieldType>,
) -> Result<(), EvalError> {
    if source
        .is_some_and(|field_type| field_type.eval_type() == tidb_datatype::EvalType::VectorFloat32)
    {
        return Err(EvalError::Unsupported(
            "VectorFloat32 is not supported for BENCHMARK()",
        ));
    }
    Ok(())
}

/// The builtins whose result is a function of their argument values AND the
/// session: `FOUND_ROWS()`, `ROW_COUNT()` and both forms of
/// `LAST_INSERT_ID`. `None` if
/// `name` is not one of them.
///
/// Ports `builtinRowCountSig.evalInt` and the `builtinLastInsertID*Sig` pair
/// (`pkg/expression/builtin_info.go`).
fn eval_session_state(
    name: &str,
    vals: &[Datum],
    cols: &dyn Columns,
) -> Option<Result<Datum, EvalError>> {
    Some(match (name, vals) {
        ("FOUND_ROWS", []) => cols
            .found_rows()
            .map(Datum::UInt)
            .ok_or(EvalError::Unsupported("FOUND_ROWS requires a session")),
        ("ROW_COUNT", []) => cols
            .row_count()
            .map(Datum::Int)
            .ok_or(EvalError::Unsupported("ROW_COUNT requires a session")),
        // `LAST_INSERT_ID()` reads the value promoted from the preceding
        // statement. Its one-argument form instead coerces through Go's
        // `EvalInt`, records the raw uint64 bits for NEXT-statement
        // promotion, and returns the same UNSIGNED result immediately.
        // Keeping these forms together makes their same-statement separation
        // explicit: `LAST_INSERT_ID(5), LAST_INSERT_ID()` is `5, old`, not
        // `5, 5` (pkg/executor/select.go's statement-context promotion).
        ("LAST_INSERT_ID", []) => cols
            .last_insert_id()
            .map(Datum::UInt)
            .ok_or(EvalError::Unsupported("LAST_INSERT_ID requires a session")),
        ("LAST_INSERT_ID", [Datum::Null]) => Ok(Datum::Null),
        ("LAST_INSERT_ID", [value]) => match last_insert_id_arg(value) {
            Ok(id) => {
                cols.set_last_insert_id(id);
                Ok(Datum::UInt(id))
            }
            Err(e) => Err(e),
        },
        // `CURRENT_RESOURCE_GROUP()` is a zero-argument session builtin just
        // like the other information functions.  Keep it in the shared
        // session-state table so the AST/value evaluator returns the same
        // effective statement group as the rewritten/chunk evaluator.
        ("CURRENT_RESOURCE_GROUP", []) => Ok(match cols.current_resource_group() {
            Some(group) => Datum::new_string(group.into_bytes()),
            None => Datum::Null,
        }),
        ("ROW_COUNT" | "LAST_INSERT_ID", _) => Err(EvalError::Unsupported("bad function arity")),
        // The sequence builtins. The first argument is the sequence's name
        // path, substituted for the column reference the parser produced (see
        // the `nextval` arm of `rewriter::rewrite_expr_resolved`).
        //
        // `NEXTVAL` is the one builtin here that MUTATES durable state, and Go
        // does it outside the statement's transaction, so a rollback does not
        // give the value back (captured).
        ("NEXTVAL", [path]) => match sequence_path(path) {
            Ok(path) => cols.sequence_nextval(&path),
            Err(e) => Err(e),
        },
        ("LASTVAL", [path]) => match sequence_path(path) {
            Ok(path) => cols.sequence_lastval(&path),
            Err(e) => Err(e),
        },
        ("SETVAL", [path, value]) => match (sequence_path(path), value) {
            // Go evaluates the second argument as an int; a NULL one makes the
            // whole call NULL before the sequence is touched
            // (`builtinSetValSig.evalInt`'s isNull short-circuit).
            (Ok(_), Datum::Null) => Ok(Datum::Null),
            (Ok(path), value) => match value.as_int() {
                Some(value) => cols.sequence_setval(&path, value),
                None => Err(EvalError::Unsupported("SETVAL needs an integer value")),
            },
            (Err(e), _) => Err(e),
        },
        ("NEXTVAL" | "LASTVAL" | "SETVAL", _) => Err(EvalError::Unsupported("bad function arity")),
        _ => return None,
    })
}

/// The name path a sequence builtin's first argument carries on the CHUNK path,
/// where the rewriter replaced the parser's column reference with one string
/// constant (see `rewriter::rewrite_expr_resolved`).
///
/// The segments are joined by NUL rather than `.` so the split back is exact:
/// a backquoted identifier may contain a dot, but no identifier can contain a
/// NUL byte. That keeps the chunk path handing `Columns::sequence_nextval` the
/// SAME `&[String]` the row path hands it straight from the parser.
pub(crate) const SEQUENCE_PATH_SEPARATOR: char = '\0';

fn sequence_path(value: &Datum) -> Result<Vec<String>, EvalError> {
    match value {
        Datum::Bytes(bytes) => Ok(String::from_utf8(bytes.clone())
            .map_err(|_| EvalError::Unsupported("a sequence name must be text"))?
            .split(SEQUENCE_PATH_SEPARATOR)
            .map(str::to_owned)
            .collect()),
        _ => Err(EvalError::Unsupported(
            "a sequence builtin's first argument must be a name",
        )),
    }
}

/// [`eval_func_values`] plus the statement-context side effects Go attaches
/// to a builtin whose VALUE is still a pure function of its arguments.
///
/// Only `JSON_MERGE` has one today: `builtinJSONMergeSig.evalJSON` appends
/// `errDeprecatedSyntaxNoReplacement` (1681) after computing the merge, so a
/// NULL argument (which returns before that line) and a failed merge both
/// leave the warning unraised. Both evaluators route through here so the
/// warning cannot depend on which one ran.
pub(crate) fn eval_func_values_in(
    name: &str,
    vals: &[Datum],
    cols: &dyn Columns,
) -> Option<Result<Datum, EvalError>> {
    // The five explicitly deferred families use narrow TiDB host adapters,
    // never the generic family dispatcher or a second expression evaluator.
    if let Some(result) = crate::host_compat::eval(name, vals, cols) {
        return Some(result);
    }

    // Go's `builtinFromBase64Sig` checks the estimated decoded length against
    // `max_allowed_packet` before decoding and routes an over-limit result
    // through the statement warning policy. Keep this context-sensitive arm
    // ahead of the values-only table so AST and chunk evaluation agree.
    if name == "FROM_BASE64" {
        return Some(from_base64_with_packet_limit(vals, cols));
    }

    // The session-state builtins: pure functions of their argument VALUES
    // plus the session, which `cols` supplies. They live here rather than in
    // `eval_func_values` (values alone) so the row path and the chunk path
    // run the SAME implementation -- `eval_func`'s own arms used to be the
    // only ones, which is why `ROW_COUNT()` in a chunk-evaluated statement
    // reported "not yet ported" while the identical AST-evaluated statement
    // answered.
    if let Some(result) = eval_session_state(name, vals, cols) {
        return Some(result);
    }
    let result = eval_func_values(name, vals, cols)?;
    if name == "JSON_MERGE" && matches!(result, Ok(ref value) if *value != Datum::Null) {
        cols.append_warning(
            1681,
            "JSON_MERGE is deprecated and will be removed in a future release.",
        );
    }
    Some(result)
}

/// Evaluates a builtin whose result is a pure function of its
/// already-evaluated argument values — the values-only subset of
/// [`eval_func`]'s eager path. This is the bridge entry
/// `crate::scalar_function::ScalarFunction::eval` uses to run builtins over
/// chunk rows; `eval_func` calls it too, so there is exactly ONE
/// implementation of each function.
///
/// Deliberately OUTSIDE this entry (they stay AST/session-bound in
/// `eval_func`):
/// - lazy control forms: `IF` (Go's `builtinIf*Sig` evaluates exactly one
///   branch, so eager-evaluating both would change semantics, e.g. a guarded
///   `1/0`), `CASE`, and the `DATE_ADD`/`DATE_SUB`/`ADDDATE`/`SUBDATE`
///   family whose second argument is an `Expr::Interval`, not a value;
/// - session-state functions: `RAND` (needs
///   the argument AST and per-call `function_key` for generator identity),
///   the sequence functions (`NEXTVAL`/`LASTVAL`/`SETVAL`), and the
///   `time_fn` family (its dispatch takes `Columns` for the statement clock,
///   time zone, and `default_week_format`);
/// - the `LENGTH`/`OCTET_LENGTH`/`CHAR_LENGTH`/`CHARACTER_LENGTH` family: Go
///   selects the signature from the argument expression's FieldType via
///   `BuildContext::build_string_length_for_expr` BEFORE seeing any runtime
///   value, so it genuinely needs the argument AST, not just the value.
///
/// `COALESCE` is eager here exactly as in `eval_func`'s existing eager path
/// (Go's `builtinCoalesceSig` evaluates arguments in order over values, not
/// lazily over unevaluated branches — no guarded-error semantics to protect).
pub(crate) fn eval_func_values(
    name: &str,
    vals: &[Datum],
    ctx: &dyn Columns,
) -> Option<Result<Datum, EvalError>> {
    // `JSON_MEMBER_OF`'s rewrite spells the signature `json_member_of`, which
    // the registry knows (underscore-insensitively) as a registered builtin —
    // so the registered-but-unimplemented fallback below would swallow it
    // before the JSON family's own dispatch ever sees the name. Route it
    // through that dispatch here, where the row evaluator lives.
    if name.eq_ignore_ascii_case("json_member_of") || name.eq_ignore_ascii_case("json_memberof") {
        return Some(
            crate::builtin_ext::json::dispatch_in("JSON_MEMBER_OF", vals, ctx)
                .unwrap_or_else(|| Err(EvalError::Unsupported("JSON_MEMBER_OF arity"))),
        );
    }
    // Go `BuildCastFunction4Union`'s in-union cast-to-unsigned CLAMPS a
    // negative result to 0 (`builtin_cast.go:998`).
    if name == "cast_unsigned_in_union" {
        let value = vals.first()?;
        if value.is_null() {
            return Some(Ok(Datum::Null));
        }
        return Some(crate::tikv::eval_cast_unsigned_union_in(ctx, value, None).map(Datum::UInt));
    }
    // Go `builtinCastDecimalAsRealSig.evalReal`
    // (`builtin_cast.go:1650-1661`): a DECIMAL source with an in-union
    // unsigned target clamps a negative to Real(0).
    // Go `castAsRealToDecimalSig` (`builtin_cast.go:1405-1420`): NOT
    // in-union, or a non-negative value, yields FromFloat64; in-union and
    // negative yields the ZERO decimal.
    if name == "cast_real_to_decimal_in_union" {
        let value = vals.first()?;
        let f = match value {
            Datum::Real(x) => *x,
            Datum::Int(i) => *i as f64,
            Datum::UInt(u) => *u as f64,
            _ => return Some(Ok(Datum::Null)),
        };
        return match tidb_query_expr::native_union_decimal_route(f < 0.0) {
            tidb_query_expr::NativeUnionDecimalRoute::Zero => Some(Ok(Datum::Decimal(
                tidb_datatype::Decimal::from_f64(0.0)
                    .unwrap_or_else(|| tidb_datatype::Decimal::parse_mysql("0").0),
            ))),
            tidb_query_expr::NativeUnionDecimalRoute::Convert => {
                let Some(dec) = tidb_datatype::Decimal::from_f64(f) else {
                    return Some(Ok(Datum::Null));
                };
                Some(Ok(Datum::Decimal(dec)))
            }
        };
    }
    // Go `builtinCastIntAsDecimalSig.evalDecimal`
    // (`builtin_cast.go:1050-1070`): an in-union signed integer source maps a
    // negative value to the zero decimal before the target shape is applied.
    if name == "cast_int_to_decimal_in_union" {
        let value = vals.first()?;
        if value.is_null() {
            return Some(Ok(Datum::Null));
        }
        let route = match value {
            Datum::Int(value) => tidb_query_expr::native_union_decimal_route(*value < 0),
            Datum::UInt(_) => tidb_query_expr::native_union_decimal_route(false),
            _ => return None,
        };
        return Some(Ok(match route {
            tidb_query_expr::NativeUnionDecimalRoute::Zero => {
                Datum::Decimal(tidb_datatype::Decimal::parse_mysql("0").0)
            }
            tidb_query_expr::NativeUnionDecimalRoute::Convert => match value {
                Datum::Int(value) => Datum::Decimal(tidb_datatype::Decimal::from_int(*value)),
                Datum::UInt(value) => Datum::Decimal(tidb_datatype::Decimal::from_uint(*value)),
                _ => return None,
            },
        }));
    }
    // Go `builtinCastStringAsDecimalSig.evalDecimal`
    // (`builtin_cast.go:1877-1901`): an in-union UNSIGNED target discards a
    // negative textual value before parsing it, so no truncation warning is
    // emitted for that branch. Positive text follows the ordinary decimal
    // prefix parser and keeps its source warning disposition.
    if name == "cast_string_to_decimal_in_union" {
        let value = vals.first()?;
        if value.is_null() {
            return Some(Ok(Datum::Null));
        }
        let text = match value {
            Datum::String(value) => match value.as_utf8() {
                Ok(text) => text.to_owned(),
                Err(_) => return None,
            },
            Datum::Bytes(value) => match std::str::from_utf8(value) {
                Ok(text) => text.to_owned(),
                Err(_) => return None,
            },
            _ => return None,
        };
        let trimmed = text.trim();
        return match tidb_query_expr::native_union_text_decimal_route(text.as_bytes()) {
            tidb_query_expr::NativeUnionDecimalRoute::Zero => Some(Ok(Datum::Decimal(
                tidb_datatype::Decimal::parse_mysql("0").0,
            ))),
            tidb_query_expr::NativeUnionDecimalRoute::Convert => {
                crate::cast::report_decimal_input_truncation(value, ctx);
                Some(Ok(Datum::Decimal(
                    tidb_datatype::Decimal::parse_mysql(trimmed).0,
                )))
            }
        };
    }
    // Go `builtinCastDecimalAsDecimalSig.evalDecimal`
    // (`builtin_cast.go:1538-1551`): an in-union unsigned-target cast of a
    // negative source yields the ZERO decimal (the `res = &MyDecimal{}`
    // default is kept); otherwise the source decimal passes through.
    if name == "cast_decimal_in_union" {
        let value = vals.first()?;
        if value.is_null() {
            return Some(Ok(Datum::Null));
        }
        let negative = matches!(value, Datum::Decimal(dec) if dec.is_negative());
        return Some(Ok(
            match tidb_query_expr::native_union_decimal_route(negative) {
                tidb_query_expr::NativeUnionDecimalRoute::Zero => {
                    Datum::Decimal(tidb_datatype::Decimal::parse_mysql("0").0)
                }
                tidb_query_expr::NativeUnionDecimalRoute::Convert => value.clone(),
            },
        ));
    }
    // Go `castAsRealToIntSig.evalReal` (`builtin_cast.go:1370-1380`): a
    // real source with an in-union unsigned int target CLAMPS a negative
    // to 0 instead of the unsigned wrap.
    if name == "cast_real_int_in_union" {
        let value = vals.first()?;
        if value.is_null() {
            return Some(Ok(Datum::Null));
        }
        let f = match value {
            Datum::Real(x) => *x,
            other => crate::cast::to_f64_for_cast(other),
        };
        return Some(Ok(Datum::Int(
            tidb_query_expr::native_union_real_to_signed(f),
        )));
    }
    if name == "cast_real_in_union" {
        // Go `builtinCastRealAsRealSig.evalReal`
        // (`builtin_cast.go:1346-1352`): an in-union unsigned-target cast
        // clamps a negative to 0.
        let value = vals.first()?;
        if value.is_null() {
            return Some(Ok(Datum::Null));
        }
        let res = match value {
            Datum::Real(f) => *f,
            Datum::Int(i) => *i as f64,
            Datum::UInt(u) => *u as f64,
            Datum::Decimal(dec) => {
                let mut text = dec.to_string();
                if text.starts_with('-') {
                    text.remove(0);
                }
                text.parse::<f64>().unwrap_or(0.0)
            }
            _ => 0.0,
        };
        return Some(Ok(Datum::Real(tidb_query_expr::native_union_real(res))));
    }
    if let Some(result) = crate::math_fn::dispatch_values(name, vals, ctx) {
        return Some(result);
    }
    let result = match name {
        // Go `builtinGetParamStringSig.evalString` reads the integer selector
        // from the plan-cache parameter list and stringifies the selected
        // datum. An unset/out-of-range selector returns the exact
        // `ErrParamIndexExceedParamCounts` identity; a datum that cannot be
        // rendered by `ToString` becomes NULL without another error.
        "GETPARAM" if vals.len() == 1 => {
            let index = match vals[0] {
                Datum::Null => return Some(Ok(Datum::Null)),
                Datum::Int(index) => {
                    usize::try_from(index).map_err(|_| EvalError::ParamIndexExceedParamCounts)
                }
                Datum::UInt(index) => {
                    usize::try_from(index).map_err(|_| EvalError::ParamIndexExceedParamCounts)
                }
                _ => Err(EvalError::Unsupported("GETPARAM index is not an integer")),
            };
            let index = match index {
                Ok(index) => index,
                Err(error) => return Some(Err(error)),
            };
            let value = match ctx.get_param_value(index) {
                Ok(value) => value,
                Err(error) => return Some(Err(error)),
            };
            return Some(Ok(match value.sql_string() {
                Ok(text) => Datum::new_string(text.into_bytes()),
                Err(_) => Datum::Null,
            }));
        }
        // Go's `in` builtin: args[0] is the tested value and the rest are the
        // list. Three-valued: a match is 1; no match with a NULL anywhere
        // (including the tested value) is NULL; otherwise 0.
        "IN" if vals.len() >= 2 => {
            let (value, list) = vals.split_first().expect("at least two arguments");
            tidb_query_expr::native_in_ready_values(
                crate::tikv::in_eq_observation(value),
                list.len(),
                |index| {
                    crate::ops::eval_binary_in(
                        BinaryOp::Eq,
                        value.clone(),
                        list[index].clone(),
                        ctx,
                    )
                    .map(|computed| crate::tikv::in_eq_observation(&computed))
                },
            )
            .map(crate::tikv::in_control_datum)
        }
        // Go `builtinIntIsNullSig`: 1 when the argument is NULL, else 0 --
        // never NULL itself. `IS UNKNOWN` is the same function.
        "ISNULL" if vals.len() == 1 => crate::eval_boolean_ready_in(
            crate::BooleanFunction::IsNull,
            (!vals[0].is_null()).then_some(false),
            ctx,
        ),
        // Go `builtinIntIsTrueSig` with keepNull false: NULL and zero are 0.
        "ISTRUE" if vals.len() == 1 => truthy_of(&vals[0]).and_then(|ready| {
            crate::eval_boolean_ready_in(crate::BooleanFunction::IsTrue, ready, ctx)
        }),
        // Go's filter wrapper uses `builtinIntIsTrueSig{keepNull:true}` for
        // predicates whose NULL result must survive a NOT/OR rewrite.  This
        // is the value-preserving sibling of ISTRUE: NULL stays NULL, while
        // every other datum follows the same Datum.ToBool truthiness rule.
        "ISTRUE_WITH_NULL" if vals.len() == 1 => truthy_of(&vals[0]).and_then(|ready| {
            crate::eval_boolean_ready_in(crate::BooleanFunction::IsTrueWithNull, ready, ctx)
        }),
        // Go `builtinIntIsFalseSig`: 1 only for a non-NULL zero.
        "ISFALSE" if vals.len() == 1 => truthy_of(&vals[0]).and_then(|ready| {
            crate::eval_boolean_ready_in(crate::BooleanFunction::IsFalse, ready, ctx)
        }),
        // COALESCE returns the first non-NULL argument.
        "COALESCE" => crate::tikv::eval_coalesce_in(ctx, vals.len(), |index, _| Ok(&vals[index])),
        "IFNULL" if vals.len() == 2 => {
            let (a, b) = (vals[0].clone(), vals[1].clone());
            crate::tikv::eval_if_null_in(ctx, |_| Ok(a), |_| Ok(b))
        }
        // NULLIF(a, b): NULL when a and b are equal, else a. Go has no
        // builtin for this at all -- the expression rewriter turns
        // `NULLIF(a, b)` into `IF(a = b, NULL, a)`
        // (`expression_rewriter.go`'s `ast.NullIf` arm) -- so the equality
        // is the ORDINARY comparison with its full type derivation:
        // Int/Decimal/Float promotion (confirmed via goeval: the MIXED pair
        // `NULLIF(150, 1.5e2)` is NULL), string collation, and the JSON
        // domain (`nullif(json_remove(..), cast('{}' as json))` is the
        // collapse `ALTER USER ... DISCARD OLD PASSWORD` writes; comparing
        // only numeric pairs left it always non-NULL). A NULL condition --
        // either operand NULL -- makes IF take the else branch and answer
        // `a`, which `eval_binary_in`'s NULL propagation reproduces.
        "NULLIF" if vals.len() == 2 => {
            let (a, b) = (vals[0].clone(), vals[1].clone());
            crate::tikv::eval_null_if_in(ctx, &a, |original| {
                crate::ops::eval_binary_in(BinaryOp::Eq, a.clone(), b, original)
            })
        }
        // ---- string functions ----
        "CONCAT" if !vals.is_empty() => concat_with_context(vals, ctx),
        "UPPER" | "UCASE" => case_convert_in(vals, true, ctx),
        "LOWER" | "LCASE" => case_convert_in(vals, false, ctx),
        "LEFT" if vals.len() == 2 => str_take_in(vals, true, ctx),
        "RIGHT" if vals.len() == 2 => str_take_in(vals, false, ctx),
        "SUBSTRING" | "SUBSTR" | "MID" if vals.len() == 3 => substring(vals, ctx),
        "REVERSE" => reverse_in(vals, ctx),
        // `ASCII`: the first BYTE's numeric value (0 for the empty string).
        "ASCII" => ascii(vals, ctx),
        "REPEAT" if vals.len() == 2 => repeat(vals, ctx),
        "REPLACE" if vals.len() == 3 => replace_in(vals, ctx),
        "SPACE" if vals.len() == 1 => space(vals, ctx),
        "STRCMP" if vals.len() == 2 => strcmp_in(vals, ctx),
        "LPAD" if vals.len() == 3 => pad(vals, true, ctx),
        "RPAD" if vals.len() == 3 => pad(vals, false, ctx),
        // `LOCATE(substr, str)` / `INSTR(str, substr)` — same 1-indexed
        // char position, arguments in the opposite order (reusing
        // `position`, which already handles the empty-substr and
        // not-found rules).
        "LOCATE" if vals.len() == 2 => locate_in(
            &vals[0],
            &vals[1],
            locate_collation(&vals[0], &vals[1]),
            ctx,
        ),
        "LOCATE" if vals.len() == 3 => {
            locate_with_position_in(vals, locate_collation(&vals[0], &vals[1]), ctx)
        }
        "INSTR" if vals.len() == 2 => locate_in(
            &vals[1],
            &vals[0],
            locate_collation(&vals[0], &vals[1]),
            ctx,
        ),
        "HEX" if vals.len() == 1 => hex_in(vals, ctx),
        "UNHEX" if vals.len() == 1 => unhex_in(vals, ctx),
        "BIN" if vals.len() == 1 => bin(vals, ctx),
        "OCT" if vals.len() == 1 => oct_in(vals, ctx),
        "BIT_LENGTH" => bit_length_in(vals, ctx),
        "FIELD" if vals.len() >= 2 => field(vals, ctx),
        "ELT" if vals.len() >= 2 => elt_in(vals, ctx),
        "EXPORT_SET" => export_set_in(vals, ctx),
        "CONCAT_WS" if vals.len() >= 2 => concat_ws_with_context(vals, ctx),
        "SUBSTRING_INDEX" if vals.len() == 3 => substring_index_in(vals, ctx),
        // The parser renames `INSERT(...)` to `INSERT_FUNC` to avoid the
        // reserved statement keyword (the same desugar `CHAR`→`CHAR_FUNC`
        // uses).
        "INSERT_FUNC" if vals.len() == 4 => str_insert(vals, ctx).and_then(|result| {
            let result_len = match &result {
                Datum::String(value) => value.bytes().len(),
                Datum::Bytes(value) => value.len(),
                Datum::Null => return Ok(Datum::Null),
                _ => return Err(EvalError::Unsupported("INSERT result type")),
            };
            if result_len as u64 > ctx.max_allowed_packet() {
                ctx.handle_allowed_packet_overflowed("insert")?;
                Ok(Datum::Null)
            } else {
                Ok(result)
            }
        }),
        "MAKE_SET" if !vals.is_empty() => make_set_in(vals, ctx),
        "DATE_FORMAT" if vals.len() == 2 => date_format_in(&vals[0], &vals[1], ctx),
        "ORD" if vals.len() == 1 => ord_in(vals, ctx),
        "QUOTE" if vals.len() == 1 => quote_in(vals, ctx),
        "BIT_COUNT" if vals.len() == 1 => bit_count(vals, ctx),
        "FORMAT" if vals.len() == 2 => format_num(vals, ctx),
        "CHAR_FUNC" if !vals.is_empty() => char_func_with_context(vals, ctx),
        "TO_BASE64" if vals.len() == 1 => to_base64(vals, ctx),
        // Go `builtinLoadFileSig.evalString` reads the argument and then
        // returns `"", true, nil` UNCONDITIONALLY: TiDB has no server-side
        // file access at all, so LOAD_FILE is NULL for every path, readable
        // or not. CAPTURED: `select load_file('/etc/hosts')` is NULL.
        "LOAD_FILE" if vals.len() == 1 => Ok(Datum::Null),
        "FROM_BASE64" if vals.len() == 1 => from_base64_in(vals, ctx),
        // ---- date-part extraction ----
        // The existing ETDatetime cast has already supplied Time or NULL.
        // Preserve its raw fields, including zero/invalid calendar values.
        "YEAR" => year_in(vals, ctx),
        // HMS retains string coercion, including Duration Display, without
        // an ETDuration cast. The shared worker owns both native parser paths
        // and their whole-value clamp, distinct from raw nanos projection.
        "HOUR" => hour_in(vals, ctx),
        "MINUTE" => minute_in(vals, ctx),
        "SECOND" => second_in(vals, ctx),
        // `DATEDIFF`: the day count between two dates' DATE parts (any
        // time-of-day component is ignored, confirmed via `goeval` — e.g.
        // the same calendar day at 23:59:59 and 00:00:01 diffs to 0), via
        // an absolute day-numbering (`days_from_civil`) whose exact epoch
        // doesn't matter since only the difference is observable.
        // `TO_DAYS`/`TO_SECONDS`: zero-date calendar arithmetic owned by the
        // time-family module, including strict invalid-suffix handling.
        // `FROM_DAYS`: the reverse of `TO_DAYS` (see `time_fn::calendar::from_days_in`).
        "FROM_DAYS" => from_days_in(vals, ctx),
        "DATEDIFF" if vals.len() == 2 => date_diff_in(vals, ctx),
        // Family extension modules (`crate::builtin_ext`) — each family owns
        // one module with its own `dispatch(name, vals) -> Option<...>`, so
        // parallel agents can add builtins without touching this match.
        // `None` from every family means this entry doesn't know the name.
        _ => return crate::builtin_ext::dispatch(name, vals, ctx),
    };
    Some(result)
}

/// Ports the `EvalInt` coercion used by
/// `builtinLastInsertIDWithIDSig.evalInt` (`pkg/expression/builtin_info.go`).
/// This is intentionally local rather than a broad signed-to-unsigned helper:
/// the builtin publishes the resulting two's-complement bits as a `uint64`,
/// while casts and column assignment have different TiDB warning/range rules.
fn last_insert_id_arg(value: &Datum) -> Result<u64, EvalError> {
    let signed = match value {
        Datum::Int(value) => *value,
        Datum::UInt(value) => return Ok(*value),
        Datum::Decimal(value) => value.round_to_i64_saturating(),
        // Go's `types.Round` conversion used by `EvalInt` rounds fractional
        // numeric values away from zero; Rust's `round` has the same tie rule
        // and its `as i64` conversion saturates at the source's signed range.
        Datum::Real(value) => value.round() as i64,
        // `EvalInt`'s string path takes the leading signed integer run, not
        // the floating/exponent prefix: `'1.9tail'` is 1 and `'1e2tail'` is
        // 1, while an invalid string normalizes to 0 with a Go warning (this
        // seed has no warning result surface).
        Datum::String(value) => value.as_utf8().map(mysql_integer_prefix).unwrap_or(0),
        Datum::Bytes(value) => std::str::from_utf8(value)
            .map(mysql_integer_prefix)
            .unwrap_or(0),
        Datum::Null => unreachable!("NULL is handled before coercion"),
        Datum::MinNotNull | Datum::MaxValue => {
            return Err(EvalError::Unsupported(
                "range sentinel LAST_INSERT_ID argument",
            ));
        }
        other => {
            other
                .to_i64()
                .map_err(|_| EvalError::Unsupported("LAST_INSERT_ID conversion"))?
                .value
        }
    };
    Ok(signed as u64)
}

fn mysql_integer_prefix(value: &str) -> i64 {
    let value = value.trim_start();
    let (negative, digits) = match value.as_bytes().first() {
        Some(b'-') => (true, &value[1..]),
        Some(b'+') => (false, &value[1..]),
        _ => (false, value),
    };
    let count = digits
        .bytes()
        .take_while(|byte| byte.is_ascii_digit())
        .count();
    if count == 0 {
        return 0;
    }
    let magnitude = digits[..count].parse::<u64>().unwrap_or(u64::MAX);
    if negative {
        if magnitude >= (1_u64 << 63) {
            i64::MIN
        } else {
            -(magnitude as i64)
        }
    } else {
        i64::try_from(magnitude).unwrap_or(i64::MAX)
    }
}

/// Negates an already-computed predicate only when requested, retaining the
/// original signed-int/NULL packing and passing other datum kinds unchanged.
pub(crate) fn negate_if(v: Datum, neg: bool, cols: &dyn Columns) -> Result<Datum, EvalError> {
    if !neg {
        return Ok(v);
    }
    let ready = match v {
        Datum::Int(i) => Some(i != 0),
        Datum::UInt(i) => Some(i != 0),
        Datum::Null => None,
        other => return Ok(other),
    };
    crate::eval_boolean_ready_in(crate::BooleanFunction::UnaryNot, ready, cols)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn d(s: &str) -> Datum {
        Datum::new_string(s.to_string())
    }

    /// Go `builtinCastRealAsRealSig.evalReal`
    /// (`builtin_cast.go:1346-1352`): an in-union unsigned-target cast
    /// clamps a negative to 0.
    #[test]
    fn cast_real_in_union_clamps_negatives_to_zero() {
        let ctx = PacketLimit::default();
        let result = eval_func_values("cast_real_in_union", &[Datum::Real(-2.5)], &ctx);
        assert_eq!(result.unwrap().unwrap(), Datum::Real(0.0));
    }

    /// Go `builtinCastDecimalAsRealSig.evalReal`
    /// (`builtin_cast.go:1650-1661`): a DECIMAL source with an in-union
    /// unsigned target clamps a negative to Real(0).
    #[test]
    fn cast_decimal_in_union_clamps_negatives_to_zero() {
        let ctx = PacketLimit::default();
        let decimal = Datum::Decimal(tidb_datatype::Decimal::from_literal("-2.5"));
        let result = eval_func_values("cast_decimal_in_union", &[decimal], &ctx);
        assert_eq!(
            result.unwrap().unwrap(),
            Datum::Decimal(tidb_datatype::Decimal::parse_mysql("0").0)
        );
    }

    #[test]
    fn cast_decimal_in_union_keeps_positives() {
        let ctx = PacketLimit::default();
        let decimal = Datum::Decimal(tidb_datatype::Decimal::from_literal("2.5"));
        let result = eval_func_values("cast_decimal_in_union", &[decimal], &ctx);
        assert_eq!(
            result.unwrap().unwrap(),
            Datum::Decimal(tidb_datatype::Decimal::from_literal("2.5"))
        );
    }

    #[test]
    fn cast_real_in_union_keeps_non_negatives() {
        let ctx = PacketLimit::default();
        let result = eval_func_values("cast_real_in_union", &[Datum::Real(7.5)], &ctx);
        assert_eq!(result.unwrap().unwrap(), Datum::Real(7.5));
    }

    use std::cell::RefCell;

    use super::*;

    #[derive(Default)]
    struct PacketLimit {
        warnings: RefCell<Vec<(u16, String)>>,
    }

    impl Columns for PacketLimit {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }

        fn max_allowed_packet(&self) -> u64 {
            3
        }

        fn append_warning(&self, code: u16, message: &str) {
            self.warnings.borrow_mut().push((code, message.to_owned()));
        }
    }

    /// Go `TestInsertBinarySig`: all seven source rows, including the one
    /// result that crosses the signature's three-byte packet limit.
    #[test]
    fn test_insert_binary_sig() {
        let ctx = PacketLimit::default();
        let binary = |value: &str| Datum::new_bytes(value.as_bytes().to_vec());
        let rows = [
            (
                [binary("abc"), Datum::Int(3), Datum::Int(-1), binary("d")],
                binary("abd"),
            ),
            (
                [binary("abc"), Datum::Int(3), Datum::Int(-1), binary("de")],
                Datum::Null,
            ),
            (
                [binary("abc"), Datum::Int(0), Datum::Int(-1), binary("d")],
                binary("abc"),
            ),
            (
                [Datum::Null, Datum::Int(3), Datum::Int(-1), binary("d")],
                Datum::Null,
            ),
            (
                [binary("abc"), Datum::Null, Datum::Int(-1), binary("d")],
                Datum::Null,
            ),
            (
                [binary("abc"), Datum::Int(3), Datum::Null, binary("d")],
                Datum::Null,
            ),
            (
                [binary("abc"), Datum::Int(3), Datum::Int(-1), Datum::Null],
                Datum::Null,
            ),
        ];

        for (args, expected) in rows {
            assert_eq!(
                eval_func_values("INSERT_FUNC", &args, &ctx)
                    .expect("INSERT must be dispatched")
                    .expect("source rows must evaluate"),
                expected
            );
        }

        assert_eq!(
            *ctx.warnings.borrow(),
            vec![(
                1301,
                "Result of insert() was larger than max_allowed_packet (3) - truncated".to_owned(),
            )]
        );
    }
}

#[cfg(test)]
mod cast_real_int_in_union_tests {
    use super::*;

    /// Go `castAsRealToIntSig` (`builtin_cast.go:1370-1380`): a real source
    /// with an in-union unsigned int target clamps a negative to 0.
    #[test]
    fn cast_real_int_in_union_clamps_negatives_to_zero() {
        let ctx = crate::context::NoColumns;
        let result = eval_func_values("cast_real_int_in_union", &[Datum::Real(-2.5)], &ctx);
        assert_eq!(result.unwrap().unwrap(), Datum::Int(0));
    }

    #[test]
    fn cast_real_int_in_union_keeps_non_negatives() {
        let ctx = crate::context::NoColumns;
        let result = eval_func_values("cast_real_int_in_union", &[Datum::Real(7.5)], &ctx);
        assert_eq!(result.unwrap().unwrap(), Datum::Int(7));
    }
}

#[cfg(test)]
mod cast_real_to_decimal_in_union_tests {
    use super::*;

    /// Go `castAsRealToDecimalSig` (`builtin_cast.go:1405-1420`): NOT
    /// in-union, or a non-negative value, yields FromFloat64; in-union and
    /// negative yields the ZERO decimal.
    #[test]
    fn cast_real_to_decimal_in_union_clamps_negatives_to_zero() {
        let ctx = crate::context::NoColumns;
        let result = eval_func_values("cast_real_to_decimal_in_union", &[Datum::Real(-2.5)], &ctx);
        assert_eq!(
            result.unwrap().unwrap(),
            Datum::Decimal(tidb_datatype::Decimal::parse_mysql("0").0)
        );
    }

    #[test]
    fn cast_real_to_decimal_in_union_keeps_non_negatives_like_go() {
        let ctx = crate::context::NoColumns;
        let result = eval_func_values("cast_real_to_decimal_in_union", &[Datum::Real(7.5)], &ctx);
        let expected = tidb_datatype::Decimal::from_f64(7.5).unwrap();
        assert_eq!(result.unwrap().unwrap(), Datum::Decimal(expected));
    }
}

#[cfg(test)]
#[cfg(test)]
#[cfg(test)]
#[cfg(test)]
#[cfg(test)]
#[test]
fn shared_union_cast_controller_covers_all_seven_source_specific_names() {
    let ctx = crate::NoColumns;
    let zero = Datum::Decimal(tidb_datatype::Decimal::parse_mysql("0").0);
    for (name, input, expected) in [
        ("cast_unsigned_in_union", Datum::Int(-1), Datum::UInt(0)),
        (
            "cast_real_to_decimal_in_union",
            Datum::Real(-2.5),
            zero.clone(),
        ),
        ("cast_int_to_decimal_in_union", Datum::Int(-2), zero.clone()),
        (
            "cast_string_to_decimal_in_union",
            Datum::new_string("-2x"),
            zero.clone(),
        ),
        (
            "cast_decimal_in_union",
            Datum::Decimal(tidb_datatype::Decimal::from_literal("-2.5")),
            zero,
        ),
        ("cast_real_int_in_union", Datum::Real(-2.5), Datum::Int(0)),
        ("cast_real_in_union", Datum::Real(-2.5), Datum::Real(0.0)),
    ] {
        assert_eq!(
            eval_func_values(name, &[input], &ctx)
                .expect("UNION CAST name must dispatch")
                .expect("source-specific conversion must succeed"),
            expected,
            "{name}"
        );
    }
}
