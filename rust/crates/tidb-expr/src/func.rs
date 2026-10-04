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

//! Thin builtin-family routing plus the non-family control functions and the
//! `[NOT] IN (list)` predicate. Called from `crate::eval_in`.

use tidb_ast::{BinaryOp, Expr};

use crate::coerce::{bool_int, truthy_of};
use crate::eval_in;
use crate::row::row_compare_in;
use crate::string_fn::{
    ascii, bin, bit_count, bit_length_in, case_convert_in, char_func_with_context,
    concat_with_context, concat_ws_with_context, elt_in, export_set_in, field, format_num,
    from_base64_in, from_base64_with_packet_limit, hex_in, locate_collation, locate_in,
    locate_with_position_in, make_set_in, oct_in, ord_in, quote_in, replace_in, reverse_in,
    str_insert, str_take_in, strcmp_in, substring, substring_index_in, unhex_in,
};
use crate::string_packet::{pad, repeat, space, to_base64};
use crate::time_fn::calendar::{
    date_add, date_diff_in, date_format_in, from_days_in, hour_in, minute_in, second_in, year_in,
};
use crate::{BuildContext, Columns, Datum, EvalError, StringLengthFunction};

/// Evaluates a builtin scalar function over its evaluated arguments.
pub(crate) fn eval_func(
    name: &str,
    args: &[Expr],
    cols: &dyn Columns,
    function_key: Option<usize>,
) -> Result<Datum, EvalError> {
    let name = name.to_ascii_uppercase();
    // The AST evaluator is also an expression-construction entry point for
    // session execution, so it must enforce the same function-class arity as
    // `new_function_impl`. DATE_ADD/SUB and ADDDATE/SUBDATE retain their
    // interval unit inside one Rust AST argument, while Go's function class
    // counts that unit as the third argument.
    let arity_count = if matches!(
        name.as_str(),
        "DATE_ADD" | "DATE_SUB" | "ADDDATE" | "SUBDATE"
    ) && matches!(args, [_, Expr::Interval { .. }])
    {
        3
    } else {
        args.len()
    };
    crate::builtin_registry::verify_args_by_count(&name, arity_count)?;
    // `DATE_ADD`/`DATE_SUB`'s second argument is an `Expr::Interval` (a
    // value *and* a unit keyword), not a plain expression `eval_in` can
    // evaluate on its own — handled here, before every other function's
    // uniform eager-eval of `args` below, which would otherwise choke on it.
    // `ADDDATE`/`SUBDATE` share this SAME `date, Expr::Interval` shape —
    // `tidb_parser::parse_adddate_or_subdate` already normalizes their own
    // dual `INTERVAL n unit` / bare-number grammar down to it (see that
    // function's own doc) — and evaluate IDENTICALLY to `DATE_ADD`/
    // `DATE_SUB` respectively (confirmed via `gorun`: `ADDDATE(d, 1)` and
    // `DATE_ADD(d, INTERVAL 1 DAY)` produce the same result for every
    // case tried, including a month-end rollover, `NULL` propagation in
    // either argument, and a sub-day `HOUR` unit) — so `ADDDATE` takes
    // the SAME `sign = 1` as `DATE_ADD`, `SUBDATE` the same `sign = -1`
    // as `DATE_SUB`, reusing `date_add` with no new logic at all.
    if name == "DATE_ADD" || name == "DATE_SUB" || name == "ADDDATE" || name == "SUBDATE" {
        let [date_expr, Expr::Interval { value, unit }] = args else {
            return Err(EvalError::Unsupported("DATE_ADD/DATE_SUB arguments"));
        };
        let date_val = eval_in(date_expr, cols)?;
        let amount_val = eval_in(value, cols)?;
        let sign = if name == "DATE_SUB" || name == "SUBDATE" {
            -1
        } else {
            1
        };
        return date_add(unit, &date_val, &amount_val, sign);
    }
    // `NEXTVAL`/`LASTVAL`/`SETVAL`'s first argument is the SEQUENCE NAME
    // (real TiDB parses it as a `TableNameExpr`, this crate as a plain
    // `Expr::Column` — see task #121's restore-equivalence finding), so it
    // must NOT be evaluated as a column reference the way the uniform
    // eager-eval below would — dispatched to the resolver's own sequence
    // catalog instead, the same interior-mutability side-effect
    // architecture `Columns::set_uservar` established.
    if name == "NEXTVAL" || name == "LASTVAL" || name == "SETVAL" {
        let Some(Expr::Column(path)) = args.first() else {
            return Err(EvalError::Unsupported("sequence function argument"));
        };
        return match (name.as_str(), args.len()) {
            ("NEXTVAL", 1) => cols.sequence_nextval(path),
            ("LASTVAL", 1) => cols.sequence_lastval(path),
            ("SETVAL", 2) => match eval_in(&args[1], cols)? {
                // NULL propagates without touching the sequence, matching
                // real TiDB's own `EvalInt` isNull short-circuit (read
                // from `builtinSetValSig.evalInt` directly).
                Datum::Null => Ok(Datum::Null),
                Datum::Int(n) => cols.sequence_setval(path, n),
                Datum::UInt(n) => cols.sequence_setval(path, n as i64),
                _ => Err(EvalError::Unsupported("SETVAL value")),
            },
            _ => Err(EvalError::Unsupported("sequence function arguments")),
        };
    }
    // Go selects these signatures from the argument expression's FieldType
    // while building the function, before EvalString produces a runtime
    // datum. Keep the same ordering here: source AST type facts choose one
    // immutable evaluator first, then only that evaluator sees the value.
    if args.len() == 1 {
        let function = match name.as_str() {
            "LENGTH" | "OCTET_LENGTH" => Some(StringLengthFunction::Length),
            "CHAR_LENGTH" | "CHARACTER_LENGTH" => Some(StringLengthFunction::CharLength),
            _ => None,
        };
        if let Some(function) = function {
            let built = BuildContext::default().build_string_length_for_expr(function, &args[0])?;
            let value = eval_in(&args[0], cols)?;
            // `LENGTH`/`OCTET_LENGTH` are binary-aware and count the ENCODED
            // bytes; `CHAR_LENGTH` is `funcPropNone` and counts characters of
            // the UTF-8 form, so only the former transcodes.
            let value = match function {
                StringLengthFunction::Length => {
                    crate::convert_charset::to_binary_by_collation(&value)?
                }
                StringLengthFunction::CharLength => value,
            };
            return built.eval_in(&value, cols);
        }
    }
    if let Some(result) = crate::builtin_ext::eval_aes_lazy(
        name.as_str(),
        args.len(),
        |index| eval_in(&args[index], cols),
        cols,
    ) {
        return result;
    }
    // `IF` is a lazy control function in Go: `builtinIf*Sig` evaluates the
    // condition through its wrapped `EvalInt`, then evaluates exactly one
    // result branch.  Handle it before the ordinary eager argument material-
    // ization below so an unreachable error (for example `1 / 0`) cannot
    // leak into the selected result.  The value-only evaluator has no
    // FieldType result-promotion pass; the selected branch therefore keeps
    // its natural Datum domain, while the Go function-class type boundary is
    // recorded as explicit partial evidence rather than guessed at runtime.
    if name == "IF" {
        let [condition, when_true, when_false] = args else {
            return Err(EvalError::Unsupported("bad IF arguments"));
        };
        return crate::tikv::eval_if_in(
            cols,
            // Keep the ordinary Datum.ToBool coercion and its error policy;
            // the shared head alone turns that nullable truth into branch demand.
            |original_cols| truthy_of(&eval_in(condition, original_cols)?),
            |scoped_cols| eval_in(when_true, scoped_cols),
            |scoped_cols| eval_in(when_false, scoped_cols),
        );
    }
    if name == "IFNULL" {
        let [first, fallback] = args else {
            return Err(EvalError::Unsupported("bad IFNULL arguments"));
        };
        return crate::tikv::eval_if_null_in(
            cols,
            |original_cols| eval_in(first, original_cols),
            |scoped_cols| eval_in(fallback, scoped_cols),
        );
    }
    if name == "COALESCE" {
        return crate::tikv::eval_coalesce_in(cols, args.len(), |index, selected_cols| {
            eval_in(&args[index], selected_cols)
        });
    }
    if name == "BENCHMARK" {
        let [count, expression] = args else {
            return Err(EvalError::Unsupported("BENCHMARK arguments"));
        };
        // Go builds both arguments left-to-right before evaluating the count.
        // The direct AST evaluator can resolve a column's VALUE through
        // `Columns` but has no planning schema, so only that one build result
        // may stay untyped; every other build error remains observable.
        let rewrite_argument = |argument| match crate::rewriter::rewrite_expr(argument) {
            Ok(rewritten) => Ok(Some(rewritten)),
            Err(EvalError::UnknownColumn(_) | EvalError::UnknownColumnInClause(..)) => Ok(None),
            Err(error) => Err(error),
        };
        let rewritten_count = rewrite_argument(count)?;
        let rewritten_expression = rewrite_argument(expression)?;
        let Some(loop_count) = benchmark_loop_count(
            eval_in(count, cols)?,
            rewritten_count.as_ref().and_then(|arg| arg.static_type()),
            cols,
        )?
        else {
            return Ok(Datum::Null);
        };
        if loop_count < 0 {
            return Ok(Datum::Null);
        }
        if let Some(rewritten) = rewritten_expression {
            ensure_benchmark_eval_type(rewritten.static_type())?;
        }
        for _ in 0..loop_count {
            eval_in(expression, cols)?;
        }
        return Ok(Datum::Int(0));
    }
    if matches!(name.as_str(), "CHARSET" | "COLLATION" | "COERCIBILITY") {
        let [arg] = args else {
            return Err(EvalError::Unsupported(
                "CHARSET/COLLATION/COERCIBILITY arguments",
            ));
        };
        let arg = crate::rewriter::rewrite_expr(arg)?;
        return crate::collation_derive::info_metadata_value(&name.to_ascii_lowercase(), &arg)
            .ok_or(EvalError::Unsupported("information metadata function"));
    }
    if name == "REGEXP_LIKE" && matches!(args.len(), 2 | 3) {
        let string_arg = |index: usize| -> Result<Option<String>, EvalError> {
            let value = eval_in(&args[index], cols)?;
            let value = crate::cast::cast_arg_as_string(&value, None, cols)?;
            if value.is_null() {
                return Ok(None);
            }
            value
                .sql_string()
                .map(Some)
                .map_err(|_| EvalError::Unsupported("invalid UTF-8 REGEXP_LIKE argument"))
        };
        return crate::tikv::evaluate_regexp_in(crate::tikv::RegexpFunction::Like, cols, || {
            let Some(text) = string_arg(0)? else {
                return Ok(crate::tikv::EvaluatedArgs::NullWitness(None));
            };
            let Some(pattern) = string_arg(1)? else {
                return Ok(crate::tikv::EvaluatedArgs::NullWitness(None));
            };
            let match_type = if args.len() == 3 {
                let Some(match_type) = string_arg(2)? else {
                    return Ok(crate::tikv::EvaluatedArgs::NullWitness(None));
                };
                match_type
            } else {
                String::new()
            };
            let match_type = crate::regexp::regexp_match_type_with_collation(
                &match_type,
                crate::ops::DERIVATION_FREE_COLLATION,
            );
            let invocation = tidb_query_expr::NativeRegexpInvocation::new(
                &Default::default(),
                &Default::default(),
                0,
                false,
                false,
            );
            Ok(crate::tikv::EvaluatedArgs::RegexpLike {
                invocation,
                text: text.into_bytes(),
                pattern: pattern.into_bytes(),
                match_type: match_type.into_bytes(),
            })
        });
    }
    let vals: Vec<Datum> = args
        .iter()
        .enumerate()
        .map(|(index, arg)| {
            if name == "CHAR_FUNC" && index + 1 == args.len() {
                if let Expr::RawString(charset) = arg {
                    return Ok(Datum::new_string(charset.clone()));
                }
            }
            eval_in(arg, cols)
        })
        .collect::<Result<_, _>>()?;
    // Go `HandleBinaryLiteral`'s `funcPropBinAware` arm. The chunk path reads
    // the argument's static charset; this value-only path reads the datum's
    // own collation, which carries the same charset -- so a `gbk` string
    // reaching `HEX`/`LENGTH`/`ASCII` transcodes here exactly as it does
    // there. See `crate::convert_charset` for why this is the only implicit
    // transcode.
    let vals: Vec<Datum> = if crate::convert_charset::func_prop(&name.to_ascii_lowercase())
        == crate::convert_charset::FuncProp::BinAware
    {
        vals.iter()
            .map(crate::convert_charset::to_binary_by_collation)
            .collect::<Result<_, _>>()?
    } else {
        vals
    };
    // Go's `newBaseBuiltinFuncWithTp` argument-cast layer. This tier has no
    // static argument types (see `crate::arg_eval_type`), so it gets Go's
    // wrap from the values alone -- which is everything except the `YEAR`
    // distinction, a type no value can carry.
    let vals = crate::arg_eval_type::wrap_datetime_args(name.as_str(), vals, &[], cols)?;
    let vals = crate::arg_eval_type::wrap_int_args(name.as_str(), vals, &[], cols)?;
    let vals = crate::arg_eval_type::wrap_string_args(name.as_str(), vals, &[], cols)?;
    if let Some(result) = crate::math_fn::dispatch(name.as_str(), args, &vals, cols, function_key) {
        return result;
    }
    if let Some(result) = eval_func_values_in(name.as_str(), &vals, cols) {
        return result;
    }
    // Family extension modules (`crate::builtin_ext`), the shared values-only
    // arms, and the session-state functions all live in
    // `eval_func_values_in`, tried above; only the session-clock time family
    // remains.
    crate::time_fn::dispatch(name.as_str(), &vals, cols)
        .unwrap_or(Err(EvalError::Unsupported("unsupported function")))
}

/// Go `builtinBenchmarkSig.evalInt`'s first-argument `EvalInt` boundary.
pub(crate) fn benchmark_loop_count(
    value: Datum,
    source: Option<&tidb_datatype::FieldType>,
    ctx: &dyn Columns,
) -> Result<Option<i64>, EvalError> {
    match crate::cast::cast_arg_as_int(&value, source, ctx)? {
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
        let res = crate::cast::to_i64_signed_with_warnings(value, ctx).ok()?;
        return Some(Ok(Datum::UInt(if res < 0 { 0 } else { res as u64 })));
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
        if f < 0.0 {
            // in-union + negative → the ZERO decimal.
            return Some(Ok(Datum::Decimal(
                tidb_datatype::Decimal::from_f64(0.0)
                    .unwrap_or_else(|| tidb_datatype::Decimal::parse_mysql("0").0),
            )));
        }
        // Non-negative: FromFloat64.
        let Some(dec) = tidb_datatype::Decimal::from_f64(f) else {
            return Some(Ok(Datum::Null));
        };
        return Some(Ok(Datum::Decimal(dec)));
    }
    // Go `builtinCastIntAsDecimalSig.evalDecimal`
    // (`builtin_cast.go:1050-1070`): an in-union signed integer source maps a
    // negative value to the zero decimal before the target shape is applied.
    if name == "cast_int_to_decimal_in_union" {
        let value = vals.first()?;
        if value.is_null() {
            return Some(Ok(Datum::Null));
        }
        return Some(Ok(match value {
            Datum::Int(value) if *value < 0 => {
                Datum::Decimal(tidb_datatype::Decimal::parse_mysql("0").0)
            }
            Datum::Int(value) => Datum::Decimal(tidb_datatype::Decimal::from_int(*value)),
            Datum::UInt(value) => Datum::Decimal(tidb_datatype::Decimal::from_uint(*value)),
            _ => return None,
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
            Datum::String(value) => value.as_utf8().ok()?.to_owned(),
            Datum::Bytes(value) => std::str::from_utf8(value).ok()?.to_owned(),
            _ => return None,
        };
        let trimmed = text.trim();
        if trimmed.len() > 1 && trimmed.starts_with('-') {
            return Some(Ok(Datum::Decimal(
                tidb_datatype::Decimal::parse_mysql("0").0,
            )));
        }
        crate::cast::report_decimal_input_truncation(value, ctx);
        return Some(Ok(Datum::Decimal(
            tidb_datatype::Decimal::parse_mysql(trimmed).0,
        )));
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
        if negative {
            return Some(Ok(Datum::Decimal(
                tidb_datatype::Decimal::parse_mysql("0").0,
            )));
        }
        return Some(Ok(value.clone()));
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
        return Some(Ok(Datum::Int(if f < 0.0 { 0 } else { f as i64 })));
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
        return Some(Ok(Datum::Real(if res < 0.0 { 0.0 } else { res })));
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
            let mut found_null = *value == Datum::Null;
            for item in list {
                match crate::ops::eval_binary_in(BinaryOp::Eq, value.clone(), item.clone(), ctx) {
                    Ok(Datum::Int(0)) => {}
                    Ok(Datum::Null) => found_null = true,
                    Ok(_) => return Some(Ok(Datum::Int(1))),
                    Err(e) => return Some(Err(e)),
                }
            }
            Ok(if found_null {
                Datum::Null
            } else {
                Datum::Int(0)
            })
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

/// Evaluates `expr [NOT] IN (list)` in MySQL three-valued logic: a match is
/// TRUE; no match with a NULL in the list (or a NULL left side) is NULL; no
/// match with no NULL is FALSE. `NOT IN` negates TRUE/FALSE but keeps NULL.
/// Element equality reuses `=`, so it honors the string collation.
///
/// A row-value operand (`(a, b) [NOT] IN (...)`, `expr` a bare
/// `Expr::Row`) is handled as its OWN case, checked FIRST: `eval_in`
/// itself has no arm for a standalone `Expr::Row` (see `crate::row`'s
/// own doc for why), so `expr`/each `list` item must be recognized as
/// row-shaped and compared via `crate::row::row_compare`'s own
/// `Eq`-mode element-wise logic BEFORE ever calling plain `eval_in` on
/// them — every `list` item is required to be a `Expr::Row` of the
/// SAME arity too (both a literal row-value list,
/// `(a,b) IN ((1,2),(3,4))`, and a resolved subquery's own captured
/// rows — see `tidb-exec`'s own `Database::in_subquery_rows` — always
/// produce `Expr::Row` list items here, never a mix).
///
/// EVERY list item is evaluated and compared, even after a match is
/// found: the boolean answer is settled by the first match, but the
/// evaluation of the remaining items is OBSERVABLE and so cannot be
/// skipped. Our string-versus-number coercion lives inside the
/// comparison (`crate::ops::eval_binary_in`), so skipping a comparison
/// skips its `1292 Truncated incorrect DOUBLE value` warning and any
/// error it would raise.
///
/// Go settles this the same way, in the vectorized `in` that real
/// execution uses (`pkg/expression/builtin_other_vec_generated.go`,
/// `builtinInRealSig.vecEvalInt`):
///
/// ```text
/// for j := 0; j < len(args); j++ {
///     if err := args[j].VecEvalReal(ctx, input, buf1); err != nil {
///         return err
///     }
///     ...
///     for i := 0; i < n; i++ {
///         if r64s[i] != 0 {
///             continue
///         }
/// ```
///
/// `args[j].VecEvalReal` -- which IS the coercion, because
/// `newBaseBuiltinFuncWithTp` wrapped every arg in `cast(... as double)`
/// at build time (`pkg/expression/builtin.go`, `WrapWithCastAsReal`) --
/// runs for every arg unconditionally; `if r64s[i] != 0 { continue }`
/// skips only the COMPARISON for an already-matched row, never the
/// evaluation. An error from a later arg is returned even for rows that
/// already matched. The scalar `builtinInRealSig.evalInt`
/// (`pkg/expression/builtin_other.go`) does `return 1, false, nil` from
/// inside its loop, but that path is the non-vectorized fallback; the
/// warning count a client observes comes from the vectorized one.
pub(crate) fn eval_in_list(
    expr: &Expr,
    list: &[Expr],
    not: bool,
    cols: &dyn Columns,
) -> Result<Datum, EvalError> {
    if let Expr::Row(left_items) = expr {
        let lv: Vec<Datum> = left_items
            .iter()
            .map(|e| eval_in(e, cols))
            .collect::<Result<_, _>>()?;
        let mut found_null = false;
        let mut found_match = false;
        for item in list {
            let Expr::Row(right_items) = item else {
                return Err(EvalError::Unsupported(
                    "row value IN list item arity mismatch",
                ));
            };
            let rv: Vec<Datum> = right_items
                .iter()
                .map(|e| eval_in(e, cols))
                .collect::<Result<_, _>>()?;
            match row_compare_in(BinaryOp::Eq, &lv, &rv, cols)? {
                Datum::Int(0) => {}
                Datum::Null => found_null = true,
                _ => found_match = true,
            }
        }
        return in_result(found_match, found_null, not, cols);
    }
    let v = eval_in(expr, cols)?;
    let mut found_null = false;
    let mut found_match = false;
    for item in list {
        let iv = eval_in(item, cols)?;
        match crate::ops::eval_binary_in(BinaryOp::Eq, v.clone(), iv, cols)? {
            Datum::Int(0) => {}
            Datum::Null => found_null = true,
            _ => found_match = true,
        }
    }
    in_result(found_match, found_null, not, cols)
}

/// The three-valued answer of an `IN` whose whole list has been compared:
/// a match anywhere is TRUE and outranks a NULL, no match with a NULL is
/// NULL, no match with no NULL is FALSE. `NOT` negates TRUE/FALSE and
/// leaves NULL alone.
///
/// Match-outranks-NULL is what the short-circuiting form produced too --
/// it returned TRUE from inside the loop even when an earlier item had
/// already set `found_null` -- so folding the whole list changes WHICH
/// items get evaluated, never the boolean this returns.
fn in_result(
    found_match: bool,
    found_null: bool,
    not: bool,
    cols: &dyn Columns,
) -> Result<Datum, EvalError> {
    let value = if found_match {
        bool_int(true)
    } else if found_null {
        Datum::Null
    } else {
        bool_int(false)
    };
    negate_if(value, not, cols)
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
#[test]
fn ifnull_workers_keep_actual_identity_lazy_demand_and_eager_values() {
    use crate::constant::{Constant, ParamMarker};
    use crate::expression::Expression;
    use crate::scalar_function::ScalarFunction;
    use std::cell::{Cell, RefCell};
    use tidb_datatype::{
        BinaryJSON, BinaryLiteral, Collation, CoreTime, Decimal, FieldType, FieldTypeCode,
        MySqlDuration, MysqlEnum, MysqlSet, Time, TimeType, VectorFloat32,
    };

    struct Demand {
        values: [Datum; 2],
        reads: RefCell<Vec<usize>>,
        fail: Cell<Option<usize>>,
    }
    impl Columns for Demand {
        fn get(&self, path: &[String]) -> Option<Datum> {
            let index = match path.first().map(String::as_str) {
                Some("first") => 0,
                Some("second") => 1,
                _ => panic!("unexpected IFNULL test column"),
            };
            self.get_param_value(index).ok()
        }
        fn get_param_value(&self, index: usize) -> Result<Datum, EvalError> {
            self.reads.borrow_mut().push(index);
            if self.fail.get() == Some(index) {
                return Err(EvalError::Unsupported("ifnull demanded child"));
            }
            Ok(self.values[index].clone())
        }
        fn param_value(&self, index: usize) -> Result<Datum, EvalError> {
            self.get_param_value(index)
        }
        fn time_zone(&self) -> tidb_datatype::SessionTimeZone {
            panic!("IFNULL must not fetch timezone")
        }
        fn date_modes(&self) -> tidb_datatype::DateModes {
            panic!("IFNULL must not fetch date modes")
        }
        fn truncate_level(&self) -> crate::ErrorLevel {
            panic!("IFNULL must not coerce either branch")
        }
        fn append_warning(&self, _: u16, _: &str) {
            panic!("IFNULL must not warn for opaque selected values")
        }
    }
    fn frame(value: &Datum) -> Option<Vec<u8>> {
        let crate::tikv::EvaluatedArgs::Bytes(bytes) =
            crate::tikv::prepare_datum_identity_args(value).unwrap()
        else {
            panic!("identity preparation must retain actual nullable bytes")
        };
        bytes
    }
    fn resource(result: Result<Datum, EvalError>) {
        let error = result.expect_err("IFNULL must retain the existing zero-slot owner");
        let EvalError::ExpressionAdapterFailure(failure) = error else {
            panic!("{error:?}")
        };
        assert_eq!(
            failure.class(),
            crate::ExpressionAdapterFailureClass::PoolResource
        );
        assert_eq!(
            failure.origin(),
            crate::ExpressionAdapterFailureOrigin::Pool
        );
    }
    let ast_args = ["first", "second"].map(|name| Expr::Column(vec![name.to_owned()]));
    let typed_args = [0, 1].map(|order| {
        let mut value = Constant::default();
        value.param_marker = Some(ParamMarker { order });
        Expression::Constant(value)
    });
    let mut typed = ScalarFunction::new(
        tidb_ast::CiString::new("ifnull"),
        FieldType::new(FieldTypeCode::LongLong),
        typed_args.to_vec(),
    );
    // The existing direct helper can have no return type. Test its complete
    // identity domain separately from typed/PB post-conversion below.
    typed.ret_type = None;
    let empty = tidb_chunk::mutrow::MutRow::from_datums(&[]);
    let evaluate = |mode, columns: &dyn Columns, values: &[Datum]| match mode {
        0 => eval_func("IFNULL", &ast_args, columns, None),
        1 => typed.eval(columns, empty.to_row()),
        _ => eval_func_values_in("IFNULL", values, columns).unwrap(),
    };
    let mut vector = VectorFloat32::init(2);
    vector
        .elements_mut()
        .copy_from_slice(&[-0.0, f32::from_bits(0x7fc01234)]);
    let values = vec![
        Datum::Null,
        Datum::MinNotNull,
        Datum::MaxValue,
        Datum::Int(0),
        Datum::Int(-7),
        Datum::UInt(u64::MAX),
        Datum::Real(f64::from_bits(0x7ff8000012345678)),
        Datum::Real(-0.0),
        Datum::Float32(1.00000049),
        Datum::Float32(f64::from_bits(0xfff8000012345678)),
        Datum::Decimal(
            Decimal::from_raw_parts(true, b"00010049".to_vec(), 2, 4).with_declared_shape(12, 2),
        ),
        Datum::Decimal(Decimal::from_raw_parts(true, Vec::new(), 0, 0)),
        Datum::new_string(""),
        Datum::new_string("text"),
        Datum::new_bytes(vec![0xff, 0, 0x80]),
        Datum::new_collation_string(vec![0xff, 0, 0x80], Collation::GbkBin),
        Datum::BinaryLiteral(BinaryLiteral::from(vec![0, 1, 0xff])),
        Datum::Bit(BinaryLiteral::from(vec![0, 0xff])),
        Datum::Duration(MySqlDuration::from_raw_parts(-123456789, 9)),
        Datum::Time(Time::from_raw_parts(
            CoreTime::from_raw(u64::MAX),
            TimeType::DateTime,
            9,
        )),
        Datum::Enum(MysqlEnum::new(vec![0xff, 0], 9), Collation::GbkBin),
        Datum::Set(MysqlSet::new(vec![0, 0xff], u64::MAX), Collation::Binary),
        Datum::Json(BinaryJSON::parse("{\"x\":null}").unwrap()),
        Datum::Raw(vec![0xff, 0, 0x80]),
        Datum::VectorFloat32(vector),
    ];
    for slots in [1, 0] {
        let owner = crate::AsciiPoolOwner::new(
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
        .unwrap();
        let execution = owner.begin_execution().unwrap();
        for value in &values {
            for null_first in [false, true] {
                let ctx = Demand {
                    values: [
                        if null_first {
                            Datum::Null
                        } else {
                            value.clone()
                        },
                        value.clone(),
                    ],
                    reads: RefCell::new(Vec::new()),
                    fail: Cell::new(None),
                };
                let needs_second = ctx.values[0].is_null();
                if !needs_second {
                    ctx.fail.set(Some(1));
                }
                for mode in 0..3 {
                    let result = execution
                        .scope()
                        .with_columns(&ctx, |columns| evaluate(mode, columns, &ctx.values));
                    if slots == 1 {
                        assert_eq!(frame(&result.unwrap()), frame(value));
                    } else {
                        resource(result);
                    }
                    let expected_reads = if mode == 2 {
                        vec![]
                    } else if slots == 1 && needs_second {
                        vec![0, 1]
                    } else {
                        vec![0]
                    };
                    assert_eq!(ctx.reads.take(), expected_reads);
                }
            }
        }
        let ctx = Demand {
            values: [Datum::Null, Datum::Int(7)],
            reads: RefCell::new(Vec::new()),
            fail: Cell::new(Some(0)),
        };
        for mode in 0..2 {
            let result = execution
                .scope()
                .with_columns(&ctx, |columns| evaluate(mode, columns, &ctx.values));
            assert!(
                matches!(result, Err(EvalError::Unsupported(message)) if message == if mode == 0 { "unknown column" } else { "ifnull demanded child" })
            );
            assert_eq!(ctx.reads.take(), vec![0]);
            ctx.fail.set(Some(1));
            let result = execution
                .scope()
                .with_columns(&ctx, |columns| evaluate(mode, columns, &ctx.values));
            if slots == 1 {
                assert!(
                    matches!(result, Err(EvalError::Unsupported(message)) if message == if mode == 0 { "unknown column" } else { "ifnull demanded child" })
                );
                assert_eq!(ctx.reads.take(), vec![0, 1]);
            } else {
                resource(result);
                assert_eq!(ctx.reads.take(), vec![0]);
            }
            ctx.fail.set(Some(0));
        }
        assert!(execution
            .scope()
            .with_columns(&ctx, |columns| { eval_func("IFNULL", &[], columns, None) })
            .is_err());
        assert!(ctx.reads.take().is_empty());
    }
    let nested = Expr::Func {
        name: "IFNULL".to_owned(),
        args: vec![
            Expr::Null,
            Expr::Func {
                name: "IFNULL".to_owned(),
                args: vec![Expr::Null, Expr::Int("7".to_owned())],
                origin_position: 0,
            },
        ],
        origin_position: 0,
    };
    assert_eq!(
        crate::eval_in(&nested, &crate::NoColumns).unwrap(),
        Datum::Int(7)
    );
}

#[cfg(test)]
#[test]
fn if_workers_keep_ordinary_truth_domains_lazy_identity_and_scope() {
    use crate::constant::{Constant, ParamMarker};
    use crate::expression::Expression;
    use crate::scalar_function::ScalarFunction;
    use std::cell::RefCell;
    use tidb_datatype::{
        BinaryJSON, BinaryLiteral, Collation, CoreTime, Decimal, FieldType, FieldTypeCode,
        MySqlDuration, MysqlEnum, MysqlSet, Time, TimeType, VectorFloat32,
    };

    struct Demand {
        values: [Datum; 3],
        reads: RefCell<Vec<usize>>,
        fail: Option<usize>,
    }
    impl Columns for Demand {
        fn get(&self, path: &[String]) -> Option<Datum> {
            let index = path[0].parse::<usize>().unwrap();
            self.param_value(index).ok()
        }
        fn param_value(&self, index: usize) -> Result<Datum, EvalError> {
            self.reads.borrow_mut().push(index);
            if self.fail == Some(index) {
                return Err(EvalError::Unsupported("IF demanded child"));
            }
            Ok(self.values[index].clone())
        }
        fn time_zone(&self) -> tidb_datatype::SessionTimeZone {
            panic!("ordinary IF has no timezone demand")
        }
        fn date_modes(&self) -> tidb_datatype::DateModes {
            panic!("ordinary IF has no date-mode demand")
        }
        fn truncate_level(&self) -> crate::ErrorLevel {
            panic!("ordinary truth coercion discards conversion events")
        }
        fn append_warning(&self, _: u16, _: &str) {
            panic!("ordinary IF must not gain PB truth warnings")
        }
    }
    fn frame(value: &Datum) -> Option<Vec<u8>> {
        let crate::tikv::EvaluatedArgs::Bytes(bytes) =
            crate::tikv::prepare_datum_identity_args(value).unwrap()
        else {
            panic!("expected actual nullable identity")
        };
        bytes
    }
    fn resource(result: Result<Datum, EvalError>) {
        let error = result.expect_err("IF must retain the existing zero-slot owner");
        let EvalError::ExpressionAdapterFailure(failure) = error else {
            panic!("{error:?}")
        };
        assert_eq!(
            failure.class(),
            crate::ExpressionAdapterFailureClass::PoolResource
        );
        assert_eq!(
            failure.origin(),
            crate::ExpressionAdapterFailureOrigin::Pool
        );
    }
    let ast_args = [0, 1, 2].map(|index| Expr::Column(vec![index.to_string()]));
    let typed_args = [0, 1, 2].map(|order| {
        let mut constant = Constant::default();
        constant.param_marker = Some(ParamMarker { order });
        Expression::Constant(constant)
    });
    let mut typed = ScalarFunction::new(
        tidb_ast::CiString::new("if"),
        FieldType::new(FieldTypeCode::LongLong),
        typed_args.to_vec(),
    );
    typed.ret_type = None;
    let empty = tidb_chunk::mutrow::MutRow::from_datums(&[]);
    let evaluate = |is_typed, columns: &dyn Columns| {
        if is_typed {
            typed.eval(columns, empty.to_row())
        } else {
            eval_func("IF", &ast_args, columns, None)
        }
    };
    let mut zero_vector = VectorFloat32::init(2);
    zero_vector.elements_mut().copy_from_slice(&[-0.0, 0.0]);
    let mut nan_vector = VectorFloat32::init(1);
    nan_vector.elements_mut()[0] = f32::from_bits(0x7fc01234);
    let cases = vec![
        (Datum::Null, Ok(false)),
        (Datum::Int(0), Ok(false)),
        (Datum::Int(-7), Ok(true)),
        (Datum::UInt(0), Ok(false)),
        (Datum::UInt(u64::MAX), Ok(true)),
        (Datum::Real(-0.0), Ok(false)),
        (Datum::Real(0.1), Ok(true)),
        (Datum::Real(f64::NAN), Ok(true)),
        (Datum::Real(f64::INFINITY), Ok(true)),
        (Datum::Float32(-0.0), Ok(false)),
        (Datum::Float32(1e-100), Ok(true)),
        (
            Datum::Decimal(Decimal::from_raw_parts(true, b"000".to_vec(), 2, 4)),
            Ok(false),
        ),
        (
            Datum::Decimal(Decimal::from_raw_parts(true, Vec::new(), 0, 0)),
            Ok(false),
        ),
        (
            Datum::Decimal(Decimal::from_raw_parts(false, b"000001".to_vec(), 2, 9)),
            Ok(true),
        ),
        (Datum::new_string(""), Ok(false)),
        (Datum::new_string("abc"), Ok(false)),
        (Datum::new_string("1abc"), Ok(true)),
        (Datum::new_string(".1"), Ok(true)),
        (Datum::new_string("0.0"), Ok(false)),
        (Datum::new_bytes(b"1tail".to_vec()), Ok(true)),
        (Datum::new_bytes(vec![0xff]), Err(())),
        (
            Datum::new_collation_string(vec![0xff], Collation::GbkBin),
            Err(()),
        ),
        (
            Datum::BinaryLiteral(BinaryLiteral::from(vec![0])),
            Ok(false),
        ),
        (
            Datum::BinaryLiteral(BinaryLiteral::from(vec![1; 9])),
            Ok(true),
        ),
        (Datum::Bit(BinaryLiteral::from(vec![0, 1])), Ok(true)),
        (
            Datum::Enum(MysqlEnum::new(b"nonempty".to_vec(), 0), Collation::Binary),
            Ok(false),
        ),
        (
            Datum::Set(MysqlSet::new(Vec::new(), 64), Collation::Binary),
            Ok(true),
        ),
        (
            Datum::Time(Time::from_raw_parts(
                CoreTime::from_raw(0),
                TimeType::DateTime,
                0,
            )),
            Ok(false),
        ),
        (
            Datum::Time(Time::from_raw_parts(
                CoreTime::from_date(2024, 1, 1, 0, 0, 0, 0),
                TimeType::DateTime,
                0,
            )),
            Ok(true),
        ),
        (
            Datum::Duration(MySqlDuration::from_raw_parts(0, 3)),
            Ok(false),
        ),
        (
            Datum::Duration(MySqlDuration::from_raw_parts(1, 3)),
            Ok(true),
        ),
        (Datum::Json(BinaryJSON::parse("0").unwrap()), Ok(false)),
        (Datum::Json(BinaryJSON::parse("false").unwrap()), Ok(true)),
        (Datum::Json(BinaryJSON::parse("null").unwrap()), Ok(true)),
        // Native VectorFloat32::is_zero_value means EMPTY, not all-zero lanes.
        (Datum::VectorFloat32(VectorFloat32::default()), Ok(false)),
        (Datum::VectorFloat32(zero_vector), Ok(true)),
        (Datum::VectorFloat32(nan_vector), Ok(true)),
        (Datum::Raw(vec![1]), Err(())),
        (Datum::MinNotNull, Err(())),
        (Datum::MaxValue, Err(())),
    ];
    for slots in [1, 0] {
        let owner = crate::AsciiPoolOwner::new(
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
        .unwrap();
        let execution = owner.begin_execution().unwrap();
        for (condition, truth) in &cases {
            let ctx = Demand {
                values: [
                    condition.clone(),
                    Datum::Raw(vec![0xfe, 0]),
                    Datum::Raw(vec![0xff, 0]),
                ],
                reads: RefCell::new(Vec::new()),
                fail: truth.ok().map(|yes| if yes { 2 } else { 1 }),
            };
            for is_typed in [false, true] {
                let result = execution
                    .scope()
                    .with_columns(&ctx, |columns| evaluate(is_typed, columns));
                if truth.is_err() {
                    assert!(matches!(
                        result,
                        Err(EvalError::Unsupported("truth coercion of a non-SQL datum"))
                    ));
                    assert_eq!(ctx.reads.take(), vec![0]);
                } else if slots == 0 {
                    resource(result);
                    assert_eq!(ctx.reads.take(), vec![0]);
                } else {
                    let branch = if truth == &Ok(true) { 1 } else { 2 };
                    assert_eq!(frame(&result.unwrap()), frame(&ctx.values[branch]));
                    assert_eq!(ctx.reads.take(), vec![0, branch]);
                }
            }
        }
        for value in [
            Datum::Null,
            Datum::Float32(f64::from_bits(0x7ff8000012345678)),
            Datum::Decimal(
                Decimal::from_raw_parts(true, b"00010049".to_vec(), 2, 4)
                    .with_declared_shape(12, 2),
            ),
            Datum::new_collation_string(vec![0xff], Collation::GbkBin),
        ] {
            for yes in [false, true] {
                let ctx = Demand {
                    values: [Datum::Int(i64::from(yes)), value.clone(), value.clone()],
                    reads: RefCell::new(Vec::new()),
                    fail: Some(if yes { 2 } else { 1 }),
                };
                for is_typed in [false, true] {
                    let result = execution
                        .scope()
                        .with_columns(&ctx, |columns| evaluate(is_typed, columns));
                    if slots == 1 {
                        assert_eq!(frame(&result.unwrap()), frame(&value));
                    } else {
                        resource(result);
                    }
                    assert_eq!(
                        ctx.reads.take(),
                        if slots == 1 {
                            vec![0, if yes { 1 } else { 2 }]
                        } else {
                            vec![0]
                        }
                    );
                }
            }
        }
        for failed in [0, 1, 2] {
            let ctx = Demand {
                values: [Datum::Int(i64::from(failed != 2)), Datum::Null, Datum::Null],
                reads: RefCell::new(Vec::new()),
                fail: Some(failed),
            };
            for is_typed in [false, true] {
                let result = execution
                    .scope()
                    .with_columns(&ctx, |columns| evaluate(is_typed, columns));
                if failed == 0 || slots == 1 {
                    assert!(
                        matches!(result, Err(EvalError::Unsupported(message)) if message == if is_typed { "IF demanded child" } else { "unknown column" })
                    );
                    assert_eq!(
                        ctx.reads.take(),
                        if failed == 0 {
                            vec![0]
                        } else {
                            vec![0, failed]
                        }
                    );
                } else {
                    resource(result);
                    assert_eq!(ctx.reads.take(), vec![0]);
                }
            }
        }
        let ctx = Demand {
            values: [Datum::Int(1), Datum::Int(2), Datum::Int(3)],
            reads: RefCell::new(Vec::new()),
            fail: None,
        };
        assert!(matches!(
            execution.scope().with_columns(&ctx, |columns| eval_func(
                "IF",
                &ast_args[..2],
                columns,
                None
            )),
            Err(EvalError::Unsupported("bad IF arguments"))
        ));
        assert!(ctx.reads.take().is_empty());
        let malformed = ScalarFunction::new(
            tidb_ast::CiString::new("if"),
            FieldType::new(FieldTypeCode::LongLong),
            typed_args[..2].to_vec(),
        );
        assert!(execution
            .scope()
            .with_columns(&ctx, |columns| malformed.eval(columns, empty.to_row()))
            .is_err());
        assert_eq!(ctx.reads.take(), vec![0, 1]);
    }
    let nested = Expr::Func {
        name: "IF".to_owned(),
        origin_position: 0,
        args: vec![
            Expr::Null,
            Expr::Column(vec!["dead".to_owned()]),
            Expr::Func {
                name: "IF".to_owned(),
                origin_position: 0,
                args: vec![
                    Expr::Int("1".to_owned()),
                    Expr::Int("7".to_owned()),
                    Expr::Column(vec!["dead".to_owned()]),
                ],
            },
        ],
    };
    assert_eq!(
        crate::eval_in(&nested, &crate::NoColumns).unwrap(),
        Datum::Int(7)
    );
}

#[cfg(test)]
#[test]
fn coalesce_workers_keep_lazy_borrowed_identity_exhaustion_and_scope() {
    use crate::constant::{Constant, ParamMarker};
    use crate::expression::Expression;
    use crate::scalar_function::ScalarFunction;
    use std::cell::RefCell;
    use tidb_datatype::{
        BinaryJSON, BinaryLiteral, Collation, CoreTime, Decimal, FieldType, FieldTypeCode,
        MySqlDuration, MysqlEnum, MysqlSet, Time, TimeType, VectorFloat32,
    };

    struct Demand {
        values: Vec<Datum>,
        reads: RefCell<Vec<usize>>,
        fail: Option<usize>,
    }
    impl Columns for Demand {
        fn get(&self, path: &[String]) -> Option<Datum> {
            self.param_value(path[0].parse().unwrap()).ok()
        }
        fn param_value(&self, index: usize) -> Result<Datum, EvalError> {
            self.reads.borrow_mut().push(index);
            if self.fail == Some(index) {
                return Err(EvalError::Unsupported("COALESCE demanded child"));
            }
            Ok(self.values[index].clone())
        }
        fn time_zone(&self) -> tidb_datatype::SessionTimeZone {
            panic!("COALESCE selection has no timezone demand")
        }
        fn date_modes(&self) -> tidb_datatype::DateModes {
            panic!("COALESCE selection has no date-mode demand")
        }
        fn truncate_level(&self) -> crate::ErrorLevel {
            panic!("COALESCE must not coerce candidates")
        }
        fn append_warning(&self, _: u16, _: &str) {
            panic!("COALESCE identity must not warn")
        }
    }
    fn frame(value: &Datum) -> Option<Vec<u8>> {
        let crate::tikv::EvaluatedArgs::Bytes(bytes) =
            crate::tikv::prepare_datum_identity_args(value).unwrap()
        else {
            panic!("expected actual nullable identity")
        };
        bytes
    }
    fn resource(result: Result<Datum, EvalError>) {
        let error =
            result.expect_err("COALESCE must retain the zero-slot owner even at exhaustion");
        let EvalError::ExpressionAdapterFailure(failure) = error else {
            panic!("{error:?}")
        };
        assert_eq!(
            failure.class(),
            crate::ExpressionAdapterFailureClass::PoolResource
        );
        assert_eq!(
            failure.origin(),
            crate::ExpressionAdapterFailureOrigin::Pool
        );
    }
    let empty = tidb_chunk::mutrow::MutRow::from_datums(&[]);
    let evaluate = |mode, values: &[Datum], columns: &dyn Columns| {
        if mode == 0 {
            let args = (0..values.len())
                .map(|index| Expr::Column(vec![index.to_string()]))
                .collect::<Vec<_>>();
            eval_func("COALESCE", &args, columns, None)
        } else if mode == 1 {
            let args = (0..values.len())
                .map(|index| {
                    let mut constant = Constant::default();
                    constant.param_marker = Some(ParamMarker {
                        order: index as i64,
                    });
                    Expression::Constant(constant)
                })
                .collect();
            let mut function = ScalarFunction::new(
                tidb_ast::CiString::new("coalesce"),
                FieldType::new(FieldTypeCode::LongLong),
                args,
            );
            function.ret_type = None;
            function.eval(columns, empty.to_row())
        } else {
            // Already evaluated arguments enter the borrowed helper; no caller
            // search, selected-value clone, or dead-suffix encoding occurs here.
            eval_func_values_in("COALESCE", values, columns).unwrap()
        }
    };
    let mut vector = VectorFloat32::init(2);
    vector
        .elements_mut()
        .copy_from_slice(&[-0.0, f32::from_bits(0x7fc01234)]);
    let candidates = vec![
        Datum::MinNotNull,
        Datum::MaxValue,
        Datum::Int(0),
        Datum::UInt(u64::MAX),
        Datum::Real(f64::from_bits(0x7ff8000012345678)),
        Datum::Real(-0.0),
        Datum::Float32(1.00000049),
        Datum::Decimal(
            Decimal::from_raw_parts(true, b"00010049".to_vec(), 2, 4).with_declared_shape(12, 2),
        ),
        Datum::Decimal(Decimal::from_raw_parts(true, Vec::new(), 0, 0)),
        Datum::new_string(""),
        Datum::new_collation_string(vec![0xff, 0], Collation::GbkBin),
        Datum::new_bytes(vec![0xff, 0]),
        Datum::Raw(vec![0xff, 0]),
        Datum::BinaryLiteral(BinaryLiteral::from(vec![0, 0xff])),
        Datum::Bit(BinaryLiteral::from(vec![0, 0xff])),
        Datum::Enum(MysqlEnum::new(vec![0xff], 0), Collation::Binary),
        Datum::Set(MysqlSet::new(vec![0xff], u64::MAX), Collation::GbkBin),
        Datum::Time(Time::from_raw_parts(
            CoreTime::from_raw(u64::MAX),
            TimeType::Date,
            9,
        )),
        Datum::Duration(MySqlDuration::from_raw_parts(-123456789, -2)),
        Datum::Json(BinaryJSON::parse("null").unwrap()),
        Datum::VectorFloat32(vector),
    ];
    for slots in [1, 0] {
        let owner = crate::AsciiPoolOwner::new(
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
        .unwrap();
        let execution = owner.begin_execution().unwrap();
        for candidate in &candidates {
            for prefix in [0, 2] {
                let mut values = vec![Datum::Null; prefix];
                values.push(candidate.clone());
                values.push(Datum::Raw(vec![0xff]));
                let ctx = Demand {
                    values,
                    reads: RefCell::new(Vec::new()),
                    fail: Some(prefix + 1),
                };
                let before = ctx.values.iter().map(frame).collect::<Vec<_>>();
                for mode in 0..3 {
                    let result = execution
                        .scope()
                        .with_columns(&ctx, |columns| evaluate(mode, &ctx.values, columns));
                    if slots == 1 {
                        assert_eq!(frame(&result.unwrap()), frame(candidate));
                    } else {
                        resource(result);
                    }
                    assert_eq!(
                        ctx.reads.take(),
                        if mode == 2 {
                            vec![]
                        } else if slots == 1 {
                            (0..=prefix).collect::<Vec<_>>()
                        } else {
                            vec![0]
                        }
                    );
                    assert_eq!(ctx.values.iter().map(frame).collect::<Vec<_>>(), before);
                }
            }
        }
        for count in [0, 1, 3, 64] {
            let ctx = Demand {
                values: vec![Datum::Null; count],
                reads: RefCell::new(Vec::new()),
                fail: None,
            };
            for mode in 0..3 {
                let result = execution
                    .scope()
                    .with_columns(&ctx, |columns| evaluate(mode, &ctx.values, columns));
                if slots == 1 {
                    assert_eq!(result.unwrap(), Datum::Null);
                } else {
                    resource(result);
                }
                assert_eq!(
                    ctx.reads.take(),
                    if mode == 2 || count == 0 {
                        vec![]
                    } else if slots == 1 {
                        (0..count).collect::<Vec<_>>()
                    } else {
                        vec![0]
                    }
                );
            }
        }
        for failed in [0, 1] {
            let ctx = Demand {
                values: vec![Datum::Null, Datum::Int(7), Datum::Int(8)],
                reads: RefCell::new(Vec::new()),
                fail: Some(failed),
            };
            for mode in 0..2 {
                let result = execution
                    .scope()
                    .with_columns(&ctx, |columns| evaluate(mode, &ctx.values, columns));
                if slots == 1 || failed == 0 {
                    assert!(
                        matches!(result, Err(EvalError::Unsupported(message)) if message == if mode == 0 { "unknown column" } else { "COALESCE demanded child" })
                    );
                    assert_eq!(ctx.reads.take(), (0..=failed).collect::<Vec<_>>());
                } else {
                    resource(result);
                    assert_eq!(ctx.reads.take(), vec![0]);
                }
            }
        }
        if slots == 1 {
            // An encoded suffix this large exceeds this invocation's budgets.
            // It is already materialized but remains completely undemanded.
            let values = [
                Datum::Null,
                Datum::Int(7),
                Datum::Raw(vec![0; 5 * 1024 * 1024]),
            ];
            assert_eq!(
                execution
                    .scope()
                    .with_columns(&crate::NoColumns, |columns| eval_func_values_in(
                        "COALESCE", &values, columns
                    )
                    .unwrap())
                    .unwrap(),
                Datum::Int(7)
            );
        }
    }
    let nested = Expr::Func {
        name: "COALESCE".to_owned(),
        origin_position: 0,
        args: vec![
            Expr::Null,
            Expr::Func {
                name: "COALESCE".to_owned(),
                args: vec![],
                origin_position: 0,
            },
            Expr::Int("7".to_owned()),
            Expr::Column(vec!["dead".to_owned()]),
        ],
    };
    assert_eq!(
        crate::eval_in(&nested, &crate::NoColumns).unwrap(),
        Datum::Int(7)
    );
}

#[cfg(test)]
#[test]
fn nullif_workers_keep_eager_operands_comparison_policy_and_left_identity() {
    use crate::constant::{Constant, ParamMarker};
    use crate::expression::Expression;
    use crate::scalar_function::ScalarFunction;
    use std::cell::RefCell;
    use tidb_datatype::{Collation, CoreTime, FieldType, FieldTypeCode, Time, TimeType};
    struct Demand {
        values: Vec<Datum>,
        fail: Option<usize>,
        level: crate::ErrorLevel,
        events: RefCell<Vec<&'static str>>,
        warnings: RefCell<Vec<(u16, String)>>,
    }
    impl Columns for Demand {
        fn get(&self, path: &[String]) -> Option<Datum> {
            self.param_value(path[0].parse().unwrap()).ok()
        }
        fn param_value(&self, index: usize) -> Result<Datum, EvalError> {
            self.events
                .borrow_mut()
                .push(["left", "right", "extra"][index]);
            if self.fail == Some(index) {
                return Err(EvalError::Unsupported("NULLIF demanded child"));
            }
            Ok(self.values[index].clone())
        }
        fn div_precision_increment(&self) -> u32 {
            self.events.borrow_mut().push("precision");
            4
        }
        fn truncate_level(&self) -> crate::ErrorLevel {
            self.events.borrow_mut().push("policy");
            self.level
        }
        fn append_warning(&self, code: u16, message: &str) {
            self.events.borrow_mut().push("warning");
            self.warnings.borrow_mut().push((code, message.to_owned()));
        }
        fn time_zone(&self) -> tidb_datatype::SessionTimeZone {
            panic!("these NULLIF comparisons do not demand a timezone")
        }
        fn date_modes(&self) -> tidb_datatype::DateModes {
            panic!("these NULLIF comparisons do not demand date modes")
        }
    }
    fn context(values: Vec<Datum>, fail: Option<usize>, level: crate::ErrorLevel) -> Demand {
        Demand {
            values,
            fail,
            level,
            events: RefCell::new(Vec::new()),
            warnings: RefCell::new(Vec::new()),
        }
    }
    fn frame(value: &Datum) -> Option<Vec<u8>> {
        let crate::tikv::EvaluatedArgs::Bytes(bytes) =
            crate::tikv::prepare_datum_identity_args(value).unwrap()
        else {
            panic!("expected actual nullable identity")
        };
        bytes
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
    let empty = tidb_chunk::mutrow::MutRow::from_datums(&[]);
    let evaluate = |mode, values: &[Datum], first_type: &FieldType, columns: &dyn Columns| {
        if mode == 0 {
            let args = (0..values.len())
                .map(|index| Expr::Column(vec![index.to_string()]))
                .collect::<Vec<_>>();
            Some(eval_func("NULLIF", &args, columns, None))
        } else if mode == 1 {
            let args = (0..values.len())
                .map(|index| {
                    let field = if index == 0 {
                        first_type.clone()
                    } else {
                        FieldType::new(FieldTypeCode::Double)
                    };
                    let mut constant = Constant::new(Datum::Null, field);
                    constant.param_marker = Some(ParamMarker {
                        order: index as i64,
                    });
                    Expression::Constant(constant)
                })
                .collect::<Vec<_>>();
            let inferred = crate::rewriter::result_type::builtin_return_type("nullif", &args);
            if !args.is_empty() {
                assert_eq!(inferred.as_ref(), Some(first_type));
            }
            Some(
                ScalarFunction::new(
                    tidb_ast::CiString::new("nullif"),
                    inferred.unwrap_or_else(|| first_type.clone()),
                    args,
                )
                .eval(columns, empty.to_row()),
            )
        } else {
            eval_func_values_in("NULLIF", values, columns)
        }
    };
    let integer = FieldType::new(FieldTypeCode::LongLong);
    let mut text_type = FieldType::new(FieldTypeCode::VarString).with_collation(Collation::GbkBin);
    text_type.set_flen(13);
    let text = Datum::new_collation_string(b"Keep".to_vec(), Collation::GbkBin);
    let mut time_type = FieldType::new(FieldTypeCode::Datetime);
    time_type.set_decimal(4);
    let time = Datum::Time(Time::from_raw_parts(
        CoreTime::from_date(2024, 1, 2, 3, 4, 5, 123456),
        TimeType::DateTime,
        4,
    ));
    let pool = owner(1);
    let execution = pool.begin_execution().unwrap();
    for (left, right, field, expected) in [
        (Datum::Int(1), Datum::Int(1), integer.clone(), Datum::Null),
        (Datum::Null, Datum::Int(9), integer.clone(), Datum::Null),
        (Datum::Int(7), Datum::Null, integer.clone(), Datum::Int(7)),
        (Datum::Int(7), Datum::Int(8), integer.clone(), Datum::Int(7)),
        (
            Datum::Int(150),
            Datum::Real(150.0),
            integer.clone(),
            Datum::Null,
        ),
        (text.clone(), Datum::new_string("other"), text_type, text),
        (time.clone(), Datum::Null, time_type, time),
    ] {
        let ctx = context(vec![left, right], None, crate::ErrorLevel::Error);
        for mode in 0..3 {
            let result = execution
                .scope()
                .with_columns(&ctx, |c| evaluate(mode, &ctx.values, &field, c))
                .unwrap()
                .unwrap();
            assert_eq!(frame(&result), frame(&expected));
            assert_eq!(
                ctx.events.take(),
                if mode == 2 {
                    vec!["precision"]
                } else {
                    vec!["left", "right", "precision"]
                }
            );
            assert!(ctx.warnings.take().is_empty());
        }
    }
    // NULL on the left does not suppress the original eager RHS evaluation.
    for failed in [0, 1] {
        let ctx = context(
            vec![Datum::Null, Datum::Int(1)],
            Some(failed),
            crate::ErrorLevel::Error,
        );
        for mode in 0..2 {
            let result = execution
                .scope()
                .with_columns(&ctx, |c| evaluate(mode, &ctx.values, &integer, c))
                .unwrap();
            assert!(
                matches!(result, Err(EvalError::Unsupported(message)) if message == if mode == 0 { "unknown column" } else { "NULLIF demanded child" })
            );
            assert_eq!(
                ctx.events.take(),
                if failed == 0 {
                    vec!["left"]
                } else {
                    vec!["left", "right"]
                }
            );
        }
    }
    for count in [0, 1, 3] {
        let ctx = context(
            vec![Datum::Null, Datum::Int(1), Datum::Int(2)],
            None,
            crate::ErrorLevel::Error,
        );
        for mode in 0..3 {
            let result = execution
                .scope()
                .with_columns(&ctx, |c| evaluate(mode, &ctx.values[..count], &integer, c));
            if mode == 2 {
                assert!(result.is_none());
            } else {
                assert!(result.unwrap().is_err());
            }
            assert_eq!(
                ctx.events.take(),
                if mode == 2 {
                    vec![]
                } else {
                    ["left", "right", "extra"][..count].to_vec()
                }
            );
        }
    }
    let extra = context(
        vec![Datum::Null, Datum::Int(1), Datum::Int(2)],
        Some(2),
        crate::ErrorLevel::Error,
    );
    for mode in 0..2 {
        let result = execution
            .scope()
            .with_columns(&extra, |c| evaluate(mode, &extra.values, &integer, c))
            .unwrap();
        assert!(
            matches!(result, Err(EvalError::Unsupported(message)) if message == if mode == 0 { "unknown column" } else { "NULLIF demanded child" })
        );
        assert_eq!(extra.events.take(), vec!["left", "right", "extra"]);
    }
    for slots in [1, 0] {
        let pool = owner(slots);
        let execution = pool.begin_execution().unwrap();
        for level in [crate::ErrorLevel::Warn, crate::ErrorLevel::Error] {
            let ctx = context(vec![Datum::new_string("1tail"), Datum::Int(2)], None, level);
            for mode in 0..3 {
                let result = execution
                    .scope()
                    .with_columns(&ctx, |c| {
                        evaluate(
                            mode,
                            &ctx.values,
                            &FieldType::new(FieldTypeCode::VarString),
                            c,
                        )
                    })
                    .unwrap();
                let mut events = if mode == 2 {
                    vec!["precision", "policy"]
                } else {
                    vec!["left", "right", "precision", "policy"]
                };
                if level == crate::ErrorLevel::Error {
                    assert!(
                        matches!(result, Err(EvalError::TruncatedWrongValue(message)) if message == "Truncated incorrect DOUBLE value: '1tail'")
                    );
                    assert!(ctx.warnings.take().is_empty());
                } else {
                    events.push("warning");
                    assert_eq!(
                        ctx.warnings.take(),
                        vec![(1292, "Truncated incorrect DOUBLE value: '1tail'".to_owned())]
                    );
                    if slots == 1 {
                        assert_eq!(result.unwrap(), Datum::new_string("1tail"));
                    } else {
                        // Existing comparison C4 refuses first: this is NOT
                        // evidence that the new NULLIF selector was reached.
                        assert!(
                            matches!(result, Err(EvalError::ExpressionAdapterFailure(failure))
                            if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource
                                && failure.origin() == crate::ExpressionAdapterFailureOrigin::Pool)
                        );
                    }
                }
                assert_eq!(ctx.events.take(), events);
            }
        }
    }
    // No explicit owner: keep the original callback context and getter count.
    let ctx = context(
        vec![Datum::Int(7), Datum::Int(8)],
        None,
        crate::ErrorLevel::Error,
    );
    for mode in 0..3 {
        assert_eq!(
            evaluate(mode, &ctx.values, &integer, &ctx)
                .unwrap()
                .unwrap(),
            Datum::Int(7)
        );
        assert_eq!(
            ctx.events.take(),
            if mode == 2 {
                vec!["precision"]
            } else {
                vec!["left", "right", "precision"]
            }
        );
    }
}
