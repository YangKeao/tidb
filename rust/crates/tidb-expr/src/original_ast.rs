// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Independent original recursive AST bodies, preserved before production retirement.
//! Only tests may call these bodies. No shared AST host imports this module.
use crate::func::{
    benchmark_loop_count, ensure_benchmark_eval_type, eval_func_values_in, negate_if,
};
use crate::string_fn::{position_in, trim_value_in};
use crate::*;
use tidb_ast::{BinaryOp, CastStyle, GetFormatSelector, IsTarget};
thread_local! { static ENTRIES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) }; }
pub(crate) fn take_entries() -> usize {
    ENTRIES.with(|v| v.replace(0))
}

pub(crate) fn eval_in(expr: &Expr, cols: &dyn Columns) -> Result<Datum, EvalError> {
    ENTRIES.with(|v| v.set(v.get() + 1));
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
            Datum::Null => crate::tikv::eval_charset_null_in(cols),
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
                convert_charset::convert_using_in(
                    &string_value,
                    &source,
                    &charset.to_ascii_lowercase(),
                    cols,
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
            let value = tikv::wildcard_value::ast(
                *ilike,
                &arguments.0,
                &arguments.1,
                *escape,
                tikv::wildcard_value::default_budget(),
            )?;
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
            crate::tikv::eval_case_in(
                cols,
                when_clauses.len(),
                else_clause.is_some(),
                // Simple CASE evaluates its base once, including zero-WHEN
                // helper shapes, inside the first guarded preparation.
                |original| {
                    value
                        .as_deref()
                        .map(|base| eval_in(base, original))
                        .transpose()
                },
                |index, base, selected| {
                    let condition = eval_in(&when_clauses[index].0, selected)?;
                    match base {
                        Some(base) => {
                            // Even a NULL base still demands each WHEN. Keep
                            // ordinary equality's exact Int(1) match policy.
                            let equal = crate::ops::eval_binary_in(
                                tidb_ast::BinaryOp::Eq,
                                base.clone(),
                                condition,
                                selected,
                            )?;
                            Ok(match equal {
                                Datum::Null => None,
                                _ => Some(equal == Datum::Int(1)),
                            })
                        }
                        None => truthy_of(&condition),
                    }
                },
                |index, selected| {
                    let result = if index == when_clauses.len() {
                        else_clause
                            .as_deref()
                            .ok_or(EvalError::Unsupported("missing CASE ELSE"))?
                    } else {
                        &when_clauses[index].1
                    };
                    eval_in(result, selected)
                },
            )
        }
        _ => Err(EvalError::Unsupported("unsupported expression")),
    }
}

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
        return crate::tikv::eval_date_add_default_in(cols, unit, &date_val, &amount_val, sign);
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
                    crate::convert_charset::to_binary_by_collation_in(&value, cols)?
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
            .map(|value| crate::convert_charset::to_binary_by_collation_in(value, cols))
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
        let result = tidb_query_expr::native_in_ast_rows(
            &lv,
            list.len(),
            |index| match &list[index] {
                Expr::Row(right_items) => right_items
                    .iter()
                    .map(|e| eval_in(e, cols))
                    .collect::<Result<Vec<_>, _>>()
                    .map(tidb_query_expr::NativeInRowCandidate::Values),
                _ => Ok(tidb_query_expr::NativeInRowCandidate::NotRow),
            },
            |left, right| {
                crate::ops::eval_comparison_values_in(
                    BinaryOp::Eq,
                    left.clone(),
                    right.clone(),
                    crate::ops::DERIVATION_FREE_COLLATION,
                    crate::ops::Operands::LITERALS,
                    cols,
                )
                .map(|computed| crate::tikv::in_eq_observation(&computed))
            },
        )
        .map_err(crate::tikv::in_row_control_error)?;
        return negate_if(crate::tikv::in_control_datum(result), not, cols);
    }
    let v = eval_in(expr, cols)?;
    let result = tidb_query_expr::native_in_ast_scalar(list.len(), |index| {
        let iv = eval_in(&list[index], cols)?;
        crate::ops::eval_binary_in(BinaryOp::Eq, v.clone(), iv, cols)
            .map(|computed| crate::tikv::in_eq_observation(&computed))
    })?;
    negate_if(crate::tikv::in_control_datum(result), not, cols)
}
