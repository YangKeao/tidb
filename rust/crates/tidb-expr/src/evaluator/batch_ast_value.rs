// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Original AST value policies over already demanded operands and genuine metadata.
//! This module cannot evaluate an expression child.
use crate::func::{eval_func_values_in, negate_if};
use crate::*;
use tidb_ast::{CastStyle, Expr, GetFormatSelector, IsTarget};
pub(super) fn finish(
    expr: &Expr,
    values: Vec<Datum>,
    cols: &dyn Columns,
    raw_string: bool,
) -> Result<Datum, EvalError> {
    let mut vals = values.into_iter();
    macro_rules! next {
        () => {
            vals.next().expect("validated actual AST value arity")
        };
    }
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
        Expr::RawString(s) if raw_string => Ok(Datum::new_string(s.clone())),
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
        Expr::Float(f) => Ok(Datum::Real(*f)),
        Expr::Hex(digits) => hex_literal_value(digits),
        Expr::Bit(digits) => bit_literal_value(digits),
        Expr::CharsetBinary { .. } => Ok(next!()),
        Expr::Null => Ok(Datum::Null),
        Expr::Column(path) => cols
            .get(path)
            .ok_or(EvalError::Unsupported("unknown column")),
        Expr::UserVar(name) => Ok(cols.get_uservar(name).unwrap_or(Datum::Null)),
        Expr::Assign { name, .. } => {
            let v = next!();
            if v != Datum::Null {
                cols.set_uservar(name, v.clone());
            }
            Ok(v)
        }
        Expr::SysVar { scope, name } => cols
            .sysvar(*scope, name)
            .ok_or(EvalError::Unsupported("unknown system variable")),
        Expr::Paren(_) => Ok(next!()),
        Expr::Unary(tidb_ast::UnaryOp::Plus, _) => Ok(next!()),
        Expr::Unary(op, _) => eval_unary(*op, next!(), ops::Operand::Literal, cols),
        Expr::Binary(op, l, r) if matches!((&**l, &**r), (Expr::Row(_), Expr::Row(_))) => {
            let Expr::Row(left) = &**l else {
                unreachable!()
            };
            let left: Vec<_> = (0..left.len()).map(|_| next!()).collect();
            let right: Vec<_> = vals.by_ref().collect();
            row_compare_in(*op, &left, &right, cols)
        }
        Expr::Binary(op, l, r) => {
            let signed = [is_signed_binary_literal(l), is_signed_binary_literal(r)];
            let (left, right) =
                binary_literal::cast_signed_literal_operands(*op, next!(), next!(), signed);
            let unsigned_result =
                !matches!(*op, tidb_ast::BinaryOp::Minus) || !cols.no_unsigned_subtraction();
            let integer_unsigned = unsigned_result
                && (matches!(&left, Datum::UInt(_)) || matches!(&right, Datum::UInt(_)));
            eval_binary_with_div_precision(*op, left, right, cols.div_precision_increment(), cols)
                .map_err(|error| ast_binary_overflow_error(*op, l, r, integer_unsigned, error))
        }
        Expr::Func { name, args, .. } => eager_function(expr, name, args, vals.collect(), cols),
        Expr::Extract { unit, .. } => time_fn::extract::extract(unit, &next!(), None, cols),
        Expr::TimestampAdd { unit, .. } => {
            let vals = vec![Datum::new_string(unit.clone()), next!(), next!()];
            let vals = arg_eval_type::wrap_datetime_args("TIMESTAMPADD", vals, &[], cols)?;
            time_fn::add_sub::timestamp_add(&vals, cols)
        }
        Expr::GetFormat { selector, .. } => {
            let location = next!();
            let format_type = match selector {
                GetFormatSelector::Date => "DATE",
                GetFormatSelector::Time => "TIME",
                GetFormatSelector::Datetime => "DATETIME",
            };
            time_fn::get_format_ast_in(format_type, &location, cols)
        }
        Expr::Cast(cast)
            if matches!(
                cast.style,
                CastStyle::DateLiteral | CastStyle::TimeLiteral | CastStyle::TimestampLiteral
            ) =>
        {
            Err(EvalError::Unsupported("date/time/timestamp literal"))
        }
        Expr::Cast(cast) if cast.array => Err(EvalError::Unsupported("ARRAY cast type")),
        Expr::Cast(cast) => match next!() {
            Datum::Null => Ok(Datum::Null),
            v => cast::eval_cast(&cast.cast_type, v, None, cols),
        },
        Expr::ConvertUsing { expr, charset } => match next!() {
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
        Expr::Collate { .. } => Ok(next!()),
        Expr::Is { target, not, .. } => {
            let v = next!();
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
            not, ilike, escape, ..
        } => {
            let arguments = (next!(), next!());
            let value = tikv::wildcard_value::ast(
                *ilike,
                &arguments.0,
                &arguments.1,
                *escape,
                tikv::wildcard_value::default_budget(),
            )?;
            negate_if(value, *not, cols)
        }
        Expr::Regexp { not, .. } => {
            let arguments = (next!(), next!());
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
        Expr::WeightString { as_type, .. } => {
            let value = next!();
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
        _ => Err(EvalError::Unsupported("unsupported expression")),
    }
}

pub(super) fn eager_function(
    root: &Expr,
    name: &str,
    args: &[Expr],
    vals: Vec<Datum>,
    cols: &dyn Columns,
) -> Result<Datum, EvalError> {
    let name = name.to_ascii_uppercase();
    let function_key = Some(root as *const Expr as usize);
    if matches!(
        name.as_str(),
        "DATE_ADD" | "DATE_SUB" | "ADDDATE" | "SUBDATE"
    ) {
        let [_, Expr::Interval { unit, .. }] = args else {
            return Err(EvalError::Unsupported("DATE_ADD/DATE_SUB arguments"));
        };
        let mut vals = vals.into_iter();
        let date = vals.next().expect("actual date");
        let amount = vals.next().expect("actual amount");
        return crate::tikv::eval_date_add_default_in(
            cols,
            unit,
            &date,
            &amount,
            if matches!(name.as_str(), "DATE_SUB" | "SUBDATE") {
                -1
            } else {
                1
            },
        );
    }

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
