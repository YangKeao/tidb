// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Immutable facts and original eager scalar value-body policy, no expression children.
use super::*;

#[derive(Debug)]
pub(crate) struct EagerArgument {
    field_type: Option<FieldType>,
    const_level: ConstLevel,
    literal: bool,
}
impl EagerArgument {
    fn static_type(&self) -> Option<&FieldType> {
        self.field_type.as_ref()
    }
    fn const_level(&self) -> ConstLevel {
        self.const_level
    }
}

#[derive(Debug)]
pub(crate) struct EagerValueKernel {
    func_name: tidb_ast::CiString,
    overflow_expression: Option<super::math_diagnostic::MathDiagnostic>,
    ret_type: Option<FieldType>,
    session_read: Option<super::session_read::SessionRead>,
    args: Box<[EagerArgument]>,
    regexp_cache: crate::builtin_ext::BuiltinFuncCache<crate::regexp::CachedRegexp>,
    regexp_replace_instruction_cache:
        crate::builtin_ext::BuiltinFuncCache<Vec<crate::builtin_ext::regexp::ReplacementPart>>,
    json_modify_path_cache:
        crate::builtin_ext::BuiltinFuncCache<Option<Vec<crate::builtin_ext::JsonPath>>>,
}
impl EagerValueKernel {
    pub(crate) fn name(&self) -> &str {
        self.func_name.lowercase()
    }
    pub(crate) fn capture(function: &ScalarFunction) -> Self {
        let cache_facts = matches!(
            function.func_name.lowercase(),
            "regexp_substr"
                | "regexp_instr"
                | "regexp_replace"
                | "json_set"
                | "json_insert"
                | "json_replace"
        );
        Self {
            func_name: function.func_name.clone(),
            overflow_expression: None,
            ret_type: function.ret_type.clone(),
            session_read: super::session_read::SessionRead::for_function(function),
            args: function
                .args
                .iter()
                .map(|arg| EagerArgument {
                    field_type: arg.static_type().cloned(),
                    const_level: if cache_facts {
                        arg.const_level()
                    } else {
                        ConstLevel::default()
                    },
                    literal: matches!(arg, Expression::Constant(_)),
                })
                .collect(),
            regexp_cache: function.regexp_cache.share_for_invocation(),
            regexp_replace_instruction_cache: function
                .regexp_replace_instruction_cache
                .share_for_invocation(),
            json_modify_path_cache: function.json_modify_path_cache.share_for_invocation(),
        }
    }
    pub(crate) fn with_math_diagnostic(
        mut self,
        expression: Option<super::math_diagnostic::MathDiagnostic>,
    ) -> Self {
        self.overflow_expression = expression;
        self
    }
    pub(crate) fn source_math_error(&self, error: EvalError) -> EvalError {
        let value = match (self.name(), &error) {
            ("abs", EvalError::IntOverflow) => "BIGINT",
            ("cot" | "exp" | "pow" | "power", EvalError::FloatOverflow) => "DOUBLE",
            _ => return error,
        };
        match &self.overflow_expression {
            Some(expression) => match expression.render() {
                Ok(expression) => EvalError::DataOutOfRange { value, expression },
                Err(error) => error,
            },
            None => error,
        }
    }
    fn get_static_type(&self) -> Option<&FieldType> {
        self.ret_type.as_ref()
    }
    pub(crate) fn eval_in(
        &self,
        ctx: &dyn Columns,
        vals: Vec<Datum>,
        math_error: impl Fn(EvalError) -> EvalError,
    ) -> Result<Datum, EvalError> {
        assert_eq!(
            vals.len(),
            self.args.len(),
            "eager value-body arity contract"
        );
        if let Some(operation) = self.session_read {
            return operation.eval(ctx);
        }
        let name = self.func_name.lowercase();
        let upper = name.to_ascii_uppercase();
        // Go `builtinExtractDatetimeSig`/`builtinExtractDurationSig`: the
        // first argument is the unit keyword the parser stored as a VARCHAR
        // constant. Keep the value's static type when selecting the datetime,
        // duration or mixed DAY_* string signature.
        if upper == "EXTRACT" && vals.len() == 2 {
            return crate::tikv::eval_extract_scalar_values_in(
                ctx,
                &vals[0],
                &vals[1],
                self.args[1].static_type(),
            );
        }
        // `JSON_ARRAY`/`JSON_OBJECT`/`JSON_{SET,INSERT,REPLACE}`/
        // `JSON_ARRAY_{APPEND,INSERT}`: Go builds each value argument through
        // an implicit `CAST(... AS JSON)`, so a genuine BINARY-charset
        // argument must render as a JSON `Opaque` value rather than an
        // ordinary JSON string -- see `builtin_ext::json::dispatch_typed`.
        // Only this chunk path has each argument's static `FieldType`
        // (`Expression::static_type`); the row/AST evaluator
        // (`crate::func::eval_func`) still falls through to the untyped
        // `dispatch` below and stays the documented partial boundary.
        let arg_types: Vec<Option<FieldType>> =
            self.args.iter().map(|a| a.static_type().cloned()).collect();
        if upper == "HEX" {
            return crate::string_fn::hex_with_type(
                &vals,
                arg_types.first().and_then(Option::as_ref),
                ctx,
            );
        }
        // Go's `newBaseBuiltinFuncWithTp` argument-cast layer
        // (`crate::arg_eval_type`), the one point where a builtin's declared
        // argument eval types are imposed. Only this tier has the static
        // types Go's `WrapWithCastAsTime` reads, so only here can a `YEAR`
        // argument take Go's `ParseTimeFromYear`.
        let vals = crate::arg_eval_type::wrap_datetime_args(&upper, vals, &arg_types, ctx)?;
        let vals = crate::arg_eval_type::wrap_int_args(&upper, vals, &arg_types, ctx)?;
        let vals = crate::arg_eval_type::wrap_string_args(&upper, vals, &arg_types, ctx)?;
        if matches!(
            upper.as_str(),
            "REGEXP_SUBSTR" | "REGEXP_INSTR" | "REGEXP_REPLACE"
        ) {
            let cache_pattern = self
                .args
                .get(1)
                .is_some_and(|argument| argument.const_level() >= ConstLevel::ONLY_IN_CONTEXT);
            let match_type_index = match upper.as_str() {
                "REGEXP_SUBSTR" if self.args.len() == 5 => Some(4),
                "REGEXP_INSTR" if self.args.len() == 6 => Some(5),
                "REGEXP_REPLACE" if self.args.len() == 6 => Some(5),
                _ => None,
            };
            let cache_match_type = match_type_index
                .is_none_or(|index| self.args[index].const_level() >= ConstLevel::ONLY_IN_CONTEXT);
            let cache_replacement = upper == "REGEXP_REPLACE"
                && self
                    .args
                    .get(2)
                    .is_some_and(|argument| argument.const_level() >= ConstLevel::ONLY_IN_CONTEXT);
            return crate::builtin_ext::regexp::dispatch_with_cache_in(
                &upper,
                &vals,
                ctx.context_id(),
                cache_pattern && cache_match_type,
                cache_replacement,
                &self.regexp_cache,
                &self.regexp_replace_instruction_cache,
                ctx,
            )
            .expect("the native regexp family is registered");
        }
        if upper == "ORD" {
            return crate::string_fn::ord_with_type_in(
                &vals,
                arg_types.first().and_then(Option::as_ref),
                ctx,
            );
        }
        if matches!(upper.as_str(), "JSON_SET" | "JSON_INSERT" | "JSON_REPLACE")
            && self.args.get(1..).is_some_and(|arguments| {
                !arguments.is_empty()
                    && arguments
                        .iter()
                        .step_by(2)
                        .all(|argument| argument.const_level() >= ConstLevel::ONLY_IN_CONTEXT)
            })
        {
            return crate::builtin_ext::json_dispatch_typed_cached_in(
                &upper,
                &vals,
                &arg_types,
                &self.json_modify_path_cache,
                ctx,
            )
            .expect("the native JSON modification family is registered");
        }
        if let Some(result) =
            crate::builtin_ext::json_dispatch_typed_in(&upper, &vals, &arg_types, ctx)
        {
            return result;
        }
        // `ADDTIME`/`SUBTIME` are the other family Go types from the
        // ARGUMENTS' `FieldType`s rather than from their values: twelve
        // signatures over the `(tp1, tp2)` cross product, whose arms differ
        // in the result fsp and in whether the answer is NULL at all. Only
        // this tier has those types; `time_fn::dispatch` below still serves
        // the AST tier on Go's `default` branch.
        if matches!(upper.as_str(), "ADDTIME" | "SUBTIME") && vals.len() == 2 {
            use crate::time_fn::add_sub::{add_sub_time, kind_of, TemporalKind};
            let kinds: [TemporalKind; 2] = [
                kind_of(arg_types[0].as_ref(), &vals[0]),
                kind_of(arg_types[1].as_ref(), &vals[1]),
            ];
            // A call whose arguments are all CONSTANT is what Go folds, and
            // folding runs the ROW body. Anything reading a column runs the
            // vectorized one.
            let row_path = self.args.iter().all(|arg| arg.literal);
            let sign = if upper == "SUBTIME" { -1 } else { 1 };
            let result = add_sub_time(&vals, kinds, sign, row_path, ctx)?;
            return match self.get_static_type().map(FieldType::code) {
                Some(tidb_datatype::FieldTypeCode::Datetime) => crate::cast::parse_computed_time(
                    &result,
                    ctx,
                    tidb_datatype::TimeType::DateTime,
                    None,
                ),
                Some(tidb_datatype::FieldTypeCode::Duration) => {
                    crate::cast::parse_computed_duration(&result, ctx)
                }
                _ => Ok(result),
            };
        }
        if upper == "TIMESTAMP" {
            let result = crate::time_fn::add_sub::timestamp(&vals, ctx)?;
            return crate::cast::parse_computed_time(
                &result,
                ctx,
                tidb_datatype::TimeType::DateTime,
                None,
            );
        }
        if upper == "SYSDATE" {
            let result = crate::time_fn::add_sub::sysdate(&vals, ctx)?;
            return crate::cast::parse_computed_time(
                &result,
                ctx,
                tidb_datatype::TimeType::DateTime,
                self.get_static_type().map(FieldType::decimal),
            );
        }
        if matches!(
            upper.as_str(),
            "NOW"
                | "CURRENT_TIMESTAMP"
                | "LOCALTIME"
                | "LOCALTIMESTAMP"
                | "UTC_TIMESTAMP"
                | "CURDATE"
                | "CURRENT_DATE"
                | "UTC_DATE"
                | "CURTIME"
                | "CURRENT_TIME"
                | "UTC_TIME"
                | "LAST_DAY"
                | "MAKEDATE"
                | "FROM_DAYS"
                | "SEC_TO_TIME"
                | "MAKETIME"
                | "STR_TO_DATE"
                | "TIMEDIFF"
                | "CONVERT_TZ"
                | "FROM_UNIXTIME"
        ) {
            let result = if upper == "STR_TO_DATE" {
                crate::time_fn::calendar::str_to_date_typed(
                    &vals,
                    ctx,
                    self.get_static_type().map(FieldType::code),
                )?
            } else {
                crate::time_fn::dispatch(&upper, &vals, ctx)
                    .expect("the native temporal family is registered")?
            };
            return finish_native_temporal_result(self.get_static_type(), result, ctx);
        }
        if matches!(upper.as_str(), "ROUND" | "TRUNCATE")
            && matches!(vals.first(), Some(Datum::Decimal(_)))
        {
            return crate::math_fn::round_or_truncate_with_result_decimal(
                &vals,
                upper == "ROUND",
                self.ret_type.as_ref().map(FieldType::decimal),
                ctx,
            );
        }
        if matches!(upper.as_str(), "CEIL" | "CEILING" | "FLOOR")
            && matches!(vals.first(), Some(Datum::Decimal(_)))
        {
            return crate::math_fn::ceil_floor_with_result_domain(
                &vals,
                upper != "FLOOR",
                self.ret_type
                    .as_ref()
                    .map(|field_type| field_type.eval_type() == tidb_datatype::EvalType::Decimal),
                ctx,
            );
        }
        if let Some(result) = crate::math_fn::dispatch_values(&upper, &vals, ctx) {
            return result.map_err(|error| math_error(error));
        }
        if let Some(result) = crate::func::eval_func_values_in(&upper, &vals, ctx) {
            return result;
        }
        // The date/time family reads the statement clock and the session
        // zone, so it takes the context rather than values alone.
        if let Some(result) = crate::time_fn::dispatch(&upper, &vals, ctx) {
            return result;
        }
        Err(EvalError::Unsupported(
            "this scalar function is not yet ported",
        ))
    }
}

pub(crate) fn supports(function: &ScalarFunction) -> bool {
    super::argument_policy::classify(function)
        == super::argument_policy::ArgumentPolicy::CommonEager
        || super::session_read::SessionRead::for_function(function).is_some()
}
/// These original common bodies take precedence over the stricter typed registry.
pub(crate) fn prefers_common_body(function: &ScalarFunction) -> bool {
    function.pb_builtin.is_none()
        && matches!(
            function.func_name.lowercase(),
            "unhex"
                | "hex"
                | "bin"
                | "oct"
                | "space"
                | "reverse"
                | "upper"
                | "lower"
                | "quote"
                | "to_base64"
                | "from_base64"
                | "bit_length"
                | "left"
                | "right"
                | "repeat"
                | "replace"
                | "substring"
                | "substr"
                | "substring_index"
                | "lpad"
                | "rpad"
                | "insert"
                | "translate"
                | "json_array"
                | "json_object"
                | "json_type"
                | "json_depth"
                | "json_valid"
                | "json_pretty"
                | "json_storage_size"
                | "json_storage_free"
                | "json_quote"
                | "json_unquote"
                | "json_keys"
                | "json_length"
                | "json_extract"
                | "json_remove"
                | "json_set"
                | "json_insert"
                | "json_replace"
                | "json_array_append"
                | "json_array_insert"
                | "json_contains"
                | "json_contains_path"
                | "json_search"
                | "json_merge"
                | "json_merge_preserve"
                | "json_merge_patch"
                | "format"
                | "conv"
                | "elt"
                | "make_set"
                | "export_set"
                | "soundex"
        )
}

/// Bound demanded original SQL string conversion only while a value body owns
/// a finite work scope. Unsupported subjects still reach their original owner.
pub(crate) fn charge_string_work(value: &Datum) -> Result<(), EvalError> {
    use tidb_query_expr::local::{charge_value_kernel_work, value_kernel_work_remaining};
    if value_kernel_work_remaining().is_none() {
        return Ok(());
    }
    let charge = |n| {
        charge_value_kernel_work(n).map_err(|e| {
            EvalError::ExpressionRuntimeFailure(
                crate::tikv::ExpressionRuntimeFailure::from_local_eval(e, None),
            )
        })
    };
    if let Datum::Decimal(v) = value {
        charge(v.coefficient_bytes().len().saturating_mul(16))?;
        if std::str::from_utf8(v.coefficient_bytes()).is_err() {
            let _ = tidb_query_expr::native_coerce_bytes(value.as_shared_json_input());
        }
    }
    match tidb_query_expr::native_byte_coercion_work(value.as_shared_json_input()) {
        Ok(n) => charge(n),
        Err(tidb_query_expr::NativeStringCoercionError::ResourceLimit) => charge(usize::MAX),
        Err(tidb_query_expr::NativeStringCoercionError::Unsupported(_)) => Ok(()),
    }
}

/// Preserve an actual returned resource cause; audit a swallowed refusal before
/// an ordinary SQL result/error can cross the value-body boundary.
pub(crate) fn audit_work_result(result: Result<Datum, EvalError>) -> Result<Datum, EvalError> {
    if matches!(&result,Err(EvalError::ExpressionRuntimeFailure(e)) if e.class()==crate::tikv::ExpressionRuntimeFailureClass::ResourceLimit)
    {
        return result;
    }
    tidb_query_expr::local::value_kernel_work_status().map_err(|e| {
        EvalError::ExpressionRuntimeFailure(crate::tikv::ExpressionRuntimeFailure::from_local_eval(
            e, None,
        ))
    })?;
    result
}
#[cfg(test)]
mod scope_receipt_tests {
    use super::*;
    #[test]
    fn eager_scalar_work_audit_retains_actual_cause_and_rejects_swallowed_null() {
        tidb_query_expr::local::with_value_kernel_work_limit(0, || {
            let actual = tidb_query_expr::local::charge_value_kernel_work(1).unwrap_err();
            let error = EvalError::ExpressionRuntimeFailure(
                crate::tikv::ExpressionRuntimeFailure::from_local_eval(actual, None),
            );
            assert_eq!(audit_work_result(Err(error.clone())).unwrap_err(), error);
            assert!(matches!(
                audit_work_result(Ok(Datum::Null)),
                Err(EvalError::ExpressionRuntimeFailure(_))
            ));
            assert!(matches!(
                audit_work_result(Err(EvalError::Unsupported("swallowed resource"))),
                Err(EvalError::ExpressionRuntimeFailure(_))
            ));
        });
        let error = EvalError::Unsupported("ordinary SQL error");
        assert_eq!(audit_work_result(Err(error.clone())).unwrap_err(), error);
    }
}
