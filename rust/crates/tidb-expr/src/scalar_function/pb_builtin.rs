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

//! Go `getSignatureByPB`: select an implementation from the wire enum.
//! SQL function names are diagnostic metadata, never an overload selector here.

use super::{class_kernels, cast_json_argument_value, eval_numeric_operand_row, eval_numeric_row, ScalarFunction};
use tidb_exprctx::context::{Columns, EvalError};
use crate::expression::{ConstLevel, Expression};
use tidb_ast::BinaryOp;
use tidb_chunk::row::Row;
use tidb_datatype::{Datum, EvalType, FieldType, VectorFloat32};
use tidb_proto::tipb::ScalarFuncSig;

pub(crate) type ValuesKernel = fn(&[Datum], &dyn Columns) -> Result<Datum, EvalError>;
/// A signature that reads its node's build state -- Go `baseBuiltinFunc`'s
/// collator or an argument's field type -- beside the argument values.
pub(crate) type MethodKernel = fn(&ScalarFunction, &[Datum], &dyn Columns) -> Result<Datum, EvalError>;

/// Go `builtinStrcmpSig`: the comparison under `b.collation`.
fn strcmp_method(function: &ScalarFunction, values: &[Datum], _: &dyn Columns) -> Result<Datum, EvalError> {
    crate::string_fn::strcmp_with_collation(values, function.derived_collation())
}

/// Go `builtinFieldStringSig` compares under `b.collation`; the int and real
/// signatures read arguments their class already cast.
fn field_method(function: &ScalarFunction, values: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    crate::string_fn::field_with_collation(values, function.derived_collation(), ctx)
}

/// Go `builtinHexStrArgSig` / `builtinHexIntArgSig` over the argument's type.
fn hex_method(function: &ScalarFunction, values: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    crate::string_fn::hex_with_type(values, function.args.first().and_then(Expression::static_type), ctx)
}

/// Go `builtinFromDaysSig.evalTime`: a DATE.
fn from_days_values(values: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    let date = crate::time_fn::calendar::from_days(values)?;
    crate::cast::parse_computed_time(&date, ctx, tidb_datatype::TimeType::Date, Some(0))
}

/// Go `builtinMakeDateSig.evalTime`: a DATE.
fn make_date_values(values: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    let date = crate::time_fn::makedate(values)?;
    crate::cast::parse_computed_time(&date, ctx, tidb_datatype::TimeType::Date, Some(0))
}

/// Go `builtinMakeTimeSig.evalDuration`: a duration of the fsp its class
/// derived into the result type.
fn make_time_method(function: &ScalarFunction, values: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    let time = crate::time_fn::maketime(values, ctx)?;
    crate::cast::parse_computed_duration(&time, ctx, function.get_static_type().map(FieldType::decimal))
}

/// Go `builtinSysDateWithFspSig` / `builtinSysDateWithoutFspSig`: a DATETIME
/// of the result type's fsp.
fn sys_date_method(function: &ScalarFunction, values: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    let now = crate::time_fn::add_sub::sysdate(values, ctx)?;
    crate::cast::parse_computed_time(
        &now,
        ctx,
        tidb_datatype::TimeType::DateTime,
        function.get_static_type().map(FieldType::decimal),
    )
}

/// A signature that reads its arguments itself, or not at all.
pub(crate) type RowKernel = fn(&ScalarFunction, &dyn Columns, Row<'_>) -> Result<Datum, EvalError>;

/// Go `builtinFtsMatchWordSig.evalReal`: only a fulltext index computes a
/// relevance score.
fn fts_match_word_row(_: &ScalarFunction, _: &dyn Columns, _: Row<'_>) -> Result<Datum, EvalError> {
    Err(EvalError::Raw("cannot use 'FTS_MATCH_WORD()' outside of fulltext index".to_owned()))
}

/// Go `builtinFtsMysqlMatchAgainstSig.evalReal`: a NULL search constant is
/// NULL; anything else needs a fulltext index.
fn fts_match_against_row(function: &ScalarFunction, _: &dyn Columns, _: Row<'_>) -> Result<Datum, EvalError> {
    if let Some(Expression::Constant(against)) = function.args.first() {
        if against.value.is_null() {
            return Ok(Datum::Null);
        }
    }
    Err(EvalError::Raw("cannot use 'MATCH ... AGAINST' outside of fulltext index".to_owned()))
}

/// Go `builtinRandWithSeedFirstGenSig.evalReal`: a nonconstant seed starts a
/// fresh generator every row; this node's address is the occurrence's
/// identity.
fn rand_row(function: &ScalarFunction, ctx: &dyn Columns, row: Row<'_>) -> Result<Datum, EvalError> {
    let values = function
        .args
        .iter()
        .map(|arg| arg.eval(ctx, row))
        .collect::<Result<Vec<_>, _>>()?;
    crate::math_fn::eval_rand_values(&values, ctx, Some(std::ptr::from_ref(function) as usize), false)
}

/// Go `builtinRandSig.evalReal`: the generator the class seeded, one per
/// occurrence.
fn rand_const_row(function: &ScalarFunction, ctx: &dyn Columns, _: Row<'_>) -> Result<Datum, EvalError> {
    rand_next(function, ctx)
}

/// Go `builtinRandSig.evalReal` / `vecEvalReal`: the signature's generator.
pub(super) fn rand_next(function: &ScalarFunction, ctx: &dyn Columns) -> Result<Datum, EvalError> {
    if let Some(generator) = &function.rand_rng { return Ok(Datum::Real(generator.0.gen())); }
    ctx.rand_next().map(Datum::Real).ok_or(EvalError::Unsupported("RAND requires a session"))
}

/// Go `builtinTruncate{Int,Uint,Real,Decimal}Sig`: the decimal signature caps
/// the scale by the result type's.
fn truncate_method(function: &ScalarFunction, values: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    crate::math_fn::round_or_truncate_with_result_decimal(
        values,
        false,
        function.ret_type.as_ref().map(FieldType::decimal),
        ctx,
    )
}

/// Go `builtinUnaryNotJSONSig`.
fn unary_not_json_row(function: &ScalarFunction, ctx: &dyn Columns, row: Row<'_>) -> Result<Datum, EvalError> {
    let value = function.args[0].eval(ctx, row)?;
    crate::ops::eval_unary(
        tidb_ast::UnaryOp::Not,
        value,
        crate::ops::Operand::Expr(&function.args[0]),
        ctx,
    )
}

/// The value a temporal signature computed, in the function's return type:
/// the chunk column Go writes it to carries that type's decimal.
pub(super) fn temporal_of_ret_type(function: &ScalarFunction, result: Datum, ctx: &dyn Columns) -> Result<Datum, EvalError> {
    use tidb_datatype::{FieldTypeCode, TimeType};
    let ret_type = function.get_static_type();
    let decimal = ret_type.map(FieldType::decimal);
    match ret_type.map(FieldType::code) {
        Some(FieldTypeCode::Datetime) => {
            crate::cast::parse_computed_time(&result, ctx, TimeType::DateTime, decimal)
        }
        Some(FieldTypeCode::Date) => {
            crate::cast::parse_computed_time(&result, ctx, TimeType::Date, Some(0))
        }
        Some(FieldTypeCode::Duration) => crate::cast::parse_computed_duration(&result, ctx, decimal),
        _ => Ok(result),
    }
}

/// Go `builtin*TimeDiffSig`: the eight signatures `timeDiffFunctionClass`
/// picks from its arguments' eval types.
fn time_diff_method(function: &ScalarFunction, values: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    use crate::time_fn::time_diff::TimeDiffArg::{Duration, String, Time};
    let kinds = match function.pb_code() {
        ScalarFuncSig::DurationDurationTimeDiff => [Duration, Duration],
        ScalarFuncSig::DurationStringTimeDiff => [Duration, String],
        ScalarFuncSig::StringDurationTimeDiff => [String, Duration],
        ScalarFuncSig::StringStringTimeDiff => [String, String],
        ScalarFuncSig::StringTimeTimeDiff => [String, Time],
        ScalarFuncSig::TimeStringTimeDiff => [Time, String],
        ScalarFuncSig::TimeTimeTimeDiff => [Time, Time],
        // `builtinNullTimeDiffSig` answers NULL without reading either side.
        _ => return Ok(Datum::Null),
    };
    let fsp = function
        .get_static_type()
        .map_or(tidb_datatype::MAX_FSP, FieldType::decimal);
    crate::time_fn::time_diff::time_diff(values, kinds, fsp, ctx)
}

/// Go `builtinAdd*Sig` / `builtinSub*Sig` of `ADDTIME` / `SUBTIME`: the
/// signature carries the two kinds its class switched on.
pub(super) fn add_sub_time_of(
    function: &ScalarFunction,
    values: &[Datum],
    sign: i64,
    row_path: bool,
    ctx: &dyn Columns,
) -> Result<Datum, EvalError> {
    use tidb_datatype::{FieldTypeCode, TimeType};
    let kinds = crate::time_fn::add_sub::kinds_of_signature(function.pb_code())
        .ok_or(EvalError::Unsupported("not an ADDTIME/SUBTIME signature"))?;
    let result = crate::time_fn::add_sub::add_sub_time(values, kinds, sign, row_path, ctx)?;
    match function.get_static_type().map(FieldType::code) {
        Some(FieldTypeCode::Datetime) => {
            crate::cast::parse_computed_time(&result, ctx, TimeType::DateTime, None)
        }
        Some(FieldTypeCode::Duration) => crate::cast::parse_computed_duration(&result, ctx, None),
        _ => Ok(result),
    }
}

fn add_time_method(function: &ScalarFunction, values: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    add_sub_time_of(function, values, 1, true, ctx)
}

fn sub_time_method(function: &ScalarFunction, values: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    add_sub_time_of(function, values, -1, true, ctx)
}

/// Go `builtinTimestamp1ArgSig` / `builtinTimestamp2ArgsSig`.
fn timestamp_method(function: &ScalarFunction, values: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    let is_float = function.sig == crate::builtin::SigFields::Timestamp { is_float: true };
    let result = crate::time_fn::add_sub::timestamp(values, is_float, ctx)?;
    crate::cast::parse_computed_time(&result, ctx, tidb_datatype::TimeType::DateTime, None)
}

/// Go `builtinNowWithArgSig` / `builtinNowWithoutArgSig`.
fn now_method(function: &ScalarFunction, values: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    temporal_of_ret_type(function, crate::time_fn::now(values, ctx)?, ctx)
}

/// Go `builtinUTCTimestampWithArgSig` / `builtinUTCTimestampWithoutArgSig`.
fn utc_timestamp_method(function: &ScalarFunction, values: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    temporal_of_ret_type(function, crate::time_fn::utc_timestamp(values, ctx)?, ctx)
}

/// Go `builtinCurrentTime0ArgSig` / `builtinCurrentTime1ArgSig`; the
/// function's own name is the one an fsp error reports.
fn current_time_method(function: &ScalarFunction, values: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    let name = if function.func_name.lowercase() == "curtime" {
        "curtime"
    } else {
        "current_time"
    };
    let result = crate::time_fn::current_time(values, name, ctx)?;
    // Go `builtinCurrentTime1ArgSig.evalDuration` parses at the row's own
    // fsp (a NULL one reads as 0), which the return type need not hold.
    match values {
        [fsp] => crate::cast::parse_computed_duration(
            &result,
            ctx,
            Some(crate::arg_eval_type::eval_int(fsp)?.unwrap_or(0)),
        ),
        _ => temporal_of_ret_type(function, result, ctx),
    }
}

/// Go `builtinUTCTimeWithArgSig` / `builtinUTCTimeWithoutArgSig`.
fn utc_time_method(function: &ScalarFunction, values: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    let result = crate::time_fn::utc_time(values, ctx)?;
    // Go `builtinUTCTimeWithArgSig.evalDuration` parses at the row's own
    // fsp, which the return type (0 for a non-constant argument) need not hold.
    match values {
        [Datum::Null] => Ok(Datum::Null),
        [fsp] => crate::cast::parse_computed_duration(&result, ctx, crate::arg_eval_type::eval_int(fsp)?),
        _ => temporal_of_ret_type(function, result, ctx),
    }
}

/// Go `builtinSecToTimeSig`.
fn sec_to_time_method(function: &ScalarFunction, values: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    temporal_of_ret_type(function, crate::time_fn::sec_to_time(values, ctx)?, ctx)
}

/// Go `builtinStrToDateDateSig` / `builtinStrToDateDatetimeSig` /
/// `builtinStrToDateDurationSig`: a time-only parse under the datetime
/// signature is the zero date with that time, which NO_ZERO_DATE refuses.
fn str_to_date_method(function: &ScalarFunction, values: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    str_to_date_of(function, values, true, ctx)
}

/// Go `builtinStrToDate{Date,Datetime,Duration}Sig`'s row form (`row_form`)
/// or vector form; see [`crate::time_fn::calendar::str_to_date_with`].
pub(super) fn str_to_date_of(
    function: &ScalarFunction,
    values: &[Datum],
    row_form: bool,
    ctx: &dyn Columns,
) -> Result<Datum, EvalError> {
    let mut result = crate::time_fn::calendar::str_to_date_with(values, row_form, ctx)?;
    if function.pb_code() == ScalarFuncSig::StrToDateDatetime {
        if let Ok(text) = result.sql_string() {
            if text.contains(':') && !text.contains('-') {
                if ctx.date_modes().no_zero_date {
                    return Ok(Datum::Null);
                }
                result = Datum::new_string_datum(format!("0000-00-00 {text}"));
            }
        }
    }
    temporal_of_ret_type(function, result, ctx)
}

/// Go `builtinConvertTzSig.evalTime` reads its arguments in turn and answers
/// NULL at the first that is NULL or fails, the error dropped: a strict
/// write stores NULL where other builtins fail.
fn convert_tz_row(function: &ScalarFunction, ctx: &dyn Columns, row: Row<'_>) -> Result<Datum, EvalError> {
    let mut values = Vec::with_capacity(function.args.len());
    for argument in &function.args {
        match argument.eval(ctx, row) {
            Ok(Datum::Null) | Err(_) => return Ok(Datum::Null),
            Ok(value) => values.push(value),
        }
    }
    temporal_of_ret_type(function, crate::time_fn::convert_tz::convert_tz(&values)?, ctx)
}

/// Go `builtinGreatest*Sig`: the string signature compares under the
/// function's collation.
pub(crate) fn greatest_method(function: &ScalarFunction, values: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    function.extremum_of(true, values, function.derived_collation(), ctx)
}

/// Go `builtinLeast*Sig`.
pub(crate) fn least_method(function: &ScalarFunction, values: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    function.extremum_of(false, values, function.derived_collation(), ctx)
}

/// Go `builtinIntervalIntSig` / `builtinIntervalRealSig`.
fn interval_row(function: &ScalarFunction, ctx: &dyn Columns, row: Row<'_>) -> Result<Datum, EvalError> {
    let arg_types = function
        .args
        .iter()
        .map(|argument| argument.static_type().cloned())
        .collect::<Vec<_>>();
    crate::builtin_ext::interval_lazy(&arg_types, |index| function.args[index].eval(ctx, row), ctx)
}

/// Go `builtinAesEncryptSig.evalString` / `builtinAesDecryptSig.evalString`.
fn aes_row(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    row: Row<'_>,
    name: &'static str,
) -> Result<Datum, EvalError> {
    let mode = function.aes_mode().ok_or(EvalError::Unsupported("not an AES signature"))?;
    crate::builtin_ext::crypto::aes(name, mode, function.args.len(), |index| function.args[index].eval(ctx, row), ctx)
}

/// Go `builtinAesEncryptIVSig.evalString` / `builtinAesDecryptIVSig.evalString`.
fn aes_iv_row(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    row: Row<'_>,
    name: &'static str,
) -> Result<Datum, EvalError> {
    let mode = function.aes_mode().ok_or(EvalError::Unsupported("not an AES signature"))?;
    crate::builtin_ext::crypto::aes_iv(name, mode, |index| function.args[index].eval(ctx, row))
}

/// Go `builtinLastInsertIDSig.evalInt`: the session's previous last insert
/// id, in the unsigned domain.
fn last_insert_id_values(_: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    let vars = tidb_expropt::SessionVarsPropReader.get_session_vars(ctx).map_err(crate::builtin_ext::misc::opt_prop_error)?;
    Ok(Datum::UInt(vars.prev_last_insert_id()))
}

/// Go `builtinLastInsertIDWithIDSig.evalInt`: records the argument as the
/// session's last insert id and returns it; NULL for a NULL argument.
fn last_insert_id_with_id_method(function: &ScalarFunction, values: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    let vars = tidb_expropt::SessionVarsPropReader.get_session_vars(ctx).map_err(crate::builtin_ext::misc::opt_prop_error)?;
    if values[0].is_null() {
        return Ok(Datum::Null);
    }
    let cast = crate::cast::cast_arg_as_int(&values[0], function.args[0].static_type(), ctx)?;
    let recorded = match cast {
        Datum::Int(i) => i as u64,
        Datum::UInt(u) => u,
        _ => return Err(EvalError::Unsupported("LAST_INSERT_ID argument")),
    };
    vars.set_last_insert_id(recorded);
    Ok(Datum::Int(recorded as i64))
}

/// Go `BuiltinGroupingImplSig.evalInt`: the planner installs the grouping
/// metadata before execution; a NULL grouping id is NULL.
fn grouping_method(function: &ScalarFunction, values: &[Datum], _: &dyn Columns) -> Result<Datum, EvalError> {
    let Some(grouping_id) = crate::arg_eval_type::eval_int(&values[0])? else {
        return Ok(Datum::Null);
    };
    let metadata = function
        .grouping_metadata
        .as_ref()
        .ok_or(EvalError::Unsupported("Meta data is not initialized"))?;
    Ok(Datum::UInt(metadata.eval(grouping_id as u64)))
}

fn left_values(values: &[Datum], _: &dyn Columns) -> Result<Datum, EvalError> {
    crate::string_fn::str_take(values, true)
}

fn lpad_values(values: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    crate::string_packet::pad(values, true, ctx)
}

fn rpad_values(values: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    crate::string_packet::pad(values, false, ctx)
}

fn oct_values(values: &[Datum], _: &dyn Columns) -> Result<Datum, EvalError> {
    crate::string_fn::oct(values)
}

fn export_set_values(values: &[Datum], _: &dyn Columns) -> Result<Datum, EvalError> {
    crate::string_fn::export_set(values)
}

fn make_set_values(values: &[Datum], _: &dyn Columns) -> Result<Datum, EvalError> {
    crate::string_fn::make_set(values)
}

/// Go `builtinLocate2ArgsSig` / `builtinLocate2ArgsUTF8Sig`: the search runs
/// under the function's collation.
fn locate_2_args_method(function: &ScalarFunction, values: &[Datum], _: &dyn Columns) -> Result<Datum, EvalError> {
    crate::string_fn::locate(&values[0], &values[1], function.derived_collation())
}

/// Go `builtinLocate3ArgsSig` / `builtinLocate3ArgsUTF8Sig`.
fn locate_3_args_method(function: &ScalarFunction, values: &[Datum], _: &dyn Columns) -> Result<Datum, EvalError> {
    crate::string_fn::locate_with_position(values, function.derived_collation())
}

/// Go `builtinInstrSig` / `builtinInstrUTF8Sig`.
fn instr_method(function: &ScalarFunction, values: &[Datum], _: &dyn Columns) -> Result<Datum, EvalError> {
    crate::string_fn::instr(&values[0], &values[1], function.derived_collation())
}

/// Go `builtinTrim1ArgSig` / `builtinTrim2ArgsSig` / `builtinTrim3ArgsSig`:
/// the optional second argument is the string to remove (a space by default)
/// and the optional third an `ast.TrimDirectionType` integer.
fn trim_row(function: &ScalarFunction, ctx: &dyn Columns, row: Row<'_>) -> Result<Datum, EvalError> {
    let value = function.args[0].eval(ctx, row)?;
    if value.is_null() {
        return Ok(Datum::Null);
    }
    let remstr = match function.args.get(1) {
        Some(argument) => argument.eval(ctx, row)?,
        None => Datum::new_string_datum(" "),
    };
    if remstr.is_null() {
        return Ok(Datum::Null);
    }
    let direction = match function.args.get(2) {
        Some(argument) => {
            let direction = argument.eval(ctx, row)?;
            if direction.is_null() {
                return Ok(Datum::Null);
            }
            match direction.get_int64() {
                2 => tidb_ast::TrimDirection::Leading,
                3 => tidb_ast::TrimDirection::Trailing,
                _ => tidb_ast::TrimDirection::Both,
            }
        }
        None => tidb_ast::TrimDirection::Both,
    };
    let binary = matches!(value, Datum::Bytes(_));
    let text = crate::coerce::coerce_str_bytes(&value)?;
    let remove = crate::coerce::coerce_str_bytes(&remstr)?;
    Ok(crate::string_fn::trim_value(text, remove, direction, binary))
}

/// Go `builtinConvertSig.evalString`.
fn convert_row(function: &ScalarFunction, ctx: &dyn Columns, row: Row<'_>) -> Result<Datum, EvalError> {
    let value = function.args[0].eval(ctx, row)?;
    if value.is_null() {
        return Ok(Datum::Null);
    }
    let target = function.args[1].eval(ctx, row)?;
    let target = crate::coerce::coerce_str_bytes(&target)?.unwrap_or_default();
    let target = String::from_utf8_lossy(&target).into_owned();
    let arg_type = function.args[0]
        .static_type()
        .cloned()
        .unwrap_or_else(|| FieldType::new(tidb_datatype::FieldTypeCode::VarString));
    let value = crate::cast::cast_arg_as_string(&value, Some(&arg_type), ctx)?;
    let explicit_collation = crate::collation_derive::coercibility_of(&function.args[0])
        == crate::expr_collation::Coercibility::EXPLICIT;
    let string_type = crate::cast::cast_arg_as_string_type(
        &arg_type,
        explicit_collation,
        ctx.connection_charset_info(),
    );
    crate::convert_charset::convert_using(&value, &string_type, &target)
}

fn right_values(values: &[Datum], _: &dyn Columns) -> Result<Datum, EvalError> {
    crate::string_fn::str_take(values, false)
}

/// Go `builtinConvSig.evalString`'s first switch: a binary literal, or a
/// cast over a hybrid-typed or binary-literal argument, is read as its bits.
fn conv_bit_literal(
    arg: &Expression,
    ctx: &dyn Columns,
    row: Row<'_>,
) -> Result<Option<Datum>, EvalError> {
    match arg {
        Expression::Constant(constant) => Ok(crate::builtin::is_binary_literal(arg).then(|| constant.value.clone())),
        Expression::ScalarFunction(cast) if cast.is_cast() => {
            let Some(arg0) = cast.args.first() else {
                return Ok(None);
            };
            let hybrid = arg0.static_type().is_some_and(FieldType::is_hybrid);
            if !hybrid && !crate::builtin::is_binary_literal(arg0) {
                return Ok(None);
            }
            // Go `Column.EvalString` of a hybrid column is `Datum.ToString`:
            // a BIT value is its bytes.
            Ok(Some(match eval_numeric_row(arg0, ctx, row, EvalType::String)? {
                Datum::Bit(bits) => Datum::BinaryLiteral(bits),
                text => match crate::arg_eval_type::eval_string(&text)? {
                    Some(bytes) => Datum::BinaryLiteral(tidb_datatype::BinaryLiteral::from(bytes)),
                    None => Datum::Null,
                },
            }))
        }
        _ => Ok(None),
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum Kernel {
    /// A TiKV-owned computation. TiDB only adapts the typed SQL values.
    RpnOnly(Option<fn() -> tidb_query_expr::RpnFnMeta>, &'static [EvalType]),
    Binary(BinaryOp, EvalType),
    /// Go comparisons evaluate both typed operands before applying NULL semantics.
    StringCompare(BinaryOp),
    In(EvalType),
    Like,
    Ilike,
    Conv,
    DateArithmetic {
        subtract: bool,
    },
    Logic(BinaryOp),
    /// Go `builtinBitNegSig` / `builtinUnaryMinus*Sig` over their one
    /// argument.
    Unary(tidb_ast::UnaryOp),
    IntegerMod {
        unsigned: [bool; 2],
    },
    IntegerMultiply {
        unsigned: bool,
    },
    // SQL vector truth tests do not have a protobuf signature.
    VectorTruth { keep_null: bool, falsity: bool },
    /// Go `builtinCoalesce*Sig`: the first non-NULL argument, each read in
    /// the result's evaluation type.
    Coalesce(EvalType),
    Case(EvalType),
    If(EvalType),
    IfNull(EvalType),
    Cast {
        source: EvalType,
        target: EvalType,
        /// Go `baseBuiltinCastFunc.inUnion`.
        in_union: bool,
    },
    String {
        operation: StringOp,
        binary: bool,
    },
    Round,
    /// Go `builtinInternalToBinarySig` / `builtinInternalFromBinarySig`. The
    /// wire signature carries no `cannotConvertStringAsWarning`, so a
    /// decoded `from_binary` errors on bytes its charset cannot hold.
    ConvertCharset {
        from_binary: bool,
    },
    FromUnixTime,
    Regexp,
    /// The positional regexp builtins, evaluated by the same method the
    /// root tier uses so the derived collation seeds the match type.
    RegexpFamily(&'static str),
    Json,
    Values(ValuesKernel),
    Method(MethodKernel),
    Row(RowKernel),
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum StringOp {
    Length,
    Upper,
    Lower,
    Substring,
}

/// The selected implementation survives cloning independently of FuncName.
#[derive(Clone, Copy, Debug)]
pub(crate) struct PbBuiltin {
    signature: ScalarFuncSig,
    pub(super) kernel: Kernel,
}

macro_rules! rpn_comparison {
    ($carrier:ty, $domain:ident, $operator:ty) => {
        Kernel::RpnOnly(
            Some(tidb_query_expr::impl_compare::compare_fn_meta::<
                tidb_query_expr::impl_compare::BasicComparer<$carrier, $operator>,
            >),
            &[EvalType::$domain; 2],
        )
    };
}

impl PbBuiltin {
    pub(crate) fn signature(self) -> ScalarFuncSig {
        self.signature
    }

    pub(crate) fn new(signature: ScalarFuncSig) -> Option<Self> {
        use tidb_query_expr::impl_op::{
            KeepNullOff, KeepNullOn, decimal_is_false_fn_meta, decimal_is_true_fn_meta,
            int_is_false_fn_meta, int_is_true_fn_meta, real_is_false_fn_meta, real_is_true_fn_meta,
            unary_not_decimal_fn_meta, unary_not_int_fn_meta, unary_not_real_fn_meta,
        };
        use tidb_query_datatype::codec::{data_type::{DateTime as QueryDateTime, Duration as QueryDuration, Real as QueryReal}, mysql::Decimal as QueryDecimal};
        use tidb_query_expr::impl_compare::{
            CmpOpEq, CmpOpGe, CmpOpGt, CmpOpLe, CmpOpLt, CmpOpNe, CmpOpNullEq, compare_json_fn_meta, compare_vector_float32_fn_meta,
        };
        use ScalarFuncSig::*;
        let kernel = match signature {
            PlusInt => Kernel::Binary(BinaryOp::Plus, EvalType::Int),
            PlusReal => Kernel::Binary(BinaryOp::Plus, EvalType::Real),
            MinusInt => Kernel::Binary(BinaryOp::Minus, EvalType::Int),
            MinusReal => Kernel::Binary(BinaryOp::Minus, EvalType::Real),
            MultiplyInt => Kernel::IntegerMultiply { unsigned: false },
            MultiplyIntUnsigned => Kernel::IntegerMultiply { unsigned: true },
            MultiplyReal => Kernel::Binary(BinaryOp::Mul, EvalType::Real),
            IntDivideInt => Kernel::Binary(BinaryOp::IntDiv, EvalType::Int),
            IntDivideDecimal => Kernel::Binary(BinaryOp::IntDiv, EvalType::Decimal),
            PlusDecimal => Kernel::Binary(BinaryOp::Plus, EvalType::Decimal),
            MinusDecimal => Kernel::Binary(BinaryOp::Minus, EvalType::Decimal),
            MultiplyDecimal => Kernel::Binary(BinaryOp::Mul, EvalType::Decimal),
            DivideReal => Kernel::Binary(BinaryOp::Div, EvalType::Real),
            DivideDecimal => Kernel::Binary(BinaryOp::Div, EvalType::Decimal),
            ModReal => Kernel::Binary(BinaryOp::Mod, EvalType::Real),
            ModDecimal => Kernel::Binary(BinaryOp::Mod, EvalType::Decimal),
            ModIntSignedSigned => Kernel::IntegerMod {
                unsigned: [false, false],
            },
            ModIntSignedUnsigned => Kernel::IntegerMod {
                unsigned: [false, true],
            },
            ModIntUnsignedSigned => Kernel::IntegerMod {
                unsigned: [true, false],
            },
            ModIntUnsignedUnsigned => Kernel::IntegerMod {
                unsigned: [true, true],
            },
            LtInt => Kernel::RpnOnly(None, &[EvalType::Int; 2]),
            LeInt => Kernel::RpnOnly(None, &[EvalType::Int; 2]),
            GtInt => Kernel::RpnOnly(None, &[EvalType::Int; 2]),
            GeInt => Kernel::RpnOnly(None, &[EvalType::Int; 2]),
            EqInt => Kernel::RpnOnly(None, &[EvalType::Int; 2]),
            NeInt => Kernel::RpnOnly(None, &[EvalType::Int; 2]),
            LtReal => rpn_comparison!(QueryReal, Real, CmpOpLt),
            LeReal => rpn_comparison!(QueryReal, Real, CmpOpLe),
            GtReal => rpn_comparison!(QueryReal, Real, CmpOpGt),
            GeReal => rpn_comparison!(QueryReal, Real, CmpOpGe),
            EqReal => rpn_comparison!(QueryReal, Real, CmpOpEq),
            NeReal => rpn_comparison!(QueryReal, Real, CmpOpNe),
            LtDecimal => rpn_comparison!(QueryDecimal, Decimal, CmpOpLt),
            LeDecimal => rpn_comparison!(QueryDecimal, Decimal, CmpOpLe),
            GtDecimal => rpn_comparison!(QueryDecimal, Decimal, CmpOpGt),
            GeDecimal => rpn_comparison!(QueryDecimal, Decimal, CmpOpGe),
            EqDecimal => rpn_comparison!(QueryDecimal, Decimal, CmpOpEq),
            NeDecimal => rpn_comparison!(QueryDecimal, Decimal, CmpOpNe),
            LtString => Kernel::StringCompare(BinaryOp::Lt),
            LeString => Kernel::StringCompare(BinaryOp::Le),
            GtString => Kernel::StringCompare(BinaryOp::Gt),
            GeString => Kernel::StringCompare(BinaryOp::Ge),
            EqString => Kernel::StringCompare(BinaryOp::Eq),
            NeString => Kernel::StringCompare(BinaryOp::Ne),
            LtTime => rpn_comparison!(QueryDateTime, Datetime, CmpOpLt),
            LeTime => rpn_comparison!(QueryDateTime, Datetime, CmpOpLe),
            GtTime => rpn_comparison!(QueryDateTime, Datetime, CmpOpGt),
            GeTime => rpn_comparison!(QueryDateTime, Datetime, CmpOpGe),
            EqTime => rpn_comparison!(QueryDateTime, Datetime, CmpOpEq),
            NeTime => rpn_comparison!(QueryDateTime, Datetime, CmpOpNe),
            InInt => Kernel::In(EvalType::Int),
            InString => Kernel::In(EvalType::String),
            LtDuration => rpn_comparison!(QueryDuration, Duration, CmpOpLt),
            LeDuration => rpn_comparison!(QueryDuration, Duration, CmpOpLe),
            GtDuration => rpn_comparison!(QueryDuration, Duration, CmpOpGt),
            GeDuration => rpn_comparison!(QueryDuration, Duration, CmpOpGe),
            EqDuration => rpn_comparison!(QueryDuration, Duration, CmpOpEq),
            NeDuration => rpn_comparison!(QueryDuration, Duration, CmpOpNe),
            NullEqDuration => rpn_comparison!(QueryDuration, Duration, CmpOpNullEq),
            LtJson => Kernel::RpnOnly(Some(compare_json_fn_meta::<CmpOpLt>), &[EvalType::Json; 2]),
            LeJson => Kernel::RpnOnly(Some(compare_json_fn_meta::<CmpOpLe>), &[EvalType::Json; 2]),
            GtJson => Kernel::RpnOnly(Some(compare_json_fn_meta::<CmpOpGt>), &[EvalType::Json; 2]),
            GeJson => Kernel::RpnOnly(Some(compare_json_fn_meta::<CmpOpGe>), &[EvalType::Json; 2]),
            EqJson => Kernel::RpnOnly(Some(compare_json_fn_meta::<CmpOpEq>), &[EvalType::Json; 2]),
            NeJson => Kernel::RpnOnly(Some(compare_json_fn_meta::<CmpOpNe>), &[EvalType::Json; 2]),
            NullEqJson => Kernel::RpnOnly(Some(compare_json_fn_meta::<CmpOpNullEq>), &[EvalType::Json; 2]),
            LtVectorFloat32 => Kernel::RpnOnly(Some(compare_vector_float32_fn_meta::<CmpOpLt>), &[EvalType::VectorFloat32; 2]),
            LeVectorFloat32 => Kernel::RpnOnly(Some(compare_vector_float32_fn_meta::<CmpOpLe>), &[EvalType::VectorFloat32; 2]),
            GtVectorFloat32 => Kernel::RpnOnly(Some(compare_vector_float32_fn_meta::<CmpOpGt>), &[EvalType::VectorFloat32; 2]),
            GeVectorFloat32 => Kernel::RpnOnly(Some(compare_vector_float32_fn_meta::<CmpOpGe>), &[EvalType::VectorFloat32; 2]),
            EqVectorFloat32 => Kernel::RpnOnly(Some(compare_vector_float32_fn_meta::<CmpOpEq>), &[EvalType::VectorFloat32; 2]),
            NeVectorFloat32 => Kernel::RpnOnly(Some(compare_vector_float32_fn_meta::<CmpOpNe>), &[EvalType::VectorFloat32; 2]),
            NullEqVectorFloat32 => Kernel::RpnOnly(Some(compare_vector_float32_fn_meta::<CmpOpNullEq>), &[EvalType::VectorFloat32; 2]),
            InReal => Kernel::In(EvalType::Real),
            InDecimal => Kernel::In(EvalType::Decimal),
            InTime => Kernel::In(EvalType::Datetime),
            InDuration => Kernel::In(EvalType::Duration),
            InJson => Kernel::In(EvalType::Json),
            LikeSig => Kernel::Like,
            IlikeSig => Kernel::Ilike,
            FtsMatchWord => Kernel::Row(fts_match_word_row),
            FtsMatchExpression => Kernel::Row(fts_match_against_row),
            LogicalAnd => Kernel::Logic(BinaryOp::LogicAnd),
            LogicalOr => Kernel::Logic(BinaryOp::LogicOr),
            LogicalXor => Kernel::Logic(BinaryOp::LogicXor),
            NullEqInt => Kernel::RpnOnly(None, &[EvalType::Int; 2]),
            NullEqReal => rpn_comparison!(QueryReal, Real, CmpOpNullEq),
            NullEqDecimal => rpn_comparison!(QueryDecimal, Decimal, CmpOpNullEq),
            NullEqString => Kernel::StringCompare(BinaryOp::NullEq),
            NullEqTime => rpn_comparison!(QueryDateTime, Datetime, CmpOpNullEq),
            BitAndSig => Kernel::Binary(BinaryOp::BitAnd, EvalType::Int),
            BitOrSig => Kernel::Binary(BinaryOp::BitOr, EvalType::Int),
            BitXorSig => Kernel::Binary(BinaryOp::BitXor, EvalType::Int),
            LeftShift => Kernel::Binary(BinaryOp::LeftShift, EvalType::Int),
            RightShift => Kernel::Binary(BinaryOp::RightShift, EvalType::Int),
            BitNegSig => Kernel::Unary(tidb_ast::UnaryOp::BitNeg),
            UnaryMinusInt | UnaryMinusDecimal | UnaryMinusReal => Kernel::Unary(tidb_ast::UnaryOp::Minus),
            IntIsNull => Kernel::RpnOnly(None, &[EvalType::Int]),
            RealIsNull => Kernel::RpnOnly(None, &[EvalType::Real]),
            DecimalIsNull => Kernel::RpnOnly(None, &[EvalType::Decimal]),
            StringIsNull => Kernel::RpnOnly(None, &[EvalType::String]),
            TimeIsNull => Kernel::RpnOnly(None, &[EvalType::Datetime]),
            DurationIsNull => Kernel::RpnOnly(None, &[EvalType::Duration]),
            VectorFloat32IsNull => Kernel::RpnOnly(None, &[EvalType::VectorFloat32]),
            UnaryNotInt => Kernel::RpnOnly(Some(unary_not_int_fn_meta), &[EvalType::Int]),
            UnaryNotReal => Kernel::RpnOnly(Some(unary_not_real_fn_meta), &[EvalType::Real]),
            UnaryNotDecimal => Kernel::RpnOnly(Some(unary_not_decimal_fn_meta), &[EvalType::Decimal]),
            IntIsTrueWithNull => Kernel::RpnOnly(Some(int_is_true_fn_meta::<KeepNullOn>), &[EvalType::Int]),
            RealIsTrueWithNull => Kernel::RpnOnly(Some(real_is_true_fn_meta::<KeepNullOn>), &[EvalType::Real]),
            DecimalIsTrueWithNull => Kernel::RpnOnly(Some(decimal_is_true_fn_meta::<KeepNullOn>), &[EvalType::Decimal]),
            IntIsTrue => Kernel::RpnOnly(Some(int_is_true_fn_meta::<KeepNullOff>), &[EvalType::Int]),
            RealIsTrue => Kernel::RpnOnly(Some(real_is_true_fn_meta::<KeepNullOff>), &[EvalType::Real]),
            DecimalIsTrue => Kernel::RpnOnly(Some(decimal_is_true_fn_meta::<KeepNullOff>), &[EvalType::Decimal]),
            IntIsFalse => Kernel::RpnOnly(Some(int_is_false_fn_meta::<KeepNullOff>), &[EvalType::Int]),
            RealIsFalse => Kernel::RpnOnly(Some(real_is_false_fn_meta::<KeepNullOff>), &[EvalType::Real]),
            DecimalIsFalse => Kernel::RpnOnly(Some(decimal_is_false_fn_meta::<KeepNullOff>), &[EvalType::Decimal]),
            IntIsFalseWithNull => Kernel::RpnOnly(Some(int_is_false_fn_meta::<KeepNullOn>), &[EvalType::Int]),
            RealIsFalseWithNull => Kernel::RpnOnly(Some(real_is_false_fn_meta::<KeepNullOn>), &[EvalType::Real]),
            DecimalIsFalseWithNull => Kernel::RpnOnly(Some(decimal_is_false_fn_meta::<KeepNullOn>), &[EvalType::Decimal]),
            CaseWhenString => Kernel::Case(EvalType::String),
            CaseWhenInt => Kernel::Case(EvalType::Int),
            CaseWhenReal => Kernel::Case(EvalType::Real),
            CaseWhenDecimal => Kernel::Case(EvalType::Decimal),
            CaseWhenTime => Kernel::Case(EvalType::Datetime),
            CaseWhenDuration => Kernel::Case(EvalType::Duration),
            CaseWhenJson => Kernel::Case(EvalType::Json),
            IfString => Kernel::If(EvalType::String),
            IfInt => Kernel::If(EvalType::Int),
            IfReal => Kernel::If(EvalType::Real),
            IfDecimal => Kernel::If(EvalType::Decimal),
            IfTime => Kernel::If(EvalType::Datetime),
            IfDuration => Kernel::If(EvalType::Duration),
            IfJson => Kernel::If(EvalType::Json),
            IfNullInt => Kernel::IfNull(EvalType::Int),
            IfNullReal => Kernel::IfNull(EvalType::Real),
            IfNullDecimal => Kernel::IfNull(EvalType::Decimal),
            IfNullString => Kernel::IfNull(EvalType::String),
            IfNullTime => Kernel::IfNull(EvalType::Datetime),
            IfNullDuration => Kernel::IfNull(EvalType::Duration),
            IfNullJson => Kernel::IfNull(EvalType::Json),
            CoalesceInt => Kernel::Coalesce(EvalType::Int),
            CoalesceReal => Kernel::Coalesce(EvalType::Real),
            CoalesceDecimal => Kernel::Coalesce(EvalType::Decimal),
            CoalesceString => Kernel::Coalesce(EvalType::String),
            CoalesceTime => Kernel::Coalesce(EvalType::Datetime),
            CoalesceDuration => Kernel::Coalesce(EvalType::Duration),
            CoalesceJson => Kernel::Coalesce(EvalType::Json),
            // Go `builtinLengthSig`: the argument's byte length, whatever its
            // collation -- `CharLength`'s binary spelling.
            Length => Kernel::RpnOnly(Some(tidb_query_expr::impl_string::length_fn_meta), &[EvalType::String]),
            CharLength => Kernel::RpnOnly(Some(tidb_query_expr::impl_string::char_length_fn_meta), &[EvalType::String]),
            Ascii => Kernel::RpnOnly(Some(tidb_query_expr::impl_string::ascii_fn_meta), &[EvalType::String]),
            BitLength => Kernel::RpnOnly(Some(tidb_query_expr::impl_string::bit_length_fn_meta), &[EvalType::String]),
            Crc32 => Kernel::RpnOnly(Some(tidb_query_expr::impl_math::crc32_fn_meta), &[EvalType::String]),
            Md5 => Kernel::RpnOnly(Some(tidb_query_expr::impl_encryption::md5_fn_meta), &[EvalType::String]),
            Sha1 => Kernel::RpnOnly(Some(tidb_query_expr::impl_encryption::sha1_fn_meta), &[EvalType::String]),
            Sha2 => Kernel::RpnOnly(Some(tidb_query_expr::impl_encryption::sha2_fn_meta), &[EvalType::String, EvalType::Int]),
            Replace => Kernel::RpnOnly(Some(tidb_query_expr::impl_string::replace_fn_meta), &[EvalType::String; 3]),
            UnHex => Kernel::RpnOnly(Some(tidb_query_expr::impl_string::unhex_fn_meta), &[EvalType::String]),
            LTrim => Kernel::RpnOnly(Some(tidb_query_expr::impl_string::ltrim_fn_meta), &[EvalType::String]),
            RTrim => Kernel::RpnOnly(Some(tidb_query_expr::impl_string::rtrim_fn_meta), &[EvalType::String]),
            CharLengthUtf8 => Kernel::String {
                operation: StringOp::Length,
                binary: false,
            },
            Upper => Kernel::RpnOnly(Some(tidb_query_expr::impl_string::upper_fn_meta), &[EvalType::String]),
            UpperUtf8 => Kernel::String {
                operation: StringOp::Upper,
                binary: false,
            },
            Lower => Kernel::RpnOnly(Some(tidb_query_expr::impl_string::lower_fn_meta), &[EvalType::String]),
            LowerUtf8 => Kernel::String {
                operation: StringOp::Lower,
                binary: false,
            },
            Substring2Args | Substring3Args => Kernel::String {
                operation: StringOp::Substring,
                binary: true,
            },
            Substring2ArgsUtf8 | Substring3ArgsUtf8 => Kernel::String {
                operation: StringOp::Substring,
                binary: false,
            },
            ToBinary => Kernel::ConvertCharset { from_binary: false },
            FromBinary => Kernel::ConvertCharset { from_binary: true },
            // Go `absFunctionClass`: one signature per argument evaluation
            // type; the planner has already cast the argument to it.
            AbsInt => Kernel::RpnOnly(Some(tidb_query_expr::impl_math::abs_int_fn_meta), &[EvalType::Int]),
            AbsUInt => Kernel::RpnOnly(Some(tidb_query_expr::impl_math::abs_uint_fn_meta), &[EvalType::Int]),
            AbsReal => Kernel::RpnOnly(Some(tidb_query_expr::impl_math::abs_real_fn_meta), &[EvalType::Real]),
            AbsDecimal => Kernel::RpnOnly(Some(tidb_query_expr::impl_math::abs_decimal_fn_meta), &[EvalType::Decimal]),
            // Go `builtinMD5Sig`, `builtinCRC32Sig`, `builtinConcatWSSig`:
            // the root tier's evaluators over the argument values.
            Sqrt => Kernel::RpnOnly(Some(tidb_query_expr::impl_math::sqrt_fn_meta), &[EvalType::Real]),
            Exp => Kernel::Values(crate::math_fn::exp),
            Log1Arg => Kernel::Values(crate::math_fn::ln),
            Log2Args => Kernel::RpnOnly(None, &[EvalType::Real; 2]),
            Log2 => Kernel::RpnOnly(None, &[EvalType::Real]),
            Log10 => Kernel::Values(crate::math_fn::log10),
            Radians => Kernel::RpnOnly(None, &[EvalType::Real]),
            Degrees => Kernel::Values(crate::math_fn::degrees),
            Sign => Kernel::RpnOnly(Some(tidb_query_expr::impl_math::sign_fn_meta), &[EvalType::Real]),
            CeilIntToInt => Kernel::RpnOnly(
                Some(tidb_query_expr::impl_math::ceil_fn_meta::<tidb_query_expr::impl_math::CeilIntToInt>),
                &[EvalType::Int],
            ),
            FloorIntToInt => Kernel::RpnOnly(
                Some(tidb_query_expr::impl_math::floor_fn_meta::<tidb_query_expr::impl_math::FloorIntToInt>),
                &[EvalType::Int],
            ),
            CeilReal => Kernel::RpnOnly(None, &[EvalType::Real]),
            FloorReal => Kernel::RpnOnly(None, &[EvalType::Real]),
            CeilDecToInt => Kernel::RpnOnly(None, &[EvalType::Decimal]),
            CeilDecToDec => Kernel::RpnOnly(None, &[EvalType::Decimal]),
            FloorDecToInt => Kernel::RpnOnly(None, &[EvalType::Decimal]),
            FloorDecToDec => Kernel::RpnOnly(None, &[EvalType::Decimal]),
            VecDimsSig => Kernel::RpnOnly(Some(tidb_query_expr::impl_vec::vec_dims_fn_meta), &[EvalType::VectorFloat32]),
            VecL2NormSig => Kernel::Values(|values, _| crate::builtin_ext::vec::l2_norm(&values[0])),
            VecAsTextSig => Kernel::Values(|values, _| crate::builtin_ext::vec::as_text(&values[0])),
            VecL1DistanceSig => Kernel::Values(|values, _| {
                crate::builtin_ext::vec::distance(&values[0], &values[1], VectorFloat32::l1_distance)
            }),
            VecL2DistanceSig => Kernel::Values(|values, _| {
                crate::builtin_ext::vec::distance(&values[0], &values[1], VectorFloat32::l2_distance)
            }),
            VecNegativeInnerProductSig => Kernel::Values(|values, _| {
                crate::builtin_ext::vec::distance(&values[0], &values[1], VectorFloat32::negative_inner_product)
            }),
            VecCosineDistanceSig => Kernel::Values(|values, _| {
                crate::builtin_ext::vec::distance(&values[0], &values[1], VectorFloat32::cosine_distance)
            }),
            Uuid => Kernel::Values(|_, _| crate::builtin_ext::misc::uuid_v1()),
            UuidTimestamp => Kernel::Values(|values, _| crate::builtin_ext::misc::uuid_timestamp(&values[0])),
            UuidVersion => Kernel::Values(|values, _| crate::builtin_ext::misc::uuid_version(&values[0])),
            SysDateWithoutFsp | SysDateWithFsp => Kernel::Method(sys_date_method),
            RandWithSeedFirstGen => Kernel::Row(rand_row),


            UncompressedLength => Kernel::Values(|values, ctx| {
                crate::builtin_ext::crypto::uncompressed_length(&values[0], ctx)
            }),
            JsonTypeSig | JsonUnquoteSig | JsonLengthSig | JsonObjectSig | JsonArraySig
            | JsonMergeSig | JsonSetSig | JsonInsertSig | JsonRemoveSig | JsonContainsSig
            | JsonValidJsonSig | JsonValidStringSig | JsonValidOthersSig => Kernel::Json,
            DayOfMonth => Kernel::Values(|values, _| crate::time_fn::day_of_month(values)),
            DayOfWeek => Kernel::Values(crate::time_fn::day_of_week),
            DayOfYear => Kernel::Values(crate::time_fn::day_of_year),
            WeekOfYear => Kernel::Values(crate::time_fn::week_of_year_builtin),
            Year => Kernel::Values(|values, _| crate::time_fn::calendar::date_part(values, |date| date.0)),
            MonthName => Kernel::Values(crate::time_fn::monthname),
            FromDays => Kernel::Values(from_days_values),
            MakeDate => Kernel::Values(make_date_values),
            MakeTime => Kernel::Method(make_time_method),
            TimeToSec => Kernel::RpnOnly(None, &[EvalType::Duration]),


            Reverse => Kernel::RpnOnly(Some(tidb_query_expr::impl_string::reverse_fn_meta), &[EvalType::String]),
            ReverseUtf8 => Kernel::Values(|values, _| crate::string_fn::reverse(values)),
            Strcmp => Kernel::Method(strcmp_method),
            Elt => Kernel::Values(|values, _| crate::string_fn::elt(values)),
            FieldInt | FieldReal | FieldString => Kernel::Method(field_method),
            HexStrArg | HexIntArg => Kernel::Method(hex_method),
            Right => Kernel::RpnOnly(Some(tidb_query_expr::impl_string::right_fn_meta), &[EvalType::String, EvalType::Int]),
            RightUtf8 => Kernel::Values(right_values),
            Space => Kernel::Values(crate::string_packet::space),


            Concat => Kernel::Values(crate::string_fn::concat_with_context),
            ConcatWs => Kernel::Values(crate::string_fn::concat_ws_with_context),
            Acos => Kernel::RpnOnly(None, &[EvalType::Real]),
            Asin => Kernel::RpnOnly(None, &[EvalType::Real]),
            Atan1Arg => Kernel::Values(crate::math_fn::atan),
            Atan2Args => Kernel::Values(crate::math_fn::atan2),
            Cos => Kernel::Values(crate::math_fn::cos),
            Cot => Kernel::Values(crate::math_fn::cot),
            Sin => Kernel::Values(crate::math_fn::sin),
            Pow => Kernel::Values(crate::math_fn::pow),
            Pi => Kernel::RpnOnly(None, &[]),
            Conv => Kernel::Conv,
            RoundInt | RoundReal | RoundDec => Kernel::Round,
            Date => Kernel::Values(crate::time_fn::date),
            DateDiff => Kernel::RpnOnly(None, &[EvalType::Datetime; 2]),
            DateFormatSig => Kernel::Values(|values, cols| {
                let [date, format] = values else {
                    return Err(EvalError::WrongParameterCount("date_format"));
                };
                crate::time_fn::calendar::date_format_in(date, format, Some(cols))
            }),
            Hour => Kernel::Values(|values, _| {
                crate::time_fn::calendar::time_part(values, |time| i64::from(time.0))
            }),
            Minute => Kernel::Values(|values, _| {
                crate::time_fn::calendar::time_part(values, |time| i64::from(time.1))
            }),
            Second => Kernel::Values(|values, _| {
                crate::time_fn::calendar::time_part(values, |time| i64::from(time.2))
            }),
            MicroSecond => Kernel::Values(|values, _| crate::time_fn::microsecond(values)),
            Month => Kernel::RpnOnly(None, &[EvalType::Datetime]),
            WeekWithMode | WeekWithoutMode => Kernel::Values(|values, ctx| {
                crate::time_fn::week(values, ctx.default_week_format(), ctx)
            }),
            TimestampDiff => {
                Kernel::Values(|values, _| crate::time_fn::calendar::timestamp_diff(values))
            }
            ExtractDatetimeFromString => Kernel::Values(|values, ctx| {
                crate::time_fn::extract::extract_values(
                    values,
                    ctx,
                    crate::time_fn::extract::extract_datetime_from_string,
                )
            }),
            ExtractDatetime => Kernel::Values(|values, ctx| {
                crate::time_fn::extract::extract_values(
                    values,
                    ctx,
                    crate::time_fn::extract::extract_datetime,
                )
            }),
            ExtractDuration => Kernel::Values(|values, ctx| {
                crate::time_fn::extract::extract_values(
                    values,
                    ctx,
                    crate::time_fn::extract::extract_duration,
                )
            }),
            UnixTimestampInt | UnixTimestampDec => {
                Kernel::Method(crate::time_fn::session_tz::unix_timestamp)
            }
            FromUnixTime1Arg | FromUnixTime2Arg => Kernel::FromUnixTime,
            RegexpLikeSig => Kernel::Regexp,
            RegexpSubstrSig => Kernel::RegexpFamily("REGEXP_SUBSTR"),
            RegexpInStrSig => Kernel::RegexpFamily("REGEXP_INSTR"),
            RegexpReplaceSig => Kernel::RegexpFamily("REGEXP_REPLACE"),
            JsonMemberOfSig | JsonReplaceSig | JsonArrayAppendSig | JsonMergePatchSig
            | JsonExtractSig => {
                Kernel::Json
            }
            AddDateDatetimeDecimal
            | AddDateDatetimeInt
            | AddDateDatetimeReal
            | AddDateDatetimeString
            | AddDateDecimalDecimal
            | AddDateDecimalInt
            | AddDateDecimalReal
            | AddDateDecimalString
            | AddDateDurationDecimal
            | AddDateDurationDecimalDatetime
            | AddDateDurationInt
            | AddDateDurationIntDatetime
            | AddDateDurationReal
            | AddDateDurationRealDatetime
            | AddDateDurationString
            | AddDateDurationStringDatetime
            | AddDateIntDecimal
            | AddDateIntInt
            | AddDateIntReal
            | AddDateIntString
            | AddDateRealDecimal
            | AddDateRealInt
            | AddDateRealReal
            | AddDateRealString
            | AddDateStringDecimal
            | AddDateStringInt
            | AddDateStringReal
            | AddDateStringString => Kernel::DateArithmetic { subtract: false },
            SubDateDatetimeDecimal
            | SubDateDatetimeInt
            | SubDateDatetimeReal
            | SubDateDatetimeString
            | SubDateDecimalDecimal
            | SubDateDecimalInt
            | SubDateDecimalReal
            | SubDateDecimalString
            | SubDateDurationDecimal
            | SubDateDurationDecimalDatetime
            | SubDateDurationInt
            | SubDateDurationIntDatetime
            | SubDateDurationReal
            | SubDateDurationRealDatetime
            | SubDateDurationString
            | SubDateDurationStringDatetime
            | SubDateIntDecimal
            | SubDateIntInt
            | SubDateIntReal
            | SubDateIntString
            | SubDateRealDecimal
            | SubDateRealInt
            | SubDateRealReal
            | SubDateRealString
            | SubDateStringDecimal
            | SubDateStringInt
            | SubDateStringReal
            | SubDateStringString => Kernel::DateArithmetic { subtract: true },
            // builtin_math.go
            Tan => Kernel::Method(class_kernels::tan),
            // builtin_string.go
            Bin => Kernel::Values(crate::string_fn::bin),
            SubstringIndex => Kernel::Values(|values, _| crate::string_fn::substring_index(values)),
            ToBase64 => Kernel::Values(crate::string_packet::to_base64),
            FromBase64 => Kernel::Values(|values, ctx| {
                crate::string_fn::from_base64_with_packet_limit(values, Some(ctx))
            }),
            Quote => Kernel::Method(class_kernels::quote),
            Ord => Kernel::Method(class_kernels::ord),
            Repeat => Kernel::Method(class_kernels::repeat),
            FindInSet => Kernel::Row(class_kernels::find_in_set),
            // builtin_time.go
            WeekDay => Kernel::Values(crate::time_fn::weekday),
            Quarter => Kernel::Values(|values, _| crate::time_fn::quarter(values)),
            GetFormat => Kernel::Values(|values, _| crate::time_fn::get_format_value(values)),
            DayName => Kernel::Values(crate::time_fn::dayname),
            TimeFormat => Kernel::Values(|values, _| crate::time_fn::time_format(values)),
            ToDays => Kernel::Values(crate::time_fn::calendar::to_days),
            ToSeconds => Kernel::Values(crate::time_fn::calendar::to_seconds),
            LastDay => Kernel::Method(class_kernels::last_day),
            // builtin_miscellaneous.go
            InetAton => Kernel::Values(|values, _| crate::builtin_ext::compare2::inet_aton(&values[0])),
            InetNtoa => Kernel::RpnOnly(None, &[EvalType::Int]),
            Inet6Aton => Kernel::Values(|values, _| crate::builtin_ext::compare2::inet6_aton(&values[0])),
            Inet6Ntoa => Kernel::RpnOnly(None, &[EvalType::String]),
            IsIPv4 => Kernel::Method(|_, values, _| crate::builtin_ext::compare2::is_ipv4_value(&values[0])),
            IsIPv4Compat => Kernel::RpnOnly(None, &[EvalType::String]),
            IsIPv4Mapped => Kernel::RpnOnly(None, &[EvalType::String]),
            IsIPv6 => Kernel::RpnOnly(None, &[EvalType::String]),
            UuiDv4 => Kernel::Method(|_, _, _| crate::builtin_ext::misc::uuid_v4()),
            UuiDv7 => Kernel::Method(|_, _, _| crate::builtin_ext::misc::uuid_v7()),
            DecimalAnyValue | DurationAnyValue | IntAnyValue | JsonAnyValue | RealAnyValue
            | StringAnyValue | TimeAnyValue | VectorFloat32AnyValue => {
                Kernel::Values(|values, _| Ok(values[0].clone()))
            }
            // builtin_encryption.go
            Compress => Kernel::Values(|values, _| crate::builtin_ext::crypto::compress(&values[0])),
            Uncompress => Kernel::Values(|values, ctx| crate::builtin_ext::crypto::uncompress(&values[0], ctx)),
            Password => Kernel::Method(class_kernels::password),
            // builtin_info.go
            RowCount => Kernel::Method(|_, values, ctx| class_kernels::row_count(values, ctx)),
            _ => {
                let (source, target) = cast_types(signature)?;
                Kernel::Cast { source, target, in_union: false }
            }
        };
        Some(Self { signature, kernel })
    }

    /// Go `builtinFunc.PbCode()` of the function a class's `getFunction`
    /// returned: the signature the class set, decodable or not.
    pub(crate) fn of_builtin_func(signature: ScalarFuncSig, kernel: Option<Kernel>) -> Self {
        let kernel = kernel
            .or_else(|| Self::new(signature).map(|builtin| builtin.kernel))
            .or_else(|| Self::class_only(signature))
            .unwrap_or(Kernel::Values(class_kernels::not_yet_ported));
        Self { signature, kernel }
    }

    /// The evaluation of a signature a class sets and `getSignatureByPB`
    /// does not decode.
    fn class_only(signature: ScalarFuncSig) -> Option<Kernel> {
        use ScalarFuncSig::*;
        Some(match signature {
            // builtin_miscellaneous.go
            IsUuid => Kernel::Values(|values, _| crate::builtin_ext::misc::is_uuid(&values[0])),
            TiDbShard => Kernel::Values(|values, ctx| crate::builtin_ext::misc::tidb_shard(&values[0], ctx)),
            VitessHash => Kernel::Values(|values, ctx| crate::builtin_ext::misc::vitess_hash(&values[0], ctx)),
            // builtin_encryption.go
            Encode => Kernel::Values(|values, _| crate::builtin_ext::crypto::sql_encode(&values[0], &values[1])),
            Decode => Kernel::Values(|values, _| crate::builtin_ext::crypto::sql_decode(&values[0], &values[1])),
            // builtin_string.go
            Left => Kernel::RpnOnly(Some(tidb_query_expr::impl_string::left_fn_meta), &[EvalType::String, EvalType::Int]),
            LeftUtf8 => Kernel::Values(left_values),
            Lpad | LpadUtf8 => Kernel::Values(lpad_values),
            Rpad | RpadUtf8 => Kernel::Values(rpad_values),
            Locate2Args | Locate2ArgsUtf8 => Kernel::Method(locate_2_args_method),
            Locate3Args | Locate3ArgsUtf8 => Kernel::Method(locate_3_args_method),
            Instr | InstrUtf8 => Kernel::Method(instr_method),
            OctInt | OctString => Kernel::Values(oct_values),
            ExportSet3Arg | ExportSet4Arg | ExportSet5Arg => Kernel::Values(export_set_values),
            MakeSet => Kernel::Values(make_set_values),
            Format => Kernel::Values(crate::string_fn::format_num),
            FormatWithLocale => Kernel::Values(crate::builtin_ext::string2::format_with_locale),
            Insert | InsertUtf8 => Kernel::Values(crate::string_fn::insert_with_context),
            Char => Kernel::Values(crate::string_fn::char_func_with_context),
            Trim1Arg | Trim2Args | Trim3Args => Kernel::Row(trim_row),
            Convert => Kernel::Row(convert_row),
            // builtin_math.go
            Rand => Kernel::Row(rand_const_row),
            RoundWithFracInt | RoundWithFracReal | RoundWithFracDec => Kernel::Round,
            TruncateInt | TruncateUint | TruncateReal | TruncateDecimal => {
                Kernel::Method(truncate_method)
            }
            // builtin_json.go
            JsonMergePreserveSig | JsonArrayInsertSig | JsonContainsPathSig | JsonDepthSig
            | JsonKeysSig | JsonKeys2ArgsSig | JsonPrettySig | JsonQuoteSig | JsonSearchSig
            | JsonStorageFreeSig | JsonStorageSizeSig => Kernel::Json,
            // builtin_compare.go
            GreatestInt | GreatestReal | GreatestDecimal | GreatestString | GreatestTime
            | GreatestDate | GreatestDuration | GreatestCmpStringAsTime
            | GreatestCmpStringAsDate => Kernel::Method(greatest_method),
            LeastInt | LeastReal | LeastDecimal | LeastString | LeastTime | LeastDate
            | LeastDuration | LeastCmpStringAsTime | LeastCmpStringAsDate => {
                Kernel::Method(least_method)
            }
            IntervalInt | IntervalReal => Kernel::Row(interval_row),
            // builtin_encryption.go
            AesEncrypt => Kernel::Row(|function, ctx, row| aes_row(function, ctx, row, "aes_encrypt")),
            AesEncryptIv => Kernel::Row(|function, ctx, row| aes_iv_row(function, ctx, row, "aes_encrypt")),
            AesDecrypt => Kernel::Row(|function, ctx, row| aes_row(function, ctx, row, "aes_decrypt")),
            AesDecryptIv => Kernel::Row(|function, ctx, row| aes_iv_row(function, ctx, row, "aes_decrypt")),
            // builtin_info.go
            LastInsertId => Kernel::Values(last_insert_id_values),
            LastInsertIdWithId => Kernel::Method(last_insert_id_with_id_method),
            // builtin_grouping.go
            GroupingSig => Kernel::Method(grouping_method),
            // builtin_op.go
            UnaryNotJson => Kernel::Row(unary_not_json_row),
            // builtin_time.go
            DurationDurationTimeDiff | DurationStringTimeDiff | StringDurationTimeDiff
            | StringStringTimeDiff | StringTimeTimeDiff | TimeStringTimeDiff | TimeTimeTimeDiff
            | NullTimeDiff => Kernel::Method(time_diff_method),
            AddDatetimeAndDuration | AddTimeDateTimeNull | AddDatetimeAndString
            | AddDateAndDuration | AddTimeStringNull | AddDateAndString | AddDurationAndDuration
            | AddTimeDurationNull | AddDurationAndString | AddStringAndDuration
            | AddStringAndString => Kernel::Method(add_time_method),
            SubDatetimeAndDuration | SubTimeDateTimeNull | SubDatetimeAndString
            | SubDateAndDuration | SubTimeStringNull | SubDateAndString | SubDurationAndDuration
            | SubTimeDurationNull | SubDurationAndString | SubStringAndDuration
            | SubStringAndString => Kernel::Method(sub_time_method),
            Timestamp1Arg | Timestamp2Args => Kernel::Method(timestamp_method),
            NowWithArg | NowWithoutArg => Kernel::Method(now_method),
            UtcTimestampWithArg | UtcTimestampWithoutArg => Kernel::Method(utc_timestamp_method),
            CurrentTime0Arg | CurrentTime1Arg => Kernel::Method(current_time_method),
            UtcTimeWithArg | UtcTimeWithoutArg => Kernel::Method(utc_time_method),
            SecToTime => Kernel::Method(sec_to_time_method),
            StrToDateDate | StrToDateDatetime | StrToDateDuration => {
                Kernel::Method(str_to_date_method)
            }
            ConvertTz => Kernel::Row(convert_tz_row),
            Time => Kernel::Values(crate::time_fn::time),
            YearWeekWithMode | YearWeekWithoutMode => Kernel::Values(crate::time_fn::yearweek),
            UnixTimestampCurrent => Kernel::Values(crate::time_fn::session_tz::unix_timestamp_current),
            TimestampAdd => Kernel::Values(crate::time_fn::add_sub::timestamp_add),
            _ => return None,
        })
    }

    /// Go `newBaseBuiltinCastFunc(bf, inUnion)` of the signature a cast
    /// class's `getFunction` set.
    pub(crate) fn of_cast_func(signature: ScalarFuncSig, kernel: Option<Kernel>, in_union: bool) -> Self {
        let mut builtin = Self::of_builtin_func(signature, kernel);
        if let Kernel::Cast { source, target, .. } = builtin.kernel {
            builtin.kernel = Kernel::Cast { source, target, in_union };
        }
        builtin
    }

    pub(super) fn cast_in_union(self) -> bool {
        matches!(self.kernel, Kernel::Cast { in_union: true, .. })
    }

    pub(super) fn numeric_domain(self) -> Option<EvalType> {
        match self.kernel {
            Kernel::Binary(_, domain) => Some(domain),
            Kernel::IntegerMod { .. } | Kernel::IntegerMultiply { .. } => Some(EvalType::Int),
            _ => None,
        }
    }

    pub(crate) fn is_date_arithmetic(self) -> bool {
        matches!(self.kernel, Kernel::DateArithmetic { .. })
    }

    /// Go signature value operations shared by `eval*` and `vecEval*`.
    pub(super) fn eval_values_in(
        self,
        function: &ScalarFunction,
        values: Vec<Datum>,
        vectorized: bool,
        ctx: &dyn Columns,
    ) -> Result<Datum, EvalError> {
        if vectorized {
            use ScalarFuncSig::*;
            if matches!(self.signature, StrToDateDate | StrToDateDatetime | StrToDateDuration) {
                return str_to_date_of(function, &values, false, ctx);
            }
            if self.signature == UtcTimestampWithArg && values.first().is_some_and(Datum::is_null) {
                return Ok(Datum::Null);
            }
            if matches!(self.signature, WeekDay | WeekWithMode | WeekWithoutMode | YearWeekWithMode | WeekOfYear) {
                if let [Datum::Time(date), args @ ..] = values.as_slice() {
                    let mode = match args.first() {
                        Some(mode) => crate::arg_eval_type::eval_int(mode)?.unwrap_or(0),
                        None => ctx.default_week_format(),
                    };
                    return crate::time_fn::calendar::week_vector_value(self.signature, *date, mode, ctx);
                }
            }
            if matches!(self.signature, GreatestString | LeastString) {
                if values.iter().any(Datum::is_null) {
                    return Ok(Datum::Null);
                }
                let collator = tidb_datatype::get_collator(function.derived_collation().name());
                let want = if self.signature == GreatestString {
                    std::cmp::Ordering::Greater
                } else {
                    std::cmp::Ordering::Less
                };
                let mut best: Option<Vec<u8>> = None;
                for value in values {
                    let candidate = crate::coerce::coerce_str_bytes(&value)?.expect("non-NULL string");
                    if best.as_ref().is_none_or(|current| collator.compare(current, &candidate) != want) {
                        best = Some(candidate);
                    }
                }
                return Ok(best.map_or(Datum::Null, Datum::new_string_datum));
            }
            if self.signature == Repeat {
                let flen = function.get_static_type().expect("REPEAT type").flen();
                return crate::string_packet::repeat_in_mode(&values, Some(flen), ctx);
            }
            if self.signature == Password && values.first().is_some_and(Datum::is_null) {
                return Ok(Datum::new_string_datum([]));
            }
            if let Some(sign) = crate::time_fn::add_sub::sign_of_signature(self.signature) {
                return add_sub_time_of(function, &values, sign, false, ctx);
            }
        }
        self.eval_values(function, values, ctx)
    }

    pub(super) fn eval_values(self, function: &ScalarFunction, mut values: Vec<Datum>, ctx: &dyn Columns) -> Result<Datum, EvalError> {
        let args = &function.args;
        let kernel = self.kernel;
        if values.iter().any(Datum::is_null) && !matches!(kernel, Kernel::Json | Kernel::RpnOnly(..) | Kernel::Values(_) | Kernel::Method(_)) {
            return Ok(Datum::Null);
        }
                match kernel {
                    Kernel::RpnOnly(meta, inputs) => {
                        let logarithm = match (self.signature, values.as_slice()) {
                            (ScalarFuncSig::Log2, [Datum::Real(x) | Datum::Float32(x)]) => {
                                Some((*x, 2.0))
                            }
                            (ScalarFuncSig::Log2Args, [
                                Datum::Real(base) | Datum::Float32(base),
                                Datum::Real(x) | Datum::Float32(x),
                            ]) => Some((*x, *base)),
                            _ => None,
                        };
                        if let Some((x, base)) = logarithm {
                            if x <= 0.0 || base <= 0.0 || base == 1.0 {
                                ctx.append_go_warning(
                                    &crate::expression_errors::ERR_INVALID_ARGUMENT_FOR_LOGARITHM,
                                    "Invalid argument for logarithm",
                                );
                                return Ok(Datum::Null);
                            }
                            // The SDK filters nonfinite results to NULL.
                            if !x.is_finite() || !base.is_finite() {
                                return Ok(Datum::Real(x.log(base)));
                            }
                        }
                        if let [Datum::Real(value) | Datum::Float32(value)] = values.as_slice() {
                            if value.is_nan() {
                                match self.signature {
                                    ScalarFuncSig::Sign => return Ok(Datum::Int(0)),
                                    ScalarFuncSig::Asin | ScalarFuncSig::Acos => return Ok(Datum::Null),
                                    ScalarFuncSig::Sqrt | ScalarFuncSig::CeilReal | ScalarFuncSig::FloorReal
                                    | ScalarFuncSig::Radians => return Ok(Datum::Real(*value)),
                                    ScalarFuncSig::AbsReal => return Ok(Datum::Real(value.abs())),
                                    _ => {}
                                }
                            }
                        }
                        let value = crate::rpn::evaluate_required_kernel(
                            function, self.signature, meta, inputs, values, ctx,
                        )?;
                        Ok(match value {
                            Datum::String(value)
                                if matches!(self.signature, ScalarFuncSig::Md5 | ScalarFuncSig::Sha1 | ScalarFuncSig::Sha2) =>
                            {
                                Datum::new_string_datum(value.into_bytes())
                            }
                            value => value,
                        })
                    },
                    Kernel::Values(eval) => eval(&values, ctx),
                    Kernel::Method(eval) => eval(function, &values, ctx),
                    Kernel::RegexpFamily(name) => function.eval_regexp_family(name, &values, ctx),
                    Kernel::ConvertCharset { from_binary } => {
                        let [value] = values.as_slice() else {
                            return Err(EvalError::Unsupported("protobuf charset builtin arity"));
                        };
                        if from_binary {
                            let charset = function
                                .get_static_type()
                                .map_or("binary", FieldType::charset_name);
                            crate::convert_charset::from_binary(
                                value,
                                charset,
                                function.cannot_convert_string_as_warning,
                                ctx,
                            )
                        } else {
                            let charset = args[0]
                                .static_type()
                                .map_or("binary", FieldType::charset_name);
                            crate::convert_charset::to_binary(value, charset)
                        }
                    }
                    Kernel::Round => crate::math_fn::round_or_truncate_with_result_decimal(
                        &values,
                        true,
                        function.ret_type.as_ref().map(FieldType::decimal),
                        ctx,
                    ),
                    Kernel::String { operation, binary } => {
                        let Some(value) = values.first_mut() else {
                            return Err(EvalError::Unsupported("protobuf string builtin arity"));
                        };
                        if !value.is_null() {
                            let bytes = crate::arg_eval_type::eval_string(value)?
                                .ok_or(EvalError::Unsupported("protobuf string argument"))?;
                            *value = if binary {
                                Datum::Bytes(bytes)
                            } else {
                                Datum::new_string_datum(bytes)
                            };
                        }
                        match operation {
                            StringOp::Length => Ok(match &values[0] {
                                Datum::Null => Datum::Null,
                                value => Datum::Int(
                                    crate::string_signature::StrUnits::of_with_signature(
                                        value, false,
                                    )?
                                    .expect("non-NULL string")
                                    .len() as i64,
                                ),
                            }),
                            StringOp::Upper => crate::string_fn::case_convert_with_type(
                                &values,
                                true,
                                args[0].static_type(),
                                binary,
                            ),
                            StringOp::Lower => crate::string_fn::case_convert_with_type(
                                &values,
                                false,
                                args[0].static_type(),
                                binary,
                            ),
                            StringOp::Substring => crate::string_fn::substring(&values, ctx),
                        }
                    }
                    Kernel::FromUnixTime => {
                        let result = crate::time_fn::session_tz::from_unixtime_with_precision(
                            &values,
                            ctx,
                            function.get_static_type().map(FieldType::decimal),
                        )?;
                        if self.signature == ScalarFuncSig::FromUnixTime1Arg {
                            crate::cast::parse_computed_time(
                                &result,
                                ctx,
                                tidb_datatype::TimeType::DateTime,
                                function.get_static_type().map(FieldType::decimal),
                            )
                        } else {
                            Ok(result)
                        }
                    }
                    Kernel::Json => {
                        let types = args
                            .iter()
                            .map(|arg| arg.static_type().cloned())
                            .collect::<Vec<_>>();
                        let cache_paths = args.get(1..).is_some_and(|arguments| {
                            !arguments.is_empty()
                                && arguments
                                    .iter()
                                    .step_by(2)
                                    .all(|arg| arg.const_level() >= ConstLevel::ONLY_IN_CONTEXT)
                        });
                        crate::builtin_ext::json::eval_pb(
                            self.signature,
                            &values,
                            &types,
                            ctx,
                            cache_paths.then_some(&function.json_modify_path_cache),
                        )
                    }
                    _ => unreachable!("lazy kernels were handled before evaluating arguments"),
                }
    }

    /// Go `builtinCast*Sig`: the conversion shared by scalar and vector kernels.
    pub(super) fn cast_value(self, function: &ScalarFunction, value: Datum, ctx: &dyn Columns) -> Result<Datum, EvalError> {
        self.cast_value_in(function, value, false, ctx)
    }

    /// [`Self::cast_value`] as Go's `builtinCast*Sig.evalX` (`!vectorized`) or
    /// `vecEvalX` (`vectorized`) converts one value.
    pub(super) fn cast_value_in(
        self,
        function: &ScalarFunction,
        value: Datum,
        vectorized: bool,
        ctx: &dyn Columns,
    ) -> Result<Datum, EvalError> {
        let Kernel::Cast { source, target, in_union } = self.kernel else { unreachable!() };
        let args = &function.args;
                if value.is_null() {
                    return Ok(Datum::Null);
                }
                // Go `builtinCastStringAs{Int,Real}Sig`: "Take the implicit
                // evalInt path if possible."
                if let Some(number) = super::can_implicit_eval_int(&args[0], &value, target) {
                    return Ok(number);
                }
                if target == EvalType::Json {
                    return cast_json_argument_value(function, value);
                }
                let field = function
                    .get_static_type()
                    .ok_or(EvalError::Unsupported("protobuf cast result type"))?;
                if vectorized && source == EvalType::String && target == EvalType::Decimal {
                    let bytes = crate::arg_eval_type::eval_string(&value)?.expect("non-NULL cast input");
                    let leading = bytes.iter().copied().find(|b| !b.is_ascii_whitespace());
                    if in_union && field.is_unsigned() && leading == Some(b'-') {
                        return Ok(Datum::Decimal(tidb_datatype::Decimal::parse_mysql("0").0));
                    }
                    return super::cast_string_as_decimal(&bytes, field, ctx, true);
                }
                if matches!(target, EvalType::Int | EvalType::Real | EvalType::Decimal) {
                    return crate::cast::eval_numeric_cast_with_type(
                        value,
                        source,
                        Some(&args[0]),
                        args[0].static_type(),
                        field,
                        in_union,
                        ctx,
                    );
                }
                if target == EvalType::String {
                    return crate::cast::eval_string_cast_with_type(
                        value,
                        args[0].static_type(),
                        field,
                        ctx,
                    );
                }
                let fsp = field.decimal().max(0);
                match target {
                    EvalType::Datetime | EvalType::Timestamp => {
                        let (tp, fsp) = match field.code() {
                            tidb_datatype::FieldTypeCode::Date => (tidb_datatype::TimeType::Date, 0),
                            _ => (tidb_datatype::TimeType::DateTime, fsp),
                        };
                        crate::cast::cast_to_time_in(&value, args[0].static_type(), ctx, tp, fsp, vectorized)
                    }
                    EvalType::Duration => {
                        crate::cast::cast_to_duration_in(&value, args[0].static_type(), ctx, fsp, vectorized)
                    }
                    EvalType::VectorFloat32 => crate::cast::cast_as_vector_float32(value, field),
                    _ => Err(EvalError::Unsupported("protobuf cast target")),
                }
    }

    pub(super) fn eval(
        self,
        function: &ScalarFunction,
        ctx: &dyn Columns,
        row: Row<'_>,
    ) -> Result<Datum, EvalError> {
        let args = &function.args;
        let typed_argument = |index: usize, domain: EvalType| {
            let arg = args
                .get(index)
                .ok_or(EvalError::Unsupported("missing protobuf builtin argument"))?;
            eval_numeric_row(arg, ctx, row, domain)
        };
        let condition = |index: usize| -> Result<Option<bool>, EvalError> {
            let value = typed_argument(index, EvalType::Int)?;
            Ok(crate::arg_eval_type::eval_int(&value)?.map(|value| value != 0))
        };
        match self.kernel {
            Kernel::Case(domain) => {
                let (pairs, remainder) = args.as_chunks::<2>();
                for (index, pair) in pairs.iter().enumerate() {
                    if condition(index * 2)? == Some(true) {
                        return eval_numeric_row(&pair[1], ctx, row, domain);
                    }
                }
                remainder.first().map_or(Ok(Datum::Null), |value| {
                    eval_numeric_row(value, ctx, row, domain)
                })
            }
            Kernel::If(domain) => {
                let branch = if condition(0)? == Some(true) { 1 } else { 2 };
                typed_argument(branch, domain)
            }
            Kernel::IfNull(domain) => {
                let value = typed_argument(0, domain)?;
                if value.is_null() {
                    typed_argument(1, domain)
                } else {
                    Ok(value)
                }
            }
            Kernel::RpnOnly(_, [domain]) if matches!(
                self.signature,
                ScalarFuncSig::IntIsNull | ScalarFuncSig::RealIsNull | ScalarFuncSig::DecimalIsNull
                    | ScalarFuncSig::StringIsNull | ScalarFuncSig::TimeIsNull
                    | ScalarFuncSig::DurationIsNull | ScalarFuncSig::VectorFloat32IsNull
            ) => {
                // Decoded coprocessor requests use the signature's typed
                // read, which can emit conversion warnings before IS NULL.
                self.eval_values(function, vec![typed_argument(0, *domain)?], ctx)
            }
            Kernel::VectorTruth { keep_null, falsity } => {
                let value = typed_argument(0, EvalType::VectorFloat32)?;
                Ok(match crate::truthy_of(&value)? {
                    None if keep_null => Datum::Null,
                    None => Datum::Int(0),
                    Some(value) => Datum::Int(i64::from(value ^ falsity)),
                })
            }
            Kernel::Coalesce(domain) => {
                for index in 0..args.len() {
                    let value = typed_argument(index, domain)?;
                    if !value.is_null() {
                        return Ok(value);
                    }
                }
                Ok(Datum::Null)
            }
            Kernel::Logic(op) => {
                let left = condition(0)?;
                if (op == BinaryOp::LogicAnd && left == Some(false))
                    || (op == BinaryOp::LogicOr && left == Some(true))
                {
                    return Ok(Datum::Int(i64::from(left.unwrap())));
                }
                let right = condition(1)?;
                Ok(match (op, left, right) {
                    (BinaryOp::LogicAnd, _, Some(false)) => Datum::Int(0),
                    (BinaryOp::LogicOr, _, Some(true)) => Datum::Int(1),
                    (_, None, _) | (_, _, None) => Datum::Null,
                    // Go `builtinLogicXorSig`: true when exactly one is.
                    (BinaryOp::LogicXor, Some(left), Some(right)) => Datum::Int(i64::from(left != right)),
                    (_, _, Some(value)) => Datum::Int(i64::from(value)),
                })
            }
            Kernel::IntegerMultiply { unsigned } => {
                if args.len() != 2 {
                    return Err(EvalError::Unsupported("protobuf multiply arity"));
                }
                let left = eval_numeric_row(&args[0], ctx, row, EvalType::Int)?;
                let Some(left) = crate::arg_eval_type::eval_int(&left)? else {
                    return Ok(Datum::Null);
                };
                let right = eval_numeric_row(&args[1], ctx, row, EvalType::Int)?;
                let Some(right) = crate::arg_eval_type::eval_int(&right)? else {
                    return Ok(Datum::Null);
                };
                crate::ops::integer_multiply(left, right, unsigned).map_err(|_| {
                    match super::arithmetic_overflow_expression(function, BinaryOp::Mul, ctx) {
                        Some(expression) => EvalError::DataOutOfRange {
                            value: if unsigned {
                                "BIGINT UNSIGNED"
                            } else {
                                "BIGINT"
                            },
                            expression,
                        },
                        None => EvalError::IntOverflow,
                    }
                })
            }
            Kernel::IntegerMod { unsigned } => {
                // The four Go MOD signatures bake signedness into the builtin,
                // rather than reselecting it from each row or the SQL name.
                let operand = |index: usize| -> Result<Datum, EvalError> {
                    let value = typed_argument(index, EvalType::Int)?;
                    Ok(match crate::arg_eval_type::eval_int(&value)? {
                        None => Datum::Null,
                        Some(value) if unsigned[index] => Datum::UInt(value as u64),
                        Some(value) => Datum::Int(value),
                    })
                };
                let left = operand(0)?;
                let right = operand(1)?;
                crate::ops::eval_binary_full(
                    BinaryOp::Mod,
                    left,
                    right,
                    ctx.div_precision_increment(),
                    function.derived_collation(),
                    crate::ops::Operands::LITERALS,
                    ctx,
                )
            }
            Kernel::Binary(op, domain) => {
                if args.len() != 2 {
                    return Err(EvalError::Unsupported("protobuf binary builtin arity"));
                }
                let left = eval_numeric_operand_row(&args[0], ctx, row, domain)?;
                // Go `builtinArithmeticPlusRealSig` and `builtinArithmeticModRealSig`
                // read both operands before the NULL check; the other
                // signatures return on a NULL left.
                let reads_both =
                    domain == EvalType::Real && matches!(op, BinaryOp::Plus | BinaryOp::Mod);
                if left.is_null() && !reads_both {
                    return Ok(Datum::Null);
                }
                let right = eval_numeric_operand_row(&args[1], ctx, row, domain)?;
                function.eval_binary_values(op, left, right, ctx)
            }
            Kernel::Unary(op) => {
                let [arg] = args.as_slice() else {
                    return Err(EvalError::Unsupported("protobuf unary builtin arity"));
                };
                let value = arg.eval(ctx, row)?;
                crate::ops::eval_unary(op, value, crate::ops::Operand::Expr(arg), ctx)
            }
            Kernel::StringCompare(op) => {
                let [left, right] = args.as_slice() else {
                    return Err(EvalError::Unsupported("protobuf comparison arity"));
                };
                let left = eval_numeric_row(left, ctx, row, EvalType::String)?;
                let right = eval_numeric_row(right, ctx, row, EvalType::String)?;
                if matches!(
                    function.derived_collation(),
                    tidb_charset::Collation::Gb18030Bin
                        | tidb_charset::Collation::Utf8Mb4ZhPinyinTiDbAsCs
                ) {
                    // These SQL collations need TiDB's comparison semantics.
                    return function.eval_binary_values(op, left, right, ctx);
                }
                crate::rpn::evaluate_required_kernel(
                    function,
                    self.signature,
                    None,
                    &[EvalType::String; 2],
                    vec![left, right],
                    ctx,
                )
            }
            Kernel::In(domain) => {
                let Some(first) = args.first() else {
                    return Err(EvalError::Unsupported("protobuf IN arity"));
                };
                let left = eval_numeric_row(first, ctx, row, domain)?;
                if left.is_null() {
                    return Ok(Datum::Null);
                }
                let cache = function.in_cache.as_deref();
                let mut has_null = cache.is_some_and(|cache| cache.has_null);
                let candidates: Vec<_> = if let Some(cache) = cache.filter(|cache| !cache.is_empty()) {
                    let collator = tidb_datatype::get_collator(function.derived_collation().name());
                    if cache.contains(&left, collator, crate::wrap_cast::type_of(first).is_unsigned())? { return Ok(Datum::Int(1)); }
                    cache.non_const_args.iter().map(|index| &args[*index]).collect()
                } else { args[1..].iter().collect() };
                for candidate in candidates {
                    let right = eval_numeric_row(candidate, ctx, row, domain)?;
                    if right.is_null() {
                        has_null = true;
                        continue;
                    }
                    let equal = crate::ops::eval_binary_full(
                        BinaryOp::Eq,
                        left.clone(),
                        right,
                        ctx.div_precision_increment(),
                        function.derived_collation(),
                        crate::ops::Operands::of(first, candidate),
                        ctx,
                    )?;
                    if equal == Datum::Int(1) {
                        return Ok(equal);
                    }
                }
                Ok(if has_null { Datum::Null } else { Datum::Int(0) })
            }
            Kernel::Conv => {
                if args.len() != 3 {
                    return Err(EvalError::Unsupported("protobuf CONV arity"));
                }
                // Go evaluates the two bases before the ordinary text argument.
                let from = typed_argument(1, EvalType::Int)?;
                if from.is_null() {
                    return Ok(Datum::Null);
                }
                let to = typed_argument(2, EvalType::Int)?;
                if to.is_null() {
                    return Ok(Datum::Null);
                }
                let text = match conv_bit_literal(&args[0], ctx, row)? {
                    Some(literal) => literal,
                    None => typed_argument(0, EvalType::String)?,
                };
                crate::math_fn::conv(&[text, from, to])
            }
            Kernel::Like => function.eval_like(ctx, row, false),
            Kernel::Ilike => function.eval_like(ctx, row, true),
            Kernel::Row(eval) => eval(function, ctx, row),
            Kernel::DateArithmetic { subtract } => {
                if args.len() != 3 {
                    return Err(EvalError::Unsupported("protobuf date arithmetic arity"));
                }
                // Go evaluates the unit before the date and interval.
                let unit = eval_numeric_row(&args[2], ctx, row, EvalType::String)?;
                let Some(unit) = crate::arg_eval_type::eval_string(&unit)? else {
                    return Ok(Datum::Null);
                };
                let unit = std::str::from_utf8(&unit)
                    .map_err(|_| EvalError::Unsupported("invalid date interval unit"))?;
                function.eval_date_arithmetic(unit, subtract, ctx, row)
            }
            Kernel::Cast { source, target, .. } => {
                if args.len() != 1 {
                    return Err(EvalError::Unsupported("protobuf cast arity"));
                }
                if source == EvalType::String
                    && matches!(target, EvalType::Real | EvalType::Decimal)
                    && super::is_numeric_binary_literal(&args[0])
                {
                    return eval_numeric_row(&args[0], ctx, row, target);
                }
                // Go `builtinCastStringAsStringSig.evalString`: `EvalString`
                // of an ENUM or SET is its name, whatever `EnumSetAsIntFlag`.
                let value = if function.pb_code() == ScalarFuncSig::CastStringAsString {
                    args[0].eval_string(ctx, row)?
                } else {
                    eval_numeric_row(&args[0], ctx, row, source)?
                };
                self.cast_value(function, value, ctx)
            }
            Kernel::Regexp => function.eval_regexp_like(ctx, row),
            kernel => {
                let mut values = Vec::with_capacity(args.len());
                for arg in args {
                    let value = arg.eval(ctx, row)?;
                    // The root tier's evaluators read a NULL argument
                    // themselves: Go `builtinConcatWSSig` skips a NULL member
                    // and `builtinWeekWithModeSig` reads a NULL mode as 0.
                    if value.is_null() && !matches!(kernel, Kernel::Json | Kernel::RpnOnly(..) | Kernel::Values(_) | Kernel::Method(_)) {
                        return Ok(Datum::Null);
                    }
                    values.push(value);
                }
                self.eval_values(function, values, ctx)
            }
        }
    }
}

fn cast_types(sig: ScalarFuncSig) -> Option<(EvalType, EvalType)> {
    use ScalarFuncSig::*;
    Some(match sig {
        CastIntAsInt => (EvalType::Int, EvalType::Int),
        CastRealAsReal => (EvalType::Real, EvalType::Real),
        CastDecimalAsDecimal => (EvalType::Decimal, EvalType::Decimal),
        CastStringAsString => (EvalType::String, EvalType::String),
        CastDurationAsDuration => (EvalType::Duration, EvalType::Duration),
        CastJsonAsJson => (EvalType::Json, EvalType::Json),
        CastDecimalAsDuration => (EvalType::Decimal, EvalType::Duration),
        CastDecimalAsInt => (EvalType::Decimal, EvalType::Int),
        CastDecimalAsJson => (EvalType::Decimal, EvalType::Json),
        CastDecimalAsReal => (EvalType::Decimal, EvalType::Real),
        CastDecimalAsString => (EvalType::Decimal, EvalType::String),
        CastDecimalAsTime => (EvalType::Decimal, EvalType::Datetime),
        CastDurationAsDecimal => (EvalType::Duration, EvalType::Decimal),
        CastDurationAsInt => (EvalType::Duration, EvalType::Int),
        CastDurationAsJson => (EvalType::Duration, EvalType::Json),
        CastDurationAsReal => (EvalType::Duration, EvalType::Real),
        CastDurationAsString => (EvalType::Duration, EvalType::String),
        CastDurationAsTime => (EvalType::Duration, EvalType::Datetime),
        CastIntAsDecimal => (EvalType::Int, EvalType::Decimal),
        CastIntAsDuration => (EvalType::Int, EvalType::Duration),
        CastIntAsJson => (EvalType::Int, EvalType::Json),
        CastIntAsReal => (EvalType::Int, EvalType::Real),
        CastIntAsString => (EvalType::Int, EvalType::String),
        CastIntAsTime => (EvalType::Int, EvalType::Datetime),
        CastJsonAsDecimal => (EvalType::Json, EvalType::Decimal),
        CastJsonAsDuration => (EvalType::Json, EvalType::Duration),
        CastJsonAsInt => (EvalType::Json, EvalType::Int),
        CastJsonAsReal => (EvalType::Json, EvalType::Real),
        CastJsonAsString => (EvalType::Json, EvalType::String),
        CastJsonAsTime => (EvalType::Json, EvalType::Datetime),
        CastRealAsDecimal => (EvalType::Real, EvalType::Decimal),
        CastRealAsDuration => (EvalType::Real, EvalType::Duration),
        CastRealAsInt => (EvalType::Real, EvalType::Int),
        CastRealAsJson => (EvalType::Real, EvalType::Json),
        CastRealAsString => (EvalType::Real, EvalType::String),
        CastRealAsTime => (EvalType::Real, EvalType::Datetime),
        CastStringAsDecimal => (EvalType::String, EvalType::Decimal),
        CastStringAsDuration => (EvalType::String, EvalType::Duration),
        CastStringAsInt => (EvalType::String, EvalType::Int),
        CastStringAsJson => (EvalType::String, EvalType::Json),
        CastStringAsReal => (EvalType::String, EvalType::Real),
        CastStringAsTime => (EvalType::String, EvalType::Datetime),
        CastTimeAsDecimal => (EvalType::Datetime, EvalType::Decimal),
        CastTimeAsDuration => (EvalType::Datetime, EvalType::Duration),
        CastTimeAsInt => (EvalType::Datetime, EvalType::Int),
        CastTimeAsJson => (EvalType::Datetime, EvalType::Json),
        CastTimeAsReal => (EvalType::Datetime, EvalType::Real),
        CastTimeAsString => (EvalType::Datetime, EvalType::String),
        CastTimeAsTime => (EvalType::Datetime, EvalType::Datetime),
        CastVectorFloat32AsString => (EvalType::VectorFloat32, EvalType::String),
        CastVectorFloat32AsVectorFloat32 => (EvalType::VectorFloat32, EvalType::VectorFloat32),
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::constant::Constant;
    use crate::expression::Expression;
    use tidb_datatype::FieldTypeCode;
    use tidb_proto::tipb;

    fn literal(domain: EvalType) -> tipb::Expr {
        let (value, code) = match domain {
            EvalType::Int => (Datum::Int(1), FieldTypeCode::LongLong),
            EvalType::Real => (Datum::Real(1.0), FieldTypeCode::Double),
            EvalType::Decimal => (
                Datum::Decimal(tidb_datatype::Decimal::from_literal("1")),
                FieldTypeCode::NewDecimal,
            ),
            EvalType::String => (Datum::new_string_datum("DAY"), FieldTypeCode::VarString),
            EvalType::Datetime => (
                Datum::Time(
                    tidb_datatype::parse_datetime("2008-02-20", &chrono::Utc, true, false)
                        .unwrap()
                        .time,
                ),
                FieldTypeCode::Datetime,
            ),
            EvalType::Duration => (
                Datum::Duration(
                    tidb_datatype::MySqlDuration::from_nanoseconds(3_600_000_000_000, 0).unwrap(),
                ),
                FieldTypeCode::Duration,
            ),
            EvalType::Json => return tipb::Expr {
                tp: Some(tipb::ExprType::ScalarFunc as i32),
                sig: Some(ScalarFuncSig::CastIntAsJson as i32),
                children: vec![literal(EvalType::Int)],
                field_type: Some(tipb::FieldType {
                    tp: Some(i32::from(FieldTypeCode::Json.mysql_type())),
                    ..Default::default()
                }),
                ..Default::default()
            },
            EvalType::VectorFloat32 => (
                Datum::VectorFloat32(VectorFloat32::parse("[1,2]").unwrap()),
                FieldTypeCode::VectorFloat32,
            ),
            domain => panic!("no literal for {domain:?}"),
        };
        let constant = Expression::Constant(Constant::new(value, FieldType::new(code)));
        crate::expr_to_pb::PbConverter::new(&crate::NoColumns, false, &tidb_datatype::SessionTimeZone::utc())
            .expr_to_pb(&constant)
            .unwrap()
    }

    fn eval(signature: ScalarFuncSig, children: Vec<tipb::Expr>) -> Datum {
        let pb = tipb::Expr {
            tp: Some(tipb::ExprType::ScalarFunc as i32),
            sig: Some(signature as i32),
            children,
            field_type: Some(tipb::FieldType {
                tp: Some(8),
                ..Default::default()
            }),
            ..Default::default()
        };
        let row = tidb_chunk::mutrow::MutRow::mut_row_from_datums(&[]);
        crate::distsql_builtin::pb_to_expr(&pb, &[])
            .unwrap_or_else(|error| panic!("{signature:?}: {error}"))
            .eval(&crate::NoColumns, row.to_row())
            .unwrap_or_else(|error| panic!("{signature:?}: {error:?}"))
    }

    /// The signatures evaluated by the root tier's evaluators see their NULL
    /// arguments. Go answers NULL for a NULL argument of every one of them
    /// but two: `builtinConcatWSSig` skips a NULL member after the separator
    /// (`TestConcatWS`), and `builtinWeekWithModeSig` reads a NULL mode as 0
    /// (`TestWeek`).
    #[test]
    fn a_null_argument_is_read_by_the_signature_as_go_does() {
        let null = tipb::Expr {
            tp: Some(tipb::ExprType::Null as i32),
            ..Default::default()
        };
        let text = |value: &str| {
            let constant = Expression::Constant(Constant::new(
                Datum::new_string_datum(value),
                FieldType::new(FieldTypeCode::VarString),
            ));
            crate::expr_to_pb::PbConverter::new(&crate::NoColumns, false, &tidb_datatype::SessionTimeZone::utc())
            .expr_to_pb(&constant)
            .unwrap()
        };
        let mut swept = std::collections::BTreeSet::new();
        // `ceil` and `floor` resolve outside the catalog's rows.
        let rounding = {
            use ScalarFuncSig::*;
            [
                (CeilIntToInt, &[EvalType::Int]),
                (CeilReal, &[EvalType::Real]),
                (CeilDecToInt, &[EvalType::Decimal]),
                (CeilDecToDec, &[EvalType::Decimal]),
                (FloorIntToInt, &[EvalType::Int]),
                (FloorReal, &[EvalType::Real]),
                (FloorDecToInt, &[EvalType::Decimal]),
                (FloorDecToDec, &[EvalType::Decimal]),
            ]
        };
        let built = crate::tests::pb_code_matrix_source::signature_arg_types();
        let rows = built
            .iter()
            .map(|(sig, domains)| (*sig, &domains[..]))
            .chain(rounding.iter().map(|(sig, domains)| (*sig, &domains[..])));
        for (sig, arg_types) in rows {
            let Some(Kernel::Values(_) | Kernel::RpnOnly(..)) = PbBuiltin::new(sig).map(|builtin| builtin.kernel) else {
                continue;
            };
            swept.insert(sig);
            // A NULL member of CONCAT_WS is skipped; its rows are below.
            let nullable = if sig == ScalarFuncSig::ConcatWs { 1 } else { arg_types.len() };
            for position in 0..nullable {
                let children = arg_types
                    .iter()
                    .enumerate()
                    .map(|(index, domain)| {
                        if index == position {
                            null.clone()
                        } else {
                            literal(*domain)
                        }
                    })
                    .collect();
                use ScalarFuncSig::*;
                let expect = match (sig, position) {
                    (WeekWithMode, 1) => Datum::Int(7),
                    (IntIsNull | RealIsNull | DecimalIsNull | StringIsNull | TimeIsNull
                    | DurationIsNull | JsonIsNull | VectorFloat32IsNull, _) => Datum::Int(1),
                    (NullEqInt | NullEqReal | NullEqDecimal | NullEqTime | NullEqDuration
                    | NullEqJson | NullEqJson | NullEqVectorFloat32 | IntIsTrue | RealIsTrue | DecimalIsTrue
                    | IntIsFalse | RealIsFalse | DecimalIsFalse, _) => Datum::Int(0),
                    _ => Datum::Null,
                };
                assert_eq!(eval(sig, children), expect, "{sig:?} {position}");
            }
        }
        assert!(swept.len() > 40, "{swept:?}");

        use ScalarFuncSig::{Concat, ConcatWs};
        let bytes = |value: Datum| value.sql_bytes().unwrap();
        assert_eq!(
            eval(Concat, vec![text("a"), null.clone(), text("b")]),
            Datum::Null
        );
        assert_eq!(eval(ConcatWs, vec![null.clone(), null.clone()]), Datum::Null);
        assert_eq!(
            eval(ConcatWs, vec![null.clone(), text("a"), text("b")]),
            Datum::Null
        );
        assert_eq!(
            bytes(eval(
                ConcatWs,
                vec![text("|"), text("a"), null.clone(), text("b"), text("c")]
            )),
            b"a|b|c"
        );
        assert_eq!(
            bytes(eval(
                ConcatWs,
                vec![text(","), text("a"), text(","), text("b"), text("c")]
            )),
            b"a,,,b,c"
        );
    }
}
