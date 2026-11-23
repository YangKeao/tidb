// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

use tidb_datatype::{EvalType, FieldType, FieldTypeCode, FieldTypeFlags};
use tidb_proto::tipb::ScalarFuncSig;

use crate::{scalar_function::ScalarFunction, wrap_cast::set_bin_chs_cln_flag};

pub(super) fn field_type_to_pb(field: &FieldType) -> Option<tidb_proto::tipb::FieldType> {
    let mut ty = crate::expr_to_pb::to_pb_field_type(field);
    ty.array = Some(field.is_array());
    Some(ty)
}

pub(super) fn local_name(function: &ScalarFunction) -> &str {
    if function.is_cast() {
        if let Some(target) = function.get_static_type() {
            return match target.eval_type() {
                EvalType::Int if target.is_unsigned() => "cast_unsigned",
                EvalType::Int => "cast_signed",
                EvalType::Real if target.code() == FieldTypeCode::Float => "cast_float",
                EvalType::Real => "cast_double",
                EvalType::Decimal => "cast_decimal",
                EvalType::String if target.is_binary_string() => "cast_binary",
                EvalType::String => "cast_char",
                EvalType::Datetime if target.code() == FieldTypeCode::Date => "cast_date",
                EvalType::Datetime => "cast_datetime",
                EvalType::Duration => "cast_time",
                EvalType::Json => "cast_json",
                EvalType::VectorFloat32 => "cast_vector",
                _ => function.func_name.lowercase(),
            };
        }
    }
    function.func_name.lowercase()
}

pub(super) fn function_signature(function: &ScalarFunction) -> Option<ScalarFuncSig> {
    let signature = function.pb_code();
    (signature != ScalarFuncSig::Unspecified).then_some(signature)
}

pub(crate) const fn cast_signature(from: EvalType, to: EvalType) -> Option<Option<ScalarFuncSig>> {
    if matches!(
        (from, to),
        (EvalType::Int, EvalType::Int)
            | (EvalType::Real, EvalType::Real)
            | (EvalType::Decimal, EvalType::Decimal)
            | (EvalType::String, EvalType::String)
            | (EvalType::Datetime, EvalType::Datetime)
            | (EvalType::Timestamp, EvalType::Timestamp)
            | (EvalType::Timestamp, EvalType::Datetime)
            | (EvalType::VectorFloat32, EvalType::VectorFloat32)
            | (EvalType::Duration, EvalType::Duration)
            | (EvalType::Json, EvalType::Json)
    ) {
        // Go returns the argument untouched, so no node is added at all.
        return Some(None);
    }
    match explicit_cast_signature(from, to) {
        // The wire encoder does not insert vector formatting implicitly.
        Some(ScalarFuncSig::CastVectorFloat32AsString) | None => None,
        Some(sig) => Some(Some(sig)),
    }
}

/// Resolve a CAST even when changing only the target's metadata.
pub(crate) const fn explicit_cast_signature(from: EvalType, to: EvalType) -> Option<ScalarFuncSig> {
    Some(match (from, to) {
        (EvalType::Int, EvalType::Int) => ScalarFuncSig::CastIntAsInt,
        (EvalType::Real, EvalType::Real) => ScalarFuncSig::CastRealAsReal,
        (EvalType::Decimal, EvalType::Decimal) => ScalarFuncSig::CastDecimalAsDecimal,
        (EvalType::String, EvalType::String) => ScalarFuncSig::CastStringAsString,
        (EvalType::Duration, EvalType::Duration) => ScalarFuncSig::CastDurationAsDuration,
        (EvalType::Json, EvalType::Json) => ScalarFuncSig::CastJsonAsJson,
        (EvalType::VectorFloat32, EvalType::String) => ScalarFuncSig::CastVectorFloat32AsString,
        (EvalType::VectorFloat32, EvalType::VectorFloat32) => {
            ScalarFuncSig::CastVectorFloat32AsVectorFloat32
        }
        (EvalType::Real, EvalType::Int) => ScalarFuncSig::CastRealAsInt,
        (EvalType::Int, EvalType::Real) => ScalarFuncSig::CastIntAsReal,
        (EvalType::Decimal, EvalType::Int) => ScalarFuncSig::CastDecimalAsInt,
        (EvalType::Decimal, EvalType::Real) => ScalarFuncSig::CastDecimalAsReal,
        (EvalType::String, EvalType::Int) => ScalarFuncSig::CastStringAsInt,
        (EvalType::String, EvalType::Real) => ScalarFuncSig::CastStringAsReal,
        (EvalType::Int, EvalType::String) => ScalarFuncSig::CastIntAsString,
        (EvalType::Int, EvalType::Decimal) => ScalarFuncSig::CastIntAsDecimal,
        (EvalType::Int, EvalType::Datetime | EvalType::Timestamp) => ScalarFuncSig::CastIntAsTime,
        (EvalType::Int, EvalType::Duration) => ScalarFuncSig::CastIntAsDuration,
        (EvalType::Int, EvalType::Json) => ScalarFuncSig::CastIntAsJson,
        (EvalType::Real, EvalType::String) => ScalarFuncSig::CastRealAsString,
        (EvalType::Real, EvalType::Decimal) => ScalarFuncSig::CastRealAsDecimal,
        (EvalType::Real, EvalType::Datetime | EvalType::Timestamp) => ScalarFuncSig::CastRealAsTime,
        (EvalType::Real, EvalType::Duration) => ScalarFuncSig::CastRealAsDuration,
        (EvalType::Real, EvalType::Json) => ScalarFuncSig::CastRealAsJson,
        (EvalType::Decimal, EvalType::String) => ScalarFuncSig::CastDecimalAsString,
        (EvalType::Decimal, EvalType::Datetime | EvalType::Timestamp) => {
            ScalarFuncSig::CastDecimalAsTime
        }
        (EvalType::Decimal, EvalType::Duration) => ScalarFuncSig::CastDecimalAsDuration,
        (EvalType::Decimal, EvalType::Json) => ScalarFuncSig::CastDecimalAsJson,
        (EvalType::String, EvalType::Decimal) => ScalarFuncSig::CastStringAsDecimal,
        (EvalType::String, EvalType::Datetime | EvalType::Timestamp) => {
            ScalarFuncSig::CastStringAsTime
        }
        (EvalType::String, EvalType::Duration) => ScalarFuncSig::CastStringAsDuration,
        (EvalType::String, EvalType::Json) => ScalarFuncSig::CastStringAsJson,
        (EvalType::Datetime | EvalType::Timestamp, EvalType::Int) => ScalarFuncSig::CastTimeAsInt,
        (EvalType::Datetime | EvalType::Timestamp, EvalType::Real) => ScalarFuncSig::CastTimeAsReal,
        (EvalType::Datetime | EvalType::Timestamp, EvalType::String) => {
            ScalarFuncSig::CastTimeAsString
        }
        (EvalType::Datetime | EvalType::Timestamp, EvalType::Decimal) => {
            ScalarFuncSig::CastTimeAsDecimal
        }
        (EvalType::Datetime | EvalType::Timestamp, EvalType::Datetime | EvalType::Timestamp) => {
            ScalarFuncSig::CastTimeAsTime
        }
        (EvalType::Datetime | EvalType::Timestamp, EvalType::Duration) => {
            ScalarFuncSig::CastTimeAsDuration
        }
        (EvalType::Datetime | EvalType::Timestamp, EvalType::Json) => ScalarFuncSig::CastTimeAsJson,
        (EvalType::Duration, EvalType::Int) => ScalarFuncSig::CastDurationAsInt,
        (EvalType::Duration, EvalType::Real) => ScalarFuncSig::CastDurationAsReal,
        (EvalType::Duration, EvalType::String) => ScalarFuncSig::CastDurationAsString,
        (EvalType::Duration, EvalType::Decimal) => ScalarFuncSig::CastDurationAsDecimal,
        (EvalType::Duration, EvalType::Datetime | EvalType::Timestamp) => {
            ScalarFuncSig::CastDurationAsTime
        }
        (EvalType::Duration, EvalType::Json) => ScalarFuncSig::CastDurationAsJson,
        (EvalType::Json, EvalType::Int) => ScalarFuncSig::CastJsonAsInt,
        (EvalType::Json, EvalType::Real) => ScalarFuncSig::CastJsonAsReal,
        (EvalType::Json, EvalType::String) => ScalarFuncSig::CastJsonAsString,
        (EvalType::Json, EvalType::Decimal) => ScalarFuncSig::CastJsonAsDecimal,
        (EvalType::Json, EvalType::Datetime | EvalType::Timestamp) => ScalarFuncSig::CastJsonAsTime,
        (EvalType::Json, EvalType::Duration) => ScalarFuncSig::CastJsonAsDuration,
        _ => return None,
    })
}

pub(super) use crate::wrap_cast::{
    decimal_cast_field_type, duration_cast_field_type, temporal_cast_field_type,
};
