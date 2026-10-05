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

//! Native projections for the SDK-owned JSON coercion policy. Document/value
//! interpretation, typed casts, metadata decisions and parser-domain selection
//! remain in the shared owner; these entry points preserve their original
//! visibility, borrowed string lifetimes and native error/result carriers.

use serde_json::Value as Json;
use std::borrow::Cow;
use tidb_datatype::{BinaryJSON, FieldType};
use tidb_query_datatype::codec::native_mysql_json::NativeDatumJsonSource;

pub(super) use crate::tikv::NativeJsonStringArgument as StringArgument;
use crate::tikv::{NativeJsonCoercionError, NativeJsonCoercionSource};
use crate::{Datum, EvalError, JsonError};

fn source(field_type: Option<&FieldType>) -> Option<NativeJsonCoercionSource<'_>> {
    field_type.map(|field| NativeJsonCoercionSource {
        datum: NativeDatumJsonSource {
            code: field.code().as_shared_type_name_code(),
            string_code: field.code().as_shared_string_type(),
            collation: field.collation_name(),
            flen: field.flen(),
        },
        flags: field.raw_flags(),
    })
}

fn coercion_error(error: NativeJsonCoercionError) -> EvalError {
    match error {
        NativeJsonCoercionError::Unsupported(message) => EvalError::Unsupported(message),
        NativeJsonCoercionError::FloatOverflow => EvalError::FloatOverflow,
        NativeJsonCoercionError::EmptyText => EvalError::Json(JsonError::EmptyText),
        NativeJsonCoercionError::InvalidText => EvalError::Json(JsonError::InvalidText),
        NativeJsonCoercionError::InvalidTypeForJson { argument, function } => {
            EvalError::Json(JsonError::InvalidTypeForJson { argument, function })
        }
    }
}

fn binary_datum((type_code, value): (u8, Vec<u8>)) -> Datum {
    Datum::Json(BinaryJSON::from_encoded_parts(type_code, value))
}

fn cast_result(
    result: Result<Option<(u8, Vec<u8>)>, NativeJsonCoercionError>,
) -> Result<Datum, EvalError> {
    result
        .map(|value| value.map_or(Datum::Null, binary_datum))
        .map_err(coercion_error)
}

/// Returns the SDK-selected borrowed SQL string, without allocating or decoding
/// a JSON datum on behalf of signatures that require a STRING specifically.
pub(super) fn json_sql_string(value: &Datum) -> Result<Option<&str>, EvalError> {
    crate::tikv::native_json_sql_string(value.as_shared_json_input()).map_err(coercion_error)
}

/// JSON document display is owned; actual SQL string storage remains borrowed.
pub(super) fn json_document_string(value: &Datum) -> Result<Option<Cow<'_, str>>, EvalError> {
    crate::tikv::native_json_document_string(value.as_shared_json_input()).map_err(coercion_error)
}

pub(crate) fn cast_as_json(value: &Datum) -> Result<Datum, EvalError> {
    cast_result(crate::tikv::native_cast_as_json(
        value.as_shared_json_input(),
    ))
}

pub(super) fn binary_json_datum(json: Json) -> Result<Datum, EvalError> {
    crate::tikv::native_binary_json_datum(json)
        .map(binary_datum)
        .map_err(coercion_error)
}

pub(crate) fn cast_as_json_typed(
    value: &Datum,
    field_type: Option<&FieldType>,
) -> Result<Datum, EvalError> {
    cast_result(crate::tikv::native_cast_as_json_typed(
        value.as_shared_json_input(),
        source(field_type),
    ))
}

pub(crate) fn cast_as_json_value_typed(
    value: &Datum,
    field_type: Option<&FieldType>,
) -> Result<Datum, EvalError> {
    cast_result(crate::tikv::native_cast_as_json_value_typed(
        value.as_shared_json_input(),
        source(field_type),
    ))
}

pub(crate) fn validate_json_cast_source(field_type: Option<&FieldType>) -> Result<(), EvalError> {
    crate::tikv::native_json_cast_admission(source(field_type)).map_err(|error| match error {
        crate::tikv::NativeJsonCastAdmissionError::MissingSource => {
            EvalError::Unsupported(error.message())
        }
        crate::tikv::NativeJsonCastAdmissionError::Vector => {
            EvalError::Vector(error.message().to_owned())
        }
    })
}

pub(crate) fn json_cast_source_supported(field_type: Option<&FieldType>) -> bool {
    crate::tikv::native_json_cast_admission(source(field_type)).is_ok()
}

pub(crate) fn cast_json_prepared(
    value: &Datum,
    field_type: Option<&FieldType>,
    target: Option<&FieldType>,
) -> Result<Datum, EvalError> {
    cast_result(crate::tikv::native_cast_json_prepared_argument(
        value.as_shared_json_input(),
        source(field_type),
        target.map(FieldType::raw_flags),
    ))
}

/// The shared mode retains the distinction between a document argument and a
/// string value, while actual source metadata stays separate from the mode.
pub(super) fn json_argument(
    value: &Datum,
    string: StringArgument,
    field_type: Option<&FieldType>,
) -> Result<Json, EvalError> {
    crate::tikv::native_json_argument(value.as_shared_json_input(), string, source(field_type))
        .map_err(coercion_error)
}

pub(crate) fn parse_json_document_argument(value: &Datum) -> Result<Option<Json>, EvalError> {
    crate::tikv::native_parse_json_document_argument(value.as_shared_json_input())
        .map_err(coercion_error)
}

pub(crate) fn json_document_text_argument(value: &Datum) -> Result<Option<String>, EvalError> {
    crate::tikv::native_json_document_text_argument(value.as_shared_json_input())
        .map_err(coercion_error)
}

pub(crate) fn parse_json_document_argument_strict(
    value: &Datum,
    argument: usize,
    function: &'static str,
) -> Result<Option<Json>, EvalError> {
    crate::tikv::native_parse_json_document_argument_strict(
        value.as_shared_json_input(),
        argument,
        function,
    )
    .map_err(coercion_error)
}

/// This is the expression document parser, not BinaryJSON's text parser with
/// its distinct surrogate-retry and trailing-input error policy.
pub(super) fn parse_json(text: &str) -> Result<Json, EvalError> {
    crate::tikv::native_parse_json_expression(text).map_err(coercion_error)
}
