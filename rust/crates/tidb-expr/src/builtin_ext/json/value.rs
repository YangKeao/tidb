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

//! SQL datum to binary-JSON conversion used by the retained cast/type bridge.

use serde_json::Value as Json;

use crate::{Datum, EvalError, JsonError};
use tidb_datatype::BinaryJSON;

/// Converts one already-evaluated SQL datum to the binary JSON representation
/// required by a `CAST(... AS JSON)` node. Expression execution itself remains
/// exclusively in the TiKV engine.
pub(crate) fn cast_as_json(value: &Datum) -> Result<Datum, EvalError> {
    if value.is_null() {
        return Ok(Datum::Null);
    }
    if let Some(typed) = typed_cast_json(value) {
        return typed;
    }
    let json = match json_sql_string(value)? {
        Some(text) => parse_json(text)?,
        None => datum_json_scalar(value)?,
    };
    binary_json_datum(json)
}

fn typed_cast_json(value: &Datum) -> Option<Result<Datum, EvalError>> {
    match value {
        Datum::Time(time) => {
            let mut time = *time;
            if time.set_fsp(tidb_datatype::MAX_FSP).is_err() {
                return Some(Err(EvalError::Unsupported("datum JSON conversion")));
            }
            Some(
                Datum::Time(time)
                    .to_mysql_json()
                    .map(Datum::Json)
                    .map_err(|_| EvalError::Unsupported("datum JSON conversion")),
            )
        }
        Datum::Duration(duration) => Some(
            tidb_datatype::MySqlDuration::from_nanoseconds(
                duration.nanoseconds(),
                tidb_datatype::MAX_FSP,
            )
            .map_err(|_| EvalError::Unsupported("datum JSON conversion"))
            .and_then(|duration| {
                Datum::Duration(duration)
                    .to_mysql_json()
                    .map(Datum::Json)
                    .map_err(|_| EvalError::Unsupported("datum JSON conversion"))
            }),
        ),
        Datum::BinaryLiteral(literal) => Some(
            BinaryJSON::from_typed_value(&tidb_datatype::BinaryJSONValue::Opaque(
                tidb_datatype::Opaque {
                    type_code: tidb_datatype::FieldTypeCode::VarString.mysql_type(),
                    bytes: literal.as_bytes().to_vec(),
                },
            ))
            .map(Datum::Json)
            .map_err(|_| EvalError::Unsupported("datum JSON conversion")),
        ),
        _ => None,
    }
}

fn json_sql_string(value: &Datum) -> Result<Option<&str>, EvalError> {
    let bytes = match value {
        Datum::String(text) => text.bytes(),
        Datum::Bytes(bytes) => bytes.as_slice(),
        _ => return Ok(None),
    };
    std::str::from_utf8(bytes)
        .map(Some)
        .map_err(|_| EvalError::Unsupported("invalid UTF-8 string datum"))
}

fn binary_json_datum(json: Json) -> Result<Datum, EvalError> {
    BinaryJSON::parse(&json.to_string())
        .map(Datum::Json)
        .map_err(|_| EvalError::Json(JsonError::InvalidText))
}

fn datum_json_scalar(value: &Datum) -> Result<Json, EvalError> {
    let binary = value
        .to_mysql_json()
        .map_err(|_| EvalError::Unsupported("datum JSON conversion"))?;
    parse_json(&binary.to_string())
}

fn parse_json(text: &str) -> Result<Json, EvalError> {
    if text.trim().is_empty() {
        return Err(EvalError::Json(JsonError::EmptyText));
    }
    serde_json::from_str(text).map_err(|_| EvalError::Json(JsonError::InvalidText))
}
