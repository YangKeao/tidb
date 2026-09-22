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

//! Scalar datum conversions retained by executor/planner bridges.

use tidb_datatype::{Datum, StringDatum};

use crate::context::EvalError;

pub fn truthy_of(value: &Datum) -> Result<Option<bool>, EvalError> {
    if matches!(value, Datum::Null) {
        return Ok(None);
    }
    value
        .to_bool()
        .map(|converted| Some(converted.value != 0))
        .map_err(|_| EvalError::Unsupported("truth coercion of a non-SQL datum"))
}

fn string_text(value: &StringDatum) -> Result<&str, EvalError> {
    value
        .as_utf8()
        .map_err(|_| EvalError::Unsupported("invalid UTF-8 string datum"))
}

pub(crate) fn coerce_str(value: &Datum) -> Result<Option<String>, EvalError> {
    match value {
        Datum::String(value) => Ok(Some(string_text(value)?.to_string())),
        Datum::Bytes(value) => std::str::from_utf8(value)
            .map(|text| Some(text.to_string()))
            .map_err(|_| EvalError::Unsupported("invalid UTF-8 byte datum")),
        Datum::Int(value) => Ok(Some(value.to_string())),
        Datum::UInt(value) => Ok(Some(value.to_string())),
        Datum::Decimal(value) => Ok(Some(value.to_string())),
        Datum::Real(value) => Ok(Some(value.to_string())),
        Datum::Float32(value) => Ok(Some((*value as f32).to_string())),
        Datum::BinaryLiteral(value) | Datum::Bit(value) => std::str::from_utf8(value.as_bytes())
            .map(|text| Some(text.to_owned()))
            .map_err(|_| EvalError::Unsupported("invalid UTF-8 binary literal")),
        Datum::Duration(value) => Ok(Some(value.to_string())),
        Datum::Enum(value, _) => value
            .name()
            .as_utf8()
            .map(|text| Some(text.to_owned()))
            .map_err(|_| EvalError::Unsupported("invalid UTF-8 ENUM name")),
        Datum::Set(value, _) => value
            .name()
            .as_utf8()
            .map(|text| Some(text.to_owned()))
            .map_err(|_| EvalError::Unsupported("invalid UTF-8 SET name")),
        Datum::Time(value) => Ok(Some(value.to_string())),
        Datum::Json(value) => Ok(Some(value.to_string())),
        Datum::Raw(value) => std::str::from_utf8(value)
            .map(|text| Some(text.to_owned()))
            .map_err(|_| EvalError::Unsupported("invalid UTF-8 raw datum")),
        Datum::VectorFloat32(value) => Ok(Some(value.to_string())),
        Datum::Null => Ok(None),
        Datum::MinNotNull | Datum::MaxValue => {
            Err(EvalError::Unsupported("range sentinel string coercion"))
        }
    }
}
