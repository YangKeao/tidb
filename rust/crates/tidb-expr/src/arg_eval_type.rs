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

//! Typed datum readers used by retained planning/type bridges.

use crate::{Datum, EvalError};

pub(crate) fn eval_string(value: &Datum) -> Result<Option<Vec<u8>>, EvalError> {
    match value {
        Datum::Null => Ok(None),
        Datum::String(value) => Ok(Some(value.bytes().to_vec())),
        Datum::Bytes(value) => Ok(Some(value.clone())),
        Datum::BinaryLiteral(value) => Ok(Some(value.as_bytes().to_vec())),
        Datum::Enum(value, _) => Ok(Some(value.name_bytes().to_vec())),
        Datum::Set(value, _) => Ok(Some(value.name_bytes().to_vec())),
        _ => Err(EvalError::Unsupported("un-cast types.ETString argument")),
    }
}
