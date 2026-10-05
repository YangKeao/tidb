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

use crate::{Datum, EvalError};
use tidb_datatype::{FieldType, FieldTypeCode, UNSPECIFIED_LENGTH};
use tidb_query_expr::{NativeArgStringResult, NativeArgStringSource, NativeArgStringType};

pub(crate) fn eval_cast_arg_as_string(value: &Datum) -> Result<Datum, EvalError> {
    tidb_query_expr::native_cast_arg_as_string(value.as_shared_json_input())
        .map(|result| match result {
            NativeArgStringResult::Original => value.clone(),
            NativeArgStringResult::Binary(bytes) => Datum::new_bytes(bytes),
            NativeArgStringResult::String(bytes) => Datum::new_string(bytes),
            NativeArgStringResult::Null => Datum::Null,
        })
        .map_err(EvalError::Unsupported)
}

pub(crate) fn eval_cast_arg_as_string_type(
    source: &FieldType,
    explicit_collation: bool,
    connection: (&str, &str),
) -> FieldType {
    let input = NativeArgStringSource {
        eval_type: source.eval_type(),
        code: source.code().as_shared_type_name_code(),
        flen: source.flen(),
        decimal: source.decimal(),
        charset: source.charset_name(),
        collation: source.collation_name(),
    };
    match tidb_query_expr::native_cast_arg_as_string_type(input, explicit_collation, connection) {
        NativeArgStringType::Original => source.clone(),
        NativeArgStringType::VarString {
            flen,
            charset,
            collation,
        } => {
            let mut target = FieldType::new(FieldTypeCode::VarString);
            target.set_charset_name(charset);
            target.set_collation_name(collation);
            target.set_flen(flen);
            target.set_decimal(UNSPECIFIED_LENGTH);
            target
        }
    }
}
