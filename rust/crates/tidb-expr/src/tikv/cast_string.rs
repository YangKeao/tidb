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

use crate::{Columns, Datum, EvalError};
use tidb_datatype::FieldType;
use tidb_query_expr::{NativeCastStringInput as Input, NativeCastStringTarget as Target};

fn input(value: &Datum) -> Input<'_> {
    match value {
        Datum::Int(value) => Input::Int(*value),
        Datum::UInt(value) => Input::UInt(*value),
        Datum::String(value) => Input::String(value.bytes()),
        Datum::Bytes(value) => Input::Bytes(value),
        Datum::BinaryLiteral(value) => Input::BinaryLiteral(value.as_bytes()),
        Datum::Bit(value) => Input::Bit(value.as_bytes()),
        _ => Input::Other,
    }
}

fn evaluate(
    ctx: &dyn Columns,
    value: &Datum,
    source: Option<&FieldType>,
    target: Target<'_>,
) -> Result<Datum, EvalError> {
    let source = source.map(|field| tidb_query_expr::NativeCastStringSource {
        code: field.code().as_shared_string_type(),
        collation: field.collation_name(),
    });
    let result = tidb_query_expr::native_cast_string(
        input(value),
        target,
        source,
        || ctx.connection_charset_info().0,
        || value.sql_string(),
        || ctx.max_allowed_packet(),
        // The original handler owns any repeated limit/policy reads. Do not
        // substitute a cached limit or a precomputed warning here.
        |name| ctx.handle_allowed_packet_overflowed(name),
        |code, message| ctx.append_warning(code, message),
    )
    .map_err(|error| match error {
        tidb_query_expr::NativeCastStringError::Child(error) => error,
        tidb_query_expr::NativeCastStringError::InvalidUtf8StringCoercion => {
            EvalError::Unsupported("invalid UTF-8 string coercion")
        }
    })?;
    Ok(match result {
        tidb_query_expr::NativeCastStringResult::Null => Datum::Null,
        tidb_query_expr::NativeCastStringResult::Text(value) => Datum::new_string(value),
        tidb_query_expr::NativeCastStringResult::Bytes(value) => Datum::new_bytes(value),
    })
}

pub(crate) fn eval_cast_char_in(
    ctx: &dyn Columns,
    value: &Datum,
    source: Option<&FieldType>,
    len: Option<u32>,
    charset: Option<&str>,
) -> Result<Datum, EvalError> {
    evaluate(ctx, value, source, Target::Char { len, charset })
}

pub(crate) fn eval_cast_binary_in(
    ctx: &dyn Columns,
    value: &Datum,
    source: Option<&FieldType>,
    len: Option<u32>,
) -> Result<Datum, EvalError> {
    evaluate(ctx, value, source, Target::Binary { len })
}
