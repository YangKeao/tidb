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
use tidb_datatype::{FieldType, MySqlDuration};
use tidb_query_expr::{NativeDurationCastOutcome, NativeDurationCastSource};

fn source_metadata(source: &FieldType) -> NativeDurationCastSource {
    NativeDurationCastSource {
        code: source.code().as_shared_type_name_code(),
        flags: source.raw_flags(),
        decimal: source.decimal(),
    }
}

fn finish(ctx: &dyn Columns, outcome: NativeDurationCastOutcome) -> Result<Datum, EvalError> {
    if let Some(message) = outcome.truncation {
        ctx.handle_truncate(&message)?;
    }
    Ok(outcome.value.map_or(Datum::Null, |parts| {
        Datum::new_duration(MySqlDuration::from_raw_parts(parts.nanoseconds, parts.fsp))
    }))
}

pub(crate) fn eval_cast_duration_in(
    ctx: &dyn Columns,
    value: &Datum,
    source: Option<&FieldType>,
    fsp: i64,
) -> Result<Datum, EvalError> {
    let outcome = tidb_query_expr::native_cast_duration(
        value.as_shared_json_input(),
        source.map(source_metadata),
        fsp,
        || ctx.time_zone(),
    )
    .map_err(EvalError::Unsupported)?;
    finish(ctx, outcome)
}

pub(crate) fn eval_cast_arg_as_duration_in(
    ctx: &dyn Columns,
    value: &Datum,
    source: Option<&FieldType>,
) -> Result<Datum, EvalError> {
    let outcome = tidb_query_expr::native_cast_arg_as_duration(
        value.as_shared_json_input(),
        source.map(source_metadata),
        || ctx.time_zone(),
    )
    .map_err(EvalError::Unsupported)?;
    finish(ctx, outcome)
}

pub(crate) fn eval_parse_computed_duration_in(
    ctx: &dyn Columns,
    value: &Datum,
) -> Result<Datum, EvalError> {
    let outcome =
        tidb_query_expr::native_parse_computed_duration(value.as_shared_json_input(), || {
            ctx.time_zone()
        })
        .map_err(EvalError::Unsupported)?;
    finish(ctx, outcome)
}
