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
use tidb_datatype::{CoreTime, FieldType, Time, TimeType};
use tidb_query_datatype::codec::mysql::time::NativeTemporalValue;
use tidb_query_expr::NativeTimeCastModes;

fn modes(ctx: &dyn Columns) -> NativeTimeCastModes {
    let modes = ctx.date_modes();
    NativeTimeCastModes {
        no_zero_date: modes.no_zero_date,
        no_zero_in_date: modes.no_zero_in_date,
        allow_invalid_dates: modes.allow_invalid_dates,
    }
}

fn restore(value: NativeTemporalValue) -> Time {
    Time::from_raw_parts(CoreTime::from_raw(value.raw), value.kind, value.fsp)
}

pub(crate) fn eval_cast_time_value_in(
    ctx: &dyn Columns,
    value: &Datum,
    source: Option<&FieldType>,
    kind: TimeType,
    fsp: Option<i64>,
) -> Result<Option<Time>, EvalError> {
    tidb_query_expr::native_cast_time(
        value.as_shared_json_input(),
        value.as_shared_numeric_input(),
        source.map(|field| field.code().as_shared_type_name_code()),
        kind,
        fsp,
        || modes(ctx),
        || ctx.now(),
        || ctx.time_zone(),
        |code, message| ctx.append_warning(code, message),
    )
    .map(|value| value.map(restore))
    .map_err(EvalError::Unsupported)
}

pub(crate) fn eval_cast_arg_as_datetime_in(
    ctx: &dyn Columns,
    value: &Datum,
    source: Option<&FieldType>,
) -> Result<Datum, EvalError> {
    tidb_query_expr::native_cast_arg_as_datetime(
        value.as_shared_json_input(),
        value.as_shared_numeric_input(),
        source.map(|field| field.code().as_shared_type_name_code()),
        || modes(ctx),
        || ctx.now(),
        || ctx.time_zone(),
        |code, message| ctx.append_warning(code, message),
    )
    .map(|value| value.map_or(Datum::Null, |time| Datum::Time(restore(time))))
    .map_err(EvalError::Unsupported)
}
