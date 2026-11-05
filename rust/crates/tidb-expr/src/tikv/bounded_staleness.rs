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
use tidb_query_expr::NativeBoundedStalenessHeadResult;

pub(crate) fn eval_bounded_staleness_in(
    ctx: &dyn Columns,
    vals: &[Datum],
) -> Result<Datum, EvalError> {
    bounded_staleness_bounded(ctx, vals, super::control_value::default_budget())
}
fn shared_time(
    value: tidb_datatype::Time,
) -> tidb_query_datatype::codec::mysql::time::NativeTemporalValue {
    use tidb_query_datatype::codec::mysql::TimeType as Kind;
    tidb_query_datatype::codec::mysql::time::NativeTemporalValue {
        raw: value.core_time().raw(),
        kind: match value.kind() {
            tidb_datatype::TimeType::Date => Kind::Date,
            tidb_datatype::TimeType::DateTime => Kind::DateTime,
            tidb_datatype::TimeType::Timestamp => Kind::Timestamp,
        },
        fsp: value.fsp(),
    }
}
fn bounded_resource() -> EvalError {
    EvalError::ExpressionRuntimeFailure(crate::ExpressionRuntimeFailure::from_local_eval(
        tidb_query_expr::local::LocalError::ResourceLimit(
            "bounded-staleness actual warning/result allowance exceeded".into(),
        ),
        None,
    ))
}
pub(crate) fn bounded_staleness_bounded(
    ctx: &dyn Columns,
    vals: &[Datum],
    maximum: usize,
) -> Result<Datum, EvalError> {
    let [left, right] = vals else {
        return Err(EvalError::WrongParameterCount("tidb_bounded_staleness"));
    };
    let (Datum::Time(left), Datum::Time(right)) = (left, right) else {
        if vals.iter().any(Datum::is_null) {
            return Ok(Datum::Null);
        }
        return Err(EvalError::Unsupported(
            "TIDB_BOUNDED_STALENESS arguments reached the signature without ETDatetime casts",
        ));
    };
    let l = shared_time(*left);
    let r = shared_time(*right);
    let endpoint = match tidb_query_expr::native_bounded_staleness_head_values(l, r) {
        NativeBoundedStalenessHeadResult::InvalidLeft => Some(left),
        NativeBoundedStalenessHeadResult::InvalidRight => Some(right),
        NativeBoundedStalenessHeadResult::RangeNull => return Ok(Datum::Null),
        NativeBoundedStalenessHeadResult::NeedSafe => {
            if maximum < 2 * std::mem::size_of::<tidb_datatype::Time>() {
                return Err(bounded_resource());
            }
            let safe = ctx.bounded_staleness_safe_time().map(shared_time);
            let result = tidb_query_expr::native_bounded_staleness_finish_values(l, r, safe)
                .ok_or_else(super::native_time_result_contract_error)?;
            return Ok(Datum::Time(tidb_datatype::Time::from_raw_parts(
                tidb_datatype::CoreTime::from_raw(result.raw),
                tidb_datatype::TimeType::DateTime,
                3,
            )));
        }
    };
    if let Some(endpoint) = endpoint {
        // The calendar core has fixed-width fields. Reserve the diagnostic's
        // conservative display bound BEFORE formatting or statement callbacks.
        if maximum < 128 {
            return Err(bounded_resource());
        }
        let warning = format!("Incorrect datetime value: '{endpoint}'");
        if warning.capacity() > maximum {
            return Err(bounded_resource());
        }
        ctx.handle_truncate(&warning)?;
    }
    Ok(Datum::Null)
}

pub(crate) fn bounded_staleness_cast_arguments(
    ctx: &dyn Columns,
    values: &mut [Datum; 2],
    fields: &[tidb_datatype::FieldType; 2],
    maximum: usize,
) -> Result<(), EvalError> {
    for i in 0..2 {
        let value = &values[i];
        let work = tidb_query_expr::native_datetime_argument_cast_work(
            value.as_shared_json_input(),
            value.as_shared_numeric_input(),
            Some(fields[i].code().as_shared_type_name_code()),
        )
        .map_err(|error| match error {
            tidb_query_expr::NativeStringCoercionError::Unsupported(reason) => {
                EvalError::Unsupported(reason)
            }
            tidb_query_expr::NativeStringCoercionError::ResourceLimit => bounded_resource(),
        })?;
        if work > maximum {
            return Err(bounded_resource());
        }
        values[i] = crate::cast::cast_arg_as_datetime(value, Some(&fields[i]), ctx)?;
    }
    Ok(())
}
