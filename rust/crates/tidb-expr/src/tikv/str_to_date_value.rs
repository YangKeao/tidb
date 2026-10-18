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

//! Original result/warning projection of actual SDK date states.
use crate::{Columns, Datum, EvalError};
use tidb_datatype::FieldTypeCode;
use tidb_query_expr::{
    NativeStrToDatePrepared as Prepared, NativeStrToDateStep as Step,
    NativeStrToDateValue as Value, NativeStrToDateValueError as Error,
};
fn value_error(error: Error) -> EvalError {
    match error {
        Error::Unsupported(message) => EvalError::Unsupported(message),
        Error::ResourceLimit(reason) => {
            EvalError::ExpressionRuntimeFailure(crate::ExpressionRuntimeFailure::from_local_eval(
                tidb_query_expr::local::LocalError::ResourceLimit(reason.into()),
                None,
            ))
        }
    }
}
pub(crate) fn prepare(
    input: &Datum,
    format: &Datum,
    maximum: usize,
) -> Result<Option<Prepared>, EvalError> {
    tidb_query_expr::native_prepare_str_to_date(
        input.as_shared_json_input(),
        format.as_shared_json_input(),
        maximum,
    )
    .map_err(value_error)
}
pub(crate) fn compute(
    ctx: &dyn Columns,
    prepared: Option<&Prepared>,
    target: Option<FieldTypeCode>,
    maximum: usize,
) -> Result<Datum, EvalError> {
    let value = if let Some(prepared) = prepared {
        match prepared
            .begin(target.map(FieldTypeCode::as_shared_type_name_code), maximum)
            .map_err(value_error)?
        {
            Step::Ready(value) => value,
            Step::Date(date) => {
                let modes = ctx.date_modes();
                date.finish(modes.no_zero_date, modes.allow_invalid_dates)
                    .map_err(value_error)?
            }
            Step::TypedClock(clock) => {
                let modes = ctx.date_modes();
                clock.finish(modes.no_zero_date).map_err(value_error)?
            }
        }
    } else {
        tidb_query_expr::native_str_to_date_null_value()
    };
    match value {
        Value::Null => Ok(Datum::Null),
        Value::Text(text) => {
            let held = match prepared {
                Some(prepared) => prepared.retained_bytes().ok_or_else(|| {
                    value_error(Error::ResourceLimit("STR_TO_DATE source capacity overflow"))
                })?,
                None => 0,
            };
            let live = held
                .checked_add(text.capacity())
                .and_then(|n| n.checked_add(text.len()))
                .ok_or_else(|| {
                    value_error(Error::ResourceLimit(
                        "STR_TO_DATE text publication extent overflow",
                    ))
                })?;
            if maximum == usize::MAX || live > maximum {
                return Err(value_error(Error::ResourceLimit(
                    "STR_TO_DATE text publication overlap exceeds allowance",
                )));
            }
            Ok(Datum::new_string(text))
        }
        Value::Warning { code, message } => {
            ctx.append_warning(code, &message);
            Ok(Datum::Null)
        }
    }
}
pub(crate) fn bounded(
    ctx: &dyn Columns,
    values: &[Datum],
    target: Option<FieldTypeCode>,
    maximum: usize,
) -> Result<Datum, EvalError> {
    let [input, format] = values else {
        return Err(EvalError::Unsupported("bad function arity"));
    };
    let prepared = prepare(input, format, maximum)?;
    compute(ctx, prepared.as_ref(), target, maximum)
}
