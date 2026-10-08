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

use tidb_datatype::{Decimal, SessionTimeZone, Time};

use super::{
    evaluate_prepared_args_in, identity_value, native_time_result_contract_error, EvaluatedArgs,
    EvaluatedBytesOp,
};
use crate::{Columns, Datum, EvalError};

fn prepare_legacy_value(
    value: Option<Time>,
    zone: &SessionTimeZone,
    operation: EvaluatedBytesOp,
) -> Result<(EvaluatedBytesOp, EvaluatedArgs), EvalError> {
    let Some(value) = value else {
        return Ok((
            EvaluatedBytesOp::UnixTimestampNullNative,
            EvaluatedArgs::Bytes(None),
        ));
    };
    let value = identity_value::encode(&Datum::Time(value))?
        .ok_or_else(native_time_result_contract_error)?;
    Ok((
        operation,
        EvaluatedArgs::TemporalValue {
            value,
            zone: zone.clone(),
        },
    ))
}

/// Evaluates the legacy integer signature using the actual typed time and zone.
pub fn unix_timestamp_int_legacy_in(
    value: Option<Time>,
    zone: &SessionTimeZone,
    cols: &dyn Columns,
) -> Result<Option<i128>, EvalError> {
    evaluate_prepared_args_in(
        cols,
        || prepare_legacy_value(value, zone, EvaluatedBytesOp::UnixTimestampIntLegacy),
        |computed| match computed.into_identity_datum()? {
            Datum::Null => Ok(None),
            Datum::Int(value) => Ok(Some(i128::from(value))),
            _ => Err(native_time_result_contract_error()),
        },
    )
}

/// Evaluates the legacy decimal signature without changing the returned scale.
pub fn unix_timestamp_dec_legacy_in(
    value: Option<Time>,
    zone: &SessionTimeZone,
    cols: &dyn Columns,
) -> Result<Option<Decimal>, EvalError> {
    evaluate_prepared_args_in(
        cols,
        || prepare_legacy_value(value, zone, EvaluatedBytesOp::UnixTimestampDecLegacy),
        |computed| match computed.into_identity_datum()? {
            Datum::Null => Ok(None),
            Datum::Decimal(value) => Ok(Some(value)),
            _ => Err(native_time_result_contract_error()),
        },
    )
}
