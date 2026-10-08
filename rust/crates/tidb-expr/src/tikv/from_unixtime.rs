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
    evaluate_prepared_args_scoped_in, identity_value, native_time_result_contract_error,
    EvaluatedArgs, EvaluatedBytesOp,
};
use crate::{Columns, Datum, EvalError};

/// Keeps the selected execution alive while the caller consumes the computed
/// legacy time, including a subsequent formatted stage with the same authority.
pub fn eval_from_unixtime_legacy_scoped_in<T>(
    value: Option<Decimal>,
    zone: &SessionTimeZone,
    ctx: &dyn Columns,
    pack: impl FnOnce(Option<Time>, &dyn Columns) -> T,
) -> Result<T, EvalError> {
    evaluate_prepared_args_scoped_in(
        ctx,
        || match value {
            None => Ok((
                EvaluatedBytesOp::FromUnixTimeNullNative,
                EvaluatedArgs::Bytes(None),
            )),
            Some(value) => {
                let value = identity_value::encode(&Datum::Decimal(value))?
                    .ok_or_else(native_time_result_contract_error)?;
                Ok((
                    EvaluatedBytesOp::FromUnixTimeLegacy,
                    EvaluatedArgs::TemporalValue {
                        value,
                        zone: zone.clone(),
                    },
                ))
            }
        },
        |computed, selected| {
            let value = match computed.into_identity_datum()? {
                Datum::Null => None,
                Datum::Time(value) => Some(value),
                _ => return Err(native_time_result_contract_error()),
            };
            Ok(pack(value, selected))
        },
    )
}
