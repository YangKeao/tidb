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

use crate::{Columns, Datum, EvalError};
/// Original integer projection from actual typed Time and explicit zone.
pub fn unix_timestamp_int_legacy_in(
    value: Option<Time>,
    zone: &SessionTimeZone,
    _cols: &dyn Columns,
) -> Result<Option<i128>, EvalError> {
    match super::epoch_value::unix_legacy_values_bounded(
        value,
        zone,
        false,
        super::identity_actual::default_budget(),
    )? {
        Datum::Null => Ok(None),
        Datum::Int(n) => Ok(Some(i128::from(n))),
        _ => Err(super::native_time_result_contract_error()),
    }
}
/// Original strict legacy Decimal projection, retaining its exact returned scale.
pub fn unix_timestamp_dec_legacy_in(
    value: Option<Time>,
    zone: &SessionTimeZone,
    _cols: &dyn Columns,
) -> Result<Option<Decimal>, EvalError> {
    match super::epoch_value::unix_legacy_values_bounded(
        value,
        zone,
        true,
        super::identity_actual::default_budget(),
    )? {
        Datum::Null => Ok(None),
        Datum::Decimal(n) => Ok(Some(n)),
        _ => Err(super::native_time_result_contract_error()),
    }
}
