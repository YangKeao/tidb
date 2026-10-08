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

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use tidb_datatype::{CoreTime, TimeType};

    use super::*;
    use crate::{ExpressionAdapterFailureClass, ReadyValuePoolOwner, ReadyValuePoolPolicy};

    #[test]
    fn legacy_unix_timestamp_keeps_explicit_zone_raw_time_and_nullable_admission() {
        struct OtherZoneColumns {
            zone: SessionTimeZone,
            forbid_reads: Cell<bool>,
        }
        impl Columns for OtherZoneColumns {
            fn get(&self, _: &[String]) -> Option<Datum> {
                panic!("typed legacy adapters must not fetch columns")
            }
            fn time_zone(&self) -> SessionTimeZone {
                assert!(
                    !self.forbid_reads.get(),
                    "the explicit borrowed zone is authoritative"
                );
                self.zone.clone()
            }
            fn now(&self) -> Option<(i64, u32, i32)> {
                panic!("typed legacy adapters must not fetch a clock")
            }
            fn div_precision_increment(&self) -> u32 {
                panic!("typed legacy adapters must not fetch decimal policy")
            }
            fn append_warning(&self, _: u16, _: &str) {
                panic!("legacy zero outcomes must not append warnings")
            }
        }
        let utc = SessionTimeZone::utc();
        let la = SessionTimeZone::Named(chrono_tz::America::Los_Angeles);
        let columns = OtherZoneColumns {
            zone: SessionTimeZone::Named(chrono_tz::Asia::Shanghai),
            forbid_reads: Cell::new(false),
        };
        assert_ne!(columns.time_zone(), utc);
        assert_ne!(columns.time_zone(), la);
        columns.forbid_reads.set(true);
        let valid =
            Time::from_date_checked(1970, 1, 1, 0, 0, 1, 123456, TimeType::DateTime, 6).unwrap();
        let gap = Time::from_date_checked(2021, 3, 14, 2, 30, 0, 0, TimeType::DateTime, 6).unwrap();
        let zero = Time::from_raw_parts(CoreTime::from_raw(0), TimeType::DateTime, 255);

        for slots in [0, 1] {
            let policy = ReadyValuePoolPolicy::checked(
                slots,
                slots,
                16 << 20,
                1 << 20,
                2 << 20,
                64,
                16,
                1 << 16,
            )
            .unwrap();
            let owner = ReadyValuePoolOwner::new(policy).unwrap();
            let execution = owner.begin_execution().unwrap();
            let scope = execution.scope();
            scope.with_columns(&columns, |bound| {
                if slots == 0 {
                    for value in [None, Some(valid), Some(zero), Some(gap)] {
                        assert!(
                            matches!(crate::unix_timestamp_int_legacy_in(value, &la, bound),
                            Err(EvalError::ExpressionAdapterFailure(failure))
                                if failure.class() == ExpressionAdapterFailureClass::PoolResource)
                        );
                        assert!(
                            matches!(crate::unix_timestamp_dec_legacy_in(value, &la, bound),
                            Err(EvalError::ExpressionAdapterFailure(failure))
                                if failure.class() == ExpressionAdapterFailureClass::PoolResource)
                        );
                    }
                    return;
                }
                assert_eq!(
                    crate::unix_timestamp_int_legacy_in(None, &la, bound),
                    Ok(None)
                );
                assert_eq!(
                    crate::unix_timestamp_dec_legacy_in(None, &la, bound),
                    Ok(None)
                );
                for kind in [TimeType::Date, TimeType::DateTime, TimeType::Timestamp] {
                    for fsp in [0, 6, 255] {
                        let value = Time::from_raw_parts(valid.core_time(), kind, fsp);
                        for (zone, seconds, decimal) in [
                            (&utc, 1_i128, "1.123456"),
                            (&la, 28_801_i128, "28801.123456"),
                        ] {
                            assert_eq!(
                                crate::unix_timestamp_int_legacy_in(Some(value), zone, bound),
                                Ok(Some(seconds))
                            );
                            let actual =
                                crate::unix_timestamp_dec_legacy_in(Some(value), zone, bound)
                                    .unwrap()
                                    .unwrap();
                            assert_eq!(actual.to_string(), decimal);
                            assert_eq!(actual.scale(), 6);
                        }
                    }
                }
                for value in [gap, zero] {
                    assert_eq!(
                        crate::unix_timestamp_int_legacy_in(Some(value), &la, bound),
                        Ok(Some(0))
                    );
                    let actual = crate::unix_timestamp_dec_legacy_in(Some(value), &la, bound)
                        .unwrap()
                        .unwrap();
                    assert_eq!(actual.to_string(), "0");
                    assert_eq!(actual.scale(), 0);
                }
            });
            drop(scope);
            execution.close();
        }
    }
}
