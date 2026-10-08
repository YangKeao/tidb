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

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use tidb_datatype::{CoreTime, TimeType};

    use super::*;
    use crate::{ExpressionAdapterFailureClass, ReadyValuePoolOwner, ReadyValuePoolPolicy};

    #[test]
    fn legacy_from_unixtime_keeps_hidden_microseconds_and_selected_format_scope() {
        struct OtherZoneColumns {
            forbid_reads: Cell<bool>,
        }
        impl Columns for OtherZoneColumns {
            fn get(&self, _: &[String]) -> Option<Datum> {
                panic!("ready legacy decimals must not fetch columns")
            }
            fn time_zone(&self) -> SessionTimeZone {
                assert!(
                    !self.forbid_reads.get(),
                    "the borrowed request zone must be used"
                );
                SessionTimeZone::utc()
            }
            fn now(&self) -> Option<(i64, u32, i32)> {
                panic!("FROM_UNIXTIME must not fetch a clock")
            }
            fn div_precision_increment(&self) -> u32 {
                panic!("FROM_UNIXTIME must not fetch decimal policy")
            }
            fn append_warning(&self, _: u16, _: &str) {
                panic!("the legacy adapter must not create warnings")
            }
        }
        let zone = SessionTimeZone::Named(chrono_tz::Asia::Shanghai);
        let columns = OtherZoneColumns {
            forbid_reads: Cell::new(false),
        };
        assert_ne!(columns.time_zone(), zone);
        columns.forbid_reads.set(true);
        // The old legacy branch passes its rounded nanosecond count through
        // the pinned x1000 unit step: this actual fraction retains one raw
        // microsecond even though the returned Time has FSP zero.
        let decimal = Decimal::from_literal("0.000000001");
        let expected = Time::from_raw_parts(
            CoreTime::from_date(1970, 1, 1, 8, 0, 0, 1),
            TimeType::DateTime,
            0,
        );
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
                for present in [false, true] {
                    let packed = Cell::new(false);
                    let result = crate::eval_from_unixtime_legacy_scoped_in(
                        present.then(|| decimal.clone()),
                        &zone,
                        bound,
                        |value, selected| {
                            packed.set(true);
                            assert!(std::ptr::eq(selected.ready_value_scope().unwrap(), &scope));
                            assert!(std::ptr::eq(
                                selected.ready_value_execution().unwrap(),
                                bound.ready_value_execution().unwrap()
                            ));
                            assert_eq!(value, present.then_some(expected));
                            crate::eval_legacy_date_format_in(
                                value.map(|time| (time.core_time(), Some("%Y-%m-%d %H:%i:%s.%f"))),
                                selected,
                            )
                        },
                    );
                    if slots == 0 {
                        assert!(!packed.get());
                        assert!(
                            matches!(result, Err(EvalError::ExpressionAdapterFailure(failure))
                            if failure.class() == ExpressionAdapterFailureClass::PoolResource)
                        );
                    } else {
                        assert!(packed.get());
                        assert_eq!(
                            result,
                            Ok(Ok(present.then(|| b"1970-01-01 08:00:00.000001".to_vec())))
                        );
                    }
                }
                if slots == 1 {
                    let marker =
                        EvalError::Unsupported("legacy caller keeps its own result domain");
                    let result: Result<Result<(), EvalError>, EvalError> =
                        crate::eval_from_unixtime_legacy_scoped_in(
                            Some(decimal.clone()),
                            &zone,
                            bound,
                            |time, _selected| {
                                assert_eq!(time, Some(expected));
                                Err(marker.clone())
                            },
                        );
                    assert_eq!(result, Ok(Err(marker)));
                }
            });
            drop(scope);
            execution.close();
        }
    }
}
