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

use std::cell::Cell;

use tidb_query_expr::{
    decode_native_bounded_staleness_head, decode_native_identity, NativeBoundedStalenessHeadResult,
    NativeIdentityRef,
};

use super::adapter_failure::{ExpressionAdapterFailure, ScopeFailureKind};
use super::{
    evaluate_args_in, evaluate_prepared_args_scoped_in, identity_value, EvaluatedArgs,
    EvaluatedBytesOp, EvaluatedBytesResult,
};
use crate::{Columns, Datum, EvalError};

fn invalid_report() -> EvalError {
    EvalError::ExpressionAdapterFailure(ExpressionAdapterFailure::from_scope(
        ScopeFailureKind::Contract,
        "invalid bounded-staleness result report",
    ))
}

fn decode_head(report: Option<Vec<u8>>) -> Result<NativeBoundedStalenessHeadResult, EvalError> {
    let report = report.ok_or_else(invalid_report)?;
    decode_native_bounded_staleness_head(&report).ok_or_else(invalid_report)
}

fn project_finish(computed: EvaluatedBytesResult) -> Result<Datum, EvalError> {
    let frame = computed.into_bytes()?.ok_or_else(invalid_report)?;
    if !matches!(
        decode_native_identity(&frame).map_err(|_| invalid_report())?,
        NativeIdentityRef::Time {
            kind: 1,
            fsp: 3,
            ..
        }
    ) {
        return Err(invalid_report());
    }
    identity_value::decode(Some(frame))
}

pub(crate) fn eval_bounded_staleness_in(
    ctx: &dyn Columns,
    vals: &[Datum],
) -> Result<Datum, EvalError> {
    let null_path = Cell::new(false);
    evaluate_prepared_args_scoped_in(
        ctx,
        || {
            let [left, right] = vals else {
                return Err(EvalError::WrongParameterCount("tidb_bounded_staleness"));
            };
            if !matches!((left, right), (Datum::Time(_), Datum::Time(_))) {
                if vals.iter().any(Datum::is_null) {
                    null_path.set(true);
                    return Ok((
                        EvaluatedBytesOp::DateDiffNullNative,
                        EvaluatedArgs::NullWitness(None),
                    ));
                }
                return Err(EvalError::Unsupported(
                    "TIDB_BOUNDED_STALENESS arguments reached the signature without ETDatetime casts",
                ));
            }
            Ok((
                EvaluatedBytesOp::BoundedStalenessHeadNative,
                EvaluatedArgs::Bytes2(
                    identity_value::encode(left)?,
                    identity_value::encode(right)?,
                ),
            ))
        },
        |computed, selected| {
            if null_path.get() {
                return match computed.into_int_datum()? {
                    value @ Datum::Null => Ok(value),
                    _ => Err(invalid_report()),
                };
            }
            let head = decode_head(computed.into_bytes()?)?;
            let invalid_endpoint = match head {
                NativeBoundedStalenessHeadResult::InvalidLeft => Some(0),
                NativeBoundedStalenessHeadResult::InvalidRight => Some(1),
                NativeBoundedStalenessHeadResult::RangeNull => None,
                NativeBoundedStalenessHeadResult::NeedSafe => {
                    return evaluate_args_in(
                        EvaluatedBytesOp::BoundedStalenessFinishNative,
                        selected,
                        || {
                            // The outer guard is still active. Authority remains
                            // the original statement, not a worker/session default.
                            let safe = ctx.bounded_staleness_safe_time();
                            Ok(EvaluatedArgs::Bytes3([
                                identity_value::encode(&vals[0])?,
                                identity_value::encode(&vals[1])?,
                                safe.map(|value| identity_value::encode(&Datum::Time(value)))
                                    .transpose()?
                                    .flatten(),
                            ]))
                        },
                        project_finish,
                    );
                }
            };
            if let Some(index) = invalid_endpoint {
                let Datum::Time(value) = &vals[index] else {
                    return Err(invalid_report());
                };
                ctx.handle_truncate(&format!("Incorrect datetime value: '{value}'"))?;
            }
            // These three decoded SDK terminal reports carry the SQL NULL
            // answer; the frontend only projects it after any requested warning.
            Ok(Datum::Null)
        },
    )
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::panic::{catch_unwind, AssertUnwindSafe};

    use tidb_datatype::{CoreTime, Time, TimeType};

    use super::*;
    use crate::{AsciiPoolOwner, AsciiPoolPolicy, ErrorLevel, ExpressionAdapterFailureClass};

    #[test]
    fn bounded_staleness_bridge_keeps_raw_time_authority_and_guarded_safe_ts() {
        struct Statement {
            safe: Cell<Option<Time>>,
            safe_reads: Cell<usize>,
            level: Cell<ErrorLevel>,
            truncate_calls: Cell<usize>,
            warnings: RefCell<Vec<(u16, String)>>,
            panic_safe: Cell<bool>,
            panic_warning: Cell<bool>,
        }
        impl Statement {
            fn new(safe: Option<Time>) -> Self {
                Self {
                    safe: Cell::new(safe),
                    safe_reads: Cell::new(0),
                    level: Cell::new(ErrorLevel::Warn),
                    truncate_calls: Cell::new(0),
                    warnings: RefCell::new(Vec::new()),
                    panic_safe: Cell::new(false),
                    panic_warning: Cell::new(false),
                }
            }
        }
        impl Columns for Statement {
            fn get(&self, _: &[String]) -> Option<Datum> {
                None
            }
            fn bounded_staleness_safe_time(&self) -> Option<Time> {
                self.safe_reads.set(self.safe_reads.get() + 1);
                assert!(!self.panic_safe.get(), "SafeTS callback panic");
                self.safe.get()
            }
            fn truncate_level(&self) -> ErrorLevel {
                panic!("must retain the original handle_truncate override")
            }
            fn handle_truncate(&self, message: &str) -> Result<(), EvalError> {
                self.truncate_calls.set(self.truncate_calls.get() + 1);
                match self.level.get() {
                    ErrorLevel::Ignore => Ok(()),
                    ErrorLevel::Warn => {
                        self.append_warning(1292, message);
                        Ok(())
                    }
                    ErrorLevel::Error => Err(EvalError::TruncatedWrongValue(message.to_owned())),
                }
            }
            fn append_warning(&self, code: u16, message: &str) {
                self.warnings.borrow_mut().push((code, message.to_owned()));
                assert!(!self.panic_warning.get(), "warning callback panic");
            }
        }
        let left = Time::from_raw_parts(
            CoreTime::from_raw(CoreTime::from_date(2015, 9, 21, 9, 53, 4, 654_321).raw() | 7),
            TimeType::Timestamp,
            6,
        );
        let right = Time::from_raw_parts(
            CoreTime::from_date(2025, 1, 2, 10, 0, 0, 111_222),
            TimeType::Date,
            0,
        );
        let safe = Time::from_raw_parts(
            CoreTime::from_raw(CoreTime::from_date(2020, 6, 7, 8, 9, 10, 123_456).raw() | 11),
            TimeType::Timestamp,
            6,
        );
        let equal_left = Time::from_raw_parts(
            CoreTime::from_raw((left.core_time().raw() & !15) | 2),
            TimeType::Date,
            255,
        );
        let zero_left = Time::from_raw_parts(
            CoreTime::from_date(2015, 0, 21, 9, 53, 4, 0),
            TimeType::DateTime,
            0,
        );
        let zero_right = Time::from_raw_parts(
            CoreTime::from_date(2025, 1, 0, 10, 0, 0, 0),
            TimeType::DateTime,
            0,
        );
        let year_zero = Time::from_raw_parts(
            CoreTime::from_date(0, 1, 2, 0, 0, 0, 0),
            TimeType::DateTime,
            0,
        );
        let zero_safe = Time::from_raw_parts(CoreTime::from_raw(0), TimeType::DateTime, 0);
        let values = [Datum::Time(left), Datum::Time(right)];
        let projected = |value: Time| {
            Datum::Time(Time::from_raw_parts(
                value.core_time(),
                TimeType::DateTime,
                3,
            ))
        };
        let policy = |slots| {
            AsciiPoolPolicy::checked(slots, slots, 16 << 20, 1 << 20, 2 << 20, 64, 16, 1 << 16)
                .unwrap()
        };
        for slots in [0, 1] {
            let native = Statement::new(Some(safe));
            let owner = AsciiPoolOwner::new(policy(slots)).unwrap();
            let execution = owner.begin_execution().unwrap();
            let scope = execution.scope();
            scope.with_columns(&native, |bound| {
                for (input, expected) in [
                    (None, left),
                    (Some(zero_safe), left),
                    (Some(safe), safe),
                    (Some(equal_left), equal_left),
                    (Some(right), right),
                    (Some(Time::from_raw_parts(
                        CoreTime::from_date(2026, 1, 2, 0, 0, 0, 0),
                        TimeType::DateTime, 0,
                    )), right),
                ] {
                    native.safe.set(input);
                    native.safe_reads.set(0);
                    let result = eval_bounded_staleness_in(bound, &values);
                    if slots == 0 {
                        assert!(matches!(result,
                            Err(EvalError::ExpressionAdapterFailure(failure))
                                if failure.class() == ExpressionAdapterFailureClass::PoolResource));
                        assert_eq!(native.safe_reads.get(), 0);
                    } else {
                        let result = result.unwrap();
                        assert_eq!(identity_value::encode(&result).unwrap(),
                            identity_value::encode(&projected(expected)).unwrap());
                        assert_eq!(native.safe_reads.get(), 1);
                    }
                    assert_eq!(native.truncate_calls.get(), 0);
                }
                native.safe_reads.set(0);
                for (arguments, warning) in [
                    ([Datum::MaxValue, Datum::Null], None),
                    ([Datum::Null, Datum::MinNotNull], None),
                    ([Datum::Null, Datum::Time(zero_left)], None),
                    ([Datum::Time(right), Datum::Time(left)], None),
                    ([Datum::Time(zero_left), Datum::Time(zero_right)], Some(zero_left)),
                    ([Datum::Time(left), Datum::Time(zero_right)], Some(zero_right)),
                ] {
                    for level in [ErrorLevel::Warn, ErrorLevel::Error] {
                        native.level.set(level);
                        native.truncate_calls.set(0);
                        native.warnings.borrow_mut().clear();
                        let result = eval_bounded_staleness_in(bound, &arguments);
                        if slots == 0 {
                            assert!(matches!(result,
                                Err(EvalError::ExpressionAdapterFailure(failure))
                                    if failure.class() == ExpressionAdapterFailureClass::PoolResource));
                            assert_eq!(native.truncate_calls.get(), 0);
                            assert!(native.warnings.borrow().is_empty());
                        } else if let Some(value) = warning {
                            let message = format!("Incorrect datetime value: '{value}'");
                            assert_eq!(native.truncate_calls.get(), 1);
                            if matches!(level, ErrorLevel::Error) {
                                assert_eq!(result, Err(EvalError::TruncatedWrongValue(message)));
                                assert!(native.warnings.borrow().is_empty());
                            } else {
                                assert_eq!(result, Ok(Datum::Null));
                                assert_eq!(*native.warnings.borrow(), [(1292, message)]);
                            }
                        } else {
                            assert_eq!(result, Ok(Datum::Null));
                            assert_eq!(native.truncate_calls.get(), 0);
                        }
                        assert_eq!(native.safe_reads.get(), 0);
                    }
                }
                assert_eq!(eval_bounded_staleness_in(bound, &[]),
                    Err(EvalError::WrongParameterCount("tidb_bounded_staleness")));
                assert_eq!(eval_bounded_staleness_in(bound, &[Datum::MaxValue, Datum::Int(1)]),
                    Err(EvalError::Unsupported(
                        "TIDB_BOUNDED_STALENESS arguments reached the signature without ETDatetime casts")));
            });
            drop(scope);
            execution.close();
        }
        for report in [None, Some(vec![]), Some(vec![4]), Some(vec![0, 0])] {
            assert!(matches!(decode_head(report),
                Err(EvalError::ExpressionAdapterFailure(failure))
                    if failure.class() == ExpressionAdapterFailureClass::ScopeContract));
        }
        for report in [
            None,
            Some(Vec::new()),
            identity_value::encode(&Datum::Int(1)).unwrap(),
            identity_value::encode(&Datum::Time(safe)).unwrap(),
        ] {
            assert!(
                matches!(project_finish(EvaluatedBytesResult::Bytes(report)),
                Err(EvalError::ExpressionAdapterFailure(failure))
                    if failure.class() == ExpressionAdapterFailureClass::ScopeContract)
            );
        }
        for warning_panic in [false, true] {
            let native = Statement::new(Some(safe));
            native.panic_safe.set(!warning_panic);
            native.panic_warning.set(warning_panic);
            let owner = AsciiPoolOwner::new(policy(1)).unwrap();
            let execution = owner.begin_execution().unwrap();
            let scope = execution.scope();
            scope.with_columns(&native, |bound| {
                let arguments = if warning_panic {
                    [Datum::Time(zero_left), Datum::Time(right)]
                } else {
                    values.clone()
                };
                assert!(catch_unwind(AssertUnwindSafe(|| {
                    eval_bounded_staleness_in(bound, &arguments)
                }))
                .is_err());
                native.panic_safe.set(false);
                native.panic_warning.set(false);
                assert!(matches!(eval_bounded_staleness_in(bound, &values),
                    Err(EvalError::ExpressionAdapterFailure(failure))
                        if failure.class() == ExpressionAdapterFailureClass::ScopePoisoned));
                assert_eq!(native.safe_reads.get(), usize::from(!warning_panic));
            });
            drop(scope);
            execution.close();
        }
        let default = eval_bounded_staleness_in(&crate::NoColumns, &values).unwrap();
        assert_eq!(
            identity_value::encode(&default).unwrap(),
            identity_value::encode(&projected(left)).unwrap()
        );
        let native = Statement::new(Some(zero_safe));
        assert_eq!(
            eval_bounded_staleness_in(&native, &[Datum::Time(year_zero), Datum::Time(right)]),
            Ok(projected(year_zero))
        );
        assert_eq!(native.safe_reads.get(), 1);
        assert_eq!(native.truncate_calls.get(), 0);
    }
}
