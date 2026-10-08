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

use tidb_datatype::{Decimal, MySqlDuration, SessionTimeZone, Time};

use super::adapter_failure::{ExpressionAdapterFailure, ScopeFailureKind};
use super::{
    evaluate_args_in, evaluate_prepared_args_scoped_in, identity_value, EvaluatedArgs,
    EvaluatedBytesOp as Op, EvaluatedBytesResult, ExpressionRuntimeFailure, LocalError,
};
use crate::{Columns, Datum, EvalError};

pub use tidb_query_expr::{
    NativeLegacyDateArithmeticChannel as LegacyDateArithmeticChannel,
    NativeLegacyDateArithmeticDateKind as LegacyDateArithmeticDateKind,
    NativeLegacyDateArithmeticIntervalKind as LegacyDateArithmeticIntervalKind,
    NativeLegacyDateArithmeticMetadata as LegacyDateArithmeticMetadata,
};

#[derive(Clone, Copy)]
enum Entry {
    Text,
    Time,
    Duration,
}

enum Output {
    Null,
    Text(Vec<u8>),
    Time(Time),
    Duration(MySqlDuration),
}

#[derive(Clone, Copy)]
enum Stage {
    Head,
    Step,
    Parse,
}

fn child_bytes(
    channel: LegacyDateArithmeticChannel,
    value: LegacyDateArithmeticValue,
) -> Result<Option<Vec<u8>>, EvalError> {
    use LegacyDateArithmeticChannel as C;
    use LegacyDateArithmeticValue as V;
    match (channel, value) {
        (C::Bytes, V::Bytes(value)) => Ok(value),
        (C::FoldedInt, V::Int(value)) => Ok(value.map(|value| value.to_le_bytes().to_vec())),
        (C::Real, V::Real(value)) => Ok(value.map(|value| value.to_le_bytes().to_vec())),
        (C::Decimal, V::Decimal(value)) => value.map_or(Ok(None), |value| {
            identity_value::encode(&Datum::Decimal(value))
        }),
        (C::Time, V::Time(value)) => value.map_or(Ok(None), |value| {
            identity_value::encode(&Datum::Time(value))
        }),
        (C::Duration, V::Duration(value)) => value.map_or(Ok(None), |value| {
            identity_value::encode(&Datum::new_duration(value))
        }),
        _ => Err(invalid_report()),
    }
}

fn drive<E: From<EvalError>>(
    computed: EvaluatedBytesResult,
    selected: &dyn Columns,
    entry: Entry,
    zone: &SessionTimeZone,
    eval: &mut impl FnMut(
        usize,
        LegacyDateArithmeticChannel,
        &dyn Columns,
    ) -> Result<LegacyDateArithmeticValue, E>,
) -> Result<(Output, i128), E> {
    use tidb_query_expr::NativeLegacyDateArithmeticOutcome as O;
    use LegacyDateArithmeticChannel as C;
    let mut state = computed
        .into_bytes()
        .map_err(E::from)?
        .ok_or_else(|| E::from(invalid_report()))?;
    let mut stage = Stage::Head;
    loop {
        let report = tidb_query_expr::decode_native_legacy_date_arithmetic_result(&state)
            .ok_or_else(|| E::from(invalid_report()))?;
        let allowed = match stage {
            Stage::Head => match (entry, &report.outcome) {
                (
                    Entry::Text,
                    O::Request {
                        index: 2,
                        channel: C::Bytes,
                        ..
                    },
                )
                | (
                    Entry::Time,
                    O::Request {
                        index: 0,
                        channel: C::Time,
                        ..
                    },
                )
                | (
                    Entry::Duration,
                    O::Request {
                        index: 0,
                        channel: C::Duration,
                        ..
                    },
                ) => true,
                _ => false,
            },
            Stage::Parse => matches!(&report.outcome, O::Null | O::Request { index: 1, .. }),
            Stage::Step => true,
        };
        if !allowed {
            return Err(E::from(invalid_report()));
        }
        let (operation, next_stage, actual) = match (report.outcome, report.presence) {
            (O::Null, Some(presence @ 0)) => return Ok((Output::Null, presence)),
            (O::Text(value), Some(presence @ 1)) if matches!(entry, Entry::Text) => {
                return Ok((Output::Text(value.to_vec()), presence));
            }
            (O::Time(value), Some(presence @ 1)) if matches!(entry, Entry::Time) => {
                let value = identity_value::decode(Some(value.to_vec())).map_err(E::from)?;
                let Datum::Time(value) = value else {
                    return Err(E::from(invalid_report()));
                };
                return Ok((Output::Time(value), presence));
            }
            (O::Duration(value), Some(presence @ 1)) if matches!(entry, Entry::Duration) => {
                let value = identity_value::decode(Some(value.to_vec())).map_err(E::from)?;
                let Datum::Duration(value) = value else {
                    return Err(E::from(invalid_report()));
                };
                return Ok((Output::Duration(value), presence));
            }
            (O::Request { index, channel, .. }, None) => {
                if index > 2 || (index == 2 && !matches!(channel, C::Bytes)) {
                    return Err(E::from(invalid_report()));
                }
                // The head's pack guard covers this original-E callback. The
                // child receives the same authority, with all other Columns
                // observations still forwarded to the original caller.
                let value = eval(index, channel, selected)?;
                let actual = child_bytes(channel, value).map_err(E::from)?;
                (Op::LegacyDateArithmeticStepNative, Stage::Step, actual)
            }
            (O::Parse { .. }, None) if matches!(entry, Entry::Text) => {
                state = evaluate_args_in(
                    Op::LegacyDateArithmeticParseNative,
                    selected,
                    || {
                        Ok(EvaluatedArgs::TemporalText {
                            value: state,
                            modes: 0,
                            // This is the legacy evaluator's actual zone, not a
                            // new statement getter or an EvalContext default.
                            zone: zone.clone(),
                        })
                    },
                    |computed| computed.into_bytes()?.ok_or_else(invalid_report),
                )
                .map_err(E::from)?;
                stage = Stage::Parse;
                continue;
            }
            (
                O::DecimalText {
                    round_to_zero,
                    text,
                    ..
                },
                None,
            ) => {
                let (mut decimal, _) = tidb_datatype::MyDecimal::from_string(text.as_ref());
                if round_to_zero {
                    decimal.round_in_place(0, tidb_datatype::RoundMode::HalfUp);
                }
                (
                    Op::LegacyDateArithmeticStepNative,
                    Stage::Step,
                    Some(decimal.to_string_bytes()),
                )
            }
            _ => return Err(E::from(invalid_report())),
        };
        state = evaluate_args_in(
            operation,
            selected,
            || Ok(EvaluatedArgs::Bytes2(Some(state), actual)),
            |computed| computed.into_bytes()?.ok_or_else(invalid_report),
        )
        .map_err(E::from)?;
        stage = next_stage;
    }
}

fn evaluate<E: From<EvalError>>(
    ctx: &dyn Columns,
    entry: Entry,
    metadata: LegacyDateArithmeticMetadata,
    zone: &SessionTimeZone,
    mut eval: impl FnMut(
        usize,
        LegacyDateArithmeticChannel,
        &dyn Columns,
    ) -> Result<LegacyDateArithmeticValue, E>,
) -> Result<(Output, i128), E> {
    evaluate_prepared_args_scoped_in(
        ctx,
        || {
            let operation = match entry {
                Entry::Text => Op::LegacyDateArithmeticTextHeadNative,
                Entry::Time => Op::LegacyDateArithmeticTimeHeadNative,
                Entry::Duration => Op::LegacyDateArithmeticDurationHeadNative,
            };
            let metadata = tidb_query_expr::encode_native_legacy_date_arithmetic_metadata(metadata)
                .map_err(frame_error)?;
            Ok((operation, EvaluatedArgs::Bytes(Some(metadata))))
        },
        |computed, selected| {
            // The selected owner and pack guard remain live after the head worker
            // is released. An original callback E remains an E, never a sentinel
            // EvalError; its unwind is still covered by that outer guard.
            Ok(drive(computed, selected, entry, zone, &mut eval))
        },
    )
    .map_err(E::from)?
}

/// Runs the legacy text-returning signature and its SDK presence projection.
pub fn eval_legacy_date_arithmetic_text_in<E: From<EvalError>>(
    ctx: &dyn Columns,
    metadata: LegacyDateArithmeticMetadata,
    zone: &SessionTimeZone,
    eval: impl FnMut(
        usize,
        LegacyDateArithmeticChannel,
        &dyn Columns,
    ) -> Result<LegacyDateArithmeticValue, E>,
) -> Result<LegacyDateArithmeticResult<Vec<u8>>, E> {
    let (value, presence) = evaluate(ctx, Entry::Text, metadata, zone, eval)?;
    let value = match value {
        Output::Null => None,
        Output::Text(value) => Some(value),
        _ => return Err(E::from(invalid_report())),
    };
    Ok(LegacyDateArithmeticResult { value, presence })
}

/// Runs the legacy typed-time signature and its SDK presence projection.
pub fn eval_legacy_date_arithmetic_time_in<E: From<EvalError>>(
    ctx: &dyn Columns,
    metadata: LegacyDateArithmeticMetadata,
    zone: &SessionTimeZone,
    eval: impl FnMut(
        usize,
        LegacyDateArithmeticChannel,
        &dyn Columns,
    ) -> Result<LegacyDateArithmeticValue, E>,
) -> Result<LegacyDateArithmeticResult<Time>, E> {
    let (value, presence) = evaluate(ctx, Entry::Time, metadata, zone, eval)?;
    let value = match value {
        Output::Null => None,
        Output::Time(value) => Some(value),
        _ => return Err(E::from(invalid_report())),
    };
    Ok(LegacyDateArithmeticResult { value, presence })
}

/// Runs the legacy typed-duration signature and its SDK presence projection.
pub fn eval_legacy_date_arithmetic_duration_in<E: From<EvalError>>(
    ctx: &dyn Columns,
    metadata: LegacyDateArithmeticMetadata,
    zone: &SessionTimeZone,
    eval: impl FnMut(
        usize,
        LegacyDateArithmeticChannel,
        &dyn Columns,
    ) -> Result<LegacyDateArithmeticValue, E>,
) -> Result<LegacyDateArithmeticResult<MySqlDuration>, E> {
    let (value, presence) = evaluate(ctx, Entry::Duration, metadata, zone, eval)?;
    let value = match value {
        Output::Null => None,
        Output::Duration(value) => Some(value),
        _ => return Err(E::from(invalid_report())),
    };
    Ok(LegacyDateArithmeticResult { value, presence })
}

/// One actual nullable result of the legacy evaluator's requested child channel.
/// In particular, the integer channel is the original full-width folded i128.
pub enum LegacyDateArithmeticValue {
    Bytes(Option<Vec<u8>>),
    Int(Option<i128>),
    Real(Option<f64>),
    Decimal(Option<Decimal>),
    Time(Option<Time>),
    Duration(Option<MySqlDuration>),
}

/// The SDK's value and separate presence projection, from one evaluation.
/// Legacy bare conditions use `presence`, not a host-derived value predicate.
#[derive(Debug, PartialEq)]
pub struct LegacyDateArithmeticResult<T> {
    pub value: Option<T>,
    pub presence: i128,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ExpressionAdapterFailureClass, ReadyValuePoolOwner, ReadyValuePoolPolicy};
    use std::panic::{catch_unwind, AssertUnwindSafe};
    use tidb_datatype::{CoreTime, DateModes, TimeType};

    #[derive(Debug, PartialEq)]
    enum ChildError {
        Native(EvalError),
        Original(&'static str),
    }
    impl From<EvalError> for ChildError {
        fn from(value: EvalError) -> Self {
            Self::Native(value)
        }
    }
    fn metadata(date: LegacyDateArithmeticDateKind) -> LegacyDateArithmeticMetadata {
        LegacyDateArithmeticMetadata {
            date,
            interval: LegacyDateArithmeticIntervalKind::Int,
            subtract: false,
        }
    }

    #[test]
    fn legacy_date_arithmetic_bridge_keeps_channels_generic_errors_zone_demand_and_selected_scope()
    {
        use LegacyDateArithmeticChannel as C;
        use LegacyDateArithmeticDateKind as D;
        use LegacyDateArithmeticValue as V;
        struct Original;
        impl Columns for Original {
            fn get(&self, _: &[String]) -> Option<Datum> {
                panic!("original channel owns child reads")
            }
            fn date_modes(&self) -> DateModes {
                panic!("legacy parser uses fixed false modes")
            }
            fn time_zone(&self) -> SessionTimeZone {
                panic!("legacy zone is the explicit argument")
            }
            fn now(&self) -> Option<(i64, u32, i32)> {
                panic!("unsupported clock-anchored signatures stay outside")
            }
        }
        let owner = |slots| {
            ReadyValuePoolOwner::new(
                ReadyValuePoolPolicy::checked(
                    slots,
                    slots,
                    16 << 20,
                    1 << 20,
                    2 << 20,
                    64,
                    16,
                    1 << 16,
                )
                .unwrap(),
            )
            .unwrap()
        };
        let time = Time::from_raw_parts(
            CoreTime::from_date(2024, 1, 1, 0, 0, 0, 0),
            TimeType::DateTime,
            2,
        );
        let next_time = Time::from_raw_parts(
            CoreTime::from_date(2024, 1, 2, 0, 0, 0, 0),
            TimeType::DateTime,
            2,
        );
        let duration = MySqlDuration::from_raw_parts(1_000_000_000, 2);
        let zone = SessionTimeZone::utc();
        let large_zone = SessionTimeZone::Fixed {
            name: "z".repeat(128 << 10),
            offset_secs: 0,
        };
        assert_eq!(
            child_bytes(C::FoldedInt, V::Int(Some(i128::MAX))).unwrap(),
            Some(i128::MAX.to_le_bytes().to_vec())
        );
        for slots in [0, 1] {
            let owner = owner(slots);
            let execution = owner.begin_execution().unwrap();
            let scope = execution.scope();
            scope.with_columns(&Original, |bound| {
                for entry in [Entry::Text, Entry::Time, Entry::Duration] {
                    let mut visited = Vec::new();
                    let mut eval = |index, channel, selected: &dyn Columns| -> Result<V, ChildError> {
                        assert!(std::ptr::eq(selected.ready_value_scope().unwrap(), &scope));
                        visited.push((index, channel));
                        Ok(match (index, channel) {
                            (2, C::Bytes) => V::Bytes(Some(if matches!(entry, Entry::Duration) { b"SECOND".to_vec() } else { b"DAY".to_vec() })),
                            (0, C::Bytes) => V::Bytes(Some(b"2024-01-01".to_vec())),
                            (0, C::Time) => V::Time(Some(time)),
                            (0, C::Duration) => V::Duration(Some(duration)),
                            (1, C::FoldedInt) => V::Int(Some(1)),
                            _ => panic!("unexpected legacy child demand"),
                        })
                    };
                    let actual = match entry {
                        Entry::Text => eval_legacy_date_arithmetic_text_in(bound, metadata(D::String), &zone, &mut eval)
                            .map(|result| (result.value.map(Datum::new_bytes), result.presence)),
                        Entry::Time => eval_legacy_date_arithmetic_time_in(bound, metadata(D::Datetime), &large_zone, &mut eval)
                            .map(|result| (result.value.map(Datum::Time), result.presence)),
                        Entry::Duration => eval_legacy_date_arithmetic_duration_in(bound, metadata(D::Duration), &large_zone, &mut eval)
                            .map(|result| (result.value.map(Datum::new_duration), result.presence)),
                    };
                    if slots == 0 {
                        assert!(matches!(actual, Err(ChildError::Native(EvalError::ExpressionAdapterFailure(failure)))
                            if failure.class() == ExpressionAdapterFailureClass::PoolResource));
                        assert!(visited.is_empty());
                    } else {
                        let expected = match entry {
                            Entry::Text => Datum::new_bytes(b"2024-01-02".to_vec()),
                            Entry::Time => Datum::Time(next_time),
                            Entry::Duration => Datum::new_duration(MySqlDuration::from_raw_parts(2_000_000_000, 2)),
                        };
                        assert_eq!(actual, Ok((Some(expected), 1)));
                        assert_eq!(visited, match entry {
                            Entry::Text => vec![(2, C::Bytes), (0, C::Bytes), (1, C::FoldedInt)],
                            Entry::Time => vec![(0, C::Time), (2, C::Bytes), (1, C::FoldedInt)],
                            Entry::Duration => vec![(0, C::Duration), (2, C::Bytes), (1, C::FoldedInt)],
                        });
                    }
                }
                if slots == 1 {
                    let result = eval_legacy_date_arithmetic_text_in(bound, metadata(D::String), &large_zone,
                        |index, channel, _| -> Result<V, ChildError> {
                            assert_eq!((index, channel), (2, C::Bytes));
                            Ok(V::Bytes(None))
                        }).unwrap();
                    assert_eq!(result, LegacyDateArithmeticResult { value: None, presence: 0 });
                    let result = eval_legacy_date_arithmetic_text_in(bound, metadata(D::String), &zone,
                        |_, _, _| -> Result<V, ChildError> { Err(ChildError::Original("unchanged child failure")) });
                    assert_eq!(result, Err(ChildError::Original("unchanged child failure")));
                    let mut visited = Vec::new();
                    let result = eval_legacy_date_arithmetic_text_in(bound, metadata(D::String), &large_zone,
                        |index, channel, _| -> Result<V, ChildError> {
                            visited.push((index, channel));
                            Ok(match index {
                                2 => V::Bytes(Some(b"DAY".to_vec())),
                                0 => V::Bytes(Some(b"2024-01-01".to_vec())),
                                _ => panic!("zone refusal precedes interval child"),
                            })
                        });
                    assert!(matches!(result, Err(ChildError::Native(EvalError::ExpressionRuntimeFailure(failure)))
                        if matches!(failure.local_error(), LocalError::ResourceLimit(_))));
                    assert_eq!(visited, vec![(2, C::Bytes), (0, C::Bytes)]);
                    let result = eval_legacy_date_arithmetic_time_in(bound, metadata(D::Datetime), &zone,
                        |index, channel, _| -> Result<V, ChildError> {
                            assert_eq!((index, channel), (0, C::Time));
                            Ok(V::Time(None))
                        }).unwrap();
                    assert_eq!(result, LegacyDateArithmeticResult { value: None, presence: 0 });
                }
            });
            drop(scope);
            execution.close();
        }
        let owner = owner(1);
        let execution = owner.begin_execution().unwrap();
        let scope = execution.scope();
        assert!(catch_unwind(AssertUnwindSafe(|| scope.with_columns(
            &Original,
            |bound| {
                let _ = eval_legacy_date_arithmetic_text_in(
                    bound,
                    metadata(D::String),
                    &zone,
                    |index, _, _| -> Result<V, ChildError> {
                        Ok(match index {
                            2 => V::Bytes(Some(b"DAY".to_vec())),
                            0 => V::Bytes(Some(b"2024-01-01".to_vec())),
                            _ => panic!("late original callback panic after zone parse"),
                        })
                    },
                );
            }
        )))
        .is_err());
        scope.with_columns(&Original, |bound| {
            assert!(
                matches!(eval_legacy_date_arithmetic_time_in(bound, metadata(D::Datetime), &zone,
                |_, _, _| -> Result<V, ChildError> { panic!("poisoned before child") }),
                Err(ChildError::Native(EvalError::ExpressionAdapterFailure(failure)))
                    if failure.class() == ExpressionAdapterFailureClass::ScopePoisoned)
            );
        });
        drop(scope);
        execution.close();
        let result = eval_legacy_date_arithmetic_time_in(
            &crate::NoColumns,
            metadata(D::Datetime),
            &zone,
            |_, _, selected| -> Result<V, ChildError> {
                assert!(selected.ready_value_scope().is_some());
                assert_eq!(
                    super::super::eval_interval_in(selected, &[Datum::Int(1), Datum::Int(2)]),
                    Ok(Datum::Int(0))
                );
                Ok(V::Time(None))
            },
        )
        .unwrap();
        assert_eq!(
            result,
            LegacyDateArithmeticResult {
                value: None,
                presence: 0
            }
        );
    }
}

fn invalid_report() -> EvalError {
    EvalError::ExpressionAdapterFailure(ExpressionAdapterFailure::from_scope(
        ScopeFailureKind::Contract,
        "invalid native legacy date arithmetic report",
    ))
}

fn frame_error(error: tidb_query_expr::NativeIdentityFrameError) -> EvalError {
    match error {
        tidb_query_expr::NativeIdentityFrameError::Invalid => invalid_report(),
        tidb_query_expr::NativeIdentityFrameError::Capacity => {
            EvalError::ExpressionRuntimeFailure(ExpressionRuntimeFailure::from_local_eval(
                LocalError::ResourceLimit(
                    "native legacy date arithmetic frame allocation or size failed".into(),
                ),
                None,
            ))
        }
    }
}
