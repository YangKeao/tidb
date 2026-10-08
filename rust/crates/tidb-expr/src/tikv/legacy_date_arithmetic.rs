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
