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

use super::adapter_failure::{ExpressionAdapterFailure, ScopeFailureKind};
use super::{ExpressionRuntimeFailure, LocalError};
use crate::{Columns, Datum, EvalError};
use tidb_datatype::{Decimal, MySqlDuration, SessionTimeZone, Time};
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

/// The actual legacy consumer's return channel, independent of SQL metadata.
#[derive(Clone, Copy)]
pub enum LegacyDateArithmeticOutputKind {
    /// Non-temporal date operands render text.
    Text,
    /// Datetime date operands keep their time value.
    Time,
    /// Duration date operands keep their duration value.
    Duration,
}

/// One value-state transition. Only the source's SDK frame evaluates the
/// requested child; this machine contains no expression/context references.
pub enum LegacyDateArithmeticDemand {
    /// Read exactly this original source index and typed channel.
    Source {
        /// Original argument position, never a lowered node ordinal.
        index: usize,
        /// Its genuine legacy typed consumer.
        channel: LegacyDateArithmeticChannel,
    },
    /// The existing SDK producer's value and separate presence observation.
    Ready {
        /// The SDK's actual nullable typed result.
        value: LegacyDateArithmeticValue,
        /// The original separate presence observation.
        presence: i128,
    },
}

/// Owned adapter over the same SDK value state used by the synchronous legacy
/// entry. Pure conversion phases complete before the next source demand.
pub struct LegacyDateArithmeticMachine {
    state: tidb_query_expr::NativeLegacyDateArithmeticValues,
    output: LegacyDateArithmeticOutputKind,
}

impl LegacyDateArithmeticMachine {
    /// Validate the actual consumer/metadata pair without reading any child.
    pub fn new(
        output: LegacyDateArithmeticOutputKind,
        metadata: LegacyDateArithmeticMetadata,
    ) -> Result<Self, EvalError> {
        let valid = match output {
            LegacyDateArithmeticOutputKind::Text => !matches!(
                metadata.date,
                LegacyDateArithmeticDateKind::Datetime | LegacyDateArithmeticDateKind::Duration
            ),
            LegacyDateArithmeticOutputKind::Time => {
                metadata.date == LegacyDateArithmeticDateKind::Datetime
            }
            LegacyDateArithmeticOutputKind::Duration => {
                metadata.date == LegacyDateArithmeticDateKind::Duration
            }
        };
        if !valid {
            return Err(invalid_report());
        }
        Ok(Self {
            state: tidb_query_expr::NativeLegacyDateArithmeticValues::new(
                metadata,
                super::control_value::default_budget(),
            )
            .map_err(actual_error)?,
            output,
        })
    }

    /// Actual known retained state, for the enclosing source service's ledger.
    pub fn retained_bytes(&self) -> Option<usize> {
        self.state.retained_bytes()
    }

    /// Begin at the SDK's original demanded phase.
    pub fn begin(
        &mut self,
        zone: &SessionTimeZone,
    ) -> Result<LegacyDateArithmeticDemand, EvalError> {
        let step = self.state.begin().map_err(actual_error)?;
        self.advance(step, zone)
    }

    /// Supply one actual nullable typed child value. Its owner is not retained
    /// by this adapter; the SDK owns its necessary bounded working storage.
    pub fn resume_source(
        &mut self,
        value: LegacyDateArithmeticValue,
        zone: &SessionTimeZone,
    ) -> Result<LegacyDateArithmeticDemand, EvalError> {
        let input = source_input(&value);
        let step = self.state.resume_source(input).map_err(actual_error)?;
        self.advance(step, zone)
    }

    fn advance(
        &mut self,
        mut step: tidb_query_expr::NativeLegacyDateArithmeticValueStep,
        zone: &SessionTimeZone,
    ) -> Result<LegacyDateArithmeticDemand, EvalError> {
        use tidb_query_expr::{
            NativeLegacyDateArithmeticValue as Value, NativeLegacyDateArithmeticValueStep as Step,
        };
        loop {
            step = match step {
                Step::Source { index, channel } => {
                    return Ok(LegacyDateArithmeticDemand::Source { index, channel })
                }
                Step::ParseDate => self.state.parse_date(zone).map_err(actual_error)?,
                Step::DecimalText { .. } => self.state.finish_decimal().map_err(actual_error)?,
                Step::Ready { value, presence } => {
                    let value = match value {
                        Value::Null => match self.output {
                            LegacyDateArithmeticOutputKind::Text => {
                                LegacyDateArithmeticValue::Bytes(None)
                            }
                            LegacyDateArithmeticOutputKind::Time => {
                                LegacyDateArithmeticValue::Time(None)
                            }
                            LegacyDateArithmeticOutputKind::Duration => {
                                LegacyDateArithmeticValue::Duration(None)
                            }
                        },
                        Value::Text(v) => LegacyDateArithmeticValue::Bytes(Some(v)),
                        Value::Time(v) => {
                            LegacyDateArithmeticValue::Time(Some(Time::from_raw_parts(
                                tidb_datatype::CoreTime::from_raw(v.raw),
                                v.kind,
                                v.fsp,
                            )))
                        }
                        Value::Duration { nanos, fsp } => LegacyDateArithmeticValue::Duration(
                            Some(MySqlDuration::from_raw_parts(nanos, fsp)),
                        ),
                    };
                    return Ok(LegacyDateArithmeticDemand::Ready { value, presence });
                }
            };
        }
    }
}

enum Output {
    Null,
    Text(Vec<u8>),
    Time(Time),
    Duration(MySqlDuration),
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

fn source_input(
    value: &LegacyDateArithmeticValue,
) -> tidb_query_expr::NativeLegacyDateArithmeticInput<'_> {
    use tidb_query_expr::NativeLegacyDateArithmeticInput as Input;
    match value {
        LegacyDateArithmeticValue::Bytes(v) => Input::Bytes(v.as_deref()),
        LegacyDateArithmeticValue::Int(v) => Input::FoldedInt(*v),
        LegacyDateArithmeticValue::Real(v) => Input::Real(*v),
        LegacyDateArithmeticValue::Decimal(v) => {
            Input::Decimal(v.as_ref().map(Decimal::as_shared_parse))
        }
        LegacyDateArithmeticValue::Time(v) => {
            Input::Time(v.map(
                |v| tidb_query_datatype::codec::mysql::time::NativeTemporalValue {
                    raw: v.core_time().raw(),
                    kind: v.kind(),
                    fsp: v.fsp(),
                },
            ))
        }
        LegacyDateArithmeticValue::Duration(v) => {
            Input::Duration(v.map(|v| (v.nanoseconds(), v.fsp())))
        }
    }
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

fn actual_error(error: tidb_query_expr::NativeLegacyDateArithmeticValueError) -> EvalError {
    match error {
        tidb_query_expr::NativeLegacyDateArithmeticValueError::InvalidResponse => invalid_report(),
        tidb_query_expr::NativeLegacyDateArithmeticValueError::ResourceLimit => {
            EvalError::ExpressionRuntimeFailure(ExpressionRuntimeFailure::from_local_eval(
                LocalError::ResourceLimit(
                    "native legacy date arithmetic actual work allowance exceeded".into(),
                ),
                None,
            ))
        }
    }
}
fn evaluate_values<E: From<EvalError>>(
    ctx: &dyn Columns,
    entry: Entry,
    metadata: LegacyDateArithmeticMetadata,
    zone: &SessionTimeZone,
    mut eval: impl FnMut(
        usize,
        LegacyDateArithmeticChannel,
        &dyn Columns,
    ) -> Result<LegacyDateArithmeticValue, E>,
    maximum: usize,
) -> Result<(Output, i128), E> {
    use tidb_query_expr::{
        NativeLegacyDateArithmeticValue as Value, NativeLegacyDateArithmeticValueStep as Step,
        NativeLegacyDateArithmeticValues as State,
    };
    let valid = match entry {
        Entry::Text => !matches!(
            metadata.date,
            LegacyDateArithmeticDateKind::Datetime | LegacyDateArithmeticDateKind::Duration
        ),
        Entry::Time => metadata.date == LegacyDateArithmeticDateKind::Datetime,
        Entry::Duration => metadata.date == LegacyDateArithmeticDateKind::Duration,
    };
    if !valid {
        return Err(E::from(invalid_report()));
    }
    let mut state = State::new(metadata, maximum).map_err(|e| E::from(actual_error(e)))?;
    let mut step = state.begin().map_err(|e| E::from(actual_error(e)))?;
    loop {
        step = match step {
            Step::Source { index, channel } => {
                // The source owner alone evaluates this exact original channel;
                // no alternate evaluator or report program runs in this layer.
                let value = eval(index, channel, ctx)?;
                let input = source_input(&value);
                state
                    .resume_source(input)
                    .map_err(|e| E::from(actual_error(e)))?
            }
            Step::ParseDate => state
                .parse_date(zone)
                .map_err(|e| E::from(actual_error(e)))?,
            Step::DecimalText { .. } => state
                .finish_decimal()
                .map_err(|e| E::from(actual_error(e)))?,
            Step::Ready { value, presence } => {
                let value = match value {
                    Value::Null => Output::Null,
                    Value::Text(v) => Output::Text(v),
                    Value::Time(v) => Output::Time(Time::from_raw_parts(
                        tidb_datatype::CoreTime::from_raw(v.raw),
                        v.kind,
                        v.fsp,
                    )),
                    Value::Duration { nanos, fsp } => {
                        Output::Duration(MySqlDuration::from_raw_parts(nanos, fsp))
                    }
                };
                return Ok((value, presence));
            }
        };
    }
}

fn evaluate<E: From<EvalError>>(
    ctx: &dyn Columns,
    entry: Entry,
    metadata: LegacyDateArithmeticMetadata,
    zone: &SessionTimeZone,
    eval: impl FnMut(
        usize,
        LegacyDateArithmeticChannel,
        &dyn Columns,
    ) -> Result<LegacyDateArithmeticValue, E>,
) -> Result<(Output, i128), E> {
    evaluate_values(
        ctx,
        entry,
        metadata,
        zone,
        eval,
        super::control_value::default_budget(),
    )
}

#[cfg(test)]
mod actual_tests {
    use super::*;
    use std::cell::RefCell;
    #[derive(Debug)]
    enum Error {
        Eval(EvalError),
        Child(usize),
    }
    impl From<EvalError> for Error {
        fn from(e: EvalError) -> Self {
            Self::Eval(e)
        }
    }
    struct Context;
    impl Columns for Context {
        fn get(&self, _: &[String]) -> Option<Datum> {
            panic!("no fabricated sources")
        }
        fn ready_value_cache(&self) -> Option<&super::super::ReadyValueCache> {
            panic!("no Ready lookup")
        }
    }
    #[test]
    fn legacy_date_arithmetic_resource_contract_and_callback_error_remain_terminal() {
        let meta = LegacyDateArithmeticMetadata {
            date: LegacyDateArithmeticDateKind::String,
            interval: LegacyDateArithmeticIntervalKind::Int,
            subtract: false,
        };
        let read = RefCell::new(vec![]);
        let value = evaluate_values::<Error>(
            &Context,
            Entry::Text,
            meta,
            &SessionTimeZone::utc(),
            |index, _, _| {
                read.borrow_mut().push(index);
                Ok(match index {
                    2 => LegacyDateArithmeticValue::Bytes(Some(b"DAY".to_vec())),
                    0 => LegacyDateArithmeticValue::Bytes(Some(b"2020-01-31".to_vec())),
                    _ => panic!("interval must not run after parse work refusal"),
                })
            },
            2048,
        );
        assert!(matches!(
            value,
            Err(Error::Eval(EvalError::ExpressionRuntimeFailure(_)))
        ));
        assert_eq!(*read.borrow(), [2, 0]);
        read.borrow_mut().clear();
        let value = evaluate_values::<Error>(
            &Context,
            Entry::Text,
            meta,
            &SessionTimeZone::utc(),
            |index, _, _| {
                read.borrow_mut().push(index);
                Err(Error::Child(index))
            },
            0,
        );
        assert!(matches!(value, Err(Error::Child(2))));
        assert_eq!(*read.borrow(), [2]);
        let value = evaluate_values::<Error>(
            &Context,
            Entry::Text,
            meta,
            &SessionTimeZone::utc(),
            |_, _, _| Ok(LegacyDateArithmeticValue::Bytes(None)),
            0,
        )
        .unwrap();
        assert!(matches!(value, (Output::Null, 0)));
        let wrong = LegacyDateArithmeticMetadata {
            date: LegacyDateArithmeticDateKind::Duration,
            ..meta
        };
        assert!(matches!(
            evaluate_values::<Error>(
                &Context,
                Entry::Text,
                wrong,
                &SessionTimeZone::utc(),
                |_, _, _| panic!("invalid domain must not read sources"),
                8192
            ),
            Err(Error::Eval(EvalError::ExpressionAdapterFailure(_)))
        ));
    }
}
