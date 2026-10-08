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

use tidb_datatype::FieldType;
use tidb_query_expr::{
    NativeDateArithmeticOutcome as Outcome, NativeDateArithmeticRequest as Request,
};

use super::adapter_failure::{ExpressionAdapterFailure, ScopeFailureKind};
use super::{
    evaluate_args_in, evaluate_prepared_args_scoped_in, identity_value, EvaluatedArgs,
    EvaluatedBytesOp as Op, EvaluatedBytesResult, ExpressionRuntimeFailure, LocalError,
};
use crate::{Columns, Datum, ErrorLevel, EvalError};

fn invalid_report() -> EvalError {
    EvalError::ExpressionAdapterFailure(ExpressionAdapterFailure::from_scope(
        ScopeFailureKind::Contract,
        "invalid native date arithmetic report",
    ))
}

fn frame_error(error: tidb_query_expr::NativeIdentityFrameError) -> EvalError {
    match error {
        tidb_query_expr::NativeIdentityFrameError::Invalid => invalid_report(),
        tidb_query_expr::NativeIdentityFrameError::Capacity => {
            EvalError::ExpressionRuntimeFailure(ExpressionRuntimeFailure::from_local_eval(
                LocalError::ResourceLimit(
                    "native date arithmetic frame allocation or size failed".into(),
                ),
                None,
            ))
        }
    }
}

#[derive(Clone, Copy)]
enum Domain {
    Calendar,
    Duration,
}
#[derive(Clone, Copy)]
enum Stage {
    Head,
    Step,
    Overflow,
}

fn unsupported(reason: &str) -> EvalError {
    let reason = match reason {
        "composite INTERVAL amount" => "composite INTERVAL amount",
        "INTERVAL unit" => "INTERVAL unit",
        "range sentinel INTERVAL amount" => "range sentinel INTERVAL amount",
        "DATE_ADD duration operand" => "DATE_ADD duration operand",
        _ => return invalid_report(),
    };
    EvalError::Unsupported(reason)
}

fn drive(
    computed: EvaluatedBytesResult,
    selected: &dyn Columns,
    semantic: &dyn Columns,
    domain: Domain,
    values: [&Datum; 2],
) -> Result<Datum, EvalError> {
    let mut state = computed.into_bytes()?.ok_or_else(invalid_report)?;
    let mut stage = Stage::Head;
    loop {
        let report = tidb_query_expr::decode_native_date_arithmetic_result(&state)
            .ok_or_else(invalid_report)?;
        let allowed = match &report.outcome {
            Outcome::Null => true,
            Outcome::Text(_) => {
                matches!(domain, Domain::Calendar) && !matches!(stage, Stage::Overflow)
            }
            Outcome::Duration { .. } => {
                matches!(domain, Domain::Duration) && !matches!(stage, Stage::Overflow)
            }
            Outcome::Unsupported(_) | Outcome::Request { .. } => !matches!(stage, Stage::Overflow),
            Outcome::Error { code, .. } => *code == 1441 && matches!(stage, Stage::Overflow),
            Outcome::Overflow { .. } => {
                matches!(domain, Domain::Calendar) && !matches!(stage, Stage::Overflow)
            }
        };
        if !allowed {
            return Err(invalid_report());
        }
        if let Some(warning) = report.warning {
            if (matches!(domain, Domain::Duration) && matches!(stage, Stage::Head))
                || (matches!(stage, Stage::Overflow) && !matches!(report.outcome, Outcome::Null))
            {
                return Err(invalid_report());
            }
            let expected = if matches!(stage, Stage::Overflow) {
                1441
            } else {
                1292
            };
            if warning.code != expected {
                return Err(invalid_report());
            }
            // These are original unconditional append_warning sites, not the
            // statement's generic truncation handler. Replay BEFORE demand.
            semantic.append_warning(warning.code, warning.message);
        }
        let (operation, next_stage) = match report.outcome {
            Outcome::Null => return Ok(Datum::Null),
            Outcome::Text(text) => return Ok(Datum::new_string(text)),
            Outcome::Duration { nanos, fsp } => {
                return Ok(Datum::new_duration(
                    tidb_datatype::MySqlDuration::from_raw_parts(nanos, fsp),
                ))
            }
            Outcome::Unsupported(reason) => return Err(unsupported(reason)),
            Outcome::Error {
                code: 1441,
                message,
            } => {
                return Err(EvalError::Conversion(
                    tidb_datatype::ERR_DATETIME_FUNCTION_OVERFLOW.generate(message),
                ))
            }
            Outcome::Error { .. } => return Err(invalid_report()),
            Outcome::Request { .. } => (Op::DateArithmeticStepNative, Stage::Step),
            Outcome::Overflow { .. } => (Op::DateArithmeticOverflowNative, Stage::Overflow),
        };
        state = evaluate_args_in(
            operation,
            selected,
            || {
                // Decode inside the prepare so SDK text may borrow this same Vec
                // until its generic cast completes; forward the WHOLE report after
                // that borrow ends. No callback or warning is replayed by decoding.
                let report = tidb_query_expr::decode_native_date_arithmetic_result(&state)
                    .ok_or_else(invalid_report)?;
                match report.outcome {
                    Outcome::Request {
                        kind, index, text, ..
                    } => {
                        let value = values.get(index).ok_or_else(invalid_report)?;
                        let actual = match kind {
                            Request::CoerceString if text.is_none() => {
                                crate::coerce::coerce_str(value)?.map(String::into_bytes)
                            }
                            Request::ToI64 if text.is_none() => {
                                let value = value
                                    .to_i64()
                                    .map_err(|_| {
                                        EvalError::Unsupported("INTERVAL amount conversion")
                                    })?
                                    .value;
                                identity_value::encode(&Datum::Int(value))?
                            }
                            Request::ParseDecimal => {
                                let text = text.ok_or_else(invalid_report)?;
                                identity_value::encode(&Datum::Decimal(
                                    tidb_datatype::Decimal::parse_mysql(text).0,
                                ))?
                            }
                            _ => return Err(invalid_report()),
                        };
                        Ok(EvaluatedArgs::Bytes2(Some(state), actual))
                    }
                    Outcome::Overflow { .. } => {
                        let level = match semantic.truncate_level() {
                            ErrorLevel::Ignore => 0,
                            ErrorLevel::Warn => 1,
                            ErrorLevel::Error => 2,
                        };
                        Ok(EvaluatedArgs::BytesInt(Some(state), Some(level)))
                    }
                    _ => Err(invalid_report()),
                }
            },
            |computed| computed.into_bytes()?.ok_or_else(invalid_report),
        )?;
        stage = next_stage;
    }
}

fn evaluate(
    authority: &dyn Columns,
    default_policy: bool,
    domain: Domain,
    unit: &str,
    date: &Datum,
    amount: &Datum,
    metadata: impl FnOnce() -> Result<Vec<u8>, tidb_query_expr::NativeIdentityFrameError>,
) -> Result<Datum, EvalError> {
    evaluate_prepared_args_scoped_in(
        authority,
        || {
            let operation = match domain {
                Domain::Calendar => Op::DateArithmeticHeadNative,
                Domain::Duration => Op::DateArithmeticDurationHeadNative,
            };
            Ok((
                operation,
                EvaluatedArgs::Bytes4([
                    identity_value::encode(date)?,
                    identity_value::encode(amount)?,
                    Some(unit.as_bytes().to_vec()),
                    Some(metadata().map_err(frame_error)?),
                ]),
            ))
        },
        |computed, selected| {
            if default_policy {
                let cache = selected.ready_value_cache().ok_or_else(invalid_report)?;
                // AST DATE_ADD formerly passed NoColumns into its entire body.
                // Keep that semantic policy while borrowing the selected lane cache.
                cache.with_columns(&crate::NoColumns, |defaults| {
                    drive(computed, defaults, defaults, domain, [date, amount])
                })
            } else {
                drive(computed, selected, authority, domain, [date, amount])
            }
        },
    )
}

pub(crate) fn eval_date_add_in(
    ctx: &dyn Columns,
    unit: &str,
    date: &Datum,
    amount: &Datum,
    sign: i64,
    result_fsp: Option<u32>,
) -> Result<Datum, EvalError> {
    evaluate(ctx, false, Domain::Calendar, unit, date, amount, || {
        tidb_query_expr::encode_native_date_arithmetic_metadata(sign, result_fsp)
    })
}

pub(crate) fn eval_date_add_default_in(
    authority: &dyn Columns,
    unit: &str,
    date: &Datum,
    amount: &Datum,
    sign: i64,
) -> Result<Datum, EvalError> {
    evaluate(
        authority,
        true,
        Domain::Calendar,
        unit,
        date,
        amount,
        || tidb_query_expr::encode_native_date_arithmetic_metadata(sign, None),
    )
}

pub(crate) fn eval_date_add_duration_in(
    ctx: &dyn Columns,
    unit: &str,
    date: &Datum,
    amount: &Datum,
    amount_type: Option<&FieldType>,
    sign: i64,
    result_fsp: i64,
) -> Result<Datum, EvalError> {
    evaluate(ctx, false, Domain::Duration, unit, date, amount, || {
        tidb_query_expr::encode_native_date_arithmetic_duration_metadata(
            sign,
            result_fsp,
            amount_type.map(FieldType::decimal),
        )
    })
}

#[cfg(test)]
mod tests {
    use std::cell::{Cell, RefCell};
    use std::panic::{catch_unwind, AssertUnwindSafe};

    use tidb_datatype::{DateModes, MySqlDuration, SessionTimeZone};

    use super::*;
    use crate::{ExpressionAdapterFailureClass, ReadyValuePoolOwner, ReadyValuePoolPolicy};

    #[test]
    fn date_arithmetic_bridge_keeps_default_policy_actual_scope_and_late_statement_callbacks() {
        struct Statement {
            warnings: RefCell<Vec<u16>>,
            levels: Cell<usize>,
            panic_warning: Cell<bool>,
            panic_level: Cell<bool>,
        }
        impl Columns for Statement {
            fn get(&self, _: &[String]) -> Option<Datum> {
                panic!("already evaluated operands")
            }
            fn date_modes(&self) -> DateModes {
                panic!("calendar body never reads date modes")
            }
            fn time_zone(&self) -> SessionTimeZone {
                panic!("calendar body never reads session zone")
            }
            fn now(&self) -> Option<(i64, u32, i32)> {
                panic!("typed pre-cast owns clock reads")
            }
            fn append_warning(&self, code: u16, _: &str) {
                self.warnings.borrow_mut().push(code);
                assert!(!self.panic_warning.get(), "original warning callback panic");
            }
            fn truncate_level(&self) -> ErrorLevel {
                self.levels.set(self.levels.get() + 1);
                assert!(
                    !self.panic_level.get(),
                    "original overflow policy callback panic"
                );
                assert_eq!(
                    *self.warnings.borrow(),
                    vec![1292],
                    "amount warning precedes overflow policy"
                );
                ErrorLevel::Error
            }
        }
        let statement = Statement {
            warnings: RefCell::new(Vec::new()),
            levels: Cell::new(0),
            panic_warning: Cell::new(false),
            panic_level: Cell::new(false),
        };
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
        let text = |value: &str| Datum::new_string(value);
        let date = text("2024-01-01");
        let overflow = text("9999-12-31");
        let duration = Datum::new_duration(MySqlDuration::from_raw_parts(1_000_000_000, 0));
        for slots in [0, 1] {
            let owner = owner(slots);
            let execution = owner.begin_execution().unwrap();
            let scope = execution.scope();
            scope.with_columns(&statement, |bound| {
                let actual = eval_date_add_in(bound, "DAY", &date, &text("1x"), 1, None);
                if slots == 0 {
                    assert!(
                        matches!(actual, Err(EvalError::ExpressionAdapterFailure(failure))
                        if failure.class() == ExpressionAdapterFailureClass::PoolResource)
                    );
                    assert!(statement.warnings.borrow().is_empty());
                } else {
                    assert_eq!(actual, Ok(text("2024-01-02")));
                    assert_eq!(*statement.warnings.borrow(), vec![1292]);
                    assert_eq!(statement.levels.get(), 0);
                }
                statement.warnings.borrow_mut().clear();
                statement.panic_warning.set(true);
                statement.panic_level.set(true);
                let defaults = eval_date_add_default_in(bound, "DAY", &date, &text("1x"), 1);
                let default_overflow =
                    eval_date_add_default_in(bound, "DAY", &overflow, &Datum::Int(1), 1);
                statement.panic_warning.set(false);
                statement.panic_level.set(false);
                for (actual, expected) in [
                    (defaults, text("2024-01-02")),
                    (default_overflow, Datum::Null),
                ] {
                    if slots == 0 {
                        assert!(
                            matches!(actual, Err(EvalError::ExpressionAdapterFailure(failure))
                            if failure.class() == ExpressionAdapterFailureClass::PoolResource)
                        );
                    } else {
                        assert_eq!(actual, Ok(expected));
                    }
                }
                assert!(statement.warnings.borrow().is_empty());
                assert_eq!(statement.levels.get(), 0);
                let actual = eval_date_add_duration_in(
                    bound,
                    "SECOND",
                    &duration,
                    &Datum::Int(1),
                    None,
                    1,
                    0,
                );
                if slots == 0 {
                    assert!(
                        matches!(actual, Err(EvalError::ExpressionAdapterFailure(failure))
                        if failure.class() == ExpressionAdapterFailureClass::PoolResource)
                    );
                } else {
                    assert_eq!(
                        actual,
                        Ok(Datum::new_duration(MySqlDuration::from_raw_parts(
                            2_000_000_000,
                            0
                        )))
                    );
                    assert!(matches!(
                        eval_date_add_in(bound, "DAY", &overflow, &text("1x"), 1, None),
                        Err(EvalError::Conversion(_))
                    ));
                    assert_eq!(statement.levels.get(), 1);
                    assert_eq!(*statement.warnings.borrow(), vec![1292]);
                    statement.warnings.borrow_mut().clear();
                    statement.levels.set(0);
                    let large = Datum::new_bytes(vec![b'1'; 128 << 10]);
                    assert!(
                        matches!(eval_date_add_in(bound, "DAY", &large, &Datum::Int(1), 1, None),
                        Err(EvalError::ExpressionRuntimeFailure(failure))
                            if matches!(failure.local_error(), LocalError::ResourceLimit(_)))
                    );
                    assert!(statement.warnings.borrow().is_empty());
                    assert_eq!(statement.levels.get(), 0);
                    assert_eq!(
                        eval_date_add_in(bound, "DAY", &date, &Datum::Int(1), 1, None),
                        Ok(text("2024-01-02"))
                    );
                }
            });
            drop(scope);
            execution.close();
        }
        for warning_panic in [true, false] {
            let owner = owner(1);
            let execution = owner.begin_execution().unwrap();
            let scope = execution.scope();
            statement.panic_warning.set(warning_panic);
            statement.panic_level.set(!warning_panic);
            assert!(catch_unwind(AssertUnwindSafe(|| scope.with_columns(
                &statement,
                |bound| {
                    let _ = if warning_panic {
                        eval_date_add_in(bound, "DAY", &date, &text("1x"), 1, None)
                    } else {
                        eval_date_add_in(bound, "DAY", &overflow, &Datum::Int(1), 1, None)
                    };
                }
            )))
            .is_err());
            statement.panic_warning.set(false);
            statement.panic_level.set(false);
            scope.with_columns(&statement, |bound| {
                assert!(
                    matches!(eval_date_add_in(bound, "DAY", &date, &Datum::Int(1), 1, None),
                    Err(EvalError::ExpressionAdapterFailure(failure))
                        if failure.class() == ExpressionAdapterFailureClass::ScopePoisoned)
                );
            });
            drop(scope);
            execution.close();
        }
        assert_eq!(
            eval_date_add_default_in(&crate::NoColumns, "DAY", &date, &text("1x"), 1),
            Ok(text("2024-01-02"))
        );
    }
}
