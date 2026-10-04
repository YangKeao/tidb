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

use tidb_datatype::{DatumKind, FieldType, FieldTypeCode};
use tidb_query_expr::{decode_native_extract_result, NativeExtractResult};

use super::adapter_failure::{ExpressionAdapterFailure, ScopeFailureKind};
use super::{
    evaluate_args_in, evaluate_prepared_args_scoped_in, EvaluatedArgs, EvaluatedBytesOp,
    EvaluatedBytesResult,
};
use crate::{Columns, Datum, EvalError};

fn invalid_report() -> EvalError {
    EvalError::ExpressionAdapterFailure(ExpressionAdapterFailure::from_scope(
        ScopeFailureKind::Contract,
        "invalid native EXTRACT report",
    ))
}

fn kind_code(kind: DatumKind) -> i64 {
    match kind {
        DatumKind::Null => 0,
        DatumKind::MinNotNull => 1,
        DatumKind::MaxValue => 2,
        DatumKind::Int => 3,
        DatumKind::UInt => 4,
        DatumKind::Decimal => 5,
        DatumKind::Real => 6,
        DatumKind::Float32 => 7,
        DatumKind::String => 8,
        DatumKind::Bytes => 9,
        DatumKind::BinaryLiteral => 10,
        DatumKind::Duration => 11,
        DatumKind::Enum => 12,
        DatumKind::Bit => 13,
        DatumKind::Set => 14,
        DatumKind::Time => 15,
        DatumKind::Json => 16,
        DatumKind::Raw => 17,
        DatumKind::VectorFloat32 => 18,
    }
}

#[derive(Clone, Copy)]
enum TerminalStage {
    Number,
    MixedDuration,
    Composite,
}

fn terminal_report(
    report: Option<Vec<u8>>,
    ctx: &dyn Columns,
    stage: TerminalStage,
) -> Result<Datum, EvalError> {
    let Some(report) = report else {
        return if matches!(stage, TerminalStage::Composite) {
            Ok(Datum::Null)
        } else {
            Err(invalid_report())
        };
    };
    match (
        stage,
        decode_native_extract_result(&report).ok_or_else(invalid_report)?,
    ) {
        (TerminalStage::Number | TerminalStage::Composite, NativeExtractResult::Value(value)) => {
            Ok(Datum::Int(value))
        }
        (TerminalStage::Number, NativeExtractResult::InvalidUnit(message)) => Err(
            EvalError::Conversion(tidb_error::terror::TerrorError::compatible(
                tidb_error::terror::TerrorCode::new(1105),
                message.to_owned(),
            )),
        ),
        (TerminalStage::MixedDuration, NativeExtractResult::InvalidTime(message)) => {
            Err(EvalError::Conversion(
                tidb_datatype::ERR_TRUNCATED_WRONG_VALUE.generate(message.to_owned()),
            ))
        }
        (TerminalStage::Composite, NativeExtractResult::Warning(message)) => {
            ctx.append_warning(1292, message);
            Ok(Datum::Null)
        }
        _ => Err(invalid_report()),
    }
}

fn null_reply(computed: EvaluatedBytesResult) -> Result<Datum, EvalError> {
    match computed.into_int_datum()? {
        value @ Datum::Null => Ok(value),
        _ => Err(invalid_report()),
    }
}

pub(crate) fn eval_extract_null_unit_in(ctx: &dyn Columns) -> Result<Datum, EvalError> {
    evaluate_args_in(
        EvaluatedBytesOp::DateDiffNullNative,
        ctx,
        || Ok(EvaluatedArgs::NullWitness(None)),
        null_reply,
    )
}

fn cast_and_extract(
    ctx: &dyn Columns,
    selected: &dyn Columns,
    unit: &str,
    value: &Datum,
    source: Option<&FieldType>,
    datetime: bool,
) -> Result<Datum, EvalError> {
    let null_path = Cell::new(false);
    evaluate_prepared_args_scoped_in(
        selected,
        || {
            // Keep the complete original source metadata and original statement
            // overrides. These existing casts own their getter/warning order.
            let cast = if datetime {
                crate::cast::cast_arg_as_datetime(value, source, ctx)?
            } else {
                crate::cast::cast_arg_as_duration(value, source, ctx)?
            };
            match cast {
                Datum::Null => {
                    null_path.set(true);
                    Ok((
                        EvaluatedBytesOp::DateDiffNullNative,
                        EvaluatedArgs::NullWitness(None),
                    ))
                }
                Datum::Time(time) if datetime => Ok((
                    EvaluatedBytesOp::ExtractDatetimeNative,
                    EvaluatedArgs::TimeCoreBitsBytes {
                        core: time.core_time().raw(),
                        bytes: Some(unit.as_bytes().to_vec()),
                    },
                )),
                Datum::Duration(duration) if !datetime => Ok((
                    EvaluatedBytesOp::ExtractDurationNative,
                    EvaluatedArgs::BytesInt(
                        Some(unit.as_bytes().to_vec()),
                        Some(duration.nanoseconds()),
                    ),
                )),
                _ => Err(invalid_report()),
            }
        },
        |computed, _| {
            if null_path.get() {
                null_reply(computed)
            } else {
                terminal_report(computed.into_bytes()?, ctx, TerminalStage::Number)
            }
        },
    )
}

fn mixed_extract(
    ctx: &dyn Columns,
    selected: &dyn Columns,
    unit: &str,
    value: &Datum,
    source: Option<&FieldType>,
) -> Result<Datum, EvalError> {
    let null_path = Cell::new(false);
    evaluate_prepared_args_scoped_in(
        selected,
        || {
            let cast = crate::cast::cast_arg_as_string(value, source, ctx)?;
            let Some(text) = crate::coerce::coerce_str(&cast)? else {
                null_path.set(true);
                return Ok((
                    EvaluatedBytesOp::DateDiffNullNative,
                    EvaluatedArgs::NullWitness(None),
                ));
            };
            let modes = ctx.date_modes();
            Ok((
                EvaluatedBytesOp::ExtractMixedDurationNative,
                EvaluatedArgs::BytesBytesInt(
                    Some(unit.as_bytes().to_vec()),
                    Some(text.into_bytes()),
                    Some(i64::from(modes.allow_invalid_dates)),
                ),
            ))
        },
        |computed, selected| {
            if null_path.get() {
                return null_reply(computed);
            }
            let report = computed.into_bytes()?.ok_or_else(invalid_report)?;
            match decode_native_extract_result(&report).ok_or_else(invalid_report)? {
                NativeExtractResult::NeedMixedDatetime(_) => evaluate_args_in(
                    EvaluatedBytesOp::ExtractMixedFinishNative,
                    selected,
                    || {
                        // This second getter is independently demanded only by
                        // a successful duration stage. Never reuse the first flag.
                        let modes = ctx.date_modes();
                        Ok(EvaluatedArgs::BytesInt(
                            Some(report),
                            Some(i64::from(modes.allow_invalid_dates)),
                        ))
                    },
                    |computed| terminal_report(computed.into_bytes()?, ctx, TerminalStage::Number),
                ),
                _ => terminal_report(Some(report), ctx, TerminalStage::MixedDuration),
            }
        },
    )
}

pub(crate) fn eval_extract_in(
    ctx: &dyn Columns,
    unit: &str,
    value: &Datum,
    source: Option<&FieldType>,
) -> Result<Datum, EvalError> {
    evaluate_prepared_args_scoped_in(
        ctx,
        || {
            Ok((
                EvaluatedBytesOp::ExtractSelectNative,
                EvaluatedArgs::BytesIntInt(
                    Some(unit.as_bytes().to_vec()),
                    source.map(|field| match field.code() {
                        FieldTypeCode::Unknown(raw) => -1 - i64::from(raw),
                        known => i64::from(known.mysql_type()),
                    }),
                    Some(kind_code(value.kind())),
                ),
            ))
        },
        |computed, selected| {
            let report = computed.into_bytes()?.ok_or_else(invalid_report)?;
            match decode_native_extract_result(&report).ok_or_else(invalid_report)? {
                NativeExtractResult::NeedDatetimeCast => {
                    cast_and_extract(ctx, selected, unit, value, source, true)
                }
                NativeExtractResult::NeedDurationCast => {
                    cast_and_extract(ctx, selected, unit, value, source, false)
                }
                NativeExtractResult::NeedMixedStringCast => {
                    mixed_extract(ctx, selected, unit, value, source)
                }
                _ => Err(invalid_report()),
            }
        },
    )
}

pub(crate) fn eval_extract_composite_in(
    ctx: &dyn Columns,
    unit: &str,
    vals: &[Datum],
) -> Result<Datum, EvalError> {
    evaluate_args_in(
        EvaluatedBytesOp::ExtractCompositeNative,
        ctx,
        || {
            let [value] = vals else {
                return Err(EvalError::Unsupported("bad function arity"));
            };
            Ok(EvaluatedArgs::Bytes2(
                Some(unit.as_bytes().to_vec()),
                crate::coerce::coerce_str(value)?.map(String::into_bytes),
            ))
        },
        |computed| terminal_report(computed.into_bytes()?, ctx, TerminalStage::Composite),
    )
}

#[cfg(test)]
mod tests {
    use std::panic::{catch_unwind, AssertUnwindSafe};

    use tidb_datatype::{CoreTime, DateModes, MySqlDuration, SessionTimeZone, Time, TimeType};

    use super::super::LocalError;
    use super::*;
    use crate::{AsciiPoolOwner, AsciiPoolPolicy, ExpressionAdapterFailureClass};

    #[test]
    fn extract_bridge_keeps_six_profiles_and_guarded_continuation_lifetimes() {
        struct Statement {
            modes: Cell<usize>,
            panic_at_mode: Cell<usize>,
            panic_warning: Cell<bool>,
        }
        impl Columns for Statement {
            fn get(&self, _: &[String]) -> Option<Datum> {
                panic!("ready extract input")
            }
            fn time_zone(&self) -> SessionTimeZone {
                panic!("identity casts and mixed UTC parser need no zone")
            }
            fn now(&self) -> Option<(i64, u32, i32)> {
                panic!("no statement clock demand")
            }
            fn date_modes(&self) -> DateModes {
                let call = self.modes.get() + 1;
                self.modes.set(call);
                assert_ne!(call, self.panic_at_mode.get(), "late mode callback panic");
                DateModes::default()
            }
            fn append_warning(&self, code: u16, message: &str) {
                assert_eq!(code, 1292);
                assert_eq!(message, "Incorrect datetime value: '0000-00-00 00:00:00'");
                assert!(
                    !self.panic_warning.get(),
                    "composite warning callback panic"
                );
            }
        }
        let statement = Statement {
            modes: Cell::new(0),
            panic_at_mode: Cell::new(0),
            panic_warning: Cell::new(false),
        };
        let owner = |slots| {
            AsciiPoolOwner::new(
                AsciiPoolPolicy::checked(slots, slots, 16 << 20, 1 << 20, 2 << 20, 64, 16, 1 << 16)
                    .unwrap(),
            )
            .unwrap()
        };
        let text = |value: &str| Datum::new_string(value);
        let time = Datum::Time(Time::from_raw_parts(
            CoreTime::from_date(2024, 1, 2, 3, 4, 5, 0),
            TimeType::DateTime,
            0,
        ));
        let duration = Datum::Duration(MySqlDuration::from_raw_parts(3_723_456_000_000, 6));
        for slots in [0, 1] {
            let owner = owner(slots);
            let execution = owner.begin_execution().unwrap();
            let scope = execution.scope();
            statement.modes.set(0);
            scope.with_columns(&statement, |bound| {
                // All six new recipes are exercised through their bridge paths;
                // every stage keeps the same explicit one-slot/zero-slot binding.
                for (actual, expected) in [
                    (
                        eval_extract_in(bound, "DAY_SECOND", &time, None),
                        Datum::Int(2_030_405),
                    ),
                    (
                        eval_extract_in(bound, "SECOND_MICROSECOND", &duration, None),
                        Datum::Int(3_456_000),
                    ),
                    (
                        eval_extract_in(bound, "DAY_SECOND", &text("01:02:03"), None),
                        Datum::Int(10_203),
                    ),
                    (
                        eval_extract_composite_in(
                            bound,
                            "DAY_SECOND",
                            &[text("2024-01-02 03:04:05")],
                        ),
                        Datum::Int(2_030_405),
                    ),
                    (
                        eval_extract_composite_in(bound, "DAY_SECOND", &[Datum::Null]),
                        Datum::Null,
                    ),
                    (
                        eval_extract_in(bound, "YEAR", &Datum::Null, None),
                        Datum::Null,
                    ),
                    (eval_extract_null_unit_in(bound), Datum::Null),
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
                assert_eq!(statement.modes.get(), if slots == 0 { 0 } else { 2 });
                assert_eq!(
                    eval_extract_composite_in(bound, "DAY_SECOND", &[]),
                    Err(EvalError::Unsupported("bad function arity"))
                );
                if slots == 1 {
                    let mut spare = Vec::with_capacity(128 << 10);
                    spare.extend_from_slice(b"01:02:03");
                    let refused = evaluate_args_in(
                        EvaluatedBytesOp::ExtractCompositeNative,
                        bound,
                        || {
                            Ok(EvaluatedArgs::Bytes2(
                                Some(b"DAY_SECOND".to_vec()),
                                Some(spare),
                            ))
                        },
                        |computed| {
                            terminal_report(
                                computed.into_bytes()?,
                                &statement,
                                TerminalStage::Composite,
                            )
                        },
                    );
                    assert!(
                        matches!(refused, Err(EvalError::ExpressionRuntimeFailure(failure))
                        if matches!(failure.local_error(), LocalError::ResourceLimit(_)))
                    );
                    assert_eq!(
                        eval_extract_in(bound, "SECOND_MICROSECOND", &duration, None),
                        Ok(Datum::Int(3_456_000))
                    );
                }
            });
            drop(scope);
            execution.close();
        }
        for report in [None, Some(vec![]), Some(vec![255])] {
            assert!(
                matches!(terminal_report(report, &statement, TerminalStage::Number),
                Err(EvalError::ExpressionAdapterFailure(failure))
                    if failure.class() == ExpressionAdapterFailureClass::ScopeContract)
            );
        }
        for mode_panic in [true, false] {
            let owner = owner(1);
            let execution = owner.begin_execution().unwrap();
            let scope = execution.scope();
            statement.modes.set(0);
            statement.panic_at_mode.set(if mode_panic { 2 } else { 0 });
            statement.panic_warning.set(!mode_panic);
            assert!(catch_unwind(AssertUnwindSafe(|| scope.with_columns(
                &statement,
                |bound| {
                    if mode_panic {
                        let _ = eval_extract_in(bound, "DAY_SECOND", &text("01:02:03"), None);
                    } else {
                        let _ = eval_extract_composite_in(bound, "DAY_SECOND", &[text("bad")]);
                    }
                }
            )))
            .is_err());
            statement.panic_at_mode.set(0);
            statement.panic_warning.set(false);
            scope.with_columns(&statement, |bound| {
                assert!(matches!(eval_extract_null_unit_in(bound),
                    Err(EvalError::ExpressionAdapterFailure(failure))
                        if failure.class() == ExpressionAdapterFailureClass::ScopePoisoned));
            });
            drop(scope);
            execution.close();
        }
        assert_eq!(
            eval_extract_in(&crate::NoColumns, "SECOND_MICROSECOND", &duration, None),
            Ok(Datum::Int(3_456_000))
        );
    }
}
