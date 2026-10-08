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

use tidb_datatype::FieldTypeCode;
use tidb_query_expr::{decode_native_str_to_date_result, NativeStrToDateResult};

use super::adapter_failure::{ExpressionAdapterFailure, ScopeFailureKind};
use super::{
    evaluate_args_in, evaluate_prepared_args_scoped_in, EvaluatedArgs, EvaluatedBytesOp,
    EvaluatedBytesResult,
};
use crate::{Columns, Datum, EvalError};

fn invalid_report() -> EvalError {
    EvalError::ExpressionAdapterFailure(ExpressionAdapterFailure::from_scope(
        ScopeFailureKind::Contract,
        "invalid native STR_TO_DATE report",
    ))
}

enum TerminalStage {
    Head,
    DateFinish,
    TypedFinish,
}

fn terminal_report(
    report: Option<Vec<u8>>,
    ctx: &dyn Columns,
    stage: TerminalStage,
) -> Result<Datum, EvalError> {
    let Some(report) = report else {
        return if matches!(stage, TerminalStage::DateFinish) {
            Err(invalid_report())
        } else {
            Ok(Datum::Null)
        };
    };
    match decode_native_str_to_date_result(&report).ok_or_else(invalid_report)? {
        NativeStrToDateResult::Value(text) => Ok(Datum::new_string(text)),
        NativeStrToDateResult::Warning {
            code: code @ (1292 | 1411),
            message,
        } if !matches!(stage, TerminalStage::TypedFinish) => {
            ctx.append_warning(code, message);
            Ok(Datum::Null)
        }
        // A continuation's output cannot request another stage or invent a
        // warning kind. The SDK owns warning text and all value classification.
        _ => Err(invalid_report()),
    }
}

pub(crate) fn eval_str_to_date_in(
    ctx: &dyn Columns,
    vals: &[Datum],
    result_type: Option<FieldTypeCode>,
) -> Result<Datum, EvalError> {
    let null_path = Cell::new(false);
    evaluate_prepared_args_scoped_in(
        ctx,
        || {
            let [input, format] = vals else {
                return Err(EvalError::Unsupported("bad function arity"));
            };
            // The caller already evaluated both children. An actual NULL
            // suppresses coercion of BOTH ready operands, not child evaluation.
            if input.is_null() || format.is_null() {
                null_path.set(true);
                return Ok((
                    EvaluatedBytesOp::DateDiffNullNative,
                    EvaluatedArgs::NullWitness(None),
                ));
            }
            let input = crate::coerce::coerce_str(input)?;
            let format = crate::coerce::coerce_str(format)?;
            Ok((
                EvaluatedBytesOp::StrToDateHeadNative,
                EvaluatedArgs::BytesBytesInt(
                    input.map(String::into_bytes),
                    format.map(String::into_bytes),
                    // Preserve enum identity: Unknown(12) is NOT Datetime,
                    // even though both expose mysql_type() == 12.
                    result_type.map(|code| match code {
                        FieldTypeCode::Unknown(raw) => -1 - i64::from(raw),
                        known => i64::from(known.mysql_type()),
                    }),
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
            let report = computed.into_bytes()?;
            let Some(report) = report else {
                return terminal_report(None, ctx, TerminalStage::Head);
            };
            match decode_native_str_to_date_result(&report).ok_or_else(invalid_report)? {
                NativeStrToDateResult::NeedDateModes(_) => evaluate_args_in(
                    EvaluatedBytesOp::StrToDateFinishNative,
                    selected,
                    || {
                        // Preserve the original statement override and its late
                        // demand, including month-zero results that will warn.
                        let modes = ctx.date_modes();
                        Ok(EvaluatedArgs::BytesIntInt(
                            Some(report),
                            Some(i64::from(modes.no_zero_date)),
                            Some(i64::from(modes.allow_invalid_dates)),
                        ))
                    },
                    |computed| {
                        terminal_report(computed.into_bytes()?, ctx, TerminalStage::DateFinish)
                    },
                ),
                NativeStrToDateResult::NeedTypedDateMode(_) => evaluate_args_in(
                    EvaluatedBytesOp::StrToDateTypedFinishNative,
                    selected,
                    || {
                        let modes = ctx.date_modes();
                        Ok(EvaluatedArgs::BytesInt(
                            Some(report),
                            Some(i64::from(modes.no_zero_date)),
                        ))
                    },
                    |computed| {
                        terminal_report(computed.into_bytes()?, ctx, TerminalStage::TypedFinish)
                    },
                ),
                _ => terminal_report(Some(report), ctx, TerminalStage::Head),
            }
        },
    )
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::panic::{catch_unwind, AssertUnwindSafe};

    use tidb_datatype::{DateModes, SessionTimeZone};

    use super::super::LocalError;
    use super::*;
    use crate::{ExpressionAdapterFailureClass, ReadyValuePoolOwner, ReadyValuePoolPolicy};

    #[test]
    fn str_to_date_bridge_preserves_late_modes_warnings_and_scoped_continuations() {
        struct Statement {
            zero: Cell<bool>,
            modes: Cell<usize>,
            events: RefCell<Vec<&'static str>>,
            warnings: RefCell<Vec<(u16, String)>>,
            panic_modes: Cell<bool>,
            panic_warning: Cell<bool>,
        }
        impl Columns for Statement {
            fn get(&self, _: &[String]) -> Option<Datum> {
                panic!("ready arguments")
            }
            fn time_zone(&self) -> SessionTimeZone {
                panic!("generic CAST remains outside this bridge")
            }
            fn handle_truncate(&self, _: &str) -> Result<(), EvalError> {
                panic!("STR_TO_DATE warnings must not be upgraded by truncation policy")
            }
            fn date_modes(&self) -> DateModes {
                self.modes.set(self.modes.get() + 1);
                self.events.borrow_mut().push("modes");
                assert!(!self.panic_modes.get(), "mode panic");
                DateModes {
                    no_zero_date: self.zero.get(),
                    no_zero_in_date: true,
                    allow_invalid_dates: false,
                }
            }
            fn append_warning(&self, code: u16, message: &str) {
                self.events.borrow_mut().push("warning");
                assert!(!self.panic_warning.get(), "warning panic");
                self.warnings.borrow_mut().push((code, message.to_owned()));
            }
        }
        let statement = Statement {
            zero: Cell::new(false),
            modes: Cell::new(0),
            events: RefCell::new(vec![]),
            warnings: RefCell::new(vec![]),
            panic_modes: Cell::new(false),
            panic_warning: Cell::new(false),
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
        let args =
            |input: &str, format: &str| [Datum::new_string(input), Datum::new_string(format)];
        for slots in [0, 1] {
            let owner = owner(slots);
            let execution = owner.begin_execution().unwrap();
            let scope = execution.scope();
            scope.with_columns(&statement, |bound| {
                for (values, metadata, zero, expected, modes, warning) in [
                    (
                        [Datum::Null, Datum::MaxValue],
                        None,
                        false,
                        Datum::Null,
                        0,
                        None,
                    ),
                    (
                        [Datum::new_bytes([255]), Datum::Null],
                        None,
                        false,
                        Datum::Null,
                        0,
                        None,
                    ),
                    (
                        args("a", "%d"),
                        None,
                        false,
                        Datum::Null,
                        0,
                        Some((1292, "Incorrect datetime value: '0000-00-00 00:00:00'")),
                    ),
                    (
                        args("01", "%d"),
                        None,
                        false,
                        Datum::Null,
                        1,
                        Some((
                            1411,
                            "Incorrect datetime value: '01' for function str_to_date",
                        )),
                    ),
                    (
                        args("2020-02-29", "%Y-%m-%d"),
                        None,
                        false,
                        Datum::new_string("2020-02-29"),
                        1,
                        None,
                    ),
                    (
                        args("12:34:56", "%T"),
                        None,
                        true,
                        Datum::new_string("12:34:56"),
                        0,
                        None,
                    ),
                    (
                        args("12:34:56", "%T"),
                        Some(FieldTypeCode::Unknown(12)),
                        true,
                        Datum::new_string("12:34:56"),
                        0,
                        None,
                    ),
                    (
                        args("12:34:56", "%T"),
                        Some(FieldTypeCode::Datetime),
                        false,
                        Datum::new_string("0000-00-00 12:34:56"),
                        1,
                        None,
                    ),
                    (
                        args("12:34:56", "%T"),
                        Some(FieldTypeCode::Datetime),
                        true,
                        Datum::Null,
                        1,
                        None,
                    ),
                ] {
                    statement.zero.set(zero);
                    statement.modes.set(0);
                    statement.events.borrow_mut().clear();
                    statement.warnings.borrow_mut().clear();
                    let actual = eval_str_to_date_in(bound, &values, metadata);
                    if slots == 0 {
                        assert!(matches!(actual,
                            Err(EvalError::ExpressionAdapterFailure(failure))
                                if failure.class() == ExpressionAdapterFailureClass::PoolResource));
                        assert_eq!(statement.modes.get(), 0);
                        assert!(statement.warnings.borrow().is_empty());
                    } else {
                        assert_eq!(actual, Ok(expected));
                        assert_eq!(statement.modes.get(), modes);
                        let expected_warnings: Vec<_> = warning
                            .into_iter()
                            .map(|(code, message)| (code, message.to_owned()))
                            .collect();
                        assert_eq!(*statement.warnings.borrow(), expected_warnings);
                        if modes == 1 && warning.is_some() {
                            assert_eq!(*statement.events.borrow(), ["modes", "warning"]);
                        }
                    }
                }
                assert_eq!(
                    eval_str_to_date_in(bound, &[Datum::Null], None),
                    Err(EvalError::Unsupported("bad function arity"))
                );
                assert_eq!(
                    eval_str_to_date_in(bound, &[Datum::new_bytes([255]), Datum::MaxValue], None),
                    Err(EvalError::Unsupported("invalid UTF-8 byte datum"))
                );
                if slots == 1 {
                    let mut spare = Vec::with_capacity(128 << 10);
                    spare.push(b'1');
                    let refused = evaluate_args_in(
                        EvaluatedBytesOp::StrToDateHeadNative,
                        bound,
                        || {
                            Ok(EvaluatedArgs::BytesBytesInt(
                                Some(spare),
                                Some(b"%T".to_vec()),
                                None,
                            ))
                        },
                        EvaluatedBytesResult::into_bytes,
                    );
                    assert!(
                        matches!(refused, Err(EvalError::ExpressionRuntimeFailure(failure))
                        if matches!(failure.local_error(), LocalError::ResourceLimit(_)))
                    );
                    assert_eq!(
                        eval_str_to_date_in(bound, &args("12:34:56", "%T"), None),
                        Ok(Datum::new_string("12:34:56"))
                    );
                }
            });
            drop(scope);
            execution.close();
        }
        for frame in [
            vec![],
            vec![255],
            vec![0, 255],
            vec![3],
            b"\x0412:34:56".to_vec(),
        ] {
            assert!(
                matches!(terminal_report(Some(frame), &statement, TerminalStage::DateFinish),
                Err(EvalError::ExpressionAdapterFailure(failure))
                    if failure.class() == ExpressionAdapterFailureClass::ScopeContract)
            );
        }
        assert!(terminal_report(
            Some(b"\x01Incorrect datetime value: '0000-00-00 00:00:00'".to_vec()),
            &statement,
            TerminalStage::TypedFinish
        )
        .is_err());
        assert!(terminal_report(None, &statement, TerminalStage::DateFinish).is_err());
        assert_eq!(
            terminal_report(None, &statement, TerminalStage::TypedFinish),
            Ok(Datum::Null)
        );
        for mode_panic in [true, false] {
            let owner = owner(1);
            let execution = owner.begin_execution().unwrap();
            let scope = execution.scope();
            statement.panic_modes.set(mode_panic);
            statement.panic_warning.set(!mode_panic);
            assert!(catch_unwind(AssertUnwindSafe(|| scope.with_columns(
                &statement,
                |bound| {
                    let values = if mode_panic {
                        args("01", "%d")
                    } else {
                        args("a", "%d")
                    };
                    let _ = eval_str_to_date_in(bound, &values, None);
                }
            )))
            .is_err());
            statement.panic_modes.set(false);
            statement.panic_warning.set(false);
            scope.with_columns(&statement, |bound| {
                assert!(
                    matches!(eval_str_to_date_in(bound, &args("12:34:56", "%T"), None),
                    Err(EvalError::ExpressionAdapterFailure(failure))
                        if failure.class() == ExpressionAdapterFailureClass::ScopePoisoned)
                );
            });
            drop(scope);
            execution.close();
        }
        assert_eq!(
            eval_str_to_date_in(&crate::NoColumns, &args("12:34:56", "%T"), None),
            Ok(Datum::new_string("12:34:56"))
        );
    }
}
