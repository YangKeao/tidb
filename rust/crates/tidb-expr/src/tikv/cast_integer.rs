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

use crate::{Columns, Datum, EvalError};
use tidb_datatype::{EvalType, FieldType, SessionTimeZone};
use tidb_query_expr::{NativeCastIntegerResult as Report, NativeCastIntegerTarget as Target};

#[cfg(test)]
fn input(value: &Datum) -> tidb_query_expr::NativeCastIntegerInput<'_> {
    tidb_query_expr::native_cast_integer_input_from_numeric(value.as_shared_numeric_input())
}

fn invalid_result() -> EvalError {
    use super::adapter_failure::{ExpressionAdapterFailure, ScopeFailureKind};
    EvalError::ExpressionAdapterFailure(ExpressionAdapterFailure::from_scope(
        ScopeFailureKind::Contract,
        "invalid native integer cast result domain",
    ))
}

fn evaluate(
    ctx: &dyn Columns,
    value: &Datum,
    target: Target,
    source: Option<tidb_query_expr::NativeIntervalEvalType>,
) -> Result<Report, EvalError> {
    tidb_query_expr::native_cast_integer_numeric(
        value.as_shared_numeric_input(),
        target,
        source,
        || ctx.time_zone(),
        // This is the existing real/Float32 worker and its original authority.
        // Unlike datatype errors, its infrastructure errors must not be folded.
        || super::eval_cast_real_unsigned_in(ctx, value),
        |message| ctx.handle_truncate(message),
        |code, message| ctx.append_warning(code, message),
    )
}

pub(crate) fn eval_cast_arg_as_int_in(
    ctx: &dyn Columns,
    value: &Datum,
    source: Option<&FieldType>,
) -> Result<Datum, EvalError> {
    use tidb_query_expr::{NativeArgIntegerError, NativeArgIntegerResult};
    tidb_query_expr::native_cast_arg_as_int(
        value.as_shared_numeric_input(),
        source.map(FieldType::is_unsigned),
        || ctx.time_zone(),
        || super::eval_cast_real_unsigned_in(ctx, value),
        |message| ctx.handle_truncate(message),
        |code, message| ctx.append_warning(code, message),
    )
    .map(|result| match result {
        NativeArgIntegerResult::Original => value.clone(),
        NativeArgIntegerResult::Signed(value) => Datum::Int(value),
        NativeArgIntegerResult::Unsigned(value) => Datum::UInt(value),
    })
    .map_err(|error| match error {
        NativeArgIntegerError::Unsupported(message) => EvalError::Unsupported(message),
        NativeArgIntegerError::Effect(error) => error,
    })
}

pub(crate) fn eval_cast_signed_value_in(value: &Datum, zone: &SessionTimeZone) -> i64 {
    tidb_query_expr::native_cast_integer_signed_numeric(value.as_shared_numeric_input(), zone)
}

pub(crate) fn eval_cast_signed_in(ctx: &dyn Columns, value: &Datum) -> Result<i64, EvalError> {
    match evaluate(ctx, value, Target::Signed, None)? {
        Report::Signed(value) => Ok(value),
        _ => Err(invalid_result()),
    }
}

pub(crate) fn eval_cast_unsigned_in(ctx: &dyn Columns, value: &Datum) -> Result<u64, EvalError> {
    match evaluate(ctx, value, Target::Unsigned, None)? {
        Report::Unsigned(value) => Ok(value),
        _ => Err(invalid_result()),
    }
}

pub(crate) fn eval_cast_unsigned_union_in(
    ctx: &dyn Columns,
    value: &Datum,
    source: Option<&FieldType>,
) -> Result<u64, EvalError> {
    match evaluate(ctx, value, Target::UnsignedInUnion, source_type(source))? {
        Report::Unsigned(value) => Ok(value),
        _ => Err(invalid_result()),
    }
}

pub(crate) fn report_cast_integer_input_in(
    ctx: &dyn Columns,
    value: &Datum,
) -> Result<(), EvalError> {
    tidb_query_expr::native_cast_integer_numeric_input_warning(
        value.as_shared_numeric_input(),
        |message| ctx.handle_truncate(message),
    )
}

// Retain the original private test entry's value-only contract. It must not
// acquire the ordinary unsigned entry's input truncation or 8031 diagnostic.
#[cfg(test)]
pub(crate) fn eval_cast_unsigned_value_in(
    ctx: &dyn Columns,
    value: &Datum,
) -> Result<u64, EvalError> {
    tidb_query_expr::native_cast_integer_unsigned_numeric(
        value.as_shared_numeric_input(),
        || ctx.time_zone(),
        || super::eval_cast_real_unsigned_in(ctx, value),
        |code, message| ctx.append_warning(code, message),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AsciiPoolOwner, AsciiPoolPolicy, ExpressionAdapterFailureClass};
    use std::cell::{Cell, RefCell};
    use tidb_datatype::{BinaryJSON, CoreTime, FieldTypeCode, Time, TimeType};

    #[test]
    fn integer_cast_bridge_keeps_warning_zone_demands_source_channels_and_real_worker_errors() {
        #[derive(Default)]
        struct Original {
            events: RefCell<Vec<String>>,
            fail_truncate: Cell<bool>,
        }
        impl Columns for Original {
            fn get(&self, _: &[String]) -> Option<Datum> {
                panic!("actual operand already evaluated")
            }
            fn handle_truncate(&self, _: &str) -> Result<(), EvalError> {
                self.events.borrow_mut().push("truncate".into());
                if self.fail_truncate.get() {
                    Err(EvalError::Unsupported("integer truncate veto"))
                } else {
                    Ok(())
                }
            }
            fn append_warning(&self, code: u16, _: &str) {
                self.events.borrow_mut().push(format!("append:{code}"));
            }
            fn time_zone(&self) -> SessionTimeZone {
                self.events.borrow_mut().push("zone".into());
                SessionTimeZone::Named(chrono_tz::America::Los_Angeles)
            }
        }
        let utc = SessionTimeZone::utc();
        let float32 = Datum::Float32(2.5);
        let calls = Cell::new(0);
        assert_eq!(
            tidb_query_expr::native_cast_integer_signed_value(input(&float32), &utc, |zone| {
                calls.set(calls.get() + 1);
                float32
                    .to_i64_in(zone)
                    .map(|converted| (converted.value, converted.event))
            }),
            2
        );
        assert_eq!(calls.get(), 1);
        assert_eq!(
            tidb_query_expr::native_cast_integer_signed_value(
                input(&Datum::Real(2.5)),
                &utc,
                |_| -> Result<(i64, ()), ()> {
                    panic!("ordinary Real does not call the Float32 service")
                }
            ),
            2
        );
        assert!(
            std::panic::catch_unwind(|| eval_cast_signed_value_in(&Datum::Null, &utc)).is_err()
        );
        let temporal = Datum::Time(Time::from_raw_parts(
            CoreTime::from_date(2011, 3, 13, 1, 59, 59, 999999),
            TimeType::DateTime,
            6,
        ));
        assert_eq!(crate::cast::to_i64_signed(&temporal), 20110313020000);
        assert_eq!(
            eval_cast_signed_value_in(
                &temporal,
                &SessionTimeZone::Named(chrono_tz::America::Los_Angeles)
            ),
            20110313030000
        );
        for slots in [0, 1] {
            let owner = AsciiPoolOwner::new(
                AsciiPoolPolicy::checked(slots, slots, 16 << 20, 1 << 20, 2 << 20, 64, 16, 1 << 16)
                    .unwrap(),
            )
            .unwrap();
            let execution = owner.begin_execution().unwrap();
            let scope = execution.scope();
            let original = Original::default();
            scope.with_columns(&original, |bound| {
                original.fail_truncate.set(true);
                assert_eq!(
                    eval_cast_signed_in(bound, &Datum::new_bytes(b"18446744073709551616".to_vec())),
                    Err(EvalError::Unsupported("integer truncate veto"))
                );
                assert_eq!(original.events.take(), vec!["truncate"]);
                original.fail_truncate.set(false);
                assert_eq!(
                    eval_cast_signed_in(bound, &Datum::new_bytes(b"18446744073709551615".to_vec())),
                    Ok(-1)
                );
                assert_eq!(original.events.take(), vec!["append:8030", "zone"]);
                assert_eq!(eval_cast_signed_in(bound, &Datum::UInt(1)), Ok(1));
                assert_eq!(original.events.take(), vec!["zone"]);
                assert_eq!(eval_cast_signed_in(bound, &temporal), Ok(20110313030000));
                assert_eq!(original.events.take(), vec!["zone"]);
                assert_eq!(
                    eval_cast_unsigned_in(bound, &Datum::UInt(u64::MAX)),
                    Ok(u64::MAX)
                );
                assert!(original.events.take().is_empty());
                assert_eq!(eval_cast_unsigned_in(bound, &Datum::Int(-1)), Ok(u64::MAX));
                assert_eq!(original.events.take(), vec!["zone"]);
                assert_eq!(
                    eval_cast_unsigned_value_in(bound, &Datum::new_bytes(b"-2x".to_vec())),
                    Ok(u64::MAX - 1)
                );
                assert_eq!(original.events.take(), vec!["zone"]);
                assert_eq!(
                    eval_cast_unsigned_in(bound, &Datum::new_bytes(b"-2".to_vec())),
                    Ok(u64::MAX - 1)
                );
                assert_eq!(original.events.take(), vec!["append:8031", "zone"]);
                let raw = Datum::Raw(vec![1]);
                assert!(raw.to_i64_in(&utc).is_err());
                assert!(raw.to_decimal().is_err());
                assert_eq!(eval_cast_signed_in(bound, &raw), Ok(0));
                assert_eq!(original.events.take(), vec!["zone"]);
                assert_eq!(eval_cast_unsigned_in(bound, &raw), Ok(0));
                assert!(original.events.take().is_empty());
                for document in ["{}", "[]"] {
                    let value = Datum::Json(BinaryJSON::parse(document).unwrap());
                    original.fail_truncate.set(true);
                    assert_eq!(
                        eval_cast_signed_in(bound, &value),
                        Err(EvalError::Unsupported("integer truncate veto"))
                    );
                    assert_eq!(original.events.take(), vec!["truncate"]);
                    original.fail_truncate.set(false);
                }
                let scalar_json = Datum::Json(BinaryJSON::parse("true").unwrap());
                assert_eq!(eval_cast_unsigned_in(bound, &scalar_json), Ok(1));
                assert!(original.events.take().is_empty());
                assert_eq!(report_cast_integer_input_in(bound, &scalar_json), Ok(()));
                assert!(original.events.take().is_empty());
                assert_eq!(
                    crate::cast::eval_cast(
                        &tidb_ast::CastType::Signed,
                        Datum::MaxValue,
                        None,
                        bound
                    ),
                    Err(EvalError::Unsupported("range sentinel cast operand"))
                );
                assert!(original.events.take().is_empty());
                assert_eq!(
                    eval_cast_unsigned_union_in(bound, &Datum::Real(-1.5), None),
                    Ok(0)
                );
                assert!(original.events.take().is_empty());
                // The actual declared temporal family does not take the
                // UNION numeric-negative gate, despite the datum being Real.
                let source = FieldType::new(FieldTypeCode::Datetime);
                for value in [Datum::Real(-1.5), Datum::Float32(-1.5)] {
                    let actual = eval_cast_unsigned_union_in(bound, &value, Some(&source));
                    if slots == 0 {
                        assert!(
                            matches!(actual, Err(EvalError::ExpressionAdapterFailure(failure))
                            if failure.class() == ExpressionAdapterFailureClass::PoolResource)
                        );
                        assert!(original.events.take().is_empty());
                    } else {
                        assert_eq!(actual, Ok(u64::MAX - 1));
                        assert_eq!(original.events.take(), vec!["append:1690"]);
                    }
                }
                // A worker refusal must not poison or replace the original
                // pure-value source path, which still has no admission gate.
                assert_eq!(eval_cast_unsigned_in(bound, &Datum::UInt(7)), Ok(7));
                assert!(original.events.take().is_empty());
            });
            drop(scope);
            execution.close();
        }
    }
}

fn source_type(source: Option<&FieldType>) -> Option<tidb_query_expr::NativeIntervalEvalType> {
    use tidb_query_expr::NativeIntervalEvalType as Type;
    source.map(|source| match source.eval_type() {
        EvalType::Int => Type::Int,
        EvalType::Real => Type::Real,
        EvalType::Decimal => Type::Decimal,
        EvalType::String => Type::String,
        EvalType::Datetime => Type::Datetime,
        EvalType::Timestamp => Type::Timestamp,
        EvalType::Duration => Type::Duration,
        EvalType::Json => Type::Json,
        EvalType::VectorFloat32 => Type::VectorFloat32,
    })
}
