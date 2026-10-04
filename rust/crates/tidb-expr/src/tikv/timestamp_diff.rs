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

use tidb_datatype::CoreTime;

use super::{
    evaluate_args_in, evaluate_prepared_args_in, native_time_result_contract_error, EvaluatedArgs,
    EvaluatedBytesOp, EvaluatedBytesResult,
};
use crate::{Columns, Datum, EvalError};

pub(crate) fn eval_timestamp_diff_in(
    ctx: &dyn Columns,
    vals: &[Datum],
) -> Result<Datum, EvalError> {
    evaluate_args_in(
        EvaluatedBytesOp::TimestampDiffTextNative,
        ctx,
        || {
            let [unit, left, right] = vals else {
                return Err(EvalError::Unsupported("bad function arity"));
            };
            // Keep all three coercions in their original order even when an
            // earlier operand is NULL; a later coercion can still fail.
            let (unit, left, right) = (
                crate::coerce::coerce_str(unit)?,
                crate::coerce::coerce_str(left)?,
                crate::coerce::coerce_str(right)?,
            );
            Ok(EvaluatedArgs::Bytes3([
                unit.map(String::into_bytes),
                left.map(String::into_bytes),
                right.map(String::into_bytes),
            ]))
        },
        EvaluatedBytesResult::into_int_datum,
    )
}

/// Only for an actually observed early NULL in the PB argument prefix.
pub(crate) fn eval_timestamp_diff_null_in(ctx: &dyn Columns) -> Result<Datum, EvalError> {
    evaluate_args_in(
        EvaluatedBytesOp::DateDiffNullNative,
        ctx,
        || Ok(EvaluatedArgs::NullWitness(None)),
        |computed| match computed.into_int_datum()? {
            value @ Datum::Null => Ok(value),
            _ => Err(native_time_result_contract_error()),
        },
    )
}

/// Actual legacy TIMESTAMPDIFF inputs, without text-rendering typed time cores.
pub enum LegacyTimestampDiffArgs {
    /// A genuinely NULL unit; Some is an invalid witness, never a value.
    NullWitness(Option<()>),
    /// The demanded raw unit and nullable calendar cores, retaining all bits.
    Values {
        unit: Vec<u8>,
        left: Option<CoreTime>,
        right: Option<CoreTime>,
    },
}

/// Evaluates the legacy raw-core signature using the caller's current scope.
pub fn eval_legacy_timestamp_diff_in(
    args: LegacyTimestampDiffArgs,
    ctx: &dyn Columns,
) -> Result<Option<i64>, EvalError> {
    evaluate_prepared_args_in(
        ctx,
        || match args {
            LegacyTimestampDiffArgs::NullWitness(value) => {
                if value.is_some() {
                    return Err(native_time_result_contract_error());
                }
                Ok((
                    EvaluatedBytesOp::DateDiffNullNative,
                    EvaluatedArgs::NullWitness(None),
                ))
            }
            LegacyTimestampDiffArgs::Values { unit, left, right } => Ok((
                EvaluatedBytesOp::TimestampDiffCoreNative,
                EvaluatedArgs::Bytes3([
                    Some(unit),
                    left.map(|value| value.raw().to_le_bytes().to_vec()),
                    right.map(|value| value.raw().to_le_bytes().to_vec()),
                ]),
            )),
        },
        |computed| match computed.into_int_datum()? {
            Datum::Int(value) => Ok(Some(value)),
            Datum::Null => Ok(None),
            _ => Err(native_time_result_contract_error()),
        },
    )
}

#[cfg(test)]
mod tests {
    use tidb_ast::CiString;
    use tidb_datatype::{FieldType, FieldTypeCode, SessionTimeZone, Time, TimeType};
    use tidb_proto::tipb::ScalarFuncSig;

    use super::*;
    use crate::constant::Constant;
    use crate::expression::Expression;
    use crate::scalar_function::{PbBuiltin, ScalarFunction};
    use crate::{AsciiPoolOwner, AsciiPoolPolicy, ExpressionAdapterFailureClass};

    #[test]
    fn timestamp_diff_bridge_keeps_text_coercion_raw_cores_and_pb_null_demand() {
        struct NoTemporalGetters;
        impl Columns for NoTemporalGetters {
            fn get(&self, _: &[String]) -> Option<Datum> {
                panic!("TIMESTAMPDIFF must not fetch a column")
            }
            fn time_zone(&self) -> SessionTimeZone {
                panic!("TIMESTAMPDIFF must not fetch a timezone")
            }
            fn now(&self) -> Option<(i64, u32, i32)> {
                panic!("TIMESTAMPDIFF must not fetch a clock")
            }
            fn append_warning(&self, _: u16, _: &str) {
                panic!("TIMESTAMPDIFF must not invent a warning")
            }
        }
        let text = |value: &str| Datum::new_string(value);
        let first = CoreTime::from_date(2000, 1, 1, 0, 0, 0, 123_400);
        let second = CoreTime::from_date(2000, 1, 1, 0, 0, 0, 123_900);
        let first_time = Time::from_raw_parts(first, TimeType::DateTime, 2);
        let second_time = Time::from_raw_parts(second, TimeType::DateTime, 2);
        let month_zero = CoreTime::from_date(2000, 0, 1, 0, 0, 0, 0);
        let raw_args = |unit: &[u8], left, right| LegacyTimestampDiffArgs::Values {
            unit: unit.to_vec(),
            left,
            right,
        };
        let constant = |value| {
            Expression::Constant(Constant::new(
                value,
                FieldType::new(FieldTypeCode::VarString),
            ))
        };
        let unreachable = || {
            Expression::ScalarFunction(ScalarFunction::new(
                CiString::new("not_a_function"),
                FieldType::new(FieldTypeCode::LongLong),
                vec![],
            ))
        };
        let pb = |args| {
            ScalarFunction::from_pb(
                PbBuiltin::new(ScalarFuncSig::TimestampDiff).unwrap(),
                FieldType::new(FieldTypeCode::LongLong),
                args,
            )
        };
        for slots in [0, 1] {
            let policy =
                AsciiPoolPolicy::checked(slots, slots, 16 << 20, 1 << 20, 2 << 20, 64, 16, 1 << 16)
                    .unwrap();
            let owner = AsciiPoolOwner::new(policy).unwrap();
            let execution = owner.begin_execution().unwrap();
            let scope = execution.scope();
            scope.with_columns(&NoTemporalGetters, |bound| {
                for (arguments, expected) in [
                    (
                        [text("day"), text("2000-01-01"), text("2000-01-02")],
                        Datum::Int(1),
                    ),
                    (
                        [
                            text("MICROSECOND"),
                            Datum::Time(first_time),
                            Datum::Time(second_time),
                        ],
                        Datum::Int(0),
                    ),
                    (
                        [text("DAY"), text("not a date"), text("2000-01-02")],
                        Datum::Null,
                    ),
                    ([Datum::Null, text("not a date"), Datum::Null], Datum::Null),
                ] {
                    let result = eval_timestamp_diff_in(bound, &arguments);
                    if slots == 0 {
                        assert!(matches!(result,
                            Err(EvalError::ExpressionAdapterFailure(failure))
                                if failure.class() == ExpressionAdapterFailureClass::PoolResource));
                    } else {
                        assert_eq!(result, Ok(expected));
                    }
                }
                for (arguments, message) in [
                    (
                        [Datum::Null, Datum::new_bytes(vec![0xff]), Datum::Null],
                        "invalid UTF-8 byte datum",
                    ),
                    (
                        [text("DAY"), Datum::Null, Datum::MaxValue],
                        "range sentinel string coercion",
                    ),
                ] {
                    assert_eq!(
                        eval_timestamp_diff_in(bound, &arguments),
                        Err(EvalError::Unsupported(message))
                    );
                }
                assert_eq!(
                    eval_timestamp_diff_in(bound, &[Datum::Null]),
                    Err(EvalError::Unsupported("bad function arity"))
                );
                for (unit, left, right, expected) in [
                    (&b"MICROSECOND"[..], Some(first), Some(second), Some(500)),
                    (&b"microsecond"[..], Some(first), Some(second), Some(0)),
                    (&b"\xff"[..], Some(first), Some(second), Some(0)),
                    (&b"SECOND"[..], Some(month_zero), Some(month_zero), Some(0)),
                    (
                        &b"SECOND"[..],
                        Some(CoreTime::from_raw(15)),
                        Some(CoreTime::from_raw(1)),
                        Some(0),
                    ),
                    (
                        &b"SECOND"[..],
                        Some(CoreTime::from_raw(0)),
                        Some(second),
                        None,
                    ),
                    (&b"SECOND"[..], None, Some(second), None),
                ] {
                    let result = eval_legacy_timestamp_diff_in(raw_args(unit, left, right), bound);
                    if slots == 0 {
                        assert!(matches!(result,
                            Err(EvalError::ExpressionAdapterFailure(failure))
                                if failure.class() == ExpressionAdapterFailureClass::PoolResource));
                    } else {
                        assert_eq!(result, Ok(expected));
                    }
                }
                let null = eval_legacy_timestamp_diff_in(
                    LegacyTimestampDiffArgs::NullWitness(None),
                    bound,
                );
                if slots == 0 {
                    assert!(matches!(null,
                        Err(EvalError::ExpressionAdapterFailure(failure))
                            if failure.class() == ExpressionAdapterFailureClass::PoolResource));
                } else {
                    assert_eq!(null, Ok(None));
                }
                assert!(matches!(eval_legacy_timestamp_diff_in(
                    LegacyTimestampDiffArgs::NullWitness(Some(())), bound),
                    Err(EvalError::ExpressionAdapterFailure(failure))
                        if failure.class() == ExpressionAdapterFailureClass::ScopeContract));
                // PB collects only its actual non-NULL prefix; neither an
                // absent suffix nor excess unreachable args trigger arity/coercion.
                for args in [
                    vec![constant(Datum::Null)],
                    vec![
                        constant(Datum::Null),
                        unreachable(),
                        unreachable(),
                        unreachable(),
                    ],
                    vec![
                        constant(Datum::new_bytes(vec![0xff])),
                        constant(Datum::Null),
                        unreachable(),
                    ],
                ] {
                    let result = pb(args).eval(bound, tidb_chunk::row::Row::empty());
                    if slots == 0 {
                        assert!(matches!(result,
                            Err(EvalError::ExpressionAdapterFailure(failure))
                                if failure.class() == ExpressionAdapterFailureClass::PoolResource));
                    } else {
                        assert_eq!(result, Ok(Datum::Null));
                    }
                }
                let result = pb(vec![
                    constant(text("day")),
                    constant(text("2000-01-01")),
                    constant(text("2000-01-02")),
                ])
                .eval(bound, tidb_chunk::row::Row::empty());
                if slots == 0 {
                    assert!(matches!(result,
                        Err(EvalError::ExpressionAdapterFailure(failure))
                            if failure.class() == ExpressionAdapterFailureClass::PoolResource));
                } else {
                    assert_eq!(result, Ok(Datum::Int(1)));
                }
            });
            drop(scope);
            execution.close();
        }
        for (operation, args) in [
            (
                EvaluatedBytesOp::TimestampDiffTextNative,
                EvaluatedArgs::NullWitness(None),
            ),
            (
                EvaluatedBytesOp::TimestampDiffCoreNative,
                EvaluatedArgs::Bytes3([None, None, None]),
            ),
            (
                EvaluatedBytesOp::TimestampDiffCoreNative,
                EvaluatedArgs::Bytes3([Some(b"DAY".to_vec()), Some(vec![0; 7]), None]),
            ),
        ] {
            assert!(evaluate_args_in(
                operation,
                &NoTemporalGetters,
                || Ok(args),
                EvaluatedBytesResult::into_int_datum
            )
            .is_err());
        }
        assert_eq!(
            eval_timestamp_diff_null_in(&NoTemporalGetters),
            Ok(Datum::Null)
        );
        assert_eq!(
            eval_timestamp_diff_in(
                &crate::NoColumns,
                &[text("DAY"), text("2000-01-01"), text("2000-01-02")]
            ),
            Ok(Datum::Int(1))
        );
    }
}
