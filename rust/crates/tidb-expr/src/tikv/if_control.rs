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

use tidb_query_expr::{decode_native_if_head_result, NativeIfBranch};

use super::adapter_failure::{ExpressionAdapterFailure, ScopeFailureKind};
use super::{
    evaluate_args_in, evaluate_prepared_args_scoped_in, identity_value, EvaluatedArgs,
    EvaluatedBytesOp, EvaluatedBytesResult,
};
use crate::{Columns, Datum, EvalError};

fn invalid_report() -> EvalError {
    EvalError::ExpressionAdapterFailure(ExpressionAdapterFailure::from_scope(
        ScopeFailureKind::Contract,
        "invalid native IF head report",
    ))
}

pub(crate) fn eval_if_in(
    ctx: &dyn Columns,
    condition: impl FnOnce(&dyn Columns) -> Result<Option<bool>, EvalError>,
    when_true: impl FnOnce(&dyn Columns) -> Result<Datum, EvalError>,
    when_false: impl FnOnce(&dyn Columns) -> Result<Datum, EvalError>,
) -> Result<Datum, EvalError> {
    evaluate_prepared_args_scoped_in(
        ctx,
        || {
            // Original preparation authority is intentional: with no capability,
            // the condition still runs before the parent's one-shot allocation.
            let condition = condition(ctx)?.map(i64::from);
            Ok((
                EvaluatedBytesOp::IfHeadNative,
                EvaluatedArgs::Int(condition),
            ))
        },
        |computed, selected| {
            let report = computed.into_bytes()?.ok_or_else(invalid_report)?;
            let value = match decode_native_if_head_result(&report).ok_or_else(invalid_report)? {
                NativeIfBranch::Then => when_true(selected)?,
                NativeIfBranch::Else => when_false(selected)?,
            };
            evaluate_args_in(
                EvaluatedBytesOp::IfFinishNative,
                selected,
                || {
                    Ok(EvaluatedArgs::Bytes2(
                        Some(report),
                        identity_value::encode(&value)?,
                    ))
                },
                EvaluatedBytesResult::into_identity_datum,
            )
        },
    )
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::panic::{catch_unwind, AssertUnwindSafe};

    use tidb_datatype::{
        BinaryJSON, BinaryLiteral, Collation, CoreTime, Decimal, MySqlDuration, MysqlEnum,
        MysqlSet, StringDatum, Time, TimeType, VectorFloat32,
    };

    use super::*;
    use crate::{AsciiPoolOwner, AsciiPoolPolicy, ExpressionAdapterFailureClass};

    #[test]
    fn if_bridge_keeps_nullable_choice_raw_identity_and_selected_scope() {
        let mut vector = VectorFloat32::init(tidb_datatype::MAX_VECTOR_DIMENSION + 1);
        vector.elements_mut()[0] = f32::from_bits(0x7fc0_1234);
        vector.elements_mut()[1] = -0.0;
        let values = vec![
            Datum::Null,
            Datum::MinNotNull,
            Datum::MaxValue,
            Datum::Int(i64::MIN),
            Datum::UInt(u64::MAX),
            Datum::Decimal(
                Decimal::from_raw_parts(true, b"0000123456".to_vec(), 2, 9)
                    .with_declared_shape(i64::MIN, i64::MAX),
            ),
            Datum::Decimal(Decimal::from_raw_parts(true, vec![0xff, 0], u32::MAX, 0)),
            Datum::Real(f64::from_bits(0x7ff8_0000_0000_1234)),
            Datum::Real(-0.0),
            Datum::Float32(f64::from_bits(0x3ff0_0000_0000_0001)),
            Datum::Float32(f64::from_bits(0xfff8_0000_0000_5678)),
            Datum::String(StringDatum::new(
                vec![0xff, 0, b'a'],
                Collation::Utf8Mb4GeneralCi,
            )),
            Datum::Bytes(vec![0xff, 0]),
            Datum::BinaryLiteral(BinaryLiteral::from(vec![0, 0xff])),
            Datum::Bit(BinaryLiteral::from(vec![0, 0, 0x80])),
            Datum::Duration(MySqlDuration::from_raw_parts(i64::MIN, i64::MAX)),
            Datum::Enum(
                MysqlEnum::new(vec![0xff, 0], u64::MAX),
                Collation::Utf8Mb4Bin,
            ),
            Datum::Set(MysqlSet::new(vec![0xfe, b','], u64::MAX), Collation::Binary),
            Datum::Time(Time::from_raw_parts(
                CoreTime::from_raw(u64::MAX),
                TimeType::Timestamp,
                255,
            )),
            Datum::Json(BinaryJSON::from_encoded_parts(0xff, vec![0, 0xfe])),
            Datum::Json(BinaryJSON::parse("null").unwrap()),
            Datum::Raw(vec![0xff, 0, 0x80]),
            Datum::VectorFloat32(vector),
        ];
        for slots in [0, 1] {
            let policy =
                AsciiPoolPolicy::checked(slots, slots, 16 << 20, 1 << 20, 2 << 20, 64, 16, 1 << 20)
                    .unwrap();
            let owner = AsciiPoolOwner::new(policy).unwrap();
            let execution = owner.begin_execution().unwrap();
            let scope = execution.scope();
            scope.with_columns(&crate::NoColumns, |bound| {
                for truth in [None, Some(false), Some(true)] {
                    for value in &values {
                        let conditions = Cell::new(0);
                        let thens = Cell::new(0);
                        let elses = Cell::new(0);
                        let selected_value = |selected: &dyn Columns| {
                            assert!(std::ptr::eq(
                                selected.evaluated_ascii_scope().unwrap(),
                                &scope
                            ));
                            assert!(std::ptr::eq(
                                selected.evaluated_ascii_execution().unwrap(),
                                bound.evaluated_ascii_execution().unwrap()
                            ));
                            // The selected branch performs real C4 work between
                            // head and finish while the same one-slot scope lives.
                            evaluate_args_in(
                                EvaluatedBytesOp::AnyValueNative,
                                selected,
                                || Ok(EvaluatedArgs::Bytes(identity_value::encode(value)?)),
                                EvaluatedBytesResult::into_identity_datum,
                            )
                        };
                        let result = eval_if_in(
                            bound,
                            |original| {
                                assert!(std::ptr::eq(
                                    original.evaluated_ascii_scope().unwrap(),
                                    &scope
                                ));
                                conditions.set(conditions.get() + 1);
                                Ok(truth)
                            },
                            |selected| {
                                assert_eq!(
                                    truth,
                                    Some(true),
                                    "a dead then branch must not be evaluated"
                                );
                                thens.set(thens.get() + 1);
                                selected_value(selected)
                            },
                            |selected| {
                                assert_ne!(
                                    truth,
                                    Some(true),
                                    "a dead else branch must not be evaluated"
                                );
                                elses.set(elses.get() + 1);
                                selected_value(selected)
                            },
                        );
                        assert_eq!(conditions.get(), 1);
                        if slots == 0 {
                            assert_eq!((thens.get(), elses.get()), (0, 0));
                            assert!(
                                matches!(result, Err(EvalError::ExpressionAdapterFailure(failure))
                                if failure.class() == ExpressionAdapterFailureClass::PoolResource)
                            );
                        } else {
                            assert_eq!(
                                (thens.get(), elses.get()),
                                if truth == Some(true) { (1, 0) } else { (0, 1) }
                            );
                            assert_eq!(
                                identity_value::encode(&result.unwrap()).unwrap(),
                                identity_value::encode(value).unwrap()
                            );
                        }
                    }
                }
                let frontend =
                    EvalError::Unsupported("IF condition failure precedes head admission");
                assert_eq!(
                    eval_if_in(
                        bound,
                        |_| Err(frontend.clone()),
                        |_| panic!("dead then"),
                        |_| panic!("dead else")
                    ),
                    Err(frontend)
                );
                if slots == 1 {
                    for truth in [false, true] {
                        let frontend = EvalError::Unsupported(
                            "IF chosen branch failure remains a frontend error",
                        );
                        assert_eq!(
                            eval_if_in(
                                bound,
                                |_| Ok(Some(truth)),
                                |_| {
                                    assert!(truth);
                                    Err(frontend.clone())
                                },
                                |_| {
                                    assert!(!truth);
                                    Err(frontend.clone())
                                },
                            ),
                            Err(frontend)
                        );
                    }
                }
            });
            drop(scope);
            if slots == 1 {
                for truth in [false, true] {
                    let scope = execution.scope();
                    scope.with_columns(&crate::NoColumns, |bound| {
                        let panic = catch_unwind(AssertUnwindSafe(|| eval_if_in(bound,
                            |_| Ok(Some(truth)),
                            |_| { assert!(truth); panic!("chosen then under head guard") },
                            |_| { assert!(!truth); panic!("chosen else under head guard") },
                        )));
                        assert!(panic.is_err());
                        assert!(matches!(eval_if_in(bound, |_| Ok(None), |_| panic!("poisoned head"), |_| panic!("poisoned head")),
                            Err(EvalError::ExpressionAdapterFailure(failure))
                                if failure.class() == ExpressionAdapterFailureClass::ScopePoisoned));
                    });
                    drop(scope);
                }
            }
            execution.close();
        }
        // Keep the no-capability exception exact: no parent scope yet during
        // condition preparation; the chosen branch receives the selected one.
        assert_eq!(
            eval_if_in(
                &crate::NoColumns,
                |original| {
                    assert!(original.evaluated_ascii_scope().is_none());
                    Ok(None)
                },
                |_| panic!("NULL must not choose then"),
                |selected| {
                    assert!(selected.evaluated_ascii_scope().is_some());
                    Ok(Datum::Int(7))
                },
            ),
            Ok(Datum::Int(7))
        );
    }
}
