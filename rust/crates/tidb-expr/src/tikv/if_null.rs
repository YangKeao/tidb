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

use std::borrow::Borrow;

use tidb_query_expr::{decode_native_if_null_head_result, NativeIfNullHeadResult};

use super::adapter_failure::{ExpressionAdapterFailure, ScopeFailureKind};
use super::{
    evaluate_args_in, evaluate_prepared_args_scoped_in, identity_value, EvaluatedArgs,
    EvaluatedBytesOp, EvaluatedBytesResult,
};
use crate::{Columns, Datum, EvalError};

pub(crate) enum IfNullHeadOutcome {
    Done(Datum),
    NeedSecond(Vec<u8>),
}

fn invalid_report() -> EvalError {
    EvalError::ExpressionAdapterFailure(ExpressionAdapterFailure::from_scope(
        ScopeFailureKind::Contract,
        "invalid native IFNULL head report",
    ))
}

pub(crate) fn eval_if_null_head_scoped_in<T, V: Borrow<Datum>>(
    ctx: &dyn Columns,
    first: impl FnOnce(&dyn Columns) -> Result<V, EvalError>,
    pack: impl FnOnce(IfNullHeadOutcome, &dyn Columns) -> Result<T, EvalError>,
) -> Result<T, EvalError> {
    evaluate_prepared_args_scoped_in(
        ctx,
        || {
            // Preserve original preparation authority and precedence. With no
            // capability this child still precedes the parent's one-shot owner.
            let first = first(ctx)?;
            Ok((
                EvaluatedBytesOp::IfNullHeadNative,
                EvaluatedArgs::Bytes(identity_value::encode(first.borrow())?),
            ))
        },
        |computed, selected| {
            let mut report = computed.into_bytes()?.ok_or_else(invalid_report)?;
            let outcome =
                match decode_native_if_null_head_result(&report).ok_or_else(invalid_report)? {
                    NativeIfNullHeadResult::NeedSecond => IfNullHeadOutcome::NeedSecond(report),
                    NativeIfNullHeadResult::Done(_) => {
                        // The SDK validated this action header and the complete
                        // computed identity frame; never reconstruct from `first`.
                        report.remove(0);
                        IfNullHeadOutcome::Done(identity_value::decode(Some(report))?)
                    }
                };
            pack(outcome, selected)
        },
    )
}

pub(crate) fn eval_if_null_finish_in(
    ctx: &dyn Columns,
    original_report: Vec<u8>,
    second: &Datum,
) -> Result<Datum, EvalError> {
    evaluate_args_in(
        EvaluatedBytesOp::IfNullFinishNative,
        ctx,
        || {
            if !matches!(
                decode_native_if_null_head_result(&original_report),
                Some(NativeIfNullHeadResult::NeedSecond)
            ) {
                return Err(invalid_report());
            }
            Ok(EvaluatedArgs::Bytes2(
                Some(original_report),
                identity_value::encode(second)?,
            ))
        },
        EvaluatedBytesResult::into_identity_datum,
    )
}

pub(crate) fn eval_if_null_in(
    ctx: &dyn Columns,
    first: impl FnOnce(&dyn Columns) -> Result<Datum, EvalError>,
    second: impl FnOnce(&dyn Columns) -> Result<Datum, EvalError>,
) -> Result<Datum, EvalError> {
    eval_if_null_head_scoped_in(ctx, first, |outcome, selected| match outcome {
        IfNullHeadOutcome::Done(value) => Ok(value),
        IfNullHeadOutcome::NeedSecond(report) => {
            let second = second(selected)?;
            eval_if_null_finish_in(selected, report, &second)
        }
    })
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
    use crate::{ExpressionAdapterFailureClass, ReadyValuePoolOwner, ReadyValuePoolPolicy};

    #[test]
    fn if_null_bridge_keeps_raw_identity_lazy_children_and_stage_authority() {
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
            let policy = ReadyValuePoolPolicy::checked(
                slots,
                slots,
                16 << 20,
                1 << 20,
                2 << 20,
                64,
                16,
                1 << 20,
            )
            .unwrap();
            let owner = ReadyValuePoolOwner::new(policy).unwrap();
            let execution = owner.begin_execution().unwrap();
            let scope = execution.scope();
            scope.with_columns(&crate::NoColumns, |bound| {
                for value in &values {
                    let first_calls = Cell::new(0);
                    let second_calls = Cell::new(0);
                    let result = eval_if_null_in(
                        bound,
                        |original| {
                            assert!(std::ptr::eq(original.ready_value_scope().unwrap(), &scope));
                            first_calls.set(first_calls.get() + 1);
                            Ok(value.clone())
                        },
                        |selected| {
                            assert!(
                                matches!(value, Datum::Null),
                                "a present first value must not demand its second child"
                            );
                            assert!(std::ptr::eq(selected.ready_value_scope().unwrap(), &scope));
                            second_calls.set(second_calls.get() + 1);
                            Ok(Datum::Null)
                        },
                    );
                    assert_eq!(first_calls.get(), 1);
                    if slots == 0 {
                        assert_eq!(second_calls.get(), 0);
                        assert!(
                            matches!(result, Err(EvalError::ExpressionAdapterFailure(failure))
                            if failure.class() == ExpressionAdapterFailureClass::PoolResource)
                        );
                    } else {
                        assert_eq!(
                            second_calls.get(),
                            usize::from(matches!(value, Datum::Null))
                        );
                        assert_eq!(
                            identity_value::encode(&result.unwrap()).unwrap(),
                            identity_value::encode(value).unwrap()
                        );
                        let second_calls = Cell::new(0);
                        let result = eval_if_null_in(
                            bound,
                            |_| Ok(Datum::Null),
                            |selected| {
                                second_calls.set(second_calls.get() + 1);
                                assert!(std::ptr::eq(
                                    selected.ready_value_scope().unwrap(),
                                    &scope
                                ));
                                // Intervening actual C4 work replaces the parked head
                                // worker without changing authority or its report.
                                evaluate_args_in(
                                    EvaluatedBytesOp::AnyValueNative,
                                    selected,
                                    || Ok(EvaluatedArgs::Bytes(identity_value::encode(value)?)),
                                    EvaluatedBytesResult::into_identity_datum,
                                )
                            },
                        )
                        .unwrap();
                        assert_eq!(second_calls.get(), 1);
                        assert_eq!(
                            identity_value::encode(&result).unwrap(),
                            identity_value::encode(value).unwrap()
                        );
                    }
                }
                let frontend = EvalError::Unsupported("IFNULL first-child failure");
                assert_eq!(
                    eval_if_null_in(
                        bound,
                        |_| Err(frontend.clone()),
                        |_| panic!("dead second child")
                    ),
                    Err(frontend)
                );
                for report in [vec![], vec![1], vec![0, 0], vec![2]] {
                    assert!(
                        matches!(eval_if_null_finish_in(bound, report, &Datum::Null),
                        Err(EvalError::ExpressionAdapterFailure(failure))
                            if failure.class() == ExpressionAdapterFailureClass::ScopeContract)
                    );
                }
            });
            drop(scope);
            if slots == 1 {
                for panic_first in [true, false] {
                    let scope = execution.scope();
                    scope.with_columns(&crate::NoColumns, |bound| {
                        let panic = catch_unwind(AssertUnwindSafe(|| eval_if_null_in(bound,
                            |_| { if panic_first { panic!("first child under existing scope guard"); } Ok(Datum::Null) },
                            |_| panic!("second child under head guard"),
                        )));
                        assert!(panic.is_err());
                        assert!(matches!(eval_if_null_in(bound, |_| Ok(Datum::Null), |_| panic!("poisoned head cannot demand second")),
                            Err(EvalError::ExpressionAdapterFailure(failure))
                                if failure.class() == ExpressionAdapterFailureClass::ScopePoisoned));
                    });
                    drop(scope);
                }
            }
            execution.close();
        }
        // The no-capability exception remains prepare-before-parent-owner;
        // only the computed head's continuation receives the selected scope.
        assert_eq!(
            eval_if_null_in(
                &crate::NoColumns,
                |original| {
                    assert!(original.ready_value_scope().is_none());
                    Ok(Datum::Null)
                },
                |selected| {
                    assert!(selected.ready_value_scope().is_some());
                    Ok(Datum::Int(7))
                },
            ),
            Ok(Datum::Int(7))
        );
    }
}
