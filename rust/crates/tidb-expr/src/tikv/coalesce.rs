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

use super::adapter_failure::{ExpressionAdapterFailure, ScopeFailureKind};
use super::if_null::{eval_if_null_head_scoped_in, IfNullHeadOutcome};
use super::{evaluate_args_in, EvaluatedArgs, EvaluatedBytesOp};
use crate::{Columns, Datum, EvalError};

fn end_in(ctx: &dyn Columns) -> Result<Datum, EvalError> {
    evaluate_args_in(
        EvaluatedBytesOp::CoalesceEndNative,
        ctx,
        || Ok(EvaluatedArgs::NoArgs),
        |computed| match computed.into_identity_datum()? {
            value @ Datum::Null => Ok(value),
            _ => Err(EvalError::ExpressionAdapterFailure(
                ExpressionAdapterFailure::from_scope(
                    ScopeFailureKind::Contract,
                    "COALESCE end returned a non-NULL identity",
                ),
            )),
        },
    )
}

pub(crate) fn eval_coalesce_in<V: Borrow<Datum>>(
    ctx: &dyn Columns,
    arity: usize,
    argument: impl Fn(usize, &dyn Columns) -> Result<V, EvalError>,
) -> Result<Datum, EvalError> {
    if arity == 0 {
        return end_in(ctx);
    }
    eval_if_null_head_scoped_in(
        ctx,
        |original| argument(0, original),
        |mut outcome, selected| {
            let mut next = 1;
            loop {
                match outcome {
                    IfNullHeadOutcome::Done(value) => return Ok(value),
                    IfNullHeadOutcome::NeedSecond(report) => drop(report),
                }
                if next == arity {
                    return end_in(selected);
                }
                // Each nested head returns its outcome immediately. Keep only the
                // first head's outer guard, never a growing continuation stack or
                // a chain of newly bound contexts across iterations.
                outcome = eval_if_null_head_scoped_in(
                    selected,
                    |original| argument(next, original),
                    |outcome, _nested| Ok(outcome),
                )?;
                next += 1;
            }
        },
    )
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use tidb_datatype::{BinaryJSON, CoreTime, Decimal, Time, TimeType};

    use super::super::{identity_value, EvaluatedBytesResult};
    use super::*;
    use crate::{AsciiPoolOwner, AsciiPoolPolicy, ExpressionAdapterFailureClass};

    #[test]
    fn coalesce_bridge_iterates_borrowed_and_owned_candidates_under_one_scope() {
        // No Clone implementation: the borrowed front does not require a clone
        // of the caller's carrier to run the shared identity head.
        struct BorrowOnly<'a>(&'a Datum);
        impl Borrow<Datum> for BorrowOnly<'_> {
            fn borrow(&self) -> &Datum {
                self.0
            }
        }
        let raw_values = [
            Datum::Float32(f64::from_bits(0xfff8_0000_0000_5678)),
            Datum::Decimal(
                Decimal::from_raw_parts(true, vec![0xff, 0], u32::MAX, 0)
                    .with_declared_shape(i64::MIN, i64::MAX),
            ),
            Datum::Time(Time::from_raw_parts(
                CoreTime::from_raw(u64::MAX),
                TimeType::Timestamp,
                255,
            )),
            Datum::Json(BinaryJSON::from_encoded_parts(0xff, vec![0, 0xfe])),
        ];
        let null = Datum::Null;
        for slots in [0, 1] {
            let policy =
                AsciiPoolPolicy::checked(slots, slots, 16 << 20, 1 << 20, 2 << 20, 64, 16, 1 << 20)
                    .unwrap();
            let owner = AsciiPoolOwner::new(policy).unwrap();
            let execution = owner.begin_execution().unwrap();
            let scope = execution.scope();
            scope.with_columns(&crate::NoColumns, |bound| {
                for arity in [0, 1, 130] {
                    let calls = Cell::new(0);
                    let result = eval_coalesce_in(bound, arity, |index, selected| {
                        assert!(index < arity);
                        assert!(std::ptr::eq(
                            selected.evaluated_ascii_scope().unwrap(),
                            &scope
                        ));
                        calls.set(calls.get() + 1);
                        Ok(BorrowOnly(&null))
                    });
                    if slots == 0 {
                        assert_eq!(calls.get(), usize::from(arity != 0));
                        assert!(
                            matches!(result, Err(EvalError::ExpressionAdapterFailure(failure))
                            if failure.class() == ExpressionAdapterFailureClass::PoolResource)
                        );
                    } else {
                        assert_eq!(calls.get(), arity);
                        assert_eq!(result, Ok(Datum::Null));
                    }
                }
                for raw in &raw_values {
                    let calls = Cell::new(0);
                    let result = eval_coalesce_in(bound, 131, |index, selected| {
                        assert!(index <= 129, "a selected value must leave the suffix dead");
                        assert!(std::ptr::eq(
                            selected.evaluated_ascii_scope().unwrap(),
                            &scope
                        ));
                        assert_eq!(calls.get(), index);
                        calls.set(calls.get() + 1);
                        Ok(BorrowOnly(if index == 129 { raw } else { &null }))
                    });
                    if slots == 0 {
                        assert_eq!(calls.get(), 1);
                        assert!(
                            matches!(result, Err(EvalError::ExpressionAdapterFailure(failure))
                            if failure.class() == ExpressionAdapterFailureClass::PoolResource)
                        );
                    } else {
                        assert_eq!(calls.get(), 130);
                        assert_eq!(
                            identity_value::encode(&result.unwrap()).unwrap(),
                            identity_value::encode(raw).unwrap()
                        );
                        let calls = Cell::new(0);
                        let result = eval_coalesce_in(bound, 131, |index, selected| {
                            assert!(
                                index <= 129,
                                "the owned front must not demand the dead suffix either"
                            );
                            assert_eq!(calls.get(), index);
                            calls.set(calls.get() + 1);
                            assert!(std::ptr::eq(
                                selected.evaluated_ascii_scope().unwrap(),
                                &scope
                            ));
                            assert!(std::ptr::eq(
                                selected.evaluated_ascii_execution().unwrap(),
                                bound.evaluated_ascii_execution().unwrap()
                            ));
                            if index < 129 {
                                return Ok(Datum::Null);
                            }
                            // Real late-child C4 work replaces the parked head;
                            // its computed raw value still goes through a head.
                            evaluate_args_in(
                                EvaluatedBytesOp::AnyValueNative,
                                selected,
                                || Ok(EvaluatedArgs::Bytes(identity_value::encode(raw)?)),
                                EvaluatedBytesResult::into_identity_datum,
                            )
                        })
                        .unwrap();
                        assert_eq!(calls.get(), 130);
                        assert_eq!(
                            identity_value::encode(&result).unwrap(),
                            identity_value::encode(raw).unwrap()
                        );
                    }
                }
                let marker =
                    EvalError::Unsupported("COALESCE actual child error precedes head admission");
                let result = eval_coalesce_in::<Datum>(bound, 1, |_, _| Err(marker.clone()));
                assert_eq!(result, Err(marker));
            });
            drop(scope);
            execution.close();
        }
        let calls = Cell::new(0);
        assert_eq!(
            eval_coalesce_in(&crate::NoColumns, 66, |index, selected| {
                calls.set(calls.get() + 1);
                assert_eq!(selected.evaluated_ascii_scope().is_some(), index != 0);
                Ok(if index == 65 {
                    Datum::Int(7)
                } else {
                    Datum::Null
                })
            }),
            Ok(Datum::Int(7))
        );
        assert_eq!(calls.get(), 66);
    }
}
