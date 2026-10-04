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
use super::{
    evaluate_args_in, identity_value, EvaluatedArgs, EvaluatedBytesOp, EvaluatedBytesResult,
};
use crate::{Columns, Datum, EvalError};

fn invalid_comparison() -> EvalError {
    EvalError::ExpressionAdapterFailure(ExpressionAdapterFailure::from_scope(
        ScopeFailureKind::Contract,
        "NULLIF comparison did not return NULL or a signed boolean",
    ))
}

pub(crate) fn eval_null_if_in(
    ctx: &dyn Columns,
    first: &Datum,
    comparison: impl FnOnce(&dyn Columns) -> Result<Datum, EvalError>,
) -> Result<Datum, EvalError> {
    evaluate_args_in(
        EvaluatedBytesOp::NullIfNative,
        ctx,
        || {
            // Complete the real comparison first, under existing preparation
            // authority. With no capability this still precedes the one-shot
            // owner; do not claim that it already has the selector's scope.
            let comparison = match comparison(ctx)? {
                Datum::Null => None,
                Datum::Int(0) => Some(0),
                Datum::Int(1) => Some(1),
                _ => return Err(invalid_comparison()),
            };
            // Even an equal result submits the actual left identity. Selection
            // and the returned value belong exclusively to the worker.
            Ok(EvaluatedArgs::BytesInt(
                identity_value::encode(first)?,
                comparison,
            ))
        },
        EvaluatedBytesResult::into_identity_datum,
    )
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use tidb_datatype::{BinaryJSON, CoreTime, Decimal, Time, TimeType};

    use super::*;
    use crate::{
        AsciiPoolOwner, AsciiPoolPolicy, ExpressionAdapterFailureClass,
        ExpressionRuntimeFailureClass,
    };

    #[test]
    fn null_if_bridge_compares_before_selector_and_keeps_actual_left_identity() {
        let values = [
            Datum::Null,
            Datum::UInt(u64::MAX),
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
            Datum::Raw(vec![0, 0xff]),
        ];
        let comparisons = [Datum::Null, Datum::Int(0), Datum::Int(1)];
        for slots in [0, 1] {
            let policy =
                AsciiPoolPolicy::checked(slots, slots, 16 << 20, 1 << 20, 2 << 20, 64, 16, 1 << 16)
                    .unwrap();
            let owner = AsciiPoolOwner::new(policy).unwrap();
            let execution = owner.begin_execution().unwrap();
            let scope = execution.scope();
            scope.with_columns(&crate::NoColumns, |bound| {
                for first in &values {
                    for comparison in &comparisons {
                        let calls = Cell::new(0);
                        // This direct completed comparison deliberately needs
                        // no worker: a zero-slot error must be the selector's.
                        let result = eval_null_if_in(bound, first, |original| {
                            assert!(std::ptr::eq(
                                original.evaluated_ascii_scope().unwrap(),
                                &scope
                            ));
                            calls.set(calls.get() + 1);
                            Ok(comparison.clone())
                        });
                        assert_eq!(calls.get(), 1);
                        if slots == 0 {
                            assert!(
                                matches!(result, Err(EvalError::ExpressionAdapterFailure(failure))
                                if failure.class() == ExpressionAdapterFailureClass::PoolResource)
                            );
                        } else {
                            let expected = if matches!(comparison, Datum::Int(1)) {
                                None
                            } else {
                                identity_value::encode(first).unwrap()
                            };
                            assert_eq!(identity_value::encode(&result.unwrap()).unwrap(), expected);
                        }
                    }
                }
                let marker = EvalError::Unsupported(
                    "NULLIF comparison error precedes left preparation and admission",
                );
                let calls = Cell::new(0);
                assert_eq!(
                    eval_null_if_in(bound, &values[3], |_| {
                        calls.set(calls.get() + 1);
                        Err(marker.clone())
                    }),
                    Err(marker)
                );
                assert_eq!(calls.get(), 1);
                for invalid in [
                    Datum::Int(2),
                    Datum::Int(-1),
                    Datum::UInt(1),
                    Datum::Real(1.0),
                ] {
                    assert!(
                        matches!(eval_null_if_in(bound, &values[3], |_| Ok(invalid)),
                        Err(EvalError::ExpressionAdapterFailure(failure))
                            if failure.class() == ExpressionAdapterFailureClass::ScopeContract)
                    );
                }
                if slots == 1 {
                    for comparison in &comparisons {
                        let calls = Cell::new(0);
                        let result = eval_null_if_in(bound, &values[2], |original| {
                            calls.set(calls.get() + 1);
                            assert!(std::ptr::eq(
                                original.evaluated_ascii_scope().unwrap(),
                                &scope
                            ));
                            assert!(std::ptr::eq(
                                original.evaluated_ascii_execution().unwrap(),
                                bound.evaluated_ascii_execution().unwrap()
                            ));
                            evaluate_args_in(
                                EvaluatedBytesOp::AnyValueNative,
                                original,
                                || Ok(EvaluatedArgs::Bytes(identity_value::encode(comparison)?)),
                                EvaluatedBytesResult::into_identity_datum,
                            )
                        })
                        .unwrap();
                        assert_eq!(calls.get(), 1);
                        let expected = if matches!(comparison, Datum::Int(1)) {
                            None
                        } else {
                            identity_value::encode(&values[2]).unwrap()
                        };
                        assert_eq!(identity_value::encode(&result).unwrap(), expected);
                    }
                    // Equality must not replace the actual large left input by
                    // fake NULL. The real input still faces the call budget.
                    assert_eq!(
                        eval_null_if_in(bound, &Datum::Null, |_| Ok(Datum::Int(1))),
                        Ok(Datum::Null)
                    );
                    let large = Datum::Bytes(vec![0x41; 1 << 17]);
                    assert!(
                        matches!(eval_null_if_in(bound, &large, |_| Ok(Datum::Int(1))),
                        Err(EvalError::ExpressionRuntimeFailure(failure))
                            if failure.class() == ExpressionRuntimeFailureClass::ResourceLimit)
                    );
                }
            });
            drop(scope);
            execution.close();
        }
        assert_eq!(
            eval_null_if_in(&crate::NoColumns, &Datum::UInt(u64::MAX), |original| {
                assert!(original.evaluated_ascii_scope().is_none());
                Ok(Datum::Int(0))
            }),
            Ok(Datum::UInt(u64::MAX))
        );
    }
}
