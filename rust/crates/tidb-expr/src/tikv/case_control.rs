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

use std::cell::OnceCell;

use tidb_query_expr::NativeIfBranch;

use super::adapter_failure::{ExpressionAdapterFailure, ScopeFailureKind};
use super::if_control::{decode_head, finish_in};
use super::{
    evaluate_args_in, evaluate_prepared_args_scoped_in, identity_value, EvaluatedArgs,
    EvaluatedBytesOp, EvaluatedBytesResult,
};
use crate::{Columns, Datum, EvalError};

fn contract_error(reason: &'static str) -> EvalError {
    EvalError::ExpressionAdapterFailure(ExpressionAdapterFailure::from_scope(
        ScopeFailureKind::Contract,
        reason,
    ))
}

fn decode_end(computed: EvaluatedBytesResult) -> Result<Datum, EvalError> {
    match computed.into_identity_datum()? {
        value @ Datum::Null => Ok(value),
        _ => Err(contract_error(
            "CASE exhaustion returned a non-NULL identity",
        )),
    }
}

pub(crate) fn eval_case_in<P>(
    ctx: &dyn Columns,
    pair_count: usize,
    has_else: bool,
    initialize: impl FnOnce(&dyn Columns) -> Result<P, EvalError>,
    condition: impl Fn(usize, &P, &dyn Columns) -> Result<Option<bool>, EvalError>,
    value: impl Fn(usize, &dyn Columns) -> Result<Datum, EvalError>,
) -> Result<Datum, EvalError> {
    let state = OnceCell::new();
    evaluate_prepared_args_scoped_in(
        ctx,
        || {
            // Preserve original preparation authority and the no-owner order.
            // Even a zero-pair simple CASE evaluates its real base exactly once.
            state
                .set(initialize(ctx)?)
                .map_err(|_| contract_error("CASE state was initialized twice"))?;
            if pair_count == 0 {
                return if has_else {
                    let value = value(0, ctx)?;
                    Ok((
                        EvaluatedBytesOp::AnyValueNative,
                        EvaluatedArgs::Bytes(identity_value::encode(&value)?),
                    ))
                } else {
                    Ok((EvaluatedBytesOp::CoalesceEndNative, EvaluatedArgs::NoArgs))
                };
            }
            let state = state
                .get()
                .ok_or_else(|| contract_error("CASE state is missing"))?;
            Ok((
                EvaluatedBytesOp::IfHeadNative,
                EvaluatedArgs::Int(condition(0, state, ctx)?.map(i64::from)),
            ))
        },
        |computed, selected| {
            if pair_count == 0 {
                return if has_else {
                    computed.into_identity_datum()
                } else {
                    decode_end(computed)
                };
            }
            let state = state
                .get()
                .ok_or_else(|| contract_error("CASE state is missing"))?;
            let (mut branch, mut report) = decode_head(computed)?;
            let mut index = 0;
            loop {
                match branch {
                    NativeIfBranch::Then => {
                        let value = value(index, selected)?;
                        return finish_in(selected, report, &value);
                    }
                    NativeIfBranch::Else => {}
                }
                index += 1;
                if index == pair_count {
                    if has_else {
                        let value = value(index, selected)?;
                        return finish_in(selected, report, &value);
                    }
                    drop(report);
                    return evaluate_args_in(
                        EvaluatedBytesOp::CoalesceEndNative,
                        selected,
                        || Ok(EvaluatedArgs::NoArgs),
                        decode_end,
                    );
                }
                drop(report);
                // Return each subsequent head immediately, retaining only the
                // outer selected context and guard rather than nested cursors.
                (branch, report) = evaluate_prepared_args_scoped_in(
                    selected,
                    || {
                        Ok((
                            EvaluatedBytesOp::IfHeadNative,
                            EvaluatedArgs::Int(condition(index, state, selected)?.map(i64::from)),
                        ))
                    },
                    |computed, _nested| decode_head(computed),
                )?;
            }
        },
    )
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use tidb_datatype::Decimal;

    use super::*;
    use crate::{ExpressionAdapterFailureClass, ReadyValuePoolOwner, ReadyValuePoolPolicy};

    #[test]
    fn case_bridge_initializes_once_and_iterates_only_demanded_conditions() {
        let null = Datum::Null;
        let raw_float = Datum::Float32(f64::from_bits(0xfff8_0000_0000_5678));
        let raw_decimal = Datum::Decimal(
            Decimal::from_raw_parts(true, vec![0xff, 0], u32::MAX, 0)
                .with_declared_shape(i64::MIN, i64::MAX),
        );
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
                for has_else in [false, true] {
                    let initialized = Cell::new(0);
                    let values = Cell::new(0);
                    let result = eval_case_in(
                        bound,
                        0,
                        has_else,
                        |original| {
                            assert!(std::ptr::eq(original.ready_value_scope().unwrap(), &scope));
                            initialized.set(initialized.get() + 1);
                            Ok(Some(Datum::Int(37)))
                        },
                        |_, _, _| panic!("zero pairs have no condition"),
                        |index, original| {
                            assert!(has_else);
                            assert_eq!(index, 0);
                            assert_eq!(initialized.get(), 1);
                            assert!(std::ptr::eq(original.ready_value_scope().unwrap(), &scope));
                            values.set(values.get() + 1);
                            Ok(raw_float.clone())
                        },
                    );
                    assert_eq!(initialized.get(), 1);
                    assert_eq!(values.get(), usize::from(has_else));
                    if slots == 0 {
                        assert!(
                            matches!(result, Err(EvalError::ExpressionAdapterFailure(failure))
                            if failure.class() == ExpressionAdapterFailureClass::PoolResource)
                        );
                    } else {
                        let expected = if has_else { &raw_float } else { &null };
                        assert_eq!(
                            identity_value::encode(&result.unwrap()).unwrap(),
                            identity_value::encode(expected).unwrap()
                        );
                    }
                }
                let initialized = Cell::new(0);
                let conditions = Cell::new(0);
                let values = Cell::new(0);
                let result = eval_case_in(
                    bound,
                    22,
                    true,
                    |_| {
                        initialized.set(initialized.get() + 1);
                        Ok(Some(Datum::Int(37)))
                    },
                    |index, base, selected| {
                        assert_eq!(initialized.get(), 1);
                        assert_eq!(base, &Some(Datum::Int(37)));
                        assert_eq!(conditions.get(), index);
                        assert!(index <= 20, "selected NULL must stop later conditions");
                        assert!(std::ptr::eq(selected.ready_value_scope().unwrap(), &scope));
                        conditions.set(conditions.get() + 1);
                        Ok(if index == 1 { None } else { Some(index == 20) })
                    },
                    |index, selected| {
                        assert_eq!(index, 20, "only the selected pair may evaluate its value");
                        assert!(std::ptr::eq(selected.ready_value_scope().unwrap(), &scope));
                        assert!(std::ptr::eq(
                            selected.ready_value_execution().unwrap(),
                            bound.ready_value_execution().unwrap()
                        ));
                        values.set(values.get() + 1);
                        evaluate_args_in(
                            EvaluatedBytesOp::AnyValueNative,
                            selected,
                            || Ok(EvaluatedArgs::Bytes(identity_value::encode(&null)?)),
                            EvaluatedBytesResult::into_identity_datum,
                        )
                    },
                );
                assert_eq!(initialized.get(), 1);
                if slots == 0 {
                    assert_eq!((conditions.get(), values.get()), (1, 0));
                    assert!(
                        matches!(result, Err(EvalError::ExpressionAdapterFailure(failure))
                        if failure.class() == ExpressionAdapterFailureClass::PoolResource)
                    );
                } else {
                    assert_eq!((conditions.get(), values.get()), (21, 1));
                    assert_eq!(result, Ok(Datum::Null));
                }
                for has_else in [false, true] {
                    let initialized = Cell::new(0);
                    let conditions = Cell::new(0);
                    let values = Cell::new(0);
                    let result = eval_case_in(
                        bound,
                        3,
                        has_else,
                        |_| {
                            initialized.set(initialized.get() + 1);
                            Ok(())
                        },
                        |index, _, _| {
                            assert_eq!(conditions.get(), index);
                            conditions.set(index + 1);
                            Ok(Some(false))
                        },
                        |index, _| {
                            assert!(has_else);
                            assert_eq!(index, 3);
                            values.set(values.get() + 1);
                            Ok(raw_decimal.clone())
                        },
                    );
                    assert_eq!(initialized.get(), 1);
                    if slots == 0 {
                        assert_eq!((conditions.get(), values.get()), (1, 0));
                        assert!(
                            matches!(result, Err(EvalError::ExpressionAdapterFailure(failure))
                            if failure.class() == ExpressionAdapterFailureClass::PoolResource)
                        );
                    } else {
                        assert_eq!((conditions.get(), values.get()), (3, usize::from(has_else)));
                        let expected = if has_else { &raw_decimal } else { &null };
                        assert_eq!(
                            identity_value::encode(&result.unwrap()).unwrap(),
                            identity_value::encode(expected).unwrap()
                        );
                    }
                }
                let marker = EvalError::Unsupported("sole ELSE error precedes pool admission");
                assert_eq!(
                    eval_case_in(
                        bound,
                        0,
                        true,
                        |_| Ok(()),
                        |_, _, _| panic!("no invented condition"),
                        |_, _| Err(marker.clone())
                    ),
                    Err(marker)
                );
                for has_else in [false, true] {
                    let marker =
                        EvalError::Unsupported("zero-pair simple CASE still evaluates its base");
                    assert_eq!(
                        eval_case_in::<()>(
                            bound,
                            0,
                            has_else,
                            |_| Err(marker.clone()),
                            |_, _, _| panic!("failed base"),
                            |_, _| panic!("failed base")
                        ),
                        Err(marker)
                    );
                }
            });
            drop(scope);
            execution.close();
        }
        let initialized = Cell::new(0);
        assert_eq!(
            eval_case_in(
                &crate::NoColumns,
                2,
                false,
                |original| {
                    assert!(original.ready_value_scope().is_none());
                    initialized.set(initialized.get() + 1);
                    Ok(())
                },
                |index, _, selected| {
                    assert_eq!(selected.ready_value_scope().is_some(), index != 0);
                    Ok(Some(index == 1))
                },
                |index, selected| {
                    assert_eq!(index, 1);
                    assert!(selected.ready_value_scope().is_some());
                    Ok(Datum::Int(7))
                },
            ),
            Ok(Datum::Int(7))
        );
        assert_eq!(initialized.get(), 1);
        assert_eq!(
            eval_case_in(
                &crate::NoColumns,
                0,
                true,
                |original| {
                    assert!(original.ready_value_scope().is_none());
                    Ok(())
                },
                |_, _, _| panic!("no condition for sole ELSE"),
                |index, original| {
                    assert_eq!(index, 0);
                    assert!(original.ready_value_scope().is_none());
                    Ok(Datum::Int(9))
                },
            ),
            Ok(Datum::Int(9))
        );
    }

    #[test]
    fn case_bridge_uses_iterative_demand_beyond_legacy_depth_limit() {
        let conditions = Cell::new(0usize);
        let values = Cell::new(0usize);
        let selected = 1_023usize;
        let result = eval_case_in(
            &crate::NoColumns,
            selected + 1,
            false,
            |_| Ok(()),
            |index, _, _| {
                assert_eq!(conditions.get(), index);
                conditions.set(index + 1);
                Ok(Some(index == selected))
            },
            |index, _| {
                assert_eq!(index, selected);
                values.set(values.get() + 1);
                Ok(Datum::Int(index as i64))
            },
        );
        assert_eq!(result, Ok(Datum::Int(selected as i64)));
        assert_eq!(conditions.get(), selected + 1);
        assert_eq!(values.get(), 1);
    }
}
