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
use super::{ExpressionRuntimeFailure, LocalError};
use crate::{Columns, Datum, EvalError};

/// Preserve the actual comparison channel, including non-Boolean integers.
/// Each SDK control service owns its source-specific interpretation.
pub(crate) fn in_eq_observation(value: &Datum) -> tidb_query_expr::NativeInControlValue {
    use tidb_query_expr::NativeInControlValue as Value;
    match value {
        Datum::Null => Value::Null,
        Datum::Int(value) => Value::Int(*value),
        _ => Value::Other,
    }
}

pub(crate) fn in_control_datum(value: tidb_query_expr::NativeInControlResult) -> Datum {
    match value {
        tidb_query_expr::NativeInControlResult::Null => Datum::Null,
        tidb_query_expr::NativeInControlResult::Bool(value) => Datum::Int(i64::from(value)),
    }
}

pub(crate) fn in_row_control_error(
    error: tidb_query_expr::NativeInRowError<EvalError>,
) -> EvalError {
    match error {
        tidb_query_expr::NativeInRowError::Child(error) => error,
        tidb_query_expr::NativeInRowError::ItemMismatch => {
            EvalError::Unsupported("row value IN list item arity mismatch")
        }
        tidb_query_expr::NativeInRowError::WidthMismatch => {
            EvalError::Unsupported("row value arity mismatch")
        }
    }
}

/// Only the already-evaluated, already-cast temporal/JSON IN branch enters here.
/// The caller retains its all-evaluation then all-casting barrier.
pub(crate) fn eval_in_typed_values_in(
    _ctx: &dyn Columns,
    domain: tidb_query_expr::NativeInTypedDomain,
    values: &[Datum],
) -> Result<Datum, EvalError> {
    use tidb_query_expr::NativeInTypedValue as Value;
    tidb_query_expr::native_in_typed_values(
        domain,
        values.iter().map(|value| match value {
            Datum::Null => None,
            Datum::Time(value) => Some(Value::Time(value.core_time().raw())),
            Datum::Duration(value) => Some(Value::Duration(value.nanoseconds())),
            Datum::Json(value) => Some(Value::Json {
                type_code: value.type_code(),
                bytes: value.value(),
            }),
            // The old wire policy accepted non-domain identity tags and compared
            // them false. Do not add a source-domain conversion or rejection here.
            _ => Some(Value::Other),
        }),
    )
    .map(|value| value.map_or(Datum::Null, |value| Datum::Int(i64::from(value))))
    .map_err(|_| invalid_report())
}

fn legacy_result<E: From<EvalError>>(
    result: Result<Option<bool>, tidb_query_expr::NativeInLegacyError<E>>,
) -> Result<Option<i128>, E> {
    match result {
        Ok(value) => Ok(value.map(i128::from)),
        Err(tidb_query_expr::NativeInLegacyError::Source(error)) => Err(error),
        Err(tidb_query_expr::NativeInLegacyError::ResourceLimit(reason)) => Err(E::from(
            EvalError::ExpressionRuntimeFailure(ExpressionRuntimeFailure::from_local_eval(
                LocalError::ResourceLimit(reason.into()),
                None,
            )),
        )),
    }
}

/// Legacy integer IN reads the caller's original native integer channel,
/// including operand0 for an empty list. No frame, scoped context or worker is
/// created; the SDK owns full-i128 equality, NULL policy and subsequent demand.
pub fn eval_legacy_in_int_in<E: From<EvalError>>(
    ctx: &dyn Columns,
    count: usize,
    mut eval: impl FnMut(usize, &dyn Columns) -> Result<Option<i128>, E>,
) -> Result<Option<i128>, E> {
    legacy_result(tidb_query_expr::native_in_legacy_i128_values(
        count,
        |index| eval(index, ctx),
    ))
}

/// Legacy byte IN keeps the actual source error and byte owners. Only a real
/// non-NULL pair resolves the original signed collation ID under current mode,
/// after its candidate read; the SDK performs comparison and early matching.
pub fn eval_legacy_in_bytes_in<E: From<EvalError>>(
    ctx: &dyn Columns,
    count: usize,
    collation: i32,
    eval: impl FnMut(usize, &dyn Columns) -> Result<Option<Vec<u8>>, E>,
) -> Result<Option<i128>, E> {
    eval_legacy_in_bytes_bounded_in(
        ctx,
        count,
        collation,
        super::control_value::default_budget(),
        eval,
    )
}
fn eval_legacy_in_bytes_bounded_in<E: From<EvalError>>(
    ctx: &dyn Columns,
    count: usize,
    collation: i32,
    maximum: usize,
    mut eval: impl FnMut(usize, &dyn Columns) -> Result<Option<Vec<u8>>, E>,
) -> Result<Option<i128>, E> {
    legacy_result(tidb_query_expr::native_in_legacy_bytes_values(
        count,
        maximum,
        |index| eval(index, ctx),
        || {
            tidb_datatype::get_collator_by_id(collation)
                .new_collation()
                .map_or(
                    super::NativeCollation::Binary,
                    tidb_datatype::Collation::native_policy,
                )
        },
    ))
}

fn invalid_report() -> EvalError {
    EvalError::ExpressionAdapterFailure(ExpressionAdapterFailure::from_scope(
        ScopeFailureKind::Contract,
        "invalid native IN report",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;
    struct NoWorkers;
    impl Columns for NoWorkers {
        fn get(&self, _: &[String]) -> Option<Datum> {
            panic!("unexpected lookup")
        }
        fn ready_value_cache(&self) -> Option<&super::super::ReadyValueCache> {
            panic!("retired worker lookup")
        }
    }
    #[test]
    fn legacy_byte_actual_capacity_refusal_keeps_resource_class_and_permits_clean_reuse() {
        let ctx = NoWorkers;
        let seen = RefCell::new(Vec::new());
        for at in [0, 1] {
            seen.borrow_mut().clear();
            let actual =
                eval_legacy_in_bytes_bounded_in::<EvalError>(&ctx, 3, 63, 128, |index, _| {
                    seen.borrow_mut().push(index);
                    let mut value = if index == at {
                        Vec::with_capacity(1024)
                    } else {
                        Vec::new()
                    };
                    value.push(b'x');
                    Ok(Some(value))
                });
            let Err(EvalError::ExpressionRuntimeFailure(error)) = actual else {
                panic!("resource refusal converted to a SQL answer")
            };
            assert_eq!(
                error.class(),
                crate::ExpressionRuntimeFailureClass::ResourceLimit
            );
            assert_eq!(error.phase(), None);
            assert_eq!(*seen.borrow(), (0..=at).collect::<Vec<_>>());
            let retry = eval_legacy_in_bytes_bounded_in::<EvalError>(&ctx, 3, 63, 2, |index, _| {
                assert!(index < 2, "matching path demanded a dead source");
                Ok(Some(vec![b'x']))
            });
            assert_eq!(retry, Ok(Some(1)));
        }
        assert!(matches!(
            eval_legacy_in_bytes_bounded_in::<EvalError>(&ctx, 0, 63, usize::MAX, |_, _| panic!(
                "invalid unlimited allowance fetched a source"
            )),
            Err(EvalError::ExpressionRuntimeFailure(_))
        ));
        assert_eq!(
            eval_legacy_in_bytes_bounded_in::<EvalError>(&ctx, 0, 63, 0, |index, _| {
                assert_eq!(index, 0);
                Ok(Some(Vec::new()))
            }),
            Ok(Some(0))
        );
    }
}
