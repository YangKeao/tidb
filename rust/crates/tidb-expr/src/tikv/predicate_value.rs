// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Actual normalized predicates, without an argument program or source demand.
use super::adapter_failure::{ExpressionAdapterFailure, ScopeFailureKind};
use crate::{BooleanFunction as B, Datum, EvalError, LogicalArgs, LogicalFunction as L};
use tidb_query_expr::local::{LocalError, LocalResult};

pub(crate) fn boolean_value(function: B, ready: Option<bool>) -> LocalResult<Option<i64>> {
    use tidb_query_expr::impl_op::{
        int_is_false, int_is_true, is_null, unary_not_int, KeepNullOff, KeepNullOn,
    };
    let value = ready.map(i64::from);
    match function {
        B::UnaryNot => unary_not_int(value.as_ref()),
        B::IsNull => is_null::<i64>(value.as_ref()),
        B::IsTrue => int_is_true::<KeepNullOff>(value.as_ref()),
        B::IsFalse => int_is_false::<KeepNullOff>(value.as_ref()),
        B::IsTrueWithNull => int_is_true::<KeepNullOn>(value.as_ref()),
        B::IsNotNull => is_null::<i64>(value.as_ref()).and_then(|v| unary_not_int(v.as_ref())),
        B::IsNotTrue => {
            int_is_true::<KeepNullOff>(value.as_ref()).and_then(|v| unary_not_int(v.as_ref()))
        }
        B::IsNotFalse => {
            int_is_false::<KeepNullOff>(value.as_ref()).and_then(|v| unary_not_int(v.as_ref()))
        }
    }
    .map_err(LocalError::Evaluation)
}
/// Both supplied markers are values/representatives already selected by the
/// caller's original demand policy. This function never requests another child.
pub(crate) fn logical_value(
    function: L,
    left: Option<bool>,
    right: Option<bool>,
) -> LocalResult<Option<i64>> {
    let left = left.map(i64::from);
    let right = right.map(i64::from);
    match function {
        L::And => tidb_query_expr::impl_op::logical_and(left.as_ref(), right.as_ref()),
        L::Or => tidb_query_expr::impl_op::logical_or(left.as_ref(), right.as_ref()),
        L::Xor => tidb_query_expr::impl_op::logical_xor(left.as_ref(), right.as_ref()),
    }
    .map_err(LocalError::Evaluation)
}
fn publish(value: LocalResult<Option<i64>>) -> Result<Datum, EvalError> {
    value
        .map(|v| v.map_or(Datum::Null, Datum::Int))
        .map_err(|error| {
            EvalError::ExpressionRuntimeFailure(super::ExpressionRuntimeFailure::from_local_eval(
                error, None,
            ))
        })
}
pub(crate) fn evaluate_boolean_in(function: B, ready: Option<bool>) -> Result<Datum, EvalError> {
    publish(boolean_value(function, ready))
}
pub(crate) fn evaluate_logical_in(function: L, arguments: LogicalArgs) -> Result<Datum, EvalError> {
    let (left, right) = match arguments {
        LogicalArgs::Both(left, right) => (left, right),
        LogicalArgs::UndemandedRight { left } => {
            if !matches!(
                (function, left),
                (L::And, Some(false)) | (L::Or, Some(true))
            ) {
                return Err(EvalError::ExpressionAdapterFailure(
                    ExpressionAdapterFailure::from_scope(
                        ScopeFailureKind::Contract,
                        "invalid undemanded logical right argument",
                    ),
                ));
            }
            // Only those absorbing values permit an irrelevant representative;
            // it is neither a demanded right input nor a fabricated SQL NULL.
            (left, Some(false))
        }
    };
    publish(logical_value(function, left, right))
}
