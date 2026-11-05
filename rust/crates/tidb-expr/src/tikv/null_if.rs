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
use super::control_value::copy;
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
    // The actual comparison remains eager, even when the first value is NULL.
    let comparison = match comparison(ctx)? {
        Datum::Null => None,
        Datum::Int(0) => Some(false),
        Datum::Int(1) => Some(true),
        _ => return Err(invalid_comparison()),
    };
    match tidb_query_expr::native_if_choose_branch(comparison) {
        tidb_query_expr::NativeIfBranch::Then => Ok(Datum::Null),
        tidb_query_expr::NativeIfBranch::Else => copy(first),
    }
}
