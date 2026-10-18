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

use super::control_value::{default_budget, exhausted, owned};
use crate::{Columns, Datum, EvalError};

pub(crate) fn eval_case_in<P>(
    ctx: &dyn Columns,
    pair_count: usize,
    has_else: bool,
    initialize: impl FnOnce(&dyn Columns) -> Result<P, EvalError>,
    condition: impl Fn(usize, &P, &dyn Columns) -> Result<Option<bool>, EvalError>,
    value: impl Fn(usize, &dyn Columns) -> Result<Datum, EvalError>,
) -> Result<Datum, EvalError> {
    // Even an empty simple CASE observes its base exactly once.
    let state = initialize(ctx)?;
    for index in 0..pair_count {
        if tidb_query_expr::native_if_choose_branch(condition(index, &state, ctx)?)
            == tidb_query_expr::NativeIfBranch::Then
        {
            return owned(value(index, ctx)?, default_budget());
        }
    }
    if has_else {
        owned(value(pair_count, ctx)?, default_budget())
    } else {
        exhausted("CASE exhaustion returned a non-NULL identity")
    }
}
