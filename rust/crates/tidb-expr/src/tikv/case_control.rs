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

use crate::{Columns, Datum, EvalError};

/// The original CASE pair cursor, shared by callback and value continuations.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CaseDemand {
    Condition(usize),
    Value(usize),
    Exhausted,
}
pub(crate) fn case_start(pair_count: usize, has_else: bool) -> CaseDemand {
    if pair_count != 0 {
        CaseDemand::Condition(0)
    } else if has_else {
        CaseDemand::Value(0)
    } else {
        CaseDemand::Exhausted
    }
}
pub(crate) fn case_after_truth(
    pair_count: usize,
    has_else: bool,
    index: usize,
    truth: Option<bool>,
) -> CaseDemand {
    debug_assert!(index < pair_count);
    if tidb_query_expr::native_if_choose_branch(truth) == tidb_query_expr::NativeIfBranch::Then {
        CaseDemand::Value(index)
    } else if index + 1 < pair_count {
        CaseDemand::Condition(index + 1)
    } else if has_else {
        CaseDemand::Value(pair_count)
    } else {
        CaseDemand::Exhausted
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
    // Even an empty simple CASE observes its base exactly once.
    let state = initialize(ctx)?;
    let mut demand = case_start(pair_count, has_else);
    loop {
        match demand {
            CaseDemand::Condition(index) => {
                demand =
                    case_after_truth(pair_count, has_else, index, condition(index, &state, ctx)?);
            }
            CaseDemand::Value(index) => return value(index, ctx),
            CaseDemand::Exhausted => return Ok(Datum::Null),
        }
    }
}
