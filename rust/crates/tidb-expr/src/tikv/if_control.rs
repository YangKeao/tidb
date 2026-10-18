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

use super::control_value::{default_budget, owned};
use crate::{Columns, Datum, EvalError};

pub(crate) fn eval_if_in(
    ctx: &dyn Columns,
    condition: impl FnOnce(&dyn Columns) -> Result<Option<bool>, EvalError>,
    when_true: impl FnOnce(&dyn Columns) -> Result<Datum, EvalError>,
    when_false: impl FnOnce(&dyn Columns) -> Result<Datum, EvalError>,
) -> Result<Datum, EvalError> {
    let value = match tidb_query_expr::native_if_choose_branch(condition(ctx)?) {
        tidb_query_expr::NativeIfBranch::Then => when_true(ctx)?,
        tidb_query_expr::NativeIfBranch::Else => when_false(ctx)?,
    };
    owned(value, default_budget())
}
