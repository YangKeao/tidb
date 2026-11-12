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

pub(crate) fn eval_if_null_in(
    ctx: &dyn Columns,
    first: impl FnOnce(&dyn Columns) -> Result<Datum, EvalError>,
    second: impl FnOnce(&dyn Columns) -> Result<Datum, EvalError>,
) -> Result<Datum, EvalError> {
    let first = first(ctx)?;
    match tidb_query_expr::native_if_null_choose_first((!first.is_null()).then_some(first)) {
        tidb_query_expr::NativeIfNullChoice::Done(value) => Ok(value),
        tidb_query_expr::NativeIfNullChoice::NeedSecond => second(ctx),
    }
}
