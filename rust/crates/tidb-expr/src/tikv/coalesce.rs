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
use std::borrow::Borrow;

pub(crate) fn eval_coalesce_in<V: Borrow<Datum>>(
    ctx: &dyn Columns,
    arity: usize,
    argument: impl Fn(usize, &dyn Columns) -> Result<V, EvalError>,
) -> Result<Datum, EvalError> {
    for index in 0..arity {
        let value = argument(index, ctx)?;
        let value = value.borrow();
        match tidb_query_expr::native_if_null_choose_first((!value.is_null()).then_some(value)) {
            tidb_query_expr::NativeIfNullChoice::Done(value) => return Ok(value.clone()),
            tidb_query_expr::NativeIfNullChoice::NeedSecond => {}
        }
    }
    Ok(Datum::Null)
}
