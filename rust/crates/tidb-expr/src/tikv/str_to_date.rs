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
use tidb_datatype::FieldTypeCode;
#[path = "str_to_date_value.rs"]
mod value;
pub(crate) use value::{
    bounded as str_to_date_bounded, compute as compute_str_to_date_value,
    prepare as prepare_str_to_date_value,
};
pub(crate) fn eval_str_to_date_in(
    ctx: &dyn Columns,
    values: &[Datum],
    target: Option<FieldTypeCode>,
) -> Result<Datum, EvalError> {
    str_to_date_bounded(
        ctx,
        values,
        target,
        super::identity_actual::default_budget(),
    )
}
