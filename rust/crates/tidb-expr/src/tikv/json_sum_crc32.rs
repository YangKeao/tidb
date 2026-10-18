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
#[path = "json_sum_crc32_value.rs"]
mod value;
pub(crate) use value::bounded as json_sum_crc32_bounded;
pub(crate) fn eval_json_sum_crc32_in(
    _ctx: &dyn Columns,
    value: &Datum,
) -> Result<Datum, EvalError> {
    json_sum_crc32_bounded(value, super::control_value::default_budget())
}
