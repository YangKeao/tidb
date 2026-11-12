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

use tidb_datatype::{Decimal, SessionTimeZone, Time};

use crate::{Columns, EvalError};
/// Convert the actual legacy Decimal, then let its original owner consume the
/// nullable time and context without selecting another expression execution.
pub fn eval_from_unixtime_legacy_scoped_in<T>(
    value: Option<Decimal>,
    zone: &SessionTimeZone,
    ctx: &dyn Columns,
    pack: impl FnOnce(Option<Time>, &dyn Columns) -> T,
) -> Result<T, EvalError> {
    super::epoch_value::from_legacy_values_bounded(
        value,
        zone,
        ctx,
        super::identity_actual::default_budget(),
        pack,
    )
}
