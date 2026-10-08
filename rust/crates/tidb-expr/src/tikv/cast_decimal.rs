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
use tidb_datatype::Decimal;

/// Converts a legacy integer without changing the ordinary cast APIs.
pub fn eval_legacy_cast_decimal_integer(value: i128) -> Option<tidb_datatype::Decimal> {
    tidb_query_expr::native_legacy_cast_decimal_integer(value).map(Decimal::from_shared_parse)
}

/// Converts a legacy datum while folding conversion events and errors.
pub fn eval_legacy_cast_decimal_datum(value: &Datum) -> Option<Decimal> {
    tidb_query_expr::native_legacy_cast_decimal_numeric(value.as_shared_numeric_input())
        .map(Decimal::from_shared_parse)
}

/// The ordinary cast caller retains its original NULL/range/vector guards.
/// Conversion decisions, warning order, error folding and precision policy
/// belong to the SDK; these closures actuate only the original primitives.
pub(crate) fn eval_cast_decimal_in(
    ctx: &dyn Columns,
    value: &Datum,
    flen: u32,
    scale: u32,
) -> Result<Datum, EvalError> {
    let converted = tidb_query_expr::native_cast_decimal_numeric(
        value.as_shared_numeric_input(),
        flen,
        scale,
        |code, message| ctx.append_warning(code, message),
    );
    Ok(Datum::Decimal(Decimal::from_shared_parse(converted)))
}

/// The existing UNION helper needs only the original input diagnostic. It
/// retains its own separate conversion domain and negative-input handling.
pub(crate) fn report_cast_decimal_input_in(ctx: &dyn Columns, value: &Datum) {
    tidb_query_expr::native_cast_decimal_numeric_input_warning(
        value.as_shared_numeric_input(),
        |code, message| ctx.append_warning(code, message),
    );
}
