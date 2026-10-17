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
use crate::{Columns, Datum, EvalError};

fn invalid_report() -> EvalError {
    EvalError::ExpressionAdapterFailure(ExpressionAdapterFailure::from_scope(
        ScopeFailureKind::Contract,
        "invalid real-to-unsigned CAST result report",
    ))
}

pub(crate) fn eval_cast_real_unsigned_in(
    ctx: &dyn Columns,
    actual: &Datum,
) -> Result<u64, EvalError> {
    let value = match actual {
        Datum::Real(value) | Datum::Float32(value) => *value,
        _ => return Err(invalid_report()),
    };
    let report = tidb_query_expr::native_cast_real_unsigned_value(value);
    if let Some(bits) = report.overflow_bits {
        ctx.append_warning(
            1690,
            &format!(
                "constant {} overflows bigint",
                tidb_datatype::format_float_g_shortest(f64::from_bits(bits)),
            ),
        );
    }
    Ok(report.value)
}
