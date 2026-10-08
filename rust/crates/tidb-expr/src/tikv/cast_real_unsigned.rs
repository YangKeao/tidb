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

use tidb_query_expr::decode_native_cast_real_unsigned_result;

use super::adapter_failure::{ExpressionAdapterFailure, ScopeFailureKind};
use super::{evaluate_args_in, identity_value, EvaluatedArgs, EvaluatedBytesOp};
use crate::{Columns, Datum, EvalError};

fn invalid_report() -> EvalError {
    EvalError::ExpressionAdapterFailure(ExpressionAdapterFailure::from_scope(
        ScopeFailureKind::Contract,
        "invalid real-to-unsigned CAST result report",
    ))
}

fn project_report(report: Option<Vec<u8>>, ctx: &dyn Columns) -> Result<u64, EvalError> {
    let report = report.ok_or_else(invalid_report)?;
    let report = decode_native_cast_real_unsigned_result(&report).ok_or_else(invalid_report)?;
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

pub(crate) fn eval_cast_real_unsigned_in(
    ctx: &dyn Columns,
    actual: &Datum,
) -> Result<u64, EvalError> {
    evaluate_args_in(
        EvaluatedBytesOp::CastRealUnsignedNative,
        ctx,
        || Ok(EvaluatedArgs::Bytes(identity_value::encode(actual)?)),
        |computed| project_report(computed.into_bytes()?, ctx),
    )
}
