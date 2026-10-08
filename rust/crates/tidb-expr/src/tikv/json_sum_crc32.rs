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

use tidb_query_expr::{decode_native_json_sum_crc32_result, NativeJsonSumCrc32Result};

use super::adapter_failure::{ExpressionAdapterFailure, ScopeFailureKind};
use super::{
    evaluate_args_in, prepare_json_serde_args, EvaluatedArgs, EvaluatedBytesOp,
    EvaluatedBytesResult,
};
use crate::{Columns, Datum, EvalError};

fn invalid_report() -> EvalError {
    EvalError::ExpressionAdapterFailure(ExpressionAdapterFailure::from_scope(
        ScopeFailureKind::Contract,
        "invalid native JSON_SUM_CRC32 report",
    ))
}

fn project_report(computed: EvaluatedBytesResult) -> Result<Datum, EvalError> {
    let EvaluatedBytesResult::Bytes(report) = computed else {
        return Err(invalid_report());
    };
    let Some(report) = report else {
        return Ok(Datum::Null);
    };
    match decode_native_json_sum_crc32_result(&report).ok_or_else(invalid_report)? {
        NativeJsonSumCrc32Result::Value(sum) => Ok(Datum::Int(sum)),
        NativeJsonSumCrc32Result::RequiresArray => {
            Err(EvalError::Unsupported("JSON_SUM_CRC32 requires JSON array"))
        }
        NativeJsonSumCrc32Result::RequiresScalar => Err(EvalError::Unsupported(
            "JSON_SUM_CRC32 requires scalar array values",
        )),
        NativeJsonSumCrc32Result::RequiresHomogeneous => Err(EvalError::Unsupported(
            "JSON_SUM_CRC32 requires homogeneous array values",
        )),
    }
}

pub(crate) fn eval_json_sum_crc32_in(ctx: &dyn Columns, value: &Datum) -> Result<Datum, EvalError> {
    evaluate_args_in(
        EvaluatedBytesOp::JsonSumCrc32SerdeNative,
        ctx,
        || match crate::builtin_ext::json::parse_json_document_argument(value)? {
            Some(document) => prepare_json_serde_args(&document, None, None),
            // SQL NULL enters the same worker. A parsed JSON null remains a
            // present serde document and receives the SDK's array-domain error.
            None => Ok(EvaluatedArgs::Bytes(None)),
        },
        project_report,
    )
}
