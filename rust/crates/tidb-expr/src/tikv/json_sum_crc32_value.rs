// Copyright 2026 PingCAP, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Presentation of actual SDK JSON checksum computation, without a report.
use crate::{Datum, EvalError};
use tidb_query_expr::{NativeJsonSumCrc32Result as Value, NativeJsonSumCrc32ValueError as Error};
pub(crate) fn bounded(value: &Datum, maximum: usize) -> Result<Datum, EvalError> {
    let result =
        tidb_query_expr::native_json_sum_crc32_value(value.as_shared_json_input(), maximum)
            .map_err(|error| match error {
                Error::Coercion(error) => crate::builtin_ext::json::json_coercion_error(error),
                Error::ResourceLimit(reason) => EvalError::ExpressionRuntimeFailure(
                    crate::ExpressionRuntimeFailure::from_local_eval(
                        tidb_query_expr::local::LocalError::ResourceLimit(reason.into()),
                        None,
                    ),
                ),
            })?;
    match result {
        None => Ok(Datum::Null),
        Some(Value::Value(sum)) => Ok(Datum::Int(sum)),
        Some(Value::RequiresArray) => {
            Err(EvalError::Unsupported("JSON_SUM_CRC32 requires JSON array"))
        }
        Some(Value::RequiresScalar) => Err(EvalError::Unsupported(
            "JSON_SUM_CRC32 requires scalar array values",
        )),
        Some(Value::RequiresHomogeneous) => Err(EvalError::Unsupported(
            "JSON_SUM_CRC32 requires homogeneous array values",
        )),
    }
}
