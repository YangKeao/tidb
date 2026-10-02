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

//! Native JSON rendering is owned by the shared serde policy. Existing value
//! consumers retain the same formatting facade; JSON_PRETTY enters its worker
//! with the actual parsed document, never a preformatted answer.

use super::value::parse_json_document_argument;
use crate::{Datum, EvalError};

pub(super) use tidb_query_expr::native_json_format as format_json;

/// Preserve document coercion and NULL demand while the worker formats the
/// final string, including original indentation, key order, and number policy.
pub(super) fn json_pretty(value: &Datum, ctx: &dyn crate::Columns) -> Result<Datum, EvalError> {
    use crate::tikv::{EvaluatedArgs, EvaluatedBytesOp as Op};
    crate::tikv::evaluate_prepared_args_in(
        ctx,
        || {
            let Some(document) = parse_json_document_argument(value)? else {
                return Ok((Op::JsonOutputNullNative, EvaluatedArgs::NullWitness(None)));
            };
            Ok((
                Op::JsonPrettySerdeNative,
                crate::tikv::prepare_json_serde_args(&document, None, None)?,
            ))
        },
        |computed| {
            Ok(computed
                .into_bytes()?
                .map_or(Datum::Null, Datum::new_string))
        },
    )
}
