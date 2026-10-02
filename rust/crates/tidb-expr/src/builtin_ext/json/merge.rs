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

//! Source-ordered document preparation for JSON_MERGE/PRESERVE and PATCH.
//! The shared workers own all merging and PATCH reset-point selection.
//! PRESERVE stops coercion at SQL NULL; PATCH retains every actual nullable
//! operand and continues coercion, distinguishing SQL NULL from JSON null.

use serde_json::Value as Json;

use super::value::{json_document_string, parse_json};
use crate::{Columns, Datum, EvalError, JsonError};

/// The deprecated JSON_MERGE spelling's warning remains with its caller,
/// after the worker's actual successful, non-NULL result has been packed.
pub(super) fn json_merge(
    vals: &[Datum],
    function: &'static str,
    ctx: &dyn Columns,
) -> Result<Datum, EvalError> {
    crate::tikv::evaluate_prepared_args_in(
        ctx,
        || {
            let mut values = Vec::with_capacity(vals.len());
            for (index, value) in vals.iter().enumerate() {
                let Some(value) = parse_json_merge_argument(value, index, function)? else {
                    return Ok((
                        crate::tikv::EvaluatedBytesOp::JsonOutputNullNative,
                        crate::tikv::EvaluatedArgs::NullWitness(None),
                    ));
                };
                values.push(value);
            }
            Ok((
                crate::tikv::EvaluatedBytesOp::JsonMergeSerdeNative,
                crate::tikv::prepare_json_array_args(&values)?,
            ))
        },
        crate::tikv::EvaluatedBytesResult::into_json_datum,
    )
}

/// One document argument of the `JSON_MERGE*` family.
///
/// Go types every argument of these as `ETJson`, and `verifyJSONArgsType`
/// (`jsonMergeFunctionClass.verifyArgs`) then demands that each argument be a
/// JSON value or a STRING: `JSON_MERGE_PRESERVE('[1]', 3)` is 3146, not a
/// merge with the number 3. A string argument carries `ParseToJSONFlag`, so
/// `'1'` is the JSON number 1 and `'{}'` is the empty object -- unlike the
/// VALUE arguments of `JSON_SET`/`JSON_ARRAY_APPEND`, which stay JSON strings.
fn parse_json_merge_argument(
    value: &Datum,
    index: usize,
    function: &'static str,
) -> Result<Option<Json>, EvalError> {
    if let Some(text) = json_document_string(value)? {
        return Ok(Some(parse_json(&text)?));
    }
    match value {
        Datum::Null => Ok(None),
        _ => Err(EvalError::Json(JsonError::InvalidTypeForJson {
            argument: index + 1,
            function,
        })),
    }
}

/// PATCH must coerce every original operand, including those before a later
/// reset and after SQL NULL. No prefix is discarded during preparation.
pub(super) fn json_merge_patch(vals: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    crate::tikv::evaluate_prepared_args_in(
        ctx,
        || {
            let mut values = Vec::with_capacity(vals.len());
            for (index, value) in vals.iter().enumerate() {
                values.push(parse_json_merge_argument(value, index, "json_merge_patch")?);
            }
            // Count zero is deliberately not normalized or rejected here: the
            // existing malformed native PB entry reaches the worker's panic,
            // unlike the raw SDK's distinct empty-list NULL policy.
            Ok((
                crate::tikv::EvaluatedBytesOp::JsonMergePatchSerdeNative,
                crate::tikv::prepare_json_merge_patch_args(&values)?,
            ))
        },
        crate::tikv::EvaluatedBytesResult::into_json_datum,
    )
}
