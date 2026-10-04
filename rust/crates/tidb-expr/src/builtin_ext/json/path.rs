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

//! JSON path expressions and extraction: `JSON_EXTRACT` (`->`, `->>`), the
//! `$`-path grammar, and the selection walk every other builtin reuses.
//!
//! Mirrors `pkg/types/json_path_expr.go` (`ParseJSONPathExpr`,
//! `JSONPathExpression`, `jsonPathLeg`) and `BinaryJSON.Extract` /
//! `extractTo` in `pkg/types/json_binary_functions.go`, plus
//! `builtinJSONExtractSig.evalJSON` in `pkg/expression/builtin_json.go`.
//!
//! [`JsonPath::could_match_multiple`] is Go's `CouldMatchMultipleValues`: the
//! flag that decides whether a caller may treat a selection as one value
//! (`JSON_LENGTH`, `JSON_KEYS`, `JSON_SET`) or must raise 3149.

use super::value::{parse_json_document_argument, parse_json_document_argument_strict};
use crate::coerce::coerce_str;
use crate::{Datum, EvalError, JsonError};

pub(crate) use tidb_query_expr::NativeJsonPath as JsonPath;
pub(super) use tidb_query_expr::{
    NativeJsonArraySelection as ArraySelection, NativeJsonPathLeg as PathLeg,
};

/// `JSON_EXTRACT(json_doc, path [, path] ...)`, port of
/// `builtinJSONExtractSig.evalJSON` and `types.BinaryJSON.Extract`.
pub(super) fn json_extract(vals: &[Datum], ctx: &dyn crate::Columns) -> Result<Datum, EvalError> {
    use crate::tikv::{EvaluatedArgs, EvaluatedBytesOp as Op};
    crate::tikv::evaluate_prepared_args_in(
        ctx,
        || {
            // Preserve the strict document error before any path demand.
            parse_json_document_argument_strict(&vals[0], 1, "json_extract")?;
            let Some(document) = parse_json_document_argument(&vals[0])? else {
                return Ok((Op::JsonOutputNullNative, EvaluatedArgs::NullWitness(None)));
            };
            let mut paths = Vec::with_capacity(vals.len() - 1);
            for value in &vals[1..] {
                let Some(path) = coerce_str(value)? else {
                    return Ok((Op::JsonOutputNullNative, EvaluatedArgs::NullWitness(None)));
                };
                paths.push(parse_path(&path)?);
            }
            Ok((
                Op::JsonExtractSerdeNative,
                crate::tikv::prepare_json_paths_args(&document, &paths)?,
            ))
        },
        crate::tikv::EvaluatedBytesResult::into_json_datum,
    )
}

/// Preserve the source's rune-position SQL diagnostic while the shared parser
/// owns the grammar, path flags, and data representation.
pub(crate) fn parse_path(input: &str) -> Result<JsonPath, EvalError> {
    tidb_query_expr::parse_native_json_path(input)
        .map_err(|error| EvalError::Json(JsonError::InvalidPath(error.position)))
}
