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

//! `JSON_SEARCH`: preserve source argument demand while the shared worker owns
//! the string-leaf walk, LIKE matching, full-path deduplication and JSON output.
//! Its array-selection rule deliberately differs from JSON_EXTRACT: an array
//! leg selects only arrays, never an otherwise eligible non-array value.

use super::path::parse_path;
use super::value::parse_json_document_argument;
use crate::coerce::coerce_str;
use crate::{Columns, Datum, EvalError, JsonError};

/// `JSON_SEARCH(json_doc, one_or_all, search_str [, escape_char [, path] ...])`.
/// The dispatcher supplies the original minimum arity. Prepare every demanded
/// path before execution, even when an earlier path could satisfy `one` mode.
pub(super) fn json_search(vals: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    use crate::tikv::{EvaluatedArgs, EvaluatedBytesOp as Op};
    crate::tikv::evaluate_prepared_args_in(
        ctx,
        || {
            let null = || (Op::JsonOutputNullNative, EvaluatedArgs::NullWitness(None));
            let Some(document) = parse_json_document_argument(&vals[0])? else {
                return Ok(null());
            };
            let Some(mode) = coerce_str(&vals[1])? else {
                return Ok(null());
            };
            // This family reports 3150, not JSON_CONTAINS_PATH's 3154.
            let one = tidb_query_expr::parse_native_json_search_mode(&mode)
                .ok_or(EvalError::Json(JsonError::InvalidContainsPathType))?;
            let Some(pattern) = coerce_str(&vals[2])? else {
                return Ok(null());
            };
            let escape = match vals.get(3) {
                None | Some(Datum::Null) => '\\',
                Some(value) => {
                    let Some(value) = coerce_str(value)? else {
                        return Ok(null());
                    };
                    if value.is_empty() {
                        '\\'
                    } else if value.chars().count() == 1 {
                        value.chars().next().expect("one character is present")
                    } else {
                        return Err(EvalError::Unsupported("JSON_SEARCH escape length"));
                    }
                }
            };
            let mut paths = Vec::new();
            if vals.len() > 4 {
                for value in &vals[4..] {
                    let Some(path) = coerce_str(value)? else {
                        return Ok(null());
                    };
                    paths.push(parse_path(&path)?);
                }
            }
            Ok((
                Op::JsonSearchSerdeNative,
                crate::tikv::prepare_json_search_args(&document, &paths, one, &pattern, escape)?,
            ))
        },
        |computed| {
            Ok(computed
                .into_bytes()?
                .map_or(Datum::Null, Datum::new_string))
        },
    )
}
