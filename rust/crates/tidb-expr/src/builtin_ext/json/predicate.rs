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

//! JSON predicate preparation and source-ordered path demand. The shared
//! workers own the native serde-value predicates, distinct from raw BinaryJSON
//! comparison policy. No extracted target or predicate answer is prepared here.

use serde_json::Value as Json;

use super::path::parse_path;
use super::value::{
    json_argument, parse_json_document_argument, parse_json_document_argument_strict,
    StringArgument,
};
use crate::coerce::coerce_str;
use crate::tikv::{EvaluatedArgs, EvaluatedBytesOp as Op, EvaluatedBytesResult};
use crate::{Columns, Datum, EvalError, JsonError};

/// The original serde-value equality helper is now only a shared-policy facade.
#[cfg(test)]
pub(super) fn json_equal(left: &Json, right: &Json) -> bool {
    tidb_query_expr::native_json_equal(left, right)
}

pub(super) fn json_predicate_null_args() -> (Op, EvaluatedArgs) {
    (
        Op::JsonPredicateNullNative,
        EvaluatedArgs::NullWitness(None),
    )
}

/// MEMBER OF converts the candidate as a value, but validates the document
/// first. In particular, an SQL string candidate is not parsed as JSON text.
pub(super) fn json_member_of(vals: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    crate::tikv::evaluate_prepared_args_in(
        ctx,
        || {
            let [candidate, document] = vals else {
                return Err(EvalError::Unsupported("JSON_MEMBER_OF arity"));
            };
            if candidate.is_null() || document.is_null() {
                return Ok(json_predicate_null_args());
            }
            parse_json_document_argument_strict(document, 2, "member of")?;
            let candidate = json_argument(candidate, StringArgument::Value, None)?;
            let document = json_argument(document, StringArgument::Document, None)?;
            Ok((
                Op::JsonMemberOfSerdeNative,
                crate::tikv::prepare_json_serde_args(&candidate, Some(&document), None)?,
            ))
        },
        EvaluatedBytesResult::into_boolean_datum,
    )
}

/// CONTAINS keeps document/candidate conversion ahead of optional path demand.
/// The path is validated here for the original SQL error; selection itself is
/// performed only by the worker over the actual document and original path.
pub(super) fn json_contains(vals: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    crate::tikv::evaluate_prepared_args_in(
        ctx,
        || {
            let ([document, candidate] | [document, candidate, ..]) = vals else {
                return Err(EvalError::Unsupported("JSON_CONTAINS arity"));
            };
            if document.is_null() || candidate.is_null() {
                return Ok(json_predicate_null_args());
            }
            parse_json_document_argument_strict(document, 1, "json_contains")?;
            let document = json_argument(document, StringArgument::Document, None)?;
            let candidate = json_argument(candidate, StringArgument::Document, None)?;
            if let Some(path_value) = vals.get(2) {
                let Some(path) = coerce_str(path_value)? else {
                    return Ok(json_predicate_null_args());
                };
                if parse_path(&path)?.could_match_multiple {
                    return Err(EvalError::Json(JsonError::InvalidPathMultipleSelection));
                }
                return Ok((
                    Op::JsonContainsPathSerdeNative,
                    crate::tikv::prepare_json_serde_args(&document, Some(&candidate), Some(&path))?,
                ));
            }
            Ok((
                Op::JsonContainsSerdeNative,
                crate::tikv::prepare_json_serde_args(&document, Some(&candidate), None)?,
            ))
        },
        EvaluatedBytesResult::into_boolean_datum,
    )
}

pub(super) fn json_overlaps(vals: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    crate::tikv::evaluate_prepared_args_in(
        ctx,
        || {
            let [left, right] = vals else {
                return Err(EvalError::Unsupported("JSON_OVERLAPS arity"));
            };
            if left.is_null() || right.is_null() {
                return Ok(json_predicate_null_args());
            }
            parse_json_document_argument_strict(left, 1, "json_overlaps")?;
            parse_json_document_argument_strict(right, 2, "json_overlaps")?;
            let left = json_argument(left, StringArgument::Document, None)?;
            let right = json_argument(right, StringArgument::Document, None)?;
            Ok((
                Op::JsonOverlapsSerdeNative,
                crate::tikv::prepare_json_serde_args(&left, Some(&right), None)?,
            ))
        },
        EvaluatedBytesResult::into_boolean_datum,
    )
}

/// Child expressions have already been evaluated, but path coercion and parse
/// remain lazy. Each visited path has an actual document/path worker call; a
/// decisive result or the last result is returned unchanged, never synthesized.
pub(super) fn json_contains_path(vals: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    let [document, contain_type, first_path, paths @ ..] = vals else {
        return Err(EvalError::Unsupported("JSON_CONTAINS_PATH arity"));
    };
    let mut prepared: Option<(Json, bool)> = None;
    let mut result = crate::tikv::evaluate_prepared_args_in(
        ctx,
        || {
            let Some(document) = parse_json_document_argument(document)? else {
                return Ok(json_predicate_null_args());
            };
            let Some(contain_type) = coerce_str(contain_type)? else {
                return Ok(json_predicate_null_args());
            };
            let one = match contain_type.to_ascii_lowercase().as_str() {
                "one" => true,
                "all" => false,
                _ => {
                    return Err(EvalError::Json(JsonError::BadOneOrAllArg {
                        function: "json_contains_path",
                    }));
                }
            };
            let Some(path) = coerce_str(first_path)? else {
                return Ok(json_predicate_null_args());
            };
            parse_path(&path)?;
            let args = crate::tikv::prepare_json_serde_args(&document, None, Some(&path))?;
            prepared = Some((document, one));
            Ok((Op::JsonPathExistsSerdeNative, args))
        },
        EvaluatedBytesResult::into_boolean_datum,
    )?;
    let Some((document, one)) = prepared else {
        return Ok(result);
    };
    for path_value in paths {
        if result.is_null() || matches!(&result, Datum::Int(value) if (*value != 0) == one) {
            return Ok(result);
        }
        result = crate::tikv::evaluate_prepared_args_in(
            ctx,
            || {
                let Some(path) = coerce_str(path_value)? else {
                    return Ok(json_predicate_null_args());
                };
                parse_path(&path)?;
                Ok((
                    Op::JsonPathExistsSerdeNative,
                    crate::tikv::prepare_json_serde_args(&document, None, Some(&path))?,
                ))
            },
            EvaluatedBytesResult::into_boolean_datum,
        )?;
    }
    Ok(result)
}
