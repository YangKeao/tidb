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

//! Source-ordered preparation for JSON document mutations. The fixed shared
//! worker owns the complete sequence over the original document; preparation
//! carries only actual parsed paths and values, never a modified document.
//!
//! SET/INSERT/REPLACE validate all paths before converting values. Array
//! append/insert instead validate and convert each pair before the next pair.

use serde_json::Value as Json;

use super::path::{parse_path, ArraySelection, JsonPath, PathLeg};
use super::value::{json_argument, parse_json_document_argument, StringArgument};
use crate::coerce::coerce_str;
use crate::tikv::{EvaluatedArgs, EvaluatedBytesOp as Op, EvaluatedBytesResult};
use crate::{Columns, Datum, EvalError, JsonError};
use tidb_datatype::FieldType;

fn null_args() -> (Op, EvaluatedArgs) {
    (Op::JsonOutputNullNative, EvaluatedArgs::NullWitness(None))
}

/// REMOVE rejects a root path before checking multiple-selection legs. The
/// worker applies actual paths in order, including shifts from earlier removes.
pub(super) fn json_remove(vals: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    crate::tikv::evaluate_prepared_args_in(
        ctx,
        || {
            let Some(document) = parse_json_document_argument(&vals[0])? else {
                return Ok(null_args());
            };
            let mut paths = Vec::new();
            for path_value in &vals[1..] {
                let Some(path) = coerce_str(path_value)? else {
                    return Ok(null_args());
                };
                let path = parse_path(&path)?;
                if path.legs.is_empty() {
                    return Err(EvalError::Json(JsonError::VacuousPath));
                }
                if path.legs.iter().any(|leg| {
                    !matches!(
                        leg,
                        PathLeg::Key(_) | PathLeg::Array(ArraySelection::Index(_))
                    )
                }) {
                    return Err(EvalError::Json(JsonError::InvalidPathMultipleSelection));
                }
                paths.push(path);
            }
            Ok((
                Op::JsonRemoveSerdeNative,
                crate::tikv::prepare_json_paths_args(&document, &paths)?,
            ))
        },
        EvaluatedBytesResult::into_json_datum,
    )
}

/// Value arguments preserve the original static-type-aware JSON value casts;
/// an SQL string is a JSON string unless its original field type says otherwise.
pub(super) fn json_array_append(
    vals: &[Datum],
    arg_types: &[Option<FieldType>],
    ctx: &dyn Columns,
) -> Result<Datum, EvalError> {
    crate::tikv::evaluate_prepared_args_in(
        ctx,
        || {
            if vals.len() < 3 || vals.len().is_multiple_of(2) {
                return Err(EvalError::Unsupported("JSON_ARRAY_APPEND arity"));
            }
            let Some(document) = parse_json_document_argument(&vals[0])? else {
                return Ok(null_args());
            };
            let mut paths = Vec::new();
            let mut values = Vec::new();
            for (pair, types) in vals[1..]
                .as_chunks::<2>()
                .0
                .iter()
                .zip(arg_types[1..].as_chunks::<2>().0)
            {
                let Some(path) = coerce_str(&pair[0])? else {
                    return Ok(null_args());
                };
                let path = parse_path(&path)?;
                if path.could_match_multiple {
                    return Err(EvalError::Json(JsonError::InvalidPathMultipleSelection));
                }
                let value = json_argument(&pair[1], StringArgument::Value, types[1].as_ref())?;
                paths.push(path);
                values.push(value);
            }
            Ok((
                Op::JsonArrayAppendSerdeNative,
                crate::tikv::prepare_json_path_values_args(&document, &paths, &values)?,
            ))
        },
        EvaluatedBytesResult::into_json_datum,
    )
}

/// Preserve multiple-selection rejection before the exact-array-cell check,
/// and both path checks before this pair's value coercion.
pub(super) fn json_array_insert(
    vals: &[Datum],
    arg_types: &[Option<FieldType>],
    ctx: &dyn Columns,
) -> Result<Datum, EvalError> {
    crate::tikv::evaluate_prepared_args_in(
        ctx,
        || {
            if vals.len() < 3 || vals.len().is_multiple_of(2) {
                return Err(EvalError::Unsupported("JSON_ARRAY_INSERT arity"));
            }
            let Some(document) = parse_json_document_argument(&vals[0])? else {
                return Ok(null_args());
            };
            let mut paths = Vec::new();
            let mut values = Vec::new();
            for (pair, types) in vals[1..]
                .as_chunks::<2>()
                .0
                .iter()
                .zip(arg_types[1..].as_chunks::<2>().0)
            {
                let Some(path) = coerce_str(&pair[0])? else {
                    return Ok(null_args());
                };
                let path = parse_path(&path)?;
                if path.could_match_multiple {
                    return Err(EvalError::Json(JsonError::InvalidPathMultipleSelection));
                }
                let Some(PathLeg::Array(ArraySelection::Index(_))) = path.legs.last() else {
                    return Err(EvalError::Json(JsonError::InvalidPathArrayCell));
                };
                if path.legs.iter().any(|leg| {
                    !matches!(
                        leg,
                        PathLeg::Key(_) | PathLeg::Array(ArraySelection::Index(_))
                    )
                }) {
                    return Err(EvalError::Json(JsonError::InvalidPathArrayCell));
                }
                let value = json_argument(&pair[1], StringArgument::Value, types[1].as_ref())?;
                paths.push(path);
                values.push(value);
            }
            Ok((
                Op::JsonArrayInsertSerdeNative,
                crate::tikv::prepare_json_path_values_args(&document, &paths, &values)?,
            ))
        },
        EvaluatedBytesResult::into_json_datum,
    )
}

/// Frontend recipe selection only; no mode or per-pair operation is transported.
#[derive(Clone, Copy)]
pub(crate) enum JsonModifyMode {
    Set,
    Insert,
    Replace,
}

/// Parse all modifier paths before any value coercion. Cached callers retain
/// these actual shared path objects, including an observed NULL path list.
pub(crate) fn parse_json_modify_paths(
    vals: &[Datum],
) -> Result<Option<Vec<super::path::JsonPath>>, EvalError> {
    if vals.len() < 3 || vals.len().is_multiple_of(2) {
        return Err(EvalError::Unsupported("JSON modification arity"));
    }
    let mut paths = Vec::with_capacity((vals.len() - 1) / 2);
    for path_value in vals[1..].iter().step_by(2) {
        let Some(path) = coerce_str(path_value)? else {
            return Ok(None);
        };
        let path = parse_path(&path)?;
        if path.could_match_multiple
            || path.legs.iter().any(|leg| {
                !matches!(
                    leg,
                    PathLeg::Key(_) | PathLeg::Array(ArraySelection::Index(_))
                )
            })
        {
            return Err(EvalError::Json(JsonError::InvalidPathMultipleSelection));
        }
        paths.push(path);
    }
    Ok(Some(paths))
}

/// Nonexecuting preparation for a caller that already parsed the document and
/// obtained cached paths. Call this inside that caller's preparation guard so
/// document/cache/value demand remains one source-ordered preparation phase.
pub(crate) fn prepare_json_modify_with_document(
    document: Json,
    vals: &[Datum],
    arg_types: &[Option<FieldType>],
    mode: JsonModifyMode,
    paths: &[JsonPath],
) -> Result<(Op, EvaluatedArgs), EvalError> {
    if vals.len() < 3 || vals.len().is_multiple_of(2) {
        return Err(EvalError::Unsupported("JSON modification arity"));
    }
    let values = vals[1..]
        .as_chunks::<2>()
        .0
        .iter()
        .zip(arg_types[1..].as_chunks::<2>().0)
        .zip(paths)
        .map(|((pair, types), _)| json_argument(&pair[1], StringArgument::Value, types[1].as_ref()))
        .collect::<Result<Vec<_>, _>>()?;
    let operation = match mode {
        JsonModifyMode::Set => Op::JsonSetSerdeNative,
        JsonModifyMode::Insert => Op::JsonInsertSerdeNative,
        JsonModifyMode::Replace => Op::JsonReplaceSerdeNative,
    };
    // The old with-document entry deliberately zipped rather than imposing a
    // new count check. Only that actually consumed prefix enters the packet.
    let args =
        crate::tikv::prepare_json_path_values_args(&document, &paths[..values.len()], &values)?;
    Ok((operation, args))
}

/// This entry retains its original arity/path-count checks before document
/// parsing; it does not re-coerce the supplied paths.
pub(super) fn json_modify_with_paths(
    vals: &[Datum],
    arg_types: &[Option<FieldType>],
    mode: JsonModifyMode,
    paths: &[JsonPath],
    ctx: &dyn Columns,
) -> Result<Datum, EvalError> {
    crate::tikv::evaluate_prepared_args_in(
        ctx,
        || {
            if vals.len() < 3 || vals.len().is_multiple_of(2) {
                return Err(EvalError::Unsupported("JSON modification arity"));
            }
            if paths.len() != (vals.len() - 1) / 2 {
                return Err(EvalError::Unsupported("JSON modification paths"));
            }
            let Some(document) = parse_json_document_argument(&vals[0])? else {
                return Ok(null_args());
            };
            prepare_json_modify_with_document(document, vals, arg_types, mode, paths)
        },
        EvaluatedBytesResult::into_json_datum,
    )
}

pub(super) fn json_modify_with_document(
    document: Json,
    vals: &[Datum],
    arg_types: &[Option<FieldType>],
    mode: JsonModifyMode,
    paths: &[JsonPath],
    ctx: &dyn Columns,
) -> Result<Datum, EvalError> {
    crate::tikv::evaluate_prepared_args_in(
        ctx,
        || prepare_json_modify_with_document(document, vals, arg_types, mode, paths),
        EvaluatedBytesResult::into_json_datum,
    )
}

pub(super) fn json_modify(
    vals: &[Datum],
    arg_types: &[Option<FieldType>],
    mode: JsonModifyMode,
    ctx: &dyn Columns,
) -> Result<Datum, EvalError> {
    crate::tikv::evaluate_prepared_args_in(
        ctx,
        || {
            if vals.len() < 3 || vals.len().is_multiple_of(2) {
                return Err(EvalError::Unsupported("JSON modification arity"));
            }
            let Some(document) = parse_json_document_argument(&vals[0])? else {
                return Ok(null_args());
            };
            let Some(paths) = parse_json_modify_paths(vals)? else {
                return Ok(null_args());
            };
            prepare_json_modify_with_document(document, vals, arg_types, mode, &paths)
        },
        EvaluatedBytesResult::into_json_datum,
    )
}
