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
// See the License for the specific language governing permissions and
// limitations under the License.

//! Scalar facts ABOUT a document: `JSON_VALID`, `JSON_TYPE`, `JSON_LENGTH`,
//! `JSON_KEYS`, `JSON_SUM_CRC32`.
//!
//! Mirrors `builtinJSON{Valid*,Type,Length,Keys,Keys2Args,SumCRC32}Sig` in
//! `pkg/expression/builtin_json.go` and `BinaryJSON.Type` /
//! `GetElemCount` in `pkg/types/json_binary_functions.go`.
//!
//! `JSON_VALID` is the odd one and deliberately so: Go resolves it to one of
//! THREE signatures at plan-build time from the argument's EvalType, and the
//! `Others` signature answers 0 without ever looking at the value. Every
//! other function here demands a real document and raises rather than
//! guessing. `JSON_DEPTH` and the storage sizes live in `super::super::json2`
//! because they read BinaryJSON's encoded layout, not its value.

use serde_json::Value as Json;

use super::path::parse_path;
use super::value::{json_document_string, parse_json_document_argument};
use crate::builtin_ext::BuiltinFuncCache;
use crate::coerce::coerce_str;
use crate::expression::{ConstLevel, Expression};
use crate::{Columns, Datum, EvalError, JsonError};
use tidb_chunk::row::Row;

/// Per-signature cache and lazy evaluator for `JSON_SCHEMA_VALID`.
///
/// Clone deliberately starts empty, matching Go's
/// `builtinJSONSchemaValidSig.Clone`. The cache is keyed by the statement
/// context and admits `ConstOnlyInContext`, matching Go's
/// `builtinFuncCache[jsonschema.Schema]` consumer.
#[derive(Debug, Default)]
pub(crate) struct JsonSchemaCache(BuiltinFuncCache<Option<PreparedJsonSchema>>);

#[derive(Debug)]
struct PreparedJsonSchema {
    schema: Json,
    validator: std::sync::OnceLock<Result<jsonschema::Validator, EvalError>>,
}

impl Clone for JsonSchemaCache {
    fn clone(&self) -> Self {
        Self::default()
    }
}

impl JsonSchemaCache {
    pub(crate) fn eval(
        &self,
        args: &[Expression],
        ctx: &dyn Columns,
        row: Row<'_>,
    ) -> Result<Datum, EvalError> {
        let [schema_arg, document_arg] = args else {
            return Err(EvalError::WrongParameterCount("json_schema_valid"));
        };
        let schema_value = schema_arg.eval(ctx, row)?;
        if schema_value.is_null() {
            return Ok(Datum::Null);
        }

        if schema_arg.const_level() >= ConstLevel::ONLY_IN_CONTEXT {
            let schema = self
                .0
                .get_or_init_cache(ctx.context_id(), || prepare_json_schema(&schema_value))?;
            let Some(schema) = schema.as_ref().as_ref() else {
                return Ok(Datum::Null);
            };
            return validate_json_schema(schema, &document_arg.eval(ctx, row)?);
        }

        let schema = prepare_json_schema(&schema_value)?;
        let Some(schema) = schema.as_ref() else {
            return Ok(Datum::Null);
        };
        validate_json_schema(schema, &document_arg.eval(ctx, row)?)
    }
}

/// Parses and checks the schema argument without resolving external `$ref`s.
/// TiDB's qri-io validator does not fetch them until document validation, so a
/// NULL document still short-circuits without filesystem or network access.
fn prepare_json_schema(value: &Datum) -> Result<Option<PreparedJsonSchema>, EvalError> {
    let Some(schema) = parse_json_document_argument(value)? else {
        return Ok(None);
    };
    if !matches!(schema, Json::Object(_) | Json::Bool(_)) {
        return Err(EvalError::Json(JsonError::InvalidJsonType {
            argument: 1,
            function: "json_schema_valid",
            required: "object".to_owned(),
        }));
    }
    jsonschema::draft201909::meta::validate(&schema).map_err(invalid_json_schema)?;
    Ok(Some(PreparedJsonSchema {
        schema,
        validator: std::sync::OnceLock::new(),
    }))
}

fn build_json_schema(schema: &Json) -> Result<jsonschema::Validator, EvalError> {
    let client = reqwest::blocking::Client::builder()
        .no_proxy()
        .build()
        .map_err(invalid_json_schema)?;
    jsonschema::options()
        .with_draft(jsonschema::Draft::Draft201909)
        // qri-io's Draft2019_09 `Format` keyword is an assertion, while the
        // Rust validator follows the newer annotation default unless enabled.
        .should_validate_formats(true)
        .with_retriever(LocalSchemaRetriever { client })
        .build(schema)
        .map_err(invalid_json_schema)
}

struct LocalSchemaRetriever {
    client: reqwest::blocking::Client,
}

impl jsonschema::Retrieve for LocalSchemaRetriever {
    fn retrieve(
        &self,
        uri: &jsonschema::Uri<String>,
    ) -> Result<Json, Box<dyn std::error::Error + Send + Sync>> {
        match uri.scheme().as_str() {
            "http" | "https" => Ok(self.client.get(uri.as_str()).send()?.json()?),
            "file" => Ok(serde_json::from_reader(std::fs::File::open(
                uri.path().as_str(),
            )?)?),
            scheme => Err(format!("unsupported schema URI scheme: {scheme}").into()),
        }
    }
}

fn invalid_json_schema(error: impl ToString) -> EvalError {
    EvalError::Json(JsonError::InvalidJsonType {
        argument: 1,
        function: "json_schema_valid",
        required: error.to_string(),
    })
}

/// Validates the document, resolving external references only after the
/// document has survived the source NULL/parse boundary.
fn validate_json_schema(schema: &PreparedJsonSchema, document: &Datum) -> Result<Datum, EvalError> {
    let Some(document) = parse_json_document_argument(document)? else {
        return Ok(Datum::Null);
    };
    let validator = match schema
        .validator
        .get_or_init(|| build_json_schema(&schema.schema))
    {
        Ok(validator) => validator,
        Err(error) => return Err(error.clone()),
    };
    Ok(Datum::Int(i64::from(validator.is_valid(&document))))
}

/// Datum-level host adapter used by callers without a reusable scalar node.
pub(crate) fn json_schema_valid(
    schema_value: &Datum,
    document: &Datum,
) -> Result<Datum, EvalError> {
    let Some(schema) = prepare_json_schema(schema_value)? else {
        return Ok(Datum::Null);
    };
    validate_json_schema(&schema, document)
}

/// `JSON_VALID(arg)`, port of `builtinJSONValid{JSON,String,Others}Sig`.
/// String arguments are JSON documents; every non-string, non-JSON SQL value
/// is the Go `Others` signature and therefore returns zero rather than being
/// stringified.  `NULL` propagates.
pub(super) fn json_valid(v: &Datum, ctx: &dyn Columns) -> Result<Datum, EvalError> {
    use crate::tikv::{EvaluatedArgs, EvaluatedBytesOp};

    // Select the SQL signature without inspecting or computing its result.
    let operation = match v {
        Datum::Null | Datum::String(_) | Datum::Bytes(_) => EvaluatedBytesOp::JsonValidTextNative,
        Datum::Json(_) => EvaluatedBytesOp::JsonValidBinaryNative,
        _ => EvaluatedBytesOp::JsonValidOtherNative,
    };
    crate::tikv::evaluate_args_in(
        operation,
        ctx,
        || {
            Ok(match v {
                Datum::Null => EvaluatedArgs::Bytes(None),
                // The text kernel, not UTF-8 coercion, decides validity.
                Datum::String(value) => EvaluatedArgs::Bytes(Some(value.bytes().to_vec())),
                Datum::Bytes(value) => EvaluatedArgs::Bytes(Some(value.clone())),
                Datum::Json(value) => EvaluatedArgs::Bytes(Some(value.encoded())),
                Datum::MinNotNull | Datum::MaxValue => {
                    return Err(EvalError::Unsupported("range sentinel JSON_VALID argument"));
                }
                // The original Others signature does not read its value.
                _ => EvaluatedArgs::NoArgs,
            })
        },
        crate::tikv::EvaluatedBytesResult::into_int_datum,
    )
}

/// `JSON_TYPE(json_doc)`, port of `builtinJSONTypeSig.evalString` and
/// `types.BinaryJSON.Type` (`pkg/types/json_binary_functions.go`).
pub(super) fn json_type(v: &Datum, ctx: &dyn Columns) -> Result<Datum, EvalError> {
    use crate::tikv::{EvaluatedBytesOp, JsonReportOutcome};

    let operation = if matches!(v, Datum::Json(_)) {
        EvaluatedBytesOp::JsonTypeBinaryNative
    } else {
        EvaluatedBytesOp::JsonTypeTextNative
    };
    crate::tikv::evaluate_bytes_in(
        operation,
        ctx,
        || match v {
            Datum::Null => Ok(None),
            // Keep all typed tags and payload bytes; the kernel owns type-name
            // validation. Display would collapse temporal and opaque kinds.
            Datum::Json(document) => Ok(Some(document.encoded())),
            _ => {
                let text = json_document_string(v)?.ok_or(EvalError::Json(
                    JsonError::InvalidTypeForJson {
                        argument: 1,
                        function: "json_type",
                    },
                ))?;
                Ok(Some(text.into_owned().into_bytes()))
            }
        },
        |computed| match computed.into_json_report()? {
            JsonReportOutcome::Null => Ok(Datum::Null),
            JsonReportOutcome::Bytes(value) => Ok(Datum::new_string(value)),
            JsonReportOutcome::EmptyText => Err(EvalError::Json(JsonError::EmptyText)),
            JsonReportOutcome::InvalidText => Err(EvalError::Json(JsonError::InvalidText)),
            JsonReportOutcome::Int(_) => {
                Err(EvalError::Unsupported("JSON_TYPE result kind mismatch"))
            }
        },
    )
}

/// `JSON_LENGTH(json_doc [, path])`, port of `builtinJSONLengthSig.evalInt`.
/// As in TiDB, a wildcard/range path is a true SQL error rather than a length
/// of an implicitly auto-wrapped selection.
pub(super) fn json_length(vals: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    use crate::tikv::EvaluatedBytesOp as Op;
    crate::tikv::evaluate_prepared_args_in(
        ctx,
        || {
            let Some(document) = parse_json_document_argument(&vals[0])? else {
                return Ok(super::predicate::json_predicate_null_args());
            };
            if let Some(path_value) = vals.get(1) {
                let Some(path) = coerce_str(path_value)? else {
                    return Ok(super::predicate::json_predicate_null_args());
                };
                if parse_path(&path)?.could_match_multiple {
                    return Err(EvalError::Json(JsonError::InvalidPathMultipleSelection));
                }
                return Ok((
                    Op::JsonLengthPathSerdeNative,
                    crate::tikv::prepare_json_serde_args(&document, None, Some(&path))?,
                ));
            }
            Ok((
                Op::JsonLengthSerdeNative,
                crate::tikv::prepare_json_serde_args(&document, None, None)?,
            ))
        },
        crate::tikv::EvaluatedBytesResult::into_int_datum,
    )
}

/// `JSON_KEYS(json_doc [, path])`, port of
/// `builtinJSONKeys{Sig,2ArgsSig}.evalJSON`.  The result is an array of the
/// selected object's keys, in BinaryJSON's byte-sorted object order.  A
/// scalar, array, missing path, or selected non-object is SQL NULL; a path
/// that could select more than one value is an error.
pub(super) fn json_keys(vals: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    use crate::tikv::{EvaluatedArgs, EvaluatedBytesOp as Op};
    crate::tikv::evaluate_prepared_args_in(
        ctx,
        || {
            let null = || (Op::JsonOutputNullNative, EvaluatedArgs::NullWitness(None));
            let Some(document) = parse_json_document_argument(&vals[0])? else {
                return Ok(null());
            };
            if let Some(path_value) = vals.get(1) {
                let Some(path) = coerce_str(path_value)? else {
                    return Ok(null());
                };
                if parse_path(&path)?.could_match_multiple {
                    return Err(EvalError::Json(JsonError::InvalidPathMultipleSelection));
                }
                return Ok((
                    Op::JsonKeysPathSerdeNative,
                    crate::tikv::prepare_json_serde_args(&document, None, Some(&path))?,
                ));
            }
            Ok((
                Op::JsonKeysSerdeNative,
                crate::tikv::prepare_json_serde_args(&document, None, None)?,
            ))
        },
        crate::tikv::EvaluatedBytesResult::into_json_datum,
    )
}

/// The existing internal `JSON_SUM_CRC32(json_doc)` scalar-array domain.
/// Document coercion accepts strings and typed JSON through the canonical-text
/// boundary. The shared worker preserves the existing numeric formatting and
/// sums IEEE CRC32 values for homogeneous numeric or string arrays.
///
/// This does not admit SQL `JSON_SUM_CRC32(expr AS type ARRAY)`: ARRAY target
/// conversion (including signedness, width, and extraction) remains unsupported.
pub(super) fn json_sum_crc32(value: &Datum, ctx: &dyn Columns) -> Result<Datum, EvalError> {
    crate::tikv::eval_json_sum_crc32_in(ctx, value)
}
