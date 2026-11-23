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

use std::sync::{Arc, Mutex};

use serde_json::{Number, Value as Json};

use super::path::{extract, parse_path};
use super::value::{
    binary_json_datum, json_document_string, json_sql_string, parse_json,
    parse_json_document_argument,
};
use crate::builtin_ext::BuiltinFuncCache;
use crate::coerce::coerce_str;
use crate::expression::{ConstLevel, Expression};
use crate::{Columns, Datum, EvalError, JsonError};
use tidb_chunk::row::Row;
/// A checked Draft 2019-09 schema. External references are resolved only
/// when a document is validated, using the caller's retrieval policy.
#[derive(Debug)]
pub struct PreparedJsonSchema {
    schema: serde_json::Value,
    validator: std::sync::OnceLock<std::result::Result<jsonschema::Validator, String>>,
}

impl PreparedJsonSchema {
    /// Check the schema before demanding its document.
    pub fn new(schema: serde_json::Value) -> std::result::Result<Self, String> {
        if !matches!(
            schema,
            serde_json::Value::Object(_) | serde_json::Value::Bool(_)
        ) {
            return Err("object".into());
        }
        jsonschema::draft201909::meta::validate(&schema).map_err(|error| error.to_string())?;
        Ok(Self {
            schema,
            validator: std::sync::OnceLock::new(),
        })
    }

    /// Build the validator on first use, then reuse it and any build error.
    pub fn validate<F>(
        &self,
        document: &serde_json::Value,
        make_retriever: impl FnOnce() -> std::result::Result<F, String>,
    ) -> std::result::Result<bool, String>
    where
        F: Fn(
                &str,
                &str,
                &str,
            )
                -> std::result::Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>>
            + Send
            + Sync
            + 'static,
    {
        self.validator
            .get_or_init(|| {
                jsonschema::options()
                    .with_draft(jsonschema::Draft::Draft201909)
                    // TiDB's source validator treats format as an assertion.
                    .should_validate_formats(true)
                    .with_retriever(SchemaRetriever(make_retriever()?))
                    .build(&self.schema)
                    .map_err(|error| error.to_string())
            })
            .as_ref()
            .map(|validator| validator.is_valid(document))
            .map_err(Clone::clone)
    }
}

struct SchemaRetriever<F>(F);

impl<F> jsonschema::Retrieve for SchemaRetriever<F>
where
    F: Fn(
            &str,
            &str,
            &str,
        )
            -> std::result::Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>>
        + Send
        + Sync,
{
    fn retrieve(
        &self,
        uri: &jsonschema::Uri<String>,
    ) -> std::result::Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
        (self.0)(uri.scheme().as_str(), uri.as_str(), uri.path().as_str())
    }
}

/// Per-signature cache and lazy evaluator for `JSON_SCHEMA_VALID`.
///
/// Clone deliberately starts empty, matching Go's
/// `builtinJSONSchemaValidSig.Clone`. The cache is keyed by the statement
/// context and admits `ConstOnlyInContext`, matching Go's
/// `builtinFuncCache[jsonschema.Schema]` consumer.
#[derive(Debug, Default)]
struct JsonSchemaCache(BuiltinFuncCache<Option<PreparedJsonSchema>>);

impl JsonSchemaCache {
    fn prepare(
        &self,
        value: &Datum,
        cache: bool,
        ctx: &dyn Columns,
    ) -> Result<Arc<Option<PreparedJsonSchema>>, EvalError> {
        if value.is_null() {
            return Ok(Arc::new(None));
        }
        if cache {
            self.0
                .get_or_init_cache(ctx.context_id(), || prepare_json_schema(value))
        } else {
            prepare_json_schema(value).map(Arc::new)
        }
    }
}

/// Schema and document are separate ordered operands of one shared program.
/// The prepared row value stays private to that program. A new schema visit
/// replaces any state left by a previous document error.
pub(crate) struct JsonSchemaExecution {
    constant_schema: bool,
    cache: JsonSchemaCache,
    current: Mutex<Option<Arc<Option<PreparedJsonSchema>>>>,
}

impl JsonSchemaExecution {
    pub(crate) fn new(constant_schema: bool) -> Self {
        Self {
            constant_schema,
            cache: JsonSchemaCache::default(),
            current: Mutex::new(None),
        }
    }

    pub(crate) fn prepare_operand(
        &self,
        value: &Datum,
        ctx: &dyn Columns,
    ) -> Result<Datum, EvalError> {
        let schema = self.cache.prepare(value, self.constant_schema, ctx)?;
        let result = if schema.is_some() {
            Datum::Int(1)
        } else {
            Datum::Null
        };
        *self
            .current
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(schema);
        Ok(result)
    }

    pub(crate) fn validate_operand(&self, document: &Datum) -> Result<Datum, EvalError> {
        let prepared = self
            .current
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
            .ok_or(EvalError::Unsupported(
                "JSON schema operand was not prepared",
            ))?;
        match prepared.as_ref().as_ref() {
            Some(schema) => validate_json_schema(schema, document),
            None => Ok(Datum::Null),
        }
    }
}

/// Parses and checks the schema argument without resolving external `$ref`s.
/// TiDB's qri-io validator does not fetch them until document validation, so a
/// NULL document still short-circuits without filesystem or network access.
fn prepare_json_schema(value: &Datum) -> Result<Option<PreparedJsonSchema>, EvalError> {
    let Some(schema) = parse_json_document_argument(value)? else {
        return Ok(None);
    };
    PreparedJsonSchema::new(schema)
        .map(Some)
        .map_err(invalid_json_schema)
}

/// TiDB owns file and HTTP(S) retrieval. The shared validator has no default
/// network or filesystem resolver.
fn schema_retriever() -> Result<
    impl Fn(&str, &str, &str) -> Result<Json, Box<dyn std::error::Error + Send + Sync>> + Send + Sync,
    String,
> {
    let client = reqwest::blocking::Client::builder()
        .no_proxy()
        .build()
        .map_err(|error| error.to_string())?;
    Ok(move |scheme: &str, uri: &str, path: &str| match scheme {
        "http" | "https" => Ok(client.get(uri).send()?.json()?),
        "file" => Ok(serde_json::from_reader::<_, Json>(std::fs::File::open(
            path,
        )?)?),
        scheme => Err(format!("unsupported schema URI scheme: {scheme}").into()),
    })
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
    schema
        .validate(&document, schema_retriever)
        .map(|valid| Datum::Int(i64::from(valid)))
        .map_err(invalid_json_schema)
}

/// Datum-level `JSON_SCHEMA_VALID(schema, document)` used by callers that do
/// not own a reusable scalar-function node.
pub(super) fn json_schema_valid(values: &[Datum]) -> Result<Datum, EvalError> {
    let Some(schema) = prepare_json_schema(&values[0])? else {
        return Ok(Datum::Null);
    };
    validate_json_schema(&schema, &values[1])
}

/// `JSON_VALID(arg)`, port of `builtinJSONValid{JSON,String,Others}Sig`.
/// String arguments are JSON documents; every non-string, non-JSON SQL value
/// is the Go `Others` signature and therefore returns zero rather than being
/// stringified.  `NULL` propagates.
pub(super) fn json_valid(v: &Datum) -> Result<Datum, EvalError> {
    match v {
        Datum::Null => Ok(Datum::Null),
        // Non-UTF-8 bytes are simply not a JSON document, which is the zero
        // this signature reports rather than a statement error.
        Datum::String(_) | Datum::Bytes(_) => Ok(Datum::Int(i64::from(
            json_sql_string(v)
                .ok()
                .flatten()
                .is_some_and(|text| parse_json(text).is_ok()),
        ))),
        Datum::Int(_) | Datum::UInt(_) | Datum::Decimal(_) | Datum::Real(_) => Ok(Datum::Int(0)),
        Datum::Float32(_)
        | Datum::BinaryLiteral(_)
        | Datum::Duration(_)
        | Datum::Enum(_, _)
        | Datum::Bit(_)
        | Datum::Set(_, _)
        | Datum::Time(_)
        | Datum::Raw(_)
        | Datum::VectorFloat32(_) => Ok(Datum::Int(0)),
        Datum::Json(_) => Ok(Datum::Int(1)),
        Datum::MinNotNull | Datum::MaxValue => {
            Err(EvalError::Unsupported("range sentinel JSON_VALID argument"))
        }
    }
}

/// `JSON_TYPE(json_doc)`, port of `builtinJSONTypeSig.evalString` and
/// `types.BinaryJSON.Type` (`pkg/types/json_binary_functions.go`).
pub(super) fn json_type(v: &Datum) -> Result<Datum, EvalError> {
    if v.is_null() {
        return Ok(Datum::Null);
    }
    // A binary JSON value carries its own type code, and Go answers straight
    // from it (`BinaryJSON.Type()`). Rendering to text first would collapse
    // the typed codes -- DATE/DATETIME/TIME and the Opaque family -- into
    // STRING, which is exactly the names `CAST(.. AS JSON)` now preserves.
    if let Datum::Json(document) = v {
        return document
            .type_name()
            .map(|name| Datum::new_string(name.to_owned()))
            .map_err(|_| EvalError::Json(JsonError::InvalidText));
    }
    let Some(s) = json_document_string(v)?.map(std::borrow::Cow::into_owned) else {
        return Err(crate::EvalError::Json(
            crate::JsonError::InvalidTypeForJson {
                argument: 1,
                function: "json_type",
            },
        ));
    };
    let json = parse_json(&s)?;
    let ty = match json {
        Json::Null => "NULL",
        Json::Bool(_) => "BOOLEAN",
        Json::Number(n) if n.is_i64() => "INTEGER",
        Json::Number(n) if n.is_u64() => "UNSIGNED INTEGER",
        Json::Number(_) => "DOUBLE",
        Json::String(_) => "STRING",
        Json::Array(_) => "ARRAY",
        Json::Object(_) => "OBJECT",
    };
    Ok(Datum::new_string(ty.to_string()))
}

/// `JSON_LENGTH(json_doc [, path])`, port of `builtinJSONLengthSig.evalInt`.
/// As in TiDB, a wildcard/range path is a true SQL error rather than a length
/// of an implicitly auto-wrapped selection.
pub(super) fn json_length(vals: &[Datum]) -> Result<Datum, EvalError> {
    let Some(document) = parse_json_document_argument(&vals[0])? else {
        return Ok(Datum::Null);
    };
    let target = if let Some(path_value) = vals.get(1) {
        let Some(path) = coerce_str(path_value)? else {
            return Ok(Datum::Null);
        };
        let path = parse_path(&path)?;
        if path.could_match_multiple {
            return Err(EvalError::Json(JsonError::InvalidPathMultipleSelection));
        }
        let Some(extracted) = extract(&document, &[path]) else {
            return Ok(Datum::Null);
        };
        extracted
    } else {
        document
    };
    let len = match target {
        Json::Array(values) => values.len(),
        Json::Object(values) => values.len(),
        Json::Null | Json::Bool(_) | Json::Number(_) | Json::String(_) => 1,
    };
    Ok(Datum::Int(len as i64))
}

/// `JSON_KEYS(json_doc [, path])`, port of
/// `builtinJSONKeys{Sig,2ArgsSig}.evalJSON`.  The result is an array of the
/// selected object's keys, in BinaryJSON's byte-sorted object order.  A
/// scalar, array, missing path, or selected non-object is SQL NULL; a path
/// that could select more than one value is an error.
pub(super) fn json_keys(vals: &[Datum]) -> Result<Datum, EvalError> {
    let Some(document) = parse_json_document_argument(&vals[0])? else {
        return Ok(Datum::Null);
    };
    let target = if let Some(path_value) = vals.get(1) {
        let Some(path) = coerce_str(path_value)? else {
            return Ok(Datum::Null);
        };
        let path = parse_path(&path)?;
        if path.could_match_multiple {
            return Err(EvalError::Json(JsonError::InvalidPathMultipleSelection));
        }
        let Some(extracted) = extract(&document, &[path]) else {
            return Ok(Datum::Null);
        };
        extracted
    } else {
        document
    };
    let Json::Object(object) = target else {
        return Ok(Datum::Null);
    };

    // BinaryJSON objects are encoded with keys sorted by their UTF-8 bytes;
    // serde_json may preserve insertion order, so sort explicitly before
    // constructing the result array.
    let mut keys: Vec<&str> = object.keys().map(String::as_str).collect();
    keys.sort_unstable();
    let keys = Json::Array(
        keys.into_iter()
            .map(|key| Json::String(key.to_owned()))
            .collect(),
    );
    binary_json_datum(keys)
}

/// `JSON_SUM_CRC32(json_doc)`, port of `builtinJSONSumCRC32Sig.evalInt` in
/// `pkg/expression/builtin_json.go`.  The Go signature receives a JSON array
/// plus an `ARRAY`-typed `FieldType` carried by the cast expression; the
/// frozen Rust evaluator has no typed JSON datum or FieldType metadata.  The
/// representable text-domain contract therefore accepts homogeneous scalar
/// arrays (numbers or strings), preserving Go's `fmt.Appendf("%v", item)`
/// bytes before each IEEE CRC32 and returning the int64 sum.  The target-type
/// checks (signed/unsigned range, fixed string width, and explicit JSON path
/// extraction) remain an orchestrator boundary rather than guessed defaults.
pub(super) fn json_sum_crc32(value: &Datum) -> Result<Datum, EvalError> {
    let Some(document) = parse_json_document_argument(value)? else {
        return Ok(Datum::Null);
    };
    let Json::Array(values) = document else {
        return Err(EvalError::Unsupported("JSON_SUM_CRC32 requires JSON array"));
    };

    let mut saw_string = false;
    let mut saw_number = false;
    let mut sum = 0_i64;
    for value in values {
        let text = match value {
            Json::String(value) if !saw_number => {
                saw_string = true;
                value
            }
            Json::Number(value) if !saw_string => {
                saw_number = true;
                format_json_sum_number(&value)
            }
            Json::Bool(_) | Json::Null | Json::Array(_) | Json::Object(_) => {
                return Err(EvalError::Unsupported(
                    "JSON_SUM_CRC32 requires scalar array values",
                ));
            }
            Json::String(_) | Json::Number(_) => {
                return Err(EvalError::Unsupported(
                    "JSON_SUM_CRC32 requires homogeneous array values",
                ));
            }
        };
        sum = sum.wrapping_add(i64::from(crc32_ieee(text.as_bytes())));
    }
    Ok(Datum::Int(sum))
}

/// Go's `%v` formatting for the integer/ordinary-double rows used by
/// `TestJSONSumCrc32`: unlike BinaryJSON text, a float integral value is
/// rendered as `1`, not `1.0`.  Rust's shortest `f64` display has the same
/// spelling for these source vectors.
fn format_json_sum_number(number: &Number) -> String {
    if let Some(integer) = number.as_i64() {
        return integer.to_string();
    }
    if let Some(integer) = number.as_u64() {
        return integer.to_string();
    }
    let value = number
        .as_f64()
        .expect("serde JSON numbers are finite f64 here");
    if value == 0.0 {
        return "0".to_string();
    }
    let mut rendered = value.to_string();
    let abs = value.abs();
    if !(1e-4..1e6).contains(&abs) {
        // Rust and Go both provide shortest round-tripping decimals, but
        // their fixed/scientific cutover differs.  Normalize the fixed Rust
        // spelling to Go's `%g`/`%v` threshold and two-digit exponent rule.
        let negative = rendered.starts_with('-');
        if negative {
            rendered.remove(0);
        }
        let (integer, fraction) = rendered
            .split_once('.')
            .map_or((rendered.as_str(), ""), |(integer, fraction)| {
                (integer, fraction)
            });
        let digits = format!("{integer}{fraction}");
        let first = digits
            .bytes()
            .position(|digit| digit != b'0')
            .expect("nonzero float has a significant digit");
        let exponent = if integer != "0" {
            integer.len() as i32 - first as i32 - 1
        } else {
            -(first as i32 - integer.len() as i32 + 1)
        };
        let mut mantissa = digits[first..].trim_end_matches('0').to_string();
        if mantissa.len() > 1 {
            mantissa.insert(1, '.');
        }
        rendered = format!(
            "{}{}e{:+03}",
            if negative { "-" } else { "" },
            mantissa,
            exponent
        );
    }
    rendered
}

/// IEEE CRC32, matching `hash/crc32.ChecksumIEEE` used by the Go builtin.
fn crc32_ieee(bytes: &[u8]) -> u32 {
    let mut crc = 0xFFFF_FFFF_u32;
    for &byte in bytes {
        crc ^= u32::from(byte);
        for _ in 0..8 {
            let mask = (crc & 1).wrapping_neg();
            crc = (crc >> 1) ^ (0xEDB8_8320 & mask);
        }
    }
    !crc
}
