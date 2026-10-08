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

#[cfg(test)]
#[test]
fn json_sum_crc32_entries_preserve_internal_domain_and_context_demand() {
    use crate::constant::{Constant, ParamMarker};
    use crate::scalar_function::ScalarFunction;
    use std::cell::RefCell;
    use tidb_datatype::{BinaryJSON, DateModes, FieldType, FieldTypeCode, SessionTimeZone};
    struct Demand {
        values: Vec<Datum>,
        fail: Option<usize>,
        reads: RefCell<Vec<usize>>,
    }
    impl Columns for Demand {
        fn get(&self, path: &[String]) -> Option<Datum> {
            self.param_value(path[0].parse().unwrap()).ok()
        }
        fn param_value(&self, index: usize) -> Result<Datum, EvalError> {
            self.reads.borrow_mut().push(index);
            if self.fail == Some(index) {
                return Err(EvalError::Unsupported("JSON_SUM_CRC32 child"));
            }
            Ok(self.values[index].clone())
        }
        fn date_modes(&self) -> DateModes {
            panic!("checksum must not read date modes")
        }
        fn time_zone(&self) -> SessionTimeZone {
            panic!("checksum must not read timezone")
        }
        fn truncate_level(&self) -> crate::ErrorLevel {
            panic!("checksum must not read truncation policy")
        }
        fn append_warning(&self, _: u16, _: &str) {
            panic!("checksum must not append warnings")
        }
    }
    let context = |values, fail| Demand {
        values,
        fail,
        reads: RefCell::new(Vec::new()),
    };
    let text = |s: &str| Datum::new_string(s);
    let row = tidb_chunk::mutrow::MutRow::from_datums(&[]);
    let evaluate = |mode, values: &[Datum], columns: &dyn Columns| {
        if mode == 0 {
            return super::dispatch_in("JSON_SUM_CRC32", values, columns)
                .expect("one-argument internal domain");
        }
        if mode == 1 {
            let args = (0..values.len())
                .map(|i| tidb_ast::Expr::Column(vec![i.to_string()]))
                .collect::<Vec<_>>();
            return crate::func::eval_func("JSON_SUM_CRC32", &args, columns, None);
        }
        let args = values
            .iter()
            .enumerate()
            .map(|(index, value)| {
                let code = match value {
                    Datum::Json(_) => FieldTypeCode::Json,
                    Datum::Int(_) => FieldTypeCode::LongLong,
                    Datum::Float32(_) => FieldTypeCode::Float,
                    _ => FieldTypeCode::VarString,
                };
                let mut constant = Constant::new(Datum::Null, FieldType::new(code));
                constant.param_marker = Some(ParamMarker {
                    order: i64::try_from(index).unwrap(),
                });
                Expression::Constant(constant)
            })
            .collect();
        // Ordinary scalar metadata only: no invented ARRAY conversion target.
        ScalarFunction::new(
            tidb_ast::CiString::new("json_sum_crc32"),
            FieldType::new(FieldTypeCode::LongLong),
            args,
        )
        .eval(columns, row.to_row())
    };
    let owner = |slots| {
        crate::ReadyValuePoolOwner::new(
            crate::ReadyValuePoolPolicy::checked(
                slots,
                slots,
                16 * 1024 * 1024,
                4 * 1024 * 1024,
                4 * 1024 * 1024,
                64,
                8,
                4 * 1024 * 1024,
            )
            .unwrap(),
        )
        .unwrap()
    };
    let pool = owner(1);
    let execution = pool.begin_execution().unwrap();
    for mode in 0..3 {
        for (value, expected) in [
            (Datum::Null, Ok(Datum::Null)),
            (text("[]"), Ok(Datum::Int(0))),
            (text("[1,2,3]"), Ok(Datum::Int(4_505_025_631))),
            (
                Datum::Json(BinaryJSON::parse("[1,2,3]").unwrap()),
                Ok(Datum::Int(4_505_025_631)),
            ),
            (
                text("null"),
                Err(EvalError::Unsupported("JSON_SUM_CRC32 requires JSON array")),
            ),
            (
                Datum::Int(1),
                Err(EvalError::Unsupported("JSON_SUM_CRC32 requires JSON array")),
            ),
            (
                Datum::Float32(1.0),
                Err(EvalError::Unsupported(
                    "JSON document requires JSON or string",
                )),
            ),
            (
                Datum::MinNotNull,
                Err(EvalError::Unsupported("JSON document requires string")),
            ),
            (text(""), Err(EvalError::Json(JsonError::EmptyText))),
            (text("["), Err(EvalError::Json(JsonError::InvalidText))),
            (
                Datum::new_bytes(vec![255]),
                Err(EvalError::Unsupported("invalid UTF-8 string datum")),
            ),
            (
                text(r#"[true,"x",1]"#),
                Err(EvalError::Unsupported(
                    "JSON_SUM_CRC32 requires scalar array values",
                )),
            ),
            (
                text(r#"["x",1,true]"#),
                Err(EvalError::Unsupported(
                    "JSON_SUM_CRC32 requires homogeneous array values",
                )),
            ),
            (
                text(r#"[1,"x",false]"#),
                Err(EvalError::Unsupported(
                    "JSON_SUM_CRC32 requires homogeneous array values",
                )),
            ),
            (
                text(r#"[1,false,"x"]"#),
                Err(EvalError::Unsupported(
                    "JSON_SUM_CRC32 requires scalar array values",
                )),
            ),
        ] {
            let ctx = context(vec![value], None);
            assert_eq!(
                execution.scope().with_columns(&ctx, |columns| evaluate(
                    mode,
                    &ctx.values,
                    columns
                )),
                expected
            );
            assert_eq!(
                *ctx.reads.borrow(),
                if mode == 0 { vec![] } else { vec![0] }
            );
        }
        let checksum = |document: &str| {
            let ctx = context(vec![text(document)], None);
            execution
                .scope()
                .with_columns(&ctx, |columns| evaluate(mode, &ctx.values, columns))
                .unwrap()
        };
        assert_eq!(checksum("[1000000]"), checksum(r#"["1000000"]"#));
        assert_eq!(checksum("[1000000.0]"), checksum(r#"["1e+06"]"#));
        assert_ne!(checksum("[1000000]"), checksum("[1000000.0]"));
        assert_eq!(checksum("[-0.0,0.0]"), checksum(r#"["0","0"]"#));
        let ctx = context(
            vec![Datum::Json(BinaryJSON::parse(r#"["é","😀"]"#).unwrap())],
            None,
        );
        // End the first scope before another single-slot scope is requested;
        // an assert_eq! operand temporary otherwise parks its lease until the
        // whole assertion ends (ReadyValueScope::Drop returns it to the pool).
        let unicode_actual = execution
            .scope()
            .with_columns(&ctx, |columns| evaluate(mode, &ctx.values, columns))
            .unwrap();
        assert_eq!(unicode_actual, checksum(r#"["é","😀"]"#));
    }
    for mode in [1, 2] {
        // Even with a NULL first operand and unsupported arity, the existing
        // eager caller still evaluates its suffix before rejecting the call.
        let ctx = context(vec![Datum::Null, text("[]")], Some(1));
        let result = execution
            .scope()
            .with_columns(&ctx, |columns| evaluate(mode, &ctx.values, columns));
        if mode == 1 {
            assert!(result.is_err());
        } else {
            assert_eq!(result, Err(EvalError::Unsupported("JSON_SUM_CRC32 child")));
        }
        assert_eq!(*ctx.reads.borrow(), vec![0, 1]);
    }
    let ctx = context(vec![], None);
    for values in [vec![], vec![Datum::Null, text("[]")]] {
        assert!(execution
            .scope()
            .with_columns(&ctx, |columns| super::dispatch_in(
                "JSON_SUM_CRC32",
                &values,
                columns
            ))
            .is_none());
    }
    assert!(ctx.reads.borrow().is_empty());
    let denied_pool = owner(0);
    let denied = denied_pool.begin_execution().unwrap();
    for mode in 0..3 {
        for value in [Datum::Null, text("[]"), text("null")] {
            let ctx = context(vec![value], None);
            assert!(
                matches!(denied.scope().with_columns(&ctx, |columns| evaluate(mode, &ctx.values, columns)),
                Err(EvalError::ExpressionAdapterFailure(failure))
                    if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource)
            );
            assert_eq!(
                *ctx.reads.borrow(),
                if mode == 0 { vec![] } else { vec![0] }
            );
        }
        let ctx = context(vec![Datum::new_bytes(vec![255])], None);
        assert_eq!(
            denied
                .scope()
                .with_columns(&ctx, |columns| evaluate(mode, &ctx.values, columns)),
            Err(EvalError::Unsupported("invalid UTF-8 string datum"))
        );
    }
}
