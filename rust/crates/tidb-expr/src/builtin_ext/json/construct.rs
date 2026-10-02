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

//! Building JSON out of SQL values: `JSON_ARRAY`, `JSON_OBJECT`,
//! `JSON_QUOTE`, `JSON_UNQUOTE`.
//!
//! Mirrors `builtinJSON{Array,Object,Quote,Unquote}Sig` in
//! `pkg/expression/builtin_json.go` and `types.UnquoteString` in
//! `pkg/types/json_binary_functions.go`.
//!
//! `JSON_QUOTE`/`JSON_UNQUOTE` are the string<->JSON pair and are NOT
//! inverses across the whole domain: quote demands a string argument and
//! escapes it, while unquote passes through anything that is not a complete
//! double-quoted JSON string. The constructors take VALUE arguments, so
//! `JSON_ARRAY('[1]')` is a one-element array holding the string `"[1]"`.

use super::value::{json_argument, json_sql_string, StringArgument};
use crate::coerce::coerce_str;
use crate::{Datum, EvalError, JsonError};
use tidb_datatype::FieldType;

/// `JSON_QUOTE(str)` preserves the existing native serde JSON escaping:
/// HTML characters and U+2028/U+2029 separators remain unescaped. The shared
/// kernel keeps this policy distinct from wire escaping; this is not a claim
/// of complete Go escaping equivalence.
pub(super) fn json_quote(v: &Datum, ctx: &dyn crate::Columns) -> Result<Datum, EvalError> {
    crate::tikv::evaluate_bytes_in(
        crate::tikv::EvaluatedBytesOp::JsonQuoteNative,
        ctx,
        || {
            if let Some(text) = json_sql_string(v)? {
                return Ok(Some(text.as_bytes().to_vec()));
            }
            match v {
                Datum::Null => Ok(None),
                _ => Err(EvalError::Json(JsonError::IncorrectType {
                    argument: 1,
                    function: "json_quote",
                })),
            }
        },
        |computed| {
            Ok(computed
                .into_bytes()?
                .map_or(Datum::Null, Datum::new_string))
        },
    )
}

/// SQL text uses the shared strict quote-bounded policy; a direct BinaryJSON
/// string retains its decoded bytes verbatim, without SDK second unescaping.
/// Only input validation precedes admission. The worker owns the final text,
/// including raw non-string formatting and its original panic behavior.
pub(super) fn json_unquote(v: &Datum, ctx: &dyn crate::Columns) -> Result<Datum, EvalError> {
    use crate::tikv::{EvaluatedArgs, EvaluatedBytesOp as Op, NativeJsonError};
    crate::tikv::evaluate_prepared_args_in(
        ctx,
        || {
            if let Datum::Json(document) = v {
                if let Some(bytes) = document.as_string() {
                    std::str::from_utf8(bytes)
                        .map_err(|_| EvalError::Unsupported("invalid UTF-8 JSON string"))?;
                }
                return Ok((
                    Op::JsonUnquoteBinaryNative,
                    crate::tikv::prepare_json_binary_args(document)?,
                ));
            }
            let Some(text) = json_sql_string(v)? else {
                return if *v == Datum::Null {
                    Ok((Op::JsonOutputNullNative, EvaluatedArgs::NullWitness(None)))
                } else {
                    Err(EvalError::Unsupported("JSON_UNQUOTE requires string"))
                };
            };
            tidb_query_expr::validate_native_json_unquote_text(text).map_err(|error| {
                EvalError::Json(match error {
                    NativeJsonError::EmptyText => JsonError::EmptyText,
                    NativeJsonError::InvalidText | NativeJsonError::InvalidBinary => {
                        JsonError::InvalidText
                    }
                })
            })?;
            Ok((
                Op::JsonUnquoteTextNative,
                EvaluatedArgs::Bytes(Some(text.as_bytes().to_vec())),
            ))
        },
        |computed| {
            Ok(computed
                .into_bytes()?
                .map_or(Datum::Null, Datum::new_string))
        },
    )
}

/// `JSON_ARRAY(value [, value] ...)`, port of `jsonArrayFunctionClass` and
/// `builtinJSONArraySig` in `pkg/expression/builtin_json.go`.  SQL strings
/// remain JSON strings, while numeric and NULL datums become their matching
/// JSON scalar values. Boolean and binary-source policies come from the
/// original argument field types, never an inference from its printed value.
pub(super) fn json_array_in(
    vals: &[Datum],
    arg_types: &[Option<FieldType>],
    ctx: &dyn crate::Columns,
) -> Result<Datum, EvalError> {
    crate::tikv::evaluate_prepared_args_in(
        ctx,
        || {
            // These are ordered input values, including real JSON nulls, not
            // the final array. The worker owns construction, including [].
            let values = vals
                .iter()
                .zip(arg_types.iter())
                .map(|(v, ft)| json_argument(v, StringArgument::Value, ft.as_ref()))
                .collect::<Result<Vec<_>, _>>()?;
            Ok((
                crate::tikv::EvaluatedBytesOp::JsonArraySerdeNative,
                crate::tikv::prepare_json_array_args(&values)?,
            ))
        },
        crate::tikv::EvaluatedBytesResult::into_json_datum,
    )
}

/// `JSON_OBJECT(key, value [, key, value] ...)`, port of
/// `jsonObjectFunctionClass` and `builtinJSONObjectSig` in
/// `pkg/expression/builtin_json.go`.  Keys are SQL-string-coerced, NULL keys
/// are rejected, and values follow the scalar JSON value boundary used by
/// `JSON_ARRAY`.
pub(super) fn json_object_in(
    vals: &[Datum],
    arg_types: &[Option<FieldType>],
    ctx: &dyn crate::Columns,
) -> Result<Datum, EvalError> {
    crate::tikv::evaluate_prepared_args_in(
        ctx,
        || {
            if !vals.len().is_multiple_of(2) {
                return Err(EvalError::Unsupported(
                    "JSON_OBJECT requires key/value pairs",
                ));
            }
            let mut pairs = Vec::new();
            for (pair, types) in vals
                .as_chunks::<2>()
                .0
                .iter()
                .zip(arg_types.as_chunks::<2>().0)
            {
                let Some(key) = coerce_str(&pair[0])? else {
                    return Err(EvalError::Json(JsonError::NullMemberName));
                };
                let value = json_argument(&pair[1], StringArgument::Value, types[1].as_ref())?;
                // Preserve duplicates and source order as actual operands;
                // only the worker constructs the last-key-wins object.
                pairs.push((key, value));
            }
            Ok((
                crate::tikv::EvaluatedBytesOp::JsonObjectSerdeNative,
                crate::tikv::prepare_json_object_args(&pairs)?,
            ))
        },
        crate::tikv::EvaluatedBytesResult::into_json_datum,
    )
}

// Preserve the original direct-test API without retaining a host implementation.
#[cfg(test)]
fn json_array(vals: &[Datum], arg_types: &[Option<FieldType>]) -> Result<Datum, EvalError> {
    json_array_in(vals, arg_types, &crate::NoColumns)
}

#[cfg(test)]
fn json_object(vals: &[Datum], arg_types: &[Option<FieldType>]) -> Result<Datum, EvalError> {
    json_object_in(vals, arg_types, &crate::NoColumns)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_datatype::{FieldTypeCode, FieldTypeFlags};

    fn boolean_int() -> FieldType {
        let mut ft = FieldType::new(FieldTypeCode::LongLong);
        ft.set_flen(1);
        ft.add_flags(FieldTypeFlags::IS_BOOLEAN);
        ft
    }

    /// A `booleanFunctions` result (here the `IS_BOOLEAN` flag stands in for the
    /// static type of a `1<2`/`IN`/`IS NULL`/`IS_IPV4` argument) becomes a JSON
    /// `true`/`false` literal, exactly as `builtinCastIntAsJSONSig.evalJSON`
    /// does. A plain integer -- the `1+1`, `IS_UUID`, or untyped row/AST case
    /// Go leaves OUT of the boolean map -- keeps its numeric rendering, so the
    /// fix cannot silently turn every integer into a boolean.
    #[test]
    fn a_boolean_flagged_int_is_a_json_literal_and_a_plain_int_is_a_number() {
        let one = Datum::Int(1);
        let zero = Datum::Int(0);
        assert_eq!(
            json_array(
                &[one.clone(), zero.clone()],
                &[Some(boolean_int()), Some(boolean_int())],
            )
            .unwrap(),
            Datum::Json(tidb_datatype::BinaryJSON::parse("[true, false]").expect("fixture parses"),),
        );
        // Same values, no boolean flag: the numeric rendering is unchanged.
        assert_eq!(
            json_array(
                &[one.clone(), zero.clone()],
                &[
                    Some(FieldType::new(FieldTypeCode::LongLong)),
                    Some(FieldType::new(FieldTypeCode::LongLong)),
                ],
            )
            .unwrap(),
            Datum::Json(tidb_datatype::BinaryJSON::parse("[1, 0]").expect("fixture parses"),),
        );
        // The untyped row/AST path (no field type) also keeps the number.
        assert_eq!(
            json_array(&[one], &[None]).unwrap(),
            Datum::Json(tidb_datatype::BinaryJSON::parse("[1]").expect("fixture parses")),
        );
        // JSON_OBJECT threads the same value coercion for its values.
        assert_eq!(
            json_object(
                &[Datum::new_string("k".to_owned()), zero],
                &[None, Some(boolean_int())],
            )
            .unwrap(),
            Datum::Json(
                tidb_datatype::BinaryJSON::parse("{\"k\": false}").expect("fixture parses"),
            ),
        );
    }
}
