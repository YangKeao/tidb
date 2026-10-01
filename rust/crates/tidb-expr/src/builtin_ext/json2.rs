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

//! JSON depth/storage leaves. They deliberately share `json`'s one textual
//! ETJson boundary rather than creating a second coercion rule.

use super::json::json_document_text_argument;
use crate::{Datum, EvalError};

/// Dispatches this leaf's builtins; `None` if `name` isn't one of them.
pub(crate) fn dispatch_in(
    name: &str,
    vals: &[Datum],
    ctx: &dyn crate::Columns,
) -> Option<Result<Datum, EvalError>> {
    match (name, vals) {
        ("JSON_DEPTH", [value]) => Some(json_depth(value, ctx)),
        ("JSON_STORAGE_FREE", [value]) => Some(json_storage_free(value, ctx)),
        ("JSON_STORAGE_SIZE", [value]) => Some(json_storage_size(value, ctx)),
        _ => None,
    }
}

#[cfg(test)]
pub(crate) fn dispatch(name: &str, vals: &[Datum]) -> Option<Result<Datum, EvalError>> {
    dispatch_in(name, vals, &crate::NoColumns)
}

/// `JSON_DEPTH(json_doc)`, ported from `builtinJSONDepthSig.evalInt` in
/// `pkg/expression/builtin_json.go`. TiDB's `BinaryJSON.GetElemDepth` gives
/// every scalar and empty container depth one; an array/object is one plus
/// the greatest depth of any child.
fn json_depth(value: &Datum, ctx: &dyn crate::Columns) -> Result<Datum, EvalError> {
    use crate::tikv::JsonReportOutcome;

    crate::tikv::evaluate_bytes_in(
        crate::tikv::EvaluatedBytesOp::JsonDepthNative,
        ctx,
        || Ok(json_document_text_argument(value)?.map(String::into_bytes)),
        |computed| match computed.into_json_report()? {
            JsonReportOutcome::Null => Ok(Datum::Null),
            JsonReportOutcome::Int(value) => Ok(Datum::Int(value)),
            JsonReportOutcome::EmptyText => Err(EvalError::Json(crate::JsonError::EmptyText)),
            JsonReportOutcome::InvalidText => Err(EvalError::Json(crate::JsonError::InvalidText)),
            JsonReportOutcome::Bytes(_) => {
                Err(EvalError::Unsupported("JSON_DEPTH result kind mismatch"))
            }
        },
    )
}

/// `JSON_STORAGE_FREE(json_doc)`, ported from `builtinJSONStorageFreeSig` in
/// `pkg/expression/builtin_json.go`. TiDB's binary JSON representation does
/// not reserve free space for a parsed document, so every valid document
/// returns zero; SQL NULL propagates and malformed/non-document arguments
/// remain errors at the shared document boundary.
fn json_storage_free(value: &Datum, ctx: &dyn crate::Columns) -> Result<Datum, EvalError> {
    use crate::tikv::JsonReportOutcome;

    crate::tikv::evaluate_bytes_in(
        crate::tikv::EvaluatedBytesOp::JsonStorageFreeNative,
        ctx,
        || Ok(json_document_text_argument(value)?.map(String::into_bytes)),
        |computed| match computed.into_json_report()? {
            JsonReportOutcome::Null => Ok(Datum::Null),
            JsonReportOutcome::Int(value) => Ok(Datum::Int(value)),
            JsonReportOutcome::EmptyText => Err(EvalError::Json(crate::JsonError::EmptyText)),
            JsonReportOutcome::InvalidText => Err(EvalError::Json(crate::JsonError::InvalidText)),
            JsonReportOutcome::Bytes(_) => Err(EvalError::Unsupported(
                "JSON_STORAGE_FREE result kind mismatch",
            )),
        },
    )
}

/// `JSON_STORAGE_SIZE(json_doc)`, ported from `builtinJSONStorageSizeSig` in
/// `pkg/expression/builtin_json.go`. The Go implementation returns the
/// binary JSON value payload length plus its one-byte root type code. This
/// helper mirrors the source encoder's fixed headers, value entries, inline
/// literal entries, varint string lengths, and recursive payload sizes for
/// the textual JSON domain.
fn json_storage_size(value: &Datum, ctx: &dyn crate::Columns) -> Result<Datum, EvalError> {
    use crate::tikv::JsonReportOutcome;

    crate::tikv::evaluate_bytes_in(
        crate::tikv::EvaluatedBytesOp::JsonStorageSizeNative,
        ctx,
        || Ok(json_document_text_argument(value)?.map(String::into_bytes)),
        |computed| match computed.into_json_report()? {
            JsonReportOutcome::Null => Ok(Datum::Null),
            JsonReportOutcome::Int(value) => Ok(Datum::Int(value)),
            JsonReportOutcome::EmptyText => Err(EvalError::Json(crate::JsonError::EmptyText)),
            JsonReportOutcome::InvalidText => Err(EvalError::Json(crate::JsonError::InvalidText)),
            JsonReportOutcome::Bytes(_) => Err(EvalError::Unsupported(
                "JSON_STORAGE_SIZE result kind mismatch",
            )),
        },
    )
}

#[cfg(test)]
mod tests {
    use super::dispatch;
    use crate::Datum;

    fn document(text: &str) -> Datum {
        Datum::new_string(text.to_string())
    }

    #[test]
    fn json_depth_matches_all_go_shape_vectors() {
        // TestJSONDepth in pkg/expression/builtin_json_test.go.
        let cases = [
            ("null", 1),
            ("true", 1),
            ("false", 1),
            ("1", 1),
            ("-1", 1),
            ("1.1", 1),
            (r#""1""#, 1),
            ("{}", 1),
            ("[]", 1),
            ("[10, 20]", 2),
            ("[[], {}]", 2),
            (r#"{"Name": "Homer"}"#, 2),
            (r#"[10, {"a": 20}]"#, 3),
            (
                r#"{"Person": {"Name": "Homer", "Age": 39, "Hobbies": ["Eating", "Sleeping"]}}"#,
                4,
            ),
            (r#"{"a":1}"#, 2),
            (r#"{"a":[1]}"#, 3),
            (r#"{"b":2, "c":3}"#, 2),
            ("[1]", 2),
            ("[1,2]", 2),
            ("[1,2,[1,3]]", 3),
            (r#"[1,2,[1,[5,[3]]]]"#, 5),
            (r#"[1,2,[1,[5,{"a":[2,3]}]]]"#, 6),
            (r#"[{"a":1}]"#, 3),
            (r#"[{"a":1,"b":2}]"#, 3),
            (r#"[{"a":{"a":1},"b":2}]"#, 4),
        ];
        for (input, want) in cases {
            assert_eq!(
                dispatch("JSON_DEPTH", &[document(input)])
                    .expect("JSON_DEPTH must dispatch")
                    .expect("valid JSON must evaluate"),
                Datum::Int(want),
                "JSON_DEPTH({input})"
            );
        }
        assert_eq!(
            dispatch("JSON_DEPTH", &[Datum::Null])
                .expect("JSON_DEPTH must dispatch")
                .expect("NULL must evaluate"),
            Datum::Null
        );
        assert!(dispatch("JSON_DEPTH", &[document("a")])
            .expect("JSON_DEPTH must dispatch")
            .is_err());
        // go's implicit scalar→document cast: `JSON_DEPTH(1)` is 1 on the
        // oracle (the numeric scalar coerces to the JSON number 1), not an
        // error.
        assert_eq!(
            dispatch("JSON_DEPTH", &[Datum::Int(1)])
                .expect("JSON_DEPTH must dispatch")
                .expect("scalar depth evaluates"),
            Datum::Int(1)
        );
    }

    #[test]
    fn json_storage_free_matches_go_vectors() {
        // TestJSONStorageFree in pkg/expression/builtin_json_test.go.
        for input in [
            "null",
            "true",
            "1",
            r#""1""#,
            "{}",
            r#"{"a":1}"#,
            r#"[{"a":{"a":1},"b":2}]"#,
            r#"{"a": 1000, "b": "wxyz", "c": "[1, 3, 5, 7]"}"#,
        ] {
            assert_eq!(
                dispatch("JSON_STORAGE_FREE", &[document(input)])
                    .expect("JSON_STORAGE_FREE must dispatch")
                    .expect("valid JSON must evaluate"),
                Datum::Int(0),
                "JSON_STORAGE_FREE({input})"
            );
        }
        assert_eq!(
            dispatch("JSON_STORAGE_FREE", &[Datum::Null])
                .expect("JSON_STORAGE_FREE must dispatch")
                .expect("NULL must evaluate"),
            Datum::Null
        );
        for input in [r#"[{"a":1]"#, r#"[{a":1]"#] {
            assert!(dispatch("JSON_STORAGE_FREE", &[document(input)])
                .expect("JSON_STORAGE_FREE must dispatch")
                .is_err());
        }
    }

    #[test]
    fn json_storage_size_matches_go_vectors() {
        // TestJSONStorageSize in pkg/expression/builtin_json_test.go.
        for (input, want) in [
            ("null", 2),
            ("true", 2),
            ("1", 9),
            (r#""1""#, 3),
            ("{}", 9),
            (r#"{"a":1}"#, 29),
            (r#"[{"a":{"a":1},"b":2}]"#, 82),
            (r#"{"a": 1000, "b": "wxyz", "c": "[1, 3, 5, 7]"}"#, 71),
        ] {
            assert_eq!(
                dispatch("JSON_STORAGE_SIZE", &[document(input)])
                    .expect("JSON_STORAGE_SIZE must dispatch")
                    .expect("valid JSON must evaluate"),
                Datum::Int(want),
                "JSON_STORAGE_SIZE({input})"
            );
        }
        assert_eq!(
            dispatch("JSON_STORAGE_SIZE", &[Datum::Null])
                .expect("JSON_STORAGE_SIZE must dispatch")
                .expect("NULL must evaluate"),
            Datum::Null
        );
        for input in [r#"[{"a":1]"#, r#"[{a":1]"#] {
            assert!(dispatch("JSON_STORAGE_SIZE", &[document(input)])
                .expect("JSON_STORAGE_SIZE must dispatch")
                .is_err());
        }
        assert!(dispatch("JSON_STORAGE_SIZE", &[]).is_none());
    }
}
