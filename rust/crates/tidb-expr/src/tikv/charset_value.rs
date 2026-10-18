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

//! Actual charset values under independent comparison with the old workers.
use super::*;
use tidb_query_expr::{NativeCharsetValue as Value, NativeCharsetValueError};
fn resource(reason: &'static str) -> EvalError {
    EvalError::ExpressionRuntimeFailure(super::super::ExpressionRuntimeFailure::from_local_eval(
        super::super::LocalError::ResourceLimit(reason.into()),
        None,
    ))
}
fn value_error(error: NativeCharsetValueError) -> EvalError {
    match error {
        NativeCharsetValueError::Invalid => invalid_report(),
        NativeCharsetValueError::ResourceLimit(reason) => resource(reason),
    }
}
fn project_value(value: Value, target: Option<&str>, maximum: usize) -> Result<Datum, EvalError> {
    match value {
        Value::Bytes(bytes) => Ok(Datum::new_bytes(bytes)),
        Value::RetagString(bytes) => {
            let target = target.ok_or_else(invalid_report)?;
            let overlap = bytes
                .capacity()
                .checked_add(bytes.len())
                .ok_or_else(|| resource("charset string publication overlap overflow"))?;
            if maximum == usize::MAX || overlap > maximum {
                return Err(resource(
                    "charset string publication exceeds value allowance",
                ));
            }
            // This presentation metadata observes the live process mode AFTER
            // actual codec computation, never during factory preparation.
            let collation = Charset::from_name(target)
                .map_or(Collation::DEFAULT, |charset| charset.default_collation());
            Ok(Datum::new_collation_string(bytes, collation))
        }
        Value::Null if target.is_some() => Ok(Datum::Null),
        Value::InvalidCharacter => Err(EvalError::Unsupported("invalid character string")),
        Value::UnknownCharset if target.is_some() => {
            Err(EvalError::Unsupported("unknown character set"))
        }
        _ => Err(invalid_report()),
    }
}
pub(crate) fn to_binary_bounded(
    value: &Datum,
    charset: &str,
    maximum: usize,
) -> Result<Datum, EvalError> {
    let value = crate::arg_eval_type::eval_string_ref(value)?;
    project_value(
        tidb_query_expr::native_to_binary_value(value, charset, maximum).map_err(value_error)?,
        None,
        maximum,
    )
}
pub(crate) fn from_binary_bounded(
    value: &Datum,
    charset: &str,
    maximum: usize,
) -> Result<Datum, EvalError> {
    let value = crate::arg_eval_type::eval_string_ref(value)?;
    project_value(
        tidb_query_expr::native_from_binary_value(value, charset, maximum).map_err(value_error)?,
        None,
        maximum,
    )
}
pub(crate) fn convert_using_bounded(
    value: &Datum,
    arg_type: &FieldType,
    target: &str,
    maximum: usize,
) -> Result<Datum, EvalError> {
    // Original direct API target metadata precedes its ETString reader. The
    // scalar SOURCE API's target/coercion/cast/context phases stay outside it.
    if !is_supported_encoding(target) {
        return Err(EvalError::Unsupported("unknown character set"));
    }
    let value = crate::arg_eval_type::eval_string_ref(value)?;
    project_value(
        tidb_query_expr::native_convert_using_value(
            value,
            arg_type.charset_name(),
            arg_type.charset().name(),
            target,
            maximum,
        )
        .map_err(value_error)?,
        Some(target),
        maximum,
    )
}
pub(crate) fn source_null() -> Result<Datum, EvalError> {
    match tidb_query_expr::native_charset_null_value() {
        Value::Null => Ok(Datum::Null),
        _ => Err(invalid_report()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn charset_known_errors_precede_skipped_reader_and_buffers_are_finite() {
        let field =
            FieldType::new(tidb_datatype::FieldTypeCode::VarString).with_charset_name("utf8mb4");
        assert_eq!(
            convert_using_bounded(&Datum::Int(1), &field, "GBK", 0),
            Err(EvalError::Unsupported("unknown character set"))
        );
        assert_eq!(
            convert_using_bounded(&Datum::Int(1), &field, "gbk", 0),
            Err(EvalError::Unsupported("un-cast types.ETString argument"))
        );
        assert_eq!(
            to_binary_bounded(&Datum::Null, "gbk", 0),
            Ok(Datum::Bytes(vec![]))
        );
        assert_eq!(source_null(), Ok(Datum::Null));
        assert_eq!(
            to_binary_bounded(&Datum::Bytes(vec![255]), "UNKNOWN", 1),
            Ok(Datum::Bytes(vec![255]))
        );
        let big = Datum::Bytes(vec![b'A'; 16384]);
        for result in [
            to_binary_bounded(&big, "binary", 4096),
            convert_using_bounded(&Datum::new_string("abc"), &field, "utf8mb4", 3),
        ] {
            let Err(EvalError::ExpressionRuntimeFailure(error)) = result else {
                panic!("resource refusal lost")
            };
            assert_eq!(
                error.class(),
                crate::ExpressionRuntimeFailureClass::ResourceLimit
            );
            assert_eq!(error.phase(), None);
        }
        // Retag publication needs its actual byte owner overlap, not a report.
        assert!(convert_using_bounded(&Datum::new_string("abc"), &field, "utf8mb4", 6).is_ok());
        assert_eq!(
            to_binary_bounded(&Datum::new_string("A"), "binary", 1),
            Ok(Datum::Bytes(b"A".to_vec()))
        );
    }
}
