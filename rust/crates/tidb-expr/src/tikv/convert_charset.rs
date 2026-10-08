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

use tidb_datatype::{Charset, Collation, FieldType};
use tidb_query_datatype::codec::collation::native_encoding::is_supported_encoding;
use tidb_query_expr::{decode_native_convert_charset_result, NativeConvertCharsetResult};

use super::adapter_failure::{ExpressionAdapterFailure, ScopeFailureKind};
use super::{evaluate_args_in, EvaluatedArgs, EvaluatedBytesOp, EvaluatedBytesResult};
use crate::{Columns, Datum, EvalError};

fn invalid_report() -> EvalError {
    EvalError::ExpressionAdapterFailure(ExpressionAdapterFailure::from_scope(
        ScopeFailureKind::Contract,
        "invalid native charset conversion report",
    ))
}

fn project_report(
    computed: EvaluatedBytesResult,
    convert_target: Option<&str>,
) -> Result<Datum, EvalError> {
    let EvaluatedBytesResult::Bytes(report) = computed else {
        return Err(invalid_report());
    };
    let Some(report) = report else {
        return if convert_target.is_some() {
            Ok(Datum::Null)
        } else {
            Err(invalid_report())
        };
    };
    match decode_native_convert_charset_result(&report).ok_or_else(invalid_report)? {
        NativeConvertCharsetResult::Bytes(bytes) => Ok(Datum::new_bytes(bytes.to_vec())),
        NativeConvertCharsetResult::RetagString(bytes) => {
            let target = convert_target.ok_or_else(invalid_report)?;
            // GB default collations depend on the live process mode. Resolve
            // this presentation metadata only after the actual worker reply.
            let collation = Charset::from_name(target)
                .map_or(Collation::DEFAULT, |charset| charset.default_collation());
            Ok(Datum::new_collation_string(bytes.to_vec(), collation))
        }
        NativeConvertCharsetResult::InvalidCharacter => {
            Err(EvalError::Unsupported("invalid character string"))
        }
        NativeConvertCharsetResult::UnknownCharset if convert_target.is_some() => {
            Err(EvalError::Unsupported("unknown character set"))
        }
        NativeConvertCharsetResult::UnknownCharset => Err(invalid_report()),
    }
}

fn eval_binary_in(
    ctx: &dyn Columns,
    value: &Datum,
    charset: &str,
    operation: EvaluatedBytesOp,
) -> Result<Datum, EvalError> {
    evaluate_args_in(
        operation,
        ctx,
        || {
            Ok(EvaluatedArgs::Bytes2(
                crate::arg_eval_type::eval_string(value)?,
                Some(charset.as_bytes().to_vec()),
            ))
        },
        |computed| project_report(computed, None),
    )
}

pub(crate) fn eval_to_binary_in(
    ctx: &dyn Columns,
    value: &Datum,
    charset: &str,
) -> Result<Datum, EvalError> {
    eval_binary_in(ctx, value, charset, EvaluatedBytesOp::ToBinaryNative)
}

pub(crate) fn eval_from_binary_in(
    ctx: &dyn Columns,
    value: &Datum,
    charset: &str,
) -> Result<Datum, EvalError> {
    eval_binary_in(ctx, value, charset, EvaluatedBytesOp::FromBinaryNative)
}

pub(crate) fn eval_convert_using_in(
    ctx: &dyn Columns,
    value: &Datum,
    arg_type: &FieldType,
    target: &str,
) -> Result<Datum, EvalError> {
    evaluate_args_in(
        EvaluatedBytesOp::ConvertUsingNative,
        ctx,
        || {
            // Preserve target-metadata validation before the signature's
            // ETString reader, including when the datum is not yet cast.
            if !is_supported_encoding(target) {
                return Err(EvalError::Unsupported("unknown character set"));
            }
            Ok(EvaluatedArgs::Bytes4([
                crate::arg_eval_type::eval_string(value)?,
                Some(arg_type.charset_name().as_bytes().to_vec()),
                Some(arg_type.charset().name().as_bytes().to_vec()),
                Some(target.as_bytes().to_vec()),
            ]))
        },
        |computed| project_report(computed, Some(target)),
    )
}

/// Callers use this only after observing an actual NULL child. Direct value
/// helpers instead pass their nullable ETString value to the conversion worker.
pub(crate) fn eval_charset_null_in(ctx: &dyn Columns) -> Result<Datum, EvalError> {
    evaluate_args_in(
        EvaluatedBytesOp::DateDiffNullNative,
        ctx,
        || Ok(EvaluatedArgs::NullWitness(None)),
        |computed| match computed.into_int_datum()? {
            value @ Datum::Null => Ok(value),
            _ => Err(invalid_report()),
        },
    )
}
