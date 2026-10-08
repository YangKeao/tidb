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

#[cfg(test)]
mod tests {
    use tidb_datatype::FieldTypeCode;

    use super::super::{identity_value, LocalError};
    use super::*;
    use crate::{ExpressionAdapterFailureClass, ReadyValuePoolOwner, ReadyValuePoolPolicy};

    #[test]
    fn charset_bridge_keeps_helper_null_reports_and_live_capacity_accounting() {
        struct NoCodecGetters;
        impl Columns for NoCodecGetters {
            fn get(&self, _: &[String]) -> Option<Datum> {
                panic!("ready charset operands must not fetch columns")
            }
            fn connection_charset_info(&self) -> (&str, &str) {
                panic!("ready charset metadata must not read connection defaults")
            }
            fn append_warning(&self, _: u16, _: &str) {
                panic!("charset codec errors must not become warnings")
            }
        }
        let text = |value: &str| Datum::new_string(value);
        let utf8 = FieldType::new(FieldTypeCode::VarString).with_collation(Collation::Utf8Mb4Bin);
        let binary = FieldType::new(FieldTypeCode::VarString).with_collation(Collation::Binary);
        for slots in [0, 1] {
            let owner = ReadyValuePoolOwner::new(
                ReadyValuePoolPolicy::checked(
                    slots,
                    slots,
                    16 << 20,
                    1 << 20,
                    2 << 20,
                    64,
                    16,
                    1 << 16,
                )
                .unwrap(),
            )
            .unwrap();
            let execution = owner.begin_execution().unwrap();
            let scope = execution.scope();
            scope.with_columns(&NoCodecGetters, |bound| {
                for (actual, expected) in [
                    (
                        eval_to_binary_in(bound, &Datum::Null, "gbk"),
                        Ok(Datum::new_bytes([])),
                    ),
                    (
                        eval_from_binary_in(bound, &Datum::Null, "gbk"),
                        Ok(Datum::new_bytes([])),
                    ),
                    (
                        eval_convert_using_in(bound, &Datum::Null, &utf8, "utf8mb4"),
                        Ok(Datum::new_collation_string([], Collation::Utf8Mb4Bin)),
                    ),
                    (eval_charset_null_in(bound), Ok(Datum::Null)),
                    (
                        eval_to_binary_in(bound, &text("一"), "gbk"),
                        Ok(Datum::new_bytes([0xd2, 0xbb])),
                    ),
                    (
                        eval_from_binary_in(bound, &Datum::new_bytes([0xd2, 0xbb]), "gbk"),
                        Ok(Datum::new_bytes("一".as_bytes())),
                    ),
                    (
                        eval_to_binary_in(bound, &text("😉"), "gbk"),
                        Err(EvalError::Unsupported("invalid character string")),
                    ),
                    (
                        eval_from_binary_in(bound, &Datum::new_bytes([0xff]), "ascii"),
                        Err(EvalError::Unsupported("invalid character string")),
                    ),
                    (
                        eval_convert_using_in(bound, &Datum::new_bytes([0xff]), &binary, "ascii"),
                        Ok(Datum::Null),
                    ),
                    (
                        eval_convert_using_in(bound, &Datum::new_bytes([0xff]), &utf8, "latin1"),
                        Ok(Datum::new_collation_string([0xff], Collation::Latin1Bin)),
                    ),
                ] {
                    if slots == 0 {
                        assert!(matches!(actual,
                            Err(EvalError::ExpressionAdapterFailure(failure))
                                if failure.class() == ExpressionAdapterFailureClass::PoolResource));
                    } else {
                        match (actual, expected) {
                            (Ok(actual), Ok(expected)) => assert_eq!(
                                identity_value::encode(&actual).unwrap(),
                                identity_value::encode(&expected).unwrap(),
                            ),
                            (actual, expected) => assert_eq!(actual, expected),
                        }
                    }
                }
                // Registry validation precedes ETString coercion and admission.
                assert_eq!(
                    eval_convert_using_in(bound, &Datum::Int(7), &utf8, "UTF8MB4"),
                    Err(EvalError::Unsupported("unknown character set"))
                );
                assert_eq!(
                    eval_convert_using_in(bound, &Datum::Int(7), &utf8, "utf8mb4"),
                    Err(EvalError::Unsupported("un-cast types.ETString argument"))
                );
                if slots == 1 {
                    // Charge actual prepared capacity, not its one-byte length
                    // or the small successful reply. A resource refusal is reusable.
                    let mut spare = Vec::with_capacity(128 << 10);
                    spare.push(b'a');
                    let refused = evaluate_args_in(
                        EvaluatedBytesOp::ToBinaryNative,
                        bound,
                        || Ok(EvaluatedArgs::Bytes2(Some(spare), Some(b"binary".to_vec()))),
                        |computed| project_report(computed, None),
                    );
                    assert!(matches!(refused,
                        Err(EvalError::ExpressionRuntimeFailure(failure))
                            if matches!(failure.local_error(), LocalError::ResourceLimit(_))));
                    assert_eq!(
                        eval_to_binary_in(bound, &text("ok"), "binary"),
                        Ok(Datum::new_bytes(b"ok"))
                    );
                }
            });
            drop(scope);
            execution.close();
        }
        for report in [vec![], vec![4], vec![2, 0], vec![3, 0]] {
            assert!(
                matches!(project_report(EvaluatedBytesResult::Bytes(Some(report)), Some("ascii")),
                Err(EvalError::ExpressionAdapterFailure(failure))
                    if failure.class() == ExpressionAdapterFailureClass::ScopeContract)
            );
        }
        for report in [None, Some(vec![1, b'a']), Some(vec![3])] {
            assert!(
                matches!(project_report(EvaluatedBytesResult::Bytes(report), None),
                Err(EvalError::ExpressionAdapterFailure(failure))
                    if failure.class() == ExpressionAdapterFailureClass::ScopeContract)
            );
        }
        assert_eq!(
            eval_to_binary_in(&crate::NoColumns, &text("一"), "GBK"),
            Ok(Datum::new_bytes("一".as_bytes()))
        );
    }
}
