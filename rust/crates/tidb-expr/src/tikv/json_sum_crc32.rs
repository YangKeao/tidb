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

use tidb_query_expr::{decode_native_json_sum_crc32_result, NativeJsonSumCrc32Result};

use super::adapter_failure::{ExpressionAdapterFailure, ScopeFailureKind};
use super::{
    evaluate_args_in, prepare_json_serde_args, EvaluatedArgs, EvaluatedBytesOp,
    EvaluatedBytesResult,
};
use crate::{Columns, Datum, EvalError};

fn invalid_report() -> EvalError {
    EvalError::ExpressionAdapterFailure(ExpressionAdapterFailure::from_scope(
        ScopeFailureKind::Contract,
        "invalid native JSON_SUM_CRC32 report",
    ))
}

fn project_report(computed: EvaluatedBytesResult) -> Result<Datum, EvalError> {
    let EvaluatedBytesResult::Bytes(report) = computed else {
        return Err(invalid_report());
    };
    let Some(report) = report else {
        return Ok(Datum::Null);
    };
    match decode_native_json_sum_crc32_result(&report).ok_or_else(invalid_report)? {
        NativeJsonSumCrc32Result::Value(sum) => Ok(Datum::Int(sum)),
        NativeJsonSumCrc32Result::RequiresArray => {
            Err(EvalError::Unsupported("JSON_SUM_CRC32 requires JSON array"))
        }
        NativeJsonSumCrc32Result::RequiresScalar => Err(EvalError::Unsupported(
            "JSON_SUM_CRC32 requires scalar array values",
        )),
        NativeJsonSumCrc32Result::RequiresHomogeneous => Err(EvalError::Unsupported(
            "JSON_SUM_CRC32 requires homogeneous array values",
        )),
    }
}

pub(crate) fn eval_json_sum_crc32_in(ctx: &dyn Columns, value: &Datum) -> Result<Datum, EvalError> {
    evaluate_args_in(
        EvaluatedBytesOp::JsonSumCrc32SerdeNative,
        ctx,
        || match crate::builtin_ext::json::parse_json_document_argument(value)? {
            Some(document) => prepare_json_serde_args(&document, None, None),
            // SQL NULL enters the same worker. A parsed JSON null remains a
            // present serde document and receives the SDK's array-domain error.
            None => Ok(EvaluatedArgs::Bytes(None)),
        },
        project_report,
    )
}

#[cfg(test)]
mod tests {
    use super::super::LocalError;
    use super::*;
    use crate::{ExpressionAdapterFailureClass, ReadyValuePoolOwner, ReadyValuePoolPolicy};

    #[test]
    fn json_sum_crc32_bridge_keeps_null_domains_reports_and_scoped_budgets() {
        struct NoJsonGetters;
        impl Columns for NoJsonGetters {
            fn get(&self, _: &[String]) -> Option<Datum> {
                panic!("ready JSON document")
            }
            fn connection_charset_info(&self) -> (&str, &str) {
                panic!("no charset demand")
            }
            fn append_warning(&self, _: u16, _: &str) {
                panic!("no CRC warnings")
            }
        }
        let text = |value: &str| Datum::new_string(value);
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
            scope.with_columns(&NoJsonGetters, |bound| {
                for (value, expected) in [
                    (Datum::Null, Ok(Datum::Null)),
                    (
                        text("null"),
                        Err(EvalError::Unsupported("JSON_SUM_CRC32 requires JSON array")),
                    ),
                    (
                        Datum::Int(1),
                        Err(EvalError::Unsupported("JSON_SUM_CRC32 requires JSON array")),
                    ),
                    (text("[]"), Ok(Datum::Int(0))),
                    (text("[-1,2,3]"), Ok(Datum::Int(3_101_005_010))),
                    (text(r#"["a","b","c"]"#), Ok(Datum::Int(5_925_539_243))),
                    (
                        text("[null]"),
                        Err(EvalError::Unsupported(
                            "JSON_SUM_CRC32 requires scalar array values",
                        )),
                    ),
                    (
                        text(r#"[1,"a",false]"#),
                        Err(EvalError::Unsupported(
                            "JSON_SUM_CRC32 requires homogeneous array values",
                        )),
                    ),
                    (
                        text(r#"[false,1,"a"]"#),
                        Err(EvalError::Unsupported(
                            "JSON_SUM_CRC32 requires scalar array values",
                        )),
                    ),
                ] {
                    let actual = eval_json_sum_crc32_in(bound, &value);
                    if slots == 0 {
                        assert!(matches!(actual,
                            Err(EvalError::ExpressionAdapterFailure(failure))
                                if failure.class() == ExpressionAdapterFailureClass::PoolResource));
                    } else {
                        assert_eq!(actual, expected);
                    }
                }
                // Existing document coercion still precedes worker admission.
                assert!(matches!(
                    eval_json_sum_crc32_in(bound, &text("not-json")),
                    Err(EvalError::Json(_))
                ));
                assert_eq!(
                    eval_json_sum_crc32_in(bound, &Datum::MaxValue),
                    Err(EvalError::Unsupported("JSON document requires string"))
                );
                if slots == 1 {
                    let refused = evaluate_args_in(
                        EvaluatedBytesOp::JsonSumCrc32SerdeNative,
                        bound,
                        || {
                            let EvaluatedArgs::Bytes(Some(mut bytes)) =
                                prepare_json_serde_args(&serde_json::json!([]), None, None)?
                            else {
                                panic!("one serde document has one byte operand");
                            };
                            // Preserve the actual serialized document, enlarging
                            // only spare capacity to exercise retained-input charge.
                            bytes.reserve(128 << 10);
                            Ok(EvaluatedArgs::Bytes(Some(bytes)))
                        },
                        project_report,
                    );
                    assert!(
                        matches!(refused, Err(EvalError::ExpressionRuntimeFailure(failure))
                        if matches!(failure.local_error(), LocalError::ResourceLimit(_)))
                    );
                    assert_eq!(
                        eval_json_sum_crc32_in(bound, &text("[]")),
                        Ok(Datum::Int(0))
                    );
                }
            });
            drop(scope);
            execution.close();
        }
        for report in [
            vec![],
            vec![0],
            vec![0; 8],
            vec![0; 10],
            vec![1, 0],
            vec![4],
        ] {
            assert!(
                matches!(project_report(EvaluatedBytesResult::Bytes(Some(report))),
                Err(EvalError::ExpressionAdapterFailure(failure))
                    if failure.class() == ExpressionAdapterFailureClass::ScopeContract)
            );
        }
        assert!(
            matches!(project_report(EvaluatedBytesResult::Int(Datum::Int(0))),
            Err(EvalError::ExpressionAdapterFailure(failure))
                if failure.class() == ExpressionAdapterFailureClass::ScopeContract)
        );
        let mut signed_report = vec![0];
        signed_report.extend_from_slice(&i64::MIN.to_le_bytes());
        assert_eq!(
            project_report(EvaluatedBytesResult::Bytes(Some(signed_report))),
            Ok(Datum::Int(i64::MIN))
        );
        assert_eq!(
            eval_json_sum_crc32_in(&crate::NoColumns, &Datum::Null),
            Ok(Datum::Null)
        );
    }
}
