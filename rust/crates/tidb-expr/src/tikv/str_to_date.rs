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

use std::cell::Cell;

use tidb_datatype::FieldTypeCode;
use tidb_query_expr::{decode_native_str_to_date_result, NativeStrToDateResult};

use super::adapter_failure::{ExpressionAdapterFailure, ScopeFailureKind};
use super::{
    evaluate_args_in, evaluate_prepared_args_scoped_in, EvaluatedArgs, EvaluatedBytesOp,
    EvaluatedBytesResult,
};
use crate::{Columns, Datum, EvalError};

fn invalid_report() -> EvalError {
    EvalError::ExpressionAdapterFailure(ExpressionAdapterFailure::from_scope(
        ScopeFailureKind::Contract,
        "invalid native STR_TO_DATE report",
    ))
}

enum TerminalStage {
    Head,
    DateFinish,
    TypedFinish,
}

fn terminal_report(
    report: Option<Vec<u8>>,
    ctx: &dyn Columns,
    stage: TerminalStage,
) -> Result<Datum, EvalError> {
    let Some(report) = report else {
        return if matches!(stage, TerminalStage::DateFinish) {
            Err(invalid_report())
        } else {
            Ok(Datum::Null)
        };
    };
    match decode_native_str_to_date_result(&report).ok_or_else(invalid_report)? {
        NativeStrToDateResult::Value(text) => Ok(Datum::new_string(text)),
        NativeStrToDateResult::Warning {
            code: code @ (1292 | 1411),
            message,
        } if !matches!(stage, TerminalStage::TypedFinish) => {
            ctx.append_warning(code, message);
            Ok(Datum::Null)
        }
        // A continuation's output cannot request another stage or invent a
        // warning kind. The SDK owns warning text and all value classification.
        _ => Err(invalid_report()),
    }
}

pub(crate) fn eval_str_to_date_in(
    ctx: &dyn Columns,
    vals: &[Datum],
    result_type: Option<FieldTypeCode>,
) -> Result<Datum, EvalError> {
    let null_path = Cell::new(false);
    evaluate_prepared_args_scoped_in(
        ctx,
        || {
            let [input, format] = vals else {
                return Err(EvalError::Unsupported("bad function arity"));
            };
            // The caller already evaluated both children. An actual NULL
            // suppresses coercion of BOTH ready operands, not child evaluation.
            if input.is_null() || format.is_null() {
                null_path.set(true);
                return Ok((
                    EvaluatedBytesOp::DateDiffNullNative,
                    EvaluatedArgs::NullWitness(None),
                ));
            }
            let input = crate::coerce::coerce_str(input)?;
            let format = crate::coerce::coerce_str(format)?;
            Ok((
                EvaluatedBytesOp::StrToDateHeadNative,
                EvaluatedArgs::BytesBytesInt(
                    input.map(String::into_bytes),
                    format.map(String::into_bytes),
                    // Preserve enum identity: Unknown(12) is NOT Datetime,
                    // even though both expose mysql_type() == 12.
                    result_type.map(|code| match code {
                        FieldTypeCode::Unknown(raw) => -1 - i64::from(raw),
                        known => i64::from(known.mysql_type()),
                    }),
                ),
            ))
        },
        |computed, selected| {
            if null_path.get() {
                return match computed.into_int_datum()? {
                    value @ Datum::Null => Ok(value),
                    _ => Err(invalid_report()),
                };
            }
            let report = computed.into_bytes()?;
            let Some(report) = report else {
                return terminal_report(None, ctx, TerminalStage::Head);
            };
            match decode_native_str_to_date_result(&report).ok_or_else(invalid_report)? {
                NativeStrToDateResult::NeedDateModes(_) => evaluate_args_in(
                    EvaluatedBytesOp::StrToDateFinishNative,
                    selected,
                    || {
                        // Preserve the original statement override and its late
                        // demand, including month-zero results that will warn.
                        let modes = ctx.date_modes();
                        Ok(EvaluatedArgs::BytesIntInt(
                            Some(report),
                            Some(i64::from(modes.no_zero_date)),
                            Some(i64::from(modes.allow_invalid_dates)),
                        ))
                    },
                    |computed| {
                        terminal_report(computed.into_bytes()?, ctx, TerminalStage::DateFinish)
                    },
                ),
                NativeStrToDateResult::NeedTypedDateMode(_) => evaluate_args_in(
                    EvaluatedBytesOp::StrToDateTypedFinishNative,
                    selected,
                    || {
                        let modes = ctx.date_modes();
                        Ok(EvaluatedArgs::BytesInt(
                            Some(report),
                            Some(i64::from(modes.no_zero_date)),
                        ))
                    },
                    |computed| {
                        terminal_report(computed.into_bytes()?, ctx, TerminalStage::TypedFinish)
                    },
                ),
                _ => terminal_report(Some(report), ctx, TerminalStage::Head),
            }
        },
    )
}
