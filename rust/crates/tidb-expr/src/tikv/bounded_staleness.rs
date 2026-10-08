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

use tidb_query_expr::{
    decode_native_bounded_staleness_head, decode_native_identity, NativeBoundedStalenessHeadResult,
    NativeIdentityRef,
};

use super::adapter_failure::{ExpressionAdapterFailure, ScopeFailureKind};
use super::{
    evaluate_args_in, evaluate_prepared_args_scoped_in, identity_value, EvaluatedArgs,
    EvaluatedBytesOp, EvaluatedBytesResult,
};
use crate::{Columns, Datum, EvalError};

fn invalid_report() -> EvalError {
    EvalError::ExpressionAdapterFailure(ExpressionAdapterFailure::from_scope(
        ScopeFailureKind::Contract,
        "invalid bounded-staleness result report",
    ))
}

fn decode_head(report: Option<Vec<u8>>) -> Result<NativeBoundedStalenessHeadResult, EvalError> {
    let report = report.ok_or_else(invalid_report)?;
    decode_native_bounded_staleness_head(&report).ok_or_else(invalid_report)
}

fn project_finish(computed: EvaluatedBytesResult) -> Result<Datum, EvalError> {
    let frame = computed.into_bytes()?.ok_or_else(invalid_report)?;
    if !matches!(
        decode_native_identity(&frame).map_err(|_| invalid_report())?,
        NativeIdentityRef::Time {
            kind: 1,
            fsp: 3,
            ..
        }
    ) {
        return Err(invalid_report());
    }
    identity_value::decode(Some(frame))
}

pub(crate) fn eval_bounded_staleness_in(
    ctx: &dyn Columns,
    vals: &[Datum],
) -> Result<Datum, EvalError> {
    let null_path = Cell::new(false);
    evaluate_prepared_args_scoped_in(
        ctx,
        || {
            let [left, right] = vals else {
                return Err(EvalError::WrongParameterCount("tidb_bounded_staleness"));
            };
            if !matches!((left, right), (Datum::Time(_), Datum::Time(_))) {
                if vals.iter().any(Datum::is_null) {
                    null_path.set(true);
                    return Ok((
                        EvaluatedBytesOp::DateDiffNullNative,
                        EvaluatedArgs::NullWitness(None),
                    ));
                }
                return Err(EvalError::Unsupported(
                    "TIDB_BOUNDED_STALENESS arguments reached the signature without ETDatetime casts",
                ));
            }
            Ok((
                EvaluatedBytesOp::BoundedStalenessHeadNative,
                EvaluatedArgs::Bytes2(
                    identity_value::encode(left)?,
                    identity_value::encode(right)?,
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
            let head = decode_head(computed.into_bytes()?)?;
            let invalid_endpoint = match head {
                NativeBoundedStalenessHeadResult::InvalidLeft => Some(0),
                NativeBoundedStalenessHeadResult::InvalidRight => Some(1),
                NativeBoundedStalenessHeadResult::RangeNull => None,
                NativeBoundedStalenessHeadResult::NeedSafe => {
                    return evaluate_args_in(
                        EvaluatedBytesOp::BoundedStalenessFinishNative,
                        selected,
                        || {
                            // The outer guard is still active. Authority remains
                            // the original statement, not a worker/session default.
                            let safe = ctx.bounded_staleness_safe_time();
                            Ok(EvaluatedArgs::Bytes3([
                                identity_value::encode(&vals[0])?,
                                identity_value::encode(&vals[1])?,
                                safe.map(|value| identity_value::encode(&Datum::Time(value)))
                                    .transpose()?
                                    .flatten(),
                            ]))
                        },
                        project_finish,
                    );
                }
            };
            if let Some(index) = invalid_endpoint {
                let Datum::Time(value) = &vals[index] else {
                    return Err(invalid_report());
                };
                ctx.handle_truncate(&format!("Incorrect datetime value: '{value}'"))?;
            }
            // These three decoded SDK terminal reports carry the SQL NULL
            // answer; the frontend only projects it after any requested warning.
            Ok(Datum::Null)
        },
    )
}
