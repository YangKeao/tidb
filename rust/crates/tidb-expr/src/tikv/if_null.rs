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

use std::borrow::Borrow;

use tidb_query_expr::{decode_native_if_null_head_result, NativeIfNullHeadResult};

use super::adapter_failure::{ExpressionAdapterFailure, ScopeFailureKind};
use super::{
    evaluate_args_in, evaluate_prepared_args_scoped_in, identity_value, EvaluatedArgs,
    EvaluatedBytesOp, EvaluatedBytesResult,
};
use crate::{Columns, Datum, EvalError};

pub(crate) enum IfNullHeadOutcome {
    Done(Datum),
    NeedSecond(Vec<u8>),
}

fn invalid_report() -> EvalError {
    EvalError::ExpressionAdapterFailure(ExpressionAdapterFailure::from_scope(
        ScopeFailureKind::Contract,
        "invalid native IFNULL head report",
    ))
}

pub(crate) fn eval_if_null_head_scoped_in<T, V: Borrow<Datum>>(
    ctx: &dyn Columns,
    first: impl FnOnce(&dyn Columns) -> Result<V, EvalError>,
    pack: impl FnOnce(IfNullHeadOutcome, &dyn Columns) -> Result<T, EvalError>,
) -> Result<T, EvalError> {
    evaluate_prepared_args_scoped_in(
        ctx,
        || {
            // Preserve original preparation authority and precedence. With no
            // capability this child still precedes the parent's one-shot cache.
            let first = first(ctx)?;
            Ok((
                EvaluatedBytesOp::IfNullHeadNative,
                EvaluatedArgs::Bytes(identity_value::encode(first.borrow())?),
            ))
        },
        |computed, selected| {
            let mut report = computed.into_bytes()?.ok_or_else(invalid_report)?;
            let outcome =
                match decode_native_if_null_head_result(&report).ok_or_else(invalid_report)? {
                    NativeIfNullHeadResult::NeedSecond => IfNullHeadOutcome::NeedSecond(report),
                    NativeIfNullHeadResult::Done(_) => {
                        // The SDK validated this action header and the complete
                        // computed identity frame; never reconstruct from `first`.
                        report.remove(0);
                        IfNullHeadOutcome::Done(identity_value::decode(Some(report))?)
                    }
                };
            pack(outcome, selected)
        },
    )
}

pub(crate) fn eval_if_null_finish_in(
    ctx: &dyn Columns,
    original_report: Vec<u8>,
    second: &Datum,
) -> Result<Datum, EvalError> {
    evaluate_args_in(
        EvaluatedBytesOp::IfNullFinishNative,
        ctx,
        || {
            if !matches!(
                decode_native_if_null_head_result(&original_report),
                Some(NativeIfNullHeadResult::NeedSecond)
            ) {
                return Err(invalid_report());
            }
            Ok(EvaluatedArgs::Bytes2(
                Some(original_report),
                identity_value::encode(second)?,
            ))
        },
        EvaluatedBytesResult::into_identity_datum,
    )
}

pub(crate) fn eval_if_null_in(
    ctx: &dyn Columns,
    first: impl FnOnce(&dyn Columns) -> Result<Datum, EvalError>,
    second: impl FnOnce(&dyn Columns) -> Result<Datum, EvalError>,
) -> Result<Datum, EvalError> {
    eval_if_null_head_scoped_in(ctx, first, |outcome, selected| match outcome {
        IfNullHeadOutcome::Done(value) => Ok(value),
        IfNullHeadOutcome::NeedSecond(report) => {
            let second = second(selected)?;
            eval_if_null_finish_in(selected, report, &second)
        }
    })
}
