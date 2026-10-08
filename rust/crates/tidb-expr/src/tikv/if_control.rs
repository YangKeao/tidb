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

use tidb_query_expr::{decode_native_if_head_result, NativeIfBranch};

use super::adapter_failure::{ExpressionAdapterFailure, ScopeFailureKind};
use super::{
    evaluate_args_in, evaluate_prepared_args_scoped_in, identity_value, EvaluatedArgs,
    EvaluatedBytesOp, EvaluatedBytesResult,
};
use crate::{Columns, Datum, EvalError};

fn invalid_report() -> EvalError {
    EvalError::ExpressionAdapterFailure(ExpressionAdapterFailure::from_scope(
        ScopeFailureKind::Contract,
        "invalid native IF head report",
    ))
}

pub(super) fn decode_head(
    computed: EvaluatedBytesResult,
) -> Result<(NativeIfBranch, Vec<u8>), EvalError> {
    let report = computed.into_bytes()?.ok_or_else(invalid_report)?;
    let branch = decode_native_if_head_result(&report).ok_or_else(invalid_report)?;
    Ok((branch, report))
}

pub(super) fn finish_in(
    ctx: &dyn Columns,
    report: Vec<u8>,
    value: &Datum,
) -> Result<Datum, EvalError> {
    evaluate_args_in(
        EvaluatedBytesOp::IfFinishNative,
        ctx,
        || {
            Ok(EvaluatedArgs::Bytes2(
                Some(report),
                identity_value::encode(value)?,
            ))
        },
        EvaluatedBytesResult::into_identity_datum,
    )
}

pub(crate) fn eval_if_in(
    ctx: &dyn Columns,
    condition: impl FnOnce(&dyn Columns) -> Result<Option<bool>, EvalError>,
    when_true: impl FnOnce(&dyn Columns) -> Result<Datum, EvalError>,
    when_false: impl FnOnce(&dyn Columns) -> Result<Datum, EvalError>,
) -> Result<Datum, EvalError> {
    evaluate_prepared_args_scoped_in(
        ctx,
        || {
            // Original preparation authority is intentional: with no capability,
            // the condition still runs before the parent's one-shot allocation.
            let condition = condition(ctx)?.map(i64::from);
            Ok((
                EvaluatedBytesOp::IfHeadNative,
                EvaluatedArgs::Int(condition),
            ))
        },
        |computed, selected| {
            let (branch, report) = decode_head(computed)?;
            let value = match branch {
                NativeIfBranch::Then => when_true(selected)?,
                NativeIfBranch::Else => when_false(selected)?,
            };
            finish_in(selected, report, &value)
        },
    )
}
