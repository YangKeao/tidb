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

use std::cell::OnceCell;

use tidb_query_expr::NativeIfBranch;

use super::adapter_failure::{ExpressionAdapterFailure, ScopeFailureKind};
use super::if_control::{decode_head, finish_in};
use super::{
    evaluate_args_in, evaluate_prepared_args_scoped_in, identity_value, EvaluatedArgs,
    EvaluatedBytesOp, EvaluatedBytesResult,
};
use crate::{Columns, Datum, EvalError};

fn contract_error(reason: &'static str) -> EvalError {
    EvalError::ExpressionAdapterFailure(ExpressionAdapterFailure::from_scope(
        ScopeFailureKind::Contract,
        reason,
    ))
}

fn decode_end(computed: EvaluatedBytesResult) -> Result<Datum, EvalError> {
    match computed.into_identity_datum()? {
        value @ Datum::Null => Ok(value),
        _ => Err(contract_error(
            "CASE exhaustion returned a non-NULL identity",
        )),
    }
}

pub(crate) fn eval_case_in<P>(
    ctx: &dyn Columns,
    pair_count: usize,
    has_else: bool,
    initialize: impl FnOnce(&dyn Columns) -> Result<P, EvalError>,
    condition: impl Fn(usize, &P, &dyn Columns) -> Result<Option<bool>, EvalError>,
    value: impl Fn(usize, &dyn Columns) -> Result<Datum, EvalError>,
) -> Result<Datum, EvalError> {
    let state = OnceCell::new();
    evaluate_prepared_args_scoped_in(
        ctx,
        || {
            // Preserve original preparation authority and the no-owner order.
            // Even a zero-pair simple CASE evaluates its real base exactly once.
            state
                .set(initialize(ctx)?)
                .map_err(|_| contract_error("CASE state was initialized twice"))?;
            if pair_count == 0 {
                return if has_else {
                    let value = value(0, ctx)?;
                    Ok((
                        EvaluatedBytesOp::AnyValueNative,
                        EvaluatedArgs::Bytes(identity_value::encode(&value)?),
                    ))
                } else {
                    Ok((EvaluatedBytesOp::CoalesceEndNative, EvaluatedArgs::NoArgs))
                };
            }
            let state = state
                .get()
                .ok_or_else(|| contract_error("CASE state is missing"))?;
            Ok((
                EvaluatedBytesOp::IfHeadNative,
                EvaluatedArgs::Int(condition(0, state, ctx)?.map(i64::from)),
            ))
        },
        |computed, selected| {
            if pair_count == 0 {
                return if has_else {
                    computed.into_identity_datum()
                } else {
                    decode_end(computed)
                };
            }
            let state = state
                .get()
                .ok_or_else(|| contract_error("CASE state is missing"))?;
            let (mut branch, mut report) = decode_head(computed)?;
            let mut index = 0;
            loop {
                match branch {
                    NativeIfBranch::Then => {
                        let value = value(index, selected)?;
                        return finish_in(selected, report, &value);
                    }
                    NativeIfBranch::Else => {}
                }
                index += 1;
                if index == pair_count {
                    if has_else {
                        let value = value(index, selected)?;
                        return finish_in(selected, report, &value);
                    }
                    drop(report);
                    return evaluate_args_in(
                        EvaluatedBytesOp::CoalesceEndNative,
                        selected,
                        || Ok(EvaluatedArgs::NoArgs),
                        decode_end,
                    );
                }
                drop(report);
                // Return each subsequent head immediately, retaining only the
                // outer selected context and guard rather than nested cursors.
                (branch, report) = evaluate_prepared_args_scoped_in(
                    selected,
                    || {
                        Ok((
                            EvaluatedBytesOp::IfHeadNative,
                            EvaluatedArgs::Int(condition(index, state, selected)?.map(i64::from)),
                        ))
                    },
                    |computed, _nested| decode_head(computed),
                )?;
            }
        },
    )
}
