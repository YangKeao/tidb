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

use super::adapter_failure::{ExpressionAdapterFailure, ScopeFailureKind};
use super::if_null::{eval_if_null_head_scoped_in, IfNullHeadOutcome};
use super::{evaluate_args_in, EvaluatedArgs, EvaluatedBytesOp};
use crate::{Columns, Datum, EvalError};

fn end_in(ctx: &dyn Columns) -> Result<Datum, EvalError> {
    evaluate_args_in(
        EvaluatedBytesOp::CoalesceEndNative,
        ctx,
        || Ok(EvaluatedArgs::NoArgs),
        |computed| match computed.into_identity_datum()? {
            value @ Datum::Null => Ok(value),
            _ => Err(EvalError::ExpressionAdapterFailure(
                ExpressionAdapterFailure::from_scope(
                    ScopeFailureKind::Contract,
                    "COALESCE end returned a non-NULL identity",
                ),
            )),
        },
    )
}

pub(crate) fn eval_coalesce_in<V: Borrow<Datum>>(
    ctx: &dyn Columns,
    arity: usize,
    argument: impl Fn(usize, &dyn Columns) -> Result<V, EvalError>,
) -> Result<Datum, EvalError> {
    if arity == 0 {
        return end_in(ctx);
    }
    eval_if_null_head_scoped_in(
        ctx,
        |original| argument(0, original),
        |mut outcome, selected| {
            let mut next = 1;
            loop {
                match outcome {
                    IfNullHeadOutcome::Done(value) => return Ok(value),
                    IfNullHeadOutcome::NeedSecond(report) => drop(report),
                }
                if next == arity {
                    return end_in(selected);
                }
                // Each nested head returns its outcome immediately. Keep only the
                // first head's outer guard, never a growing continuation stack or
                // a chain of newly bound contexts across iterations.
                outcome = eval_if_null_head_scoped_in(
                    selected,
                    |original| argument(next, original),
                    |outcome, _nested| Ok(outcome),
                )?;
                next += 1;
            }
        },
    )
}
