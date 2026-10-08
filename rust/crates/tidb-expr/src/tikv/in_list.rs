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

use super::adapter_failure::{ExpressionAdapterFailure, ScopeFailureKind};
use super::{
    evaluate_args_in, evaluate_prepared_args_scoped_in, identity_value, EvaluatedArgs,
    EvaluatedBytesOp as Op, EvaluatedBytesResult, ExpressionRuntimeFailure, LocalError,
};
use crate::{Columns, Datum, EvalError};
use tidb_query_expr::{NativeInRequest as Request, NativeInResult as Report};

/// Preserve the actual comparison channel, including non-Boolean integers.
/// Each SDK control service owns its source-specific interpretation.
pub(crate) fn in_eq_observation(value: &Datum) -> tidb_query_expr::NativeInControlValue {
    use tidb_query_expr::NativeInControlValue as Value;
    match value {
        Datum::Null => Value::Null,
        Datum::Int(value) => Value::Int(*value),
        _ => Value::Other,
    }
}

pub(crate) fn in_control_datum(value: tidb_query_expr::NativeInControlResult) -> Datum {
    match value {
        tidb_query_expr::NativeInControlResult::Null => Datum::Null,
        tidb_query_expr::NativeInControlResult::Bool(value) => Datum::Int(i64::from(value)),
    }
}

pub(crate) fn in_row_control_error(
    error: tidb_query_expr::NativeInRowError<EvalError>,
) -> EvalError {
    match error {
        tidb_query_expr::NativeInRowError::Child(error) => error,
        tidb_query_expr::NativeInRowError::ItemMismatch => {
            EvalError::Unsupported("row value IN list item arity mismatch")
        }
        tidb_query_expr::NativeInRowError::WidthMismatch => {
            EvalError::Unsupported("row value arity mismatch")
        }
    }
}

/// Only the already-evaluated, already-cast temporal/JSON IN branch enters here.
/// The caller retains its all-evaluation then all-casting barrier.
pub(crate) fn eval_in_typed_values_in(
    ctx: &dyn Columns,
    domain: tidb_query_expr::NativeInTypedDomain,
    values: &[Datum],
) -> Result<Datum, EvalError> {
    evaluate_args_in(
        Op::InTypedValuesNative,
        ctx,
        || {
            let identities = values
                .iter()
                .map(identity_value::encode)
                .collect::<Result<Vec<_>, _>>()?;
            let views = identities.iter().map(Option::as_deref).collect::<Vec<_>>();
            let packet = tidb_query_expr::encode_native_in_typed_values(domain, &views)
                .map_err(frame_error)?;
            Ok(EvaluatedArgs::Bytes(Some(packet)))
        },
        |computed| {
            let report = computed.into_bytes()?.ok_or_else(invalid_report)?;
            match tidb_query_expr::decode_native_in_result(&report).ok_or_else(invalid_report)? {
                Report::Null => Ok(Datum::Null),
                Report::Bool(value) => Ok(Datum::Int(i64::from(value))),
                Report::Request { .. } => Err(invalid_report()),
            }
        },
    )
}

#[derive(Clone, Copy)]
enum LegacyKind {
    Int,
    Bytes,
}

fn drive<E: From<EvalError>>(
    computed: EvaluatedBytesResult,
    selected: &dyn Columns,
    kind: LegacyKind,
    count: usize,
    eval: &mut impl FnMut(usize, &dyn Columns) -> Result<Option<Vec<u8>>, E>,
) -> Result<Option<i128>, E> {
    let mut state = computed
        .into_bytes()
        .map_err(E::from)?
        .ok_or_else(|| E::from(invalid_report()))?;
    let mut at_head = true;
    loop {
        let report = tidb_query_expr::decode_native_in_result(&state)
            .ok_or_else(|| E::from(invalid_report()))?;
        let request = match report {
            Report::Null if !at_head => return Ok(None),
            Report::Bool(value) if !at_head => return Ok(Some(i128::from(value))),
            Report::Request { kind: request, .. } => request,
            _ => return Err(E::from(invalid_report())),
        };
        let actual = match (kind, request) {
            (LegacyKind::Int, Request::Int128 { index })
            | (LegacyKind::Bytes, Request::Bytes { index }) => {
                // The original reader is asked for operand zero even for an
                // empty child list; it supplies its actual missing-child NULL.
                if (at_head && index != 0) || (index != 0 && index >= count) {
                    return Err(E::from(invalid_report()));
                }
                eval(index, selected)?
            }
            (LegacyKind::Bytes, Request::Collation { collation_id }) if !at_head => {
                // Resolve only when the SDK requests the original late
                // non-NULL-pair lookup. Do not snapshot this mutable mode in
                // the head and do not return a host-computed equality result.
                let tag = tidb_datatype::get_collator_by_id(collation_id)
                    .new_collation()
                    .map_or(
                        super::NativeCollation::Binary,
                        tidb_datatype::Collation::native_policy,
                    )
                    .tag();
                Some(tag.to_le_bytes().to_vec())
            }
            _ => return Err(E::from(invalid_report())),
        };
        state = evaluate_args_in(
            Op::InLegacyStepNative,
            selected,
            || Ok(EvaluatedArgs::Bytes2(Some(state), actual)),
            |computed| computed.into_bytes()?.ok_or_else(invalid_report),
        )
        .map_err(E::from)?;
        at_head = false;
    }
}

fn legacy<E: From<EvalError>>(
    ctx: &dyn Columns,
    kind: LegacyKind,
    count: usize,
    metadata: impl FnOnce() -> Result<Vec<u8>, tidb_query_expr::NativeIdentityFrameError>,
    mut eval: impl FnMut(usize, &dyn Columns) -> Result<Option<Vec<u8>>, E>,
) -> Result<Option<i128>, E> {
    evaluate_prepared_args_scoped_in(
        ctx,
        || {
            let operation = match kind {
                LegacyKind::Int => Op::InLegacyIntHeadNative,
                LegacyKind::Bytes => Op::InLegacyStringHeadNative,
            };
            Ok((
                operation,
                EvaluatedArgs::Bytes(Some(metadata().map_err(frame_error)?)),
            ))
        },
        |computed, selected| {
            // Preserve the original callback E under the live head pack guard;
            // no artificial EvalError, SQL folding, or extra worker is involved.
            Ok(drive(computed, selected, kind, count, &mut eval))
        },
    )
    .map_err(E::from)?
}

/// Legacy integer IN reads the caller's native integer channel, not a folded
/// surrogate. Its full i128 domain and original errors survive unchanged.
pub fn eval_legacy_in_int_in<E: From<EvalError>>(
    ctx: &dyn Columns,
    count: usize,
    mut eval: impl FnMut(usize, &dyn Columns) -> Result<Option<i128>, E>,
) -> Result<Option<i128>, E> {
    legacy(
        ctx,
        LegacyKind::Int,
        count,
        || tidb_query_expr::encode_native_in_legacy_int_head(count),
        |index, selected| {
            eval(index, selected).map(|value| value.map(|value| value.to_le_bytes().to_vec()))
        },
    )
}

/// Legacy byte IN retains the caller's byte-channel SQL folding and receives
/// the actual collation ID; only the SDK owns comparison and list progress.
pub fn eval_legacy_in_bytes_in<E: From<EvalError>>(
    ctx: &dyn Columns,
    count: usize,
    collation: i32,
    eval: impl FnMut(usize, &dyn Columns) -> Result<Option<Vec<u8>>, E>,
) -> Result<Option<i128>, E> {
    legacy(
        ctx,
        LegacyKind::Bytes,
        count,
        || tidb_query_expr::encode_native_in_legacy_string_head(count, collation),
        eval,
    )
}

fn invalid_report() -> EvalError {
    EvalError::ExpressionAdapterFailure(ExpressionAdapterFailure::from_scope(
        ScopeFailureKind::Contract,
        "invalid native IN report",
    ))
}

fn frame_error(error: tidb_query_expr::NativeIdentityFrameError) -> EvalError {
    match error {
        tidb_query_expr::NativeIdentityFrameError::Invalid => invalid_report(),
        tidb_query_expr::NativeIdentityFrameError::Capacity => {
            EvalError::ExpressionRuntimeFailure(ExpressionRuntimeFailure::from_local_eval(
                LocalError::ResourceLimit("native IN frame allocation or size failed".into()),
                None,
            ))
        }
    }
}
