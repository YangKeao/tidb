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

use tidb_datatype::{EvalType, FieldType};
use tidb_query_expr::{
    NativeIntervalCast as Cast, NativeIntervalEvalType as Type, NativeIntervalFieldType,
    NativeIntervalResult as Report,
};

use super::adapter_failure::{ExpressionAdapterFailure, ScopeFailureKind};
use super::{
    evaluate_args_in, evaluate_prepared_args_scoped_in, identity_value, EvaluatedArgs,
    EvaluatedBytesOp as Op, EvaluatedBytesResult, ExpressionRuntimeFailure, LocalError,
};
use crate::{Columns, Datum, EvalError};

fn invalid_report() -> EvalError {
    EvalError::ExpressionAdapterFailure(ExpressionAdapterFailure::from_scope(
        ScopeFailureKind::Contract,
        "invalid native INTERVAL report",
    ))
}

fn frame_error(error: tidb_query_expr::NativeIdentityFrameError) -> EvalError {
    match error {
        tidb_query_expr::NativeIdentityFrameError::Invalid => invalid_report(),
        tidb_query_expr::NativeIdentityFrameError::Capacity => {
            EvalError::ExpressionRuntimeFailure(ExpressionRuntimeFailure::from_local_eval(
                LocalError::ResourceLimit("native INTERVAL frame allocation or size failed".into()),
                None,
            ))
        }
    }
}

fn real_identity(value: &Datum, ctx: &dyn Columns) -> Result<Option<Vec<u8>>, EvalError> {
    if value.is_null() {
        Ok(None)
    } else {
        identity_value::encode(&Datum::Real(crate::ops::to_f64_with_mysql_string(
            value, ctx,
        )?))
    }
}

fn integer_identity(
    value: &Datum,
    source: Option<&FieldType>,
    ctx: &dyn Columns,
) -> Result<Option<Vec<u8>>, EvalError> {
    let cast = crate::cast::cast_arg_as_int(value, source, ctx)?;
    if !matches!(cast, Datum::Int(_) | Datum::UInt(_) | Datum::Null) {
        return Err(invalid_report());
    }
    identity_value::encode(&cast)
}

fn field_metadata(field: &FieldType) -> NativeIntervalFieldType {
    NativeIntervalFieldType {
        eval_type: match field.eval_type() {
            EvalType::Int => Type::Int,
            EvalType::Real => Type::Real,
            EvalType::Decimal => Type::Decimal,
            EvalType::String => Type::String,
            EvalType::Datetime => Type::Datetime,
            EvalType::Timestamp => Type::Timestamp,
            EvalType::Duration => Type::Duration,
            EvalType::Json => Type::Json,
            EvalType::VectorFloat32 => Type::VectorFloat32,
        },
        flags: field.flags(),
    }
}

#[derive(Clone, Copy)]
enum Entry {
    Eager,
    Lazy,
}

fn drive(
    computed: EvaluatedBytesResult,
    selected: &dyn Columns,
    entry: Entry,
    count: usize,
    mut prepare: impl FnMut(usize, Cast) -> Result<Option<Vec<u8>>, EvalError>,
) -> Result<Datum, EvalError> {
    let mut state = computed.into_bytes()?.ok_or_else(invalid_report)?;
    let mut at_head = true;
    loop {
        let (index, cast) = match tidb_query_expr::decode_native_interval_result(&state)
            .ok_or_else(invalid_report)?
        {
            Report::IntIndex(index) if !at_head || matches!(entry, Entry::Eager) => {
                return Ok(Datum::Int(index));
            }
            Report::SentinelsError if at_head && matches!(entry, Entry::Eager) => {
                return Err(EvalError::Unsupported("range sentinel INTERVAL argument"));
            }
            Report::Request { index, cast, .. } => {
                if index >= count
                    || (at_head && index != 0)
                    || (matches!(entry, Entry::Eager) && !matches!(cast, Cast::Real))
                {
                    return Err(invalid_report());
                }
                (index, cast)
            }
            _ => return Err(invalid_report()),
        };
        state = evaluate_args_in(
            Op::IntervalStepNative,
            selected,
            || {
                // Evaluation and original casts are guarded, but only the SDK
                // decides which index to visit and when enough values were read.
                let actual = prepare(index, cast)?;
                Ok(EvaluatedArgs::Bytes2(Some(state), actual))
            },
            |computed| computed.into_bytes()?.ok_or_else(invalid_report),
        )?;
        at_head = false;
    }
}

pub(crate) fn eval_interval_in(ctx: &dyn Columns, vals: &[Datum]) -> Result<Datum, EvalError> {
    evaluate_prepared_args_scoped_in(
        ctx,
        || {
            let identities = vals
                .iter()
                .map(identity_value::encode)
                .collect::<Result<Vec<_>, _>>()?;
            let views = identities.iter().map(Option::as_deref).collect::<Vec<_>>();
            let packet =
                tidb_query_expr::encode_native_interval_eager_head(&views).map_err(frame_error)?;
            Ok((
                Op::IntervalEagerHeadNative,
                EvaluatedArgs::Bytes(Some(packet)),
            ))
        },
        |computed, selected| {
            drive(
                computed,
                selected,
                Entry::Eager,
                vals.len(),
                |index, cast| {
                    let value = vals.get(index).ok_or_else(invalid_report)?;
                    match cast {
                        Cast::Real => real_identity(value, ctx),
                        Cast::Int => Err(invalid_report()),
                    }
                },
            )
        },
    )
}

pub(crate) fn eval_interval_lazy_in(
    ctx: &dyn Columns,
    arg_types: &[Option<FieldType>],
    mut eval: impl FnMut(usize) -> Result<Datum, EvalError>,
) -> Result<Datum, EvalError> {
    debug_assert!(arg_types.len() >= 2);
    evaluate_prepared_args_scoped_in(
        ctx,
        || {
            let types = arg_types
                .iter()
                .map(|field| field.as_ref().map(field_metadata))
                .collect::<Vec<_>>();
            let packet =
                tidb_query_expr::encode_native_interval_lazy_head(&types).map_err(frame_error)?;
            Ok((
                Op::IntervalLazyHeadNative,
                EvaluatedArgs::Bytes(Some(packet)),
            ))
        },
        |computed, selected| {
            drive(
                computed,
                selected,
                Entry::Lazy,
                arg_types.len(),
                |index, cast| {
                    let source = arg_types.get(index).ok_or_else(invalid_report)?.as_ref();
                    let value = eval(index)?;
                    match cast {
                        Cast::Int => integer_identity(&value, source, ctx),
                        Cast::Real => real_identity(&value, ctx),
                    }
                },
            )
        },
    )
}
