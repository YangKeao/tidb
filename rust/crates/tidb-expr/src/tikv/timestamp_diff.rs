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

use tidb_datatype::CoreTime;

use super::{
    evaluate_args_in, evaluate_prepared_args_in, native_time_result_contract_error, EvaluatedArgs,
    EvaluatedBytesOp, EvaluatedBytesResult,
};
use crate::{Columns, Datum, EvalError};

pub(crate) fn eval_timestamp_diff_in(
    ctx: &dyn Columns,
    vals: &[Datum],
) -> Result<Datum, EvalError> {
    evaluate_args_in(
        EvaluatedBytesOp::TimestampDiffTextNative,
        ctx,
        || {
            let [unit, left, right] = vals else {
                return Err(EvalError::Unsupported("bad function arity"));
            };
            // Keep all three coercions in their original order even when an
            // earlier operand is NULL; a later coercion can still fail.
            let (unit, left, right) = (
                crate::coerce::coerce_str(unit)?,
                crate::coerce::coerce_str(left)?,
                crate::coerce::coerce_str(right)?,
            );
            Ok(EvaluatedArgs::Bytes3([
                unit.map(String::into_bytes),
                left.map(String::into_bytes),
                right.map(String::into_bytes),
            ]))
        },
        EvaluatedBytesResult::into_int_datum,
    )
}

/// Only for an actually observed early NULL in the PB argument prefix.
pub(crate) fn eval_timestamp_diff_null_in(ctx: &dyn Columns) -> Result<Datum, EvalError> {
    evaluate_args_in(
        EvaluatedBytesOp::DateDiffNullNative,
        ctx,
        || Ok(EvaluatedArgs::NullWitness(None)),
        |computed| match computed.into_int_datum()? {
            value @ Datum::Null => Ok(value),
            _ => Err(native_time_result_contract_error()),
        },
    )
}

/// Actual legacy TIMESTAMPDIFF inputs, without text-rendering typed time cores.
pub enum LegacyTimestampDiffArgs {
    /// A genuinely NULL unit; Some is an invalid witness, never a value.
    NullWitness(Option<()>),
    /// The demanded raw unit and nullable calendar cores, retaining all bits.
    Values {
        unit: Vec<u8>,
        left: Option<CoreTime>,
        right: Option<CoreTime>,
    },
}

/// Evaluates the legacy raw-core signature using the caller's current scope.
pub fn eval_legacy_timestamp_diff_in(
    args: LegacyTimestampDiffArgs,
    ctx: &dyn Columns,
) -> Result<Option<i64>, EvalError> {
    evaluate_prepared_args_in(
        ctx,
        || match args {
            LegacyTimestampDiffArgs::NullWitness(value) => {
                if value.is_some() {
                    return Err(native_time_result_contract_error());
                }
                Ok((
                    EvaluatedBytesOp::DateDiffNullNative,
                    EvaluatedArgs::NullWitness(None),
                ))
            }
            LegacyTimestampDiffArgs::Values { unit, left, right } => Ok((
                EvaluatedBytesOp::TimestampDiffCoreNative,
                EvaluatedArgs::Bytes3([
                    Some(unit),
                    left.map(|value| value.raw().to_le_bytes().to_vec()),
                    right.map(|value| value.raw().to_le_bytes().to_vec()),
                ]),
            )),
        },
        |computed| match computed.into_int_datum()? {
            Datum::Int(value) => Ok(Some(value)),
            Datum::Null => Ok(None),
            _ => Err(native_time_result_contract_error()),
        },
    )
}
