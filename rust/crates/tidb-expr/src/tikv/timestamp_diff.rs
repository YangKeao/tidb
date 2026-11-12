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

use super::native_time_result_contract_error;
use crate::{Columns, Datum, EvalError};

pub(crate) fn eval_timestamp_diff_in(
    _ctx: &dyn Columns,
    vals: &[Datum],
) -> Result<Datum, EvalError> {
    timestamp_diff_bounded(vals, super::identity_actual::default_budget())
}
/// Only an actually observed early NULL in the PB argument prefix.
pub(crate) fn eval_timestamp_diff_null_in(_ctx: &dyn Columns) -> Result<Datum, EvalError> {
    Ok(Datum::Null)
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

/// Evaluates the legacy raw-core signature using shared value arithmetic.
pub fn eval_legacy_timestamp_diff_in(
    args: LegacyTimestampDiffArgs,
    _ctx: &dyn Columns,
) -> Result<Option<i64>, EvalError> {
    match args {
        LegacyTimestampDiffArgs::NullWitness(None) => Ok(None),
        LegacyTimestampDiffArgs::NullWitness(Some(())) => Err(native_time_result_contract_error()),
        LegacyTimestampDiffArgs::Values { unit, left, right } => {
            Ok(tidb_query_expr::native_timestamp_diff_core_values(
                &unit,
                left.map(CoreTime::raw),
                right.map(CoreTime::raw),
            ))
        }
    }
}

pub(crate) fn timestamp_diff_bounded(vals: &[Datum], maximum: usize) -> Result<Datum, EvalError> {
    let [unit, left, right] = vals else {
        return Err(EvalError::Unsupported("bad function arity"));
    };
    timestamp_diff_values_bounded(unit, left, right, maximum)
}
pub(crate) fn timestamp_diff_cast_arguments_bounded(
    ctx: &dyn Columns,
    values: &mut [Datum; 3],
    fields: &[tidb_datatype::FieldType; 3],
    maximum: usize,
) -> Result<(), EvalError> {
    for i in 1..3 {
        // The original argument-cast layer runs AFTER all three sources and
        // BEFORE the text kernel, even when the unit itself is NULL/invalid.
        let value = &values[i];
        let work = tidb_query_expr::native_datetime_argument_cast_work(
            value.as_shared_json_input(),
            value.as_shared_numeric_input(),
            Some(fields[i].code().as_shared_type_name_code()),
        )
        .map_err(string_error)?;
        if work > maximum {
            return Err(string_error(
                tidb_query_expr::NativeStringCoercionError::ResourceLimit,
            ));
        }
        values[i] = crate::cast::cast_arg_as_datetime(value, Some(&fields[i]), ctx)?;
    }
    Ok(())
}
fn string_error(error: tidb_query_expr::NativeStringCoercionError) -> EvalError {
    match error {
        tidb_query_expr::NativeStringCoercionError::Unsupported(reason) => {
            EvalError::Unsupported(reason)
        }
        tidb_query_expr::NativeStringCoercionError::ResourceLimit => {
            EvalError::ExpressionRuntimeFailure(crate::ExpressionRuntimeFailure::from_local_eval(
                tidb_query_expr::local::LocalError::ResourceLimit(
                    "TIMESTAMPDIFF actual text/work allowance exceeded".into(),
                ),
                None,
            ))
        }
    }
}
pub(crate) fn timestamp_diff_values_bounded(
    unit: &Datum,
    left: &Datum,
    right: &Datum,
    maximum: usize,
) -> Result<Datum, EvalError> {
    tidb_query_expr::native_timestamp_diff_text_values(
        [
            unit.as_shared_json_input(),
            left.as_shared_json_input(),
            right.as_shared_json_input(),
        ],
        maximum,
    )
    .map(|value| value.map_or(Datum::Null, Datum::Int))
    .map_err(string_error)
}
