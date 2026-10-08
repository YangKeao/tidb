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

use crate::{Columns, Datum, EvalError};
#[cfg(test)]
use tidb_query_expr::NativeCastFloatInput as Input;
use tidb_query_expr::NativeCastFloatTarget as Target;

#[cfg(test)]
fn input(value: &Datum) -> Input<'_> {
    match value {
        Datum::Null => Input::Null,
        Datum::MinNotNull => Input::MinNotNull,
        Datum::MaxValue => Input::MaxValue,
        Datum::Int(value) => Input::Int(*value),
        Datum::UInt(value) => Input::UInt(*value),
        Datum::Decimal(value) => Input::Decimal(value.as_shared_parse()),
        Datum::Real(value) => Input::Real(*value),
        Datum::Float32(value) => Input::Float32(*value),
        Datum::String(value) => Input::String(value.bytes()),
        Datum::Bytes(value) => Input::Bytes(value),
        Datum::Json(_) => Input::Json,
        _ => Input::Other,
    }
}

pub fn eval_legacy_numeric_prefix(text: &str, allow_float: bool) -> Option<String> {
    tidb_query_datatype::codec::native_float_parse::native_legacy_numeric_prefix(text, allow_float)
}

pub fn eval_legacy_cast_real_integer(value: i128) -> f64 {
    tidb_query_datatype::codec::native_scalar_convert::native_legacy_cast_real_integer(value)
}

pub fn eval_legacy_cast_real_datum(value: &Datum) -> Option<f64> {
    tidb_query_datatype::codec::native_scalar_convert::native_legacy_cast_real(
        value.as_shared_numeric_input(),
    )
}

fn evaluate(ctx: &dyn Columns, value: &Datum, target: Target) -> Result<f64, EvalError> {
    tidb_query_expr::native_cast_float_numeric(value.as_shared_numeric_input(), target, |message| {
        ctx.handle_truncate(message)
    })
    .map_err(|error| match error {
        tidb_query_expr::NativeCastFloatError::Child(error) => error,
        tidb_query_expr::NativeCastFloatError::ConstantFloatCastOverflow { value } => {
            EvalError::ConstantFloatCastOverflow { value }
        }
    })
}

pub(crate) fn eval_cast_double_in(ctx: &dyn Columns, value: &Datum) -> Result<f64, EvalError> {
    evaluate(ctx, value, Target::Double)
}

pub(crate) fn eval_cast_float_in(ctx: &dyn Columns, value: &Datum) -> Result<f64, EvalError> {
    evaluate(ctx, value, Target::Float)
}

/// This is the distinct strict-UTF-8 value-only surface, not the ordinary
/// lossy string/JSON cast with a statement warning callback.
pub(crate) fn eval_cast_float_value(value: &Datum) -> f64 {
    tidb_query_expr::native_cast_float_numeric_value(value.as_shared_numeric_input())
}

#[cfg(test)]
#[test]
fn legacy_real_cast_bridge_keeps_i128_datum_and_folded_error_projection() {
    assert_eq!(eval_legacy_cast_real_integer(-7), -7.0);
    assert_eq!(eval_legacy_cast_real_datum(&Datum::Real(2.5)), Some(2.5));
    assert_eq!(
        eval_legacy_cast_real_datum(&Datum::new_bytes(b"12.5tail".to_vec())),
        Some(12.5)
    );
    assert_eq!(eval_legacy_cast_real_datum(&Datum::MinNotNull), None);
}
