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
use tidb_datatype::{EvalType, FieldType, SessionTimeZone};
use tidb_query_expr::{NativeCastIntegerResult as Report, NativeCastIntegerTarget as Target};

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum LegacyCastIntegerResult {
    Value(i128),
    Overflow(f64),
}

pub fn eval_legacy_cast_integer_datum(value: &Datum) -> Option<LegacyCastIntegerResult> {
    use tidb_query_datatype::codec::native_numeric::NativeLegacyIntegerCast as Native;

    tidb_query_datatype::codec::native_numeric::native_legacy_cast_integer(
        value.as_shared_numeric_input(),
    )
    .map(|result| match result {
        Native::Value(value) => LegacyCastIntegerResult::Value(i128::from(value)),
        Native::Overflow(value) => LegacyCastIntegerResult::Overflow(value),
    })
}

#[cfg(test)]
fn input(value: &Datum) -> tidb_query_expr::NativeCastIntegerInput<'_> {
    tidb_query_expr::native_cast_integer_input_from_numeric(value.as_shared_numeric_input())
}

fn invalid_result() -> EvalError {
    use super::adapter_failure::{ExpressionAdapterFailure, ScopeFailureKind};
    EvalError::ExpressionAdapterFailure(ExpressionAdapterFailure::from_scope(
        ScopeFailureKind::Contract,
        "invalid native integer cast result domain",
    ))
}

fn evaluate(
    ctx: &dyn Columns,
    value: &Datum,
    target: Target,
    source: Option<tidb_query_expr::NativeIntervalEvalType>,
) -> Result<Report, EvalError> {
    tidb_query_expr::native_cast_integer_numeric(
        value.as_shared_numeric_input(),
        target,
        source,
        || ctx.time_zone(),
        // This is the existing real/Float32 worker and its original authority.
        // Unlike datatype errors, its infrastructure errors must not be folded.
        || super::eval_cast_real_unsigned_in(ctx, value),
        |message| ctx.handle_truncate(message),
        |code, message| ctx.append_warning(code, message),
    )
}

pub(crate) fn eval_cast_arg_as_int_in(
    ctx: &dyn Columns,
    value: &Datum,
    source: Option<&FieldType>,
) -> Result<Datum, EvalError> {
    use tidb_query_expr::{NativeArgIntegerError, NativeArgIntegerResult};
    tidb_query_expr::native_cast_arg_as_int(
        value.as_shared_numeric_input(),
        source.map(FieldType::is_unsigned),
        || ctx.time_zone(),
        || super::eval_cast_real_unsigned_in(ctx, value),
        |message| ctx.handle_truncate(message),
        |code, message| ctx.append_warning(code, message),
    )
    .map(|result| match result {
        NativeArgIntegerResult::Original => value.clone(),
        NativeArgIntegerResult::Signed(value) => Datum::Int(value),
        NativeArgIntegerResult::Unsigned(value) => Datum::UInt(value),
    })
    .map_err(|error| match error {
        NativeArgIntegerError::Unsupported(message) => EvalError::Unsupported(message),
        NativeArgIntegerError::Effect(error) => error,
    })
}

pub(crate) fn eval_cast_signed_value_in(value: &Datum, zone: &SessionTimeZone) -> i64 {
    tidb_query_expr::native_cast_integer_signed_numeric(value.as_shared_numeric_input(), zone)
}

pub(crate) fn eval_cast_signed_in(ctx: &dyn Columns, value: &Datum) -> Result<i64, EvalError> {
    match evaluate(ctx, value, Target::Signed, None)? {
        Report::Signed(value) => Ok(value),
        _ => Err(invalid_result()),
    }
}

pub(crate) fn eval_cast_unsigned_in(ctx: &dyn Columns, value: &Datum) -> Result<u64, EvalError> {
    match evaluate(ctx, value, Target::Unsigned, None)? {
        Report::Unsigned(value) => Ok(value),
        _ => Err(invalid_result()),
    }
}

pub(crate) fn eval_cast_unsigned_union_in(
    ctx: &dyn Columns,
    value: &Datum,
    source: Option<&FieldType>,
) -> Result<u64, EvalError> {
    match evaluate(ctx, value, Target::UnsignedInUnion, source_type(source))? {
        Report::Unsigned(value) => Ok(value),
        _ => Err(invalid_result()),
    }
}

pub(crate) fn report_cast_integer_input_in(
    ctx: &dyn Columns,
    value: &Datum,
) -> Result<(), EvalError> {
    tidb_query_expr::native_cast_integer_numeric_input_warning(
        value.as_shared_numeric_input(),
        |message| ctx.handle_truncate(message),
    )
}

// Retain the original private test entry's value-only contract. It must not
// acquire the ordinary unsigned entry's input truncation or 8031 diagnostic.
#[cfg(test)]
pub(crate) fn eval_cast_unsigned_value_in(
    ctx: &dyn Columns,
    value: &Datum,
) -> Result<u64, EvalError> {
    tidb_query_expr::native_cast_integer_unsigned_numeric(
        value.as_shared_numeric_input(),
        || ctx.time_zone(),
        || super::eval_cast_real_unsigned_in(ctx, value),
        |code, message| ctx.append_warning(code, message),
    )
}

fn source_type(source: Option<&FieldType>) -> Option<tidb_query_expr::NativeIntervalEvalType> {
    use tidb_query_expr::NativeIntervalEvalType as Type;
    source.map(|source| match source.eval_type() {
        EvalType::Int => Type::Int,
        EvalType::Real => Type::Real,
        EvalType::Decimal => Type::Decimal,
        EvalType::String => Type::String,
        EvalType::Datetime => Type::Datetime,
        EvalType::Timestamp => Type::Timestamp,
        EvalType::Duration => Type::Duration,
        EvalType::Json => Type::Json,
        EvalType::VectorFloat32 => Type::VectorFloat32,
    })
}

#[cfg(test)]
#[test]
fn legacy_integer_cast_bridge_projects_values_overflow_and_unsupported() {
    assert_eq!(
        eval_legacy_cast_integer_datum(&Datum::Real(2.5)),
        Some(LegacyCastIntegerResult::Value(3))
    );
    assert!(matches!(
        eval_legacy_cast_integer_datum(&Datum::Real(9.3e18)),
        Some(LegacyCastIntegerResult::Overflow(_))
    ));
    assert_eq!(eval_legacy_cast_integer_datum(&Datum::Null), None);
}
