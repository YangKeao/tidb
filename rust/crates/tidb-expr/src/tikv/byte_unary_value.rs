// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Original byte-unary coercion policies over actual values.
use crate::{Datum, EvalError};
use tidb_query_expr::{NativeByteUnaryError as E, NativeByteUnaryOperation as O};
fn error(e: E) -> EvalError {
    match e {
        E::Resource => {
            EvalError::ExpressionRuntimeFailure(super::ExpressionRuntimeFailure::from_local_eval(
                super::LocalError::ResourceLimit("byte unary work allowance exceeded".into()),
                None,
            ))
        }
        E::Contract => {
            use super::adapter_failure::{ExpressionAdapterFailure, ScopeFailureKind};
            EvalError::ExpressionAdapterFailure(ExpressionAdapterFailure::from_scope(
                ScopeFailureKind::Contract,
                "byte unary actual value contract failure",
            ))
        }
    }
}
fn check(n: usize, max: usize) -> Result<(), EvalError> {
    if max == usize::MAX || n > max {
        Err(error(E::Resource))
    } else {
        Ok(())
    }
}
pub(crate) fn operation(name: &str) -> Option<O> {
    if name.eq_ignore_ascii_case("ascii") {
        Some(O::Ascii)
    } else if name.eq_ignore_ascii_case("ltrim") {
        Some(O::LTrim)
    } else if name.eq_ignore_ascii_case("rtrim") {
        Some(O::RTrim)
    } else {
        None
    }
}
fn format_work(value: &Datum, max: usize) -> Result<(), EvalError> {
    check(0, max)?;
    if let Datum::Decimal(v) = value {
        check(
            v.coefficient_bytes()
                .len()
                .checked_mul(16)
                .ok_or_else(|| error(E::Resource))?,
            max,
        )?;
        if std::str::from_utf8(v.coefficient_bytes()).is_err() {
            // Execute the genuine formatter's coefficient panic before irrelevant scale.
            let _ = crate::coerce::coerce_str_bytes(value)?;
        }
    }
    match tidb_query_expr::native_byte_coercion_work(value.as_shared_json_input()) {
        Ok(n) => check(n, max),
        Err(tidb_query_expr::NativeStringCoercionError::Unsupported(e)) => {
            Err(EvalError::Unsupported(e))
        }
        Err(tidb_query_expr::NativeStringCoercionError::ResourceLimit) => Err(error(E::Resource)),
    }
}
pub(crate) fn body(op: O, value: &Datum, max: usize) -> Result<Datum, EvalError> {
    check(0, max)?;
    if op == O::Ascii {
        use tidb_query_datatype::codec::native_sql_string::NativeSqlStringInput as I;
        let bytes = match value.as_shared_json_input() {
            I::Null => None,
            I::String(v)
            | I::Bytes(v)
            | I::BinaryLiteral(v)
            | I::Bit(v)
            | I::Enum(v)
            | I::Set(v)
            | I::Raw(v) => Some(v),
            _ => {
                format_work(value, max)?;
                let owned = crate::coerce::coerce_str_bytes(value)?;
                let held = owned.as_ref().map_or(0, Vec::capacity);
                check(held, max)?;
                return tidb_query_expr::native_ascii_actual_value(owned.as_deref(), max - held)
                    .map(|v| v.map_or(Datum::Null, Datum::Int))
                    .map_err(error);
            }
        };
        return tidb_query_expr::native_ascii_actual_value(bytes, max)
            .map(|v| v.map_or(Datum::Null, Datum::Int))
            .map_err(error);
    }
    let bytes = crate::arg_eval_type::eval_string_ref(value)?;
    let trimmed =
        tidb_query_expr::native_trim_actual_value(bytes, op == O::LTrim, max).map_err(error)?;
    let Some(trimmed) = trimmed else {
        return Ok(Datum::Null);
    };
    check(trimmed.len(), max)?;
    let bytes = trimmed.to_vec();
    check(bytes.capacity(), max)?;
    Ok(if crate::string_signature::is_binary_str(value) {
        Datum::new_bytes(bytes)
    } else {
        Datum::new_string(bytes)
    })
}
/// Apply the original pure ETString wrapper only where the signature has one.
pub(crate) fn scalar_body(op: O, value: &Datum, max: usize) -> Result<Datum, EvalError> {
    check(0, max)?;
    if op == O::Ascii
        || matches!(
            value,
            Datum::Null
                | Datum::String(_)
                | Datum::Bytes(_)
                | Datum::BinaryLiteral(_)
                | Datum::Enum(..)
                | Datum::Set(..)
        )
    {
        // Original ETString action is identity; retain its actual header/collation.
        return body(op, value, max);
    }
    format_work(value, max)?;
    let cast = crate::cast::cast_arg_as_string(value, None, &crate::NoColumns)?;
    let held = super::identity_actual::copy_extent(&cast).map_err(|_| error(E::Resource))?;
    check(held, max)?;
    body(op, &cast, max - held)
}
