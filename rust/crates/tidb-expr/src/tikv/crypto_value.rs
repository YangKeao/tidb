// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Actual deterministic crypto inputs; original warning and coercion phases.
use crate::{Columns, Datum, EvalError};
use std::borrow::Cow;
use tidb_query_expr::{NativeCryptoOperation as O, NativeCryptoValueError as E};
fn error(e: E) -> EvalError {
    match e {
        E::Resource => {
            EvalError::ExpressionRuntimeFailure(super::ExpressionRuntimeFailure::from_local_eval(
                super::LocalError::ResourceLimit("crypto value work allowance exceeded".into()),
                None,
            ))
        }
        E::Contract => {
            use super::adapter_failure::{ExpressionAdapterFailure, ScopeFailureKind};
            EvalError::ExpressionAdapterFailure(ExpressionAdapterFailure::from_scope(
                ScopeFailureKind::Contract,
                "crypto actual value contract failure",
            ))
        }
    }
}
fn check(n: usize, maximum: usize) -> Result<(), EvalError> {
    if maximum == usize::MAX || n > maximum {
        Err(error(E::Resource))
    } else {
        Ok(())
    }
}
pub(crate) fn operation(name: &str) -> Option<O> {
    if name.eq_ignore_ascii_case("sm3") {
        Some(O::Sm3)
    } else if name.eq_ignore_ascii_case("password") {
        Some(O::Password)
    } else if name.eq_ignore_ascii_case("encode") {
        Some(O::Encode)
    } else if name.eq_ignore_ascii_case("decode") {
        Some(O::Decode)
    } else {
        None
    }
}
/// This original warning precedes coercion/NULL, and runs without a store borrow.
pub(crate) fn before_body(op: O, ctx: &dyn Columns) {
    if op == O::Password {
        ctx.append_warning(
            1681,
            "PASSWORD is deprecated and will be removed in a future release.",
        );
    }
}
fn input<'a>(
    value: &'a Datum,
    hash: bool,
    maximum: usize,
) -> Result<Option<Cow<'a, [u8]>>, EvalError> {
    check(0, maximum)?;
    let original = || {
        if hash {
            crate::builtin_ext::crypto::hash_input(value)
        } else {
            crate::builtin_ext::crypto::sql_string_bytes(value)
        }
    };
    match value {
        Datum::Null => return Ok(None),
        Datum::String(v) => return Ok(Some(Cow::Borrowed(v.bytes()))),
        Datum::Bytes(v) => return Ok(Some(Cow::Borrowed(v))),
        Datum::Decimal(v) => {
            check(
                v.coefficient_bytes()
                    .len()
                    .checked_mul(16)
                    .ok_or_else(|| error(E::Resource))?,
                maximum,
            )?;
            if std::str::from_utf8(v.coefficient_bytes()).is_err() {
                return original().map(|v| v.map(Cow::Owned));
            }
        }
        _ => {}
    }
    match tidb_query_expr::native_byte_coercion_work(value.as_shared_json_input()) {
        Ok(n) => check(n, maximum)?,
        Err(tidb_query_expr::NativeStringCoercionError::ResourceLimit) => {
            return Err(error(E::Resource))
        }
        // The original helper owns HASH versus STRING subjects and Raw byte policy.
        Err(tidb_query_expr::NativeStringCoercionError::Unsupported(_)) => {}
    }
    let owned = original()?;
    check(owned.as_ref().map_or(0, Vec::capacity), maximum)?;
    Ok(owned.map(Cow::Owned))
}
fn held(input: &Option<Cow<'_, [u8]>>) -> usize {
    match input {
        Some(Cow::Owned(v)) => v.capacity(),
        _ => 0,
    }
}
/// Pure body: all expression demand and PASSWORD's warning have already run.
pub(crate) fn body(op: O, values: &[&Datum], maximum: usize) -> Result<Datum, EvalError> {
    check(0, maximum)?;
    if values.len() != op.arity() {
        return Err(error(E::Contract));
    }
    let unary = matches!(op, O::Sm3 | O::Password);
    let data = input(values[0], unary, maximum)?;
    let base = held(&data);
    check(base, maximum)?;
    if unary {
        return tidb_query_expr::native_crypto_unary_actual_value(
            op,
            data.as_deref(),
            maximum - base,
        )
        .map(|v| v.map_or(Datum::Null, Datum::new_string))
        .map_err(error);
    }
    let Some(data_ref) = data.as_deref() else {
        return Ok(Datum::Null);
    };
    let password = input(values[1], false, maximum - base)?;
    let Some(password_ref) = password.as_deref() else {
        return Ok(Datum::Null);
    };
    let held = base
        .checked_add(held(&password))
        .ok_or_else(|| error(E::Resource))?;
    check(held, maximum)?;
    tidb_query_expr::native_sql_crypt_actual_value(op, data_ref, password_ref, maximum - held)
        .map(Datum::new_string)
        .map_err(error)
}
pub(crate) fn eval_refs(
    op: O,
    values: &[&Datum],
    ctx: &dyn Columns,
    maximum: usize,
) -> Result<Datum, EvalError> {
    if values.len() != op.arity() {
        return Err(error(E::Contract));
    }
    before_body(op, ctx);
    body(op, values, maximum)
}
