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
        digest_operation(name)
    }
}
/// Original digest operation spellings in the same actual-value frame.
pub(crate) fn digest_operation(name: &str) -> Option<O> {
    if name.eq_ignore_ascii_case("md5") {
        Some(O::Md5)
    } else if name.eq_ignore_ascii_case("sha") {
        Some(O::Sha)
    } else if name.eq_ignore_ascii_case("sha1") {
        Some(O::Sha1)
    } else if name.eq_ignore_ascii_case("sha2") {
        Some(O::Sha2)
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
pub(super) fn input<'a>(
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
pub(super) fn held(input: &Option<Cow<'_, [u8]>>) -> usize {
    match input {
        Some(Cow::Owned(v)) => v.capacity(),
        _ => 0,
    }
}
// Bound only storage actually read by the original length converter. Visible
// Decimal precision, hybrid names and unsupported Raw/vector suffixes are unused.
fn digest_length(value: &Datum, maximum: usize) -> Result<Option<i64>, EvalError> {
    check(0, maximum)?;
    let mut n = match value {
        Datum::String(v) => v.bytes().len(),
        Datum::Bytes(v) => v.len(),
        Datum::Decimal(v) => v.coefficient_bytes().len(),
        Datum::Json(_) => {
            if let tidb_query_datatype::codec::native_numeric::NativeNumericInput::Json {
                type_code: 0x0c,
                value,
            } = value.as_shared_numeric_input()
            {
                value.len()
            } else {
                0
            }
        }
        _ => 0,
    };
    if let Datum::Bit(v) | Datum::BinaryLiteral(v) = value {
        check(4096, maximum)?;
        let bytes = v.as_bytes();
        let scan = (maximum - 4096) / 32;
        n = match bytes.iter().take(scan).position(|byte| *byte != 0) {
            Some(index) => index + 9.min(bytes.len() - index),
            None if bytes.len() <= scan => bytes.len(),
            None => return Err(error(E::Resource)),
        };
    }
    check(
        n.checked_mul(32)
            .and_then(|n| n.checked_add(4096))
            .ok_or_else(|| error(E::Resource))?,
        maximum,
    )?;
    crate::builtin_ext::crypto::encryption_int_argument(value)
}
/// Pure body: all expression demand and PASSWORD's warning have already run.
pub(crate) fn body(op: O, values: &[&Datum], maximum: usize) -> Result<Datum, EvalError> {
    check(0, maximum)?;
    if values.len() != op.arity() {
        return Err(error(E::Contract));
    }
    let unary = matches!(op, O::Md5 | O::Sha | O::Sha1 | O::Sm3 | O::Password);
    let data = input(values[0], unary || op == O::Sha2, maximum)?;
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
    if op == O::Sha2 {
        let Some(length) = digest_length(values[1], maximum - base)? else {
            return Ok(Datum::Null);
        };
        return tidb_query_expr::native_sha2_actual_value(data_ref, length, maximum - base)
            .map(|v| v.map_or(Datum::Null, Datum::new_string))
            .map_err(error);
    }
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
