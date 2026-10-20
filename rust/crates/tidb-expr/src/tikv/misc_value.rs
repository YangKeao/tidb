// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Actual misc operands; distinct flag phases stay outside SDK value kernels.
use crate::{Columns, Datum, EvalError};
use tidb_query_expr::{NativeMiscOperation as O, NativeMiscValueError as E};
fn resource() -> EvalError {
    EvalError::ExpressionRuntimeFailure(super::ExpressionRuntimeFailure::from_local_eval(
        super::LocalError::ResourceLimit("native misc work allowance exceeded".into()),
        None,
    ))
}
fn contract() -> EvalError {
    use super::adapter_failure::{ExpressionAdapterFailure, ScopeFailureKind};
    EvalError::ExpressionAdapterFailure(ExpressionAdapterFailure::from_scope(
        ScopeFailureKind::Contract,
        "native misc actual value contract failure",
    ))
}
fn error(e: E) -> EvalError {
    match e {
        E::Resource => resource(),
        E::Contract => contract(),
        E::ToBinWhitespace => EvalError::Unsupported("invalid UUID_TO_BIN whitespace"),
        E::ToBinInvalid => EvalError::Unsupported("invalid UUID for UUID_TO_BIN"),
        E::VersionInvalid => EvalError::Unsupported("invalid UUID for UUID_VERSION"),
        E::TimestampInvalid => EvalError::Unsupported("invalid UUID for UUID_TIMESTAMP"),
        E::BinLength(input) => EvalError::WrongValueForType {
            value_class: "string",
            value: String::from_utf8_lossy(&input).into_owned(),
            function: "bin_to_uuid",
        },
    }
}
fn check(n: usize, maximum: usize) -> Result<(), EvalError> {
    tidb_query_expr::native_misc_work(maximum, n).map_err(error)
}
fn extent(n: usize, factor: usize, maximum: usize) -> Result<(), EvalError> {
    check(
        n.checked_mul(factor)
            .and_then(|n| n.checked_add(4096))
            .ok_or_else(resource)?,
        maximum,
    )
}
pub(crate) fn operation(name: &str) -> Option<O> {
    // ScalarFunction probes this selector even for other function names. Keep
    // the closed ASCII lookup allocation-free on every ordinary scalar row.
    Some(if name.eq_ignore_ascii_case("is_uuid") {
        O::IsUuid
    } else if name.eq_ignore_ascii_case("uuid_version") {
        O::UuidVersion
    } else if name.eq_ignore_ascii_case("uuid_timestamp") {
        O::UuidTimestamp
    } else if name.eq_ignore_ascii_case("uuid_to_bin") {
        O::UuidToBin
    } else if name.eq_ignore_ascii_case("bin_to_uuid") {
        O::BinToUuid
    } else if name.eq_ignore_ascii_case("tidb_shard") {
        O::TidbShard
    } else if name.eq_ignore_ascii_case("vitess_hash") {
        O::VitessHash
    } else {
        return None;
    })
}
fn bytes(value: &Datum, strict: bool, maximum: usize) -> Result<Option<Vec<u8>>, EvalError> {
    check(0, maximum)?;
    let original = || {
        if strict {
            crate::coerce::coerce_str(value).map(|v| v.map(String::into_bytes))
        } else {
            crate::coerce::coerce_str_bytes(value)
        }
    };
    if value.is_null() {
        return original();
    }
    if let Datum::Decimal(v) = value {
        extent(v.coefficient_bytes().len(), 16, maximum)?;
        if std::str::from_utf8(v.coefficient_bytes()).is_err() {
            return original();
        }
    }
    match tidb_query_expr::native_byte_coercion_work(value.as_shared_json_input()) {
        Ok(n) => check(n, maximum)?,
        Err(tidb_query_expr::NativeStringCoercionError::ResourceLimit) => return Err(resource()),
        Err(tidb_query_expr::NativeStringCoercionError::Unsupported(_)) => {}
    }
    let result = original()?;
    check(result.as_ref().map_or(0, Vec::capacity), maximum)?;
    Ok(result)
}
/// Bound only demanded integer storage and exceptional diagnostics. Hybrid
/// ordinals ignore their actual shared names; raw temporal FSP retains the
/// original constant-size numeric slicing/panic boundary.
fn integer_work(value: &Datum, warnings: bool, maximum: usize) -> Result<(), EvalError> {
    check(0, maximum)?;
    match value {
        Datum::String(v) => extent(v.bytes().len(), 32, maximum)?,
        Datum::Bytes(v) => extent(v.len(), 32, maximum)?,
        Datum::Json(v) => extent(v.value().len(), 128, maximum)?,
        Datum::Decimal(v) => {
            extent(v.coefficient_bytes().len(), 32, maximum)?;
            if warnings
                && v.round_to_i64().is_none()
                && std::str::from_utf8(v.coefficient_bytes()).is_ok()
            {
                let n = v
                    .coefficient_bytes()
                    .len()
                    .checked_add(v.scale().max(v.storage_scale()) as usize)
                    .ok_or_else(resource)?;
                extent(n, 32, maximum)?;
            }
        }
        _ => check(4096, maximum)?,
    }
    Ok(())
}
/// The two original optional flag policies. Callers supply real evaluated
/// values or genuine absence; no operand/answer is synthesized here.
pub(crate) fn flag(
    op: O,
    value: Option<&Datum>,
    ctx: &dyn Columns,
    maximum: usize,
) -> Result<i64, EvalError> {
    check(0, maximum)?;
    let Some(value) = value.filter(|v| !v.is_null()) else {
        return Ok(0);
    };
    integer_work(value, false, maximum)?;
    if op == O::BinToUuid && matches!(value, Datum::String(_) | Datum::Bytes(_)) {
        crate::cast::report_int_truncation(value, ctx)?;
    }
    Ok(crate::cast::to_i64_signed(value))
}
pub(crate) fn parse(value: &Datum, maximum: usize) -> Result<Option<[u8; 16]>, EvalError> {
    let input = bytes(value, false, maximum)?;
    let held = input.as_ref().map_or(0, Vec::capacity);
    check(held, maximum)?;
    tidb_query_expr::native_uuid_parse_value(input.as_deref(), maximum - held).map_err(error)
}
pub(crate) fn swap(parsed: &[u8; 16], flag: i64, maximum: usize) -> Result<Datum, EvalError> {
    tidb_query_expr::native_uuid_swap_value(parsed, flag, maximum)
        .map(Datum::Bytes)
        .map_err(error)
}
pub(crate) fn binary(value: &Datum, flag: i64, maximum: usize) -> Result<Datum, EvalError> {
    let input = bytes(value, false, maximum)?;
    let held = input.as_ref().map_or(0, Vec::capacity);
    check(held, maximum)?;
    tidb_query_expr::native_bin_to_uuid_value(input.as_deref(), flag, maximum - held)
        .map(|v| v.map_or(Datum::Null, Datum::new_string))
        .map_err(error)
}
pub(crate) fn unary(op: O, value: &Datum, maximum: usize) -> Result<Datum, EvalError> {
    let input = bytes(value, op != O::IsUuid, maximum)?;
    let held = input.as_ref().map_or(0, Vec::capacity);
    check(held, maximum)?;
    let maximum = maximum - held;
    match op {
        O::IsUuid => tidb_query_expr::native_is_uuid_value(input.as_deref(), maximum)
            .map(|v| v.map_or(Datum::Null, Datum::Int))
            .map_err(error),
        O::UuidVersion => tidb_query_expr::native_uuid_version_value(input.as_deref(), maximum)
            .map(|v| v.map_or(Datum::Null, Datum::Int))
            .map_err(error),
        O::UuidTimestamp => {
            let value = tidb_query_expr::native_uuid_timestamp_value(input.as_deref(), maximum)
                .map_err(error)?;
            match value {
                None => Ok(Datum::Null),
                Some(v) => tidb_datatype::Decimal::try_from_shared_math(&v, maximum)
                    .map(Datum::Decimal)
                    .map_err(|_| contract()),
            }
        }
        _ => Err(contract()),
    }
}
pub(crate) fn hash(
    op: O,
    value: &Datum,
    ctx: &dyn Columns,
    maximum: usize,
) -> Result<Datum, EvalError> {
    check(0, maximum)?;
    let input = if value.is_null() {
        None
    } else {
        integer_work(value, true, maximum)?;
        Some(crate::cast::to_i64_signed_with_warnings(value, ctx)?)
    };
    tidb_query_expr::native_shard_value(input, op == O::TidbShard, maximum)
        .map(|v| v.map_or(Datum::Null, Datum::UInt))
        .map_err(error)
}
pub(crate) fn eval_refs(
    op: O,
    values: &[&Datum],
    ctx: &dyn Columns,
    maximum: usize,
) -> Result<Datum, EvalError> {
    if !op.accepts_arity(values.len()) {
        return Err(EvalError::Unsupported("bad function arity"));
    }
    match op {
        O::UuidToBin => {
            let Some(parsed) = parse(values[0], maximum)? else {
                return Ok(Datum::Null);
            };
            let flag = flag(op, values.get(1).copied(), ctx, maximum)?;
            swap(&parsed, flag, maximum)
        }
        O::BinToUuid => {
            let flag = flag(op, values.get(1).copied(), ctx, maximum)?;
            binary(values[0], flag, maximum)
        }
        O::TidbShard | O::VitessHash => hash(op, values[0], ctx, maximum),
        _ => unary(op, values[0], maximum),
    }
}

/// These two original integer-source kinds have neither input nor signed
/// warnings. The ordinary cast still reads the session zone BEFORE its pure
/// datatype conversion; their unused payload must not be copied for that call.
pub(crate) fn prepare_borrowed_hash(value: &Datum, maximum: usize) -> Result<bool, EvalError> {
    let borrowed = matches!(value, Datum::Raw(_) | Datum::VectorFloat32(_));
    if borrowed {
        integer_work(value, true, maximum)?;
    }
    Ok(borrowed)
}
/// Continue the same original signed conversion with its genuinely captured
/// zone, over the authenticated actual operand. No context callback occurs.
pub(crate) fn hash_after_zone(
    op: O,
    value: &Datum,
    zone: &tidb_datatype::SessionTimeZone,
    maximum: usize,
) -> Result<Datum, EvalError> {
    let input = crate::cast::to_i64_signed_in(value, zone);
    tidb_query_expr::native_shard_value(Some(input), op == O::TidbShard, maximum)
        .map(|v| v.map_or(Datum::Null, Datum::UInt))
        .map_err(error)
}
