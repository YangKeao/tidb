// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Actual identity transport, with the datatype's original Clone primitive.
//! The identity body does not coerce, normalize, format or encode. Separate
//! helpers retain the original scalar signature prefix and return conversion.
use crate::{Datum, EvalError};
fn resource() -> EvalError {
    EvalError::ExpressionRuntimeFailure(super::ExpressionRuntimeFailure::from_local_eval(
        super::LocalError::ResourceLimit("native identity copy allowance exceeded".into()),
        None,
    ))
}
/// Payload copied by the original actual-value Clone. Fixed scalar metadata and
/// shared GoString headers require no input-length-proportional copy. Decimal
/// scale/shape and invalid JSON/vector values are opaque identity metadata.
pub(crate) fn copy_extent(value: &Datum) -> Result<usize, EvalError> {
    Ok(match value {
        Datum::Bytes(v) | Datum::Raw(v) => v.len(),
        Datum::BinaryLiteral(v) | Datum::Bit(v) => v.as_bytes().len(),
        Datum::Decimal(v) => v.coefficient_bytes().len(),
        Datum::Json(v) => v.value().len(),
        Datum::VectorFloat32(v) => v
            .len()
            .checked_mul(std::mem::size_of::<f32>())
            .ok_or_else(resource)?,
        _ => 0,
    })
}
/// Bound demanded owned payload before cloning the authentic selected value.
/// This is a logical copy allowance, not a physical whole-statement heap limit.
pub(crate) fn copy_bounded(value: &Datum, maximum: usize) -> Result<Datum, EvalError> {
    if maximum == usize::MAX || copy_extent(value)? > maximum {
        return Err(resource());
    }
    Ok(value.clone())
}
pub(crate) fn copy(value: &Datum) -> Result<Datum, EvalError> {
    copy_bounded(value, super::control_value::default_budget())
}

/// Preserve ScalarFunction's pre-existing constant-first uservar-name probe,
/// whose ignored formatting result can still panic before child evaluation.
/// This is separate from the actual identity body and never evaluates a child.
pub(crate) fn probe(value: Option<&Datum>, maximum: usize) -> Result<(), EvalError> {
    let Some(value) = value else { return Ok(()) };
    if maximum == usize::MAX {
        return Err(resource());
    }
    if let Datum::Decimal(v) = value {
        // The original formatter validates coefficient UTF-8 BEFORE it touches
        // visible precision. Bound that demanded prefix first, and retain the
        // same real formatter panic even with huge unused scale metadata.
        let prefix = v
            .coefficient_bytes()
            .len()
            .checked_mul(16)
            .and_then(|n| n.checked_add(1024))
            .ok_or_else(resource)?;
        if prefix > maximum {
            return Err(resource());
        }
        if std::str::from_utf8(v.coefficient_bytes()).is_err() {
            let _ = value.sql_string();
            return Ok(());
        }
    }
    let work = match tidb_query_expr::native_byte_coercion_work(value.as_shared_json_input()) {
        Ok(n) => n,
        // Original .sql_string().ok() discards ordinary coercion errors.
        Err(tidb_query_expr::NativeStringCoercionError::Unsupported(_)) => 0,
        Err(tidb_query_expr::NativeStringCoercionError::ResourceLimit) => return Err(resource()),
    };
    if work > maximum {
        return Err(resource());
    }
    let _ = value.sql_string();
    Ok(())
}

pub(crate) fn default_budget() -> usize {
    super::control_value::default_budget()
}

/// The ordinary result conversion still validates a demanded malformed Decimal
/// coefficient before visible scale. Do not let conservative formatter work
/// estimation turn that original panic into a metadata-only Resource failure.
pub(crate) fn coerce_result(
    name: &str,
    field: &tidb_datatype::FieldType,
    value: Datum,
    maximum: usize,
) -> Result<Datum, EvalError> {
    if maximum == usize::MAX {
        return Err(resource());
    }
    if !crate::scalar_function::same_eval_family(&value, field) {
        if let Datum::Decimal(decimal) = &value {
            let prefix = decimal
                .coefficient_bytes()
                .len()
                .checked_mul(16)
                .and_then(|n| n.checked_add(1024))
                .ok_or_else(resource)?;
            if prefix > maximum {
                return Err(resource());
            }
            if std::str::from_utf8(decimal.coefficient_bytes()).is_err() {
                // The real original conversion owns disposition and panic. No
                // synthetic value, reconstructed answer or forced panic.
                return crate::scalar_function::coerce_function_result(name, Some(field), value);
            }
        }
    }
    super::coerce_result_bounded(name, field, value, maximum)
}
