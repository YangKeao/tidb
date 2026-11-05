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

pub(crate) fn copy(value: &Datum) -> Result<Datum, EvalError> {
    Ok(value.clone())
}

/// Preserve ScalarFunction's pre-existing constant-first uservar-name probe,
/// whose ignored formatting result can still panic before child evaluation.
/// This is separate from the actual identity body and never evaluates a child.
pub(crate) fn probe(value: Option<&Datum>) -> Result<(), EvalError> {
    probe_name(value).map(|_| ())
}
/// Same original formatting operation, retaining its name only when demanded.
pub(crate) fn probe_name(value: Option<&Datum>) -> Result<Option<String>, EvalError> {
    Ok(value.and_then(|value| value.sql_string().ok()))
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
) -> Result<Datum, EvalError> {
    crate::scalar_function::coerce_function_result(name, Some(field), value)
}
