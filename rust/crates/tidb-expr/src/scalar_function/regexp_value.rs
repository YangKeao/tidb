// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Original regexp value conversions, separate from child demands and caches.
use crate::{Columns, Datum, EvalError};
use tidb_datatype::FieldType;

pub(crate) fn named_string(
    value: Datum,
    source: Option<&FieldType>,
    ctx: &dyn Columns,
) -> Result<Option<String>, EvalError> {
    let value = crate::cast::cast_arg_as_string(&value, source, ctx)?;
    if value.is_null() {
        return Ok(None);
    }
    value
        .sql_string()
        .map(Some)
        .map_err(|_| EvalError::Unsupported("invalid UTF-8 REGEXP_LIKE argument"))
}
pub(crate) fn operator_strings(
    value: &Datum,
    pattern: &Datum,
) -> Result<(String, String), EvalError> {
    let text = value
        .sql_string()
        .map_err(|_| EvalError::Unsupported("invalid UTF-8 REGEXP operand"))?;

    let pattern = pattern
        .sql_string()
        .map_err(|_| EvalError::Unsupported("invalid UTF-8 REGEXP pattern"))?;
    Ok((text, pattern))
}

/// The selected scalar implementation rejects arity before cache or children.
pub(crate) fn accepts_arity(arity: usize) -> bool {
    matches!(arity, 2 | 3)
}
pub(crate) fn arity_error() -> EvalError {
    EvalError::WrongParameterCount("regexp_like")
}
