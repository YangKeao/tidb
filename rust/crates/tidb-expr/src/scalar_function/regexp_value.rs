// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Original regexp value conversions, separate from child demands and caches.
use crate::{Columns, Datum, EvalError};
use tidb_datatype::FieldType;

pub(crate) fn named_string(
    value: Datum,
    source: Option<&FieldType>,
    ctx: &dyn Columns,
) -> Result<Option<String>, EvalError> {
    super::eager_values::charge_string_work(&value)?;
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
    super::eager_values::charge_string_work(value)?;
    let text = value
        .sql_string()
        .map_err(|_| EvalError::Unsupported("invalid UTF-8 REGEXP operand"))?;
    super::eager_values::charge_string_work(pattern)?;
    let pattern = pattern
        .sql_string()
        .map_err(|_| EvalError::Unsupported("invalid UTF-8 REGEXP pattern"))?;
    Ok((text, pattern))
}
