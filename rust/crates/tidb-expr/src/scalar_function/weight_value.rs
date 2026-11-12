// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Original WEIGHT_STRING value boundaries, without an expression callback.
use super::ScalarFunction;
use crate::{Columns, Datum, EvalError};
use tidb_datatype::{FieldType, FieldTypeCode};

pub(crate) fn supports(function: &ScalarFunction) -> bool {
    function.func_name.lowercase() == "weight_string"
        && !function.args.is_empty()
        && function.pb_signature().is_none()
        && function.pb_origin().is_none()
}
/// Logical SQL arity is separate from values the original owner can demand.
pub(crate) fn data_arity(logical: usize, source: Option<&FieldType>) -> usize {
    if logical == 3 {
        3
    } else if numeric_code(source, None).is_some() {
        0
    } else {
        1
    }
}
/// This default belongs to the original scalar signature, not compiled fields.
pub(crate) fn source_type(source: Option<&FieldType>) -> std::borrow::Cow<'_, FieldType> {
    match source {
        Some(source) => std::borrow::Cow::Borrowed(source),
        None => std::borrow::Cow::Owned(FieldType::new(FieldTypeCode::VarString)),
    }
}
pub(crate) fn padding(kind: &Datum, length: &Datum) -> (bool, i64) {
    let binary = kind.sql_string().is_ok_and(|k| k == "BINARY");
    (binary, crate::cast::to_i64_signed(length))
}
pub(crate) fn numeric_code(
    source: Option<&FieldType>,
    padding: Option<(bool, i64)>,
) -> Option<FieldTypeCode> {
    source
        .map(FieldType::code)
        .filter(|code| code.is_type_numeric() && !matches!(padding, Some((true, _))))
}
pub(crate) fn evaluate(
    value: Datum,
    arg_type: &FieldType,
    explicit_collation: bool,
    padding: Option<(bool, i64)>,
    ctx: &dyn Columns,
) -> Result<Datum, EvalError> {
    let value = crate::cast::cast_arg_as_string(&value, Some(arg_type), ctx)?;
    let string_type = crate::cast::cast_arg_as_string_type(
        arg_type,
        explicit_collation,
        ctx.connection_charset_info(),
    );
    let arg_collation = tidb_datatype::Collation::from_name(string_type.collation_name())
        .unwrap_or(crate::ops::DERIVATION_FREE_COLLATION);
    crate::string_packet::weight_string(&value, padding, arg_collation, ctx)
}
