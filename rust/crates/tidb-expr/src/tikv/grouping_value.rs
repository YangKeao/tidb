// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Original strict id reading followed by actual GROUPING metadata demand.
use crate::{Datum, EvalError};
use tidb_query_expr::{GroupingMetadata, NativeGroupingValueError as E};
fn error(e: E) -> EvalError {
    match e {
        E::Uninitialized => EvalError::Unsupported("Meta data is not initialized"),
        E::Resource => {
            EvalError::ExpressionRuntimeFailure(super::ExpressionRuntimeFailure::from_local_eval(
                super::LocalError::ResourceLimit("native GROUPING work allowance exceeded".into()),
                None,
            ))
        }
    }
}
pub(crate) fn value(
    id: Option<u64>,
    metadata: Option<&GroupingMetadata>,
    maximum: usize,
) -> Result<Datum, EvalError> {
    tidb_query_expr::native_grouping_value(id, metadata, maximum)
        .map(|v| v.map_or(Datum::Null, Datum::UInt))
        .map_err(error)
}
pub(crate) fn eval(
    value: &Datum,
    metadata: Option<&GroupingMetadata>,
    maximum: usize,
) -> Result<Datum, EvalError> {
    let id = crate::arg_eval_type::eval_int(value)?.map(|v| v as u64);
    self::value(id, metadata, maximum)
}
