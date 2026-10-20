// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Actual compression values and completed warning facts; no context under a store borrow.
use crate::{Columns, Datum, EvalError};
use tidb_query_expr::{NativeCompressionOperation as O, NativeCompressionValueError as E};
fn error(e: E) -> EvalError {
    match e {
        E::Resource => {
            EvalError::ExpressionRuntimeFailure(super::ExpressionRuntimeFailure::from_local_eval(
                super::LocalError::ResourceLimit(
                    "compression actual work allowance exceeded".into(),
                ),
                None,
            ))
        }
        E::Contract => {
            use super::adapter_failure::{ExpressionAdapterFailure, ScopeFailureKind};
            EvalError::ExpressionAdapterFailure(ExpressionAdapterFailure::from_scope(
                ScopeFailureKind::Contract,
                "compression actual owner contract",
            ))
        }
    }
}
pub(crate) fn operation(name: &str) -> Option<O> {
    if name.eq_ignore_ascii_case("compress") {
        Some(O::Compress)
    } else if name.eq_ignore_ascii_case("uncompress") {
        Some(O::Uncompress)
    } else if name.eq_ignore_ascii_case("uncompressed_length") {
        Some(O::Length)
    } else {
        None
    }
}
pub(crate) struct Report {
    pub(crate) value: Result<Datum, EvalError>,
    pub(crate) warning: Option<(u16, &'static str)>,
}
impl Report {
    pub(crate) fn warn(&self, ctx: &dyn Columns) {
        if let Some((code, message)) = self.warning {
            ctx.append_warning(code, message);
        }
    }
}
pub(crate) fn body(op: O, value: &Datum, maximum: usize) -> Result<Report, EvalError> {
    let input = super::crypto_value::input(value, false, maximum)?;
    let held = super::crypto_value::held(&input);
    let maximum = maximum
        .checked_sub(held)
        .ok_or_else(|| error(E::Resource))?;
    let mut warning = None;
    let value=match op {
        O::Compress=>tidb_query_expr::native_compress_actual_value(input.as_deref(),maximum).map(|v|v.map_or(Datum::Null,Datum::new_string)).map_err(error),
        O::Uncompress=>tidb_query_expr::native_uncompress_actual_value(input.as_deref(),maximum).map(|v|match v {
            tidb_query_expr::local::UncompressOutcome::Null=>Datum::Null,
            tidb_query_expr::local::UncompressOutcome::Value(v)=>Datum::new_string(v),
            tidb_query_expr::local::UncompressOutcome::Corrupt=>{warning=Some((1259,"ZLIB: Input data corrupted"));Datum::Null},
            tidb_query_expr::local::UncompressOutcome::OutputLimit=>{warning=Some((1258,"ZLIB: Not enough room in the output buffer (probably, length of uncompressed data was corrupted)"));Datum::Null},
        }).map_err(error),
        O::Length=>{
            if input.as_ref().is_some_and(|v|!v.is_empty() && v.len()<=4){warning=Some((1259,"ZLIB: Input data corrupted"));}
            tidb_query_expr::native_uncompressed_length_actual_value(input.as_deref(),maximum).map(|(v,_)|v.map_or(Datum::Null,Datum::Int)).map_err(error)
        },
    };
    Ok(Report { value, warning })
}
pub(crate) fn eval(
    op: O,
    value: &Datum,
    ctx: &dyn Columns,
    maximum: usize,
) -> Result<Datum, EvalError> {
    let report = body(op, value, maximum)?;
    report.warn(ctx);
    report.value
}
