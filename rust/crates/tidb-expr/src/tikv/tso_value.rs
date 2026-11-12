// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Actual transaction timestamps, not prepared RPN invocations.
use crate::{Columns, Datum, EvalError};
use tidb_datatype::{CoreTime, SessionTimeZone, Time, TimeType};
use tidb_query_expr::NativeTsoOperation as Op;
fn resource() -> EvalError {
    EvalError::ExpressionRuntimeFailure(super::ExpressionRuntimeFailure::from_local_eval(
        super::LocalError::ResourceLimit("native TSO work allowance exceeded".into()),
        None,
    ))
}
/// Bound original integer parsing/conversion work before it can allocate. The
/// caller separately accounts for retained source/copy/result ownership.
fn preflight(
    value: Option<&Datum>,
    maximum: usize,
    decimal_display: bool,
) -> Result<(), EvalError> {
    let bytes = match value {
        Some(Datum::String(v)) => v.bytes().len(),
        Some(Datum::Bytes(v) | Datum::Raw(v)) => v.len(),
        Some(Datum::Decimal(v)) => v
            .coefficient_bytes()
            .len()
            .checked_add(if decimal_display {
                v.scale().max(v.storage_scale()) as usize
            } else {
                0
            })
            .ok_or_else(resource)?,
        Some(Datum::BinaryLiteral(v) | Datum::Bit(v)) => v.as_bytes().len(),
        Some(Datum::Json(v)) => v.value().len(),
        Some(Datum::Time(t)) => usize::from(t.fsp()),
        Some(Datum::Duration(t)) => {
            usize::try_from(t.fsp().unsigned_abs()).map_err(|_| resource())?
        }
        _ => 0,
    };
    let work = bytes
        .checked_mul(16)
        .and_then(|n| n.checked_add(1024))
        .ok_or_else(resource)?;
    if work > maximum {
        return Err(resource());
    }
    Ok(())
}
pub(crate) fn eval_bounded(
    op: Op,
    values: &[Datum],
    ctx: &dyn Columns,
    maximum: usize,
) -> Result<Datum, EvalError> {
    if values.len() != op.arity() {
        return Err(EvalError::Unsupported("bad function arity"));
    }
    preflight(values.first(), maximum, false)?;
    if op == Op::Current {
        return Ok(Datum::Int(ctx.current_tso()));
    }
    let tso = crate::time_fn::int_arg(&values[0])?;
    if op == Op::Logical {
        return Ok(tidb_query_expr::native_tso_logical(tso).map_or(Datum::Null, Datum::Int));
    }
    let Some(tso) = tso.filter(|v| *v > 0) else {
        return Ok(Datum::Null);
    };
    // This getter precedes even UTC preparation. Preserve raw fixed offsets;
    // normalizing them through FixedOffset changes existing SQL behavior.
    use chrono::{Offset, TimeZone};
    let zone = ctx.time_zone();
    let offset = match &zone {
        SessionTimeZone::Fixed { offset_secs, .. } => *offset_secs,
        SessionTimeZone::Named(_) | SessionTimeZone::Local => {
            let instant = tidb_query_expr::native_tso_utc(tso)
                .expect("a positive signed TSO is within Chrono's timestamp range");
            zone.offset_from_utc_datetime(&instant.naive_utc())
                .fix()
                .local_minus_utc()
        }
    };
    Ok(
        tidb_query_expr::native_tso_core(tso, offset).map_or(Datum::Null, |core| {
            Datum::Time(Time::from_raw_parts(
                CoreTime::from_raw(core),
                TimeType::DateTime,
                6,
            ))
        }),
    )
}
pub(crate) fn eval(op: Op, values: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    eval_bounded(op, values, ctx, super::identity_actual::default_budget())
}

/// ScalarFunction's original ETInt argument boundary precedes the signature.
/// Direct time_fn calls intentionally retain their separate int_arg policy.
pub(crate) fn eval_source_bounded(
    op: Op,
    values: &[Datum],
    source: Option<&tidb_datatype::FieldType>,
    ctx: &dyn Columns,
    maximum: usize,
) -> Result<Datum, EvalError> {
    if values.len() != op.arity() {
        return Err(EvalError::Unsupported("bad function arity"));
    }
    preflight(values.first(), maximum, false)?;
    if op == Op::Current {
        return eval_bounded(op, values, ctx, maximum);
    }
    if let Datum::Decimal(value) = &values[0] {
        // Match only the original cast's diagnostic demand. Direct integer
        // storage reads and quiet casts never render unused visible precision.
        let display = if source.is_some_and(tidb_datatype::FieldType::is_unsigned) {
            value.round_to_i64_saturating() < 0
        } else {
            value.round_to_i64().is_none()
        };
        if display {
            preflight(values.first(), maximum, true)?;
        }
    }
    let converted = crate::cast::cast_arg_as_int(&values[0], source, ctx)?;
    eval_bounded(op, std::slice::from_ref(&converted), ctx, maximum)
}
