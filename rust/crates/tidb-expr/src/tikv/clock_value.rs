// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Precision preparation and actual clock ownership, without a second driver.
use crate::time_fn::duration_parse::MAX_FSP;
use crate::{Columns, Datum, EvalError};
use tidb_query_expr::{NativeClockInput, NativeClockOperation as Op, NativeClockValueError as E};
fn resource() -> EvalError {
    EvalError::ExpressionRuntimeFailure(super::ExpressionRuntimeFailure::from_local_eval(
        super::LocalError::ResourceLimit("native clock actual work allowance exceeded".into()),
        None,
    ))
}
fn work(maximum: usize, n: usize) -> Result<(), EvalError> {
    if maximum == usize::MAX || n > maximum {
        Err(resource())
    } else {
        Ok(())
    }
}
fn invalid_fsp() -> EvalError {
    EvalError::Unsupported("bad fractional-seconds-precision argument")
}
/// The alias getter is original SQL demand and precedes even arity validation.
/// Hosts invoke it only with authenticated inputs and no value-store borrow.
pub(crate) fn resolve(operation: Op, ctx: &dyn Columns) -> Op {
    if operation == Op::Sysdate && ctx.sysdate_is_now() {
        Op::Now
    } else {
        operation
    }
}
/// Caller-owned actual precision, not an adjusted clock or computed answer.
pub(crate) struct Prepared {
    operation: Op,
    fsp: Option<u32>,
}
/// NULL denotes UTC_TIME's actual NULL operand and suppresses clock demand.
/// No Columns callbacks occur during borrowed precision preparation.
pub(crate) fn prepare(
    operation: Op,
    values: &[Datum],
    maximum: usize,
) -> Result<Option<Prepared>, EvalError> {
    work(maximum, 0)?;
    let fsp = match operation {
        Op::CurrentDate | Op::UtcDate => {
            if !values.is_empty() {
                return Err(EvalError::Unsupported("bad function arity"));
            }
            None
        }
        Op::UtcTime => {
            if matches!(values, [Datum::Null]) {
                return Ok(None);
            }
            crate::time_fn::parse_fsp_for(values, "utc_time")?
        }
        Op::Sysdate => {
            if values.len() > 1 {
                return Err(EvalError::Unsupported("bad function arity"));
            }
            Some(match values.first() {
                None | Some(Datum::Null) => 0,
                Some(Datum::Int(v)) if (0..=i64::from(MAX_FSP)).contains(v) => *v as u32,
                Some(Datum::UInt(v)) if *v <= MAX_FSP as u64 => *v as u32,
                Some(value) => {
                    // Datum.ToInt64 uses storage precision, never Decimal's
                    // unused visible shape. Bound only actual conversion work.
                    let n = match value {
                        Datum::String(v) => v.bytes().len(),
                        Datum::Bytes(v) => v.len(),
                        Datum::Decimal(v) => v.coefficient_bytes().len(),
                        Datum::Json(v) => v.value().len(),
                        Datum::BinaryLiteral(v) | Datum::Bit(v) => v.as_bytes().len(),
                        _ => 0,
                    };
                    work(
                        maximum,
                        n.checked_mul(16)
                            .and_then(|n| n.checked_add(1024))
                            .ok_or_else(resource)?,
                    )?;
                    let n = value.to_i64().map_err(|_| invalid_fsp())?.value;
                    if !(0..=i64::from(MAX_FSP)).contains(&n) {
                        return Err(invalid_fsp());
                    }
                    n as u32
                }
            })
        }
        Op::Now | Op::UtcTimestamp => Some(
            crate::time_fn::parse_fsp_with_null_as_zero(values, operation.name())?.unwrap_or(0),
        ),
        Op::Curtime | Op::CurrentTime => {
            crate::time_fn::parse_fsp_with_null_as_zero(values, operation.name())?
        }
    };
    Ok(Some(Prepared { operation, fsp }))
}
fn live_clock() -> Result<(i64, u32), EvalError> {
    let elapsed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| EvalError::Unsupported("the host clock is before the Unix epoch"))?;
    Ok((elapsed.as_secs() as i64, elapsed.subsec_nanos()))
}
fn finish_with_live(
    prepared: Option<Prepared>,
    ctx: &dyn Columns,
    maximum: usize,
    live: impl FnOnce() -> Result<(i64, u32), EvalError>,
) -> Result<Datum, EvalError> {
    let Some(Prepared { operation, fsp }) = prepared else {
        return Ok(Datum::Null);
    };
    work(maximum, 1024)?;
    let clock = if operation == Op::Sysdate {
        let (_, _, offset) = ctx.now().ok_or(EvalError::Unsupported(
            "SYSDATE needs the session clock, which is not wired here",
        ))?;
        let (secs, nanos) = live()?;
        (secs, nanos, offset)
    } else {
        ctx.now()
            .ok_or(EvalError::Unsupported("no session clock (SET timestamp)"))?
    };
    tidb_query_expr::native_clock_values(
        operation,
        NativeClockInput {
            utc_secs: clock.0,
            nanos: clock.1,
            tz_offset: clock.2,
        },
        fsp,
        maximum,
    )
    .map(Datum::new_string)
    .map_err(|e| match e {
        E::Resource => resource(),
        E::InvalidPrecision => super::native_time_result_contract_error(),
    })
}
pub(crate) fn finish(
    prepared: Option<Prepared>,
    ctx: &dyn Columns,
    maximum: usize,
) -> Result<Datum, EvalError> {
    finish_with_live(prepared, ctx, maximum, live_clock)
}
pub(crate) fn eval_bounded(
    operation: Op,
    values: &[Datum],
    ctx: &dyn Columns,
    maximum: usize,
) -> Result<Datum, EvalError> {
    let resolved = resolve(operation, ctx);
    finish(prepare(resolved, values, maximum)?, ctx, maximum)
}
pub(crate) fn eval(operation: Op, values: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    eval_bounded(
        operation,
        values,
        ctx,
        super::control_value::default_budget(),
    )
}

/// Preserve the original temporal parse followed by the ordinary return-family
/// boundary. SYSDATE parses DateTime unconditionally, including string results.
pub(crate) fn finish_source(
    operation: Op,
    result: Datum,
    field: &tidb_datatype::FieldType,
    ctx: &dyn Columns,
    maximum: usize,
) -> Result<Datum, EvalError> {
    let result = if operation == Op::Sysdate {
        if !result.is_null() {
            let n = tidb_query_expr::native_string_coercion_work(result.as_shared_json_input())
                .map_err(|_| resource())?;
            work(
                maximum,
                n.checked_mul(64)
                    .and_then(|n| n.checked_add(4096))
                    .ok_or_else(resource)?,
            )?;
        }
        crate::cast::parse_computed_time(
            &result,
            ctx,
            tidb_datatype::TimeType::DateTime,
            Some(field.decimal()),
        )?
    } else {
        super::finish_temporal_values_bounded(ctx, result, field, maximum)?
    };
    super::coerce_result_bounded(operation.name(), field, result, maximum)
}
