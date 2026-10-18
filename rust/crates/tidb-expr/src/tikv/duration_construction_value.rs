// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Actual time operands and consuming conversion stages, without an evaluator.
use crate::{Columns, Datum, EvalError};
use tidb_datatype::{CoreTime, Time, TimeType};
use tidb_query_expr::{
    NativeDurationConstructionError as E, NativeDurationConstructionOperation as O,
};
fn resource() -> EvalError {
    EvalError::ExpressionRuntimeFailure(super::ExpressionRuntimeFailure::from_local_eval(
        super::LocalError::ResourceLimit(
            "native duration/construction work allowance exceeded".into(),
        ),
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
fn error(e: E) -> EvalError {
    match e {
        E::Resource => resource(),
        E::PeriodAdd => EvalError::IncorrectArguments("Incorrect arguments to period_add".into()),
        E::PeriodDiff => EvalError::IncorrectArguments("Incorrect arguments to period_diff".into()),
        E::Contract => super::native_time_result_contract_error(),
    }
}
fn string_error(e: tidb_query_expr::NativeStringCoercionError) -> EvalError {
    match e {
        tidb_query_expr::NativeStringCoercionError::Unsupported(s) => EvalError::Unsupported(s),
        _ => resource(),
    }
}
fn text(v: &Datum, maximum: usize) -> Result<Option<String>, EvalError> {
    if v.is_null() {
        return Ok(None);
    };
    tidb_query_expr::native_coerce_string_bounded(v.as_shared_json_input(), maximum)
        .map(Some)
        .map_err(string_error)
}
fn string_datum(v: Option<String>) -> Datum {
    v.map_or(Datum::Null, Datum::new_string)
}
fn int_datum(v: Option<i64>) -> Datum {
    v.map_or(Datum::Null, Datum::Int)
}
/// Numeric float conversion actually renders Decimal's visible precision.
/// Integer conversions below deliberately keep their separate storage policy.
fn numeric_work(v: &Datum, maximum: usize, display: bool) -> Result<(), EvalError> {
    let n = match v {
        Datum::String(v) => v.bytes().len(),
        Datum::Bytes(v) | Datum::Raw(v) => v.len(),
        Datum::Decimal(v) => v
            .coefficient_bytes()
            .len()
            .checked_add(if display {
                v.scale().max(v.storage_scale()) as usize
            } else {
                0
            })
            .ok_or_else(resource)?,
        Datum::Json(v) => v.value().len(),
        Datum::BinaryLiteral(v) | Datum::Bit(v) => v.as_bytes().len(),
        _ => 0,
    };
    work(
        maximum,
        n.checked_mul(32)
            .and_then(|n| n.checked_add(2048))
            .ok_or_else(resource)?,
    )
}
fn integer(v: &Datum, maximum: usize) -> Result<Option<i64>, EvalError> {
    numeric_work(v, maximum, false)?;
    crate::time_fn::int_arg(v)
}
fn number(v: &Datum, ctx: &dyn Columns, maximum: usize) -> Result<Option<f64>, EvalError> {
    numeric_work(v, maximum, true)?;
    crate::time_fn::number_arg(v, ctx)
}
fn precision(v: &Datum, maximum: usize) -> Result<usize, EvalError> {
    // Only Decimal and the fallback SQL-string branches actually format. Raw
    // Time/Duration precision is metadata until the final bounded formatter.
    if matches!(v, Datum::Decimal(_)) {
        numeric_work(v, maximum, true)?;
    } else if !matches!(
        v,
        Datum::Int(_)
            | Datum::UInt(_)
            | Datum::String(_)
            | Datum::Bytes(_)
            | Datum::Real(_)
            | Datum::Float32(_)
            | Datum::Time(_)
            | Datum::Duration(_)
            | Datum::Null
            | Datum::MinNotNull
            | Datum::MaxValue
    ) {
        numeric_work(v, maximum, true)?;
    }
    crate::time_fn::duration_precision(v)
}
pub(crate) enum Pair {
    Integer {
        op: O,
        left: Option<i64>,
    },
    MakeTime {
        hour: Option<(i64, bool)>,
    },
    DateDiff {
        left: Option<String>,
    },
    TimeDiff(tidb_query_expr::NativeTimeDiffValue),
    TimeFormat(tidb_query_expr::NativeTimeFormatValue),
    ConvertTz(Option<String>),
    TimeAdd {
        left: Option<String>,
        metadata: tidb_query_expr::NativeTimeAddMetadata,
        subtract: bool,
    },
    Timestamp(tidb_query_expr::NativeTimestampBaseValue),
    TimestampAddUnit(Option<String>),
}
/// Each stage owns actual values and is consumed before conversion can fail or
/// panic. No executable child, source callback or replayable validity flag lives here.
pub(crate) enum Prepared {
    Value(Datum),
    Pair(Pair),
    TimestampAddDate {
        unit: String,
        amount: f64,
    },
    TimeZoneDestination {
        datetime: Option<String>,
        from: Option<String>,
    },
    Third {
        hour: Option<(i64, bool)>,
        minute: Option<i64>,
    },
}
impl Prepared {
    pub(crate) fn retained_bytes(&self) -> usize {
        match self {
            Self::Pair(Pair::DateDiff { left }) | Self::Pair(Pair::ConvertTz(left)) => {
                left.as_ref().map_or(0, String::capacity)
            }
            Self::TimeZoneDestination { datetime, from } => datetime
                .as_ref()
                .map_or(0, String::capacity)
                .saturating_add(from.as_ref().map_or(0, String::capacity)),
            Self::Pair(Pair::TimeAdd { left, .. }) | Self::Pair(Pair::TimestampAddUnit(left)) => {
                left.as_ref().map_or(0, String::capacity)
            }
            Self::TimestampAddDate { unit, .. } => unit.capacity(),
            Self::Pair(Pair::TimeFormat(v)) => v.retained_bytes(),
            _ => 0,
        }
    }

    /// Required original argument index and whether conversion calls Columns.
    pub(crate) fn argument(&self) -> Option<(usize, bool)> {
        match self {
            Self::Value(_) => None,
            Self::Pair(Pair::TimeAdd { .. }) => Some((1, true)),
            Self::Pair(_) => Some((1, false)),
            Self::TimestampAddDate { .. } => Some((2, true)),
            Self::Third { .. } => Some((2, true)),
            Self::TimeZoneDestination { .. } => Some((2, false)),
        }
    }
    pub(crate) fn advance(
        self,
        value: &Datum,
        ctx: &dyn Columns,
        maximum: usize,
    ) -> Result<Self, EvalError> {
        work(maximum, 0)?;
        let result = match self {
            Self::Value(_) => return Err(super::native_time_result_contract_error()),
            Self::TimestampAddDate { unit, amount } => {
                let date = text(value, maximum)?;
                let remaining = maximum
                    .checked_sub(date.as_ref().map_or(0, String::capacity))
                    .ok_or_else(resource)?;
                let result = tidb_query_expr::native_timestamp_add_value(
                    &unit,
                    date.as_deref(),
                    amount,
                    remaining,
                )
                .map_err(error)?;
                match result {
                    None => Datum::Null,
                    Some(tidb_query_expr::NativeTimestampAddValue::Value(value)) => {
                        Datum::new_string(value)
                    }
                    Some(tidb_query_expr::NativeTimestampAddValue::UnknownUnit) => {
                        return Err(EvalError::Unsupported("TIMESTAMPADD unit"))
                    }
                    Some(tidb_query_expr::NativeTimestampAddValue::IncorrectDateTimeInput) => {
                        let source = date
                            .as_deref()
                            .ok_or_else(super::native_time_result_contract_error)?;
                        work(
                            remaining,
                            source.len().checked_add(2048).ok_or_else(resource)?,
                        )?;
                        ctx.append_warning(1292, &format!("Incorrect datetime value: '{source}'"));
                        Datum::Null
                    }
                    Some(tidb_query_expr::NativeTimestampAddValue::IncorrectTimeResult(
                        message,
                    )) => {
                        ctx.append_warning(1292, &message);
                        Datum::Null
                    }
                }
            }
            Self::TimeZoneDestination { datetime, from } => {
                let to = text(value, maximum)?;
                let remaining = maximum
                    .checked_sub(to.as_ref().map_or(0, String::capacity))
                    .ok_or_else(resource)?;
                string_datum(
                    tidb_query_expr::native_convert_tz_value(
                        datetime.as_deref(),
                        from.as_deref(),
                        to.as_deref(),
                        remaining,
                    )
                    .map_err(error)?,
                )
            }
            Self::Third { hour, minute } => {
                let second = number(value, ctx, maximum)?;
                let seconds =
                    tidb_query_expr::native_make_time_parts_value(hour, minute, second, maximum)
                        .map_err(error)?;
                let fsp = if seconds.is_some() {
                    Some(precision(value, maximum)?)
                } else {
                    None
                };
                string_datum(
                    tidb_query_expr::native_seconds_to_time_value(seconds, fsp, maximum)
                        .map_err(error)?,
                )
            }
            Self::Pair(pair) => match pair {
                Pair::TimeAdd {
                    left,
                    mut metadata,
                    subtract,
                } => {
                    let right = text(value, maximum)?;
                    metadata.right_binary =
                        matches!(value, Datum::BinaryLiteral(_) | Datum::Bit(_));
                    let remaining = maximum
                        .checked_sub(right.as_ref().map_or(0, String::capacity))
                        .ok_or_else(resource)?;
                    match tidb_query_expr::native_time_add_value(
                        left.as_deref(),
                        right.as_deref(),
                        metadata,
                        subtract,
                        remaining,
                    )
                    .map_err(error)?
                    {
                        None => Datum::Null,
                        Some(tidb_query_expr::NativeTimeAddValue::Value(value)) => {
                            Datum::new_string(value)
                        }
                        Some(tidb_query_expr::NativeTimeAddValue::Warning(warning)) => {
                            use tidb_query_expr::NativeTimeAddWarning as W;
                            let source = if warning == W::TruncatedRight {
                                right.as_deref()
                            } else {
                                left.as_deref()
                            }
                            .ok_or_else(super::native_time_result_contract_error)?;
                            let (label, source) = match warning {
                                W::TruncatedLeft | W::TruncatedRight => (
                                    "Truncated incorrect time value",
                                    tidb_datatype::warning_subject_byte_cap(source),
                                ),
                                W::IncorrectTimeLeft => ("Incorrect time value", source),
                                W::IncorrectDateTimeLeft => ("Incorrect datetime value", source),
                            };
                            work(
                                remaining,
                                source.len().checked_add(2048).ok_or_else(resource)?,
                            )?;
                            ctx.append_warning(1292, &format!("{label}: '{source}'"));
                            Datum::Null
                        }
                    }
                }
                Pair::Timestamp(base) => {
                    let right = text(value, maximum)?;
                    let remaining = maximum
                        .checked_sub(right.as_ref().map_or(0, String::capacity))
                        .ok_or_else(resource)?;
                    string_datum(base.finish(right.as_deref(), remaining).map_err(error)?)
                }
                Pair::TimestampAddUnit(unit) => {
                    // This is the original full-float parser, not number_arg's
                    // numeric-prefix/warning controller. NULL unit still demands it.
                    if matches!(
                        value,
                        Datum::Int(_)
                            | Datum::UInt(_)
                            | Datum::Real(_)
                            | Datum::Float32(_)
                            | Datum::Null
                    ) {
                        work(maximum, 0)?;
                    } else if matches!(value, Datum::Decimal(_)) {
                        numeric_work(value, maximum, true)?;
                    } else {
                        let n = tidb_query_expr::native_string_coercion_work(
                            value.as_shared_json_input(),
                        )
                        .map_err(string_error)?;
                        work(maximum, n)?;
                    }
                    let amount = crate::time_fn::add_sub::number_of(value)?;
                    match (unit, amount) {
                        (Some(unit), Some(amount)) => {
                            return Ok(Self::TimestampAddDate { unit, amount })
                        }
                        _ => Datum::Null,
                    }
                }
                Pair::ConvertTz(datetime) => {
                    return Ok(Self::TimeZoneDestination {
                        datetime,
                        from: text(value, maximum)?,
                    });
                }
                Pair::Integer { op, left } => {
                    let right = integer(value, maximum)?;
                    if op == O::MakeDate {
                        string_datum(
                            tidb_query_expr::native_make_date_value(left, right, maximum)
                                .map_err(error)?,
                        )
                    } else {
                        int_datum(
                            tidb_query_expr::native_period_value(
                                op == O::PeriodAdd,
                                left,
                                right,
                                maximum,
                            )
                            .map_err(error)?,
                        )
                    }
                }
                Pair::MakeTime { hour } => {
                    return Ok(Self::Third {
                        hour,
                        minute: integer(value, maximum)?,
                    })
                }
                Pair::DateDiff { left } => {
                    let right = text(value, maximum)?;
                    int_datum(
                        tidb_query_expr::native_date_diff_value(
                            left.as_deref(),
                            right.as_deref(),
                            maximum,
                        )
                        .map_err(error)?,
                    )
                }
                Pair::TimeDiff(left) => {
                    let right = text(value, maximum)?;
                    string_datum(left.finish(right.as_deref(), maximum).map_err(error)?)
                }
                Pair::TimeFormat(left) => {
                    let layout = text(value, maximum)?;
                    string_datum(left.finish(layout.as_deref(), maximum).map_err(error)?)
                }
            },
        };
        Ok(Self::Value(result))
    }
    pub(crate) fn into_value(self) -> Result<Datum, EvalError> {
        match self {
            Self::Value(v) => Ok(v),
            _ => Err(super::native_time_result_contract_error()),
        }
    }
}
pub(crate) fn start(
    op: O,
    value: &Datum,
    ctx: &dyn Columns,
    maximum: usize,
) -> Result<Prepared, EvalError> {
    work(maximum, 0)?;
    let v = match op {
        O::AddTime { kinds, row_path } | O::SubTime { kinds, row_path } => {
            if kinds[1] == tidb_query_expr::NativeTimeAddKind::Datetime {
                return Ok(Prepared::Value(Datum::Null));
            }
            return Ok(Prepared::Pair(Pair::TimeAdd {
                left: text(value, maximum)?,
                metadata: tidb_query_expr::NativeTimeAddMetadata {
                    left: kinds[0],
                    right: kinds[1],
                    row_path,
                    right_binary: false,
                },
                subtract: matches!(op, O::SubTime { .. }),
            }));
        }
        O::Timestamp1 | O::Timestamp2 => {
            let Some(source) = text(value, maximum)? else {
                return Ok(Prepared::Value(Datum::Null));
            };
            let remaining = maximum
                .checked_sub(source.capacity())
                .ok_or_else(resource)?;
            let is_float = matches!(
                value,
                Datum::Int(_)
                    | Datum::UInt(_)
                    | Datum::Decimal(_)
                    | Datum::Real(_)
                    | Datum::Float32(_)
            );
            let zone = ctx.time_zone();
            match tidb_query_expr::native_timestamp_head_value(
                &source,
                is_float,
                &zone,
                op == O::Timestamp2,
                remaining,
            )
            .map_err(error)?
            {
                tidb_query_expr::NativeTimestampHeadValue::Value(value) => Datum::new_string(value),
                tidb_query_expr::NativeTimestampHeadValue::NeedRhs(base) => {
                    return Ok(Prepared::Pair(Pair::Timestamp(base)))
                }
                tidb_query_expr::NativeTimestampHeadValue::Warning(message) => {
                    ctx.append_warning(1292, &message);
                    Datum::Null
                }
            }
        }
        O::TimestampAdd => {
            return Ok(Prepared::Pair(Pair::TimestampAddUnit(text(
                value, maximum,
            )?)))
        }
        O::ConvertTz => return Ok(Prepared::Pair(Pair::ConvertTz(text(value, maximum)?))),
        O::MakeDate | O::PeriodAdd | O::PeriodDiff => {
            return Ok(Prepared::Pair(Pair::Integer {
                op,
                left: integer(value, maximum)?,
            }))
        }
        O::MakeTime => {
            let hour = integer(value, maximum)?;
            return Ok(Prepared::Pair(Pair::MakeTime {
                hour: hour.map(|v| (v, matches!(value, Datum::UInt(_)))),
            }));
        }
        O::DateDiff => {
            return Ok(Prepared::Pair(Pair::DateDiff {
                left: text(value, maximum)?,
            }))
        }
        O::GetFormat => return Err(super::native_time_result_contract_error()),
        O::FromDays => {
            numeric_work(value, maximum, false)?;
            let n = if value.is_null() {
                None
            } else {
                Some(crate::cast::to_i64_signed(value))
            };
            match tidb_query_expr::native_from_days_value(n, maximum).map_err(error)? {
                Some(s) if s == "0000-00-00" => Datum::Time(Time::from_raw_parts(
                    CoreTime::from_raw(0),
                    TimeType::Date,
                    0,
                )),
                v => string_datum(v),
            }
        }
        O::SecToTime => {
            let seconds = number(value, ctx, maximum)?;
            let fsp = if seconds.is_some() {
                Some(precision(value, maximum)?)
            } else {
                None
            };
            string_datum(
                tidb_query_expr::native_seconds_to_time_value(seconds, fsp, maximum)
                    .map_err(error)?,
            )
        }
        O::Microsecond | O::Time | O::TimeToSec | O::TimeDiff | O::TimeFormat => {
            let source = text(value, maximum)?;
            match op {
                O::Microsecond => int_datum(
                    tidb_query_expr::native_microsecond_value(source.as_deref(), maximum)
                        .map_err(error)?,
                ),
                O::TimeToSec => int_datum(
                    tidb_query_expr::native_time_to_sec_value(source.as_deref(), maximum)
                        .map_err(error)?,
                ),
                O::Time => {
                    let Some(report) =
                        tidb_query_expr::native_time_value(source.as_deref(), maximum)
                            .map_err(error)?
                    else {
                        return Ok(Prepared::Value(Datum::Null));
                    };
                    if report.truncated {
                        ctx.handle_truncate(&format!(
                            "Truncated incorrect time value: '{}'",
                            tidb_datatype::warning_subject_byte_cap(
                                source
                                    .as_deref()
                                    .ok_or_else(super::native_time_result_contract_error)?
                            )
                        ))?;
                    }
                    Datum::new_string(report.value)
                }
                O::TimeDiff => {
                    return tidb_query_expr::native_time_diff_value(source.as_deref(), maximum)
                        .map(|v| {
                            v.map_or(Prepared::Value(Datum::Null), |v| {
                                Prepared::Pair(Pair::TimeDiff(v))
                            })
                        })
                        .map_err(error)
                }
                O::TimeFormat => {
                    return tidb_query_expr::native_time_format_value(source, maximum)
                        .map(|v| {
                            v.map_or(Prepared::Value(Datum::Null), |v| {
                                Prepared::Pair(Pair::TimeFormat(v))
                            })
                        })
                        .map_err(error)
                }
                _ => unreachable!(),
            }
        }
    };
    Ok(Prepared::Value(v))
}
pub(crate) fn eval_bounded(
    op: O,
    values: &[Datum],
    ctx: &dyn Columns,
    maximum: usize,
) -> Result<Datum, EvalError> {
    if values.len() != op.arity() {
        return Err(EvalError::Unsupported("bad function arity"));
    }
    if op == O::GetFormat {
        work(maximum, 0)?;
        let Some(kind) = crate::arg_eval_type::eval_string_ref(&values[0])? else {
            return Ok(Datum::Null);
        };
        let locale = crate::arg_eval_type::eval_string_ref(&values[1])?;
        return tidb_query_expr::native_get_format_value(Some(kind), locale, maximum)
            .map(string_datum)
            .map_err(error);
    }
    let mut prepared = start(op, &values[0], ctx, maximum)?;
    while let Some((index, _)) = prepared.argument() {
        let remaining = if matches!(
            op,
            O::ConvertTz
                | O::AddTime { .. }
                | O::SubTime { .. }
                | O::Timestamp1
                | O::Timestamp2
                | O::TimestampAdd
        ) {
            maximum
                .checked_sub(prepared.retained_bytes())
                .ok_or_else(resource)?
        } else {
            maximum
        };
        prepared = prepared.advance(&values[index], ctx, remaining)?;
    }
    prepared.into_value()
}
pub(crate) fn eval(op: O, values: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    eval_bounded(op, values, ctx, super::control_value::default_budget())
}
pub(crate) fn get_format_ast(
    kind: &str,
    location: &Datum,
    maximum: usize,
) -> Result<Datum, EvalError> {
    let text = text(location, maximum)?;
    tidb_query_expr::native_get_format_value(
        Some(kind.as_bytes()),
        text.as_deref().map(str::as_bytes),
        maximum,
    )
    .map(string_datum)
    .map_err(error)
}
pub(crate) fn legacy(
    which: usize,
    left: Option<u64>,
    right: Option<u64>,
    maximum: usize,
) -> Result<Option<i64>, EvalError> {
    match which {
        0 => tidb_query_expr::native_date_diff_core_value(left, right, maximum),
        1 => tidb_query_expr::native_microsecond_nanos_value(left.map(|v| v as i64), maximum),
        _ => tidb_query_expr::native_date_predicate_core_value(left, maximum),
    }
    .map_err(error)
}

/// Only these first-operand paths report through Columns. Others can safely
/// operate over an authenticated borrow, with later callback demand explicit.
pub(crate) fn start_calls_context(op: O) -> bool {
    matches!(op, O::Time | O::SecToTime | O::Timestamp1 | O::Timestamp2)
}
/// Preserve both original ETString casts before signature-level NULL demand.
/// Identity actions borrow actual storage; no ignored huge source is copied.
pub(crate) fn get_format_source(values: [&Datum; 2], maximum: usize) -> Result<Datum, EvalError> {
    use tidb_query_expr::NativeArgStringResult as A;
    fn cast(v: &Datum, maximum: usize) -> Result<A, EvalError> {
        work(maximum, 0)?;
        if !matches!(
            v,
            Datum::Null
                | Datum::String(_)
                | Datum::Bytes(_)
                | Datum::Enum(..)
                | Datum::Set(..)
                | Datum::BinaryLiteral(_)
        ) {
            let n = tidb_query_expr::native_byte_coercion_work(v.as_shared_json_input())
                .map_err(string_error)?;
            work(maximum, n)?;
        }
        tidb_query_expr::native_cast_arg_as_string(v.as_shared_json_input())
            .map_err(EvalError::Unsupported)
    }
    fn capacity(a: &A) -> usize {
        match a {
            A::Binary(v) | A::String(v) => v.capacity(),
            _ => 0,
        }
    }
    fn bytes<'a>(a: &'a A, v: &'a Datum) -> Result<Option<&'a [u8]>, EvalError> {
        match a {
            A::Original => crate::arg_eval_type::eval_string_ref(v),
            A::Binary(v) | A::String(v) => Ok(Some(v)),
            A::Null => Ok(None),
        }
    }
    let a = cast(values[0], maximum)?;
    let remaining = maximum.checked_sub(capacity(&a)).ok_or_else(resource)?;
    let b = cast(values[1], remaining)?;
    let remaining = remaining.checked_sub(capacity(&b)).ok_or_else(resource)?;
    let Some(kind) = bytes(&a, values[0])? else {
        return Ok(Datum::Null);
    };
    tidb_query_expr::native_get_format_value(Some(kind), bytes(&b, values[1])?, remaining)
        .map(string_datum)
        .map_err(error)
}
pub(crate) fn finish_source(
    op: O,
    value: Datum,
    field: &tidb_datatype::FieldType,
    ctx: &dyn Columns,
    maximum: usize,
) -> Result<Datum, EvalError> {
    let value = if matches!(
        op,
        O::AddTime { .. } | O::SubTime { .. } | O::Timestamp1 | O::Timestamp2
    ) {
        use tidb_datatype::FieldTypeCode as F;
        let timestamp = matches!(op, O::Timestamp1 | O::Timestamp2);
        let demanded = timestamp || matches!(field.code(), F::Datetime | F::Duration);
        if demanded && !value.is_null() {
            let n = tidb_query_expr::native_string_coercion_work(value.as_shared_json_input())
                .map_err(string_error)?;
            work(
                maximum,
                n.checked_mul(64)
                    .and_then(|n| n.checked_add(4096))
                    .ok_or_else(resource)?,
            )?;
        }
        if timestamp || field.code() == F::Datetime {
            crate::cast::parse_computed_time(&value, ctx, TimeType::DateTime, None)?
        } else if field.code() == F::Duration {
            crate::cast::parse_computed_duration(&value, ctx)?
        } else {
            value
        }
    } else if op.temporal_result() {
        super::finish_temporal_values_bounded(ctx, value, field, maximum)?
    } else {
        value
    };
    super::coerce_result_bounded(op.name(), field, value, maximum)
}

pub(crate) fn default_budget() -> usize {
    super::control_value::default_budget()
}
