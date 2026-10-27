// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Actual consumer views and their existing value kernels. Source ports retain
//! genuine node identity; this layer never evaluates a child expression.
use super::*;
use tidb_datatype::{Datum, Decimal, FieldType, FieldTypeCode, MySqlDuration, Time};

fn recipe(signature: SimpleSig, nulls: NullDemand, views: &'static [View]) -> (Recipe, Ports) {
    (Recipe::Typed { signature, nulls }, Ports::Children(views))
}
fn one(signature: SimpleSig, view: View) -> (Recipe, Ports) {
    let views: &'static [View] = match view {
        View::FoldedInt => &[View::FoldedInt],
        View::Int => &[View::Int],
        View::Decimal => &[View::Decimal],
        View::Json => &[View::Json],
        View::Real => &[View::Real],
        View::Duration => &[View::Duration],
        View::Time => &[View::Time],
        View::Bytes => &[View::Bytes],
        View::Datum => &[View::Datum],
    };
    recipe(signature, NullDemand::Never, views)
}
fn condition(view: View) -> (Recipe, Ports) {
    (Recipe::Condition(view), Ports::SelfView(view))
}

pub(super) fn ports(signature: SimpleSig, view: View) -> Option<(Recipe, Ports)> {
    use NullDemand::{First, Never};
    use SimpleSig::*;
    if let AddSubDate {
        subtract,
        date,
        interval,
        datetime_result,
    } = signature
    {
        let output = match (view, date, datetime_result) {
            (View::Int, DateArithArg::Duration, true) => {
                return Some((Recipe::ConstantInt(0), Ports::Children(&[])))
            }
            (View::Int, DateArithArg::Duration, false)
            | (View::Duration, DateArithArg::Duration, false) => {
                Some(tidb_expr::LegacyDateArithmeticOutputKind::Duration)
            }
            (View::Int, DateArithArg::Datetime, _) | (View::Time, DateArithArg::Datetime, _) => {
                Some(tidb_expr::LegacyDateArithmeticOutputKind::Time)
            }
            (
                View::Int | View::Bytes,
                DateArithArg::String
                | DateArithArg::Int
                | DateArithArg::Real
                | DateArithArg::Decimal,
                _,
            ) => Some(tidb_expr::LegacyDateArithmeticOutputKind::Text),
            _ => None,
        };
        return Some(match output {
            Some(output) => {
                let date_view = match date {
                    DateArithArg::String => View::Bytes,
                    DateArithArg::Int => View::FoldedInt,
                    DateArithArg::Real => View::Real,
                    DateArithArg::Decimal => View::Decimal,
                    DateArithArg::Datetime => View::Time,
                    DateArithArg::Duration => View::Duration,
                };
                let interval_view = match interval {
                    IntervalArg::String => View::Bytes,
                    IntervalArg::Int => View::FoldedInt,
                    IntervalArg::Real => View::Real,
                    IntervalArg::Decimal => View::Decimal,
                };
                (
                    Recipe::Date {
                        metadata: legacy_date_arithmetic_metadata(date, interval, subtract),
                        output,
                    },
                    Ports::Dynamic([date_view, interval_view, View::Bytes]),
                )
            }
            None => (Recipe::Null, Ports::Children(&[])),
        });
    }
    let result = match view {
        View::Int => match signature {
            InInt => (Recipe::In { collation: None }, Ports::All(View::Int)),
            InString(collation) => (
                Recipe::In {
                    collation: Some(collation),
                },
                Ports::All(View::Bytes),
            ),
            TimestampDiff => recipe(signature, First, &[View::Bytes, View::Time, View::Time]),
            JsonMemberOfSig => (
                Recipe::JsonMember,
                Ports::Children(&[View::Datum, View::Json]),
            ),
            PlusDecimal
            | MinusDecimal
            | MultiplyDecimal
            | ModDecimal
            | DivideDecimal
            | CastIntAsDecimal
            | CastRealAsDecimal
            | CastDecimalAsDecimal
            | CastStringAsDecimal
            | CastTimeAsDecimal
            | CastDurationAsDecimal
            | UnixTimestampDec => condition(View::Decimal),
            PlusReal | MinusReal | MultiplyReal | ModReal | DivideReal | CastJsonAsReal
            | CastIntAsReal | CastRealAsReal | CastDecimalAsReal | CastStringAsReal | RoundReal
            | RoundDec | Pow | Acos | Asin | Atan1Arg | Atan2Args | Cos | Cot | Pi | Sin => {
                condition(View::Real)
            }
            Lower | LowerUtf8 | Upper | UpperUtf8 | Substring2Args | Substring2ArgsUtf8
            | Substring3Args | Substring3ArgsUtf8 | Conv | CastIntAsString | CastRealAsString
            | CastDecimalAsString | CastStringAsString | CastTimeAsString
            | CastDurationAsString | FromUnixTime2Arg => condition(View::Bytes),
            CastIntAsJson | CastRealAsJson | CastDecimalAsJson | CastStringAsJson
            | CastTimeAsJson | CastDurationAsJson | CastJsonAsJson | JsonReplaceSig
            | JsonArrayAppendSig | JsonMergePatchSig => condition(View::Json),
            CastIntAsTime | CastRealAsTime | CastDecimalAsTime | CastStringAsTime
            | CastTimeAsTime | CastJsonAsTime => condition(View::Time),
            CastIntAsDuration
            | CastRealAsDuration
            | CastDecimalAsDuration
            | CastStringAsDuration
            | CastDurationAsDuration
            | CastJsonAsDuration
            | FromUnixTime1Arg => condition(View::Duration),
            CastRealAsInt => one(signature, View::Real),
            CastDecimalAsInt => one(signature, View::Decimal),
            CastStringAsInt => one(signature, View::Bytes),
            CastJsonAsInt => one(signature, View::Json),
            CastTimeAsDuration | Hour | Minute | Second | MicroSecond => {
                one(signature, View::Duration)
            }
            Date | Month | WeekWithoutMode | UnixTimestampInt => one(signature, View::Time),
            DateDiff | LtTime | LeTime | GtTime | GeTime | EqTime | NeTime => {
                recipe(signature, Never, &[View::Time, View::Time])
            }
            LtDecimal | LeDecimal | GtDecimal | GeDecimal | EqDecimal | NeDecimal
            | IntDivideDecimal => recipe(signature, Never, &[View::Decimal, View::Decimal]),
            LtReal | LeReal | GtReal | GeReal | EqReal | NeReal => {
                recipe(signature, Never, &[View::Real, View::Real])
            }
            LtString(_) | LeString(_) | GtString(_) | GeString(_) | EqString(_) | NeString(_)
            | RegexpLike(_) => recipe(signature, Never, &[View::Bytes, View::Bytes]),
            Like(_) => recipe(signature, Never, &[View::Bytes, View::Bytes, View::Int]),
            CharLength | CharLengthUtf8 => one(signature, View::Bytes),
            DateFormatSig | DecimalIsNull | DurationIsNull | RealIsNull | StringIsNull
            | TimeIsNull | VectorFloat32IsNull => one(signature, View::Datum),
            AddSubDate {
                date: DateArithArg::Duration,
                datetime_result: true,
                ..
            } => (Recipe::ConstantInt(0), Ports::Children(&[])),
            // Date arithmetic is classified above for every actual view.
            AddSubDate { .. } => return None,
            _ => return None,
        },
        View::Decimal => match signature {
            PlusDecimal | MinusDecimal | MultiplyDecimal | ModDecimal | DivideDecimal => {
                recipe(signature, First, &[View::Decimal, View::Decimal])
            }
            UnixTimestampDec => one(signature, View::Time),
            CastIntAsDecimal => one(signature, View::FoldedInt),
            CastRealAsDecimal => one(signature, View::Real),
            CastDecimalAsDecimal => one(signature, View::Decimal),
            CastStringAsDecimal => one(signature, View::Bytes),
            CastTimeAsDecimal => one(signature, View::Time),
            CastDurationAsDecimal => one(signature, View::Duration),
            _ => (Recipe::Null, Ports::Children(&[])),
        },
        View::Real => match signature {
            PlusReal | MinusReal | MultiplyReal | ModReal | DivideReal | Pow | Atan2Args => {
                recipe(signature, First, &[View::Real, View::Real])
            }
            CastIntAsReal | RoundInt => one(signature, View::FoldedInt),
            CastDecimalAsReal | RoundDec => one(signature, View::Decimal),
            CastStringAsReal => one(signature, View::Bytes),
            CastJsonAsReal => one(signature, View::Json),
            CastRealAsReal | RoundReal | Acos | Asin | Atan1Arg | Cos | Cot | Sin => {
                one(signature, View::Real)
            }
            Pi => recipe(signature, Never, &[]),
            _ => (Recipe::Null, Ports::Children(&[])),
        },
        View::Duration => match signature {
            CastTimeAsDuration => one(signature, View::Time),
            CastIntAsDuration => one(signature, View::FoldedInt),
            CastRealAsDuration => one(signature, View::Real),
            CastDecimalAsDuration => one(signature, View::Decimal),
            CastStringAsDuration => one(signature, View::Bytes),
            CastDurationAsDuration => one(signature, View::Duration),
            CastJsonAsDuration => one(signature, View::Json),
            AddSubDate {
                date: DateArithArg::Duration,
                datetime_result: false,
                ..
            } => return None,
            _ => (Recipe::Null, Ports::Children(&[])),
        },
        View::Time => match signature {
            CastIntAsTime => one(signature, View::FoldedInt),
            CastRealAsTime => one(signature, View::Real),
            CastDecimalAsTime | FromUnixTime1Arg => one(signature, View::Decimal),
            CastStringAsTime => one(signature, View::Bytes),
            CastTimeAsTime => one(signature, View::Time),
            CastJsonAsTime => one(signature, View::Json),
            AddSubDate {
                date: DateArithArg::Datetime,
                ..
            } => return None,
            _ => (Recipe::Null, Ports::Children(&[])),
        },
        View::Bytes => match signature {
            CastIntAsString => one(signature, View::FoldedInt),
            CastRealAsString => one(signature, View::Real),
            CastDecimalAsString => one(signature, View::Decimal),
            CastStringAsString | Lower | LowerUtf8 | Upper | UpperUtf8 => {
                one(signature, View::Bytes)
            }
            CastTimeAsString => one(signature, View::Time),
            CastDurationAsString => one(signature, View::Duration),
            DateFormatSig => recipe(signature, First, &[View::Time, View::Bytes]),
            Substring2Args | Substring2ArgsUtf8 => {
                recipe(signature, First, &[View::Bytes, View::FoldedInt])
            }
            Substring3Args | Substring3ArgsUtf8 | Conv => recipe(
                signature,
                First,
                &[View::Bytes, View::FoldedInt, View::FoldedInt],
            ),
            FromUnixTime2Arg => (
                Recipe::EpochFormat,
                Ports::Children(&[View::Decimal, View::Bytes]),
            ),
            AddSubDate {
                date:
                    DateArithArg::String
                    | DateArithArg::Int
                    | DateArithArg::Real
                    | DateArithArg::Decimal,
                ..
            } => return None,
            _ => (Recipe::Null, Ports::Children(&[])),
        },
        View::Json => match signature {
            CastIntAsJson => one(signature, View::FoldedInt),
            CastRealAsJson => one(signature, View::Real),
            CastDecimalAsJson => one(signature, View::Decimal),
            CastStringAsJson => one(signature, View::Bytes),
            CastTimeAsJson => one(signature, View::Time),
            CastDurationAsJson => one(signature, View::Duration),
            CastJsonAsJson => one(signature, View::Json),
            JsonMergePatchSig => (
                Recipe::Typed {
                    signature,
                    nulls: NullDemand::Any,
                },
                Ports::All(View::Json),
            ),
            JsonReplaceSig | JsonArrayAppendSig => (
                Recipe::JsonModify {
                    append: signature == JsonArrayAppendSig,
                },
                Ports::JsonPairs,
            ),
            _ => (Recipe::Null, Ports::Children(&[])),
        },
        _ => return None,
    };
    Some(result)
}

pub(super) fn null(view: View) -> Value {
    match view {
        View::Int | View::FoldedInt => Value::Int(None),
        View::Decimal => Value::Decimal(None),
        View::Json => Value::Json(None),
        View::Real => Value::Real(None),
        View::Duration => Value::Duration(None),
        View::Time => Value::Time(None),
        View::Bytes => Value::Bytes(None),
        View::Datum => Value::Datum(Datum::Null),
    }
}

type Args = std::vec::IntoIter<LegacyResult<Value>>;
macro_rules! argument {
    ($name:ident, $variant:ident, $ty:ty) => {
        fn $name(args: &mut Args) -> LegacyResult<Option<$ty>> {
            match args.next() {
                None => Ok(None),
                Some(Ok(Value::$variant(v))) => Ok(v),
                Some(Err(error)) => Err(error),
                _ => Err(LegacyEvalError::InvalidResult(
                    "legacy typed port view differs",
                )),
            }
        }
    };
}
argument!(int, Int, i128);
argument!(decimal, Decimal, Decimal);
argument!(json, Json, tidb_datatype::BinaryJSON);
argument!(real, Real, f64);
argument!(duration, Duration, MySqlDuration);
argument!(time, Time, Time);
argument!(bytes, Bytes, Vec<u8>);
fn datum(args: &mut Args) -> LegacyResult<Option<Datum>> {
    match args.next() {
        None => Ok(None),
        Some(Ok(Value::Datum(v))) => Ok(Some(v)),
        Some(Err(e)) => Err(e),
        _ => Err(LegacyEvalError::InvalidResult(
            "legacy datum port view differs",
        )),
    }
}
fn cast_datum(args: &mut Args) -> LegacyResult<Option<Datum>> {
    Ok(match args.next() {
        None => None,
        Some(Err(error)) => return Err(error),
        Some(Ok(Value::Int(v))) => v.and_then(|v| i64::try_from(v).ok()).map(Datum::Int),
        Some(Ok(Value::Decimal(v))) => v.map(Datum::Decimal),
        Some(Ok(Value::Json(v))) => v.map(Datum::Json),
        Some(Ok(Value::Real(v))) => v.map(Datum::Real),
        Some(Ok(Value::Duration(v))) => v.map(Datum::Duration),
        Some(Ok(Value::Time(v))) => v.map(Datum::Time),
        Some(Ok(Value::Bytes(v))) => v.map(Datum::Bytes),
        _ => return Err(LegacyEvalError::InvalidResult("legacy cast port differs")),
    })
}
fn binary<T>(arity: usize, left: Option<T>, right: Option<T>) -> tidb_expr::LegacyBinaryArgs<T> {
    if arity == 0 {
        tidb_expr::LegacyBinaryArgs::Missing
    } else if let Some(left) = left {
        if arity < 2 {
            tidb_expr::LegacyBinaryArgs::Missing
        } else if let Some(right) = right {
            tidb_expr::LegacyBinaryArgs::Values(left, right)
        } else {
            tidb_expr::LegacyBinaryArgs::NullWitness(None)
        }
    } else {
        tidb_expr::LegacyBinaryArgs::NullWitness(None)
    }
}

pub(super) fn condition_value(view: View, mut args: Args) -> LegacyResult<Value> {
    let value = match view {
        View::Decimal => decimal(&mut args)?.map(|v| i128::from(!v.is_zero())),
        View::Real => real(&mut args)?
            .filter(|v| !v.is_nan())
            .map(|v| i128::from(v != 0.0)),
        View::Bytes => bytes(&mut args)?.map(|raw| {
            let text = String::from_utf8_lossy(&raw);
            let numeric =
                tidb_expr::eval_legacy_numeric_prefix(text.trim_start(), true).unwrap_or_default();
            let value: f64 = numeric.parse().unwrap_or(0.0);
            i128::from(value != 0.0)
        }),
        View::Json => Some(i128::from(json(&mut args)?.is_some())),
        View::Time => Some(i128::from(time(&mut args)?.is_some())),
        View::Duration => Some(i128::from(duration(&mut args)?.is_some())),
        _ => {
            return Err(LegacyEvalError::InvalidResult(
                "unknown legacy condition view",
            ))
        }
    };
    Ok(Value::Int(value))
}

pub(super) fn evaluate(
    node: &Node<'_>,
    mut args: Args,
    evaluator: &LegacyEvaluator<'_>,
    columns: &dyn tidb_expr::Columns,
    need_length: Option<bool>,
) -> LegacyResult<Value> {
    let Recipe::Typed { signature, .. } = node.recipe else {
        return Err(LegacyEvalError::InvalidResult("typed recipe missing"));
    };
    use tidb_expr::BinaryArithmeticOperation as Op;
    use SimpleSig::*;
    let operation = match signature {
        PlusDecimal | PlusReal => Op::Add,
        MinusDecimal | MinusReal => Op::Subtract,
        ModDecimal | ModReal => Op::Modulo,
        DivideReal => Op::Divide,
        _ => Op::Multiply,
    };
    Ok(match node.view {
        View::Decimal => Value::Decimal(match signature {
            PlusDecimal | MinusDecimal | MultiplyDecimal | ModDecimal | DivideDecimal => {
                let left = decimal(&mut args)?;
                let right = decimal(&mut args)?;
                let input = binary(node.original_arity, left, right);
                if signature == DivideDecimal {
                    tidb_expr::eval_legacy_decimal_division_in(
                        input,
                        evaluator.div_precision_increment as u32,
                        columns,
                    )?
                } else {
                    tidb_expr::eval_legacy_decimal_arithmetic_in(operation, input, columns)?
                }
            }
            CastIntAsDecimal => {
                int(&mut args)?.and_then(tidb_expr::eval_legacy_cast_decimal_integer)
            }
            CastRealAsDecimal
            | CastDecimalAsDecimal
            | CastStringAsDecimal
            | CastTimeAsDecimal
            | CastDurationAsDecimal => cast_datum(&mut args)?
                .as_ref()
                .and_then(tidb_expr::eval_legacy_cast_decimal_datum),
            UnixTimestampDec => tidb_expr::unix_timestamp_dec_legacy_in(
                time(&mut args)?,
                evaluator.time_zone,
                columns,
            )?,
            _ => return Err(LegacyEvalError::InvalidResult("unknown decimal recipe")),
        }),
        View::Real => Value::Real(match signature {
            PlusReal | MinusReal | MultiplyReal | ModReal | DivideReal => {
                let left = real(&mut args)?;
                let right = real(&mut args)?;
                tidb_expr::eval_legacy_real_arithmetic_in(
                    operation,
                    binary(node.original_arity, left, right),
                    columns,
                )?
            }
            CastIntAsReal => int(&mut args)?.map(tidb_expr::eval_legacy_cast_real_integer),
            CastDecimalAsReal | CastRealAsReal | CastStringAsReal => cast_datum(&mut args)?
                .as_ref()
                .and_then(tidb_expr::eval_legacy_cast_real_datum),
            CastJsonAsReal => json(&mut args)?
                .as_ref()
                .map(tidb_expr::eval_legacy_cast_json_real),
            RoundReal => tidb_expr::eval_legacy_round_real_in(real(&mut args)?, columns)?,
            RoundInt => {
                tidb_expr::eval_legacy_round_int_in(int(&mut args)?, columns)?.map(|v| v as f64)
            }
            RoundDec => {
                tidb_expr::eval_legacy_round_decimal_in(decimal(&mut args)?.as_ref(), columns)?
            }
            Pow => {
                let input = match real(&mut args)? {
                    None => tidb_expr::RawPowReadyArgs::LeftNull,
                    Some(base) => tidb_expr::RawPowReadyArgs::Values {
                        base,
                        exponent: real(&mut args)?,
                    },
                };
                match tidb_expr::eval_raw_pow_ready_in(input, columns)? {
                    Datum::Real(v) => Some(v),
                    Datum::Null => None,
                    _ => {
                        return Err(LegacyEvalError::InvalidResult(
                            "POW returned a non-real datum",
                        ))
                    }
                }
            }
            Atan2Args => {
                let input = match real(&mut args)? {
                    None => None,
                    Some(v) => Some((v, real(&mut args)?)),
                };
                tidb_expr::eval_legacy_atan2_in(input, columns)?
            }
            Acos | Asin => {
                let function = if signature == Acos {
                    tidb_expr::RawInverseTrigFunction::Acos
                } else {
                    tidb_expr::RawInverseTrigFunction::Asin
                };
                match tidb_expr::eval_raw_inverse_trig_ready_in(
                    function,
                    real(&mut args)?,
                    columns,
                )? {
                    Datum::Real(v) => Some(v),
                    Datum::Null => None,
                    _ => {
                        return Err(LegacyEvalError::InvalidResult(
                            "raw inverse trig returned a non-real datum",
                        ))
                    }
                }
            }
            Pi => match tidb_expr::eval_pi_in(columns)? {
                Datum::Real(v) => Some(v),
                _ => {
                    return Err(LegacyEvalError::InvalidResult(
                        "PI returned a non-real datum",
                    ))
                }
            },
            Atan1Arg | Cos | Sin | Cot => {
                let function = match signature {
                    Atan1Arg => tidb_expr::LegacyTrigFunction::Atan,
                    Cos => tidb_expr::LegacyTrigFunction::Cos,
                    Sin => tidb_expr::LegacyTrigFunction::Sin,
                    _ => tidb_expr::LegacyTrigFunction::Cot,
                };
                tidb_expr::eval_legacy_trig_in(function, real(&mut args)?, columns)?
            }
            _ => return Err(LegacyEvalError::InvalidResult("unknown real recipe")),
        }),
        View::Duration => Value::Duration(
            cast_datum(&mut args)?
                .as_ref()
                .and_then(tidb_expr::eval_legacy_cast_duration_datum),
        ),
        View::Time => Value::Time(if signature == FromUnixTime1Arg {
            tidb_expr::eval_from_unixtime_legacy_scoped_in(
                decimal(&mut args)?,
                evaluator.time_zone,
                columns,
                |time, _| time,
            )?
        } else {
            cast_datum(&mut args)?
                .as_ref()
                .and_then(|v| tidb_expr::eval_legacy_cast_time_datum(v, evaluator.time_zone))
        }),
        View::Bytes => Value::Bytes(match signature {
            CastIntAsString => int(&mut args)?.map(tidb_expr::eval_legacy_cast_string_integer),
            CastRealAsString | CastDecimalAsString | CastStringAsString | CastTimeAsString
            | CastDurationAsString => cast_datum(&mut args)?
                .as_ref()
                .and_then(tidb_expr::eval_legacy_cast_string_datum),
            DateFormatSig => match time(&mut args)? {
                None => tidb_expr::eval_legacy_date_format_in(None, columns)?,
                Some(time) => {
                    let layout = bytes(&mut args)?;
                    let layout = layout.as_deref().map(String::from_utf8_lossy);
                    tidb_expr::eval_legacy_date_format_in(
                        Some((time.core_time(), layout.as_deref())),
                        columns,
                    )?
                }
            },
            Lower | LowerUtf8 | Upper | UpperUtf8 => {
                let function = match signature {
                    Lower => tidb_expr::RawCaseFunction::LowerAscii,
                    Upper => tidb_expr::RawCaseFunction::UpperAscii,
                    LowerUtf8 => tidb_expr::RawCaseFunction::LowerUtf8,
                    _ => tidb_expr::RawCaseFunction::UpperUtf8,
                };
                let raw = bytes(&mut args)?;
                let raw = if matches!(signature, LowerUtf8 | UpperUtf8) {
                    raw.map(|v| String::from_utf8_lossy(&v).into_owned().into_bytes())
                } else {
                    raw
                };
                match tidb_expr::eval_raw_case_ready_in(function, raw, columns)? {
                    Datum::Bytes(v) => Some(v),
                    Datum::Null => None,
                    _ => {
                        return Err(LegacyEvalError::InvalidResult(
                            "raw case conversion returned a non-bytes datum",
                        ))
                    }
                }
            }
            Substring2Args | Substring2ArgsUtf8 | Substring3Args | Substring3ArgsUtf8 => {
                let raw = bytes(&mut args)?;
                let pos = if raw.is_some() {
                    tidb_expr::RawSubstringInt::Value(int(&mut args)?)
                } else {
                    tidb_expr::RawSubstringInt::Undemanded
                };
                let utf8 = matches!(signature, Substring2ArgsUtf8 | Substring3ArgsUtf8);
                let function = if utf8 {
                    tidb_expr::RawSubstringFunction::Utf8
                } else {
                    tidb_expr::RawSubstringFunction::Bytes
                };
                let input = if matches!(signature, Substring3Args | Substring3ArgsUtf8) {
                    let needs = need_length.unwrap_or_else(|| match (&raw, pos) {
                        (Some(raw), tidb_expr::RawSubstringInt::Value(Some(position))) => {
                            tidb_expr::raw_substring_needs_len(raw, position, utf8)
                        }
                        _ => false,
                    });
                    let len = if needs {
                        tidb_expr::RawSubstringInt::Value(int(&mut args)?)
                    } else {
                        tidb_expr::RawSubstringInt::Undemanded
                    };
                    tidb_expr::RawSubstringReadyArgs::Three {
                        bytes: raw,
                        pos,
                        len,
                    }
                } else {
                    tidb_expr::RawSubstringReadyArgs::Two { bytes: raw, pos }
                };
                match tidb_expr::eval_raw_substring_ready_in(function, input, columns)? {
                    Datum::Bytes(v) => Some(v),
                    Datum::Null => None,
                    _ => {
                        return Err(LegacyEvalError::InvalidResult(
                            "raw substring returned a non-bytes datum",
                        ))
                    }
                }
            }
            Conv => {
                let text = bytes(&mut args)?;
                let (from, to) = if text.is_some() {
                    let from = int(&mut args)?;
                    let to = if from.is_some_and(|v| i64::try_from(v).is_ok()) {
                        tidb_expr::LegacyConvBase::Value(int(&mut args)?)
                    } else {
                        tidb_expr::LegacyConvBase::Undemanded
                    };
                    (tidb_expr::LegacyConvBase::Value(from), to)
                } else {
                    (
                        tidb_expr::LegacyConvBase::Undemanded,
                        tidb_expr::LegacyConvBase::Undemanded,
                    )
                };
                tidb_expr::eval_legacy_conv_in(text, from, to, columns)?
            }
            _ => return Err(LegacyEvalError::InvalidResult("unknown bytes recipe")),
        }),
        View::Json => Value::Json(if signature == JsonMergePatchSig {
            let mut values = Vec::new();
            while !args.as_slice().is_empty() {
                let Some(value) = json(&mut args)? else {
                    return Ok(Value::Json(tidb_expr::eval_legacy_json_output_none_in(
                        columns,
                    )?));
                };
                values.push(value);
            }
            tidb_expr::eval_legacy_json_merge_patch_in(&values, columns)?
        } else {
            cast_datum(&mut args)?
                .as_ref()
                .and_then(tidb_expr::eval_legacy_cast_json_datum)
        }),
        View::Int => return integer(signature, args, node.original_arity, evaluator, columns),
        _ => return Err(LegacyEvalError::InvalidResult("unknown typed source view")),
    })
}

fn comparison(signature: SimpleSig) -> tidb_expr::ComparisonOp {
    use SimpleSig::*;
    match signature {
        LtReal | LtDecimal | LtTime | LtString(_) => tidb_expr::ComparisonOp::Lt,
        LeReal | LeDecimal | LeTime | LeString(_) => tidb_expr::ComparisonOp::Le,
        GtReal | GtDecimal | GtTime | GtString(_) => tidb_expr::ComparisonOp::Gt,
        GeReal | GeDecimal | GeTime | GeString(_) => tidb_expr::ComparisonOp::Ge,
        EqReal | EqDecimal | EqTime | EqString(_) => tidb_expr::ComparisonOp::Eq,
        _ => tidb_expr::ComparisonOp::Ne,
    }
}
fn integer(
    signature: SimpleSig,
    mut args: Args,
    arity: usize,
    evaluator: &LegacyEvaluator<'_>,
    columns: &dyn tidb_expr::Columns,
) -> LegacyResult<Value> {
    use SimpleSig::*;
    let value = match signature {
        TimestampDiff => {
            let input = match bytes(&mut args)? {
                None => tidb_expr::LegacyTimestampDiffArgs::NullWitness(None),
                Some(unit) => {
                    let left = time(&mut args)?.map(|v| v.core_time());
                    let right = time(&mut args)?.map(|v| v.core_time());
                    tidb_expr::LegacyTimestampDiffArgs::Values { unit, left, right }
                }
            };
            tidb_expr::eval_legacy_timestamp_diff_in(input, columns)?.map(i128::from)
        }
        CastRealAsInt | CastDecimalAsInt | CastStringAsInt => match cast_datum(&mut args)?
            .as_ref()
            .and_then(tidb_expr::eval_legacy_cast_integer_datum)
        {
            Some(tidb_expr::LegacyCastIntegerResult::Value(v)) => Some(v),
            Some(tidb_expr::LegacyCastIntegerResult::Overflow(v)) => {
                return Err(LegacyEvalError::Sql(format!(
                    "constant {} overflows bigint",
                    go_float_display(v)
                )))
            }
            None => None,
        },
        CastJsonAsInt => json(&mut args)?
            .as_ref()
            .map(tidb_expr::eval_legacy_cast_json_integer),
        IntDivideDecimal => {
            let l = decimal(&mut args)?;
            let r = decimal(&mut args)?;
            tidb_expr::eval_legacy_decimal_integer_division_in(
                legacy_comparison_args(arity, l, r),
                columns,
            )?
        }
        LtReal | LeReal | GtReal | GeReal | EqReal | NeReal => {
            let l = real(&mut args)?;
            let r = real(&mut args)?;
            tidb_expr::eval_legacy_real_comparison_in(
                comparison(signature),
                legacy_comparison_args(arity, l, r),
                columns,
            )?
        }
        LtDecimal | LeDecimal | GtDecimal | GeDecimal | EqDecimal | NeDecimal => {
            let l = decimal(&mut args)?;
            let r = decimal(&mut args)?;
            tidb_expr::eval_legacy_decimal_comparison_in(
                comparison(signature),
                legacy_comparison_args(arity, l, r),
                columns,
            )?
        }
        LtTime | LeTime | GtTime | GeTime | EqTime | NeTime => {
            let l = time(&mut args)?;
            let r = time(&mut args)?;
            tidb_expr::eval_legacy_time_comparison_in(
                comparison(signature),
                legacy_comparison_args(arity, l, r),
                columns,
            )?
        }
        LtString(c) | LeString(c) | GtString(c) | GeString(c) | EqString(c) | NeString(c) => {
            let l = bytes(&mut args)?;
            let r = bytes(&mut args)?;
            tidb_expr::eval_legacy_bytes_comparison_in(
                comparison(signature),
                legacy_comparison_args(arity, l, r),
                c,
                columns,
            )?
        }
        CharLength | CharLengthUtf8 => {
            let binary = signature == CharLength;
            let value = match bytes(&mut args)? {
                None => Datum::Null,
                Some(v) if binary => Datum::new_bytes(v),
                Some(v) => Datum::new_string(String::from_utf8_lossy(&v).into_owned()),
            };
            let collation = if binary {
                tidb_datatype::Collation::Binary
            } else {
                tidb_datatype::Collation::DEFAULT
            };
            let built = tidb_expr::BuildContext::default().build_string_length(
                tidb_expr::StringLengthFunction::CharLength,
                FieldType::new(FieldTypeCode::VarString).with_collation(collation),
            );
            match built.eval(&value)? {
                Datum::Null => None,
                Datum::Int(v) => Some(i128::from(v)),
                _ => {
                    return Err(LegacyEvalError::InvalidResult(
                        "CHAR_LENGTH returned a non-integer datum",
                    ))
                }
            }
        }
        DateFormatSig if arity == 0 => tidb_expr::eval_legacy_date_format_missing_in(columns)?,
        DateFormatSig | DecimalIsNull | DurationIsNull | RealIsNull | StringIsNull | TimeIsNull
        | VectorFloat32IsNull => {
            let ready = match datum(&mut args)? {
                None | Some(Datum::Null) => None,
                Some(_) => Some(false),
            };
            if signature == DateFormatSig {
                match tidb_expr::eval_boolean_ready_in(
                    tidb_expr::BooleanFunction::IsNotNull,
                    ready,
                    columns,
                )? {
                    Datum::Int(v @ (0 | 1)) => Some(i128::from(v)),
                    _ => {
                        return Err(LegacyEvalError::InvalidResult(
                            "legacy DATE_FORMAT presence result kind mismatch",
                        ))
                    }
                }
            } else {
                eval_boolean_ready(tidb_expr::BooleanFunction::IsNull, ready, columns)?
            }
        }
        CastTimeAsDuration => duration(&mut args)?.map(|v| i128::from(v.nanoseconds() != 0)),
        Date => {
            tidb_expr::eval_legacy_date_in(time(&mut args)?.map(|v| v.core_time().raw()), columns)?
                .map(i128::from)
        }
        Month => tidb_expr::eval_legacy_month_in(time(&mut args)?.map(|v| v.core_time()), columns)?
            .map(i128::from),
        WeekWithoutMode => {
            match tidb_expr::eval_legacy_week_in(time(&mut args)?.map(|v| v.core_time()), columns)?
            {
                Datum::Null => None,
                Datum::Int(v) => Some(i128::from(v)),
                _ => {
                    return Err(LegacyEvalError::InvalidResult(
                        "legacy WEEK result kind mismatch",
                    ))
                }
            }
        }
        Hour | Minute | Second => {
            let field = match signature {
                Hour => tidb_expr::LegacyHmsField::Hour,
                Minute => tidb_expr::LegacyHmsField::Minute,
                _ => tidb_expr::LegacyHmsField::Second,
            };
            tidb_expr::eval_legacy_hms_in(
                field,
                duration(&mut args)?.map(MySqlDuration::nanoseconds),
                columns,
            )?
            .map(i128::from)
        }
        MicroSecond => tidb_expr::eval_legacy_microsecond_in(
            duration(&mut args)?.map(MySqlDuration::nanoseconds),
            columns,
        )?
        .map(i128::from),
        DateDiff => {
            let l = time(&mut args)?.map(|v| v.core_time());
            let r = time(&mut args)?.map(|v| v.core_time());
            tidb_expr::eval_legacy_date_diff_in(l, r, columns)?.map(i128::from)
        }
        UnixTimestampInt => {
            tidb_expr::unix_timestamp_int_legacy_in(time(&mut args)?, evaluator.time_zone, columns)?
        }
        Like(collation) => {
            let target = bytes(&mut args)?;
            let pattern = bytes(&mut args)?;
            let escape = int(&mut args)?;
            let (insensitive, input) = if arity < 3 {
                (false, tidb_expr::LegacyLikeArgs::Missing)
            } else if let (Some(target), Some(pattern), Some(escape)) = (target, pattern, escape) {
                (
                    tidb_datatype::get_collator_by_id(collation)
                        .compare(b"a", b"A")
                        .is_eq(),
                    tidb_expr::LegacyLikeArgs::Values {
                        target,
                        pattern,
                        escape: escape as u8,
                    },
                )
            } else {
                (false, tidb_expr::LegacyLikeArgs::NullWitness(None))
            };
            tidb_expr::eval_legacy_like_in(insensitive, input, columns)?
        }
        RegexpLike(collation) => {
            let target = bytes(&mut args)?;
            let pattern = bytes(&mut args)?;
            let input = if arity < 2 {
                tidb_expr::RegexpLegacyInput::Missing
            } else {
                let case_insensitive = if target.is_some() && pattern.is_some() {
                    Some(
                        tidb_datatype::get_collator_by_id(collation)
                            .compare(b"a", b"A")
                            .is_eq(),
                    )
                } else {
                    None
                };
                tidb_expr::RegexpLegacyInput::Values {
                    text: target,
                    pattern,
                    case_insensitive,
                }
            };
            match tidb_expr::eval_regexp_legacy_ready_in(columns, input)? {
                Datum::Null => None,
                Datum::Int(v @ (0 | 1)) => Some(i128::from(v)),
                _ => {
                    return Err(LegacyEvalError::InvalidResult(
                        "legacy REGEXP result kind mismatch",
                    ))
                }
            }
        }
        _ => {
            return Err(LegacyEvalError::InvalidResult(
                "unknown typed integer source",
            ))
        }
    };
    Ok(Value::Int(value))
}
