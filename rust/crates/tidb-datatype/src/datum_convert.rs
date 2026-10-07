// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Source-shaped `pkg/types/datum.go::Datum.ConvertTo` implementation.
//!
//! Contextful conversion reports typed diagnostics at their originating stage
//! through the caller's warning sink. The legacy value/event interface shares
//! the value engine, but cannot represent multiple diagnostics for one value.

use chrono::Utc;

pub(crate) mod diagnostics;
use crate::parser_types_errors::{
    ERR_DATA_TOO_LONG, ERR_OVERFLOW, ERR_TRUNCATED, ERR_TRUNCATED_WRONG_VALUE,
};
pub use diagnostics::DatumConversion;
use diagnostics::Diagnostics;

#[cfg(test)]
use crate::VectorFloat32;
use crate::{
    convert_decimal_to_uint, convert_float_to_int, convert_float_to_uint, convert_int_to_int,
    convert_int_to_uint, convert_uint_to_int, convert_uint_to_uint, integer_signed_lower_bound,
    integer_signed_upper_bound, integer_unsigned_upper_bound, json_to_int, parse_enum,
    parse_enum_value, parse_set, parse_set_value, parse_time, parse_time_from_num, BinaryJSON,
    BinaryLiteral, BinaryLiteralWidth, Charset, Collation, ConversionFlags, Converted, CoreTime,
    Datum, DatumValueError, Decimal, FieldType, FieldTypeCode, MySqlDuration,
    ScalarConversionError, ScalarConversionEvent, SessionTimeZone, Time, TimeType,
    UNSPECIFIED_LENGTH,
};

/// Direction used by reverse expression evaluation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RoundingType {
    /// Round toward positive infinity.
    Ceiling,
    /// Round toward negative infinity.
    Floor,
}

impl Datum {
    /// Converts this datum into the target MySQL field domain.
    ///
    /// This is the pure value/event half of Go `Datum.ConvertTo`. Callers own
    /// statement warning/error policy and consume [`Converted::event`].
    ///
    /// Go's `Datum.ConvertTo` takes a `types.Context`, which carries a
    /// LOCATION beside the flags, and hands it to `ParseTime`. This overload
    /// keeps the zone-free callers unchanged by supplying UTC; a caller that
    /// owns a session zone must use [`Datum::convert_to_in`], because a
    /// `TIMESTAMP` target is range-checked in that zone.
    pub fn convert_to(
        &self,
        target: &FieldType,
        flags: ConversionFlags,
    ) -> Result<Converted<Self>, DatumValueError> {
        self.convert_to_in(target, flags, &SessionTimeZone::utc())
    }

    /// Go `Datum.ConvertTo(ctx, target)` with the statement's own
    /// `ctx.Location()`.
    ///
    /// The zone is not decoration: Go's `checkTimestampType` converts a
    /// `TIMESTAMP` out of `ctx.Location()` into UTC before comparing it
    /// against `MinTimestamp`/`MaxTimestamp`, so which literals a
    /// `TIMESTAMP` column admits MOVES with the session zone -- on the write
    /// path and on the DDL `DEFAULT` path alike.
    pub fn convert_to_in(
        &self,
        target: &FieldType,
        flags: ConversionFlags,
        zone: &SessionTimeZone,
    ) -> Result<Converted<Self>, DatumValueError> {
        self.convert_to_reported(target, flags, zone, &mut Diagnostics::new(None))
    }

    /// Converts through the same value engine while preserving each stage's
    /// typed diagnostics and the caller-owned warning order. The zone is the
    /// evaluated session location; the context supplies flags and warning policy.
    /// Unported diagnostic stages return `DatumValueError::Unsupported`; they
    /// must not be treated as successful contextful conversions.
    pub fn convert_to_in_context(
        &self,
        target: &FieldType,
        context: &crate::ConversionContext<'_>,
        zone: &SessionTimeZone,
    ) -> Result<DatumConversion, DatumValueError> {
        let mut diagnostics = Diagnostics::new(Some(context));
        let converted =
            self.convert_to_reported(target, context.flags(), zone, &mut diagnostics)?;
        if diagnostics.unmapped {
            return Err(DatumValueError::Unsupported(
                self.kind(),
                "conversion diagnostic",
            ));
        }
        Ok(DatumConversion {
            value: converted.value,
            error: diagnostics.error,
        })
    }

    fn convert_to_reported(
        &self,
        target: &FieldType,
        flags: ConversionFlags,
        zone: &SessionTimeZone,
        diagnostics: &mut Diagnostics<'_, '_>,
    ) -> Result<Converted<Self>, DatumValueError> {
        if self.is_null() || matches!(target.code(), FieldTypeCode::Null) {
            return Ok(exact(Self::Null));
        }
        match target.code() {
            FieldTypeCode::Tiny
            | FieldTypeCode::Short
            | FieldTypeCode::Int24
            | FieldTypeCode::Long
            | FieldTypeCode::LongLong => {
                if target.is_unsigned() {
                    self.convert_to_unsigned_reported(target.code(), flags, diagnostics)
                        .map(map_converted(Self::UInt))
                } else {
                    self.convert_to_signed_reported(target.code(), flags, zone, diagnostics)
                        .map(map_converted(Self::Int))
                }
            }
            FieldTypeCode::Float | FieldTypeCode::Double => {
                let converted = match self {
                    Self::String(value) => {
                        crate::convert::str_to_float_reported(value.as_utf8()?, false, diagnostics)
                    }
                    Self::Bytes(value) => crate::convert::str_to_float_reported(
                        std::str::from_utf8(value)?,
                        false,
                        diagnostics,
                    ),
                    _ => {
                        let converted = self.to_f64()?;
                        diagnostics.unhandled(converted.event.as_ref());
                        converted
                    }
                };
                let produced = produce_float_reported(converted.value, target, diagnostics);
                let event = numeric_conversion_event(converted.event, produced.event, flags);
                Ok(Converted {
                    value: if matches!(target.code(), FieldTypeCode::Float) {
                        Self::Float32(f64::from(produced.value as f32))
                    } else {
                        Self::Real(produced.value)
                    },
                    event,
                })
            }
            FieldTypeCode::String
            | FieldTypeCode::Varchar
            | FieldTypeCode::VarString
            | FieldTypeCode::Blob
            | FieldTypeCode::TinyBlob
            | FieldTypeCode::MediumBlob
            | FieldTypeCode::LongBlob => {
                let bytes = self.string_conversion_bytes(target, flags)?;
                let produced = produce_string_reported(bytes, target, true, diagnostics)?;
                Ok(Converted {
                    value: if target.charset() == Charset::Binary {
                        Self::new_bytes(produced.value)
                    } else {
                        Self::new_collation_string(produced.value, target.collation())
                    },
                    event: produced.event,
                })
            }
            FieldTypeCode::NewDecimal => self.convert_to_decimal_target(target, diagnostics),
            FieldTypeCode::Date | FieldTypeCode::Datetime | FieldTypeCode::Timestamp => {
                diagnostics.unreported(self.convert_to_time_target(target, flags, zone))
            }
            FieldTypeCode::Duration => {
                diagnostics.unreported(self.convert_to_duration_target(target, zone))
            }
            FieldTypeCode::Year => diagnostics.unreported(self.convert_to_year(flags, zone)),
            FieldTypeCode::Enum => diagnostics.unreported(self.convert_to_enum(target, flags)),
            FieldTypeCode::Set => diagnostics.unreported(self.convert_to_set(target, flags)),
            FieldTypeCode::Bit => diagnostics.unreported(self.convert_to_bit(target, flags)),
            FieldTypeCode::Json => diagnostics.unreported(self.convert_to_json_target()),
            FieldTypeCode::VectorFloat32 => diagnostics.unreported(self.convert_to_vector(target)),
            other => Err(DatumValueError::Unsupported(
                self.kind(),
                field_target_name(other),
            )),
        }
    }

    fn string_conversion_bytes(
        &self,
        target: &FieldType,
        flags: ConversionFlags,
    ) -> Result<Vec<u8>, DatumValueError> {
        if matches!(self, Self::String(_) | Self::Bytes(_)) {
            let from_binary = self.collation() == Some(Collation::Binary);
            let to_binary = target.charset() == Charset::Binary;
            if from_binary && to_binary {
                return Ok(self.as_raw_bytes().unwrap().to_vec());
            }
            let transformed = if from_binary {
                self.binary_string_decoded(flags, target.charset().name())
            } else if to_binary {
                return Ok(self.binary_string_encoded().unwrap());
            } else {
                self.string_with_check(flags, target.charset().name())
                    .unwrap()
            };
            let (bytes, error) = transformed.into_parts();
            if let Some(error) = error {
                return Err(DatumValueError::Comparison(error.to_string()));
            }
            return Ok(bytes);
        }
        // Go `convertToString`'s `KindBinaryLiteral` arm, which is the same
        // accessor the `fromBinary` arm above uses.
        if matches!(self, Self::BinaryLiteral(_)) {
            let (bytes, error) = self
                .binary_string_decoded(flags, target.charset().name())
                .into_parts();
            if let Some(error) = error {
                return Err(DatumValueError::Comparison(error.to_string()));
            }
            return Ok(bytes);
        }
        self.to_bytes().map_err(|error| {
            DatumValueError::Comparison(format!("string conversion failed: {error}"))
        })
    }

    fn convert_to_signed(
        &self,
        target: FieldTypeCode,
        flags: ConversionFlags,
        zone: &SessionTimeZone,
    ) -> Result<Converted<i64>, DatumValueError> {
        self.convert_to_signed_reported(target, flags, zone, &mut Diagnostics::new(None))
    }

    fn convert_to_signed_reported(
        &self,
        target: FieldTypeCode,
        flags: ConversionFlags,
        zone: &SessionTimeZone,
        diagnostics: &mut Diagnostics<'_, '_>,
    ) -> Result<Converted<i64>, DatumValueError> {
        let lower = integer_signed_lower_bound(target);
        let upper = integer_signed_upper_bound(target);
        let converted = match self {
            Self::Int(value) => numeric_outcome(convert_int_to_int(*value, lower, upper, target)),
            Self::UInt(value) => numeric_outcome(convert_uint_to_int(*value, upper, target)),
            Self::Real(value) | Self::Float32(value) => {
                numeric_outcome(convert_float_to_int(*value, lower, upper, target))
            }
            Self::String(value) => {
                let parsed = crate::convert::str_to_int_reported(
                    value.as_utf8()?,
                    false,
                    flags.truncate_as_warning() || flags.ignore_truncate_err(),
                    diagnostics,
                );
                let bounded =
                    numeric_outcome(convert_int_to_int(parsed.value, lower, upper, target));
                diagnostics.numeric_overflow(bounded.event.as_ref());
                Converted {
                    value: bounded.value,
                    event: numeric_conversion_event(parsed.event, bounded.event, flags),
                }
            }
            Self::Bytes(value) => {
                let parsed = crate::convert::str_to_int_reported(
                    std::str::from_utf8(value)?,
                    false,
                    flags.truncate_as_warning() || flags.ignore_truncate_err(),
                    diagnostics,
                );
                let bounded =
                    numeric_outcome(convert_int_to_int(parsed.value, lower, upper, target));
                diagnostics.numeric_overflow(bounded.event.as_ref());
                Converted {
                    value: bounded.value,
                    event: numeric_conversion_event(parsed.event, bounded.event, flags),
                }
            }
            // Go rounds the temporal value itself before rendering it as a
            // number, so a carry propagates through the sexagesimal fields:
            // `11:59:59.999999` becomes 120000, not 115960.
            //
            // The zone is load-bearing, not decoration. Go's
            // `Time.RoundFrac` (pkg/types/time.go) rounds through
            // `t.GoTime(ctx.Location())`, so when the carry lands exactly on
            // a DST transition instant the wall clock read back is the
            // SESSION zone's, not UTC's. Measured against Go:
            // `2011-03-13 01:59:59.999999` rounds to 20110313020000 in UTC
            // but 20110313030000 in America/Los_Angeles (02:00 does not
            // exist there), and `2011-11-06 01:59:59.999999` rounds to
            // 20111106020000 in UTC but 20111106010000 in
            // America/Los_Angeles (the repeated hour). Passing `Utc` here
            // silently returned the UTC answer for every session.
            Self::Time(value) => decimal_to_signed(
                &value
                    .round_frac(crate::DEFAULT_FSP, zone)
                    .map_err(conversion_error)?
                    .to_number(),
                lower,
                upper,
                target,
            ),
            Self::Duration(value) => decimal_to_signed(
                &value
                    .round_frac(crate::DEFAULT_FSP)
                    .map_err(conversion_error)?
                    .to_number(),
                lower,
                upper,
                target,
            ),
            Self::Decimal(value) => decimal_to_signed(value, lower, upper, target),
            Self::Enum(value, _) => numeric_outcome(convert_float_to_int(
                value.to_number(),
                lower,
                upper,
                target,
            )),
            Self::Set(value, _) => numeric_outcome(convert_float_to_int(
                value.to_number(),
                lower,
                upper,
                target,
            )),
            Self::BinaryLiteral(value) | Self::Bit(value) => {
                let literal = value.to_int();
                if literal.is_truncated() {
                    // Go's `toSignedInteger` returns immediately when
                    // `BinaryLiteral.ToInt` reports the too-wide literal;
                    // the value beside that error is the zero `int64`.
                    Converted {
                        value: 0,
                        event: Some(ScalarConversionEvent::Truncated),
                    }
                } else {
                    numeric_outcome(convert_uint_to_int(literal.value(), upper, target))
                }
            }
            Self::Json(value) => json_to_int(value, false, target, flags),
            _ => return Err(DatumValueError::Unsupported(self.kind(), "signed integer")),
        };
        match self {
            Self::String(_) | Self::Bytes(_) => {}
            Self::Int(_) | Self::UInt(_) => diagnostics.numeric_overflow(converted.event.as_ref()),
            Self::Real(_) | Self::Float32(_) | Self::Enum(..) | Self::Set(..)
                if converted.event.is_some() =>
            {
                let value = match self {
                    Self::Real(value) | Self::Float32(value) => *value,
                    Self::Enum(value, _) => value.to_number(),
                    Self::Set(value, _) => value.to_number(),
                    _ => unreachable!(),
                };
                diagnostics.error(|| {
                    ERR_OVERFLOW.generate(format!(
                        "constant {} overflows {}",
                        crate::format_float_g_shortest(crate::round_float(value)),
                        crate::type_str(target),
                    ))
                });
            }
            Self::Decimal(value) if converted.event.is_some() => {
                if value.round_to_i64().is_none() {
                    diagnostics.error(|| ERR_OVERFLOW.clone());
                } else {
                    diagnostics.numeric_overflow(converted.event.as_ref());
                }
            }
            _ => diagnostics.unhandled(converted.event.as_ref()),
        }
        Ok(converted)
    }

    fn convert_to_unsigned(
        &self,
        target: FieldTypeCode,
        flags: ConversionFlags,
    ) -> Result<Converted<u64>, DatumValueError> {
        self.convert_to_unsigned_reported(target, flags, &mut Diagnostics::new(None))
    }

    fn convert_to_unsigned_reported(
        &self,
        target: FieldTypeCode,
        flags: ConversionFlags,
        diagnostics: &mut Diagnostics<'_, '_>,
    ) -> Result<Converted<u64>, DatumValueError> {
        let upper = integer_unsigned_upper_bound(target);
        let converted = match self {
            Self::Int(value) => numeric_outcome(convert_int_to_uint(flags, *value, upper, target)),
            Self::UInt(value) => numeric_outcome(convert_uint_to_uint(*value, upper, target)),
            Self::Real(value) | Self::Float32(value) => {
                numeric_outcome(convert_float_to_uint(flags, *value, upper, target))
            }
            Self::String(_) | Self::Bytes(_) => {
                let text = match self {
                    Self::String(value) => value.as_utf8()?,
                    Self::Bytes(value) => std::str::from_utf8(value)?,
                    _ => unreachable!(),
                };
                let parsed = crate::convert::str_to_uint_reported(
                    text,
                    false,
                    flags.truncate_as_warning() || flags.ignore_truncate_err(),
                    diagnostics,
                );
                let bounded = numeric_outcome(convert_uint_to_uint(parsed.value, upper, target));
                // Go unsigned conversion gives a width error precedence over
                // the prefix/parser error, while preserving any warning.
                if let Some(ScalarConversionEvent::Overflow(ScalarConversionError::Overflow {
                    value,
                    target,
                })) = &bounded.event
                {
                    diagnostics.replace_error(|| {
                        ERR_OVERFLOW.generate(format!(
                            "constant {value} overflows {}",
                            crate::type_str(*target)
                        ))
                    });
                }
                Converted {
                    value: bounded.value,
                    event: prefer_event(parsed.event, bounded.event),
                }
            }
            Self::Time(value) => decimal_to_unsigned(&value.to_number(), upper, target),
            Self::Duration(value) => decimal_to_unsigned(&value.to_number(), upper, target),
            Self::Decimal(value) => decimal_to_unsigned(value, upper, target),
            Self::Enum(value, _) => numeric_outcome(convert_float_to_uint(
                flags,
                value.to_number(),
                upper,
                target,
            )),
            Self::Set(value, _) => numeric_outcome(convert_float_to_uint(
                flags,
                value.to_number(),
                upper,
                target,
            )),
            Self::BinaryLiteral(value) | Self::Bit(value) => {
                let literal = value.to_int();
                let bounded = numeric_outcome(convert_uint_to_uint(literal.value(), upper, target));
                Converted {
                    value: bounded.value,
                    event: prefer_event(
                        literal
                            .is_truncated()
                            .then_some(ScalarConversionEvent::Truncated),
                        bounded.event,
                    ),
                }
            }
            Self::Json(value) => {
                let converted = json_to_int(value, true, target, flags);
                Converted {
                    value: converted.value as u64,
                    event: converted.event,
                }
            }
            _ => {
                return Err(DatumValueError::Unsupported(
                    self.kind(),
                    "unsigned integer",
                ))
            }
        };
        match self {
            Self::String(_) | Self::Bytes(_) => {}
            Self::Int(_)
            | Self::UInt(_)
            | Self::Real(_)
            | Self::Float32(_)
            | Self::Decimal(_)
            | Self::Enum(..)
            | Self::Set(..) => {
                diagnostics.numeric_overflow(converted.event.as_ref());
            }
            _ => diagnostics.unhandled(converted.event.as_ref()),
        }
        Ok(converted)
    }

    fn convert_to_decimal_target(
        &self,
        target: &FieldType,
        diagnostics: &mut Diagnostics<'_, '_>,
    ) -> Result<Converted<Self>, DatumValueError> {
        let converted = self.to_decimal()?;
        match (self, converted.event.as_ref()) {
            (Self::String(_) | Self::Bytes(_), Some(ScalarConversionEvent::Truncated)) => {
                // Datum.ConvertTo uses MyDecimal.FromString directly, not
                // ConvertDatumToDecimal's context-dependent truncation policy.
                diagnostics.error(|| ERR_TRUNCATED.clone());
            }
            (_, event) => diagnostics.unhandled(event),
        }
        let original = converted.value;
        let mut value = original.clone();
        let mut event = converted.event;
        if target.flen() != UNSPECIFIED_LENGTH && target.decimal() != UNSPECIFIED_LENGTH {
            if target.flen() < target.decimal() {
                return Err(DatumValueError::Comparison(
                    "For float(M,D), double(M,D) or decimal(M,D), M must be >= D".to_owned(),
                ));
            }
            let rounded = value.round_to_scale(target.decimal() as i32);
            let fitted = rounded
                .fit_precision_scale(target.flen().max(0) as u32, target.decimal().max(0) as u32);
            let overflowed = fitted.is_none();
            value = fitted.unwrap_or_else(|| {
                Decimal::from_signed_literal(&format!(
                    "{}{}",
                    if rounded.is_negative() { "-" } else { "" },
                    max_decimal_text(target.flen() as usize, target.decimal() as usize)
                ))
            });
            if overflowed {
                diagnostics.error(|| decimal_target_overflow(target));
                event = event.or_else(|| Some(overflow_event(value.to_string(), target.code())));
            } else if value != original {
                diagnostics.warn(|| {
                    ERR_TRUNCATED_WRONG_VALUE
                        .generate(format!("Truncated incorrect DECIMAL value: '{original}'",))
                });
                event = event.or(Some(ScalarConversionEvent::RoundedToScale));
            }
        }
        if target.is_unsigned() && value.is_negative() {
            diagnostics.error(|| decimal_target_overflow(target));
            value = Decimal::from_int(0);
            event = event
                .filter(|event| !matches!(event, ScalarConversionEvent::RoundedToScale))
                .or_else(|| Some(overflow_event(original.to_string(), target.code())));
        }
        // Go `convertToMysqlDecimal` opens with
        // `ret.SetLength(target.GetFlen()); ret.SetFrac(target.GetDecimal())`,
        // and the row-v2 encoder hands that pair to `codec.EncodeDecimal`. The
        // declared shape decides the stored byte width, so it must survive the
        // conversion even though the value itself never learns it. Stamping is
        // skipped when the target leaves either half unspecified, because a
        // `-1` written into Go's `uint32`/`uint16` fields is garbage no real
        // column produces: every DECIMAL column carries a resolved `(M, D)`.
        if target.flen() != UNSPECIFIED_LENGTH && target.decimal() != UNSPECIFIED_LENGTH {
            value = value.with_declared_shape(target.flen(), target.decimal());
        }
        Ok(Converted {
            value: Self::new_decimal(value),
            event,
        })
    }

    /// Go `Datum.convertToMysqlTime` / `convertToMysqlTimestamp`.
    ///
    /// The three date flags are read off `flags` rather than hardcoded, because
    /// they are exactly what the SQL mode moves: Go's `Time.Check` takes
    /// `IgnoreZeroDateErr` for `NO_ZERO_DATE`, `IgnoreZeroInDate` for
    /// `NO_ZERO_IN_DATE`, and `IgnoreInvalidDateErr` for
    /// `ALLOW_INVALID_DATES`, so an all-zero value, `'2024-00-01'`, or
    /// `'2024-02-31'` either parses into a real value or fails HERE depending
    /// on the mode the statement runs under.
    ///
    /// A failure returns [`DatumValueError::IncorrectTemporal`] carrying the
    /// zero value of the target type, which is what Go returns in the datum
    /// beside the error and what the non-strict write path stores.
    fn convert_to_time_target(
        &self,
        target: &FieldType,
        flags: ConversionFlags,
        zone: &SessionTimeZone,
    ) -> Result<Converted<Self>, DatumValueError> {
        let kind = match target.code() {
            FieldTypeCode::Date => TimeType::Date,
            FieldTypeCode::Datetime => TimeType::DateTime,
            FieldTypeCode::Timestamp => TimeType::Timestamp,
            _ => unreachable!(),
        };
        let fsp = if target.decimal() == UNSPECIFIED_LENGTH {
            0
        } else {
            target.decimal()
        };
        let zero_in_date = flags.ignore_zero_in_date_err();
        let ignore_zero_date_err = flags.ignore_zero_date_err();
        let invalid_date = flags.ignore_invalid_date_err();
        // Go's fallback datum: `NewTime(ZeroCoreTime, tp, DefaultFsp)`.
        let zero = Time::new(CoreTime::default(), kind, 0).map_err(conversion_error)?;
        let wrong_value = move |_error| DatumValueError::IncorrectTemporal(zero);
        let mut event = None;
        let time = match self {
            Self::Time(value) => {
                let (converted, adjusted) = value
                    .convert_kind(kind, zero_in_date, invalid_date, zone)
                    .map_err(wrong_value)?;
                if adjusted {
                    event = Some(ScalarConversionEvent::TimestampInDSTTransition);
                }
                converted.round_frac(fsp, zone).map_err(wrong_value)?
            }
            // Go `Duration.ConvertToTime`: `gotime.Now().In(ctx.Location())`,
            // which is [`session_now`] -- the same one the YEAR arm reads.
            Self::Duration(value) => value
                .convert_to_time(session_now(zone), kind, zero_in_date, invalid_date)
                .and_then(|time| time.round_frac(fsp, zone))
                .map_err(wrong_value)?,
            Self::String(value) => {
                let parsed = parse_time(
                    value.as_utf8()?,
                    kind,
                    fsp,
                    false,
                    zero_in_date,
                    invalid_date,
                    zone,
                )
                .map_err(wrong_value)?;
                if parsed.dst_adjusted {
                    event = Some(ScalarConversionEvent::TimestampInDSTTransition);
                }
                parsed.time
            }
            Self::Bytes(value) => {
                let parsed = parse_time(
                    std::str::from_utf8(value)?,
                    kind,
                    fsp,
                    false,
                    zero_in_date,
                    invalid_date,
                    zone,
                )
                .map_err(wrong_value)?;
                if parsed.dst_adjusted {
                    event = Some(ScalarConversionEvent::TimestampInDSTTransition);
                }
                parsed.time
            }
            Self::Int(value) => {
                let parsed = parse_time_from_num(
                    *value,
                    kind,
                    fsp,
                    zero_in_date,
                    invalid_date,
                    ignore_zero_date_err,
                    zone,
                )
                .map_err(wrong_value)?;
                if parsed.dst_adjusted {
                    event = Some(ScalarConversionEvent::TimestampInDSTTransition);
                }
                parsed.time
            }
            Self::UInt(value) if *value <= i64::MAX as u64 => {
                let parsed = parse_time_from_num(
                    *value as i64,
                    kind,
                    fsp,
                    zero_in_date,
                    invalid_date,
                    ignore_zero_date_err,
                    zone,
                )
                .map_err(wrong_value)?;
                if parsed.dst_adjusted {
                    event = Some(ScalarConversionEvent::TimestampInDSTTransition);
                }
                parsed.time
            }
            Self::Decimal(value) => {
                // Datum.ConvertTo uses ParseTimeFromFloatString, not the
                // distinct ParseTimeFromDecimal helper used by numeric casts.
                // Parse at the target FSP so every discarded digit and any
                // carry across a date/DST boundary are handled in one step.
                let parsed = crate::time_parse::parse_time_with_flags(
                    &value.to_string(),
                    kind,
                    fsp,
                    true,
                    flags,
                    zone,
                )
                .map_err(wrong_value)?;
                if parsed.dst_adjusted {
                    event = Some(ScalarConversionEvent::TimestampInDSTTransition);
                }
                parsed.time
            }
            Self::Json(value) => {
                let parsed = parse_time(
                    &value.unquote()?,
                    kind,
                    fsp,
                    false,
                    zero_in_date,
                    invalid_date,
                    zone,
                )
                .map_err(wrong_value)?;
                if parsed.dst_adjusted {
                    event = Some(ScalarConversionEvent::TimestampInDSTTransition);
                }
                parsed.time
            }
            _ => return Err(DatumValueError::Unsupported(self.kind(), "time")),
        };
        Ok(Converted {
            value: Self::new_time(time),
            event,
        })
    }

    fn convert_to_duration_target(
        &self,
        target: &FieldType,
        zone: &SessionTimeZone,
    ) -> Result<Converted<Self>, DatumValueError> {
        use tidb_query_datatype::codec::native_duration_convert::NativeDurationTargetError as Error;
        let converted =
            tidb_query_datatype::codec::native_duration_convert::native_convert_to_duration_target(
                self.as_shared_json_input(),
                target.decimal(),
                zone,
            )
            .map_err(|error| match error {
                Error::InvalidUtf8(error) => DatumValueError::InvalidUtf8(error),
                Error::Unsupported => DatumValueError::Unsupported(self.kind(), "duration"),
                Error::Time(error) => conversion_error(error),
                Error::Round(error) => conversion_error(error),
                Error::Duration(error) => conversion_error(error),
                Error::JsonInvalidBinary => crate::BinaryJSONError::InvalidBinary.into(),
                Error::JsonInvalidText => crate::BinaryJSONError::InvalidText.into(),
                Error::SqlString(error) => {
                    use tidb_query_datatype::codec::native_sql_string::NativeSqlStringError;
                    let error = match error {
                        NativeSqlStringError::InvalidUtf8(error) => {
                            crate::DatumStringError::InvalidUtf8(error)
                        }
                        NativeSqlStringError::MinNotNull => {
                            crate::DatumStringError::RangeSentinel(crate::DatumKind::MinNotNull)
                        }
                        NativeSqlStringError::MaxValue => {
                            crate::DatumStringError::RangeSentinel(crate::DatumKind::MaxValue)
                        }
                    };
                    DatumValueError::Comparison(format!("duration conversion failed: {error}"))
                }
            })?;
        Ok(crate::convert::from_shared_duration_conversion(
            converted,
            |value| Self::new_duration(MySqlDuration::from_raw_parts(value.nanoseconds, value.fsp)),
        ))
    }

    fn convert_to_year(
        &self,
        flags: ConversionFlags,
        zone: &SessionTimeZone,
    ) -> Result<Converted<Self>, DatumValueError> {
        let (year, adjust_zero, event) = match self {
            Self::String(value) => year_from_text(value.as_utf8()?)?,
            Self::Bytes(value) => year_from_text(std::str::from_utf8(value)?)?,
            Self::Time(value) => (i64::from(value.core_time().year()), false, None),
            // Go `Duration.ConvertToYearFromNow` (`pkg/types/time.go`):
            //
            // ```go
            // if ctx.Flags().CastTimeToYearThroughConcat() { ... }
            // year, month, day := now.In(ctx.Location()).Date()
            // ```
            //
            // Both halves come from the STATEMENT and both were hardcoded
            // here. `now` is an INSTANT, and only projecting it into the
            // session zone picks the calendar day -- and so the YEAR -- that
            // Go picks; that projection is [`session_now`], shared with the
            // DATETIME arm so the two cannot pick different days. The flag
            // selects Go's OTHER source entirely (the time fields read as a
            // number, `00:20:12` -> 2012), so pinning it to `false` made that
            // whole branch unreachable.
            Self::Duration(value) => {
                let converted = value
                    .convert_to_year_with_event(
                        session_now(zone),
                        flags.cast_time_to_year_through_concat(),
                    )
                    .map_err(conversion_error)?;
                return Ok(map_converted(Self::Int)(converted));
            }
            Self::Json(value) => {
                let converted = crate::json_to_int64(value, false, crate::DEFAULT_STATEMENT_FLAGS);
                (converted.value, false, converted.event)
            }
            _ => {
                let converted = self.convert_to_signed(FieldTypeCode::LongLong, flags, zone)?;
                (converted.value, false, converted.event)
            }
        };
        let adjusted = crate::time_parse::adjust_year_with_event(year, adjust_zero);
        Ok(Converted {
            value: Self::Int(adjusted.value),
            event: prefer_event(event, adjusted.event),
        })
    }

    fn convert_to_enum(
        &self,
        target: &FieldType,
        flags: ConversionFlags,
    ) -> Result<Converted<Self>, DatumValueError> {
        let parsed = target.with_elems_visible(|elements| match self {
            Self::String(value) => {
                parse_enum(elements, value.bytes(), target.runtime_collator()).map_err(|_| ())
            }
            Self::Bytes(value) => {
                parse_enum(elements, value.as_slice(), target.runtime_collator()).map_err(|_| ())
            }
            Self::BinaryLiteral(value) => {
                parse_enum(elements, value.as_bytes(), target.runtime_collator()).map_err(|_| ())
            }
            Self::Enum(value, _) if value.value() == 0 => Ok(crate::MysqlEnum::new("", 0)),
            Self::Enum(value, _) => {
                parse_enum(elements, value.name(), target.runtime_collator()).map_err(|_| ())
            }
            Self::Set(value, _) => {
                parse_enum(elements, value.name(), target.runtime_collator()).map_err(|_| ())
            }
            // Go wraps `convertToUint`'s own failure in `ErrTruncated` too
            // (`datum.go`'s "convert to MySQL enum failed: " arm), so it
            // reaches the caller as the same truncation event, not an error.
            _ => match self.convert_to_unsigned(FieldTypeCode::LongLong, flags) {
                Ok(number) => parse_enum_value(elements, number.value).map_err(|_| ()),
                Err(_) => Err(()),
            },
        });
        // Go `convertToMysqlEnum` calls `SetMysqlEnum` UNCONDITIONALLY and
        // returns the value beside `ErrTruncated`: a failed parse stores the
        // zero enum (`Enum{Name: "", Value: 0}`) and only raises an event, so
        // a non-strict write keeps the row and warns 1265.
        Ok(match parsed {
            Ok(value) => exact(Self::new_enum(value, target.collation())),
            Err(_) => Converted {
                value: Self::new_enum(crate::MysqlEnum::default(), target.collation()),
                event: Some(ScalarConversionEvent::Truncated),
            },
        })
    }

    fn convert_to_set(
        &self,
        target: &FieldType,
        flags: ConversionFlags,
    ) -> Result<Converted<Self>, DatumValueError> {
        // Go keeps this as a hard invalid conversion instead of wrapping it
        // in the SET truncation event used by every other failed source.
        if matches!(self, Self::VectorFloat32(_)) {
            return Err(DatumValueError::Unsupported(self.kind(), "set"));
        }
        // `convertToMysqlSet` leaves the zero SET beside a failed numeric
        // `convertToUint` and wraps that failure as `ErrTruncated`.  Keep
        // that event instead of letting a saturated numeric value of zero
        // look like the valid SET zero (notably `INSERT ... VALUES (-1)`).
        let mut numeric_conversion_failed = false;
        let parsed = target.with_elems_visible(|elements| match self {
            Self::String(value) => {
                parse_set(elements, value.bytes(), target.runtime_collator()).map_err(|_| ())
            }
            Self::Bytes(value) => {
                parse_set(elements, value.as_slice(), target.runtime_collator()).map_err(|_| ())
            }
            Self::BinaryLiteral(value) => {
                parse_set(elements, value.as_bytes(), target.runtime_collator()).map_err(|_| ())
            }
            Self::Enum(value, _) => {
                parse_set(elements, value.name(), target.runtime_collator()).map_err(|_| ())
            }
            Self::Set(value, _) => {
                parse_set(elements, value.name(), target.runtime_collator()).map_err(|_| ())
            }
            Self::VectorFloat32(_) => unreachable!("vector returned before borrowing elements"),
            _ => match self.convert_to_unsigned(FieldTypeCode::LongLong, flags) {
                Ok(number) if number.event.is_none() => {
                    parse_set_value(elements, number.value).map_err(|_| ())
                }
                Ok(_) | Err(_) => {
                    numeric_conversion_failed = true;
                    Err(())
                }
            },
        });
        // Go `convertToMysqlSet` wraps EVERY failure in `ErrTruncated` and
        // still calls `SetMysqlSet`, so the zero set is stored and the caller
        // decides between a 1265 warning and a strict error.
        Ok(if numeric_conversion_failed {
            Converted {
                value: Self::new_set(crate::MysqlSet::default(), target.collation()),
                event: Some(ScalarConversionEvent::Truncated),
            }
        } else {
            match parsed {
                Ok(value) => exact(Self::new_set(value, target.collation())),
                Err(()) => Converted {
                    value: Self::new_set(crate::MysqlSet::default(), target.collation()),
                    event: Some(ScalarConversionEvent::Truncated),
                },
            }
        })
    }

    fn convert_to_bit(
        &self,
        target: &FieldType,
        flags: ConversionFlags,
    ) -> Result<Converted<Self>, DatumValueError> {
        let flen = target.flen();
        if !(1..=64).contains(&flen) {
            return Err(DatumValueError::Comparison(format!(
                "Data Too Long, field len {flen}"
            )));
        }
        let mut event = None;
        let mut value = match self {
            Self::String(value) => value_to_literal_uint(value.bytes(), &mut event),
            Self::Bytes(value) => value_to_literal_uint(value, &mut event),
            Self::Int(value) => *value as u64,
            _ => {
                let converted = self.convert_to_unsigned(target.code(), flags)?;
                event = converted.event;
                converted.value
            }
        };
        if flen < 64 {
            let upper = (1_u64 << flen) - 1;
            if value > upper {
                value = upper;
                event = Some(ScalarConversionEvent::Truncated);
            }
        }
        let width = BinaryLiteralWidth::try_from(((flen + 7) / 8) as u8)
            .map_err(|error| DatumValueError::Comparison(error.to_string()))?;
        Ok(Converted {
            value: Self::new_mysql_bit(BinaryLiteral::from_uint(value, Some(width))),
            event,
        })
    }

    fn convert_to_json_target(&self) -> Result<Converted<Self>, DatumValueError> {
        use tidb_query_datatype::codec::native_mysql_json::{
            native_convert_to_json_target, NativeDatumJsonError as DatumError,
            NativeJsonTargetError as Error,
        };
        let (type_code, value) = native_convert_to_json_target(self.as_shared_json_input())
            .map_err(|error| match error {
                Error::CannotCreateJsonFromBinary => DatumValueError::Comparison(
                    "Cannot create a JSON value from a string with CHARACTER SET 'binary'"
                        .to_owned(),
                ),
                Error::Parse(error) => crate::binary_json::native_json_parse_error(error).into(),
                Error::Datum(DatumError::InvalidUtf8(error)) => DatumValueError::InvalidUtf8(error),
                Error::Datum(DatumError::Unsupported) => {
                    DatumValueError::Unsupported(self.kind(), "json")
                }
                Error::Datum(DatumError::Construct(error)) => {
                    crate::binary_json::native_json_construct_error(error).into()
                }
            })?;
        Ok(exact(Self::new_json(BinaryJSON::from_encoded_parts(
            type_code, value,
        ))))
    }

    fn convert_to_vector(&self, target: &FieldType) -> Result<Converted<Self>, DatumValueError> {
        use tidb_query_datatype::codec::native_vector_convert::{
            native_convert_to_vector, NativeVectorConvertError as Error,
            NativeVectorConvertInput as Input,
        };
        let input = match self {
            Self::VectorFloat32(value) => Input::Vector(value),
            Self::String(value) => Input::String(value.bytes()),
            Self::Bytes(value) => Input::Bytes(value),
            _ => Input::Other,
        };
        let value =
            native_convert_to_vector(input, target.flen()).map_err(|error| match error {
                Error::Unsupported => DatumValueError::Unsupported(self.kind(), "vector float32"),
                Error::InvalidUtf8(error) => DatumValueError::InvalidUtf8(error),
                Error::Vector(error) => DatumValueError::Comparison(error.to_string()),
            })?;
        Ok(exact(Self::new_vector_float32(value)))
    }
}

/// Source `ProduceFloatWithSpecifiedTp`.
pub fn produce_float_with_type(value: f64, target: &FieldType) -> Converted<f64> {
    produce_float_reported(value, target, &mut Diagnostics::new(None))
}

fn produce_float_reported(
    value: f64,
    target: &FieldType,
    diagnostics: &mut Diagnostics<'_, '_>,
) -> Converted<f64> {
    let converted = tidb_query_datatype::codec::native_float_convert::native_produce_float(
        value,
        target.code().as_shared_type_name_code(),
        target.flen(),
        target.decimal(),
        target.is_unsigned(),
        |diagnostic| diagnostics.error(|| ERR_OVERFLOW.generate(diagnostic.message())),
    );
    Converted {
        value: converted.value,
        event: converted
            .overflow
            .map(|value| overflow_event(value, target.code())),
    }
}

/// Source `ProduceStrWithSpecifiedTp`, retaining truncation as an event.
pub fn produce_string_with_type(
    value: Vec<u8>,
    target: &FieldType,
    pad_zero: bool,
) -> Result<Converted<Vec<u8>>, DatumValueError> {
    produce_string_reported(value, target, pad_zero, &mut Diagnostics::new(None))
}

fn produce_string_reported(
    value: Vec<u8>,
    target: &FieldType,
    pad_zero: bool,
    diagnostics: &mut Diagnostics<'_, '_>,
) -> Result<Converted<Vec<u8>>, DatumValueError> {
    let converted = tidb_query_datatype::codec::native_string_convert::native_produce_string(
        value,
        target.flen(),
        target.code().as_shared_string_type(),
        target.charset() == Charset::Binary,
        pad_zero,
        diagnostics.enabled(),
        |diagnostic| {
            if diagnostic.is_warning() {
                diagnostics.warn(|| ERR_TRUNCATED.generate(diagnostic.message()));
            } else {
                diagnostics.truncate(|| ERR_DATA_TOO_LONG.generate(diagnostic.message()));
            }
        },
    );
    Ok(Converted {
        value: converted.value,
        event: converted
            .truncated
            .then_some(ScalarConversionEvent::Truncated),
    })
}

fn decimal_target_overflow(target: &FieldType) -> tidb_error::terror::TerrorError {
    ERR_OVERFLOW.generate(format!(
        "DECIMAL value is out of range in '({}, {})'",
        target.flen(),
        target.decimal(),
    ))
}

/// Source `GetMaxValue`.
pub fn get_max_value(target: &FieldType) -> Datum {
    project_bound(target, true)
}

/// Source `GetMinValue`.
pub fn get_min_value(target: &FieldType) -> Datum {
    project_bound(target, false)
}

fn project_bound(target: &FieldType, maximum: bool) -> Datum {
    use tidb_query_datatype::codec::native_eval_type::{
        native_type_bound, NativeBoundTemporalKind, NativeBoundValue,
    };
    match native_type_bound(
        target.code().as_shared_type_name_code(),
        target.flen(),
        target.decimal(),
        target.is_unsigned(),
        maximum,
    ) {
        NativeBoundValue::Null => Datum::Null,
        NativeBoundValue::Int(value) => Datum::Int(value),
        NativeBoundValue::UInt(value) => Datum::UInt(value),
        NativeBoundValue::Real { value, float32 } => {
            if float32 {
                Datum::Float32(value)
            } else {
                Datum::Real(value)
            }
        }
        NativeBoundValue::StringByte(value) => {
            Datum::new_collation_string([value], target.collation())
        }
        NativeBoundValue::DecimalText(text) => {
            Datum::new_decimal(Decimal::from_signed_literal(&text))
        }
        NativeBoundValue::DurationNanos(nanos) => Datum::new_duration(
            MySqlDuration::from_nanoseconds(nanos, 0).expect("SDK bound duration is valid"),
        ),
        NativeBoundValue::Temporal {
            year,
            month,
            day,
            hour,
            minute,
            second,
            microsecond,
            kind,
        } => {
            let kind = match kind {
                NativeBoundTemporalKind::Date => TimeType::Date,
                NativeBoundTemporalKind::DateTime => TimeType::DateTime,
                NativeBoundTemporalKind::Timestamp => TimeType::Timestamp,
            };
            Datum::new_time(
                Time::new(
                    CoreTime::from_date(
                        year as u16,
                        month as u8,
                        day as u8,
                        hour as u8,
                        minute as u8,
                        second as u8,
                        microsecond as u32,
                    ),
                    kind,
                    0,
                )
                .expect("SDK bound temporal value is valid"),
            )
        }
    }
}

/// Source `ChangeReverseResultByUpperLowerBound`.
pub fn change_reverse_result_by_bound(
    target: &FieldType,
    result: &Datum,
    rounding: RoundingType,
    flags: ConversionFlags,
) -> Result<Converted<Datum>, DatumValueError> {
    use tidb_query_datatype::codec::native_reverse_bound::{
        native_reverse_finish, native_reverse_prepare, NativeReverseFinish, NativeReversePrepare,
    };
    let mut converted = result.convert_to(target, flags)?;
    match native_reverse_prepare(matches!(
        converted.event,
        Some(ScalarConversionEvent::Overflow(_))
    )) {
        NativeReversePrepare::ReturnConverted => return Ok(converted),
        NativeReversePrepare::CompareSourceBound => {}
    }
    let source_bound = source_kind_bound(result, rounding);
    let equal_source_bound = converted
        .value
        .compare(
            &source_bound,
            source_bound.collation().unwrap_or(Collation::Binary),
        )?
        .is_eq();
    match native_reverse_finish(
        equal_source_bound,
        matches!(rounding, RoundingType::Ceiling),
    ) {
        NativeReverseFinish::ReplaceTargetBound => {
            converted.value = project_bound(target, matches!(rounding, RoundingType::Ceiling));
        }
        NativeReverseFinish::Increment => {
            converted.value = increment_for_reverse(converted.value, target);
        }
        NativeReverseFinish::Keep => {}
    }
    Ok(converted)
}

fn source_kind_bound(source: &Datum, rounding: RoundingType) -> Datum {
    use tidb_query_datatype::codec::native_reverse_bound::{
        native_reverse_source_bound, NativeReverseSourceBound, NativeReverseSourceKind,
    };
    let kind = match source {
        Datum::Int(_) => NativeReverseSourceKind::Int,
        Datum::UInt(_) => NativeReverseSourceKind::UInt,
        Datum::Float32(_) => NativeReverseSourceKind::Float32,
        Datum::Real(_) => NativeReverseSourceKind::Real,
        Datum::Decimal(value) => NativeReverseSourceKind::Decimal {
            digits: value.coefficient_digits().len(),
            scale: value.scale() as usize,
        },
        _ => NativeReverseSourceKind::Other,
    };
    match native_reverse_source_bound(kind, matches!(rounding, RoundingType::Ceiling)) {
        NativeReverseSourceBound::Int(value) => Datum::Int(value),
        NativeReverseSourceBound::UInt(value) => Datum::UInt(value),
        NativeReverseSourceBound::Real { value, float32 } => {
            if float32 {
                Datum::Float32(value)
            } else {
                Datum::Real(value)
            }
        }
        NativeReverseSourceBound::DecimalText(text) => {
            Datum::new_decimal(Decimal::from_signed_literal(&text))
        }
        NativeReverseSourceBound::Maximum => Datum::MaxValue,
        NativeReverseSourceBound::MinimumNotNull => Datum::MinNotNull,
    }
}

fn increment_for_reverse(value: Datum, target: &FieldType) -> Datum {
    use tidb_query_datatype::codec::native_reverse_bound::{
        native_reverse_increment, NativeReverseIncrement, NativeReverseIncrementInput,
    };
    let input = match &value {
        Datum::Int(value) => NativeReverseIncrementInput::Int(*value),
        Datum::UInt(value) => NativeReverseIncrementInput::UInt(*value),
        Datum::Float32(value) => NativeReverseIncrementInput::Float32(*value),
        Datum::Real(value) => NativeReverseIncrementInput::Real(*value),
        Datum::Decimal(decimal) => {
            let maximum = get_max_value(target);
            let at_target_max = maximum
                .compare(&Datum::new_decimal(decimal.clone()), Collation::Binary)
                .is_ok_and(|ordering| ordering.is_eq());
            NativeReverseIncrementInput::Decimal { at_target_max }
        }
        _ => NativeReverseIncrementInput::Other,
    };
    match native_reverse_increment(input, target.code().as_shared_type_name_code()) {
        NativeReverseIncrement::Int(value) => Datum::Int(value),
        NativeReverseIncrement::UInt(value) => Datum::UInt(value),
        NativeReverseIncrement::Float32(value) => Datum::Float32(value),
        NativeReverseIncrement::Real(value) => Datum::Real(value),
        NativeReverseIncrement::IncrementDecimal => {
            let Datum::Decimal(value) = value else {
                unreachable!("SDK decimal increment requires an actual decimal datum");
            };
            Datum::new_decimal(value.add(&Decimal::from_int(1)))
        }
        NativeReverseIncrement::Keep => value,
    }
}

fn decimal_to_signed(
    value: &Decimal,
    lower: i64,
    upper: i64,
    target: FieldTypeCode,
) -> Converted<i64> {
    let rounded = value.round_to_i64();
    let raw = rounded.unwrap_or_else(|| value.round_to_i64_saturating());
    let bounded = numeric_outcome(convert_int_to_int(raw, lower, upper, target));
    Converted {
        value: bounded.value,
        event: if rounded.is_none() {
            Some(overflow_event(value.to_string(), target))
        } else {
            bounded.event
        },
    }
}

fn decimal_to_unsigned(value: &Decimal, upper: u64, target: FieldTypeCode) -> Converted<u64> {
    let converted = convert_decimal_to_uint(value, upper, target);
    numeric_outcome(converted)
}

/// Go `StrToDuration(ctx, str, fsp)` (pkg/types/convert.go), whose `ctx`
/// carries the session location: a 12-or-more-digit literal is parsed as a
/// DATETIME first, and rounding it to `fsp` goes through the same
/// zone-sensitive `RoundFrac` as [`Datum::convert_to_signed`]'s time arm.
/// Measured against Go at `fsp=0`, `"20110313015959.999999"` yields
/// `2011-03-13 02:00:00` under UTC but `2011-03-13 03:00:00` under
/// America/Los_Angeles, and `"20111106015959.999999"` yields
/// `2011-11-06 02:00:00` under UTC but `2011-11-06 01:00:00` there.
/// Go's `gotime.Now()` projected with `.In(ctx.Location())`: the one place
/// this file decides what "now" means.
///
/// The two Duration arms -- `ConvertToTime` and `ConvertToYearFromNow` --
/// both read it, and Go hands both the SAME `ctx`, so it is one decision
/// rather than two that can disagree. `Utc::now()` here is the instant, not a
/// zone choice; `with_timezone` is Go's `In`.
fn session_now(zone: &SessionTimeZone) -> chrono::DateTime<SessionTimeZone> {
    Utc::now().with_timezone(zone)
}

#[cfg(test)]
#[test]
fn duration_target_facade_keeps_null_raw_storage_events_and_error_domains() {
    let target = FieldType::new(FieldTypeCode::Duration);
    let bad_fsp = target.clone().with_decimal(-2);
    let convert = |value: &Datum, field: &FieldType| {
        value.convert_to_in(field, ConversionFlags::default(), &SessionTimeZone::utc())
    };
    assert_eq!(
        convert(&Datum::Null, &bad_fsp).unwrap(),
        Converted {
            value: Datum::Null,
            event: None
        }
    );
    let raw = Datum::Duration(MySqlDuration::from_raw_parts(123, 6));
    assert_eq!(
        convert(&raw, &target.clone().with_decimal(6)).unwrap(),
        Converted {
            value: raw,
            event: None
        }
    );
    let rounded = convert(
        &Datum::Duration(MySqlDuration::from_raw_parts(-1_500_000, 6)),
        &target.clone().with_decimal(3),
    )
    .unwrap();
    assert_eq!(
        rounded,
        Converted {
            value: Datum::Duration(MySqlDuration::from_raw_parts(-1_000_000, 3)),
            event: None
        }
    );
    assert_eq!(
        convert(
            &Datum::Duration(MySqlDuration::from_raw_parts(1, 6)),
            &bad_fsp
        ),
        Err(DatumValueError::Comparison("Invalid fsp -2".into()))
    );
    assert_eq!(
        convert(
            &Datum::Duration(MySqlDuration::from_raw_parts(i64::MAX, 6)),
            &target
        ),
        Err(DatumValueError::Comparison(
            "rounded duration is out of range".into()
        ))
    );

    let calendar = Time::new(
        CoreTime::from_date(2024, 2, 3, 1, 2, 3, 500_000),
        TimeType::DateTime,
        6,
    )
    .unwrap();
    assert_eq!(
        convert(&Datum::Time(calendar), &target).unwrap(),
        Converted {
            value: Datum::Duration(MySqlDuration::from_raw_parts(3_724_000_000_000, 0)),
            event: None,
        }
    );
    for value in [
        Datum::new_string("20190412123456"),
        Datum::Int(20_190_412_123_456),
        Datum::Float32(123456.0),
        Datum::Json(BinaryJSON::parse(r#""12:34:56""#).unwrap()),
    ] {
        assert_eq!(
            convert(&value, &target).unwrap(),
            Converted {
                value: Datum::Duration(MySqlDuration::from_raw_parts(45_296_000_000_000, 0)),
                event: None,
            }
        );
    }
    let saturated = convert(&Datum::new_string(" 839:00:00 "), &target).unwrap();
    assert_eq!(
        saturated.value,
        Datum::Duration(MySqlDuration::from_raw_parts(3_020_399_000_000_000, 0))
    );
    assert_eq!(
        saturated.event,
        Some(ScalarConversionEvent::Overflow(
            ScalarConversionError::Overflow {
                value: "839:00:00".into(),
                target: FieldTypeCode::Duration,
            }
        ))
    );
    assert!(matches!(
        convert(&Datum::new_bytes(vec![0xff]), &bad_fsp),
        Err(DatumValueError::InvalidUtf8(_))
    ));
    assert_eq!(
        convert(&Datum::MinNotNull, &bad_fsp),
        Err(DatumValueError::Unsupported(
            crate::DatumKind::MinNotNull,
            "duration"
        ))
    );
    assert_eq!(
        convert(
            &Datum::Json(BinaryJSON::from_encoded_parts(
                crate::JSON_TYPE_CODE_STRING,
                vec![1, 0xff]
            )),
            &target
        ),
        Err(DatumValueError::Json(crate::BinaryJSONError::InvalidBinary))
    );
    assert_eq!(
        convert(
            &Datum::Json(BinaryJSON::from_encoded_parts(
                crate::JSON_TYPE_CODE_STRING,
                vec![3, b'"', b'\\', b'"']
            )),
            &target
        ),
        Err(DatumValueError::Json(crate::BinaryJSONError::InvalidText))
    );
    // Unknown tags display as empty text; duration parsing then rejects the
    // empty input as InvalidFormat, mapped to the original Comparison message.
    let malformed_json = BinaryJSON::from_encoded_parts(255, Vec::new());
    assert_eq!(malformed_json.unquote().unwrap(), "");
    let malformed = Datum::Json(malformed_json);
    assert_eq!(
        convert(&malformed, &target),
        Err(DatumValueError::Comparison(
            "invalid duration format".into()
        ))
    );
    // A root nonfinite double, unlike an unknown tag, returns fmt::Error from
    // JSON Display and retains to_string's formatting-error panic.
    let nonfinite_json = BinaryJSON::from_encoded_parts(
        crate::JSON_TYPE_CODE_FLOAT64,
        f64::INFINITY.to_le_bytes().to_vec(),
    );
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| nonfinite_json.unquote()))
            .is_err()
    );
    let nonfinite = Datum::Json(nonfinite_json);
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| convert(
            &nonfinite, &target
        )))
        .is_err()
    );
}

fn year_from_text(
    text: &str,
) -> Result<(i64, bool, Option<ScalarConversionEvent>), DatumValueError> {
    let trimmed = text.trim();
    let mut converted = crate::str_to_int(trimmed, false);
    // Go `ConvertToMysqlYear` returns the zero YEAR beside any `StrToInt`
    // error. In particular, an overflowing decimal string must not continue
    // through `AdjustYear` with `i64::MAX`, which would turn the error-side
    // value into the upper YEAR bound (2155).
    if matches!(
        converted.event.as_ref(),
        Some(ScalarConversionEvent::Overflow(_))
    ) {
        converted.value = 0;
    }
    let adjust_zero = text.len() != 4 && converted.value == 0 && trimmed.starts_with('0');
    Ok((converted.value, adjust_zero, converted.event))
}

fn value_to_literal_uint(bytes: &[u8], event: &mut Option<ScalarConversionEvent>) -> u64 {
    let literal = BinaryLiteral::from(bytes);
    let outcome = literal.to_int();
    if outcome.is_truncated() {
        *event = Some(ScalarConversionEvent::Truncated);
    }
    outcome.value()
}

fn max_decimal_text(flen: usize, scale: usize) -> String {
    tidb_query_datatype::codec::native_decimal_convert::native_bound_decimal_text(
        flen as i64,
        scale as i64,
        true,
    )
}

fn numeric_outcome<T>(result: Result<T, (T, ScalarConversionError)>) -> Converted<T> {
    match result {
        Ok(value) => exact(value),
        Err((value, error)) => Converted {
            value,
            event: Some(ScalarConversionEvent::Overflow(error)),
        },
    }
}

fn exact<T>(value: T) -> Converted<T> {
    Converted { value, event: None }
}

fn map_converted<T, U>(map: impl FnOnce(T) -> U) -> impl FnOnce(Converted<T>) -> Converted<U> {
    move |converted| Converted {
        value: map(converted.value),
        event: converted.event,
    }
}

fn prefer_event(
    first: Option<ScalarConversionEvent>,
    second: Option<ScalarConversionEvent>,
) -> Option<ScalarConversionEvent> {
    second.or(first)
}

fn numeric_conversion_event(
    parsed: Option<ScalarConversionEvent>,
    bounded: Option<ScalarConversionEvent>,
    flags: ConversionFlags,
) -> Option<ScalarConversionEvent> {
    if matches!(parsed, Some(ScalarConversionEvent::Truncated))
        && (flags.truncate_as_warning() || flags.ignore_truncate_err())
    {
        bounded.or(parsed)
    } else {
        parsed.or(bounded)
    }
}

fn overflow_event(value: String, target: FieldTypeCode) -> ScalarConversionEvent {
    ScalarConversionEvent::Overflow(ScalarConversionError::Overflow { value, target })
}

fn conversion_error(error: impl std::fmt::Display) -> DatumValueError {
    DatumValueError::Comparison(error.to_string())
}

const fn field_target_name(code: FieldTypeCode) -> &'static str {
    match code {
        FieldTypeCode::Unspecified => "unspecified",
        FieldTypeCode::NewDate => "new date",
        FieldTypeCode::Geometry => "geometry",
        FieldTypeCode::Unknown(_) => "unknown",
        _ => "field type",
    }
}

#[cfg(test)]
#[path = "datum_convert_go_tests.rs"]
mod go_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{parse_enum_value, parse_set_value, FieldTypeFlags};

    #[test]
    fn decimal_temporal_conversion_uses_float_string_parser() {
        // TestDecimalTemporalConversionOracle: 96 source conversions.
        for (zone, daylight) in [
            (SessionTimeZone::utc(), false),
            (
                SessionTimeZone::Named(chrono_tz::America::Los_Angeles),
                true,
            ),
        ] {
            for code in [
                FieldTypeCode::Date,
                FieldTypeCode::Datetime,
                FieldTypeCode::Timestamp,
            ] {
                for fsp in [0, 6] {
                    for (text, expected) in [
                        ("0.0", Some("0000-00-00 00:00:00")),
                        ("0.01", Some("0000-00-00 00:00:00")),
                        ("0.1", None),
                        ("20170118.123", Some("2017-01-18 00:00:00")),
                        (
                            "20110313015959.9",
                            Some(if fsp == 6 {
                                "2011-03-13 01:59:59.900000"
                            } else if daylight {
                                "2011-03-13 03:00:00"
                            } else {
                                "2011-03-13 02:00:00"
                            }),
                        ),
                        (
                            "20111106015959.9",
                            Some(if fsp == 6 {
                                "2011-11-06 01:59:59.900000"
                            } else if daylight {
                                "2011-11-06 01:00:00"
                            } else {
                                "2011-11-06 02:00:00"
                            }),
                        ),
                        ("20240809235959.9999999", Some("2024-08-10 00:00:00")),
                        ("201705051315111.22", None),
                    ] {
                        let input = Datum::Decimal(Decimal::parse_mysql(text).0);
                        let target = FieldType::new(code).with_decimal(fsp);
                        let actual = input.convert_to_in(&target, crate::STRICT_FLAGS, &zone);
                        let case = format!("{zone:?} {code:?} fsp={fsp} input={text}");
                        if let Some(expected) = expected {
                            let actual = actual.unwrap_or_else(|error| panic!("{case}: {error:?}"));
                            assert!(actual.event.is_none(), "{case}");
                            let expected = if code == FieldTypeCode::Date {
                                expected[..10].to_string()
                            } else if fsp == 6
                                && !text.starts_with("0.0")
                                && !expected.contains('.')
                            {
                                format!("{expected}.000000")
                            } else {
                                expected.to_string()
                            };
                            assert_eq!(actual.value.sql_string().unwrap(), expected, "{case}");
                        } else {
                            assert!(actual.is_err(), "{case}: {actual:?}");
                        }
                    }
                }
            }
        }
    }

    /// Casting a `TIME` to `YEAR` reads BOTH statement inputs Go reads:
    /// `ctx.Flags().CastTimeToYearThroughConcat()` and `ctx.Location()`.
    ///
    /// Go `Duration.ConvertToYearFromNow` (`pkg/types/time.go`):
    ///
    /// ```go
    /// if ctx.Flags().CastTimeToYearThroughConcat() {
    ///     dur, _ := d.RoundFrac(DefaultFsp, ctx.Location())
    ///     ival, _ := dur.ToNumber().ToInt()
    ///     return AdjustYear(ival, false)
    /// }
    /// year, month, day := now.In(ctx.Location()).Date()
    /// ```
    ///
    /// The flag was hardcoded `false` here, which made the concat branch --
    /// Go's only source for `MODIFY COLUMN ... YEAR` reorg
    /// (`pkg/ddl/reorg.go`'s `WithCastTimeToYearThroughConcat(true)`) --
    /// unreachable whatever the statement said. `00:20:12` is
    /// `pkg/types/time_test.go`'s own row: it concatenates to `2012`, a year
    /// the calendar branch could not produce.
    ///
    /// The other half, `now.In(ctx.Location())`, is [`session_now`] -- the
    /// one place this file decides what "now" means, read by the
    /// Duration->DATETIME arm too -- and is pinned by the test above rather
    /// than here: its effect on the YEAR is only visible on New Year's Eve,
    /// but its effect on the DATE is visible every second of every day.
    /// Go's `now.In(ctx.Location())` picks the session's CALENDAR DAY, and
    /// two zones 25 hours apart are never on the same one -- whatever the
    /// instant, whatever the day this runs. That is what makes the zone half
    /// of the Duration conversions testable without waiting for midnight:
    /// hardcoding `Utc` collapses both sides onto the UTC date.
    #[test]
    fn session_now_lands_on_the_zones_own_calendar_day() {
        let at = |name: &str, offset_secs: i32| {
            session_now(&SessionTimeZone::Fixed {
                name: name.to_owned(),
                offset_secs,
            })
            .date_naive()
        };
        assert_ne!(at("+14:00", 14 * 3600), at("-11:00", -11 * 3600));
    }

    #[test]
    fn a_time_to_year_cast_reads_the_statements_concat_flag() {
        let year = FieldType::new(FieldTypeCode::Year);
        let time = Datum::new_duration(MySqlDuration::new(0, 20, 12, 0, 0).unwrap());
        let zone = SessionTimeZone::Fixed {
            name: "+05:30".to_owned(),
            offset_secs: 5 * 3600 + 1800,
        };

        assert_eq!(
            time.convert_to_in(
                &year,
                crate::DEFAULT_STATEMENT_FLAGS.with_cast_time_to_year_through_concat(true),
                &zone,
            )
            .unwrap()
            .value,
            Datum::Int(2012)
        );

        // Without the flag Go takes TODAY's year in the session zone, so the
        // assertion is the branch, not a literal: a fixed year would be a
        // test that expires.
        let calendar = time
            .convert_to_in(&year, crate::DEFAULT_STATEMENT_FLAGS, &zone)
            .unwrap()
            .value;
        assert_eq!(
            calendar,
            Datum::Int(i64::from(
                chrono::Datelike::year(&session_now(&zone)).unsigned_abs()
            )),
            "the calendar branch reads the session zone's own date"
        );
        assert_ne!(calendar, Datum::Int(2012));
    }

    #[test]
    fn duration_year_cast_preserves_clamped_value_and_overflow_event() {
        let year = FieldType::new(FieldTypeCode::Year);
        let duration = Datum::new_duration(MySqlDuration::new(200, 0, 0, 0, 0).unwrap());
        let converted = duration
            .convert_to(
                &year,
                crate::DEFAULT_STATEMENT_FLAGS.with_cast_time_to_year_through_concat(true),
            )
            .unwrap();

        assert_eq!(converted.value, Datum::Int(2155));
        assert_eq!(
            converted.event,
            Some(ScalarConversionEvent::Overflow(
                ScalarConversionError::Overflow {
                    value: "2000000".to_owned(),
                    target: FieldTypeCode::Year,
                }
            ))
        );
    }

    #[test]
    fn overflowing_year_text_returns_zero_beside_the_error() {
        let converted = Datum::new_string("99999999999999999999999999999999999")
            .convert_to(
                &FieldType::new(FieldTypeCode::Year),
                crate::DEFAULT_STATEMENT_FLAGS.with_ignore_truncate_err(true),
            )
            .unwrap();

        assert_eq!(converted.value, Datum::Int(0));
        assert!(matches!(
            converted.event,
            Some(ScalarConversionEvent::Overflow(_))
        ));
    }

    #[test]
    fn contextual_legacy_numeric_events_preserve_first_fatal_error() {
        let source = Datum::new_string("128tail");
        let float = FieldType::new(FieldTypeCode::Double)
            .with_flen(3)
            .with_decimal(1);
        assert_eq!(
            source
                .convert_to(&float, crate::STRICT_FLAGS)
                .unwrap()
                .event,
            Some(ScalarConversionEvent::Truncated)
        );
    }

    #[test]
    fn contextual_legacy_decimal_events_preserve_first_fatal_error() {
        let source = Datum::new_string("128tail");
        let decimal = FieldType::new(FieldTypeCode::NewDecimal)
            .with_flen(3)
            .with_decimal(1);
        for flags in [
            crate::STRICT_FLAGS,
            crate::STRICT_FLAGS.with_truncate_as_warning(true),
            crate::STRICT_FLAGS.with_ignore_truncate_err(true),
        ] {
            assert_eq!(
                source.convert_to(&decimal, flags).unwrap().event,
                Some(ScalarConversionEvent::Truncated)
            );
        }
        let unsigned = FieldType::new(FieldTypeCode::NewDecimal)
            .with_flen(10)
            .with_decimal(2)
            .with_added_flags(FieldTypeFlags::UNSIGNED);
        let converted = Datum::new_string("-12.345")
            .convert_to(&unsigned, crate::STRICT_FLAGS)
            .unwrap();
        assert!(
            matches!(converted.event, Some(ScalarConversionEvent::Overflow(_))),
            "a rounding warning must not hide a later fatal unsigned overflow"
        );
    }

    #[test]
    fn contextual_string_conversion_preserves_binary_and_utf8_boundaries() {
        let binary = FieldType::new(FieldTypeCode::String)
            .with_flen(2)
            .with_collation(Collation::Binary);
        let converted = Datum::new_string("ab  ")
            .convert_to(&binary, crate::STRICT_FLAGS)
            .unwrap();
        assert_eq!(converted.value.as_raw_bytes(), Some(&b"ab"[..]));
        assert_eq!(converted.event, Some(ScalarConversionEvent::Truncated));

        let text = FieldType::new(FieldTypeCode::Blob)
            .with_flen(3)
            .with_collation(Collation::Utf8Mb4Bin);
        let converted = Datum::new_string("a界z")
            .convert_to(&text, crate::STRICT_FLAGS)
            .unwrap();
        assert_eq!(converted.value.as_raw_bytes(), Some(&b"a"[..]));

        // ProduceStrWithSpecifiedTp checks only the final rune, preserving
        // earlier invalid bytes. A valid U+FFFD is not an invalid one-byte rune.
        for (input, limit, expected) in [
            (&b"abc"[..], 0, &b""[..]),
            ("界z".as_bytes(), 1, &b""[..]),
            ("界z".as_bytes(), 2, &b""[..]),
            ("界z".as_bytes(), 3, "界".as_bytes()),
            ("a😀z".as_bytes(), 4, &b"a"[..]),
            ("a😀z".as_bytes(), 5, "a😀".as_bytes()),
            ("�z".as_bytes(), 3, "�".as_bytes()),
            (&b"\xffa\xffz"[..], 3, &b"\xffa"[..]),
            (&b"\xff\xffz"[..], 2, &b""[..]),
        ] {
            let target = text.clone().with_flen(limit);
            let converted = produce_string_with_type(input.to_vec(), &target, false).unwrap();
            assert_eq!(converted.value, expected, "{input:?} / {limit}");
            assert_eq!(converted.event, Some(ScalarConversionEvent::Truncated));
        }
    }

    #[test]
    fn source_convert_to_integer_float_string_decimal_rows() {
        let signed_tiny = FieldType::new(FieldTypeCode::Tiny);
        let unsigned_tiny =
            FieldType::new(FieldTypeCode::Tiny).with_added_flags(FieldTypeFlags::UNSIGNED);
        assert_eq!(
            Datum::Int(128)
                .convert_to(&signed_tiny, crate::DEFAULT_STATEMENT_FLAGS)
                .unwrap()
                .value,
            Datum::Int(127)
        );
        assert_eq!(
            Datum::Int(-1)
                .convert_to(&unsigned_tiny, crate::DEFAULT_STATEMENT_FLAGS)
                .unwrap()
                .value,
            Datum::UInt(255)
        );

        let float = FieldType::new(FieldTypeCode::Float)
            .with_flen(5)
            .with_decimal(2);
        assert_eq!(
            Datum::Real(123.456)
                .convert_to(&float, crate::DEFAULT_STATEMENT_FLAGS)
                .unwrap()
                .value,
            Datum::Float32(f64::from(123.46_f32))
        );

        let string = FieldType::new(FieldTypeCode::Varchar)
            .with_flen(3)
            .with_collation(Collation::Utf8Mb4Bin);
        let converted = Datum::new_string("abcd")
            .convert_to(&string, crate::DEFAULT_STATEMENT_FLAGS)
            .unwrap();
        assert_eq!(converted.value.as_raw_bytes(), Some(&b"abc"[..]));
        assert!(converted.event.is_some());

        let decimal = FieldType::new(FieldTypeCode::NewDecimal)
            .with_flen(5)
            .with_decimal(2);
        assert_eq!(
            Datum::new_decimal(Decimal::from_signed_literal("12.345"))
                .convert_to(&decimal, crate::DEFAULT_STATEMENT_FLAGS)
                .unwrap()
                .value,
            Datum::new_decimal(Decimal::from_signed_literal("12.35"))
        );
    }

    #[test]
    fn contextual_integer_conversion_keeps_first_fatal_error() {
        let target = FieldType::new(FieldTypeCode::Tiny);
        let source = Datum::new_string("128tail".to_owned());
        let strict = source.convert_to(&target, crate::STRICT_FLAGS).unwrap();
        assert_eq!(strict.value, Datum::Int(127));
        // Go StrToInt retains the prefix error after successful ParseInt;
        // toSignedInteger only substitutes the range error if err == nil.
        assert_eq!(strict.event, Some(ScalarConversionEvent::Truncated));
        let warn = source
            .convert_to(
                &target,
                crate::DEFAULT_STATEMENT_FLAGS.with_truncate_as_warning(true),
            )
            .unwrap();
        assert!(matches!(
            warn.event,
            Some(ScalarConversionEvent::Overflow(_))
        ));
    }

    /// Go's signed string conversion keeps the parse/truncation error when a
    /// narrower integer target also reports a range clamp; unsigned conversion
    /// deliberately retains the opposite precedence.
    #[test]
    fn signed_string_conversion_prefers_source_truncation_over_clamp() {
        let signed_tiny = FieldType::new(FieldTypeCode::Tiny);
        let unsigned_tiny =
            FieldType::new(FieldTypeCode::Tiny).with_added_flags(FieldTypeFlags::UNSIGNED);

        for input in [
            Datum::new_string("999abc"),
            Datum::new_bytes(b"999abc".to_vec()),
        ] {
            let signed = input
                .clone()
                .convert_to(&signed_tiny, crate::DEFAULT_STATEMENT_FLAGS)
                .unwrap();
            assert_eq!(signed.value, Datum::Int(127));
            assert_eq!(signed.event, Some(ScalarConversionEvent::Truncated));

            let unsigned = input
                .convert_to(&unsigned_tiny, crate::DEFAULT_STATEMENT_FLAGS)
                .unwrap();
            assert_eq!(unsigned.value, Datum::UInt(255));
            assert!(matches!(
                unsigned.event,
                Some(ScalarConversionEvent::Overflow(_))
            ));
        }
    }

    /// Go `pkg/types/convert_test.go::TestGetValidIntPrefix` keeps the
    /// decimal/exponent prefix when `Context.HandleTruncate` returns an
    /// error. `strconv.ParseInt`/`ParseUint` then replaces the truncation
    /// with BIGINT overflow and value zero. A warning or ignored truncation
    /// instead continues through `floatStrToIntStr` and returns the rounded
    /// integer prefix.
    #[test]
    fn strict_datum_integer_conversion_uses_source_truncation_policy() {
        let signed = FieldType::new(FieldTypeCode::LongLong);
        let unsigned = FieldType::new(FieldTypeCode::LongLong).with_unsigned(true);

        for input in [
            Datum::new_string("123..34"),
            Datum::new_bytes(b"123..34".to_vec()),
        ] {
            for (target, strict_value, warning_value) in [
                (&signed, Datum::Int(0), Datum::Int(123)),
                (&unsigned, Datum::UInt(0), Datum::UInt(123)),
            ] {
                let strict = input.convert_to(target, crate::STRICT_FLAGS).unwrap();
                assert_eq!(strict.value, strict_value);
                assert!(matches!(
                    strict.event,
                    Some(ScalarConversionEvent::Overflow(_))
                ));

                for flags in [
                    crate::STRICT_FLAGS.with_truncate_as_warning(true),
                    crate::STRICT_FLAGS.with_ignore_truncate_err(true),
                ] {
                    let converted = input.convert_to(target, flags).unwrap();
                    assert_eq!(converted.value, warning_value);
                    assert_eq!(converted.event, Some(ScalarConversionEvent::Truncated));
                }
            }
        }
    }

    /// Source: `pkg/types/convert_test.go::TestConvertToBinaryString`.
    #[test]
    fn test_convert_to_binary_string() {
        let utf8 = "你好".as_bytes().to_vec();
        let gbk = vec![0xC4, 0xE3, 0xBA, 0xC3];
        let invalid_utf8 = [utf8.as_slice(), &[0x81]].concat();
        let invalid_gbk = [gbk.as_slice(), &[0x81]].concat();

        for (input, input_collation, output_collation, expected) in [
            (
                utf8.clone(),
                Collation::Utf8Bin,
                Collation::Utf8Bin,
                Some(utf8.clone()),
            ),
            (
                utf8.clone(),
                Collation::Utf8Mb4Bin,
                Collation::Utf8Mb4Bin,
                Some(utf8.clone()),
            ),
            (
                utf8.clone(),
                Collation::GbkBin,
                Collation::Utf8Bin,
                Some(utf8.clone()),
            ),
            (
                utf8.clone(),
                Collation::GbkBin,
                Collation::GbkBin,
                Some(utf8.clone()),
            ),
            (
                utf8.clone(),
                Collation::Binary,
                Collation::Utf8Mb4Bin,
                Some(utf8.clone()),
            ),
            (
                gbk.clone(),
                Collation::Binary,
                Collation::GbkBin,
                Some(utf8.clone()),
            ),
            (
                utf8.clone(),
                Collation::Utf8Bin,
                Collation::Binary,
                Some(utf8.clone()),
            ),
            (
                utf8.clone(),
                Collation::GbkBin,
                Collation::Binary,
                Some(gbk.clone()),
            ),
            (invalid_utf8, Collation::Utf8Bin, Collation::Utf8Bin, None),
            (invalid_gbk, Collation::GbkBin, Collation::GbkBin, None),
        ] {
            let input = Datum::new_collation_string(input, input_collation);
            let target = FieldType::new(FieldTypeCode::Varchar)
                .with_flen(255)
                .with_collation(output_collation);
            let converted = input.convert_to(&target, crate::DEFAULT_STATEMENT_FLAGS);
            match expected {
                Some(expected) => {
                    assert_eq!(
                        converted.unwrap().value.as_raw_bytes(),
                        Some(expected.as_slice())
                    );
                }
                None => assert!(converted.is_err()),
            }
        }
    }

    #[test]
    fn source_convert_to_enum_set_bit_json_vector_and_temporal_rows() {
        let enum_type = FieldType::new(FieldTypeCode::Enum)
            .with_elems(["a", "b"])
            .with_collation(Collation::Binary);
        assert_eq!(
            Datum::new_string("b")
                .convert_to(&enum_type, crate::DEFAULT_STATEMENT_FLAGS)
                .unwrap()
                .value,
            Datum::new_enum(parse_enum_value(&["a", "b"], 2).unwrap(), Collation::Binary)
        );

        let set_type = FieldType::new(FieldTypeCode::Set)
            .with_elems(["a", "b"])
            .with_collation(Collation::Binary);
        assert_eq!(
            Datum::UInt(3)
                .convert_to(&set_type, crate::DEFAULT_STATEMENT_FLAGS)
                .unwrap()
                .value,
            Datum::new_set(parse_set_value(&["a", "b"], 3).unwrap(), Collation::Binary)
        );

        let bit_type = FieldType::new(FieldTypeCode::Bit).with_flen(9);
        assert_eq!(
            Datum::UInt(0x101)
                .convert_to(&bit_type, crate::DEFAULT_STATEMENT_FLAGS)
                .unwrap()
                .value,
            Datum::new_mysql_bit(BinaryLiteral::from(&[0x01, 0x01]))
        );

        let json_type = FieldType::new(FieldTypeCode::Json);
        assert_eq!(
            Datum::new_string(r#"{"a":1}"#)
                .convert_to(&json_type, crate::DEFAULT_STATEMENT_FLAGS)
                .unwrap()
                .value,
            Datum::new_json(BinaryJSON::parse(r#"{"a":1}"#).unwrap())
        );

        let vector_type = FieldType::new(FieldTypeCode::VectorFloat32).with_flen(2);
        assert_eq!(
            Datum::new_string("[1,2]")
                .convert_to(&vector_type, crate::DEFAULT_STATEMENT_FLAGS)
                .unwrap()
                .value,
            Datum::new_vector_float32(VectorFloat32::parse("[1,2]").unwrap())
        );

        let datetime = FieldType::new(FieldTypeCode::Datetime).with_decimal(0);
        assert_eq!(
            Datum::new_string("2011-01-01 11:11:11")
                .convert_to(&datetime, crate::DEFAULT_STATEMENT_FLAGS)
                .unwrap()
                .value
                .sql_string()
                .unwrap(),
            "2011-01-01 11:11:11"
        );
        let duration = FieldType::new(FieldTypeCode::Duration).with_decimal(0);
        assert_eq!(
            Datum::new_string("12:34:56")
                .convert_to(&duration, crate::DEFAULT_STATEMENT_FLAGS)
                .unwrap()
                .value
                .sql_string()
                .unwrap(),
            "12:34:56"
        );
    }

    /// Go `Datum.ConvertTo` threads `FlagIgnoreZeroDateErr` into
    /// `ParseTimeFromNum`: strict flags return the zero temporal value beside
    /// `ErrTruncatedWrongVal`, while default statement flags keep it silently.
    #[test]
    fn numeric_zero_temporal_conversion_obeys_zero_date_flag() {
        let datetime = FieldType::new(FieldTypeCode::Datetime);
        let strict = Datum::Int(0)
            .convert_to(&datetime, crate::STRICT_FLAGS)
            .expect_err("strict numeric zero must be rejected by ParseTimeFromNum");
        assert_eq!(
            strict,
            DatumValueError::IncorrectTemporal(
                Time::new(CoreTime::default(), TimeType::DateTime, 0).unwrap()
            )
        );

        let permissive = Datum::Int(0)
            .convert_to(&datetime, crate::DEFAULT_STATEMENT_FLAGS)
            .expect("DefaultStmtFlags ignore zero-date errors");
        assert!(matches!(permissive.value, Datum::Time(time) if time.is_zero()));
    }

    /// Source: `pkg/types/datum_test.go::TestConvertToFloat`.
    #[test]
    fn test_convert_to_float() {
        let double = FieldType::new(FieldTypeCode::Double);
        let double_rows = [
            (Datum::Float32(f64::from(3.0_f32)), 3.0),
            (Datum::Real(12_345.678), 12_345.678),
            (Datum::new_string("12345.678"), 12_345.678),
            (Datum::new_bytes(b"12345.678"), 12_345.678),
            (Datum::Int(12_345), 12_345.0),
            (Datum::UInt(123_456), 123_456.0),
        ];
        for (input, expected) in double_rows {
            let converted = input
                .convert_to(&double, crate::DEFAULT_STATEMENT_FLAGS)
                .unwrap();
            assert_eq!(converted.value, Datum::Real(expected), "{input:?}");
            assert_eq!(converted.event, None, "{input:?}");
        }

        // Go's `byte(123)` becomes its unsupported KindInterface. Raw is the
        // corresponding unsupported stored kind in the Rust representation.
        assert!(Datum::new_raw([123])
            .convert_to(&double, crate::DEFAULT_STATEMENT_FLAGS)
            .is_err());

        for (input, expected) in [
            (f64::NAN, 0.0),
            (f64::NEG_INFINITY, f64::NEG_INFINITY),
            (f64::INFINITY, f64::INFINITY),
        ] {
            let converted = Datum::Real(input)
                .convert_to(&double, crate::DEFAULT_STATEMENT_FLAGS)
                .unwrap();
            let Datum::Real(actual) = converted.value else {
                panic!("DOUBLE conversion returned another datum kind")
            };
            assert_eq!(actual, expected);
            assert!(matches!(
                converted.event,
                Some(ScalarConversionEvent::Overflow(_))
            ));
        }

        let float = FieldType::new(FieldTypeCode::Float);
        for input in [
            Datum::Float32(f64::from(281.37_f32)),
            Datum::new_string("281.37"),
        ] {
            let converted = input
                .convert_to(&float, crate::DEFAULT_STATEMENT_FLAGS)
                .unwrap();
            assert_eq!(converted.value, Datum::Float32(f64::from(281.37_f32)));
            assert_eq!(converted.event, None);
        }
    }

    #[test]
    fn test_to_uint32() {
        let uint32 = FieldType::new(FieldTypeCode::Long).with_unsigned(true);
        for (input, expected, overflow) in [
            (Datum::Int(5_000_000_000), u32::MAX as u64, true),
            (Datum::Int(-1), u32::MAX as u64, true),
            (Datum::new_string("5000000000"), u32::MAX as u64, true),
            (Datum::Int(12_345), 12_345, false),
            (Datum::Int(0), 0, false),
            (Datum::Int(2_147_483_648), 2_147_483_648, false),
            (
                Datum::new_enum(parse_enum_value(&["a"], 1).unwrap(), Collation::Binary),
                1,
                false,
            ),
            (
                Datum::new_set(parse_set_value(&["a"], 1).unwrap(), Collation::Binary),
                1,
                false,
            ),
        ] {
            let converted = input
                .convert_to(&uint32, crate::DEFAULT_STATEMENT_FLAGS)
                .unwrap();
            assert_eq!(converted.value, Datum::UInt(expected));
            assert_eq!(converted.event.is_some(), overflow);
        }
    }

    #[test]
    fn test_to_json() {
        let target = FieldType::new(FieldTypeCode::Json);
        let timestamp = crate::parse_time(
            "2011-11-10 11:11:11.111111",
            TimeType::Timestamp,
            6,
            false,
            true,
            false,
            &chrono_tz::UTC,
        )
        .unwrap()
        .time;

        for (input, expected) in [
            (
                Datum::Int(1),
                BinaryJSON::from_typed_value(&crate::BinaryJSONValue::Int64(1)).unwrap(),
            ),
            (
                Datum::Real(2.0),
                BinaryJSON::from_typed_value(&crate::BinaryJSONValue::Float64(2.0)).unwrap(),
            ),
            (
                Datum::new_string("\"hello, 世界\""),
                BinaryJSON::parse("\"hello, 世界\"").unwrap(),
            ),
            (
                Datum::new_string("[1, 2, 3]"),
                BinaryJSON::parse("[1, 2, 3]").unwrap(),
            ),
            (Datum::new_string("{}"), BinaryJSON::parse("{}").unwrap()),
            (Datum::new_time(timestamp), BinaryJSON::from_time(timestamp)),
            (
                Datum::new_string(r#"{"a": "9223372036854775809"}"#),
                BinaryJSON::parse(r#"{"a": "9223372036854775809"}"#).unwrap(),
            ),
        ] {
            let converted = input
                .convert_to(&target, crate::DEFAULT_STATEMENT_FLAGS)
                .unwrap();
            assert_eq!(converted.value, Datum::new_json(expected), "{input:?}");
            assert_eq!(converted.event, None, "{input:?}");
        }

        for input in [
            Datum::new_binary_literal(BinaryLiteral::from(vec![0x81])),
            Datum::new_string("hello, 世界"),
        ] {
            assert!(
                input
                    .convert_to(&target, crate::DEFAULT_STATEMENT_FLAGS)
                    .is_err(),
                "{input:?}"
            );
        }
    }

    #[test]
    fn test_string_to_mysql_bit() {
        for (text, flen, expected, truncated) in [
            ("true", 1, vec![1], true),
            ("true", 32, b"true".to_vec(), false),
            ("false", 1, vec![1], true),
            ("false", 40, b"false".to_vec(), false),
            ("1", 1, vec![1], true),
            ("1", 8, vec![0x31], false),
            ("0", 1, vec![1], true),
            ("0", 8, vec![0x30], false),
            ("b'1'", 32, b"b'1'".to_vec(), false),
            ("b'0'", 32, b"b'0'".to_vec(), false),
        ] {
            let target = FieldType::new(FieldTypeCode::Bit).with_flen(flen);
            let converted = Datum::new_string(text)
                .convert_to(&target, crate::DEFAULT_STATEMENT_FLAGS)
                .unwrap();
            let Datum::Bit(value) = converted.value else {
                panic!("BIT conversion returned another datum kind");
            };
            assert_eq!(value.as_bytes(), expected, "{text} BIT({flen})");
            assert_eq!(converted.event.is_some(), truncated, "{text} BIT({flen})");
        }
    }

    #[test]
    fn test_produce_dec_with_specified_tp() {
        for (input, flen, scale, expected, overflow, rounded) in [
            ("0.0000", 4, 3, "0.000", false, false),
            ("0.0001", 4, 3, "0.000", false, true),
            ("123", 8, 5, "123.00000", false, false),
            ("-123", 8, 5, "-123.00000", false, false),
            ("123.899", 5, 2, "123.90", false, true),
            ("-123.899", 5, 2, "-123.90", false, true),
            ("123.899", 6, 2, "123.90", false, true),
            ("-123.899", 6, 2, "-123.90", false, true),
            ("123.99", 4, 1, "124.0", false, true),
            ("123.99", 3, 0, "124", false, true),
            ("-123.99", 3, 0, "-124", false, true),
            ("123.99", 3, 1, "99.9", true, false),
            ("-123.99", 3, 1, "-99.9", true, false),
            ("99.9999", 5, 3, "99.999", true, false),
            ("-99.9999", 5, 3, "-99.999", true, false),
            ("99.9999", 6, 3, "100.000", false, true),
            ("-99.9999", 6, 3, "-100.000", false, true),
        ] {
            let target = FieldType::new(FieldTypeCode::NewDecimal)
                .with_flen(flen)
                .with_decimal(scale);
            let converted = Datum::new_decimal(Decimal::from_signed_literal(input))
                .convert_to(&target, crate::DEFAULT_STATEMENT_FLAGS)
                .unwrap();
            assert_eq!(
                converted.value.as_decimal().unwrap().to_string(),
                expected,
                "{input} DECIMAL({flen},{scale})"
            );
            assert_eq!(
                matches!(converted.event, Some(ScalarConversionEvent::Overflow(_))),
                overflow,
                "overflow: {input} DECIMAL({flen},{scale})"
            );
            assert_eq!(
                converted.event == Some(ScalarConversionEvent::RoundedToScale),
                rounded,
                "rounded: {input} DECIMAL({flen},{scale})"
            );
        }
    }

    #[test]
    fn test_change_reverse_result_by_upper_lower_bound() {
        let unsigned = FieldType::new(FieldTypeCode::LongLong).with_unsigned(true);
        assert_eq!(get_min_value(&unsigned), Datum::UInt(0));
        assert_eq!(get_max_value(&unsigned), Datum::UInt(u64::MAX));

        let double = FieldType::new(FieldTypeCode::Double)
            .with_flen(23)
            .with_decimal(UNSPECIFIED_LENGTH);
        let decimal = FieldType::new(FieldTypeCode::NewDecimal)
            .with_flen(30)
            .with_decimal(3);
        for (input, target, rounding, expected) in vec![
            (
                Datum::Int(1),
                unsigned.clone(),
                RoundingType::Ceiling,
                Datum::UInt(2),
            ),
            (
                Datum::Int(1),
                unsigned.clone(),
                RoundingType::Floor,
                Datum::UInt(1),
            ),
            (
                Datum::Int(i64::MAX),
                unsigned.clone(),
                RoundingType::Ceiling,
                Datum::UInt(u64::MAX),
            ),
            (
                Datum::Int(i64::MAX),
                unsigned,
                RoundingType::Floor,
                Datum::UInt(i64::MAX as u64),
            ),
            (
                Datum::Int(1),
                double.clone(),
                RoundingType::Ceiling,
                Datum::Real(2.0),
            ),
            (
                Datum::Int(1),
                double.clone(),
                RoundingType::Floor,
                Datum::Real(1.0),
            ),
            (
                Datum::Int(i64::MAX),
                double.clone(),
                RoundingType::Ceiling,
                get_max_value(&double),
            ),
            (
                Datum::Int(i64::MAX),
                double,
                RoundingType::Floor,
                Datum::Real(i64::MAX as f64),
            ),
            (
                Datum::Int(1),
                decimal.clone(),
                RoundingType::Ceiling,
                Datum::new_decimal(Decimal::from_int(2)),
            ),
            (
                Datum::Int(1),
                decimal.clone(),
                RoundingType::Floor,
                Datum::new_decimal(Decimal::from_int(1)),
            ),
            (
                Datum::Int(i64::MAX),
                decimal.clone(),
                RoundingType::Ceiling,
                get_max_value(&decimal),
            ),
            (
                Datum::Int(i64::MAX),
                decimal,
                RoundingType::Floor,
                Datum::new_decimal(Decimal::from_int(i64::MAX)),
            ),
        ] {
            let converted = change_reverse_result_by_bound(
                &target,
                &input,
                rounding,
                crate::DEFAULT_STATEMENT_FLAGS,
            )
            .unwrap();
            assert_eq!(converted.event, None, "{input:?} -> {target:?}");
            assert_eq!(
                converted
                    .value
                    .compare(&expected, Collation::Binary)
                    .unwrap(),
                std::cmp::Ordering::Equal,
                "{input:?} -> {target:?}: {:?} versus {expected:?}",
                converted.value
            );
        }
    }

    #[test]
    fn shared_string_target_keeps_byte_rune_padding_and_typed_diagnostic_domains() {
        use crate::{ConversionContext, ConversionLocation, ConversionWarningAppender};
        use std::cell::RefCell;
        use tidb_error::terror::TerrorError;

        let target = |code, flen, binary| {
            FieldType::new(code)
                .with_flen(flen)
                .with_charset_name(if binary { "binary" } else { "utf8mb4" })
                .with_collation_name(if binary { "binary" } else { "utf8mb4_bin" })
        };
        for flen in [-1, -7] {
            let input = vec![0xff, b'a'];
            let converted = produce_string_with_type(
                input.clone(),
                &target(FieldTypeCode::String, flen, true),
                true,
            )
            .unwrap();
            assert_eq!(converted.value, input);
            assert_eq!(converted.event, None);
        }
        let binary = target(FieldTypeCode::String, 4, true);
        let padded = produce_string_with_type(vec![b'a', 0xff], &binary, true).unwrap();
        assert_eq!(padded.value, [b'a', 0xff, 0, 0]);
        assert_eq!(padded.event, None);
        assert_eq!(
            produce_string_with_type(vec![b'a'], &binary, false)
                .unwrap()
                .value,
            [b'a']
        );
        assert_eq!(
            produce_string_with_type(vec![b'a'], &target(FieldTypeCode::VarString, 4, true), true)
                .unwrap()
                .value,
            [b'a']
        );
        let invalid_tail = vec![0xff, b'a', 0xe2, 0x82, b'X'];
        for (is_binary, expected) in [
            (true, vec![0xff, b'a', 0xe2, 0x82]),
            (false, vec![0xff, b'a']),
        ] {
            let converted = produce_string_with_type(
                invalid_tail.clone(),
                &target(FieldTypeCode::Blob, 4, is_binary),
                false,
            )
            .unwrap();
            assert_eq!(converted.value, expected);
            assert_eq!(converted.event, Some(ScalarConversionEvent::Truncated));
        }
        #[derive(Default)]
        struct Warnings(RefCell<Vec<TerrorError>>);
        impl ConversionWarningAppender for Warnings {
            fn append_conversion_warning(&self, error: TerrorError) {
                self.0.borrow_mut().push(error);
            }
        }
        let warnings = Warnings::default();
        let strict = ConversionFlags::default()
            .with_ignore_truncate_err(false)
            .with_truncate_as_warning(false);
        for (mode, flags) in [
            ("strict", strict),
            ("warn", strict.with_truncate_as_warning(true)),
            ("ignore", strict.with_ignore_truncate_err(true)),
        ] {
            let context = ConversionContext::new(flags, ConversionLocation::UTC, &warnings);
            let mut diagnostics = Diagnostics::new(Some(&context));
            // Invalid FF is one rune; C3 A9 is one rune; diagnostic length is 3, not 4 bytes.
            let converted = produce_string_reported(
                vec![0xff, 0xc3, 0xa9, b'z'],
                &target(FieldTypeCode::Varchar, 2, false),
                false,
                &mut diagnostics,
            )
            .unwrap();
            assert_eq!(converted.value, [0xff, 0xc3, 0xa9]);
            assert_eq!(converted.event, Some(ScalarConversionEvent::Truncated));
            match mode {
                "strict" => {
                    let error = diagnostics.error.unwrap();
                    assert_eq!(error.identity(), ERR_DATA_TOO_LONG.identity());
                    assert_eq!(error.message(), "Data Too Long, field len 2, data len 3");
                    assert!(warnings.0.borrow().is_empty());
                }
                "warn" => {
                    assert!(diagnostics.error.is_none());
                    let recorded = warnings.0.take();
                    assert_eq!(recorded.len(), 1);
                    assert_eq!(recorded[0].identity(), ERR_DATA_TOO_LONG.identity());
                    assert_eq!(
                        recorded[0].message(),
                        "Data Too Long, field len 2, data len 3"
                    );
                }
                _ => {
                    assert!(diagnostics.error.is_none());
                    assert!(warnings.0.borrow().is_empty());
                }
            }
        }
        let context = ConversionContext::new(strict, ConversionLocation::UTC, &warnings);
        let mut diagnostics = Diagnostics::new(Some(&context));
        let varchar = produce_string_reported(
            b"a \t".to_vec(),
            &target(FieldTypeCode::Varchar, 1, false),
            false,
            &mut diagnostics,
        )
        .unwrap();
        assert_eq!(varchar.value, b"a");
        assert_eq!(varchar.event, Some(ScalarConversionEvent::Truncated));
        assert!(diagnostics.error.is_none());
        let fixed = produce_string_reported(
            b"a \t".to_vec(),
            &target(FieldTypeCode::String, 1, false),
            false,
            &mut diagnostics,
        )
        .unwrap();
        assert_eq!(fixed.value, b"a");
        assert_eq!(fixed.event, None);
        assert!(diagnostics.error.is_none());
        let varstring = produce_string_reported(
            b"a \t".to_vec(),
            &target(FieldTypeCode::VarString, 1, false),
            false,
            &mut diagnostics,
        )
        .unwrap();
        assert_eq!(varstring.event, Some(ScalarConversionEvent::Truncated));
        let error = diagnostics.error.unwrap();
        assert_eq!(error.identity(), ERR_DATA_TOO_LONG.identity());
        assert_eq!(error.message(), "Data Too Long, field len 1, data len 3");
        let recorded = warnings.0.take();
        assert_eq!(recorded.len(), 1);
        assert_eq!(recorded[0].identity(), ERR_TRUNCATED.identity());
        assert_eq!(
            recorded[0].message(),
            "Data truncated, field len 1, data len 3"
        );
        let mut diagnostics = Diagnostics::new(Some(&context));
        let nbsp = produce_string_reported(
            "a\u{00a0}".as_bytes().to_vec(),
            &target(FieldTypeCode::String, 1, false),
            false,
            &mut diagnostics,
        )
        .unwrap();
        assert_eq!(nbsp.event, Some(ScalarConversionEvent::Truncated));
        assert_eq!(
            diagnostics.error.unwrap().message(),
            "Data Too Long, field len 1, data len 2"
        );
        let converted = Datum::new_bytes(b"a".to_vec())
            .convert_to_in_context(&binary, &context, &SessionTimeZone::utc())
            .unwrap();
        assert_eq!(converted.value, Datum::new_bytes(vec![b'a', 0, 0, 0]));
        assert!(converted.error.is_none());
        assert!(warnings.0.borrow().is_empty());
    }

    #[test]
    fn shared_datatype_bounds_keep_effective_codes_metadata_storage_and_source_bounds() {
        for (code, lower, upper, unsigned_upper) in [
            (FieldTypeCode::Tiny, -128, 127, 255),
            (FieldTypeCode::Short, -32768, 32767, 65535),
            (FieldTypeCode::Int24, -8388608, 8388607, 16777215),
            (
                FieldTypeCode::Long,
                i64::from(i32::MIN),
                i64::from(i32::MAX),
                u64::from(u32::MAX),
            ),
            (FieldTypeCode::LongLong, i64::MIN, i64::MAX, u64::MAX),
        ] {
            let target = FieldType::new(code).with_flen(-7).with_decimal(-3);
            assert_eq!(get_min_value(&target), Datum::Int(lower));
            assert_eq!(get_max_value(&target), Datum::Int(upper));
            let target = target.with_added_flags(FieldTypeFlags::UNSIGNED);
            assert_eq!(get_min_value(&target), Datum::UInt(0));
            assert_eq!(get_max_value(&target), Datum::UInt(unsigned_upper));
        }
        for (code, max, min, negative_max, negative_min) in [
            (
                FieldTypeCode::Float,
                Datum::Float32(f64::from(99.9_f32)),
                Datum::Float32(-f64::from(99.9_f32)),
                Datum::Float32(-9.0),
                Datum::Float32(9.0),
            ),
            (
                FieldTypeCode::Double,
                Datum::Real(99.9),
                Datum::Real(-99.9),
                Datum::Real(-9.0),
                Datum::Real(9.0),
            ),
        ] {
            let target = FieldType::new(code)
                .with_flen(3)
                .with_decimal(1)
                .with_added_flags(FieldTypeFlags::UNSIGNED);
            assert_eq!(get_max_value(&target), max);
            assert_eq!(get_min_value(&target), min); // Unsigned does not change these float bounds.
            let target = target.with_flen(-1).with_decimal(-1);
            assert_eq!(get_max_value(&target), negative_max);
            assert_eq!(get_min_value(&target), negative_min);
        }
        for code in [
            FieldTypeCode::String,
            FieldTypeCode::Varchar,
            FieldTypeCode::VarString,
            FieldTypeCode::Blob,
            FieldTypeCode::TinyBlob,
            FieldTypeCode::MediumBlob,
            FieldTypeCode::LongBlob,
        ] {
            for collation in ["binary", "utf8mb4_general_ci"] {
                let target = FieldType::new(code)
                    .with_flen(0)
                    .with_collation_name(collation);
                for (value, byte) in [(get_min_value(&target), 1), (get_max_value(&target), 250)] {
                    assert_eq!(value.collation(), Some(target.collation()));
                    let Datum::String(value) = value else {
                        panic!("string bound storage, including binary collation")
                    };
                    assert_eq!(value.bytes(), &[byte]);
                }
            }
        }
        for (flen, scale, maximum, minimum) in [
            (5, 2, "999.99", "-999.99"),
            (2, 5, "9.99999", "-9.99999"),
            (-1, 2, "9.99", "-9.99"),
            (3, -7, "999", "-999"),
            (-1, -1, "0", "0"),
        ] {
            let target = FieldType::new(FieldTypeCode::NewDecimal)
                .with_flen(flen)
                .with_decimal(scale)
                .with_added_flags(FieldTypeFlags::UNSIGNED);
            for (value, expected) in [
                (get_max_value(&target), maximum),
                (get_min_value(&target), minimum),
            ] {
                let Datum::Decimal(value) = value else {
                    panic!("decimal bound storage")
                };
                assert_eq!(value.to_string(), expected);
                assert_eq!(value.declared_shape(), None);
            }
        }
        let duration = FieldType::new(FieldTypeCode::Duration).with_decimal(6);
        for (value, expected) in [
            (get_max_value(&duration), crate::MAX_TIME_NANOS),
            (get_min_value(&duration), crate::MIN_TIME_NANOS),
        ] {
            let Datum::Duration(value) = value else {
                panic!("duration bound")
            };
            assert_eq!(value.nanoseconds(), expected);
            assert_eq!(value.fsp(), 0);
        }
        for (code, kind, min_core, max_core) in [
            (
                FieldTypeCode::Date,
                TimeType::Date,
                CoreTime::from_date(1, 1, 1, 0, 0, 0, 0),
                CoreTime::from_date(9999, 12, 31, 23, 59, 59, 999999),
            ),
            (
                FieldTypeCode::Datetime,
                TimeType::DateTime,
                CoreTime::from_date(1, 1, 1, 0, 0, 0, 0),
                CoreTime::from_date(9999, 12, 31, 23, 59, 59, 999999),
            ),
            (
                FieldTypeCode::Timestamp,
                TimeType::Timestamp,
                CoreTime::from_date(1970, 1, 1, 0, 0, 1, 0),
                CoreTime::from_date(2038, 1, 19, 3, 14, 7, 999999),
            ),
        ] {
            let target = FieldType::new(code).with_decimal(6);
            for (value, core) in [
                (get_min_value(&target), min_core),
                (get_max_value(&target), max_core),
            ] {
                let expected = Time::new(core, kind, 0).unwrap();
                let Datum::Time(value) = value else {
                    panic!("temporal bound")
                };
                assert_eq!(value.core_time(), expected.core_time());
                assert_eq!(value.kind(), kind);
                assert_eq!(value.fsp(), 0);
            }
        }
        for code in [
            FieldTypeCode::Unspecified,
            FieldTypeCode::Null,
            FieldTypeCode::Year,
            FieldTypeCode::NewDate,
            FieldTypeCode::Bit,
            FieldTypeCode::Json,
            FieldTypeCode::Enum,
            FieldTypeCode::Set,
            FieldTypeCode::Geometry,
            FieldTypeCode::VectorFloat32,
        ] {
            let target = FieldType::new(code);
            assert_eq!(get_min_value(&target), Datum::Null);
            assert_eq!(get_max_value(&target), Datum::Null);
        }
        for byte in 0..=u8::MAX {
            let unknown = FieldType::new(FieldTypeCode::Unknown(byte))
                .with_flen(5)
                .with_decimal(2);
            assert_eq!(get_min_value(&unknown), Datum::Null);
            assert_eq!(get_max_value(&unknown), Datum::Null);
        }
        let array = FieldType::new(FieldTypeCode::Tiny).with_array(true);
        assert_eq!(get_min_value(&array), Datum::Null);
        assert_eq!(get_max_value(&array), Datum::Null);
        for (source, minimum, maximum) in [
            (Datum::Int(1), Datum::Int(i64::MIN), Datum::Int(i64::MAX)),
            (Datum::UInt(1), Datum::UInt(0), Datum::UInt(u64::MAX)),
            (
                Datum::Float32(1.0),
                Datum::Float32(-f64::from(f32::MAX)),
                Datum::Float32(f64::from(f32::MAX)),
            ),
            (
                Datum::Real(1.0),
                Datum::Real(-f64::MAX),
                Datum::Real(f64::MAX),
            ),
            (
                Datum::Decimal(Decimal::from_literal("0.0007")),
                Datum::Decimal(Decimal::from_signed_literal("-9.9999")),
                Datum::Decimal(Decimal::from_literal("9.9999")),
            ),
            (Datum::new_string("x"), Datum::MinNotNull, Datum::MaxValue),
        ] {
            assert_eq!(source_kind_bound(&source, RoundingType::Floor), minimum);
            assert_eq!(source_kind_bound(&source, RoundingType::Ceiling), maximum);
        }
    }

    #[test]
    fn shared_reverse_bound_controller_keeps_overflow_replacement_increment_and_floor() {
        let flags = ConversionFlags::default();
        let double = FieldType::new(FieldTypeCode::Double);
        let early = change_reverse_result_by_bound(
            &double,
            &Datum::Real(f64::NAN),
            RoundingType::Ceiling,
            flags,
        )
        .unwrap();
        assert_eq!(early.value, Datum::Real(0.0)); // Continuing to increment would incorrectly yield one.
        assert!(matches!(
            early.event,
            Some(ScalarConversionEvent::Overflow(_))
        ));
        let target = FieldType::new(FieldTypeCode::NewDecimal)
            .with_flen(25)
            .with_decimal(0);
        let replaced = change_reverse_result_by_bound(
            &target,
            &Datum::Int(i64::MAX),
            RoundingType::Ceiling,
            flags,
        )
        .unwrap();
        let Datum::Decimal(value) = replaced.value else {
            panic!("target decimal bound")
        };
        assert_eq!(value.to_string(), "9".repeat(25));
        assert_eq!(value.declared_shape(), None);
        assert_eq!(replaced.event, None);
        let lower = change_reverse_result_by_bound(
            &double.clone().with_flen(3).with_decimal(1),
            &Datum::UInt(0),
            RoundingType::Floor,
            flags,
        )
        .unwrap();
        assert_eq!(lower.value, Datum::Real(-99.9));
        assert_eq!(lower.event, None);
        for (target, source, rounding, expected) in [
            (
                FieldType::new(FieldTypeCode::Tiny),
                Datum::Int(126),
                RoundingType::Ceiling,
                Datum::Int(127),
            ),
            (
                FieldType::new(FieldTypeCode::Tiny),
                Datum::Int(127),
                RoundingType::Ceiling,
                Datum::Int(127),
            ),
            (
                FieldType::new(FieldTypeCode::Tiny),
                Datum::Int(126),
                RoundingType::Floor,
                Datum::Int(126),
            ),
            (
                double,
                Datum::Real(1.25),
                RoundingType::Ceiling,
                Datum::Real(2.25),
            ),
            (
                FieldType::new(FieldTypeCode::Float),
                Datum::Float32(1.25),
                RoundingType::Ceiling,
                Datum::Float32(2.25),
            ),
            (
                FieldType::new(FieldTypeCode::NewDecimal)
                    .with_flen(3)
                    .with_decimal(1),
                Datum::Decimal(Decimal::from_literal("9.5")),
                RoundingType::Ceiling,
                Datum::Decimal(Decimal::from_literal("10.5")),
            ),
            (
                FieldType::new(FieldTypeCode::NewDecimal)
                    .with_flen(2)
                    .with_decimal(1),
                Datum::Real(9.9),
                RoundingType::Ceiling,
                Datum::Decimal(Decimal::from_literal("9.9")),
            ),
            (
                FieldType::new(FieldTypeCode::VarString).with_flen(1),
                Datum::new_string("x"),
                RoundingType::Ceiling,
                Datum::new_string("x"),
            ),
        ] {
            let converted =
                change_reverse_result_by_bound(&target, &source, rounding, flags).unwrap();
            assert_eq!(converted.value, expected);
            assert_eq!(converted.event, None);
        }
    }
}

#[cfg(test)]
#[test]
fn shared_json_target_convert_to_keeps_null_events_metadata_and_error_classes() {
    use crate::{BinaryJSONError, DatumKind, MysqlEnum, MysqlSet};
    fn assert_json(converted: Converted<Datum>, tag: u8, bytes: &[u8]) {
        assert_eq!(converted.event, None);
        let Datum::Json(value) = converted.value else {
            panic!("expected JSON datum")
        };
        assert_eq!(value.type_code(), tag);
        assert_eq!(value.value(), bytes);
    }
    let target = FieldType::new(FieldTypeCode::Json)
        .with_flen(1)
        .with_decimal(6)
        .with_collation(Collation::Binary)
        .with_flags(u32::MAX);
    let flags = ConversionFlags::default();
    let zone = SessionTimeZone::utc();
    let cases = vec![
        (Datum::new_string("12"), 0x09, vec![12, 0, 0, 0, 0, 0, 0, 0]),
        (
            Datum::Bytes(b"1".to_vec()),
            0x09,
            vec![1, 0, 0, 0, 0, 0, 0, 0],
        ),
        (
            Datum::new_enum(MysqlEnum::new("1", 99), Collation::Binary),
            0x09,
            vec![1, 0, 0, 0, 0, 0, 0, 0],
        ),
        (
            Datum::new_set(MysqlSet::new("1", 99), Collation::Binary),
            0x09,
            vec![1, 0, 0, 0, 0, 0, 0, 0],
        ),
        (Datum::UInt(1), 0x0a, vec![1, 0, 0, 0, 0, 0, 0, 0]),
        (
            Datum::Bit(BinaryLiteral::from(b"1".to_vec())),
            0x0c,
            vec![1, b'1'],
        ),
        (Datum::Raw(b"1".to_vec()), 0x0c, vec![1, b'1']),
        (
            Datum::Float32(16_777_217.0),
            0x0b,
            16_777_217.0_f64.to_bits().to_le_bytes().to_vec(),
        ),
        (
            Datum::Duration(MySqlDuration::from_raw_parts(-1, -1)),
            0x11,
            vec![0xff; 12],
        ),
        (
            Datum::Time(Time::from_raw_parts(
                CoreTime::from_raw(u64::MAX),
                TimeType::DateTime,
                u8::MAX,
            )),
            0x0f,
            vec![0xff; 8],
        ),
        (
            Datum::Json(BinaryJSON::from_encoded_parts(0x03, vec![0xff])),
            0x03,
            vec![0xff],
        ),
    ];
    for (value, tag, bytes) in cases {
        assert_json(value.convert_to(&target, flags).unwrap(), tag, &bytes);
        let mut diagnostics = Diagnostics::new(None);
        assert_json(
            value
                .convert_to_reported(&target, flags, &zone, &mut diagnostics)
                .unwrap(),
            tag,
            &bytes,
        );
        assert!(diagnostics.error.is_none());
        assert!(!diagnostics.unmapped);
    }
    for (value, expected) in [
        (
            Datum::new_string(" "),
            DatumValueError::Json(BinaryJSONError::EmptyDocument),
        ),
        (
            Datum::Bytes(b"1 2".to_vec()),
            DatumValueError::Json(BinaryJSONError::TrailingValues),
        ),
        (
            Datum::new_set(MysqlSet::new("[", 1), Collation::Binary),
            DatumValueError::Json(BinaryJSONError::InvalidText),
        ),
        (
            Datum::BinaryLiteral(BinaryLiteral::from(vec![0xff])),
            DatumValueError::Comparison(
                "Cannot create a JSON value from a string with CHARACTER SET 'binary'".to_owned(),
            ),
        ),
        (
            Datum::Raw(vec![0xff]),
            DatumValueError::Unsupported(DatumKind::Raw, "json"),
        ),
        (
            Datum::MinNotNull,
            DatumValueError::Unsupported(DatumKind::MinNotNull, "json"),
        ),
        (
            Datum::Float32(f64::INFINITY),
            DatumValueError::Json(BinaryJSONError::InvalidText),
        ),
    ] {
        assert_eq!(value.convert_to(&target, flags).unwrap_err(), expected);
        let mut diagnostics = Diagnostics::new(None);
        assert_eq!(
            value
                .convert_to_reported(&target, flags, &zone, &mut diagnostics)
                .unwrap_err(),
            expected
        );
        assert!(diagnostics.error.is_none());
        assert!(!diagnostics.unmapped);
    }
    for value in [
        Datum::new_string(vec![0xff]),
        Datum::Bytes(vec![0xff]),
        Datum::new_enum(MysqlEnum::new([0xff], 1), Collation::Binary),
        Datum::new_set(MysqlSet::new([0xff], 1), Collation::Binary),
        Datum::Bit(BinaryLiteral::from(vec![0xff])),
    ] {
        assert!(matches!(
            value.convert_to(&target, flags),
            Err(DatumValueError::InvalidUtf8(_))
        ));
        let mut diagnostics = Diagnostics::new(None);
        assert!(matches!(
            value.convert_to_reported(&target, flags, &zone, &mut diagnostics),
            Err(DatumValueError::InvalidUtf8(_))
        ));
        assert!(diagnostics.error.is_none());
        assert!(!diagnostics.unmapped);
    }
    for (value, target) in [
        (Datum::Null, target),
        (
            Datum::BinaryLiteral(BinaryLiteral::from(vec![0xff])),
            FieldType::new(FieldTypeCode::Null),
        ),
    ] {
        let converted = value.convert_to(&target, flags).unwrap();
        assert_eq!(converted.value, Datum::Null);
        assert_eq!(converted.event, None);
        let mut diagnostics = Diagnostics::new(None);
        let converted = value
            .convert_to_reported(&target, flags, &zone, &mut diagnostics)
            .unwrap();
        assert_eq!(converted.value, Datum::Null);
        assert_eq!(converted.event, None);
        assert!(diagnostics.error.is_none());
        assert!(!diagnostics.unmapped);
    }
}

#[cfg(test)]
#[test]
fn shared_float_target_preserves_nonfinite_rounding_error_order_and_typed_diagnostics() {
    use crate::{ConversionContext, ConversionLocation, ConversionWarningAppender, FieldTypeFlags};
    use tidb_error::terror::TerrorError;

    struct NoWarnings;
    impl ConversionWarningAppender for NoWarnings {
        fn append_conversion_warning(&self, _: TerrorError) {
            panic!("strict typed errors are retained, not appended")
        }
    }
    let flags = ConversionFlags::default()
        .with_ignore_truncate_err(false)
        .with_truncate_as_warning(false);
    let context = ConversionContext::new(flags, ConversionLocation::UTC, &NoWarnings);
    let unsigned = FieldType::new(FieldTypeCode::Double)
        .with_flen(2)
        .with_decimal(0)
        .with_added_flags(FieldTypeFlags::UNSIGNED);
    for (input, event_text, diagnostic_text) in [
        (f64::NAN, "NaN", "constant NaN overflows double"),
        (f64::INFINITY, "inf", "constant +Inf overflows double"),
        (f64::NEG_INFINITY, "-inf", "constant -Inf overflows double"),
    ] {
        let mut diagnostics = Diagnostics::new(Some(&context));
        let converted = produce_float_reported(input, &unsigned, &mut diagnostics);
        if input.is_nan() {
            assert_eq!(converted.value, 0.0);
        } else {
            assert_eq!(converted.value, input);
        }
        assert_eq!(
            converted.event,
            Some(overflow_event(event_text.to_owned(), FieldTypeCode::Double))
        );
        let error = diagnostics.error.unwrap();
        assert_eq!(error.identity(), ERR_OVERFLOW.identity());
        assert_eq!(error.message(), diagnostic_text);
        // The context-free surface retains the overflow event but no Terror.
        let mut disabled = Diagnostics::new(None);
        disabled.error(|| panic!("disabled diagnostic constructor must be lazy"));
        let plain = produce_float_reported(input, &unsigned, &mut disabled);
        assert_eq!(plain.event, converted.event);
        assert!(disabled.error.is_none());
    }
    let mut diagnostics = Diagnostics::new(Some(&context));
    let rounded_zero = produce_float_reported(-0.4, &unsigned, &mut diagnostics);
    assert_eq!(rounded_zero.value.to_bits(), (-0.0_f64).to_bits());
    assert_eq!(rounded_zero.event, None);
    assert!(diagnostics.error.is_none());
    let rejected = produce_float_reported(-0.6, &unsigned, &mut diagnostics);
    assert_eq!(rejected.value, 0.0);
    assert_eq!(
        rejected.event,
        Some(overflow_event("-1".to_owned(), FieldTypeCode::Double))
    );
    assert_eq!(
        diagnostics.error.unwrap().message(),
        "constant -1 overflows double"
    );

    let float = FieldType::new(FieldTypeCode::Float)
        .with_flen(40)
        .with_decimal(0);
    let mut diagnostics = Diagnostics::new(Some(&context));
    let converted = produce_float_reported(1e100, &float, &mut diagnostics);
    assert!(converted.value.is_finite());
    assert!(converted.value > f64::from(f32::MAX)); // TruncateFloat error returns BEFORE the FLOAT range clamp.
    assert_eq!(
        converted.event,
        Some(overflow_event(
            "DOUBLE value is out of range".to_owned(),
            FieldTypeCode::Float
        ))
    );
    assert_eq!(
        diagnostics.error.unwrap().message(),
        "DOUBLE value is out of range in ''"
    );
    let typed = Datum::Real(1e100)
        .convert_to_in_context(&float, &context, &SessionTimeZone::utc())
        .unwrap();
    let Datum::Float32(value) = typed.value else {
        panic!("FLOAT storage")
    };
    assert_eq!(value, f64::INFINITY); // Existing outer conversion still narrows the returned value.
    let error = typed.error.unwrap();
    assert_eq!(error.identity(), ERR_OVERFLOW.identity());
    assert_eq!(error.message(), "DOUBLE value is out of range in ''");
    let first = Datum::new_string("1e100x")
        .convert_to_in_context(&float, &context, &SessionTimeZone::utc())
        .unwrap();
    let Datum::Float32(value) = first.value else {
        panic!("best-effort FLOAT storage")
    };
    assert_eq!(value, f64::INFINITY);
    let error = first.error.unwrap();
    assert_eq!(error.identity(), ERR_TRUNCATED_WRONG_VALUE.identity());
    assert_eq!(
        error.message(),
        "Truncated incorrect DOUBLE value: '1e100x'"
    );

    let float = FieldType::new(FieldTypeCode::Float);
    assert_eq!(
        produce_float_with_type(1e40, &float).value,
        f64::from(f32::MAX)
    );
    let unknown = FieldType::new(FieldTypeCode::Unknown(4));
    let untouched = produce_float_with_type(1e40, &unknown);
    assert_eq!(untouched.value, 1e40);
    assert_eq!(untouched.event, None); // Unknown(4) is NOT the known FLOAT code.
}
