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

//! Scalar conversion primitives transcreated from `pkg/types/convert.go`.

#[cfg(test)]
use crate::TimeType;
use std::fmt;
use tidb_query_datatype::codec::native_duration_convert as shared_duration_convert;
use tidb_query_datatype::codec::native_integer_convert as shared_integer_convert;

use crate::{
    BinaryJSON, BinaryLiteral, ConversionFlags, Decimal, FieldTypeCode, MySqlDuration, MysqlEnum,
    MysqlSet, Time,
};
#[cfg(test)]
use crate::{
    JSON_TYPE_CODE_ARRAY, JSON_TYPE_CODE_FLOAT64, JSON_TYPE_CODE_INT64, JSON_TYPE_CODE_LITERAL,
    JSON_TYPE_CODE_OBJECT, JSON_TYPE_CODE_STRING, JSON_TYPE_CODE_UINT64,
};

/// Failure returned with the source-compatible saturated conversion result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScalarConversionError {
    /// The input is outside the target MySQL integer domain.
    Overflow {
        /// The value rendered for the source `ErrOverflow` argument.
        value: String,
        /// Target MySQL field type.
        target: FieldTypeCode,
    },
    /// The exponent following `e` or `E` is not a signed decimal integer.
    InvalidScientificExponent(String),
    /// The integral part is not an unsigned decimal integer.
    InvalidUnsignedInteger(String),
}

impl fmt::Display for ScalarConversionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Overflow { value, target } => {
                write!(formatter, "{value} is out of range for {target:?}")
            }
            Self::InvalidScientificExponent(value) => {
                write!(formatter, "invalid scientific exponent in {value:?}")
            }
            Self::InvalidUnsignedInteger(value) => {
                write!(formatter, "invalid unsigned integer {value:?}")
            }
        }
    }
}

impl std::error::Error for ScalarConversionError {}

/// Non-fatal source event normally routed through `Context.HandleTruncate`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScalarConversionEvent {
    /// The accepted numeric prefix did not consume the input.
    Truncated,
    /// `ProduceDecWithSpecifiedTp` rounded a well-formed value to the target
    /// scale. Go appends this to the warning list directly rather than
    /// returning it, so it never becomes a statement error even in strict
    /// mode -- unlike [`Self::Truncated`], which does.
    RoundedToScale,
    /// Conversion saturated at a target boundary.
    Overflow(ScalarConversionError),
    /// A TIMESTAMP wall clock fell in a daylight-saving gap and was adjusted
    /// to Go's closest valid transition boundary. The statement layer keeps
    /// this diagnostic distinct from ordinary truncation because Go reports
    /// errno 8179 while still storing the adjusted value.
    TimestampInDSTTransition,
}

/// A best-effort source conversion result and its warning/error event.
#[derive(Debug, Clone, PartialEq)]
pub struct Converted<T> {
    /// Value returned by Go beside the error.
    pub value: T,
    /// Event whose final error/warning policy belongs to the caller context.
    pub event: Option<ScalarConversionEvent>,
}

impl<T> Converted<T> {
    fn exact(value: T) -> Self {
        Self { value, event: None }
    }

    fn truncated(value: T) -> Self {
        Self {
            value,
            event: Some(ScalarConversionEvent::Truncated),
        }
    }
}

/// Result of `StrToDuration`, which deliberately accepts datetime syntax.
#[derive(Debug, Clone, PartialEq)]
pub enum DurationOrTime {
    /// Ordinary MySQL TIME input.
    Duration(MySqlDuration),
    /// Twelve-or-more-digit datetime input accepted by the source fallback.
    Time(Time),
}

fn overflow(value: impl ToString, target: FieldTypeCode) -> ScalarConversionError {
    ScalarConversionError::Overflow {
        value: value.to_string(),
        target,
    }
}

pub(crate) fn from_shared_integer_error(
    error: shared_integer_convert::NativeIntegerError,
) -> ScalarConversionError {
    use shared_integer_convert::NativeIntegerError;
    use tidb_query_datatype::codec::native_type_name::NativeTypeNameCode;
    match error {
        NativeIntegerError::Overflow { value, target } => ScalarConversionError::Overflow {
            value,
            target: match target {
                NativeTypeNameCode::Known(code) => FieldTypeCode::from_mysql_type(code),
                NativeTypeNameCode::Unknown(code) => FieldTypeCode::Unknown(code),
            },
        },
        NativeIntegerError::InvalidUnsignedInteger(value) => {
            ScalarConversionError::InvalidUnsignedInteger(value)
        }
        NativeIntegerError::InvalidScientificExponent(value) => {
            ScalarConversionError::InvalidScientificExponent(value)
        }
    }
}

fn from_shared_integer_event(
    event: shared_integer_convert::NativeIntegerEvent,
) -> ScalarConversionEvent {
    match event {
        shared_integer_convert::NativeIntegerEvent::Truncated => ScalarConversionEvent::Truncated,
        shared_integer_convert::NativeIntegerEvent::Overflow(error) => {
            ScalarConversionEvent::Overflow(from_shared_integer_error(error))
        }
    }
}

pub(crate) fn from_shared_integer_conversion<T>(
    converted: shared_integer_convert::NativeIntegerConverted<T>,
) -> Converted<T> {
    Converted {
        value: converted.value,
        event: converted.event.map(from_shared_integer_event),
    }
}

fn apply_integer_diagnostic(
    diagnostics: &mut crate::datum_convert::diagnostics::Diagnostics<'_, '_>,
    effect: shared_integer_convert::NativeIntegerDiagnostic<'_>,
) {
    use shared_integer_convert::NativeIntegerDiagnostic;
    match effect {
        NativeIntegerDiagnostic::TruncatedNumericInput(input) => {
            diagnostics.truncated_numeric_input(input)
        }
        NativeIntegerDiagnostic::ParsedInteger(event) => {
            let event = from_shared_integer_event(event.clone());
            diagnostics.parsed_integer(Some(&event));
        }
        NativeIntegerDiagnostic::ErrorOverflow(input) => diagnostics.error(|| {
            crate::ERR_OVERFLOW.generate(format!("BIGINT value is out of range in '{input}'"))
        }),
        NativeIntegerDiagnostic::ReplaceUnsignedOverflow(input) => {
            diagnostics.replace_error(|| {
                crate::ERR_OVERFLOW.generate(format!(
                    "BIGINT UNSIGNED value is out of range in '{input}'"
                ))
            })
        }
    }
}

/// `IntegerUnsignedUpperBound`.
pub const fn integer_unsigned_upper_bound(target: FieldTypeCode) -> u64 {
    shared_integer_convert::native_integer_unsigned_upper_bound(target.as_shared_type_name_code())
}

/// `IntegerSignedUpperBound`.
pub const fn integer_signed_upper_bound(target: FieldTypeCode) -> i64 {
    shared_integer_convert::native_integer_signed_upper_bound(target.as_shared_type_name_code())
}

/// `IntegerSignedLowerBound`.
pub const fn integer_signed_lower_bound(target: FieldTypeCode) -> i64 {
    shared_integer_convert::native_integer_signed_lower_bound(target.as_shared_type_name_code())
}

/// `ConvertFloatToInt`, including MySQL half-away-from-zero rounding.
pub fn convert_float_to_int(
    value: f64,
    lower_bound: i64,
    upper_bound: i64,
    target: FieldTypeCode,
) -> Result<i64, (i64, ScalarConversionError)> {
    shared_integer_convert::native_convert_float_to_int(
        value,
        lower_bound,
        upper_bound,
        target.as_shared_type_name_code(),
    )
    .map_err(|(value, error)| (value, from_shared_integer_error(error)))
}

/// `ConvertIntToInt`.
pub fn convert_int_to_int(
    value: i64,
    lower_bound: i64,
    upper_bound: i64,
    target: FieldTypeCode,
) -> Result<i64, (i64, ScalarConversionError)> {
    shared_integer_convert::native_convert_int_to_int(
        value,
        lower_bound,
        upper_bound,
        target.as_shared_type_name_code(),
    )
    .map_err(|(value, error)| (value, from_shared_integer_error(error)))
}

/// `ConvertUintToInt`.
pub fn convert_uint_to_int(
    value: u64,
    upper_bound: i64,
    target: FieldTypeCode,
) -> Result<i64, (i64, ScalarConversionError)> {
    shared_integer_convert::native_convert_uint_to_int(
        value,
        upper_bound,
        target.as_shared_type_name_code(),
    )
    .map_err(|(value, error)| (value, from_shared_integer_error(error)))
}

/// `ConvertIntToUint`.
pub fn convert_int_to_uint(
    flags: ConversionFlags,
    value: i64,
    upper_bound: u64,
    target: FieldTypeCode,
) -> Result<u64, (u64, ScalarConversionError)> {
    shared_integer_convert::native_convert_int_to_uint(
        flags.bits(),
        value,
        upper_bound,
        target.as_shared_type_name_code(),
    )
    .map_err(|(value, error)| (value, from_shared_integer_error(error)))
}

/// `ConvertUintToUint`.
pub fn convert_uint_to_uint(
    value: u64,
    upper_bound: u64,
    target: FieldTypeCode,
) -> Result<u64, (u64, ScalarConversionError)> {
    shared_integer_convert::native_convert_uint_to_uint(
        value,
        upper_bound,
        target.as_shared_type_name_code(),
    )
    .map_err(|(value, error)| (value, from_shared_integer_error(error)))
}

/// `ConvertFloatToUint`.
///
/// Go's `big.Float.SetFloat64` panics for NaN; infinities remain accepted and
/// are reported as out-of-range by the subsequent `Uint64` conversion.
pub fn convert_float_to_uint(
    flags: ConversionFlags,
    value: f64,
    upper_bound: u64,
    target: FieldTypeCode,
) -> Result<u64, (u64, ScalarConversionError)> {
    shared_integer_convert::native_convert_float_to_uint(
        flags.bits(),
        value,
        upper_bound,
        target.as_shared_type_name_code(),
    )
    .map_err(|(value, error)| (value, from_shared_integer_error(error)))
}

/// Expands the scientific notation accepted by `convertScientificNotation`.
pub fn convert_scientific_notation(input: &str) -> Result<String, ScalarConversionError> {
    shared_integer_convert::native_convert_scientific_notation(input)
        .map_err(from_shared_integer_error)
}

/// `convertDecimalStrToUint`, kept public because decimal conversion delegates
/// through this exact string path to avoid float precision loss.
pub fn convert_decimal_str_to_uint(
    input: &str,
    upper_bound: u64,
    target: FieldTypeCode,
) -> Result<u64, (u64, ScalarConversionError)> {
    shared_integer_convert::native_convert_decimal_str_to_uint(
        input,
        upper_bound,
        target.as_shared_type_name_code(),
    )
    .map_err(|(value, error)| (value, from_shared_integer_error(error)))
}

/// `ConvertDecimalToUint`.
pub fn convert_decimal_to_uint(
    value: &Decimal,
    upper_bound: u64,
    target: FieldTypeCode,
) -> Result<u64, (u64, ScalarConversionError)> {
    shared_integer_convert::native_convert_decimal_to_uint(
        value.as_shared_parse(),
        upper_bound,
        target.as_shared_type_name_code(),
    )
    .map_err(|(value, error)| (value, from_shared_integer_error(error)))
}

/// A source numeric prefix plus the truncation event that `Context` decides
/// whether to return, ignore, or publish as a warning.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NumericPrefix {
    value: String,
    truncated: bool,
}

/// Returns the subject Go includes in a `Truncated incorrect DOUBLE value`
/// diagnostic. `StrToFloat` trims Unicode whitespace before
/// `getValidFloatPrefix`, and that helper shortens the same subject at the
/// first NUL byte before formatting the error.
pub fn float_warning_input(input: &str) -> &str {
    tidb_query_datatype::codec::native_float_parse::native_float_warning_input(input)
}

/// Go's `ErrTruncatedWrongVal` template caps the quoted subject at 128 bytes
/// (`"Truncated incorrect %-.64s value: '%-.128s'"`). The cut rounds down to
/// a char boundary: identical for ASCII, multi-byte input loses the partial
/// rune Go's byte cut would have split.
pub use tidb_query_datatype::codec::convert::native_warning_subject_byte_cap as warning_subject_byte_cap;

impl NumericPrefix {
    /// Prefix accepted by Go's `strconv` call.
    pub fn value(&self) -> &str {
        &self.value
    }

    /// Whether source `Context.HandleTruncate` is invoked.
    pub const fn truncated(&self) -> bool {
        self.truncated
    }
}

/// `getValidFloatPrefix` without hiding its truncation event in an error.
pub fn valid_float_prefix(input: &str, is_function_cast: bool) -> NumericPrefix {
    let prefix = tidb_query_datatype::codec::native_float_parse::native_valid_float_prefix(
        input,
        is_function_cast,
    );
    NumericPrefix {
        value: prefix.value.to_owned(),
        truncated: prefix.truncated,
    }
}

/// `roundIntStr`.
pub fn round_integer_string(next_fraction_digit: u8, integer: &str) -> String {
    shared_integer_convert::native_round_integer_string(next_fraction_digit, integer)
}

/// `floatStrToIntStr`. The error carries the same saturated BIGINT text used
/// by the Go caller for an exponent too large to materialize.
pub fn float_string_to_integer_string(
    valid_float: &str,
    original: &str,
) -> Result<String, (String, ScalarConversionError)> {
    shared_integer_convert::native_float_string_to_integer_string(valid_float, original)
        .map_err(|(value, error)| (value, from_shared_integer_error(error)))
}

/// `getValidIntPrefix`.
///
/// `truncate_as_warning` mirrors the source `Context` truncation flag, and it
/// is a parameter rather than a caller-side policy because the two policies
/// return DIFFERENT VALUES. Go returns early with the *float* prefix when
/// `HandleTruncate` turns the truncation into a statement error, and otherwise
/// falls through to `floatStrToIntStr` and returns the *integer* prefix beside
/// a warning: `"123..34"` is `"123."` in strict mode and `"123"` in warning
/// mode. Handing the caller one prefix plus an error would make one of those
/// two answers unreachable.
pub fn valid_integer_prefix(
    input: &str,
    is_function_cast: bool,
    truncate_as_warning: bool,
) -> Result<Converted<String>, (String, ScalarConversionError)> {
    shared_integer_convert::native_valid_integer_prefix(
        input,
        is_function_cast,
        truncate_as_warning,
    )
    .map(from_shared_integer_conversion)
    .map_err(|(value, error)| (value, from_shared_integer_error(error)))
}

/// `StrToInt`, preserving the best-effort value and truncation/overflow event.
pub fn str_to_int(input: &str, is_function_cast: bool) -> Converted<i64> {
    from_shared_integer_conversion(shared_integer_convert::native_str_to_int(
        input,
        is_function_cast,
    ))
}

/// `StrToInt` with the source context's `HandleTruncate` decision.
///
/// When truncation is an error, Go returns the float prefix directly from
/// `getValidIntPrefix`; `strconv.ParseInt` then turns a prefix containing a
/// decimal point or exponent into the BIGINT overflow result. When truncation
/// is ignored or becomes a warning, Go continues through `floatStrToIntStr`.
pub(crate) fn str_to_int_with_truncate_policy(
    input: &str,
    is_function_cast: bool,
    truncate_as_warning: bool,
) -> Converted<i64> {
    from_shared_integer_conversion(shared_integer_convert::native_str_to_int_reported(
        input,
        is_function_cast,
        truncate_as_warning,
        |_| {},
    ))
}

pub(crate) fn str_to_int_reported(
    input: &str,
    is_function_cast: bool,
    truncate_as_warning: bool,
    diagnostics: &mut crate::datum_convert::diagnostics::Diagnostics<'_, '_>,
) -> Converted<i64> {
    from_shared_integer_conversion(shared_integer_convert::native_str_to_int_reported(
        input,
        is_function_cast,
        truncate_as_warning,
        |effect| apply_integer_diagnostic(diagnostics, effect),
    ))
}

/// `StrToUint`, including the source rule that only negative zero is valid.
pub fn str_to_uint(input: &str, is_function_cast: bool) -> Converted<u64> {
    from_shared_integer_conversion(shared_integer_convert::native_str_to_uint(
        input,
        is_function_cast,
    ))
}

/// `StrToUint` with the source context's `HandleTruncate` decision. See
/// [`str_to_int_with_truncate_policy`] for why strict truncation keeps the
/// unrounded float prefix.
pub(crate) fn str_to_uint_with_truncate_policy(
    input: &str,
    is_function_cast: bool,
    truncate_as_warning: bool,
) -> Converted<u64> {
    from_shared_integer_conversion(shared_integer_convert::native_str_to_uint_reported(
        input,
        is_function_cast,
        truncate_as_warning,
        |_| {},
    ))
}

pub(crate) fn str_to_uint_reported(
    input: &str,
    is_function_cast: bool,
    truncate_as_warning: bool,
    diagnostics: &mut crate::datum_convert::diagnostics::Diagnostics<'_, '_>,
) -> Converted<u64> {
    from_shared_integer_conversion(shared_integer_convert::native_str_to_uint_reported(
        input,
        is_function_cast,
        truncate_as_warning,
        |effect| apply_integer_diagnostic(diagnostics, effect),
    ))
}

/// `StrToFloat`.
pub fn str_to_float(input: &str, is_function_cast: bool) -> Converted<f64> {
    str_to_float_reported(
        input,
        is_function_cast,
        &mut crate::datum_convert::diagnostics::Diagnostics::new(None),
    )
}

pub(crate) fn str_to_float_reported(
    input: &str,
    is_function_cast: bool,
    diagnostics: &mut crate::datum_convert::diagnostics::Diagnostics<'_, '_>,
) -> Converted<f64> {
    use tidb_query_datatype::codec::native_float_parse::{
        native_str_to_float_reported, NativeFloatDiagnostic,
    };
    let converted =
        native_str_to_float_reported(input, is_function_cast, |diagnostic| match diagnostic {
            NativeFloatDiagnostic::TruncatedNumericInput(input) => {
                diagnostics.truncated_numeric_input(input);
            }
            NativeFloatDiagnostic::UnhandledTruncated => {
                diagnostics.unhandled(Some(&ScalarConversionEvent::Truncated));
            }
        });
    Converted {
        value: converted.value,
        event: converted
            .truncated
            .then_some(ScalarConversionEvent::Truncated),
    }
}

/// `StrToDateTime`.
pub fn str_to_datetime<TZ: chrono::TimeZone>(
    input: &str,
    fsp: i64,
    timezone: &TZ,
) -> Result<Converted<Time>, crate::TimeError> {
    shared_duration_convert::native_str_to_datetime(input, fsp, timezone)
        .map(|value| from_shared_duration_conversion(value, time_from_shared_duration))
}

/// `StrToDuration`.
pub fn str_to_duration<TZ: chrono::TimeZone>(
    input: &str,
    fsp: i64,
    timezone: &TZ,
) -> Result<Converted<DurationOrTime>, crate::DurationValueError> {
    shared_duration_convert::native_str_to_duration(input, fsp, timezone).map(|value| {
        from_shared_duration_conversion(value, |value| match value {
            shared_duration_convert::NativeDurationOrTime::Duration(value) => {
                DurationOrTime::Duration(MySqlDuration::from_raw_parts(
                    value.nanoseconds,
                    value.fsp,
                ))
            }
            shared_duration_convert::NativeDurationOrTime::Time(value) => {
                DurationOrTime::Time(time_from_shared_duration(value))
            }
        })
    })
}

/// `NumberToDuration`.
pub fn number_to_duration(
    number: i64,
    fsp: i64,
) -> Result<Converted<MySqlDuration>, crate::TimeError> {
    shared_duration_convert::native_number_to_duration(number, fsp).map(|value| {
        from_shared_duration_conversion(value, |value| {
            MySqlDuration::from_raw_parts(value.nanoseconds, value.fsp)
        })
    })
}

fn time_from_shared_duration(
    value: tidb_query_datatype::codec::mysql::time::NativeTemporalValue,
) -> Time {
    Time::from_raw_parts(crate::CoreTime::from_raw(value.raw), value.kind, value.fsp)
}

pub(crate) fn from_shared_duration_conversion<T, U>(
    converted: shared_duration_convert::NativeDurationConverted<T>,
    project: impl FnOnce(T) -> U,
) -> Converted<U> {
    Converted {
        value: project(converted.value),
        event: converted.event.map(|event| match event {
            shared_duration_convert::NativeDurationConvertEvent::Truncated => {
                ScalarConversionEvent::Truncated
            }
            shared_duration_convert::NativeDurationConvertEvent::Overflow(value) => {
                ScalarConversionEvent::Overflow(ScalarConversionError::Overflow {
                    value,
                    target: FieldTypeCode::Duration,
                })
            }
        }),
    }
}

#[cfg(test)]
#[test]
fn duration_conversion_facades_keep_raw_rounding_numeric_errors_and_datetime_alternatives() {
    use crate::{round_duration_fsp, DurationRoundError, FspError};
    let rounded = round_duration_fsp(123, 6, 9).unwrap();
    assert_eq!((rounded.nanoseconds(), rounded.fsp()), (123, 6));
    assert_eq!(
        round_duration_fsp(-1_500_000, 6, 3).unwrap().nanoseconds(),
        -1_000_000
    );
    assert_eq!(
        round_duration_fsp(-1_500_001, 6, 3).unwrap().nanoseconds(),
        -2_000_000
    );
    assert_eq!(
        round_duration_fsp(123, -2, -2),
        Err(DurationRoundError::InvalidFsp(FspError::InvalidFsp(-2)))
    );
    assert_eq!(
        round_duration_fsp(i64::MAX, 6, 0),
        Err(DurationRoundError::Overflow)
    );
    assert_eq!(
        DurationRoundError::Overflow.to_string(),
        "rounded duration is out of range"
    );
    assert_eq!(format!("{:?}", DurationRoundError::Overflow), "Overflow");

    let saturated = number_to_duration(i64::MIN, 6).unwrap();
    assert_eq!(
        (saturated.value.nanoseconds(), saturated.value.fsp()),
        (-3_020_399_000_000_000, 6)
    );
    assert_eq!(
        saturated.event,
        Some(ScalarConversionEvent::Overflow(
            ScalarConversionError::Overflow {
                value: "-9223372036854775808".into(),
                target: FieldTypeCode::Duration,
            }
        ))
    );
    let zero = number_to_duration(60, -2).unwrap();
    assert_eq!((zero.value.nanoseconds(), zero.value.fsp()), (0, 0));
    assert_eq!(zero.event, Some(ScalarConversionEvent::Truncated));
    assert_eq!(
        number_to_duration(0, -2),
        Err(crate::TimeError::InvalidFsp(FspError::InvalidFsp(-2)))
    );
    let numeric_date = number_to_duration(20_190_412_123_456, 3).unwrap();
    assert_eq!(
        (numeric_date.value.nanoseconds(), numeric_date.value.fsp()),
        (45_296_000_000_000, 3)
    );
    assert_eq!(numeric_date.event, None);

    let datetime = str_to_datetime("2019-04-12 12:34:56", 3, &chrono_tz::UTC).unwrap();
    assert_eq!(
        datetime.value.core_time(),
        crate::CoreTime::from_date(2019, 4, 12, 12, 34, 56, 0)
    );
    assert_eq!(
        (datetime.value.kind(), datetime.value.fsp()),
        (TimeType::DateTime, 3)
    );
    assert_eq!(datetime.event, None);
    let alternative = str_to_duration(" 20190412123456 ", 3, &chrono_tz::UTC).unwrap();
    assert_eq!(alternative.value, DurationOrTime::Time(datetime.value));
    assert_eq!(alternative.event, None);
    let duration = str_to_duration("12:34:56", 0, &chrono_tz::UTC).unwrap();
    assert_eq!(
        duration.value,
        DurationOrTime::Duration(MySqlDuration::from_raw_parts(45_296_000_000_000, 0))
    );
    assert_eq!(duration.event, None);
    let overflowed = str_to_duration(" 839:00:00 ", 0, &chrono_tz::UTC).unwrap();
    assert_eq!(
        overflowed.value,
        DurationOrTime::Duration(MySqlDuration::from_raw_parts(3_020_399_000_000_000, 0))
    );
    assert_eq!(
        overflowed.event,
        Some(ScalarConversionEvent::Overflow(
            ScalarConversionError::Overflow {
                value: "839:00:00".into(),
                target: FieldTypeCode::Duration,
            }
        ))
    );
}

#[cfg(test)]
#[test]
fn integer_text_facades_preserve_prefix_domains_and_typed_diagnostic_order() {
    assert_eq!(round_integer_string(b'5', "99"), "100");
    assert_eq!(
        float_string_to_integer_string("-0.5", "-0.5").unwrap(),
        "-1"
    );
    assert_eq!(
        valid_integer_prefix("123..34", false, false),
        Err((
            "123.".into(),
            ScalarConversionError::InvalidUnsignedInteger("123..34".into())
        ))
    );
    assert_eq!(
        str_to_int(" 3.5 ", false),
        Converted {
            value: 4,
            event: None
        }
    );
    assert_eq!(
        str_to_int("3.5", true),
        Converted {
            value: 3,
            event: Some(ScalarConversionEvent::Truncated)
        }
    );
    assert_eq!(
        str_to_int_with_truncate_policy("3.5tail", false, false),
        Converted {
            value: 0,
            event: Some(ScalarConversionEvent::Overflow(
                ScalarConversionError::Overflow {
                    value: "3.5".into(),
                    target: FieldTypeCode::LongLong,
                }
            )),
        }
    );
    assert_eq!(
        str_to_int_with_truncate_policy("3.5tail", false, true),
        Converted {
            value: 4,
            event: Some(ScalarConversionEvent::Truncated),
        }
    );
    assert_eq!(
        str_to_uint("-000", false),
        Converted {
            value: 0,
            event: None
        }
    );
    assert_eq!(
        str_to_uint("-1e100", false),
        Converted {
            value: 0,
            event: Some(ScalarConversionEvent::Overflow(
                ScalarConversionError::Overflow {
                    value: "-9223372036854775808".into(),
                    target: FieldTypeCode::LongLong,
                }
            )),
        }
    );

    #[derive(Default)]
    struct Warnings(std::cell::RefCell<Vec<String>>);
    impl crate::ConversionWarningAppender for Warnings {
        fn append_conversion_warning(&self, error: tidb_error::terror::TerrorError) {
            self.0.borrow_mut().push(error.to_string());
        }
    }
    let warnings = Warnings::default();
    let flags = crate::DEFAULT_STATEMENT_FLAGS.with_truncate_as_warning(true);
    let context = crate::ConversionContext::new(flags, crate::ConversionLocation::UTC, &warnings);
    let target = crate::FieldType::new(FieldTypeCode::LongLong)
        .with_added_flags(crate::FieldTypeFlags::UNSIGNED);
    let converted = crate::Datum::new_string("-1e100tail")
        .convert_to_in_context(&target, &context, &crate::SessionTimeZone::utc())
        .unwrap();
    assert_eq!(converted.value, crate::Datum::UInt(0));
    assert_eq!(
        converted.error.unwrap().to_string(),
        "[types:1690]BIGINT UNSIGNED value is out of range in '-9223372036854775808'"
    );
    assert_eq!(
        *warnings.0.borrow(),
        vec!["[types:1292]Truncated incorrect DOUBLE value: '-1e100tail'"]
    );
}

#[cfg(test)]
#[test]
fn integer_json_facades_preserve_target_identity_flags_raw_tags_and_panics() {
    let flags = ConversionFlags::from_bits(0);
    assert_eq!(integer_unsigned_upper_bound(FieldTypeCode::Bit), u64::MAX);
    assert_eq!(integer_signed_lower_bound(FieldTypeCode::Enum), 0);
    assert_eq!(integer_signed_upper_bound(FieldTypeCode::Enum), 65535);
    assert!(
        std::panic::catch_unwind(|| integer_signed_upper_bound(FieldTypeCode::Unknown(8))).is_err()
    );
    assert_eq!(
        convert_int_to_int(200, -128, 127, FieldTypeCode::Unknown(1)),
        Err((
            127,
            ScalarConversionError::Overflow {
                value: "200".into(),
                target: FieldTypeCode::Unknown(1),
            }
        ))
    );
    assert_eq!(
        convert_float_to_int(2.5, i64::MIN, i64::MAX, FieldTypeCode::LongLong),
        Ok(2)
    );
    let minus_one =
        BinaryJSON::from_encoded_parts(JSON_TYPE_CODE_INT64, (-1_i64).to_le_bytes().to_vec());
    assert_eq!(json_to_int64(&minus_one, true, flags).value, 0);
    assert_eq!(
        json_to_int64(
            &minus_one,
            true,
            flags.with_allow_negative_to_unsigned(true)
        ),
        Converted {
            value: -1,
            event: None
        }
    );
    let text = BinaryJSON::parse(r#""18446744073709551616""#).unwrap();
    assert_eq!(
        json_to_int(&text, false, FieldTypeCode::Tiny, flags),
        Converted {
            value: -1,
            event: Some(ScalarConversionEvent::Overflow(
                ScalarConversionError::Overflow {
                    value: "18446744073709551616".into(),
                    target: FieldTypeCode::LongLong,
                }
            )),
        }
    );
    let negative = BinaryJSON::parse(r#""-1""#).unwrap();
    let spaced = BinaryJSON::parse(r#"" -1""#).unwrap();
    assert_eq!(
        json_to_int(&negative, true, FieldTypeCode::Tiny, flags),
        Converted {
            value: -1,
            event: None
        }
    );
    assert_eq!(
        json_to_int64(&spaced, false, flags),
        Converted {
            value: 0,
            event: Some(ScalarConversionEvent::Overflow(
                ScalarConversionError::Overflow {
                    value: "-1".into(),
                    target: FieldTypeCode::LongLong
                }
            )),
        }
    );
    assert_eq!(
        json_to_int64(
            &BinaryJSON::from_encoded_parts(JSON_TYPE_CODE_LITERAL, vec![255]),
            false,
            flags
        ),
        Converted {
            value: 1,
            event: None
        }
    );
    assert_eq!(
        json_to_int64(
            &BinaryJSON::from_encoded_parts(JSON_TYPE_CODE_STRING, vec![1, 255]),
            false,
            flags
        ),
        Converted {
            value: 0,
            event: Some(ScalarConversionEvent::Truncated)
        }
    );
    let malformed = BinaryJSON::from_encoded_parts(JSON_TYPE_CODE_INT64, Vec::new());
    assert!(std::panic::catch_unwind(|| json_to_int64(&malformed, false, flags)).is_err());
    let nan =
        BinaryJSON::from_encoded_parts(JSON_TYPE_CODE_FLOAT64, f64::NAN.to_le_bytes().to_vec());
    assert_eq!(
        json_to_int64(&nan, false, flags),
        Converted {
            value: 0,
            event: None
        }
    );
    assert!(std::panic::catch_unwind(|| json_to_int64(&nan, true, flags)).is_err());
}

/// `ConvertJSONToInt`.
pub fn json_to_int(
    json: &BinaryJSON,
    unsigned: bool,
    target: FieldTypeCode,
    flags: ConversionFlags,
) -> Converted<i64> {
    from_shared_integer_conversion(shared_integer_convert::native_json_to_int(
        json.type_code(),
        json.value(),
        unsigned,
        target.as_shared_type_name_code(),
        flags.bits(),
    ))
}

/// `ConvertJSONToInt64`.
pub fn json_to_int64(json: &BinaryJSON, unsigned: bool, flags: ConversionFlags) -> Converted<i64> {
    from_shared_integer_conversion(shared_integer_convert::native_json_to_int64(
        json.type_code(),
        json.value(),
        unsigned,
        flags.bits(),
    ))
}

/// `ConvertJSONToFloat`.
pub fn json_to_float(json: &BinaryJSON) -> Converted<f64> {
    let converted = tidb_query_datatype::codec::native_scalar_convert::native_json_to_float(
        json.type_code(),
        json.value(),
    );
    Converted {
        value: converted.value,
        event: converted
            .truncated
            .then_some(ScalarConversionEvent::Truncated),
    }
}

/// `MyDecimal.FromString`, which is prefix-accepting: it keeps the longest
/// leading `[space|tab]* [sign] digits [. digits] [eE exponent]` it can read
/// and reports `ErrTruncated` for whatever follows. Go returns that partial
/// value beside the error, so `'1,999.00'` converts to `1`, not to `0`.
///
/// The whole rule -- prefix acceptance, the fixed word buffer, and the
/// `MaxInt32/2` exponent clamp -- lives in [`Decimal::parse_mysql`], which is
/// the byte-for-byte transcreation of `FromString`. This function only maps its
/// error disposition onto the conversion event. It deliberately does NOT expand
/// the exponent into a digit string first: `convert_scientific_notation` is the
/// transcreation of a DIFFERENT Go function (`convertScientificNotation`, used
/// only by `convertDecimalStrToUint`) and materializes the exponent as literal
/// zeros, so routing decimals through it turned `'1e9223372036854775807'` into a
/// 9.2-exabyte allocation instead of Go's clamped 81 nines plus `ErrOverflow`.
pub(crate) fn decimal_from_text(text: &str) -> Converted<Decimal> {
    from_shared_decimal_conversion(
        tidb_query_datatype::codec::native_decimal_convert::native_decimal_from_text(text),
    )
}

pub(crate) fn from_shared_decimal_conversion(
    converted: tidb_query_datatype::codec::native_decimal_convert::NativeDecimalConverted,
) -> Converted<Decimal> {
    use tidb_query_datatype::codec::native_decimal_convert::NativeDecimalConversionEvent;
    Converted {
        value: Decimal::from_shared_parse(converted.value),
        event: converted.event.map(|event| match event {
            NativeDecimalConversionEvent::Truncated => ScalarConversionEvent::Truncated,
            NativeDecimalConversionEvent::Overflow { value } => {
                ScalarConversionEvent::Overflow(overflow(&value, FieldTypeCode::NewDecimal))
            }
        }),
    }
}

/// `ConvertJSONToDecimal`.
pub fn json_to_decimal(json: &BinaryJSON) -> Converted<Decimal> {
    from_shared_decimal_conversion(
        tidb_query_datatype::codec::native_decimal_convert::native_json_to_decimal(
            json.type_code(),
            json.value(),
        ),
    )
}

/// Typed replacement for Go `ToString(any)`.
pub enum ScalarStringValue<'a> {
    /// Boolean renders as `1` or `0`.
    Bool(bool),
    /// Signed integer.
    Int(i64),
    /// Unsigned integer.
    Uint(u64),
    /// Source float32 formatting.
    Float32(f32),
    /// Source float64 formatting.
    Float64(f64),
    /// UTF-8 string.
    String(&'a str),
    /// Raw byte string.
    Bytes(&'a [u8]),
    /// MySQL temporal.
    Time(Time),
    /// MySQL duration.
    Duration(MySqlDuration),
    /// Exact decimal.
    Decimal(&'a Decimal),
    /// Binary literal.
    BinaryLiteral(&'a BinaryLiteral),
    /// MySQL enum.
    Enum(&'a MysqlEnum),
    /// MySQL set.
    Set(&'a MysqlSet),
    /// Binary JSON.
    Json(&'a BinaryJSON),
}

/// `ToString`.
pub fn scalar_to_string(value: ScalarStringValue<'_>) -> Result<String, std::str::Utf8Error> {
    match value {
        ScalarStringValue::Bool(value) => Ok(if value { "1" } else { "0" }.to_owned()),
        ScalarStringValue::Int(value) => Ok(value.to_string()),
        ScalarStringValue::Uint(value) => Ok(value.to_string()),
        ScalarStringValue::Float32(value) => Ok(value.to_string()),
        ScalarStringValue::Float64(value) => Ok(value.to_string()),
        ScalarStringValue::String(value) => Ok(value.to_owned()),
        ScalarStringValue::Bytes(value) => std::str::from_utf8(value).map(str::to_owned),
        ScalarStringValue::Time(value) => Ok(value.to_string()),
        ScalarStringValue::Duration(value) => Ok(value.to_string()),
        ScalarStringValue::Decimal(value) => Ok(value.to_string()),
        ScalarStringValue::BinaryLiteral(value) => {
            std::str::from_utf8(value.as_bytes()).map(str::to_owned)
        }
        ScalarStringValue::Enum(value) => value.name().as_utf8().map(str::to_owned),
        ScalarStringValue::Set(value) => value.name().as_utf8().map(str::to_owned),
        ScalarStringValue::Json(value) => Ok(value.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_integer_bounds_and_conversions() {
        assert_eq!(integer_unsigned_upper_bound(FieldTypeCode::Tiny), 255);
        assert_eq!(
            integer_unsigned_upper_bound(FieldTypeCode::Int24),
            16_777_215
        );
        assert_eq!(integer_signed_lower_bound(FieldTypeCode::Int24), -8_388_608);
        assert_eq!(integer_signed_upper_bound(FieldTypeCode::Int24), 8_388_607);

        assert_eq!(
            convert_float_to_int(1.5, i8::MIN.into(), i8::MAX.into(), FieldTypeCode::Tiny),
            Ok(2)
        );
        assert_eq!(
            convert_float_to_int(-1.5, i8::MIN.into(), i8::MAX.into(), FieldTypeCode::Tiny),
            Ok(-2)
        );
        assert_eq!(
            convert_int_to_int(256, 0, 255, FieldTypeCode::Tiny)
                .unwrap_err()
                .0,
            255
        );
        assert_eq!(
            convert_uint_to_int(u64::MAX, i64::MAX, FieldTypeCode::LongLong)
                .unwrap_err()
                .0,
            i64::MAX
        );
        assert_eq!(
            convert_int_to_uint(
                ConversionFlags::from_bits(0),
                -1,
                u64::MAX,
                FieldTypeCode::LongLong
            )
            .unwrap_err()
            .0,
            0
        );
    }

    #[test]
    #[should_panic(expected = "Float.SetFloat64(NaN)")]
    fn convert_float_to_uint_nan_panics_like_go() {
        let _ = convert_float_to_uint(
            ConversionFlags::from_bits(0),
            f64::NAN,
            u64::MAX,
            FieldTypeCode::LongLong,
        );
    }

    /// Source: `pkg/types/convert_test.go::TestConvertScientificNotation`.
    #[test]
    fn test_convert_scientific_notation() {
        for (input, expected) in [
            ("123.456e0", "123.456"),
            ("123.456e1", "1234.56"),
            ("123.456e3", "123456"),
            ("123.456e4", "1234560"),
            ("123.456e5", "12345600"),
            ("123.456e6", "123456000"),
            ("123.456e7", "1234560000"),
            ("123.456e-1", "12.3456"),
            ("123.456e-2", "1.23456"),
            ("123.456e-3", "0.123456"),
            ("123.456e-4", "0.0123456"),
            ("123.456e-5", "0.00123456"),
            ("123.456e-6", "0.000123456"),
            ("123.456e-7", "0.0000123456"),
        ] {
            assert_eq!(
                convert_scientific_notation(input).unwrap(),
                expected,
                "{input}"
            );
        }
        for input in ["123.456e-", "123.456e-7.5", "123.456e"] {
            assert!(convert_scientific_notation(input).is_err(), "{input}");
        }
    }

    #[test]
    fn convert_scientific_notation_supplemental_rows() {
        for (input, expected) in [(".12345E+5", "12345"), ("1E6", "1000000")] {
            assert_eq!(convert_scientific_notation(input).unwrap(), expected);
        }
    }

    /// Source: `pkg/types/convert_test.go::TestConvertDecimalStrToUint`.
    #[test]
    fn test_convert_decimal_str_to_uint() {
        for (input, expected) in [
            ("0.", 0),
            ("72.40", 72),
            ("072.40", 72),
            ("123.456e2", 12_346),
            ("123.456e-2", 1),
            ("072.50000000001", 73),
            (".5757", 1),
            (".12345E+4", 1_235),
            ("9223372036854775807.5", 9_223_372_036_854_775_808),
            ("9223372036854775807.4999", 9_223_372_036_854_775_807),
            ("18446744073709551614.55", u64::MAX),
            ("18446744073709551615.344", u64::MAX),
        ] {
            assert_eq!(
                convert_decimal_str_to_uint(input, u64::MAX, FieldTypeCode::LongLong).unwrap(),
                expected,
                "{input}"
            );
        }
        for (input, expected) in [
            ("18446744073709551615.544", u64::MAX),
            ("-111.111", 0),
            ("-10000000000000000000.0", 0),
        ] {
            assert_eq!(
                convert_decimal_str_to_uint(input, u64::MAX, FieldTypeCode::LongLong)
                    .unwrap_err()
                    .0,
                expected,
                "{input}"
            );
        }
        for input in ["-99.0", "-100.0"] {
            assert_eq!(
                convert_decimal_str_to_uint(input, u64::from(u8::MAX), FieldTypeCode::Tiny)
                    .unwrap_err()
                    .0,
                0,
                "{input}"
            );
        }
    }

    #[test]
    fn shared_decimal_uint_facade_preserves_typed_errors_subjects_and_panic_order() {
        assert_eq!(
            convert_scientific_notation("1e+").unwrap_err(),
            ScalarConversionError::InvalidScientificExponent("1e+".into()),
        );
        assert_eq!(
            convert_decimal_str_to_uint("1.5", u8::MAX.into(), FieldTypeCode::Tiny),
            Ok(2),
        );
        assert_eq!(
            convert_decimal_str_to_uint("25.6e1", u8::MAX.into(), FieldTypeCode::Tiny),
            Err((
                u8::MAX.into(),
                ScalarConversionError::Overflow {
                    value: "256".into(),
                    target: FieldTypeCode::Tiny,
                }
            )),
        );
        assert!(std::panic::catch_unwind(|| {
            convert_decimal_str_to_uint("0.5", 0, FieldTypeCode::Tiny)
        })
        .is_err());
    }

    #[test]
    fn shared_decimal_ref_to_uint_keeps_visible_scale_and_typed_target_errors() {
        for (text, expected) in [("072.500", 73), ("255.4", 255)] {
            assert_eq!(
                convert_decimal_to_uint(
                    &Decimal::from_literal(text),
                    u8::MAX.into(),
                    FieldTypeCode::Tiny,
                ),
                Ok(expected),
            );
        }
        assert_eq!(
            convert_decimal_to_uint(
                &Decimal::from_literal("255.50"),
                u8::MAX.into(),
                FieldTypeCode::Tiny,
            ),
            Err((
                u8::MAX.into(),
                ScalarConversionError::Overflow {
                    value: "255.50".into(),
                    target: FieldTypeCode::Tiny,
                }
            )),
        );
        assert_eq!(
            convert_decimal_to_uint(
                &Decimal::from_literal("-1.00"),
                u8::MAX.into(),
                FieldTypeCode::Tiny,
            ),
            Err((
                0,
                ScalarConversionError::Overflow {
                    value: "-1.00".into(),
                    target: FieldTypeCode::Tiny,
                }
            )),
        );
    }

    /// `pkg/types/convert_test.go:921` `TestGetValidFloat`, first table.
    #[test]
    fn test_get_valid_float() {
        for (input, expected, cast, truncated) in [
            ("-100", "-100", false, false),
            ("1abc", "1", false, true),
            ("-1-1", "-1", false, true),
            ("+1+1", "+1", false, true),
            ("123..34", "123.", false, true),
            ("123.23E-10", "123.23E-10", false, false),
            ("1.1e1.3", "1.1e1", false, true),
            ("11e1.3", "11e1", false, true),
            ("1.1e-13a", "1.1e-13", false, true),
            ("1.", "1.", false, false),
            (".1", ".1", false, false),
            ("", "0", false, true),
            ("", "0", true, false),
            ("123e+", "123", false, true),
            ("0-123", "0", false, true),
            ("9-3", "9", false, true),
            ("1001001\0\0\0", "1001001", false, false),
            ("5e", "5", false, false),
            ("+.e", "0", false, true),
            ("1e5e", "1e5", false, true),
            ("e", "0", false, true),
            ("e123", "0", false, true),
            ("e+", "0", false, true),
        ] {
            let actual = valid_float_prefix(input, cast);
            assert_eq!(actual.value(), expected, "{input:?}");
            assert_eq!(actual.truncated(), truncated, "{input:?}");
        }
        assert_source_float_string_to_integer_rows();
    }

    #[test]
    fn float_warning_input_truncates_at_nul_like_go() {
        assert_eq!(float_warning_input("\0 12"), "");
        assert_eq!(float_warning_input(" 12\0suffix "), "12");
        assert_eq!(float_warning_input(" 12abc "), "12abc");
    }

    /// `pkg/types/convert_test.go:843` `TestGetValidInt`, first table: the
    /// `Context` turns a truncation into a WARNING, so Go falls through to
    /// `floatStrToIntStr` and the answer is an integer prefix. `signed` in the
    /// Go row only selects which `strconv.Parse*` re-parses the prefix; both
    /// succeed for every row, so the obligation it carries is that the prefix
    /// is a parseable integer literal, asserted here directly.
    #[test]
    fn test_get_valid_int() {
        for (input, expected, signed, warned) in [
            ("100", "100", true, false),
            ("-100", "-100", true, false),
            ("9223372036854775808", "9223372036854775808", false, false),
            ("1abc", "1", true, true),
            ("-1-1", "-1", true, true),
            ("+1+1", "+1", true, true),
            ("123..34", "123", true, true),
            ("123.23E-10", "0", true, false),
            ("1.1e1.3", "11", true, true),
            ("11e1.3", "110", true, true),
            ("1.", "1", true, false),
            (".1", "0", true, false),
            ("", "0", true, true),
            ("123e+", "123", true, true),
            ("123de", "123", true, true),
        ] {
            let actual = valid_integer_prefix(input, false, true)
                .unwrap_or_else(|error| panic!("{input:?} must not error: {error:?}"));
            assert_eq!(actual.value, expected, "{input:?}");
            assert_eq!(
                actual.event == Some(ScalarConversionEvent::Truncated),
                warned,
                "{input:?}"
            );
            if signed {
                assert!(actual.value.parse::<i64>().is_ok(), "{input:?}");
            } else {
                assert!(actual.value.parse::<u64>().is_ok(), "{input:?}");
            }
        }
        assert_source_valid_integer_prefix_strict_rows();
    }

    /// `pkg/types/convert_test.go:889` `TestGetValidInt`, second table: the
    /// same inputs under `DefaultStmtFlags`, where a truncation IS the
    /// statement error. Go stops at the float prefix, so `"123..34"` answers
    /// `"123."` here and `"123"` above -- the reason the policy is a parameter
    /// of `valid_integer_prefix` rather than something its caller applies.
    ///
    /// The Rust helper keeps the source value boundary while representing the
    /// numeric error as a typed event. Caller-level tests separately verify
    /// that strict conversion keeps the unrounded float prefix and therefore
    /// reaches the BIGINT overflow path:
    ///
    /// | input       | strict result          | warning result |
    /// | ----------- | ---------------------- | -------------- |
    /// | `"123..34"` | `0`, overflow `"123."` | `123`, truncate |
    /// | `"1.1e1.3"` | `0`, overflow `"1.1e1"`| `11`, truncate  |
    fn assert_source_valid_integer_prefix_strict_rows() {
        for (input, expected, errored) in [
            ("100", "100", false),
            ("-100", "-100", false),
            ("1abc", "1", true),
            ("-1-1", "-1", true),
            ("+1+1", "+1", true),
            ("123..34", "123.", true),
            ("123.23E-10", "0", false),
            ("1.1e1.3", "1.1e1", true),
            ("11e1.3", "11e1", true),
            ("1.", "1", false),
            (".1", "0", false),
            ("", "0", true),
            ("123e+", "123", true),
            ("123de", "123", true),
        ] {
            match valid_integer_prefix(input, false, false) {
                Ok(prefix) => {
                    assert!(!errored, "{input:?} must error");
                    assert_eq!(prefix.value, expected, "{input:?}");
                    assert_eq!(prefix.event, None, "{input:?}");
                }
                Err((value, _)) => {
                    assert!(errored, "{input:?} must not error");
                    assert_eq!(value, expected, "{input:?}");
                }
            }
        }
    }

    #[test]
    fn test_get_valid_int_strict_and_warning_source_rows() {
        for (input, strict_value, warning_value) in [("123..34", 0, 123), ("1.1e1.3", 0, 11)] {
            let strict = str_to_int_with_truncate_policy(input, false, false);
            assert_eq!(strict.value, strict_value, "strict {input:?}");
            assert!(matches!(
                strict.event,
                Some(ScalarConversionEvent::Overflow(_))
            ));

            let warning = str_to_int_with_truncate_policy(input, false, true);
            assert_eq!(warning.value, warning_value, "warning {input:?}");
            assert_eq!(warning.event, Some(ScalarConversionEvent::Truncated));
        }

        for (input, expected, truncated) in [
            ("123abc", 123, true),
            ("-1-1", -1, true),
            ("  12  ", 12, false),
            ("", 0, true),
        ] {
            let strict = str_to_int_with_truncate_policy(input, false, false);
            assert_eq!(strict.value, expected, "{input:?}");
            assert_eq!(
                strict.event == Some(ScalarConversionEvent::Truncated),
                truncated,
                "{input:?}"
            );
        }

        let strict = str_to_uint_with_truncate_policy("123..34", false, false);
        assert_eq!(strict.value, 0);
        assert!(matches!(
            strict.event,
            Some(ScalarConversionEvent::Overflow(_))
        ));
        let warning = str_to_uint_with_truncate_policy("123..34", false, true);
        assert_eq!(warning.value, 123);
        assert_eq!(warning.event, Some(ScalarConversionEvent::Truncated));
    }

    /// `pkg/types/convert_test.go:828` `TestRoundIntStr`. Three rows, and all
    /// three take the all-nines carry that grows the string, including the
    /// signed forms where the carry must land after the sign byte.
    #[test]
    fn test_round_int_str() {
        for (integer, next_fraction_digit, expected) in [
            ("+999", b'5', "+1000"),
            ("999", b'5', "1000"),
            ("-999", b'5', "-1000"),
        ] {
            assert_eq!(
                round_integer_string(next_fraction_digit, integer),
                expected,
                "{integer}"
            );
        }
    }

    /// `pkg/types/convert_test.go:965` `TestGetValidFloat`, second table
    /// (`floatStrToIntStr`), including every saturating-exponent row.
    fn assert_source_float_string_to_integer_rows() {
        for (input, expected) in [
            ("1e5", "100000"),
            ("-123.45678e5", "-12345678"),
            ("+0.5", "1"),
            ("-0.5", "-1"),
            (".5e0", "1"),
            ("+.5e0", "+1"),
            ("-.5e0", "-1"),
            (".5", "1"),
            ("123.456789e5", "12345679"),
            ("123.456784e5", "12345678"),
            ("+999.9999e2", "+100000"),
        ] {
            assert_eq!(
                float_string_to_integer_string(input, input).unwrap(),
                expected,
                "{input}"
            );
        }
        for (input, expected) in [
            ("1e29223372036854775807", u64::MAX.to_string()),
            ("1e9223372036854775807", u64::MAX.to_string()),
            ("125e342", u64::MAX.to_string()),
            ("1e21", u64::MAX.to_string()),
            ("-1e29223372036854775807", i64::MIN.to_string()),
            ("-1e9223372036854775807", i64::MIN.to_string()),
        ] {
            assert_eq!(
                float_string_to_integer_string(input, input).unwrap_err().0,
                expected,
                "{input}"
            );
        }
    }

    /// `getValidIntPrefix`'s `isFuncCast` arm (`CAST(<string> AS
    /// SIGNED/UNSIGNED)`): the scan advances the valid length only on a DIGIT,
    /// so an operand whose accepted prefix is a bare sign has prefix `"0"`,
    /// value `0`, and a truncation event -- not a parse failure saturating to
    /// an `i64`/`u64` endpoint.
    #[test]
    fn function_cast_integer_prefix_needs_a_digit() {
        for (input, expected, truncated) in [
            ("-", 0, true),
            ("+", 0, true),
            ("-abc", 0, true),
            ("+-1", 0, true),
            ("", 0, true),
            ("12abc", 12, true),
            ("12", 12, false),
            ("-12", -12, false),
            ("  12  ", 12, false),
        ] {
            let actual = str_to_int(input, true);
            assert_eq!(actual.value, expected, "{input:?}");
            assert_eq!(
                actual.event == Some(ScalarConversionEvent::Truncated),
                truncated,
                "{input:?}"
            );
        }

        for (input, expected, truncated) in [
            ("-", 0, true),
            ("+", 0, true),
            ("+abc", 0, true),
            ("", 0, true),
            ("12abc", 12, true),
            ("12", 12, false),
            ("-000", 0, false),
        ] {
            let actual = str_to_uint(input, true);
            assert_eq!(actual.value, expected, "{input:?}");
            assert_eq!(
                actual.event == Some(ScalarConversionEvent::Truncated),
                truncated,
                "{input:?}"
            );
        }

        // Go `StrToUint`: only `-000*` is a valid unsigned negative, every
        // other negative prefix is `ErrOverflow` with value zero.
        let negative = str_to_uint("-12", true);
        assert_eq!(negative.value, 0);
        assert!(matches!(
            negative.event,
            Some(ScalarConversionEvent::Overflow(_))
        ));
    }

    /// Source: `pkg/types/convert_test.go::TestStrToNum`.
    #[test]
    fn test_str_to_num() {
        for (input, expected, truncated) in [
            ("0", 0, false),
            ("-1", -1, false),
            ("100", 100, false),
            ("65.0", 65, false),
            ("65.0", 65, false),
            ("", 0, true),
            ("", 0, true),
            ("xx", 0, true),
            ("xx", 0, true),
            ("11xx", 11, true),
            ("11xx", 11, true),
            ("xx11", 0, true),
        ] {
            let actual = str_to_int(input, false);
            assert_eq!(actual.value, expected, "{input:?}");
            assert_eq!(
                actual.event == Some(ScalarConversionEvent::Truncated),
                truncated,
                "{input:?}"
            );
        }

        for (input, expected, event_kind) in [
            ("0", Some(0), 0),
            ("", Some(0), 1),
            ("", Some(0), 1),
            // Go asserts only ErrOverflow for this row, not the value.
            ("-1", None, 2),
            ("100", Some(100), 0),
            ("+100", Some(100), 0),
            ("65.0", Some(65), 0),
            ("xx", Some(0), 1),
            ("11xx", Some(11), 1),
            ("xx11", Some(0), 1),
            ("-00", Some(0), 0),
        ] {
            let actual = str_to_uint(input, false);
            if let Some(expected) = expected {
                assert_eq!(actual.value, expected, "{input:?}");
            }
            match event_kind {
                0 => assert_eq!(actual.event, None, "{input:?}"),
                1 => assert_eq!(
                    actual.event,
                    Some(ScalarConversionEvent::Truncated),
                    "{input:?}"
                ),
                2 => assert!(
                    matches!(actual.event, Some(ScalarConversionEvent::Overflow(_))),
                    "{input:?}"
                ),
                _ => unreachable!(),
            }
        }

        for (input, expected, truncated) in [
            ("", 0.0, true),
            ("-1", -1.0, false),
            ("1.11", 1.11, false),
            ("1.11.00", 1.11, true),
            ("1.11.00", 1.11, true),
            ("xx", 0.0, true),
            ("0x00", 0.0, true),
            ("11.xx", 11.0, true),
            ("11.xx", 11.0, true),
            ("xx.11", 0.0, true),
            ("1e649", f64::MAX, true),
            ("1e649", f64::MAX, true),
            ("-1e649", -f64::MAX, true),
            ("-1e649", -f64::MAX, true),
        ] {
            let actual = str_to_float(input, false);
            assert_eq!(actual.value, expected, "{input:?}");
            assert_eq!(
                actual.event == Some(ScalarConversionEvent::Truncated),
                truncated,
                "{input:?}"
            );
        }

        // `testSelectUpdateDeleteEmptyStringError` repeats the empty input
        // under `TruncateAsWarning`. The scalar result still carries the
        // recoverable event; the statement context consumes it as a warning.
        assert_eq!(str_to_int("", false).value, 0);
        assert_eq!(str_to_uint("", false).value, 0);
        assert_eq!(str_to_float("", false).value, 0.0);
        for event in [
            str_to_int("", false).event,
            str_to_uint("", false).event,
            str_to_float("", false).event,
        ] {
            assert_eq!(event, Some(ScalarConversionEvent::Truncated));
        }
    }

    /// Source: `pkg/types/convert_test.go::TestNumberToDuration`.
    #[test]
    fn test_number_to_duration() {
        for (number, fsp, has_error, hour, minute, second) in [
            (20_171_222, 0, true, 0, 0, 0),
            (171_222, 0, false, 17, 12, 22),
            (20_171_222_020_005, 0, false, 2, 0, 5),
            (10_000_000_000, 0, true, 0, 0, 0),
            (171_222, 1, false, 17, 12, 22),
            (176_022, 1, true, 0, 0, 0),
            (8_391_222, 1, true, 0, 0, 0),
            (8_381_222, 0, false, 838, 12, 22),
            (1_001_222, 0, false, 100, 12, 22),
            (171_260, 1, true, 0, 0, 0),
        ] {
            let actual = number_to_duration(number, fsp).unwrap();
            assert_eq!(actual.event.is_some(), has_error, "{number} fsp={fsp}");
            if !has_error {
                assert_eq!(actual.value.hour(), hour, "{number} fsp={fsp}");
                assert_eq!(actual.value.minute(), minute, "{number} fsp={fsp}");
                assert_eq!(actual.value.second(), second, "{number} fsp={fsp}");
            }
        }

        let positive = number_to_duration(171_222, 0).unwrap().value;
        let negative = number_to_duration(-171_222, 0).unwrap().value;
        assert_eq!(positive.nanoseconds(), -negative.nanoseconds());
    }

    #[test]
    fn number_to_duration_supplemental_extremes() {
        // `i64::MIN` must not be negated before its range is ruled out.
        for (number, expected) in [(i64::MIN, "-838:59:59"), (i64::MAX, "838:59:59")] {
            let actual = number_to_duration(number, 0).unwrap();
            assert_eq!(actual.value.to_string(), expected, "{number}");
            assert!(actual.event.is_some(), "{number}");
        }
    }

    /// Source: `pkg/types/convert_test.go::TestStrToDuration`.
    #[test]
    fn test_str_to_duration() {
        for (input, fsp, is_duration) in [
            ("20190412120000", 4, false),
            ("20190101180000", 6, false),
            ("20190101180000", 1, false),
            ("20190101181234", 3, false),
            ("00:00:00.000000", 6, true),
            ("00:00:00", 0, true),
        ] {
            let actual = str_to_duration(input, fsp, &chrono_tz::UTC).unwrap();
            assert_eq!(
                matches!(actual.value, DurationOrTime::Duration(_)),
                is_duration,
                "{input}"
            );
        }
    }

    /// Source: `pkg/types/convert_test.go::TestConvertJSONToInt`.
    #[test]
    fn test_convert_json_to_int() {
        for (input, expected, truncated) in [
            ("{}", 0, true),
            ("[]", 0, true),
            ("3", 3, false),
            ("-3", -3, false),
            ("4.5", 4, false),
            ("true", 1, false),
            ("false", 0, false),
            ("null", 0, true),
            ("\"hello\"", 0, true),
            ("\"123hello\"", 123, true),
            ("\"1234\"", 1234, false),
        ] {
            let json = BinaryJSON::parse(input).unwrap();
            let actual = json_to_int64(&json, false, ConversionFlags::from_bits(0));
            assert_eq!(actual.value, expected, "{input}");
            assert_eq!(actual.event.is_some(), truncated, "{input}");
        }
    }

    /// Source: `pkg/types/convert_test.go::TestConvertJSONToFloat`.
    #[test]
    fn test_convert_json_to_float() {
        for (input, expected, type_code, truncated) in [
            ("{}", 0.0, JSON_TYPE_CODE_OBJECT, true),
            ("[]", 0.0, JSON_TYPE_CODE_ARRAY, true),
            ("3", 3.0, JSON_TYPE_CODE_INT64, false),
            ("-3", -3.0, JSON_TYPE_CODE_INT64, false),
            (
                "9223372036854775808",
                9_223_372_036_854_775_808.0,
                JSON_TYPE_CODE_UINT64,
                false,
            ),
            ("4.5", 4.5, JSON_TYPE_CODE_FLOAT64, false),
            ("true", 1.0, JSON_TYPE_CODE_LITERAL, false),
            ("false", 0.0, JSON_TYPE_CODE_LITERAL, false),
            ("null", 0.0, JSON_TYPE_CODE_LITERAL, true),
            ("\"hello\"", 0.0, JSON_TYPE_CODE_STRING, true),
            ("\"123.456hello\"", 123.456, JSON_TYPE_CODE_STRING, true),
            ("\"1234\"", 1234.0, JSON_TYPE_CODE_STRING, false),
        ] {
            let json = BinaryJSON::parse(input).unwrap();
            assert_eq!(json.type_code(), type_code, "{input}");
            let actual = json_to_float(&json);
            assert_eq!(actual.value, expected, "{input}");
            assert_eq!(actual.event.is_some(), truncated, "{input}");
        }
    }

    /// Source: `pkg/types/convert_test.go::TestConvertJSONToDecimal`.
    #[test]
    fn test_convert_json_to_decimal() {
        for (input, expected, truncated) in [
            ("3", "3", false),
            ("-3", "-3", false),
            ("4.5", "4.5", false),
            ("\"1234\"", "1234", false),
            (
                "\"1234567890123456789012345678901234567890123456789012345\"",
                "1234567890123456789012345678901234567890123456789012345",
                false,
            ),
            ("true", "1", false),
            ("false", "0", false),
            ("null", "0", true),
        ] {
            let json = BinaryJSON::parse(input).unwrap();
            let actual = json_to_decimal(&json);
            assert_eq!(actual.value.to_string(), expected, "{input}");
            assert_eq!(actual.event.is_some(), truncated, "{input}");
        }
    }

    #[test]
    fn source_to_string_rows() {
        for (value, expected) in [
            (ScalarStringValue::String("0"), "0"),
            (ScalarStringValue::Bool(true), "1"),
            (ScalarStringValue::String("false"), "false"),
            (ScalarStringValue::Int(0), "0"),
            (ScalarStringValue::Uint(0), "0"),
            (ScalarStringValue::Float32(1.6), "1.6"),
            (ScalarStringValue::Float64(-0.6), "-0.6"),
        ] {
            assert_eq!(scalar_to_string(value).unwrap(), expected);
        }
        assert_eq!(
            scalar_to_string(ScalarStringValue::Bytes(&[1])).unwrap(),
            "\u{1}"
        );

        let mysql = BinaryLiteral::from_uint(0x004d_7953_514c, None);
        assert_eq!(
            scalar_to_string(ScalarStringValue::BinaryLiteral(&mysql)).unwrap(),
            "MySQL"
        );
        let time = crate::parse_time(
            "2011-11-10 11:11:11.999999",
            TimeType::Timestamp,
            6,
            false,
            true,
            false,
            &chrono_tz::UTC,
        )
        .unwrap()
        .time;
        assert_eq!(
            scalar_to_string(ScalarStringValue::Time(time)).unwrap(),
            "2011-11-10 11:11:11.999999"
        );
        let duration = MySqlDuration::new(11, 11, 11, 999_999, 6).unwrap();
        assert_eq!(
            scalar_to_string(ScalarStringValue::Duration(duration)).unwrap(),
            "11:11:11.999999"
        );
        let decimal = Decimal::from_signed_literal("3.14159");
        assert_eq!(
            scalar_to_string(ScalarStringValue::Decimal(&decimal)).unwrap(),
            "3.14159"
        );
    }
}

#[cfg(test)]
mod decimal_from_text_source_rows {
    use super::*;

    /// Captured from Go in this checkout with a throwaway
    /// `MyDecimal.FromString` probe. The first row used to abort the process:
    /// the old `decimal_text` expanded the exponent into literal zeros, so
    /// `1e9223372036854775807` asked for a 9.2-exabyte `String`.
    #[test]
    fn source_from_string_exponent_rows() {
        let nines = "9".repeat(81);
        for (input, expected, event) in [
            ("1e9223372036854775807", nines.as_str(), "overflow"),
            ("1e2147483647", nines.as_str(), "overflow"),
            ("1e15", "1000000000000000", "exact"),
            ("1e-9223372036854775808", "0", "truncated"),
            ("1.5e3", "1500", "exact"),
            ("123abc", "123", "truncated"),
            ("1,999.00", "1", "truncated"),
        ] {
            let actual = decimal_from_text(input);
            assert_eq!(actual.value.to_string(), expected, "{input:?}");
            let kind = match actual.event {
                None => "exact",
                Some(ScalarConversionEvent::Truncated) => "truncated",
                Some(ScalarConversionEvent::Overflow(_)) => "overflow",
                Some(ScalarConversionEvent::RoundedToScale) => "rounded",
                Some(ScalarConversionEvent::TimestampInDSTTransition) => "timestamp-dst",
            };
            assert_eq!(kind, event, "{input:?}");
        }
        let negative = decimal_from_text("-1e9223372036854775807");
        assert_eq!(negative.value.to_string(), format!("-{nines}"));
    }

    /// `ConvertJSONToDecimal`'s string arm is Go's `FromString`, so it accepts
    /// a numeric prefix instead of collapsing the value to `0`.
    #[test]
    fn source_json_string_decimal_is_prefix_accepting() {
        for (literal, expected) in [
            (r#""123abc""#, "123"),
            (r#""1,999.00""#, "1"),
            (r#""12""#, "12"),
        ] {
            let json = BinaryJSON::parse(literal).unwrap();
            let actual = json_to_decimal(&json);
            assert_eq!(actual.value.to_string(), expected, "{literal:?}");
        }
    }
}
