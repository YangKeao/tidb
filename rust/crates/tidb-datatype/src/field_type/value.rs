// Copyright 2026 PingCAP, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");

use super::{FieldType, FieldTypeCode, FieldTypeFlags, UNSPECIFIED_LENGTH};
use crate::{Datum, TimeType};
use tidb_query_datatype::codec::{
    native_field_value::{
        native_default_field_type_for_value, native_parser_default_field_type_for_value,
        NativeFieldCharsetPolicy, NativeFieldTypeSpec, NativeFieldValue,
    },
    native_type_name::NativeTypeNameCode,
};

/// Go `types.InferParamTypeFromDatum`: execute-time parameter metadata, not
/// literal display widths. The datum remains byte-preserving and unchanged.
pub fn infer_param_type_from_datum(datum: &Datum) -> FieldType {
    // String/ENUM/SET payloads can contain non-UTF-8 bytes. Their parameter
    // width is unspecified, so inference needs no decoding or replacement.
    let mut field_type = match datum {
        Datum::Null | Datum::MinNotNull | Datum::MaxValue | Datum::Raw(_) => {
            // Internal range sentinels and raw encoded storage cells are
            // not SQL EXECUTE values and carry no typed parameter payload
            // in this representation.
            return FieldType::parser(FieldTypeCode::Null)
                .with_flen(0)
                .with_decimal(0)
                .with_charset_name("utf8mb4")
                .with_collation_name("utf8mb4_bin");
        }
        Datum::String(value) => FieldType::parser(FieldTypeCode::VarString)
            .with_added_flags(FieldTypeFlags::NOT_NULL)
            .with_charset_name(value.charset().name())
            .with_collation_name(value.collation().name()),
        Datum::Enum(..) | Datum::Set(..) => {
            FieldType::parser(if matches!(datum, Datum::Enum(..)) {
                FieldTypeCode::Enum
            } else {
                FieldTypeCode::Set
            })
            .with_added_flags(FieldTypeFlags::NOT_NULL | FieldTypeFlags::BINARY)
            .with_charset_name("binary")
            .with_collation_name("binary")
        }
        _ => {
            let value = match datum {
                Datum::Int(value) => FieldTypeValue::Signed(*value),
                Datum::UInt(value) => FieldTypeValue::Unsigned(*value),
                Datum::Real(value) => FieldTypeValue::Float64(*value),
                Datum::Float32(value) => FieldTypeValue::Float32(*value as f32),
                Datum::Bytes(value) => FieldTypeValue::Bytes(value),
                Datum::BinaryLiteral(value) | Datum::Bit(value) => {
                    FieldTypeValue::BinaryLiteral(value.as_bytes())
                }
                Datum::Decimal(value) => FieldTypeValue::Decimal {
                    display_len: value.to_string().len() as i64,
                    fraction_digits: i64::from(value.precision_and_frac().1),
                },
                Datum::Duration(value) => FieldTypeValue::Duration {
                    display_len: value.to_string().len() as i64,
                    fsp: value.fsp(),
                },
                Datum::Time(value) => match value.kind() {
                    TimeType::Date => FieldTypeValue::Date,
                    TimeType::DateTime => FieldTypeValue::Datetime {
                        fsp: i64::from(value.fsp()),
                    },
                    TimeType::Timestamp => FieldTypeValue::Timestamp {
                        fsp: i64::from(value.fsp()),
                    },
                },
                Datum::Json(_) => FieldTypeValue::Json,
                Datum::VectorFloat32(_) => FieldTypeValue::VectorFloat32,
                Datum::Null
                | Datum::MinNotNull
                | Datum::MaxValue
                | Datum::Raw(_)
                | Datum::String(_)
                | Datum::Enum(..)
                | Datum::Set(..) => {
                    unreachable!("handled non-scalar datum above")
                }
            };
            default_field_type_for_value(value, "utf8mb4", "utf8mb4_bin")
        }
    };
    if matches!(
        field_type.code(),
        FieldTypeCode::LongLong
            | FieldTypeCode::VarString
            | FieldTypeCode::Double
            | FieldTypeCode::Blob
            | FieldTypeCode::Bit
            | FieldTypeCode::Duration
            | FieldTypeCode::Enum
            | FieldTypeCode::Set
    ) {
        field_type.set_flen(UNSPECIFIED_LENGTH);
    }
    field_type
}

/// Source value shapes accepted by `DefaultTypeForValue`.
#[derive(Debug, Clone, PartialEq)]
pub enum FieldTypeValue<'a> {
    /// SQL `NULL`.
    Null,
    /// A Boolean value.
    Bool(bool),
    /// A signed integer value.
    Signed(i64),
    /// An unsigned integer value.
    Unsigned(u64),
    /// A character string value.
    String(&'a str),
    /// A single-precision floating-point value.
    Float32(f32),
    /// A double-precision floating-point value.
    Float64(f64),
    /// An arbitrary byte string.
    Bytes(&'a [u8]),
    /// A bit-string literal's decoded bytes.
    BitLiteral(&'a [u8]),
    /// A hexadecimal literal's decoded bytes.
    HexLiteral(&'a [u8]),
    /// A binary literal's decoded bytes.
    BinaryLiteral(&'a [u8]),
    /// A calendar date.
    Date,
    /// A date and time with fractional-second precision.
    Datetime {
        /// Fractional-second precision.
        fsp: i64,
    },
    /// A timestamp with fractional-second precision.
    Timestamp {
        /// Fractional-second precision.
        fsp: i64,
    },
    /// A time duration and its display metadata.
    Duration {
        /// Display width before fractional-second adjustment.
        display_len: i64,
        /// Fractional-second precision.
        fsp: i64,
    },
    /// An exact decimal and its display metadata.
    Decimal {
        /// Display width of the decimal value.
        display_len: i64,
        /// Number of digits after the decimal point.
        fraction_digits: i64,
    },
    /// An enum member name.
    Enum(&'a str),
    /// A set member name.
    Set(&'a str),
    /// A JSON value.
    Json,
    /// A vector of single-precision floating-point values.
    VectorFloat32,
    /// A value shape with no supported default field type.
    Unsupported,
}

fn native_field_value(value: &FieldTypeValue<'_>) -> NativeFieldValue {
    match value {
        FieldTypeValue::Null => NativeFieldValue::Null,
        FieldTypeValue::Bool(_) => NativeFieldValue::Bool,
        FieldTypeValue::Signed(value) => NativeFieldValue::Signed(*value),
        FieldTypeValue::Unsigned(value) => NativeFieldValue::Unsigned(*value),
        FieldTypeValue::String(value) => NativeFieldValue::StringLen(value.len()),
        FieldTypeValue::Float32(value) => NativeFieldValue::Float32(*value),
        FieldTypeValue::Float64(value) => NativeFieldValue::Float64(*value),
        FieldTypeValue::Bytes(value) => NativeFieldValue::BytesLen(value.len()),
        FieldTypeValue::BitLiteral(value) => NativeFieldValue::BitLiteralLen(value.len()),
        FieldTypeValue::HexLiteral(value) => NativeFieldValue::HexLiteralLen(value.len()),
        FieldTypeValue::BinaryLiteral(value) => NativeFieldValue::BinaryLiteralLen(value.len()),
        FieldTypeValue::Date => NativeFieldValue::Date,
        FieldTypeValue::Datetime { fsp } => NativeFieldValue::Datetime { fsp: *fsp },
        FieldTypeValue::Timestamp { fsp } => NativeFieldValue::Timestamp { fsp: *fsp },
        FieldTypeValue::Duration { display_len, fsp } => NativeFieldValue::Duration {
            display_len: *display_len,
            fsp: *fsp,
        },
        FieldTypeValue::Decimal {
            display_len,
            fraction_digits,
        } => NativeFieldValue::Decimal {
            display_len: *display_len,
            fraction_digits: *fraction_digits,
        },
        FieldTypeValue::Enum(value) => NativeFieldValue::EnumLen(value.len()),
        FieldTypeValue::Set(value) => NativeFieldValue::SetLen(value.len()),
        FieldTypeValue::Json => NativeFieldValue::Json,
        FieldTypeValue::VectorFloat32 => NativeFieldValue::VectorFloat32,
        FieldTypeValue::Unsupported => NativeFieldValue::Unsupported,
    }
}

fn apply_native_field_type_spec(
    spec: NativeFieldTypeSpec,
    charset: &str,
    collation: &str,
) -> FieldType {
    let raw_code = match spec.code {
        NativeTypeNameCode::Known(raw) | NativeTypeNameCode::Unknown(raw) => raw,
    };
    let field_type = FieldType::parser(FieldTypeCode::from_mysql_type(raw_code))
        .with_flags(spec.flags)
        .with_flen(spec.flen)
        .with_decimal(spec.decimal);
    match spec.charset_policy {
        NativeFieldCharsetPolicy::Binary => field_type
            .with_charset_name("binary")
            .with_collation_name("binary"),
        NativeFieldCharsetPolicy::Input => field_type
            .with_charset_name(charset)
            .with_collation_name(collation),
        NativeFieldCharsetPolicy::Utf8 => field_type
            .with_charset_name("utf8mb4")
            .with_collation_name("utf8mb4_bin"),
        NativeFieldCharsetPolicy::Preserve => field_type,
    }
}

/// Mechanically mirrors `pkg/types.DefaultTypeForValue` metadata decisions.
pub fn default_field_type_for_value(
    value: FieldTypeValue<'_>,
    charset: &str,
    collation: &str,
) -> FieldType {
    let spec = native_default_field_type_for_value(
        native_field_value(&value),
        FieldTypeFlags::NOT_NULL,
        FieldTypeFlags::BINARY,
        FieldTypeFlags::UNSIGNED,
        FieldTypeFlags::IS_BOOLEAN,
    );
    apply_native_field_type_spec(spec, charset, collation)
}

/// Mirrors `pkg/parser/test_driver.DefaultTypeForValue`.
///
/// This is intentionally separate from [`default_field_type_for_value`]: the
/// parser's lightweight driver and TiDB's runtime `types` package assign
/// different flags and widths to the same literal shapes.
pub fn parser_default_field_type_for_value(
    value: FieldTypeValue<'_>,
    charset: &str,
    collation: &str,
) -> FieldType {
    let spec = native_parser_default_field_type_for_value(
        native_field_value(&value),
        FieldTypeFlags::BINARY,
        FieldTypeFlags::UNSIGNED,
        FieldTypeFlags::IS_BOOLEAN,
    );
    apply_native_field_type_spec(spec, charset, collation)
}

#[cfg(test)]
mod shared_value_policy_tests {
    use super::*;

    #[test]
    fn shared_field_value_policy_keeps_runtime_parser_literal_and_charset_differences() {
        let runtime = default_field_type_for_value(
            FieldTypeValue::BinaryLiteral(&[0xaa, 0xbb]),
            "utf8mb4",
            "utf8mb4_bin",
        );
        assert_eq!(
            (runtime.code(), runtime.flen(), runtime.decimal()),
            (FieldTypeCode::VarString, 2, 0)
        );
        assert!(runtime.is_unsigned());
        assert!(!runtime.has_flag(FieldTypeFlags::BINARY));
        assert_eq!(
            (runtime.charset_name(), runtime.collation_name()),
            ("binary", "binary")
        );

        let parser = parser_default_field_type_for_value(
            FieldTypeValue::BinaryLiteral(&[0xaa, 0xbb]),
            "utf8mb4",
            "utf8mb4_bin",
        );
        assert_eq!(
            (parser.code(), parser.flen(), parser.decimal()),
            (FieldTypeCode::Bit, 16, 0)
        );
        assert!(parser.is_unsigned());
        assert!(!parser.has_flag(FieldTypeFlags::BINARY));

        let runtime_decimal = default_field_type_for_value(
            FieldTypeValue::Decimal {
                display_len: 100,
                fraction_digits: 40,
            },
            "utf8mb4",
            "utf8mb4_bin",
        );
        let parser_decimal = parser_default_field_type_for_value(
            FieldTypeValue::Decimal {
                display_len: 100,
                fraction_digits: 40,
            },
            "utf8mb4",
            "utf8mb4_bin",
        );
        assert_eq!(
            (runtime_decimal.flen(), runtime_decimal.decimal()),
            (65, 30)
        );
        assert_eq!((parser_decimal.flen(), parser_decimal.decimal()), (100, 40));

        let runtime_unsupported =
            default_field_type_for_value(FieldTypeValue::Unsupported, "ignored", "ignored");
        assert!(runtime_unsupported.has_flag(FieldTypeFlags::NOT_NULL));
        assert_eq!(
            (
                runtime_unsupported.charset_name(),
                runtime_unsupported.collation_name()
            ),
            ("utf8mb4", "utf8mb4_bin")
        );
        let parser_unsupported =
            parser_default_field_type_for_value(FieldTypeValue::Unsupported, "ignored", "ignored");
        assert_eq!(
            (
                parser_unsupported.charset_name(),
                parser_unsupported.collation_name()
            ),
            ("", "")
        );
    }
}
