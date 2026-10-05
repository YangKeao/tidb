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

//! Datum-to-scalar conversions.
//!
//! Mirrors the `ToBool` / `ToInt64` / `ToFloat64` / `ToDecimal` / `ToBytes` /
//! `ToHashKey` / `ToMysqlJSON` block of `pkg/types/datum.go`, plus the
//! aggregate-side opaque-JSON rule of `getRealJSONValue`
//! (`pkg/executor/aggfuncs/func_json_objectagg.go`).

#[cfg(test)]
use crate::BinaryLiteral;
use std::cmp::Ordering;
use tidb_query_datatype::codec::native_mysql_json::{
    native_datum_to_mysql_json, native_datum_to_mysql_json_with_source, NativeDatumJsonError,
    NativeDatumJsonSource,
};

use super::{Datum, DatumStringError, DatumValueError};
use crate::{
    compare_binary_json, json_to_float, str_to_float, BinaryJSON, Collation, Converted, Decimal,
    ScalarConversionEvent,
};

impl Datum {
    /// Source `Datum.ToBool`, retaining conversion warning/error disposition.
    pub fn to_bool(&self) -> Result<Converted<i64>, DatumValueError> {
        let converted = match self {
            Self::Int(value) => Converted {
                value: i64::from(*value != 0),
                event: None,
            },
            Self::UInt(value) => Converted {
                value: i64::from(*value != 0),
                event: None,
            },
            Self::Real(value) | Self::Float32(value) => Converted {
                value: i64::from(*value != 0.0),
                event: None,
            },
            Self::String(value) => {
                let parsed = str_to_float(value.as_utf8()?, false);
                Converted {
                    value: i64::from(parsed.value != 0.0),
                    event: parsed.event,
                }
            }
            Self::Bytes(value) => {
                let parsed = str_to_float(std::str::from_utf8(value)?, false);
                Converted {
                    value: i64::from(parsed.value != 0.0),
                    event: parsed.event,
                }
            }
            Self::Time(value) => Converted {
                value: i64::from(!value.is_zero()),
                event: None,
            },
            Self::Duration(value) => Converted {
                value: i64::from(value.nanoseconds() != 0),
                event: None,
            },
            Self::Decimal(value) => Converted {
                value: i64::from(!value.is_zero()),
                event: None,
            },
            Self::Enum(value, _) => Converted {
                value: i64::from(value.value() != 0),
                event: None,
            },
            Self::Set(value, _) => Converted {
                value: i64::from(value.value() != 0),
                event: None,
            },
            Self::BinaryLiteral(value) | Self::Bit(value) => {
                let outcome = value.to_int();
                Converted {
                    value: i64::from(outcome.value() != 0),
                    event: outcome
                        .is_truncated()
                        .then_some(ScalarConversionEvent::Truncated),
                }
            }
            Self::Json(value) => {
                let zero = BinaryJSON::parse("0")?;
                Converted {
                    value: i64::from(compare_binary_json(value, &zero) != Ordering::Equal),
                    event: None,
                }
            }
            Self::VectorFloat32(value) => Converted {
                value: i64::from(!value.is_zero_value()),
                event: None,
            },
            other => return Err(DatumValueError::Unsupported(other.kind(), "bool")),
        };
        Ok(converted)
    }

    /// Source `Datum.ToInt64`.
    ///
    /// Go's takes a `types.Context`, whose LOCATION reaches
    /// `Time.RoundFrac`. This overload supplies UTC for the zone-free
    /// callers, exactly as [`Datum::convert_to`] does for
    /// [`Datum::convert_to_in`]; a caller that owns a session zone must use
    /// [`Datum::to_i64_in`].
    pub fn to_i64(&self) -> Result<Converted<i64>, DatumValueError> {
        self.to_i64_in(&crate::SessionTimeZone::utc())
    }

    /// Source `Datum.ToInt64` = `toSignedInteger(ctx, TypeLonglong)` with the
    /// statement's own `ctx.Location()`.
    pub fn to_i64_in(
        &self,
        zone: &crate::SessionTimeZone,
    ) -> Result<Converted<i64>, DatumValueError> {
        use tidb_query_datatype::codec::native_numeric::{native_datum_to_i64, NativeNumericError};
        native_datum_to_i64(self.as_shared_numeric_input(), zone)
            .map(crate::convert::from_shared_integer_conversion)
            .map_err(|error| match error {
                NativeNumericError::InvalidUtf8(error) => error.into(),
                NativeNumericError::Comparison(message) => DatumValueError::Comparison(message),
                NativeNumericError::Unsupported => {
                    DatumValueError::Unsupported(self.kind(), "int64")
                }
            })
    }

    /// Borrows actual numeric storage without conversion or context demand.
    pub fn as_shared_numeric_input(
        &self,
    ) -> tidb_query_datatype::codec::native_numeric::NativeNumericInput<'_> {
        use tidb_query_datatype::codec::{
            mysql::time::NativeTemporalValue, native_duration_convert::NativeDurationParts,
            native_numeric::NativeNumericInput as I,
        };
        match self {
            Self::Int(value) => I::Int(*value),
            Self::UInt(value) => I::UInt(*value),
            Self::Decimal(value) => I::Decimal(value.as_shared_parse()),
            Self::Real(value) => I::Real(*value),
            Self::Float32(value) => I::Float32(*value),
            Self::String(value) => I::String(value.bytes()),
            Self::Bytes(value) => I::Bytes(value),
            Self::BinaryLiteral(value) => I::BinaryLiteral(value.as_bytes()),
            Self::Bit(value) => I::Bit(value.as_bytes()),
            Self::Duration(value) => I::Duration(NativeDurationParts {
                nanoseconds: value.nanoseconds(),
                fsp: value.fsp(),
            }),
            Self::Enum(value, _) => I::Enum(value.value()),
            Self::Set(value, _) => I::Set(value.value()),
            Self::Time(value) => I::Time(NativeTemporalValue {
                raw: value.core_time().raw(),
                kind: value.kind(),
                fsp: value.fsp(),
            }),
            Self::Json(value) => I::Json {
                type_code: value.type_code(),
                value: value.value(),
            },
            Self::Raw(value) => I::Raw(value),
            Self::VectorFloat32(value) => I::VectorFloat32(value),
            Self::Null => I::Null,
            Self::MinNotNull => I::MinNotNull,
            Self::MaxValue => I::MaxValue,
        }
    }

    /// Source `Datum.ToFloat64`.
    pub fn to_f64(&self) -> Result<Converted<f64>, DatumValueError> {
        let converted = match self {
            Self::Int(value) => Converted {
                value: *value as f64,
                event: None,
            },
            Self::UInt(value) => Converted {
                value: *value as f64,
                event: None,
            },
            Self::Real(value) => Converted {
                value: *value,
                event: None,
            },
            Self::Float32(value) => Converted {
                value: f64::from(*value as f32),
                event: None,
            },
            Self::String(value) => str_to_float(value.as_utf8()?, false),
            Self::Bytes(value) => str_to_float(std::str::from_utf8(value)?, false),
            Self::Time(value) => Converted {
                value: value.to_number().to_f64(),
                event: None,
            },
            Self::Duration(value) => Converted {
                value: value.to_number().to_f64(),
                event: None,
            },
            Self::Decimal(value) => Converted {
                value: value.to_f64(),
                event: None,
            },
            Self::Enum(value, _) => Converted {
                value: value.to_number(),
                event: None,
            },
            Self::Set(value, _) => Converted {
                value: value.to_number(),
                event: None,
            },
            Self::BinaryLiteral(value) | Self::Bit(value) => {
                let outcome = value.to_int();
                Converted {
                    value: outcome.value() as f64,
                    event: outcome
                        .is_truncated()
                        .then_some(ScalarConversionEvent::Truncated),
                }
            }
            Self::Json(value) => json_to_float(value),
            other => return Err(DatumValueError::Unsupported(other.kind(), "float64")),
        };
        Ok(converted)
    }

    /// Go `Datum.ToDecimal` with its original conversion-stage diagnostics.
    /// Unlike `ConvertTo(DECIMAL)`, this does not fit a declared column shape.
    pub fn to_decimal_with_context(
        &self,
        context: &crate::ConversionContext<'_>,
    ) -> Result<(Decimal, Option<tidb_error::terror::TerrorError>), DatumValueError> {
        use tidb_query_datatype::codec::native_decimal_context::native_datum_to_decimal_with_context;
        use tidb_query_datatype::codec::native_numeric::NativeNumericError;
        native_datum_to_decimal_with_context(
            self.as_shared_numeric_input(),
            |error| context.handle_truncate(error.map(decimal_context_error)),
            decimal_context_error,
        )
        .map(|(value, error)| (Decimal::from_shared_parse(value), error))
        .map_err(|error| match error {
            NativeNumericError::InvalidUtf8(error) => error.into(),
            NativeNumericError::Comparison(message) => DatumValueError::Comparison(message),
            NativeNumericError::Unsupported => DatumValueError::Unsupported(self.kind(), "decimal"),
        })
    }

    /// Source `Datum.ToDecimal`.
    pub fn to_decimal(&self) -> Result<Converted<Decimal>, DatumValueError> {
        use tidb_query_datatype::codec::native_decimal_convert::native_datum_to_decimal;
        use tidb_query_datatype::codec::native_numeric::NativeNumericError;
        native_datum_to_decimal(self.as_shared_numeric_input())
            .map(crate::convert::from_shared_decimal_conversion)
            .map_err(|error| match error {
                NativeNumericError::InvalidUtf8(error) => error.into(),
                NativeNumericError::Comparison(message) => DatumValueError::Comparison(message),
                NativeNumericError::Unsupported => {
                    DatumValueError::Unsupported(self.kind(), "decimal")
                }
            })
    }

    /// Source `Datum.ToBytes`, whose default arm is `ToString`.
    ///
    /// `ToString`'s `KindBinaryLiteral`/`KindMysqlBit` arm is
    /// `d.GetBinaryLiteral().ToString()`, which is `string(b)` -- a Go string
    /// conversion, so the OCTETS pass through unvalidated, exactly as they do
    /// for `KindString`/`KindBytes`. `sql_string` cannot serve that arm here
    /// because a Rust `String` must be UTF-8, and refusing `0xAABBCCDDEEFF`
    /// is not something Go ever does (`UNCOMPRESSED_LENGTH(0xAABBCCDDEEFF)`
    /// is 3721182122, not an error).
    pub fn to_bytes(&self) -> Result<Vec<u8>, DatumStringError> {
        self.sql_bytes()
    }

    /// Source `Datum.ToHashKey`.
    pub fn to_hash_key(&self) -> Result<Vec<u8>, DatumStringError> {
        let bytes = self.to_bytes()?;
        Ok(self.collation().unwrap_or(Collation::Binary).key(&bytes))
    }

    /// Source `Datum.ToMysqlJSON`.
    pub fn to_mysql_json(&self) -> Result<BinaryJSON, DatumValueError> {
        native_datum_to_mysql_json(self.as_shared_sql_string())
            .map(|(type_code, value)| BinaryJSON::from_encoded_parts(type_code, value))
            .map_err(|error| from_shared_mysql_json_error(self.kind(), error))
    }

    /// As [`Self::to_mysql_json`], but a `Bytes` payload -- and a `String`
    /// payload whose `field_type` is BINARY-charset -- embeds
    /// `field_type`'s own MySQL type code as a JSON `Opaque` value instead
    /// of an ordinary JSON string. Go's `getRealJSONValue`
    /// (`pkg/executor/aggfuncs/func_json_objectagg.go`), the value rule
    /// shared by `JSON_ARRAYAGG` and `JSON_OBJECTAGG`, wraps `KindBytes`
    /// unconditionally (a byte datum has no other charset) and `KindString`
    /// only when its field type's charset is `binary`.
    ///
    /// A fixed-length `BINARY(n)` column (`FieldTypeCode::String`) pads the
    /// embedded buffer to `flen` bytes before encoding, matching Go's own
    /// tailing-zero rule (captured: `BINARY(3)` holding `"ab"` renders
    /// `base64:type254:YWIA`, the trailing NUL included). Every other datum
    /// kind defers to `to_mysql_json` unchanged.
    pub fn to_mysql_json_with_source_type(
        &self,
        field_type: &crate::FieldType,
    ) -> Result<BinaryJSON, DatumValueError> {
        let code = field_type.code();
        let source = NativeDatumJsonSource {
            code: code.as_shared_type_name_code(),
            string_code: code.as_shared_string_type(),
            collation: field_type.collation_name(),
            flen: field_type.flen(),
        };
        native_datum_to_mysql_json_with_source(self.as_shared_sql_string(), source)
            .map(|(type_code, value)| BinaryJSON::from_encoded_parts(type_code, value))
            .map_err(|error| from_shared_mysql_json_error(self.kind(), error))
    }
}

fn from_shared_mysql_json_error(
    kind: super::DatumKind,
    error: NativeDatumJsonError,
) -> DatumValueError {
    match error {
        NativeDatumJsonError::InvalidUtf8(error) => DatumValueError::InvalidUtf8(error),
        NativeDatumJsonError::Unsupported => DatumValueError::Unsupported(kind, "json"),
        NativeDatumJsonError::Construct(error) => {
            crate::binary_json::native_json_construct_error(error).into()
        }
    }
}

// MyDecimal keeps compact Rust errors; conversion adds Go's error identity
// and the exact input slice used by FromString after whitespace/sign removal.
fn decimal_context_error(
    error: tidb_query_datatype::codec::native_decimal_context::NativeDecimalContextError,
) -> tidb_error::terror::TerrorError {
    use tidb_query_datatype::codec::native_decimal_context::NativeDecimalContextError;
    match error {
        NativeDecimalContextError::Truncated => crate::ERR_TRUNCATED.clone(),
        NativeDecimalContextError::Overflow => crate::ERR_OVERFLOW.clone(),
        NativeDecimalContextError::BadNumber => crate::ERR_BAD_NUMBER.clone(),
        NativeDecimalContextError::TruncatedWrongValue { message } => {
            crate::ERR_TRUNCATED_WRONG_VALUE.generate(message)
        }
        NativeDecimalContextError::BinaryTruncatedWrongValue { literal } => {
            crate::binary_literal::binary_literal_truncated_wrong_value_error(&literal)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Datum;
    use crate::{
        BinaryJSON, BinaryLiteral, Collation, ConversionFlags, Decimal, FieldType, FieldTypeCode,
        MySqlDuration, ScalarConversionEvent, TimeType,
    };

    #[test]
    fn test_to_bool() {
        let rows = vec![
            // Go's first two rows differ by `int` versus `int64`; both map to
            // the source-compatible signed Datum representation in Rust.
            (Datum::Int(0), 0),
            (Datum::Int(0), 0),
            (Datum::UInt(0), 0),
            (Datum::Float32(0.1), 1),
            (Datum::Real(0.1), 1),
            (Datum::Real(0.5), 1),
            (Datum::Real(0.499), 1),
            (Datum::new_string(""), 0),
            (Datum::new_string("0.1"), 1),
            (Datum::new_bytes([]), 0),
            (Datum::new_bytes(b"0.1"), 1),
            (
                Datum::new_binary_literal(BinaryLiteral::from_uint(0, None)),
                0,
            ),
            (
                Datum::new_enum(crate::MysqlEnum::new("a", 1), Collation::DEFAULT),
                1,
            ),
            (
                Datum::new_set(crate::MysqlSet::new("a", 1), Collation::DEFAULT),
                1,
            ),
            (Datum::new_json(BinaryJSON::parse("1").unwrap()), 1),
            (Datum::new_json(BinaryJSON::parse("0").unwrap()), 0),
            (Datum::new_json(BinaryJSON::parse("\"0\"").unwrap()), 1),
            (Datum::new_json(BinaryJSON::parse("\"aaabbb\"").unwrap()), 1),
            (Datum::new_json(BinaryJSON::parse("0.0").unwrap()), 0),
            (Datum::new_json(BinaryJSON::parse("3.1415").unwrap()), 1),
            (Datum::new_json(BinaryJSON::parse("[1,2]").unwrap()), 1),
            (
                Datum::new_json(BinaryJSON::parse(r#"{"ke":"val"}"#).unwrap()),
                1,
            ),
            (
                Datum::new_json(BinaryJSON::parse("\"0000-00-00 00:00:00\"").unwrap()),
                1,
            ),
            (Datum::new_json(BinaryJSON::parse("\"0778\"").unwrap()), 1),
            (Datum::new_json(BinaryJSON::parse("\"0000\"").unwrap()), 1),
            (Datum::new_json(BinaryJSON::parse("null").unwrap()), 1),
            (Datum::new_json(BinaryJSON::parse("[null]").unwrap()), 1),
            (Datum::new_json(BinaryJSON::parse("true").unwrap()), 1),
            (Datum::new_json(BinaryJSON::parse("false").unwrap()), 1),
            (Datum::new_json(BinaryJSON::parse("\"\"").unwrap()), 1),
        ];
        for (datum, expected) in rows.iter() {
            assert_eq!(datum.to_bool().unwrap().value, *expected, "{datum:?}");
        }
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
        assert_eq!(Datum::new_time(time).to_bool().unwrap().value, 1);
        let mut source_rows = rows.len() + 1;
        let duration = MySqlDuration::new(11, 11, 11, 999_999, 6).unwrap();
        assert_eq!(Datum::new_duration(duration).to_bool().unwrap().value, 1);
        source_rows += 1;
        assert_eq!(
            Datum::new_decimal(Decimal::from_signed_literal("0.14159"))
                .to_bool()
                .unwrap()
                .value,
            1
        );
        source_rows += 1;
        assert_eq!(source_rows, 33, "one entry per Go success source row");
        assert!(Datum::new_raw(b"unsupported").to_bool().is_err());
    }

    #[test]
    fn source_to_int_float_and_decimal_rows() {
        for (datum, expected) in [
            (Datum::new_string("0"), 0),
            (Datum::Int(0), 0),
            (Datum::UInt(0), 0),
            (Datum::Float32(3.1), 3),
            (Datum::Real(3.1), 3),
            (
                Datum::new_binary_literal(BinaryLiteral::from_uint(100, None)),
                100,
            ),
            (Datum::new_json(BinaryJSON::parse("3").unwrap()), 3),
            (
                Datum::new_decimal(Decimal::from_signed_literal("3.1415926")),
                3,
            ),
        ] {
            assert_eq!(datum.to_i64().unwrap().value, expected, "{datum:?}");
        }

        for (datum, expected) in [
            (Datum::Int(-3), -3.0),
            (Datum::UInt(3), 3.0),
            (Datum::Float32(3.1), f64::from(3.1_f32)),
            (Datum::Real(3.1), 3.1),
            (Datum::new_string("3.25"), 3.25),
            (
                Datum::new_decimal(Decimal::from_signed_literal("-4.5")),
                -4.5,
            ),
            (Datum::new_json(BinaryJSON::parse("4.5").unwrap()), 4.5),
        ] {
            assert_eq!(datum.to_f64().unwrap().value, expected, "{datum:?}");
        }

        // `MyDecimal.FromString` keeps the accepted `1.1` prefix and reports
        // ErrTruncated for the trailing `.1`; it does not fall back to zero.
        let malformed = Datum::new_string("1.1.1").to_decimal().unwrap();
        assert_eq!(malformed.value, Decimal::from_signed_literal("1.1"));
        assert_eq!(
            malformed.event,
            Some(crate::ScalarConversionEvent::Truncated)
        );
    }

    /// Go `ConvertDatumToDecimal` keeps `MyDecimal.FromFloat64`'s best-effort
    /// value and returns its overflow error. A `KindFloat32` is first widened
    /// from the stored float32 bits (`GetFloat32`), so it must not reuse the
    /// raw float64 payload as if it were a double.
    #[test]
    fn source_float_to_decimal_preserves_error_and_float32_precision() {
        let overflow = Datum::Real(1e308).to_decimal().unwrap();
        assert!(matches!(
            overflow.event,
            Some(ScalarConversionEvent::Overflow(_))
        ));
        assert_eq!(overflow.value.to_string(), "9".repeat(81));

        let float32 = Datum::Float32(3.1).to_decimal().unwrap();
        assert_eq!(float32.event, None);
        assert_eq!(float32.value.to_string(), "3.0999999046325684");
    }

    /// Go's `ToInt64` keeps `KindBinaryLiteral` on the bounded signed path,
    /// while `KindMysqlBit` deliberately reinterprets the unsigned payload.
    /// The two datum kinds must therefore differ for a value above int64::MAX.
    #[test]
    fn source_binary_literal_to_i64_saturates_but_mysql_bit_reinterprets() {
        let payload = BinaryLiteral::from(vec![0xff; 8]);

        let literal = Datum::new_binary_literal(payload.clone()).to_i64().unwrap();
        assert_eq!(literal.value, i64::MAX);
        assert!(matches!(
            literal.event,
            Some(ScalarConversionEvent::Overflow(_))
        ));

        let bit = Datum::new_mysql_bit(payload.clone()).to_i64().unwrap();
        assert_eq!(bit.value, -1);
        assert_eq!(bit.event, None);

        let wide = BinaryLiteral::from(vec![1; 9]);
        let literal = Datum::new_binary_literal(wide.clone()).to_i64().unwrap();
        assert_eq!(literal.value, 0);
        assert_eq!(literal.event, Some(ScalarConversionEvent::Truncated));

        let bit = Datum::new_mysql_bit(wide).to_i64().unwrap();
        assert_eq!(bit.value, -1);
        assert_eq!(bit.event, Some(ScalarConversionEvent::Truncated));

        let longlong = FieldType::new(FieldTypeCode::LongLong);
        let converted = Datum::new_binary_literal(payload)
            .convert_to(&longlong, ConversionFlags::default())
            .unwrap();
        assert_eq!(converted.value, Datum::Int(i64::MAX));
        assert!(matches!(
            converted.event,
            Some(ScalarConversionEvent::Overflow(_))
        ));
    }

    #[test]
    fn test_to_bytes() {
        for (datum, expected) in [
            (Datum::Int(1), b"1".as_slice()),
            (Datum::new_decimal(Decimal::from_int(1)), b"1".as_slice()),
            (Datum::Real(1.23), b"1.23".as_slice()),
            (Datum::new_string("abc"), b"abc".as_slice()),
            (Datum::Null, b"".as_slice()),
        ] {
            assert_eq!(datum.to_bytes().unwrap(), expected, "{datum:?}");
        }
    }

    /// `Datum::to_mysql_json_with_source_type`: a BINARY-charset argument
    /// embeds the source column's own MySQL type code as a JSON `Opaque`
    /// value, Go's `getRealJSONValue`
    /// (`pkg/executor/aggfuncs/func_json_objectagg.go`), the value rule
    /// `JSON_ARRAYAGG`/`JSON_OBJECTAGG` share.
    ///
    /// Every expected string below is captured verbatim from a real TiDB
    /// server (`zz_dump_opaque_test.go`, `TestZZDumpOpaque`):
    /// `SELECT JSON_ARRAYAGG(col) FROM t` over one-column tables of each
    /// listed type, each holding the two-byte string `"ab"`.
    #[test]
    fn to_mysql_json_with_source_type_matches_captured_opaque_rendering() {
        use crate::{FieldType, FieldTypeCode};

        // VARBINARY(10): mysql.TypeVarchar (15) -- VARBINARY and VARCHAR
        // share this parse-time code, so the binary distinction rides the
        // collation, not the code, at DDL time.
        let varbinary = FieldType::new(FieldTypeCode::Varchar).with_collation(Collation::Binary);
        assert_eq!(
            Datum::new_bytes(*b"ab")
                .to_mysql_json_with_source_type(&varbinary)
                .unwrap()
                .to_string(),
            "\"base64:type15:YWI=\""
        );

        // BINARY(3): mysql.TypeString (254), fixed-length and zero-padded to
        // `flen` before encoding -- the captured `YWIA` decodes to
        // `61 62 00` (`ab\0`), the tailing pad byte included.
        let mut binary = FieldType::new(FieldTypeCode::String);
        binary.set_flen(3);
        assert_eq!(
            Datum::new_bytes(*b"ab")
                .to_mysql_json_with_source_type(&binary)
                .unwrap()
                .to_string(),
            "\"base64:type254:YWIA\""
        );

        // TINYBLOB/BLOB/MEDIUMBLOB/LONGBLOB: mysql.Type{Tiny,Medium,Long}Blob
        // and mysql.TypeBlob (249/250/251/252), never padded.
        for (code, expected) in [
            (FieldTypeCode::TinyBlob, "\"base64:type249:YWI=\""),
            (FieldTypeCode::MediumBlob, "\"base64:type250:YWI=\""),
            (FieldTypeCode::LongBlob, "\"base64:type251:YWI=\""),
            (FieldTypeCode::Blob, "\"base64:type252:YWI=\""),
        ] {
            let field_type = FieldType::new(code);
            assert_eq!(
                Datum::new_bytes(*b"ab")
                    .to_mysql_json_with_source_type(&field_type)
                    .unwrap()
                    .to_string(),
                expected,
                "{code:?}"
            );
        }

        // `CAST(x AS BINARY)`: mysql.TypeVarString (253), captured from
        // `JSON_ARRAY(CAST('ab' AS BINARY))` = `["base64:type253:YWI="]`.
        let cast_binary = FieldType::new(FieldTypeCode::VarString);
        assert_eq!(
            Datum::new_bytes(*b"ab")
                .to_mysql_json_with_source_type(&cast_binary)
                .unwrap()
                .to_string(),
            "\"base64:type253:YWI=\""
        );

        // A non-binary-charset argument (an ordinary VARCHAR column) is
        // unaffected: it stays a plain JSON string, matching
        // `to_mysql_json`.
        let varchar = FieldType::new(FieldTypeCode::Varchar);
        assert_eq!(
            Datum::new_string("ab")
                .to_mysql_json_with_source_type(&varchar)
                .unwrap()
                .to_string(),
            "\"ab\""
        );
    }

    /// `JSON_TYPE()` of a BINARY-charset opaque value reports `"BLOB"`, not
    /// `"OPAQUE"` -- captured: `JSON_TYPE(JSON_EXTRACT(arrayagg_result,
    /// '$[0]'))` over a VARBINARY-sourced element is `"BLOB"`.
    #[test]
    fn opaque_json_type_of_binary_charset_value_is_blob() {
        use crate::{FieldType, FieldTypeCode};

        let varbinary = FieldType::new(FieldTypeCode::Varchar).with_collation(Collation::Binary);
        let opaque = Datum::new_bytes(*b"ab")
            .to_mysql_json_with_source_type(&varbinary)
            .unwrap();
        assert_eq!(opaque.type_name().unwrap(), "BLOB");
    }
}

#[cfg(test)]
#[test]
fn shared_mysql_json_facade_keeps_actual_kinds_errors_and_effective_source_metadata() {
    use crate::{
        BinaryJSONError, CoreTime, DatumKind, FieldType, FieldTypeCode, MySqlDuration, MysqlEnum,
        MysqlSet, Time, TimeType, VectorFloat32,
    };
    fn assert_parts(value: BinaryJSON, kind: u8, bytes: &[u8]) {
        assert_eq!(value.type_code(), kind);
        assert_eq!(value.value(), bytes);
    }
    let cases = vec![
        (Datum::Null, 0x04, vec![0]),
        (Datum::Int(-1), 0x09, vec![0xff; 8]),
        (Datum::UInt(u64::MAX), 0x0a, vec![0xff; 8]),
        (
            Datum::Real(1.25),
            0x0b,
            1.25_f64.to_bits().to_le_bytes().to_vec(),
        ),
        (
            Datum::Float32(16_777_217.0),
            0x0b,
            16_777_217.0_f64.to_bits().to_le_bytes().to_vec(),
        ),
        (
            Datum::Decimal(
                Decimal::from_raw_parts(false, b"125".to_vec(), 1, 2).with_declared_shape(20, 8),
            ),
            0x0b,
            1.3_f64.to_bits().to_le_bytes().to_vec(),
        ),
        (Datum::new_string("s"), 0x0c, vec![1, b's']),
        (Datum::Bytes(vec![b'b']), 0x0c, vec![1, b'b']),
        (
            Datum::BinaryLiteral(BinaryLiteral::from(vec![b'l'])),
            0x0c,
            vec![1, b'l'],
        ),
        (
            Datum::Bit(BinaryLiteral::from(vec![b't'])),
            0x0c,
            vec![1, b't'],
        ),
        (
            Datum::new_enum(MysqlEnum::new("e", 99), Collation::Binary),
            0x0c,
            vec![1, b'e'],
        ),
        (
            Datum::new_set(MysqlSet::new("s", 99), Collation::Binary),
            0x0c,
            vec![1, b's'],
        ),
        (Datum::Raw(vec![b'r']), 0x0c, vec![1, b'r']),
        (
            Datum::Time(Time::from_raw_parts(
                CoreTime::from_raw(u64::MAX),
                TimeType::Timestamp,
                u8::MAX,
            )),
            0x10,
            vec![0xff; 8],
        ),
        (
            Datum::Duration(MySqlDuration::from_raw_parts(-1, -1)),
            0x11,
            vec![0xff; 12],
        ),
        (
            Datum::Json(BinaryJSON::from_encoded_parts(0x03, vec![0xff])),
            0x03,
            vec![0xff],
        ),
        (
            Datum::VectorFloat32(VectorFloat32::default()),
            0x0c,
            vec![2, b'[', b']'],
        ),
    ];
    for (value, kind, bytes) in cases {
        assert_parts(value.to_mysql_json().unwrap(), kind, &bytes);
    }
    for value in [
        Datum::new_string(vec![0xff]),
        Datum::Bytes(vec![0xff]),
        Datum::BinaryLiteral(BinaryLiteral::from(vec![0xff])),
        Datum::Bit(BinaryLiteral::from(vec![0xff])),
    ] {
        assert!(matches!(
            value.to_mysql_json(),
            Err(DatumValueError::InvalidUtf8(_))
        ));
    }
    for value in [
        Datum::new_enum(MysqlEnum::new([0xff], 1), Collation::Binary),
        Datum::new_set(MysqlSet::new([0xff], 1), Collation::Binary),
        Datum::Raw(vec![0xff]),
        Datum::MinNotNull,
        Datum::MaxValue,
    ] {
        assert_eq!(
            value.to_mysql_json().unwrap_err(),
            DatumValueError::Unsupported(value.kind(), "json")
        );
    }
    assert_eq!(
        Datum::Float32(f64::INFINITY).to_mysql_json().unwrap_err(),
        DatumValueError::Json(BinaryJSONError::InvalidText)
    );
    assert_eq!(
        Datum::MinNotNull.to_mysql_json().unwrap_err(),
        DatumValueError::Unsupported(DatumKind::MinNotNull, "json")
    );
    let fixed = FieldType::new(FieldTypeCode::String)
        .with_collation_name("binary")
        .with_flen(5);
    assert_parts(
        Datum::new_string("abcd")
            .to_mysql_json_with_source_type(&fixed)
            .unwrap(),
        0x0d,
        &[254, 5, b'a', b'b', b'c', b'd', 0],
    );
    assert_parts(
        Datum::Bytes(b"abcd".to_vec())
            .to_mysql_json_with_source_type(&fixed.clone().with_flen(2))
            .unwrap(),
        0x0d,
        &[254, 2, b'a', b'b'],
    );
    let unknown = FieldType::new(FieldTypeCode::Unknown(254))
        .with_collation_name("binary")
        .with_flen(5);
    assert_parts(
        Datum::Bytes(b"ab".to_vec())
            .to_mysql_json_with_source_type(&unknown)
            .unwrap(),
        0x0d,
        &[254, 2, b'a', b'b'],
    );
    assert_parts(
        Datum::new_string("ab")
            .to_mysql_json_with_source_type(&unknown)
            .unwrap(),
        0x0c,
        &[2, b'a', b'b'],
    );
    let array = fixed.clone().with_array(true);
    assert_parts(
        Datum::Bytes(vec![0xff])
            .to_mysql_json_with_source_type(&array)
            .unwrap(),
        0x0d,
        &[245, 1, 0xff],
    );
    assert!(matches!(
        Datum::new_string(vec![0xff]).to_mysql_json_with_source_type(&array),
        Err(DatumValueError::InvalidUtf8(_))
    ));
    let not_binary = fixed
        .clone()
        .with_charset_name("binary")
        .with_collation_name("utf8mb4_bin");
    assert_parts(
        Datum::new_string("ab")
            .to_mysql_json_with_source_type(&not_binary)
            .unwrap(),
        0x0c,
        &[2, b'a', b'b'],
    );
    assert_parts(
        Datum::new_string("ab")
            .to_mysql_json_with_source_type(&fixed.with_collation_name("BINARY"))
            .unwrap(),
        0x0c,
        &[2, b'a', b'b'],
    );
}
