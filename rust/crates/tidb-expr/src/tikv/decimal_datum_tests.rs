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

#[test]
fn decimal_datum_conversion_preserves_source_events_shape_and_unsigned_consumers() {
    use crate::{Columns, Datum};
    use tidb_ast::CastType;
    use tidb_datatype::{
        BinaryJSON, BinaryLiteral, Collation, DatumValueError, Decimal, FieldTypeCode, GoString,
        MysqlEnum, MysqlSet, ScalarConversionError, ScalarConversionEvent, SessionTimeZone,
        JSON_LITERAL_FALSE, JSON_LITERAL_NULL, JSON_TYPE_CODE_ARRAY, JSON_TYPE_CODE_FLOAT64,
        JSON_TYPE_CODE_LITERAL, JSON_TYPE_CODE_STRING,
    };

    for (value, expected, truncated) in [
        (Datum::Real(16_777_217.0), "16777217", false),
        (Datum::Float32(16_777_217.0), "16777216", false),
        (
            Datum::Enum(
                MysqlEnum::new(GoString::from_bytes([0xff]), 17),
                Collation::DEFAULT,
            ),
            "17",
            false,
        ),
        (
            Datum::Set(MysqlSet::new("2024-01-01", u64::MAX), Collation::DEFAULT),
            "18446744073709551615",
            false,
        ),
        (Datum::new_string("123abc"), "123", true),
        (
            Datum::BinaryLiteral(BinaryLiteral::from(vec![1; 9])),
            "18446744073709551615",
            true,
        ),
        (
            Datum::Bit(BinaryLiteral::from(vec![0, 0, 1, 2])),
            "258",
            false,
        ),
        (
            Datum::Json(BinaryJSON::from_encoded_parts(
                JSON_TYPE_CODE_LITERAL,
                vec![JSON_LITERAL_FALSE],
            )),
            "0",
            false,
        ),
        (
            Datum::Json(BinaryJSON::from_encoded_parts(
                JSON_TYPE_CODE_LITERAL,
                vec![JSON_LITERAL_NULL],
            )),
            "0",
            true,
        ),
        (
            Datum::Json(BinaryJSON::from_encoded_parts(
                JSON_TYPE_CODE_LITERAL,
                vec![],
            )),
            "0",
            true,
        ),
        (
            Datum::Json(BinaryJSON::from_encoded_parts(
                JSON_TYPE_CODE_LITERAL,
                vec![0xfe],
            )),
            "1",
            false,
        ),
        (
            Datum::Json(BinaryJSON::from_encoded_parts(JSON_TYPE_CODE_ARRAY, vec![])),
            "0",
            true,
        ),
        (
            Datum::Json(BinaryJSON::from_encoded_parts(
                JSON_TYPE_CODE_STRING,
                vec![6, b'1', b'2', b'3', b'a', b'b', b'c'],
            )),
            "123",
            true,
        ),
        (
            Datum::Json(BinaryJSON::from_encoded_parts(
                JSON_TYPE_CODE_STRING,
                vec![4, b'1', b'2', 0xff, b'3'],
            )),
            "0",
            true,
        ),
        (
            Datum::Json(BinaryJSON::from_encoded_parts(
                JSON_TYPE_CODE_FLOAT64,
                1.5_f64.to_le_bytes().to_vec(),
            )),
            "1.5",
            false,
        ),
        (
            Datum::Json(BinaryJSON::from_encoded_parts(
                JSON_TYPE_CODE_FLOAT64,
                f64::INFINITY.to_le_bytes().to_vec(),
            )),
            "0",
            true,
        ),
    ] {
        let converted = value.to_decimal().unwrap();
        assert_eq!(converted.value.to_string(), expected);
        assert_eq!(
            converted.event,
            truncated.then_some(ScalarConversionEvent::Truncated)
        );
        if let Datum::Json(json) = &value {
            assert_eq!(tidb_datatype::json_to_decimal(json), converted);
        }
    }
    let overflow = Datum::new_string("1e999").to_decimal().unwrap();
    assert_eq!(overflow.value.to_string(), "9".repeat(81));
    assert_eq!(
        overflow.event,
        Some(ScalarConversionEvent::Overflow(
            ScalarConversionError::Overflow {
                value: "1e999".to_owned(),
                target: FieldTypeCode::NewDecimal,
            }
        ))
    );
    // Datum text uses lossy UTF-8 and retains the numeric prefix, whereas the
    // same invalid bytes inside JSON above become an empty string before parsing.
    for value in [
        Datum::new_bytes([b'1', b'2', 0xff, b'3']),
        Datum::new_string(vec![b'1', b'2', 0xff, b'3']),
    ] {
        let converted = value.to_decimal().unwrap();
        assert_eq!(converted.value.to_string(), "12");
        assert_eq!(converted.event, Some(ScalarConversionEvent::Truncated));
    }
    for value in [
        Datum::Null,
        Datum::Raw(vec![b'1']),
        Datum::MinNotNull,
        Datum::MaxValue,
    ] {
        assert!(
            matches!(value.to_decimal(), Err(DatumValueError::Unsupported(kind, "decimal")) if kind == value.kind())
        );
    }
    let original =
        Decimal::from_raw_parts(true, b"1234500".to_vec(), 2, 4).with_declared_shape(12, 2);
    let converted = Datum::Decimal(original.clone()).to_decimal().unwrap();
    assert_eq!(converted.event, None);
    let expected = original.as_shared_parse();
    let actual = converted.value.as_shared_parse();
    assert_eq!(actual.negative, expected.negative);
    assert_eq!(actual.digits, expected.digits);
    assert_eq!(actual.scale, expected.scale);
    assert_eq!(actual.storage_scale, expected.storage_scale);
    assert_eq!(actual.declared_shape, expected.declared_shape);

    struct Quiet;
    impl Columns for Quiet {
        fn get(&self, _: &[String]) -> Option<Datum> {
            panic!("unsigned value consumer must not read children");
        }
        fn time_zone(&self) -> SessionTimeZone {
            panic!("unsigned hybrid/literal fallback must not read the zone");
        }
        fn append_warning(&self, _: u16, _: &str) {
            panic!("unsigned Other-source fallback drops decimal conversion events");
        }
    }
    for (value, expected) in [
        (
            Datum::Enum(
                MysqlEnum::new(GoString::from_bytes([0xff]), 17),
                Collation::DEFAULT,
            ),
            17,
        ),
        (
            Datum::Set(MysqlSet::new("mask", u64::MAX), Collation::DEFAULT),
            u64::MAX,
        ),
        (Datum::Bit(BinaryLiteral::from(vec![1; 9])), u64::MAX),
        (
            Datum::BinaryLiteral(BinaryLiteral::from(vec![1; 9])),
            u64::MAX,
        ),
    ] {
        assert_eq!(
            crate::cast::eval_cast(&CastType::Unsigned, value, None, &Quiet),
            Ok(Datum::UInt(expected))
        );
    }
}
