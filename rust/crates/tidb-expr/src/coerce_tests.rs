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
fn coerce_string_preserves_nineteen_domains_utf8_classes_and_year_consumer() {
    use crate::coerce::coerce_str;
    use crate::{Columns, Datum, EvalError};
    use tidb_ast::CastType;
    use tidb_datatype::{
        BinaryJSON, BinaryLiteral, Collation, CoreTime, Decimal, MySqlDuration, MysqlEnum,
        MysqlSet, Time, TimeType, VectorFloat32,
    };

    let (decimal, warning) = Decimal::parse_mysql("1.25");
    assert!(warning.is_none());
    let literal = BinaryLiteral::from_uint(0x6162, None);
    let time = Time::new(
        CoreTime::from_date(2024, 1, 2, 3, 4, 5, 120000),
        TimeType::DateTime,
        2,
    )
    .unwrap();
    // Every row supplies a real native Datum. Expected strings and failures
    // are fixed source facts, not another renderer's output used as an oracle.
    let cases = [
        (Datum::Null, Ok(None)),
        (Datum::MinNotNull, Err("range sentinel string coercion")),
        (Datum::MaxValue, Err("range sentinel string coercion")),
        (Datum::Int(-7), Ok(Some("-7"))),
        (Datum::UInt(u64::MAX), Ok(Some("18446744073709551615"))),
        (Datum::Decimal(decimal), Ok(Some("1.25"))),
        (Datum::Real(16_777_217.0), Ok(Some("16777217"))),
        (Datum::Float32(16_777_217.0), Ok(Some("16777216"))),
        (Datum::new_string("é"), Ok(Some("é"))),
        (Datum::Bytes(b"bytes".to_vec()), Ok(Some("bytes"))),
        (Datum::BinaryLiteral(literal.clone()), Ok(Some("ab"))),
        (
            Datum::Duration(MySqlDuration::from_raw_parts(45_296_120_000_000, 2)),
            Ok(Some("12:34:56.12")),
        ),
        (
            Datum::Enum(MysqlEnum::new("enum-name", 7), Collation::DEFAULT),
            Ok(Some("enum-name")),
        ),
        (Datum::Bit(literal), Ok(Some("ab"))),
        (
            Datum::Set(MysqlSet::new("set-name", 5), Collation::DEFAULT),
            Ok(Some("set-name")),
        ),
        (Datum::Time(time), Ok(Some("2024-01-02 03:04:05.12"))),
        (
            Datum::Json(BinaryJSON::from_encoded_parts(0x04, vec![1])),
            Ok(Some("true")),
        ),
        (Datum::Raw(b"raw".to_vec()), Ok(Some("raw"))),
        (
            Datum::VectorFloat32(VectorFloat32::default()),
            Ok(Some("[]")),
        ),
    ];
    assert_eq!(cases.len(), 19);
    for (value, expected) in cases {
        match expected {
            Ok(text) => assert_eq!(coerce_str(&value).unwrap().as_deref(), text),
            Err(message) => assert!(
                matches!(coerce_str(&value), Err(EvalError::Unsupported(actual)) if actual == message)
            ),
        }
    }
    let invalid_literal = BinaryLiteral::from_uint(255, None);
    for (value, message) in [
        (Datum::new_string(vec![255]), "invalid UTF-8 string datum"),
        (Datum::Bytes(vec![255]), "invalid UTF-8 byte datum"),
        (
            Datum::BinaryLiteral(invalid_literal.clone()),
            "invalid UTF-8 binary literal",
        ),
        (Datum::Bit(invalid_literal), "invalid UTF-8 binary literal"),
        (
            Datum::Enum(MysqlEnum::new(vec![255], 7), Collation::DEFAULT),
            "invalid UTF-8 ENUM name",
        ),
        (
            Datum::Set(MysqlSet::new(vec![255], 5), Collation::DEFAULT),
            "invalid UTF-8 SET name",
        ),
        (Datum::Raw(vec![255]), "invalid UTF-8 raw datum"),
    ] {
        assert!(
            matches!(coerce_str(&value), Err(EvalError::Unsupported(actual)) if actual == message)
        );
    }
    // The checked expression helper uses Rust Display, not SQL's Go formatter.
    // Both sides have independent literal expectations; neither is an oracle.
    for (value, checked, sql) in [
        (Datum::Real(f64::INFINITY), "inf", "+Inf"),
        (Datum::Float32(f64::INFINITY), "inf", "+Inf"),
        (Datum::Real(f64::NEG_INFINITY), "-inf", "-Inf"),
        (Datum::Float32(f64::NEG_INFINITY), "-inf", "-Inf"),
        (Datum::Real(f64::NAN), "NaN", "NaN"),
        (Datum::Float32(f64::NAN), "NaN", "NaN"),
    ] {
        assert_eq!(coerce_str(&value).unwrap().as_deref(), Some(checked));
        assert_eq!(value.sql_string().unwrap(), sql);
    }
    let unknown = Datum::Json(BinaryJSON::from_encoded_parts(0xfe, vec![17, 23]));
    assert_eq!(coerce_str(&unknown).unwrap().as_deref(), Some(""));
    let infinity = Datum::Json(BinaryJSON::from_encoded_parts(
        0x0b,
        vec![0, 0, 0, 0, 0, 0, 240, 127],
    ));
    // Preserve the source-known root JSON nonfinite Display panic rather than
    // converting it into NULL, replacement text, or an unrelated UTF-8 error.
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| coerce_str(&infinity))).is_err()
    );

    struct Session;
    impl Columns for Session {
        fn get(&self, _: &[String]) -> Option<Datum> {
            panic!("direct YEAR consumer smoke must not perform AST lookup")
        }
    }
    // Consumer smoke only: YEAR remains native and this is not a closed-family
    // or SQL/row execution claim. Names precede the unchanged ordinal fallback.
    for (value, year) in [
        (
            Datum::Enum(MysqlEnum::new("2024-03-15", 7), Collation::DEFAULT),
            2024,
        ),
        (
            Datum::Set(MysqlSet::new("2023-02-01", 2), Collation::DEFAULT),
            2023,
        ),
        (
            Datum::Enum(MysqlEnum::new("not-a-date", 7), Collation::DEFAULT),
            7,
        ),
        (
            Datum::Set(MysqlSet::new("not-a-date", 5), Collation::DEFAULT),
            5,
        ),
    ] {
        assert_eq!(
            crate::cast::eval_cast(&CastType::Year, value, None, &Session).unwrap(),
            Datum::Int(year)
        );
    }
}
