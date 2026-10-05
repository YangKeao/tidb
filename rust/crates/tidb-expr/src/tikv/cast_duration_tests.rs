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
fn duration_wrappers_preserve_source_kinds_lazy_zone_and_truncation_effects() {
    use super::{
        eval_cast_arg_as_duration_in, eval_cast_duration_in, eval_parse_computed_duration_in,
    };
    use crate::{Columns, Datum, EvalError};
    use std::cell::{Cell, RefCell};
    use tidb_datatype::{
        BinaryJSON, BinaryLiteral, Collation, CoreTime, Decimal, FieldType, FieldTypeCode,
        MySqlDuration, MysqlEnum, MysqlSet, SessionTimeZone, Time, TimeType, VectorFloat32,
    };

    #[derive(Default)]
    struct Session {
        events: RefCell<Vec<&'static str>>,
        messages: RefCell<Vec<String>>,
        warnings: RefCell<Vec<(u16, String)>>,
        strict: Cell<bool>,
    }
    impl Columns for Session {
        fn get(&self, _: &[String]) -> Option<Datum> {
            panic!("direct duration wrappers do not read AST columns");
        }
        fn time_zone(&self) -> SessionTimeZone {
            self.events.borrow_mut().push("zone");
            SessionTimeZone::utc()
        }
        fn append_warning(&self, code: u16, message: &str) {
            self.warnings.borrow_mut().push((code, message.to_owned()));
        }
        fn handle_truncate(&self, message: &str) -> Result<(), EvalError> {
            self.events.borrow_mut().push("truncate");
            self.messages.borrow_mut().push(message.to_owned());
            if self.strict.get() {
                return Err(EvalError::TruncatedWrongValue(message.to_owned()));
            }
            self.append_warning(1292, message);
            Ok(())
        }
    }
    impl Session {
        fn reset(&self) {
            self.events.borrow_mut().clear();
            self.messages.borrow_mut().clear();
            self.warnings.borrow_mut().clear();
            self.strict.set(false);
        }
        fn check(&self, events: &[&str], warning: Option<&str>) {
            assert_eq!(self.events.borrow().as_slice(), events);
            let expected: Vec<String> = warning.into_iter().map(str::to_owned).collect();
            assert_eq!(*self.messages.borrow(), expected);
            let expected: Vec<_> = warning
                .into_iter()
                .map(|text| (1292, text.to_owned()))
                .collect();
            assert_eq!(*self.warnings.borrow(), expected);
        }
    }
    enum Expected {
        Duration(i64, i64),
        Null,
        Unsupported(&'static str),
    }
    fn check(result: Result<Datum, EvalError>, expected: Expected) {
        match (result, expected) {
            (Ok(Datum::Duration(value)), Expected::Duration(nanos, fsp)) => {
                assert_eq!(value.nanoseconds(), nanos);
                assert_eq!(value.fsp(), fsp);
            }
            (Ok(Datum::Null), Expected::Null) => {}
            (Err(EvalError::Unsupported(message)), Expected::Unsupported(expected)) => {
                assert_eq!(message, expected)
            }
            (actual, _) => panic!("unexpected duration outcome: {actual:?}"),
        }
    }
    let ctx = Session::default();
    let (decimal, warning) = Decimal::parse_mysql("1.25");
    assert!(warning.is_none());
    let literal = BinaryLiteral::from_uint(49, None);
    let time = Time::new(
        CoreTime::from_date(2024, 1, 1, 0, 0, 1, 250000),
        TimeType::DateTime,
        2,
    )
    .unwrap();
    // All nineteen native Datum variants are actual constructed values. These
    // fixed source expectations do not use the provider as a value oracle.
    let cases = [
        (Datum::Null, Expected::Null, true, None),
        (
            Datum::MinNotNull,
            Expected::Unsupported("CAST AS TIME source datum"),
            true,
            None,
        ),
        (
            Datum::MaxValue,
            Expected::Unsupported("CAST AS TIME source datum"),
            true,
            None,
        ),
        (
            Datum::Int(123456),
            Expected::Duration(45_296_000_000_000, 2),
            false,
            None,
        ),
        (
            Datum::UInt(u64::MAX),
            Expected::Duration(-1_000_000_000, 2),
            false,
            None,
        ),
        (
            Datum::Decimal(decimal),
            Expected::Duration(1_250_000_000, 2),
            true,
            None,
        ),
        (
            Datum::Real(1.25),
            Expected::Duration(1_250_000_000, 2),
            true,
            None,
        ),
        (
            Datum::Float32(1.25),
            Expected::Duration(1_250_000_000, 2),
            true,
            None,
        ),
        (
            Datum::new_string("00:00:01.25"),
            Expected::Duration(1_250_000_000, 2),
            true,
            None,
        ),
        (
            Datum::Bytes(b"00:00:01.25".to_vec()),
            Expected::Duration(1_250_000_000, 2),
            true,
            None,
        ),
        (
            Datum::BinaryLiteral(literal.clone()),
            Expected::Unsupported("CAST AS TIME source datum"),
            true,
            None,
        ),
        (
            Datum::Duration(MySqlDuration::from_raw_parts(1_250_000_000, 2)),
            Expected::Duration(1_250_000_000, 2),
            true,
            None,
        ),
        (
            Datum::Enum(MysqlEnum::new("1", 1), Collation::DEFAULT),
            Expected::Unsupported("CAST AS TIME source datum"),
            true,
            None,
        ),
        (
            Datum::Bit(literal),
            Expected::Unsupported("CAST AS TIME source datum"),
            true,
            None,
        ),
        (
            Datum::Set(MysqlSet::new("1", 1), Collation::DEFAULT),
            Expected::Unsupported("CAST AS TIME source datum"),
            true,
            None,
        ),
        (
            Datum::Time(time),
            Expected::Duration(1_250_000_000, 2),
            true,
            None,
        ),
        (
            Datum::Json(BinaryJSON::from_encoded_parts(0x04, vec![1])),
            Expected::Null,
            false,
            Some("Truncated incorrect time value: 'true'"),
        ),
        (
            Datum::Raw(b"1".to_vec()),
            Expected::Unsupported("CAST AS TIME source datum"),
            true,
            None,
        ),
        (
            Datum::VectorFloat32(VectorFloat32::default()),
            Expected::Unsupported("CAST AS TIME source datum"),
            true,
            None,
        ),
    ];
    assert_eq!(cases.len(), 19);
    for (value, expected, zone, warning) in cases {
        ctx.reset();
        check(eval_cast_duration_in(&ctx, &value, None, 2), expected);
        let mut events = Vec::new();
        if zone {
            events.push("zone");
        }
        if warning.is_some() {
            events.push("truncate");
        }
        ctx.check(&events, warning);
    }
    let integer = FieldType::new(FieldTypeCode::LongLong);
    for value in [Datum::Null, Datum::new_string("1")] {
        ctx.reset();
        check(
            eval_cast_duration_in(&ctx, &value, Some(&integer), 0),
            Expected::Unsupported("CAST AS TIME integer datum"),
        );
        ctx.check(&[], None);
    }
    ctx.reset();
    check(
        eval_cast_duration_in(&ctx, &Datum::UInt(u64::MAX), Some(&integer), 3),
        Expected::Duration(-1_000_000_000, 3),
    );
    ctx.check(&[], None);
    ctx.reset();
    check(
        eval_cast_duration_in(&ctx, &Datum::Int(9_000_000), None, 0),
        Expected::Null,
    );
    ctx.check(
        &["truncate"],
        Some("Truncated incorrect time value: '9000000'"),
    );
    let overflow = Datum::new_string("900:00:00");
    ctx.reset();
    check(
        eval_cast_duration_in(&ctx, &overflow, None, 0),
        Expected::Duration(3_020_399_000_000_000, 0),
    );
    ctx.check(
        &["zone", "truncate"],
        Some("Truncated incorrect time value: '900:00:00'"),
    );
    let real = FieldType::new(FieldTypeCode::Double);
    ctx.reset();
    check(
        eval_cast_duration_in(&ctx, &overflow, Some(&real), 0),
        Expected::Null,
    );
    ctx.check(
        &["zone", "truncate"],
        Some("Truncated incorrect time value: '900:00:00'"),
    );
    ctx.reset();
    ctx.strict.set(true);
    assert!(
        matches!(eval_cast_duration_in(&ctx, &overflow, None, 0), Err(EvalError::TruncatedWrongValue(message)) if message == "Truncated incorrect time value: '900:00:00'")
    );
    assert_eq!(ctx.events.borrow().as_slice(), &["zone", "truncate"]);
    assert_eq!(
        ctx.messages.borrow().as_slice(),
        &["Truncated incorrect time value: '900:00:00'"]
    );
    assert!(ctx.warnings.borrow().is_empty());

    // Existing duration and NULL argument values skip rendering, source
    // selection and zone demand, preserving even raw noncanonical FSP.
    for (nanos, fsp) in [(1001, 255), (i64::MAX, -123)] {
        ctx.reset();
        check(
            eval_cast_arg_as_duration_in(
                &ctx,
                &Datum::Duration(MySqlDuration::from_raw_parts(nanos, fsp)),
                Some(&integer),
            ),
            Expected::Duration(nanos, fsp),
        );
        ctx.check(&[], None);
    }
    ctx.reset();
    check(
        eval_cast_arg_as_duration_in(&ctx, &Datum::Null, Some(&integer)),
        Expected::Null,
    );
    ctx.check(&[], None);
    for (field, fsp) in [
        (FieldType::new(FieldTypeCode::Date).with_decimal(2), 2),
        (FieldType::new(FieldTypeCode::Datetime).with_decimal(2), 2),
        (FieldType::new(FieldTypeCode::Timestamp).with_decimal(2), 2),
        (
            FieldType::new(FieldTypeCode::Unknown(10)).with_decimal(2),
            6,
        ),
        (
            FieldType::new(FieldTypeCode::Date)
                .with_decimal(2)
                .with_array(true),
            6,
        ),
    ] {
        ctx.reset();
        check(
            eval_cast_arg_as_duration_in(&ctx, &Datum::new_string("12:34:56.12"), Some(&field)),
            Expected::Duration(45_296_120_000_000, fsp),
        );
        ctx.check(&["zone"], None);
    }
    // Computed precision counts the untrimmed suffix's UTF-8 BYTES; JSON's
    // closing quote counts too. Conversion subsequently trims/unquotes itself.
    for (value, fsp) in [
        (Datum::new_string("12:34:56.12"), 2),
        (Datum::new_string("12:34:56.12\u{2003}"), 5),
        (Datum::new_string("12:34:56.120000   "), 6),
        (
            Datum::Json(BinaryJSON::from_encoded_parts(
                0x0c,
                b"\x0b12:34:56.12".to_vec(),
            )),
            3,
        ),
    ] {
        ctx.reset();
        check(
            eval_parse_computed_duration_in(&ctx, &value),
            Expected::Duration(45_296_120_000_000, fsp),
        );
        ctx.check(&["zone"], None);
    }
    ctx.reset();
    check(
        eval_parse_computed_duration_in(&ctx, &Datum::Null),
        Expected::Null,
    );
    ctx.check(&["zone"], None);
    ctx.reset();
    check(
        eval_parse_computed_duration_in(&ctx, &Datum::Bytes(vec![255])),
        Expected::Null,
    );
    ctx.check(
        &["zone", "truncate"],
        Some("Truncated incorrect time value: '<binary>'"),
    );

    // This panic domain is source-known: ordinary rendering occurs BEFORE
    // JSON-tag rejection, even though finite JSON DOUBLE would warn/no-zone.
    ctx.reset();
    let infinity = Datum::Json(BinaryJSON::from_encoded_parts(
        0x0b,
        vec![0, 0, 0, 0, 0, 0, 240, 127],
    ));
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| eval_cast_duration_in(
            &ctx,
            &infinity,
            Some(&integer),
            0
        )))
        .is_err()
    );
    ctx.check(&[], None);
    // Direct wrapper/effect tests only: no SQL admission, worker-slot, physical
    // heap, or allocator-peak claims are made by these fixtures.
}
