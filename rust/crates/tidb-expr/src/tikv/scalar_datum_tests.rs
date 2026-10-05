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
fn scalar_datum_bridge_keeps_truth_float_domains_events_and_cast_consumers() {
    use crate::{context::NoColumns, Datum};
    use tidb_ast::CastType;
    use tidb_datatype::{
        BinaryJSON, BinaryLiteral, Collation, CoreTime, DatumValueError, Decimal, GoString,
        MySqlDuration, MysqlEnum, MysqlSet, ScalarConversionEvent, Time, TimeType, VectorFloat32,
        JSON_TYPE_CODE_STRING,
    };

    for (value, boolean, float, truncated) in [
        (Datum::Int(-7), 1, -7.0, false),
        (Datum::UInt(0), 0, 0.0, false),
        // Bool reads Float32's actual f64 carrier; ToFloat64 narrows it first.
        (Datum::Float32(1e-50), 1, 0.0, false),
        (Datum::Float32(16_777_217.0), 1, 16_777_216.0, false),
        (Datum::Real(16_777_217.0), 1, 16_777_217.0, false),
        (Datum::new_string("12x"), 1, 12.0, true),
        (Datum::new_bytes(b"0x".to_vec()), 0, 0.0, true),
        (
            Datum::Enum(
                MysqlEnum::new(GoString::from_bytes([0xff]), 0),
                Collation::DEFAULT,
            ),
            0,
            0.0,
            false,
        ),
        (
            Datum::Set(MysqlSet::new("0", 17), Collation::DEFAULT),
            1,
            17.0,
            false,
        ),
        (
            Datum::Bit(BinaryLiteral::from(vec![1; 9])),
            1,
            u64::MAX as f64,
            true,
        ),
        (
            Datum::BinaryLiteral(BinaryLiteral::from(vec![0; 9])),
            0,
            0.0,
            false,
        ),
        (
            Datum::Decimal(Decimal::from_literal("1.25")),
            1,
            1.25,
            false,
        ),
        (
            Datum::Time(Time::from_raw_parts(
                CoreTime::from_date(2024, 1, 2, 0, 0, 0, 0),
                TimeType::Date,
                0,
            )),
            1,
            20240102.0,
            false,
        ),
        // Duration truth sees the raw nanos, not the zero-FSP numeric spelling.
        (
            Datum::Duration(MySqlDuration::from_raw_parts(-1, 0)),
            1,
            0.0,
            false,
        ),
    ] {
        let event = truncated.then_some(ScalarConversionEvent::Truncated);
        let truth = value.to_bool().unwrap();
        assert_eq!(truth.value, boolean);
        assert_eq!(truth.event, event);
        let number = value.to_f64().unwrap();
        assert_eq!(number.value, float);
        assert_eq!(number.event, event);
        assert_eq!(crate::coerce::truthy_of(&value), Ok(Some(boolean != 0)));
    }
    let raw_zero = Datum::Decimal(Decimal::from_raw_parts(true, b"000".to_vec(), 2, 2));
    assert_eq!(raw_zero.to_bool().unwrap().value, 0);
    assert_eq!(raw_zero.to_bool().unwrap().event, None);
    for value in [Datum::Real(-0.0), Datum::Float32(-0.0)] {
        assert_eq!(value.to_bool().unwrap().value, 0);
        assert_eq!(
            value.to_f64().unwrap().value.to_bits(),
            (-0.0_f64).to_bits()
        );
    }
    // JSON truth compares the original typed document against JSON numeric zero.
    // Thus JSON false and JSON string "0" are not the numeric zero document.
    for (document, boolean, float, truncated) in [
        ("0", 0, 0.0, false),
        ("false", 1, 0.0, false),
        ("null", 1, 0.0, true),
        ("\"0\"", 1, 0.0, false),
        ("\"12x\"", 1, 12.0, true),
        ("[]", 1, 0.0, true),
    ] {
        let json = BinaryJSON::parse(document).unwrap();
        let value = Datum::Json(json.clone());
        let truth = value.to_bool().unwrap();
        assert_eq!(truth.value, boolean, "{document}");
        assert_eq!(truth.event, None);
        let number = value.to_f64().unwrap();
        assert_eq!(number.value, float);
        assert_eq!(
            number.event,
            truncated.then_some(ScalarConversionEvent::Truncated)
        );
        assert_eq!(tidb_datatype::json_to_float(&json), number);
    }
    for value in [
        Datum::new_string(vec![b'1', 0xff]),
        Datum::new_bytes([b'1', 0xff]),
    ] {
        assert!(matches!(
            value.to_bool(),
            Err(DatumValueError::InvalidUtf8(_))
        ));
        assert!(matches!(
            value.to_f64(),
            Err(DatumValueError::InvalidUtf8(_))
        ));
    }
    let json = BinaryJSON::from_encoded_parts(JSON_TYPE_CODE_STRING, vec![2, b'1', 0xff]);
    let float = tidb_datatype::json_to_float(&json);
    assert_eq!(float.value, 0.0);
    assert_eq!(float.event, Some(ScalarConversionEvent::Truncated));
    assert_eq!(Datum::Json(json).to_f64().unwrap(), float);
    for (vector, boolean) in [
        (VectorFloat32::default(), 0),
        (VectorFloat32::parse("[0]").unwrap(), 1),
    ] {
        let value = Datum::VectorFloat32(vector);
        assert_eq!(value.to_bool().unwrap().value, boolean);
        assert_eq!(value.to_bool().unwrap().event, None);
        assert!(
            matches!(value.to_f64(), Err(DatumValueError::Unsupported(kind, "float64")) if kind == value.kind())
        );
    }
    for value in [
        Datum::Null,
        Datum::Raw(vec![b'1']),
        Datum::MinNotNull,
        Datum::MaxValue,
    ] {
        assert!(
            matches!(value.to_bool(), Err(DatumValueError::Unsupported(kind, "bool")) if kind == value.kind())
        );
        assert!(
            matches!(value.to_f64(), Err(DatumValueError::Unsupported(kind, "float64")) if kind == value.kind())
        );
    }
    assert_eq!(crate::coerce::truthy_of(&Datum::Null), Ok(None));
    assert_eq!(
        crate::cast::eval_cast(&CastType::Double, Datum::Float32(1e-50), None, &NoColumns),
        Ok(Datum::Real(0.0)),
    );
}
