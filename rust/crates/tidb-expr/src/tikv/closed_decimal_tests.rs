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
fn closed_decimal_and_numeric_coercion_keep_storage_effects_signedness_and_truth_domains() {
    use super::eval_cast_decimal_in;
    use crate::coerce::{
        integer_bits, integer_cmp, integer_of, integer_to_decimal, integer_to_f64, truthy_of,
        Integer,
    };
    use crate::{Columns, Datum, EvalError};
    use std::{cell::RefCell, cmp::Ordering};
    use tidb_datatype::{
        BinaryJSON, BinaryLiteral, Collation, Decimal, GoString, MysqlEnum, SessionTimeZone,
        VectorFloat32,
    };

    #[derive(Default)]
    struct Session(RefCell<Vec<(u16, String)>>);
    impl Columns for Session {
        fn get(&self, _: &[String]) -> Option<Datum> {
            panic!("operand already evaluated")
        }
        fn time_zone(&self) -> SessionTimeZone {
            panic!("decimal cast has no zone demand")
        }
        fn handle_truncate(&self, _: &str) -> Result<(), EvalError> {
            panic!("decimal cast appends directly")
        }
        fn append_warning(&self, code: u16, message: &str) {
            self.0.borrow_mut().push((code, message.to_owned()));
        }
    }
    let ctx = Session::default();
    let decimal = |value: &Datum, flen, scale| {
        let Datum::Decimal(value) = eval_cast_decimal_in(&ctx, value, flen, scale).unwrap() else {
            panic!("expected DECIMAL storage")
        };
        value
    };
    assert_eq!(
        decimal(&Datum::new_string("999.9x"), 2, 0).to_string(),
        "99"
    );
    assert_eq!(
        ctx.0.take(),
        vec![
            (
                1292,
                "Truncated incorrect DECIMAL value: '999.9x'".to_owned()
            ),
            (1690, "DECIMAL value is out of range in '(2, 0)'".to_owned()),
        ]
    );
    let ordinal = Datum::Enum(
        MysqlEnum::new(GoString::from_bytes([0xff]), 17),
        Collation::DEFAULT,
    );
    let wide_literal = Datum::Bit(BinaryLiteral::from(vec![1; 9]));
    for (value, expected) in [
        (ordinal.clone(), "17"),
        (wide_literal.clone(), "18446744073709551615"),
        (Datum::Raw(b"42".to_vec()), "0"),
        (Datum::Json(BinaryJSON::parse("null").unwrap()), "0"),
        (Datum::Float32(16_777_217.0), "16777216"),
        (Datum::new_bytes([0xff]), "0"),
    ] {
        assert_eq!(decimal(&value, 0, u32::MAX).to_string(), expected);
        assert!(ctx.0.borrow().is_empty());
    }
    let original =
        Decimal::from_raw_parts(true, b"0001234567".to_vec(), 2, 4).with_declared_shape(20, 4);
    let actual = decimal(&Datum::Decimal(original.clone()), 2, u32::MAX);
    let expected = original.as_shared_parse();
    let actual = actual.as_shared_parse();
    assert_eq!(actual.negative, expected.negative);
    assert_eq!(actual.digits, expected.digits);
    assert_eq!(actual.scale, expected.scale);
    assert_eq!(actual.storage_scale, expected.storage_scale);
    assert_eq!(actual.declared_shape, expected.declared_shape);

    assert_eq!(integer_of(&ordinal), Ok(Some(Integer::Unsigned(17))));
    assert_eq!(
        integer_of(&wide_literal),
        Ok(Some(Integer::Unsigned(u64::MAX)))
    );
    assert_eq!(integer_of(&Datum::Int(-1)), Ok(Some(Integer::Signed(-1))));
    for value in [
        Datum::Null,
        Datum::new_string("12"),
        Datum::Raw(b"12".to_vec()),
        Datum::Real(12.0),
    ] {
        assert_eq!(integer_of(&value), Ok(None));
    }
    for (left, right, expected) in [
        (Integer::Signed(-1), Integer::Unsigned(0), Ordering::Less),
        (Integer::Unsigned(0), Integer::Signed(-1), Ordering::Greater),
        (
            Integer::Signed(i64::MAX),
            Integer::Unsigned(1_u64 << 63),
            Ordering::Less,
        ),
        (Integer::Signed(42), Integer::Unsigned(42), Ordering::Equal),
    ] {
        assert_eq!(integer_cmp(left, right), expected);
    }
    assert_eq!(integer_bits(Integer::Signed(-1)), u64::MAX);
    assert_eq!(
        integer_to_decimal(Integer::Unsigned(u64::MAX)).to_string(),
        "18446744073709551615"
    );
    assert_eq!(integer_to_f64(Integer::Unsigned(u64::MAX)), u64::MAX as f64);
    assert_eq!(truthy_of(&Datum::Null), Ok(None));
    for value in [
        Datum::Json(BinaryJSON::parse("null").unwrap()),
        Datum::Json(BinaryJSON::parse("false").unwrap()),
        Datum::VectorFloat32(VectorFloat32::parse("[0]").unwrap()),
        wide_literal,
    ] {
        assert_eq!(truthy_of(&value), Ok(Some(true)));
    }
    assert_eq!(
        truthy_of(&Datum::VectorFloat32(VectorFloat32::default())),
        Ok(Some(false))
    );
    for value in [Datum::MinNotNull, Datum::MaxValue] {
        assert_eq!(
            integer_of(&value),
            Err(EvalError::Unsupported("range sentinel integer coercion"))
        );
        assert_eq!(
            truthy_of(&value),
            Err(EvalError::Unsupported("truth coercion of a non-SQL datum"))
        );
    }
    for value in [Datum::Raw(b"1".to_vec()), Datum::new_bytes([0xff])] {
        assert_eq!(
            truthy_of(&value),
            Err(EvalError::Unsupported("truth coercion of a non-SQL datum"))
        );
    }
    assert!(ctx.0.borrow().is_empty());
}
