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
fn argument_string_values_preserve_identity_collation_bytes_and_rust_float_display() {
    use crate::{context::NoColumns, Datum, EvalError};
    use tidb_datatype::{
        BinaryLiteral, Collation, FieldType, FieldTypeCode, GoString, MysqlEnum, MysqlSet,
    };

    let source = FieldType::new(FieldTypeCode::LongLong);
    let cast = |value: &Datum| crate::cast::cast_arg_as_string(value, Some(&source), &NoColumns);
    for value in [
        Datum::Null,
        Datum::new_collation_string([0xff, b'A'], Collation::Utf8Mb4GeneralCi),
        Datum::new_bytes([0xff, 0]),
        Datum::BinaryLiteral(BinaryLiteral::from(vec![0xff, 0])),
        Datum::Enum(
            MysqlEnum::new(GoString::from_bytes([0xff]), 17),
            Collation::Utf8Mb4GeneralCi,
        ),
        Datum::Set(
            MysqlSet::new(GoString::from_bytes([0xff]), 9),
            Collation::Utf8Mb4GeneralCi,
        ),
    ] {
        let actual = cast(&value).unwrap();
        assert_eq!(actual.kind(), value.kind());
        assert_eq!(actual.collation(), value.collation());
        assert_eq!(actual, value);
    }
    assert_eq!(
        cast(&Datum::Bit(BinaryLiteral::from(vec![0, 0xff]))),
        Ok(Datum::new_bytes([0, 0xff])),
    );
    for (value, bytes) in [
        (Datum::Raw(vec![0xff, 0]), vec![0xff, 0]),
        (Datum::Real(f64::INFINITY), b"inf".to_vec()),
        (Datum::Real(-0.0), b"-0".to_vec()),
        (Datum::Float32(1.234567890123), b"1.2345679".to_vec()),
        (Datum::Int(-7), b"-7".to_vec()),
    ] {
        assert_eq!(
            crate::coerce::coerce_str_bytes(&value),
            Ok(Some(bytes.clone()))
        );
        let actual = cast(&value).unwrap();
        assert!(matches!(actual, Datum::String(_)));
        assert_eq!(actual, Datum::new_string(bytes));
    }
    assert_eq!(crate::coerce::coerce_str_bytes(&Datum::Null), Ok(None));
    assert_eq!(
        crate::coerce::coerce_str_bytes(&Datum::new_bytes([0xff])),
        Ok(Some(vec![0xff]))
    );
    for value in [Datum::MinNotNull, Datum::MaxValue] {
        let error = Err(EvalError::Unsupported("range sentinel byte coercion"));
        assert_eq!(cast(&value), error);
        assert_eq!(
            crate::coerce::coerce_str_bytes(&value),
            Err(EvalError::Unsupported("range sentinel byte coercion"))
        );
    }
}

#[test]
fn argument_string_metadata_preserves_early_return_explicit_bit_and_source_widths() {
    use crate::cast::cast_arg_as_string_type;
    use crate::rewriter::result_type::string_cast_flen;
    use tidb_datatype::{FieldType, FieldTypeCode as C, FieldTypeFlags, UNSPECIFIED_LENGTH};

    let connection = ("utf8mb4", "utf8mb4_bin");
    for code in [C::VarString, C::Enum, C::Set, C::Unknown(13)] {
        let mut source = FieldType::new(code)
            .with_flen(42)
            .with_decimal(3)
            .with_raw_flags(1_u64 << 50);
        source.set_charset_name("latin1");
        source.set_collation_name("latin1_bin");
        for explicit in [false, true] {
            assert_eq!(
                cast_arg_as_string_type(&source, explicit, connection),
                source
            );
        }
        assert_eq!(string_cast_flen(&source), 42);
    }
    let mut bit = FieldType::new(C::Bit).with_flen(9);
    bit.set_charset_name("latin1");
    bit.set_collation_name("latin1_bin");
    for (explicit, charset, collation) in
        [(false, "binary", "binary"), (true, "latin1", "latin1_bin")]
    {
        let target = cast_arg_as_string_type(&bit, explicit, connection);
        assert_eq!(target.code(), C::VarString);
        assert_eq!(target.flen(), 2);
        assert_eq!(target.decimal(), UNSPECIFIED_LENGTH);
        assert_eq!(target.charset_name(), charset);
        assert_eq!(target.collation_name(), collation);
    }
    for (source, expected) in [
        (FieldType::new(C::Tiny).with_flen(4), 20),
        (FieldType::new(C::Bit).with_flen(-1), 0),
        (
            FieldType::new(C::NewDecimal).with_flen(10).with_decimal(2),
            13,
        ),
        (FieldType::new(C::NewDecimal).with_flen(-1), -1),
        (FieldType::new(C::Float).with_flen(12), 87),
        (FieldType::new(C::Double).with_flen(22), 370),
        (FieldType::new(C::Date).with_flen(-1).with_decimal(6), 10),
        (FieldType::new(C::Date).with_flen(123), 123),
        (
            FieldType::new(C::Datetime).with_flen(-1).with_decimal(3),
            23,
        ),
        (
            FieldType::new(C::Timestamp).with_flen(-1).with_decimal(6),
            26,
        ),
        (
            FieldType::new(C::Duration).with_flen(-1).with_decimal(2),
            13,
        ),
        (FieldType::new(C::Json).with_flen(-1), 4_294_967_295),
        (FieldType::new(C::VectorFloat32).with_flen(-1), -1),
        (
            FieldType::new(C::Enum).with_flags(FieldTypeFlags::ENUM_SET_AS_INT),
            20,
        ),
    ] {
        assert_eq!(string_cast_flen(&source), expected, "{source:?}");
        let target = cast_arg_as_string_type(&source, false, connection);
        assert_eq!(target.code(), C::VarString);
        assert_eq!(target.flen(), expected);
        assert_eq!(target.decimal(), UNSPECIFIED_LENGTH);
        if source.code() == C::Bit {
            assert_eq!(
                (target.charset_name(), target.collation_name()),
                ("binary", "binary")
            );
        } else {
            assert_eq!((target.charset_name(), target.collation_name()), connection);
        }
    }
}
