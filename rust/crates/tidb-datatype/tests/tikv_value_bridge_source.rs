// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Exact initial-domain transport assertions, not SQL-evaluation acceptance.
//!
//! FieldType/Datum cases follow the source metadata and representation contracts.
//! This source is included by the crate's existing `--test all` aggregator.

use tidb_datatype::tikv_compat::value::{
    from_scalar, project_field_type, snapshot_field_type, to_scalar, BridgeError, ValueMetadata,
};
use tidb_datatype::{
    BinaryLiteral, Collation, Datum, DatumKind, Decimal, FieldType, FieldTypeCode as C,
    FieldTypeFlags as F, GoString, StringDatum,
};
use tidb_query_datatype::codec::data_type::{ScalarValue, ScalarValueRef};
use tidb_query_datatype::EvalType;

fn output_metadata(kind: DatumKind) -> ValueMetadata {
    ValueMetadata {
        kind,
        string_collation: None,
        decimal_declared_shape: None,
    }
}

#[test]
fn complete_field_type_snapshot_detaches_mutable_backing() {
    let mut source = FieldType::parser(C::Enum)
        .with_raw_flags((1_u64 << 63) | u64::from(F::UNSIGNED | F::BINARY))
        .with_flen(i64::MAX)
        .with_decimal(i64::MIN)
        .with_collation(Collation::Utf8Mb4Bin)
        .with_charset_name("UTF8MB4")
        .with_collation_name("utf8mb4_bin")
        .with_elems([
            GoString::from_bytes(vec![0xff, 0, b'a']),
            GoString::from("text"),
        ])
        .with_array(true);
    source.set_elem_with_binary_literal(0, GoString::from_bytes(vec![0xff, 0, b'a']), true);
    let snapshot = snapshot_field_type(&source);
    assert_eq!(snapshot, source);
    assert_eq!(
        snapshot.raw_flags(),
        (1_u64 << 63) | u64::from(F::UNSIGNED | F::BINARY)
    );
    assert_eq!(snapshot.flen(), i64::MAX);
    assert_eq!(snapshot.decimal(), i64::MIN);
    assert_eq!(snapshot.charset_name(), "UTF8MB4");
    assert_eq!(snapshot.collation_name(), "utf8mb4_bin");
    assert_eq!(snapshot.collation(), Collation::Utf8Mb4Bin);
    assert!(snapshot.is_array());
    assert_eq!(snapshot.code(), C::Json);
    assert_eq!(snapshot.array_element_code(), C::Enum);
    assert_eq!(snapshot.elem(0).as_bytes(), &[0xff, 0, b'a']);
    assert!(snapshot.elem_is_binary_literal(0));
    assert!(!snapshot.elem_is_binary_literal(1));

    // Both element contents and marker bits are mutable through a shallow clone.
    let mut alias = source.clone();
    alias.set_elem_with_binary_literal(1, "changed", true);
    source.set_elem(0, "replaced");
    source.clean_elem_binary_literals();
    source.set_code(C::String);
    assert_eq!(snapshot.elem(0).as_bytes(), &[0xff, 0, b'a']);
    assert_eq!(snapshot.elem(1).as_bytes(), b"text");
    assert!(snapshot.elem_is_binary_literal(0));
    assert!(!snapshot.elem_is_binary_literal(1));
    assert!(snapshot.is_array());
    assert_eq!(snapshot.array_element_code(), C::Enum);

    // Snapshot is independent of admission, including unknown type bytes.
    for code in [C::NewDecimal, C::VectorFloat32, C::Unknown(0xf4)] {
        let source = FieldType::parser(code).with_raw_flags(u64::MAX);
        assert_eq!(snapshot_field_type(&source), source);
    }
}

#[test]
fn checked_projection_preserves_wire_metadata_without_sql_defaults() {
    for (code, wire_code) in [
        (C::Tiny, 1),
        (C::Short, 2),
        (C::Long, 3),
        (C::Float, 4),
        (C::Double, 5),
        (C::Null, 6),
        (C::LongLong, 8),
        (C::Int24, 9),
        (C::Year, 13),
        (C::Varchar, 15),
        (C::TinyBlob, 0xf9),
        (C::MediumBlob, 0xfa),
        (C::LongBlob, 0xfb),
        (C::Blob, 0xfc),
        (C::VarString, 0xfd),
        (C::String, 0xfe),
    ] {
        let sql = FieldType::parser(code)
            .with_raw_flags(u64::from(u32::MAX))
            .with_flen(-1)
            .with_decimal(-1)
            .with_charset_name("UTF8MB4")
            .with_collation_name("utf8mb4_bin");
        for signed_id in [46, -46] {
            let projected = project_field_type(&sql, signed_id).unwrap();
            assert_eq!(projected.get_tp(), wire_code, "{code:?}");
            assert_eq!(
                projected.get_flag(),
                u32::MAX,
                "unknown flags must not disappear"
            );
            assert_eq!(projected.get_flen(), -1);
            assert_eq!(projected.get_decimal(), -1);
            assert_eq!(projected.get_charset(), "UTF8MB4");
            assert_eq!(projected.get_collate(), signed_id);
            assert_eq!(sql.collation_name(), "utf8mb4_bin");
        }
    }
    let extremes = FieldType::parser(C::String)
        .with_flen(i64::from(i32::MIN))
        .with_decimal(i64::from(i32::MAX))
        .with_elems(["", "unchanged\0é"]);
    let projected = project_field_type(&extremes, 63).unwrap();
    assert_eq!(projected.get_flen(), i32::MIN);
    assert_eq!(projected.get_decimal(), i32::MAX);
    assert_eq!(projected.get_charset(), "");
    assert_eq!(
        projected
            .get_elems()
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        ["", "unchanged\0é"]
    );
}

#[test]
fn checked_projection_rejects_narrowing_without_mutating_sql_metadata() {
    let base = FieldType::parser(C::LongLong);
    let cases = [
        (
            base.clone().with_raw_flags(1_u64 << 32),
            "flags",
            1_i128 << 32,
        ),
        (
            base.clone().with_raw_flags(u64::MAX),
            "flags",
            i128::from(u64::MAX),
        ),
        (
            base.clone().with_flen(i64::from(i32::MAX) + 1),
            "flen",
            i128::from(i32::MAX) + 1,
        ),
        (
            base.clone().with_flen(i64::from(i32::MIN) - 1),
            "flen",
            i128::from(i32::MIN) - 1,
        ),
        (
            base.clone().with_decimal(i64::from(i32::MAX) + 1),
            "decimal",
            i128::from(i32::MAX) + 1,
        ),
        (
            base.with_decimal(i64::from(i32::MIN) - 1),
            "decimal",
            i128::from(i32::MIN) - 1,
        ),
    ];
    for (sql, field, value) in cases {
        let snapshot = snapshot_field_type(&sql);
        assert_eq!(
            project_field_type(&sql, 63).unwrap_err(),
            BridgeError::MetadataOutOfRange { field, value }
        );
        assert_eq!(snapshot, sql);
    }
}

#[test]
fn checked_projection_reports_non_slice_types_and_raw_elements() {
    for code in [
        C::Unspecified,
        C::Timestamp,
        C::Date,
        C::Duration,
        C::Datetime,
        C::NewDate,
        C::Bit,
        C::Json,
        C::NewDecimal,
        C::Enum,
        C::Set,
        C::Geometry,
        C::VectorFloat32,
        C::Unknown(0xf4),
        C::Unknown(1),
    ] {
        assert_eq!(
            project_field_type(&FieldType::parser(code), 63).unwrap_err(),
            BridgeError::UnsupportedFieldType(code),
        );
    }
    let array = FieldType::parser(C::LongLong).with_array(true);
    assert_eq!(array.code(), C::Json);
    assert_eq!(
        project_field_type(&array, 63).unwrap_err(),
        BridgeError::ArrayFieldType(C::LongLong)
    );
    let sql = FieldType::parser(C::String).with_elems([GoString::from_bytes(vec![0xff])]);
    assert_eq!(
        project_field_type(&sql, 63).unwrap_err(),
        BridgeError::InvalidElementEncoding(0)
    );
    assert_eq!(snapshot_field_type(&sql).elem(0).as_bytes(), &[0xff]);
}

#[test]
fn typed_null_round_trip_and_empty_bytes_stay_distinct() {
    for expected in [EvalType::Int, EvalType::Real, EvalType::Bytes] {
        let (scalar, metadata) = to_scalar(&Datum::Null, expected).unwrap();
        let expected_null = match expected {
            EvalType::Int => ScalarValue::Int(None),
            EvalType::Real => ScalarValue::Real(None),
            EvalType::Bytes => ScalarValue::Bytes(None),
            _ => unreachable!(),
        };
        assert_eq!(scalar, expected_null);
        assert_eq!(metadata, output_metadata(DatumKind::Null));
        assert_eq!(
            from_scalar(scalar.as_scalar_value_ref(), expected, &metadata).unwrap(),
            Datum::Null
        );
    }
    let (empty, metadata) = to_scalar(&Datum::Bytes(Vec::new()), EvalType::Bytes).unwrap();
    assert_eq!(empty, ScalarValue::Bytes(Some(Vec::new())));
    assert_ne!(empty, ScalarValue::Bytes(None));
    assert_eq!(
        from_scalar(empty.as_scalar_value_ref(), EvalType::Bytes, &metadata).unwrap(),
        Datum::Bytes(Vec::new())
    );
    // Computed typed NULL has the output boundary's kind, not necessarily Null.
    assert_eq!(
        from_scalar(
            ScalarValueRef::Int(None),
            EvalType::Int,
            &output_metadata(DatumKind::UInt)
        )
        .unwrap(),
        Datum::Null
    );
}

#[test]
fn signed_and_unsigned_integer_carriers_round_trip_without_numeric_casts() {
    for integer in [i64::MIN, -5, -1, 0, 1, i64::MAX] {
        let datum = Datum::Int(integer);
        let (scalar, metadata) = to_scalar(&datum, EvalType::Int).unwrap();
        assert_eq!(scalar, ScalarValue::Int(Some(integer)));
        assert_eq!(metadata.kind, DatumKind::Int);
        assert_eq!(
            from_scalar(scalar.as_scalar_value_ref(), EvalType::Int, &metadata).unwrap(),
            datum
        );
    }
    for integer in [0_u64, 1, i64::MAX as u64, (i64::MAX as u64) + 1, u64::MAX] {
        let datum = Datum::UInt(integer);
        let (scalar, metadata) = to_scalar(&datum, EvalType::Int).unwrap();
        assert_eq!(
            scalar,
            ScalarValue::Int(Some(i64::from_ne_bytes(integer.to_ne_bytes())))
        );
        assert_eq!(metadata.kind, DatumKind::UInt);
        assert_eq!(
            from_scalar(scalar.as_scalar_value_ref(), EvalType::Int, &metadata).unwrap(),
            datum
        );
    }
    // The same carrier is interpreted by the selected kernel, not rewritten by
    // the input Datum tag or copied input metadata at a computed-output boundary.
    let (signed, _) = to_scalar(&Datum::Int(-1), EvalType::Int).unwrap();
    let (unsigned, _) = to_scalar(&Datum::UInt(u64::MAX), EvalType::Int).unwrap();
    assert_eq!(signed, unsigned);
    assert_eq!(
        from_scalar(
            signed.as_scalar_value_ref(),
            EvalType::Int,
            &output_metadata(DatumKind::UInt)
        )
        .unwrap(),
        Datum::UInt(u64::MAX)
    );
}

#[test]
fn real_bits_and_float32_host_kind_round_trip_without_rounding() {
    for bits in [
        0_u64,
        1_u64 << 63,
        1,
        1.0000000000000002_f64.to_bits(),
        f64::MAX.to_bits(),
        f64::MIN.to_bits(),
        f64::INFINITY.to_bits(),
        f64::NEG_INFINITY.to_bits(),
    ] {
        for kind in [DatumKind::Real, DatumKind::Float32] {
            let datum = match kind {
                DatumKind::Real => Datum::Real(f64::from_bits(bits)),
                DatumKind::Float32 => Datum::Float32(f64::from_bits(bits)),
                _ => unreachable!(),
            };
            let (scalar, metadata) = to_scalar(&datum, EvalType::Real).unwrap();
            assert_eq!(metadata.kind, kind);
            match scalar.as_scalar_value_ref() {
                ScalarValueRef::Real(Some(real)) => {
                    assert_eq!((*real).into_inner().to_bits(), bits)
                }
                other => panic!("expected a present Real, got {other:?}"),
            }
            let decoded =
                from_scalar(scalar.as_scalar_value_ref(), EvalType::Real, &metadata).unwrap();
            assert_eq!(decoded.kind(), kind);
            match decoded {
                Datum::Real(real) | Datum::Float32(real) => assert_eq!(real.to_bits(), bits),
                other => panic!("unexpected decoded datum {other:?}"),
            }
        }
    }
}

#[test]
fn nan_admission_failure_never_becomes_sql_null() {
    for bits in [
        0x7ff8_0000_0000_0000_u64,
        0x7ff0_0000_0000_0001,
        0xfff8_0000_0000_0123,
    ] {
        let value = f64::from_bits(bits);
        assert!(value.is_nan());
        for datum in [Datum::Real(value), Datum::Float32(value)] {
            assert_eq!(
                to_scalar(&datum, EvalType::Real).unwrap_err(),
                BridgeError::NonRepresentableReal { bits }
            );
        }
    }
}

#[test]
fn raw_bytes_and_explicit_literal_origin_round_trip() {
    for bytes in [vec![], vec![0xff, 0, b' ', 0x80], vec![0, 0, b'1', b'2']] {
        let values = [
            Datum::Bytes(bytes.clone()),
            Datum::String(StringDatum::new(bytes.clone(), Collation::Binary)),
            Datum::String(StringDatum::new(bytes.clone(), Collation::Utf8Mb4Bin)),
            Datum::BinaryLiteral(BinaryLiteral::from(bytes.clone())),
        ];
        for datum in values {
            let (scalar, metadata) = to_scalar(&datum, EvalType::Bytes).unwrap();
            assert_eq!(scalar, ScalarValue::Bytes(Some(bytes.clone())));
            assert_eq!(metadata.kind, datum.kind());
            assert_eq!(
                from_scalar(scalar.as_scalar_value_ref(), EvalType::Bytes, &metadata).unwrap(),
                datum
            );
        }
    }
    // Identical bytes + binary semantics do not imply the same literal origin.
    let text = Datum::String(StringDatum::new(b"12".to_vec(), Collation::Binary));
    let bytes = Datum::Bytes(b"12".to_vec());
    let literal = Datum::BinaryLiteral(BinaryLiteral::from(b"12".as_slice()));
    let (text_value, text_meta) = to_scalar(&text, EvalType::Bytes).unwrap();
    let (bytes_value, bytes_meta) = to_scalar(&bytes, EvalType::Bytes).unwrap();
    let (literal_value, literal_meta) = to_scalar(&literal, EvalType::Bytes).unwrap();
    assert_eq!(text_value, literal_value);
    assert_eq!(bytes_value, literal_value);
    assert_eq!(text_meta.kind, DatumKind::String);
    assert_eq!(text_meta.string_collation, Some(Collation::Binary));
    assert_eq!(bytes_meta.kind, DatumKind::Bytes);
    assert_eq!(literal_meta.kind, DatumKind::BinaryLiteral);
    assert_eq!(literal_meta.string_collation, None);
    assert_ne!(bytes_meta, literal_meta);
    assert_eq!(
        from_scalar(
            literal_value.as_scalar_value_ref(),
            EvalType::Bytes,
            &text_meta
        )
        .unwrap(),
        text
    );
    // Transport does not perform the literal's later numeric cast.
    assert_eq!(
        to_scalar(&literal, EvalType::Int).unwrap_err(),
        BridgeError::EvalTypeMismatch {
            expected: EvalType::Int,
            actual: EvalType::Bytes
        }
    );
}

#[test]
fn mismatched_carriers_and_incomplete_output_metadata_are_diagnostics() {
    assert_eq!(
        to_scalar(&Datum::Int(1), EvalType::Real).unwrap_err(),
        BridgeError::EvalTypeMismatch {
            expected: EvalType::Real,
            actual: EvalType::Int
        }
    );
    let int_meta = output_metadata(DatumKind::Int);
    assert_eq!(
        from_scalar(ScalarValueRef::Bytes(None), EvalType::Int, &int_meta).unwrap_err(),
        BridgeError::EvalTypeMismatch {
            expected: EvalType::Int,
            actual: EvalType::Bytes
        }
    );
    assert_eq!(
        from_scalar(ScalarValueRef::Int(None), EvalType::Bytes, &int_meta).unwrap_err(),
        BridgeError::EvalTypeMismatch {
            expected: EvalType::Bytes,
            actual: EvalType::Int
        }
    );
    assert_eq!(
        from_scalar(
            ScalarValueRef::Int(Some(&1)),
            EvalType::Int,
            &output_metadata(DatumKind::Null)
        )
        .unwrap_err(),
        BridgeError::InvalidValueMetadata {
            kind: DatumKind::Null,
            field: "kind"
        }
    );

    for (metadata, expected_field) in [
        (output_metadata(DatumKind::String), "string_collation"),
        (
            ValueMetadata {
                string_collation: Some(Collation::Binary),
                ..output_metadata(DatumKind::Bytes)
            },
            "string_collation",
        ),
        (
            ValueMetadata {
                decimal_declared_shape: Some((10, 4)),
                ..output_metadata(DatumKind::Bytes)
            },
            "decimal_declared_shape",
        ),
    ] {
        assert_eq!(
            from_scalar(
                ScalarValueRef::Bytes(Some(b"12")),
                EvalType::Bytes,
                &metadata
            )
            .unwrap_err(),
            BridgeError::InvalidValueMetadata {
                kind: metadata.kind,
                field: expected_field
            },
        );
    }
    assert_eq!(
        from_scalar(
            ScalarValueRef::Int(None),
            EvalType::Int,
            &output_metadata(DatumKind::Real)
        )
        .unwrap_err(),
        BridgeError::EvalTypeMismatch {
            expected: EvalType::Int,
            actual: EvalType::Real
        }
    );
}

#[test]
fn non_slice_values_are_not_cast_or_replayed() {
    for datum in [
        Datum::MinNotNull,
        Datum::MaxValue,
        Datum::Raw(b"12".to_vec()),
        Datum::Bit(BinaryLiteral::from(b"12".as_slice())),
        Datum::Decimal(Decimal::from_int(1)),
    ] {
        assert_eq!(
            to_scalar(&datum, EvalType::Bytes).unwrap_err(),
            BridgeError::UnsupportedDatumKind(datum.kind())
        );
    }
    for eval_type in [
        EvalType::Decimal,
        EvalType::DateTime,
        EvalType::Duration,
        EvalType::Json,
        EvalType::Enum,
        EvalType::Set,
        EvalType::VectorFloat32,
    ] {
        assert_eq!(
            to_scalar(&Datum::Null, eval_type).unwrap_err(),
            BridgeError::UnsupportedEvalType(eval_type)
        );
        assert_eq!(
            from_scalar(
                ScalarValueRef::Int(None),
                eval_type,
                &output_metadata(DatumKind::Null)
            )
            .unwrap_err(),
            BridgeError::UnsupportedEvalType(eval_type)
        );
    }
    for kind in [
        DatumKind::Decimal,
        DatumKind::Time,
        DatumKind::Duration,
        DatumKind::Json,
        DatumKind::VectorFloat32,
        DatumKind::Enum,
        DatumKind::Set,
        DatumKind::Bit,
        DatumKind::Raw,
        DatumKind::MinNotNull,
        DatumKind::MaxValue,
    ] {
        assert_eq!(
            from_scalar(
                ScalarValueRef::Bytes(None),
                EvalType::Bytes,
                &output_metadata(kind)
            )
            .unwrap_err(),
            BridgeError::UnsupportedDatumKind(kind)
        );
    }
}
