// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

use super::{FieldType, FieldTypeCode, FieldTypeFlags};
use crate::EvalType;
use tidb_query_datatype::codec::{
    native_eval_type::{
        native_agg_field_type, native_aggregate_eval_type, native_set_type_flag,
        NativeAggregateField,
    },
    native_type_name::{native_merge_field_type, NativeTypeNameCode},
};

/// Exact table lookup used by Go `mergeFieldType`.
pub const fn merge_field_type(left: FieldTypeCode, right: FieldTypeCode) -> FieldTypeCode {
    match native_merge_field_type(
        left.as_shared_type_name_code(),
        right.as_shared_type_name_code(),
    ) {
        NativeTypeNameCode::Known(raw) | NativeTypeNameCode::Unknown(raw) => {
            FieldTypeCode::from_mysql_type(raw)
        }
    }
}

fn native_aggregate_field(field_type: &FieldType) -> NativeAggregateField {
    NativeAggregateField::new(
        field_type.code().as_shared_type_name_code(),
        field_type.code().as_shared_string_type(),
        field_type.eval_type(),
        field_type.flags(),
    )
}

/// Exact `AggFieldType`, including mixed-sign integral promotion.
pub fn agg_field_type(types: &[FieldType]) -> FieldType {
    let Some((code, flags)) = native_agg_field_type(
        types.iter().map(native_aggregate_field),
        FieldTypeFlags::NOT_NULL,
        FieldTypeFlags::UNSIGNED,
    ) else {
        return FieldType::parser(FieldTypeCode::Unspecified)
            .with_flen(0)
            .with_decimal(0);
    };
    let mut result = types[0].clone();
    result.set_code(match code {
        NativeTypeNameCode::Known(raw) | NativeTypeNameCode::Unknown(raw) => {
            FieldTypeCode::from_mysql_type(raw)
        }
    });
    result.with_flags(flags)
}

/// Sets or clears a source type flag.
pub const fn set_type_flag(flags: &mut u32, item: u32, on: bool) {
    *flags = native_set_type_flag(*flags, item, on)
}

/// Exact `AggregateEvalType` merge and output-flag behavior.
pub fn aggregate_eval_type(types: &[FieldType], flags: &mut u32) -> EvalType {
    let result = native_aggregate_eval_type(
        types.iter().map(native_aggregate_field),
        FieldTypeFlags::UNSIGNED,
        FieldTypeFlags::BINARY,
    );
    set_type_flag(flags, FieldTypeFlags::UNSIGNED, result.unsigned);
    set_type_flag(flags, FieldTypeFlags::BINARY, result.binary_output);
    result.eval
}

#[cfg(test)]
mod shared_merge_tests {
    use super::{
        agg_field_type, aggregate_eval_type, merge_field_type, set_type_flag, FieldType,
        FieldTypeCode, FieldTypeFlags,
    };
    use crate::EvalType;

    #[test]
    fn shared_field_merge_table_keeps_matrix_and_zero_index_policy() {
        for (left, right, expected) in [
            (
                FieldTypeCode::Tiny,
                FieldTypeCode::Short,
                FieldTypeCode::Short,
            ),
            (
                FieldTypeCode::Float,
                FieldTypeCode::Long,
                FieldTypeCode::Double,
            ),
            (
                FieldTypeCode::Json,
                FieldTypeCode::Blob,
                FieldTypeCode::LongBlob,
            ),
            (
                FieldTypeCode::NewDate,
                FieldTypeCode::Date,
                FieldTypeCode::NewDate,
            ),
            (
                FieldTypeCode::Unknown(17),
                FieldTypeCode::Unknown(34),
                FieldTypeCode::NewDecimal,
            ),
            (
                FieldTypeCode::Unknown(FieldTypeCode::Tiny.mysql_type()),
                FieldTypeCode::Short,
                FieldTypeCode::NewDecimal,
            ),
        ] {
            assert_eq!(
                merge_field_type(left, right),
                expected,
                "{left:?}/{right:?}"
            );
        }
    }

    #[test]
    fn shared_field_aggregate_policy_keeps_flags_bumps_and_unknown_zero_identity() {
        let mut flags = 0;
        set_type_flag(&mut flags, FieldTypeFlags::UNSIGNED, true);
        assert_eq!(flags, FieldTypeFlags::UNSIGNED);
        set_type_flag(&mut flags, FieldTypeFlags::UNSIGNED, false);
        assert_eq!(flags, 0);

        let mixed = agg_field_type(&[
            FieldType::parser(FieldTypeCode::Tiny).with_unsigned(true),
            FieldType::parser(FieldTypeCode::Tiny),
        ]);
        assert_eq!(mixed.code(), FieldTypeCode::Short);

        let mut output_flags = 0;
        let aggregate = aggregate_eval_type(
            &[
                FieldType::parser(FieldTypeCode::Unspecified),
                FieldType::parser(FieldTypeCode::Long),
            ],
            &mut output_flags,
        );
        assert_eq!(aggregate, EvalType::Int);
        assert_ne!(output_flags & FieldTypeFlags::BINARY, 0);

        output_flags = 0;
        let aggregate = aggregate_eval_type(
            &[
                FieldType::parser(FieldTypeCode::Unknown(0)),
                FieldType::parser(FieldTypeCode::Long),
            ],
            &mut output_flags,
        );
        assert_eq!(aggregate, EvalType::String);
        assert_eq!(output_flags & FieldTypeFlags::BINARY, 0);
    }

    #[test]
    fn shared_field_aggregate_controller_keeps_empty_first_metadata_null_and_binary_shapes() {
        let empty = agg_field_type(&[]);
        assert_eq!(
            (empty.code(), empty.flen(), empty.decimal()),
            (FieldTypeCode::Unspecified, 0, 0)
        );
        let first = FieldType::parser(FieldTypeCode::Tiny)
            .with_unsigned(true)
            .with_flen(11)
            .with_decimal(2);
        let mixed = agg_field_type(&[first, FieldType::parser(FieldTypeCode::Tiny)]);
        assert_eq!(
            (mixed.code(), mixed.flen(), mixed.decimal()),
            (FieldTypeCode::Short, 11, 2)
        );

        let mut flags = FieldTypeFlags::UNSIGNED | FieldTypeFlags::BINARY;
        assert_eq!(
            aggregate_eval_type(&[FieldType::parser(FieldTypeCode::Null)], &mut flags),
            EvalType::String
        );
        assert_eq!(
            flags & (FieldTypeFlags::UNSIGNED | FieldTypeFlags::BINARY),
            0
        );

        flags = 0;
        assert_eq!(
            aggregate_eval_type(
                &[FieldType::parser(FieldTypeCode::Varchar)
                    .with_added_flags(FieldTypeFlags::BINARY)],
                &mut flags,
            ),
            EvalType::String
        );
        assert_ne!(flags & FieldTypeFlags::BINARY, 0);
        assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let mut empty_flags = 0;
            aggregate_eval_type(&[], &mut empty_flags);
        }))
        .is_err());
    }
}
