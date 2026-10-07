// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

use super::{FieldType, FieldTypeCode, FieldTypeFlags};
use crate::EvalType;
use tidb_query_datatype::codec::native_type_name::{native_merge_field_type, NativeTypeNameCode};

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

const fn merge_type_flags(left: u32, right: u32) -> u32 {
    left & (right & FieldTypeFlags::NOT_NULL | !FieldTypeFlags::NOT_NULL)
        & (right & FieldTypeFlags::UNSIGNED | !FieldTypeFlags::UNSIGNED)
}

/// Exact `AggFieldType`, including mixed-sign integral promotion.
pub fn agg_field_type(types: &[FieldType]) -> FieldType {
    let Some(first) = types.first() else {
        return FieldType::parser(FieldTypeCode::Unspecified)
            .with_flen(0)
            .with_decimal(0);
    };
    let mut current = first.clone();
    let mut mixed_sign = false;
    for next in &types[1..] {
        mixed_sign |= current.is_unsigned() != next.is_unsigned();
        current.set_code(merge_field_type(current.code(), next.code()));
        let merged_flags = merge_type_flags(current.flags(), next.flags());
        current = current.with_flags(merged_flags);
    }
    if mixed_sign && current.code().is_type_integer() {
        let bumps_range = types.iter().any(|field_type| {
            field_type.is_unsigned()
                && (field_type.code() == current.code() || field_type.code() == FieldTypeCode::Bit)
        });
        if bumps_range {
            current.set_code(match current.code() {
                FieldTypeCode::Tiny => FieldTypeCode::Short,
                FieldTypeCode::Short => FieldTypeCode::Int24,
                FieldTypeCode::Int24 => FieldTypeCode::Long,
                FieldTypeCode::Long => FieldTypeCode::LongLong,
                FieldTypeCode::LongLong => FieldTypeCode::NewDecimal,
                other => other,
            });
        }
    }
    if current.is_unsigned() && !mixed_sign {
        current = current.with_added_flags(FieldTypeFlags::UNSIGNED);
    }
    current
}

/// Sets or clears a source type flag.
pub const fn set_type_flag(flags: &mut u32, item: u32, on: bool) {
    if on {
        *flags |= item
    } else {
        *flags &= !item
    }
}

/// Exact `AggregateEvalType` merge and output-flag behavior.
pub fn aggregate_eval_type(types: &[FieldType], flags: &mut u32) -> EvalType {
    let mut aggregate = EvalType::String;
    let mut unsigned = false;
    let mut first = false;
    let mut binary_string = false;
    let mut left = types
        .first()
        .expect("AggregateEvalType requires an argument");
    for field_type in types {
        if field_type.code() == FieldTypeCode::Null {
            continue;
        }
        let right_eval = field_type.eval_type();
        if (field_type.code().is_type_blob()
            || field_type.code().is_type_varchar()
            || field_type.code().is_type_char())
            && field_type.has_flag(FieldTypeFlags::BINARY)
        {
            binary_string = true;
        }
        if !first {
            first = true;
            aggregate = right_eval;
            unsigned = field_type.is_unsigned();
        } else {
            aggregate = merge_eval_type(
                aggregate,
                right_eval,
                left,
                field_type,
                unsigned,
                field_type.is_unsigned(),
            );
            unsigned &= field_type.is_unsigned();
        }
        left = field_type;
    }
    set_type_flag(flags, FieldTypeFlags::UNSIGNED, unsigned);
    set_type_flag(
        flags,
        FieldTypeFlags::BINARY,
        !aggregate.is_string_kind() || binary_string,
    );
    aggregate
}

fn merge_eval_type(
    mut left_eval: EvalType,
    mut right_eval: EvalType,
    left: &FieldType,
    right: &FieldType,
    left_unsigned: bool,
    right_unsigned: bool,
) -> EvalType {
    if left.code() == FieldTypeCode::Unspecified || right.code() == FieldTypeCode::Unspecified {
        if left.code() == right.code() {
            return EvalType::String;
        }
        if left.code() == FieldTypeCode::Unspecified {
            left_eval = right_eval;
        } else {
            right_eval = left_eval;
        }
    }
    if left_eval.is_string_kind() || right_eval.is_string_kind() {
        EvalType::String
    } else if left_eval == EvalType::Real || right_eval == EvalType::Real {
        EvalType::Real
    } else if left_eval == EvalType::Decimal
        || right_eval == EvalType::Decimal
        || left_unsigned != right_unsigned
    {
        EvalType::Decimal
    } else {
        EvalType::Int
    }
}

#[cfg(test)]
mod shared_merge_tests {
    use super::{merge_field_type, FieldTypeCode};

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
}
