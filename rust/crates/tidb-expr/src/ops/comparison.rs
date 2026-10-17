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
// See the License for the specific language governing permissions and
// limitations under the License.

//! Comparison preparation owns only values and immutable operand facts.
//! SQL coercion stays separate from the existing shared comparison kernels.

use super::*;
use tidb_query_datatype::codec::collation::native::NativeCollation;
use tidb_query_datatype::codec::mysql::Decimal as SharedDecimal;
use tidb_query_expr::local::{LocalError, LocalResult};
use tidb_query_expr::{impl_compare, ComparisonOp};

pub(super) enum PreparedComparison {
    Null,
    Integer(ComparisonOp, Integer, Integer),
    Decimal(ComparisonOp, SharedDecimal, SharedDecimal),
    Real(ComparisonOp, f64, f64),
    Bytes(ComparisonOp, Vec<u8>, Vec<u8>, NativeCollation),
    Json(ComparisonOp, Vec<u8>, Vec<u8>),
    Vector(
        ComparisonOp,
        tidb_datatype::VectorFloat32,
        tidb_datatype::VectorFloat32,
    ),
    Time(ComparisonOp, u64, u64),
    Duration(ComparisonOp, i64, i64),
}

pub(super) fn prepare_int(op: ComparisonOp, left: Integer, right: Integer) -> PreparedComparison {
    PreparedComparison::Integer(op, left, right)
}
pub(super) fn prepare_real(op: ComparisonOp, left: f64, right: f64) -> PreparedComparison {
    PreparedComparison::Real(op, left, right)
}
pub(super) fn prepare_decimal(
    op: ComparisonOp,
    left: &Decimal,
    right: &Decimal,
) -> Result<PreparedComparison, EvalError> {
    Ok(PreparedComparison::Decimal(
        op,
        crate::tikv::prepare_math_decimal(left)?,
        crate::tikv::prepare_math_decimal(right)?,
    ))
}

impl PreparedComparison {
    /// Execute the same pure value helper as the registered RPN kernel, with
    /// no argument program, callback or expression owner at this boundary.
    pub(super) fn compute(self, available: usize) -> LocalResult<Option<i64>> {
        if available == usize::MAX {
            return Err(LocalError::ResourceLimit(
                "comparison requires a finite value allowance".into(),
            ));
        }
        let result = match self {
            Self::Null => impl_compare::compare_null_native(None),
            Self::Integer(op, left, right) => {
                let (l, r) = (integer_bits(left) as i64, integer_bits(right) as i64);
                match (left, right) {
                    (Integer::Signed(_), Integer::Signed(_)) => {
                        impl_compare::compare_int_ss_native_value(op, &l, &r)
                    }
                    (Integer::Signed(_), Integer::Unsigned(_)) => {
                        impl_compare::compare_int_su_native_value(op, &l, &r)
                    }
                    (Integer::Unsigned(_), Integer::Signed(_)) => {
                        impl_compare::compare_int_us_native_value(op, &l, &r)
                    }
                    (Integer::Unsigned(_), Integer::Unsigned(_)) => {
                        impl_compare::compare_int_uu_native_value(op, &l, &r)
                    }
                }
            }
            Self::Real(op, left, right) => impl_compare::compare_real_native_value(
                op,
                &left.to_bits().to_le_bytes(),
                &right.to_bits().to_le_bytes(),
            ),
            Self::Decimal(op, left, right) => {
                let budget = u64::try_from(available).map_err(|_| {
                    LocalError::ResourceLimit("comparison decimal allowance overflow".into())
                })? as i64;
                impl_compare::compare_decimal_native_value(op, &left, &right, &budget)
            }
            Self::Bytes(op, left, right, collation) => {
                impl_compare::compare_bytes_native_value(op, &left, &right, &collation.tag())
            }
            Self::Json(op, left, right) => {
                impl_compare::compare_json_native_value(op, &left, &right)
            }
            Self::Time(op, left, right) => impl_compare::compare_time_core_native_value(
                op,
                &left.to_le_bytes(),
                &right.to_le_bytes(),
            ),
            Self::Duration(op, left, right) => {
                impl_compare::compare_duration_native_value(op, &left, &right)
            }
            Self::Vector(op, left, right) => {
                // Both original owners remain live until conversion begins.
                // Check the conservative source-plus-copy peak before either
                // native-endian byte representation is allocated.
                let bytes = left
                    .elements_capacity()
                    .checked_add(right.elements_capacity())
                    .and_then(|live| {
                        left.len()
                            .checked_add(right.len())
                            .and_then(|copies| live.checked_add(copies))
                    })
                    .and_then(|cells| cells.checked_mul(std::mem::size_of::<f32>()))
                    .ok_or_else(|| {
                        LocalError::ResourceLimit("comparison vector allowance overflow".into())
                    })?;
                if bytes > available {
                    return Err(LocalError::ResourceLimit(
                        "comparison vector conversion exceeds value allowance".into(),
                    ));
                }
                let left = left.into_wire_raw();
                let right = right.into_wire_raw();
                impl_compare::compare_vector_native_value(op, left.as_ref(), right.as_ref())
            }
        };
        result.map_err(LocalError::Evaluation)
    }
}

pub(super) fn prepare<O: ComparisonOperand>(
    op: tidb_query_expr::ComparisonOp,
    l: Datum,
    r: Datum,
    collation: tidb_datatype::Collation,
    operands: ComparisonOperands<O>,
    ctx: &dyn crate::context::Columns,
) -> Result<PreparedComparison, EvalError> {
    if l.is_range_sentinel() || r.is_range_sentinel() {
        return Err(EvalError::Unsupported("range sentinel expression operand"));
    }
    let l = unsigned_operand_flag(l, operands.lhs.is_unsigned());
    let r = unsigned_operand_flag(r, operands.rhs.is_unsigned());
    if matches!(l, Datum::VectorFloat32(_)) || matches!(r, Datum::VectorFloat32(_)) {
        if l.is_null() || r.is_null() {
            return Ok(PreparedComparison::Null);
        }
        let left = binary_vector_operand(l)?;
        let right = binary_vector_operand(r)?;
        return Ok(PreparedComparison::Vector(op, left, right));
    }
    if matches!(l, Datum::Json(_)) || matches!(r, Datum::Json(_)) {
        if l.is_null() || r.is_null() {
            return Ok(PreparedComparison::Null);
        }
        let parse_string_side = |value: Datum| -> Result<Datum, EvalError> {
            let text: &[u8] = match &value {
                Datum::String(text) => text.bytes(),
                Datum::Bytes(text) => text,
                _ => return Ok(value),
            };
            let text =
                std::str::from_utf8(text).map_err(|_| EvalError::Unsupported("JSON comparison"))?;
            tidb_datatype::BinaryJSON::parse(text)
                .map(Datum::Json)
                .map_err(|_| EvalError::Json(crate::JsonError::InvalidText))
        };
        let l = parse_string_side(l)?;
        let r = parse_string_side(r)?;
        let left = l
            .to_mysql_json()
            .map_err(|_| EvalError::Unsupported("JSON comparison"))?;
        let right = r
            .to_mysql_json()
            .map_err(|_| EvalError::Unsupported("JSON comparison"))?;
        return Ok(PreparedComparison::Json(
            op,
            left.encoded(),
            right.encoded(),
        ));
    }
    if let (Some(left), Some(right)) = (string_cmp_operand(&l, true), string_cmp_operand(&r, true))
    {
        return Ok(PreparedComparison::Bytes(
            op,
            left.to_vec(),
            right.to_vec(),
            collation.native_policy(),
        ));
    }
    if matches!(l, Datum::Raw(_) | Datum::VectorFloat32(_))
        || matches!(r, Datum::Raw(_) | Datum::VectorFloat32(_))
    {
        return Err(EvalError::UnsupportedOperandPair(l.kind(), r.kind()));
    }
    let numeric_partner = |value: &Datum| {
        matches!(
            value,
            Datum::Int(_) | Datum::UInt(_) | Datum::Decimal(_) | Datum::Real(_) | Datum::Float32(_)
        )
    };
    if (matches!(l, Datum::Time(_)) || matches!(r, Datum::Time(_)))
        && !(matches!(l, Datum::Time(_)) && numeric_partner(&r))
        && !(matches!(r, Datum::Time(_)) && numeric_partner(&l))
    {
        if l.is_null() || r.is_null() {
            return Ok(PreparedComparison::Null);
        }
        let (left, right) = match (&l, &r) {
            (Datum::Time(left), Datum::Time(right)) => (*left, *right),
            (Datum::Time(left), other) => {
                let Some(right) = parse_time_comparison_operand(*left, other, ctx)? else {
                    return Ok(PreparedComparison::Null);
                };
                (*left, right)
            }
            (other, Datum::Time(right)) => {
                let Some(left) = parse_time_comparison_operand(*right, other, ctx)? else {
                    return Ok(PreparedComparison::Null);
                };
                (left, *right)
            }
            _ => unreachable!("one comparison operand is a Time"),
        };
        return Ok(PreparedComparison::Time(
            op,
            left.core_time().raw(),
            right.core_time().raw(),
        ));
    }
    let duration_vs_constant = (operands.lhs.is_duration_column() && operands.rhs.is_constant())
        || (operands.rhs.is_duration_column() && operands.lhs.is_constant());
    if duration_vs_constant {
        if l.is_null() || r.is_null() {
            return Ok(PreparedComparison::Null);
        }
        if let (Datum::Duration(left), Datum::Duration(right)) = (&l, &r) {
            return Ok(PreparedComparison::Duration(
                op,
                left.nanoseconds(),
                right.nanoseconds(),
            ));
        }
        let text_side = |value: &Datum| match value {
            Datum::String(value) => Some(String::from_utf8_lossy(value.bytes()).into_owned()),
            Datum::Bytes(value) => Some(String::from_utf8_lossy(value).into_owned()),
            _ => None,
        };
        let pair = match (&l, &r) {
            (Datum::Duration(value), other) => text_side(other).map(|text| (*value, text, false)),
            (other, Datum::Duration(value)) => text_side(other).map(|text| (*value, text, true)),
            _ => None,
        };
        if let Some((duration, text, reversed)) = pair {
            let parsed = match tidb_datatype::parse_duration(text.as_bytes(), 6) {
                Ok(value) => value,
                Err(_) => {
                    ctx.append_warning(1292, &format!("Incorrect time value: '{text}'"));
                    return Ok(PreparedComparison::Null);
                }
            };
            let (left, right) = if reversed {
                (parsed.nanoseconds(), duration.nanoseconds())
            } else {
                (duration.nanoseconds(), parsed.nanoseconds())
            };
            return Ok(PreparedComparison::Duration(op, left, right));
        }
    }
    // Non-column duration/text comparisons keep their formatted-string domain.
    let is_text = |value: &Datum| matches!(value, Datum::String(_) | Datum::Bytes(_));
    let rewritten = match (&l, &r) {
        (Datum::Duration(value), other) if is_text(other) => Some((
            Datum::new_collation_string(value.to_string(), collation),
            r.clone(),
        )),
        (other, Datum::Duration(value)) if is_text(other) => Some((
            l.clone(),
            Datum::new_collation_string(value.to_string(), collation),
        )),
        _ => None,
    };
    if let Some((left, right)) = rewritten {
        return prepare(op, left, right, collation, operands, ctx);
    }
    if matches!(l, Datum::Time(_) | Datum::Duration(_))
        || matches!(r, Datum::Time(_) | Datum::Duration(_))
    {
        return prepare(
            op,
            numeric_context_value(l),
            numeric_context_value(r),
            collation,
            operands,
            ctx,
        );
    }
    if matches!(l, Datum::String(_) | Datum::Bytes(_))
        || matches!(r, Datum::String(_) | Datum::Bytes(_))
    {
        if l.is_null() || r.is_null() {
            return Ok(PreparedComparison::Null);
        }
        let decimal_vs_const_string = |numeric: O, text: O| {
            numeric.eval_type() == Some(tidb_datatype::EvalType::Decimal)
                && !numeric.is_constant()
                && text.is_string_kind()
                && text.is_constant()
        };
        if decimal_vs_const_string(operands.lhs, operands.rhs)
            || decimal_vs_const_string(operands.rhs, operands.lhs)
        {
            let as_decimal = |value: Datum| -> Result<Datum, EvalError> {
                if !matches!(value, Datum::String(_) | Datum::Bytes(_)) {
                    return Ok(value);
                }
                let converted = value
                    .to_decimal()
                    .map_err(|_| EvalError::Unsupported("string operand"))?;
                if converted.event.is_some() {
                    ctx.handle_truncate(&format!(
                        "Truncated incorrect DECIMAL value: '{}'",
                        string_operand_text(&value)
                    ))?;
                }
                Ok(Datum::Decimal(converted.value))
            };
            let left = as_decimal(l)?;
            let right = as_decimal(r)?;
            return prepare_decimal(op, &to_decimal(left), &to_decimal(right));
        }
        return Ok(prepare_real(
            op,
            to_f64_with_mysql_string(&l, ctx)?,
            to_f64_with_mysql_string(&r, ctx)?,
        ));
    }
    if l.is_null() || r.is_null() {
        return Ok(PreparedComparison::Null);
    }
    if matches!(l, Datum::Real(_) | Datum::Float32(_))
        || matches!(r, Datum::Real(_) | Datum::Float32(_))
    {
        return Ok(prepare_real(op, to_f64(l), to_f64(r)));
    }
    if matches!(l, Datum::Decimal(_)) || matches!(r, Datum::Decimal(_)) {
        return prepare_decimal(op, &to_decimal(l), &to_decimal(r));
    }
    let (left, right) = match (integer_of(&l)?, integer_of(&r)?) {
        (Some(left), Some(right)) => (left, right),
        _ => return Err(EvalError::UnsupportedOperandPair(l.kind(), r.kind())),
    };
    Ok(prepare_int(op, left, right))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{column::Column, constant::Constant, expression::Expression};
    use std::cell::RefCell;
    use tidb_datatype::{
        BinaryJSON, Collation, FieldType, FieldTypeCode, MySqlDuration, VectorFloat32,
    };

    #[derive(Default)]
    struct Context(RefCell<Vec<(u16, String)>>);
    impl crate::Columns for Context {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }
        fn append_warning(&self, code: u16, text: &str) {
            self.0.borrow_mut().push((code, text.to_owned()));
        }
    }
    fn expression(value: &Datum, field: &FieldType, constant: bool) -> Expression {
        if constant {
            Expression::Constant(Constant::new(value.clone(), field.clone()))
        } else {
            Expression::Column(Column::new(1, field.clone()))
        }
    }
    fn facts(field: &FieldType, constant: bool) -> OperandFacts {
        OperandFacts {
            eval_type: Some(field.eval_type()),
            unsigned: field.is_unsigned(),
            constant,
            duration_column: !constant && field.code() == FieldTypeCode::Duration,
        }
    }

    #[test]
    fn prepared_values_and_owned_facts_match_original_comparison_domains_and_warnings() {
        let integer = FieldType::new(FieldTypeCode::LongLong);
        let string = FieldType::new(FieldTypeCode::VarString);
        let duration = FieldType::new(FieldTypeCode::Duration).with_decimal(6);
        let time = tidb_datatype::parse_datetime(
            "2026-10-17 12:34:56.123456",
            &chrono_tz::UTC,
            true,
            false,
        )
        .unwrap()
        .time;
        let mut nonfinite_vector = VectorFloat32::must_create(vec![1.0, 0.0]);
        nonfinite_vector
            .elements_mut()
            .copy_from_slice(&[f32::NAN, f32::INFINITY]);
        let fixtures = [
            (Datum::Null, integer.clone()),
            (Datum::Int(-1), integer.clone()),
            (Datum::Int(-1), integer.clone().with_unsigned(true)),
            (Datum::UInt(u64::MAX), integer.clone().with_unsigned(true)),
            (Datum::Int(2), integer.clone()),
            (Datum::Real(f64::NAN), FieldType::new(FieldTypeCode::Double)),
            (Datum::Float32(-0.0), FieldType::new(FieldTypeCode::Float)),
            (
                Datum::Decimal(Decimal::from_literal(
                    "1.000000000000000000000000000000000000001",
                )),
                FieldType::new(FieldTypeCode::NewDecimal),
            ),
            (Datum::new_string("1tail"), string.clone()),
            (Datum::Bytes(b"01:00:00".to_vec()), string.clone()),
            (
                Datum::Duration(MySqlDuration::from_nanoseconds(3_600_000_000_000, 6).unwrap()),
                duration,
            ),
            (
                Datum::Time(time),
                FieldType::new(FieldTypeCode::Datetime).with_decimal(6),
            ),
            (
                Datum::Json(BinaryJSON::parse("null").unwrap()),
                FieldType::new(FieldTypeCode::Json),
            ),
            (
                Datum::Json(BinaryJSON::parse("[1,0]").unwrap()),
                FieldType::new(FieldTypeCode::Json),
            ),
            (
                Datum::VectorFloat32(VectorFloat32::must_create(vec![1.0, -0.0])),
                FieldType::new(FieldTypeCode::VectorFloat32),
            ),
            (
                Datum::VectorFloat32(nonfinite_vector),
                FieldType::new(FieldTypeCode::VectorFloat32),
            ),
            (Datum::MinNotNull, integer.clone()),
            (Datum::Raw(b"1".to_vec()), string),
        ];
        for op in [
            ComparisonOp::Eq,
            ComparisonOp::Ne,
            ComparisonOp::Lt,
            ComparisonOp::Le,
            ComparisonOp::Gt,
            ComparisonOp::Ge,
        ] {
            let binary = match op {
                ComparisonOp::Eq => BinaryOp::Eq,
                ComparisonOp::Ne => BinaryOp::Ne,
                ComparisonOp::Lt => BinaryOp::Lt,
                ComparisonOp::Le => BinaryOp::Le,
                ComparisonOp::Gt => BinaryOp::Gt,
                ComparisonOp::Ge => BinaryOp::Ge,
            };
            for collation in [
                Collation::Binary,
                Collation::Utf8Mb4GeneralCi,
                Collation::GbkBin,
            ] {
                for (left, left_type) in &fixtures {
                    for (right, right_type) in &fixtures {
                        for (lc, rc) in [(false, false), (false, true), (true, false), (true, true)]
                        {
                            let original = Context::default();
                            let unified = Context::default();
                            let lhs = expression(left, left_type, lc);
                            let rhs = expression(right, right_type, rc);
                            let expected = eval_comparison_values_in(
                                binary,
                                left.clone(),
                                right.clone(),
                                collation,
                                Operands::of(&lhs, &rhs),
                                &original,
                            );
                            let prepared = prepare(
                                op,
                                left.clone(),
                                right.clone(),
                                collation,
                                ComparisonOperands {
                                    lhs: facts(left_type, lc),
                                    rhs: facts(right_type, rc),
                                },
                                &unified,
                            );
                            match prepared {
                                Err(error) => assert_eq!(
                                    expected.unwrap_err(),
                                    error,
                                    "{op:?} {left:?} {right:?}"
                                ),
                                Ok(prepared) => {
                                    let actual = prepared
                                        .compute(1024 * 1024)
                                        .unwrap()
                                        .map_or(Datum::Null, Datum::Int);
                                    assert_eq!(
                                        expected.unwrap(),
                                        actual,
                                        "{op:?} {left:?} {right:?}"
                                    );
                                }
                            }
                            assert_eq!(
                                *original.0.borrow(),
                                *unified.0.borrow(),
                                "{op:?} {left:?} {right:?}"
                            );
                        }
                    }
                }
            }
        }
    }
    #[test]
    fn prepared_comparison_enforces_finite_allowance_before_vector_conversion() {
        let vector = || tidb_datatype::VectorFloat32::must_create(vec![1.0, 2.0]);
        assert!(matches!(
            PreparedComparison::Vector(ComparisonOp::Eq, vector(), vector()).compute(0),
            Err(LocalError::ResourceLimit(_))
        ));
        assert_eq!(
            PreparedComparison::Vector(ComparisonOp::Eq, vector(), vector())
                .compute(32)
                .unwrap(),
            Some(1)
        );
        assert!(matches!(
            PreparedComparison::Null.compute(usize::MAX),
            Err(LocalError::ResourceLimit(_))
        ));
    }
}
