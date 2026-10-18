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

pub(crate) enum PreparedComparison {
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

pub(crate) enum PreparedNullSafeComparison {
    Presence(Option<i64>),
    Comparison {
        value: PreparedComparison,
        normalize_duration: bool,
    },
}
impl PreparedNullSafeComparison {
    pub(crate) fn compute(self, available: usize) -> LocalResult<Option<i64>> {
        finite_allowance(available)?;
        use tidb_query_expr::impl_op::{int_is_true, is_null, KeepNullOff};
        match self {
            Self::Presence(other) => is_null::<i64>(other.as_ref()).map_err(LocalError::Evaluation),
            Self::Comparison {
                value,
                normalize_duration,
            } => {
                let result = value.compute(available)?;
                if normalize_duration {
                    int_is_true::<KeepNullOff>(result.as_ref()).map_err(LocalError::Evaluation)
                } else {
                    Ok(result)
                }
            }
        }
    }
}
fn finite_allowance(available: usize) -> LocalResult<()> {
    if available == usize::MAX {
        Err(LocalError::ResourceLimit(
            "comparison requires a finite value allowance".into(),
        ))
    } else {
        Ok(())
    }
}
pub(super) fn prepare_null_safe<O: ComparisonOperand>(
    left: Datum,
    right: Datum,
    collation: tidb_datatype::Collation,
    operands: ComparisonOperands<O>,
    ctx: &dyn crate::Columns,
) -> Result<PreparedNullSafeComparison, EvalError> {
    // The original generic null-safe owner rejects sentinels before actual
    // NULL. It does not use the ordinary typed integer comparison shortcut.
    if left.is_range_sentinel() || right.is_range_sentinel() {
        return Err(EvalError::Unsupported("range sentinel expression operand"));
    }
    let other = if left.is_null() {
        Some(&right)
    } else if right.is_null() {
        Some(&left)
    } else {
        None
    };
    if let Some(other) = other {
        return Ok(PreparedNullSafeComparison::Presence(
            (!other.is_null()).then_some(0),
        ));
    }
    let normalize_duration = matches!(
        (&left, &right),
        (Datum::Duration(_), Datum::String(_) | Datum::Bytes(_))
            | (Datum::String(_) | Datum::Bytes(_), Datum::Duration(_))
    ) && ((operands.lhs.is_duration_column()
        && operands.rhs.is_constant())
        || (operands.rhs.is_duration_column() && operands.lhs.is_constant()));
    let value = prepare(ComparisonOp::Eq, left, right, collation, operands, ctx)?;
    Ok(PreparedNullSafeComparison::Comparison {
        value,
        normalize_duration,
    })
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
    pub(crate) fn compute(self, available: usize) -> LocalResult<Option<i64>> {
        finite_allowance(available)?;
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
    struct Context(
        RefCell<Vec<(u16, String)>>,
        Option<crate::ErrorLevel>,
        RefCell<Vec<&'static str>>,
    );
    impl crate::Columns for Context {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }
        fn truncate_level(&self) -> crate::ErrorLevel {
            self.2.borrow_mut().push("truncate_level");
            self.1.unwrap_or(crate::ErrorLevel::Warn)
        }
        fn div_precision_increment(&self) -> u32 {
            self.2.borrow_mut().push("precision");
            4
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

    fn comparison_fixtures() -> Vec<(Datum, FieldType)> {
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
        let mut fixtures = vec![
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
        fixtures.extend([
            (Datum::Real(f64::MAX), FieldType::new(FieldTypeCode::Double)),
            (Datum::Real(-0.0), FieldType::new(FieldTypeCode::Double)),
            (
                Datum::Decimal(Decimal::from_literal("-1.2300")),
                FieldType::new(FieldTypeCode::NewDecimal),
            ),
            (Datum::UInt(0), integer.clone().with_unsigned(true)),
            (
                Datum::new_string("A"),
                FieldType::new(FieldTypeCode::VarString),
            ),
            (
                Datum::new_string("a"),
                FieldType::new(FieldTypeCode::VarString),
            ),
            (
                Datum::BinaryLiteral(tidb_datatype::BinaryLiteral::from(vec![0xff; 8])),
                FieldType::new(FieldTypeCode::VarString),
            ),
            (
                Datum::Enum(tidb_datatype::MysqlEnum::new("b", 2), Collation::GbkBin),
                FieldType::new(FieldTypeCode::Enum),
            ),
            (
                Datum::Set(tidb_datatype::MysqlSet::new("a,b", 3), Collation::GbkBin),
                FieldType::new(FieldTypeCode::Set),
            ),
        ]);
        fixtures
    }
    // Captured from independently active old argument-program callers before
    // retirement (185,274 cases). Reproduce with the N6s baseline patch at
    // TiDB7e76f620/SDK6a36ba0; never regenerate constants from the new callers.
    fn comparison_witness(hash: &mut u64, result: &Result<Datum, EvalError>, ctx: &Context) {
        for byte in format!("{result:?}|{:?}|{:?}\n", ctx.0.borrow(), ctx.2.borrow()).bytes() {
            *hash = (*hash ^ u64::from(byte)).wrapping_mul(0x100000001b3);
        }
    }
    #[test]
    fn comparison_callers_match_original_recipe_witness() {
        let fixtures = comparison_fixtures();
        let mut hash = 0xcbf29ce484222325;
        let mut cases = 0;
        for level in [
            crate::ErrorLevel::Warn,
            crate::ErrorLevel::Error,
            crate::ErrorLevel::Ignore,
        ] {
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
                            for (lc, rc) in
                                [(false, false), (false, true), (true, false), (true, true)]
                            {
                                let original =
                                    Context(Default::default(), Some(level), Default::default());
                                let unified =
                                    Context(Default::default(), Some(level), Default::default());
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
                                comparison_witness(&mut hash, &expected, &original);
                                cases += 1;
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
                                assert_eq!(*original.2.borrow(), *unified.2.borrow());
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
        assert_eq!(cases, 157464);
        assert_eq!(
            hash, 0x4e3678354b488fcd,
            "original comparison result/context witness"
        );
    }
    #[test]
    fn null_safe_comparison_matches_original_recipe_witness() {
        let fixtures = comparison_fixtures();
        let mut hash = 0xcbf29ce484222325;
        let mut cases = 0;
        for level in [
            crate::ErrorLevel::Warn,
            crate::ErrorLevel::Error,
            crate::ErrorLevel::Ignore,
        ] {
            for collation in [
                Collation::Binary,
                Collation::Utf8Mb4GeneralCi,
                Collation::GbkBin,
            ] {
                for (left, left_type) in &fixtures {
                    for (right, right_type) in &fixtures {
                        for (lc, rc) in [(false, false), (false, true), (true, false), (true, true)]
                        {
                            let original =
                                Context(Default::default(), Some(level), Default::default());
                            let unified =
                                Context(Default::default(), Some(level), Default::default());
                            let lhs = expression(left, left_type, lc);
                            let rhs = expression(right, right_type, rc);
                            let expected = eval_null_safe_eq_values_in(
                                left.clone(),
                                right.clone(),
                                collation,
                                Operands::of(&lhs, &rhs),
                                &original,
                            );
                            let actual = prepare_null_safe(
                                left.clone(),
                                right.clone(),
                                collation,
                                ComparisonOperands {
                                    lhs: facts(left_type, lc),
                                    rhs: facts(right_type, rc),
                                },
                                &unified,
                            )
                            .and_then(|value| {
                                value.compute(1024 * 1024).map_err(arithmetic::kernel_error)
                            })
                            .map(|value| value.map_or(Datum::Null, Datum::Int));
                            assert_eq!(actual, expected, "nullsafe {collation:?} {left:?} {right:?} constants={lc}/{rc} level={level:?}");
                            assert_eq!(*original.0.borrow(), *unified.0.borrow());
                            assert_eq!(*original.2.borrow(), *unified.2.borrow());
                            comparison_witness(&mut hash, &expected, &original);
                            cases += 1;
                        }
                    }
                }
            }
        }
        assert_eq!(cases, 26244);
        assert_eq!(
            hash, 0x93bb0d8b5b45573b,
            "original NULL-safe comparison result/context witness"
        );
    }

    #[test]
    fn typed_comparison_entries_match_original_recipe_witness() {
        let mut hash = 0xcbf29ce484222325;
        let mut cases = 0;
        let bits = [0, 1, -1, i64::MIN, i64::MAX, 2, -2];
        let decimals = [
            "0",
            "-0.0000",
            "1",
            "-1.2300",
            "9223372036854775808",
            "-9223372036854775809",
            "1.000000000000000000000000000000000001",
            "99999999999999999999999999999999999999999999999999999999999999999",
        ];
        for (op, binary) in [
            (ComparisonOp::Eq, BinaryOp::Eq),
            (ComparisonOp::Ne, BinaryOp::Ne),
            (ComparisonOp::Lt, BinaryOp::Lt),
            (ComparisonOp::Le, BinaryOp::Le),
            (ComparisonOp::Gt, BinaryOp::Gt),
            (ComparisonOp::Ge, BinaryOp::Ge),
        ] {
            for lu in [false, true] {
                for ru in [false, true] {
                    for left in bits {
                        for right in bits {
                            let a = if lu {
                                Integer::Unsigned(left as u64)
                            } else {
                                Integer::Signed(left)
                            };
                            let b = if ru {
                                Integer::Unsigned(right as u64)
                            } else {
                                Integer::Signed(right)
                            };
                            let original = Context::default();
                            let expected = eval_comparison_int_in(binary, a, b, &original);
                            let actual = prepare_int(op, a, b)
                                .compute(1024 * 1024)
                                .map_err(arithmetic::kernel_error)
                                .map(|value| value.map_or(Datum::Null, Datum::Int));
                            assert_eq!(actual, expected);
                            comparison_witness(&mut hash, &expected, &original);
                            cases += 1;
                        }
                    }
                }
            }
            for left in decimals {
                for right in decimals {
                    let a = Decimal::from_literal(left);
                    let b = Decimal::from_literal(right);
                    let original = Context::default();
                    let expected = eval_comparison_decimal_in(binary, &a, &b, &original);
                    let actual = prepare_decimal(op, &a, &b)
                        .and_then(|value| {
                            value.compute(1024 * 1024).map_err(arithmetic::kernel_error)
                        })
                        .map(|value| value.map_or(Datum::Null, Datum::Int));
                    assert_eq!(actual, expected);
                    comparison_witness(&mut hash, &expected, &original);
                    cases += 1;
                }
            }
            let original = Context::default();
            let expected = eval_comparison_null_in(binary, &original);
            assert_eq!(expected, Ok(Datum::Null));
            comparison_witness(&mut hash, &expected, &original);
            cases += 1;
        }
        assert_eq!(cases, 1566);
        assert_eq!(
            hash, 0x3d32f94b793fd8a9,
            "original typed comparison result/context witness"
        );
    }

    #[test]
    fn direct_comparison_publication_keeps_zero_work_decimal_and_terminal_failures() {
        use crate::tikv::ExpressionRuntimeFailureClass as Class;
        let decimal = Decimal::from_literal("123456789012345678901234567890.0001");
        for (op, equal) in [
            (ComparisonOp::Eq, 1),
            (ComparisonOp::Ne, 0),
            (ComparisonOp::Lt, 0),
            (ComparisonOp::Le, 1),
            (ComparisonOp::Gt, 0),
            (ComparisonOp::Ge, 1),
        ] {
            // Grow Ord borrows the actual words without scratch allocation;
            // a zero work allowance must not invent arithmetic-buffer charges.
            assert_eq!(
                comparison_value_result(
                    prepare_decimal(op, &decimal, &decimal).unwrap().compute(0)
                ),
                Ok(Datum::Int(equal))
            );
            // Deliberately corrupt only the SDK transport to obtain a genuine
            // kernel failure, then move that original error into publication.
            let error = PreparedComparison::Json(op, vec![], vec![])
                .compute(0)
                .unwrap_err();
            assert!(matches!(error, LocalError::Evaluation(_)));
            let Err(EvalError::ExpressionRuntimeFailure(failure)) =
                comparison_value_result(Err(error))
            else {
                panic!("actual SDK failure lost")
            };
            assert_eq!(failure.class(), Class::Evaluation);
            assert_eq!(failure.phase(), None);
            let vector = || VectorFloat32::must_create(vec![1.0, 2.0]);
            let Err(EvalError::ExpressionRuntimeFailure(failure)) = comparison_value_result(
                PreparedComparison::Vector(op, vector(), vector()).compute(0),
            ) else {
                panic!("terminal value budget failure lost")
            };
            assert_eq!(failure.class(), Class::ResourceLimit);
            assert_eq!(failure.phase(), None);
        }
        assert_eq!(
            comparison_value_result(PreparedComparison::Null.compute(0)),
            Ok(Datum::Null)
        );
    }
    #[test]
    fn direct_comparison_entries_reject_operator_before_bridge_or_context() {
        let context = Context::default();
        let invalid = Decimal::from_raw_parts(true, vec![255], u32::MAX, u32::MAX);
        for op in [BinaryOp::Plus, BinaryOp::NullEq, BinaryOp::IntDiv] {
            let expected = Err(EvalError::Unsupported("comparison operator"));
            assert_eq!(
                eval_comparison_int_in(
                    op,
                    Integer::Signed(1),
                    Integer::Unsigned(u64::MAX),
                    &context
                ),
                expected
            );
            assert_eq!(
                eval_comparison_decimal_in(op, &invalid, &invalid, &context),
                expected
            );
            assert_eq!(eval_comparison_null_in(op, &context), expected);
            assert_eq!(
                eval_comparison_values_in(
                    op,
                    Datum::Decimal(invalid.clone()),
                    Datum::Null,
                    Collation::Binary,
                    Operands::LITERALS,
                    &context
                ),
                expected
            );
        }
        assert!(context.0.borrow().is_empty());
        assert!(context.2.borrow().is_empty());
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
    #[test]
    fn null_safe_equality_keeps_duration_false_datetime_null_and_presence_order() {
        let context = Context::default();
        let duration_type = FieldType::new(FieldTypeCode::Duration);
        let time_type = FieldType::new(FieldTypeCode::Datetime);
        let text_type = FieldType::new(FieldTypeCode::VarString);
        let time =
            tidb_datatype::parse_datetime("2026-10-17 12:34:56", &chrono_tz::UTC, true, false)
                .unwrap()
                .time;
        let cases = [
            (
                Datum::Duration(MySqlDuration::from_nanoseconds(3_600_000_000_000, 0).unwrap()),
                duration_type,
                Some(0),
            ),
            (Datum::Time(time), time_type, None),
        ];
        for (left, field, expected) in cases {
            let prepared = prepare_null_safe(
                left,
                Datum::new_string("not a time"),
                Collation::Binary,
                ComparisonOperands {
                    lhs: facts(&field, false),
                    rhs: facts(&text_type, true),
                },
                &context,
            )
            .unwrap();
            assert_eq!(prepared.compute(1024 * 1024).unwrap(), expected);
        }
        assert_eq!(context.0.borrow().len(), 2);
        let integer = FieldType::new(FieldTypeCode::LongLong);
        let pair = || ComparisonOperands {
            lhs: facts(&integer, true),
            rhs: facts(&integer, true),
        };
        assert_eq!(
            prepare_null_safe(
                Datum::Null,
                Datum::Null,
                Collation::Binary,
                pair(),
                &context
            )
            .unwrap()
            .compute(1024)
            .unwrap(),
            Some(1)
        );
        assert_eq!(
            prepare_null_safe(
                Datum::Null,
                Datum::Int(0),
                Collation::Binary,
                pair(),
                &context
            )
            .unwrap()
            .compute(1024)
            .unwrap(),
            Some(0)
        );
        let error = match prepare_null_safe(
            Datum::MinNotNull,
            Datum::Null,
            Collation::Binary,
            pair(),
            &context,
        ) {
            Err(error) => error,
            Ok(_) => panic!("sentinel rejection precedes actual NULL"),
        };
        assert_eq!(
            error,
            EvalError::Unsupported("range sentinel expression operand")
        );
        assert!(matches!(
            prepare_null_safe(
                Datum::Null,
                Datum::Null,
                Collation::Binary,
                pair(),
                &context
            )
            .unwrap()
            .compute(usize::MAX),
            Err(LocalError::ResourceLimit(_))
        ));
    }
}
