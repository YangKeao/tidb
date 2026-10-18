// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Actual native vector SQL values; conversion and NULL demand stay distinct.
use crate::{Datum, EvalError};
use std::borrow::Cow;
use tidb_datatype::{ConversionFlags, FieldType, FieldTypeCode, VectorFloat32};
use tidb_query_expr::{
    NativeVectorOperation as O, NativeVectorValue as V, NativeVectorValueError as E,
};
fn resource() -> EvalError {
    EvalError::ExpressionRuntimeFailure(super::ExpressionRuntimeFailure::from_local_eval(
        super::LocalError::ResourceLimit("native vector work allowance exceeded".into()),
        None,
    ))
}
fn error(e: E) -> EvalError {
    match e {
        E::Resource => resource(),
        E::Contract => super::native_time_result_contract_error(),
        E::Vector(e) => EvalError::Vector(e.to_string()),
    }
}
fn work(maximum: usize, n: usize) -> Result<(), EvalError> {
    tidb_query_expr::native_vector_work(maximum, n).map_err(error)
}
fn extent(maximum: usize, n: usize) -> Result<(), EvalError> {
    work(
        maximum,
        n.checked_mul(128)
            .and_then(|n| n.checked_add(4096))
            .ok_or_else(resource)?,
    )
}
fn vector(value: &Datum, maximum: usize) -> Result<Option<Cow<'_, VectorFloat32>>, EvalError> {
    work(maximum, 0)?;
    match value {
        Datum::Null => Ok(None),
        Datum::VectorFloat32(v) => Ok(Some(Cow::Borrowed(v))),
        value => {
            if let Datum::String(v) = value {
                extent(maximum, v.bytes().len())?;
            } else if let Datum::Bytes(v) = value {
                extent(maximum, v.len())?;
            }
            match value
                .convert_to(
                    &FieldType::new(FieldTypeCode::VectorFloat32),
                    ConversionFlags::default(),
                )
                .map_err(|e| EvalError::Vector(e.to_string()))?
                .value
            {
                Datum::VectorFloat32(v) => Ok(Some(Cow::Owned(v))),
                _ => unreachable!("original vector conversion yields vector"),
            }
        }
    }
}
fn retained(value: &Cow<'_, VectorFloat32>) -> Result<usize, EvalError> {
    match value {
        Cow::Borrowed(_) => Ok(0),
        Cow::Owned(v) => v.elements_capacity().checked_mul(8).ok_or_else(resource),
    }
}
pub(crate) fn eval(op: O, values: &[Datum]) -> Result<Datum, EvalError> {
    let maximum = super::control_value::default_budget();
    match values {
        [value] => eval_refs(op, &[value], maximum),
        [left, right] => eval_refs(op, &[left, right], maximum),
        _ => Err(EvalError::Unsupported("bad function arity")),
    }
}
pub(crate) fn eval_refs(op: O, values: &[&Datum], maximum: usize) -> Result<Datum, EvalError> {
    work(maximum, 0)?;
    if values.len() != op.arity() {
        return Err(EvalError::Unsupported("bad function arity"));
    }
    if op == O::FromText {
        let input = crate::arg_eval_type::eval_string_ref(values[0])?;
        return tidb_query_expr::native_vector_from_text_value(input, maximum)
            .map(|v| v.map_or(Datum::Null, Datum::VectorFloat32))
            .map_err(error);
    }
    let Some(left) = vector(values[0], maximum)? else {
        return Ok(Datum::Null);
    };
    let remaining = maximum.checked_sub(retained(&left)?).ok_or_else(resource)?;
    let right = if op.arity() == 2 {
        let Some(right) = vector(values[1], remaining)? else {
            return Ok(Datum::Null);
        };
        Some(right)
    } else {
        None
    };
    let remaining = remaining
        .checked_sub(right.as_ref().map(retained).transpose()?.unwrap_or(0))
        .ok_or_else(resource)?;
    tidb_query_expr::native_vector_value(op, Some(left.as_ref()), right.as_deref(), remaining)
        .map(|v| match v {
            V::Null => Datum::Null,
            V::Int(v) => Datum::Int(v),
            V::Real(v) => Datum::Real(v),
            V::Text(v) => Datum::new_string(v),
        })
        .map_err(error)
}
pub(crate) fn eval_source(op: O, values: &[&Datum], maximum: usize) -> Result<Datum, EvalError> {
    work(maximum, 0)?;
    if values.len() != op.arity() {
        return Err(EvalError::Unsupported("bad function arity"));
    }
    if op != O::FromText {
        return eval_refs(op, values, maximum);
    }
    // Original VEC_FROM_TEXT source has one ETString cast before its strict body.
    // Original byte/name/binary-literal views remain borrowed without a clone.
    use tidb_query_expr::NativeArgStringResult as A;
    let value = values[0];
    work(maximum, 0)?;
    if !matches!(
        value,
        Datum::Null
            | Datum::String(_)
            | Datum::Bytes(_)
            | Datum::Enum(..)
            | Datum::Set(..)
            | Datum::BinaryLiteral(_)
    ) {
        let n = tidb_query_expr::native_byte_coercion_work(value.as_shared_json_input()).map_err(
            |e| match e {
                tidb_query_expr::NativeStringCoercionError::Unsupported(m) => {
                    EvalError::Unsupported(m)
                }
                _ => resource(),
            },
        )?;
        work(maximum, n)?;
    }
    let cast = tidb_query_expr::native_cast_arg_as_string(value.as_shared_json_input())
        .map_err(EvalError::Unsupported)?;
    match cast {
        A::Original => eval_refs(op, values, maximum),
        A::Null => Ok(Datum::Null),
        A::Binary(v) | A::String(v) => {
            let remaining = maximum.checked_sub(v.capacity()).ok_or_else(resource)?;
            tidb_query_expr::native_vector_from_text_value(Some(&v), remaining)
                .map(|v| v.map_or(Datum::Null, Datum::VectorFloat32))
                .map_err(error)
        }
    }
}
