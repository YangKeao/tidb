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

//! Vector SQL functions from `pkg/expression/builtin_vec.go`.

use tidb_datatype::{ConversionFlags, FieldType, FieldTypeCode, VectorFloat32};

use crate::tikv::{EvaluatedArgs, EvaluatedBytesOp, EvaluatedBytesResult};
use crate::{Columns, Datum, EvalError};

#[cfg(test)]
pub(crate) fn dispatch(name: &str, vals: &[Datum]) -> Option<Result<Datum, EvalError>> {
    dispatch_in(name, vals, &crate::NoColumns)
}

pub(crate) fn dispatch_in(
    name: &str,
    vals: &[Datum],
    ctx: &dyn Columns,
) -> Option<Result<Datum, EvalError>> {
    match (name, vals) {
        ("VEC_DIMS", [value]) => Some(dims(value, ctx)),
        ("VEC_L1_DISTANCE", [left, right]) => Some(distance(
            left,
            right,
            EvaluatedBytesOp::VecL1DistanceNative,
            ctx,
        )),
        ("VEC_L2_DISTANCE", [left, right]) => Some(distance(
            left,
            right,
            EvaluatedBytesOp::VecL2DistanceNative,
            ctx,
        )),
        ("VEC_NEGATIVE_INNER_PRODUCT", [left, right]) => Some(distance(
            left,
            right,
            EvaluatedBytesOp::VecNegativeInnerProductNative,
            ctx,
        )),
        ("VEC_COSINE_DISTANCE", [left, right]) => Some(distance(
            left,
            right,
            EvaluatedBytesOp::VecCosineDistanceNative,
            ctx,
        )),
        ("VEC_L2_NORM", [value]) => Some(l2_norm(value, ctx)),
        ("VEC_FROM_TEXT", [value]) => Some(from_text(value, ctx)),
        ("VEC_AS_TEXT", [value]) => Some(as_text(value, ctx)),
        _ => None,
    }
}

fn vector(value: &Datum) -> Result<Option<VectorFloat32>, EvalError> {
    if matches!(value, Datum::Null) {
        return Ok(None);
    }
    let target = FieldType::new(FieldTypeCode::VectorFloat32);
    match value
        .convert_to(&target, ConversionFlags::default())
        .map_err(|error| EvalError::Vector(error.to_string()))?
        .value
    {
        Datum::VectorFloat32(value) => Ok(Some(value)),
        _ => unreachable!("a VectorFloat32 conversion returns a vector datum"),
    }
}

fn dims(value: &Datum, ctx: &dyn Columns) -> Result<Datum, EvalError> {
    crate::tikv::evaluate_args_in(
        EvaluatedBytesOp::VecDimsNative,
        ctx,
        || Ok(EvaluatedArgs::NativeVector(vector(value)?)),
        EvaluatedBytesResult::into_int_datum,
    )
}

fn distance(
    left: &Datum,
    right: &Datum,
    operation: EvaluatedBytesOp,
    ctx: &dyn Columns,
) -> Result<Datum, EvalError> {
    // Inspect only actual datum NULL tags to select the closed recipe. All
    // conversions stay under its guard, in the original left-to-right order:
    // a bad left still errors before a NULL right, and a NULL left skips right.
    let operation = if matches!(left, Datum::Null) || matches!(right, Datum::Null) {
        EvaluatedBytesOp::VecRealNullNative
    } else {
        operation
    };
    crate::tikv::evaluate_args_in(
        operation,
        ctx,
        || {
            let Some(left) = vector(left)? else {
                return Ok(EvaluatedArgs::NullWitness(None));
            };
            let Some(right) = vector(right)? else {
                return Ok(EvaluatedArgs::NullWitness(None));
            };
            Ok(EvaluatedArgs::NativeVector2(Some(left), Some(right)))
        },
        pack_real,
    )
}

fn l2_norm(value: &Datum, ctx: &dyn Columns) -> Result<Datum, EvalError> {
    crate::tikv::evaluate_args_in(
        EvaluatedBytesOp::VecL2NormNative,
        ctx,
        || Ok(EvaluatedArgs::NativeVector(vector(value)?)),
        pack_real,
    )
}

fn pack_real(computed: EvaluatedBytesResult) -> Result<Datum, EvalError> {
    // The worker has already applied the original NaN-to-NULL policy. Preserve
    // every remaining result bit, including infinities, without host filtering.
    Ok(computed
        .into_ieee754_bits()?
        .map_or(Datum::Null, |bits| Datum::Real(f64::from_bits(bits))))
}

fn from_text(value: &Datum, ctx: &dyn Columns) -> Result<Datum, EvalError> {
    crate::tikv::evaluate_args_in(
        EvaluatedBytesOp::VecFromTextNative,
        ctx,
        || {
            Ok(EvaluatedArgs::Bytes(crate::arg_eval_type::eval_string(
                value,
            )?))
        },
        EvaluatedBytesResult::into_native_vector_datum,
    )
}

fn as_text(value: &Datum, ctx: &dyn Columns) -> Result<Datum, EvalError> {
    crate::tikv::evaluate_args_in(
        EvaluatedBytesOp::VecAsTextNative,
        ctx,
        || Ok(EvaluatedArgs::NativeVector(vector(value)?)),
        |computed| {
            Ok(computed
                .into_bytes()?
                .map_or(Datum::Null, Datum::new_string))
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vector(values: Vec<f32>) -> Datum {
        Datum::new_vector_float32(VectorFloat32::must_create(values))
    }

    #[test]
    fn vector_functions_follow_the_scalar_go_signatures() {
        assert_eq!(
            dispatch("VEC_DIMS", &[vector(vec![1.0, 2.0])]),
            Some(Ok(Datum::Int(2)))
        );
        assert_eq!(
            dispatch(
                "VEC_L1_DISTANCE",
                &[vector(vec![1.0, 2.0]), vector(vec![3.0, 5.0])]
            ),
            Some(Ok(Datum::Real(5.0)))
        );
        assert_eq!(
            dispatch(
                "VEC_L2_DISTANCE",
                &[vector(vec![0.0, 0.0]), vector(vec![3.0, 4.0])]
            ),
            Some(Ok(Datum::Real(5.0)))
        );
        assert_eq!(
            dispatch(
                "VEC_NEGATIVE_INNER_PRODUCT",
                &[vector(vec![1.0, 2.0]), vector(vec![3.0, 4.0])]
            ),
            Some(Ok(Datum::Real(-11.0)))
        );
        assert_eq!(
            dispatch("VEC_L2_NORM", &[vector(vec![3.0, 4.0])]),
            Some(Ok(Datum::Real(5.0)))
        );
        assert_eq!(
            dispatch("VEC_AS_TEXT", &[vector(vec![1.0, 2.0])]),
            Some(Ok(Datum::new_string("[1,2]")))
        );
        assert_eq!(
            dispatch("VEC_FROM_TEXT", &[Datum::new_string("[1,2]")]),
            Some(Ok(vector(vec![1.0, 2.0])))
        );
    }

    #[test]
    fn vector_functions_propagate_null_and_source_domain_errors() {
        assert_eq!(dispatch("VEC_DIMS", &[Datum::Null]), Some(Ok(Datum::Null)));
        assert_eq!(
            dispatch(
                "VEC_COSINE_DISTANCE",
                &[vector(vec![0.0]), vector(vec![1.0])]
            ),
            Some(Ok(Datum::Null))
        );
        assert!(matches!(
            dispatch("VEC_L2_DISTANCE", &[vector(vec![1.0]), vector(vec![1.0, 2.0])]),
            Some(Err(EvalError::Vector(message))) if message == "vectors have different dimensions: 1 and 2"
        ));
    }

    #[test]
    fn vector_functions_are_reachable_from_the_sql_expression_path() {
        let expression = tidb_ast::Expr::Func {
            name: "vec_l2_distance".to_owned(),
            args: vec![
                tidb_ast::Expr::Func {
                    name: "vec_from_text".to_owned(),
                    args: vec![tidb_ast::Expr::String("[0,0]".to_owned())],
                    origin_position: 0,
                },
                tidb_ast::Expr::Func {
                    name: "vec_from_text".to_owned(),
                    args: vec![tidb_ast::Expr::String("[3,4]".to_owned())],
                    origin_position: 0,
                },
            ],
            origin_position: 0,
        };
        assert_eq!(
            crate::eval_in(&expression, &crate::NoColumns),
            Ok(Datum::Real(5.0))
        );
        let rewritten = crate::rewriter::rewrite_expr(&expression).expect("vector rewrite");
        assert_eq!(
            rewritten.static_type().expect("vector result type").code(),
            FieldTypeCode::Double
        );
    }
}
