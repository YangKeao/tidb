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

use crate::{Columns, Datum, EvalError};
use tidb_query_expr::NativeVectorOperation as O;
#[cfg(test)]
pub(crate) fn dispatch(name: &str, values: &[Datum]) -> Option<Result<Datum, EvalError>> {
    dispatch_in(name, values, &crate::NoColumns)
}
pub(crate) fn dispatch_in(
    name: &str,
    values: &[Datum],
    _ctx: &dyn Columns,
) -> Option<Result<Datum, EvalError>> {
    let op = match name {
        "VEC_DIMS" => O::Dims,
        "VEC_L1_DISTANCE" => O::L1Distance,
        "VEC_L2_DISTANCE" => O::L2Distance,
        "VEC_NEGATIVE_INNER_PRODUCT" => O::NegativeInnerProduct,
        "VEC_COSINE_DISTANCE" => O::CosineDistance,
        "VEC_L2_NORM" => O::L2Norm,
        "VEC_FROM_TEXT" => O::FromText,
        "VEC_AS_TEXT" => O::AsText,
        _ => return None,
    };
    if values.len() != op.arity() {
        return None;
    }
    Some(crate::tikv::vector_value::eval(op, values))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_datatype::{FieldTypeCode, VectorFloat32};

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
