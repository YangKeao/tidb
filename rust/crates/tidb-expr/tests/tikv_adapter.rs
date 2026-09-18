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

#![cfg(feature = "tikv-expr")]

use std::cell::Cell;
use tidb_ast::CiString;
use tidb_chunk::chunk::Chunk;
use tidb_datatype::{Datum, FieldType, FieldTypeCode};
use tidb_expr::column::Column;
use tidb_expr::constant::{Constant, ParamMarker};
use tidb_expr::evaluator::EvaluatorSuite;
use tidb_expr::expression::{Expression, ScalarFunction};
use tidb_expr::tikv::{Context, TikvExpression};
use tidb_expr::Columns;

struct TestContext {
    enabled: bool,
    rows: Cell<usize>,
}
impl Columns for TestContext {
    fn get(&self, _: &[String]) -> Option<Datum> {
        None
    }
    fn tikv_expression_context(&self) -> Option<Context> {
        self.enabled.then(|| Context {
            flags: 482,
            ..Context::default()
        })
    }
    fn record_tikv_expression_rows(&self, rows: usize) {
        self.rows.set(self.rows.get() + rows);
    }
}
fn input_column(index: i64, ty: &FieldType) -> Expression {
    let mut column = Column::new(index + 1, ty.clone());
    column.index = index;
    Expression::Column(column)
}
fn call(name: &str, ty: &FieldType, args: Vec<Expression>) -> Expression {
    Expression::ScalarFunction(ScalarFunction::new(CiString::new(name), ty.clone(), args))
}
fn constant(value: i64, ty: &FieldType) -> Expression {
    Expression::Constant(Constant::new(Datum::Int(value), ty.clone()))
}
fn compare(expression: Expression, input: &mut Chunk, output_type: &FieldType) {
    let mut results = Vec::new();
    for enabled in [false, true] {
        let context = TestContext {
            enabled,
            rows: Cell::new(0),
        };
        let suite = EvaluatorSuite::new(vec![expression.clone()], true);
        let mut output =
            Chunk::new_with_capacity(std::slice::from_ref(output_type), input.num_rows());
        suite.run(&context, input, &mut output).unwrap();
        assert_eq!(
            context.rows.get(),
            if enabled { input.num_rows() } else { 0 },
            "must actually enter TiKV for {expression:?}"
        );
        results.push(
            (0..output.num_rows())
                .map(|row| output.get_row(row).get_datum(0, output_type))
                .collect::<Vec<_>>(),
        );
    }
    assert_eq!(results[0], results[1]);
}

#[test]
fn tikv_adapter_all_admitted_numeric_signatures_match_native() {
    let int = FieldType::new(FieldTypeCode::LongLong);
    for ty in [int.clone(), FieldType::new(FieldTypeCode::Double)] {
        let mut input = Chunk::new_with_capacity(&[ty.clone(), ty.clone()], 6);
        for (left, right) in [
            (Some(-4), Some(3)),
            (Some(2), Some(2)),
            (Some(9), Some(-3)),
            (None, Some(1)),
            (Some(5), None),
            (None, None),
        ] {
            for (index, value) in [left, right].into_iter().enumerate() {
                match value {
                    None => input.append_null(index),
                    Some(value) if ty.code() == FieldTypeCode::Double => {
                        input.append_float64(index, value as f64 * 0.25)
                    }
                    Some(value) => input.append_int64(index, value),
                }
            }
        }
        for name in [
            "plus", "minus", "mul", "eq", "ne", "lt", "le", "gt", "ge", "nulleq", "abs",
        ] {
            let result = if matches!(name, "plus" | "minus" | "mul" | "abs") {
                ty.clone()
            } else {
                int.clone()
            };
            let args = if name == "abs" {
                vec![input_column(0, &ty)]
            } else {
                vec![input_column(0, &ty), input_column(1, &ty)]
            };
            compare(call(name, &result, args), &mut input, &result);
        }
    }
}

#[test]
fn tikv_adapter_bytes_are_not_utf8_or_c_strings() {
    let bytes = FieldType::new(FieldTypeCode::VarString);
    let int = FieldType::new(FieldTypeCode::LongLong);
    let mut input = Chunk::new_with_capacity(std::slice::from_ref(&bytes), 5);
    input.append_bytes(0, b"a\0b");
    input.append_bytes(0, &[0xff, 0xfe]);
    input.append_bytes(0, "中文".as_bytes());
    input.append_bytes(0, b"");
    input.append_null(0);
    compare(
        call("length", &int, vec![input_column(0, &bytes)]),
        &mut input,
        &int,
    );
    compare(
        call("octet_length", &int, vec![input_column(0, &bytes)]),
        &mut input,
        &int,
    );
}

#[test]
fn tikv_adapter_compacts_only_referenced_columns_and_splits_large_batches() {
    let int = FieldType::new(FieldTypeCode::LongLong);
    let unsupported = FieldType::new(FieldTypeCode::Datetime);
    let expression = call(
        "mul",
        &int,
        vec![
            call("plus", &int, vec![input_column(1, &int), constant(3, &int)]),
            constant(2, &int),
        ],
    );
    for rows in [0, 1, 1024, 1025, 4097] {
        let mut input = Chunk::new_with_capacity(&[unsupported.clone(), int.clone()], rows);
        for row in 0..rows {
            input.append_null(0);
            if row % 9 == 0 {
                input.append_null(1);
            } else {
                input.append_int64(1, row as i64);
            }
        }
        compare(expression.clone(), &mut input, &int);
        input.set_sel(Some((0..rows).rev().flat_map(|row| [row, row]).collect()));
        compare(expression.clone(), &mut input, &int);
    }
}

#[test]
fn tikv_adapter_refuses_unverified_types_and_execution_time_values() {
    let int = FieldType::new(FieldTypeCode::LongLong);
    let decimal = FieldType::new(FieldTypeCode::NewDecimal).with_decimal(4);
    let unsigned = int.clone().with_flags(1 << 5);
    let mut parameter = Constant::new(Datum::Int(1), int.clone());
    parameter.param_marker = Some(ParamMarker { order: 0 });
    let nonfinite = Expression::Constant(Constant::new(Datum::Real(f64::INFINITY), int.clone()));
    let refused = [
        // A marker's value is only known at execution time.
        Expression::Constant(parameter),
        // Nonfinite REAL literals would panic TiKV arithmetic.
        nonfinite,
        // Session clock / effects / RNG names have no local session binding.
        call("sleep", &int, vec![constant(1, &int)]),
        call("get_lock", &int, vec![constant(1, &int), constant(1, &int)]),
        call("rand", &int, vec![]),
        // TiKV's UUID parsers accept malformed strings; Go raises 1411.
        call(
            "uuid_version",
            &int,
            vec![Expression::Constant(Constant::new(
                Datum::Bytes(b"abc".to_vec()),
                FieldType::new(FieldTypeCode::VarString),
            ))],
        ),
        // A lazy node whose possibly-skipped children are not leaves: TiKV RPN
        // evaluates every child eagerly, so a dead division must stay native.
        call(
            "case",
            &int,
            vec![
                call("eq", &int, vec![input_column(0, &int), constant(0, &int)]),
                constant(1, &int),
                call(
                    "intdiv",
                    &int,
                    vec![input_column(0, &int), constant(0, &int)],
                ),
            ],
        ),
    ];
    for expression in refused {
        assert!(
            TikvExpression::compile(&expression, Context::default())
                .unwrap()
                .is_none(),
            "{expression:?}"
        );
    }
    // The broadened surface admits these shapes; the coverage fixtures pin
    // their values, and this list guards against silent re-narrowing.
    let admitted = [
        input_column(0, &decimal),
        input_column(0, &unsigned),
        call(
            "if",
            &int,
            vec![constant(1, &int), constant(2, &int), constant(3, &int)],
        ),
        call(
            "plus",
            &decimal,
            vec![input_column(0, &decimal), input_column(1, &decimal)],
        ),
    ];
    for expression in admitted {
        assert!(
            TikvExpression::compile(&expression, Context::default())
                .unwrap()
                .is_some(),
            "{expression:?}"
        );
    }
}

#[test]
fn tikv_adapter_nonfinite_input_stays_native_before_evaluation() {
    let ty = FieldType::new(FieldTypeCode::Double);
    let suite = EvaluatorSuite::new(vec![input_column(0, &ty)], true);
    let mut input = Chunk::new_with_capacity(std::slice::from_ref(&ty), 1);
    input.append_float64(0, f64::NAN);
    let mut output = Chunk::new_with_capacity(std::slice::from_ref(&ty), 1);
    let context = TestContext {
        enabled: true,
        rows: Cell::new(0),
    };
    suite.run(&context, &mut input, &mut output).unwrap();
    assert!(output.get_row(0).get_float64(0).is_nan());
    assert_eq!(context.rows.get(), 0);
}

#[test]
fn tikv_adapter_public_evaluate_rejects_nonfinite_without_panicking() {
    let ty = FieldType::new(FieldTypeCode::Double);
    let expression = call(
        "mul",
        &ty,
        vec![
            input_column(0, &ty),
            Expression::Constant(Constant::new(Datum::Real(0.0), ty.clone())),
        ],
    );
    let mut program = TikvExpression::compile(&expression, Context::default())
        .unwrap()
        .unwrap();
    let context = TestContext {
        enabled: true,
        rows: Cell::new(0),
    };
    for value in [f64::INFINITY, f64::NEG_INFINITY, f64::NAN] {
        let mut input = Chunk::new_with_capacity(std::slice::from_ref(&ty), 1);
        input.append_float64(0, value);
        assert!(program.evaluate(&context, &input).is_err());
    }
    assert_eq!(context.rows.get(), 0);
}

#[test]
fn tikv_adapter_nested_null_arithmetic_keeps_eager_error_behavior() {
    let ty = FieldType::new(FieldTypeCode::LongLong);
    let mut input = Chunk::new_with_capacity(&[ty.clone(), ty.clone()], 1);
    input.append_null(0);
    input.append_int64(1, i64::MAX);
    let overflow = call("plus", &ty, vec![input_column(1, &ty), constant(1, &ty)]);
    for name in ["plus", "mul"] {
        let expression = call(name, &ty, vec![input_column(0, &ty), overflow.clone()]);
        for enabled in [false, true] {
            let context = TestContext {
                enabled,
                rows: Cell::new(0),
            };
            let suite = EvaluatorSuite::new(vec![expression.clone()], true);
            let mut output = Chunk::new_with_capacity(std::slice::from_ref(&ty), 1);
            assert!(suite.run(&context, &mut input, &mut output).is_err());
        }
    }
    // Moving the addition outside the NULL-producing subtree must not overflow.
    let expression = call(
        "plus",
        &ty,
        vec![
            call(
                "plus",
                &ty,
                vec![input_column(0, &ty), input_column(1, &ty)],
            ),
            constant(1, &ty),
        ],
    );
    compare(expression, &mut input, &ty);
}
