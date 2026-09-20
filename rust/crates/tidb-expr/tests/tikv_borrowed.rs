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
use tidb_chunk::codec::Codec;
use tidb_chunk::mutrow::MutRow;
use tidb_datatype::{Datum, FieldType, FieldTypeCode};
use tidb_expr::column::Column;
use tidb_expr::constant::Constant;
use tidb_expr::evaluator::EvaluatorSuite;
use tidb_expr::expression::{Expression, ScalarFunction};
use tidb_expr::tikv::{Backend, Context, TikvExpression};
use tidb_expr::{Columns, EvalError};

struct TestContext {
    backend: Option<Backend>,
    total_rows: Cell<usize>,
    borrowed_rows: Cell<usize>,
}

impl TestContext {
    fn new(backend: Option<Backend>) -> Self {
        Self {
            backend,
            total_rows: Cell::new(0),
            borrowed_rows: Cell::new(0),
        }
    }
}

impl Columns for TestContext {
    fn get(&self, _: &[String]) -> Option<Datum> {
        None
    }
    fn tikv_expression_context(&self) -> Option<Context> {
        self.backend.map(|_| Context {
            flags: 482,
            ..Context::default()
        })
    }
    fn tikv_expression_backend(&self) -> Backend {
        self.backend.unwrap_or(Backend::Copying)
    }
    fn record_tikv_expression_rows(&self, rows: usize) {
        self.total_rows.set(self.total_rows.get() + rows);
    }
    fn record_tikv_borrowed_expression_rows(&self, rows: usize) {
        self.borrowed_rows.set(self.borrowed_rows.get() + rows);
    }
}

fn col(index: i64, ty: &FieldType) -> Expression {
    let mut column = Column::new(index + 1, ty.clone());
    column.index = index;
    Expression::Column(column)
}

fn call(name: &str, ty: &FieldType, args: Vec<Expression>) -> Expression {
    Expression::ScalarFunction(ScalarFunction::new(CiString::new(name), ty.clone(), args))
}

fn integer(value: i64, ty: &FieldType) -> Expression {
    Expression::Constant(Constant::new(Datum::Int(value), ty.clone()))
}

fn values(chunk: &Chunk, ty: &FieldType) -> Vec<Datum> {
    (0..chunk.num_rows())
        .map(|row| chunk.get_row(row).get_datum(0, ty))
        .collect()
}

#[test]
fn explicit_physical_selection_preserves_input_and_skips_other_rows() {
    let ty = FieldType::new(FieldTypeCode::LongLong);
    let expr = call("plus", &ty, vec![col(0, &ty), integer(1, &ty)]);
    let mut input = Chunk::new_with_capacity(std::slice::from_ref(&ty), 3);
    for value in [1, i64::MAX, 3] {
        input.append_int64(0, value);
    }
    input.set_sel(Some(vec![1]));
    for backend in [Backend::Copying, Backend::Borrowed] {
        let context = TestContext::new(Some(backend));
        let program = TikvExpression::compile(&expr, context.tikv_expression_context().unwrap())
            .unwrap()
            .unwrap();
        let selected = [2, 0, 2];
        let mut output = Chunk::new_with_capacity(std::slice::from_ref(&ty), 3);
        if backend == Backend::Borrowed {
            program
                .evaluate_into_selected(&context, &input, Some(&selected), &mut output, 0)
                .unwrap();
        } else {
            for value in program
                .evaluate_selected(&context, &input, Some(&selected))
                .unwrap()
            {
                output.append_datum(0, &value);
            }
        }
        assert_eq!(
            values(&output, &ty),
            vec![Datum::Int(4), Datum::Int(2), Datum::Int(4)]
        );
        assert_eq!(context.total_rows.get(), 3);
        assert_eq!(
            context.borrowed_rows.get(),
            if backend == Backend::Borrowed { 3 } else { 0 }
        );
        assert_eq!(input.sel(), Some(&[1][..]));
        assert_eq!(input.get_row(0).get_int64(0), i64::MAX);

        let error_context = TestContext::new(Some(backend));
        let mut partial = Chunk::new_with_capacity(std::slice::from_ref(&ty), 2);
        if backend == Backend::Borrowed {
            assert!(program
                .evaluate_into_selected(&error_context, &input, Some(&[0, 1]), &mut partial, 0)
                .is_err());
            assert_eq!(partial.num_rows(), 0);
        } else {
            assert!(program
                .evaluate_selected(&error_context, &input, Some(&[0, 1]))
                .is_err());
        }
        assert_eq!(error_context.total_rows.get(), 0);
        assert_eq!(input.sel(), Some(&[1][..]));
        assert!(program
            .evaluate_selected(&error_context, &input, Some(&[3]))
            .is_err());
        assert!(program
            .evaluate_into_selected(&error_context, &input, Some(&[3]), &mut partial, 0)
            .is_err());
        program
            .evaluate_into_selected(&error_context, &input, Some(&[]), &mut partial, 0)
            .unwrap();
        assert_eq!(partial.num_rows(), 0);
    }
}

#[test]
fn explicit_dense_selection_ignores_existing_chunk_selection() {
    let ty = FieldType::new(FieldTypeCode::LongLong);
    let context = TestContext::new(Some(Backend::Borrowed));
    let program = TikvExpression::compile(&col(0, &ty), Context::default())
        .unwrap()
        .unwrap();
    let mut input = Chunk::new_with_capacity(std::slice::from_ref(&ty), 2);
    input.append_int64(0, 7);
    input.append_int64(0, 9);
    input.set_sel(Some(vec![1]));
    assert_eq!(
        program.evaluate_selected(&context, &input, None).unwrap(),
        vec![Datum::Int(7), Datum::Int(9)]
    );
    let mut output = Chunk::new_with_capacity(std::slice::from_ref(&ty), 2);
    program
        .evaluate_into_selected(&context, &input, None, &mut output, 0)
        .unwrap();
    assert_eq!(values(&output, &ty), vec![Datum::Int(7), Datum::Int(9)]);
    assert_eq!(input.sel(), Some(&[1][..]));
}

fn three_way(expression: Expression, input: &mut Chunk, ty: &FieldType) -> Vec<Datum> {
    let selection = input.sel().map(<[usize]>::to_vec);
    let mut expected = None;
    for backend in [None, Some(Backend::Copying), Some(Backend::Borrowed)] {
        let context = TestContext::new(backend);
        // Keep even direct-column expressions on the expression path rather
        // than satisfying them by swapping a column into the output.
        let suite = EvaluatorSuite::new(vec![expression.clone()], true);
        let mut output = Chunk::new_with_capacity(std::slice::from_ref(ty), input.num_rows());
        suite.run(&context, input, &mut output).unwrap();
        assert_eq!(
            context.total_rows.get(),
            if backend.is_some() {
                input.num_rows()
            } else {
                0
            }
        );
        assert_eq!(
            context.borrowed_rows.get(),
            if backend == Some(Backend::Borrowed) {
                input.num_rows()
            } else {
                0
            },
            "borrowed mode must not silently copy: {expression:?}"
        );
        assert_eq!(output.num_rows(), input.num_rows());
        let actual = values(&output, ty);
        if let Some(expected) = &expected {
            assert_eq!(&actual, expected);
        } else {
            expected = Some(actual);
        }
        assert_eq!(input.sel(), selection.as_deref());
    }
    expected.unwrap()
}

#[test]
fn tikv_borrowed_suite_preserves_append_semantics_with_copying_fallback() {
    let ty = FieldType::new(FieldTypeCode::LongLong);
    let expression = call("plus", &ty, vec![col(0, &ty), integer(1, &ty)]);
    for backend in [None, Some(Backend::Copying), Some(Backend::Borrowed)] {
        let context = TestContext::new(backend);
        let suite = EvaluatorSuite::new(vec![expression.clone()], true);
        let mut input = Chunk::new_with_capacity(std::slice::from_ref(&ty), 3);
        input.append_int64(0, 4);
        input.append_null(0);
        input.append_int64(0, -7);
        input.set_sel(Some(vec![2, 1, 0, 2]));
        let mut output = Chunk::new_with_capacity(std::slice::from_ref(&ty), 5);
        output.append_int64(0, 99);
        suite
            .run(&context, &mut input, &mut output)
            .unwrap_or_else(|error| {
                panic!("{backend:?} must append after existing output: {error:?}")
            });
        assert_eq!(
            values(&output, &ty),
            vec![
                Datum::Int(99),
                Datum::Int(-6),
                Datum::Null,
                Datum::Int(5),
                Datum::Int(-6)
            ],
            "{backend:?} must preserve the output prefix and selection order"
        );
        assert_eq!(
            context.total_rows.get(),
            if backend.is_some() { 4 } else { 0 }
        );
        assert_eq!(
            context.borrowed_rows.get(),
            0,
            "nonempty output must take the copying fallback"
        );
    }
}

#[test]
fn tikv_borrowed_all_numeric_signatures_match_copying_and_native() {
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
        input.set_sel(Some(vec![5, 2, 0, 3, 1, 2, 4]));
        for name in [
            "plus", "minus", "mul", "eq", "ne", "lt", "le", "gt", "ge", "nulleq", "abs",
        ] {
            let result = if matches!(name, "plus" | "minus" | "mul" | "abs") {
                &ty
            } else {
                &int
            };
            let args = if name == "abs" {
                vec![col(0, &ty)]
            } else {
                vec![col(0, &ty), col(1, &ty)]
            };
            three_way(call(name, result, args), &mut input, result);
        }
        let nested = call(
            "mul",
            &ty,
            vec![
                call("plus", &ty, vec![col(0, &ty), col(1, &ty)]),
                col(1, &ty),
            ],
        );
        three_way(nested, &mut input, &ty);
    }
}

#[test]
fn tikv_borrowed_empty_and_large_selected_batches_remain_dense() {
    let ty = FieldType::new(FieldTypeCode::LongLong);
    let expression = call("plus", &ty, vec![col(0, &ty), integer(1, &ty)]);
    for rows in [0, 1, 1023, 1024, 1025, 4097] {
        let mut input = Chunk::new_with_capacity(std::slice::from_ref(&ty), rows);
        for row in 0..rows {
            if row % 7 == 0 {
                input.append_null(0);
            } else {
                input.append_int64(0, row as i64);
            }
        }
        three_way(expression.clone(), &mut input, &ty);
        input.set_sel(Some((0..rows).rev().flat_map(|row| [row, row]).collect()));
        three_way(expression.clone(), &mut input, &ty);
        input.set_sel(Some(Vec::new()));
        assert!(three_way(expression.clone(), &mut input, &ty).is_empty());
    }
}

#[test]
fn tikv_borrowed_bytes_length_and_direct_bytes_output_preserve_payloads() {
    let bytes = FieldType::new(FieldTypeCode::VarString);
    let int = FieldType::new(FieldTypeCode::LongLong);
    let mut input = Chunk::new_with_capacity(std::slice::from_ref(&bytes), 6);
    for value in [
        b"a\0b".as_slice(),
        &[0xff, 0xfe],
        b"",
        "中文".as_bytes(),
        &vec![0x81; 131_072],
    ] {
        input.append_bytes(0, value);
    }
    input.append_null(0);
    input.set_sel(Some(vec![4, 2, 5, 0, 4, 1, 3]));
    let original_pointer = input.column(0).get_bytes(0).as_ptr();
    for name in ["length", "octet_length"] {
        let rows = three_way(call(name, &int, vec![col(0, &bytes)]), &mut input, &int);
        assert_eq!(
            rows,
            vec![
                Datum::Int(131_072),
                Datum::Int(0),
                Datum::Null,
                Datum::Int(3),
                Datum::Int(131_072),
                Datum::Int(2),
                Datum::Int(6)
            ]
        );
    }
    // Direct output must preserve arbitrary bytes too, not just their lengths.
    // This writes final output bytes once; it is not a zero-output-copy claim.
    three_way(col(0, &bytes), &mut input, &bytes);
    assert_eq!(input.column(0).get_bytes(0).as_ptr(), original_pointer);
}

#[test]
fn tikv_borrowed_distinct_input_indexes_can_share_one_owner() {
    let ty = FieldType::new(FieldTypeCode::LongLong);
    let mut input = Chunk::new_with_capacity(&[ty.clone(), ty.clone()], 3);
    input.append_int64(0, 4);
    input.append_null(0);
    input.append_int64(0, -7);
    input.make_ref(0, 1);
    assert!(input.columns_share_identity(0, &input, 1));
    input.set_sel(Some(vec![2, 0, 1, 2]));
    let result = three_way(
        call("plus", &ty, vec![col(0, &ty), col(1, &ty)]),
        &mut input,
        &ty,
    );
    assert_eq!(
        result,
        vec![Datum::Int(-14), Datum::Int(8), Datum::Null, Datum::Int(-14)]
    );
}

#[test]
fn tikv_borrowed_frozen_unaligned_codec_input_stays_borrowed() {
    let ty = FieldType::new(FieldTypeCode::LongLong);
    let mut source = Chunk::new_with_capacity(std::slice::from_ref(&ty), 3);
    source.append_int64(0, 4);
    source.append_null(0);
    source.append_int64(0, -8);
    let codec = Codec::new(vec![ty.clone()]);
    // Let the codec signature infer Bytes, avoiding a new direct dependency.
    let encoded = codec.encode(&source).into();
    let mut input = Chunk::new_with_capacity(std::slice::from_ref(&ty), 0);
    assert!(codec
        .try_decode_bytes_to_chunk(&encoded, &mut input)
        .unwrap()
        .is_empty());
    let pointer = input.column(0).get_raw(0).as_ptr();
    assert_eq!(
        pointer,
        encoded[9..].as_ptr(),
        "header plus one validity byte precedes payload"
    );
    assert!(
        !(pointer as usize).is_multiple_of(8),
        "fixture must exercise unaligned decoding"
    );
    assert!(!input.column(0).has_shared_mutable_storage());
    input.set_sel(Some(vec![2, 1, 0, 2]));
    let result = three_way(
        call("plus", &ty, vec![col(0, &ty), integer(1, &ty)]),
        &mut input,
        &ty,
    );
    assert_eq!(
        result,
        vec![Datum::Int(-7), Datum::Null, Datum::Int(5), Datum::Int(-7)]
    );
    assert_eq!(input.column(0).get_raw(0).as_ptr(), pointer);
}

#[test]
fn tikv_borrowed_public_error_after_first_batch_resets_output_without_replay() {
    let ty = FieldType::new(FieldTypeCode::LongLong);
    let expression = call("plus", &ty, vec![col(0, &ty), integer(1, &ty)]);
    let mut program = TikvExpression::compile(&expression, Context::default())
        .unwrap()
        .unwrap();
    let context = TestContext::new(Some(Backend::Borrowed));
    let mut input = Chunk::new_with_capacity(std::slice::from_ref(&ty), 1025);
    for _ in 0..1024 {
        input.append_int64(0, 1);
    }
    input.append_int64(0, i64::MAX);
    let mut output = Chunk::new_with_capacity(std::slice::from_ref(&ty), 1025);
    assert!(matches!(
        program.evaluate_into(&context, &input, &mut output, 0),
        Err(EvalError::ExternalEngine { code: 1690, .. })
    ));
    assert_eq!(
        output.column(0).rows(),
        0,
        "successful earlier internal batches must be discarded"
    );
    assert_eq!(context.total_rows.get(), 0);
    assert_eq!(context.borrowed_rows.get(), 0);
    input.set_sel(Some(vec![0, 1023, 0]));
    program
        .evaluate_into(&context, &input, &mut output, 0)
        .unwrap();
    assert_eq!(values(&output, &ty), vec![Datum::Int(2); 3]);
    assert_eq!(context.total_rows.get(), 3);
    assert_eq!(context.borrowed_rows.get(), 3);
}

#[test]
fn tikv_borrowed_nonfinite_public_input_errors_but_unselected_poison_is_ignored() {
    let ty = FieldType::new(FieldTypeCode::Double);
    let expression = call(
        "mul",
        &ty,
        vec![
            col(0, &ty),
            Expression::Constant(Constant::new(Datum::Real(0.0), ty.clone())),
        ],
    );
    let mut program = TikvExpression::compile(&expression, Context::default())
        .unwrap()
        .unwrap();
    for value in [f64::INFINITY, f64::NEG_INFINITY, f64::NAN] {
        let context = TestContext::new(Some(Backend::Borrowed));
        let mut input = Chunk::new_with_capacity(std::slice::from_ref(&ty), 2);
        input.append_float64(0, value);
        input.append_float64(0, 3.0);
        let mut output = Chunk::new_with_capacity(std::slice::from_ref(&ty), 2);
        assert!(program
            .evaluate_into(&context, &input, &mut output, 0)
            .is_err());
        assert_eq!(output.column(0).rows(), 0);
        assert_eq!(context.borrowed_rows.get(), 0);
        input.set_sel(Some(vec![1, 1]));
        program
            .evaluate_into(&context, &input, &mut output, 0)
            .unwrap();
        assert_eq!(values(&output, &ty), vec![Datum::Real(0.0); 2]);
        assert_eq!(context.borrowed_rows.get(), 2);
    }
    // Dispatch preflight, unlike the explicit public API, leaves nonfinite
    // inputs native before evaluation. A direct column keeps NaN representable.
    let context = TestContext::new(Some(Backend::Borrowed));
    let suite = EvaluatorSuite::new(vec![col(0, &ty)], true);
    let mut input = Chunk::new_with_capacity(std::slice::from_ref(&ty), 1);
    input.append_float64(0, f64::NAN);
    let mut output = Chunk::new_with_capacity(std::slice::from_ref(&ty), 1);
    suite.run(&context, &mut input, &mut output).unwrap();
    assert!(output.get_row(0).get_float64(0).is_nan());
    assert_eq!(context.total_rows.get(), 0);
    assert_eq!(context.borrowed_rows.get(), 0);
}

#[test]
fn tikv_borrowed_rejects_wrong_or_nonempty_output_layout_before_writing() {
    let ty = FieldType::new(FieldTypeCode::LongLong);
    let expression = call("plus", &ty, vec![col(0, &ty), integer(1, &ty)]);
    let mut program = TikvExpression::compile(&expression, Context::default())
        .unwrap()
        .unwrap();
    let context = TestContext::new(Some(Backend::Borrowed));
    let mut input = Chunk::new_with_capacity(std::slice::from_ref(&ty), 1);
    input.append_int64(0, 4);
    for wrong_type in [
        FieldType::new(FieldTypeCode::VarString),
        FieldType::new(FieldTypeCode::Float),
    ] {
        let mut output = Chunk::new_with_capacity(&[wrong_type], 1);
        assert!(program
            .evaluate_into(&context, &input, &mut output, 0)
            .is_err());
        assert_eq!(output.column(0).rows(), 0);
    }
    let mut output = Chunk::new_with_capacity(std::slice::from_ref(&ty), 1);
    output.append_int64(0, 99);
    assert!(program
        .evaluate_into(&context, &input, &mut output, 0)
        .is_err());
    assert_eq!(values(&output, &ty), vec![Datum::Int(99)]);
    assert_eq!(context.total_rows.get(), 0);
}

#[test]
fn tikv_borrowed_shared_mutable_output_falls_back_before_holding_guards() {
    let ty = FieldType::new(FieldTypeCode::LongLong);
    let expression = call("plus", &ty, vec![col(0, &ty), integer(1, &ty)]);
    let mut program = TikvExpression::compile(&expression, Context::default())
        .unwrap()
        .unwrap();
    let context = TestContext::new(Some(Backend::Borrowed));
    let mut input = Chunk::new_with_capacity(std::slice::from_ref(&ty), 2);
    input.append_int64(0, 4);
    input.append_null(0);
    let mut output = Chunk::new_with_capacity(std::slice::from_ref(&ty), 1);
    output.append_int64(0, 123);
    let mut alias = MutRow::from_types(std::slice::from_ref(&ty));
    alias.shallow_copy_partial_row(0, &mut output, 0);
    output.reset();
    assert!(output.column(0).has_shared_mutable_storage());
    program
        .evaluate_into(&context, &input, &mut output, 0)
        .unwrap();
    assert_eq!(values(&output, &ty), vec![Datum::Int(5), Datum::Null]);
    assert_eq!(context.total_rows.get(), 2);
    assert_eq!(
        context.borrowed_rows.get(),
        0,
        "a copying fallback must not count as borrowed"
    );
}
