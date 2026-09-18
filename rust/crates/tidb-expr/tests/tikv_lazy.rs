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

//! Short-circuit acceptance tests for the local engine adapter.
//!
//! These assert the *semantic* property — a branch a row does not select is
//! never entered — and the *routing* property: an expression with an engine
//! context either runs in the engine, or records a listed reason for staying
//! native. They are written to hold both before and after the engine's lazy
//! kernels replace the current "leaf-only, otherwise native" boundary, so they
//! fail if that boundary is relaxed without real short-circuit support.

#![cfg(feature = "tikv-expr")]

use std::cell::{Cell, RefCell};

use tidb_ast::CiString;
use tidb_chunk::chunk::Chunk;
use tidb_datatype::{Datum, FieldType, FieldTypeCode, SessionTimeZone};
use tidb_expr::constant::Constant;
use tidb_expr::evaluator::EvaluatorSuite;
use tidb_expr::expression::{Expression, ScalarFunction};
use tidb_expr::tikv::{Backend, Context, FallbackReason};
use tidb_expr::Columns;

#[derive(Default)]
struct LazyContext {
    backend: Option<Backend>,
    rows: Cell<usize>,
    fallbacks: RefCell<Vec<FallbackReason>>,
    warnings: RefCell<Vec<u16>>,
}
impl Columns for LazyContext {
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
        self.backend.unwrap_or_default()
    }
    fn record_tikv_expression_rows(&self, rows: usize) {
        self.rows.set(self.rows.get() + rows);
    }
    fn record_tikv_expression_fallback(&self, reason: FallbackReason) {
        self.fallbacks.borrow_mut().push(reason);
    }
    fn append_warning(&self, code: u16, _: &str) {
        self.warnings.borrow_mut().push(code);
    }
    fn time_zone(&self) -> SessionTimeZone {
        SessionTimeZone::Fixed {
            name: "UTC".to_owned(),
            offset_secs: 0,
        }
    }
}

fn int() -> FieldType {
    FieldType::new(FieldTypeCode::LongLong)
}
fn literal(value: Datum, ty: &FieldType) -> Expression {
    Expression::Constant(Constant::new(value, ty.clone()))
}
fn call(name: &str, ty: &FieldType, args: Vec<Expression>) -> Expression {
    Expression::ScalarFunction(ScalarFunction::new(CiString::new(name), ty.clone(), args))
}
/// An expression that provably errors when it is evaluated:
/// `BIGINT_MAX + 1` raises MySQL 1690.
fn erroring_add(ty: &FieldType) -> Expression {
    call(
        "plus",
        ty,
        vec![
            literal(Datum::Int(i64::MAX), ty),
            literal(Datum::Int(1), ty),
        ],
    )
}

struct Outcome {
    values: Vec<Datum>,
    engine_ran: bool,
    fallbacks: Vec<FallbackReason>,
    warnings: Vec<u16>,
}

/// Run one expression with and without an engine context and require the
/// native answer to be the reference.
fn outcome(expression: &Expression, input: &mut Chunk, ty: &FieldType) -> Outcome {
    let native = LazyContext::default();
    let suite = EvaluatorSuite::new(vec![expression.clone()], true);
    let mut output = Chunk::new_with_capacity(std::slice::from_ref(ty), input.num_rows());
    suite
        .run(&native, input, &mut output)
        .unwrap_or_else(|error| panic!("native {expression:?}: {error:?}"));
    assert_eq!(native.rows.get(), 0, "native must not use the engine");
    let expected = (0..output.num_rows())
        .map(|row| output.get_row(row).get_datum(0, ty))
        .collect::<Vec<_>>();

    let context = LazyContext {
        backend: Some(Backend::Copying),
        ..LazyContext::default()
    };
    let suite = EvaluatorSuite::new(vec![expression.clone()], true);
    let mut output = Chunk::new_with_capacity(std::slice::from_ref(ty), input.num_rows());
    suite
        .run(&context, input, &mut output)
        .unwrap_or_else(|error| panic!("engine {expression:?}: {error:?}"));
    let values = (0..output.num_rows())
        .map(|row| output.get_row(row).get_datum(0, ty))
        .collect::<Vec<_>>();
    assert_eq!(values, expected, "value: {expression:?}");

    let fallbacks = context.fallbacks.into_inner();
    let engine_ran = context.rows.get() > 0;
    let warnings = context.warnings.into_inner();
    if engine_ran {
        assert!(
            fallbacks.is_empty(),
            "engine ran but also recorded a decline: {expression:?}"
        );
    } else {
        assert!(
            !fallbacks.is_empty(),
            "stayed native without recording a reason: {expression:?}"
        );
    }
    Outcome {
        engine_ran,
        values,
        fallbacks,
        warnings,
    }
}

fn row_input(ty: &FieldType, values: &[Option<i64>]) -> Chunk {
    let mut input = Chunk::new_with_capacity(std::slice::from_ref(ty), values.len());
    for value in values {
        match value {
            None => input.append_null(0),
            Some(value) => input.append_int64(0, *value),
        }
    }
    input
}

/// Every construct here must return the selected branch's value with no error
/// and no warning, whether the engine is lazy yet or the expression stays
/// native because its lazy shape is not admitted.
#[test]
fn tikv_lazy_control_flow_never_enters_a_dead_branch() {
    let ty = int();
    let mut input = row_input(&ty, &[Some(0), Some(1)]);
    let cases: [(&str, Expression, Vec<Datum>); 6] = [
        (
            "if false",
            call(
                "if",
                &ty,
                vec![
                    literal(Datum::Int(0), &ty),
                    erroring_add(&ty),
                    literal(Datum::Int(7), &ty),
                ],
            ),
            vec![Datum::Int(7), Datum::Int(7)],
        ),
        (
            "if true",
            call(
                "if",
                &ty,
                vec![
                    literal(Datum::Int(1), &ty),
                    literal(Datum::Int(9), &ty),
                    erroring_add(&ty),
                ],
            ),
            vec![Datum::Int(9), Datum::Int(9)],
        ),
        (
            "ifnull",
            call(
                "ifnull",
                &ty,
                vec![literal(Datum::Int(5), &ty), erroring_add(&ty)],
            ),
            vec![Datum::Int(5), Datum::Int(5)],
        ),
        (
            "coalesce",
            call(
                "coalesce",
                &ty,
                vec![literal(Datum::Int(4), &ty), erroring_add(&ty)],
            ),
            vec![Datum::Int(4), Datum::Int(4)],
        ),
        (
            "and",
            call(
                "and",
                &ty,
                vec![literal(Datum::Int(0), &ty), erroring_add(&ty)],
            ),
            vec![Datum::Int(0), Datum::Int(0)],
        ),
        (
            "or",
            call(
                "or",
                &ty,
                vec![literal(Datum::Int(1), &ty), erroring_add(&ty)],
            ),
            vec![Datum::Int(1), Datum::Int(1)],
        ),
    ];
    for (label, expression, expected) in cases {
        let outcome = outcome(&expression, &mut input, &ty);
        assert_eq!(outcome.values, expected, "{label}");
        assert!(
            outcome.warnings.is_empty(),
            "{label}: a dead branch warned: {:?}",
            outcome.warnings
        );
        eprintln!(
            "lazy {label}: engine_ran={} fallbacks={:?}",
            outcome.engine_ran, outcome.fallbacks
        );
    }
}
