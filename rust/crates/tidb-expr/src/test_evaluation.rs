// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Test observations of current shared evaluation and source preparation.
use std::cell::Cell;
thread_local! {
    static SHARED_ENTRIES: Cell<usize> = const { Cell::new(0) };
    static SCALAR_PREPARATIONS: Cell<usize> = const { Cell::new(0) };
}
pub(crate) fn note_shared() {
    SHARED_ENTRIES.with(|n| n.set(n.get() + 1));
}
pub(crate) fn shared_entries() -> usize {
    SHARED_ENTRIES.with(Cell::get)
}
pub(crate) fn note_scalar_preparation() {
    SCALAR_PREPARATIONS.with(|n| n.set(n.get() + 1));
}
pub(crate) fn scalar_preparations() -> usize {
    SCALAR_PREPARATIONS.with(Cell::get)
}

#[test]
fn public_default_uses_one_shared_driver_per_call() {
    use crate::{
        constant::Constant, expression::Expression, scalar_function::ScalarFunction, NoColumns,
    };
    use tidb_chunk::row::Row;
    use tidb_datatype::{Datum, FieldType, FieldTypeCode};
    let integer = FieldType::new(FieldTypeCode::LongLong);
    let expression = Expression::Constant(Constant::new(Datum::Int(7), integer.clone()));
    let before = shared_entries();
    assert_eq!(
        expression.eval(&NoColumns, Row::empty()).unwrap(),
        Datum::Int(7)
    );
    assert_eq!(shared_entries(), before + 1);
    assert_eq!(
        expression
            .eval_with_error_value(&NoColumns, Row::empty())
            .unwrap(),
        Datum::Int(7)
    );
    let function = ScalarFunction::new(tidb_ast::CiString::new("abs"), integer, vec![expression]);
    assert_eq!(
        function.eval(&NoColumns, Row::empty()).unwrap(),
        Datum::Int(7)
    );
    assert_eq!(shared_entries(), before + 3);
}

#[test]
fn public_default_budget_exhaustion_is_terminal_and_later_call_recovers() {
    use crate::{
        constant::Constant, expression::Expression, scalar_function::ScalarFunction, EvalError,
        NoColumns,
    };
    use tidb_chunk::row::Row;
    use tidb_datatype::{Datum, FieldType, FieldTypeCode};
    let integer = FieldType::new(FieldTypeCode::LongLong);
    let arguments = (0..tidb_query_expr::local::CompileLimits::default().max_nodes)
        .map(|_| Expression::Constant(Constant::new(Datum::Int(7), integer.clone())))
        .collect();
    let mut function = ScalarFunction::new(tidb_ast::CiString::new("coalesce"), integer, arguments);
    let before = shared_entries();
    let error = function.eval(&NoColumns, Row::empty()).unwrap_err();
    assert!(
        matches!(error,EvalError::ExpressionRuntimeFailure(error) if error.class()==crate::ExpressionRuntimeFailureClass::ResourceLimit)
    );
    assert_eq!(shared_entries(), before + 1);
    function.args.truncate(1);
    assert_eq!(
        function.eval(&NoColumns, Row::empty()).unwrap(),
        Datum::Int(7)
    );
    assert_eq!(shared_entries(), before + 2);
}
