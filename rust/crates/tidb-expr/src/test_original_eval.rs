// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Explicit original recursion for differential tests; default execution stays public.
use std::cell::Cell;
thread_local! {
    static ORIGINAL: Cell<bool> = const { Cell::new(false) };
    static ORIGINAL_ENTRIES: Cell<usize> = const { Cell::new(0) };
    static SHARED_ENTRIES: Cell<usize> = const { Cell::new(0) };
}
pub(crate) fn requested() -> bool {
    ORIGINAL.with(Cell::get)
}
pub(crate) fn note_original() {
    ORIGINAL_ENTRIES.with(|n| n.set(n.get() + 1));
}
pub(crate) fn note_shared() {
    SHARED_ENTRIES.with(|n| n.set(n.get() + 1));
}
pub(crate) fn entries() -> (usize, usize) {
    (
        ORIGINAL_ENTRIES.with(Cell::get),
        SHARED_ENTRIES.with(Cell::get),
    )
}
pub(crate) fn with_original<T>(body: impl FnOnce() -> T) -> T {
    struct Restore(bool);
    impl Drop for Restore {
        fn drop(&mut self) {
            ORIGINAL.with(|flag| flag.set(self.0));
        }
    }
    let _restore = Restore(ORIGINAL.with(|flag| flag.replace(true)));
    body()
}
pub(crate) fn compare_records(mut capture: impl FnMut() -> Vec<String>) -> Vec<String> {
    assert!(!requested());
    let before = entries();
    let original = with_original(&mut capture);
    let after = entries();
    assert!(
        after.0 > before.0,
        "original corpus did not use the independent old body"
    );
    assert_eq!(
        after.1, before.1,
        "original corpus reached the shared public route"
    );
    let actual = capture();
    let current = entries();
    assert_eq!(
        current.0, after.0,
        "default public corpus reached original recursion"
    );
    assert_eq!(
        current.1 - after.1,
        actual.len(),
        "public calls did not each enter one shared driver"
    );
    assert_eq!(original.len(), actual.len());
    for (index, (old, new)) in original.iter().zip(&actual).enumerate() {
        assert_eq!(old, new, "explicit original/public record {index}");
    }
    actual
}
#[test]
fn explicit_original_scope_restores_after_nested_unwind() {
    assert!(!requested());
    with_original(|| {
        assert!(requested());
        let failed =
            std::panic::catch_unwind(|| with_original(|| panic!("original scope witness")));
        assert!(failed.is_err());
        assert!(requested());
    });
    assert!(!requested());
    let failed = std::panic::catch_unwind(|| with_original(|| panic!("outer original witness")));
    assert!(failed.is_err());
    assert!(!requested());
}

#[test]
fn public_default_uses_shared_driver_without_original_scope() {
    use crate::constant::Constant;
    use crate::context::NoColumns;
    use crate::expression::Expression;
    use crate::scalar_function::ScalarFunction;
    use tidb_chunk::row::Row;
    use tidb_datatype::{Datum, FieldType, FieldTypeCode};
    assert!(!requested());
    let integer = FieldType::new(FieldTypeCode::LongLong);
    let expression = Expression::Constant(Constant::new(Datum::Int(7), integer.clone()));
    let before = entries();
    assert_eq!(
        expression.eval(&NoColumns, Row::empty()).unwrap(),
        Datum::Int(7)
    );
    let first = entries();
    assert_eq!(first.0, before.0, "public default used original recursion");
    assert_eq!(
        first.1,
        before.1 + 1,
        "public default did not enter the shared driver"
    );
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
    let after = entries();
    assert_eq!(after.0, before.0);
    assert_eq!(after.1, before.1 + 3);
    assert_eq!(
        with_original(|| function.eval(&NoColumns, Row::empty())).unwrap(),
        Datum::Int(7)
    );
    assert!(entries().0 > after.0);
    assert_eq!(entries().1, after.1);
    assert!(!requested());
}

#[test]
fn public_default_budget_exhaustion_is_terminal_without_original_fallback() {
    use crate::constant::Constant;
    use crate::context::NoColumns;
    use crate::expression::Expression;
    use crate::scalar_function::ScalarFunction;
    use crate::EvalError;
    use tidb_chunk::row::Row;
    use tidb_datatype::{Datum, FieldType, FieldTypeCode};
    let integer = FieldType::new(FieldTypeCode::LongLong);
    // The native driver bounds the complete source graph even when a lazy
    // original coalesce can return its first argument without later demand.
    let count = tidb_query_expr::local::CompileLimits::default().max_nodes;
    let arguments = (0..count)
        .map(|_| Expression::Constant(Constant::new(Datum::Int(7), integer.clone())))
        .collect();
    let mut function = ScalarFunction::new(tidb_ast::CiString::new("coalesce"), integer, arguments);
    let before = entries();
    let error = function.eval(&NoColumns, Row::empty()).unwrap_err();
    assert!(
        matches!(error,EvalError::ExpressionRuntimeFailure(error) if error.class()==crate::ExpressionRuntimeFailureClass::ResourceLimit)
    );
    let after = entries();
    assert_eq!(
        after.0, before.0,
        "resource exhaustion ran the original interpreter"
    );
    assert_eq!(after.1, before.1 + 1);
    assert_eq!(
        with_original(|| function.eval(&NoColumns, Row::empty())).unwrap(),
        Datum::Int(7)
    );
    assert!(entries().0 > after.0);
    assert_eq!(entries().1, after.1);
    function.args.truncate(1);
    assert_eq!(
        function.eval(&NoColumns, Row::empty()).unwrap(),
        Datum::Int(7)
    );
    assert_eq!(entries().1, after.1 + 1);
}
