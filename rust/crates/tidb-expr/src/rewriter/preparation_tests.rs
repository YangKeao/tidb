// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::cell::{Cell, RefCell};

use super::fold_mode::FoldModeResolver;
use super::preparation::{PreparationPurpose, StructuralExpression, StructuralLimits};
use super::{rewrite_expr_resolved, rewrite_expr_structural, ColumnResolver};
use crate::column::Column;
use crate::constant::{Constant, ParamMarker};
use crate::constant_fold::{
    fold_warnings_len, record_fold_warning, take_fold_warnings, ConstantFoldMode,
};
use crate::expression::{Expression, ScalarFunction};
use crate::new_function::{
    new_function_impl, new_function_impl_with_purpose, new_function_structural,
};
use crate::{Columns, EvalError};
use tidb_ast::{Expr, QueryStmt, SelectField, Stmt};
use tidb_datatype::{Datum, FieldType, FieldTypeCode, FieldTypeFlags, SessionTimeZone};

fn limits() -> StructuralLimits {
    StructuralLimits {
        max_nodes: 256,
        max_depth: 32,
    }
}

fn bigint() -> FieldType {
    FieldType::new(FieldTypeCode::LongLong)
        .with_flen(20)
        .with_decimal(0)
}

fn int(value: i64) -> Expression {
    Expression::Constant(Constant::new(Datum::Int(value), bigint()))
}

fn ast(source: &str) -> Expr {
    let Stmt::Query(query) = tidb_parser::parse(&format!("SELECT {source}")).expect("parse") else {
        panic!("query")
    };
    let QueryStmt::Select(select) = query.into_inner() else {
        panic!("select")
    };
    let SelectField::Expr { expr, .. } = &select.fields[0] else {
        panic!("expression")
    };
    expr.clone()
}

fn function(expression: &Expression) -> &ScalarFunction {
    let Expression::ScalarFunction(function) = expression else {
        panic!("unfolded function")
    };
    function
}

struct Poison {
    columns: Vec<Column>,
    lookups: Cell<usize>,
}

impl Poison {
    fn new() -> Self {
        let columns = (0..3)
            .map(|index| {
                let mut column = Column::new(index + 100, bigint());
                column.index = index;
                column.id = index + 10;
                column.orig_name = format!("t.{}", ["a", "b", "c"][index as usize]);
                column
            })
            .collect();
        Self {
            columns,
            lookups: Cell::new(0),
        }
    }
}

impl ColumnResolver for Poison {
    fn resolve(&self, _: &[String]) -> Option<(usize, FieldType, i64)> {
        panic!("must bind a complete Column, not reconstruct one")
    }
    fn resolve_column(&self, path: &[String]) -> Option<Column> {
        self.lookups.set(self.lookups.get() + 1);
        let index = match path.last()?.as_str() {
            "a" => 0,
            "b" => 1,
            "c" => 2,
            _ => return None,
        };
        Some(self.columns[index].clone())
    }
    fn resolve_expression(&self, _: &[String]) -> Option<Expression> {
        panic!("expression binding hook")
    }
    fn resolve_constant(&self, _: &[String]) -> Option<Expression> {
        panic!("resolved constant hook")
    }
    fn resolve_default(&self, _: &[String]) -> Option<Expression> {
        panic!("default value hook")
    }
    fn param_value(&self, _: usize) -> Result<Datum, EvalError> {
        panic!("parameter")
    }
    fn fold_constant(&self, _: &mut Expression, _: ConstantFoldMode) {
        panic!("fold")
    }
    fn eval_constant(&self, _: &Expression) -> Result<Datum, EvalError> {
        panic!("evaluation")
    }
    fn comparison_context(&self) -> Option<&dyn Columns> {
        panic!("comparison context")
    }
    fn rewrite_grouping(&self, _: &[Expression]) -> Result<Expression, EvalError> {
        panic!("grouping")
    }
    fn time_zone(&self) -> SessionTimeZone {
        SessionTimeZone::utc()
    }
}

impl Columns for Poison {
    fn get(&self, _: &[String]) -> Option<Datum> {
        panic!("row read")
    }
    fn param_value(&self, _: usize) -> Result<Datum, EvalError> {
        panic!("parameter read")
    }
    fn get_param_value(&self, _: usize) -> Result<Datum, EvalError> {
        panic!("parameter read")
    }
    fn now(&self) -> Option<(i64, u32, i32)> {
        panic!("SQL clock")
    }
    fn rand_next(&self) -> Option<f64> {
        panic!("RNG")
    }
    fn rand_seeded_next(&self, _: usize, _: i64) -> Option<f64> {
        panic!("seeded RNG")
    }
    fn get_uservar(&self, _: &str) -> Option<Datum> {
        panic!("uservar read")
    }
    fn set_uservar(&self, _: &str, _: Datum) {
        panic!("uservar write")
    }
    fn sequence_nextval(&self, _: &[String]) -> Result<Datum, EvalError> {
        panic!("sequence next")
    }
    fn sequence_lastval(&self, _: &[String]) -> Result<Datum, EvalError> {
        panic!("sequence last")
    }
    fn sequence_setval(&self, _: &[String], _: i64) -> Result<Datum, EvalError> {
        panic!("sequence set")
    }
    fn acquire_advisory_lock(&self, _: &str, _: std::time::Duration) -> Result<bool, EvalError> {
        panic!("lock acquisition")
    }
    fn advisory_lock_owner(&self, _: &str) -> Result<Option<u64>, EvalError> {
        panic!("lock owner")
    }
    fn release_advisory_lock(&self, _: &str) -> Result<bool, EvalError> {
        panic!("lock release")
    }
    fn release_all_advisory_locks(&self) -> Result<usize, EvalError> {
        panic!("lock release all")
    }
    fn append_warning(&self, _: u16, _: &str) {
        panic!("warning")
    }
    fn append_note(&self, _: u16, _: &str) {
        panic!("note")
    }
    fn warning_count(&self) -> usize {
        panic!("warning bookmark")
    }
    fn truncate_warnings(&self, _: usize) {
        panic!("warning rollback")
    }
    fn take_warnings_since(&self, _: usize) -> Vec<(u16, String)> {
        panic!("warning drain")
    }
    fn skip_plan_cache_for_comparison(&self, _: &Constant, _: &str) {
        panic!("plan-cache mutation")
    }
    fn time_zone(&self) -> SessionTimeZone {
        SessionTimeZone::utc()
    }
}

// Same schema facts, ordinary SqlBuild hooks. Do not forward Poison's hooks.
struct SqlResolver<'a>(&'a Poison);
impl ColumnResolver for SqlResolver<'_> {
    fn resolve(&self, _: &[String]) -> Option<(usize, FieldType, i64)> {
        panic!("incomplete column")
    }
    fn resolve_column(&self, path: &[String]) -> Option<Column> {
        self.0.resolve_column(path)
    }
    fn time_zone(&self) -> SessionTimeZone {
        SessionTimeZone::utc()
    }
}

#[test]
fn structural_controls_never_call_value_hooks() {
    for (source, expected_lookups) in [
        ("IF(a,IFNULL(b,c),COALESCE(c,b))", 5),
        ("(a AND b) OR c", 3),
        ("CASE WHEN a THEN b ELSE c END", 3),
        ("IF(1,IFNULL(2,3),COALESCE(4,5))", 0),
    ] {
        let resolver = Poison::new();
        let built = rewrite_expr_structural(&ast(source), &resolver, limits()).unwrap();
        assert_eq!(resolver.lookups.get(), expected_lookups, "{source}");
        let root = function(built.as_expression());
        assert!(!root
            .ret_type
            .as_ref()
            .unwrap()
            .has_flag(FieldTypeFlags::NOT_NULL));
        assert!(root
            .args
            .iter()
            .all(|arg| arg.static_type().unwrap().code() == FieldTypeCode::LongLong));
    }
    for (name, args) in [
        ("if", vec![int(1), int(2), int(3)]),
        ("ifnull", vec![int(1), int(2)]),
        ("coalesce", vec![int(1), int(2)]),
        ("case", vec![int(1), int(2), int(3)]),
        ("and", vec![int(1), int(0)]),
        ("or", vec![int(0), int(1)]),
    ] {
        let built =
            new_function_structural(&Poison::new(), name, bigint(), args, limits()).unwrap();
        assert_eq!(function(built.as_expression()).func_name.lowercase(), name);
    }
}

#[test]
fn structural_explicit_signed_cast_ignores_forced_normal_fold() {
    for source in [
        "CAST('bad' AS SIGNED)",
        "CAST(CAST('bad' AS SIGNED) AS SIGNED)",
        "IF(a,CAST('bad' AS SIGNED),b)",
    ] {
        let built = rewrite_expr_structural(&ast(source), &Poison::new(), limits()).unwrap();
        let mut pending = vec![built.as_expression()];
        let mut original_literal = 0;
        let mut casts = 0;
        while let Some(node) = pending.pop() {
            match node {
                Expression::Constant(constant) => {
                    if let Some(Datum::String(value)) = constant.literal_value() {
                        assert_eq!(value.bytes(), b"bad");
                        original_literal += 1;
                    }
                }
                Expression::ScalarFunction(function) => {
                    casts += usize::from(function.func_name.lowercase() == "cast_signed");
                    pending.extend(&function.args);
                }
                _ => {}
            }
        }
        assert_eq!(original_literal, 1);
        assert!(casts >= 1);
        assert!(crate::tikv::lower_int_control_seed(
            built.as_expression(),
            &[bigint(), bigint(), bigint()],
            true,
            tidb_query_expr::local::CompileLimits::default(),
        )
        .is_err());
    }
}

#[test]
fn structural_rejection_precedes_all_value_effects() {
    for source in [
        "IF(a,1.2+3.4,b)",
        "IF(a,b > '10ab',c)",
        "IF(a,-(-9223372036854775808),b)",
        "IF(a,ROUND(1.25,'bad'),b)",
        "IF(a,DATE '2020-01-01',b)",
        "IF(a,DEFAULT(a),b)",
        "IF(a,?,b)",
        "IF(a,RAND(),b)",
        "IF(a,(@x := 1),b)",
        "IF(a,a IN (1,2),b)",
        "CASE a WHEN b THEN c ELSE a END",
    ] {
        let resolver = Poison::new();
        assert!(
            rewrite_expr_structural(&ast(source), &resolver, limits()).is_err(),
            "{source}"
        );
        assert_eq!(
            resolver.lookups.get(),
            0,
            "preflight before binding: {source}"
        );
    }
    // Use the builder's CHAR shape directly so parser charset validation is
    // not confused with the effectful invalid-charset builder branch.
    let invalid_char = Expr::Func {
        name: "char_func".into(),
        args: vec![
            Expr::String("bad".into()),
            Expr::String("no_such_charset".into()),
        ],
        origin_position: 0,
    };
    assert!(rewrite_expr_structural(&invalid_char, &Poison::new(), limits()).is_err());
    for parameter in [false, true] {
        let mut constant = Constant::new(Datum::Int(7), bigint());
        if parameter {
            constant.param_marker = Some(ParamMarker { order: 0 });
        } else {
            constant.deferred_expr = Some(Box::new(int(99)));
        }
        assert!(new_function_structural(
            &Poison::new(),
            "ifnull",
            bigint(),
            vec![Expression::Constant(constant), int(1)],
            limits(),
        )
        .is_err());
    }
    let callback = |_: ScalarFunction| -> Result<ScalarFunction, EvalError> { panic!("callback") };
    assert!(new_function_impl_with_purpose(
        &Poison::new(),
        PreparationPurpose::StructuralOnly,
        ConstantFoldMode::Normal,
        "ifnull",
        bigint(),
        Some(&callback),
        vec![int(1), int(2)],
    )
    .is_err());
}

struct RestoreStash(Vec<(u16, String)>);
impl Drop for RestoreStash {
    fn drop(&mut self) {
        let _ = take_fold_warnings();
        for (code, message) in &self.0 {
            record_fold_warning(*code, message);
        }
    }
}

#[test]
fn structural_preparation_preserves_diagnostics_and_state() {
    let _restore = RestoreStash(take_fold_warnings());
    record_fold_warning(1234, "already present");
    let poison = Poison::new();
    rewrite_expr_structural(&ast("CAST('bad' AS SIGNED)"), &poison, limits()).unwrap();
    new_function_structural(&poison, "ifnull", bigint(), vec![int(1), int(2)], limits()).unwrap();
    for source in [
        "NOW()",
        "RAND()",
        "@x",
        "(@x := 7)",
        "NEXTVAL(s)",
        "GET_LOCK('x',0)",
        "CHAR('bad' USING ascii)",
    ] {
        assert!(rewrite_expr_structural(&ast(source), &poison, limits()).is_err());
    }
    assert_eq!(fold_warnings_len(), 1);
    assert_eq!(
        take_fold_warnings(),
        vec![(1234, "already present".to_owned())]
    );
}

#[derive(Default)]
struct Legacy {
    folds: RefCell<Vec<ConstantFoldMode>>,
    warnings: RefCell<Vec<(u16, String)>>,
}
impl Columns for Legacy {
    fn get(&self, _: &[String]) -> Option<Datum> {
        None
    }
    fn append_warning(&self, code: u16, message: &str) {
        self.warnings.borrow_mut().push((code, message.to_owned()));
    }
    fn warning_count(&self) -> usize {
        self.warnings.borrow().len()
    }
    fn truncate_warnings(&self, count: usize) {
        self.warnings.borrow_mut().truncate(count);
    }
    fn time_zone(&self) -> SessionTimeZone {
        SessionTimeZone::utc()
    }
}
impl ColumnResolver for Legacy {
    fn resolve(&self, _: &[String]) -> Option<(usize, FieldType, i64)> {
        None
    }
    fn time_zone(&self) -> SessionTimeZone {
        SessionTimeZone::utc()
    }
    fn fold_constant(&self, expression: &mut Expression, mode: ConstantFoldMode) {
        self.folds.borrow_mut().push(mode);
        crate::constant_fold::fold_constant_in_mode(expression, self, mode);
    }
}

#[test]
fn structural_purpose_survives_nested_resolver_scopes() {
    let poison = Poison::new();
    let borrowed: &dyn ColumnResolver = &poison;
    let node = ast("IF(a,CAST('bad' AS SIGNED),b)");
    rewrite_expr_structural(&node, &borrowed, limits()).unwrap();
    for mode in [
        ConstantFoldMode::Normal,
        ConstantFoldMode::Try,
        ConstantFoldMode::Disabled,
    ] {
        let decorated = FoldModeResolver::new(borrowed, mode);
        let function_scope = FoldModeResolver::for_function(&decorated, "ifnull");
        rewrite_expr_structural(&node, &function_scope, limits()).unwrap();

        // Legacy explicit CAST still overrides Try/Disabled with Normal.
        let legacy = Legacy::default();
        let scope = FoldModeResolver::new(&legacy, mode);
        let built = rewrite_expr_resolved(&ast("CAST('7' AS SIGNED)"), &scope).unwrap();
        let Expression::Constant(constant) = built else {
            panic!("legacy cast fold")
        };
        assert_eq!(constant.value, Datum::Int(7));
        assert_eq!(
            legacy.folds.borrow().last(),
            Some(&ConstantFoldMode::Normal)
        );

        // Original callback occurs before the outer fold in all three modes.
        let calls = Cell::new(0);
        let callback = |function: ScalarFunction| {
            calls.set(calls.get() + 1);
            assert_eq!(function.func_name.lowercase(), "plus");
            assert_eq!(function.args.len(), 2);
            Ok(function)
        };
        let built = new_function_impl(
            &legacy,
            mode,
            "plus",
            bigint(),
            Some(&callback),
            vec![int(1), int(2)],
        )
        .unwrap();
        assert_eq!(calls.get(), 1);
        if mode == ConstantFoldMode::Disabled {
            assert_eq!(function(&built).func_name.lowercase(), "plus");
        } else {
            let Expression::Constant(constant) = built else {
                panic!("legacy arithmetic fold")
            };
            assert_eq!(constant.value, Datum::Int(3));
        }

        // Comparison refinement and its warnings still precede the callback,
        // even when outer folding is disabled or Try.
        let legacy = Legacy::default();
        let checked = Cell::new(false);
        let callback = |function: ScalarFunction| {
            checked.set(true);
            let Expression::Constant(right) = &function.args[1] else {
                panic!("refined right")
            };
            assert_eq!(right.value, Datum::Int(10));
            assert_eq!(legacy.warnings.borrow().len(), 2);
            Ok(function)
        };
        new_function_impl(
            &legacy,
            mode,
            "gt",
            FieldType::new(FieldTypeCode::Tiny),
            Some(&callback),
            vec![
                Expression::Column(Column::new(1, bigint())),
                Expression::Constant(Constant::new(
                    Datum::new_string("10ab"),
                    FieldType::new(FieldTypeCode::VarString),
                )),
            ],
        )
        .unwrap();
        assert!(checked.get());
        assert_eq!(legacy.warnings.borrow().len(), 2);
    }
}

#[test]
fn structural_reuses_declared_metadata_without_normalizing() {
    let mut resolver = Poison::new();
    resolver.columns[0].ret_type = Some(bigint().with_elems(["one"]));
    resolver.columns[0].is_hidden = true;
    resolver.columns[0]
        .collation
        .set_coercibility(crate::expr_collation::Coercibility::EXPLICIT);
    resolver.columns[0].collation.set_explicit_charset(true);
    resolver.columns[0]
        .collation
        .set_charset_and_collation("binary", "binary");
    let prepared = rewrite_expr_structural(&ast("a"), &resolver, limits()).unwrap();
    let Expression::Column(column) = prepared.as_expression() else {
        panic!("complete column")
    };
    assert_eq!(column.id, 10);
    assert_eq!(column.unique_id, 100);
    assert_eq!(column.index, 0);
    assert_eq!(column.orig_name, "t.a");
    assert!(column.is_hidden);
    assert_eq!(column.ret_type, resolver.columns[0].ret_type);
    assert!(column.collation.has_coercibility());
    assert!(column.collation.is_explicit_charset());
    assert_eq!(
        column.collation.coercibility(),
        crate::expr_collation::Coercibility::EXPLICIT
    );
    let mut alias = resolver.columns[0].ret_type.as_ref().unwrap().clone();
    alias.set_elem_with_binary_literal(0, "changed", true);
    assert_eq!(
        column.ret_type.as_ref().unwrap().elems_snapshot()[0].as_bytes(),
        b"one"
    );

    // The direct constructor also detaches metadata; consume without Clone.
    let mut field = bigint().with_elems(["literal"]);
    let direct = new_function_structural(
        &Poison::new(),
        "ifnull",
        bigint(),
        vec![
            Expression::Constant(Constant::new(Datum::Int(7), field.clone())),
            int(8),
        ],
        limits(),
    )
    .unwrap()
    .into_expression();
    field.set_elem(0, "mutated");
    assert_eq!(
        function(&direct).args[0]
            .static_type()
            .unwrap()
            .elems_snapshot()[0]
            .as_bytes(),
        b"literal"
    );

    let resolver = Poison::new();
    let node = ast("IF(a,IFNULL(b,c),COALESCE(c,b))");
    let structural = rewrite_expr_structural(&node, &resolver, limits()).unwrap();
    let sql = rewrite_expr_resolved(&node, &SqlResolver(&resolver)).unwrap();
    let mut pairs = vec![(structural.as_expression(), &sql)];
    while let Some((left, right)) = pairs.pop() {
        assert_eq!(left.static_type(), right.static_type());
        if let (Expression::ScalarFunction(left), Expression::ScalarFunction(right)) = (left, right)
        {
            assert_eq!(left.func_name, right.func_name);
            assert_eq!(
                left.collation.charset_and_collation(),
                right.collation.charset_and_collation()
            );
            pairs.extend(left.args.iter().zip(&right.args));
        }
    }
    for field in [
        FieldType::new(FieldTypeCode::Tiny),
        bigint().with_flags(FieldTypeFlags::UNSIGNED),
    ] {
        let mut resolver = Poison::new();
        resolver.columns[0].ret_type = Some(field.clone());
        assert!(rewrite_expr_structural(&ast("a"), &resolver, limits()).is_err());
        let sql = rewrite_expr_resolved(&ast("a"), &SqlResolver(&resolver)).unwrap();
        assert_eq!(sql.static_type(), Some(&field));
    }
    let null = rewrite_expr_structural(&ast("NULL"), &Poison::new(), limits()).unwrap();
    assert_eq!(
        null.as_expression().static_type().unwrap().code(),
        FieldTypeCode::Null
    );
    assert!(rewrite_expr_structural(&ast("IF(a,NULL,b)"), &Poison::new(), limits()).is_err());
    assert!(
        rewrite_expr_structural(&ast("18446744073709551615"), &Poison::new(), limits()).is_err()
    );

    // Structural constants do not masquerade as SqlBuild's computed flags.
    let structural = new_function_structural(
        &Poison::new(),
        "ifnull",
        bigint(),
        vec![int(1), int(2)],
        limits(),
    )
    .unwrap();
    let sql = new_function_impl(
        &Legacy::default(),
        ConstantFoldMode::Normal,
        "ifnull",
        bigint(),
        None,
        vec![int(1), int(2)],
    )
    .unwrap();
    assert!(!structural
        .as_expression()
        .static_type()
        .unwrap()
        .has_flag(FieldTypeFlags::NOT_NULL));
    assert!(matches!(sql, Expression::Constant(_)));
    assert!(sql
        .static_type()
        .unwrap()
        .has_flag(FieldTypeFlags::NOT_NULL));
}

#[test]
fn structural_limits_and_rejected_syntax_do_not_evaluate() {
    let node = ast("IF(a,b,c)");
    let exact = StructuralLimits {
        max_nodes: 4,
        max_depth: 2,
    };
    rewrite_expr_structural(&node, &Poison::new(), exact).unwrap();
    for limits in [
        StructuralLimits {
            max_nodes: 3,
            max_depth: 2,
        },
        StructuralLimits {
            max_nodes: 4,
            max_depth: 1,
        },
        StructuralLimits {
            max_nodes: 0,
            max_depth: 2,
        },
    ] {
        let resolver = Poison::new();
        assert!(rewrite_expr_structural(&node, &resolver, limits).is_err());
        assert_eq!(resolver.lookups.get(), 0);
        assert!(new_function_structural(
            &resolver,
            "if",
            bigint(),
            vec![int(1), int(2), int(3)],
            limits
        )
        .is_err());
    }
    new_function_structural(
        &Poison::new(),
        "if",
        bigint(),
        vec![int(1), int(2), int(3)],
        exact,
    )
    .unwrap();
    for name in ["if", "no_such_function"] {
        let node = Expr::Func {
            name: name.into(),
            args: vec![Expr::Int("1".into())],
            origin_position: 0,
        };
        assert!(rewrite_expr_structural(&node, &Poison::new(), limits()).is_err());
    }
    let mut nested = Expr::Int("1".into());
    for _ in 0..32 {
        nested = Expr::Paren(Box::new(nested));
    }
    assert!(rewrite_expr_structural(&nested, &Poison::new(), limits()).is_err());
    // This does not claim an iterative Drop for arbitrary native trees.
    assert!(StructuralExpression::checked(
        int(1),
        StructuralLimits {
            max_nodes: 1,
            max_depth: 0
        }
    )
    .is_err());
}

#[test]
fn structural_bigint_controls_feed_frozen_d1() {
    use tidb_chunk::chunk::Chunk;
    use tidb_query_datatype::expr::EvalContext;
    use tidb_query_expr::local::{CompileLimits, ExecutionLimits};

    let expression = rewrite_expr_structural(
        &ast("IF(a,IFNULL(b,c),COALESCE(c,b))"),
        &Poison::new(),
        limits(),
    )
    .unwrap()
    .into_expression();
    let schema = vec![bigint(); 3];
    let spec =
        crate::tikv::lower_int_control_seed(&expression, &schema, true, CompileLimits::default())
            .unwrap();
    let mut program =
        crate::tikv::PreparedIntControlSeed::compile(spec, ExecutionLimits::default()).unwrap();
    let mut input = Chunk::new_with_capacity(&schema, 3);
    for row in [
        [Datum::Int(1), Datum::Int(7), Datum::Int(9)],
        [Datum::Int(0), Datum::Int(8), Datum::Int(10)],
        [Datum::Null, Datum::Int(11), Datum::Null],
    ] {
        for (index, datum) in row.iter().enumerate() {
            input.append_datum(index, datum);
        }
    }
    input.set_sel(Some(vec![1, 0]));
    assert_eq!(
        program
            .eval_selected(&mut EvalContext::default(), &input, &schema, &[2, 0, 2, 1],)
            .unwrap(),
        vec![
            Datum::Int(11),
            Datum::Int(7),
            Datum::Int(11),
            Datum::Int(10)
        ]
    );
    assert!(program
        .eval_selected(&mut EvalContext::default(), &input, &schema, &[])
        .unwrap()
        .is_empty());
}
