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

use std::sync::Arc;

use tidb_ast::{CiString, QueryStmt, SelectField, Stmt};
use tidb_chunk::chunk::Chunk;
use tidb_datatype::{Datum, FieldType, FieldTypeCode};
use tidb_proto::tipb as db_pb;
use tidb_query_datatype::{codec::data_type::VectorValue, expr::EvalContext};
use tidb_query_expr::local::{
    CompileLimits, ExecutionLimits, InputRow, LocalError, LocalResult, LocalRuntimeServices,
};

use crate::column::Column;
use crate::constant::Constant;
use crate::distsql_builtin::{pb_to_expr, pb_type_to_field_type};
use crate::expression::Expression;
use crate::rewriter::{rewrite_expr_resolved, ColumnResolver};
use crate::scalar_function::ScalarFunction;

use super::batch::PreparedIntControlSeed;
use super::context::NativeInputs;
use super::lower::{lower_int_control_seed, LoweredSpec};
use super::SeedError;

fn bigint() -> FieldType {
    FieldType::new(FieldTypeCode::LongLong)
        .with_flen(20)
        .with_decimal(0)
}

struct Resolver;
impl ColumnResolver for Resolver {
    fn resolve(&self, path: &[String]) -> Option<(usize, FieldType, i64)> {
        let index = match path.last()?.as_str() {
            "a" => 0,
            "b" => 1,
            "c" => 2,
            _ => return None,
        };
        Some((index, bigint(), index as i64 + 1))
    }

    fn time_zone(&self) -> tidb_datatype::SessionTimeZone {
        // Explicit fixture timezone, as in the existing rewriter tests.
        tidb_datatype::SessionTimeZone::utc()
    }
}

fn sql(source: &str) -> Expression {
    let Stmt::Query(query) = tidb_parser::parse(&format!("SELECT {source}")).expect("parse") else {
        panic!("query")
    };
    let QueryStmt::Select(select) = query.into_inner() else {
        panic!("select")
    };
    let SelectField::Expr { expr, .. } = &select.fields[0] else {
        panic!("expression")
    };
    rewrite_expr_resolved(expr, &Resolver).expect("SQL binding and inference")
}

fn lower(expression: &Expression, schema: &[FieldType]) -> Arc<LoweredSpec> {
    lower_int_control_seed(expression, schema, true, CompileLimits::default())
        .expect("seed admission")
}

fn prepared(spec: Arc<LoweredSpec>) -> PreparedIntControlSeed {
    PreparedIntControlSeed::compile(spec, ExecutionLimits::default()).expect("local compile")
}

fn chunk(schema: &[FieldType], rows: &[Vec<Datum>]) -> Chunk {
    let mut chunk = Chunk::new_with_capacity(schema, rows.len());
    for row in rows {
        assert_eq!(row.len(), schema.len());
        for (index, value) in row.iter().enumerate() {
            chunk.append_datum(index, value);
        }
    }
    if schema.is_empty() {
        chunk.set_num_virtual_rows(rows.len());
    }
    chunk
}

fn literal(value: Datum) -> Expression {
    Expression::Constant(Constant::new(value, bigint()))
}
fn column(index: usize) -> Expression {
    let mut column = Column::new(index as i64 + 1, bigint());
    column.index = index as i64;
    Expression::Column(column)
}
fn call(name: &str, args: Vec<Expression>) -> Expression {
    Expression::ScalarFunction(ScalarFunction::new(CiString::new(name), bigint(), args))
}

#[test]
fn sql_bigint_controls_use_local_rpn() {
    let schema = vec![bigint(); 3];
    let input = chunk(
        &schema,
        &[
            vec![Datum::Int(1), Datum::Int(7), Datum::Int(9)],
            vec![Datum::Int(0), Datum::Int(8), Datum::Int(10)],
            vec![Datum::Null, Datum::Int(11), Datum::Null],
        ],
    );
    for source in ["IF(a,b,c)", "IF(a,IFNULL(b,c),COALESCE(c,b))"] {
        let expression = sql(source); // Real parser/resolver, no field retagging.
        let mut program = prepared(lower(&expression, &schema));
        let expected = if source == "IF(a,b,c)" {
            vec![Datum::Int(7), Datum::Int(10), Datum::Null]
        } else {
            vec![Datum::Int(7), Datum::Int(10), Datum::Int(11)]
        };
        assert_eq!(
            program
                .eval_selected(&mut EvalContext::default(), &input, &schema, &[0, 1, 2])
                .unwrap(),
            expected
        );
    }
}

struct Recording<'a> {
    inner: NativeInputs<'a>,
    poison: Option<usize>,
    reads: Vec<(usize, InputRow)>,
}
impl LocalRuntimeServices for Recording<'_> {
    fn binding_schema(&self) -> &[tipb::FieldType] {
        self.inner.binding_schema()
    }
    fn read_input(
        &mut self,
        ctx: &mut EvalContext,
        slot: usize,
        row: InputRow,
        expected: &tipb::FieldType,
    ) -> LocalResult<VectorValue> {
        self.reads.push((slot, row));
        if self.poison == Some(slot) {
            return Err(LocalError::BindingContract("poison demanded".into()));
        }
        self.inner.read_input(ctx, slot, row, expected)
    }
}

#[test]
fn demanded_input_only_and_local_error_retained() {
    let schema = vec![bigint(); 3];
    let spec = lower(&sql("IF(a,b,c)"), &schema);
    let input = chunk(
        &schema,
        &[vec![Datum::Int(1), Datum::Int(7), Datum::Int(9)]],
    );
    let selection = [0, 0];
    let mut inputs = Recording {
        inner: NativeInputs::new(&spec, &input, &schema, &selection).unwrap(),
        poison: Some(2),
        reads: vec![],
    };
    let mut program = prepared(Arc::clone(&spec));
    assert_eq!(
        program
            .eval_test_services(&mut EvalContext::default(), 1, &selection, &mut inputs)
            .unwrap(),
        vec![Datum::Int(7); 2]
    );
    assert_eq!(
        inputs.reads,
        vec![
            (
                0,
                InputRow {
                    occurrence: 0,
                    input_row: 0
                }
            ),
            (
                1,
                InputRow {
                    occurrence: 0,
                    input_row: 0
                }
            ),
            (
                0,
                InputRow {
                    occurrence: 1,
                    input_row: 0
                }
            ),
            (
                1,
                InputRow {
                    occurrence: 1,
                    input_row: 0
                }
            ),
        ]
    );
    inputs.reads.clear();
    inputs.poison = Some(1);
    assert!(
        matches!(program.eval_test_services(&mut EvalContext::default(), 1, &selection, &mut inputs),
        Err(SeedError::Local(LocalError::BindingContract(message))) if message == "poison demanded")
    );
    assert_eq!(inputs.reads.len(), 2); // aborts before the next occurrence
    inputs.reads.clear();
    assert!(program
        .eval_test_services(&mut EvalContext::default(), 1, &[], &mut inputs)
        .unwrap()
        .is_empty());
    assert!(inputs.reads.is_empty());
}

#[test]
fn all_int_controls_have_strict_demand() {
    // A closed typed seed test supplements, not replaces, real SQL/PB ingestion.
    let schema = vec![bigint()];
    let input = chunk(&schema, &[vec![Datum::Int(99)]]);
    let int = |value| literal(Datum::Int(value));
    let cases = [
        (call("and", vec![int(0), column(0)]), Datum::Int(0)),
        (call("or", vec![int(1), column(0)]), Datum::Int(1)),
        (call("if", vec![int(0), column(0), int(8)]), Datum::Int(8)),
        (call("ifnull", vec![int(7), column(0)]), Datum::Int(7)),
        (
            call("case", vec![int(0), column(0), int(1), int(6), column(0)]),
            Datum::Int(6),
        ),
        (
            call("coalesce", vec![literal(Datum::Null), int(5), column(0)]),
            Datum::Int(5),
        ),
    ];
    for (expression, expected) in cases {
        let spec = lower(&expression, &schema);
        let mut inputs = Recording {
            inner: NativeInputs::new(&spec, &input, &schema, &[0]).unwrap(),
            poison: Some(0),
            reads: vec![],
        };
        let mut program = prepared(Arc::clone(&spec));
        assert_eq!(
            program
                .eval_test_services(&mut EvalContext::default(), 1, &[0], &mut inputs)
                .unwrap(),
            vec![expected]
        );
        assert!(inputs.reads.is_empty());
    }
    for (expression, expected) in [
        (
            call("and", vec![literal(Datum::Null), int(0)]),
            Datum::Int(0),
        ),
        (call("and", vec![literal(Datum::Null), int(1)]), Datum::Null),
        (
            call("or", vec![literal(Datum::Null), int(1)]),
            Datum::Int(1),
        ),
        (call("or", vec![literal(Datum::Null), int(0)]), Datum::Null),
    ] {
        assert_eq!(
            prepared(lower(&expression, &schema))
                .eval_one(&mut EvalContext::default(), &input, &schema, 0)
                .unwrap(),
            expected
        );
    }
}

#[test]
fn physical_rows_and_occurrences() {
    let schema = vec![bigint()];
    let spec = lower(&column(0), &schema);
    let mut program = prepared(spec);
    for count in [0usize, 1, 1024, 1025] {
        let rows = (0..count)
            .map(|i| vec![Datum::Int(i as i64)])
            .collect::<Vec<_>>();
        let mut input = chunk(&schema, &rows);
        let selection = (0..count).rev().collect::<Vec<_>>();
        input.set_sel(Some(vec![])); // must NOT be applied again by read_input
        let actual = program
            .eval_selected(&mut EvalContext::default(), &input, &schema, &selection)
            .unwrap();
        assert_eq!(
            actual,
            selection
                .iter()
                .map(|i| Datum::Int(*i as i64))
                .collect::<Vec<_>>()
        );
        if count >= 3 {
            input.set_sel(Some(vec![1]));
            assert_eq!(
                program
                    .eval_selected(&mut EvalContext::default(), &input, &schema, &[2, 0, 2])
                    .unwrap(),
                vec![Datum::Int(2), Datum::Int(0), Datum::Int(2)]
            );
        }
    }
}

#[test]
fn schema_and_layout_rejected_before_reads() {
    let schema = vec![bigint(); 2];
    let spec = lower(&column(0), &schema);
    let mut input = chunk(&schema, &[vec![Datum::Int(1), Datum::Int(2)]]);
    input.append_int64(1, 3); // first-column row count alone is insufficient
    assert!(matches!(
        NativeInputs::new(&spec, &input, &schema, &[0]),
        Err(LocalError::InvalidBatch(_))
    ));
    let input = chunk(&schema, &[vec![Datum::Int(1), Datum::Int(2)]]);
    assert!(matches!(
        NativeInputs::new(&spec, &input, &schema, &[1]),
        Err(LocalError::InvalidBatch(_))
    ));
    assert!(matches!(
        NativeInputs::new(&spec, &input, &schema[..1], &[]),
        Err(LocalError::InvalidBatch(_))
    ));
    let wrong = chunk(
        &[FieldType::new(FieldTypeCode::Float), bigint()],
        &[vec![Datum::Float32(1.0), Datum::Int(2)]],
    );
    assert!(matches!(
        NativeInputs::new(&spec, &wrong, &schema, &[0]),
        Err(LocalError::InvalidBatch(_))
    ));
    let changed = vec![bigint().with_unsigned(true), bigint()];
    assert!(matches!(
        NativeInputs::new(&spec, &input, &changed, &[]),
        Err(LocalError::InvalidBatch(_))
    ));
}

fn wire_type(collate: Option<i32>) -> db_pb::FieldType {
    db_pb::FieldType {
        tp: Some(i32::from(FieldTypeCode::LongLong.mysql_type())),
        flag: None,
        flen: Some(20),
        decimal: Some(0),
        charset: Some("binary".into()),
        collate,
        ..Default::default()
    }
}
fn wire_column(index: i64, field_type: &db_pb::FieldType) -> db_pb::Expr {
    let mut val = Vec::new();
    tidb_codec::encode_int(&mut val, index);
    db_pb::Expr {
        tp: Some(db_pb::ExprType::ColumnRef as i32),
        val: Some(val),
        field_type: Some(field_type.clone()),
        ..Default::default()
    }
}
fn wire_if(field_type: &db_pb::FieldType) -> db_pb::Expr {
    db_pb::Expr {
        tp: Some(db_pb::ExprType::ScalarFunc as i32),
        sig: Some(db_pb::ScalarFuncSig::IfInt as i32),
        field_type: Some(field_type.clone()),
        children: (0..3).map(|index| wire_column(index, field_type)).collect(),
        ..Default::default()
    }
}

#[test]
fn pb_identity_and_wire_metadata_survive_lowering() {
    for collate in [Some(46), Some(-46), None] {
        let field_type = wire_type(collate);
        let mut wire = wire_if(&field_type);
        let schema = vec![bigint(); 3]; // effective scan type differs from wire collation
        let mut expression = pb_to_expr(&wire, &schema).unwrap();
        let Expression::ScalarFunction(function) = &mut expression else {
            panic!("PB call")
        };
        function.func_name = CiString::new("plus");
        let original = Arc::clone(function.pb_origin().unwrap());
        let clone = function.clone();
        assert!(Arc::ptr_eq(&original, clone.pb_origin().unwrap()));
        let spec = lower(&expression, &schema);
        let retained = spec.nodes[0].wire_origin.as_ref().unwrap();
        assert_eq!(retained.field_type.as_ref().unwrap(), &field_type);
        assert_eq!(retained.signature, Some(db_pb::ScalarFuncSig::IfInt as i32));
        assert_eq!(retained.child_count, 3);
        assert!(retained.val.is_none()); // shallow metadata, not children
        assert_eq!(spec.expr.field_type().get_collate(), collate.unwrap_or(0));
        assert_eq!(spec.schema[0].get_collate(), collate.unwrap_or(0));
        wire.field_type.as_mut().unwrap().collate = Some(63);
        assert_eq!(retained.field_type.as_ref().unwrap().collate, collate);
        let input = chunk(
            &schema,
            &[vec![Datum::Int(0), Datum::Int(7), Datum::Int(9)]],
        );
        assert_eq!(
            prepared(spec)
                .eval_one(&mut EvalContext::default(), &input, &schema, 0)
                .unwrap(),
            Datum::Int(9)
        );
    }
}

#[test]
fn pb_literal_transport_is_exact_and_stale_values_are_rejected() {
    let field_type = wire_type(Some(-63));
    for value in [i64::MIN, 0, i64::MAX] {
        let mut val = Vec::new();
        tidb_codec::encode_int(&mut val, value);
        let wire = db_pb::Expr {
            tp: Some(db_pb::ExprType::Int64 as i32),
            val: Some(val),
            field_type: Some(field_type.clone()),
            ..Default::default()
        };
        let mut expression = pb_to_expr(&wire, &[]).unwrap();
        let spec = lower(&expression, &[]);
        let input = chunk(&[], &[vec![]]);
        assert_eq!(
            prepared(spec)
                .eval_one(&mut EvalContext::default(), &input, &[], 0)
                .unwrap(),
            Datum::Int(value)
        );
        let Expression::Constant(constant) = &mut expression else {
            panic!("literal")
        };
        assert!(Arc::ptr_eq(
            constant.pb_origin().unwrap(),
            constant.clone().pb_origin().unwrap()
        ));
        constant.value = Datum::Int(value.wrapping_add(1));
        assert!(lower_int_control_seed(&expression, &[], true, CompileLimits::default()).is_err());
    }
}

#[test]
fn signature_factoring_keeps_remote_admission_separate() {
    use crate::pushdown_catalog::{conditional_signature, CATALOG};
    use tidb_datatype::EvalType;
    assert_eq!(
        conditional_signature("if", 3, EvalType::Int),
        Some(db_pb::ScalarFuncSig::IfInt)
    );
    assert_eq!(
        conditional_signature("if", 3, EvalType::Real),
        Some(db_pb::ScalarFuncSig::IfReal)
    );
    assert_eq!(conditional_signature("if", 2, EvalType::Int), None);
    assert_eq!(
        conditional_signature("coalesce", 1, EvalType::Int),
        Some(db_pb::ScalarFuncSig::CoalesceInt)
    );
    assert!(!CATALOG.iter().any(|row| row.name == "coalesce"));
}

#[test]
fn resource_error_is_not_replayed_or_stringified() {
    let spec = lower(&literal(Datum::Int(7)), &[]);
    let limits = ExecutionLimits {
        max_steps: 0,
        ..ExecutionLimits::default()
    };
    let mut program = PreparedIntControlSeed::compile(spec, limits).unwrap();
    let input = chunk(&[], &[vec![]]);
    assert!(matches!(
        program.eval_one(&mut EvalContext::default(), &input, &[], 0),
        Err(SeedError::Local(LocalError::ResourceLimit(_)))
    ));
}

#[test]
fn pb_stale_or_missing_origin_is_not_guessed() {
    let field_type = wire_type(Some(-63));
    let schema = vec![pb_type_to_field_type(&field_type); 3];
    let original = pb_to_expr(&wire_if(&field_type), &schema).unwrap();
    for mutation in 0..5 {
        let mut expression = original.clone();
        let Expression::ScalarFunction(function) = &mut expression else {
            panic!("call")
        };
        match mutation {
            0 => function.ret_type.as_mut().unwrap().set_flen(21),
            1 => function.pb_origin = None,
            2 => function.args[0] = column(0),
            3 => {
                let Expression::Column(column) = &mut function.args[0] else {
                    panic!("column")
                };
                column.index = 1;
            }
            _ => {
                function.args.pop();
            }
        }
        assert!(
            lower_int_control_seed(&expression, &schema, true, CompileLimits::default()).is_err()
        );
    }
    let mut wire = wire_if(&field_type);
    wire.field_type.as_mut().unwrap().array = Some(true);
    let expression = pb_to_expr(&wire, &schema).unwrap(); // native effective type still unchanged
    assert!(lower_int_control_seed(&expression, &schema, true, CompileLimits::default()).is_err());
    let mut wire = wire_column(0, &field_type);
    wire.field_type = None;
    let expression = pb_to_expr(&wire, &schema).unwrap();
    assert!(lower_int_control_seed(&expression, &schema, true, CompileLimits::default()).is_err());
    let wire = db_pb::Expr {
        tp: Some(db_pb::ExprType::Null as i32),
        field_type: Some(field_type),
        ..Default::default()
    };
    let expression = pb_to_expr(&wire, &schema).unwrap();
    assert_eq!(
        expression.static_type().unwrap().code(),
        FieldTypeCode::Null
    );
    assert!(lower_int_control_seed(&expression, &schema, true, CompileLimits::default()).is_err());
    // no retagging
}

#[test]
fn control_tree_admission_is_closed() {
    let schema = vec![bigint(); 3];
    for name in ["plus", "abs", "nullif", "getvar", "cast_signed"] {
        let expression = call(
            "if",
            vec![column(0), call(name, vec![column(1), column(2)]), column(2)],
        );
        assert!(
            lower_int_control_seed(&expression, &schema, true, CompileLimits::default()).is_err()
        );
    }
    for expression in [
        Expression::Constant(Constant::new_null()),
        literal(Datum::UInt(1)),
        Expression::Constant(Constant::new(Datum::Int(1), bigint().with_unsigned(true))),
    ] {
        assert!(
            lower_int_control_seed(&expression, &schema, true, CompileLimits::default()).is_err()
        );
    }
    let mut parameter = Constant::new(Datum::Int(1), bigint());
    parameter.param_marker = Some(crate::constant::ParamMarker { order: 0 });
    assert!(lower_int_control_seed(
        &Expression::Constant(parameter),
        &schema,
        true,
        CompileLimits::default()
    )
    .is_err());
    let mut deferred = Constant::new(Datum::Int(1), bigint());
    deferred.deferred_expr = Some(Box::new(literal(Datum::Int(1))));
    assert!(lower_int_control_seed(
        &Expression::Constant(deferred),
        &schema,
        true,
        CompileLimits::default()
    )
    .is_err());
    let field_type = wire_type(Some(63));
    let wire = db_pb::Expr {
        tp: Some(db_pb::ExprType::ScalarFunc as i32),
        sig: Some(db_pb::ScalarFuncSig::PlusInt as i32),
        field_type: Some(field_type.clone()),
        children: vec![wire_column(0, &field_type), wire_column(1, &field_type)],
        ..Default::default()
    };
    assert!(lower_int_control_seed(
        &pb_to_expr(&wire, &schema).unwrap(),
        &schema,
        true,
        CompileLimits::default()
    )
    .is_err());
}

#[test]
fn full_metadata_is_detached_and_collation_state_is_retained() {
    let mut field_type = bigint().with_elems(["one"]);
    let mut constant = Constant::new(Datum::Int(7), field_type.clone());
    constant
        .collation
        .set_coercibility(crate::expr_collation::Coercibility::EXPLICIT);
    constant.collation.set_explicit_charset(true);
    constant
        .collation
        .set_repertoire(crate::expr_collation::Repertoire::UNICODE);
    constant
        .collation
        .set_charset_and_collation("binary", "binary");
    let expression = Expression::Constant(constant);
    let spec = lower(&expression, &[]);
    field_type.set_elem_with_binary_literal(0, "changed", true);
    assert_ne!(spec.nodes[0].sql_type, field_type);
    assert_eq!(
        spec.nodes[0].sql_type.elems_snapshot()[0].as_bytes(),
        b"one"
    );
    assert!(spec.nodes[0].collation.initialized);
    assert!(spec.nodes[0].collation.explicit_charset);
    assert_eq!(
        spec.nodes[0].collation.coercibility,
        crate::expr_collation::Coercibility::EXPLICIT
    );
    let default = lower(&literal(Datum::Null), &[]);
    assert!(!default.nodes[0].collation.initialized);
}

#[test]
fn pb_origin_metadata_cannot_mutate_a_published_spec_or_relowering() {
    // LongLong with element metadata is admitted; no ENUM/SET value or retagging.
    let schema = vec![bigint().with_elems(["one"])];
    let wire = wire_column(0, &wire_type(Some(-63)));
    let expression = pb_to_expr(&wire, &schema).unwrap();
    let Expression::Column(column) = &expression else {
        panic!("PB column")
    };
    let mut consumer_type = column
        .pb_origin()
        .unwrap()
        .effective_type_snapshot()
        .unwrap();
    let spec = lower(&expression, &schema);
    consumer_type.set_elem(0, "changed");

    // Only the consumer's copy should change, never the source binding.
    assert_eq!(consumer_type.elems_snapshot()[0].as_bytes(), b"changed");
    assert_eq!(schema[0].elems_snapshot()[0].as_bytes(), b"one");
    assert_eq!(column.ret_type.as_ref(), Some(&schema[0]));
    assert_eq!(spec.nodes[0].sql_type, schema[0]);

    let retained_type = spec.nodes[0]
        .wire_origin
        .as_ref()
        .unwrap()
        .effective_type_snapshot()
        .unwrap();
    let relowered = lower_int_control_seed(&expression, &schema, true, CompileLimits::default());
    assert_eq!(
        (retained_type, relowered.is_ok()),
        (schema[0].clone(), true)
    );
}

#[test]
fn lossy_metadata_and_unsigned_wire_are_not_normalized() {
    use tidb_datatype::tikv_compat::value::BridgeError;
    let field_type = bigint().with_added_raw_flags(1u64 << 63);
    let expression = Expression::Constant(Constant::new(Datum::Int(1), field_type.clone()));
    assert!(matches!(
        lower_int_control_seed(&expression, &[], true, CompileLimits::default()),
        Err(SeedError::Bridge(BridgeError::MetadataOutOfRange {
            field: "flags",
            ..
        }))
    ));
    assert_eq!(
        expression.static_type().unwrap().raw_flags(),
        field_type.raw_flags()
    );
    let mut wire_type = wire_type(Some(-63));
    wire_type.flag = Some(tidb_datatype::FieldTypeFlags::UNSIGNED);
    let schema = vec![bigint()];
    let expression = pb_to_expr(&wire_column(0, &wire_type), &schema).unwrap();
    assert!(!expression.static_type().unwrap().is_unsigned()); // scan type remains authoritative natively
    assert!(lower_int_control_seed(&expression, &schema, true, CompileLimits::default()).is_err());
}

#[test]
fn immutable_spec_has_independent_worker_programs() {
    fn assert_send<T: Send>() {}
    assert_send::<PreparedIntControlSeed>(); // deliberately no Sync assertion
    let spec = lower(&sql("IF(a,b,c)"), &vec![bigint(); 3]);
    let workers = (0..2)
        .map(|worker| {
            let spec = Arc::clone(&spec);
            std::thread::spawn(move || {
                let mut prepared = prepared(spec);
                let schema = vec![bigint(); 3];
                for value in 0..3 {
                    let value = worker * 10 + value;
                    let input = chunk(
                        &schema,
                        &[vec![Datum::Int(1), Datum::Int(value), Datum::Int(-1)]],
                    );
                    assert_eq!(
                        prepared
                            .eval_one(&mut EvalContext::default(), &input, &schema, 0)
                            .unwrap(),
                        Datum::Int(value)
                    );
                }
            })
        })
        .collect::<Vec<_>>();
    for worker in workers {
        worker.join().unwrap();
    }
}

// Existing native Expression has recursive derived Drop. Tear down this test's
// source tree separately so it does not masquerade as local-spec drop coverage.
fn drop_source(root: Expression) {
    let mut pending = vec![root];
    while let Some(node) = pending.pop() {
        if let Expression::ScalarFunction(mut function) = node {
            pending.append(&mut function.args);
        }
    }
}

#[test]
fn deep_seed_prepare_eval_drop_and_limits() {
    for depth in [33, 64, 256] {
        let mut expression = literal(Datum::Int(7));
        for _ in 0..depth {
            expression = call("ifnull", vec![expression, literal(Datum::Int(9))]);
        }
        let limits = CompileLimits {
            max_nodes: depth * 2 + 1,
            max_depth: depth + 1,
        };
        let spec = lower_int_control_seed(&expression, &[], true, limits).unwrap();
        assert!(matches!(
            lower_int_control_seed(
                &expression,
                &[],
                true,
                CompileLimits {
                    max_nodes: depth,
                    max_depth: depth + 1
                }
            ),
            Err(SeedError::Local(LocalError::ResourceLimit(_)))
        ));
        assert!(matches!(
            lower_int_control_seed(
                &expression,
                &[],
                true,
                CompileLimits {
                    max_nodes: depth * 2 + 1,
                    max_depth: 32
                }
            ),
            Err(SeedError::Local(LocalError::ResourceLimit(_)))
        ));
        drop_source(expression);
        std::thread::Builder::new()
            .stack_size(256 * 1024)
            .spawn(move || {
                let input = chunk(&[], &[vec![]]);
                let mut program = prepared(spec); // Arc-share, never deep Clone/Debug
                assert_eq!(
                    program
                        .eval_one(&mut EvalContext::default(), &input, &[], 0)
                        .unwrap(),
                    Datum::Int(7)
                );
            })
            .unwrap()
            .join()
            .unwrap();
    }
}
