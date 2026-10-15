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

use std::cell::Cell;

use super::*;
use crate::column::Column;
use crate::constant::{Constant, ParamMarker};
use crate::distsql_builtin::pb_to_expr;
use crate::evaluator::BatchRouteSnapshot;
use tidb_ast::CiString;
use tidb_datatype::{
    BinaryJSON, Collation, Decimal, MySqlDuration, SessionTimeZone, StringDatum, Time,
    VectorFloat32,
};
use tidb_proto::tipb as db_pb;
use tidb_query_datatype::expr::Error as KernelError;
use tidb_query_expr::local::OrdinaryProfile;

fn ty() -> FieldType {
    FieldType::new(FieldTypeCode::LongLong)
        .with_flen(20)
        .with_decimal(0)
}
fn string_ty() -> FieldType {
    FieldType::new(FieldTypeCode::Varchar)
        .with_flen(128)
        .with_collation(Collation::Utf8Mb4Bin)
}
fn real_ty() -> FieldType {
    FieldType::new(FieldTypeCode::Double)
        .with_flen(22)
        .with_decimal(-1)
}
fn literal(value: Datum, field: FieldType) -> Expression {
    Expression::Constant(Constant::new(value, field))
}
fn int(value: i64) -> Expression {
    literal(Datum::Int(value), ty())
}
fn null() -> Expression {
    literal(Datum::Null, ty())
}
fn column(index: usize) -> Expression {
    let mut column = Column::new(index as i64 + 1, ty());
    column.index = index as i64;
    column.orig_name = format!("t.c{index}");
    Expression::Column(column)
}
fn string_column(index: usize) -> Expression {
    let mut column = Column::new(index as i64 + 1, string_ty());
    column.index = index as i64;
    column.orig_name = format!("t.s{index}");
    Expression::Column(column)
}
fn real_column(index: usize) -> Expression {
    let mut column = Column::new(index as i64 + 1, real_ty());
    column.index = index as i64;
    column.orig_name = format!("t.r{index}");
    Expression::Column(column)
}
fn decimal_column(index: usize, field_type: &FieldType) -> Expression {
    let mut column = Column::new(index as i64 + 1, field_type.clone());
    column.index = index as i64;
    column.orig_name = format!("t.d{index}");
    Expression::Column(column)
}
fn typed_column(index: usize, field_type: &FieldType) -> Expression {
    let mut column = Column::new(index as i64 + 1, field_type.clone());
    column.index = index as i64;
    column.orig_name = format!("t.v{index}");
    Expression::Column(column)
}
fn call(name: &str, field: FieldType, args: Vec<Expression>) -> Expression {
    Expression::ScalarFunction(ScalarFunction::new(CiString::new(name), field, args))
}
fn plus(left: Expression, right: Expression) -> Expression {
    call("plus", ty(), vec![left, right])
}
fn minus(left: Expression, right: Expression) -> Expression {
    call("minus", ty(), vec![left, right])
}
fn mul(left: Expression, right: Expression) -> Expression {
    call("mul", ty(), vec![left, right])
}
fn abs(arg: Expression) -> Expression {
    call("abs", ty(), vec![arg])
}
fn suite(root: Expression) -> EvaluatorSuite {
    EvaluatorSuite::new(vec![root], false)
}
fn limits() -> BatchSourceLimits {
    BatchSourceLimits {
        tree: CompileLimits::default(),
        max_metadata_bytes: 1024 * 1024,
    }
}
fn prepare(suite: &EvaluatorSuite, schema: &[FieldType]) -> PreparedBatch {
    PreparedBatch::compile(
        suite,
        schema,
        true,
        93,
        limits(),
        ExecutionLimits::default(),
        1024 * 1024,
    )
    .unwrap()
}
fn chunk(schema: &[FieldType], rows: &[Vec<Datum>]) -> Chunk {
    let mut result = Chunk::new_with_capacity(schema, rows.len());
    for row in rows {
        assert_eq!(row.len(), schema.len());
        for (index, value) in row.iter().enumerate() {
            result.append_datum(index, value);
        }
    }
    if schema.is_empty() {
        result.set_num_virtual_rows(rows.len());
    }
    result
}
fn output() -> Chunk {
    Chunk::new_with_capacity(&[ty()], 0)
}
fn output_for(field_type: &FieldType) -> Chunk {
    Chunk::new_with_capacity(std::slice::from_ref(field_type), 0)
}
fn values(chunk: &Chunk) -> Vec<Datum> {
    (0..chunk.num_rows())
        .map(|row| chunk.get_row(row).get_datum(0, &ty()))
        .collect()
}
fn values_for(chunk: &Chunk, field_type: &FieldType) -> Vec<Datum> {
    (0..chunk.num_rows())
        .map(|row| chunk.get_row(row).get_datum(0, field_type))
        .collect()
}

struct Context {
    enabled: Cell<bool>,
    calls: Cell<usize>,
}
impl Context {
    fn new(enabled: bool) -> Self {
        Self {
            enabled: Cell::new(enabled),
            calls: Cell::new(0),
        }
    }
}
impl Columns for Context {
    fn get(&self, _: &[String]) -> Option<Datum> {
        panic!("unexpected value lookup")
    }
    fn enable_vectorized_expression(&self) -> bool {
        self.calls.set(self.calls.get() + 1);
        self.enabled.get()
    }
    fn param_value(&self, _: usize) -> std::result::Result<Datum, EvalError> {
        panic!("parameter read before private refusal")
    }
    fn time_zone(&self) -> SessionTimeZone {
        SessionTimeZone::utc()
    }
}
fn warn(ctx: &mut EvalContext, value: &str) {
    ctx.warnings
        .append_warning(KernelError::overflow("BIGINT", value));
}

struct Raw {
    schema: Vec<tipb::FieldType>,
    indexes: Vec<usize>,
    rows: Vec<Vec<Datum>>,
    reads: Vec<(usize, InputRow)>,
    poison: Option<usize>,
    fail: Option<usize>,
    warnings: bool,
}
impl Raw {
    fn new(worker: &PreparedBatch, rows: Vec<Vec<Datum>>) -> Self {
        let source = &worker.source;
        let indexes = source
            .bindings
            .iter()
            .map(|ordinal| {
                let SourceKind::Column { index, .. } = &source.nodes[*ordinal].kind else {
                    panic!("column")
                };
                *index
            })
            .collect();
        Self {
            schema: source.schema.to_vec(),
            indexes,
            rows,
            reads: Vec::new(),
            poison: None,
            fail: None,
            warnings: false,
        }
    }
}
impl NativeDatumSource for Raw {
    fn schema(&self) -> &[tipb::FieldType] {
        &self.schema
    }
    fn read(
        &mut self,
        ctx: &mut EvalContext,
        slot: usize,
        row: InputRow,
        expected: &tipb::FieldType,
    ) -> LocalResult<Datum> {
        assert_eq!(self.schema.get(slot), Some(expected));
        self.reads.push((slot, row));
        assert_ne!(self.poison, Some(slot), "later operand phase ran");
        if self.warnings {
            warn(
                ctx,
                &format!("read:{slot}:{}:{}", row.occurrence, row.input_row),
            );
        }
        if self.fail == Some(slot) {
            return Err(LocalError::Evaluation(
                KernelError::overflow("BIGINT", "input-primary").into(),
            ));
        }
        Ok(self.rows[row.input_row][self.indexes[slot]].clone())
    }
}
fn run_provider(
    suite: &EvaluatorSuite,
    worker: &mut PreparedBatch,
    ctx: &mut EvalContext,
    input: &mut Chunk,
    output: &mut Chunk,
    schema: &[FieldType],
    source: &mut Raw,
) -> Result<()> {
    suite.run_with_consumer(
        &Context::new(true),
        input,
        output,
        &mut Consumer {
            worker,
            ctx,
            row_schema: schema,
            override_source: Some(source),
        },
    )
}

#[test]
fn own_batch_facts_preserve_source_ordinals_and_exact_declarations() {
    let root = plus(plus(column(0), int(1)), plus(column(1), null()));
    let suite = suite(root);
    let worker = prepare(&suite, &[ty(), ty()]);
    assert!(Arc::ptr_eq(&worker.source.owner, &suite.program));
    assert_eq!(worker.source.bindings.as_ref(), &[2, 5]);
    let sites = worker.source.facts.call_sites();
    assert_eq!(
        sites
            .iter()
            .map(OrdinaryCallSite::ordinal)
            .collect::<Vec<_>>(),
        vec![0, 1, 4]
    );
    for call in sites {
        assert_eq!(
            call.source(),
            OrdinarySourceId::new(93, call.ordinal() as u64)
        );
        assert_eq!(call.profile(), OrdinaryProfile::NativeBatch);
        assert_eq!(call.original_pb_signature(), None);
    }
    let LocalExpr::Call {
        function,
        return_type,
        ..
    } = &worker.source.expr
    else {
        panic!("call")
    };
    assert_eq!(*function, FunctionRef::TiPb(tipb::ScalarFuncSig::PlusInt));
    assert_eq!(
        return_type.get_tp(),
        i32::from(FieldTypeCode::LongLong.mysql_type())
    );
    assert_eq!(worker.declared_type_snapshot(), ty());
}

#[test]
fn actual_suite_native_and_private_consumers_match_selected_batch_results() {
    let root = plus(plus(column(0), int(1)), plus(column(1), int(2)));
    let schema = vec![ty(), ty()];
    let rows = vec![
        vec![Datum::Int(2), Datum::Int(3)],
        vec![Datum::Null, Datum::Int(7)],
        vec![Datum::Int(-5), Datum::Int(1)],
    ];
    let suite = suite(root);
    let mut worker = prepare(&suite, &schema);
    for selected in [vec![2, 0, 2], vec![1, 0], vec![]] {
        let mut native_input = chunk(&schema, &rows);
        native_input.set_sel(Some(selected.clone()));
        let mut private_input = chunk(&schema, &rows);
        private_input.set_sel(Some(selected));
        let mut expected = output();
        let mut actual = output();
        let native_ctx = Context::new(true);
        suite
            .run(&native_ctx, &mut native_input, &mut expected)
            .unwrap();
        assert_eq!(native_ctx.calls.get(), 1);
        let native_ctx = Context::new(true);
        suite
            .run_batch(
                &native_ctx,
                &mut EvalContext::default(),
                &mut worker,
                &mut private_input,
                &mut actual,
                &schema,
            )
            .unwrap();
        assert_eq!(native_ctx.calls.get(), 1);
        assert_eq!(values(&actual), values(&expected));
    }
}

#[test]
fn signed_int_family_reuses_the_same_batch_substrate() {
    let root = abs(mul(minus(column(0), int(2)), plus(column(1), int(3))));
    let schema = vec![ty(), ty()];
    let rows = vec![
        vec![Datum::Int(7), Datum::Int(1)],
        vec![Datum::Null, Datum::Int(9)],
        vec![Datum::Int(-4), Datum::Int(2)],
    ];
    let suite = suite(root);
    let mut worker = prepare(&suite, &schema);
    let mut input = chunk(&schema, &rows);
    input.set_sel(Some(vec![2, 0, 2, 1]));
    let mut actual = output();
    suite
        .run_batch(
            &Context::new(true),
            &mut EvalContext::default(),
            &mut worker,
            &mut input,
            &mut actual,
            &schema,
        )
        .unwrap();
    assert_eq!(
        values(&actual),
        vec![Datum::Int(30), Datum::Int(20), Datum::Int(30), Datum::Null]
    );
    let functions = worker
        .source
        .nodes
        .iter()
        .filter_map(|node| match &node.kind {
            SourceKind::Call { function, .. } => Some(*function),
            SourceKind::Constant { .. } | SourceKind::Column { .. } => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        functions,
        vec![
            FunctionRef::TiPb(tipb::ScalarFuncSig::AbsInt),
            FunctionRef::TiPb(tipb::ScalarFuncSig::MultiplyInt),
            FunctionRef::TiPb(tipb::ScalarFuncSig::MinusInt),
            FunctionRef::TiPb(tipb::ScalarFuncSig::PlusInt),
        ]
    );
}

#[test]
fn bytes_to_fixed_and_varlen_share_the_whole_selection_driver() {
    let input_type = string_ty();
    let schema = vec![input_type.clone()];
    let rows = vec![
        vec![Datum::String(StringDatum::new(
            b"AbC".to_vec(),
            Collation::Utf8Mb4Bin,
        ))],
        vec![Datum::String(StringDatum::new(
            "你好ABC".as_bytes().to_vec(),
            Collation::Utf8Mb4Bin,
        ))],
        vec![Datum::Null],
    ];
    let selected = vec![1, 0, 1, 2];
    for (root, result_type) in [
        (call("char_length", ty(), vec![string_column(0)]), ty()),
        (
            call("lower", input_type.clone(), vec![string_column(0)]),
            input_type.clone(),
        ),
    ] {
        let suite = suite(root);
        let mut worker = prepare(&suite, &schema);
        let mut native_input = chunk(&schema, &rows);
        native_input.set_sel(Some(selected.clone()));
        let mut batch_input = chunk(&schema, &rows);
        batch_input.set_sel(Some(selected.clone()));
        let mut expected = output_for(&result_type);
        let mut actual = output_for(&result_type);
        suite
            .run(&Context::new(true), &mut native_input, &mut expected)
            .unwrap();
        suite
            .run_batch(
                &Context::new(true),
                &mut EvalContext::default(),
                &mut worker,
                &mut batch_input,
                &mut actual,
                &schema,
            )
            .unwrap();
        assert_eq!(
            values_for(&actual, &result_type),
            values_for(&expected, &result_type)
        );
        assert!(worker.source.nodes.iter().any(|node| matches!(
            &node.kind,
            SourceKind::Call {
                function: FunctionRef::TiPb(
                    tipb::ScalarFuncSig::CharLengthUtf8 | tipb::ScalarFuncSig::LowerUtf8
                ),
                ..
            }
        )));
    }
}

#[test]
fn mixed_carrier_and_three_argument_calls_use_the_shared_registry() {
    let input_type = string_ty();
    let schema = vec![input_type.clone()];
    let rows = vec![
        vec![Datum::String(StringDatum::new(
            "你好ABC".as_bytes().to_vec(),
            Collation::Utf8Mb4Bin,
        ))],
        vec![Datum::String(StringDatum::new(
            b"abcdef".to_vec(),
            Collation::Utf8Mb4Bin,
        ))],
        vec![Datum::Null],
    ];
    let root = call(
        "substring",
        input_type.clone(),
        vec![string_column(0), int(2), int(3)],
    );
    let string_suite = suite(root);
    let mut worker = prepare(&string_suite, &schema);
    let mut native_input = chunk(&schema, &rows);
    native_input.set_sel(Some(vec![1, 0, 1, 2]));
    let mut batch_input = chunk(&schema, &rows);
    batch_input.set_sel(Some(vec![1, 0, 1, 2]));
    let mut expected = output_for(&input_type);
    let mut actual = output_for(&input_type);
    string_suite
        .run(&Context::new(true), &mut native_input, &mut expected)
        .unwrap();
    string_suite
        .run_batch(
            &Context::new(true),
            &mut EvalContext::default(),
            &mut worker,
            &mut batch_input,
            &mut actual,
            &schema,
        )
        .unwrap();
    assert_eq!(
        values_for(&actual, &input_type),
        values_for(&expected, &input_type)
    );
    assert!(worker.source.nodes.iter().any(|node| matches!(
        &node.kind,
        SourceKind::Call {
            function: FunctionRef::TiPb(tipb::ScalarFuncSig::Substring3ArgsUtf8),
            arity: 3,
        }
    )));

    let compare_schema = vec![ty(), ty()];
    let compare_rows = vec![
        vec![Datum::Int(3), Datum::Int(2)],
        vec![Datum::Int(1), Datum::Int(4)],
        vec![Datum::Null, Datum::Int(0)],
    ];
    for name in ["gt", "eq"] {
        let suite = suite(call(name, ty(), vec![column(0), column(1)]));
        let mut worker = prepare(&suite, &compare_schema);
        let mut native_input = chunk(&compare_schema, &compare_rows);
        native_input.set_sel(Some(vec![1, 0, 2, 0]));
        let mut batch_input = chunk(&compare_schema, &compare_rows);
        batch_input.set_sel(Some(vec![1, 0, 2, 0]));
        let mut expected = output();
        let mut actual = output();
        suite
            .run(&Context::new(true), &mut native_input, &mut expected)
            .unwrap();
        suite
            .run_batch(
                &Context::new(true),
                &mut EvalContext::default(),
                &mut worker,
                &mut batch_input,
                &mut actual,
                &compare_schema,
            )
            .unwrap();
        assert_eq!(
            values(&expected),
            if name == "gt" {
                vec![Datum::Int(0), Datum::Int(1), Datum::Null, Datum::Int(1)]
            } else {
                vec![Datum::Int(0), Datum::Int(0), Datum::Null, Datum::Int(0)]
            }
        );
        assert_eq!(values(&actual), values(&expected));
    }
}

#[test]
fn real_arithmetic_uses_the_same_batch_carrier_contract() {
    let field = real_ty();
    let schema = vec![field.clone(), field.clone()];
    let rows = vec![
        vec![Datum::Real(1.5), Datum::Real(2.25)],
        vec![Datum::Null, Datum::Real(4.0)],
        vec![Datum::Real(-3.0), Datum::Real(0.5)],
    ];
    let root = call("plus", field.clone(), vec![real_column(0), real_column(1)]);
    let suite = suite(root);
    let mut worker = prepare(&suite, &schema);
    let mut native_input = chunk(&schema, &rows);
    native_input.set_sel(Some(vec![2, 0, 2, 1]));
    let mut batch_input = chunk(&schema, &rows);
    batch_input.set_sel(Some(vec![2, 0, 2, 1]));
    let mut expected = output_for(&field);
    let mut actual = output_for(&field);
    suite
        .run(&Context::new(true), &mut native_input, &mut expected)
        .unwrap();
    suite
        .run_batch(
            &Context::new(true),
            &mut EvalContext::default(),
            &mut worker,
            &mut batch_input,
            &mut actual,
            &schema,
        )
        .unwrap();
    assert_eq!(values_for(&actual, &field), values_for(&expected, &field));
    assert!(worker.source.nodes.iter().any(|node| matches!(
        &node.kind,
        SourceKind::Call {
            function: FunctionRef::TiPb(tipb::ScalarFuncSig::PlusReal),
            arity: 2,
        }
    )));
}

#[test]
fn decimal_arithmetic_uses_the_exact_shared_representation() {
    let field = FieldType::new(FieldTypeCode::NewDecimal)
        .with_flen(18)
        .with_decimal(2);
    let schema = vec![field.clone(), field.clone()];
    let dec = |value| Datum::Decimal(Decimal::from_literal(value));
    let rows = vec![
        vec![dec("1.25"), dec("2.50")],
        vec![Datum::Null, dec("4.00")],
        vec![dec("-3.75"), dec("0.50")],
    ];
    let root = call(
        "plus",
        field.clone(),
        vec![decimal_column(0, &field), decimal_column(1, &field)],
    );
    let suite = suite(root);
    let mut worker = prepare(&suite, &schema);
    let mut native_input = chunk(&schema, &rows);
    native_input.set_sel(Some(vec![2, 0, 2, 1]));
    let mut batch_input = chunk(&schema, &rows);
    batch_input.set_sel(Some(vec![2, 0, 2, 1]));
    let mut expected = output_for(&field);
    let mut actual = output_for(&field);
    suite
        .run(&Context::new(true), &mut native_input, &mut expected)
        .unwrap();
    suite
        .run_batch(
            &Context::new(true),
            &mut EvalContext::default(),
            &mut worker,
            &mut batch_input,
            &mut actual,
            &schema,
        )
        .unwrap();
    assert_eq!(values_for(&actual, &field), values_for(&expected, &field));
    assert!(worker.source.nodes.iter().any(|node| matches!(
        &node.kind,
        SourceKind::Call {
            function: FunctionRef::TiPb(tipb::ScalarFuncSig::PlusDecimal),
            arity: 2,
        }
    )));
}

#[test]
fn varlen_production_route_streams_reverse_duplicates_across_windows() {
    let input_type = string_ty();
    let schema = vec![input_type.clone()];
    let rows = (0..2050)
        .map(|row| {
            vec![Datum::String(StringDatum::new(
                format!("Row{row}ABC").into_bytes(),
                Collation::Utf8Mb4Bin,
            ))]
        })
        .collect::<Vec<_>>();
    let selection = (0..2050).rev().map(|row| row / 2).collect::<Vec<_>>();
    let root = call("lower", input_type.clone(), vec![string_column(0)]);
    let suite = suite(root);
    let mut native_input = chunk(&schema, &rows);
    native_input.set_sel(Some(selection.clone()));
    let mut expected = output_for(&input_type);
    suite
        .run(&Context::new(true), &mut native_input, &mut expected)
        .unwrap();

    let mut batch_input = chunk(&schema, &rows);
    batch_input.set_sel(Some(selection));
    let mut actual = output_for(&input_type);
    suite
        .run_with_tikv_batch(&Context::new(true), &mut batch_input, &mut actual, &schema)
        .unwrap();
    assert_eq!(
        values_for(&actual, &input_type),
        values_for(&expected, &input_type)
    );
    assert_eq!(
        suite.batch_route_snapshot(),
        BatchRouteSnapshot {
            prepared: 1,
            fallback: 0,
            refusals: 0,
            refusal: None,
        }
    );
}

#[test]
fn any_value_batch_round_trips_every_native_eval_type() {
    let mut decimal = FieldType::new(FieldTypeCode::NewDecimal);
    decimal.set_flen(20);
    decimal.set_decimal(4);
    let mut duration = FieldType::new(FieldTypeCode::Duration);
    duration.set_decimal(6);
    let cases = vec![
        (ty(), Datum::Int(-17)),
        (real_ty(), Datum::Real(3.25)),
        (decimal, Datum::Decimal(Decimal::from_literal("123.4567"))),
        (
            string_ty(),
            Datum::String(StringDatum::new(
                b"All carriers".to_vec(),
                Collation::Utf8Mb4Bin,
            )),
        ),
        (
            FieldType::new(FieldTypeCode::Datetime),
            Datum::Time(Time::from_go_raw_like_go(0)),
        ),
        (
            FieldType::new(FieldTypeCode::Timestamp),
            Datum::Time(Time::from_go_raw_like_go(1)),
        ),
        (
            duration,
            Datum::Duration(MySqlDuration::from_raw_parts(-12_345_678_000, 6)),
        ),
        (
            FieldType::new(FieldTypeCode::Json),
            Datum::Json(BinaryJSON::parse(r#"{"batch":true}"#).unwrap()),
        ),
        (
            FieldType::new(FieldTypeCode::VectorFloat32),
            Datum::VectorFloat32(VectorFloat32::must_create(vec![1.25, -2.5, 3.0])),
        ),
    ];

    for (field, value) in cases {
        let schema = vec![field.clone()];
        let root = call("any_value", field.clone(), vec![typed_column(0, &field)]);
        let suite = suite(root);
        let rows = vec![vec![Datum::Null], vec![value.clone()]];
        let selection = vec![1, 0, 1];
        let mut native_input = chunk(&schema, &rows);
        native_input.set_sel(Some(selection.clone()));
        let mut expected = output_for(&field);
        suite
            .run(&Context::new(true), &mut native_input, &mut expected)
            .unwrap();

        let mut batch_input = chunk(&schema, &rows);
        batch_input.set_sel(Some(selection));
        let mut actual = output_for(&field);
        suite
            .run_with_tikv_batch(&Context::new(true), &mut batch_input, &mut actual, &schema)
            .unwrap_or_else(|error| panic!("failed carrier {:?}: {error:?}", field.eval_type()));
        assert_eq!(
            values_for(&actual, &field),
            values_for(&expected, &field),
            "failed carrier {:?}",
            field.eval_type()
        );
        assert_eq!(
            suite.batch_route_snapshot(),
            BatchRouteSnapshot {
                prepared: 1,
                fallback: 0,
                refusals: 0,
                refusal: None,
            }
        );
    }
}

#[test]
fn current_global_flag_is_read_once_per_invoke_and_never_row_fallback() {
    let suite = suite(plus(null(), plus(int(i64::MAX), int(1))));
    let mut worker = prepare(&suite, &[]);
    let native_ctx = Context::new(false);
    let mut input = chunk(&[], &[vec![]]);
    let mut out = output();
    out.append_int64(0, 99);
    let error = suite
        .run_batch(
            &native_ctx,
            &mut EvalContext::default(),
            &mut worker,
            &mut input,
            &mut out,
            &[],
        )
        .unwrap_err();
    assert!(matches!(error.kind, FailureKind::Admission(_)));
    assert_eq!(native_ctx.calls.get(), 1);
    assert_eq!(values(&out), vec![Datum::Int(99)]); // no native NULL fallback append
    native_ctx.enabled.set(true);
    let error = suite
        .run_batch(
            &native_ctx,
            &mut EvalContext::default(),
            &mut worker,
            &mut input,
            &mut out,
            &[],
        )
        .unwrap_err();
    assert_eq!(native_ctx.calls.get(), 2);
    assert!(matches!(
        error.local_error(),
        Some(LocalError::Evaluation(_))
    ));
    assert_eq!(values(&out), vec![Datum::Int(99)]);
    let mut native_out = output();
    native_ctx.enabled.set(false);
    suite.run(&native_ctx, &mut input, &mut native_out).unwrap();
    assert_eq!(values(&native_out), vec![Datum::Null]);
    assert_eq!(native_ctx.calls.get(), 3);
}

#[test]
fn actual_nonvectorizable_program_does_not_query_global_flag_or_run_values() {
    // Hostile cached classification in the owning module: a root-shape shortcut
    // would batch this anyway. The mandatory consumer follows the actual suite.
    let mut program = EvaluatorProgram::new(vec![plus(int(1), int(2))], false);
    program.vectorizable = false;
    let suite = EvaluatorSuite::from_program(Arc::new(program));
    let mut worker = prepare(&suite, &[]);
    let ctx = Context::new(true);
    let mut out = output();
    let error = suite
        .run_batch(
            &ctx,
            &mut EvalContext::default(),
            &mut worker,
            &mut chunk(&[], &[vec![]]),
            &mut out,
            &[],
        )
        .unwrap_err();
    assert!(matches!(error.kind, FailureKind::Admission(_)));
    assert_eq!(ctx.calls.get(), 0);
    assert!(values(&out).is_empty());
}

#[test]
fn native_decimal_priority_and_leaf_admission_remain_coherent() {
    let decimal = FieldType::new(FieldTypeCode::NewDecimal)
        .with_flen(12)
        .with_decimal(0);
    let root = call(
        "plus",
        decimal.clone(),
        vec![
            literal(Datum::Decimal(Decimal::from_int(1)), decimal.clone()),
            literal(Datum::Decimal(Decimal::from_int(2)), decimal.clone()),
        ],
    );
    let suite = suite(root);
    let ctx = Context::new(false);
    let mut out = Chunk::new_with_capacity(&[decimal.clone()], 1);
    suite
        .run(&ctx, &mut chunk(&[], &[vec![]]), &mut out)
        .unwrap();
    assert_eq!(ctx.calls.get(), 0); // Decimal consumes before the global getter.
    assert_eq!(
        out.get_row(0).get_datum(0, &decimal),
        Datum::Decimal(Decimal::from_int(3))
    );
    // The ordinary consumer still preserves the existing native Decimal
    // priority, while the mandatory batch consumer can now compile the same
    // expression through the exact shared Decimal carrier.
    assert!(PreparedBatch::compile(
        &suite,
        &[],
        true,
        1,
        limits(),
        ExecutionLimits::default(),
        4096
    )
    .is_ok());
    for root in [int(1), column(0)] {
        let input = chunk(&[ty()], &[vec![Datum::Int(9)]]);
        assert!(
            crate::scalar_function::try_eval_batch(&root, &Context::new(true), &input)
                .unwrap()
                .is_none()
        );
        assert!(PreparedBatch::compile(
            &self::suite(root),
            &[ty()],
            true,
            1,
            limits(),
            ExecutionLimits::default(),
            4096
        )
        .is_err());
    }
}

#[test]
fn foreign_suite_decimal_or_control_cannot_do_work_before_private_refusal() {
    let own = suite(plus(int(1), int(2)));
    let mut worker = prepare(&own, &[]);
    let mut parameter = Constant::new(Datum::Int(1), ty());
    parameter.param_marker = Some(ParamMarker { order: 0 });
    for root in [
        plus(int(1), int(2)),
        call(
            "if",
            ty(),
            vec![int(1), Expression::Constant(parameter), int(0)],
        ),
        call(
            "plus",
            FieldType::new(FieldTypeCode::NewDecimal),
            vec![int(1), int(2)],
        ),
    ] {
        let foreign = suite(root);
        let ctx = Context::new(true);
        let mut out = output();
        let error = foreign
            .run_batch(
                &ctx,
                &mut EvalContext::default(),
                &mut worker,
                &mut chunk(&[], &[vec![]]),
                &mut out,
                &[],
            )
            .unwrap_err();
        assert!(matches!(error.kind, FailureKind::Admission(_)));
        assert_eq!(ctx.calls.get(), 0);
        assert!(values(&out).is_empty());
    }
    // Even a privately corrupted owner link cannot bypass independent source
    // preflight and reach the earlier Decimal worker or control value work.
    let mut parameter = Constant::new(Datum::Int(1), ty());
    parameter.param_marker = Some(ParamMarker { order: 0 });
    for root in [
        call(
            "if",
            ty(),
            vec![int(1), Expression::Constant(parameter), int(0)],
        ),
        call(
            "plus",
            FieldType::new(FieldTypeCode::NewDecimal),
            vec![int(1), int(2)],
        ),
    ] {
        let blocked = suite(root);
        let mut spliced = prepare(&own, &[]);
        Arc::get_mut(&mut spliced.source).unwrap().owner = Arc::clone(&blocked.program);
        let ctx = Context::new(true);
        let mut out = output();
        let error = blocked
            .run_batch(
                &ctx,
                &mut EvalContext::default(),
                &mut spliced,
                &mut chunk(&[], &[vec![]]),
                &mut out,
                &[],
            )
            .unwrap_err();
        assert!(matches!(error.kind, FailureKind::Admission(_)));
        assert_eq!(ctx.calls.get(), 0);
        assert!(values(&out).is_empty());
    }
    for other in [
        EvaluatorSuite::new(vec![plus(int(1), int(2)), int(3)], false),
        EvaluatorSuite::new(vec![plus(column(0), int(1)), column(0)], false),
    ] {
        assert!(PreparedBatch::compile(
            &other,
            &[ty()],
            true,
            1,
            limits(),
            ExecutionLimits::default(),
            4096
        )
        .is_err());
    }
}

#[test]
fn left_failure_stops_the_later_input_phase() {
    let root = plus(plus(column(0), int(1)), plus(column(1), int(1)));
    let schema = vec![ty(), ty()];
    let rows = vec![
        vec![Datum::Int(0), Datum::Int(i64::MAX)],
        vec![Datum::Int(i64::MAX), Datum::Int(0)],
    ];
    let suite = suite(root);
    let mut worker = prepare(&suite, &schema);
    let mut raw = Raw::new(&worker, rows.clone());
    raw.poison = Some(1);
    let error = run_provider(
        &suite,
        &mut worker,
        &mut EvalContext::default(),
        &mut chunk(&schema, &rows),
        &mut output(),
        &schema,
        &mut raw,
    )
    .unwrap_err();
    assert!(matches!(
        error.local_error(),
        Some(LocalError::Evaluation(_))
    ));
    assert_eq!(
        raw.reads
            .iter()
            .map(|(slot, row)| (*slot, row.occurrence))
            .collect::<Vec<_>>(),
        vec![(0, 0), (0, 1)]
    );
}

#[test]
fn both_input_phases_complete_before_a_right_failure() {
    let root = plus(column(0), plus(column(1), int(1)));
    let schema = vec![ty(), ty()];
    let rows = vec![
        vec![Datum::Int(i64::MAX), Datum::Int(0)],
        vec![Datum::Int(0), Datum::Int(i64::MAX)],
    ];
    let suite = suite(root);
    let mut worker = prepare(&suite, &schema);
    let mut raw = Raw::new(&worker, rows.clone());
    let error = run_provider(
        &suite,
        &mut worker,
        &mut EvalContext::default(),
        &mut chunk(&schema, &rows),
        &mut output(),
        &schema,
        &mut raw,
    )
    .unwrap_err();
    assert!(matches!(
        error.local_error(),
        Some(LocalError::Evaluation(_))
    ));
    assert_eq!(
        raw.reads
            .iter()
            .map(|(slot, row)| (*slot, row.occurrence))
            .collect::<Vec<_>>(),
        vec![(0, 0), (0, 1), (1, 0), (1, 1)]
    );
}

#[test]
fn selected_occurrences_complete_the_input_phase_before_failure() {
    let schema = vec![ty()];
    let rows = vec![
        vec![Datum::Int(0)],
        vec![Datum::Int(8)],
        vec![Datum::Int(i64::MAX)],
    ];
    let suite = suite(plus(column(0), int(1)));
    let mut worker = prepare(&suite, &schema);
    let mut raw = Raw::new(&worker, rows.clone());
    let mut input = chunk(&schema, &rows);
    input.set_sel(Some(vec![1, 0, 2, 2]));
    let error = run_provider(
        &suite,
        &mut worker,
        &mut EvalContext::default(),
        &mut input,
        &mut output(),
        &schema,
        &mut raw,
    )
    .unwrap_err();
    assert!(matches!(
        error.local_error(),
        Some(LocalError::Evaluation(_))
    ));
    assert_eq!(
        raw.reads
            .iter()
            .map(|(_, row)| (row.occurrence, row.input_row))
            .collect::<Vec<_>>(),
        vec![(0, 1), (1, 0), (2, 2), (3, 2)]
    );
}

#[test]
fn selected_occurrences_tile_across_driver_windows_for_virtual_constant_broadcasts() {
    let suite = suite(plus(int(1), int(2)));
    let mut worker = prepare(&suite, &[]);
    for count in [0, 1, 1024, 1025, 2048] {
        let mut input = chunk(&[], &vec![vec![]; 4096]);
        input.set_sel(Some((0..count).map(|row| 4095 - row).collect()));
        let mut out = output();
        suite
            .run_batch(
                &Context::new(true),
                &mut EvalContext::default(),
                &mut worker,
                &mut input,
                &mut out,
                &[],
            )
            .unwrap();
        assert_eq!(values(&out), vec![Datum::Int(3); count]);
    }
}

#[test]
fn wide_selected_input_preserves_reverse_and_duplicate_order_across_windows() {
    let schema = vec![ty()];
    let rows = (0..2049)
        .map(|value| vec![Datum::Int(value)])
        .collect::<Vec<_>>();
    let suite = suite(plus(column(0), int(1)));
    let mut worker = prepare(&suite, &schema);
    let mut input = chunk(&schema, &rows);
    let mut selection = (0..2049).rev().collect::<Vec<_>>();
    selection.extend([17, 17, 0]);
    input.set_sel(Some(selection.clone()));
    let mut out = output();
    suite
        .run_batch(
            &Context::new(true),
            &mut EvalContext::default(),
            &mut worker,
            &mut input,
            &mut out,
            &schema,
        )
        .unwrap();
    assert_eq!(
        values(&out),
        selection
            .into_iter()
            .map(|row| Datum::Int(row as i64 + 1))
            .collect::<Vec<_>>()
    );
}

#[test]
fn strict_native_kind_validation_precedes_erasure() {
    let schema = vec![ty()];
    let suite = suite(plus(column(0), int(1)));
    let mut worker = prepare(&suite, &schema);
    let mut raw = Raw::new(&worker, vec![vec![Datum::UInt(0)]]);
    assert!(to_scalar(&Datum::UInt(0), TikvEvalType::Int).is_ok());
    let error = run_provider(
        &suite,
        &mut worker,
        &mut EvalContext::default(),
        &mut chunk(&schema, &[vec![Datum::Int(0)]]),
        &mut output(),
        &schema,
        &mut raw,
    )
    .unwrap_err();
    assert!(matches!(
        error.local_error(),
        Some(LocalError::BindingContract(_))
    ));
    assert_eq!(raw.reads.len(), 1);
}

#[test]
fn warnings_and_provider_errors_survive_retries() {
    let schema = vec![ty(), ty()];
    let rows = vec![
        vec![Datum::Int(1), Datum::Int(2)],
        vec![Datum::Int(3), Datum::Int(4)],
    ];
    let suite = suite(plus(column(0), column(1)));
    let mut worker = prepare(&suite, &schema);
    let mut ctx = EvalContext::default();
    warn(&mut ctx, "prior");
    let mut raw = Raw::new(&worker, rows.clone());
    raw.warnings = true;
    raw.fail = Some(1);
    let error = run_provider(
        &suite,
        &mut worker,
        &mut ctx,
        &mut chunk(&schema, &rows),
        &mut output(),
        &schema,
        &mut raw,
    )
    .unwrap_err();
    assert_eq!(ctx.warnings.warning_cnt, 4); // prior + two left + first right
    assert_eq!(raw.reads.len(), 3);
    assert!(ctx
        .warnings
        .warnings
        .iter()
        .any(|warning| warning.get_msg().contains("prior")));
    let message = error.local_error().unwrap().to_string();
    assert!(message.contains("input-primary"));
    let mut out = output();
    suite
        .run_batch(
            &Context::new(true),
            &mut ctx,
            &mut worker,
            &mut chunk(&schema, &rows),
            &mut out,
            &schema,
        )
        .unwrap();
    assert_eq!(values(&out), vec![Datum::Int(3), Datum::Int(7)]);
    let mut empty = chunk(&schema, &rows);
    empty.set_sel(Some(vec![]));
    suite
        .run_batch(
            &Context::new(true),
            &mut ctx,
            &mut worker,
            &mut empty,
            &mut output(),
            &schema,
        )
        .unwrap();
    assert_eq!(ctx.warnings.warning_cnt, 4);
    let bad = vec![vec![Datum::Int(i64::MAX), Datum::Int(1)]];
    let bindings_error = suite
        .run_batch(
            &Context::new(true),
            &mut ctx,
            &mut worker,
            &mut chunk(&schema, &bad),
            &mut output(),
            &schema,
        )
        .unwrap_err();
    assert!(matches!(
        bindings_error.local_error(),
        Some(LocalError::Evaluation(_))
    ));
}

#[test]
fn program_metadata_detachment_and_incoming_caps_precede_equality_and_effects() {
    let mut field = ty().with_elems(["first", "second"]);
    field.set_elem_with_binary_literal(1, "second", true);
    let mut alias = field.clone();
    let root = call(
        "plus",
        field.clone(),
        vec![
            literal(Datum::Int(1), field.clone()),
            literal(Datum::Int(2), field),
        ],
    );
    let suite = suite(root);
    let mut worker = prepare(&suite, &[]);
    let mut copy = worker.declared_type_snapshot();
    copy.set_elem(0, "outside");
    assert_eq!(worker.declared_type_snapshot().elem(0).as_bytes(), b"first");
    assert!(worker.declared_type_snapshot().elem_is_binary_literal(1));
    alias.set_elem(0, "source changed");
    let ctx = Context::new(true);
    let mut out = output();
    suite
        .run_batch(
            &ctx,
            &mut EvalContext::default(),
            &mut worker,
            &mut chunk(&[], &[vec![]]),
            &mut out,
            &[],
        )
        .unwrap();
    assert_eq!(values(&out), vec![Datum::Int(3)]);
    assert_eq!(ctx.calls.get(), 1);
    let suite = self::suite(plus(column(0), int(1)));
    let mut worker = prepare(&suite, &[ty()]);
    let huge = ty().with_elems(["x".repeat(limits().max_metadata_bytes)]);
    let ctx = Context::new(true);
    let error = suite
        .run_batch(
            &ctx,
            &mut EvalContext::default(),
            &mut worker,
            &mut chunk(&[ty()], &[vec![Datum::Int(1)]]),
            &mut output(),
            &[huge],
        )
        .unwrap_err();
    assert!(matches!(
        error.local_error(),
        Some(LocalError::ResourceLimit(_))
    ));
    assert_eq!(ctx.calls.get(), 0); // Resource gate, not later allocating Eq/eligibility.
}

#[test]
fn malformed_empty_selection_schema_and_native_layout_are_not_skipped() {
    let suite = suite(plus(column(0), int(1)));
    let mut worker = prepare(&suite, &[ty()]);
    let mut input = chunk(&[ty()], &[vec![Datum::Int(0)]]);
    input.set_sel(Some(vec![]));
    let wrong = ty().with_flen(7);
    let _error = suite
        .run_batch(
            &Context::new(true),
            &mut EvalContext::default(),
            &mut worker,
            &mut input,
            &mut output(),
            &[wrong],
        )
        .unwrap_err();
    input.set_sel(Some(vec![1]));
    let error = suite
        .run_batch(
            &Context::new(true),
            &mut EvalContext::default(),
            &mut worker,
            &mut input,
            &mut output(),
            &[ty()],
        )
        .unwrap_err();
    assert!(matches!(
        error.local_error(),
        Some(LocalError::InvalidBatch(_))
    ));
    let string = FieldType::new(FieldTypeCode::Varchar).with_collation(Collation::Binary);
    let mut input = chunk(&[string], &[vec![Datum::Bytes(vec![1])]]);
    input.set_sel(Some(vec![]));
    let error = suite
        .run_batch(
            &Context::new(true),
            &mut EvalContext::default(),
            &mut worker,
            &mut input,
            &mut output(),
            &[ty()],
        )
        .unwrap_err();
    assert!(matches!(
        error.local_error(),
        Some(LocalError::InvalidBatch(_))
    ));
    let mut input = chunk(&[ty()], &[vec![Datum::Int(0)]]);
    let mut wrong_output = Chunk::new_with_capacity(&[], 0);
    assert!(suite
        .run_batch(
            &Context::new(true),
            &mut EvalContext::default(),
            &mut worker,
            &mut input,
            &mut wrong_output,
            &[ty()]
        )
        .is_err());
}

#[test]
fn whole_source_closure_refuses_unproved_nodes_flags_and_pb_even_when_empty() {
    let mut parameter = Constant::new(Datum::Int(1), ty());
    parameter.param_marker = Some(ParamMarker { order: 0 });
    let mut deferred = Constant::new(Datum::Int(1), ty());
    deferred.deferred_expr = Some(Box::new(int(1)));
    let mut virtual_column = Column::new(8, ty());
    virtual_column.index = 0;
    virtual_column.virtual_expr = Some(Box::new(int(1)));
    let mut opaque = ScalarFunction::new_values(0, ty());
    opaque.func_name = CiString::new("plus");
    opaque.args = vec![int(1), int(2)];
    let mut encoded = Vec::new();
    tidb_codec::encode_int(&mut encoded, 1);
    let pb = pb_to_expr(
        &db_pb::Expr {
            tp: Some(db_pb::ExprType::Int64 as i32),
            val: Some(encoded),
            field_type: Some(db_pb::FieldType {
                tp: Some(i32::from(FieldTypeCode::LongLong.mysql_type())),
                flen: Some(20),
                decimal: Some(0),
                charset: Some("binary".into()),
                collate: Some(-63),
                ..Default::default()
            }),
            ..Default::default()
        },
        &[],
    )
    .unwrap();
    for bad in [
        Expression::Constant(parameter),
        Expression::Constant(deferred),
        Expression::Column(virtual_column),
        Expression::ScalarFunction(opaque),
        pb,
        call("cast", ty(), vec![int(1)]),
        call("if", ty(), vec![int(1), int(2), int(3)]),
        literal(Datum::UInt(1), ty().with_unsigned(true)),
        literal(Datum::UInt(1), ty()),
        literal(Datum::Null, FieldType::new(FieldTypeCode::Null)),
        literal(Datum::Int(1), FieldType::new(FieldTypeCode::Year)),
        literal(Datum::Int(1), ty().with_array(true)),
        literal(
            Datum::Int(1),
            ty().with_flags(FieldTypeFlags::ENUM_SET_AS_INT),
        ),
        literal(
            Datum::Int(1),
            ty().with_flags(FieldTypeFlags::PARSE_TO_JSON),
        ),
        literal(Datum::Int(1), ty().with_flags(1 << 31)),
    ] {
        assert!(PreparedBatch::compile(
            &suite(plus(null(), bad)),
            &[ty()],
            true,
            1,
            limits(),
            ExecutionLimits::default(),
            65536
        )
        .is_err());
    }
    let root = call("if", ty(), vec![int(1), plus(int(1), int(2)), int(0)]);
    assert!(PreparedBatch::compile(
        &suite(root),
        &[],
        true,
        1,
        limits(),
        ExecutionLimits::default(),
        65536
    )
    .is_err());
    let root = call("plus", ty(), vec![int(1)]);
    assert!(PreparedBatch::compile(
        &suite(root),
        &[],
        true,
        1,
        limits(),
        ExecutionLimits::default(),
        65536
    )
    .is_err());
}

#[test]
fn source_depth_and_retained_limits_are_resources_not_sql_null_or_retry() {
    let suite = suite(plus(column(0), int(1)));
    let mut source_limits = limits();
    source_limits.tree.max_depth = 1;
    assert!(matches!(
        PreparedBatch::compile(
            &suite,
            &[ty()],
            true,
            1,
            source_limits,
            ExecutionLimits::default(),
            4096
        ),
        Err(BatchFailure {
            kind: FailureKind::Local(LocalError::ResourceLimit(_))
        })
    ));
    let mut worker = prepare(&suite, &[ty()]);
    worker.max_materialization_retained_bytes = size_of::<usize>(); // selection fits, result/source/native Vec do not
    let rows = vec![vec![Datum::Int(1)]];
    let mut raw = Raw::new(&worker, rows.clone());
    raw.warnings = true;
    let mut ctx = EvalContext::default();
    let error = run_provider(
        &suite,
        &mut worker,
        &mut ctx,
        &mut chunk(&[ty()], &rows),
        &mut output(),
        &[ty()],
        &mut raw,
    )
    .unwrap_err();
    assert!(matches!(
        error.local_error(),
        Some(LocalError::ResourceLimit(_))
    ));
    assert_eq!(raw.reads.len(), 1);
    assert_eq!(ctx.warnings.warning_cnt, 1);
    let mut worker = PreparedBatch::compile(
        &suite,
        &[ty()],
        true,
        93,
        limits(),
        ExecutionLimits {
            max_retained_bytes: 0,
            ..ExecutionLimits::default()
        },
        usize::MAX,
    )
    .unwrap();
    let mut raw = Raw::new(&worker, rows.clone());
    let error = run_provider(
        &suite,
        &mut worker,
        &mut ctx,
        &mut chunk(&[ty()], &rows),
        &mut output(),
        &[ty()],
        &mut raw,
    )
    .unwrap_err();
    assert!(matches!(
        error.local_error(),
        Some(LocalError::ResourceLimit(_))
    ));
    assert!(raw.reads.is_empty());
}

#[test]
fn direct_output_counts_public_vector_and_selection_capacities() {
    let suite = suite(plus(int(1), int(2)));
    let mut worker = prepare(&suite, &[]);
    let mut selection = Vec::with_capacity(256);
    selection.push(0);
    let vector = VectorValue::from_scalar(&ScalarValue::Int(Some(3)), 1);
    worker.max_materialization_retained_bytes = 0;
    let error = worker
        .write_output(vector, &selection, &mut output(), 0)
        .unwrap_err();
    assert!(matches!(
        error.local_error(),
        Some(LocalError::ResourceLimit(_))
    ));
    assert!(array_bytes::<Datum>(usize::MAX).is_err());
    worker.max_materialization_retained_bytes = usize::MAX;
    let mut result = output();
    worker
        .write_output(
            VectorValue::from_scalar(&ScalarValue::Int(Some(3)), 1),
            &selection,
            &mut result,
            0,
        )
        .unwrap();
    assert_eq!(values(&result), vec![Datum::Int(3)]);
    assert!(worker
        .write_output(
            VectorValue::from_scalar(&ScalarValue::Int(Some(3)), 1),
            &Vec::new(),
            &mut output(),
            0,
        )
        .is_err());
}

#[test]
fn integer_comparison_family_matches_signed_unsigned_order_and_nulls() {
    let raw = [
        (Some(0), Some(0)),
        (Some(1), Some(2)),
        (Some(2), Some(1)),
        (Some(-1), Some(0)),
        (Some(i64::MIN), Some(i64::MAX)),
        (Some(i64::MAX), Some(i64::MIN)),
        (Some(-1), Some(i64::MAX)),
        (Some(i64::MAX), Some(-1)),
        (None, Some(1)),
        (Some(1), None),
        (None, None),
        (Some(-1), Some(-1)),
    ];
    let selection: Vec<usize> = (0..2050).map(|i| raw.len() - 1 - i % raw.len()).collect();
    for left_unsigned in [false, true] {
        for right_unsigned in [false, true] {
            let schema = vec![
                ty().with_unsigned(left_unsigned),
                ty().with_unsigned(right_unsigned),
            ];
            let datum = |value: Option<i64>, unsigned| match value {
                None => Datum::Null,
                Some(value) if unsigned => Datum::UInt(value as u64),
                Some(value) => Datum::Int(value),
            };
            let rows: Vec<_> = raw
                .iter()
                .map(|&(left, right)| {
                    vec![datum(left, left_unsigned), datum(right, right_unsigned)]
                })
                .collect();
            let number = |value: i64, unsigned| {
                if unsigned {
                    (value as u64) as i128
                } else {
                    value as i128
                }
            };
            for right_literal in [false, true] {
                for name in ["lt", "le", "gt", "ge", "eq", "ne", "nulleq"] {
                    let suite = suite(call(
                        name,
                        ty(),
                        vec![
                            typed_column(0, &schema[0]),
                            if right_literal {
                                literal(datum(Some(-1), right_unsigned), schema[1].clone())
                            } else {
                                typed_column(1, &schema[1])
                            },
                        ],
                    ));
                    let expected: Vec<_> = selection
                        .iter()
                        .map(|&row| {
                            let (left, mut right) = raw[row];
                            if right_literal {
                                right = Some(-1);
                            }
                            match (left, right) {
                                (Some(left), Some(right)) => {
                                    let order = number(left, left_unsigned)
                                        .cmp(&number(right, right_unsigned));
                                    let result = match name {
                                        "lt" => order.is_lt(),
                                        "le" => order.is_le(),
                                        "gt" => order.is_gt(),
                                        "ge" => order.is_ge(),
                                        "eq" | "nulleq" => order.is_eq(),
                                        "ne" => !order.is_eq(),
                                        _ => unreachable!(),
                                    };
                                    Datum::Int(i64::from(result))
                                }
                                _ if name == "nulleq" => {
                                    Datum::Int(i64::from(left.is_none() && right.is_none()))
                                }
                                _ => Datum::Null,
                            }
                        })
                        .collect();
                    let mut input = chunk(&schema, &rows);
                    input.set_sel(Some(selection.clone()));
                    let mut actual = output();
                    suite
                        .run_with_tikv_batch(&Context::new(true), &mut input, &mut actual, &schema)
                        .unwrap();
                    assert_eq!(
                        values(&actual),
                        expected,
                        "{name} unsigned={left_unsigned}/{right_unsigned}"
                    );
                    let route = suite.batch_route_snapshot();
                    assert_eq!(
                        (route.prepared, route.fallback, route.refusals),
                        (1, 0, 0),
                        "{name} unsigned={left_unsigned}/{right_unsigned}: {route:?}"
                    );
                }
            }
        }
    }
}

#[test]
fn real_decimal_comparisons_preserve_order_nulls_and_selection() {
    use std::cmp::Ordering::{Equal, Greater, Less};
    let cases = [
        (
            "-9999999999999999999999999999.125",
            "-9999999999999999999999999999.124",
            Less,
        ),
        ("-1.25", "-2.5", Greater),
        ("0", "-0.00", Equal),
        ("1.2500", "1.25", Equal),
        ("0.000000000000000000000000000001", "0", Greater),
        (
            "9999999999999999999999999999.124",
            "9999999999999999999999999999.125",
            Less,
        ),
    ];
    for decimal in [false, true] {
        let field = if decimal {
            FieldType::new(FieldTypeCode::NewDecimal)
                .with_flen(65)
                .with_decimal(30)
        } else {
            real_ty()
        };
        let convert = |value: &str| {
            if decimal {
                Datum::Decimal(Decimal::from_literal(value))
            } else {
                Datum::Real(value.parse().unwrap())
            }
        };
        let mut rows: Vec<_> = cases
            .iter()
            .map(|(l, r, _)| vec![convert(l), convert(r)])
            .collect();
        let mut orders: Vec<_> = cases.iter().map(|(_, _, o)| Some(*o)).collect();
        // The very large and very small decimal cases have distinct f64 rounding.
        if !decimal {
            orders[0] = Some(Equal);
            orders[5] = Some(Equal);
            rows.extend([
                vec![Datum::Real(f64::NEG_INFINITY), Datum::Real(-f64::MAX)],
                vec![Datum::Real(f64::INFINITY), Datum::Real(f64::MAX)],
                vec![Datum::Real(f64::INFINITY), Datum::Real(f64::INFINITY)],
            ]);
            orders.extend([Some(Less), Some(Greater), Some(Equal)]);
        }
        let nonnull = rows.len();
        rows.extend([
            vec![Datum::Null, convert("1")],
            vec![convert("1"), Datum::Null],
            vec![Datum::Null, Datum::Null],
        ]);
        orders.extend([None, None, None]);
        let selection: Vec<usize> = (0..2050).map(|i| rows.len() - 1 - i % rows.len()).collect();
        let schema = vec![field.clone(), field.clone()];
        for name in ["lt", "le", "gt", "ge", "eq", "ne", "nulleq"] {
            let suite = suite(call(
                name,
                ty(),
                vec![typed_column(0, &field), typed_column(1, &field)],
            ));
            let expected: Vec<_> = selection
                .iter()
                .map(|&i| match orders[i] {
                    Some(order) => Datum::Int(i64::from(match name {
                        "lt" => order.is_lt(),
                        "le" => order.is_le(),
                        "gt" => order.is_gt(),
                        "ge" => order.is_ge(),
                        "eq" | "nulleq" => order.is_eq(),
                        "ne" => !order.is_eq(),
                        _ => unreachable!(),
                    })),
                    None if name == "nulleq" => Datum::Int(i64::from(i == nonnull + 2)),
                    None => Datum::Null,
                })
                .collect();
            let mut input = chunk(&schema, &rows);
            input.set_sel(Some(selection.clone()));
            let mut actual = output();
            suite
                .run_with_tikv_batch(&Context::new(true), &mut input, &mut actual, &schema)
                .unwrap();
            assert_eq!(values(&actual), expected, "{name} decimal={decimal}");
            let route = suite.batch_route_snapshot();
            assert_eq!(
                (route.prepared, route.fallback, route.refusals),
                (1, 0, 0),
                "{name} decimal={decimal}: {route:?}"
            );
        }
    }
}

#[test]
fn narrow_integer_fields_use_the_shared_int_carrier() {
    for (code, min, max, umax) in [
        (FieldTypeCode::Tiny, -128_i64, 127_i64, 255_u64),
        (FieldTypeCode::Short, -32768, 32767, 65535),
        (FieldTypeCode::Int24, -8388608, 8388607, 16777215),
        (FieldTypeCode::Long, -2147483648, 2147483647, 4294967295),
    ] {
        for left_unsigned in [false, true] {
            for right_unsigned in [false, true] {
                let schema = vec![
                    FieldType::new(code).with_unsigned(left_unsigned),
                    FieldType::new(code).with_unsigned(right_unsigned),
                ];
                let left = if left_unsigned {
                    vec![
                        Datum::UInt(umax),
                        Datum::UInt(0),
                        Datum::Null,
                        Datum::UInt(1),
                    ]
                } else {
                    vec![
                        Datum::Int(min),
                        Datum::Int(max),
                        Datum::Null,
                        Datum::Int(-1),
                    ]
                };
                let right = if right_unsigned {
                    vec![
                        Datum::UInt(0),
                        Datum::UInt(umax),
                        Datum::UInt(1),
                        Datum::Null,
                    ]
                } else {
                    vec![Datum::Int(max), Datum::Int(min), Datum::Int(1), Datum::Null]
                };
                let rows: Vec<_> = left
                    .iter()
                    .cloned()
                    .zip(right.iter().cloned())
                    .map(|(l, r)| vec![l, r])
                    .collect();
                let selection: Vec<_> = (0..2050).map(|i| 3 - i % 4).collect();
                let numeric = |v: &Datum| match v {
                    Datum::Int(v) => Some(*v as i128),
                    Datum::UInt(v) => Some(*v as i128),
                    Datum::Null => None,
                    _ => unreachable!(),
                };
                for name in ["lt", "le", "gt", "ge", "eq", "ne", "nulleq"] {
                    let suite = suite(call(
                        name,
                        ty(),
                        vec![typed_column(0, &schema[0]), typed_column(1, &schema[1])],
                    ));
                    let expected: Vec<_> = selection
                        .iter()
                        .map(|&i| match (numeric(&left[i]), numeric(&right[i])) {
                            (Some(l), Some(r)) => Datum::Int(i64::from(match name {
                                "lt" => l < r,
                                "le" => l <= r,
                                "gt" => l > r,
                                "ge" => l >= r,
                                "eq" | "nulleq" => l == r,
                                "ne" => l != r,
                                _ => unreachable!(),
                            })),
                            (l, r) if name == "nulleq" => {
                                Datum::Int(i64::from(l.is_none() && r.is_none()))
                            }
                            _ => Datum::Null,
                        })
                        .collect();
                    let mut input = chunk(&schema, &rows);
                    input.set_sel(Some(selection.clone()));
                    let mut actual = output();
                    suite
                        .run_with_tikv_batch(&Context::new(true), &mut input, &mut actual, &schema)
                        .unwrap();
                    assert_eq!(
                        values(&actual),
                        expected,
                        "{code:?} {name} unsigned={left_unsigned}/{right_unsigned}"
                    );
                    let route = suite.batch_route_snapshot();
                    assert_eq!(
                        (route.prepared, route.fallback, route.refusals),
                        (1, 0, 0),
                        "{route:?}"
                    );
                }
            }
        }
        let schema = vec![FieldType::new(code)];
        let suite = suite(plus(typed_column(0, &schema[0]), int(1)));
        let mut input = chunk(
            &schema,
            &[
                vec![Datum::Int(max)],
                vec![Datum::Int(min)],
                vec![Datum::Null],
            ],
        );
        let mut actual = output();
        suite
            .run_with_tikv_batch(&Context::new(true), &mut input, &mut actual, &schema)
            .unwrap();
        assert_eq!(
            values(&actual),
            vec![Datum::Int(max + 1), Datum::Int(min + 1), Datum::Null]
        );
        assert_eq!(suite.batch_route_snapshot().prepared, 1);
    }
}

#[test]
fn bitwise_family_keeps_unsigned_results_shift_limits_and_window_selection() {
    // Golden raw-bit answers include the sign bit, shift 63/64 and negative counts.
    let cases = [
        (
            Some(-1),
            Some(1),
            Some([1, u64::MAX, u64::MAX - 1, u64::MAX - 1, i64::MAX as u64]),
        ),
        (
            Some(i64::MIN),
            Some(63),
            Some([0, (1u64 << 63) + 63, (1u64 << 63) + 63, 0, 1]),
        ),
        (Some(1), Some(63), Some([1, 63, 62, 1u64 << 63, 0])),
        (
            Some(-1),
            Some(64),
            Some([64, u64::MAX, u64::MAX - 64, 0, 0]),
        ),
        (
            Some(i64::MIN),
            Some(-1),
            Some([1u64 << 63, u64::MAX, i64::MAX as u64, 0, 0]),
        ),
        (Some(0), Some(65), Some([0, 65, 65, 0, 0])),
        (None, Some(1), None),
        (Some(1), None, None),
        (None, None, None),
    ];
    let selection: Vec<_> = (0..2050)
        .map(|i| cases.len() - 1 - i % cases.len())
        .collect();
    for left_unsigned in [false, true] {
        for right_unsigned in [false, true] {
            let schema = vec![
                ty().with_unsigned(left_unsigned),
                ty().with_unsigned(right_unsigned),
            ];
            let datum = |raw: Option<i64>, unsigned| match raw {
                None => Datum::Null,
                Some(value) if unsigned => Datum::UInt(value as u64),
                Some(value) => Datum::Int(value),
            };
            let rows: Vec<_> = cases
                .iter()
                .map(|&(l, r, _)| vec![datum(l, left_unsigned), datum(r, right_unsigned)])
                .collect();
            for (index, name) in ["bitand", "bitor", "bitxor", "leftshift", "rightshift"]
                .into_iter()
                .enumerate()
            {
                let suite = suite(call(
                    name,
                    ty().with_unsigned(true),
                    vec![typed_column(0, &schema[0]), typed_column(1, &schema[1])],
                ));
                let mut input = chunk(&schema, &rows);
                input.set_sel(Some(selection.clone()));
                let mut actual = output_for(&ty().with_unsigned(true));
                suite
                    .run_with_tikv_batch(&Context::new(true), &mut input, &mut actual, &schema)
                    .unwrap();
                let expected: Vec<_> = selection
                    .iter()
                    .map(|&row| cases[row].2.map_or(Datum::Null, |v| Datum::UInt(v[index])))
                    .collect();
                assert_eq!(
                    values_for(&actual, &ty().with_unsigned(true)),
                    expected,
                    "{name} flags={left_unsigned}/{right_unsigned}"
                );
                let route = suite.batch_route_snapshot();
                assert_eq!(
                    (route.prepared, route.fallback, route.refusals),
                    (1, 0, 0),
                    "{name}: {route:?}"
                );
            }
        }
    }
}

#[test]
fn is_null_batch_returns_nonnull_booleans_for_all_admitted_carriers() {
    let cases = vec![
        (ty(), Datum::Int(-1)),
        (ty().with_unsigned(true), Datum::UInt(u64::MAX)),
        (real_ty(), Datum::Real(0.0)),
        (
            FieldType::new(FieldTypeCode::NewDecimal).with_decimal(0),
            Datum::Decimal(Decimal::from_literal("0")),
        ),
        (
            string_ty(),
            Datum::String(StringDatum::new(Vec::new(), Collation::Utf8Mb4Bin)),
        ),
        (
            FieldType::new(FieldTypeCode::Datetime),
            Datum::Time(Time::from_go_raw_like_go(0)),
        ),
        (
            FieldType::new(FieldTypeCode::Timestamp),
            Datum::Time(Time::from_go_raw_like_go(1)),
        ),
        (
            FieldType::new(FieldTypeCode::Duration).with_decimal(6),
            Datum::Duration(MySqlDuration::from_raw_parts(0, 6)),
        ),
        (
            FieldType::new(FieldTypeCode::VectorFloat32),
            Datum::VectorFloat32(VectorFloat32::must_create(vec![0.0])),
        ),
        // JSON's existing catalog inserts CastJsonAsString, outside this batch closure.
        (
            FieldType::new(FieldTypeCode::Json),
            Datum::Json(BinaryJSON::parse("null").unwrap()),
        ),
    ];
    let selection: Vec<_> = (0..2050).map(|i| (2050 - i) % 3).collect();
    for (field, value) in cases {
        for constant in [false, true] {
            let schema = vec![field.clone()];
            let rows = vec![vec![Datum::Null], vec![value.clone()], vec![Datum::Null]];
            let root = call(
                "isnull",
                ty(),
                vec![if constant {
                    literal(Datum::Null, field.clone())
                } else {
                    typed_column(0, &field)
                }],
            );
            let suite = suite(root);
            let mut input = chunk(&schema, &rows);
            input.set_sel(Some(selection.clone()));
            let mut actual = output();
            suite
                .run_with_tikv_batch(&Context::new(true), &mut input, &mut actual, &schema)
                .unwrap();
            let expected: Vec<_> = selection
                .iter()
                .map(|&row| Datum::Int(i64::from(constant || row != 1)))
                .collect();
            assert_eq!(
                values(&actual),
                expected,
                "{:?} constant={constant}",
                field.eval_type()
            );
            let route = suite.batch_route_snapshot();
            if field.eval_type() == NativeEvalType::Json {
                assert_eq!((route.prepared, route.fallback, route.refusals), (0, 1, 1));
            } else {
                assert_eq!(
                    (route.prepared, route.fallback, route.refusals),
                    (1, 0, 0),
                    "{:?}: {route:?}",
                    field.eval_type()
                );
            }
        }
    }
}

#[test]
fn inverse_trig_batch_matches_native_bits_and_out_of_domain_nulls() {
    let inputs = [
        None,
        Some(f64::NEG_INFINITY),
        Some(-1.5),
        Some(f64::from_bits((-1.0f64).to_bits() + 1)),
        Some(-1.0),
        Some(-0.5),
        Some(-0.0),
        Some(0.0),
        Some(0.5),
        Some(1.0),
        Some(f64::from_bits(1.0f64.to_bits() + 1)),
        Some(1.5),
        Some(f64::INFINITY),
    ];
    let field = real_ty();
    let schema = [field.clone()];
    let rows: Vec<_> = inputs
        .iter()
        .map(|x| vec![x.map_or(Datum::Null, Datum::Real)])
        .collect();
    let selection: Vec<_> = (0..2050)
        .map(|i| inputs.len() - 1 - i % inputs.len())
        .collect();
    for name in ["asin", "acos"] {
        let suite = suite(call(name, field.clone(), vec![typed_column(0, &field)]));
        let mut native_input = chunk(&schema, &rows);
        native_input.set_sel(Some(selection.clone()));
        let mut expected = output_for(&field);
        suite
            .run(&Context::new(true), &mut native_input, &mut expected)
            .unwrap();
        let mut input = chunk(&schema, &rows);
        input.set_sel(Some(selection.clone()));
        let mut actual = output_for(&field);
        suite
            .run_with_tikv_batch(&Context::new(true), &mut input, &mut actual, &schema)
            .unwrap();
        for (row, &source) in selection.iter().enumerate() {
            let lhs = actual.get_row(row);
            let rhs = expected.get_row(row);
            let null = inputs[source].is_none_or(|x| x.abs() > 1.0);
            assert_eq!(lhs.is_null(0), null, "{name} input={:?}", inputs[source]);
            assert_eq!(lhs.is_null(0), rhs.is_null(0));
            if !null {
                assert_eq!(
                    lhs.get_float64(0).to_bits(),
                    rhs.get_float64(0).to_bits(),
                    "{name} input={:?}",
                    inputs[source]
                );
                if name == "asin" && inputs[source] == Some(0.0) {
                    assert_eq!(
                        lhs.get_float64(0).to_bits(),
                        inputs[source].unwrap().to_bits()
                    );
                }
                if name == "acos" && inputs[source] == Some(1.0) {
                    assert_eq!(lhs.get_float64(0).to_bits(), 0);
                }
            }
        }
        let route = suite.batch_route_snapshot();
        assert_eq!(
            (route.prepared, route.fallback, route.refusals),
            (1, 0, 0),
            "{name}: {route:?}"
        );
    }
}

#[test]
fn integer_not_batch_preserves_unsigned_truth_and_null() {
    let inputs = [
        None,
        Some(0),
        Some(1),
        Some(-1),
        Some(i64::MIN),
        Some(i64::MAX),
    ];
    let selection: Vec<_> = (0..2050)
        .map(|i| inputs.len() - 1 - i % inputs.len())
        .collect();
    for unsigned in [false, true] {
        let field = ty().with_unsigned(unsigned);
        let schema = [field.clone()];
        let rows: Vec<_> = inputs
            .iter()
            .map(|x| {
                vec![match x {
                    None => Datum::Null,
                    Some(v) if unsigned => Datum::UInt(*v as u64),
                    Some(v) => Datum::Int(*v),
                }]
            })
            .collect();
        let suite = suite(call("not", ty(), vec![typed_column(0, &field)]));
        let mut input = chunk(&schema, &rows);
        input.set_sel(Some(selection.clone()));
        let mut actual = output();
        suite
            .run_with_tikv_batch(&Context::new(true), &mut input, &mut actual, &schema)
            .unwrap();
        let expected: Vec<_> = selection
            .iter()
            .map(|&i| inputs[i].map_or(Datum::Null, |v| Datum::Int(i64::from(v == 0))))
            .collect();
        assert_eq!(values(&actual), expected);
        let route = suite.batch_route_snapshot();
        assert_eq!((route.prepared, route.fallback, route.refusals), (1, 0, 0));
    }
}
