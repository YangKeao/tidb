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
            SourceKind::Call {
                target: CallTarget::Kernel(function),
                ..
            } => Some(*function),
            SourceKind::Call {
                target: CallTarget::Host(_),
                ..
            }
            | SourceKind::DecimalFastInput(_)
            | SourceKind::Constant { .. }
            | SourceKind::NativeConstant { .. }
            | SourceKind::Column { .. } => None,
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
                target: CallTarget::Kernel(FunctionRef::TiPb(
                    tipb::ScalarFuncSig::CharLengthUtf8 | tipb::ScalarFuncSig::LowerUtf8
                )),
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
            target: CallTarget::Kernel(FunctionRef::TiPb(tipb::ScalarFuncSig::Substring3ArgsUtf8)),
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
            target: CallTarget::Kernel(FunctionRef::TiPb(tipb::ScalarFuncSig::PlusReal)),
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
            target: CallTarget::Kernel(FunctionRef::TiPb(tipb::ScalarFuncSig::PlusDecimal)),
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

fn native_carrier_cases() -> Vec<(FieldType, Datum)> {
    let mut decimal = FieldType::new(FieldTypeCode::NewDecimal);
    decimal.set_flen(20);
    decimal.set_decimal(4);
    let mut duration = FieldType::new(FieldTypeCode::Duration);
    duration.set_decimal(6);
    vec![
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
    ]
}

#[test]
fn any_value_batch_round_trips_every_native_eval_type() {
    let cases = native_carrier_cases();

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
        call("if", ty(), vec![int(1), int(2)]), // malformed even in a dead branch
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
    let root = call(
        "if",
        ty(),
        vec![
            int(1),
            plus(int(1), int(2)),
            call("cast", ty(), vec![int(0)]),
        ],
    );
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
        // Native IS NULL now consumes JSON directly, without a text conversion.
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
            assert_eq!(
                (route.prepared, route.fallback, route.refusals),
                (1, 0, 0),
                "{:?}: {route:?}",
                field.eval_type()
            );
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

#[test]
fn integer_identities_preserve_first_argument_flags_and_all_bits() {
    let raw = [
        None,
        Some(0),
        Some(1),
        Some(-1),
        Some(i64::MIN),
        Some(i64::MAX),
        Some(i64::MAX - 1),
        Some((1i64 << 53) + 1),
    ];
    let selection: Vec<_> = (0..2050).map(|i| raw.len() - 1 - i % raw.len()).collect();
    for unsigned in [false, true] {
        let field = ty().with_unsigned(unsigned);
        let schema = [field.clone()];
        let datum = |x: Option<i64>| match x {
            None => Datum::Null,
            Some(v) if unsigned => Datum::UInt(v as u64),
            Some(v) => Datum::Int(v),
        };
        let rows: Vec<_> = raw.iter().map(|&x| vec![datum(x)]).collect();
        for name in ["round", "any_value", "round_any_value"] {
            for constant in [false, true] {
                let arg = if constant {
                    literal(datum(Some((1i64 << 53) + 1)), field.clone())
                } else {
                    typed_column(0, &field)
                };
                let root = if name == "round_any_value" {
                    call(
                        "round",
                        field.clone(),
                        vec![call("any_value", field.clone(), vec![arg])],
                    )
                } else {
                    call(name, field.clone(), vec![arg])
                };
                let suite = suite(root);
                let mut input = chunk(&schema, &rows);
                input.set_sel(Some(selection.clone()));
                let mut actual = output_for(&field);
                suite
                    .run_with_tikv_batch(&Context::new(true), &mut input, &mut actual, &schema)
                    .unwrap();
                let expected: Vec<_> = selection
                    .iter()
                    .map(|&row| {
                        if constant {
                            datum(Some((1i64 << 53) + 1))
                        } else {
                            rows[row][0].clone()
                        }
                    })
                    .collect();
                assert_eq!(
                    values_for(&actual, &field),
                    expected,
                    "{name} unsigned={unsigned} constant={constant}"
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
    // The two-argument integer ROUND is a different f64-based signature.
    let suite = suite(call("round", ty(), vec![column(0), int(0)]));
    let schema = [ty()];
    let mut input = chunk(&schema, &[vec![Datum::Int(i64::MAX - 1)]]);
    let mut actual = output();
    suite
        .run_with_tikv_batch(&Context::new(true), &mut input, &mut actual, &schema)
        .unwrap();
    assert_eq!(values(&actual), vec![Datum::Int(i64::MAX)]);
    let route = suite.batch_route_snapshot();
    assert_eq!((route.prepared, route.fallback, route.refusals), (0, 1, 1));
}

#[test]
fn pure_niladic_pi_uses_explicit_rows_without_admitting_stateful_calls() {
    let field = real_ty();
    let pi = || call("pi", field.clone(), vec![]);
    for selected in [
        None,
        Some(vec![]),
        Some((0..2050).map(|i| 16 - i % 17).collect()),
    ] {
        let mut input = Chunk::new_with_capacity(&[], 2050);
        input.set_num_virtual_rows(2050);
        input.set_sel(selected.clone());
        let expected_rows = selected.as_ref().map_or(2050, Vec::len);
        for nested in [false, true] {
            let root = if nested {
                call(
                    "plus",
                    field.clone(),
                    vec![pi(), literal(Datum::Real(1.25), field.clone())],
                )
            } else {
                pi()
            };
            let suite = suite(root);
            let mut actual = output_for(&field);
            suite
                .run_with_tikv_batch(&Context::new(true), &mut input, &mut actual, &[])
                .unwrap();
            assert_eq!(actual.num_rows(), expected_rows);
            let expected: f64 = if nested {
                3.141592653589793 + 1.25
            } else {
                3.141592653589793
            };
            for row in 0..actual.num_rows() {
                assert!(!actual.get_row(row).is_null(0));
                assert_eq!(
                    actual.get_row(row).get_float64(0).to_bits(),
                    expected.to_bits()
                );
            }
            let route = suite.batch_route_snapshot();
            assert_eq!((route.prepared, route.fallback, route.refusals), (1, 0, 0));
        }
    }
    // An empty argument list is only arity; the registry still decides purity.
    let random = suite(call("rand", field.clone(), vec![]));
    assert!(PreparedBatch::compile(
        &random,
        &[],
        true,
        1,
        limits(),
        ExecutionLimits::default(),
        usize::MAX
    )
    .is_err());
    let pure = suite(pi());
    let mut source_limits = limits();
    source_limits.tree.max_nodes = 0;
    assert!(matches!(
        PreparedBatch::compile(
            &pure,
            &[],
            true,
            1,
            source_limits,
            ExecutionLimits::default(),
            usize::MAX
        ),
        Err(BatchFailure {
            kind: FailureKind::Local(LocalError::ResourceLimit(_))
        })
    ));
}

#[test]
fn preparation_resource_failure_is_cached_without_entering_another_evaluator() {
    let suite = suite(plus(column(0), int(1)));
    let oversized = ty().with_elems(["x".repeat(1 << 20)]);
    let ctx = Context::new(true);
    let mut input = chunk(&[ty()], &[vec![Datum::Int(7)]]);
    let mut out = output();
    let mut first_error = None;
    for _ in 0..2 {
        let error = suite
            .run_with_tikv_batch(&ctx, &mut input, &mut out, &[oversized.clone()])
            .expect_err("a resource gate must not retry through the generic evaluator");
        let EvaluatorError::Eval(EvalError::ExpressionRuntimeFailure(failure)) = &error else {
            panic!("lost structured preparation failure: {error:?}");
        };
        assert_eq!(
            failure.class(),
            crate::ExpressionRuntimeFailureClass::ResourceLimit
        );
        assert_eq!(
            failure.phase(),
            Some(crate::ExpressionRuntimeFailurePhase::Prepare)
        );
        if let Some(first) = &first_error {
            assert_eq!(first, &error, "the same failure ownership must be retained");
        }
        first_error = Some(error);
        assert_eq!(ctx.calls.get(), 0, "no value/eligibility phase may run");
        assert_eq!(input.num_rows(), 1);
        assert_eq!(input.get_row(0).get_datum(0, &ty()), Datum::Int(7));
        assert_eq!(out.num_rows(), 0);
        let route = suite.batch_route_snapshot();
        assert_eq!((route.prepared, route.fallback, route.refusals), (0, 0, 1));
        assert_eq!(
            route.refusal,
            Some(crate::evaluator::BatchRefusal::Resource)
        );
    }
}

#[test]
fn complete_projection_keeps_multiple_outputs_and_selected_columns_in_shared_execution() {
    let schema = [ty(), string_ty()];
    let rows = vec![
        vec![
            Datum::Int(2),
            Datum::String(StringDatum::new(b"Ab".to_vec(), Collation::Utf8Mb4Bin)),
        ],
        vec![Datum::Null, Datum::Null],
        vec![
            Datum::Int(7),
            Datum::String(StringDatum::new(b"XYZ".to_vec(), Collation::Utf8Mb4Bin)),
        ],
    ];
    let out_schema = [ty(), ty(), string_ty(), ty(), ty(), ty()];
    for enabled in [true, false] {
        for selection in [
            None,
            Some(vec![]),
            Some((0..2050).map(|i| (2050 - i) % 3).collect()),
        ] {
            let suite = EvaluatorSuite::new(
                vec![
                    column(0),
                    plus(column(0), int(1)),
                    call("lower", string_ty(), vec![string_column(1)]),
                    int(42),
                    call("char_length", ty(), vec![string_column(1)]),
                    column(0),
                ],
                false,
            );
            let ctx = Context::new(enabled);
            let reference = EvaluatorSuite::new(suite.program.calculated.clone(), false);
            let reference_ctx = Context::new(enabled);
            let reference_schema = [ty(), string_ty(), ty(), ty()];
            for _ in 0..2 {
                let mut native_input = chunk(&schema, &rows);
                native_input.set_sel(selection.clone());
                let mut native_output = Chunk::new_with_capacity(&reference_schema, 0);
                reference
                    .run(&reference_ctx, &mut native_input, &mut native_output)
                    .unwrap();
                let mut input = chunk(&schema, &rows);
                input.set_sel(selection.clone());
                let mut out = Chunk::new_with_capacity(&out_schema, 0);
                suite
                    .run_with_tikv_batch(&ctx, &mut input, &mut out, &schema)
                    .unwrap();
                let selected = selection.clone().unwrap_or_else(|| vec![0, 1, 2]);
                assert_eq!(out.num_rows(), selected.len());
                for (logical, physical) in selected.into_iter().enumerate() {
                    let (number, lower, length) = match physical {
                        0 => (Datum::Int(3), Some(b"ab".to_vec()), Datum::Int(2)),
                        1 => (Datum::Null, None, Datum::Null),
                        2 => (Datum::Int(8), Some(b"xyz".to_vec()), Datum::Int(3)),
                        _ => unreachable!(),
                    };
                    let row = out.get_row(logical);
                    assert_eq!(row.get_datum(0, &ty()), rows[physical][0]);
                    assert_eq!(row.get_datum(1, &ty()), number);
                    assert_eq!(
                        row.get_datum(2, &string_ty()),
                        lower.map_or(Datum::Null, |s| Datum::String(StringDatum::new(
                            s,
                            Collation::Utf8Mb4Bin
                        )))
                    );
                    assert_eq!(row.get_datum(3, &ty()), Datum::Int(42));
                    assert_eq!(row.get_datum(4, &ty()), length);
                    assert_eq!(row.get_datum(5, &ty()), rows[physical][0]);
                }
                if selection.is_none() {
                    assert!(out.columns_share_identity(0, &out, 5));
                } else {
                    assert_eq!(
                        input.physical_rows(),
                        3,
                        "selected input owners stay intact"
                    );
                }
            }
            assert_eq!(
                ctx.calls.get(),
                reference_ctx.calls.get(),
                "preserve native's mode observation for each calculated output"
            );
            let route = suite.batch_route_snapshot();
            assert_eq!((route.prepared, route.fallback, route.refusals), (2, 0, 0));
        }
    }
}

#[test]
fn direct_native_columns_preserve_storage_without_wire_conversion() {
    use tidb_datatype::{BinaryLiteral, MysqlEnum, MysqlSet};
    let cases = [
        (FieldType::new(FieldTypeCode::Float), Datum::Float32(-0.0)),
        (real_ty(), Datum::Real(f64::from_bits(0x7ff8000000000042))),
        (FieldType::new(FieldTypeCode::Year), Datum::Int(0)),
        (
            FieldType::new(FieldTypeCode::Bit).with_flen(8),
            Datum::Bit(BinaryLiteral::from(vec![0xa5])),
        ),
        (
            FieldType::new(FieldTypeCode::Enum).with_elems(["a", "b"]),
            Datum::Enum(MysqlEnum::new("b", 2), Collation::Binary),
        ),
        (
            FieldType::new(FieldTypeCode::Set).with_elems(["a", "b"]),
            Datum::Set(MysqlSet::new("a,b", 3), Collation::Binary),
        ),
    ];
    for (field, value) in cases {
        for selection in [None, Some(vec![]), Some((0..2050).map(|i| i % 2).collect())] {
            for enabled in [true, false] {
                for avoid_column_evaluator in [false, true] {
                    let schema = [field.clone()];
                    let mut input = chunk(&schema, &[vec![value.clone()], vec![Datum::Null]]);
                    input.set_sel(selection.clone());
                    // Native reference publishes physical rows; compare each selected
                    // occurrence explicitly because its direct-swap path rejects Sel.
                    let mut reference_input =
                        chunk(&schema, &[vec![value.clone()], vec![Datum::Null]]);
                    let make_suite = || {
                        EvaluatorSuite::new(
                            vec![typed_column(0, &field), typed_column(0, &field)],
                            avoid_column_evaluator,
                        )
                    };
                    let reference = make_suite();
                    let prepared = make_suite();
                    let output_schema = [field.clone(), field.clone()];
                    let mut expected = Chunk::new_with_capacity(&output_schema, 0);
                    let mut actual = Chunk::new_with_capacity(&output_schema, 0);
                    reference
                        .run(&Context::new(enabled), &mut reference_input, &mut expected)
                        .unwrap();
                    prepared
                        .run_with_tikv_batch(
                            &Context::new(enabled),
                            &mut input,
                            &mut actual,
                            &schema,
                        )
                        .unwrap();
                    let selected = selection.clone().unwrap_or_else(|| vec![0, 1]);
                    assert_eq!(actual.num_rows(), selected.len());
                    for (row, physical) in selected.into_iter().enumerate() {
                        for col in 0..2 {
                            assert_eq!(
                                actual.get_row(row).is_null(col),
                                expected.get_row(physical).is_null(col)
                            );
                            // Raw storage equality covers NaN payloads and signed zero.
                            assert_eq!(
                                actual.column(col).get_raw(row).as_ref(),
                                expected.column(col).get_raw(physical).as_ref()
                            );
                        }
                    }
                    if selection.is_none() && !avoid_column_evaluator {
                        assert!(actual.columns_share_identity(0, &actual, 1));
                    } else {
                        assert_eq!(input.physical_rows(), 2);
                    }
                    let route = prepared.batch_route_snapshot();
                    assert_eq!(
                        (route.prepared, route.fallback, route.refusals),
                        (1, 0, 0),
                        "{field:?}"
                    );
                }
            }
        }
    }
}

#[test]
fn projection_late_error_does_not_move_direct_column_owners_or_retry() {
    let suite = EvaluatorSuite::new(vec![column(0), plus(int(i64::MAX), int(1))], false);
    let mut input = chunk(&[ty()], &[vec![Datum::Int(7)]]);
    let mut out = Chunk::new_with_capacity(&[ty(), ty()], 0);
    assert!(suite
        .run_with_tikv_batch(&Context::new(true), &mut input, &mut out, &[ty()])
        .is_err());
    assert_eq!(input.get_row(0).get_datum(0, &ty()), Datum::Int(7));
    assert_eq!(input.column(0).rows(), 1);
    assert_eq!(out.column(0).rows(), 0);
    let route = suite.batch_route_snapshot();
    assert_eq!((route.prepared, route.fallback, route.refusals), (1, 0, 0));
}

#[test]
fn projection_validates_later_destination_before_any_kernel_runs() {
    let suite = EvaluatorSuite::new(
        vec![
            plus(column(0), int(1)),
            call("lower", string_ty(), vec![string_column(1)]),
        ],
        false,
    );
    let schema = [ty(), string_ty()];
    let mut input = chunk(
        &schema,
        &[vec![
            Datum::Int(7),
            Datum::String(StringDatum::new(b"A".to_vec(), Collation::Utf8Mb4Bin)),
        ]],
    );
    let mut out = Chunk::new_with_capacity(&[ty(), ty()], 0); // wrong second layout
    let ctx = Context::new(true);
    assert!(suite
        .run_with_tikv_batch(&ctx, &mut input, &mut out, &schema)
        .is_err());
    assert_eq!(ctx.calls.get(), 0);
    assert_eq!(out.column(0).rows(), 0);
    assert_eq!(input.column(0).rows(), 1);
    assert_eq!(suite.batch_route_snapshot().fallback, 0);
}

#[test]
fn projection_leaf_roots_and_direct_only_selection_need_no_scalar_interpreter() {
    for avoid_column_evaluator in [false, true] {
        let suite = EvaluatorSuite::new(vec![column(0), column(0)], avoid_column_evaluator);
        let schema = [ty()];
        let mut input = chunk(&schema, &[vec![Datum::Int(7)], vec![Datum::Null]]);
        input.set_sel(Some(vec![1, 0, 1]));
        let mut out = Chunk::new_with_capacity(&[ty(), ty()], 0);
        suite
            .run_with_tikv_batch(&Context::new(true), &mut input, &mut out, &schema)
            .unwrap();
        for (row, expected) in [Datum::Null, Datum::Int(7), Datum::Null]
            .into_iter()
            .enumerate()
        {
            assert_eq!(out.get_row(row).get_datum(0, &ty()), expected);
            assert_eq!(out.get_row(row).get_datum(1, &ty()), expected);
        }
        let route = suite.batch_route_snapshot();
        assert_eq!((route.prepared, route.fallback, route.refusals), (1, 0, 0));
    }
}

#[test]
fn complete_projection_source_budget_is_shared_across_outputs() {
    let suite = EvaluatorSuite::new(vec![int(1), int(2)], false);
    let mut limits = limits();
    limits.tree.max_nodes = 1;
    let error = PreparedBatchSuite::compile(
        &suite,
        &[],
        true,
        0,
        limits,
        ExecutionLimits::default(),
        8192,
    )
    .err()
    .expect("two roots cannot each consume a one-node suite budget");
    assert!(matches!(
        error.local_error(),
        Some(LocalError::ResourceLimit(_))
    ));
}

#[test]
fn native_strcmp_batch_keeps_sql_collation_and_nested_call_identities() {
    for (collation, first, padded) in [
        (Collation::Binary, -1, 1),
        (Collation::Utf8Mb4Bin, -1, 0),
        (Collation::Utf8Mb4GeneralCi, 0, 0),
        (Collation::Utf8Mb4UnicodeCi, 0, 0),
    ] {
        let string = string_ty().with_collation(collation);
        let result = ty().with_collation(collation);
        let make = |bytes: &[u8]| Datum::String(StringDatum::new(bytes.to_vec(), collation));
        let rows = vec![
            vec![make(b"A"), make(b"a")],
            vec![Datum::Null, make(b"b")],
            vec![make(b"b"), make(b"a")],
            vec![make(b"b "), make(b"b")],
        ];
        let cmp = call(
            "strcmp",
            result.clone(),
            vec![typed_column(0, &string), typed_column(1, &string)],
        );
        let nested = plus(cmp.clone(), cmp.clone());
        let schema = [string.clone(), string];
        let expected = [Some(first), None, Some(1), Some(padded)];
        let ctx = Context::new(true);
        // Keep literal SQL expectations and compare the existing scalar adapter,
        // a second producer of the same shared native kernel.
        let input = chunk(&schema, &rows);
        for (index, value) in expected.iter().enumerate() {
            assert_eq!(
                cmp.eval(&ctx, input.get_row(index)).unwrap(),
                value.map_or(Datum::Null, Datum::Int)
            );
        }
        for selection in [
            Some(vec![]),
            None,
            Some((0..2050).map(|i| (2050 - i) % 4).collect()),
        ] {
            let suite = EvaluatorSuite::new(vec![cmp.clone(), nested.clone()], false);
            let mut input = chunk(&schema, &rows);
            input.set_sel(selection.clone());
            let mut output = Chunk::new_with_capacity(&[result.clone(), ty()], 0);
            suite
                .run_with_tikv_batch(&ctx, &mut input, &mut output, &schema)
                .unwrap();
            let selected = selection.unwrap_or_else(|| (0..4).collect());
            assert_eq!(output.num_rows(), selected.len());
            for (row, physical) in selected.into_iter().enumerate() {
                assert_eq!(
                    output.get_row(row).get_datum(0, &result),
                    expected[physical].map_or(Datum::Null, Datum::Int)
                );
                assert_eq!(
                    output.get_row(row).get_datum(1, &ty()),
                    expected[physical].map_or(Datum::Null, |value| Datum::Int(value * 2))
                );
            }
            let route = suite.batch_route_snapshot();
            assert_eq!((route.prepared, route.fallback, route.refusals), (1, 0, 0));
            let lane = suite.batch_lane.borrow();
            let super::super::BatchLane::Prepared(plan) = &*lane else {
                panic!("STRCMP was not prepared")
            };
            let sites = plan.workers[1].source.facts.call_sites();
            assert_eq!(
                sites
                    .iter()
                    .map(OrdinaryCallSite::ordinal)
                    .collect::<Vec<_>>(),
                vec![0, 1, 5]
            );
            assert_eq!(
                sites
                    .iter()
                    .map(|site| site.source().node())
                    .collect::<Vec<_>>(),
                vec![0, 1, 4]
            );
        }
    }
}

#[test]
fn demanded_controls_preserve_skipped_errors_and_selected_occurrence_order() {
    let overflow = plus(int(i64::MAX), int(1));
    let ctx = Context::new(true);
    let roots = vec![
        call("if", ty(), vec![column(0), int(7), overflow.clone()]),
        call("ifnull", ty(), vec![column(1), overflow.clone()]),
        call("coalesce", ty(), vec![null(), column(1), overflow.clone()]),
        call(
            "case",
            ty(),
            vec![
                int(0),
                overflow.clone(),
                column(0),
                int(9),
                overflow.clone(),
            ],
        ),
        call("and", ty(), vec![int(0), overflow.clone()]),
        call("or", ty(), vec![int(1), overflow.clone()]),
        call(
            "coalesce",
            ty(),
            vec![call("nullif", ty(), vec![column(0), int(0)]), column(1)],
        ),
    ];
    let schema = [ty(), ty()];
    for (root, expected) in roots.into_iter().zip([7, 5, 5, 9, 0, 1, 1]) {
        let suite = EvaluatorSuite::new(vec![root], false);
        for selection in [Some(vec![]), None, Some((0..2050).map(|i| i % 2).collect())] {
            let mut input = chunk(&schema, &vec![vec![Datum::Int(1), Datum::Int(5)]; 2]);
            input.set_sel(selection.clone());
            let mut output = Chunk::new_with_capacity(&[ty()], 0);
            suite
                .run_with_tikv_batch(&ctx, &mut input, &mut output, &schema)
                .unwrap();
            assert_eq!(output.num_rows(), selection.as_ref().map_or(2, Vec::len));
            for row in 0..output.num_rows() {
                assert_eq!(
                    output.get_row(row).get_datum(0, &ty()),
                    Datum::Int(expected)
                );
            }
        }
        let route = suite.batch_route_snapshot();
        assert_eq!((route.prepared, route.fallback, route.refusals), (3, 0, 0));
    }
    let suite = EvaluatorSuite::new(
        vec![call("if", ty(), vec![column(0), overflow, int(4)])],
        false,
    );
    let mut input = chunk(&[ty()], &[vec![Datum::Int(0)], vec![Datum::Int(1)]]);
    input.set_sel(Some(vec![0]));
    let mut output = Chunk::new_with_capacity(&[ty()], 0);
    suite
        .run_with_tikv_batch(&ctx, &mut input, &mut output, &[ty()])
        .unwrap();
    assert_eq!(output.get_row(0).get_datum(0, &ty()), Datum::Int(4));
    input.set_sel(Some(vec![1]));
    output.reset();
    assert!(suite
        .run_with_tikv_batch(&ctx, &mut input, &mut output, &[ty()])
        .is_err());
    assert_eq!(suite.batch_route_snapshot().fallback, 0);
}

#[test]
fn demanded_controls_select_native_carriers_without_losing_payload_metadata() {
    for (field, value) in native_carrier_cases() {
        if field.eval_type() == NativeEvalType::VectorFloat32 {
            continue;
        }
        let schema = [ty(), field.clone()];
        let choose = call(
            "if",
            field.clone(),
            vec![
                column(0),
                typed_column(1, &field),
                literal(Datum::Null, field.clone()),
            ],
        );
        let coalesce = call(
            "coalesce",
            field.clone(),
            vec![literal(Datum::Null, field.clone()), choose],
        );
        let suite = EvaluatorSuite::new(vec![coalesce], false);
        let rows = vec![
            vec![Datum::Int(1), value.clone()],
            vec![Datum::Int(0), value],
            vec![Datum::Int(1), Datum::Null],
        ];
        let selection: Vec<_> = (0..2050).map(|index| (2050 - index) % 3).collect();
        let mut expected_input = chunk(&schema, &rows);
        expected_input.set_sel(Some(selection.clone()));
        let mut expected = output_for(&field);
        suite
            .run(&Context::new(true), &mut expected_input, &mut expected)
            .unwrap();
        let mut actual_input = chunk(&schema, &rows);
        actual_input.set_sel(Some(selection));
        let mut actual = output_for(&field);
        suite
            .run_with_tikv_batch(&Context::new(true), &mut actual_input, &mut actual, &schema)
            .unwrap();
        assert_eq!(
            values_for(&actual, &field),
            values_for(&expected, &field),
            "{field:?}"
        );
        let route = suite.batch_route_snapshot();
        assert_eq!((route.prepared, route.fallback, route.refusals), (1, 0, 0));
    }
}

#[test]
fn native_json_type_preserves_raw_tags_payloads_and_selected_rows() {
    let json = FieldType::new(FieldTypeCode::Json);
    let make = |tag, payload: &[u8]| Datum::Json(BinaryJSON::from_encoded_parts(tag, payload));
    let cases = vec![
        (Datum::Null, None),
        (make(4, &[0]), Some("NULL")),
        (make(4, &[9]), Some("BOOLEAN")),
        (make(0x0e, &[]), Some("DATE")),
        (make(0x0d, &[0xfc, 1, b'x']), Some("BLOB")),
        (
            Datum::Json(BinaryJSON::parse("[1,true]").unwrap()),
            Some("ARRAY"),
        ),
    ];
    let rows: Vec<_> = cases.iter().map(|(value, _)| vec![value.clone()]).collect();
    for vectorized in [false, true] {
        let ctx = Context::new(vectorized);
        let root = call("json_type", string_ty(), vec![typed_column(0, &json)]);
        for selection in [
            None,
            Some(vec![]),
            Some(
                (0..2050)
                    .map(|index| 2050 - index)
                    .map(|index| index % cases.len())
                    .collect(),
            ),
        ] {
            let suite = EvaluatorSuite::new(vec![root.clone()], false);
            let mut input = chunk(&[json.clone()], &rows);
            input.set_sel(selection.clone());
            let mut output = output_for(&string_ty());
            suite
                .run_with_tikv_batch(&ctx, &mut input, &mut output, &[json.clone()])
                .unwrap();
            let selected = selection.unwrap_or_else(|| (0..cases.len()).collect());
            for (row, physical) in selected.into_iter().enumerate() {
                let actual = output.get_row(row).get_datum(0, &string_ty());
                let expected = cases[physical].1.map_or(Datum::Null, |value| {
                    Datum::String(StringDatum::new(
                        value.as_bytes().to_vec(),
                        Collation::Utf8Mb4Bin,
                    ))
                });
                assert_eq!(actual, expected);
            }
            let route = suite.batch_route_snapshot();
            assert_eq!((route.prepared, route.fallback, route.refusals), (1, 0, 0));
        }
    }
    // Literal JSON leaves use the same raw view, including source-specific tags.
    let root = call(
        "json_type",
        string_ty(),
        vec![literal(make(0x0e, &[]), json)],
    );
    let suite = self::suite(root);
    let mut input = chunk(&[], &[vec![]]);
    let mut output = output_for(&string_ty());
    suite
        .run_with_tikv_batch(&Context::new(true), &mut input, &mut output, &[])
        .unwrap();
    assert_eq!(output.get_row(0).get_string(0), "DATE");
    assert_eq!(suite.batch_route_snapshot().fallback, 0);
}

#[test]
fn native_json_type_errors_keep_native_sql_disposition_and_do_not_retry() {
    let json = FieldType::new(FieldTypeCode::Json);
    let text = string_ty();
    let cases = vec![
        (text.clone(), Datum::new_string(b" ".to_vec())),
        (text.clone(), Datum::new_string(b"{".to_vec())),
        (text, Datum::new_string(vec![0xff])),
        (
            json.clone(),
            Datum::Json(BinaryJSON::from_encoded_parts(0xff, [])),
        ),
        (
            json.clone(),
            Datum::Json(BinaryJSON::from_encoded_parts(0x0d, [0xfc, 1, b'x', b'y'])),
        ),
    ];
    for (field, value) in cases {
        let root = call("json_type", string_ty(), vec![typed_column(0, &field)]);
        let rows = vec![vec![value.clone()]];
        let mut input = chunk(&[field.clone()], &rows);
        let expected = root
            .eval(&Context::new(true), input.get_row(0))
            .unwrap_err();
        let suite = self::suite(root);
        let mut output = output_for(&string_ty());
        let error = suite
            .run_with_tikv_batch(
                &Context::new(true),
                &mut input,
                &mut output,
                &[field.clone()],
            )
            .unwrap_err();
        let EvaluatorError::Eval(actual) = error else {
            panic!("unexpected error carrier")
        };
        assert_eq!(actual, expected);
        assert_eq!(output.num_rows(), 0);
        let route = suite.batch_route_snapshot();
        assert_eq!((route.prepared, route.fallback, route.refusals), (1, 0, 0));
        // Also validate the literal producer, without converting an invalid tag
        // to the wire Json enum at preparation time.
        let root = call("json_type", string_ty(), vec![literal(value, field)]);
        let suite = self::suite(root);
        let mut input = chunk(&[], &[vec![]]);
        let mut output = output_for(&string_ty());
        let EvaluatorError::Eval(actual) = suite
            .run_with_tikv_batch(&Context::new(true), &mut input, &mut output, &[])
            .unwrap_err()
        else {
            panic!("unexpected literal error")
        };
        assert_eq!(actual, expected);
        assert_eq!(suite.batch_route_snapshot().fallback, 0);
    }
}

#[test]
fn native_json_type_skipped_bad_document_is_never_read_or_parsed() {
    let json = FieldType::new(FieldTypeCode::Json);
    let root = call(
        "if",
        string_ty(),
        vec![
            column(0),
            literal(Datum::new_string(b"safe".to_vec()), string_ty()),
            call("json_type", string_ty(), vec![typed_column(1, &json)]),
        ],
    );
    let suite = self::suite(root);
    let schema = [ty(), json];
    let rows = vec![
        vec![
            Datum::Int(1),
            Datum::Json(BinaryJSON::from_encoded_parts(0xff, [])),
        ],
        vec![
            Datum::Int(0),
            Datum::Json(BinaryJSON::from_encoded_parts(0x0e, [])),
        ],
    ];
    let mut input = chunk(&schema, &rows);
    input.set_sel(Some(vec![1, 0, 1, 0]));
    let mut output = output_for(&string_ty());
    suite
        .run_with_tikv_batch(&Context::new(true), &mut input, &mut output, &schema)
        .unwrap();
    assert_eq!(
        (0..4)
            .map(|row| output.get_row(row).get_string(0))
            .collect::<Vec<_>>(),
        ["DATE", "safe", "DATE", "safe"]
    );
    assert_eq!(suite.batch_route_snapshot().fallback, 0);
}

#[test]
fn raw_json_literal_source_budget_fails_before_preparation_clones() {
    let root = call(
        "json_type",
        string_ty(),
        vec![literal(
            Datum::Json(BinaryJSON::from_encoded_parts(4, vec![0; 600_000])),
            FieldType::new(FieldTypeCode::Json),
        )],
    );
    let suite = self::suite(root);
    let mut input = chunk(&[], &[vec![]]);
    let mut output = output_for(&string_ty());
    let error = suite
        .run_with_tikv_batch(&Context::new(true), &mut input, &mut output, &[])
        .unwrap_err();
    let EvaluatorError::Eval(EvalError::ExpressionRuntimeFailure(error)) = error else {
        panic!("expected source resource failure")
    };
    assert_eq!(
        error.class(),
        crate::ExpressionRuntimeFailureClass::ResourceLimit
    );
    assert_eq!(suite.batch_route_snapshot().fallback, 0);
    assert_eq!(output.num_rows(), 0);
}

struct PacketTrace {
    limit: Cell<u64>,
    limits: std::cell::RefCell<std::collections::VecDeque<u64>>,
    strict: bool,
    events: std::cell::RefCell<Vec<String>>,
}
impl PacketTrace {
    fn new(limit: u64, strict: bool) -> Self {
        Self {
            limit: Cell::new(limit),
            limits: Default::default(),
            strict,
            events: Default::default(),
        }
    }
}
impl Columns for PacketTrace {
    fn get(&self, _: &[String]) -> Option<Datum> {
        None
    }
    fn enable_vectorized_expression(&self) -> bool {
        true
    }
    fn max_allowed_packet(&self) -> u64 {
        let value = self
            .limits
            .borrow_mut()
            .pop_front()
            .unwrap_or_else(|| self.limit.get());
        self.events.borrow_mut().push(format!("limit:{value}"));
        value
    }
    fn truncate_level(&self) -> crate::context::ErrorLevel {
        self.events.borrow_mut().push("truncate".into());
        if self.strict {
            crate::context::ErrorLevel::Error
        } else {
            crate::context::ErrorLevel::Warn
        }
    }
    fn append_warning(&self, code: u16, _: &str) {
        self.events.borrow_mut().push(format!("warning:{code}"));
    }
}
fn text(value: Option<&[u8]>) -> Expression {
    literal(
        value.map_or(Datum::Null, |bytes| Datum::new_string(bytes.to_vec())),
        string_ty(),
    )
}
fn bad_document_string() -> Expression {
    call(
        "json_type",
        string_ty(),
        vec![literal(
            Datum::Json(BinaryJSON::from_encoded_parts(0xff, [])),
            FieldType::new(FieldTypeCode::Json),
        )],
    )
}

#[test]
fn concat_host_preserves_packet_observations_and_never_demands_the_skipped_tail() {
    for (root, limit, expected, events) in [
        (
            call(
                "concat",
                string_ty(),
                vec![text(None), bad_document_string()],
            ),
            2,
            None,
            vec![],
        ),
        (
            call(
                "concat",
                string_ty(),
                vec![text(Some(b"abc")), bad_document_string()],
            ),
            2,
            None,
            vec!["limit:2", "limit:2", "truncate", "warning:1301"],
        ),
        (
            call(
                "concat_ws",
                string_ty(),
                vec![text(None), bad_document_string()],
            ),
            2,
            None,
            vec![],
        ),
        // Native packet accounting uses the original data-argument index,
        // including the preceding NULL; the displayed result would be only x.
        (
            call(
                "concat_ws",
                string_ty(),
                vec![
                    text(Some(b",")),
                    text(None),
                    text(Some(b"x")),
                    bad_document_string(),
                ],
            ),
            1,
            None,
            vec!["limit:1", "limit:1", "truncate", "warning:1301"],
        ),
        (
            call(
                "concat_ws",
                string_ty(),
                vec![
                    text(Some(b",")),
                    text(None),
                    text(Some(b"x")),
                    text(None),
                    text(Some(b"y")),
                ],
            ),
            99,
            Some(&b"x,y"[..]),
            vec!["limit:99", "limit:99"],
        ),
        (
            call("concat_ws", string_ty(), vec![text(Some(b",")), text(None)]),
            99,
            Some(&b""[..]),
            vec![],
        ),
        (
            call(
                "concat",
                string_ty(),
                vec![text(Some(b"a")), text(Some(b"b"))],
            ),
            99,
            Some(&b"ab"[..]),
            vec!["limit:99", "limit:99"],
        ),
    ] {
        let native = PacketTrace::new(limit, false);
        let mut native_input = chunk(&[], &[vec![]]);
        let mut reference = output_for(&string_ty());
        let suite = self::suite(root);
        suite
            .run(&native, &mut native_input, &mut reference)
            .unwrap();
        let context = PacketTrace::new(limit, false);
        let mut input = chunk(&[], &[vec![]]);
        let mut output = output_for(&string_ty());
        suite
            .run_with_tikv_batch(&context, &mut input, &mut output, &[])
            .unwrap();
        assert_eq!(
            values_for(&output, &string_ty()),
            values_for(&reference, &string_ty())
        );
        assert_eq!(output.get_row(0).is_null(0), expected.is_none());
        if let Some(expected) = expected {
            assert_eq!(output.get_row(0).get_string(0).as_bytes(), expected);
        }
        let expected: Vec<String> = events.into_iter().map(str::to_owned).collect();
        assert_eq!(*context.events.borrow(), expected);
        assert_eq!(*native.events.borrow(), expected);
        let route = suite.batch_route_snapshot();
        assert_eq!((route.prepared, route.fallback, route.refusals), (1, 0, 0));
    }
}

#[test]
fn concat_host_forwards_the_exact_native_error_and_reads_fresh_packet_policy() {
    let root = call(
        "concat",
        string_ty(),
        vec![text(Some(b"abc")), bad_document_string()],
    );
    let native = PacketTrace::new(2, true);
    let input = chunk(&[], &[vec![]]);
    let expected = root.eval(&native, input.get_row(0)).unwrap_err();
    let suite = self::suite(root);
    let context = PacketTrace::new(2, true);
    let mut input = chunk(&[], &[vec![]]);
    let mut output = output_for(&string_ty());
    let EvaluatorError::Eval(actual) = suite
        .run_with_tikv_batch(&context, &mut input, &mut output, &[])
        .unwrap_err()
    else {
        panic!("expected native SQL error")
    };
    assert_eq!(actual, expected);
    assert_eq!(*context.events.borrow(), *native.events.borrow());
    assert_eq!(output.num_rows(), 0);
    assert_eq!(suite.batch_route_snapshot().fallback, 0);
    let suite = self::suite(call(
        "concat",
        string_ty(),
        vec![text(Some(b"a")), text(Some(b"b"))],
    ));
    let context = PacketTrace::new(10, false);
    let mut input = chunk(&[], &[vec![]]);
    let mut output = output_for(&string_ty());
    suite
        .run_with_tikv_batch(&context, &mut input, &mut output, &[])
        .unwrap();
    assert_eq!(output.get_row(0).get_string(0), "ab");
    context.limit.set(0);
    context.events.borrow_mut().clear();
    output.reset();
    suite
        .run_with_tikv_batch(&context, &mut input, &mut output, &[])
        .unwrap();
    assert!(output.get_row(0).is_null(0));
    assert_eq!(
        *context.events.borrow(),
        ["limit:0", "limit:0", "truncate", "warning:1301"]
    );
    assert_eq!(suite.batch_route_snapshot().fallback, 0);
    let context = PacketTrace::new(0, false);
    context.limits.borrow_mut().extend([10, 0, 0]);
    output.reset();
    suite
        .run_with_tikv_batch(&context, &mut input, &mut output, &[])
        .unwrap();
    assert!(output.get_row(0).is_null(0));
    assert_eq!(
        *context.events.borrow(),
        ["limit:10", "limit:0", "limit:0", "truncate", "warning:1301"]
    );
}

#[test]
fn concat_host_selection_and_nested_tasks_match_native_results_without_fallback() {
    let field = string_ty();
    let schema = [field.clone(), field.clone(), field.clone()];
    let rows = vec![
        vec![
            Datum::new_string(b"a".to_vec()),
            Datum::new_string(b"b".to_vec()),
            Datum::new_string(b"c".to_vec()),
        ],
        vec![Datum::Null, Datum::new_string(b"d".to_vec()), Datum::Null],
        vec![
            Datum::new_string(vec![0xff]),
            Datum::new_string(b"X".to_vec()),
            Datum::new_string(vec![0]),
        ],
    ];
    for root in [
        call(
            "concat",
            field.clone(),
            vec![typed_column(0, &field), typed_column(1, &field)],
        ),
        call(
            "concat_ws",
            field.clone(),
            vec![
                text(Some(b",")),
                typed_column(0, &field),
                typed_column(1, &field),
            ],
        ),
        call(
            "concat",
            field.clone(),
            vec![
                typed_column(0, &field),
                call(
                    "concat_ws",
                    field.clone(),
                    vec![
                        text(Some(b",")),
                        typed_column(1, &field),
                        typed_column(2, &field),
                    ],
                ),
            ],
        ),
    ] {
        let suite = self::suite(root);
        for selection in [
            None,
            Some(vec![]),
            Some((0..2050).map(|index| (2050 - index) % 3).collect()),
        ] {
            let mut native_input = chunk(&schema, &rows);
            native_input.set_sel(selection.clone());
            let mut expected = output_for(&field);
            suite
                .run(
                    &PacketTrace::new(99, false),
                    &mut native_input,
                    &mut expected,
                )
                .unwrap();
            let mut input = chunk(&schema, &rows);
            input.set_sel(selection);
            let mut actual = output_for(&field);
            suite
                .run_with_tikv_batch(
                    &PacketTrace::new(99, false),
                    &mut input,
                    &mut actual,
                    &schema,
                )
                .unwrap();
            assert_eq!(values_for(&actual, &field), values_for(&expected, &field));
        }
        let route = suite.batch_route_snapshot();
        assert_eq!((route.prepared, route.fallback, route.refusals), (3, 0, 0));
    }
}

#[test]
fn concat_host_resource_denial_precedes_packet_observations_and_has_no_retry() {
    let suite = self::suite(call(
        "concat",
        string_ty(),
        vec![text(Some(b"a")), text(Some(b"b"))],
    ));
    let mut worker = PreparedBatch::compile(
        &suite,
        &[],
        true,
        1,
        limits(),
        ExecutionLimits {
            max_active_tasks: 0,
            ..ExecutionLimits::default()
        },
        65536,
    )
    .unwrap();
    let context = PacketTrace::new(99, false);
    let mut input = chunk(&[], &[vec![]]);
    let mut output = output_for(&string_ty());
    let error = suite
        .run_batch(
            &context,
            &mut EvalContext::default(),
            &mut worker,
            &mut input,
            &mut output,
            &[],
        )
        .unwrap_err();
    assert!(matches!(
        error.local_error(),
        Some(LocalError::ResourceLimit(_))
    ));
    assert!(context.events.borrow().is_empty());
    assert_eq!(output.num_rows(), 0);
}

#[test]
fn regexp_host_preserves_null_and_utf8_demand_and_exact_native_errors() {
    for args in [
        vec![text(None), bad_document_string()],
        vec![text(Some(b"x")), text(None), bad_document_string()],
        vec![text(Some(b"x")), text(Some(b"(")), text(None)],
        vec![text(Some(b"x")), text(Some(b"("))],
        vec![text(Some(b"x")), text(Some(b""))],
        vec![text(Some(b"x")), text(Some(b"x")), text(Some(b"z"))],
        vec![text(Some(b"\xff")), bad_document_string()],
        vec![text(Some(b"x")), text(Some(b"\xff")), bad_document_string()],
        vec![text(Some(b"x")), text(Some(b"(")), text(Some(b"\xff"))],
    ] {
        let root = call("regexp_like", ty(), args);
        let mut input = chunk(&[], &[vec![]]);
        let expected = root.clone().eval(&crate::NoColumns, input.get_row(0));
        let suite = self::suite(root);
        let mut output = self::output();
        match expected {
            Ok(value) => {
                suite
                    .run_with_tikv_batch(&crate::NoColumns, &mut input, &mut output, &[])
                    .unwrap();
                assert_eq!(values(&output), vec![value]);
            }
            Err(expected) => {
                let EvaluatorError::Eval(actual) = suite
                    .run_with_tikv_batch(&crate::NoColumns, &mut input, &mut output, &[])
                    .unwrap_err()
                else {
                    panic!("expected exact native error");
                };
                assert_eq!(actual, expected);
                assert_eq!(output.num_rows(), 0);
            }
        }
        let route = suite.batch_route_snapshot();
        assert_eq!(route.fallback, 0);
        assert_eq!(route.refusals, 0);
    }
}

#[test]
fn regexp_host_dynamic_patterns_flags_and_nested_calls_match_native_selection() {
    let field = string_ty();
    let schema = [field.clone(), field.clone(), field];
    let datum = |s: &str| Datum::new_string(s.as_bytes().to_vec());
    let rows = vec![
        vec![datum("AbC"), datum("^abc$"), datum("i")],
        vec![datum("AbC"), datum("^abc$"), datum("ic")],
        vec![Datum::Null, datum("("), datum("z")],
        vec![datum("aa\nbb"), datum("^bb$"), datum("m")],
    ];
    for selection in [
        vec![],
        vec![3, 0, 2, 1, 0],
        (0..2050).map(|i| i % 4).collect(),
    ] {
        for nested in [false, true] {
            let text = if nested {
                call(
                    "concat",
                    string_ty(),
                    vec![string_column(0), text(Some(b""))],
                )
            } else {
                string_column(0)
            };
            let root = call(
                "regexp_like",
                ty(),
                vec![text, string_column(1), string_column(2)],
            );
            let suite = self::suite(root);
            let mut native = chunk(&schema, &rows);
            let mut batch = chunk(&schema, &rows);
            native.set_sel(Some(selection.clone()));
            batch.set_sel(Some(selection.clone()));
            let mut expected = output();
            let mut actual = output();
            suite
                .run(&crate::NoColumns, &mut native, &mut expected)
                .unwrap();
            suite
                .run_with_tikv_batch(&crate::NoColumns, &mut batch, &mut actual, &schema)
                .unwrap();
            assert_eq!(values(&actual), values(&expected));
            let route = suite.batch_route_snapshot();
            assert_eq!((route.prepared, route.fallback, route.refusals), (1, 0, 0));
        }
    }
}

#[test]
fn regexp_host_collation_defaults_and_explicit_flag_precedence_match_native() {
    for collation in [
        Collation::Utf8Mb4Bin,
        Collation::Utf8Mb4GeneralCi,
        Collation::Binary,
    ] {
        for flags in [None, Some("i"), Some("c"), Some("ic"), Some("ci")] {
            let field = string_ty().with_collation(collation);
            let mut args = vec![
                literal(Datum::new_string(b"ABC".to_vec()), field.clone()),
                literal(Datum::new_string(b"abc".to_vec()), field),
            ];
            if let Some(flags) = flags {
                args.push(text(Some(flags.as_bytes())));
            }
            let root = call("regexp_like", ty().with_collation(collation), args);
            let mut input = chunk(&[], &[vec![]]);
            let expected = root
                .clone()
                .eval(&crate::NoColumns, input.get_row(0))
                .unwrap();
            let suite = self::suite(root);
            let mut actual = output();
            suite
                .run_with_tikv_batch(&crate::NoColumns, &mut input, &mut actual, &[])
                .unwrap();
            assert_eq!(values(&actual), vec![expected]);
            assert_eq!(suite.batch_route_snapshot().fallback, 0);
        }
    }
}

#[derive(Default)]
struct RegexpContext {
    id: Cell<u64>,
    reads: Cell<usize>,
}
impl Columns for RegexpContext {
    fn get(&self, _: &[String]) -> Option<Datum> {
        None
    }
    fn context_id(&self) -> u64 {
        self.reads.set(self.reads.get() + 1);
        self.id.get()
    }
}
#[test]
fn regexp_host_shares_original_cache_handles_but_rebinds_context_at_demand() {
    let suite = self::suite(call(
        "regexp_like",
        ty(),
        vec![text(Some(b"x")), text(Some(b"x"))],
    ));
    let mut worker = prepare(&suite, &[]);
    let invocation = match &worker.source.host_metadata[0] {
        NativeHostMetadata::Regexp(invocation) => invocation,
        _ => panic!("expected regexp metadata"),
    };
    // A deliberately seeded cached failure makes handle identity observable:
    // the same context must see it, a new context must resolve the real literal.
    assert!(invocation
        .with_context_id(7)
        .prepare_like("(", "")
        .execute("x")
        .is_err());
    let context = RegexpContext::default();
    context.id.set(7);
    let mut input = chunk(&[], &[vec![]]);
    let mut output = self::output();
    let mut ctx = EvalContext::default();
    let error = suite
        .run_batch(
            &context,
            &mut ctx,
            &mut worker,
            &mut input,
            &mut output,
            &[],
        )
        .unwrap_err();
    assert!(matches!(
        error.kind,
        FailureKind::Native(EvaluatorError::Eval(EvalError::Unsupported(
            "invalid regular expression pattern"
        )))
    ));
    assert_eq!(context.reads.get(), 1);
    assert_eq!(output.num_rows(), 0);
    for id in [8, 7] {
        context.id.set(id);
        output.reset();
        suite
            .run_batch(
                &context,
                &mut ctx,
                &mut worker,
                &mut input,
                &mut output,
                &[],
            )
            .unwrap();
        assert_eq!(values(&output), vec![Datum::Int(1)]);
    }
    assert_eq!(context.reads.get(), 3);
    let suite = self::suite(call(
        "regexp_like",
        ty(),
        vec![text(None), bad_document_string()],
    ));
    let mut output = self::output();
    suite
        .run_with_tikv_batch(&context, &mut input, &mut output, &[])
        .unwrap();
    assert_eq!(values(&output), vec![Datum::Null]);
    assert_eq!(context.reads.get(), 3);
}

#[test]
fn regexp_host_task_denial_does_not_read_context_or_retry() {
    let suite = self::suite(call(
        "regexp_like",
        ty(),
        vec![text(Some(b"x")), text(Some(b"("))],
    ));
    let mut worker = PreparedBatch::compile(
        &suite,
        &[],
        true,
        1,
        limits(),
        ExecutionLimits {
            max_active_tasks: 0,
            ..ExecutionLimits::default()
        },
        65536,
    )
    .unwrap();
    let context = RegexpContext::default();
    let mut input = chunk(&[], &[vec![]]);
    let mut output = self::output();
    let error = suite
        .run_batch(
            &context,
            &mut EvalContext::default(),
            &mut worker,
            &mut input,
            &mut output,
            &[],
        )
        .unwrap_err();
    assert!(matches!(
        error.local_error(),
        Some(LocalError::ResourceLimit(_))
    ));
    assert_eq!(context.reads.get(), 0);
    assert_eq!(output.num_rows(), 0);
}

#[test]
fn all_owned_literal_payloads_are_gated_before_source_snapshot_clones() {
    let candidates = [
        (
            "String",
            literal(Datum::new_string(vec![b'x'; 100_000]), string_ty()),
        ),
        (
            "JSON",
            literal(
                Datum::Json(BinaryJSON::from_encoded_parts(4, vec![0; 100_000])),
                FieldType::new(FieldTypeCode::Json),
            ),
        ),
        (
            "Vector",
            literal(
                Datum::VectorFloat32(VectorFloat32::must_create(vec![1.0; 8192])),
                FieldType::new(FieldTypeCode::VectorFloat32),
            ),
        ),
        (
            "Decimal",
            literal(
                Datum::Decimal(Decimal::from_raw_parts(false, vec![b'1'; 100_000], 0, 0)),
                FieldType::new(FieldTypeCode::NewDecimal),
            ),
        ),
    ];
    let mut ungated = Vec::new();
    for (name, root) in candidates {
        let result = preflight(
            &root,
            &[],
            BatchSourceLimits {
                max_metadata_bytes: 65536,
                ..limits()
            },
        );
        if !matches!(result, Err(error) if matches!(error.local_error(), Some(LocalError::ResourceLimit(_))))
        {
            ungated.push(name);
        }
    }
    assert!(
        ungated.is_empty(),
        "literal copies bypassed source byte gate: {ungated:?}"
    );
}

#[test]
fn complete_literal_budget_precedes_even_earlier_temporary_conversions() {
    // This first argument is a small unrepresentable literal. The complete
    // source byte gate must reject the later huge literal before trying even
    // this first temporary conversion. No input evaluation/context is involved.
    let root = call(
        "concat",
        string_ty(),
        vec![
            literal(Datum::Int(1), string_ty()),
            text(Some(&vec![b'x'; 100_000])),
        ],
    );
    let error = preflight(
        &root,
        &[],
        BatchSourceLimits {
            max_metadata_bytes: 65536,
            ..limits()
        },
    )
    .map(|_| ())
    .unwrap_err();
    assert!(matches!(
        error.local_error(),
        Some(LocalError::ResourceLimit(_))
    ));
    let small = call(
        "concat",
        string_ty(),
        vec![literal(Datum::Int(1), string_ty()), text(Some(b"x"))],
    );
    assert_eq!(
        preflight(&small, &[], limits())
            .map(|_| ())
            .unwrap_err()
            .refusal(),
        BatchRefusal::Admission
    );
}

#[derive(Default)]
struct CastWarnings(std::cell::RefCell<Vec<(u16, String)>>);
impl Columns for CastWarnings {
    fn get(&self, _: &[String]) -> Option<Datum> {
        None
    }
    fn append_warning(&self, code: u16, message: &str) {
        self.0.borrow_mut().push((code, message.to_owned()));
    }
    fn connection_charset_info(&self) -> (&str, &str) {
        panic!("explicit target must not read connection charset")
    }
    fn max_allowed_packet(&self) -> u64 {
        panic!("integer CHAR must not read packet limit")
    }
}

#[test]
fn native_integer_char_cast_preserves_unsigned_bits_nulls_and_warning_contents() {
    for unsigned in [false, true] {
        let mut source = ty();
        if unsigned {
            source.add_flags(FieldTypeFlags::UNSIGNED);
        }
        let rows = if unsigned {
            vec![
                vec![Datum::UInt(u64::MAX)],
                vec![Datum::UInt(0)],
                vec![Datum::Null],
            ]
        } else {
            vec![
                vec![Datum::Int(i64::MIN)],
                vec![Datum::Int(0)],
                vec![Datum::Null],
            ]
        };
        for width in [-1, 0, 1, 20, 100] {
            for collation in [Collation::Utf8Mb4Bin, Collation::Binary, Collation::GbkBin] {
                let target = string_ty().with_flen(width).with_collation(collation);
                let root = call("cast_char", target.clone(), vec![typed_column(0, &source)]);
                let suite = self::suite(root);
                let mut native_input = chunk(std::slice::from_ref(&source), &rows);
                native_input.set_sel(Some(vec![2, 0, 1, 0]));
                let mut batch_input = chunk(std::slice::from_ref(&source), &rows);
                batch_input.set_sel(Some(vec![2, 0, 1, 0]));
                let mut expected = output_for(&target);
                let mut actual = output_for(&target);
                let native = CastWarnings::default();
                let context = CastWarnings::default();
                suite
                    .run(&native, &mut native_input, &mut expected)
                    .unwrap();
                suite
                    .run_with_tikv_batch(
                        &context,
                        &mut batch_input,
                        &mut actual,
                        std::slice::from_ref(&source),
                    )
                    .unwrap();
                assert_eq!(values_for(&actual, &target), values_for(&expected, &target));
                assert_eq!(*context.0.borrow(), *native.0.borrow());
                let route = suite.batch_route_snapshot();
                assert_eq!((route.prepared, route.fallback, route.refusals), (1, 0, 0));
            }
        }
    }
}

#[test]
fn integer_char_cast_composes_with_existing_host_tasks_and_short_circuit() {
    for root in [
        call(
            "concat",
            string_ty(),
            vec![
                call("cast_char", string_ty(), vec![column(0)]),
                text(Some(b"!")),
            ],
        ),
        call(
            "if",
            string_ty(),
            vec![
                int(0),
                bad_document_string(),
                call("cast_char", string_ty(), vec![column(0)]),
            ],
        ),
    ] {
        let target = string_ty();
        let suite = self::suite(root);
        let rows = vec![
            vec![Datum::Int(-123)],
            vec![Datum::Null],
            vec![Datum::Int(4)],
        ];
        let schema = [ty()];
        let mut reference_input = chunk(&schema, &rows);
        let mut batch_input = chunk(&schema, &rows);
        let mut expected = output_for(&target);
        let mut actual = output_for(&target);
        suite
            .run(&crate::NoColumns, &mut reference_input, &mut expected)
            .unwrap();
        suite
            .run_with_tikv_batch(&crate::NoColumns, &mut batch_input, &mut actual, &schema)
            .unwrap();
        assert_eq!(values_for(&actual, &target), values_for(&expected, &target));
        let route = suite.batch_route_snapshot();
        assert_eq!((route.prepared, route.fallback, route.refusals), (1, 0, 0));
    }
}

#[test]
fn rewritten_integer_to_char_sql_runs_in_the_shared_projection() {
    use crate::rewriter::{rewrite_expr_resolved, ColumnResolver};
    use tidb_ast::{QueryStmt, SelectField, Stmt};
    struct Resolver(FieldType);
    impl ColumnResolver for Resolver {
        fn time_zone(&self) -> SessionTimeZone {
            SessionTimeZone::utc()
        }
        fn resolve(&self, path: &[String]) -> Option<(usize, FieldType, i64)> {
            (path.last()?.as_str() == "i").then(|| (0, self.0.clone(), 1))
        }
    }
    for unsigned in [false, true] {
        let mut source = ty();
        if unsigned {
            source.add_flags(FieldTypeFlags::UNSIGNED);
        }
        let resolver = Resolver(source.clone());
        for sql in [
            "cast(i as char)",
            "cast(i as char(1))",
            "concat(cast(i as char), '!')",
            "regexp_like(cast(i as char), '^1')",
        ] {
            let Stmt::Query(query) = tidb_parser::parse(&format!("select {sql}")).unwrap() else {
                panic!("query")
            };
            let QueryStmt::Select(select) = query.into_inner() else {
                panic!("select")
            };
            let SelectField::Expr { expr, .. } = &select.fields[0] else {
                panic!("expression")
            };
            let root = rewrite_expr_resolved(expr, &resolver).unwrap();
            let target = root.static_type().unwrap().clone();
            let suite = self::suite(root);
            let rows = vec![
                vec![if unsigned {
                    Datum::UInt(u64::MAX)
                } else {
                    Datum::Int(i64::MIN)
                }],
                vec![Datum::Null],
                vec![Datum::Int(1)],
            ];
            for selected in [vec![], vec![0, 1, 2, 0], (0..2050).map(|i| i % 3).collect()] {
                let mut native_input = chunk(std::slice::from_ref(&source), &rows);
                let mut batch_input = chunk(std::slice::from_ref(&source), &rows);
                native_input.set_sel(Some(selected.clone()));
                batch_input.set_sel(Some(selected));
                let mut expected = output_for(&target);
                let mut actual = output_for(&target);
                suite
                    .run(&crate::NoColumns, &mut native_input, &mut expected)
                    .unwrap();
                suite
                    .run_with_tikv_batch(
                        &crate::NoColumns,
                        &mut batch_input,
                        &mut actual,
                        std::slice::from_ref(&source),
                    )
                    .unwrap();
                assert_eq!(
                    values_for(&actual, &target),
                    values_for(&expected, &target),
                    "{sql}"
                );
                let route = suite.batch_route_snapshot();
                assert_eq!(route.fallback, 0, "{sql}");
                assert_eq!(route.refusals, 0, "{sql}");
            }
        }
    }
}

#[test]
fn integer_char_cast_preserves_actual_literal_kind_when_flags_disagree() {
    let mut field = ty();
    field.add_flags(FieldTypeFlags::UNSIGNED);
    let bad = || literal(Datum::Int(-1), field.clone());
    for argument in [
        bad(),
        call("any_value", field.clone(), vec![bad()]),
        call(
            "round",
            field.clone(),
            vec![call("any_value", field.clone(), vec![bad()])],
        ),
        call(
            "bitxor",
            field.clone(),
            vec![bad(), literal(Datum::UInt(0), field.clone())],
        ),
    ] {
        let root = call("cast_char", string_ty(), vec![argument]);
        let mut input = chunk(&[], &[vec![]]);
        let expected = root
            .clone()
            .eval(&crate::NoColumns, input.get_row(0))
            .unwrap();
        let suite = self::suite(root);
        let mut actual = output_for(&string_ty());
        suite
            .run_with_tikv_batch(&crate::NoColumns, &mut input, &mut actual, &[])
            .unwrap();
        assert_eq!(values_for(&actual, &string_ty()), vec![expected]);
        let route = suite.batch_route_snapshot();
        assert_eq!((route.prepared, route.fallback, route.refusals), (1, 0, 0));
    }
}

#[test]
fn native_char_value_inputs_preserve_all_nineteen_kinds_and_exact_sql_errors() {
    use tidb_datatype::{BinaryLiteral, MysqlEnum, MysqlSet};
    let time =
        tidb_datatype::parse_datetime("2026-10-17 12:34:56.123456", &chrono_tz::UTC, true, false)
            .unwrap()
            .time;
    let fixtures = [
        Datum::Null,
        Datum::MinNotNull,
        Datum::MaxValue,
        Datum::Int(i64::MIN),
        Datum::UInt(u64::MAX),
        Datum::Decimal(
            Decimal::from_raw_parts(true, b"0123400".to_vec(), 2, 4).with_declared_shape(7, 2),
        ),
        Datum::Real(f64::from_bits(0x7ff8000000000042)),
        Datum::Float32(1.0000000000000002),
        Datum::new_string(vec![0xff, 0]),
        Datum::Bytes(vec![0xff, 0]),
        Datum::BinaryLiteral(BinaryLiteral::from(vec![0, b'A'])),
        Datum::Duration(MySqlDuration::from_nanoseconds(-123456789012, 4).unwrap()),
        Datum::Enum(MysqlEnum::new("b", 2), Collation::GbkBin),
        Datum::Bit(BinaryLiteral::from(vec![0, b'A'])),
        Datum::Set(MysqlSet::new("a,b", 3), Collation::GbkBin),
        Datum::Time(time),
        Datum::Json(BinaryJSON::parse("{\"a\":[1,true]}").unwrap()),
        Datum::Raw(vec![0xff, 0]),
        Datum::VectorFloat32(VectorFloat32::must_create(vec![1.0, -0.0])),
    ];
    for value in fixtures {
        for collation in [Collation::Utf8Mb4Bin, Collation::Binary] {
            let target = string_ty().with_flen(-1).with_collation(collation);
            // Use an independently declared source: actual Datum kind must not
            // be reinterpreted by the transport as its static SQL EvalType.
            let root = call(
                "cast_char",
                target.clone(),
                vec![literal(value.clone(), string_ty())],
            );
            let mut input = chunk(&[], &[vec![]]);
            let reference = CastWarnings::default();
            let expected = root.clone().eval(&reference, input.get_row(0));
            let suite = self::suite(root);
            let context = CastWarnings::default();
            let mut actual = output_for(&target);
            match expected {
                Ok(value) => {
                    suite
                        .run_with_tikv_batch(&context, &mut input, &mut actual, &[])
                        .unwrap();
                    let mut published = output_for(&target);
                    published.append_datum(0, &value);
                    assert_eq!(
                        values_for(&actual, &target),
                        values_for(&published, &target)
                    );
                }
                Err(expected) => {
                    let EvaluatorError::Eval(actual) = suite
                        .run_with_tikv_batch(&context, &mut input, &mut actual, &[])
                        .unwrap_err()
                    else {
                        panic!("native CHAR error must retain its type");
                    };
                    assert_eq!(actual, expected);
                }
            }
            assert_eq!(*context.0.borrow(), *reference.0.borrow());
            let route = suite.batch_route_snapshot();
            assert_eq!((route.prepared, route.fallback, route.refusals), (1, 0, 0));
        }
    }
}

#[test]
fn native_char_value_column_inputs_keep_nonfinite_float32_decimal_json_and_time() {
    let time =
        tidb_datatype::parse_datetime("2026-10-17 12:34:56.123456", &chrono_tz::UTC, true, false)
            .unwrap()
            .time;
    let cases = [
        (
            real_ty(),
            vec![
                Datum::Real(f64::NAN),
                Datum::Real(-0.0),
                Datum::Real(f64::INFINITY),
            ],
        ),
        (
            FieldType::new(FieldTypeCode::Float),
            vec![Datum::Float32(1.0000000000000002), Datum::Float32(f64::NAN)],
        ),
        (
            FieldType::new(FieldTypeCode::NewDecimal)
                .with_flen(20)
                .with_decimal(4),
            vec![
                Datum::Decimal(Decimal::from_literal("1.2300")),
                Datum::Decimal(Decimal::from_literal("-0.0010")),
            ],
        ),
        (
            FieldType::new(FieldTypeCode::Json),
            vec![
                Datum::Json(BinaryJSON::parse("{\"x\":1}").unwrap()),
                Datum::Json(BinaryJSON::parse("null").unwrap()),
            ],
        ),
        (
            FieldType::new(FieldTypeCode::Datetime).with_decimal(6),
            vec![Datum::Time(time)],
        ),
        (
            FieldType::new(FieldTypeCode::Year),
            vec![Datum::Int(0), Datum::Int(2026)],
        ),
        (
            FieldType::new(FieldTypeCode::Enum).with_elems(["a", "b"]),
            vec![Datum::Enum(
                tidb_datatype::MysqlEnum::new("b", 2),
                Collation::Binary,
            )],
        ),
        (
            FieldType::new(FieldTypeCode::Set).with_elems(["a", "b"]),
            vec![Datum::Set(
                tidb_datatype::MysqlSet::new("a,b", 3),
                Collation::Binary,
            )],
        ),
        (
            FieldType::new(FieldTypeCode::Bit).with_flen(8),
            vec![Datum::Bit(tidb_datatype::BinaryLiteral::from(vec![b'A']))],
        ),
        (
            FieldType::new(FieldTypeCode::Duration).with_decimal(6),
            vec![Datum::Duration(
                MySqlDuration::from_nanoseconds(-123456789012, 6).unwrap(),
            )],
        ),
    ];
    for (field, mut inputs) in cases {
        inputs.push(Datum::Null);
        let rows: Vec<_> = inputs.into_iter().map(|value| vec![value]).collect();
        let target = string_ty().with_flen(-1);
        let root = call("cast_char", target.clone(), vec![typed_column(0, &field)]);
        let suite = self::suite(root);
        for selection in [
            vec![],
            vec![0, rows.len() - 1, 0],
            (0..2050).map(|i| i % rows.len()).collect(),
        ] {
            let mut native_input = chunk(std::slice::from_ref(&field), &rows);
            let mut batch_input = chunk(std::slice::from_ref(&field), &rows);
            native_input.set_sel(Some(selection.clone()));
            batch_input.set_sel(Some(selection));
            let reference = CastWarnings::default();
            let context = CastWarnings::default();
            let mut expected = output_for(&target);
            let mut actual = output_for(&target);
            suite
                .run(&reference, &mut native_input, &mut expected)
                .unwrap();
            suite
                .run_with_tikv_batch(
                    &context,
                    &mut batch_input,
                    &mut actual,
                    std::slice::from_ref(&field),
                )
                .unwrap();
            assert_eq!(values_for(&actual, &target), values_for(&expected, &target));
            assert_eq!(*context.0.borrow(), *reference.0.borrow());
            let route = suite.batch_route_snapshot();
            assert_eq!(route.fallback, 0);
            assert_eq!(route.refusals, 0);
        }
    }
}

#[test]
fn native_char_binary_decode_preserves_complete_warning_contents_and_order() {
    let source = string_ty().with_collation(Collation::Binary);
    for width in [-1, 0, 1] {
        let target = string_ty()
            .with_flen(width)
            .with_collation(Collation::GbkBin);
        let root = call(
            "cast_char",
            target.clone(),
            vec![literal(
                Datum::Bytes(vec![0xff, 0x81, 0x30]),
                source.clone(),
            )],
        );
        let mut input = chunk(&[], &[vec![]]);
        let reference = CastWarnings::default();
        let value = root.clone().eval(&reference, input.get_row(0)).unwrap();
        let mut expected = output_for(&target);
        expected.append_datum(0, &value);
        let suite = self::suite(root);
        let context = CastWarnings::default();
        let mut actual = output_for(&target);
        suite
            .run_with_tikv_batch(&context, &mut input, &mut actual, &[])
            .unwrap();
        assert_eq!(values_for(&actual, &target), values_for(&expected, &target));
        assert_eq!(*context.0.borrow(), *reference.0.borrow());
        assert!(context.0.borrow().iter().any(|(code, _)| *code == 3854));
        assert_eq!(suite.batch_route_snapshot().fallback, 0);
    }
}

#[test]
fn native_char_unselected_invalid_utf8_is_never_read_or_formatted() {
    let root = call(
        "if",
        string_ty(),
        vec![
            int(0),
            call(
                "cast_char",
                string_ty(),
                vec![literal(Datum::Raw(vec![0xff]), string_ty())],
            ),
            text(Some(b"safe")),
        ],
    );
    let suite = self::suite(root);
    let mut input = chunk(&[], &[vec![]]);
    let mut actual = output_for(&string_ty());
    let context = CastWarnings::default();
    suite
        .run_with_tikv_batch(&context, &mut input, &mut actual, &[])
        .unwrap();
    assert_eq!(actual.get_row(0).get_string(0), "safe");
    assert!(context.0.borrow().is_empty());
    let route = suite.batch_route_snapshot();
    assert_eq!((route.prepared, route.fallback, route.refusals), (1, 0, 0));
}

#[test]
fn rewritten_native_char_casts_compose_across_full_selection_windows() {
    use crate::rewriter::{rewrite_expr_resolved, ColumnResolver};
    use tidb_ast::{QueryStmt, SelectField, Stmt};
    struct Resolver(FieldType);
    impl ColumnResolver for Resolver {
        fn time_zone(&self) -> SessionTimeZone {
            SessionTimeZone::utc()
        }
        fn resolve(&self, path: &[String]) -> Option<(usize, FieldType, i64)> {
            (path.last()?.as_str() == "v").then(|| (0, self.0.clone(), 1))
        }
    }
    for (field, value) in [
        (real_ty(), Datum::Real(f64::NAN)),
        (
            FieldType::new(FieldTypeCode::Float),
            Datum::Float32(1.0000000000000002),
        ),
        (FieldType::new(FieldTypeCode::Year), Datum::Int(0)),
        (
            FieldType::new(FieldTypeCode::Json),
            Datum::Json(BinaryJSON::parse("{\"x\":1}").unwrap()),
        ),
    ] {
        let resolver = Resolver(field.clone());
        for sql in [
            "cast(v as char)",
            "concat(cast(v as char), '!')",
            "regexp_like(cast(v as char), '^N')",
        ] {
            let Stmt::Query(query) = tidb_parser::parse(&format!("select {sql}")).unwrap() else {
                panic!("query")
            };
            let QueryStmt::Select(select) = query.into_inner() else {
                panic!("select")
            };
            let SelectField::Expr { expr, .. } = &select.fields[0] else {
                panic!("expression")
            };
            let root = rewrite_expr_resolved(expr, &resolver).unwrap();
            let target = root.static_type().unwrap().clone();
            let suite = self::suite(root);
            let rows = vec![vec![value.clone()], vec![Datum::Null]];
            for selection in [vec![], vec![0, 1, 0], (0..2050).map(|i| i % 2).collect()] {
                let mut native = chunk(std::slice::from_ref(&field), &rows);
                let mut batch = chunk(std::slice::from_ref(&field), &rows);
                native.set_sel(Some(selection.clone()));
                batch.set_sel(Some(selection));
                let mut expected = output_for(&target);
                let mut actual = output_for(&target);
                suite
                    .run(&crate::NoColumns, &mut native, &mut expected)
                    .unwrap();
                suite
                    .run_with_tikv_batch(
                        &crate::NoColumns,
                        &mut batch,
                        &mut actual,
                        std::slice::from_ref(&field),
                    )
                    .unwrap();
                assert_eq!(
                    values_for(&actual, &target),
                    values_for(&expected, &target),
                    "{sql}"
                );
                let route = suite.batch_route_snapshot();
                assert_eq!(route.fallback, 0, "{sql}");
                assert_eq!(route.refusals, 0, "{sql}");
            }
        }
    }
}

#[test]
fn native_literal_roots_publish_actual_chunk_storage_without_wire_normalization() {
    let time =
        tidb_datatype::parse_datetime("2026-10-18 12:34:56.123456", &chrono_tz::UTC, true, false)
            .unwrap()
            .time;
    let cases = [
        (FieldType::new(FieldTypeCode::Float), Datum::Float32(-0.0)),
        (real_ty(), Datum::Real(f64::from_bits(0x7ff8000000000042))),
        (ty(), Datum::UInt(u64::MAX)),
        (FieldType::new(FieldTypeCode::Year), Datum::Int(0)),
        (
            FieldType::new(FieldTypeCode::Bit).with_flen(8),
            Datum::Bit(tidb_datatype::BinaryLiteral::from(vec![0xa5])),
        ),
        (
            FieldType::new(FieldTypeCode::Enum).with_elems(["a", "b"]),
            Datum::Enum(tidb_datatype::MysqlEnum::new("b", 2), Collation::Binary),
        ),
        (
            FieldType::new(FieldTypeCode::Set).with_elems(["a", "b"]),
            Datum::Set(tidb_datatype::MysqlSet::new("a,b", 3), Collation::Binary),
        ),
        (ty(), Datum::Int(i64::MIN)),
        (ty().with_unsigned(true), Datum::UInt(u64::MAX)),
        (string_ty(), Datum::new_string(vec![0xff, b'a'])),
        (string_ty(), Datum::Bytes(vec![0, 0xff])),
        (string_ty(), Datum::Raw(vec![0, 0xff])),
        (
            string_ty(),
            Datum::BinaryLiteral(tidb_datatype::BinaryLiteral::from(vec![0, 0xa5])),
        ),
        (
            FieldType::new(FieldTypeCode::NewDecimal)
                .with_flen(20)
                .with_decimal(4),
            Datum::Decimal(Decimal::from_literal("-1.2300")),
        ),
        (
            FieldType::new(FieldTypeCode::Json),
            Datum::Json(BinaryJSON::parse("{\"a\":[1,null]}").unwrap()),
        ),
        (
            FieldType::new(FieldTypeCode::Datetime).with_decimal(6),
            Datum::Time(time),
        ),
        (
            FieldType::new(FieldTypeCode::Duration).with_decimal(6),
            Datum::Duration(MySqlDuration::from_nanoseconds(-123456789012, 6).unwrap()),
        ),
        (
            FieldType::new(FieldTypeCode::VectorFloat32),
            Datum::VectorFloat32(VectorFloat32::must_create(vec![-0.0, 1.25])),
        ),
    ];
    for (field, value) in cases {
        for value in [value, Datum::Null] {
            for selection in [None, Some(vec![]), Some((0..2050).map(|i| i % 2).collect())] {
                for enabled in [true, false] {
                    let root = literal(value.clone(), field.clone());
                    let suite = self::suite(root.clone());
                    let mut input = Chunk::new_with_capacity(&[], 2);
                    input.set_num_virtual_rows(2);
                    input.set_sel(selection.clone());
                    let count = selection.as_ref().map_or(2, Vec::len);
                    let mut actual = output_for(&field);
                    let mut expected = output_for(&field);
                    let native = root
                        .eval(&Context::new(enabled), tidb_chunk::row::Row::empty())
                        .unwrap();
                    for _ in 0..count {
                        expected.append_datum(0, &native);
                    }
                    suite
                        .run_with_tikv_batch(&Context::new(enabled), &mut input, &mut actual, &[])
                        .unwrap();
                    assert_eq!(actual.num_rows(), expected.num_rows());
                    for row in 0..count {
                        assert_eq!(
                            actual.get_row(row).is_null(0),
                            expected.get_row(row).is_null(0)
                        );
                        assert_eq!(
                            actual.column(0).get_raw(row).as_ref(),
                            expected.column(0).get_raw(row).as_ref()
                        );
                    }
                    let route = suite.batch_route_snapshot();
                    assert_eq!(
                        (route.prepared, route.fallback, route.refusals),
                        (1, 0, 0),
                        "{field:?}"
                    );
                }
            }
        }
    }
}

#[test]
fn native_output_checks_every_handle_before_publishing_any_cell() {
    let field = FieldType::new(FieldTypeCode::Float);
    let suite = self::suite(literal(Datum::Float32(1.25), field.clone()));
    let mut worker = PreparedBatch::compile_calculation(
        &suite,
        0,
        &[],
        true,
        93,
        limits(),
        ExecutionLimits::default(),
        1 << 20,
    )
    .unwrap();
    let store = RefCell::new(NativeValueStore::new(1 << 20).unwrap());
    store.borrow_mut().begin_window().unwrap();
    let origin = |occurrence| NativeValueOrigin {
        node: 0,
        row: InputRow {
            occurrence,
            input_row: occurrence,
        },
    };
    let token = |role, row| {
        store
            .borrow_mut()
            .insert_clone(&Datum::Float32(1.25), role, origin(row))
            .unwrap()
    };
    let good = token(NativeValueRole::Output, 0);
    let wrong_role = token(NativeValueRole::CharInput, 1);
    let wrong_row = token(NativeValueRole::Output, 0);
    let mut output = output_for(&field);
    for bad in [Some(wrong_role), Some(wrong_row), None] {
        let values = VectorValue::Bytes(ChunkedVecBytes::from(vec![Some(good.clone()), bad]));
        let error = worker
            .write_native_output(values, &[0, 1], &mut output, &store, None)
            .unwrap_err();
        assert!(matches!(
            error.local_error(),
            Some(LocalError::BindingContract(_))
        ));
        assert_eq!(output.num_rows(), 0);
    }
    worker.max_materialization_retained_bytes = 0;
    let values = VectorValue::Bytes(ChunkedVecBytes::from(vec![Some(good)]));
    let error = worker
        .write_native_output(values, &[0], &mut output, &store, None)
        .unwrap_err();
    assert!(matches!(
        error.local_error(),
        Some(LocalError::ResourceLimit(_))
    ));
    assert_eq!(output.num_rows(), 0);
}

#[derive(Default)]
struct NativeCastDiagnostics(RefCell<Vec<(u16, String)>>);
impl crate::Columns for NativeCastDiagnostics {
    fn get(&self, _: &[String]) -> Option<Datum> {
        None
    }
    fn append_warning(&self, code: u16, message: &str) {
        self.0.borrow_mut().push((code, message.to_owned()));
    }
}

#[test]
fn native_computed_cast_roots_match_value_kernels_and_diagnostics() {
    let text = |value: &str| literal(Datum::new_string(value), string_ty());
    let cases = [
        call("cast_signed", ty(), vec![text("12.5tail")]),
        call("cast_unsigned", ty().with_unsigned(true), vec![text("-1")]),
        call(
            "cast_unsigned_in_union",
            ty().with_unsigned(true),
            vec![int(-3)],
        ),
        call("cast_double", real_ty(), vec![text("2.75tail")]),
        call(
            "cast_float",
            real_ty(),
            vec![literal(Datum::Real(1.23456789), real_ty())],
        ),
        call(
            "cast_decimal",
            FieldType::new(FieldTypeCode::NewDecimal)
                .with_flen(8)
                .with_decimal(2),
            vec![literal(Datum::Real(1.235), real_ty())],
        ),
        call(
            "cast_year",
            FieldType::new(FieldTypeCode::Year),
            vec![text("70")],
        ),
        call(
            "cast_binary",
            FieldType::new(FieldTypeCode::String)
                .with_flen(5)
                .with_collation(Collation::Binary),
            vec![text("abc")],
        ),
        call(
            "cast_datetime",
            FieldType::new(FieldTypeCode::Datetime).with_decimal(3),
            vec![text("2026-10-18 12:34:56.123456")],
        ),
        call(
            "cast_date",
            FieldType::new(FieldTypeCode::Date),
            vec![text("2026-10-18 12:34:56.123456")],
        ),
        call(
            "cast_time",
            FieldType::new(FieldTypeCode::Duration).with_decimal(3),
            vec![text("12:34:56.123456")],
        ),
        call(
            "cast_vector",
            FieldType::new(FieldTypeCode::VectorFloat32).with_flen(2),
            vec![text("[1.25,2.5]")],
        ),
        call(
            "cast_signed",
            ty(),
            vec![literal(
                Datum::VectorFloat32(VectorFloat32::must_create(vec![1.0])),
                FieldType::new(FieldTypeCode::VectorFloat32),
            )],
        ),
        call("cast_signed", ty(), vec![null()]),
        call(
            "cast_signed",
            ty(),
            vec![call("cast_double", real_ty(), vec![text("12.75tail")])],
        ),
        call(
            "cast_decimal",
            FieldType::new(FieldTypeCode::NewDecimal)
                .with_flen(8)
                .with_decimal(2),
            vec![call(
                "cast_char",
                string_ty(),
                vec![literal(Datum::Real(1.235), real_ty())],
            )],
        ),
        call(
            "cast_char",
            string_ty(),
            vec![call("cast_signed", ty(), vec![text("12.5tail")])],
        ),
        call(
            "cast_datetime",
            FieldType::new(FieldTypeCode::Datetime),
            vec![literal(
                Datum::Int(2026),
                FieldType::new(FieldTypeCode::Year),
            )],
        ),
        call(
            "cast_unsigned",
            ty().with_unsigned(true),
            vec![literal(Datum::Real(-1.5), real_ty())],
        ),
        call(
            "cast_unsigned",
            ty().with_unsigned(true),
            vec![literal(Datum::Real(f64::INFINITY), real_ty())],
        ),
        call(
            "cast_unsigned",
            ty().with_unsigned(true),
            vec![literal(
                Datum::Real(f64::from_bits(0x7ff8000000000042)),
                real_ty(),
            )],
        ),
        call(
            "cast_unsigned",
            ty().with_unsigned(true),
            vec![literal(
                Datum::Float32(1.5),
                FieldType::new(FieldTypeCode::Float),
            )],
        ),
    ];
    for root in cases {
        let field = root.static_type().unwrap().clone();
        for selected in [vec![], vec![0, 1, 0], (0..2050).map(|i| i % 2).collect()] {
            let actual_ctx = NativeCastDiagnostics::default();
            let reference_ctx = NativeCastDiagnostics::default();
            let mut expected = output_for(&field);
            let mut expected_error = None;
            for _ in &selected {
                match root.eval(&reference_ctx, tidb_chunk::row::Row::empty()) {
                    Ok(value) => expected.append_datum(0, &value),
                    Err(error) => {
                        expected_error = Some(error);
                        break;
                    }
                }
            }
            let suite = self::suite(root.clone());
            let mut input = Chunk::new_with_capacity(&[], 2);
            input.set_num_virtual_rows(2);
            input.set_sel(Some(selected));
            let mut actual = output_for(&field);
            let result = suite.run_with_tikv_batch(&actual_ctx, &mut input, &mut actual, &[]);
            if let Some(expected_error) = expected_error {
                let Err(EvaluatorError::Eval(actual_error)) = result else {
                    panic!("lost native error: {result:?}");
                };
                assert_eq!(actual_error, expected_error);
            } else {
                result.unwrap();
                assert_eq!(actual.num_rows(), expected.num_rows());
                for row in 0..actual.num_rows() {
                    assert_eq!(
                        actual.get_row(row).is_null(0),
                        expected.get_row(row).is_null(0)
                    );
                    assert_eq!(
                        actual.column(0).get_raw(row).as_ref(),
                        expected.column(0).get_raw(row).as_ref()
                    );
                }
            }
            assert_eq!(*actual_ctx.0.borrow(), *reference_ctx.0.borrow());
            let route = suite.batch_route_snapshot();
            assert_eq!(
                (route.prepared, route.fallback, route.refusals),
                (1, 0, 0),
                "{root:?}"
            );
        }
    }
}

#[test]
fn rewritten_native_cast_islands_keep_column_rows_and_exact_diagnostics() {
    use crate::rewriter::{rewrite_expr_resolved, ColumnResolver};
    use tidb_ast::{QueryStmt, SelectField, Stmt};
    struct Resolver;
    impl ColumnResolver for Resolver {
        fn time_zone(&self) -> SessionTimeZone {
            SessionTimeZone::utc()
        }
        fn resolve(&self, path: &[String]) -> Option<(usize, FieldType, i64)> {
            (path.last()?.as_str() == "v").then(|| (0, string_ty(), 1))
        }
    }
    let schema = [string_ty()];
    let rows = vec![
        vec![Datum::new_string("12.5tail")],
        vec![Datum::Null],
        vec![Datum::new_string("-3")],
    ];
    for sql in [
        "cast(v as signed)",
        "cast(v as unsigned)",
        "cast(v as decimal(8,2))",
        "cast(v as double)",
        "cast(cast(v as signed) as decimal(8,2))",
        "cast(cast(v as signed) as char)",
        "concat(cast(cast(v as signed) as char), '!')",
        "regexp_like(cast(cast(v as signed) as char), '^1')",
    ] {
        let Stmt::Query(query) = tidb_parser::parse(&format!("select {sql}")).unwrap() else {
            panic!("query")
        };
        let QueryStmt::Select(select) = query.into_inner() else {
            panic!("select")
        };
        let SelectField::Expr { expr, .. } = &select.fields[0] else {
            panic!("expression")
        };
        let root = rewrite_expr_resolved(expr, &Resolver).unwrap();
        let field = root.static_type().unwrap().clone();
        for selected in [vec![], vec![2, 0, 1, 0], (0..2050).map(|i| i % 3).collect()] {
            let reference = NativeCastDiagnostics::default();
            let context = NativeCastDiagnostics::default();
            let mut input = chunk(&schema, &rows);
            input.set_sel(Some(selected.clone()));
            let mut expected = output_for(&field);
            for row in selected {
                let value = root.eval(&reference, input.physical_row(row)).unwrap();
                expected.append_datum(0, &value);
            }
            let suite = self::suite(root.clone());
            let mut actual = output_for(&field);
            suite
                .run_with_tikv_batch(&context, &mut input, &mut actual, &schema)
                .unwrap();
            assert_eq!(actual.num_rows(), expected.num_rows(), "{sql}");
            for row in 0..actual.num_rows() {
                assert_eq!(
                    actual.get_row(row).is_null(0),
                    expected.get_row(row).is_null(0),
                    "{sql}"
                );
                assert_eq!(
                    actual.column(0).get_raw(row).as_ref(),
                    expected.column(0).get_raw(row).as_ref(),
                    "{sql}"
                );
            }
            assert_eq!(*context.0.borrow(), *reference.0.borrow(), "{sql}");
            let route = suite.batch_route_snapshot();
            assert_eq!(
                (route.prepared, route.fallback, route.refusals),
                (1, 0, 0),
                "{sql}"
            );
        }
    }
}

#[test]
fn native_json_cast_preserves_typed_literal_and_column_inputs() {
    let cases = [
        (ty(), Datum::Int(-1)),
        (ty().with_unsigned(true), Datum::UInt(u64::MAX)),
        (real_ty(), Datum::Real(f64::NAN)),
        (FieldType::new(FieldTypeCode::Float), Datum::Float32(1.25)),
        (
            FieldType::new(FieldTypeCode::NewDecimal)
                .with_flen(20)
                .with_decimal(4),
            Datum::Decimal(Decimal::from_literal("-1.2300")),
        ),
        (string_ty(), Datum::new_string("{\"a\":1}")),
        (string_ty(), Datum::new_string("bad JSON")),
        (
            FieldType::new(FieldTypeCode::VarString).with_collation(Collation::Binary),
            Datum::Bytes(vec![0xff, 0]),
        ),
        (
            FieldType::new(FieldTypeCode::Enum).with_elems(["a", "b"]),
            Datum::Enum(tidb_datatype::MysqlEnum::new("b", 2), Collation::Binary),
        ),
        (
            FieldType::new(FieldTypeCode::Bit).with_flen(8),
            Datum::Bit(tidb_datatype::BinaryLiteral::from(vec![0xa5])),
        ),
        (
            FieldType::new(FieldTypeCode::Json),
            Datum::Json(BinaryJSON::parse("[1,null]").unwrap()),
        ),
        (
            FieldType::new(FieldTypeCode::VectorFloat32),
            Datum::VectorFloat32(VectorFloat32::must_create(vec![1.0])),
        ),
        (FieldType::new(FieldTypeCode::VectorFloat32), Datum::Null),
    ];
    for (source, value) in cases {
        for literal_input in [false, true] {
            for parse_document in [false, true] {
                let target = FieldType::new(FieldTypeCode::Json).with_flags(if parse_document {
                    FieldTypeFlags::PARSE_TO_JSON
                } else {
                    0
                });
                let argument = if literal_input {
                    literal(value.clone(), source.clone())
                } else {
                    typed_column(0, &source)
                };
                let root = call("cast_json", target.clone(), vec![argument]);
                let schema = [source.clone()];
                for selected in [vec![], vec![0, 1, 0], (0..2050).map(|i| i % 2).collect()] {
                    let reference = NativeCastDiagnostics::default();
                    let context = NativeCastDiagnostics::default();
                    let mut input = chunk(&schema, &[vec![value.clone()], vec![Datum::Null]]);
                    input.set_sel(Some(selected.clone()));
                    let mut expected = output_for(&target);
                    let mut expected_error = None;
                    for row in selected {
                        match root.eval(&reference, input.physical_row(row)) {
                            Ok(value) => expected.append_datum(0, &value),
                            Err(error) => {
                                expected_error = Some(error);
                                break;
                            }
                        }
                    }
                    let suite = self::suite(root.clone());
                    let mut actual = output_for(&target);
                    let result =
                        suite.run_with_tikv_batch(&context, &mut input, &mut actual, &schema);
                    if let Some(expected_error) = expected_error {
                        let Err(EvaluatorError::Eval(actual_error)) = result else {
                            panic!("lost native JSON error: {result:?}");
                        };
                        assert_eq!(actual_error, expected_error);
                    } else {
                        result.unwrap();
                        assert_eq!(actual.num_rows(), expected.num_rows());
                        for row in 0..actual.num_rows() {
                            assert_eq!(
                                actual.get_row(row).is_null(0),
                                expected.get_row(row).is_null(0)
                            );
                            assert_eq!(
                                actual.column(0).get_raw(row).as_ref(),
                                expected.column(0).get_raw(row).as_ref()
                            );
                        }
                    }
                    assert_eq!(*context.0.borrow(), *reference.0.borrow());
                    let route = suite.batch_route_snapshot();
                    assert_eq!(
                        (route.prepared, route.fallback, route.refusals),
                        (1, 0, 0),
                        "literal={literal_input} document={parse_document} {source:?}"
                    );
                }
            }
        }
    }
}

#[test]
fn native_json_cast_rejects_vector_source_before_demanding_even_a_null_argument() {
    let source = FieldType::new(FieldTypeCode::VectorFloat32);
    let target = FieldType::new(FieldTypeCode::Json);
    let root = call("cast_json", target.clone(), vec![typed_column(0, &source)]);
    let suite = self::suite(root.clone());
    let schema = [source];
    let rows = vec![vec![Datum::Null]];
    let input = chunk(&schema, &rows);
    let expected = root
        .eval(&crate::NoColumns, input.physical_row(0))
        .unwrap_err();
    let mut worker = PreparedBatch::compile_calculation(
        &suite,
        0,
        &schema,
        true,
        93,
        limits(),
        ExecutionLimits::default(),
        1 << 20,
    )
    .unwrap();
    let mut source = Raw::new(&worker, rows);
    source.poison = Some(0);
    let mut output = output_for(&target);
    worker
        .validate_invocation(&suite, &input, &output, &schema)
        .unwrap();
    let error = worker
        .execute_chunk(
            &crate::NoColumns,
            &mut EvalContext::default(),
            &input,
            &mut output,
            Some(&mut source),
        )
        .unwrap_err();
    assert_eq!(error.into_evaluator_error(), EvaluatorError::Eval(expected));
    assert!(source.reads.is_empty());
    assert_eq!(output.num_rows(), 0);
}

#[test]
fn rewritten_json_cast_islands_preserve_documents_errors_and_nested_warnings() {
    use crate::rewriter::{rewrite_expr_resolved, ColumnResolver};
    use tidb_ast::{QueryStmt, SelectField, Stmt};
    struct Resolver;
    impl ColumnResolver for Resolver {
        fn time_zone(&self) -> SessionTimeZone {
            SessionTimeZone::utc()
        }
        fn resolve(&self, path: &[String]) -> Option<(usize, FieldType, i64)> {
            (path.last()?.as_str() == "v").then(|| (0, string_ty(), 1))
        }
    }
    let schema = [string_ty()];
    let rows = vec![
        vec![Datum::new_string("{\"a\":1}")],
        vec![Datum::Null],
        vec![Datum::new_string("12")],
        vec![Datum::new_string("bad JSON")],
    ];
    for sql in [
        "cast(v as json)",
        "cast(cast(v as json) as char)",
        "concat(cast(cast(v as json) as char), '!')",
        "regexp_like(cast(cast(v as json) as char), 'a|1')",
        "cast(cast(v as signed) as json)",
        "cast(cast(v as json) as json)",
    ] {
        let Stmt::Query(query) = tidb_parser::parse(&format!("select {sql}")).unwrap() else {
            panic!("query")
        };
        let QueryStmt::Select(select) = query.into_inner() else {
            panic!("select")
        };
        let SelectField::Expr { expr, .. } = &select.fields[0] else {
            panic!("expression")
        };
        let root = rewrite_expr_resolved(expr, &Resolver).unwrap();
        let field = root.static_type().unwrap().clone();
        for selected in [
            vec![],
            vec![2, 0, 1, 0],
            vec![3, 0],
            (0..2050).map(|i| i % 3).collect(),
        ] {
            let reference = NativeCastDiagnostics::default();
            let context = NativeCastDiagnostics::default();
            let mut input = chunk(&schema, &rows);
            input.set_sel(Some(selected.clone()));
            let mut expected = output_for(&field);
            let mut expected_error = None;
            for row in selected {
                match root.eval(&reference, input.physical_row(row)) {
                    Ok(value) => expected.append_datum(0, &value),
                    Err(error) => {
                        expected_error = Some(error);
                        break;
                    }
                }
            }
            let suite = self::suite(root.clone());
            let mut actual = output_for(&field);
            let result = suite.run_with_tikv_batch(&context, &mut input, &mut actual, &schema);
            if let Some(expected_error) = expected_error {
                let Err(EvaluatorError::Eval(actual_error)) = result else {
                    panic!("lost JSON error: {result:?}");
                };
                assert_eq!(actual_error, expected_error, "{sql}");
            } else {
                result.unwrap();
                assert_eq!(actual.num_rows(), expected.num_rows(), "{sql}");
                for row in 0..actual.num_rows() {
                    assert_eq!(
                        actual.get_row(row).is_null(0),
                        expected.get_row(row).is_null(0),
                        "{sql}"
                    );
                    assert_eq!(
                        actual.column(0).get_raw(row).as_ref(),
                        expected.column(0).get_raw(row).as_ref(),
                        "{sql}"
                    );
                }
            }
            assert_eq!(*context.0.borrow(), *reference.0.borrow(), "{sql}");
            let route = suite.batch_route_snapshot();
            assert_eq!(
                (route.prepared, route.fallback, route.refusals),
                (1, 0, 0),
                "{sql}"
            );
        }
    }
}

#[test]
fn native_cast_arithmetic_producers_are_prepared() {
    let schema = [ty()];
    for (case, root) in [
        call("cast_signed", ty(), vec![abs(column(0))]),
        call(
            "cast_json",
            FieldType::new(FieldTypeCode::Json),
            vec![abs(column(0))],
        ),
        call(
            "cast_char",
            string_ty(),
            vec![call("cast_signed", ty(), vec![plus(column(0), int(1))])],
        ),
        call(
            "cast_signed",
            ty(),
            vec![call("if", ty(), vec![column(0), int(2), int(3)])],
        ),
    ]
    .into_iter()
    .enumerate()
    {
        let field = root.static_type().unwrap().clone();
        let suite = self::suite(root);
        let rows = vec![vec![Datum::Int(-3)], vec![Datum::Int(2)], vec![Datum::Null]];
        let mut native = chunk(&schema, &rows);
        let mut actual_input = chunk(&schema, &rows);
        let selected = vec![1, 0, 2, 0];
        native.set_sel(Some(selected.clone()));
        actual_input.set_sel(Some(selected));
        let mut expected = output_for(&field);
        let mut actual = output_for(&field);
        suite
            .run(&crate::NoColumns, &mut native, &mut expected)
            .unwrap();
        suite
            .run_with_tikv_batch(&crate::NoColumns, &mut actual_input, &mut actual, &schema)
            .unwrap();
        assert_eq!(values_for(&actual, &field), values_for(&expected, &field));
        let route = suite.batch_route_snapshot();
        assert_eq!(
            (route.prepared, route.fallback, route.refusals),
            (1, 0, 0),
            "case {case}"
        );
        assert_eq!(route.refusal, None);
    }
}

#[test]
fn native_nullable_controls_keep_actual_values_short_circuit_and_result_metadata() {
    let json = FieldType::new(FieldTypeCode::Json).with_flags(FieldTypeFlags::PARSE_TO_JSON);
    let vector = FieldType::new(FieldTypeCode::VectorFloat32);
    let bad_signed = || {
        call(
            "cast_signed",
            ty(),
            vec![literal(
                Datum::VectorFloat32(VectorFloat32::must_create(vec![1.0])),
                vector.clone(),
            )],
        )
    };
    let bad_json = || {
        call(
            "cast_json",
            json.clone(),
            vec![literal(Datum::Null, vector.clone())],
        )
    };
    let json_null = || call("cast_json", json.clone(), vec![null()]);
    let json_doc = || {
        call(
            "cast_json",
            json.clone(),
            vec![literal(Datum::new_string("{\"a\":1}"), string_ty())],
        )
    };
    let signed = || {
        call(
            "cast_signed",
            ty(),
            vec![literal(Datum::new_string("12.5tail"), string_ty())],
        )
    };
    let time =
        tidb_datatype::parse_datetime("2026-10-18 12:34:56.123456", &chrono_tz::UTC, true, false)
            .unwrap()
            .time;
    let precise_time = FieldType::new(FieldTypeCode::Datetime).with_decimal(6);
    let merged_time = FieldType::new(FieldTypeCode::Datetime).with_decimal(3);
    let unsigned = ty().with_unsigned(true);
    let cases = [
        call("coalesce", ty(), vec![signed(), bad_signed()]),
        call("ifnull", json.clone(), vec![json_doc(), bad_json()]),
        call(
            "coalesce",
            json.clone(),
            vec![json_null(), json_doc(), bad_json()],
        ),
        call("ifnull", json.clone(), vec![json_null(), json_null()]),
        call(
            "coalesce",
            json.clone(),
            vec![json_null(), json_null(), json_null()],
        ),
        call("ifnull", json.clone(), vec![json_null(), bad_json()]),
        call(
            "ifnull",
            real_ty(),
            vec![
                literal(Datum::Real(f64::from_bits(0x7ff8000000000042)), real_ty()),
                call(
                    "cast_double",
                    real_ty(),
                    vec![literal(
                        Datum::VectorFloat32(VectorFloat32::must_create(vec![1.0])),
                        vector.clone(),
                    )],
                ),
            ],
        ),
        call(
            "coalesce",
            merged_time.clone(),
            vec![
                literal(Datum::Null, precise_time.clone()),
                literal(Datum::Time(time), precise_time),
            ],
        ),
        call(
            "cast_char",
            string_ty(),
            vec![call(
                "coalesce",
                unsigned.clone(),
                vec![
                    literal(Datum::UInt(u64::MAX), unsigned.clone()),
                    literal(Datum::Int(1), unsigned.clone()),
                ],
            )],
        ),
        call(
            "cast_char",
            string_ty(),
            vec![call(
                "ifnull",
                unsigned.clone(),
                vec![
                    literal(Datum::Int(-1), unsigned.clone()),
                    literal(Datum::UInt(u64::MAX), unsigned),
                ],
            )],
        ),
        call(
            "cast_char",
            string_ty(),
            vec![call("coalesce", ty(), vec![signed(), bad_signed()])],
        ),
        call(
            "ifnull",
            json.clone(),
            vec![
                call(
                    "cast_json",
                    json.clone(),
                    vec![literal(Datum::new_string("null"), string_ty())],
                ),
                bad_json(),
            ],
        ),
    ];
    for root in cases {
        let field = root.static_type().unwrap().clone();
        for selected in [vec![], vec![0, 1, 0], (0..2050).map(|i| i % 2).collect()] {
            let reference = NativeCastDiagnostics::default();
            let context = NativeCastDiagnostics::default();
            let mut expected = output_for(&field);
            let mut expected_error = None;
            for _ in &selected {
                match root.eval(&reference, tidb_chunk::row::Row::empty()) {
                    Ok(value) => expected.append_datum(0, &value),
                    Err(error) => {
                        expected_error = Some(error);
                        break;
                    }
                }
            }
            let suite = self::suite(root.clone());
            let mut input = Chunk::new_with_capacity(&[], 2);
            input.set_num_virtual_rows(2);
            input.set_sel(Some(selected));
            let mut actual = output_for(&field);
            let result = suite.run_with_tikv_batch(&context, &mut input, &mut actual, &[]);
            if let Some(expected_error) = expected_error {
                let Err(EvaluatorError::Eval(actual_error)) = result else {
                    panic!("lost nullable error: {result:?}");
                };
                assert_eq!(actual_error, expected_error);
            } else {
                result.unwrap();
                assert_eq!(actual.num_rows(), expected.num_rows());
                for row in 0..actual.num_rows() {
                    assert_eq!(
                        actual.get_row(row).is_null(0),
                        expected.get_row(row).is_null(0)
                    );
                    assert_eq!(
                        actual.column(0).get_raw(row).as_ref(),
                        expected.column(0).get_raw(row).as_ref()
                    );
                }
            }
            assert_eq!(*context.0.borrow(), *reference.0.borrow());
            let route = suite.batch_route_snapshot();
            assert_eq!(
                (route.prepared, route.fallback, route.refusals),
                (1, 0, 0),
                "{root:?}"
            );
        }
    }
}

#[test]
fn rewritten_native_nullable_controls_keep_selected_columns_and_skipped_json_errors() {
    use crate::rewriter::{rewrite_expr_resolved, ColumnResolver};
    use tidb_ast::{QueryStmt, SelectField, Stmt};
    struct Resolver;
    impl ColumnResolver for Resolver {
        fn time_zone(&self) -> SessionTimeZone {
            SessionTimeZone::utc()
        }
        fn resolve(&self, path: &[String]) -> Option<(usize, FieldType, i64)> {
            let index = match path.last()?.as_str() {
                "v" => 0,
                "w" => 1,
                _ => return None,
            };
            Some((index, string_ty(), index as i64 + 1))
        }
    }
    let schema = [string_ty(), string_ty()];
    let rows = vec![
        vec![
            Datum::new_string("{\"a\":1}"),
            Datum::new_string("bad JSON"),
        ],
        vec![Datum::Null, Datum::new_string("12")],
        vec![Datum::Null, Datum::new_string("bad JSON")],
        vec![Datum::new_string("null"), Datum::new_string("bad JSON")],
        vec![Datum::new_string("bad JSON"), Datum::new_string("12")],
    ];
    for sql in [
        "coalesce(cast(v as json), cast(w as json))",
        "ifnull(cast(v as json), cast(w as json))",
        "cast(coalesce(cast(v as json), cast(w as json)) as char)",
        "concat(cast(ifnull(cast(v as json), cast(w as json)) as char), '!')",
        "regexp_like(cast(coalesce(cast(v as json), cast(w as json)) as char), 'a|1')",
        "coalesce(cast(null as json), ifnull(cast(v as json), cast(w as json)))",
    ] {
        let Stmt::Query(query) = tidb_parser::parse(&format!("select {sql}")).unwrap() else {
            panic!("query")
        };
        let QueryStmt::Select(select) = query.into_inner() else {
            panic!("select")
        };
        let SelectField::Expr { expr, .. } = &select.fields[0] else {
            panic!("expression")
        };
        let root = rewrite_expr_resolved(expr, &Resolver).unwrap();
        let field = root.static_type().unwrap().clone();
        for selected in [
            vec![],
            vec![0, 1, 3, 0],
            vec![2, 0],
            vec![4, 0],
            (0..2050).map(|i| [0, 1, 3][i % 3]).collect(),
        ] {
            let reference = NativeCastDiagnostics::default();
            let context = NativeCastDiagnostics::default();
            let mut input = chunk(&schema, &rows);
            input.set_sel(Some(selected.clone()));
            let mut expected = output_for(&field);
            let mut expected_error = None;
            for row in selected {
                match root.eval(&reference, input.physical_row(row)) {
                    Ok(value) => expected.append_datum(0, &value),
                    Err(error) => {
                        expected_error = Some(error);
                        break;
                    }
                }
            }
            let suite = self::suite(root.clone());
            let mut actual = output_for(&field);
            let result = suite.run_with_tikv_batch(&context, &mut input, &mut actual, &schema);
            if let Some(expected_error) = expected_error {
                let Err(EvaluatorError::Eval(actual_error)) = result else {
                    panic!("lost nullable SQL error: {result:?}");
                };
                assert_eq!(actual_error, expected_error, "{sql}");
            } else {
                result.unwrap();
                assert_eq!(actual.num_rows(), expected.num_rows(), "{sql}");
                for row in 0..actual.num_rows() {
                    assert_eq!(
                        actual.get_row(row).is_null(0),
                        expected.get_row(row).is_null(0),
                        "{sql}"
                    );
                    assert_eq!(
                        actual.column(0).get_raw(row).as_ref(),
                        expected.column(0).get_raw(row).as_ref(),
                        "{sql}"
                    );
                }
            }
            assert_eq!(*context.0.borrow(), *reference.0.borrow(), "{sql}");
            let route = suite.batch_route_snapshot();
            assert_eq!(
                (route.prepared, route.fallback, route.refusals),
                (1, 0, 0),
                "{sql}"
            );
        }
    }
}

#[test]
fn native_nullable_node_policy_precedes_argument_declaration_capability_scan() {
    let root = call(
        "coalesce",
        ty(),
        vec![
            call("cast_signed", ty(), vec![int(1)]),
            Expression::Constant(Constant::default()),
        ],
    );
    let suite = self::suite(root);
    let mut source_limits = limits();
    source_limits.tree.max_nodes = 1;
    let error = match PreparedBatchSuite::compile(
        &suite,
        &[],
        true,
        93,
        source_limits,
        ExecutionLimits::default(),
        1 << 20,
    ) {
        Ok(_) => panic!("the source exceeds its node policy"),
        Err(error) => error,
    };
    assert!(matches!(
        error.local_error(),
        Some(LocalError::ResourceLimit(_))
    ));
}

#[test]
fn native_if_case_controls_preserve_truth_demand_and_selected_value_kind() {
    let json = FieldType::new(FieldTypeCode::Json).with_flags(FieldTypeFlags::PARSE_TO_JSON);
    let vector = FieldType::new(FieldTypeCode::VectorFloat32);
    let bad_signed = || {
        call(
            "cast_signed",
            ty(),
            vec![literal(
                Datum::VectorFloat32(VectorFloat32::must_create(vec![1.0])),
                vector.clone(),
            )],
        )
    };
    let bad_json = || {
        call(
            "cast_json",
            json.clone(),
            vec![literal(Datum::Null, vector.clone())],
        )
    };
    let json_value = |text: &str| {
        call(
            "cast_json",
            json.clone(),
            vec![literal(Datum::new_string(text), string_ty())],
        )
    };
    let signed = || {
        call(
            "cast_signed",
            ty(),
            vec![literal(Datum::new_string("12.5tail"), string_ty())],
        )
    };
    let condition = || call("cast_signed", ty(), vec![int(1)]);
    let unsigned = ty().with_unsigned(true);
    let cases = [
        call("if", ty(), vec![condition(), signed(), bad_signed()]),
        call(
            "if",
            json.clone(),
            vec![null(), bad_json(), json_value("{\"a\":1}")],
        ),
        call(
            "case",
            json.clone(),
            vec![
                int(0),
                bad_json(),
                condition(),
                json_value("{\"a\":1}"),
                bad_json(),
            ],
        ),
        call(
            "case",
            json.clone(),
            vec![int(0), bad_json(), null(), bad_json()],
        ),
        call(
            "if",
            json.clone(),
            vec![json_value("null"), json_value("{\"a\":1}"), bad_json()],
        ),
        call(
            "if",
            ty(),
            vec![
                literal(Datum::new_string("1abc"), string_ty()),
                int(7),
                int(8),
            ],
        ),
        call("if", ty(), vec![json_value("false"), int(7), int(8)]),
        call(
            "if",
            ty(),
            vec![
                literal(Datum::Real(f64::from_bits(0x7ff8000000000042)), real_ty()),
                signed(),
                bad_signed(),
            ],
        ),
        call(
            "case",
            ty(),
            vec![
                literal(Datum::new_string("0"), string_ty()),
                bad_signed(),
                literal(Datum::new_string("2"), string_ty()),
                signed(),
            ],
        ),
        call(
            "case",
            ty(),
            vec![literal(Datum::new_string("0"), string_ty()), int(7)],
        ),
        call(
            "cast_char",
            string_ty(),
            vec![call(
                "if",
                unsigned.clone(),
                vec![
                    condition(),
                    literal(Datum::Int(-1), unsigned.clone()),
                    literal(Datum::UInt(u64::MAX), unsigned),
                ],
            )],
        ),
        call(
            "coalesce",
            json.clone(),
            vec![
                call("case", json.clone(), vec![int(0), bad_json()]),
                call(
                    "if",
                    json.clone(),
                    vec![condition(), json_value("{\"a\":1}"), bad_json()],
                ),
            ],
        ),
        call("if", ty(), vec![int(0), signed(), bad_signed()]),
        call(
            "case",
            json.clone(),
            vec![int(0), json_value("{\"a\":1}"), bad_json()],
        ),
    ];
    for root in cases {
        let field = root.static_type().unwrap().clone();
        for selected in [vec![], vec![0, 1, 0], (0..2050).map(|i| i % 2).collect()] {
            let reference = NativeCastDiagnostics::default();
            let context = NativeCastDiagnostics::default();
            let mut expected = output_for(&field);
            let mut expected_error = None;
            for _ in &selected {
                match root.eval(&reference, tidb_chunk::row::Row::empty()) {
                    Ok(value) => expected.append_datum(0, &value),
                    Err(error) => {
                        expected_error = Some(error);
                        break;
                    }
                }
            }
            let suite = self::suite(root.clone());
            let mut input = Chunk::new_with_capacity(&[], 2);
            input.set_num_virtual_rows(2);
            input.set_sel(Some(selected));
            let mut actual = output_for(&field);
            let result = suite.run_with_tikv_batch(&context, &mut input, &mut actual, &[]);
            if let Some(expected_error) = expected_error {
                let Err(EvaluatorError::Eval(actual_error)) = result else {
                    panic!("lost conditional error: {result:?}");
                };
                assert_eq!(actual_error, expected_error);
            } else {
                result.unwrap();
                assert_eq!(actual.num_rows(), expected.num_rows());
                for row in 0..actual.num_rows() {
                    assert_eq!(
                        actual.get_row(row).is_null(0),
                        expected.get_row(row).is_null(0)
                    );
                    assert_eq!(
                        actual.column(0).get_raw(row).as_ref(),
                        expected.column(0).get_raw(row).as_ref()
                    );
                }
            }
            assert_eq!(*context.0.borrow(), *reference.0.borrow());
            let route = suite.batch_route_snapshot();
            assert_eq!(
                (route.prepared, route.fallback, route.refusals),
                (1, 0, 0),
                "{root:?}"
            );
        }
    }
}

#[test]
fn rewritten_native_if_case_controls_keep_column_branches_and_json_errors() {
    use crate::rewriter::{rewrite_expr_resolved, ColumnResolver};
    use tidb_ast::{QueryStmt, SelectField, Stmt};
    struct Resolver;
    impl ColumnResolver for Resolver {
        fn time_zone(&self) -> SessionTimeZone {
            SessionTimeZone::utc()
        }
        fn resolve(&self, path: &[String]) -> Option<(usize, FieldType, i64)> {
            match path.last()?.as_str() {
                "c" => Some((0, ty(), 1)),
                "v" => Some((1, string_ty(), 2)),
                "w" => Some((2, string_ty(), 3)),
                _ => None,
            }
        }
    }
    let schema = [ty(), string_ty(), string_ty()];
    let rows = vec![
        vec![
            Datum::Int(1),
            Datum::new_string("{\"a\":1}"),
            Datum::new_string("bad JSON"),
        ],
        vec![
            Datum::Int(0),
            Datum::new_string("bad JSON"),
            Datum::new_string("12"),
        ],
        vec![
            Datum::Null,
            Datum::new_string("bad JSON"),
            Datum::new_string("null"),
        ],
        vec![
            Datum::Int(1),
            Datum::new_string("bad JSON"),
            Datum::new_string("12"),
        ],
        vec![
            Datum::Int(0),
            Datum::new_string("{\"a\":1}"),
            Datum::new_string("bad JSON"),
        ],
    ];
    for sql in [
        "if(c, cast(v as json), cast(w as json))",
        "case when c then cast(v as json) else cast(w as json) end",
        "case when c then cast(v as json) end",
        "cast(if(c, cast(v as json), cast(w as json)) as char)",
        "concat(cast(case when c then cast(v as json) else cast(w as json) end as char), '!')",
        "coalesce(case when c then cast(v as json) end, cast(w as json))",
    ] {
        let Stmt::Query(query) = tidb_parser::parse(&format!("select {sql}")).unwrap() else {
            panic!("query")
        };
        let QueryStmt::Select(select) = query.into_inner() else {
            panic!("select")
        };
        let SelectField::Expr { expr, .. } = &select.fields[0] else {
            panic!("expression")
        };
        let root = rewrite_expr_resolved(expr, &Resolver).unwrap();
        let field = root.static_type().unwrap().clone();
        for selected in [
            vec![],
            vec![0, 1, 2, 0],
            vec![3, 0],
            vec![4, 0],
            (0..2050).map(|i| i % 3).collect(),
        ] {
            let reference = NativeCastDiagnostics::default();
            let context = NativeCastDiagnostics::default();
            let mut input = chunk(&schema, &rows);
            input.set_sel(Some(selected.clone()));
            let mut expected = output_for(&field);
            let mut expected_error = None;
            for row in selected {
                match root.eval(&reference, input.physical_row(row)) {
                    Ok(value) => expected.append_datum(0, &value),
                    Err(error) => {
                        expected_error = Some(error);
                        break;
                    }
                }
            }
            let suite = self::suite(root.clone());
            let mut actual = output_for(&field);
            let result = suite.run_with_tikv_batch(&context, &mut input, &mut actual, &schema);
            if let Some(expected_error) = expected_error {
                let Err(EvaluatorError::Eval(actual_error)) = result else {
                    panic!("lost conditional SQL error: {result:?}");
                };
                assert_eq!(actual_error, expected_error, "{sql}");
            } else {
                result.unwrap();
                assert_eq!(actual.num_rows(), expected.num_rows(), "{sql}");
                for row in 0..actual.num_rows() {
                    assert_eq!(
                        actual.get_row(row).is_null(0),
                        expected.get_row(row).is_null(0),
                        "{sql}"
                    );
                    assert_eq!(
                        actual.column(0).get_raw(row).as_ref(),
                        expected.column(0).get_raw(row).as_ref(),
                        "{sql}"
                    );
                }
            }
            assert_eq!(*context.0.borrow(), *reference.0.borrow(), "{sql}");
            let route = suite.batch_route_snapshot();
            assert_eq!(
                (route.prepared, route.fallback, route.refusals),
                (1, 0, 0),
                "{sql}"
            );
        }
    }
}

#[test]
fn native_predicate_resource_and_provenance_failures_are_terminal() {
    use tidb_query_expr::local::{HostArgReply, HostInvocation, HostStart, LocalHostServices};
    for (operation, name, nodes) in [
        (SqlBatchHostOperation::IfNative, "if", vec![3, 4, 8]),
        (SqlBatchHostOperation::UnaryNotNative, "not", vec![3]),
        (SqlBatchHostOperation::IsNullNative, "isnull", vec![3]),
        (SqlBatchHostOperation::IsTrueNative, "istrue", vec![3]),
        (
            SqlBatchHostOperation::IsTrueWithNullNative,
            "istrue_with_null",
            vec![3],
        ),
        (SqlBatchHostOperation::IsFalseNative, "isfalse", vec![3]),
        (SqlBatchHostOperation::IsNotTrueNative, "isnottrue", vec![3]),
        (
            SqlBatchHostOperation::IsNotFalseNative,
            "isnotfalse",
            vec![3],
        ),
        (SqlBatchHostOperation::IsUnknownNative, "isunknown", vec![3]),
        (
            SqlBatchHostOperation::IsNotUnknownNative,
            "isnotunknown",
            vec![3],
        ),
    ] {
        let field = projected(&native_bytes_type(), true).unwrap();
        let args = vec![field.clone(); nodes.len()];
        let catalog = HostCatalog::for_sql_batch(vec![(
            operation,
            HostSignature {
                arg_types: args.clone().into_boxed_slice(),
                return_type: field.clone(),
            },
        )])
        .unwrap();
        let slot = catalog.slot(0).unwrap();
        let metadata = [NativeHostMetadata::Selection {
            name: name.into(),
            result_type: ty(),
            argument_nodes: nodes.into_boxed_slice(),
            output_node: 2,
            output_role: NativeValueRole::Output,
        }];
        let store = RefCell::new(NativeValueStore::new(1 << 20).unwrap());
        store.borrow_mut().begin_window().unwrap();
        let row = InputRow {
            occurrence: 0,
            input_row: 0,
        };
        let origin = NativeValueOrigin { node: 3, row };
        let large = store
            .borrow_mut()
            .insert_clone(
                &Datum::Bytes(vec![b'1'; 16384]),
                NativeValueRole::CharInput,
                origin,
            )
            .unwrap();
        let wrong = store
            .borrow_mut()
            .insert_clone(&Datum::Int(1), NativeValueRole::Output, origin)
            .unwrap();
        let mut provider = NativeBatchHosts::new(catalog, &metadata, &crate::NoColumns, 512)
            .with_native_values(Some(&store));
        let mut ctx = EvalContext::default();
        for (token, resource_failure) in [(large, true), (wrong, false)] {
            let HostStart::Pending { task, .. } = provider
                .start(
                    &mut ctx,
                    HostInvocation {
                        slot,
                        row,
                        arg_types: &args,
                        return_type: &field,
                    },
                )
                .unwrap()
            else {
                panic!("request")
            };
            let values = VectorValue::from_scalar(&ScalarValue::Bytes(Some(token)), 1);
            let error = provider
                .resume(
                    &mut ctx,
                    &task,
                    HostArgReply {
                        index: 0,
                        field_type: &field,
                        values: &values,
                    },
                )
                .unwrap_err();
            if resource_failure {
                assert!(matches!(error, LocalError::ResourceLimit(_)));
            } else {
                assert!(matches!(error, LocalError::BindingContract(_)));
            }
            assert!(provider.take_native_error().is_none());
            provider.cancel(&task);
        }
    }
}

#[test]
fn native_logical_controls_keep_three_valued_demand_and_string_warnings() {
    let json = FieldType::new(FieldTypeCode::Json).with_flags(FieldTypeFlags::PARSE_TO_JSON);
    let text = |text: &str| literal(Datum::new_string(text), string_ty());
    let cast = |text: &str| {
        call(
            "cast_signed",
            ty(),
            vec![literal(Datum::new_string(text), string_ty())],
        )
    };
    let bad = || {
        call(
            "cast_json",
            json.clone(),
            vec![literal(
                Datum::Null,
                FieldType::new(FieldTypeCode::VectorFloat32),
            )],
        )
    };
    let json_null = || call("cast_json", json.clone(), vec![text("null")]);
    let null_cast = || call("cast_signed", ty(), vec![null()]);
    let cases = [
        call("and", ty(), vec![cast("0abc"), bad()]),
        call("or", ty(), vec![cast("1abc"), bad()]),
        call("and", ty(), vec![null_cast(), int(0)]),
        call("and", ty(), vec![null_cast(), int(1)]),
        call("or", ty(), vec![null_cast(), int(1)]),
        call("or", ty(), vec![null_cast(), int(0)]),
        call("and", ty(), vec![text("1abc"), bad()]),
        call("or", ty(), vec![text("0abc"), bad()]),
        call("and", ty(), vec![text("1abc"), text("2tail")]),
        call("or", ty(), vec![text("0abc"), text("0tail")]),
        call(
            "and",
            ty(),
            vec![literal(Datum::Real(f64::NAN), real_ty()), cast("1")],
        ),
        call("or", ty(), vec![json_null(), bad()]),
        call("and", ty(), vec![null(), bad()]),
        call(
            "cast_char",
            string_ty(),
            vec![call("and", ty(), vec![cast("0abc"), bad()])],
        ),
        call(
            "if",
            json.clone(),
            vec![
                call("or", ty(), vec![text("1abc"), bad()]),
                json_null(),
                bad(),
            ],
        ),
        call("and", ty(), vec![null_cast(), null_cast()]),
        call("or", ty(), vec![null_cast(), null_cast()]),
    ];
    for root in cases {
        let field = root.static_type().unwrap().clone();
        for selected in [vec![], vec![0, 1, 0], (0..2050).map(|i| i % 2).collect()] {
            let reference = NativeCastDiagnostics::default();
            let context = NativeCastDiagnostics::default();
            let mut expected = output_for(&field);
            let mut expected_error = None;
            for _ in &selected {
                match root.eval(&reference, tidb_chunk::row::Row::empty()) {
                    Ok(value) => expected.append_datum(0, &value),
                    Err(error) => {
                        expected_error = Some(error);
                        break;
                    }
                }
            }
            let suite = self::suite(root.clone());
            let mut input = Chunk::new_with_capacity(&[], 2);
            input.set_num_virtual_rows(2);
            input.set_sel(Some(selected));
            let mut actual = output_for(&field);
            let result = suite.run_with_tikv_batch(&context, &mut input, &mut actual, &[]);
            if let Some(expected_error) = expected_error {
                let Err(EvaluatorError::Eval(actual_error)) = result else {
                    panic!("lost logical error: {result:?}");
                };
                assert_eq!(actual_error, expected_error);
            } else {
                result.unwrap();
                assert_eq!(actual.num_rows(), expected.num_rows());
                for row in 0..actual.num_rows() {
                    assert_eq!(
                        actual.get_row(row).is_null(0),
                        expected.get_row(row).is_null(0)
                    );
                    assert_eq!(
                        actual.column(0).get_raw(row).as_ref(),
                        expected.column(0).get_raw(row).as_ref()
                    );
                }
            }
            assert_eq!(*context.0.borrow(), *reference.0.borrow());
            let route = suite.batch_route_snapshot();
            assert_eq!(
                (route.prepared, route.fallback, route.refusals),
                (1, 0, 0),
                "{root:?}"
            );
        }
    }
}

#[test]
fn rewritten_native_logical_controls_preserve_warning_prefix_and_nullable_columns() {
    use crate::rewriter::{rewrite_expr_resolved, ColumnResolver};
    use tidb_ast::{QueryStmt, SelectField, Stmt};
    struct Resolver;
    impl ColumnResolver for Resolver {
        fn time_zone(&self) -> SessionTimeZone {
            SessionTimeZone::utc()
        }
        fn resolve(&self, path: &[String]) -> Option<(usize, FieldType, i64)> {
            let index = match path.last()?.as_str() {
                "l" => 0,
                "r" => 1,
                _ => return None,
            };
            Some((index, string_ty(), index as i64 + 1))
        }
    }
    let schema = [string_ty(), string_ty()];
    let rows = vec![
        vec![Datum::new_string("0abc"), Datum::new_string("bad JSON")],
        vec![Datum::new_string("1abc"), Datum::new_string("bad JSON")],
        vec![Datum::Null, Datum::new_string("0")],
        vec![Datum::Null, Datum::new_string("1")],
        vec![Datum::Null, Datum::Null],
        vec![Datum::new_string("0abc"), Datum::new_string("2tail")],
    ];
    for sql in [
        "cast(l as signed) and cast(r as signed)",
        "l and cast(r as json)",
        "l or cast(r as json)",
        "cast((l and cast(r as json)) as char)",
        "if(l or cast(r as json), cast('1' as json), cast('0' as json))",
        "coalesce(l and cast(r as json), cast('9' as signed))",
    ] {
        let Stmt::Query(query) = tidb_parser::parse(&format!("select {sql}")).unwrap() else {
            panic!("query")
        };
        let QueryStmt::Select(select) = query.into_inner() else {
            panic!("select")
        };
        let SelectField::Expr { expr, .. } = &select.fields[0] else {
            panic!("expression")
        };
        let root = rewrite_expr_resolved(expr, &Resolver).unwrap();
        let field = root.static_type().unwrap().clone();
        for selected in [
            vec![],
            vec![0],
            vec![1],
            vec![2, 3, 4],
            vec![5, 2],
            (0..2050).map(|i| [2, 3, 4][i % 3]).collect(),
        ] {
            let reference = NativeCastDiagnostics::default();
            let context = NativeCastDiagnostics::default();
            let mut input = chunk(&schema, &rows);
            input.set_sel(Some(selected.clone()));
            let mut expected = output_for(&field);
            let mut expected_error = None;
            for row in selected {
                match root.eval(&reference, input.physical_row(row)) {
                    Ok(value) => expected.append_datum(0, &value),
                    Err(error) => {
                        expected_error = Some(error);
                        break;
                    }
                }
            }
            let suite = self::suite(root.clone());
            let mut actual = output_for(&field);
            let result = suite.run_with_tikv_batch(&context, &mut input, &mut actual, &schema);
            if let Some(expected_error) = expected_error {
                let Err(EvaluatorError::Eval(actual_error)) = result else {
                    panic!("lost logical SQL error: {result:?}");
                };
                assert_eq!(actual_error, expected_error, "{sql}");
            } else {
                result.unwrap();
                assert_eq!(actual.num_rows(), expected.num_rows(), "{sql}");
                for row in 0..actual.num_rows() {
                    assert_eq!(
                        actual.get_row(row).is_null(0),
                        expected.get_row(row).is_null(0),
                        "{sql}"
                    );
                    assert_eq!(
                        actual.column(0).get_raw(row).as_ref(),
                        expected.column(0).get_raw(row).as_ref(),
                        "{sql}"
                    );
                }
            }
            assert_eq!(*context.0.borrow(), *reference.0.borrow(), "{sql}");
            let route = suite.batch_route_snapshot();
            assert_eq!(
                (route.prepared, route.fallback, route.refusals),
                (1, 0, 0),
                "{sql}"
            );
        }
    }
}

#[test]
fn native_logical_input_policy_and_role_rejection_do_not_run_the_value_kernel() {
    use tidb_query_expr::local::{HostArgReply, HostInvocation, HostStart, LocalHostServices};
    let field = projected(&native_bytes_type(), true).unwrap();
    let args = vec![field.clone(); 2];
    let catalog = HostCatalog::for_sql_batch(vec![(
        SqlBatchHostOperation::AndNative,
        HostSignature {
            arg_types: args.clone().into_boxed_slice(),
            return_type: field.clone(),
        },
    )])
    .unwrap();
    let slot = catalog.slot(0).unwrap();
    let metadata = [NativeHostMetadata::Selection {
        name: "and".into(),
        result_type: ty(),
        argument_nodes: vec![3, 8].into_boxed_slice(),
        output_node: 2,
        output_role: NativeValueRole::Output,
    }];
    let store = RefCell::new(NativeValueStore::new(1 << 20).unwrap());
    store.borrow_mut().begin_window().unwrap();
    let row = InputRow {
        occurrence: 0,
        input_row: 0,
    };
    let origin = NativeValueOrigin { node: 3, row };
    let large = store
        .borrow_mut()
        .insert_clone(
            &Datum::Bytes(vec![b'1'; 16384]),
            NativeValueRole::CharInput,
            origin,
        )
        .unwrap();
    let wrong = store
        .borrow_mut()
        .insert_clone(&Datum::Int(1), NativeValueRole::Output, origin)
        .unwrap();
    let mut provider = NativeBatchHosts::new(catalog, &metadata, &crate::NoColumns, 512)
        .with_native_values(Some(&store));
    let mut ctx = EvalContext::default();
    for (token, resource_failure) in [(large, true), (wrong, false)] {
        let HostStart::Pending { task, .. } = provider
            .start(
                &mut ctx,
                HostInvocation {
                    slot,
                    row,
                    arg_types: &args,
                    return_type: &field,
                },
            )
            .unwrap()
        else {
            panic!("request")
        };
        let values = VectorValue::from_scalar(&ScalarValue::Bytes(Some(token)), 1);
        let error = provider
            .resume(
                &mut ctx,
                &task,
                HostArgReply {
                    index: 0,
                    field_type: &field,
                    values: &values,
                },
            )
            .unwrap_err();
        if resource_failure {
            assert!(matches!(error, LocalError::ResourceLimit(_)));
        } else {
            assert!(matches!(error, LocalError::BindingContract(_)));
        }
        assert!(provider.take_native_error().is_none());
        provider.cancel(&task);
    }
}

#[test]
fn native_boolean_predicates_keep_null_truth_and_validation_in_native_parents() {
    let json = FieldType::new(FieldTypeCode::Json);
    let mut cases = vec![
        (Datum::Null, ty()),
        (Datum::Int(0), ty()),
        (
            Datum::UInt(u64::MAX),
            ty().with_flags(FieldTypeFlags::UNSIGNED),
        ),
        (Datum::new_string("0tail"), string_ty()),
        (Datum::Bytes(b"1tail".to_vec()), string_ty()),
        (Datum::Real(f64::NAN), FieldType::new(FieldTypeCode::Double)),
        (
            Datum::Json(BinaryJSON::parse("null").unwrap()),
            json.clone(),
        ),
        (Datum::Json(BinaryJSON::parse("false").unwrap()), json),
        (Datum::MinNotNull, ty()),
        (Datum::MaxValue, ty()),
    ];
    let time =
        tidb_datatype::parse_datetime("2026-10-17 12:34:56.123456", &chrono_tz::UTC, true, false)
            .unwrap()
            .time;
    use tidb_datatype::{BinaryLiteral, MysqlEnum, MysqlSet};
    for value in [
        Datum::Decimal(Decimal::from_literal("-0.0010")),
        Datum::Float32(-0.0),
        Datum::BinaryLiteral(BinaryLiteral::from(vec![0, b'A'])),
        Datum::Duration(MySqlDuration::from_nanoseconds(-123456789012, 4).unwrap()),
        Datum::Enum(MysqlEnum::new("b", 2), Collation::GbkBin),
        Datum::Bit(BinaryLiteral::from(vec![0, b'A'])),
        Datum::Set(MysqlSet::new("a,b", 3), Collation::GbkBin),
        Datum::Time(time),
        Datum::Raw(vec![0xff, 0]),
        Datum::VectorFloat32(VectorFloat32::must_create(vec![1.0, -0.0])),
    ] {
        cases.push((value, string_ty()));
    }
    for name in [
        "not",
        "isnull",
        "istrue",
        "istrue_with_null",
        "isnottrue",
        "isfalse",
        "isnotfalse",
        "isunknown",
        "isnotunknown",
    ] {
        for (value, field) in &cases {
            let child = call(
                name,
                ty(),
                vec![Expression::Constant(Constant::new(
                    value.clone(),
                    field.clone(),
                ))],
            );
            let root = call("cast_signed", ty(), vec![child]);
            for selected in [vec![], vec![0, 1, 0], (0..2050).map(|i| i % 2).collect()] {
                let reference = NativeCastDiagnostics::default();
                let context = NativeCastDiagnostics::default();
                let mut expected = output_for(&ty());
                let mut expected_error = None;
                for _ in &selected {
                    match root.eval(&reference, tidb_chunk::row::Row::empty()) {
                        Ok(value) => expected.append_datum(0, &value),
                        Err(error) => {
                            expected_error = Some(error);
                            break;
                        }
                    }
                }
                let suite = self::suite(root.clone());
                let mut input = Chunk::new_with_capacity(&[], 2);
                input.set_num_virtual_rows(2);
                input.set_sel(Some(selected));
                let mut actual = output_for(&ty());
                let result = suite.run_with_tikv_batch(&context, &mut input, &mut actual, &[]);
                if let Some(expected_error) = expected_error {
                    let Err(EvaluatorError::Eval(actual_error)) = result else {
                        panic!("lost boolean error {name}: {result:?}");
                    };
                    assert_eq!(actual_error, expected_error, "{name} {value:?}");
                } else {
                    result.unwrap();
                    assert_eq!(actual.num_rows(), expected.num_rows());
                    for row in 0..actual.num_rows() {
                        assert_eq!(
                            actual.get_row(row).is_null(0),
                            expected.get_row(row).is_null(0)
                        );
                        assert_eq!(
                            actual.column(0).get_raw(row).as_ref(),
                            expected.column(0).get_raw(row).as_ref(),
                            "{name} {value:?}"
                        );
                    }
                }
                assert_eq!(*context.0.borrow(), *reference.0.borrow());
                let route = suite.batch_route_snapshot();
                assert_eq!(
                    (route.prepared, route.fallback, route.refusals),
                    (1, 0, 0),
                    "{name} {value:?}"
                );
            }
        }
    }
}

#[test]
fn rewritten_native_boolean_predicates_preserve_diagnostics_and_composition() {
    use crate::rewriter::{rewrite_expr_resolved, ColumnResolver};
    use tidb_ast::{QueryStmt, SelectField, Stmt};
    struct Resolver;
    impl ColumnResolver for Resolver {
        fn time_zone(&self) -> SessionTimeZone {
            SessionTimeZone::utc()
        }
        fn resolve(&self, path: &[String]) -> Option<(usize, FieldType, i64)> {
            let index = match path.last()?.as_str() {
                "l" => 0,
                "r" => 1,
                _ => return None,
            };
            Some((index, string_ty(), index as i64 + 1))
        }
    }
    let schema = [string_ty(), string_ty()];
    let rows = vec![
        vec![Datum::new_string("0abc"), Datum::new_string("bad JSON")],
        vec![Datum::new_string("1abc"), Datum::new_string("bad JSON")],
        vec![Datum::Null, Datum::new_string("0")],
        vec![Datum::Null, Datum::new_string("1")],
        vec![Datum::Null, Datum::Null],
        vec![Datum::new_string("0abc"), Datum::new_string("2tail")],
    ];
    for sql in [
        "cast((not l) as signed)",
        "cast((l is null) as signed)",
        "cast((l is true) as signed)",
        "cast((l is false) as signed)",
        "cast((l is not true) as signed)",
        "cast((l is not false) as signed)",
        "cast((l is unknown) as signed)",
        "cast((l is not unknown) as signed)",
        "if(not l, cast('1' as json), cast('0' as json))",
        "cast((cast(r as json) is true) as char)",
        "coalesce(cast((not l) as signed), cast('9' as signed))",
        "cast((not (l or cast(r as json))) as signed)",
    ] {
        let Stmt::Query(query) = tidb_parser::parse(&format!("select {sql}")).unwrap() else {
            panic!("query")
        };
        let QueryStmt::Select(select) = query.into_inner() else {
            panic!("select")
        };
        let SelectField::Expr { expr, .. } = &select.fields[0] else {
            panic!("expression")
        };
        let root = rewrite_expr_resolved(expr, &Resolver).unwrap();
        let field = root.static_type().unwrap().clone();
        for selected in [
            vec![],
            vec![0],
            vec![1],
            vec![2, 3, 4],
            vec![5, 2],
            (0..2050).map(|i| [2, 3, 4][i % 3]).collect(),
        ] {
            let reference = NativeCastDiagnostics::default();
            let context = NativeCastDiagnostics::default();
            let mut input = chunk(&schema, &rows);
            input.set_sel(Some(selected.clone()));
            let mut expected = output_for(&field);
            let mut expected_error = None;
            for row in selected {
                match root.eval(&reference, input.physical_row(row)) {
                    Ok(value) => expected.append_datum(0, &value),
                    Err(error) => {
                        expected_error = Some(error);
                        break;
                    }
                }
            }
            let suite = self::suite(root.clone());
            let mut actual = output_for(&field);
            let result = suite.run_with_tikv_batch(&context, &mut input, &mut actual, &schema);
            if let Some(expected_error) = expected_error {
                let Err(EvaluatorError::Eval(actual_error)) = result else {
                    panic!("lost logical SQL error: {result:?}");
                };
                assert_eq!(actual_error, expected_error, "{sql}");
            } else {
                result.unwrap();
                assert_eq!(actual.num_rows(), expected.num_rows(), "{sql}");
                for row in 0..actual.num_rows() {
                    assert_eq!(
                        actual.get_row(row).is_null(0),
                        expected.get_row(row).is_null(0),
                        "{sql}"
                    );
                    assert_eq!(
                        actual.column(0).get_raw(row).as_ref(),
                        expected.column(0).get_raw(row).as_ref(),
                        "{sql}"
                    );
                }
            }
            assert_eq!(*context.0.borrow(), *reference.0.borrow(), "{sql}");
            let route = suite.batch_route_snapshot();
            assert_eq!(
                (route.prepared, route.fallback, route.refusals),
                (1, 0, 0),
                "{sql}"
            );
        }
    }
}

#[test]
fn native_comparison_producers_preserve_values_diagnostics_and_raw_operand_demand() {
    let json = FieldType::new(FieldTypeCode::Json).with_flags(FieldTypeFlags::PARSE_TO_JSON);
    let text = |s: &str| literal(Datum::new_string(s), string_ty());
    let cast = |s: &str| call("cast_signed", ty(), vec![text(s)]);
    let json_value = |s: &str| call("cast_json", json.clone(), vec![text(s)]);
    let unsigned = || literal(Datum::Int(-1), ty().with_unsigned(true));
    let decimal = |s: &str| {
        literal(
            Datum::Decimal(Decimal::from_literal(s)),
            FieldType::new(FieldTypeCode::NewDecimal),
        )
    };
    let pairs = vec![
        (null(), null()),
        (null(), int(0)),
        (int(0), null()),
        (cast("1tail"), int(2)),
        (unsigned(), int(-1)),
        (int(-1), unsigned()),
        (
            unsigned(),
            literal(Datum::UInt(u64::MAX), ty().with_unsigned(true)),
        ),
        (literal(Datum::Real(f64::NAN), real_ty()), cast("0")),
        (
            decimal("1.000000000000000000000000000000000000001"),
            decimal("1.000000000000000000000000000000000000002"),
        ),
        (text("A"), text("a")),
        (json_value("null"), json_value("false")),
        (json_value("[1,0]"), text("[1,0]")),
        (
            literal(
                Datum::VectorFloat32(VectorFloat32::must_create(vec![1.0, 0.0])),
                FieldType::new(FieldTypeCode::VectorFloat32),
            ),
            literal(
                Datum::VectorFloat32(VectorFloat32::must_create(vec![1.0, 1.0])),
                FieldType::new(FieldTypeCode::VectorFloat32),
            ),
        ),
        (literal(Datum::MinNotNull, ty()), int(0)),
        (null(), literal(Datum::MinNotNull, ty())),
        (literal(Datum::MinNotNull, ty()), null()),
        (
            null(),
            call("cast_signed", ty(), vec![json_value("bad JSON")]),
        ),
        (text("1tail"), json_value("bad JSON")),
        (null(), json_value("bad JSON")),
        (text("1tail"), int(0)),
        (
            call("coalesce", ty(), vec![null(), cast("3")]),
            call("if", ty(), vec![int(1), int(3), cast("bad")]),
        ),
    ];
    for name in ["eq", "ne", "lt", "le", "gt", "ge", "nulleq"] {
        for (left, right) in &pairs {
            let root = call(
                "cast_signed",
                ty(),
                vec![call(name, ty(), vec![left.clone(), right.clone()])],
            );
            for selected in [vec![], vec![0, 1, 0], (0..2050).map(|i| i % 2).collect()] {
                let reference = NativeCastDiagnostics::default();
                let context = NativeCastDiagnostics::default();
                let mut expected = output_for(&ty());
                let mut expected_error = None;
                for _ in &selected {
                    match root.eval(&reference, tidb_chunk::row::Row::empty()) {
                        Ok(value) => expected.append_datum(0, &value),
                        Err(error) => {
                            expected_error = Some(error);
                            break;
                        }
                    }
                }
                let suite = self::suite(root.clone());
                let mut input = Chunk::new_with_capacity(&[], 2);
                input.set_num_virtual_rows(2);
                input.set_sel(Some(selected));
                let mut actual = output_for(&ty());
                let result = suite.run_with_tikv_batch(&context, &mut input, &mut actual, &[]);
                if let Some(expected_error) = expected_error {
                    let Err(EvaluatorError::Eval(actual_error)) = result else {
                        panic!("lost comparison error: {result:?}")
                    };
                    assert_eq!(actual_error, expected_error, "{name} {left:?} {right:?}");
                } else {
                    result.unwrap();
                    assert_eq!(actual.num_rows(), expected.num_rows());
                    for row in 0..actual.num_rows() {
                        assert_eq!(
                            actual.get_row(row).is_null(0),
                            expected.get_row(row).is_null(0)
                        );
                        assert_eq!(
                            actual.column(0).get_raw(row).as_ref(),
                            expected.column(0).get_raw(row).as_ref()
                        );
                    }
                }
                assert_eq!(
                    *context.0.borrow(),
                    *reference.0.borrow(),
                    "{name} {left:?} {right:?}"
                );
                let route = suite.batch_route_snapshot();
                assert_eq!(
                    (route.prepared, route.fallback, route.refusals),
                    (1, 0, 0),
                    "{name}"
                );
            }
        }
    }
}

#[test]
fn rewritten_native_comparisons_preserve_column_constant_domains_and_composition() {
    use crate::rewriter::{rewrite_expr_resolved, ColumnResolver};
    use tidb_ast::{QueryStmt, SelectField, Stmt};
    fn fields() -> Vec<FieldType> {
        vec![
            string_ty(),
            string_ty(),
            ty().with_unsigned(true),
            FieldType::new(FieldTypeCode::NewDecimal)
                .with_flen(60)
                .with_decimal(38),
            FieldType::new(FieldTypeCode::Duration).with_decimal(6),
            FieldType::new(FieldTypeCode::Datetime).with_decimal(6),
            real_ty(),
        ]
    }
    struct Resolver;
    impl ColumnResolver for Resolver {
        fn time_zone(&self) -> SessionTimeZone {
            SessionTimeZone::utc()
        }
        fn resolve(&self, path: &[String]) -> Option<(usize, FieldType, i64)> {
            let index = match path.last()?.as_str() {
                "a" => 0,
                "b" => 1,
                "u" => 2,
                "d" => 3,
                "t" => 4,
                "dt" => 5,
                "f" => 6,
                _ => return None,
            };
            Some((index, fields()[index].clone(), index as i64 + 1))
        }
    }
    let schema = fields();
    let time =
        tidb_datatype::parse_datetime("2026-10-17 12:34:56.123456", &chrono_tz::UTC, true, false)
            .unwrap()
            .time;
    let duration = |n| Datum::Duration(MySqlDuration::from_nanoseconds(n, 6).unwrap());
    let text = |s: &str| Datum::new_string(s);
    let decimal = |s: &str| Datum::Decimal(Decimal::from_literal(s));
    let rows = vec![
        vec![
            text("1tail"),
            text("2tail"),
            Datum::UInt(u64::MAX),
            decimal("1.000000000000000000000000000000000000001"),
            duration(3_600_000_000_000),
            Datum::Time(time),
            Datum::Real(f64::NAN),
        ],
        vec![
            text("0"),
            text("0"),
            Datum::UInt(0),
            decimal("1.2"),
            duration(0),
            Datum::Time(time),
            Datum::Real(0.0),
        ],
        vec![
            Datum::Null,
            text("bad JSON"),
            Datum::UInt(1),
            Datum::Null,
            Datum::Null,
            Datum::Null,
            Datum::Real(f64::INFINITY),
        ],
        vec![
            text("[1,0]"),
            text("[1,0]"),
            Datum::UInt(7),
            decimal("-1"),
            duration(-3_600_000_000_000),
            Datum::Time(time),
            Datum::Real(f64::NAN),
        ],
        vec![
            text("bad datetime"),
            text("1"),
            Datum::UInt(u64::MAX),
            decimal("1"),
            duration(3_600_000_000_000),
            Datum::Time(time),
            Datum::Real(-0.0),
        ],
    ];
    for sql in [
        "cast((cast(a as signed) + cast(b as signed)) as char)",
        "cast((u + cast(a as signed)) as char)",
        "cast((d + d) as char)",
        "cast((f + f) as char)",
        "cast((a + b) as char)",
        "cast((if(u > 0, NULL, cast(a as double)) + cast(b as double)) as char)",
        "cast((cast(a as signed) - cast(b as signed)) as char)",
        "cast((u - cast(a as signed)) as char)",
        "cast((d - d) as char)",
        "cast((f - f) as char)",
        "cast((a - b) as char)",
        "cast((if(u > 0, NULL, cast(a as double)) - cast(b as double)) as char)",
        "cast((cast(a as signed) * cast(b as signed)) as char)",
        "cast((u * cast(a as signed)) as char)",
        "cast((d * d) as char)",
        "cast((f * f) as char)",
        "cast((a * b) as char)",
        "cast((if(u > 0, NULL, cast(a as double)) * cast(b as double)) as char)",
        "abs(a)",
        "abs(cast(a as signed))",
        "cast(abs(u) as char)",
        "abs(d)",
        "abs(f)",
        "abs(coalesce(cast(a as signed), cast('-9' as signed)))",
        "abs(if(u > 0, cast(a as signed), cast(b as signed)))",
        "cast((a = 1) as signed)",
        "nullif(a, b)",
        "cast(nullif(a, b) as char)",
        "nullif(u, cast('-1' as signed))",
        "nullif(d, '9007199254740992')",
        "nullif(t, 'not a time')",
        "nullif(dt, 'not a datetime')",
        "nullif(cast(a as json), cast(b as json))",
        "coalesce(nullif(a, b), cast('fallback' as char))",
        "cast(nullif(f, f) as char)",
        "nullif(NULL, cast(a as json))",
        "cast((a <=> 1) as signed)",
        "cast((t <=> 'not a time') as signed)",
        "cast((dt <=> 'not a datetime') as signed)",
        "cast((u <=> cast('-1' as signed)) as char)",
        "cast((NULL <=> cast(a as json)) as signed)",
        "if(t <=> '01:00:00', cast('1' as json), cast('0' as json))",
        "cast((f <=> f) as signed)",
        "cast((u < cast('-1' as signed)) as char)",
        "cast((d = '1.000000000000000000000000000000000000001') as signed)",
        "cast((t = '01:00:00') as signed)",
        "cast((t = cast(a as char)) as signed)",
        "cast((dt = 'bad datetime') as signed)",
        "cast((cast(a as json) = cast(b as json)) as signed)",
        "f <> f",
        "if(t = '01:00:00', cast('1' as json), cast('0' as json))",
        "cast((cast(a as signed) < cast(b as signed)) as char)",
        "not (d >= '1.2')",
        "coalesce((cast(a as signed) = NULL), cast('9' as signed))",
        "cast(((a = 1) and (u > 0)) as signed)",
        "cast((dt >= cast('2026-10-17 12:34:56.123456' as datetime(6))) as signed)",
        "cast((t = cast('01:00:00' as char)) as signed)",
    ] {
        let Stmt::Query(query) = tidb_parser::parse(&format!("select {sql}")).unwrap() else {
            panic!("query")
        };
        let QueryStmt::Select(select) = query.into_inner() else {
            panic!("select")
        };
        let SelectField::Expr { expr, .. } = &select.fields[0] else {
            panic!("expression")
        };
        let root = rewrite_expr_resolved(expr, &Resolver).unwrap();
        let field = root.static_type().unwrap().clone();
        for selected in [
            vec![],
            vec![0],
            vec![1],
            vec![2, 3, 4],
            vec![1, 0, 1],
            (0..2050).map(|i| [2, 3, 4][i % 3]).collect(),
        ] {
            let reference = NativeCastDiagnostics::default();
            let context = NativeCastDiagnostics::default();
            let mut input = chunk(&schema, &rows);
            input.set_sel(Some(selected.clone()));
            let mut expected = output_for(&field);
            let mut expected_error = None;
            for row in selected {
                match root.eval(&reference, input.physical_row(row)) {
                    Ok(value) => expected.append_datum(0, &value),
                    Err(error) => {
                        expected_error = Some(error);
                        break;
                    }
                }
            }
            let suite = self::suite(root.clone());
            let mut actual = output_for(&field);
            let result = suite.run_with_tikv_batch(&context, &mut input, &mut actual, &schema);
            if let Some(expected_error) = expected_error {
                let Err(EvaluatorError::Eval(actual_error)) = result else {
                    panic!("lost logical SQL error: {result:?}");
                };
                assert_eq!(actual_error, expected_error, "{sql}");
            } else {
                result.unwrap();
                assert_eq!(actual.num_rows(), expected.num_rows(), "{sql}");
                for row in 0..actual.num_rows() {
                    assert_eq!(
                        actual.get_row(row).is_null(0),
                        expected.get_row(row).is_null(0),
                        "{sql}"
                    );
                    assert_eq!(
                        actual.column(0).get_raw(row).as_ref(),
                        expected.column(0).get_raw(row).as_ref(),
                        "{sql}"
                    );
                }
            }
            assert_eq!(*context.0.borrow(), *reference.0.borrow(), "{sql}");
            let route = suite.batch_route_snapshot();
            assert_eq!(
                (route.prepared, route.fallback, route.refusals),
                (1, 0, 0),
                "{sql}"
            );
        }
    }
}

#[test]
fn native_comparison_handles_and_borrowed_copy_budget_are_terminal() {
    use tidb_query_datatype::codec::data_type::ScalarValueRef;
    use tidb_query_expr::local::{
        HostArgReply, HostInvocation, HostStart, HostStep, LocalHostServices,
    };
    use tidb_query_expr::ComparisonOp;
    for (operation, name) in [
        (SqlBatchHostOperation::CompareNative(ComparisonOp::Eq), "eq"),
        (SqlBatchHostOperation::NullIfNative, "nullif"),
    ] {
        let field = projected(&native_bytes_type(), true).unwrap();
        let args = vec![field.clone(); 2];
        let catalog = HostCatalog::for_sql_batch(vec![(
            operation,
            HostSignature {
                arg_types: args.clone().into_boxed_slice(),
                return_type: field.clone(),
            },
        )])
        .unwrap();
        let slot = catalog.slot(0).unwrap();
        let facts = crate::ops::OperandFacts {
            eval_type: Some(NativeEvalType::Int),
            unsigned: false,
            constant: false,
            duration_column: false,
        };
        let metadata = [NativeHostMetadata::Comparison {
            operation,
            name: name.into(),
            result_type: ty(),
            argument_nodes: [3, 8],
            operands: [facts; 2],
            collation: Collation::Binary,
            output_node: 2,
            output_role: NativeValueRole::Output,
        }];
        let store = RefCell::new(NativeValueStore::new(1 << 20).unwrap());
        store.borrow_mut().begin_window().unwrap();
        let row = InputRow {
            occurrence: 0,
            input_row: 0,
        };
        let left_origin = NativeValueOrigin { node: 3, row };
        let stale = store
            .borrow_mut()
            .insert_clone(&Datum::Int(0), NativeValueRole::CharInput, left_origin)
            .unwrap();
        store.borrow_mut().begin_window().unwrap();
        let large = store
            .borrow_mut()
            .insert_clone(
                &Datum::Bytes(vec![b'X'; 16384]),
                NativeValueRole::CharInput,
                left_origin,
            )
            .unwrap();
        let wrong_role = store
            .borrow_mut()
            .insert_clone(&Datum::Int(0), NativeValueRole::Output, left_origin)
            .unwrap();
        let wrong_row = store
            .borrow_mut()
            .insert_clone(
                &Datum::Int(0),
                NativeValueRole::CharInput,
                NativeValueOrigin {
                    node: 3,
                    row: InputRow {
                        occurrence: 1,
                        input_row: 0,
                    },
                },
            )
            .unwrap();
        let left = store
            .borrow_mut()
            .insert_clone(
                &Datum::UInt(u64::MAX),
                NativeValueRole::CharInput,
                left_origin,
            )
            .unwrap();
        let right = store
            .borrow_mut()
            .insert_clone(
                &Datum::Int(1),
                NativeValueRole::CharInput,
                NativeValueOrigin { node: 8, row },
            )
            .unwrap();
        let expected = call(
            name,
            ty(),
            vec![literal(Datum::UInt(u64::MAX), ty()), int(1)],
        )
        .eval(&crate::NoColumns, tidb_chunk::row::Row::empty())
        .unwrap();
        let context = NativeCastDiagnostics::default();
        let mut provider = NativeBatchHosts::new(catalog, &metadata, &context, 512)
            .with_native_values(Some(&store));
        let mut ctx = EvalContext::default();
        for token in [stale, wrong_role, wrong_row] {
            let HostStart::Pending { task, .. } = provider
                .start(
                    &mut ctx,
                    HostInvocation {
                        slot,
                        row,
                        arg_types: &args,
                        return_type: &field,
                    },
                )
                .unwrap()
            else {
                panic!("request")
            };
            let values = VectorValue::from_scalar(&ScalarValue::Bytes(Some(token)), 1);
            let error = provider
                .resume(
                    &mut ctx,
                    &task,
                    HostArgReply {
                        index: 0,
                        field_type: &field,
                        values: &values,
                    },
                )
                .unwrap_err();
            assert!(matches!(error, LocalError::BindingContract(_)));
            assert!(provider.take_native_error().is_none());
            provider.cancel(&task);
        }
        for bad_right in [true, false] {
            let HostStart::Pending { task, .. } = provider
                .start(
                    &mut ctx,
                    HostInvocation {
                        slot,
                        row,
                        arg_types: &args,
                        return_type: &field,
                    },
                )
                .unwrap()
            else {
                panic!("request")
            };
            let values = VectorValue::from_scalar(&ScalarValue::Bytes(Some(large.clone())), 1);
            let HostStep::NeedArg(request) = provider
                .resume(
                    &mut ctx,
                    &task,
                    HostArgReply {
                        index: 0,
                        field_type: &field,
                        values: &values,
                    },
                )
                .unwrap()
            else {
                panic!("must demand raw RHS before coercion")
            };
            assert_eq!(request.index, 1);
            let values = VectorValue::from_scalar(
                &ScalarValue::Bytes(Some(if bad_right {
                    left.clone()
                } else {
                    right.clone()
                })),
                1,
            );
            let error = provider
                .resume(
                    &mut ctx,
                    &task,
                    HostArgReply {
                        index: 1,
                        field_type: &field,
                        values: &values,
                    },
                )
                .unwrap_err();
            if bad_right {
                assert!(matches!(error, LocalError::BindingContract(_)));
            } else {
                assert!(matches!(error, LocalError::ResourceLimit(_)));
            }
            assert!(provider.take_native_error().is_none());
            assert!(context.0.borrow().is_empty());
            provider.cancel(&task);
        }
        let HostStart::Pending { task, .. } = provider
            .start(
                &mut ctx,
                HostInvocation {
                    slot,
                    row,
                    arg_types: &args,
                    return_type: &field,
                },
            )
            .unwrap()
        else {
            panic!("request")
        };
        for (index, token) in [left, right].into_iter().enumerate() {
            let values = VectorValue::from_scalar(&ScalarValue::Bytes(Some(token)), 1);
            let step = provider
                .resume(
                    &mut ctx,
                    &task,
                    HostArgReply {
                        index,
                        field_type: &field,
                        values: &values,
                    },
                )
                .unwrap();
            if index == 1 {
                let HostStep::Ready(result) = step else {
                    panic!("comparison result")
                };
                let ScalarValueRef::Bytes(Some(token)) = result.get_scalar_ref(0) else {
                    panic!("output capability")
                };
                assert_eq!(
                    store
                        .borrow()
                        .get(
                            token,
                            NativeValueRole::Output,
                            NativeValueOrigin { node: 2, row }
                        )
                        .unwrap()
                        .kind(),
                    expected.kind()
                );
                assert_eq!(
                    store
                        .borrow()
                        .get(
                            token,
                            NativeValueRole::Output,
                            NativeValueOrigin { node: 2, row }
                        )
                        .unwrap(),
                    &expected
                );
            }
        }
        let other_catalog = HostCatalog::for_sql_batch(vec![(
            SqlBatchHostOperation::CompareNative(ComparisonOp::Ge),
            HostSignature {
                arg_types: args.clone().into_boxed_slice(),
                return_type: field.clone(),
            },
        )])
        .unwrap();
        let other_slot = other_catalog.slot(0).unwrap();
        let mut wrong = NativeBatchHosts::new(other_catalog, &metadata, &context, 512)
            .with_native_values(Some(&store));
        let error = wrong
            .start(
                &mut ctx,
                HostInvocation {
                    slot: other_slot,
                    row,
                    arg_types: &args,
                    return_type: &field,
                },
            )
            .unwrap_err();
        assert!(matches!(error, LocalError::BindingContract(_)));
        assert!(wrong.take_native_error().is_none());
    }
}

#[test]
fn native_nullif_keeps_first_identity_literal_comparison_and_eager_right_demand() {
    use tidb_datatype::BinaryLiteral;
    let json = FieldType::new(FieldTypeCode::Json).with_flags(FieldTypeFlags::PARSE_TO_JSON);
    let text = |s: &str| literal(Datum::new_string(s), string_ty());
    let bad = || call("cast_json", json.clone(), vec![text("bad JSON")]);
    let decimal = FieldType::new(FieldTypeCode::NewDecimal)
        .with_flen(40)
        .with_decimal(6);
    let time_type = FieldType::new(FieldTypeCode::Datetime).with_decimal(6);
    let time =
        tidb_datatype::parse_datetime("2026-10-17 12:34:56.123456", &chrono_tz::UTC, true, false)
            .unwrap()
            .time;
    let vector = FieldType::new(FieldTypeCode::VectorFloat32);
    let cases = vec![
        (string_ty(), text("A"), text("a")),
        (
            string_ty().with_collation(Collation::Utf8Mb4GeneralCi),
            text("A"),
            text("a"),
        ),
        (ty(), int(1), int(1)),
        (ty(), null(), null()),
        (ty(), null(), bad()),
        (
            ty(),
            literal(Datum::Int(-1), ty().with_unsigned(true)),
            literal(Datum::UInt(u64::MAX), ty().with_unsigned(true)),
        ),
        (
            string_ty(),
            literal(
                Datum::BinaryLiteral(BinaryLiteral::from(vec![0xff; 8])),
                ty(),
            ),
            literal(Datum::UInt(u64::MAX), ty().with_unsigned(true)),
        ),
        (
            ty().with_unsigned(true),
            literal(Datum::UInt(u64::MAX), ty().with_unsigned(true)),
            int(0),
        ),
        (
            real_ty(),
            literal(Datum::Real(f64::from_bits(0x7ff8000000000042)), real_ty()),
            literal(Datum::Real(f64::NAN), real_ty()),
        ),
        (
            decimal.clone(),
            literal(
                Datum::Decimal(Decimal::from_literal("1.2300")),
                decimal.clone(),
            ),
            int(0),
        ),
        (
            time_type.clone(),
            literal(Datum::Time(time), time_type.clone()),
            null(),
        ),
        (
            json.clone(),
            call("cast_json", json.clone(), vec![text("null")]),
            call("cast_json", json.clone(), vec![text("false")]),
        ),
        (
            json.clone(),
            call("cast_json", json.clone(), vec![text("[1,0]")]),
            call("cast_json", json.clone(), vec![text("[1,0]")]),
        ),
        (
            vector.clone(),
            literal(
                Datum::VectorFloat32(VectorFloat32::must_create(vec![1.0, 0.0])),
                vector.clone(),
            ),
            literal(
                Datum::VectorFloat32(VectorFloat32::must_create(vec![1.0, 1.0])),
                vector.clone(),
            ),
        ),
        (ty(), literal(Datum::MinNotNull, ty()), null()),
        (string_ty(), text("1tail"), bad()),
        (ty(), call("cast_signed", ty(), vec![text("1tail")]), bad()),
        (
            string_ty(),
            call(
                "cast_char",
                string_ty(),
                vec![call("coalesce", ty(), vec![null(), int(3)])],
            ),
            text("4"),
        ),
    ];
    for (field, left, right) in cases {
        let root = call(
            "if",
            field.clone(),
            vec![
                call("cast_signed", ty(), vec![int(1)]),
                call("nullif", field.clone(), vec![left, right]),
                bad(),
            ],
        );
        for selected in [vec![], vec![0, 1, 0], (0..2050).map(|i| i % 2).collect()] {
            let reference = NativeCastDiagnostics::default();
            let context = NativeCastDiagnostics::default();
            let mut expected = output_for(&field);
            let mut expected_error = None;
            for _ in &selected {
                match root.eval(&reference, tidb_chunk::row::Row::empty()) {
                    Ok(value) => expected.append_datum(0, &value),
                    Err(error) => {
                        expected_error = Some(error);
                        break;
                    }
                }
            }
            let suite = self::suite(root.clone());
            let mut input = Chunk::new_with_capacity(&[], 2);
            input.set_num_virtual_rows(2);
            input.set_sel(Some(selected));
            let mut actual = output_for(&field);
            let result = suite.run_with_tikv_batch(&context, &mut input, &mut actual, &[]);
            if let Some(error) = expected_error {
                let Err(EvaluatorError::Eval(actual_error)) = result else {
                    panic!("lost NULLIF error: {result:?}")
                };
                assert_eq!(actual_error, error, "{root:?}");
            } else {
                result.unwrap();
                assert_eq!(actual.num_rows(), expected.num_rows());
                for row in 0..actual.num_rows() {
                    assert_eq!(
                        actual.get_row(row).is_null(0),
                        expected.get_row(row).is_null(0)
                    );
                    assert_eq!(
                        actual.column(0).get_raw(row).as_ref(),
                        expected.column(0).get_raw(row).as_ref(),
                        "{root:?}"
                    );
                }
            }
            assert_eq!(*context.0.borrow(), *reference.0.borrow(), "{root:?}");
            let route = suite.batch_route_snapshot();
            assert_eq!(
                (route.prepared, route.fallback, route.refusals),
                (1, 0, 0),
                "{root:?}"
            );
        }
    }
}

#[test]
fn native_nullif_column_values_do_not_adopt_comparison_column_classification() {
    let duration = FieldType::new(FieldTypeCode::Duration).with_decimal(6);
    let decimal = FieldType::new(FieldTypeCode::NewDecimal)
        .with_flen(20)
        .with_decimal(0);
    let schema = vec![duration.clone(), decimal.clone()];
    let rows = vec![
        vec![
            Datum::Duration(MySqlDuration::from_nanoseconds(3_600_000_000_000, 6).unwrap()),
            Datum::Decimal(Decimal::from_literal("9007199254740993")),
        ],
        vec![Datum::Null, Datum::Null],
        vec![
            Datum::Duration(MySqlDuration::from_nanoseconds(0, 6).unwrap()),
            Datum::Decimal(Decimal::from_literal("2")),
        ],
    ];
    for (index, field, right) in [
        (0, duration, "not a time"),
        (1, decimal, "9007199254740992"),
    ] {
        let json = FieldType::new(FieldTypeCode::Json).with_flags(FieldTypeFlags::PARSE_TO_JSON);
        let root = call(
            "if",
            field.clone(),
            vec![
                call("cast_signed", ty(), vec![int(1)]),
                call(
                    "nullif",
                    field.clone(),
                    vec![
                        typed_column(index, &field),
                        literal(Datum::new_string(right), string_ty()),
                    ],
                ),
                call(
                    "cast_json",
                    json,
                    vec![literal(Datum::new_string("bad JSON"), string_ty())],
                ),
            ],
        );
        for selected in [vec![], vec![0, 2, 1, 0], (0..2050).map(|i| i % 3).collect()] {
            let reference = NativeCastDiagnostics::default();
            let context = NativeCastDiagnostics::default();
            let mut input = chunk(&schema, &rows);
            input.set_sel(Some(selected.clone()));
            let mut expected = output_for(&field);
            for row in selected {
                expected.append_datum(0, &root.eval(&reference, input.physical_row(row)).unwrap());
            }
            assert!(
                reference.0.borrow().is_empty(),
                "NULLIF literal comparison adds no duration parse warning"
            );
            let suite = self::suite(root.clone());
            let mut actual = output_for(&field);
            suite
                .run_with_tikv_batch(&context, &mut input, &mut actual, &schema)
                .unwrap();
            assert_eq!(actual.num_rows(), expected.num_rows());
            for row in 0..actual.num_rows() {
                assert_eq!(
                    actual.get_row(row).is_null(0),
                    expected.get_row(row).is_null(0)
                );
                assert_eq!(
                    actual.column(0).get_raw(row).as_ref(),
                    expected.column(0).get_raw(row).as_ref()
                );
            }
            assert_eq!(*context.0.borrow(), *reference.0.borrow());
            let route = suite.batch_route_snapshot();
            assert_eq!((route.prepared, route.fallback, route.refusals), (1, 0, 0));
        }
    }
}

#[test]
fn native_value_comparisons_keep_context_observation_after_raw_operand_demand() {
    #[derive(Default)]
    struct Trace(RefCell<Vec<String>>);
    impl crate::Columns for Trace {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }
        fn div_precision_increment(&self) -> u32 {
            self.0.borrow_mut().push("div".into());
            4
        }
        fn append_warning(&self, code: u16, message: &str) {
            self.0.borrow_mut().push(format!("{code}:{message}"));
        }
    }
    for name in ["nullif", "nulleq"] {
        for bad_right in [false, true] {
            let json =
                FieldType::new(FieldTypeCode::Json).with_flags(FieldTypeFlags::PARSE_TO_JSON);
            let right = if bad_right {
                call(
                    "cast_json",
                    json,
                    vec![literal(Datum::new_string("bad JSON"), string_ty())],
                )
            } else {
                call(
                    "cast_signed",
                    ty(),
                    vec![literal(Datum::new_string("2tail"), string_ty())],
                )
            };
            let field = if name == "nullif" { string_ty() } else { ty() };
            let root = call(
                "cast_char",
                string_ty(),
                vec![call(
                    name,
                    field,
                    vec![literal(Datum::new_string("1tail"), string_ty()), right],
                )],
            );
            let reference = Trace::default();
            let context = Trace::default();
            let expected = root.eval(&reference, tidb_chunk::row::Row::empty());
            let suite = self::suite(root);
            let mut input = Chunk::new_with_capacity(&[], 1);
            input.set_num_virtual_rows(1);
            let mut actual = output_for(&string_ty());
            let result = suite.run_with_tikv_batch(&context, &mut input, &mut actual, &[]);
            match expected {
                Ok(value) => {
                    result.unwrap();
                    let mut expected = output_for(&string_ty());
                    expected.append_datum(0, &value);
                    assert_eq!(
                        actual.column(0).get_raw(0).as_ref(),
                        expected.column(0).get_raw(0).as_ref()
                    );
                }
                Err(expected) => {
                    let Err(EvaluatorError::Eval(actual)) = result else {
                        panic!("lost error")
                    };
                    assert_eq!(actual, expected);
                }
            }
            assert_eq!(
                *context.0.borrow(),
                *reference.0.borrow(),
                "{name} bad_right={bad_right}"
            );
            assert_eq!(
                context
                    .0
                    .borrow()
                    .iter()
                    .filter(|event| event.as_str() == "div")
                    .count(),
                usize::from(!bad_right)
            );
            let route = suite.batch_route_snapshot();
            assert_eq!((route.prepared, route.fallback, route.refusals), (1, 0, 0));
        }
    }
}

#[test]
fn native_abs_preserves_actual_kinds_warnings_and_source_overflow_diagnostics() {
    use tidb_datatype::{BinaryLiteral, MysqlEnum, MysqlSet};
    let decimal = FieldType::new(FieldTypeCode::NewDecimal)
        .with_flen(60)
        .with_decimal(6);
    let vector = FieldType::new(FieldTypeCode::VectorFloat32);
    let duration = FieldType::new(FieldTypeCode::Duration).with_decimal(6);
    let values = vec![
        (Datum::Null, vector.clone(), real_ty()),
        (Datum::Int(-7), ty(), ty()),
        (Datum::Int(i64::MIN), ty(), ty()),
        (
            Datum::Int(i64::MIN),
            ty().with_unsigned(true),
            ty().with_unsigned(true),
        ),
        (
            Datum::UInt(u64::MAX),
            ty().with_unsigned(true),
            ty().with_unsigned(true),
        ),
        (
            Datum::UInt(1u64 << 63),
            ty().with_unsigned(true),
            ty().with_unsigned(true),
        ),
        (Datum::Real(-0.0), real_ty(), real_ty()),
        (
            Datum::Real(f64::from_bits(0xfff8000000000042)),
            real_ty(),
            real_ty(),
        ),
        (Datum::Real(f64::NEG_INFINITY), real_ty(), real_ty()),
        (
            Datum::Float32(-0.10000000149011612),
            FieldType::new(FieldTypeCode::Float),
            real_ty(),
        ),
        (
            Datum::Decimal(Decimal::from_literal("-1.2300")),
            decimal.clone(),
            decimal.clone(),
        ),
        (
            Datum::Decimal(Decimal::from_literal(
                "-123456789012345678901234567890123456789012345678901234567890.001",
            )),
            decimal.clone(),
            decimal,
        ),
        (Datum::new_string("-12.5tail"), string_ty(), real_ty()),
        (Datum::Bytes(b"-4tail".to_vec()), string_ty(), real_ty()),
        (
            Datum::BinaryLiteral(BinaryLiteral::from(vec![0xff; 8])),
            string_ty(),
            real_ty(),
        ),
        (
            Datum::Enum(MysqlEnum::new("b", 2), Collation::GbkBin),
            string_ty(),
            real_ty(),
        ),
        (
            Datum::Set(MysqlSet::new("a,b", 3), Collation::GbkBin),
            string_ty(),
            real_ty(),
        ),
        (
            Datum::Duration(MySqlDuration::from_nanoseconds(-123456789000, 6).unwrap()),
            duration,
            real_ty(),
        ),
        (
            Datum::VectorFloat32(VectorFloat32::must_create(vec![1.0])),
            vector,
            real_ty(),
        ),
        (Datum::MinNotNull, ty(), real_ty()),
    ];
    let mut roots = values
        .into_iter()
        .map(|(value, input, ret)| call("abs", ret, vec![literal(value, input)]))
        .collect::<Vec<_>>();
    roots.push(call(
        "abs",
        ty(),
        vec![call("if", ty(), vec![int(1), int(i64::MIN), int(3)])],
    ));
    roots.push(call(
        "abs",
        ty(),
        vec![call(
            "if",
            ty(),
            vec![
                call("eq", ty(), vec![int(1), int(1)]),
                int(i64::MIN),
                int(3),
            ],
        )],
    ));
    roots.push(call(
        "cast_char",
        string_ty(),
        vec![call(
            "abs",
            ty(),
            vec![call("coalesce", ty(), vec![null(), int(-9)])],
        )],
    ));
    for root in roots {
        let field = root.static_type().unwrap().clone();
        for selected in [vec![], vec![0, 1, 0], (0..2050).map(|i| i % 2).collect()] {
            let reference = NativeCastDiagnostics::default();
            let context = NativeCastDiagnostics::default();
            let mut expected = output_for(&field);
            let mut expected_error = None;
            for _ in &selected {
                match root.eval(&reference, tidb_chunk::row::Row::empty()) {
                    Ok(value) => expected.append_datum(0, &value),
                    Err(error) => {
                        expected_error = Some(error);
                        break;
                    }
                }
            }
            let suite = self::suite(root.clone());
            let mut input = Chunk::new_with_capacity(&[], 2);
            input.set_num_virtual_rows(2);
            input.set_sel(Some(selected));
            let mut actual = output_for(&field);
            let result = suite.run_with_tikv_batch(&context, &mut input, &mut actual, &[]);
            if let Some(error) = expected_error {
                let Err(EvaluatorError::Eval(actual_error)) = result else {
                    panic!("lost ABS error: {result:?}")
                };
                assert_eq!(actual_error, error, "{root:?}");
            } else {
                result.unwrap();
                assert_eq!(actual.num_rows(), expected.num_rows());
                for row in 0..actual.num_rows() {
                    assert_eq!(
                        actual.get_row(row).is_null(0),
                        expected.get_row(row).is_null(0)
                    );
                    assert_eq!(
                        actual.column(0).get_raw(row).as_ref(),
                        expected.column(0).get_raw(row).as_ref(),
                        "{root:?}"
                    );
                }
            }
            assert_eq!(*context.0.borrow(), *reference.0.borrow(), "{root:?}");
            let route = suite.batch_route_snapshot();
            assert_eq!(
                (route.prepared, route.fallback, route.refusals),
                (1, 0, 0),
                "{root:?}"
            );
        }
    }
}

#[test]
fn math_diagnostic_budget_precedes_formatting_and_skips_unrenderable_trees() {
    let huge = literal(
        Datum::Decimal(Decimal::from_raw_parts(false, b"1".to_vec(), 200_000, 0)),
        FieldType::new(FieldTypeCode::NewDecimal),
    );
    let branch = call(
        "if",
        ty(),
        vec![
            call("cast_signed", ty(), vec![int(1)]),
            int(i64::MIN),
            huge.clone(),
        ],
    );
    let mut source_limits = limits();
    source_limits.max_metadata_bytes = 32 * 1024;
    let compile = |root| {
        PreparedBatch::compile_calculation(
            &self::suite(root),
            0,
            &[],
            true,
            191,
            source_limits,
            ExecutionLimits::default(),
            1 << 20,
        )
    };
    let before = crate::scalar_function::static_math_formats_for_test();
    compile(branch.clone()).unwrap();
    assert_eq!(
        crate::scalar_function::static_math_formats_for_test(),
        before
    );
    let error = match compile(call("abs", ty(), vec![branch])) {
        Err(error) => error,
        Ok(_) => panic!("oversized diagnostic was admitted"),
    };
    assert!(matches!(
        error.local_error(),
        Some(LocalError::ResourceLimit(_))
    ));
    assert_eq!(
        crate::scalar_function::static_math_formats_for_test(),
        before
    );
    // A later nonnumeric literal makes the original diagnostic unavailable.
    // Do not format an earlier enormous decimal just to discover that fact.
    let unsupported = call(
        "coalesce",
        ty(),
        vec![huge, literal(Datum::new_string("text"), string_ty())],
    );
    let root = call(
        "abs",
        ty(),
        vec![call("if", ty(), vec![int(1), int(i64::MIN), unsupported])],
    );
    let worker = compile(root).unwrap();
    assert_eq!(
        crate::scalar_function::static_math_formats_for_test(),
        before
    );
    assert!(worker.source.host_metadata.iter().any(|meta| matches!(
        meta,
        NativeHostMetadata::Math {
            overflow_expression: None,
            ..
        }
    )));
    let worker = compile(abs(int(i64::MIN))).unwrap();
    assert_eq!(
        crate::scalar_function::static_math_formats_for_test(),
        before + 1
    );
    assert!(worker.source.host_metadata.iter().any(|meta|matches!(meta,NativeHostMetadata::Math{overflow_expression:Some(text),..} if text.as_ref()=="abs(-9223372036854775808)")));
}

#[derive(Default)]
struct NativeArithmeticDiagnostics {
    forced: bool,
    warnings: RefCell<Vec<(u16, String)>>,
    observations: RefCell<Vec<&'static str>>,
}
impl crate::Columns for NativeArithmeticDiagnostics {
    fn get(&self, _: &[String]) -> Option<Datum> {
        None
    }
    fn append_warning(&self, code: u16, message: &str) {
        self.warnings.borrow_mut().push((code, message.to_owned()));
    }
    fn no_unsigned_subtraction(&self) -> bool {
        self.observations.borrow_mut().push("subtraction");
        self.forced
    }
    fn div_precision_increment(&self) -> u32 {
        self.observations.borrow_mut().push("div_precision");
        4
    }
}

#[test]
fn native_arithmetic_islands_preserve_typed_demands_values_diagnostics_and_mode_reads() {
    use tidb_datatype::{BinaryLiteral, MysqlEnum, MysqlSet};
    let real = real_ty();
    let integer = ty();
    let unsigned = ty().with_unsigned(true);
    let decimal = FieldType::new(FieldTypeCode::NewDecimal)
        .with_flen(60)
        .with_decimal(6);
    let json = FieldType::new(FieldTypeCode::Json).with_flags(FieldTypeFlags::PARSE_TO_JSON);
    let vector = FieldType::new(FieldTypeCode::VectorFloat32);
    let literal_real = |value| literal(Datum::Real(value), real.clone());
    let literal_decimal = |value| {
        literal(
            Datum::Decimal(Decimal::from_literal(value)),
            decimal.clone(),
        )
    };
    let bad = || {
        call(
            "cast_json",
            json.clone(),
            vec![literal(Datum::new_string("bad JSON"), string_ty())],
        )
    };
    let operands = vec![
        (integer.clone(), int(-7), int(3)),
        (integer.clone(), int(i64::MAX), int(1)),
        (integer.clone(), int(0), int(i64::MIN)),
        (
            unsigned.clone(),
            literal(Datum::UInt(u64::MAX), unsigned.clone()),
            int(1),
        ),
        (
            unsigned.clone(),
            literal(Datum::Int(-1), unsigned.clone()),
            int(1),
        ),
        (
            unsigned.clone(),
            literal(Datum::UInt(0), unsigned.clone()),
            int(1),
        ),
        (
            integer.clone(),
            null(),
            call("cast_signed", integer.clone(), vec![bad()]),
        ),
        (real.clone(), literal(Datum::Null, real.clone()), bad()),
        (
            real.clone(),
            literal(Datum::Null, real.clone()),
            literal(Datum::new_string("2tail"), string_ty()),
        ),
        (real.clone(), literal_real(-0.0), literal_real(0.0)),
        (real.clone(), literal_real(f64::MAX), literal_real(2.0)),
        (
            real.clone(),
            literal_real(f64::from_bits(0x7ff8000000000042)),
            literal_real(1.0),
        ),
        (real.clone(), literal_real(f64::INFINITY), literal_real(1.0)),
        (
            real.clone(),
            literal(Datum::new_string("2.5tail"), string_ty()),
            literal(Datum::Bytes(b"-3.5tail".to_vec()), string_ty()),
        ),
        (
            decimal.clone(),
            literal_decimal("-1.2300"),
            literal_decimal("2.5000"),
        ),
        (
            decimal.clone(),
            literal_decimal("123456789012345678901234567890123456789012345678901234567890.001"),
            int(2),
        ),
        (
            decimal.clone(),
            literal_real(f64::MAX),
            literal_decimal("0"),
        ),
        (
            real.clone(),
            literal(
                Datum::Float32(-0.10000000149011612),
                FieldType::new(FieldTypeCode::Float),
            ),
            literal_real(2.0),
        ),
        (
            real.clone(),
            literal(
                Datum::BinaryLiteral(BinaryLiteral::from(vec![0xff; 8])),
                string_ty(),
            ),
            literal_real(1.0),
        ),
        (
            real.clone(),
            literal(
                Datum::Enum(MysqlEnum::new("b", 2), Collation::GbkBin),
                FieldType::new(FieldTypeCode::Enum),
            ),
            literal_real(1.0),
        ),
        (
            real.clone(),
            literal(
                Datum::Set(MysqlSet::new("a,b", 3), Collation::GbkBin),
                FieldType::new(FieldTypeCode::Set),
            ),
            literal_real(1.0),
        ),
        (
            decimal.clone(),
            literal(
                Datum::Duration(MySqlDuration::from_nanoseconds(-123456789000, 6).unwrap()),
                FieldType::new(FieldTypeCode::Duration).with_decimal(6),
            ),
            int(2),
        ),
        (
            real.clone(),
            literal(
                Datum::Json(BinaryJSON::parse("[1,true]").unwrap()),
                json.clone(),
            ),
            literal_real(1.0),
        ),
        (
            real.clone(),
            literal(Datum::MinNotNull, integer.clone()),
            literal_real(1.0),
        ),
        (
            vector.clone(),
            literal(
                Datum::VectorFloat32(VectorFloat32::must_create(vec![1.0, -0.0])),
                vector.clone(),
            ),
            literal(
                Datum::VectorFloat32(VectorFloat32::must_create(vec![2.0, 3.0])),
                vector.clone(),
            ),
        ),
        (
            vector.clone(),
            literal(
                Datum::VectorFloat32(VectorFloat32::must_create(vec![1.0])),
                vector.clone(),
            ),
            literal(
                Datum::VectorFloat32(VectorFloat32::must_create(vec![2.0, 3.0])),
                vector.clone(),
            ),
        ),
    ];
    for name in ["plus", "minus", "mul"] {
        let mut roots = operands
            .iter()
            .map(|(field, left, right)| {
                call(name, field.clone(), vec![left.clone(), right.clone()])
            })
            .collect::<Vec<_>>();
        roots.push(call(
            name,
            integer.clone(),
            vec![
                call("abs", integer.clone(), vec![int(-7)]),
                call("coalesce", integer.clone(), vec![null(), int(2)]),
            ],
        ));
        // A mismatched computed Int declaration can fail the old fast
        // probe. Its warning-producing child must be freshly demanded again.
        roots.push(call(
            name,
            integer.clone(),
            vec![
                call(
                    "if",
                    integer.clone(),
                    vec![
                        call(
                            "and",
                            integer.clone(),
                            vec![literal(Datum::new_string("1tail"), string_ty()), int(1)],
                        ),
                        literal(
                            Datum::VectorFloat32(VectorFloat32::must_create(vec![1.0])),
                            vector.clone(),
                        ),
                        int(0),
                    ],
                ),
                int(1),
            ],
        ));
        for root in roots {
            let field = root.static_type().unwrap().clone();
            // CAST in the condition selects the native row-demand boundary.
            // Existing complete typed arithmetic retains whole-batch phases.
            let root = call(
                "if",
                field.clone(),
                vec![
                    call("cast_signed", integer.clone(), vec![int(1)]),
                    root,
                    bad(),
                ],
            );
            for forced in [false, true] {
                for selected in [vec![], vec![0, 1, 0], (0..2050).map(|i| i % 2).collect()] {
                    let reference = NativeArithmeticDiagnostics {
                        forced,
                        ..NativeArithmeticDiagnostics::default()
                    };
                    let context = NativeArithmeticDiagnostics {
                        forced,
                        ..NativeArithmeticDiagnostics::default()
                    };
                    let mut expected = output_for(&field);
                    let mut expected_error = None;
                    for _ in &selected {
                        match root.eval(&reference, tidb_chunk::row::Row::empty()) {
                            Ok(value) => expected.append_datum(0, &value),
                            Err(error) => {
                                expected_error = Some(error);
                                break;
                            }
                        }
                    }
                    let suite = self::suite(root.clone());
                    let mut input = Chunk::new_with_capacity(&[], 2);
                    input.set_num_virtual_rows(2);
                    input.set_sel(Some(selected));
                    let mut actual = output_for(&field);
                    let result = suite.run_with_tikv_batch(&context, &mut input, &mut actual, &[]);
                    if let Some(expected) = expected_error {
                        let Err(EvaluatorError::Eval(actual)) = result else {
                            panic!("lost native arithmetic error: {result:?}")
                        };
                        assert_eq!(actual, expected, "{root:?}");
                    } else {
                        result.unwrap();
                        assert_eq!(actual.num_rows(), expected.num_rows());
                        for row in 0..actual.num_rows() {
                            assert_eq!(
                                actual.get_row(row).is_null(0),
                                expected.get_row(row).is_null(0)
                            );
                            assert_eq!(
                                actual.column(0).get_raw(row).as_ref(),
                                expected.column(0).get_raw(row).as_ref(),
                                "{root:?}"
                            );
                        }
                    }
                    assert_eq!(
                        *context.warnings.borrow(),
                        *reference.warnings.borrow(),
                        "{root:?}"
                    );
                    assert_eq!(
                        *context.observations.borrow(),
                        *reference.observations.borrow(),
                        "{root:?}"
                    );
                    let route = suite.batch_route_snapshot();
                    assert_eq!(
                        (route.prepared, route.fallback, route.refusals),
                        (1, 0, 0),
                        "{root:?}"
                    );
                }
            }
        }
    }
}

#[test]
fn arithmetic_diagnostics_are_gated_before_formatting_and_skip_unrenderable_trees() {
    let huge = literal(
        Datum::Decimal(Decimal::from_raw_parts(false, b"1".to_vec(), 200_000, 0)),
        FieldType::new(FieldTypeCode::NewDecimal),
    );
    let wrap = |root| call("cast_char", string_ty(), vec![root]);
    let mut source_limits = limits();
    source_limits.max_metadata_bytes = 128 * 1024;
    let compile = |root| {
        PreparedBatch::compile_calculation(
            &suite(root),
            0,
            &[],
            true,
            211,
            source_limits,
            ExecutionLimits::default(),
            1 << 20,
        )
    };
    let before = crate::scalar_function::static_numeric_formats_for_test();
    let failure = match compile(wrap(plus(huge.clone(), int(1)))) {
        Err(error) => error,
        Ok(_) => panic!("oversized numeric diagnostic admitted"),
    };
    assert!(matches!(
        failure.local_error(),
        Some(LocalError::ResourceLimit(_))
    ));
    assert_eq!(
        crate::scalar_function::static_numeric_formats_for_test(),
        before
    );
    // IF has no numeric source rendering, so no earlier large literal is
    // formatted just to discover that unavailable diagnostic shape later.
    let root = wrap(plus(int(1), call("if", ty(), vec![int(1), int(2), huge])));
    let worker = compile(root).unwrap();
    assert_eq!(
        crate::scalar_function::static_numeric_formats_for_test(),
        before
    );
    assert!(worker.source.host_metadata.iter().any(|metadata| matches!(
        metadata,
        NativeHostMetadata::Arithmetic {
            overflow_expressions: [None, None],
            ..
        }
    )));
    let json = BinaryJSON::parse(&format!("{{\"text\":\"{}\"}}", "x".repeat(10_000))).unwrap();
    let root = wrap(call(
        "plus",
        real_ty(),
        vec![
            literal(Datum::Json(json), FieldType::new(FieldTypeCode::Json)),
            literal(Datum::Real(1.0), real_ty()),
        ],
    ));
    let failure = match compile(root) {
        Err(error) => error,
        Ok(_) => panic!("full JSON formatting work escaped source gate"),
    };
    assert!(matches!(
        failure.local_error(),
        Some(LocalError::ResourceLimit(_))
    ));
    assert_eq!(
        crate::scalar_function::static_numeric_formats_for_test(),
        before
    );
    let worker = compile(wrap(plus(int(i64::MAX), int(1)))).unwrap();
    assert!(crate::scalar_function::static_numeric_formats_for_test() > before);
    assert!(worker.source.host_metadata.iter().any(|metadata|matches!(metadata,NativeHostMetadata::Arithmetic{overflow_expressions:[Some(text),_],..} if text.as_ref()=="(9223372036854775807 + 1)")));
    let worker = compile(wrap(call(
        "plus",
        real_ty(),
        vec![
            literal(Datum::Real(f64::from_bits(1)), real_ty()),
            literal(Datum::Real(1.0), real_ty()),
        ],
    )))
    .unwrap();
    assert!(worker.source.host_metadata.iter().any(|metadata|matches!(metadata,NativeHostMetadata::Arithmetic{overflow_expressions:[Some(text),_],..} if text.len()>300)));
}

#[test]
fn numeric_diagnostic_format_failure_is_demanded_only_after_its_actual_overflow() {
    use std::panic::{catch_unwind, AssertUnwindSafe};
    let field = real_ty();
    let poison = literal(
        Datum::Json(BinaryJSON::from_encoded_parts(
            0x0b,
            f64::NAN.to_bits().to_le_bytes().to_vec(),
        )),
        FieldType::new(FieldTypeCode::Json),
    );
    for shape in 0..3 {
        for selected in [false, true] {
            let opaque = || {
                call(
                    "if",
                    field.clone(),
                    vec![
                        int(1),
                        literal(Datum::Real(1.0), field.clone()),
                        literal(Datum::Real(0.0), field.clone()),
                    ],
                )
            };
            let args = match shape {
                0 => vec![poison.clone(), literal(Datum::Real(1.0), field.clone())],
                1 => vec![opaque(), poison.clone()],
                _ => vec![poison.clone(), opaque()],
            };
            let bad = call("plus", field.clone(), args);
            let root = call(
                "if",
                field.clone(),
                vec![
                    call("cast_signed", ty(), vec![int(i64::from(selected))]),
                    bad,
                    literal(Datum::Real(7.0), field.clone()),
                ],
            );
            let reference = catch_unwind(AssertUnwindSafe(|| {
                root.eval(&crate::NoColumns, tidb_chunk::row::Row::empty())
            }));
            assert_eq!(
                reference.is_err(),
                selected && shape != 1,
                "original formatting order"
            );
            let expected_panic = reference.is_err();
            let suite = self::suite(root);
            let mut input = Chunk::new_with_capacity(&[], 1);
            input.set_num_virtual_rows(1);
            let mut output = output_for(&field);
            let actual = catch_unwind(AssertUnwindSafe(|| {
                suite.run_with_tikv_batch(&crate::NoColumns, &mut input, &mut output, &[])
            }));
            assert_eq!(
                actual.is_err(),
                expected_panic,
                "an unselected formatting failure must not run during compilation"
            );
            if !selected {
                actual.unwrap().unwrap();
                assert_eq!(output.get_row(0).get_float64(0), 7.0);
            } else if shape == 1 {
                let Err(expected) = reference.unwrap() else {
                    panic!("earlier unavailable source text must keep overflow");
                };
                let Err(EvaluatorError::Eval(actual)) = actual.unwrap() else {
                    panic!("actual overflow missing");
                };
                assert_eq!(actual, expected);
            }
        }
    }
}

#[derive(Default)]
struct ArithmeticProfilesContext {
    enabled: Cell<bool>,
    trace: std::cell::RefCell<Vec<String>>,
}
impl Columns for ArithmeticProfilesContext {
    fn get(&self, _: &[String]) -> Option<Datum> {
        panic!("compiled arithmetic must use value bindings")
    }
    fn enable_vectorized_expression(&self) -> bool {
        self.trace.borrow_mut().push("enable".into());
        self.enabled.get()
    }
    fn time_zone(&self) -> SessionTimeZone {
        self.trace.borrow_mut().push("zone".into());
        SessionTimeZone::utc()
    }
    fn div_precision_increment(&self) -> u32 {
        self.trace.borrow_mut().push("precision".into());
        4
    }
    fn no_unsigned_subtraction(&self) -> bool {
        self.trace.borrow_mut().push("unsigned-mode".into());
        false
    }
    fn truncate_level(&self) -> crate::ErrorLevel {
        crate::ErrorLevel::Warn
    }
    fn append_warning(&self, code: u16, message: &str) {
        self.trace
            .borrow_mut()
            .push(format!("warn:{code}:{message}"));
    }
    fn handle_truncate(&self, message: &str) -> std::result::Result<(), EvalError> {
        self.trace.borrow_mut().push(format!("truncate:{message}"));
        Ok(())
    }
}
fn compare_arithmetic_profile(
    root: Expression,
    schema: &[FieldType],
    rows: &[Vec<Datum>],
    selection: &[usize],
) {
    let field = snapshot_field_type(root.static_type().unwrap());
    let actual_suite = suite(root.clone());
    let reference_suite = suite(root);
    let actual_ctx = ArithmeticProfilesContext::default();
    let reference_ctx = ArithmeticProfilesContext::default();
    // Reuse the same prepared owner across flag changes. A previous scalar
    // call must not latch the following bulk call to a one-row tuning value.
    for enabled in [true, false, true, false] {
        actual_ctx.enabled.set(enabled);
        reference_ctx.enabled.set(enabled);
        actual_ctx.trace.borrow_mut().clear();
        reference_ctx.trace.borrow_mut().clear();
        let mut input = chunk(schema, rows);
        input.set_sel(Some(selection.to_vec()));
        let mut reference_input = chunk(schema, rows);
        reference_input.set_sel(Some(selection.to_vec()));
        let mut output = output_for(&field);
        let mut expected_output = output_for(&field);
        let actual = actual_suite.run_with_tikv_batch(&actual_ctx, &mut input, &mut output, schema);
        let expected =
            reference_suite.run(&reference_ctx, &mut reference_input, &mut expected_output);
        assert_eq!(actual, expected, "flag={enabled}, selection={selection:?}");
        assert_eq!(
            actual_ctx.trace.take(),
            reference_ctx.trace.take(),
            "flag={enabled}"
        );
        assert_eq!(output.num_rows(), expected_output.num_rows());
        for position in 0..output.num_rows() {
            assert_eq!(
                output.get_row(position).is_null(0),
                expected_output.get_row(position).is_null(0)
            );
            assert_eq!(
                output.column(0).get_raw(position).as_ref(),
                expected_output.column(0).get_raw(position).as_ref(),
                "flag={enabled}, row={position}"
            );
        }
        let routes = actual_suite.batch_route_snapshot();
        assert_eq!((routes.refusals, routes.fallback), (0, 0));
        let crate::evaluator::BatchLane::Prepared(ref prepared) = *actual_suite.batch_lane.borrow()
        else {
            panic!("arithmetic root was not prepared");
        };
        assert_eq!(prepared.workers.len(), 1);
        assert!(prepared.workers[0].source.nodes[0]
            .view
            .value_role()
            .is_some());
        assert_eq!(
            prepared.workers[0].program.requires_whole_selection(),
            prepared.workers[0].row_profile.is_some()
        );
    }
}
#[test]
fn native_arithmetic_root_profiles_preserve_bulk_row_values_diagnostics_and_context_toggles() {
    let decimal = FieldType::new(FieldTypeCode::NewDecimal)
        .with_flen(40)
        .with_decimal(4);
    let schemas = vec![
        (
            vec![ty(), ty()],
            ty(),
            vec![
                vec![Datum::Null, Datum::Int(3)],
                vec![Datum::Int(-7), Datum::Int(2)],
                vec![Datum::Int(11), Datum::Int(0)],
            ],
        ),
        (
            vec![ty().with_unsigned(true), ty()],
            ty().with_unsigned(true),
            vec![
                vec![Datum::UInt(7), Datum::Int(3)],
                vec![Datum::UInt(u64::MAX), Datum::Int(1)],
                vec![Datum::Null, Datum::Int(-1)],
            ],
        ),
        (
            vec![real_ty(), real_ty()],
            real_ty(),
            vec![
                vec![Datum::Null, Datum::Real(2.0)],
                vec![Datum::Real(-1.5), Datum::Real(0.25)],
                vec![Datum::Real(f64::MAX), Datum::Real(f64::MAX)],
            ],
        ),
        (
            vec![decimal.clone(), decimal.clone()],
            decimal.clone(),
            vec![
                vec![Datum::Null, Datum::Decimal(Decimal::from_int(3))],
                vec![
                    Datum::Decimal(Decimal::from_literal("-1.25")),
                    Datum::Decimal(Decimal::from_literal("2.5")),
                ],
                vec![
                    Datum::Decimal(Decimal::from_int(11)),
                    Datum::Decimal(Decimal::from_int(0)),
                ],
            ],
        ),
        (
            vec![string_ty(), decimal.clone()],
            decimal,
            vec![
                vec![
                    Datum::new_string("3x"),
                    Datum::Decimal(Decimal::from_int(2)),
                ],
                vec![Datum::Null, Datum::Decimal(Decimal::from_int(1))],
                vec![Datum::new_string("12.5"), Datum::Null],
            ],
        ),
    ];
    let selections = vec![
        vec![],
        vec![0],
        vec![0, 1, 2],
        vec![2, 0, 2],
        (0..2050).map(|position| [2, 0, 2][position % 3]).collect(),
    ];
    for (schema, result, rows) in schemas {
        for name in ["plus", "minus", "mul"] {
            for selection in &selections {
                let root = call(
                    name,
                    result.clone(),
                    vec![typed_column(0, &schema[0]), typed_column(1, &schema[1])],
                );
                compare_arithmetic_profile(root, &schema, &rows, selection);
            }
        }
    }
    // A genuine bulk call must demand the overflowing right subtree even
    // when the outer left lane is NULL; scalar mode must skip that subtree.
    let schema = vec![ty(), ty(), ty()];
    let root = plus(column(0), plus(column(1), column(2)));
    let rows = vec![
        vec![Datum::Null, Datum::Int(i64::MAX), Datum::Int(1)],
        vec![Datum::Int(1), Datum::Int(1), Datum::Int(2)],
        vec![Datum::Int(3), Datum::Int(2), Datum::Int(1)],
    ];
    for selection in &selections {
        compare_arithmetic_profile(root.clone(), &schema, &rows, selection);
    }
}
#[test]
fn native_arithmetic_root_profiles_keep_whole_error_order_despite_window_tuning() {
    let root = plus(mul(column(0), int(2)), plus(column(1), int(1)));
    let schema = vec![ty(), ty()];
    let mut rows = vec![vec![Datum::Int(1), Datum::Int(i64::MAX)]; 2050];
    rows[2049] = vec![Datum::Int(i64::MAX), Datum::Int(1)];
    let selection: Vec<_> = (0..2050).collect();
    compare_arithmetic_profile(root.clone(), &schema, &rows, &selection);
    let actual_suite = suite(root.clone());
    let reference_suite = suite(root);
    let ctx = ArithmeticProfilesContext::default();
    ctx.enabled.set(true);
    let expected_ctx = ArithmeticProfilesContext::default();
    expected_ctx.enabled.set(true);
    let mut input = chunk(&schema, &rows);
    let mut expected_input = chunk(&schema, &rows);
    let mut output = output();
    let mut expected_output = self::output();
    let actual = actual_suite.run_with_tikv_row_ablation(&ctx, &mut input, &mut output, &schema);
    let expected = reference_suite.run(&expected_ctx, &mut expected_input, &mut expected_output);
    assert_eq!(actual, expected);
    assert_eq!(ctx.trace.take(), expected_ctx.trace.take());
    let routes = actual_suite.batch_route_snapshot();
    assert_eq!((routes.refusals, routes.fallback), (0, 0));
}

#[test]
fn native_arithmetic_root_decimal_priority_preserves_wide_refusals_nested_phases_and_empty_literals(
) {
    let decimal = FieldType::new(FieldTypeCode::NewDecimal)
        .with_flen(65)
        .with_decimal(6);
    let schema = vec![decimal.clone(), decimal.clone()];
    let dec = |value: &str| Datum::Decimal(Decimal::from_literal(value));
    let rows = vec![
        vec![
            dec("-170141183460469231731687303715884105728"),
            dec("-170141183460469231731687303715884105728"),
        ],
        vec![dec("100000000000000000000"), dec("100000000000000000000")],
        vec![
            dec("100000000000000000000000000000000000000000000000000"),
            dec("1"),
        ],
        vec![Datum::Null, dec("2.500")],
    ];
    let c = |index| typed_column(index, &decimal);
    let d = |value: &str| literal(dec(value), decimal.clone());
    let roots = vec![
        call("minus", decimal.clone(), vec![c(0), c(1)]),
        call("mul", decimal.clone(), vec![c(0), c(1)]),
        call("plus", decimal.clone(), vec![c(0), d("0")]),
        call(
            "plus",
            decimal.clone(),
            vec![
                d("100000000000000000000000000000000000000000000000000"),
                c(1),
            ],
        ),
        call(
            "plus",
            decimal.clone(),
            vec![
                call("mul", decimal.clone(), vec![c(0), d("2")]),
                call("minus", decimal.clone(), vec![c(1), d("1")]),
            ],
        ),
        // A Decimal result alone does not enable native Decimal priority:
        // the original fast owner also requires a Decimal argument.
        call("plus", decimal.clone(), vec![int(1), int(2)]),
    ];
    let selections = vec![
        vec![],
        vec![0],
        vec![1],
        vec![2],
        vec![3],
        vec![3, 2, 0, 3],
        (0..2050).map(|index| [3, 2, 0][index % 3]).collect(),
    ];
    for root in roots {
        for selection in &selections {
            compare_arithmetic_profile(root.clone(), &schema, &rows, selection);
        }
    }
}

#[test]
fn native_arithmetic_decimal_priority_resources_are_terminal_and_phase_metadata_is_shared() {
    let decimal = FieldType::new(FieldTypeCode::NewDecimal)
        .with_flen(40)
        .with_decimal(4);
    let schema = vec![decimal.clone(), decimal.clone()];
    let root = call(
        "plus",
        decimal.clone(),
        vec![typed_column(0, &decimal), typed_column(1, &decimal)],
    );
    let owner = suite(root);
    let mut too_few = limits();
    too_few.tree.max_nodes = 8;
    let rejected = PreparedBatchSuite::compile(
        &owner,
        &schema,
        true,
        771,
        too_few,
        ExecutionLimits::default(),
        1 << 20,
    )
    .err()
    .unwrap();
    assert!(matches!(
        rejected.local_error(),
        Some(LocalError::ResourceLimit(_))
    ));
    let mut exact = limits();
    exact.tree.max_nodes = 9;
    let mut prepared = PreparedBatchSuite::compile(
        &owner,
        &schema,
        true,
        771,
        exact,
        ExecutionLimits {
            max_retained_bytes: 0,
            ..ExecutionLimits::default()
        },
        1 << 20,
    )
    .unwrap();
    assert_eq!(prepared.workers[0].compiled_node_count().unwrap(), 9);
    assert!(prepared.workers[0].fast_profile.is_some());
    let ctx = ArithmeticProfilesContext::default();
    ctx.enabled.set(false);
    let rows = vec![vec![
        Datum::Decimal(Decimal::from_int(2)),
        Datum::Decimal(Decimal::from_int(3)),
    ]];
    let mut input = chunk(&schema, &rows);
    let mut output = output_for(&decimal);
    let rejected = prepared
        .run(
            &owner,
            &ctx,
            &mut EvalContext::default(),
            &mut input,
            &mut output,
            &schema,
        )
        .unwrap_err();
    assert!(matches!(
        rejected.local_error(),
        Some(LocalError::ResourceLimit(_))
    ));
    assert_eq!(output.num_rows(), 0);
    assert!(
        ctx.trace.borrow().is_empty(),
        "no vector flag or generic context after resource refusal"
    );
    assert_eq!(prepared.workers[0].windows, 0);
    prepared.workers[0].limits = ExecutionLimits::default();
    prepared
        .run(
            &owner,
            &ctx,
            &mut EvalContext::default(),
            &mut input,
            &mut output,
            &schema,
        )
        .unwrap();
    assert!(
        ctx.trace.borrow().is_empty(),
        "successful Decimal priority ignores disabled vector flag"
    );
    assert_eq!(output.num_rows(), 1);
    assert_eq!(
        output.get_row(0).get_datum(0, &decimal),
        Datum::Decimal(Decimal::from_int(5))
    );
}

#[test]
fn rewritten_arithmetic_roots_use_canonical_priority_and_bulk_row_profiles() {
    use crate::rewriter::{rewrite_expr_resolved, ColumnResolver};
    use tidb_ast::{QueryStmt, SelectField, Stmt};
    struct Resolver(Vec<FieldType>);
    impl ColumnResolver for Resolver {
        fn time_zone(&self) -> SessionTimeZone {
            SessionTimeZone::utc()
        }
        fn resolve(&self, path: &[String]) -> Option<(usize, FieldType, i64)> {
            let index = match path.last()?.as_str() {
                "i" => 0,
                "r" => 1,
                "d" => 2,
                "s" => 3,
                _ => return None,
            };
            Some((index, self.0[index].clone(), index as i64 + 1))
        }
    }
    let schema = vec![
        ty(),
        real_ty(),
        FieldType::new(FieldTypeCode::NewDecimal)
            .with_flen(65)
            .with_decimal(6),
        string_ty(),
    ];
    let rows = vec![
        vec![
            Datum::Int(2),
            Datum::Real(1.25),
            Datum::Decimal(Decimal::from_literal("1.500")),
            Datum::new_string("2.5tail"),
        ],
        vec![Datum::Null; 4],
        vec![
            Datum::Int(i64::MAX),
            Datum::Real(f64::MAX),
            Datum::Decimal(Decimal::from_literal(
                "100000000000000000000000000000000000000000000000000",
            )),
            Datum::new_string("3"),
        ],
    ];
    let resolver = Resolver(schema.clone());
    for sql in [
        "i+i",
        "i-i",
        "i*i",
        "(i*2)+(i+1)",
        "r+r",
        "r-r",
        "r*r",
        "d+d",
        "d-d",
        "d*d",
        "d*(d+1)",
        "s+r",
        "abs(i)",
        "abs(i+1)",
        "cast(i as decimal(65,4))",
        "cast(i as decimal(3,1))",
        "cast(i as decimal)",
        "cast(i as json)",
        "cast(s as json)",
        "cast(cast(i as decimal(65,4)) as json)",
        "d+cast(i as decimal(65,4))",
        "abs(i-1)+1",
        "cast(i as json)+r",
        "cast((i+1) as decimal(3,1))",
        "if(1, i + (9223372036854775807 * 2), 0)",
        "coalesce(i + (9223372036854775807 * 2), 7)",
        "ifnull(i + (9223372036854775807 * 2), 7)",
        "case when 1 then i + (9223372036854775807 * 2) else 0 end",
        "isnull(i + (9223372036854775807 * 2))",
        "(i + (9223372036854775807 * 2)) = 1",
        "abs(r*2)",
        "abs(r*r)",
        "abs(d*2)",
        "abs(d*d)",
        "abs(i)=abs(i)",
        "coalesce(abs(i),7)",
        "ifnull(abs(i),7)",
        "if(i,abs(i),7)",
        "case when i then abs(i) else 7 end",
        "abs(i) and abs(i)",
        "abs(i) or abs(i)",
        "isnull(abs(i))",
    ] {
        let Stmt::Query(query) = tidb_parser::parse(&format!("select {sql}")).unwrap() else {
            panic!("query")
        };
        let QueryStmt::Select(select) = query.into_inner() else {
            panic!("select")
        };
        let SelectField::Expr { expr, .. } = &select.fields[0] else {
            panic!("expression")
        };
        let root = rewrite_expr_resolved(expr, &resolver).unwrap();
        for selection in [
            vec![],
            vec![1, 0, 2, 1],
            (0..2050).map(|index| [1, 0, 2][index % 3]).collect(),
        ] {
            compare_arithmetic_profile(root.clone(), &schema, &rows, &selection);
        }
    }
}

#[test]
fn native_arithmetic_nonbatch_vector_roots_keep_original_row_domain() {
    let vector = FieldType::new(FieldTypeCode::VectorFloat32);
    let schema = vec![vector.clone(), vector.clone()];
    let v = |values: Vec<f32>| Datum::VectorFloat32(VectorFloat32::must_create(values));
    let rows = vec![
        vec![v(vec![1.0, -0.0]), v(vec![2.0, 3.0])],
        vec![Datum::Null, v(vec![2.0, 3.0])],
        vec![v(vec![1.0]), v(vec![2.0, 3.0])],
    ];
    for name in ["plus", "minus", "mul"] {
        let root = call(
            name,
            vector.clone(),
            vec![typed_column(0, &vector), typed_column(1, &vector)],
        );
        for selection in [
            vec![],
            vec![0, 1, 2, 0],
            (0..2050).map(|index| [0, 1, 2][index % 3]).collect(),
        ] {
            compare_arithmetic_profile(root.clone(), &schema, &rows, &selection);
        }
    }
}

#[test]
fn native_unary_roots_preserve_abs_cast_window_domains_and_decimal_priority() {
    let decimal = FieldType::new(FieldTypeCode::NewDecimal)
        .with_flen(65)
        .with_decimal(4);
    let json = FieldType::new(FieldTypeCode::Json).with_flags(FieldTypeFlags::PARSE_TO_JSON);
    let selections = vec![
        vec![],
        vec![0],
        vec![2, 0, 2],
        (0..2050).map(|index| [2, 0, 2][index % 3]).collect(),
    ];
    for unsigned in [false, true] {
        let source = ty().with_unsigned(unsigned);
        let schema = vec![source.clone()];
        let rows = vec![
            vec![Datum::Null],
            vec![Datum::Int(1)],
            vec![if unsigned {
                Datum::UInt(u64::MAX)
            } else {
                Datum::Int(i64::MIN)
            }],
        ];
        let c = || typed_column(0, &source);
        let roots = vec![
            call("abs", source.clone(), vec![c()]),
            call("cast_decimal", decimal.clone(), vec![c()]),
            call(
                "cast_decimal",
                decimal.clone().with_unsigned(true),
                vec![c()],
            ),
            call(
                "cast_decimal",
                FieldType::new(FieldTypeCode::NewDecimal)
                    .with_flen(3)
                    .with_decimal(1),
                vec![c()],
            ),
            call(
                "cast_decimal",
                FieldType::new(FieldTypeCode::NewDecimal),
                vec![c()],
            ),
            call("cast_json", json.clone(), vec![c()]),
        ];
        for root in roots {
            for selection in &selections {
                compare_arithmetic_profile(root.clone(), &schema, &rows, selection);
            }
        }
    }
    // The genuine integer bulk owner reads a strict bit carrier. Row ABS and
    // row DECIMAL CAST retain the original UInt value of this signed literal.
    let raw = || literal(Datum::UInt(u64::MAX), ty());
    for root in [
        call("abs", ty(), vec![raw()]),
        call("cast_decimal", decimal, vec![raw()]),
    ] {
        for selection in &selections {
            compare_arithmetic_profile(root.clone(), &[], &vec![vec![]; 3], selection);
        }
    }
    let schema = vec![string_ty()];
    let rows = vec![
        vec![Datum::new_string("{\"a\":[1,null]}")],
        vec![Datum::Null],
        vec![Datum::new_string("bad JSON")],
    ];
    for parse in [true, false] {
        let root = call(
            "cast_json",
            if parse {
                json.clone()
            } else {
                FieldType::new(FieldTypeCode::Json)
            },
            vec![string_column(0)],
        );
        for selection in &selections {
            compare_arithmetic_profile(root.clone(), &schema, &rows, selection);
        }
    }
}

#[test]
fn native_unary_arithmetic_nested_windows_keep_child_phases_before_conversion() {
    let decimal = FieldType::new(FieldTypeCode::NewDecimal)
        .with_flen(65)
        .with_decimal(4);
    let json = FieldType::new(FieldTypeCode::Json).with_flags(FieldTypeFlags::PARSE_TO_JSON);
    let schema = vec![ty(), decimal.clone()];
    let rows = vec![
        vec![Datum::Int(1), Datum::Decimal(Decimal::from_int(2))],
        vec![Datum::Int(i64::MIN), Datum::Null],
        vec![Datum::Int(i64::MAX), Datum::Decimal(Decimal::from_int(4))],
    ];
    let cast = || {
        call(
            "cast_decimal",
            decimal.clone(),
            vec![plus(column(0), int(1))],
        )
    };
    let roots = vec![
        cast(),
        plus(call("abs", ty(), vec![minus(column(0), int(1))]), int(0)),
        call(
            "plus",
            decimal.clone(),
            vec![cast(), typed_column(1, &decimal)],
        ),
        call("cast_json", json.clone(), vec![cast()]),
        call(
            "plus",
            real_ty(),
            vec![
                call("cast_json", json, vec![column(0)]),
                literal(Datum::Real(1.0), real_ty()),
            ],
        ),
    ];
    let selections = vec![
        vec![],
        vec![0],
        vec![2, 0, 2],
        (0..2050).map(|index| [2, 0, 1][index % 3]).collect(),
    ];
    for root in roots {
        for selection in &selections {
            compare_arithmetic_profile(root.clone(), &schema, &rows, selection);
        }
    }
}

#[test]
fn native_control_roots_keep_selected_arithmetic_null_demand() {
    let selected = || plus(column(0), mul(column(1), column(2)));
    let roots = [
        call("if", ty(), vec![int(1), selected(), int(0)]),
        call("coalesce", ty(), vec![selected(), int(7)]),
        call("ifnull", ty(), vec![selected(), int(7)]),
        call("case", ty(), vec![int(1), selected(), int(0)]),
        call("if", ty(), vec![int(0), selected(), int(7)]),
        call("coalesce", ty(), vec![int(7), selected()]),
        call("ifnull", ty(), vec![int(7), selected()]),
        call("case", ty(), vec![int(0), selected(), int(7)]),
        call("and", ty(), vec![selected(), int(0)]),
        call("or", ty(), vec![selected(), int(1)]),
        call("and", ty(), vec![int(0), selected()]),
        call("or", ty(), vec![int(1), selected()]),
        call("isnull", ty(), vec![selected()]),
        call("not", ty(), vec![selected()]),
        call("eq", ty(), vec![selected(), int(1)]),
        call("nulleq", ty(), vec![selected(), null()]),
    ];
    let schema = vec![ty(); 3];
    let rows = vec![
        vec![Datum::Null, Datum::Int(i64::MAX), Datum::Int(2)],
        vec![Datum::Int(1), Datum::Int(i64::MAX), Datum::Int(2)],
        vec![Datum::Int(2), Datum::Int(3), Datum::Int(4)],
    ];
    let selections = [
        vec![],
        vec![0],
        vec![0, 0],
        vec![1],
        vec![0, 1],
        vec![2, 0, 2],
        (0..2050).map(|index| [2, 0, 2][index % 3]).collect(),
    ];
    for root in roots {
        for selection in &selections {
            compare_arithmetic_profile(root.clone(), &schema, &rows, selection);
        }
    }
}

#[test]
fn native_control_batch_staging_is_bounded_terminal_and_reusable() {
    let root = call("coalesce", ty(), vec![plus(column(0), int(1)), int(7)]);
    let owner = suite(root);
    let schema = vec![ty()];
    let mut prepared = PreparedBatchSuite::compile(
        &owner,
        &schema,
        true,
        991,
        limits(),
        ExecutionLimits::default(),
        1 << 20,
    )
    .unwrap();
    assert!(prepared.workers[0].defer_batch_publication);
    let rows = vec![vec![Datum::Int(2)], vec![Datum::Null]];
    let selection: Vec<_> = (0..2050).map(|row| row % 2).collect();
    // Enough for the selection, imported values and one result; insufficient
    // for an owned complete result batch. Failure must leave output empty.
    prepared.workers[0].max_materialization_retained_bytes = 32 * 1024;
    let ctx = ArithmeticProfilesContext::default();
    ctx.enabled.set(true);
    let mut input = chunk(&schema, &rows);
    input.set_sel(Some(selection.clone()));
    let mut output = output_for(&ty());
    let error = prepared
        .run(
            &owner,
            &ctx,
            &mut EvalContext::default(),
            &mut input,
            &mut output,
            &schema,
        )
        .unwrap_err();
    assert!(matches!(
        error.local_error(),
        Some(LocalError::ResourceLimit(_))
    ));
    assert_eq!(output.num_rows(), 0);
    assert!(
        prepared.workers[0].windows > 0,
        "refusal occurs after genuine row execution"
    );
    prepared.workers[0].max_materialization_retained_bytes = 1 << 20;
    prepared
        .run(
            &owner,
            &ctx,
            &mut EvalContext::default(),
            &mut input,
            &mut output,
            &schema,
        )
        .unwrap();
    assert_eq!(output.num_rows(), selection.len());
    assert_eq!(
        values(&output),
        selection
            .into_iter()
            .map(|row| Datum::Int(if row == 0 { 3 } else { 7 }))
            .collect::<Vec<_>>()
    );
}

#[test]
fn native_abs_real_root_keeps_batch_publication_after_late_row_error() {
    let root = call(
        "abs",
        real_ty(),
        vec![call(
            "mul",
            real_ty(),
            vec![real_column(0), literal(Datum::Real(2.0), real_ty())],
        )],
    );
    let schema = vec![real_ty()];
    let rows = vec![
        vec![Datum::Real(1.0)],
        vec![Datum::Real(f64::MAX)],
        vec![Datum::Null],
    ];
    compare_arithmetic_profile(root, &schema, &rows, &[0, 1]);
}

#[test]
fn native_row_value_roots_keep_batch_publication_and_independent_row_demand() {
    let decimal = FieldType::new(FieldTypeCode::NewDecimal)
        .with_flen(65)
        .with_decimal(4);
    let real_mul = || {
        call(
            "mul",
            real_ty(),
            vec![real_column(0), literal(Datum::Real(2.0), real_ty())],
        )
    };
    let decimal_mul = || {
        call(
            "mul",
            decimal.clone(),
            vec![
                typed_column(1, &decimal),
                literal(Datum::Decimal(Decimal::from_int(2)), decimal.clone()),
            ],
        )
    };
    let roots = [
        call("abs", real_ty(), vec![real_mul()]),
        call("abs", decimal.clone(), vec![decimal_mul()]),
        call("cast_double", real_ty(), vec![decimal_mul()]),
        call("cast_decimal", decimal.clone(), vec![real_mul()]),
    ];
    let schema = vec![real_ty(), decimal.clone()];
    let rows = vec![
        vec![
            Datum::Real(-1.0),
            Datum::Decimal(Decimal::from_literal("-1.2500")),
        ],
        vec![
            Datum::Real(f64::MAX),
            Datum::Decimal(Decimal::from_literal(
                "99999999999999999999999999999999999999999999999999999999999999999",
            )),
        ],
        vec![Datum::Null, Datum::Null],
    ];
    let selections = [
        vec![],
        vec![0],
        vec![2, 0, 2],
        vec![0, 1],
        vec![1, 0],
        (0..2050).map(|row| [2, 0, 2][row % 3]).collect(),
        (0..2050).map(|row| usize::from(row == 2049)).collect(),
    ];
    for root in roots {
        for selection in &selections {
            compare_arithmetic_profile(root.clone(), &schema, &rows, selection);
        }
    }
}

#[test]
fn native_row_value_publication_preserves_existing_output_and_row_failure_prefix() {
    let root = call(
        "abs",
        real_ty(),
        vec![call(
            "mul",
            real_ty(),
            vec![real_column(0), literal(Datum::Real(2.0), real_ty())],
        )],
    );
    let actual_suite = suite(root.clone());
    let reference_suite = suite(root);
    let schema = vec![real_ty()];
    let rows = vec![vec![Datum::Real(1.0)], vec![Datum::Real(f64::MAX)]];
    for enabled in [true, false, true, false] {
        let actual_ctx = ArithmeticProfilesContext::default();
        let expected_ctx = ArithmeticProfilesContext::default();
        actual_ctx.enabled.set(enabled);
        expected_ctx.enabled.set(enabled);
        let mut input = chunk(&schema, &rows);
        let mut expected_input = chunk(&schema, &rows);
        let mut output = output_for(&real_ty());
        let mut expected_output = output_for(&real_ty());
        output.append_float64(0, -99.0);
        expected_output.append_float64(0, -99.0);
        let actual =
            actual_suite.run_with_tikv_batch(&actual_ctx, &mut input, &mut output, &schema);
        let expected =
            reference_suite.run(&expected_ctx, &mut expected_input, &mut expected_output);
        assert_eq!(actual, expected);
        assert!(actual.is_err());
        assert_eq!(actual_ctx.trace.take(), expected_ctx.trace.take());
        assert_eq!(output.num_rows(), expected_output.num_rows());
        assert_eq!(output.num_rows(), if enabled { 1 } else { 2 });
        for row in 0..output.num_rows() {
            assert_eq!(
                output.column(0).get_raw(row).as_ref(),
                expected_output.column(0).get_raw(row).as_ref()
            );
        }
        let routes = actual_suite.batch_route_snapshot();
        assert_eq!((routes.fallback, routes.refusals), (0, 0));
    }
}

#[test]
fn native_noncall_roots_do_not_acquire_batch_publication_storage() {
    let decimal = FieldType::new(FieldTypeCode::NewDecimal)
        .with_flen(65)
        .with_decimal(4);
    let schema = vec![decimal.clone()];
    let rows = vec![vec![Datum::Decimal(Decimal::from_literal("-1.2500"))]];
    let selection = vec![0; 2050];
    for root in [
        literal(Datum::Decimal(Decimal::from_int(2)), decimal.clone()),
        typed_column(0, &decimal),
    ] {
        // Force a calculated column, so the genuine non-call native route is
        // tested without the independent direct-column ownership move.
        let owner = EvaluatorSuite::new(vec![root.clone()], true);
        let reference = EvaluatorSuite::new(vec![root], true);
        let mut prepared = PreparedBatchSuite::compile(
            &owner,
            &schema,
            true,
            993,
            limits(),
            ExecutionLimits::default(),
            32 * 1024,
        )
        .unwrap();
        prepared.set_window_size(1);
        let ctx = ArithmeticProfilesContext::default();
        let reference_ctx = ArithmeticProfilesContext::default();
        ctx.enabled.set(true);
        reference_ctx.enabled.set(true);
        let mut input = chunk(&schema, &rows);
        input.set_sel(Some(selection.clone()));
        let mut reference_input = chunk(&schema, &rows);
        reference_input.set_sel(Some(selection.clone()));
        let mut output = output_for(&decimal);
        let mut expected_output = output_for(&decimal);
        let actual = prepared.run(
            &owner,
            &ctx,
            &mut EvalContext::default(),
            &mut input,
            &mut output,
            &schema,
        );
        let expected = reference.run(&reference_ctx, &mut reference_input, &mut expected_output);
        assert!(expected.is_ok());
        assert!(
            actual.is_ok(),
            "non-call row/broadcast route gained batch staging: {actual:?}"
        );
        assert_eq!(ctx.trace.take(), reference_ctx.trace.take());
        assert_eq!(output.num_rows(), expected_output.num_rows());
        for row in 0..output.num_rows() {
            assert_eq!(
                output.column(0).get_raw(row).as_ref(),
                expected_output.column(0).get_raw(row).as_ref()
            );
        }
    }
}

#[test]
fn native_comparison_abs_descendants_keep_row_operand_error_order() {
    let root = call("eq", ty(), vec![abs(column(0)), abs(column(1))]);
    let schema = vec![ty(), ty()];
    let rows = vec![
        vec![Datum::Int(1), Datum::Int(i64::MIN)],
        vec![Datum::Int(i64::MIN), Datum::Int(1)],
    ];
    compare_arithmetic_profile(root, &schema, &rows, &[0, 1]);
}

#[test]
fn native_closed_controls_without_arithmetic_keep_row_demand_and_error_identity() {
    let a = || abs(column(0));
    let b = || abs(column(1));
    let mut roots = vec![
        call("if", ty(), vec![column(2), a(), b()]),
        call("ifnull", ty(), vec![a(), b()]),
        call("coalesce", ty(), vec![a(), b(), int(7)]),
        call("case", ty(), vec![column(2), a(), int(1), b(), int(7)]),
        call("and", ty(), vec![a(), b()]),
        call("or", ty(), vec![a(), b()]),
    ];
    for name in [
        "not",
        "isnull",
        "istrue",
        "isfalse",
        "isnottrue",
        "isnotfalse",
        "isunknown",
        "isnotunknown",
        "istrue_with_null",
    ] {
        roots.push(call(name, ty(), vec![a()]));
    }
    for name in ["eq", "ne", "lt", "le", "gt", "ge", "nulleq", "nullif"] {
        roots.push(call(name, ty(), vec![a(), b()]));
    }
    let schema = vec![ty(); 3];
    let rows = vec![
        vec![Datum::Int(1), Datum::Int(i64::MIN), Datum::Int(1)],
        vec![Datum::Int(i64::MIN), Datum::Int(1), Datum::Int(0)],
        vec![Datum::Null, Datum::Int(i64::MIN), Datum::Null],
        vec![Datum::Int(0), Datum::Int(i64::MIN), Datum::Int(0)],
        vec![Datum::Int(2), Datum::Int(3), Datum::Null],
    ];
    let mut late_error = vec![4; 2050];
    late_error[2049] = 0;
    let selections = [
        vec![],
        vec![0, 1],
        vec![1, 0],
        vec![2, 0, 2],
        vec![3, 0, 3],
        vec![4; 2050],
        late_error,
    ];
    for root in roots {
        for selection in &selections {
            compare_arithmetic_profile(root.clone(), &schema, &rows, selection);
        }
    }
}

fn compare_typed_profile(
    root: Expression,
    schema: &[FieldType],
    rows: &[Vec<Datum>],
    selection: &[usize],
) {
    let field = snapshot_field_type(root.static_type().unwrap());
    let actual_suite = suite(root.clone());
    let reference_suite = suite(root);
    for enabled in [true, false, true, false] {
        let actual_ctx = ArithmeticProfilesContext::default();
        let reference_ctx = ArithmeticProfilesContext::default();
        actual_ctx.enabled.set(enabled);
        reference_ctx.enabled.set(enabled);
        let mut input = chunk(schema, rows);
        let mut reference_input = chunk(schema, rows);
        input.set_sel(Some(selection.to_vec()));
        reference_input.set_sel(Some(selection.to_vec()));
        let mut output = output_for(&field);
        let mut expected_output = output_for(&field);
        let actual = actual_suite.run_with_tikv_batch(&actual_ctx, &mut input, &mut output, schema);
        let expected =
            reference_suite.run(&reference_ctx, &mut reference_input, &mut expected_output);
        assert_eq!(actual, expected, "flag={enabled}, selection={selection:?}");
        assert_eq!(actual_ctx.trace.take(), reference_ctx.trace.take());
        assert_eq!(
            output.num_rows(),
            expected_output.num_rows(),
            "flag={enabled}"
        );
        for row in 0..output.num_rows() {
            assert_eq!(
                output.get_row(row).is_null(0),
                expected_output.get_row(row).is_null(0)
            );
            assert_eq!(
                output.column(0).get_raw(row).as_ref(),
                expected_output.column(0).get_raw(row).as_ref()
            );
        }
        let route = actual_suite.batch_route_snapshot();
        assert_eq!((route.refusals, route.fallback), (0, 0));
    }
}
#[test]
fn typed_concat_output_is_published_only_after_eligible_batch_succeeds() {
    let root = call(
        "concat",
        string_ty(),
        vec![
            call("cast_char", string_ty(), vec![abs(column(0))]),
            text(Some(b"x")),
        ],
    );
    let schema = vec![ty()];
    let rows = vec![vec![Datum::Int(1)], vec![Datum::Int(i64::MIN)]];
    compare_typed_profile(root, &schema, &rows, &[0, 1]);
}

#[test]
fn typed_pending_outputs_keep_all_carriers_and_native_null_layouts() {
    let time =
        tidb_datatype::parse_datetime("2026-10-17 12:34:56.123456", &chrono_tz::UTC, true, false)
            .unwrap()
            .time;
    let cases = [
        (ty(), Datum::Int(-17)),
        (ty().with_unsigned(true), Datum::UInt(u64::MAX)),
        (real_ty(), Datum::Real(-0.0)),
        (
            FieldType::new(FieldTypeCode::NewDecimal)
                .with_flen(65)
                .with_decimal(4),
            Datum::Decimal(Decimal::from_literal("-1.2300")),
        ),
        (string_ty(), Datum::new_string("A\0ß")),
        (
            FieldType::new(FieldTypeCode::Datetime).with_decimal(6),
            Datum::Time(time),
        ),
        (
            FieldType::new(FieldTypeCode::Duration).with_decimal(6),
            Datum::Duration(MySqlDuration::from_nanoseconds(-123456789012, 6).unwrap()),
        ),
        (
            FieldType::new(FieldTypeCode::Json),
            Datum::Json(BinaryJSON::parse("{\"a\":[1,null]}").unwrap()),
        ),
        (
            FieldType::new(FieldTypeCode::VectorFloat32),
            Datum::VectorFloat32(VectorFloat32::must_create(vec![-0.0, 1.25])),
        ),
    ];
    for (field, value) in cases {
        let roots = [
            call("any_value", field.clone(), vec![typed_column(0, &field)]),
            call(
                "any_value",
                field.clone(),
                vec![literal(value.clone(), field.clone())],
            ),
        ];
        let rows = vec![vec![value.clone()], vec![Datum::Null], vec![value]];
        for selection in [
            vec![],
            vec![0],
            vec![2, 1, 2],
            (0..2050).map(|row| [2, 1, 2][row % 3]).collect(),
        ] {
            for root in &roots {
                compare_typed_profile(
                    root.clone(),
                    std::slice::from_ref(&field),
                    &rows,
                    &selection,
                );
            }
        }
    }
}

#[test]
fn typed_concat_pending_success_and_errors_preserve_output_aliases_and_prefix() {
    let field = string_ty();
    let root = call(
        "concat",
        field.clone(),
        vec![
            call("cast_char", field.clone(), vec![abs(column(0))]),
            text(Some(b"x")),
        ],
    );
    let owner = suite(root.clone());
    let reference = suite(root);
    let schema = vec![ty()];
    let rows = vec![
        vec![Datum::Int(1)],
        vec![Datum::Int(i64::MIN)],
        vec![Datum::Int(2)],
    ];
    for enabled in [true, false, true, false] {
        for selection in [vec![], vec![0, 2, 0], vec![0, 1]] {
            let ctx = ArithmeticProfilesContext::default();
            let reference_ctx = ArithmeticProfilesContext::default();
            ctx.enabled.set(enabled);
            reference_ctx.enabled.set(enabled);
            let mut input = chunk(&schema, &rows);
            input.set_sel(Some(selection.clone()));
            let mut reference_input = chunk(&schema, &rows);
            reference_input.set_sel(Some(selection));
            let mut output = output_for(&field);
            let mut expected = output_for(&field);
            output.append_datum(0, &Datum::new_string("seed"));
            expected.append_datum(0, &Datum::new_string("seed"));
            let mut alias = output_for(&field);
            let mut reference_alias = output_for(&field);
            alias.make_ref_to(0, &mut output, 0).unwrap();
            reference_alias.make_ref_to(0, &mut expected, 0).unwrap();
            let actual = owner.run_with_tikv_batch(&ctx, &mut input, &mut output, &schema);
            let native = reference.run(&reference_ctx, &mut reference_input, &mut expected);
            assert_eq!(actual, native);
            assert_eq!(ctx.trace.take(), reference_ctx.trace.take());
            assert!(output.columns_share_identity(0, &alias, 0));
            assert_eq!(alias.num_rows(), reference_alias.num_rows());
            for row in 0..alias.num_rows() {
                assert_eq!(
                    alias.column(0).get_raw(row).as_ref(),
                    reference_alias.column(0).get_raw(row).as_ref()
                );
            }
            assert_eq!(output.num_rows(), expected.num_rows());
            let routes = owner.batch_route_snapshot();
            assert_eq!((routes.fallback, routes.refusals), (0, 0));
        }
    }
}

#[test]
fn typed_pending_materialization_budget_is_terminal_and_reusable() {
    let field = string_ty();
    let suffix = vec![b'x'; 1024];
    let root = call(
        "concat",
        field.clone(),
        vec![
            call("cast_char", field.clone(), vec![abs(column(0))]),
            text(Some(&suffix)),
        ],
    );
    let owner = suite(root);
    let schema = vec![ty()];
    let mut prepared = PreparedBatchSuite::compile(
        &owner,
        &schema,
        true,
        994,
        limits(),
        ExecutionLimits::default(),
        32 * 1024,
    )
    .unwrap();
    let ctx = ArithmeticProfilesContext::default();
    ctx.enabled.set(true);
    let mut input = chunk(&schema, &[vec![Datum::Int(1)]]);
    input.set_sel(Some(vec![0; 2050]));
    let mut output = output_for(&field);
    output.append_datum(0, &Datum::new_string("seed"));
    let error = prepared
        .run(
            &owner,
            &ctx,
            &mut EvalContext::default(),
            &mut input,
            &mut output,
            &schema,
        )
        .unwrap_err();
    assert!(matches!(
        error.local_error(),
        Some(LocalError::ResourceLimit(_))
    ));
    assert_eq!(output.num_rows(), 1);
    assert_eq!(values_for(&output, &field), vec![Datum::new_string("seed")]);
    assert!(prepared.workers[0].windows > 0);
    assert_eq!(
        ctx.trace
            .borrow()
            .iter()
            .filter(|event| event.as_str() == "enable")
            .count(),
        1
    );
    prepared.workers[0].max_materialization_retained_bytes = 8 << 20;
    ctx.trace.borrow_mut().clear();
    prepared
        .run(
            &owner,
            &ctx,
            &mut EvalContext::default(),
            &mut input,
            &mut output,
            &schema,
        )
        .unwrap();
    assert_eq!(output.num_rows(), 2051);
    assert_eq!(output.column(0).get_raw(1).len(), 1025);
    assert_eq!(
        ctx.trace
            .borrow()
            .iter()
            .filter(|event| event.as_str() == "enable")
            .count(),
        1
    );
    // Generic empty input has no result owner, even under a zero runtime
    // materialization budget; the native per-output getter still occurs.
    let mut empty = PreparedBatchSuite::compile(
        &owner,
        &schema,
        true,
        995,
        limits(),
        ExecutionLimits::default(),
        0,
    )
    .unwrap();
    let mut input = chunk(&schema, &[]);
    let mut output = output_for(&field);
    ctx.trace.borrow_mut().clear();
    empty
        .run(
            &owner,
            &ctx,
            &mut EvalContext::default(),
            &mut input,
            &mut output,
            &schema,
        )
        .unwrap();
    assert_eq!(output.num_rows(), 0);
    assert_eq!(ctx.trace.take(), vec!["enable"]);
}

#[test]
fn typed_duration_single_binding_import_preserves_full_native_nanoseconds() {
    let field = FieldType::new(FieldTypeCode::Duration).with_decimal(6);
    let owner = suite(call(
        "any_value",
        field.clone(),
        vec![typed_column(0, &field)],
    ));
    let schema = vec![field.clone()];
    let rows: Vec<_> = [-123_456_789_012, 123_456_789_012, i64::MIN, i64::MAX]
        .into_iter()
        .map(|nanos| vec![Datum::Duration(MySqlDuration::from_raw_parts(nanos, 6))])
        .collect();
    let mut worker = prepare(&owner, &schema);
    let mut source = Raw::new(&worker, rows.clone());
    let mut input = chunk(&schema, &rows);
    input.set_sel(Some(vec![1, 0, 1, 3, 2]));
    let mut output = output_for(&field);
    run_provider(
        &owner,
        &mut worker,
        &mut EvalContext::default(),
        &mut input,
        &mut output,
        &schema,
        &mut source,
    )
    .unwrap();
    let mut reference_input = chunk(&schema, &rows);
    reference_input.set_sel(Some(vec![1, 0, 1, 3, 2]));
    let mut expected = output_for(&field);
    owner
        .run(&Context::new(true), &mut reference_input, &mut expected)
        .unwrap();
    assert_eq!(output.num_rows(), expected.num_rows());
    for row in 0..output.num_rows() {
        assert_eq!(
            output.column(0).get_raw(row).as_ref(),
            expected.column(0).get_raw(row).as_ref()
        );
    }
    assert_eq!(
        source.reads,
        [1, 0, 1, 3, 2]
            .into_iter()
            .enumerate()
            .map(|(occurrence, input_row)| (
                0,
                InputRow {
                    occurrence,
                    input_row
                }
            ))
            .collect::<Vec<_>>()
    );
}

#[test]
fn native_bitwise_abs_operands_keep_original_row_demand_and_sql_errors() {
    let root = call(
        "bitxor",
        ty().with_unsigned(true),
        vec![abs(column(0)), abs(column(1))],
    );
    let schema = vec![ty(), ty()];
    let rows = vec![
        vec![Datum::Int(1), Datum::Int(i64::MIN)],
        vec![Datum::Int(i64::MIN), Datum::Int(1)],
    ];
    compare_typed_profile(root, &schema, &rows, &[0, 1]);
}

#[test]
fn native_bitwise_roots_and_whole_parents_keep_original_operand_phases() {
    let bits = ty().with_unsigned(true);
    let schema = vec![ty(); 3];
    let rows = vec![
        vec![Datum::Int(1), Datum::Int(2), Datum::Int(i64::MIN)],
        vec![Datum::Int(i64::MIN), Datum::Int(1), Datum::Int(1)],
        vec![Datum::Null, Datum::Int(i64::MIN), Datum::Int(1)],
        vec![Datum::Int(2), Datum::Int(3), Datum::Int(4)],
    ];
    let mut roots = Vec::new();
    for name in ["bitand", "bitor", "bitxor", "leftshift", "rightshift"] {
        let b = || call(name, bits.clone(), vec![abs(column(0)), abs(column(1))]);
        roots.extend([
            b(),
            call("plus", bits.clone(), vec![b(), abs(column(2))]),
            call("abs", bits.clone(), vec![b()]),
            call("ifnull", bits.clone(), vec![b(), int(0)]),
            call(
                name,
                bits.clone(),
                vec![plus(column(0), column(1)), abs(column(2))],
            ),
        ]);
    }
    roots.push(call("bitneg", bits.clone(), vec![abs(column(0))]));
    roots.push(call(
        "abs",
        bits.clone(),
        vec![call("bitneg", bits, vec![plus(column(0), column(1))])],
    ));
    for selection in [
        vec![],
        vec![3],
        vec![0, 1],
        vec![1, 0],
        vec![2, 3, 2],
        vec![3; 2050],
        (0..2050)
            .map(|row| if row == 2049 { 1 } else { 0 })
            .collect(),
    ] {
        for root in &roots {
            compare_arithmetic_profile(root.clone(), &schema, &rows, &selection);
        }
    }
}

fn native_operator_value_cases() -> Vec<(Datum, FieldType)> {
    use tidb_datatype::{BinaryLiteral, MysqlEnum, MysqlSet};
    let decimal = FieldType::new(FieldTypeCode::NewDecimal)
        .with_flen(65)
        .with_decimal(6);
    let time =
        tidb_datatype::parse_datetime("2026-10-17 12:34:56.123456", &chrono_tz::UTC, true, false)
            .unwrap()
            .time;
    let values = vec![
        (Datum::Null, ty()),
        (Datum::Int(-1), ty()),
        (Datum::Int(-1), ty().with_unsigned(true)),
        (Datum::UInt(u64::MAX), ty().with_unsigned(true)),
        (Datum::Real(-1.5), real_ty()),
        (Datum::Real(f64::INFINITY), real_ty()),
        (Datum::Real(f64::from_bits(0x7ff8000000000042)), real_ty()),
        (Datum::Float32(2.5), FieldType::new(FieldTypeCode::Float)),
        (
            Datum::Decimal(Decimal::from_literal("-1.500")),
            decimal.clone(),
        ),
        (
            Datum::Decimal(Decimal::from_literal(
                "99999999999999999999999999999999999999999999999999999999999999999",
            )),
            decimal.clone(),
        ),
        (Datum::new_string("-3.7tail"), string_ty()),
        (Datum::Bytes(vec![b'2', 0xff]), string_ty()),
        (
            Datum::BinaryLiteral(BinaryLiteral::from(vec![0xff; 8])),
            string_ty(),
        ),
        (
            Datum::Bit(BinaryLiteral::from(vec![0xa5])),
            FieldType::new(FieldTypeCode::Bit),
        ),
        (
            Datum::Enum(MysqlEnum::new("b", 2), Collation::GbkBin),
            string_ty(),
        ),
        (
            Datum::Set(MysqlSet::new("a,b", 3), Collation::GbkBin),
            string_ty(),
        ),
        (
            Datum::Duration(MySqlDuration::from_nanoseconds(-123456789012, 6).unwrap()),
            FieldType::new(FieldTypeCode::Duration).with_decimal(6),
        ),
        (
            Datum::Time(time),
            FieldType::new(FieldTypeCode::Datetime).with_decimal(6),
        ),
        (
            Datum::Json(BinaryJSON::parse("{\"a\":[1,null]}").unwrap()),
            FieldType::new(FieldTypeCode::Json),
        ),
        (
            Datum::VectorFloat32(VectorFloat32::must_create(vec![1.0])),
            FieldType::new(FieldTypeCode::VectorFloat32),
        ),
        (Datum::Raw(vec![0xff]), string_ty()),
        (Datum::MinNotNull, ty()),
        (Datum::MaxValue, ty()),
    ];
    values
}

#[test]
fn native_bitwise_values_keep_all_carriers_null_conversions_and_warnings() {
    let values = native_operator_value_cases();
    let decimal = FieldType::new(FieldTypeCode::NewDecimal)
        .with_flen(65)
        .with_decimal(6);
    let bits = ty().with_unsigned(true);
    for (value, field) in &values {
        let arg = || literal(value.clone(), field.clone());
        let mut roots = vec![call("bitneg", bits.clone(), vec![arg()])];
        for name in ["bitand", "bitor", "bitxor", "leftshift", "rightshift"] {
            for partner in [
                int(1),
                literal(Datum::Real(2.5), real_ty()),
                null(),
                literal(
                    Datum::Decimal(Decimal::from_literal(
                        "99999999999999999999999999999999999999999999999999999999999999999",
                    )),
                    decimal.clone(),
                ),
            ] {
                roots.push(call(name, bits.clone(), vec![arg(), partner.clone()]));
                roots.push(call(name, bits.clone(), vec![partner, arg()]));
            }
        }
        for root in roots {
            for selection in [vec![], vec![0], vec![2, 0, 2]] {
                compare_arithmetic_profile(root.clone(), &[], &vec![vec![]; 3], &selection);
            }
        }
    }
    // Column transport supplies the eight real scalar carriers, independent
    // of the literal path. Retain NULL and repeated physical rows as well.
    for (value, field) in values.into_iter().filter(|(value, _)| {
        matches!(
            value,
            Datum::Int(_)
                | Datum::UInt(_)
                | Datum::Real(_)
                | Datum::Float32(_)
                | Datum::Decimal(_)
                | Datum::String(_)
                | Datum::Duration(_)
                | Datum::Time(_)
                | Datum::Json(_)
        )
    }) {
        let schema = vec![field.clone()];
        let rows = vec![vec![value], vec![Datum::Null]];
        for name in [
            "bitand",
            "bitor",
            "bitxor",
            "leftshift",
            "rightshift",
            "bitneg",
        ] {
            let mut args = vec![typed_column(0, &field)];
            if name != "bitneg" {
                args.push(int(1));
            }
            let root = call(name, bits.clone(), args);
            for selection in [vec![], vec![0], vec![1, 0, 1]] {
                compare_arithmetic_profile(root.clone(), &schema, &rows, &selection);
            }
        }
    }
}

#[test]
fn native_logical_xor_abs_operands_keep_original_row_demand_and_sql_errors() {
    let root = call("xor", ty(), vec![abs(column(0)), abs(column(1))]);
    let schema = vec![ty(), ty()];
    let rows = vec![
        vec![Datum::Int(1), Datum::Int(i64::MIN)],
        vec![Datum::Int(i64::MIN), Datum::Int(1)],
    ];
    compare_typed_profile(root, &schema, &rows, &[0, 1]);
}

#[test]
fn native_logical_xor_roots_and_parents_keep_native_operand_phases() {
    let schema = vec![ty(); 3];
    let rows = vec![
        vec![Datum::Int(1), Datum::Int(2), Datum::Int(i64::MIN)],
        vec![Datum::Int(i64::MIN), Datum::Int(1), Datum::Int(1)],
        vec![Datum::Null, Datum::Int(i64::MIN), Datum::Int(1)],
        vec![Datum::Int(2), Datum::Int(3), Datum::Int(4)],
    ];
    let x = || call("xor", ty(), vec![abs(column(0)), abs(column(1))]);
    let roots = [
        x(),
        plus(x(), abs(column(2))),
        abs(x()),
        call("if", ty(), vec![x(), abs(column(2)), int(0)]),
        call(
            "xor",
            ty(),
            vec![plus(column(0), column(1)), abs(column(2))],
        ),
        call("xor", ty(), vec![null(), abs(column(1))]),
        call("ifnull", ty(), vec![x(), int(0)]),
        call("xor", ty(), vec![x(), abs(column(2))]),
    ];
    for selection in [
        vec![],
        vec![3],
        vec![0, 1],
        vec![1, 0],
        vec![2, 3, 2],
        vec![3; 2050],
        (0..2050)
            .map(|row| if row == 2049 { 1 } else { 0 })
            .collect(),
    ] {
        for root in &roots {
            compare_arithmetic_profile(root.clone(), &schema, &rows, &selection);
        }
    }
}

#[test]
fn native_logical_xor_values_keep_all_carriers_and_both_warning_probes() {
    let values = native_operator_value_cases();
    for (value, field) in &values {
        let arg = || literal(value.clone(), field.clone());
        for partner in [
            int(1),
            int(0),
            null(),
            text(Some(b"2tail")),
            literal(
                Datum::Decimal(Decimal::from_literal(
                    "99999999999999999999999999999999999999999999999999999999999999999",
                )),
                FieldType::new(FieldTypeCode::NewDecimal).with_flen(65),
            ),
        ] {
            for args in [vec![arg(), partner.clone()], vec![partner, arg()]] {
                let root = call("xor", ty(), args);
                for selection in [vec![], vec![0], vec![2, 0, 2]] {
                    compare_arithmetic_profile(root.clone(), &[], &vec![vec![]; 3], &selection);
                }
            }
        }
    }
    for (value, field) in values.into_iter().filter(|(value, _)| {
        matches!(
            value,
            Datum::Int(_)
                | Datum::UInt(_)
                | Datum::Real(_)
                | Datum::Float32(_)
                | Datum::Decimal(_)
                | Datum::String(_)
                | Datum::Duration(_)
                | Datum::Time(_)
                | Datum::Json(_)
        )
    }) {
        let rows = vec![vec![value], vec![Datum::Null]];
        for args in [
            vec![typed_column(0, &field), int(1)],
            vec![int(1), typed_column(0, &field)],
        ] {
            let root = call("xor", ty(), args);
            for selection in [vec![], vec![0], vec![1, 0, 1]] {
                compare_arithmetic_profile(
                    root.clone(),
                    std::slice::from_ref(&field),
                    &rows,
                    &selection,
                );
            }
        }
    }
}

#[test]
fn native_logical_xor_strict_warning_probe_errors_are_ignored_before_truth_conversion() {
    struct Strict {
        enabled: bool,
        trace: RefCell<Vec<String>>,
    }
    impl Columns for Strict {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }
        fn enable_vectorized_expression(&self) -> bool {
            self.trace.borrow_mut().push("enable".into());
            self.enabled
        }
        fn div_precision_increment(&self) -> u32 {
            self.trace.borrow_mut().push("div".into());
            4
        }
        fn handle_truncate(&self, message: &str) -> std::result::Result<(), EvalError> {
            self.trace.borrow_mut().push(message.into());
            Err(EvalError::Unsupported("rejected XOR warning probe"))
        }
    }
    let warn = || text(Some(b"2tail"));
    let raw = || literal(Datum::Raw(vec![0xff]), string_ty());
    let roots = [
        call("xor", ty(), vec![warn(), text(Some(b"0tail"))]),
        call("xor", ty(), vec![null(), warn()]),
        call("xor", ty(), vec![raw(), warn()]),
        call("xor", ty(), vec![warn(), raw()]),
        call("xor", ty(), vec![literal(Datum::MinNotNull, ty()), warn()]),
        call(
            "xor",
            ty(),
            vec![call("cast_signed", ty(), vec![warn()]), abs(int(i64::MIN))],
        ),
    ];
    for root in roots {
        let actual_suite = suite(root.clone());
        let reference = suite(root);
        for enabled in [true, false, true, false] {
            let context = Strict {
                enabled,
                trace: RefCell::default(),
            };
            let native = Strict {
                enabled,
                trace: RefCell::default(),
            };
            let mut input = Chunk::new_with_capacity(&[], 1);
            input.set_num_virtual_rows(1);
            let mut expected_input = Chunk::new_with_capacity(&[], 1);
            expected_input.set_num_virtual_rows(1);
            let mut output = output_for(&ty());
            let mut expected = output_for(&ty());
            let result = actual_suite.run_with_tikv_batch(&context, &mut input, &mut output, &[]);
            let native_result = reference.run(&native, &mut expected_input, &mut expected);
            assert_eq!(result, native_result);
            assert_eq!(context.trace.take(), native.trace.take());
            assert_eq!(values_for(&output, &ty()), values_for(&expected, &ty()));
            let route = actual_suite.batch_route_snapshot();
            assert_eq!((route.refusals, route.fallback), (0, 0));
        }
    }
}

fn native_unary_sign_abs_operand_profile(name: &str) {
    let root = call(name, ty(), vec![abs(column(0))]);
    let schema = vec![ty()];
    let rows = vec![vec![Datum::Int(1)], vec![Datum::Int(i64::MIN)]];
    compare_typed_profile(root, &schema, &rows, &[0, 1]);
}
#[test]
fn native_unary_sign_abs_plus_requires_canonical_driver() {
    native_unary_sign_abs_operand_profile("unaryplus");
}
#[test]
fn native_unary_sign_abs_minus_requires_canonical_driver() {
    native_unary_sign_abs_operand_profile("unaryminus");
}

fn unary_sign_call(name: &str, argument: Expression) -> Expression {
    let field = crate::builtin_op::infer_unary_op_type(name, &argument)
        .unwrap_or_else(|| argument.static_type().unwrap().clone());
    call(name, field, vec![argument])
}
#[test]
fn native_unary_sign_literals_and_columns_keep_kinds_shape_flags_and_promotion() {
    let mut values = native_operator_value_cases();
    values.extend([
        (Datum::Int(i64::MIN), ty()),
        (Datum::Int(i64::MAX), ty()),
        (Datum::UInt(1u64 << 63), ty().with_unsigned(true)),
        (Datum::UInt((1u64 << 63) + 1), ty().with_unsigned(true)),
        (Datum::UInt(0), ty().with_unsigned(true)),
        (Datum::Real(-0.0), real_ty()),
        (
            Datum::Decimal(Decimal::from_literal("-1.2300").with_declared_shape(60, 4)),
            FieldType::new(FieldTypeCode::NewDecimal)
                .with_flen(60)
                .with_decimal(4),
        ),
        (
            Datum::Decimal(Decimal::from_literal("-0.0000").with_declared_shape(60, 4)),
            FieldType::new(FieldTypeCode::NewDecimal)
                .with_flen(60)
                .with_decimal(4),
        ),
    ]);
    for (value, field) in &values {
        for name in ["unaryplus", "unaryminus"] {
            let sign = unary_sign_call(name, literal(value.clone(), field.clone()));
            let root = call("cast_char", string_ty(), vec![sign]);
            for selection in [vec![], vec![0], vec![2, 0, 2], vec![2; 2050]] {
                compare_typed_profile(root.clone(), &[], &vec![vec![]; 3], &selection);
            }
        }
    }
    for (value, field) in values.into_iter().filter(|(value, _)| {
        matches!(
            value,
            Datum::Int(_)
                | Datum::UInt(_)
                | Datum::Real(_)
                | Datum::Float32(_)
                | Datum::Decimal(_)
                | Datum::String(_)
                | Datum::Duration(_)
                | Datum::Time(_)
                | Datum::Json(_)
        )
    }) {
        let rows = vec![vec![value], vec![Datum::Null]];
        for name in ["unaryplus", "unaryminus"] {
            let sign = unary_sign_call(name, typed_column(0, &field));
            let root = call("cast_char", string_ty(), vec![sign]);
            for selection in [
                vec![],
                vec![0],
                vec![1, 0, 1],
                (0..2050).map(|row| row % 2).collect(),
            ] {
                compare_typed_profile(
                    root.clone(),
                    std::slice::from_ref(&field),
                    &rows,
                    &selection,
                );
            }
        }
    }
}

#[test]
fn native_unary_sign_parents_keep_operand_phases_and_folded_constant_facts() {
    let schema = vec![ty(); 3];
    let rows = vec![
        vec![Datum::Int(1), Datum::Int(2), Datum::Int(i64::MIN)],
        vec![Datum::Int(i64::MIN), Datum::Int(1), Datum::Int(1)],
        vec![Datum::Null, Datum::Int(i64::MIN), Datum::Int(1)],
        vec![Datum::Int(2), Datum::Int(3), Datum::Int(4)],
    ];
    let mut roots = Vec::new();
    for name in ["unaryplus", "unaryminus"] {
        let sign = || unary_sign_call(name, abs(column(0)));
        roots.extend([
            sign(),
            plus(sign(), abs(column(2))),
            abs(sign()),
            call("if", ty(), vec![sign(), abs(column(2)), int(0)]),
            call("xor", ty(), vec![sign(), abs(column(1))]),
            call(
                "bitxor",
                ty().with_unsigned(true),
                vec![sign(), abs(column(1))],
            ),
            unary_sign_call(name, call("unaryminus", ty(), vec![column(0)])),
            unary_sign_call(name, call("if", ty(), vec![int(1), int(i64::MIN), int(0)])),
            unary_sign_call(name, call("coalesce", ty(), vec![null(), int(i64::MIN)])),
        ]);
    }
    for selection in [
        vec![],
        vec![3],
        vec![0, 1],
        vec![1, 0],
        vec![2, 3, 2],
        vec![3; 2050],
        (0..2050)
            .map(|row| if row == 2049 { 1 } else { 0 })
            .collect(),
    ] {
        for root in &roots {
            compare_arithmetic_profile(root.clone(), &schema, &rows, &selection);
        }
    }
}

#[test]
fn native_unary_sign_parent_keeps_original_native_qualification() {
    for field in [
        ty(),
        ty().with_unsigned(true),
        real_ty(),
        FieldType::new(FieldTypeCode::NewDecimal)
            .with_flen(65)
            .with_decimal(6),
        string_ty(),
    ] {
        for name in ["unaryplus", "unaryminus"] {
            let sign = unary_sign_call(name, typed_column(0, &field));
            let root = call("cast_char", string_ty(), vec![sign]);
            // Unified admission must not grow the original native signature
            // set or introduce a vectorization getter on this new producer.
            assert!(
                crate::scalar_function::static_batch_target(&root).is_none(),
                "{name} changed CHAR qualification for {field:?}"
            );
        }
    }
}

#[test]
fn native_unary_sign_arguments_honor_enum_set_integer_view() {
    use tidb_datatype::MysqlSet;
    let field =
        FieldType::new(FieldTypeCode::Set).with_elems((0..64).map(|index| format!("v{index}")));
    let integer_view = field
        .clone()
        .with_added_flags(FieldTypeFlags::ENUM_SET_AS_INT);
    let value = Datum::Set(MysqlSet::new("v0,v53", (1u64 << 53) | 1), Collation::Binary);
    let literal = || self::literal(value.clone(), field.clone());
    let roots = [
        unary_sign_call(
            "unaryminus",
            self::literal(value.clone(), integer_view.clone()),
        ),
        unary_sign_call(
            "unaryminus",
            call(
                "if",
                integer_view.clone(),
                vec![int(1), literal(), literal()],
            ),
        ),
        unary_sign_call(
            "unaryminus",
            call("coalesce", integer_view.clone(), vec![null(), literal()]),
        ),
        unary_sign_call("unaryminus", typed_column(0, &integer_view)),
    ];
    for root in roots {
        compare_arithmetic_profile(
            root,
            std::slice::from_ref(&integer_view),
            &[vec![value.clone()]],
            &[0],
        );
    }
}

fn quotient_call(name: &str, left: Expression, right: Expression) -> Expression {
    let field = crate::builtin_arithmetic::infer_arithmetic_type(name, &left, &right)
        .unwrap_or_else(|| {
            // Go's builder rejects quotient vector arguments. Keep an explicit
            // generic declaration here to exercise that already-declared callable's
            // runtime error path as well, rather than dropping the carrier case.
            assert!(
                left.static_type()
                    .is_some_and(|field| field.eval_type().is_vector_kind())
                    || right
                        .static_type()
                        .is_some_and(|field| field.eval_type().is_vector_kind())
            );
            crate::builtin_arithmetic::new_return_field_type(if name == "intdiv" {
                NativeEvalType::Int
            } else {
                NativeEvalType::Real
            })
        });
    call(name, field, vec![left, right])
}
fn native_quotient_abs_operand_profile(name: &str) {
    let root = quotient_call(name, abs(column(0)), int(2));
    let schema = vec![ty()];
    let rows = vec![vec![Datum::Int(7)], vec![Datum::Int(i64::MIN)]];
    compare_typed_profile(root, &schema, &rows, &[0, 1]);
}
#[test]
fn native_modulo_abs_requires_canonical_driver() {
    native_quotient_abs_operand_profile("mod");
}
#[test]
fn native_division_abs_requires_canonical_driver() {
    native_quotient_abs_operand_profile("div");
}
#[test]
fn native_integer_division_abs_requires_canonical_driver() {
    native_quotient_abs_operand_profile("intdiv");
}

#[test]
fn native_quotient_literals_and_columns_keep_original_domains_and_warnings() {
    let mut values = native_operator_value_cases();
    values.extend([
        (Datum::Int(i64::MIN), ty()),
        (Datum::Int(i64::MAX), ty()),
        (Datum::UInt(0), ty().with_unsigned(true)),
        (Datum::UInt((1u64 << 53) + 1), ty().with_unsigned(true)),
        (Datum::Real(-0.0), real_ty()),
        (
            Datum::Decimal(Decimal::from_literal("-7.0000")),
            FieldType::new(FieldTypeCode::NewDecimal)
                .with_flen(65)
                .with_decimal(4),
        ),
    ]);
    let decimal = FieldType::new(FieldTypeCode::NewDecimal)
        .with_flen(65)
        .with_decimal(4);
    for (value, field) in &values {
        for name in ["mod", "div", "intdiv"] {
            for partner in [
                int(2),
                int(0),
                null(),
                literal(Datum::Real(0.5), real_ty()),
                literal(
                    Datum::Decimal(Decimal::from_literal("3.0000")),
                    decimal.clone(),
                ),
            ] {
                let argument = literal(value.clone(), field.clone());
                for root in [
                    quotient_call(name, argument.clone(), partner.clone()),
                    quotient_call(name, partner, argument),
                ] {
                    for selection in [vec![], vec![0], vec![2, 0, 2]] {
                        compare_arithmetic_profile(root.clone(), &[], &vec![vec![]; 3], &selection);
                    }
                }
            }
        }
    }
    for (value, field) in values.into_iter().filter(|(value, _)| {
        matches!(
            value,
            Datum::Int(_)
                | Datum::UInt(_)
                | Datum::Real(_)
                | Datum::Float32(_)
                | Datum::Decimal(_)
                | Datum::String(_)
                | Datum::Time(_)
                | Datum::Duration(_)
                | Datum::Json(_)
        )
    }) {
        let schema = vec![field.clone()];
        let rows = vec![vec![value], vec![Datum::Null]];
        for name in ["mod", "div", "intdiv"] {
            for root in [
                quotient_call(name, typed_column(0, &field), int(2)),
                quotient_call(name, int(2), typed_column(0, &field)),
            ] {
                for selection in [vec![], vec![0], vec![1, 0, 1]] {
                    compare_arithmetic_profile(root.clone(), &schema, &rows, &selection);
                }
            }
        }
    }
}

#[test]
fn native_quotient_column_pairs_keep_whole_sources_and_null_modes() {
    let values: Vec<_> = native_operator_value_cases()
        .into_iter()
        .filter(|(value, _)| {
            matches!(
                value,
                Datum::Int(_)
                    | Datum::UInt(_)
                    | Datum::Real(_)
                    | Datum::Float32(_)
                    | Datum::Decimal(_)
                    | Datum::String(_)
                    | Datum::Time(_)
                    | Datum::Duration(_)
                    | Datum::Json(_)
            )
        })
        .collect();
    for (left, lf) in &values {
        for (right, rf) in &values {
            let schema = vec![lf.clone(), rf.clone()];
            let rows = vec![
                vec![left.clone(), right.clone()],
                vec![Datum::Null, right.clone()],
                vec![left.clone(), Datum::Null],
            ];
            for name in ["mod", "div", "intdiv"] {
                let root = quotient_call(name, typed_column(0, lf), typed_column(1, rf));
                for selection in [vec![], vec![0], vec![1, 2, 0, 1]] {
                    compare_arithmetic_profile(root.clone(), &schema, &rows, &selection);
                }
            }
        }
    }
}

#[test]
fn native_quotient_parents_keep_row_control_and_whole_arithmetic_phases() {
    let schema = vec![ty(); 3];
    let rows = vec![
        vec![Datum::Null, Datum::Int(i64::MIN), Datum::Int(-1)],
        vec![Datum::Int(1), Datum::Int(i64::MAX), Datum::Int(0)],
        vec![Datum::Int(i64::MIN), Datum::Int(1), Datum::Int(-1)],
        vec![Datum::Int(7), Datum::Int(2), Datum::Int(3)],
    ];
    for name in ["mod", "div", "intdiv"] {
        let value = || quotient_call(name, column(0), column(1));
        let field = value().static_type().unwrap().clone();
        let roots = vec![
            quotient_call(name, column(0), abs(column(1))),
            quotient_call(name, abs(column(0)), abs(column(1))),
            quotient_call(name, column(0), plus(column(1), int(1))),
            quotient_call("plus", value(), column(2)),
            quotient_call(
                name,
                quotient_call("mul", column(0), int(2)),
                plus(column(1), int(1)),
            ),
            call("abs", field.clone(), vec![value()]),
            call(
                "cast_decimal",
                FieldType::new(FieldTypeCode::NewDecimal)
                    .with_flen(65)
                    .with_decimal(4),
                vec![value()],
            ),
            call(
                "cast_json",
                FieldType::new(FieldTypeCode::Json),
                vec![value()],
            ),
            call("if", field.clone(), vec![column(2), value(), null()]),
            call("coalesce", field.clone(), vec![column(0), value()]),
            call("and", ty(), vec![column(2), value()]),
            call("isnull", ty(), vec![value()]),
        ];
        let char_root = call("cast_char", string_ty(), vec![value()]);
        for selection in [
            vec![],
            vec![3],
            vec![0, 1],
            vec![2, 0],
            vec![3, 0, 3],
            vec![3; 2050],
            (0..2050)
                .map(|position| if position == 2049 { 1 } else { 3 })
                .collect(),
        ] {
            for root in &roots {
                compare_arithmetic_profile(root.clone(), &schema, &rows, &selection);
            }
            compare_typed_profile(char_root.clone(), &schema, &rows, &selection);
        }
    }
}

struct QuotientPrecisionContext {
    enabled: bool,
    strict: bool,
    increments: [u32; 2],
    reads: std::cell::Cell<usize>,
    events: std::cell::RefCell<Vec<String>>,
}
impl Columns for QuotientPrecisionContext {
    fn get(&self, _: &[String]) -> Option<Datum> {
        None
    }
    fn enable_vectorized_expression(&self) -> bool {
        self.events.borrow_mut().push("enabled".into());
        self.enabled
    }
    fn div_precision_increment(&self) -> u32 {
        let read = self.reads.get();
        self.reads.set(read + 1);
        let value = self.increments[read % 2];
        self.events
            .borrow_mut()
            .push(format!("precision:{read}:{value}"));
        value
    }
    fn truncate_level(&self) -> crate::ErrorLevel {
        self.events.borrow_mut().push("truncate level".into());
        if self.strict {
            crate::ErrorLevel::Error
        } else {
            crate::ErrorLevel::Warn
        }
    }
    fn handle_truncate(&self, message: &str) -> std::result::Result<(), EvalError> {
        self.events.borrow_mut().push(format!("truncate:{message}"));
        if self.strict {
            Err(EvalError::Unsupported("quotient strict truncation"))
        } else {
            Ok(())
        }
    }
    fn handle_division_by_zero(&self) -> std::result::Result<(), EvalError> {
        self.events.borrow_mut().push("division by zero".into());
        if self.strict {
            Err(EvalError::Unsupported("quotient strict zero divisor"))
        } else {
            Ok(())
        }
    }
    fn append_warning(&self, code: u16, message: &str) {
        self.events
            .borrow_mut()
            .push(format!("warn:{code}:{message}"));
    }
}
fn compare_quotient_precision_profile(
    root: Expression,
    schema: &[FieldType],
    rows: &[Vec<Datum>],
    selection: &[usize],
) {
    let field = root.static_type().unwrap().clone();
    let actual = suite(root.clone());
    let expected = suite(root);
    for strict in [false, true] {
        for increments in [[0, 4], [31, 0], [4, 100]] {
            for enabled in [true, false, true, false] {
                let ctx = || QuotientPrecisionContext {
                    enabled,
                    strict,
                    increments,
                    reads: std::cell::Cell::new(0),
                    events: std::cell::RefCell::default(),
                };
                let ac = ctx();
                let ec = ctx();
                let mut input = chunk(schema, rows);
                let mut reference = chunk(schema, rows);
                input.set_sel(Some(selection.to_vec()));
                reference.set_sel(Some(selection.to_vec()));
                let mut output = output_for(&field);
                let mut result = output_for(&field);
                let a = actual.run_with_tikv_batch(&ac, &mut input, &mut output, schema);
                let e = expected.run(&ec, &mut reference, &mut result);
                assert_eq!(
                    a, e,
                    "enabled={enabled} strict={strict} {increments:?} selection={selection:?}"
                );
                assert_eq!(
                    ac.events.take(),
                    ec.events.take(),
                    "enabled={enabled} strict={strict} {increments:?}"
                );
                assert_eq!(values_for(&output, &field), values_for(&result, &field));
                let route = actual.batch_route_snapshot();
                assert_eq!((route.refusals, route.fallback), (0, 0));
            }
        }
    }
}
#[test]
fn native_integer_division_keeps_conditional_precision_and_row_window_diagnostic_values() {
    let decimal = FieldType::new(FieldTypeCode::NewDecimal)
        .with_flen(65)
        .with_decimal(4);
    let real = real_ty();
    for unsigned in [false, true] {
        let schema = vec![decimal.clone().with_unsigned(unsigned), decimal.clone()];
        let rows = vec![
            vec![
                Datum::Decimal(Decimal::from_literal("1.0000")),
                Datum::Decimal(Decimal::from_literal("3.0000")),
            ],
            vec![
                Datum::Decimal(Decimal::from_literal("9223372036854775808")),
                Datum::Decimal(Decimal::from_int(1)),
            ],
            vec![
                Datum::Decimal(Decimal::from_literal("-0.5000")),
                Datum::Decimal(Decimal::from_int(1)),
            ],
            vec![Datum::Null, Datum::Decimal(Decimal::from_int(0))],
        ];
        let root = quotient_call(
            "intdiv",
            typed_column(0, &schema[0]),
            typed_column(1, &schema[1]),
        );
        for selection in [
            vec![],
            vec![0],
            vec![1],
            vec![2, 0, 2],
            vec![3, 0, 1],
            vec![0; 2050],
        ] {
            compare_quotient_precision_profile(root.clone(), &schema, &rows, &selection);
        }
    }
    let schema = vec![real.clone(), decimal.clone()];
    let rows = vec![
        vec![Datum::Real(1e20), Datum::Decimal(Decimal::from_int(1))],
        vec![Datum::Real(-0.5), Datum::Decimal(Decimal::from_int(1))],
        vec![Datum::Null, Datum::Decimal(Decimal::from_int(0))],
    ];
    let root = quotient_call("intdiv", typed_column(0, &real), typed_column(1, &decimal));
    for selection in [vec![], vec![0], vec![1, 2, 0]] {
        compare_quotient_precision_profile(root.clone(), &schema, &rows, &selection);
    }
    let computed = quotient_call(
        "plus",
        literal(Datum::Real(1e20), real.clone()),
        literal(Datum::Real(0.0), real),
    );
    let json = call(
        "cast_json",
        FieldType::new(FieldTypeCode::Json),
        vec![literal(Datum::UInt(u64::MAX), ty().with_unsigned(true))],
    );
    for operand in [computed, json] {
        let root = quotient_call("intdiv", operand, int(1));
        for selection in [vec![], vec![0], vec![2, 0, 2]] {
            compare_quotient_precision_profile(root.clone(), &[], &vec![vec![]; 3], &selection);
        }
    }
}

#[test]
fn native_in_generic_abs_operands_require_canonical_driver() {
    let root = call("in", ty(), vec![abs(column(0)), int(1), int(2)]);
    compare_typed_profile(
        root,
        &[ty()],
        &[vec![Datum::Int(7)], vec![Datum::Int(i64::MIN)]],
        &[0, 1],
    );
}
#[test]
fn native_in_duration_columns_require_canonical_driver() {
    let field = FieldType::new(FieldTypeCode::Duration).with_decimal(6);
    let duration = Datum::Duration(
        tidb_datatype::MySqlDuration::from_nanoseconds(3_600_000_000_000, 6).unwrap(),
    );
    let root = call(
        "in",
        ty(),
        vec![
            typed_column(0, &field),
            literal(duration.clone(), field.clone()),
            null(),
        ],
    );
    compare_typed_profile(
        root,
        &[field],
        &[vec![duration], vec![Datum::Null]],
        &[0, 1, 0],
    );
}
#[test]
fn native_in_computed_json_requires_canonical_driver() {
    let json = FieldType::new(FieldTypeCode::Json);
    let root = call(
        "in",
        ty(),
        vec![
            call("cast_json", json.clone(), vec![column(0)]),
            literal(
                Datum::Json(tidb_datatype::BinaryJSON::parse("1").unwrap()),
                json,
            ),
            literal(Datum::new_string("2"), string_ty()),
        ],
    );
    compare_typed_profile(
        root,
        &[ty()],
        &[vec![Datum::Int(1)], vec![Datum::Int(2)], vec![Datum::Null]],
        &[0, 1, 2, 0],
    );
}

#[test]
fn native_in_exhaustive_sources_keep_match_null_error_and_reuse_order() {
    let schema = [ty(), ty(), ty()];
    let rows = vec![
        vec![Datum::Int(1), Datum::Int(1), Datum::Int(i64::MIN)],
        vec![Datum::Null, Datum::Int(1), Datum::Int(i64::MIN)],
        vec![Datum::Int(7), Datum::Null, Datum::Int(2)],
        vec![Datum::Int(2), Datum::Int(2), Datum::Int(3)],
    ];
    let root = call("in", ty(), vec![column(0), column(1), abs(column(2))]);
    for selection in [
        vec![],
        vec![0],
        vec![1],
        vec![2, 3, 2],
        vec![3; 2050],
        {
            let mut selection = vec![3; 2050];
            selection[2049] = 0;
            selection
        },
        vec![3, 0, 3],
        vec![3, 1, 3],
    ] {
        compare_arithmetic_profile(root.clone(), &schema, &rows, &selection);
    }
}

#[test]
fn native_in_prepared_strings_keep_dynamic_errors_after_cache_hits() {
    let mut root = call(
        "in",
        ty(),
        vec![
            typed_column(0, &string_ty()),
            text(Some(b"a")),
            null(),
            call("cast_char", string_ty(), vec![abs(column(1))]),
        ],
    );
    let Expression::ScalarFunction(function) = &mut root else {
        unreachable!()
    };
    function.prepare_in_string_hash_set();
    let (keys, dynamic, has_null) = function.native_in_cache();
    assert!(keys.is_some());
    assert!(has_null);
    assert_eq!(dynamic, &[3]);
    let schema = [string_ty(), ty()];
    let rows = vec![
        vec![Datum::new_string("a"), Datum::Int(i64::MIN)],
        vec![Datum::Null, Datum::Int(i64::MIN)],
        vec![Datum::new_string("7"), Datum::Int(7)],
        vec![Datum::new_string("z"), Datum::Int(7)],
    ];
    for selection in [
        vec![],
        vec![0],
        vec![1],
        vec![2, 3, 2],
        vec![2; 2050],
        vec![2, 0],
    ] {
        compare_arithmetic_profile(root.clone(), &schema, &rows, &selection);
    }
}

#[test]
fn native_in_typed_all_sources_then_all_casts_keep_json_document_member_modes() {
    let json = FieldType::new(FieldTypeCode::Json);
    let duration = FieldType::new(FieldTypeCode::Duration).with_decimal(6);
    let datetime = FieldType::new(FieldTypeCode::Datetime).with_decimal(6);
    let timestamp = FieldType::new(FieldTypeCode::Timestamp).with_decimal(6);
    for domain in [json, duration, datetime, timestamp] {
        let schema = [domain.clone(), string_ty(), ty()];
        let base = match domain.eval_type() {
            NativeEvalType::Json => Datum::Json(tidb_datatype::BinaryJSON::parse("1").unwrap()),
            NativeEvalType::Duration => Datum::Duration(
                tidb_datatype::MySqlDuration::from_nanoseconds(3_600_000_000_000, 6).unwrap(),
            ),
            _ => Datum::Time(
                tidb_datatype::parse_datetime("2026-10-17 12:34:56", &chrono_tz::UTC, true, false)
                    .unwrap()
                    .time,
            ),
        };
        let rows = vec![
            vec![base.clone(), Datum::new_string("1"), Datum::Int(1)],
            vec![
                Datum::Null,
                Datum::new_string("invalid"),
                Datum::Int(i64::MIN),
            ],
            vec![base.clone(), Datum::new_string("invalid"), Datum::Int(1)],
            vec![
                base.clone(),
                Datum::new_string("2026-10-17 12:34:56"),
                Datum::Int(1),
            ],
            vec![base, Datum::new_string("01:00:00"), Datum::Int(1)],
        ];
        let root = call(
            "in",
            ty(),
            vec![
                typed_column(0, &domain),
                typed_column(1, &string_ty()),
                abs(column(2)),
            ],
        );
        for selection in [
            vec![],
            vec![0],
            vec![1],
            vec![2],
            vec![3, 4, 0, 3],
            vec![4; 2050],
            vec![0, 1],
        ] {
            compare_arithmetic_profile(root.clone(), &schema, &rows, &selection);
        }
    }
}

#[test]
fn native_in_handles_resource_failure_cancellation_and_reuse_are_terminal() {
    use tidb_query_expr::local::{
        HostArgReply, HostInvocation, HostStart, HostStep, LocalHostServices,
    };
    for domain in [
        ty(),
        FieldType::new(FieldTypeCode::Duration).with_decimal(6),
        FieldType::new(FieldTypeCode::Json),
    ] {
        let field = projected(&native_bytes_type(), true).unwrap();
        let args = vec![field.clone(); 3];
        let catalog = HostCatalog::for_sql_batch(vec![(
            SqlBatchHostOperation::InNative,
            HostSignature {
                arg_types: args.clone().into_boxed_slice(),
                return_type: field.clone(),
            },
        )])
        .unwrap();
        let slot = catalog.slot(0).unwrap();
        let metadata = [NativeHostMetadata::In(hosts::NativeInMetadata {
            arguments: [3, 8, 9]
                .into_iter()
                .map(|node| hosts::NativeInArgument {
                    node,
                    field: domain.clone(),
                    facts: crate::ops::OperandFacts {
                        eval_type: Some(domain.eval_type()),
                        unsigned: false,
                        constant: false,
                        duration_column: domain.code() == FieldTypeCode::Duration,
                    },
                })
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            prepared: None,
            dynamic: Box::new([]),
            has_null: false,
            collation: Collation::Binary,
            result_type: ty(),
            output_node: 2,
            output_role: NativeValueRole::Output,
        })];
        let store = RefCell::new(NativeValueStore::new(1 << 20).unwrap());
        store.borrow_mut().begin_window().unwrap();
        let row = InputRow {
            occurrence: 0,
            input_row: 0,
        };
        let origin = NativeValueOrigin { node: 9, row };
        let stale = store
            .borrow_mut()
            .insert_clone(&Datum::Int(1), NativeValueRole::CharInput, origin)
            .unwrap();
        store.borrow_mut().begin_window().unwrap();
        let value = match domain.eval_type() {
            NativeEvalType::Duration => {
                Datum::Duration(tidb_datatype::MySqlDuration::from_nanoseconds(1, 6).unwrap())
            }
            NativeEvalType::Json => Datum::Json(tidb_datatype::BinaryJSON::parse("1").unwrap()),
            _ => Datum::Int(1),
        };
        let tokens = [3, 8, 9].map(|node| {
            store
                .borrow_mut()
                .insert_clone(
                    &value,
                    NativeValueRole::CharInput,
                    NativeValueOrigin { node, row },
                )
                .unwrap()
        });
        let wrong_role = store
            .borrow_mut()
            .insert_clone(&value, NativeValueRole::Output, origin)
            .unwrap();
        let wrong_node = store
            .borrow_mut()
            .insert_clone(
                &value,
                NativeValueRole::CharInput,
                NativeValueOrigin { node: 8, row },
            )
            .unwrap();
        let wrong_row = store
            .borrow_mut()
            .insert_clone(
                &value,
                NativeValueRole::CharInput,
                NativeValueOrigin {
                    node: 9,
                    row: InputRow {
                        occurrence: 1,
                        input_row: 0,
                    },
                },
            )
            .unwrap();
        let large = store
            .borrow_mut()
            .insert_clone(
                &Datum::Bytes(vec![b'X'; 16384]),
                NativeValueRole::CharInput,
                origin,
            )
            .unwrap();
        let context = ArithmeticProfilesContext::default();
        let mut provider = NativeBatchHosts::new(catalog, &metadata, &context, 8192)
            .with_native_values(Some(&store));
        let mut ctx = EvalContext::default();
        for (bad, resource) in [
            (stale, false),
            (wrong_role, false),
            (wrong_node, false),
            (wrong_row, false),
            (large, true),
        ] {
            let HostStart::Pending { task, .. } = provider
                .start(
                    &mut ctx,
                    HostInvocation {
                        slot,
                        row,
                        arg_types: &args,
                        return_type: &field,
                    },
                )
                .unwrap()
            else {
                panic!("IN pending")
            };
            for (index, token) in tokens.iter().take(2).enumerate() {
                let values = VectorValue::from_scalar(&ScalarValue::Bytes(Some(token.clone())), 1);
                assert!(matches!(
                    provider
                        .resume(
                            &mut ctx,
                            &task,
                            HostArgReply {
                                index,
                                field_type: &field,
                                values: &values
                            }
                        )
                        .unwrap(),
                    HostStep::NeedArg(_)
                ));
            }
            let before = context.trace.borrow().clone();
            let values = VectorValue::from_scalar(&ScalarValue::Bytes(Some(bad)), 1);
            let error = provider
                .resume(
                    &mut ctx,
                    &task,
                    HostArgReply {
                        index: 2,
                        field_type: &field,
                        values: &values,
                    },
                )
                .unwrap_err();
            assert!(
                if resource {
                    matches!(error, LocalError::ResourceLimit(_))
                } else {
                    matches!(error, LocalError::BindingContract(_))
                },
                "{error:?}"
            );
            assert_eq!(
                *context.trace.borrow(),
                before,
                "invalid capability/resource refusal must not run later comparison/casts"
            );
            assert!(provider.take_native_error().is_none());
            provider.cancel(&task);
            provider.cancel(&task);
        }
        // Reuse the exact provider after every cancellation/resource failure.
        let HostStart::Pending { task, .. } = provider
            .start(
                &mut ctx,
                HostInvocation {
                    slot,
                    row,
                    arg_types: &args,
                    return_type: &field,
                },
            )
            .unwrap()
        else {
            panic!("IN pending")
        };
        for (index, token) in tokens.iter().enumerate() {
            let values = VectorValue::from_scalar(&ScalarValue::Bytes(Some(token.clone())), 1);
            let step = provider
                .resume(
                    &mut ctx,
                    &task,
                    HostArgReply {
                        index,
                        field_type: &field,
                        values: &values,
                    },
                )
                .unwrap();
            if index < 2 {
                assert!(matches!(step, HostStep::NeedArg(_)));
            } else {
                let HostStep::Ready(output) = step else {
                    panic!("IN did not finish")
                };
                use tidb_query_datatype::codec::data_type::ScalarValueRef;
                let ScalarValueRef::Bytes(Some(token)) = output.get_scalar_ref(0) else {
                    panic!("output capability")
                };
                assert_eq!(
                    *store
                        .borrow()
                        .get(
                            token,
                            NativeValueRole::Output,
                            NativeValueOrigin { node: 2, row }
                        )
                        .unwrap(),
                    Datum::Int(1)
                );
            }
        }
        provider.cancel(&task);
    }
}

#[test]
fn native_in_composes_with_whole_arithmetic_and_row_controls_without_replay() {
    let inside = || call("in", ty(), vec![column(0), column(1), abs(column(2))]);
    let roots = [
        plus(inside(), abs(column(2))),
        plus(abs(column(2)), inside()),
        call("if", ty(), vec![inside(), abs(column(2)), int(0)]),
        call("in", ty(), vec![inside(), int(1), abs(column(2))]),
    ];
    let schema = [ty(), ty(), ty()];
    let rows = vec![
        vec![Datum::Int(1), Datum::Int(1), Datum::Int(i64::MIN)],
        vec![Datum::Null, Datum::Int(1), Datum::Int(1)],
        vec![Datum::Int(7), Datum::Null, Datum::Int(2)],
        vec![Datum::Int(2), Datum::Int(2), Datum::Int(3)],
    ];
    for root in roots {
        for selection in [
            vec![],
            vec![0],
            vec![1, 2, 3],
            vec![3; 2050],
            {
                let mut selection = vec![3; 2050];
                selection[2049] = 0;
                selection
            },
            vec![3, 0, 3],
        ] {
            compare_arithmetic_profile(root.clone(), &schema, &rows, &selection);
        }
    }
}

#[test]
fn native_in_generic_raw_literal_sources_match_original_generic_dispatch_facts() {
    // Int declarations deliberately choose the original generic-IN branch;
    // raw constant representations still reach it unchanged. This exercises
    // the private owner's accepted kinds, not typed integer comparison probes.
    let values = crate::ops::arithmetic::tests::fixtures();
    let mut cases = 0;
    for unsigned in [[false, false], [false, true], [true, false], [true, true]] {
        for left in &values {
            for right in &values {
                for null_first in [false, true] {
                    let mut args = vec![literal(left.clone(), ty().with_unsigned(unsigned[0]))];
                    if null_first {
                        args.push(null());
                    }
                    args.push(literal(right.clone(), ty().with_unsigned(unsigned[1])));
                    if !null_first {
                        args.push(null());
                    }
                    compare_arithmetic_profile(call("in", ty(), args), &[], &[vec![]], &[0]);
                    cases += 1;
                }
            }
        }
    }
    assert_eq!(cases, 8192);
}

#[test]
fn native_in_generic_duration_column_and_constant_text_keep_comparison_facts() {
    let duration = FieldType::new(FieldTypeCode::Duration).with_decimal(6);
    let schema = [string_ty(), duration.clone()];
    let rows = vec![
        vec![
            Datum::new_string("01:00:00"),
            Datum::Duration(
                tidb_datatype::MySqlDuration::from_nanoseconds(3_600_000_000_000, 6).unwrap(),
            ),
        ],
        vec![
            Datum::new_string("invalid"),
            Datum::Duration(tidb_datatype::MySqlDuration::from_nanoseconds(1, 6).unwrap()),
        ],
        vec![Datum::Null, Datum::Null],
    ];
    for first in [typed_column(0, &string_ty()), text(Some(b"01:00:00"))] {
        let root = call("in", ty(), vec![first, typed_column(1, &duration), null()]);
        for selection in [vec![], vec![0, 1, 2], vec![0; 2050], vec![1, 0, 2, 1]] {
            compare_arithmetic_profile(root.clone(), &schema, &rows, &selection);
        }
    }
}

#[test]
fn native_interval_integer_abs_sources_require_canonical_driver() {
    let root = call(
        "interval",
        ty(),
        vec![abs(column(0)), int(1), int(3), abs(column(1))],
    );
    compare_typed_profile(
        root,
        &[ty(), ty()],
        &[
            vec![Datum::Int(2), Datum::Int(5)],
            vec![Datum::Int(7), Datum::Int(i64::MIN)],
            vec![Datum::Null, Datum::Int(i64::MIN)],
        ],
        &[0, 1, 2],
    );
}
#[test]
fn native_interval_not_null_real_binary_sources_require_canonical_driver() {
    let real = real_ty().with_flags(FieldTypeFlags::NOT_NULL);
    let integer = ty().with_flags(FieldTypeFlags::NOT_NULL);
    let root = call(
        "interval",
        ty(),
        vec![
            typed_column(0, &real),
            call("abs", integer.clone(), vec![typed_column(1, &integer)]),
            literal(Datum::Real(1.0), real.clone()),
            literal(Datum::Real(2.0), real.clone()),
            literal(Datum::Real(3.0), real.clone()),
        ],
    );
    compare_typed_profile(
        root,
        &[real, integer],
        &[
            vec![Datum::Real(100.0), Datum::Int(i64::MIN)],
            vec![Datum::Real(0.0), Datum::Int(i64::MIN)],
            vec![Datum::Real(f64::NAN), Datum::Int(i64::MIN)],
        ],
        &[0, 1, 2],
    );
}
#[test]
fn native_interval_computed_json_sources_require_canonical_driver() {
    let root = call(
        "interval",
        ty(),
        vec![
            call(
                "cast_json",
                FieldType::new(FieldTypeCode::Json),
                vec![column(0)],
            ),
            int(1),
            int(2),
        ],
    );
    compare_typed_profile(
        root,
        &[ty()],
        &[vec![Datum::Int(1)], vec![Datum::Int(7)], vec![Datum::Null]],
        &[0, 1, 2, 0],
    );
}

#[test]
fn native_interval_raw_constant_sources_match_original_metadata_selected_probes() {
    let values = crate::ops::arithmetic::tests::fixtures();
    let mut cases = 0;
    for field in [
        ty(),
        ty().with_flags(1),
        ty().with_flags(1).with_unsigned(true),
        real_ty(),
        real_ty().with_flags(1),
        string_ty().with_flags(1),
    ] {
        for first in &values {
            for other in &values {
                for input in [
                    vec![first.clone(), other.clone()],
                    vec![
                        first.clone(),
                        Datum::Int(0),
                        Datum::Null,
                        other.clone(),
                        Datum::UInt(u64::MAX),
                    ],
                ] {
                    let root = call(
                        "interval",
                        ty(),
                        input
                            .into_iter()
                            .map(|value| literal(value, field.clone()))
                            .collect(),
                    );
                    compare_arithmetic_profile(root, &[], &[vec![]], &[0]);
                    cases += 1;
                }
            }
        }
    }
    assert_eq!(cases, 12288);
}
#[test]
fn native_interval_parent_phases_preserve_probe_skips_errors_duplicates_and_reuse() {
    let integer = ty().with_flags(1);
    let interval = || {
        call(
            "interval",
            ty(),
            vec![
                typed_column(0, &integer),
                call("abs", integer.clone(), vec![typed_column(1, &integer)]),
                literal(Datum::Int(10), integer.clone()),
                literal(Datum::Int(20), integer.clone()),
            ],
        )
    };
    let roots = [
        interval(),
        plus(interval(), abs(typed_column(1, &integer))),
        plus(abs(typed_column(1, &integer)), interval()),
        call(
            "if",
            ty(),
            vec![interval(), int(1), abs(typed_column(1, &integer))],
        ),
        call(
            "interval",
            ty(),
            vec![interval(), int(1), abs(typed_column(1, &integer))],
        ),
    ];
    let schema = [integer.clone(), integer];
    let rows = vec![
        vec![Datum::Int(100), Datum::Int(i64::MIN)],
        vec![Datum::Int(0), Datum::Int(i64::MIN)],
        vec![Datum::Null, Datum::Int(i64::MIN)],
        vec![Datum::Int(12), Datum::Int(2)],
    ];
    for root in roots {
        for selection in [
            vec![],
            vec![0],
            vec![1],
            vec![2],
            vec![3, 0, 2, 3],
            vec![3; 2050],
            {
                let mut selection = vec![3; 2050];
                selection[2049] = 1;
                selection
            },
        ] {
            compare_arithmetic_profile(root.clone(), &schema, &rows, &selection);
        }
    }
}

#[test]
fn native_interval_probe_capabilities_resources_cancel_and_reuse_are_terminal() {
    use tidb_query_expr::local::{
        HostArgReply, HostInvocation, HostStart, HostStep, LocalHostServices,
    };
    let field = projected(&native_bytes_type(), true).unwrap();
    let args = vec![field.clone(); 3];
    let catalog = HostCatalog::for_sql_batch(vec![(
        SqlBatchHostOperation::IntervalNative,
        HostSignature {
            arg_types: args.clone().into_boxed_slice(),
            return_type: field.clone(),
        },
    )])
    .unwrap();
    let slot = catalog.slot(0).unwrap();
    let domain = real_ty().with_flags(1);
    let metadata = [NativeHostMetadata::Interval(
        hosts::NativeIntervalMetadata {
            arguments: [3, 8, 9]
                .into_iter()
                .map(|node| hosts::NativeIntervalArgument {
                    node,
                    field: domain.clone(),
                })
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            search: tidb_query_expr::NativeIntervalSearch::new(
                [Some(crate::tikv::interval_field_metadata(&domain)); 3].into_iter(),
            )
            .unwrap(),
            result_type: ty(),
            output_node: 2,
            output_role: NativeValueRole::Output,
        },
    )];
    let store = RefCell::new(NativeValueStore::new(1 << 20).unwrap());
    store.borrow_mut().begin_window().unwrap();
    let row = InputRow {
        occurrence: 0,
        input_row: 0,
    };
    let origin = NativeValueOrigin { node: 8, row };
    let stale = store
        .borrow_mut()
        .insert_clone(&Datum::Real(-1.0), NativeValueRole::CharInput, origin)
        .unwrap();
    store.borrow_mut().begin_window().unwrap();
    let tokens = [(3, 0.0), (8, -1.0), (9, 1.0)].map(|(node, value)| {
        store
            .borrow_mut()
            .insert_clone(
                &Datum::Real(value),
                NativeValueRole::CharInput,
                NativeValueOrigin { node, row },
            )
            .unwrap()
    });
    let wrong_role = store
        .borrow_mut()
        .insert_clone(&Datum::Real(-1.0), NativeValueRole::Output, origin)
        .unwrap();
    let wrong_node = store
        .borrow_mut()
        .insert_clone(
            &Datum::Real(-1.0),
            NativeValueRole::CharInput,
            NativeValueOrigin { node: 9, row },
        )
        .unwrap();
    let wrong_row = store
        .borrow_mut()
        .insert_clone(
            &Datum::Real(-1.0),
            NativeValueRole::CharInput,
            NativeValueOrigin {
                node: 8,
                row: InputRow {
                    occurrence: 1,
                    input_row: 0,
                },
            },
        )
        .unwrap();
    let large = store
        .borrow_mut()
        .insert_clone(
            &Datum::Bytes(vec![b'X'; 16384]),
            NativeValueRole::CharInput,
            origin,
        )
        .unwrap();
    let huge = store
        .borrow_mut()
        .insert_clone(
            &Datum::Decimal(tidb_datatype::Decimal::from_raw_parts(
                false,
                vec![b'1'],
                u32::MAX,
                0,
            )),
            NativeValueRole::CharInput,
            origin,
        )
        .unwrap();
    let context = ArithmeticProfilesContext::default();
    let mut provider =
        NativeBatchHosts::new(catalog, &metadata, &context, 8192).with_native_values(Some(&store));
    let mut ctx = EvalContext::default();
    for (bad, kind) in [
        (stale, 0),
        (wrong_role, 0),
        (wrong_node, 0),
        (wrong_row, 0),
        (large, 1),
        (huge, 2),
    ] {
        let HostStart::Pending { task, .. } = provider
            .start(
                &mut ctx,
                HostInvocation {
                    slot,
                    row,
                    arg_types: &args,
                    return_type: &field,
                },
            )
            .unwrap()
        else {
            panic!("INTERVAL pending")
        };
        // NOT NULL real metadata chooses binary probes 0,2,1, rather than a prefix.
        for (index, next) in [(0, 2), (2, 1)] {
            let values =
                VectorValue::from_scalar(&ScalarValue::Bytes(Some(tokens[index].clone())), 1);
            assert!(
                matches!(provider.resume(&mut ctx,&task,HostArgReply {index,field_type:&field,values:&values}).unwrap(),HostStep::NeedArg(request) if request.index == next)
            );
        }
        let before = context.trace.borrow().clone();
        let values = VectorValue::from_scalar(&ScalarValue::Bytes(Some(bad)), 1);
        let error = provider
            .resume(
                &mut ctx,
                &task,
                HostArgReply {
                    index: 1,
                    field_type: &field,
                    values: &values,
                },
            )
            .unwrap_err();
        match kind {
            0 => assert!(matches!(error, LocalError::BindingContract(_)), "{error:?}"),
            1 => assert!(matches!(error, LocalError::ResourceLimit(_)), "{error:?}"),
            _ => {
                let native = provider
                    .take_native_error()
                    .expect("bounded conversion preserves native resource error");
                let EvalError::ExpressionRuntimeFailure(native) = native else {
                    panic!("resource error lost")
                };
                assert_eq!(
                    native.class(),
                    crate::ExpressionRuntimeFailureClass::ResourceLimit
                );
                assert_eq!(native.phase(), None);
            }
        }
        assert_eq!(*context.trace.borrow(), before);
        assert!(provider.take_native_error().is_none());
        provider.cancel(&task);
        provider.cancel(&task);
    }
    let HostStart::Pending { task, .. } = provider
        .start(
            &mut ctx,
            HostInvocation {
                slot,
                row,
                arg_types: &args,
                return_type: &field,
            },
        )
        .unwrap()
    else {
        panic!("INTERVAL pending")
    };
    for index in [0, 2, 1] {
        let values = VectorValue::from_scalar(&ScalarValue::Bytes(Some(tokens[index].clone())), 1);
        let step = provider
            .resume(
                &mut ctx,
                &task,
                HostArgReply {
                    index,
                    field_type: &field,
                    values: &values,
                },
            )
            .unwrap();
        if index != 1 {
            assert!(matches!(step, HostStep::NeedArg(_)));
        } else {
            let HostStep::Ready(output) = step else {
                panic!("INTERVAL did not finish")
            };
            use tidb_query_datatype::codec::data_type::ScalarValueRef;
            let ScalarValueRef::Bytes(Some(token)) = output.get_scalar_ref(0) else {
                panic!("output capability")
            };
            assert_eq!(
                *store
                    .borrow()
                    .get(
                        token,
                        NativeValueRole::Output,
                        NativeValueOrigin { node: 2, row }
                    )
                    .unwrap(),
                Datum::Int(1)
            );
        }
    }
    provider.cancel(&task);
}

#[test]
fn native_extremum_integer_abs_sources_require_canonical_driver() {
    compare_typed_profile(
        call(
            "greatest",
            ty(),
            vec![abs(column(0)), int(1), abs(column(1))],
        ),
        &[ty(), ty()],
        &[
            vec![Datum::Int(2), Datum::Int(5)],
            vec![Datum::Null, Datum::Int(i64::MIN)],
        ],
        &[0, 1],
    );
}
#[test]
fn native_extremum_decimal_cast_sources_require_canonical_driver() {
    let decimal = FieldType::new(FieldTypeCode::NewDecimal)
        .with_flen(40)
        .with_decimal(4);
    compare_typed_profile(
        call(
            "least",
            decimal.clone(),
            vec![
                call("cast_decimal", decimal.clone(), vec![column(0)]),
                literal(
                    Datum::Decimal(tidb_datatype::Decimal::from_literal("2.5000")),
                    decimal,
                ),
            ],
        ),
        &[ty()],
        &[vec![Datum::Int(1)], vec![Datum::Int(7)], vec![Datum::Null]],
        &[0, 1, 2, 0],
    );
}
#[test]
fn native_extremum_real_column_sources_require_canonical_driver() {
    let real = real_ty();
    compare_typed_profile(
        call(
            "greatest",
            real.clone(),
            vec![typed_column(0, &real), typed_column(1, &real)],
        ),
        &[real.clone(), real],
        &[
            vec![Datum::Real(-0.0), Datum::Real(0.0)],
            vec![Datum::Real(f64::NAN), Datum::Real(1.0)],
            vec![Datum::Null, Datum::Real(2.0)],
        ],
        &[0, 1, 2, 0],
    );
}
#[test]
fn native_extremum_string_sources_require_canonical_driver() {
    compare_typed_profile(
        call(
            "greatest",
            string_ty(),
            vec![
                call("cast_char", string_ty(), vec![abs(column(0))]),
                typed_column(1, &string_ty()),
            ],
        ),
        &[ty(), string_ty()],
        &[
            vec![Datum::Int(2), Datum::new_string("a")],
            vec![Datum::Null, Datum::new_string("z")],
        ],
        &[0, 1, 0],
    );
}
#[test]
fn native_extremum_time_sources_require_canonical_driver() {
    let datetime = FieldType::new(FieldTypeCode::Datetime).with_decimal(6);
    let time = |text| {
        Datum::Time(
            tidb_datatype::parse_datetime(text, &chrono_tz::UTC, true, false)
                .unwrap()
                .time,
        )
    };
    compare_typed_profile(
        call(
            "greatest",
            datetime.clone(),
            vec![typed_column(0, &datetime), typed_column(1, &datetime)],
        ),
        &[datetime.clone(), datetime],
        &[
            vec![
                time("2026-10-18 12:34:56.123456"),
                time("2026-10-18 12:34:56.123457"),
            ],
            vec![Datum::Null, time("2020-01-01 00:00:00")],
        ],
        &[0, 1, 0],
    );
}
#[test]
fn native_extremum_string_as_time_sources_require_canonical_driver() {
    let date = FieldType::new(FieldTypeCode::Date);
    let time = tidb_datatype::parse_datetime("2020-01-01", &chrono_tz::UTC, true, false)
        .unwrap()
        .time;
    compare_typed_profile(
        call(
            "least",
            string_ty(),
            vec![typed_column(0, &date), typed_column(1, &string_ty())],
        ),
        &[date, string_ty()],
        &[
            vec![Datum::Time(time), Datum::new_string("99-1-1")],
            vec![Datum::Time(time), Datum::new_string("zzz")],
        ],
        &[0, 1, 0],
    );
}
#[test]
fn native_extremum_vector_sources_require_canonical_driver() {
    let vector = FieldType::new(FieldTypeCode::VectorFloat32);
    let value =
        |elements| Datum::VectorFloat32(tidb_datatype::VectorFloat32::must_create(elements));
    compare_typed_profile(
        call(
            "greatest",
            vector.clone(),
            vec![typed_column(0, &vector), typed_column(1, &vector)],
        ),
        &[vector.clone(), vector],
        &[
            vec![value(vec![1.0, -0.0]), value(vec![2.0])],
            vec![Datum::Null, value(vec![])],
        ],
        &[0, 1, 0],
    );
}

#[test]
fn native_extremum_eager_sources_and_parent_phases_preserve_null_errors_and_reuse() {
    let inside = || call("greatest", ty(), vec![column(0), abs(column(1))]);
    let roots = [
        inside(),
        plus(inside(), abs(column(1))),
        plus(abs(column(1)), inside()),
        call("if", ty(), vec![column(0), inside(), int(7)]),
        call("least", ty(), vec![inside(), abs(column(1))]),
    ];
    let schema = [ty(), ty()];
    let rows = vec![
        vec![Datum::Null, Datum::Int(i64::MIN)],
        vec![Datum::Int(100), Datum::Int(i64::MIN)],
        vec![Datum::Int(3), Datum::Int(2)],
        vec![Datum::Int(0), Datum::Int(1)],
    ];
    for root in roots {
        for selection in [
            vec![],
            vec![0],
            vec![1],
            vec![2, 3, 2],
            vec![3; 2050],
            {
                let mut selection = vec![3; 2050];
                selection[2049] = 0;
                selection
            },
            vec![2, 0, 3],
        ] {
            compare_arithmetic_profile(root.clone(), &schema, &rows, &selection);
        }
    }
}
#[test]
fn native_extremum_source_decimal_precision_constantness_and_result_flags_match_original() {
    let decimal = FieldType::new(FieldTypeCode::NewDecimal)
        .with_flen(40)
        .with_decimal(4);
    for name in ["greatest", "least"] {
        for root in [
            call(
                name,
                decimal.clone(),
                vec![column(0), typed_column(1, &decimal)],
            ),
            call(
                name,
                decimal.clone(),
                vec![
                    int(1),
                    literal(
                        Datum::Decimal(tidb_datatype::Decimal::from_literal("2.5000")),
                        decimal.clone(),
                    ),
                ],
            ),
            call(
                name,
                ty().with_unsigned(true),
                vec![
                    literal(Datum::UInt(u64::MAX), ty().with_unsigned(true)),
                    column(0),
                ],
            ),
        ] {
            compare_typed_profile(
                root,
                &[ty(), decimal.clone()],
                &[
                    vec![
                        Datum::Int(1),
                        Datum::Decimal(tidb_datatype::Decimal::from_literal("2.5000")),
                    ],
                    vec![
                        Datum::Int(-5),
                        Datum::Decimal(tidb_datatype::Decimal::from_literal("-5.0000")),
                    ],
                    vec![
                        Datum::Null,
                        Datum::Decimal(tidb_datatype::Decimal::from_literal("1.2300")),
                    ],
                ],
                &[0, 1, 2, 0],
            );
        }
    }
}

#[test]
fn native_extremum_source_capabilities_resources_cancel_and_reuse_are_terminal() {
    use tidb_query_expr::local::{
        HostArgReply, HostInvocation, HostStart, HostStep, LocalHostServices,
    };
    let field = projected(&native_bytes_type(), true).unwrap();
    let args = vec![field.clone(); 3];
    let catalog = HostCatalog::for_sql_batch(vec![(
        SqlBatchHostOperation::ExtremumNative,
        HostSignature {
            arg_types: args.clone().into_boxed_slice(),
            return_type: field.clone(),
        },
    )])
    .unwrap();
    let slot = catalog.slot(0).unwrap();
    let metadata = [NativeHostMetadata::Extremum(
        hosts::NativeExtremumMetadata {
            arguments: Box::new([3, 8, 9]),
            signature: Some(crate::builtin_ext::GlSignature {
                arg_type: NativeEvalType::String,
                cmp_string_mode: crate::builtin_ext::GlCmpStringMode::Directly,
                ret_date: false,
            }),
            decimals: Box::new([0, 0, 0]),
            all_constant: false,
            collation: Collation::Binary,
            greatest: true,
            result_type: string_ty(),
            output_node: 2,
            output_role: NativeValueRole::Output,
        },
    )];
    let store = RefCell::new(NativeValueStore::new(1 << 20).unwrap());
    store.borrow_mut().begin_window().unwrap();
    let row = InputRow {
        occurrence: 0,
        input_row: 0,
    };
    let origin = NativeValueOrigin { node: 9, row };
    let stale = store
        .borrow_mut()
        .insert_clone(&Datum::Int(3), NativeValueRole::CharInput, origin)
        .unwrap();
    store.borrow_mut().begin_window().unwrap();
    let tokens = [3, 8, 9]
        .into_iter()
        .enumerate()
        .map(|(i, node)| {
            store
                .borrow_mut()
                .insert_clone(
                    &Datum::Int(i as i64 + 1),
                    NativeValueRole::CharInput,
                    NativeValueOrigin { node, row },
                )
                .unwrap()
        })
        .collect::<Vec<_>>();
    let null = store
        .borrow_mut()
        .insert_clone(
            &Datum::Null,
            NativeValueRole::CharInput,
            NativeValueOrigin { node: 3, row },
        )
        .unwrap();
    let wrong_role = store
        .borrow_mut()
        .insert_clone(&Datum::Int(3), NativeValueRole::Output, origin)
        .unwrap();
    let wrong_node = store
        .borrow_mut()
        .insert_clone(
            &Datum::Int(3),
            NativeValueRole::CharInput,
            NativeValueOrigin { node: 8, row },
        )
        .unwrap();
    let wrong_row = store
        .borrow_mut()
        .insert_clone(
            &Datum::Int(3),
            NativeValueRole::CharInput,
            NativeValueOrigin {
                node: 9,
                row: InputRow {
                    occurrence: 1,
                    input_row: 0,
                },
            },
        )
        .unwrap();
    let large = store
        .borrow_mut()
        .insert_clone(
            &Datum::Bytes(vec![b'X'; 16384]),
            NativeValueRole::CharInput,
            origin,
        )
        .unwrap();
    let huge = store
        .borrow_mut()
        .insert_clone(
            &Datum::Decimal(tidb_datatype::Decimal::from_raw_parts(
                false,
                vec![b'1'],
                u32::MAX,
                0,
            )),
            NativeValueRole::CharInput,
            origin,
        )
        .unwrap();
    let context = ArithmeticProfilesContext::default();
    let mut provider =
        NativeBatchHosts::new(catalog, &metadata, &context, 8192).with_native_values(Some(&store));
    let mut ctx = EvalContext::default();
    for nullable in [false, true] {
        for (bad, kind) in [
            (stale.clone(), 0),
            (wrong_role.clone(), 0),
            (wrong_node.clone(), 0),
            (wrong_row.clone(), 0),
            (large.clone(), 1),
            (huge.clone(), 2),
        ] {
            // A NULL prepass must skip Decimal formatting; only the non-NULL
            // case actually demands that work. Bad capability/clone errors still
            // precede the kernel even after an already-fetched NULL.
            if nullable && kind == 2 {
                continue;
            }
            let HostStart::Pending { task, .. } = provider
                .start(
                    &mut ctx,
                    HostInvocation {
                        slot,
                        row,
                        arg_types: &args,
                        return_type: &field,
                    },
                )
                .unwrap()
            else {
                panic!("extremum pending")
            };
            for index in 0..2 {
                let token = if nullable && index == 0 {
                    null.clone()
                } else {
                    tokens[index].clone()
                };
                let values = VectorValue::from_scalar(&ScalarValue::Bytes(Some(token)), 1);
                assert!(
                    matches!(provider.resume(&mut ctx,&task,HostArgReply{index,field_type:&field,values:&values}).unwrap(),HostStep::NeedArg(request) if request.index==index+1)
                );
            }
            let before = context.trace.borrow().clone();
            let values = VectorValue::from_scalar(&ScalarValue::Bytes(Some(bad)), 1);
            let error = provider
                .resume(
                    &mut ctx,
                    &task,
                    HostArgReply {
                        index: 2,
                        field_type: &field,
                        values: &values,
                    },
                )
                .unwrap_err();
            match kind {
                0 => assert!(matches!(error, LocalError::BindingContract(_)), "{error:?}"),
                1 => assert!(matches!(error, LocalError::ResourceLimit(_)), "{error:?}"),
                _ => {
                    assert!(matches!(error, LocalError::HostContract(_)));
                    let Some(EvalError::ExpressionRuntimeFailure(native)) =
                        provider.take_native_error()
                    else {
                        panic!("native bounded work refusal")
                    };
                    assert_eq!(
                        native.class(),
                        crate::ExpressionRuntimeFailureClass::ResourceLimit
                    );
                    assert_eq!(native.phase(), None);
                }
            }
            assert_eq!(*context.trace.borrow(), before);
            assert!(provider.take_native_error().is_none());
            provider.cancel(&task);
            provider.cancel(&task);
        }
    }
    for nullable in [false, true] {
        let HostStart::Pending { task, .. } = provider
            .start(
                &mut ctx,
                HostInvocation {
                    slot,
                    row,
                    arg_types: &args,
                    return_type: &field,
                },
            )
            .unwrap()
        else {
            panic!("extremum pending")
        };
        for index in 0..3 {
            let token = if nullable && index == 0 {
                null.clone()
            } else {
                tokens[index].clone()
            };
            let values = VectorValue::from_scalar(&ScalarValue::Bytes(Some(token)), 1);
            let step = provider
                .resume(
                    &mut ctx,
                    &task,
                    HostArgReply {
                        index,
                        field_type: &field,
                        values: &values,
                    },
                )
                .unwrap();
            if index < 2 {
                assert!(matches!(step, HostStep::NeedArg(_)));
            } else {
                let HostStep::Ready(output) = step else {
                    panic!("extremum incomplete")
                };
                use tidb_query_datatype::codec::data_type::ScalarValueRef;
                let ScalarValueRef::Bytes(Some(token)) = output.get_scalar_ref(0) else {
                    panic!("output capability")
                };
                let value = store
                    .borrow()
                    .get(
                        token,
                        NativeValueRole::Output,
                        NativeValueOrigin { node: 2, row },
                    )
                    .unwrap()
                    .clone();
                assert_eq!(
                    value,
                    if nullable {
                        Datum::Null
                    } else {
                        Datum::new_string("3")
                    }
                );
            }
        }
        provider.cancel(&task);
    }
}

#[test]
fn native_charset_to_binary_gbk_sources_require_canonical_driver() {
    let gbk = string_ty().with_collation(Collation::GbkBin);
    let binary = string_ty().with_collation(Collation::Binary);
    compare_typed_profile(
        call("to_binary", binary, vec![typed_column(0, &gbk)]),
        &[gbk],
        &[
            vec![Datum::new_collation_string("一", Collation::GbkBin)],
            vec![Datum::Null],
            vec![Datum::new_collation_string("😂", Collation::GbkBin)],
        ],
        &[0, 1, 2],
    );
}
#[test]
fn native_charset_from_binary_gbk_sources_require_canonical_driver() {
    let binary = string_ty().with_collation(Collation::Binary);
    let gbk = string_ty().with_collation(Collation::GbkBin);
    compare_typed_profile(
        call("from_binary", gbk, vec![typed_column(0, &binary)]),
        &[binary],
        &[
            vec![Datum::Bytes(vec![0xd2, 0xbb])],
            vec![Datum::Null],
            vec![Datum::Bytes(vec![0x81])],
        ],
        &[0, 1, 2],
    );
}
#[test]
fn native_charset_convert_using_dynamic_target_sources_require_canonical_driver() {
    let string = string_ty();
    compare_typed_profile(
        call(
            "convert_using",
            string.clone(),
            vec![typed_column(0, &string), typed_column(1, &string)],
        ),
        &[string.clone(), string],
        &[
            vec![Datum::new_string("一"), Datum::new_string("gbk")],
            vec![Datum::new_string("😂"), Datum::new_string("gbk")],
            vec![Datum::Null, Datum::new_string("UNKNOWN")],
            vec![Datum::new_string("a"), Datum::new_string("UNKNOWN")],
        ],
        &[0, 1, 2, 3],
    );
}
#[test]
fn native_charset_convert_using_binary_decode_sources_require_canonical_driver() {
    let binary = string_ty().with_collation(Collation::Binary);
    let string = string_ty();
    compare_typed_profile(
        call(
            "convert_using",
            string.clone(),
            vec![typed_column(0, &binary), typed_column(1, &string)],
        ),
        &[binary, string],
        &[
            vec![Datum::Bytes(vec![0xd2, 0xbb]), Datum::new_string("gbk")],
            vec![Datum::Bytes(vec![255]), Datum::new_string("utf8mb4")],
            vec![Datum::Null, Datum::Null],
        ],
        &[0, 1, 2, 0],
    );
}
#[test]
fn native_charset_convert_using_source_null_skips_target_errors_in_canonical_driver() {
    let string = string_ty();
    compare_typed_profile(
        call(
            "convert_using",
            string.clone(),
            vec![
                typed_column(0, &string),
                call("cast_char", string.clone(), vec![abs(column(1))]),
            ],
        ),
        &[string, ty()],
        &[
            vec![Datum::Null, Datum::Int(i64::MIN)],
            vec![Datum::new_string("a"), Datum::Int(i64::MIN)],
        ],
        &[0, 1],
    );
}
#[test]
fn native_charset_convert_using_numeric_cast_sources_require_canonical_driver() {
    let string = string_ty();
    compare_typed_profile(
        call(
            "convert_using",
            string.clone(),
            vec![abs(column(0)), typed_column(1, &string)],
        ),
        &[ty(), string],
        &[
            vec![Datum::Int(2), Datum::new_string("utf8mb4")],
            vec![Datum::Int(7), Datum::new_string("gbk")],
            vec![Datum::Null, Datum::new_string("UNKNOWN")],
        ],
        &[0, 1, 2, 0],
    );
}

#[derive(Default)]
struct CharsetSourceContext {
    enabled: Cell<bool>,
    connection: Cell<usize>,
    events: std::cell::RefCell<Vec<String>>,
}
impl Columns for CharsetSourceContext {
    fn get(&self, _: &[String]) -> Option<Datum> {
        panic!("charset sources must use checked value bindings")
    }
    fn enable_vectorized_expression(&self) -> bool {
        self.events.borrow_mut().push("enable".into());
        self.enabled.get()
    }
    fn connection_charset_info(&self) -> (&str, &str) {
        let n = self.connection.get();
        self.connection.set(n + 1);
        self.events.borrow_mut().push(format!("connection:{n}"));
        match n % 3 {
            0 => ("gbk", "gbk_bin"),
            1 => ("binary", "binary"),
            _ => ("utf8mb4", "utf8mb4_bin"),
        }
    }
    fn append_warning(&self, code: u16, message: &str) {
        self.events
            .borrow_mut()
            .push(format!("warn:{code}:{message}"));
    }
    fn truncate_level(&self) -> crate::ErrorLevel {
        crate::ErrorLevel::Warn
    }
    fn time_zone(&self) -> SessionTimeZone {
        self.events.borrow_mut().push("zone".into());
        SessionTimeZone::utc()
    }
}
fn compare_charset_source_context(
    root: Expression,
    schema: &[FieldType],
    rows: &[Vec<Datum>],
    selection: &[usize],
) {
    let field = snapshot_field_type(root.static_type().unwrap());
    let actual_suite = suite(root.clone());
    let reference_suite = suite(root);
    for enabled in [true, false, true, false] {
        let actual_ctx = CharsetSourceContext::default();
        let reference_ctx = CharsetSourceContext::default();
        actual_ctx.enabled.set(enabled);
        reference_ctx.enabled.set(enabled);
        let mut input = chunk(schema, rows);
        input.set_sel(Some(selection.to_vec()));
        let mut expected_input = chunk(schema, rows);
        expected_input.set_sel(Some(selection.to_vec()));
        let mut output = output_for(&field);
        let mut expected = output_for(&field);
        let actual = actual_suite.run_with_tikv_batch(&actual_ctx, &mut input, &mut output, schema);
        let reference = reference_suite.run(&reference_ctx, &mut expected_input, &mut expected);
        assert_eq!(actual, reference, "flag={enabled} selection={selection:?}");
        assert_eq!(*actual_ctx.events.borrow(), *reference_ctx.events.borrow());
        assert_eq!(actual_ctx.connection.get(), reference_ctx.connection.get());
        assert_eq!(output.num_rows(), expected.num_rows());
        for position in 0..output.num_rows() {
            assert_eq!(
                output.get_row(position).is_null(0),
                expected.get_row(position).is_null(0)
            );
            assert_eq!(
                output.column(0).get_raw(position).as_ref(),
                expected.column(0).get_raw(position).as_ref()
            );
        }
        let routes = actual_suite.batch_route_snapshot();
        assert_eq!((routes.refusals, routes.fallback), (0, 0));
    }
}
#[test]
fn native_charset_dynamic_connection_read_and_parent_phases_preserve_order_and_reuse() {
    let string = string_ty();
    let binary = string.clone().with_collation(Collation::Binary);
    let gbk = string.clone().with_collation(Collation::GbkBin);
    let inside = || {
        call(
            "convert_using",
            string.clone(),
            vec![typed_column(0, &string), typed_column(1, &string)],
        )
    };
    let roots = [
        inside(),
        call("concat", string.clone(), vec![inside(), text(Some(b"x"))]),
        call(
            "ifnull",
            string.clone(),
            vec![inside(), text(Some(b"fallback"))],
        ),
        call(
            "to_binary",
            binary,
            vec![call(
                "convert_using",
                gbk,
                vec![typed_column(0, &string), text(Some(b"gbk"))],
            )],
        ),
    ];
    let rows = vec![
        vec![Datum::new_string("一"), Datum::new_string("gbk")],
        vec![Datum::Null, Datum::new_string("UNKNOWN")],
        vec![Datum::new_string("😂"), Datum::new_string("gb18030")],
        vec![Datum::new_string("a"), Datum::new_string("UNKNOWN")],
    ];
    for root in roots {
        for selection in [
            vec![],
            vec![0, 1, 2, 0],
            vec![3],
            vec![0; 2050],
            {
                let mut selection = vec![0; 2050];
                selection[2049] = 3;
                selection
            },
            vec![1, 3, 1],
        ] {
            compare_charset_source_context(
                root.clone(),
                &[string.clone(), string.clone()],
                &rows,
                &selection,
            );
        }
    }
}
#[test]
fn native_charset_numeric_source_metadata_tracks_each_connection_getter() {
    let string = string_ty();
    let root = call(
        "convert_using",
        string.clone(),
        vec![column(0), typed_column(1, &string)],
    );
    compare_charset_source_context(
        root,
        &[ty(), string],
        &[
            vec![Datum::Int(2), Datum::new_string("gbk")],
            vec![Datum::Null, Datum::new_string("UNKNOWN")],
            vec![Datum::Int(3), Datum::new_string("binary")],
            vec![Datum::Int(4), Datum::new_string("utf8mb4")],
        ],
        &[0, 2, 1, 3, 0, 2],
    );
}
#[test]
fn native_charset_actual_source_metadata_keeps_alias_spelling_and_target_null_semantics() {
    for name in ["gbk", "GBK", "gb18030", "utf8mb3", "unknown", ""] {
        let source = string_ty()
            .with_collation(Collation::GbkBin)
            .with_charset_name(name);
        let binary = string_ty().with_collation(Collation::Binary);
        compare_typed_profile(
            call("to_binary", binary.clone(), vec![typed_column(0, &source)]),
            &[source.clone()],
            &[
                vec![Datum::new_string("一")],
                vec![Datum::new_string("😂")],
                vec![Datum::Null],
            ],
            &[0, 1, 2],
        );
        compare_typed_profile(
            call(
                "from_binary",
                source.clone(),
                vec![typed_column(0, &binary)],
            ),
            &[binary],
            &[vec![Datum::Bytes(vec![0xd2, 0xbb])], vec![Datum::Null]],
            &[0, 1],
        );
        compare_charset_source_context(
            call(
                "convert_using",
                string_ty(),
                vec![typed_column(0, &source), typed_column(1, &string_ty())],
            ),
            &[source, string_ty()],
            &[
                vec![Datum::new_string("一"), Datum::new_string("binary")],
                vec![Datum::Null, Datum::Null],
                vec![Datum::new_string("a"), Datum::Null],
            ],
            &[0, 1, 2],
        );
    }
}

#[test]
fn native_charset_source_capabilities_resource_phases_cancel_and_reuse_are_terminal() {
    use tidb_query_datatype::codec::data_type::ScalarValueRef;
    use tidb_query_expr::local::{
        HostArgReply, HostInvocation, HostStart, HostStep, LocalHostServices,
    };
    use tidb_query_expr::NativeCharsetOperation as Op;
    for operation in [Op::ToBinary, Op::FromBinary, Op::ConvertUsing] {
        let field = projected(&native_bytes_type(), true).unwrap();
        let args = vec![field.clone(); operation.arity()];
        let catalog = HostCatalog::for_sql_batch(vec![(
            SqlBatchHostOperation::CharsetNative(operation),
            HostSignature {
                arg_types: args.clone().into_boxed_slice(),
                return_type: field.clone(),
            },
        )])
        .unwrap();
        let slot = catalog.slot(0).unwrap();
        let metadata = [NativeHostMetadata::Charset(hosts::NativeCharsetMetadata {
            operation,
            arguments: [3, 8][..operation.arity()].into(),
            source_type: string_ty(),
            explicit_collation: false,
            result_type: string_ty(),
            output_node: 2,
            output_role: Some(NativeValueRole::Output),
        })];
        let store = RefCell::new(NativeValueStore::new(1 << 20).unwrap());
        store.borrow_mut().begin_window().unwrap();
        let row = InputRow {
            occurrence: 0,
            input_row: 0,
        };
        let insert = |value: &Datum, role, node, row| {
            store
                .borrow_mut()
                .insert_clone(value, role, NativeValueOrigin { node, row })
                .unwrap()
        };
        let stale = insert(&Datum::new_string("a"), NativeValueRole::CharInput, 3, row);
        store.borrow_mut().begin_window().unwrap();
        let huge = Datum::Decimal(tidb_datatype::Decimal::from_raw_parts(
            false,
            vec![b'1'],
            u32::MAX,
            0,
        ));
        let context = CharsetSourceContext::default();
        let mut provider = NativeBatchHosts::new(catalog, &metadata, &context, 8192)
            .with_native_values(Some(&store));
        let mut ctx = EvalContext::default();
        // Each position validates epoch, role, producer and occurrence before
        // any value formatting or context callback. Then cancel twice and reuse.
        for index in 0..operation.arity() {
            let node = [3, 8][index];
            for bad in [
                stale.clone(),
                insert(&huge, NativeValueRole::Output, node, row),
                insert(&huge, NativeValueRole::CharInput, 9, row),
                insert(
                    &huge,
                    NativeValueRole::CharInput,
                    node,
                    InputRow {
                        occurrence: 1,
                        input_row: 0,
                    },
                ),
            ] {
                let HostStart::Pending { task, .. } = provider
                    .start(
                        &mut ctx,
                        HostInvocation {
                            slot,
                            row,
                            arg_types: &args,
                            return_type: &field,
                        },
                    )
                    .unwrap()
                else {
                    panic!("charset pending")
                };
                if index == 1 {
                    let first = insert(&Datum::new_string("a"), NativeValueRole::CharInput, 3, row);
                    let values = VectorValue::from_scalar(&ScalarValue::Bytes(Some(first)), 1);
                    assert!(
                        matches!(provider.resume(&mut ctx,&task,HostArgReply{index:0,field_type:&field,values:&values}).unwrap(),HostStep::NeedArg(request) if request.index==1)
                    );
                }
                let before = context.connection.get();
                let values = VectorValue::from_scalar(&ScalarValue::Bytes(Some(bad)), 1);
                assert!(matches!(
                    provider.resume(
                        &mut ctx,
                        &task,
                        HostArgReply {
                            index,
                            field_type: &field,
                            values: &values
                        }
                    ),
                    Err(LocalError::BindingContract(_))
                ));
                assert_eq!(context.connection.get(), before);
                assert!(provider.take_native_error().is_none());
                provider.cancel(&task);
                provider.cancel(&task);
            }
        }
        // Codec allocation, target formatting, source formatting, and an early
        // target SQL error have distinct terminal classifications and phases.
        let cases = if operation == Op::ConvertUsing {
            vec![
                (Datum::new_string("a"), huge.clone(), 0, 0),
                (huge.clone(), Datum::new_string("utf8mb4"), 0, 0),
                (huge.clone(), Datum::MinNotNull, 1, 0),
                (
                    Datum::new_string("a".repeat(16384)),
                    Datum::new_string("UNKNOWN"),
                    2,
                    1,
                ),
            ]
        } else {
            vec![(Datum::Bytes(vec![b'A'; 16384]), Datum::Null, 3, 0)]
        };
        for (source, target, kind, getters) in cases {
            let first = insert(&source, NativeValueRole::CharInput, 3, row);
            let HostStart::Pending { task, .. } = provider
                .start(
                    &mut ctx,
                    HostInvocation {
                        slot,
                        row,
                        arg_types: &args,
                        return_type: &field,
                    },
                )
                .unwrap()
            else {
                panic!("charset pending")
            };
            let before = context.connection.get();
            let mut result = provider.resume(
                &mut ctx,
                &task,
                HostArgReply {
                    index: 0,
                    field_type: &field,
                    values: &VectorValue::from_scalar(&ScalarValue::Bytes(Some(first)), 1),
                },
            );
            if operation == Op::ConvertUsing {
                assert!(matches!(result.unwrap(), HostStep::NeedArg(_)));
                let second = insert(&target, NativeValueRole::CharInput, 8, row);
                result = provider.resume(
                    &mut ctx,
                    &task,
                    HostArgReply {
                        index: 1,
                        field_type: &field,
                        values: &VectorValue::from_scalar(&ScalarValue::Bytes(Some(second)), 1),
                    },
                );
            }
            let error = result.unwrap_err();
            assert_eq!(context.connection.get() - before, getters);
            match kind {
                0 => {
                    assert!(matches!(error, LocalError::ResourceLimit(_)), "{error:?}");
                    assert!(provider.take_native_error().is_none());
                }
                1 | 2 => {
                    assert!(matches!(error, LocalError::HostContract(_)), "{error:?}");
                    assert_eq!(
                        provider.take_native_error(),
                        Some(EvalError::Unsupported(if kind == 1 {
                            "range sentinel byte coercion"
                        } else {
                            "unknown character set"
                        }))
                    );
                }
                _ => {
                    assert!(matches!(error, LocalError::HostContract(_)), "{error:?}");
                    let Some(EvalError::ExpressionRuntimeFailure(error)) =
                        provider.take_native_error()
                    else {
                        panic!("codec resource cause")
                    };
                    assert_eq!(
                        error.class(),
                        crate::ExpressionRuntimeFailureClass::ResourceLimit
                    );
                    assert_eq!(error.phase(), None);
                }
            }
            assert!(provider.take_native_error().is_none());
            provider.cancel(&task);
            provider.cancel(&task);
        }
        // Retained first input also expires: authenticate it before formatting a
        // valid but enormous second argument. The failure is binding, not work.
        if operation == Op::ConvertUsing {
            let first = insert(&Datum::new_string("a"), NativeValueRole::CharInput, 3, row);
            let HostStart::Pending { task, .. } = provider
                .start(
                    &mut ctx,
                    HostInvocation {
                        slot,
                        row,
                        arg_types: &args,
                        return_type: &field,
                    },
                )
                .unwrap()
            else {
                panic!()
            };
            assert!(matches!(
                provider
                    .resume(
                        &mut ctx,
                        &task,
                        HostArgReply {
                            index: 0,
                            field_type: &field,
                            values: &VectorValue::from_scalar(&ScalarValue::Bytes(Some(first)), 1)
                        }
                    )
                    .unwrap(),
                HostStep::NeedArg(_)
            ));
            store.borrow_mut().begin_window().unwrap();
            let second = insert(&huge, NativeValueRole::CharInput, 8, row);
            let before = context.connection.get();
            assert!(matches!(
                provider.resume(
                    &mut ctx,
                    &task,
                    HostArgReply {
                        index: 1,
                        field_type: &field,
                        values: &VectorValue::from_scalar(&ScalarValue::Bytes(Some(second)), 1)
                    }
                ),
                Err(LocalError::BindingContract(_))
            ));
            assert_eq!(context.connection.get(), before);
            assert!(provider.take_native_error().is_none());
            provider.cancel(&task);
        }
        for nullable in [true, false, true, false] {
            let first = insert(
                &if nullable {
                    Datum::Null
                } else {
                    Datum::new_string("a")
                },
                NativeValueRole::CharInput,
                3,
                row,
            );
            let before = context.connection.get();
            let HostStart::Pending { task, .. } = provider
                .start(
                    &mut ctx,
                    HostInvocation {
                        slot,
                        row,
                        arg_types: &args,
                        return_type: &field,
                    },
                )
                .unwrap()
            else {
                panic!()
            };
            let mut step = provider
                .resume(
                    &mut ctx,
                    &task,
                    HostArgReply {
                        index: 0,
                        field_type: &field,
                        values: &VectorValue::from_scalar(&ScalarValue::Bytes(Some(first)), 1),
                    },
                )
                .unwrap();
            if !nullable && operation == Op::ConvertUsing {
                assert!(matches!(step, HostStep::NeedArg(_)));
                let second = insert(
                    &Datum::new_string("utf8mb4"),
                    NativeValueRole::CharInput,
                    8,
                    row,
                );
                step = provider
                    .resume(
                        &mut ctx,
                        &task,
                        HostArgReply {
                            index: 1,
                            field_type: &field,
                            values: &VectorValue::from_scalar(&ScalarValue::Bytes(Some(second)), 1),
                        },
                    )
                    .unwrap();
            }
            assert_eq!(
                context.connection.get() - before,
                usize::from(!nullable && operation == Op::ConvertUsing)
            );
            let HostStep::Ready(output) = step else {
                panic!("charset result")
            };
            let ScalarValueRef::Bytes(Some(token)) = output.get_scalar_ref(0) else {
                panic!("charset output provenance")
            };
            let value = store
                .borrow()
                .get(
                    token,
                    NativeValueRole::Output,
                    NativeValueOrigin { node: 2, row },
                )
                .unwrap()
                .clone();
            if nullable {
                assert_eq!(value, Datum::Null)
            } else {
                assert_eq!(
                    crate::arg_eval_type::eval_string_ref(&value).unwrap(),
                    Some(b"a".as_slice())
                )
            };
            assert!(provider.take_native_error().is_none());
            provider.cancel(&task);
        }
    }
}

#[test]
fn native_charset_context_callback_releases_store_and_gb_default_is_read_after_conversion() {
    const CHILD: &str = "TIDB_CHARSET_LATE_MODE_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let output=std::process::Command::new(std::env::current_exe().unwrap()).args(["--exact","evaluator::batch::tests::native_charset_context_callback_releases_store_and_gb_default_is_read_after_conversion","--nocapture"]).env(CHILD,"1").output().unwrap();
        assert!(
            output.status.success(),
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        return;
    }
    use tidb_query_datatype::codec::data_type::ScalarValueRef;
    use tidb_query_expr::local::{
        HostArgReply, HostInvocation, HostStart, HostStep, LocalHostServices,
    };
    use tidb_query_expr::NativeCharsetOperation as Op;
    struct Late<'a> {
        store: &'a RefCell<NativeValueStore>,
        calls: Cell<usize>,
    }
    impl Columns for Late<'_> {
        fn get(&self, _: &[String]) -> Option<Datum> {
            panic!("no interpreter")
        }
        fn connection_charset_info(&self) -> (&str, &str) {
            let _exclusive = self.store.borrow_mut();
            self.calls.set(self.calls.get() + 1);
            tidb_datatype::set_new_collation_enabled(true);
            ("utf8mb4", "utf8mb4_bin")
        }
    }
    let field = projected(&native_bytes_type(), true).unwrap();
    let args = vec![field.clone(); 2];
    let catalog = HostCatalog::for_sql_batch(vec![(
        SqlBatchHostOperation::CharsetNative(Op::ConvertUsing),
        HostSignature {
            arg_types: args.clone().into_boxed_slice(),
            return_type: field.clone(),
        },
    )])
    .unwrap();
    let slot = catalog.slot(0).unwrap();
    let metadata = [NativeHostMetadata::Charset(hosts::NativeCharsetMetadata {
        operation: Op::ConvertUsing,
        arguments: Box::new([3, 8]),
        source_type: string_ty(),
        explicit_collation: false,
        result_type: string_ty().with_collation(Collation::GbkBin),
        output_node: 2,
        output_role: Some(NativeValueRole::Output),
    })];
    let store = RefCell::new(NativeValueStore::new(1 << 20).unwrap());
    store.borrow_mut().begin_window().unwrap();
    let context = Late {
        store: &store,
        calls: Cell::new(0),
    };
    let mut provider =
        NativeBatchHosts::new(catalog, &metadata, &context, 8192).with_native_values(Some(&store));
    let mut ctx = EvalContext::default();
    let row = InputRow {
        occurrence: 0,
        input_row: 0,
    };
    for nullable in [false, true, false] {
        tidb_datatype::set_new_collation_enabled(false);
        let before = context.calls.get();
        let value = if nullable {
            Datum::Null
        } else {
            Datum::new_string("一")
        };
        let first = store
            .borrow_mut()
            .insert_clone(
                &value,
                NativeValueRole::CharInput,
                NativeValueOrigin { node: 3, row },
            )
            .unwrap();
        let HostStart::Pending { task, .. } = provider
            .start(
                &mut ctx,
                HostInvocation {
                    slot,
                    row,
                    arg_types: &args,
                    return_type: &field,
                },
            )
            .unwrap()
        else {
            panic!()
        };
        let mut step = provider
            .resume(
                &mut ctx,
                &task,
                HostArgReply {
                    index: 0,
                    field_type: &field,
                    values: &VectorValue::from_scalar(&ScalarValue::Bytes(Some(first)), 1),
                },
            )
            .unwrap();
        if !nullable {
            assert!(matches!(step, HostStep::NeedArg(_)));
            let second = store
                .borrow_mut()
                .insert_clone(
                    &Datum::new_string("gbk"),
                    NativeValueRole::CharInput,
                    NativeValueOrigin { node: 8, row },
                )
                .unwrap();
            step = provider
                .resume(
                    &mut ctx,
                    &task,
                    HostArgReply {
                        index: 1,
                        field_type: &field,
                        values: &VectorValue::from_scalar(&ScalarValue::Bytes(Some(second)), 1),
                    },
                )
                .unwrap();
        }
        assert_eq!(context.calls.get() - before, usize::from(!nullable));
        let HostStep::Ready(output) = step else {
            panic!()
        };
        let ScalarValueRef::Bytes(Some(token)) = output.get_scalar_ref(0) else {
            panic!()
        };
        let value = store
            .borrow()
            .get(
                token,
                NativeValueRole::Output,
                NativeValueOrigin { node: 2, row },
            )
            .unwrap()
            .clone();
        if nullable {
            assert_eq!(value, Datum::Null)
        } else {
            assert_eq!(
                value,
                Datum::new_collation_string(
                    "一".as_bytes().to_vec(),
                    tidb_datatype::Charset::Gbk.default_collation()
                )
            );
        }
        provider.cancel(&task);
    }
}
