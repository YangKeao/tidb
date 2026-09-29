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
use tidb_ast::CiString;
use tidb_datatype::{Collation, Decimal, SessionTimeZone};
use tidb_proto::tipb as db_pb;
use tidb_query_datatype::expr::Error as KernelError;
use tidb_query_expr::local::OrdinaryProfile;

fn ty() -> FieldType {
    FieldType::new(FieldTypeCode::LongLong)
        .with_flen(20)
        .with_decimal(0)
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
fn call(name: &str, field: FieldType, args: Vec<Expression>) -> Expression {
    Expression::ScalarFunction(ScalarFunction::new(CiString::new(name), field, args))
}
fn plus(left: Expression, right: Expression) -> Expression {
    call("plus", ty(), vec![left, right])
}
fn suite(root: Expression) -> EvaluatorSuite {
    EvaluatorSuite::new(vec![root], false)
}
fn limits() -> NumericSourceLimits {
    NumericSourceLimits {
        tree: CompileLimits::default(),
        max_metadata_bytes: 1024 * 1024,
    }
}
fn prepare(suite: &EvaluatorSuite, schema: &[FieldType]) -> PreparedNumericBatch {
    PreparedNumericBatch::compile(
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
fn values(chunk: &Chunk) -> Vec<Datum> {
    (0..chunk.num_rows())
        .map(|row| chunk.get_row(row).get_datum(0, &ty()))
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
fn native_failure(
    root: &Expression,
    schema: &[FieldType],
    rows: &[Vec<Datum>],
    selected: &[usize],
) -> EvaluatorError {
    let suite = suite(root.clone());
    let mut input = chunk(schema, rows);
    input.set_sel(Some(selected.to_vec()));
    suite
        .run(&Context::new(true), &mut input, &mut output())
        .unwrap_err()
}
fn overflow(expression: &str) -> EvaluatorError {
    EvaluatorError::Eval(EvalError::DataOutOfRange {
        value: "BIGINT",
        expression: expression.into(),
    })
}
fn kernel_site(failure: &NumericBatchFailure) -> (usize, InputRow) {
    let Some(LocalFailureSite::Kernel { call, row }) = failure.site() else {
        panic!("actual kernel site: {failure:?}")
    };
    assert_eq!(call.profile(), OrdinaryProfile::NativeNumericBatch);
    assert_eq!(call.source().unit(), 93);
    assert_eq!(failure.source_ordinal(), Some(call.ordinal()));
    (call.ordinal(), *row)
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
    fn new(worker: &PreparedNumericBatch, rows: Vec<Vec<Datum>>) -> Self {
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
fn run_raw(
    suite: &EvaluatorSuite,
    worker: &mut PreparedNumericBatch,
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
            selection: Vec::new(),
            override_source: Some(source),
        },
    )
}

#[test]
fn own_numeric_facts_preserve_source_ordinals_and_exact_declarations() {
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
        assert_eq!(call.profile(), OrdinaryProfile::NativeNumericBatch);
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
fn actual_suite_native_and_private_consumers_match_selected_numeric_results() {
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
            .run_numeric_batch_reported(
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
fn current_global_flag_is_read_once_per_invoke_and_never_row_fallback() {
    let suite = suite(plus(null(), plus(int(i64::MAX), int(1))));
    let mut worker = prepare(&suite, &[]);
    let native_ctx = Context::new(false);
    let mut input = chunk(&[], &[vec![]]);
    let mut out = output();
    out.append_int64(0, 99);
    let error = suite
        .run_numeric_batch_reported(
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
        .run_numeric_batch_reported(
            &native_ctx,
            &mut EvalContext::default(),
            &mut worker,
            &mut input,
            &mut out,
            &[],
        )
        .unwrap_err();
    assert_eq!(native_ctx.calls.get(), 2);
    assert_eq!(kernel_site(&error).0, 2); // actual right child, even at N1/left NULL
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
        .run_numeric_batch_reported(
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
fn native_decimal_priority_and_leaf_admission_remain_unchanged() {
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
    assert!(PreparedNumericBatch::compile(
        &suite,
        &[],
        true,
        1,
        limits(),
        ExecutionLimits::default(),
        4096
    )
    .is_err());
    for root in [int(1), column(0)] {
        let input = chunk(&[ty()], &[vec![Datum::Int(9)]]);
        assert!(
            crate::scalar_function::try_eval_numeric_batch(&root, &Context::new(true), &input)
                .unwrap()
                .is_none()
        );
        assert!(PreparedNumericBatch::compile(
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
            .run_numeric_batch_reported(
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
            .run_numeric_batch_reported(
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
        assert!(PreparedNumericBatch::compile(
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
fn whole_left_later_failure_suppresses_whole_right_earlier_failure() {
    let root = plus(plus(column(0), int(1)), plus(column(1), int(1)));
    let schema = vec![ty(), ty()];
    let rows = vec![
        vec![Datum::Int(0), Datum::Int(i64::MAX)],
        vec![Datum::Int(i64::MAX), Datum::Int(0)],
    ];
    assert_eq!(
        native_failure(&root, &schema, &rows, &[0, 1]),
        overflow("(t.c0 + 1)")
    );
    let suite = suite(root);
    let mut worker = prepare(&suite, &schema);
    let mut raw = Raw::new(&worker, rows.clone());
    raw.poison = Some(1);
    let error = run_raw(
        &suite,
        &mut worker,
        &mut EvalContext::default(),
        &mut chunk(&schema, &rows),
        &mut output(),
        &schema,
        &mut raw,
    )
    .unwrap_err();
    let (ordinal, row) = kernel_site(&error);
    assert_eq!((ordinal, row.occurrence, row.input_row), (1, 1, 1));
    assert_eq!(
        raw.reads
            .iter()
            .map(|(slot, row)| (*slot, row.occurrence))
            .collect::<Vec<_>>(),
        vec![(0, 0), (0, 1)]
    );
}

#[test]
fn whole_right_failure_precedes_an_earlier_possible_parent_overflow() {
    let root = plus(column(0), plus(column(1), int(1)));
    let schema = vec![ty(), ty()];
    let rows = vec![
        vec![Datum::Int(i64::MAX), Datum::Int(0)],
        vec![Datum::Int(0), Datum::Int(i64::MAX)],
    ];
    assert_eq!(
        native_failure(&root, &schema, &rows, &[0, 1]),
        overflow("(t.c1 + 1)")
    );
    let suite = suite(root);
    let mut worker = prepare(&suite, &schema);
    let mut raw = Raw::new(&worker, rows.clone());
    let error = run_raw(
        &suite,
        &mut worker,
        &mut EvalContext::default(),
        &mut chunk(&schema, &rows),
        &mut output(),
        &schema,
        &mut raw,
    )
    .unwrap_err();
    let (ordinal, row) = kernel_site(&error);
    assert_eq!((ordinal, row.occurrence, row.input_row), (2, 1, 1));
    assert_eq!(
        raw.reads
            .iter()
            .map(|(slot, row)| (*slot, row.occurrence))
            .collect::<Vec<_>>(),
        vec![(0, 0), (0, 1), (1, 0), (1, 1)]
    );
}

#[test]
fn kernel_sites_use_actual_selected_occurrence_after_complete_input_phase() {
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
    let error = run_raw(
        &suite,
        &mut worker,
        &mut EvalContext::default(),
        &mut input,
        &mut output(),
        &schema,
        &mut raw,
    )
    .unwrap_err();
    let (ordinal, row) = kernel_site(&error);
    assert_eq!((ordinal, row.occurrence, row.input_row), (0, 2, 2));
    assert_eq!(
        raw.reads
            .iter()
            .map(|(_, row)| (row.occurrence, row.input_row))
            .collect::<Vec<_>>(),
        vec![(0, 1), (1, 0), (2, 2), (3, 2)]
    );
}

#[test]
fn selected_occurrences_not_physical_width_bound_virtual_constant_broadcasts() {
    let suite = suite(plus(int(1), int(2)));
    let mut worker = prepare(&suite, &[]);
    for count in [0, 1, 1024] {
        let mut input = chunk(&[], &vec![vec![]; 2048]);
        input.set_sel(Some((0..count).map(|row| 2047 - row).collect()));
        let mut out = output();
        suite
            .run_numeric_batch_raw(
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
    let error = suite
        .run_numeric_batch_reported(
            &Context::new(true),
            &mut EvalContext::default(),
            &mut worker,
            &mut chunk(&[], &vec![vec![]; 1025]),
            &mut output(),
            &[],
        )
        .unwrap_err();
    assert!(matches!(
        error.local_error(),
        Some(LocalError::ResourceLimit(_))
    ));
    assert!(error.site().is_none());
}

#[test]
fn documented1025_witness_refuses_without_tiling_or_leaf_effects() {
    let root = plus(plus(column(0), int(1)), plus(column(1), int(1)));
    let schema = vec![ty(), ty()];
    let mut rows = vec![vec![Datum::Int(0), Datum::Int(0)]; 1025];
    rows[0][1] = Datum::Int(i64::MAX);
    rows[1024][0] = Datum::Int(i64::MAX);
    assert_eq!(
        native_failure(&root, &schema, &rows, &(0..1025).collect::<Vec<_>>()),
        overflow("(t.c0 + 1)")
    );
    let suite = suite(root);
    let mut worker = prepare(&suite, &schema);
    let mut raw = Raw::new(&worker, rows.clone());
    raw.poison = Some(0);
    let error = run_raw(
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
        Some(LocalError::ResourceLimit(_))
    ));
    assert!(raw.reads.is_empty());
    assert!(error.site().is_none());
}

#[test]
fn strict_native_kind_validation_precedes_erasure_and_preserves_input_site() {
    let schema = vec![ty()];
    let suite = suite(plus(column(0), int(1)));
    let mut worker = prepare(&suite, &schema);
    let mut raw = Raw::new(&worker, vec![vec![Datum::UInt(0)]]);
    assert!(to_scalar(&Datum::UInt(0), EvalType::Int).is_ok());
    let error = run_raw(
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
    assert!(matches!(
        error.site(),
        Some(LocalFailureSite::InputSlot { slot: 0, .. })
    ));
    assert_eq!(error.source_ordinal(), Some(1));
    assert_eq!(raw.reads.len(), 1);
}

#[test]
fn warning_prefix_raw_reported_parity_and_fresh_retry_sites() {
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
    let error = run_raw(
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
    assert!(ctx.warnings.warnings[0].get_msg().contains("prior"));
    assert_eq!(error.source_ordinal(), Some(2));
    let message = error.local_error().unwrap().to_string();
    let raw_error = error.into_raw();
    assert!(raw_error.site().is_none());
    assert_eq!(raw_error.local_error().unwrap().to_string(), message);
    let mut out = output();
    suite
        .run_numeric_batch_reported(
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
        .run_numeric_batch_reported(
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
    let reported = suite
        .run_numeric_batch_reported(
            &Context::new(true),
            &mut ctx,
            &mut worker,
            &mut chunk(&schema, &bad),
            &mut output(),
            &schema,
        )
        .unwrap_err();
    let raw = suite
        .run_numeric_batch_raw(
            &Context::new(true),
            &mut ctx,
            &mut worker,
            &mut chunk(&schema, &bad),
            &mut output(),
            &schema,
        )
        .unwrap_err();
    assert_eq!(kernel_site(&reported).0, 0);
    assert!(raw.site().is_none());
    assert_eq!(
        raw.local_error().unwrap().to_string(),
        reported.local_error().unwrap().to_string()
    );
}

#[test]
fn complete_metadata_detachment_and_incoming_caps_precede_equality_and_effects() {
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
    let error = suite
        .run_numeric_batch_reported(
            &ctx,
            &mut EvalContext::default(),
            &mut worker,
            &mut chunk(&[], &[vec![]]),
            &mut output(),
            &[],
        )
        .unwrap_err();
    assert!(matches!(error.kind, FailureKind::Admission(_)));
    assert_eq!(ctx.calls.get(), 0);
    let suite = self::suite(plus(column(0), int(1)));
    let mut worker = prepare(&suite, &[ty()]);
    let huge = ty().with_elems(["x".repeat(limits().max_metadata_bytes)]);
    let error = suite
        .run_numeric_batch_reported(
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
    let error = suite
        .run_numeric_batch_reported(
            &Context::new(true),
            &mut EvalContext::default(),
            &mut worker,
            &mut input,
            &mut output(),
            &[wrong],
        )
        .unwrap_err();
    assert!(error.site().is_none());
    input.set_sel(Some(vec![1]));
    let error = suite
        .run_numeric_batch_reported(
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
        .run_numeric_batch_reported(
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
        .run_numeric_batch_reported(
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
        call("minus", ty(), vec![int(1), int(2)]),
        call("cast", ty(), vec![int(1)]),
        call("if", ty(), vec![int(1), int(2), int(3)]),
        literal(Datum::UInt(1), ty().with_unsigned(true)),
        literal(Datum::UInt(1), ty()),
        literal(Datum::Null, FieldType::new(FieldTypeCode::Null)),
        literal(Datum::Int(1), FieldType::new(FieldTypeCode::Tiny)),
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
        assert!(PreparedNumericBatch::compile(
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
    assert!(PreparedNumericBatch::compile(
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
    assert!(PreparedNumericBatch::compile(
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
        PreparedNumericBatch::compile(
            &suite,
            &[ty()],
            true,
            1,
            source_limits,
            ExecutionLimits::default(),
            4096
        ),
        Err(NumericBatchFailure {
            kind: FailureKind::Local(LocalError::ResourceLimit(_))
        })
    ));
    let mut worker = prepare(&suite, &[ty()]);
    worker.max_materialization_retained_bytes = size_of::<usize>(); // selection fits, result/source/native Vec do not
    let rows = vec![vec![Datum::Int(1)]];
    let mut raw = Raw::new(&worker, rows.clone());
    raw.warnings = true;
    let mut ctx = EvalContext::default();
    let error = run_raw(
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
    assert!(error.site().is_none());
    assert_eq!(raw.reads.len(), 1);
    assert_eq!(ctx.warnings.warning_cnt, 1);
    let mut worker = PreparedNumericBatch::compile(
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
    let error = run_raw(
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
    assert!(error.site().is_none());
    assert!(raw.reads.is_empty());
}

#[test]
fn materialization_counts_public_vector_and_selection_capacities() {
    let suite = suite(plus(int(1), int(2)));
    let mut worker = prepare(&suite, &[]);
    let mut selection = Vec::with_capacity(256);
    selection.push(0);
    let vector = VectorValue::from_scalar(&ScalarValue::Int(Some(3)), 1);
    worker.max_materialization_retained_bytes = size_of::<Datum>() + 128;
    let error = worker.materialize(vector, &selection).unwrap_err();
    assert!(matches!(
        error.local_error(),
        Some(LocalError::ResourceLimit(_))
    ));
    assert!(error.site().is_none());
    assert!(array_bytes::<Datum>(usize::MAX).is_err());
    assert!(worker
        .materialize(
            VectorValue::from_scalar(&ScalarValue::Int(Some(3)), 1),
            &Vec::new()
        )
        .is_err());
}
