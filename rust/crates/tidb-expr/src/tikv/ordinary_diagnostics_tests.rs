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

use std::panic::{catch_unwind, AssertUnwindSafe};

use super::super::{lower_pb_int_plus_row, lower_typed_int_plus_row, NativeDatumSource};
use super::*;
use crate::column::Column;
use crate::constant::Constant;
use crate::distsql_builtin::pb_to_expr;
use crate::expression::Expression;
use crate::scalar_function::ScalarFunction;
use tidb_ast::CiString;
use tidb_datatype::FieldTypeFlags;
use tidb_proto::tipb as db_pb;
use tidb_query_datatype::{
    expr::{Error as KernelError, EvalConfig, EvalWarnings},
    EvalType,
};
use tidb_query_expr::local::{
    compile_local, CompileLimits, ExecutionLimits, LocalCompileContext, LocalEvalState, LocalExpr,
    LocalResult,
};

fn bigint() -> FieldType {
    FieldType::new(FieldTypeCode::LongLong)
        .with_flen(20)
        .with_decimal(0)
}
fn int(value: i64) -> Expression {
    Expression::Constant(Constant::new(Datum::Int(value), bigint()))
}
fn column(index: usize) -> Expression {
    let mut value = Column::new(index as i64 + 1, bigint());
    value.index = index as i64;
    value.orig_name = format!("t.c{index}");
    Expression::Column(value)
}
fn plus(left: Expression, right: Expression) -> Expression {
    Expression::ScalarFunction(ScalarFunction::new(
        CiString::new("plus"),
        bigint(),
        vec![left, right],
    ))
}
fn prepared_with_unit(
    expression: &Expression,
    schema: &[FieldType],
    unit: u64,
) -> PreparedIntPlusRow {
    PreparedIntPlusRow::compile(
        lower_typed_int_plus_row(expression, schema, true, unit, CompileLimits::default()).unwrap(),
        ExecutionLimits::default(),
    )
    .unwrap()
}
fn prepared(expression: &Expression, schema: &[FieldType]) -> PreparedIntPlusRow {
    prepared_with_unit(expression, schema, 77)
}
fn plan(program: &PreparedIntPlusRow) -> PlusDiagnosticPlan {
    program.prepare_diagnostics(1024, 256, 16384).unwrap()
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
fn failure(result: PlusEvaluation) -> (PlusFailure, WarningEndpoints) {
    let (result, warnings) = result.into_parts();
    (result.unwrap_err(), warnings)
}
fn native_reference(expression: &Expression, input: &Chunk, row: usize) -> EvalError {
    // Isolated native oracle, never called by production adaptation or retry.
    expression
        .eval(&crate::NoColumns, input.physical_row(row))
        .unwrap_err()
}
fn wire_type() -> db_pb::FieldType {
    db_pb::FieldType {
        tp: Some(i32::from(FieldTypeCode::LongLong.mysql_type())),
        flen: Some(20),
        decimal: Some(0),
        charset: Some("binary".into()),
        collate: Some(-63),
        ..Default::default()
    }
}
fn wire_leaf(tp: db_pb::ExprType, value: i64) -> db_pb::Expr {
    let mut bytes = Vec::new();
    tidb_codec::encode_int(&mut bytes, value);
    db_pb::Expr {
        tp: Some(tp as i32),
        val: Some(bytes),
        field_type: Some(wire_type()),
        ..Default::default()
    }
}
fn wire_plus(left: db_pb::Expr, right: db_pb::Expr) -> db_pb::Expr {
    db_pb::Expr {
        tp: Some(db_pb::ExprType::ScalarFunc as i32),
        sig: Some(203),
        children: vec![left, right],
        field_type: Some(wire_type()),
        ..Default::default()
    }
}
fn pb_prepared(
    expression: &Expression,
    wire: &db_pb::Expr,
    schema: &[FieldType],
) -> PreparedIntPlusRow {
    PreparedIntPlusRow::compile(
        lower_pb_int_plus_row(expression, wire, schema, true, 88, CompileLimits::default())
            .unwrap(),
        ExecutionLimits::default(),
    )
    .unwrap()
}
fn warn(ctx: &mut EvalContext, text: &str) {
    ctx.warnings
        .append_warning(KernelError::Eval(text.into(), 1265));
}

#[derive(Clone, Copy)]
enum Fault {
    Evaluation,
    Resource,
    Other,
    Panic,
    Reset,
}
struct Raw {
    schema: Vec<tipb::FieldType>,
    rows: Vec<Vec<Datum>>,
    reads: Vec<(usize, InputRow)>,
    fault: Option<(usize, Fault)>,
    warnings: bool,
}
impl Raw {
    fn new(program: &PreparedIntPlusRow, rows: Vec<Vec<Datum>>) -> Self {
        Self {
            schema: program.spec.core.schema.to_vec(),
            rows,
            reads: Vec::new(),
            fault: None,
            warnings: false,
        }
    }
}
impl NativeDatumSource for Raw {
    fn schema(&self) -> &[tipb::FieldType] {
        &self.schema
    }
    fn read_datum(
        &mut self,
        ctx: &mut EvalContext,
        slot: usize,
        row: InputRow,
        expected: &tipb::FieldType,
    ) -> LocalResult<Datum> {
        assert_eq!(self.schema.get(slot), Some(expected));
        self.reads.push((slot, row));
        if self.warnings {
            warn(
                ctx,
                &format!("read:{}:{}:{slot}", row.occurrence, row.input_row),
            );
        }
        if let Some((at, fault)) = self.fault {
            if at == slot {
                return match fault {
                    Fault::Evaluation => Err(LocalError::Evaluation(
                        KernelError::overflow("BIGINT", "raw input, not PLUS").into(),
                    )),
                    Fault::Resource => {
                        Err(LocalError::ResourceLimit("input resource cause".into()))
                    }
                    Fault::Other => Err(LocalError::Evaluation(
                        KernelError::InvalidDataType("1690 BIGINT overflow text".into()).into(),
                    )),
                    Fault::Panic => panic!("primary input panic"),
                    Fault::Reset => {
                        ctx.warnings.warning_cnt = 0;
                        ctx.warnings.warnings.clear();
                        Err(LocalError::BindingContract(
                            "synthetic receiver reset".into(),
                        ))
                    }
                };
            }
        }
        Ok(self.rows[row.input_row][slot].clone())
    }
}

#[test]
fn reported_plus_joins_exact_inner_call_and_row() {
    let schema = vec![bigint(); 2];
    let expression = plus(plus(column(0), int(1)), column(1));
    for (left, right, ordinal) in [(i64::MAX, 0, 1), (i64::MAX - 1, 1, 0)] {
        let input = chunk(
            &schema,
            &[
                vec![Datum::Int(0); 2],
                vec![Datum::Int(5); 2],
                vec![Datum::Int(left), Datum::Int(right)],
            ],
        );
        let mut program = prepared(&expression, &schema);
        let diagnostics = plan(&program);
        let (error, endpoints) = failure(program.eval_selected_reported(
            &mut EvalContext::default(),
            &input,
            &schema,
            &[0, 2, 2],
            &diagnostics,
        ));
        let joined = error.joined_site().unwrap();
        assert_eq!(joined.node_ordinal(), ordinal);
        assert_eq!(
            joined.row(),
            InputRow {
                occurrence: 1,
                input_row: 2
            }
        );
        assert_eq!(joined.call().unwrap().source().unit(), 77);
        assert_eq!(joined.call().unwrap().source().node(), ordinal as u64);
        assert_eq!(error.reported().unwrap().stage(), LocalFailureStage::Kernel);
        assert_eq!(error.reported().unwrap().sql_error_code(), Some(1690));
        assert_eq!(
            error.native_error(),
            Some(&native_reference(&expression, &input, 2))
        );
        assert_eq!(endpoints.before, endpoints.after);
        let mut raw = Raw::new(
            &program,
            vec![
                vec![Datum::Int(left), Datum::Int(right)],
                vec![Datum::Int(0); 2],
            ],
        );
        let (_, _) = failure(program.eval_test_reported_native(
            &mut EvalContext::default(),
            2,
            &[0, 1],
            &diagnostics,
            &mut raw,
        ));
        assert!(raw.reads.iter().all(|(_, row)| row.occurrence == 0));
        assert_eq!(raw.reads.len(), if ordinal == 1 { 1 } else { 2 });
    }
}

#[test]
fn reported_input_is_a_leaf_not_an_enclosing_plus() {
    let schema = vec![bigint(); 3];
    let mut program = prepared(&plus(column(0), plus(column(1), column(2))), &schema);
    let diagnostics = plan(&program);
    for fault in [Fault::Evaluation, Fault::Resource] {
        let mut raw = Raw::new(&program, vec![vec![Datum::Int(1); 3]]);
        raw.fault = Some((1, fault));
        let (error, _) = failure(program.eval_test_reported_native(
            &mut EvalContext::default(),
            1,
            &[0],
            &diagnostics,
            &mut raw,
        ));
        assert_eq!(error.reported().unwrap().stage(), LocalFailureStage::Input);
        assert_eq!(error.joined_site().unwrap().input_slot(), Some(1));
        assert_eq!(error.joined_site().unwrap().node_ordinal(), 3);
        assert!(error.joined_site().unwrap().call().is_none());
        assert!(error.native_error().is_none());
        match fault {
            Fault::Evaluation => assert_eq!(error.reported().unwrap().sql_error_code(), Some(1690)),
            _ => assert!(matches!(
                error.reported().unwrap().error(),
                LocalError::ResourceLimit(_)
            )),
        }
        assert_eq!(raw.reads.len(), 2);
    }
    let mut raw = Raw::new(
        &program,
        vec![vec![Datum::Int(1), Datum::UInt(0), Datum::Int(1)]],
    );
    let (error, _) = failure(program.eval_test_reported_native(
        &mut EvalContext::default(),
        1,
        &[0],
        &diagnostics,
        &mut raw,
    ));
    assert_eq!(error.reported().unwrap().stage(), LocalFailureStage::Input);
    assert!(matches!(
        error.reported().unwrap().error(),
        LocalError::BindingContract(_)
    ));
    assert!(error.native_error().is_none());
    let mut repeated = prepared(&plus(column(0), column(0)), &[bigint()]);
    let diagnostics = plan(&repeated);
    let mut raw = Raw::new(&repeated, vec![vec![Datum::Int(1); 2]]);
    raw.fault = Some((1, Fault::Evaluation));
    let (error, _) = failure(repeated.eval_test_reported_native(
        &mut EvalContext::default(),
        1,
        &[0],
        &diagnostics,
        &mut raw,
    ));
    assert_eq!(error.joined_site().unwrap().node_ordinal(), 2);
}

#[test]
fn reported_plus_uses_typed_code_only_after_join() {
    let expression = plus(column(0), int(1));
    let schema = vec![bigint()];
    let mut program = prepared(&expression, &schema);
    let diagnostics = plan(&program);
    for fault in [Fault::Evaluation, Fault::Other, Fault::Resource] {
        let mut raw = Raw::new(&program, vec![vec![Datum::Int(1)]]);
        raw.fault = Some((0, fault));
        let (error, _) = failure(program.eval_test_reported_native(
            &mut EvalContext::default(),
            1,
            &[0],
            &diagnostics,
            &mut raw,
        ));
        assert!(error.native_error().is_none());
        if matches!(fault, Fault::Other) {
            assert_eq!(error.reported().unwrap().sql_error_code(), Some(10000));
        }
    }
    let other = prepared_with_unit(&expression, &schema, 99);
    let other_plan = plan(&other);
    for bad_join in 0..3 {
        let mut raw = Raw::new(&program, vec![vec![Datum::Int(i64::MAX)]]);
        let report = program
            .program
            .eval_with_bindings_reported(
                &mut program.state,
                &mut EvalContext::default(),
                1,
                &[0],
                &mut PlusInputs(&mut raw),
            )
            .unwrap_err();
        // Private counterexample fixtures, NOT a public arbitrary-report API.
        let error = match bad_join {
            0 => diagnostics.failure(report, 0, &[0]),
            1 => diagnostics.failure(report, 1, &[1]),
            _ => other_plan.failure(report, 1, &[0]),
        };
        assert!(error.joined_site().is_none());
        assert!(error.native_error().is_none());
        assert_eq!(error.reported().unwrap().sql_error_code(), Some(1690));
    }
    let mut graph = program.spec.core.expr.clone(); // three-node fixture only
    let LocalExpr::Call { function, .. } = &mut graph else {
        panic!("call")
    };
    *function = FunctionRef::TiPb(tipb::ScalarFuncSig::PlusIntSignedSigned);
    let mut eager = compile_local(
        &graph,
        &program.spec.core.schema,
        LocalCompileContext::default(),
    )
    .unwrap();
    let mut raw = Raw::new(&program, vec![vec![Datum::Int(i64::MAX)]]);
    let report = eager
        .eval_with_bindings_reported(
            &mut LocalEvalState::default(),
            &mut EvalContext::default(),
            1,
            &[0],
            &mut PlusInputs(&mut raw),
        )
        .unwrap_err();
    assert_eq!(report.stage(), LocalFailureStage::Unattributed);
    assert_eq!(report.sql_error_code(), Some(1690));
    assert!(diagnostics
        .failure(report, 1, &[0])
        .native_error()
        .is_none());
    let mut invalid = prepared(&expression, &schema);
    Arc::get_mut(&mut invalid.spec).unwrap().outputs[0]
        .as_mut()
        .unwrap()
        .sql_type
        .add_flags(FieldTypeFlags::UNSIGNED);
    assert!(invalid.prepare_diagnostics(10, 10, 100).is_err());
}

#[test]
fn pb_plus_keeps_own_rendering_and_operand_fallback_distinct() {
    let schema = vec![bigint(); 2];
    let wire = wire_plus(
        wire_plus(
            wire_leaf(db_pb::ExprType::ColumnRef, 0),
            wire_leaf(db_pb::ExprType::Int64, 1),
        ),
        wire_leaf(db_pb::ExprType::ColumnRef, 1),
    );
    for (left, right, renamed) in [
        (i64::MAX, 0, false),
        (i64::MAX - 1, 1, false),
        (i64::MAX - 1, 1, true),
    ] {
        let mut expression = pb_to_expr(&wire, &schema).unwrap();
        if renamed {
            let Expression::ScalarFunction(root) = &mut expression else {
                panic!("root")
            };
            let Expression::ScalarFunction(child) = &mut root.args[0] else {
                panic!("child")
            };
            child.func_name = CiString::new("MiNuS");
        }
        let mut program = pb_prepared(&expression, &wire, &schema);
        let diagnostics = plan(&program);
        assert!(diagnostics.facts[1].overflow_bytes.is_some());
        assert_eq!(diagnostics.facts[1].operand_bytes.is_some(), renamed);
        let input = chunk(&schema, &[vec![Datum::Int(left), Datum::Int(right)]]);
        let (error, _) = failure(program.eval_selected_reported(
            &mut EvalContext::default(),
            &input,
            &schema,
            &[0],
            &diagnostics,
        ));
        assert_eq!(
            error.native_error(),
            Some(&native_reference(&expression, &input, 0))
        );
        if left != i64::MAX && !renamed {
            assert_eq!(error.native_error(), Some(&EvalError::IntOverflow));
        } else {
            assert!(matches!(
                error.native_error(),
                Some(EvalError::DataOutOfRange { .. })
            ));
        }
        if renamed {
            let SourceShape::Call { display_name, .. } = &program.spec.shapes[1] else {
                panic!("shape")
            };
            assert_eq!(display_name.original(), "MiNuS");
            let Some(EvalError::DataOutOfRange { expression, .. }) = error.native_error() else {
                panic!("rendered")
            };
            assert!(expression.contains(" - "));
        }
    }
}

#[test]
fn plus_diagnostic_plan_is_bound_to_its_prepared_spec() {
    let expression = plus(column(0), int(1));
    let schema = vec![bigint()];
    let first = prepared(&expression, &schema);
    let first_plan = plan(&first);
    let mut second = prepared(&expression, &schema); // SAME owner-supplied IDs
    assert_eq!(
        first.spec.profiles.call_sites(),
        second.spec.profiles.call_sites()
    );
    let mut raw = Raw::new(&second, vec![vec![Datum::Int(1)]]);
    raw.fault = Some((0, Fault::Panic));
    let mut ctx = EvalContext::default();
    warn(&mut ctx, "prior");
    let (error, endpoints) =
        failure(second.eval_test_reported_native(&mut ctx, 1, &[0], &first_plan, &mut raw));
    assert_eq!(error.caller_stage(), Some(CallerFailureStage::Preflight));
    assert!(error.reported().is_none());
    assert!(raw.reads.is_empty());
    assert_eq!(endpoints.before, endpoints.after);
    let second_plan = plan(&second);
    raw.fault = None;
    assert_eq!(
        second
            .eval_test_reported_native(&mut ctx, 1, &[0], &second_plan, &mut raw)
            .outcome()
            .unwrap(),
        &[Datum::Int(2)]
    );
}

#[test]
fn plus_rendering_is_iterative_and_byte_bounded() {
    let mut col = Column::new(-9, bigint());
    col.index = 0;
    col.orig_name = "表.列".into();
    let expression = plus(Expression::Column(col), int(1));
    let program = prepared(&expression, &[bigint()]);
    let bytes = "(表.列 + 1)".len();
    assert!(program.prepare_diagnostics(3, 2, bytes).is_ok());
    for (nodes, depth, bytes) in [(2, 2, bytes), (3, 1, bytes), (3, 2, bytes - 1)] {
        assert!(matches!(
            program.prepare_diagnostics(nodes, depth, bytes),
            Err(SeedError::Local(LocalError::ResourceLimit(_)))
        ));
    }
    for expression in [plus(int(i64::MIN), int(-1)), {
        let mut col = Column::new(-9, bigint());
        col.index = 0;
        plus(Expression::Column(col), int(1))
    }] {
        let schema = if matches!(&expression, Expression::ScalarFunction(f) if matches!(&f.args[0], Expression::Column(_)))
        {
            vec![bigint()]
        } else {
            vec![]
        };
        let input = chunk(
            &schema,
            &[if schema.is_empty() {
                vec![]
            } else {
                vec![Datum::Int(i64::MAX)]
            }],
        );
        let mut program = prepared(&expression, &schema);
        let diagnostics = plan(&program);
        let (error, _) = failure(program.eval_selected_reported(
            &mut EvalContext::default(),
            &input,
            &schema,
            &[0],
            &diagnostics,
        ));
        assert_eq!(
            error.native_error(),
            Some(&native_reference(&expression, &input, 0))
        );
    }
    let mut deep = plus(column(0), int(1));
    for _ in 0..63 {
        deep = plus(deep, int(0));
    }
    let mut program = prepared(&deep, &[bigint()]);
    let diagnostics = program.prepare_diagnostics(129, 65, 4096).unwrap();
    assert_eq!(diagnostics.facts.len(), 129);
    assert!(program.prepare_diagnostics(129, 64, 4096).is_err());
    let input = chunk(&[bigint()], &[vec![Datum::Int(i64::MAX)]]);
    let (error, _) = failure(program.eval_selected_reported(
        &mut EvalContext::default(),
        &input,
        &[bigint()],
        &[0],
        &diagnostics,
    ));
    assert_eq!(error.joined_site().unwrap().node_ordinal(), 63);
    assert_eq!(
        error.native_error(),
        Some(&EvalError::DataOutOfRange {
            value: "BIGINT",
            expression: "(t.c0 + 1)".into()
        })
    );
    let mut diagnostics = diagnostics;
    diagnostics.max_rendered_bytes = 0; // private corruption fixture: adaptation must decline, not fake native fallback
    let (error, _) = failure(program.eval_selected_reported(
        &mut EvalContext::default(),
        &input,
        &[bigint()],
        &[0],
        &diagnostics,
    ));
    assert!(error.native_error().is_none());
    assert_eq!(error.reported().unwrap().sql_error_code(), Some(1690));
}

#[test]
fn reported_endpoints_include_all_normal_exit_paths() {
    let schema = vec![bigint(); 2];
    let mut program = prepared(&plus(column(0), column(1)), &schema);
    let diagnostics = plan(&program);
    let input = chunk(&schema, &[vec![Datum::Int(1), Datum::Int(2)]]);
    let mut ctx = EvalContext::default();
    warn(&mut ctx, "prior");
    let expected = WarningEndpoint::capture(&ctx);
    for selection in [&[][..], &[0][..]] {
        let result =
            program.eval_selected_reported(&mut ctx, &input, &schema, selection, &diagnostics);
        assert!(result.outcome().is_ok());
        assert_eq!(
            result.warnings(),
            WarningEndpoints {
                before: expected,
                after: expected
            }
        );
    }
    for (schema, selection) in [(&schema[..1], &[][..]), (&schema[..], &[1][..])] {
        let (error, endpoints) = failure(program.eval_selected_reported(
            &mut ctx,
            &input,
            schema,
            selection,
            &diagnostics,
        ));
        assert_eq!(error.caller_stage(), Some(CallerFailureStage::Preflight));
        assert_eq!(
            endpoints,
            WarningEndpoints {
                before: expected,
                after: expected
            }
        );
    }
    let mut raw = Raw::new(&program, vec![vec![Datum::Int(1), Datum::Int(2)]]);
    raw.warnings = true;
    raw.fault = Some((1, Fault::Evaluation));
    let (error, endpoints) =
        failure(program.eval_test_reported_native(&mut ctx, 1, &[0], &diagnostics, &mut raw));
    assert_eq!(endpoints.before, expected);
    assert_eq!(endpoints.after, WarningEndpoint::capture(&ctx));
    assert_eq!(endpoints.after.warning_cnt, expected.warning_cnt + 2);
    assert_eq!(error.reported().unwrap().stage(), LocalFailureStage::Input);
    raw.schema[0].set_flen(99);
    raw.reads.clear();
    let before = WarningEndpoint::capture(&ctx);
    let (error, endpoints) =
        failure(program.eval_test_reported_native(&mut ctx, 1, &[], &diagnostics, &mut raw));
    assert!(error.reported().unwrap().site().is_none());
    assert_eq!(
        endpoints,
        WarningEndpoints {
            before,
            after: before
        }
    );
    assert!(raw.reads.is_empty());
    // Defensive materialization is a caller failure, not a synthetic C report.
    // The same finish sampler is used after every normal production return.
    let before = WarningEndpoint::capture(&ctx);
    let outcome = program.finish_reported(
        &diagnostics,
        1,
        &[0],
        Ok(VectorValue::with_capacity(0, EvalType::Int)),
    );
    let (error, endpoints) = failure(PlusEvaluation::finish(before, &ctx, outcome));
    assert_eq!(
        error.caller_stage(),
        Some(CallerFailureStage::Materialization)
    );
    assert!(error.reported().is_none());
    assert_eq!(
        endpoints,
        WarningEndpoints {
            before,
            after: before
        }
    );
}

#[test]
fn reported_endpoints_use_live_receiver_not_configured_cap() {
    let schema = vec![bigint(); 2];
    let mut program = prepared(&plus(column(0), column(1)), &schema);
    let diagnostics = plan(&program);
    for case in 0..3 {
        let mut config = EvalConfig::default();
        if case == 1 {
            config.max_warning_cnt = 1;
        }
        let mut ctx = EvalContext::new(Arc::new(config));
        if case == 0 {
            ctx.warnings = EvalWarnings::default();
        }
        if case == 1 {
            warn(&mut ctx, "prior capped prefix");
            ctx.cfg = Arc::new(EvalConfig::default()); // live receiver is still cap1
        }
        if case == 2 {
            let mut replacement = EvalConfig::default();
            replacement.max_warning_cnt = 0;
            ctx.cfg = Arc::new(replacement); // live receiver has its old nonzero cap
        }
        let prefix = ctx.warnings.warnings.clone(); // observation in tests, not in production receipts
        let before = WarningEndpoint::capture(&ctx);
        let mut raw = Raw::new(&program, vec![vec![Datum::Int(1), Datum::Int(2)]]);
        raw.warnings = true;
        let result = program.eval_test_reported_native(&mut ctx, 1, &[0], &diagnostics, &mut raw);
        assert_eq!(result.outcome().unwrap(), &[Datum::Int(3)]);
        assert_eq!(result.warnings().before, before);
        assert_eq!(result.warnings().after.warning_cnt, before.warning_cnt + 2);
        assert_eq!(result.warnings().after.stored_len, [0, 1, 2][case]);
        assert_eq!(&ctx.warnings.warnings[..prefix.len()], prefix.as_slice());
    }
}

#[test]
fn reported_endpoints_do_not_repair_nonmonotone_or_panic_state() {
    let schema = vec![bigint(); 2];
    let mut program = prepared(&plus(column(0), column(1)), &schema);
    let diagnostics = plan(&program);
    let mut ctx = EvalContext::default();
    warn(&mut ctx, "prior");
    ctx.warnings.warning_cnt = usize::MAX;
    let mut raw = Raw::new(&program, vec![vec![Datum::Int(1), Datum::Int(2)]]);
    raw.fault = Some((0, Fault::Reset));
    let (_, endpoints) =
        failure(program.eval_test_reported_native(&mut ctx, 1, &[0], &diagnostics, &mut raw));
    assert_eq!(endpoints.before.warning_cnt, usize::MAX);
    assert_eq!(
        endpoints.after,
        WarningEndpoint {
            warning_cnt: 0,
            stored_len: 0
        }
    );
    raw.fault = Some((0, Fault::Panic));
    assert!(
        catch_unwind(AssertUnwindSafe(|| program.eval_test_reported_native(
            &mut ctx,
            1,
            &[0],
            &diagnostics,
            &mut raw
        )))
        .is_err()
    );
    // Panic produced no receipt/after endpoint and no stored last-failure state.
    raw.fault = None;
    assert_eq!(
        program
            .eval_test_reported_native(&mut ctx, 1, &[0], &diagnostics, &mut raw)
            .outcome()
            .unwrap(),
        &[Datum::Int(3)]
    );
    assert!(program
        .eval_test_reported_native(&mut ctx, 1, &[], &diagnostics, &mut raw)
        .outcome()
        .unwrap()
        .is_empty());
    raw.fault = Some((1, Fault::Evaluation));
    let (error, _) =
        failure(program.eval_test_reported_native(&mut ctx, 1, &[0], &diagnostics, &mut raw));
    assert_eq!(error.joined_site().unwrap().input_slot(), Some(1));
}

#[test]
fn pure_overflow_view_preserves_raw_error_and_legacy_paths() {
    let expression = plus(column(0), int(1));
    let schema = vec![bigint()];
    let input = chunk(&schema, &[vec![Datum::Int(i64::MAX)]]);
    let mut program = prepared(&expression, &schema);
    let diagnostics = plan(&program);
    let (error, _) = failure(program.eval_selected_reported(
        &mut EvalContext::default(),
        &input,
        &schema,
        &[0],
        &diagnostics,
    ));
    assert_eq!(
        error.native_error(),
        Some(&native_reference(&expression, &input, 0))
    );
    let address = match error.reported().unwrap().error() {
        LocalError::Evaluation(error) => error.0.as_ref() as *const _ as usize,
        _ => panic!("evaluation"),
    };
    let SeedError::Local(LocalError::Evaluation(raw)) = error.into_raw() else {
        panic!("original cause")
    };
    assert_eq!(raw.0.as_ref() as *const _ as usize, address);
    let raw_text = raw.to_string(); // test oracle only, never diagnostic classification
    let SeedError::Local(LocalError::Evaluation(old)) = program
        .eval_one(&mut EvalContext::default(), &input, &schema, 0)
        .unwrap_err()
    else {
        panic!("raw legacy route")
    };
    assert_eq!(old.to_string(), raw_text);
    assert_eq!(
        arithmetic_symbol(binary_op_for_name("plus").unwrap()),
        Some("+")
    );
    assert_eq!(
        arithmetic_symbol(binary_op_for_name("intdiv").unwrap()),
        Some("DIV")
    );
    assert_eq!(binary_op_for_name("sig_plusint"), None);
}
