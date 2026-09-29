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

use super::*;
use crate::column::{Column, CorrelatedColumn};
use crate::constant::{Constant, ParamMarker};
use crate::distsql_builtin::pb_to_expr;
use crate::rewriter::preparation::StructuralLimits;
use crate::rewriter::{rewrite_expr_resolved, rewrite_expr_structural, ColumnResolver};
use crate::scalar_function::{PbBuiltin, ScalarFunction};
use crate::Columns;
use tidb_ast::{CiString, Expr, QueryStmt, SelectField, Stmt};
use tidb_datatype::{BinaryLiteral, FieldTypeCode, FieldTypeFlags, SessionTimeZone};
use tidb_query_datatype::codec::data_type::ScalarValue;
use tidb_query_datatype::expr::Error as KernelError;
use tidb_query_expr::local::{compile_local, LocalFunctionId};

fn bigint() -> FieldType {
    FieldType::new(FieldTypeCode::LongLong)
        .with_flen(20)
        .with_decimal(0)
}
fn literal(value: Datum) -> Expression {
    Expression::Constant(Constant::new(value, bigint()))
}
fn int(value: i64) -> Expression {
    literal(Datum::Int(value))
}
fn column(index: usize) -> Expression {
    let mut column = Column::new(index as i64 + 100, bigint());
    column.index = index as i64;
    column.id = index as i64 + 10;
    column.orig_name = format!("t.c{index}");
    Expression::Column(column)
}
fn call(name: &str, args: Vec<Expression>) -> Expression {
    Expression::ScalarFunction(ScalarFunction::new(CiString::new(name), bigint(), args))
}
fn plus(left: Expression, right: Expression) -> Expression {
    call("plus", vec![left, right])
}
fn function_mut(expression: &mut Expression) -> &mut ScalarFunction {
    let Expression::ScalarFunction(function) = expression else {
        panic!("call")
    };
    function
}
fn lower(root: &Expression, schema: &[FieldType]) -> Arc<LoweredIntPlusRow> {
    lower_typed_int_plus_row(root, schema, true, 77, CompileLimits::default()).unwrap()
}
fn prepared(spec: Arc<LoweredIntPlusRow>) -> PreparedIntPlusRow {
    PreparedIntPlusRow::compile(spec, ExecutionLimits::default()).unwrap()
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
        sig: Some(db_pb::ScalarFuncSig::PlusInt as i32),
        field_type: Some(wire_type()),
        children: vec![left, right],
        ..Default::default()
    }
}
fn pb_lower(
    root: &Expression,
    wire: &db_pb::Expr,
    schema: &[FieldType],
) -> SeedResult<Arc<LoweredIntPlusRow>> {
    lower_pb_int_plus_row(root, wire, schema, true, 88, CompileLimits::default())
}

// Deliberately supplies RAW Datums. An already-erased VectorValue::Int cannot
// distinguish UInt and therefore cannot test the native bridge boundary.
struct Raw {
    schema: Vec<tipb::FieldType>,
    rows: Vec<Vec<Datum>>,
    reads: Vec<(usize, InputRow)>,
    poison: Option<usize>,
    failure: Option<usize>,
    warnings: bool,
}
impl Raw {
    fn new(spec: &LoweredIntPlusRow, rows: Vec<Vec<Datum>>) -> Self {
        Self {
            schema: spec.core.schema.to_vec(),
            rows,
            reads: Vec::new(),
            poison: None,
            failure: None,
            warnings: false,
        }
    }
}
fn warn(ctx: &mut EvalContext, text: &str) {
    ctx.warnings
        .append_warning(KernelError::overflow("BIGINT", text));
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
        assert_ne!(self.poison, Some(slot), "undemanded raw slot was read");
        if self.warnings {
            warn(
                ctx,
                &format!("read:{}:{}:{slot}", row.occurrence, row.input_row),
            );
        }
        if self.failure == Some(slot) {
            return Err(LocalError::Evaluation(
                KernelError::overflow("BIGINT", "raw-primary").into(),
            ));
        }
        Ok(self.rows[row.input_row][slot].clone())
    }
}

#[test]
fn typed_plus_uses_exact_203_and_all_node_preorder() {
    let schema = vec![bigint(); 3];
    let expression = plus(column(0), plus(column(1), column(2)));
    let spec = lower(&expression, &schema);
    assert_eq!(spec.profiles.consumer(), OrdinaryProfile::TypedRow);
    assert_eq!(spec.profiles.node_count(), 5);
    let sites = spec.profiles.call_sites();
    assert_eq!(
        sites
            .iter()
            .map(OrdinaryCallSite::ordinal)
            .collect::<Vec<_>>(),
        vec![0, 2]
    );
    for site in sites {
        assert_eq!(site.source().unit(), 77);
        assert_eq!(site.source().node(), site.ordinal() as u64);
        assert_eq!(site.original_pb_signature(), None);
    }
    assert_eq!(&*spec.binding_nodes, &[1, 3, 4]);
    for ordinal in [0, 2] {
        let SourceNode::Call { selected } = &spec.core.nodes[ordinal].source else {
            panic!("call metadata")
        };
        assert_eq!(*selected, FunctionRef::TiPb(tipb::ScalarFuncSig::PlusInt));
    }
    let SourceShape::Call { children, .. } = &spec.shapes[0] else {
        panic!("source shape")
    };
    assert_eq!(*children, [1, 2]);
    assert!(compile_local(
        &spec.core.expr,
        &spec.core.schema,
        LocalCompileContext::default()
    )
    .is_err());
    let input = chunk(
        &schema,
        &[vec![Datum::Int(10), Datum::Int(20), Datum::Int(3)]],
    );
    assert_eq!(
        prepared(spec)
            .eval_one(&mut EvalContext::default(), &input, &schema, 0)
            .unwrap(),
        Datum::Int(33)
    );
    assert!(
        lower::lower_int_control_seed(&expression, &schema, true, CompileLimits::default())
            .is_err()
    );
}

#[test]
fn pb_plus_requires_actual_ingestion_and_original_tree() {
    let schema = vec![bigint(); 2];
    let wire = wire_plus(
        wire_leaf(db_pb::ExprType::ColumnRef, 0),
        wire_leaf(db_pb::ExprType::ColumnRef, 1),
    );
    let original = pb_to_expr(&wire, &schema).unwrap();
    let spec = pb_lower(&original, &wire, &schema).unwrap();
    assert_eq!(spec.profiles.consumer(), OrdinaryProfile::PbRow);
    assert_eq!(
        spec.profiles.call_sites()[0].original_pb_signature(),
        Some(203)
    );
    assert_eq!(spec.profiles.call_sites()[0].source().unit(), 88);
    assert!(
        lower_typed_int_plus_row(&original, &schema, true, 88, CompileLimits::default()).is_err()
    );
    let mut renamed = original.clone();
    function_mut(&mut renamed).func_name = CiString::new("Unrelated_Display_Label");
    let renamed = pb_lower(&renamed, &wire, &schema).unwrap();
    let SourceShape::Call { display_name, .. } = &renamed.shapes[0] else {
        panic!("source")
    };
    assert_eq!(display_name.original(), "Unrelated_Display_Label");
    let input = chunk(&schema, &[vec![Datum::Int(4), Datum::Int(7)]]);
    assert_eq!(
        prepared(renamed)
            .eval_one(&mut EvalContext::default(), &input, &schema, 0)
            .unwrap(),
        Datum::Int(11)
    );
    for mutation in 0..6 {
        let mut changed = original.clone();
        let function = function_mut(&mut changed);
        match mutation {
            0 => function.pb_origin = None,
            1 => function.args.swap(0, 1), // both still have genuine node-local origins
            2 => function.ret_type.as_mut().unwrap().set_flen(17),
            3 => function.args[0] = column(0),
            4 => {
                let Expression::Column(column) = &mut function.args[0] else {
                    panic!("column")
                };
                column.index = 1;
            }
            _ => function.args.push(int(9)),
        }
        assert!(pb_lower(&changed, &wire, &schema).is_err());
    }
    let fabricated = Expression::ScalarFunction(ScalarFunction::from_pb(
        PbBuiltin::new(db_pb::ScalarFuncSig::PlusInt).unwrap(),
        bigint(),
        vec![column(0), column(1)],
    ));
    assert!(pb_lower(&fabricated, &wire, &schema).is_err());
    assert!(pb_lower(&plus(column(0), column(1)), &wire, &schema).is_err());
    let mut changed_wire = wire.clone();
    changed_wire.children.swap(0, 1);
    assert!(pb_lower(&original, &changed_wire, &schema).is_err());
    let mut trailing = wire_plus(
        wire_leaf(db_pb::ExprType::Int64, 1),
        wire_leaf(db_pb::ExprType::Int64, 2),
    );
    trailing.children[0].val.as_mut().unwrap().push(0);
    let decoded = pb_to_expr(&trailing, &[]).unwrap();
    assert!(pb_lower(&decoded, &trailing, &[]).is_err());
    let mut changed = pb_to_expr(
        &wire_plus(
            wire_leaf(db_pb::ExprType::Int64, 1),
            wire_leaf(db_pb::ExprType::Int64, 2),
        ),
        &[],
    )
    .unwrap();
    let Expression::Constant(value) = &mut function_mut(&mut changed).args[0] else {
        panic!("constant")
    };
    value.value = Datum::Int(9);
    assert!(pb_lower(
        &changed,
        &wire_plus(
            wire_leaf(db_pb::ExprType::Int64, 1),
            wire_leaf(db_pb::ExprType::Int64, 2)
        ),
        &[]
    )
    .is_err());
    let nested_wire = wire_plus(
        wire.children[0].clone(),
        wire_plus(
            wire_leaf(db_pb::ExprType::Int64, 2),
            wire.children[1].clone(),
        ),
    );
    let nested = pb_to_expr(&nested_wire, &schema).unwrap();
    let nested = pb_lower(&nested, &nested_wire, &schema).unwrap();
    assert_eq!(
        nested
            .profiles
            .call_sites()
            .iter()
            .map(OrdinaryCallSite::ordinal)
            .collect::<Vec<_>>(),
        vec![0, 2]
    );
    assert_eq!(
        prepared(nested)
            .eval_one(&mut EvalContext::default(), &input, &schema, 0)
            .unwrap(),
        Datum::Int(13)
    );
    drop(original);
    drop(wire);
    assert_eq!(
        prepared(spec)
            .eval_one(&mut EvalContext::default(), &input, &schema, 0)
            .unwrap(),
        Datum::Int(11)
    );
}

#[test]
fn pb_null_literal_is_not_retagged() {
    let null = db_pb::Expr {
        tp: Some(db_pb::ExprType::Null as i32),
        field_type: Some(wire_type()),
        ..Default::default()
    };
    let wire = wire_plus(null, wire_leaf(db_pb::ExprType::Int64, 2));
    let expression = pb_to_expr(&wire, &[]).unwrap();
    let Expression::ScalarFunction(function) = &expression else {
        panic!("call")
    };
    assert_eq!(
        function.args[0].static_type().unwrap().code(),
        FieldTypeCode::Null
    );
    assert!(pb_lower(&expression, &wire, &[]).is_err());
    let typed = lower(&plus(literal(Datum::Null), int(2)), &[]);
    let LocalExpr::Call { args, .. } = &typed.core.expr else {
        panic!("call")
    };
    assert!(matches!(
        &args[0],
        LocalExpr::Constant {
            value: ScalarValue::Int(None),
            literal_kind: LiteralKind::Typed,
            ..
        }
    ));
    assert_eq!(
        prepared(typed)
            .eval_one(&mut EvalContext::default(), &chunk(&[], &[vec![]]), &[], 0)
            .unwrap(),
        Datum::Null
    );
    let schema = vec![bigint(); 2];
    let wire = wire_plus(
        wire_leaf(db_pb::ExprType::ColumnRef, 0),
        wire_leaf(db_pb::ExprType::ColumnRef, 1),
    );
    let expression = pb_to_expr(&wire, &schema).unwrap();
    let spec = pb_lower(&expression, &wire, &schema).unwrap();
    let mut raw = Raw::new(&spec, vec![vec![Datum::Null, Datum::UInt(u64::MAX)]]);
    raw.poison = Some(1);
    assert_eq!(
        prepared(spec)
            .eval_test_native(&mut EvalContext::default(), 1, &[0], &mut raw)
            .unwrap(),
        vec![Datum::Null]
    );
    assert_eq!(raw.reads.len(), 1);
}

#[test]
fn plus_native_kind_is_checked_before_bridge() {
    let schema = vec![bigint(); 2];
    let spec = lower(&plus(column(0), column(1)), &schema);
    assert!(to_scalar(&Datum::UInt(0), EvalType::Int).is_ok()); // the carrier alone is insufficient
    for value in [
        Datum::UInt(0),
        Datum::UInt(u64::MAX),
        Datum::Bytes(vec![1]),
        Datum::BinaryLiteral(BinaryLiteral::from(vec![1])),
        Datum::new_string("1"),
        Datum::Real(1.0),
    ] {
        let mut raw = Raw::new(&spec, vec![vec![Datum::Int(1), value]]);
        assert!(matches!(
            prepared(Arc::clone(&spec)).eval_test_native(
                &mut EvalContext::default(),
                1,
                &[0],
                &mut raw
            ),
            Err(SeedError::Local(LocalError::BindingContract(_)))
        ));
        assert_eq!(raw.reads.len(), 2);
    }
    let mut raw = Raw::new(&spec, vec![vec![Datum::Null, Datum::UInt(0)]]);
    raw.poison = Some(1);
    assert_eq!(
        prepared(Arc::clone(&spec))
            .eval_test_native(&mut EvalContext::default(), 1, &[0], &mut raw)
            .unwrap(),
        vec![Datum::Null]
    );
    assert_eq!(raw.reads.len(), 1);
    // Static unsupported children are never excused by left NULL.
    assert!(lower_typed_int_plus_row(
        &plus(literal(Datum::Null), literal(Datum::UInt(0))),
        &[],
        true,
        1,
        CompileLimits::default()
    )
    .is_err());
}

#[test]
fn plus_row_demand_preserves_error_prefix() {
    let schema = vec![bigint(); 3];
    let spec = lower(&plus(column(0), plus(column(1), column(2))), &schema);
    for fail_slot in [Some(0), Some(1), Some(2), None] {
        let mut raw = Raw::new(
            &spec,
            vec![
                vec![Datum::Int(0), Datum::Int(i64::MAX), Datum::Int(1)],
                vec![Datum::Int(1); 3],
            ],
        );
        raw.failure = fail_slot;
        raw.warnings = true;
        let mut ctx = EvalContext::default();
        warn(&mut ctx, "prior");
        let error = prepared(Arc::clone(&spec))
            .eval_test_native(&mut ctx, 2, &[0, 1], &mut raw)
            .unwrap_err();
        assert!(matches!(
            &error,
            SeedError::Local(LocalError::Evaluation(_))
        ));
        let count = fail_slot.map_or(3, |slot| slot + 1);
        assert_eq!(
            raw.reads
                .iter()
                .map(|(slot, row)| (*slot, row.occurrence, row.input_row))
                .collect::<Vec<_>>(),
            (0..count).map(|slot| (slot, 0, 0)).collect::<Vec<_>>()
        );
        assert_eq!(ctx.warnings.warning_cnt, count + 1);
        assert!(ctx.warnings.warnings[0].get_msg().contains("prior"));
        for (slot, warning) in ctx.warnings.warnings.iter().skip(1).enumerate() {
            assert!(warning.get_msg().contains(&format!("read:0:0:{slot}")));
        }
        if let (Some(_), SeedError::Local(LocalError::Evaluation(error))) = (fail_slot, error) {
            assert!(error.to_string().contains("raw-primary"));
        }
    }
    let input = chunk(
        &schema,
        &[vec![Datum::Int(1), Datum::Int(2), Datum::Int(3)]],
    );
    let mut ctx = EvalContext::default();
    warn(&mut ctx, "untouched");
    let before = ctx.warnings.warnings.clone();
    assert_eq!(
        prepared(spec)
            .eval_one(&mut ctx, &input, &schema, 0)
            .unwrap(),
        Datum::Int(6)
    );
    assert_eq!(ctx.warnings.warning_cnt, 1);
    assert_eq!(ctx.warnings.warnings, before);
}

#[test]
fn plus_computed_outputs_own_detached_metadata() {
    let field = bigint().with_elems(["original"]);
    let mut col = Column::new(42, field.clone());
    col.index = 0;
    col.id = 84;
    col.orig_name = "t.original".into();
    col.is_hidden = true;
    col.collation
        .set_coercibility(crate::expr_collation::Coercibility::EXPLICIT);
    let value = Expression::Constant(Constant::new(Datum::Null, field.clone()));
    let nested = Expression::ScalarFunction(ScalarFunction::new(
        CiString::new("plus"),
        field.clone(),
        vec![value, int(0)],
    ));
    let expression = Expression::ScalarFunction(ScalarFunction::new(
        CiString::new("plus"),
        field.clone(),
        vec![Expression::Column(col), nested],
    ));
    let spec = lower(&expression, &[field.clone()]);
    let mut alias = field.clone();
    alias.set_elem_with_binary_literal(0, "changed", true);
    for ordinal in [0, 2] {
        let output = spec.outputs[ordinal].as_ref().unwrap();
        assert_eq!(output.identity.kind, DatumKind::Int);
        assert_eq!(output.identity.string_collation, None);
        assert_eq!(output.identity.decimal_declared_shape, None);
        assert_eq!(output.sql_type.elems_snapshot()[0].as_bytes(), b"original");
        assert_eq!(spec.core.nodes[ordinal].sql_type, output.sql_type);
    }
    assert!(spec.outputs[1].is_none());
    assert!(matches!(spec.shapes[3], SourceShape::Literal(None)));
    assert!(matches!(
        &spec.core.nodes[3].source,
        SourceNode::Literal {
            kind: DatumKind::Null,
            ..
        }
    ));
    let SourceNode::Column {
        id,
        unique_id,
        original_name,
        hidden,
        ..
    } = &spec.core.nodes[1].source
    else {
        panic!("column")
    };
    assert_eq!(
        (*id, *unique_id, original_name.as_str(), *hidden),
        (84, 42, "t.original", true)
    );
    assert!(spec.core.nodes[1].collation.initialized);
    let stable = spec
        .core
        .row_schema
        .iter()
        .map(snapshot_field_type)
        .collect::<Vec<_>>();
    let input = chunk(&stable, &[vec![Datum::Int(9)]]);
    assert_eq!(
        prepared(spec)
            .eval_one(&mut EvalContext::default(), &input, &stable, 0)
            .unwrap(),
        Datum::Null
    );
    let identity = lower(&plus(column(0), int(0)), &[bigint()]);
    assert_eq!(
        identity.outputs[0].as_ref().unwrap().identity.kind,
        DatumKind::Int
    );
}

#[test]
fn plus_selections_and_bindings_are_occurrence_local() {
    let schema = vec![bigint(); 2];
    let spec = lower(&plus(column(0), column(1)), &schema);
    let rows = vec![
        vec![Datum::Int(1), Datum::Int(10)],
        vec![Datum::Int(2), Datum::Int(20)],
        vec![Datum::Int(3), Datum::Int(30)],
    ];
    let mut input = chunk(&schema, &rows);
    input.set_sel(Some(vec![1, 0]));
    let mut program = prepared(Arc::clone(&spec));
    assert_eq!(
        program
            .eval_selected(&mut EvalContext::default(), &input, &schema, &[2, 0, 2])
            .unwrap(),
        vec![Datum::Int(33), Datum::Int(11), Datum::Int(33)]
    );
    for count in [0, 1, 1024, 1025] {
        let mut raw = Raw::new(&spec, rows.clone());
        let selection = vec![2; count];
        assert_eq!(
            program
                .eval_test_native(&mut EvalContext::default(), 3, &selection, &mut raw)
                .unwrap(),
            vec![Datum::Int(33); count]
        );
        assert_eq!(raw.reads.len(), count * 2);
        for (index, (slot, row)) in raw.reads.iter().enumerate() {
            assert_eq!(
                (*slot, row.occurrence, row.input_row),
                (index % 2, index / 2, 2)
            );
        }
    }
    let changed = vec![bigint().with_unsigned(true), bigint()];
    assert!(matches!(
        program.eval_selected(&mut EvalContext::default(), &input, &changed, &[]),
        Err(SeedError::Local(LocalError::InvalidBatch(_)))
    ));
    assert!(program
        .eval_selected(&mut EvalContext::default(), &input, &schema, &[3])
        .is_err());
    input.append_int64(1, 99);
    assert!(program
        .eval_selected(&mut EvalContext::default(), &input, &schema, &[])
        .is_err());
    let wrong = chunk(
        &[FieldType::new(FieldTypeCode::Float), bigint()],
        &[vec![Datum::Float32(1.0), Datum::Int(2)]],
    );
    assert!(program
        .eval_selected(&mut EvalContext::default(), &wrong, &schema, &[0])
        .is_err());
    let repeated = lower(&plus(column(0), column(0)), &[bigint()]);
    assert_eq!(&*repeated.binding_nodes, &[1, 2]);
    assert_eq!(
        repeated.core.bindings[0].index,
        repeated.core.bindings[1].index
    );
}

#[test]
fn plus_profile_snapshot_and_limits_are_checked() {
    let expression = plus(column(0), int(1));
    let spec = lower(&expression, &[bigint()]);
    for mutation in 0..3 {
        let mut graph = spec.core.expr.clone(); // tiny fixture only, not a deep-tree API
        let LocalExpr::Call {
            args, return_type, ..
        } = &mut graph
        else {
            panic!("call")
        };
        match mutation {
            0 => {
                let LocalExpr::Constant { value, .. } = &mut args[1] else {
                    panic!("literal")
                };
                *value = ScalarValue::Int(Some(9));
            }
            1 => return_type.set_flen(7),
            _ => {
                let LocalExpr::InputSlot { slot, .. } = &mut args[0] else {
                    panic!("slot")
                };
                *slot = 1;
            }
        }
        assert!(compile_local_profiled(
            &graph,
            &spec.core.schema,
            LocalCompileContext::default(),
            &spec.profiles
        )
        .is_err());
    }
    let original = spec.profiles.call_sites()[0].clone();
    for sites in [
        vec![],
        vec![original.clone(), original.clone()],
        vec![OrdinaryCallSite::typed_row(1, original.source())],
    ] {
        assert!(OrdinaryProfileSpec::new(
            &spec.core.expr,
            &spec.core.schema,
            OrdinaryProfile::TypedRow,
            sites,
            CompileLimits::default()
        )
        .is_err());
    }
    for limits in [
        CompileLimits {
            max_nodes: 2,
            max_depth: 2,
        },
        CompileLimits {
            max_nodes: 3,
            max_depth: 1,
        },
        CompileLimits {
            max_nodes: 0,
            max_depth: 2,
        },
    ] {
        assert!(matches!(
            lower_typed_int_plus_row(&expression, &[bigint()], true, 7, limits),
            Err(SeedError::Local(LocalError::ResourceLimit(_)))
        ));
    }
    assert!(lower_typed_int_plus_row(
        &expression,
        &[bigint()],
        true,
        7,
        CompileLimits {
            max_nodes: 3,
            max_depth: 2
        }
    )
    .is_ok());
    let mut raw = Raw::new(&spec, vec![vec![Datum::Int(1)]]);
    raw.poison = Some(0);
    let mut program = PreparedIntPlusRow::compile(
        spec,
        ExecutionLimits {
            max_steps: 0,
            ..ExecutionLimits::default()
        },
    )
    .unwrap();
    assert!(matches!(
        program.eval_test_native(&mut EvalContext::default(), 1, &[0], &mut raw),
        Err(SeedError::Local(LocalError::ResourceLimit(_)))
    ));
    assert!(raw.reads.is_empty());
    for depth in [33, 64] {
        let mut expression = column(0);
        for _ in 0..depth {
            expression = plus(expression, int(1));
        }
        let spec = lower(&expression, &[bigint()]);
        assert_eq!(spec.profiles.node_count(), 2 * depth + 1);
        let input = chunk(&[bigint()], &[vec![Datum::Int(0)]]);
        assert_eq!(
            prepared(spec)
                .eval_one(&mut EvalContext::default(), &input, &[bigint()], 0)
                .unwrap(),
            Datum::Int(depth as i64)
        );
    }
    // This does not promise iterative Drop for arbitrary native Expressions.
}

#[test]
fn plus_closed_domain_never_reaches_native_hooks() {
    let spec = lower(&plus(int(1), int(2)), &[]);
    for consumer in [
        OrdinaryProfile::AstValueScalar,
        OrdinaryProfile::NativeNumericBatch,
    ] {
        assert!(OrdinaryProfileSpec::new(
            &spec.core.expr,
            &[],
            consumer,
            spec.profiles.call_sites().to_vec(),
            CompileLimits::default()
        )
        .is_err());
    }
    for function in [
        FunctionRef::TiPb(tipb::ScalarFuncSig::PlusIntSignedSigned),
        FunctionRef::Local(LocalFunctionId::NullIfIntSignedSigned),
    ] {
        let mut graph = spec.core.expr.clone();
        let LocalExpr::Call {
            function: selected, ..
        } = &mut graph
        else {
            panic!("call")
        };
        *selected = function;
        assert!(compile_local_profiled(
            &graph,
            &[],
            LocalCompileContext::default(),
            &spec.profiles
        )
        .is_err());
    }
    for kind in [LiteralKind::Text, LiteralKind::BinaryLiteral] {
        let mut graph = spec.core.expr.clone();
        let LocalExpr::Call { args, .. } = &mut graph else {
            panic!("call")
        };
        let LocalExpr::Constant { literal_kind, .. } = &mut args[0] else {
            panic!("literal")
        };
        *literal_kind = kind;
        assert!(OrdinaryProfileSpec::new(
            &graph,
            &[],
            OrdinaryProfile::TypedRow,
            spec.profiles.call_sites().to_vec(),
            CompileLimits::default()
        )
        .is_err());
    }
    for name in ["minus", "ifnull", "cast_signed", "rand", "grouping"] {
        assert!(lower_typed_int_plus_row(
            &call(name, vec![int(1), int(2)]),
            &[],
            true,
            1,
            CompileLimits::default()
        )
        .is_err());
    }
    for field in [
        bigint().with_unsigned(true),
        FieldType::new(FieldTypeCode::Tiny),
        FieldType::new(FieldTypeCode::Null),
    ] {
        let child = Expression::Constant(Constant::new(Datum::Null, field));
        assert!(lower_typed_int_plus_row(
            &plus(int(1), child),
            &[],
            true,
            1,
            CompileLimits::default()
        )
        .is_err());
    }
    for parameter in [false, true] {
        let mut value = Constant::new(Datum::Int(1), bigint());
        if parameter {
            value.param_marker = Some(ParamMarker { order: 0 });
        } else {
            value.deferred_expr = Some(Box::new(call("rand", vec![])));
        }
        assert!(lower_typed_int_plus_row(
            &plus(literal(Datum::Null), Expression::Constant(value)),
            &[],
            true,
            1,
            CompileLimits::default()
        )
        .is_err());
    }
    let mut virtual_column = Column::new(1, bigint());
    virtual_column.index = 0;
    virtual_column.virtual_expr = Some(Box::new(call("rand", vec![])));
    for child in [
        Expression::Column(virtual_column),
        Expression::CorrelatedColumn(CorrelatedColumn::new(Column::new(1, bigint()))),
        Expression::ScalarFunction(ScalarFunction::new_values(0, bigint())),
    ] {
        assert!(lower_typed_int_plus_row(
            &plus(literal(Datum::Null), child),
            &[bigint()],
            true,
            1,
            CompileLimits::default()
        )
        .is_err());
    }
    let wire = wire_plus(
        wire_leaf(db_pb::ExprType::Int64, 1),
        wire_leaf(db_pb::ExprType::Int64, 2),
    );
    let pb = pb_to_expr(&wire, &[]).unwrap();
    assert!(
        lower_typed_int_plus_row(&plus(int(1), pb), &[], true, 1, CompileLimits::default())
            .is_err()
    );
    assert!(lower_typed_int_plus_row(&int(1), &[], true, 1, CompileLimits::default()).is_err());
}

fn ast(sql: &str) -> Expr {
    let Stmt::Query(query) = tidb_parser::parse(&format!("SELECT {sql}")).unwrap() else {
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
struct Legacy;
impl Columns for Legacy {
    fn get(&self, _: &[String]) -> Option<Datum> {
        panic!("row read during build")
    }
    fn time_zone(&self) -> SessionTimeZone {
        SessionTimeZone::utc()
    }
}
impl ColumnResolver for Legacy {
    fn resolve(&self, _: &[String]) -> Option<(usize, FieldType, i64)> {
        panic!("unexpected binding")
    }
    fn time_zone(&self) -> SessionTimeZone {
        SessionTimeZone::utc()
    }
    fn fold_constant(
        &self,
        expression: &mut Expression,
        mode: crate::constant_fold::ConstantFoldMode,
    ) {
        crate::constant_fold::fold_constant_in_mode(expression, self, mode);
    }
}
struct Poison;
impl ColumnResolver for Poison {
    fn resolve(&self, _: &[String]) -> Option<(usize, FieldType, i64)> {
        panic!("structural arithmetic bound a column")
    }
    fn time_zone(&self) -> SessionTimeZone {
        panic!("structural arithmetic consulted metadata")
    }
    fn fold_constant(&self, _: &mut Expression, _: crate::constant_fold::ConstantFoldMode) {
        panic!("structural arithmetic folded")
    }
}

#[test]
fn plus_caller_coexists_with_frozen_d1_d2() {
    let control = call("ifnull", vec![int(7), int(8)]);
    assert!(lower::lower_int_control_seed(&control, &[], true, CompileLimits::default()).is_ok());
    assert!(lower_typed_int_plus_row(&control, &[], true, 1, CompileLimits::default()).is_err());
    assert!(lower::lower_int_control_seed(
        &plus(int(1), int(2)),
        &[],
        true,
        CompileLimits::default()
    )
    .is_err());
    assert!(rewrite_expr_structural(
        &ast("a + 1"),
        &Poison,
        StructuralLimits {
            max_nodes: 10,
            max_depth: 10
        }
    )
    .is_err());
    let Expression::Constant(value) = rewrite_expr_resolved(&ast("1 + 2"), &Legacy).unwrap() else {
        panic!("legacy SqlBuild fold")
    };
    assert_eq!(value.literal_value(), Some(&Datum::Int(3)));
    let structural = rewrite_expr_structural(
        &ast("IF(1,2,3)"),
        &Poison,
        StructuralLimits {
            max_nodes: 10,
            max_depth: 10,
        },
    )
    .unwrap();
    assert!(matches!(
        structural.as_expression(),
        Expression::ScalarFunction(_)
    ));
    assert!(!structural
        .as_expression()
        .static_type()
        .unwrap()
        .has_flag(FieldTypeFlags::NOT_NULL));
}
