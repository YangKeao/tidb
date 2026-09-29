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
use crate::column::Column;
use crate::constant::{Constant, ParamMarker};
use crate::distsql_builtin::pb_to_expr;
use crate::scalar_function::ScalarFunction;
use crate::Columns;
use tidb_ast::CiString;
use tidb_datatype::{BinaryLiteral, Collation, SessionTimeZone};
use tidb_proto::tipb as db_pb;
use tidb_query_datatype::expr::Error as KernelError;

fn bigint() -> FieldType {
    FieldType::new(FieldTypeCode::LongLong)
        .with_flen(20)
        .with_decimal(0)
}
fn string_type() -> FieldType {
    FieldType::new(FieldTypeCode::Varchar)
        .with_flen(40)
        .with_collation(Collation::Utf8Mb4Bin)
}
fn literal(value: Datum, ty: FieldType) -> Expression {
    Expression::Constant(Constant::new(value, ty))
}
fn int(value: i64) -> Expression {
    literal(Datum::Int(value), bigint())
}
fn null(ty: FieldType) -> Expression {
    literal(Datum::Null, ty)
}
fn column(index: usize, ty: FieldType) -> Expression {
    let mut result = Column::new(index as i64 + 100, ty);
    result.index = index as i64;
    result.id = index as i64 + 10;
    result.orig_name = format!("t.c{index}");
    Expression::Column(result)
}
fn call(name: &str, ty: FieldType, args: Vec<Expression>) -> Expression {
    Expression::ScalarFunction(ScalarFunction::new(CiString::new(name), ty, args))
}
fn choose(condition: Expression, yes: Expression, no: Expression, ty: FieldType) -> Expression {
    call("if", ty, vec![condition, yes, no])
}
fn datum_string(bytes: &[u8], collation: Collation) -> Datum {
    Datum::String(StringDatum::new(bytes.to_vec(), collation))
}
fn source_limits() -> ControlSourceLimits {
    ControlSourceLimits {
        tree: CompileLimits::default(),
        max_literal_bytes: 1024 * 1024,
        max_metadata_bytes: 1024 * 1024,
    }
}
fn ids_for(root: &Expression) -> Vec<ResultMetaId> {
    let mut pending = vec![root];
    let mut result = Vec::new();
    while let Some(node) = pending.pop() {
        result.push(ResultMetaId::new(91, 1000 + result.len() as u64 * 17));
        if let Expression::ScalarFunction(function) = node {
            pending.extend(function.args.iter().rev());
        }
    }
    result
}
fn lower(root: &Expression, schema: &[FieldType]) -> Arc<LoweredControlLineage> {
    lower_typed_control_lineage(root, schema, true, &ids_for(root), source_limits()).unwrap()
}
fn prepare(spec: Arc<LoweredControlLineage>) -> PreparedControlLineage {
    PreparedControlLineage::compile(spec, ExecutionLimits::default(), 64 * 1024 * 1024).unwrap()
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
struct Legacy;
impl Columns for Legacy {
    fn get(&self, _: &[String]) -> Option<Datum> {
        panic!("unexpected named value lookup")
    }
    fn time_zone(&self) -> SessionTimeZone {
        SessionTimeZone::utc()
    }
}
fn compare_native(
    root: &Expression,
    schema: &[FieldType],
    rows: &[Vec<Datum>],
    selection: &[usize],
) -> NativeControlBatch {
    let input = chunk(schema, rows);
    let expected = selection
        .iter()
        .map(|row| root.eval(&Legacy, input.physical_row(*row)).unwrap())
        .collect::<Vec<_>>();
    let result = prepare(lower(root, schema))
        .eval_selected(&mut EvalContext::default(), &input, schema, selection)
        .unwrap();
    assert_eq!(result.values(), expected.as_slice());
    assert_eq!(result.result_metadata().len(), selection.len());
    result
}

// Raw native values test the pre-erasure seam, not an already-normalized carrier.
struct Raw {
    schema: Vec<tipb::FieldType>,
    rows: Vec<Vec<Datum>>,
    reads: Vec<(usize, InputRow)>,
    poison: Option<usize>,
    failure: Option<usize>,
    warnings: bool,
}
impl Raw {
    fn new(spec: &LoweredControlLineage, rows: Vec<Vec<Datum>>) -> Self {
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
        assert_ne!(self.poison, Some(slot), "skipped native source was read");
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
fn sparse_ids_preorder_facts_and_actual_control_signature() {
    let ty = string_type().with_flen(61);
    let root = choose(
        int(1),
        column(0, ty.clone()),
        call(
            "coalesce",
            ty.clone(),
            vec![null(ty.clone()), literal(Datum::Bytes(vec![9]), ty.clone())],
        ),
        ty.clone(),
    );
    let mut ids = ids_for(&root);
    ids[0] = ResultMetaId::new(91, u64::MAX);
    let spec =
        lower_typed_control_lineage(&root, &[ty.clone()], true, &ids, source_limits()).unwrap();
    assert_eq!(spec.facts.namespace(), 91);
    assert_eq!(spec.facts.node_count(), 6);
    assert_eq!(spec.binding_nodes.as_ref(), &[2]);
    for (ordinal, fact) in spec.facts.producers().iter().enumerate() {
        assert_eq!(fact.ordinal(), ordinal);
        assert_eq!(fact.id(), ids[ordinal]);
        assert_eq!(spec.records[ordinal].id, ids[ordinal]);
        assert!(spec.core.nodes[ordinal].wire_origin.is_none());
    }
    let LocalExpr::Call {
        function,
        return_type,
        ..
    } = &spec.core.expr
    else {
        panic!("call")
    };
    assert_eq!(*function, FunctionRef::TiPb(tipb::ScalarFuncSig::IfString));
    assert_eq!(
        return_type.get_tp(),
        i32::from(FieldTypeCode::Varchar.mysql_type())
    );
    assert_eq!(return_type.get_flen(), 61);
    assert_eq!(
        spec.records[0].display_name.as_ref().unwrap().original(),
        "if"
    );
    assert_eq!(spec.records[2].role, ControlProducerRole::InputSlot);
    assert_eq!(spec.records[2].identity.kind, DatumKind::String);
    assert_eq!(spec.records[5].identity.kind, DatumKind::Bytes);
    assert!(!spec.records[0].may_escape_root); // IF's own NULL record is never selected.
    assert!(!spec.records[1].may_escape_root); // predicate
    assert!(spec.records[3].may_escape_root); // COALESCE's generated NULL
    let SourceNode::Column {
        index,
        id,
        unique_id,
        original_name,
        ..
    } = &spec.core.nodes[2].source
    else {
        panic!("column metadata")
    };
    assert_eq!(
        (*index, *id, *unique_id, original_name.as_str()),
        (0, 10, 100, "t.c0")
    );
    let input = chunk(&[ty.clone()], &[vec![datum_string(b"x", ty.collation())]]);
    let result = prepare(spec)
        .eval_selected(&mut EvalContext::default(), &input, &[ty], &[0])
        .unwrap();
    assert_eq!(result.result_metadata(), &[ids[2]]);
}

#[test]
fn ids_require_complete_unique_single_namespace_producers() {
    let root = choose(int(1), int(7), int(8), bigint());
    let ids = ids_for(&root);
    let cases = [
        ids[..3].to_vec(),
        [ids.clone(), vec![ResultMetaId::new(91, 9999)]].concat(),
        vec![ids[0], ids[1], ids[2], ids[2]],
        vec![ids[0], ids[1], ids[2], ResultMetaId::new(92, 1)],
        Vec::new(),
    ];
    for changed in cases {
        assert!(lower_typed_control_lineage(&root, &[], true, &changed, source_limits()).is_err());
    }
    let sparse = vec![
        ResultMetaId::new(5, u64::MAX),
        ResultMetaId::new(5, 0),
        ResultMetaId::new(5, 9000000),
        ResultMetaId::new(5, 2),
    ];
    assert!(lower_typed_control_lineage(&root, &[], true, &sparse, source_limits()).is_ok());
}

#[test]
fn every_admitted_control_matches_native_datum_row_path() {
    let schema = vec![bigint(); 3];
    let rows = vec![
        vec![Datum::Int(0), Datum::Null, Datum::Int(8)],
        vec![Datum::Int(1), Datum::Int(4), Datum::Int(9)],
        vec![Datum::Null, Datum::Int(0), Datum::Null],
    ];
    let make = |name, args| call(name, bigint(), args);
    for root in [
        make(
            "if",
            vec![
                column(0, bigint()),
                column(1, bigint()),
                column(2, bigint()),
            ],
        ),
        make("ifnull", vec![column(1, bigint()), column(2, bigint())]),
        make(
            "case",
            vec![
                column(0, bigint()),
                column(1, bigint()),
                column(2, bigint()),
            ],
        ),
        make("case", vec![column(0, bigint()), column(1, bigint())]),
        make("coalesce", vec![column(1, bigint()), column(2, bigint())]),
        make("and", vec![column(0, bigint()), column(1, bigint())]),
        make("or", vec![column(0, bigint()), column(1, bigint())]),
    ] {
        compare_native(&root, &schema, &rows, &[2, 0, 2, 1]);
    }
    let ty = string_type();
    let a = datum_string(b"a", Collation::Utf8Mb4GeneralCi);
    let b = Datum::Bytes(vec![0xff, 0, 0x80]);
    for root in [
        call(
            "ifnull",
            ty.clone(),
            vec![null(ty.clone()), literal(a.clone(), ty.clone())],
        ),
        call(
            "case",
            ty.clone(),
            vec![
                int(1),
                literal(a.clone(), ty.clone()),
                literal(b.clone(), ty.clone()),
            ],
        ),
        call(
            "case",
            ty.clone(),
            vec![int(0), literal(a.clone(), ty.clone())],
        ),
        call(
            "coalesce",
            ty.clone(),
            vec![null(ty.clone()), literal(b, ty.clone())],
        ),
    ] {
        compare_native(&root, &[], &[vec![]], &[0, 0]);
    }
}

#[test]
fn selection_preserves_string_bytes_collation_and_invalid_utf8_identity() {
    let ty = string_type();
    let payload = vec![0xff, 0, 0x80];
    let left = datum_string(&payload, Collation::Utf8Mb4GeneralCi);
    let root = choose(
        column(0, bigint()),
        literal(left.clone(), ty.clone()),
        literal(Datum::Bytes(payload.clone()), ty.clone()),
        ty.clone(),
    );
    let rows = vec![vec![Datum::Int(1)], vec![Datum::Int(0)]];
    let result = compare_native(&root, &[bigint()], &rows, &[1, 0, 1]);
    assert_eq!(
        result.values(),
        &[Datum::Bytes(payload.clone()), left, Datum::Bytes(payload)]
    );
    let ids = ids_for(&root);
    assert_eq!(result.result_metadata(), &[ids[3], ids[2], ids[3]]);
    assert_eq!(result.declared_type_snapshot(), ty);
    let root = choose(
        column(0, bigint()),
        literal(datum_string(b"equal", Collation::Binary), ty.clone()),
        literal(
            datum_string(b"equal", Collation::Utf8Mb4GeneralCi),
            ty.clone(),
        ),
        ty,
    );
    let result = compare_native(&root, &[bigint()], &rows, &[0, 1]);
    let [Datum::String(a), Datum::String(b)] = result.values() else {
        panic!("strings")
    };
    assert_eq!(a.bytes(), b.bytes());
    assert_eq!(a.collation(), Collation::Binary);
    assert_eq!(b.collation(), Collation::Utf8Mb4GeneralCi);
}

#[test]
fn native_binary_and_blob_columns_are_string_not_binary_literals_or_bytes() {
    for code in [
        FieldTypeCode::Varchar,
        FieldTypeCode::VarString,
        FieldTypeCode::String,
        FieldTypeCode::TinyBlob,
        FieldTypeCode::MediumBlob,
        FieldTypeCode::LongBlob,
        FieldTypeCode::Blob,
    ] {
        let ty = FieldType::new(code)
            .with_collation(Collation::Binary)
            .with_flags(FieldTypeFlags::BINARY | FieldTypeFlags::BLOB);
        let root = call(
            "ifnull",
            ty.clone(),
            vec![column(0, ty.clone()), null(ty.clone())],
        );
        let result = compare_native(
            &root,
            &[ty.clone()],
            &[
                vec![Datum::Bytes(vec![0xff])],
                vec![Datum::Bytes(Vec::new())],
                vec![Datum::Null],
            ],
            &[0, 1, 2],
        );
        assert_eq!(
            result.values(),
            &[
                datum_string(&[0xff], Collation::Binary),
                datum_string(&[], Collation::Binary),
                Datum::Null
            ]
        );
        assert_eq!(result.producer_type_snapshot(0).unwrap().code(), code);
        let ids = ids_for(&root);
        assert_eq!(result.result_metadata(), &[ids[1], ids[1], ids[2]]);
    }
}

#[test]
fn selected_null_generated_null_and_boolean_own_ids_do_not_collapse() {
    let ty = string_type();
    let cases = vec![
        (
            choose(int(1), null(ty.clone()), null(ty.clone()), ty.clone()),
            2,
        ),
        (
            call(
                "ifnull",
                ty.clone(),
                vec![null(ty.clone()), null(ty.clone())],
            ),
            2,
        ),
        (call("case", ty.clone(), vec![int(1), null(ty.clone())]), 2),
        (call("case", ty.clone(), vec![int(0), null(ty.clone())]), 0),
        (
            call(
                "coalesce",
                ty.clone(),
                vec![null(ty.clone()), null(ty.clone())],
            ),
            0,
        ),
        (
            choose(
                int(1),
                call("case", ty.clone(), vec![int(0), null(ty.clone())]),
                null(ty.clone()),
                ty.clone(),
            ),
            2,
        ),
        (
            choose(
                int(1),
                call("and", bigint(), vec![null(bigint()), int(1)]),
                int(7),
                bigint(),
            ),
            2,
        ),
    ];
    for (root, ordinal) in cases {
        let result = compare_native(&root, &[], &[vec![]], &[0]);
        assert_eq!(result.values(), &[Datum::Null]);
        assert_eq!(result.result_metadata(), &[ids_for(&root)[ordinal]]);
    }
    for (name, left, right, expected) in [
        ("and", Datum::Null, Datum::Int(0), Datum::Int(0)),
        ("or", Datum::Null, Datum::Int(1), Datum::Int(1)),
        ("and", Datum::Int(1), Datum::Int(1), Datum::Int(1)),
    ] {
        let root = call(
            name,
            bigint(),
            vec![literal(left, bigint()), literal(right, bigint())],
        );
        let result = compare_native(&root, &[], &[vec![]], &[0]);
        assert_eq!(result.values(), &[expected]);
        assert_eq!(result.result_metadata(), &[ids_for(&root)[0]]);
    }
    // NULL input retains its immutable String non-NULL contract and source ID.
    let root = choose(int(1), column(0, ty.clone()), null(ty.clone()), ty.clone());
    let result = compare_native(&root, &[ty], &[vec![Datum::Null]], &[0]);
    let (_, record) = result.spec.record(result.result_metadata()[0]).unwrap();
    assert_eq!(record.identity.kind, DatumKind::String);
    assert!(record.identity.string_collation.is_some());
    assert_eq!(result.values(), &[Datum::Null]);
}

#[test]
fn unsigned_bits_survive_value_selection_but_predicate_rejection_is_transitive() {
    let unsigned = bigint().with_unsigned(true);
    let root = choose(int(1), column(0, unsigned.clone()), int(0), bigint());
    let result = compare_native(
        &root,
        &[unsigned.clone()],
        &[vec![Datum::UInt(u64::MAX)]],
        &[0],
    );
    assert_eq!(result.values(), &[Datum::UInt(u64::MAX)]);
    assert!(!result.declared_type_snapshot().is_unsigned());
    assert!(result.producer_type_snapshot(0).unwrap().is_unsigned());
    let outer = choose(root.clone(), int(8), int(9), bigint());
    assert!(lower_typed_control_lineage(
        &outer,
        &[unsigned.clone()],
        true,
        &ids_for(&outer),
        source_limits()
    )
    .is_err());
    let dead_unsigned = choose(
        int(0),
        literal(Datum::UInt(0), unsigned.clone()),
        int(1),
        bigint(),
    );
    let outer = call("and", bigint(), vec![int(1), dead_unsigned]);
    assert!(
        lower_typed_control_lineage(&outer, &[], true, &ids_for(&outer), source_limits()).is_err()
    );
    // UInt truth is valid natively. This is stricter admission, not a fake native error.
    assert_eq!(
        crate::truthy_of(&Datum::UInt(u64::MAX)).unwrap(),
        Some(true)
    );
    for leaf in [
        literal(Datum::UInt(0), bigint()),
        literal(Datum::Int(0), unsigned),
    ] {
        let invalid = choose(int(1), leaf, int(0), bigint());
        assert!(lower_typed_control_lineage(
            &invalid,
            &[],
            true,
            &ids_for(&invalid),
            source_limits()
        )
        .is_err());
    }
}

#[test]
fn same_demanded_read_checks_kind_and_collation_before_erasure() {
    let ty = string_type();
    let string_root = call("coalesce", ty.clone(), vec![column(0, ty.clone())]);
    let string_spec = lower(&string_root, &[ty.clone()]);
    for wrong in [
        Datum::Bytes(b"x".to_vec()),
        datum_string(b"x", Collation::Binary),
        Datum::BinaryLiteral(BinaryLiteral::from(b"x".to_vec())),
    ] {
        assert!(to_scalar(&wrong, EvalType::Bytes).is_ok());
        let mut raw = Raw::new(&string_spec, vec![vec![wrong]]);
        let result = prepare(Arc::clone(&string_spec)).eval_test_native(
            &mut EvalContext::default(),
            1,
            &[0],
            &mut raw,
        );
        assert!(matches!(
            result,
            Err(SeedError::Local(LocalError::BindingContract(_)))
        ));
        assert_eq!(raw.reads.len(), 1);
    }
    let mut raw = Raw::new(&string_spec, vec![vec![Datum::Null]]);
    assert_eq!(
        prepare(string_spec)
            .eval_test_native(&mut EvalContext::default(), 1, &[0], &mut raw)
            .unwrap()
            .values(),
        &[Datum::Null]
    );
    assert_eq!(raw.reads.len(), 1);
    for (ty, wrong) in [
        (bigint(), Datum::UInt(0)),
        (bigint().with_unsigned(true), Datum::Int(0)),
    ] {
        let root = call("coalesce", ty.clone(), vec![column(0, ty.clone())]);
        let spec = lower(&root, &[ty]);
        assert!(to_scalar(&wrong, EvalType::Int).is_ok());
        let mut raw = Raw::new(&spec, vec![vec![wrong]]);
        assert!(matches!(
            prepare(spec).eval_test_native(&mut EvalContext::default(), 1, &[0], &mut raw),
            Err(SeedError::Local(LocalError::BindingContract(_)))
        ));
        assert_eq!(raw.reads.len(), 1);
    }
}

#[test]
fn lazy_reads_selection_occurrences_and_native_chunk_selection_are_preserved() {
    let ty = string_type();
    let schema = vec![bigint(), ty.clone(), ty.clone()];
    let root = choose(
        column(0, bigint()),
        column(1, ty.clone()),
        column(2, ty.clone()),
        ty.clone(),
    );
    let spec = lower(&root, &schema);
    let rows = vec![
        vec![
            Datum::Int(1),
            datum_string(b"yes", ty.collation()),
            datum_string(b"skip", ty.collation()),
        ],
        vec![
            Datum::Int(0),
            datum_string(b"skip", ty.collation()),
            datum_string(b"no", ty.collation()),
        ],
    ];
    let mut raw = Raw::new(&spec, rows.clone());
    raw.poison = Some(2);
    let output = prepare(Arc::clone(&spec))
        .eval_test_native(&mut EvalContext::default(), 2, &[0, 0], &mut raw)
        .unwrap();
    assert_eq!(
        output.values(),
        &[
            datum_string(b"yes", ty.collation()),
            datum_string(b"yes", ty.collation())
        ]
    );
    assert_eq!(
        raw.reads
            .iter()
            .map(|(slot, row)| (*slot, row.occurrence, row.input_row))
            .collect::<Vec<_>>(),
        vec![(0, 0, 0), (1, 0, 0), (0, 1, 0), (1, 1, 0)]
    );
    let mut input = chunk(&schema, &rows);
    input.set_sel(Some(vec![1, 0]));
    let output = prepare(spec)
        .eval_selected(&mut EvalContext::default(), &input, &schema, &[1, 0, 1])
        .unwrap();
    assert_eq!(
        output.values(),
        &[
            datum_string(b"no", ty.collation()),
            datum_string(b"yes", ty.collation()),
            datum_string(b"no", ty.collation())
        ]
    );
    for (name, left, right, expected_reads) in [
        ("and", Datum::Int(0), Datum::Int(9), 1),
        ("or", Datum::Int(1), Datum::Int(0), 1),
        ("and", Datum::Null, Datum::Int(0), 2),
        ("or", Datum::Null, Datum::Int(1), 2),
    ] {
        let root = call(
            name,
            bigint(),
            vec![column(0, bigint()), column(1, bigint())],
        );
        let spec = lower(&root, &[bigint(), bigint()]);
        let mut raw = Raw::new(&spec, vec![vec![left, right]]);
        if expected_reads == 1 {
            raw.poison = Some(1);
        }
        prepare(spec)
            .eval_test_native(&mut EvalContext::default(), 1, &[0], &mut raw)
            .unwrap();
        assert_eq!(raw.reads.len(), expected_reads);
    }
}

#[test]
fn demanded_failure_and_late_materialization_refusal_keep_existing_prefix() {
    let schema = vec![bigint(); 2];
    let root = call(
        "and",
        bigint(),
        vec![column(0, bigint()), column(1, bigint())],
    );
    let spec = lower(&root, &schema);
    let mut raw = Raw::new(&spec, vec![vec![Datum::Int(1), Datum::Int(1)]]);
    raw.warnings = true;
    raw.failure = Some(1);
    let mut ctx = EvalContext::default();
    warn(&mut ctx, "prior");
    assert!(matches!(
        prepare(spec).eval_test_native(&mut ctx, 1, &[0, 0], &mut raw),
        Err(SeedError::Local(LocalError::Evaluation(_)))
    ));
    assert_eq!(ctx.warnings.warning_cnt, 3);
    assert_eq!(raw.reads.len(), 2);
    assert!(ctx.warnings.warnings[0].get_msg().contains("prior"));

    let ty = string_type();
    let root = call("coalesce", ty.clone(), vec![column(0, ty.clone())]);
    let spec = lower(&root, &[ty.clone()]);
    let mut raw = Raw::new(&spec, vec![vec![datum_string(b"payload", ty.collation())]]);
    raw.warnings = true;
    let mut prepared =
        PreparedControlLineage::compile(spec, ExecutionLimits::default(), 0).unwrap();
    let mut ctx = EvalContext::default();
    assert!(matches!(
        prepared.eval_test_native(&mut ctx, 1, &[0, 0], &mut raw),
        Err(SeedError::Local(LocalError::ResourceLimit(_)))
    ));
    assert_eq!(raw.reads.len(), 2); // C finished; native materialization never replays it.
    assert_eq!(ctx.warnings.warning_cnt, 2);
}

#[test]
fn all_node_native_closure_refuses_pb_and_unproved_shapes_even_when_dead() {
    let wire_type = db_pb::FieldType {
        tp: Some(i32::from(FieldTypeCode::LongLong.mysql_type())),
        flen: Some(20),
        decimal: Some(0),
        charset: Some("binary".into()),
        collate: Some(-63),
        ..Default::default()
    };
    let mut encoded = Vec::new();
    tidb_codec::encode_int(&mut encoded, 1);
    let leaf = db_pb::Expr {
        tp: Some(db_pb::ExprType::Int64 as i32),
        val: Some(encoded),
        field_type: Some(wire_type.clone()),
        ..Default::default()
    };
    let pb_leaf = pb_to_expr(&leaf, &[]).unwrap();
    let pb_call = pb_to_expr(
        &db_pb::Expr {
            tp: Some(db_pb::ExprType::ScalarFunc as i32),
            sig: Some(db_pb::ScalarFuncSig::IfInt as i32),
            field_type: Some(wire_type),
            children: vec![leaf.clone(), leaf.clone(), leaf],
            ..Default::default()
        },
        &[],
    )
    .unwrap();
    let mut parameter = Constant::new(Datum::Int(1), bigint());
    parameter.param_marker = Some(ParamMarker { order: 0 });
    let mut deferred = Constant::new(Datum::Int(1), bigint());
    deferred.deferred_expr = Some(Box::new(int(1)));
    let mut virtual_column = Column::new(8, bigint());
    virtual_column.index = 0;
    virtual_column.virtual_expr = Some(Box::new(int(1)));
    let mut correlated = Column::new(9, bigint());
    correlated.index = 0;
    correlated.correlated_col_unique_id = 88;
    let mut opaque = ScalarFunction::new_values(0, bigint());
    opaque.func_name = CiString::new("if");
    opaque.args = vec![int(1), int(2), int(3)];
    for bad in [
        pb_leaf,
        pb_call,
        Expression::Constant(parameter),
        Expression::Constant(deferred),
        Expression::Column(virtual_column),
        Expression::Column(correlated),
        Expression::ScalarFunction(opaque),
        call("plus", bigint(), vec![int(1), int(2)]),
        call("cast", bigint(), vec![int(1)]),
        call("eq", bigint(), vec![int(1), int(1)]),
        call("nullif", bigint(), vec![int(1), int(2)]),
    ] {
        let root = choose(int(0), bad, int(7), bigint());
        assert!(lower_typed_control_lineage(
            &root,
            &[bigint()],
            true,
            &ids_for(&root),
            source_limits()
        )
        .is_err());
    }
    for (name, arity) in [
        ("if", 2),
        ("ifnull", 1),
        ("case", 1),
        ("coalesce", 0),
        ("and", 3),
        ("or", 1),
    ] {
        let root = call(name, bigint(), (0..arity).map(|_| int(1)).collect());
        assert!(
            lower_typed_control_lineage(&root, &[], true, &ids_for(&root), source_limits())
                .is_err()
        );
    }
    assert!(
        lower_typed_control_lineage(&int(1), &[], true, &ids_for(&int(1)), source_limits())
            .is_err()
    );
}

#[test]
fn actual_sql_types_flags_and_literal_provenance_are_not_retagged() {
    for ty in [
        FieldType::new(FieldTypeCode::Tiny),
        FieldType::new(FieldTypeCode::Null),
        FieldType::new(FieldTypeCode::Double),
        FieldType::new(FieldTypeCode::Bit),
        FieldType::new(FieldTypeCode::Enum),
        FieldType::new(FieldTypeCode::Set),
        bigint().with_array(true),
        bigint().with_flags(FieldTypeFlags::ENUM_SET_AS_INT),
        bigint().with_flags(FieldTypeFlags::PARSE_TO_JSON),
        bigint().with_flags(1 << 31),
    ] {
        let root = choose(int(0), literal(Datum::Null, ty), int(1), bigint());
        assert!(
            lower_typed_control_lineage(&root, &[], true, &ids_for(&root), source_limits())
                .is_err()
        );
    }
    let ty = string_type().with_collation(Collation::Binary);
    let root = call(
        "coalesce",
        ty.clone(),
        vec![literal(
            Datum::BinaryLiteral(BinaryLiteral::from(vec![0xff])),
            ty,
        )],
    );
    assert!(
        lower_typed_control_lineage(&root, &[], true, &ids_for(&root), source_limits()).is_err()
    );
    let cross = choose(
        int(1),
        literal(Datum::Bytes(vec![1]), string_type()),
        int(0),
        bigint(),
    );
    assert!(
        lower_typed_control_lineage(&cross, &[], true, &ids_for(&cross), source_limits()).is_err()
    );
    let bad_predicate = choose(
        literal(Datum::Bytes(b"1".to_vec()), string_type()),
        int(1),
        int(0),
        bigint(),
    );
    assert!(lower_typed_control_lineage(
        &bad_predicate,
        &[],
        true,
        &ids_for(&bad_predicate),
        source_limits()
    )
    .is_err());
}

#[test]
fn source_and_incoming_metadata_caps_precede_projection_snapshots_and_equality() {
    let ty = string_type();
    let root = choose(
        int(0),
        literal(Datum::Bytes(vec![8; 64]), ty.clone()),
        null(ty.clone()),
        ty.clone(),
    );
    let mut limits = source_limits();
    limits.max_literal_bytes = 127; // two64-byte source/fact payload charges
    assert!(matches!(
        lower_typed_control_lineage(&root, &[], true, &ids_for(&root), limits),
        Err(SeedError::Local(LocalError::ResourceLimit(_)))
    ));
    let root = call("coalesce", bigint(), vec![int(1)]);
    limits = source_limits();
    limits.max_metadata_bytes = 1;
    assert!(matches!(
        lower_typed_control_lineage(&root, &[], true, &ids_for(&root), limits),
        Err(SeedError::Local(LocalError::ResourceLimit(_)))
    ));
    limits = source_limits();
    limits.tree.max_depth = 1;
    assert!(matches!(
        lower_typed_control_lineage(&root, &[], true, &ids_for(&root), limits),
        Err(SeedError::Local(LocalError::ResourceLimit(_)))
    ));
    let huge = bigint().with_elems(["x".repeat(source_limits().max_metadata_bytes)]);
    // Unused schema metadata is capped too, before any Node/C snapshot.
    assert!(matches!(
        lower_typed_control_lineage(
            &root,
            &[huge.clone()],
            true,
            &ids_for(&root),
            source_limits()
        ),
        Err(SeedError::Local(LocalError::ResourceLimit(_)))
    ));
    let root = call("coalesce", bigint(), vec![column(0, bigint())]);
    let spec = lower(&root, &[bigint()]);
    let mut prepared = prepare(spec);
    let input = chunk(&[bigint()], &[vec![Datum::Int(1)]]);
    let mut ctx = EvalContext::default();
    warn(&mut ctx, "prior");
    // Existing Eq would fail schema matching. Resource refusal here proves the
    // incoming byte gate ran first, not after its allocating slice comparisons.
    assert!(matches!(
        prepared.eval_selected(&mut ctx, &input, &[huge], &[0]),
        Err(SeedError::Local(LocalError::ResourceLimit(_)))
    ));
    assert_eq!(ctx.warnings.warning_cnt, 1);
}

#[test]
fn complete_metadata_is_detached_and_no_shallow_output_type_escapes() {
    let mut ty = string_type()
        .with_flen(73)
        .with_decimal(7)
        .with_elems(["first", "second"]);
    ty.set_elem_with_binary_literal(1, "second", true);
    let mut alias = ty.clone();
    let root = call(
        "IFNULL",
        ty.clone(),
        vec![column(0, ty.clone()), null(ty.clone())],
    );
    let spec = lower(&root, &[ty.clone()]);
    alias.set_elem_with_binary_literal(0, "changed", true);
    assert_eq!(spec.core.nodes[0].sql_type.elem(0).as_bytes(), b"first");
    assert!(!spec.core.nodes[0].sql_type.elem_is_binary_literal(0));
    assert!(spec.core.nodes[0].sql_type.elem_is_binary_literal(1));
    assert_eq!(
        spec.records[0].display_name.as_ref().unwrap().original(),
        "IFNULL"
    );
    let schema = vec![snapshot_field_type(&spec.core.row_schema[0])];
    let input = chunk(&schema, &[vec![datum_string(b"data", ty.collation())]]);
    let result = prepare(Arc::clone(&spec))
        .eval_selected(&mut EvalContext::default(), &input, &schema, &[0])
        .unwrap();
    let mut copied = result.declared_type_snapshot();
    copied.set_elem(0, "external change");
    assert_eq!(result.declared_type_snapshot().elem(0).as_bytes(), b"first");
    assert_eq!(result.declared_type_snapshot().flen(), 73);
    assert_eq!(result.declared_type_snapshot().decimal(), 7);
    assert_eq!(
        result.producer_type_snapshot(0).unwrap().code(),
        FieldTypeCode::Varchar
    );
    assert!(Arc::ptr_eq(&result.spec, &spec));
}

#[test]
fn materialization_rejects_foreign_unknown_predicate_and_false_null_records() {
    let root = choose(int(1), int(7), int(7), bigint());
    let spec = lower(&root, &[]);
    let ids = ids_for(&root);
    let prepared = prepare(spec);
    for id in [
        ResultMetaId::new(92, ids[2].record()),
        ResultMetaId::new(91, 999),
        ids[0],
        ids[1],
    ] {
        assert!(matches!(
            prepared.materialize_parts(
                VectorValue::from_scalar(&ScalarValue::Int(Some(7)), 1),
                vec![id],
                1
            ),
            Err(SeedError::Local(LocalError::BindingContract(_)))
        ));
    }
    assert!(prepared
        .materialize_parts(
            VectorValue::from_scalar(&ScalarValue::Int(Some(7)), 1),
            vec![],
            1
        )
        .is_err());
    assert!(prepared
        .materialize_parts(
            VectorValue::from_scalar(&ScalarValue::Int(Some(7)), 1),
            vec![ids[2]],
            0
        )
        .is_err());
    let result = prepared
        .materialize_parts(
            VectorValue::from_scalar(&ScalarValue::Int(Some(7)), 1),
            vec![ids[3]],
            1,
        )
        .unwrap();
    assert_eq!(result.result_metadata(), &[ids[3]]); // not chosen by equal value7
    let root = call("case", bigint(), vec![int(0), null(bigint())]);
    let ids = ids_for(&root);
    assert!(matches!(
        prepare(lower(&root, &[])).materialize_parts(
            VectorValue::from_scalar(&ScalarValue::Int(Some(7)), 1),
            vec![ids[0]],
            1
        ),
        Err(SeedError::Local(LocalError::BindingContract(_)))
    ));
}

#[test]
fn retained_native_string_vec_capacity_is_measured_without_copy_or_retag() {
    let mut bytes = Vec::with_capacity(512);
    bytes.extend_from_slice(&[0xff, 0, 1]);
    let pointer = bytes.as_ptr();
    let capacity = bytes.capacity();
    let (value, measured) =
        measured_native_datum(Datum::String(StringDatum::new(bytes, Collation::Binary))).unwrap();
    let Datum::String(value) = value else {
        panic!("String identity")
    };
    assert_eq!(measured, capacity);
    assert_eq!(value.bytes().as_ptr(), pointer);
    assert_eq!(value.collation(), Collation::Binary);
    assert_eq!(value.bytes(), &[0xff, 0, 1]);
    let mut bytes = Vec::with_capacity(128);
    bytes.push(3);
    let capacity = bytes.capacity();
    let (_, measured) = measured_native_datum(Datum::Bytes(bytes)).unwrap();
    assert_eq!(measured, capacity);
    assert_eq!(
        measured_native_datum(Datum::UInt(u64::MAX)).unwrap(),
        (Datum::UInt(u64::MAX), 0)
    );
}

#[test]
fn materialization_meters_source_offsets_bitmap_ids_and_live_native_output() {
    let ty = string_type();
    let root = call(
        "coalesce",
        ty.clone(),
        vec![literal(Datum::Bytes(vec![1]), ty)],
    );
    let spec = lower(&root, &[]);
    let id = ids_for(&root)[1];
    let mut bytes = ChunkedVecBytes::try_with_capacities(1, 4096).unwrap();
    bytes.push_ref(Some(&[1]));
    let values = VectorValue::Bytes(bytes);
    let source_bytes = vector_heap_bytes(&values).unwrap();
    assert!(source_bytes >= 4096 + 2 * size_of::<usize>());
    let mut ids = Vec::with_capacity(32);
    ids.push(id);
    let naive = source_bytes + size_of::<Datum>() + size_of::<ResultMetaId>() + 1;
    let prepared =
        PreparedControlLineage::compile(Arc::clone(&spec), ExecutionLimits::default(), naive)
            .unwrap();
    assert!(matches!(
        prepared.materialize_parts(values, ids, 1),
        Err(SeedError::Local(LocalError::ResourceLimit(_)))
    )); // spare ID capacity is real
    let mut empty = ChunkedVecBytes::try_with_capacities(1, 0).unwrap();
    empty.push_ref(None);
    assert!(vector_heap_bytes(&VectorValue::Bytes(empty)).unwrap() > 0);
    let input_root = call("coalesce", string_type(), vec![column(0, string_type())]);
    let spec = lower(&input_root, &[string_type()]);
    let mut raw = Raw::new(&spec, vec![vec![Datum::Null]]);
    let mut execution = ExecutionLimits::default();
    execution.max_retained_bytes = 0;
    let mut prepared = PreparedControlLineage::compile(spec, execution, usize::MAX).unwrap();
    assert!(matches!(
        prepared.eval_test_native(&mut EvalContext::default(), 1, &[0], &mut raw),
        Err(SeedError::Local(LocalError::ResourceLimit(_)))
    ));
    assert!(raw.reads.is_empty());
}

#[test]
fn workers_share_only_immutable_spec_and_keep_native_result_ownership() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<LoweredControlLineage>();
    let root = call("coalesce", bigint(), vec![int(7)]);
    let spec = lower(&root, &[]);
    let threads = (0..2)
        .map(|_| {
            let spec = Arc::clone(&spec);
            std::thread::spawn(move || {
                let input = chunk(&[], &[vec![]]);
                let result = prepare(Arc::clone(&spec))
                    .eval_selected(&mut EvalContext::default(), &input, &[], &[0])
                    .unwrap();
                assert!(Arc::ptr_eq(&result.spec, &spec));
                assert_eq!(result.values(), &[Datum::Int(7)]);
            })
        })
        .collect::<Vec<_>>();
    for thread in threads {
        thread.join().unwrap();
    }
}
