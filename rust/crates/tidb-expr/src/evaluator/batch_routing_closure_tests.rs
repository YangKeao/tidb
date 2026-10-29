// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Independent actual Suite contracts before generic Admission retry retirement.
use super::*;
use crate::tikv::dynamic_leaf_tests as leaf;
use crate::ReadyValueCache;
fn declarations() -> Vec<FieldType> {
    vec![
        ty(),
        ty().with_unsigned(true),
        FieldType::new(FieldTypeCode::Tiny),
        real_ty(),
        FieldType::new(FieldTypeCode::Float),
        FieldType::new(FieldTypeCode::NewDecimal)
            .with_flen(20)
            .with_decimal(2),
        string_ty(),
    ]
}
fn wire_field(code: i32) -> db_pb::FieldType {
    db_pb::FieldType {
        tp: Some(code),
        flag: Some(0),
        flen: Some(65),
        decimal: Some(if code == 246 { 5 } else { -1 }),
        charset: Some("utf8mb4".into()),
        collate: Some(-46),
        ..Default::default()
    }
}
fn pb_root(
    name: &str,
    target: &FieldType,
    source: &FieldType,
    arity: usize,
    kind: usize,
) -> Expression {
    let mut args = Vec::new();
    for i in 0..arity {
        let mut bytes = Vec::new();
        tidb_codec::encode_int(&mut bytes, if kind == 0 { -4 } else { 0 });
        args.push(db_pb::Expr {
            tp: Some(if kind == 1 {
                db_pb::ExprType::ColumnRef
            } else {
                db_pb::ExprType::Int64
            } as i32),
            val: Some(bytes),
            field_type: Some(wire_field(8)),
            ..Default::default()
        });
        let _ = i;
    }
    let node = db_pb::Expr {
        tp: Some(db_pb::ExprType::ScalarFunc as i32),
        sig: Some(db_pb::ScalarFuncSig::RegexpLikeSig as i32),
        children: args,
        field_type: Some(wire_field(8)),
        ..Default::default()
    };
    let mut root = pb_to_expr(&node, &[ty()]).unwrap();
    let Expression::ScalarFunction(f) = &mut root else {
        unreachable!()
    };
    // Genuine decoded selected PB owner; the existing public display metadata
    // may differ without replacing that owner or its original wire signature.
    f.func_name = CiString::new(name);
    f.ret_type = Some(target.clone());
    for arg in &mut f.args {
        match arg {
            Expression::Constant(c) => {
                c.ret_type = Some(source.clone());
                if kind == 2 {
                    c.param_marker = Some(ParamMarker { order: 0 });
                }
            }
            Expression::Column(c) => c.ret_type = Some(source.clone()),
            _ => unreachable!(),
        }
    }
    root
}
fn observe(
    suite: &EvaluatorSuite,
    context: &leaf::Context,
    input: &mut Chunk,
    output_type: &FieldType,
    evaluate: &mut impl FnMut(
        &EvaluatorSuite,
        &leaf::Context,
        &mut Chunk,
        &mut Chunk,
        &[FieldType],
    ) -> std::result::Result<(), EvaluatorError>,
    schema: &[FieldType],
) -> String {
    let mut output = Chunk::new_with_capacity(std::slice::from_ref(output_type), 0);
    output.append_null(0);
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        evaluate(suite, context, input, &mut output, schema)
    }));
    let status = match result {
        Ok(v) => format!("{v:?}"),
        Err(p) => format!(
            "PANIC:{}",
            p.downcast_ref::<String>()
                .cloned()
                .or_else(|| p.downcast_ref::<&str>().map(|v| v.to_string()))
                .unwrap_or_else(|| "opaque native panic".into())
        ),
    };
    let cells = (0..output.num_rows())
        .map(|i| {
            if output.column(0).is_null(i) {
                "NULL".to_owned()
            } else {
                format!("{:02X?}", output.column(0).get_raw(i).as_ref())
            }
        })
        .collect::<Vec<_>>();
    format!("{status}|{cells:?}|{}", context.snapshot())
}
fn pb_records(
    mut evaluate: impl FnMut(
        &EvaluatorSuite,
        &leaf::Context,
        &mut Chunk,
        &mut Chunk,
        &[FieldType],
    ) -> std::result::Result<(), EvaluatorError>,
) -> Vec<String> {
    let fields = declarations();
    let mut records = Vec::new();
    for name in [
        "abs",
        "plus",
        "minus",
        "mul",
        "mod",
        "cast_decimal",
        "cast_json",
        "regexp_like",
        "__renamed_pb__",
    ] {
        for (ti, target) in fields.iter().enumerate() {
            for (si, source) in fields.iter().enumerate() {
                for arity in 0..4 {
                    for kind in 0..3 {
                        let suite = EvaluatorSuite::new(
                            vec![pb_root(name, target, source, arity, kind)],
                            false,
                        );
                        for enabled in [false, true] {
                            for (selection_case, selection) in [
                                vec![],
                                vec![1],
                                vec![0, 1, 2],
                                vec![2, 0, 2],
                                vec![usize::MAX],
                            ]
                            .into_iter()
                            .enumerate()
                            {
                                let context = leaf::Context::new(0, Datum::Int(7));
                                context.enabled.set(enabled);
                                let mut input = chunk(
                                    &[ty()],
                                    &[vec![Datum::Int(7)], vec![Datum::Null], vec![Datum::Int(-3)]],
                                );
                                input.set_sel(Some(selection));
                                records.push(format!("pb/{name}/{ti}/{si}/{arity}/{kind}/{enabled}/{selection_case}|{}",observe(&suite,&context,&mut input,target,&mut evaluate,&[ty()])));
                            }
                        }
                    }
                }
            }
        }
    }
    records
}
fn schema_records(
    mut evaluate: impl FnMut(
        &EvaluatorSuite,
        &leaf::Context,
        &mut Chunk,
        &mut Chunk,
        &[FieldType],
    ) -> std::result::Result<(), EvaluatorError>,
) -> Vec<String> {
    let fields = declarations();
    let mut records = Vec::new();
    for (ai, actual) in fields.iter().enumerate() {
        for (di, declared) in fields.iter().enumerate() {
            for name in [
                "abs",
                "plus",
                "cast_decimal",
                "cast_json",
                "upper",
                "coalesce",
            ] {
                let target = match name {
                    "cast_decimal" => fields[5].clone(),
                    "cast_json" => FieldType::new(FieldTypeCode::Json),
                    "upper" => string_ty(),
                    _ => declared.clone(),
                };
                let arg = typed_column(0, declared);
                let args = if matches!(name, "plus" | "coalesce") {
                    vec![arg, literal(Datum::Null, declared.clone())]
                } else {
                    vec![arg]
                };
                let suite = EvaluatorSuite::new(vec![call(name, target.clone(), args)], false);
                let values = match actual.code() {
                    FieldTypeCode::Float => {
                        [Datum::Float32(1.25), Datum::Null, Datum::Float32(-2.5)]
                    }
                    FieldTypeCode::Double => [Datum::Real(1.25), Datum::Null, Datum::Real(-2.5)],
                    FieldTypeCode::NewDecimal => [
                        Datum::Decimal(Decimal::from_literal("1.25")),
                        Datum::Null,
                        Datum::Decimal(Decimal::from_literal("-2.50")),
                    ],
                    FieldTypeCode::Varchar | FieldTypeCode::VarString | FieldTypeCode::String => [
                        Datum::new_string("1.25"),
                        Datum::Null,
                        Datum::new_string("bad"),
                    ],
                    _ => [Datum::Int(7), Datum::Null, Datum::Int(-3)],
                };
                for enabled in [false, true] {
                    for (selection_case, selection) in [
                        vec![],
                        vec![1],
                        vec![0, 1, 2],
                        vec![2, 0, 2],
                        vec![usize::MAX],
                    ]
                    .into_iter()
                    .enumerate()
                    {
                        let context = leaf::Context::new(0, Datum::Int(7));
                        context.enabled.set(enabled);
                        let schema = std::slice::from_ref(actual);
                        let mut input = chunk(
                            schema,
                            &values.iter().cloned().map(|v| vec![v]).collect::<Vec<_>>(),
                        );
                        input.set_sel(Some(selection));
                        records.push(format!(
                            "schema/{ai}/{di}/{name}/{enabled}/{selection_case}|{}",
                            observe(&suite, &context, &mut input, &target, &mut evaluate, schema)
                        ));
                    }
                }
            }
        }
    }
    records
}
fn digest(records: &[String]) -> u64 {
    let mut hash = 0xcbf29ce484222325u64;
    for record in records {
        for b in (record.len() as u64)
            .to_le_bytes()
            .iter()
            .copied()
            .chain(record.bytes())
        {
            hash ^= u64::from(b);
            hash = hash.wrapping_mul(0x100000001b3);
        }
    }
    hash
}

#[test]
fn routing_actual_matches_original_and_has_no_generic_retry() {
    let old_pb = (|| projection_pb_records(|s, c, i, o, _| s.run(c, i, o)))();
    let old_schema = (|| projection_schema_records(|s, c, i, o, _| s.run(c, i, o)))();
    assert_eq!(
        (old_pb.len(), digest(&old_pb)),
        (127008, 0xC738_E3D0_F524_444E)
    );
    assert_eq!(
        (old_schema.len(), digest(&old_schema)),
        (432, 0xE60E_F3AB_FBDA_A21B)
    );
    let mut pb_routes = Vec::new();
    let mut schema_routes = Vec::new();
    let actual_pb = projection_pb_records(|s, c, i, o, schema| {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            s.run_with_tikv_batch(c, i, o, schema)
        }));
        pb_routes.push(s.batch_route_snapshot());
        match result {
            Ok(value) => value,
            Err(payload) => std::panic::resume_unwind(payload),
        }
    });
    let actual_schema = projection_schema_records(|s, c, i, o, schema| {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            s.run_with_tikv_batch(c, i, o, schema)
        }));
        schema_routes.push(s.batch_route_snapshot());
        match result {
            Ok(value) => value,
            Err(payload) => std::panic::resume_unwind(payload),
        }
    });
    let mut differences = 0;
    let mut refusals = 0;
    let mut fallbacks = 0;
    let mut shown = 0;
    for (cohort, old, actual, routes) in [
        ("projection-pb", old_pb, actual_pb, pb_routes),
        (
            "projection-schema",
            old_schema,
            actual_schema,
            schema_routes,
        ),
    ] {
        let (mut group_differences, mut group_refusals, mut group_fallbacks) = (0, 0, 0);
        assert_eq!(old.len(), actual.len());
        assert_eq!(old.len(), routes.len());
        for ((old, actual), route) in old.iter().zip(&actual).zip(&routes) {
            let different = old != actual;
            let refused = route.refusals != 0;
            let fallback = route.fallback != 0;
            group_differences += usize::from(different);
            group_refusals += usize::from(refused);
            group_fallbacks += usize::from(fallback);
            if (different || refused || fallback) && shown < 20 {
                eprintln!("N62 routing witness {cohort} different{different} route{route:?}\nOLD {}\nACTUAL {}",&old[..old.len().min(750)],&actual[..actual.len().min(750)]);
                shown += 1;
            }
        }
        eprintln!("N62 routing {cohort} count={} differences={group_differences} refused_records={group_refusals} fallback_records={group_fallbacks}",old.len());
        differences += group_differences;
        refusals += group_refusals;
        fallbacks += group_fallbacks;
    }
    assert_eq!(differences, 0, "actual values/effects differ");
    assert_eq!(
        (refusals, fallbacks),
        (0, 0),
        "actual generic retry remains"
    );
}

fn wire_declaration(field: &FieldType) -> db_pb::FieldType {
    db_pb::FieldType {
        tp: Some(i32::from(field.code().mysql_type())),
        flag: Some(field.flags()),
        flen: Some(field.flen() as i32),
        decimal: Some(field.decimal() as i32),
        charset: Some(field.charset_name().to_owned()),
        collate: Some(
            -tidb_datatype::get_collation_by_name(field.collation_name())
                .unwrap()
                .id,
        ),
        ..Default::default()
    }
}
fn sealed_pb_root(
    signature: db_pb::ScalarFuncSig,
    name: &str,
    target: &FieldType,
    source: &FieldType,
    arity: usize,
    kind: usize,
) -> Expression {
    let mut args = Vec::new();
    for _ in 0..arity {
        let mut bytes = Vec::new();
        tidb_codec::encode_int(&mut bytes, if kind == 1 { 0 } else { -4 });
        args.push(db_pb::Expr {
            tp: Some(if kind == 1 {
                db_pb::ExprType::ColumnRef
            } else {
                db_pb::ExprType::Int64
            } as i32),
            val: Some(bytes),
            field_type: Some(wire_declaration(source)),
            ..Default::default()
        });
    }
    let node = db_pb::Expr {
        tp: Some(db_pb::ExprType::ScalarFunc as i32),
        sig: Some(signature as i32),
        children: args,
        field_type: Some(wire_declaration(target)),
        ..Default::default()
    };
    let mut root = pb_to_expr(&node, std::slice::from_ref(source)).unwrap();
    let Expression::ScalarFunction(function) = &mut root else {
        unreachable!()
    };
    function.func_name = CiString::new(name);
    assert_eq!(function.get_static_type(), Some(target));
    for arg in &mut function.args {
        assert_eq!(arg.static_type(), Some(source));
        if kind == 2 {
            let Expression::Constant(c) = arg else {
                unreachable!()
            };
            c.param_marker = Some(ParamMarker { order: 0 });
        }
    }
    root
}

fn sealed_pb_records(
    mut evaluate: impl FnMut(
        &EvaluatorSuite,
        &leaf::Context,
        &mut Chunk,
        &mut Chunk,
        &[FieldType],
    ) -> std::result::Result<(), EvaluatorError>,
) -> Vec<String> {
    let fields = declarations();
    let mut records = Vec::new();
    for signature in [
        db_pb::ScalarFuncSig::RegexpLikeSig,
        db_pb::ScalarFuncSig::CastStringAsReal,
        db_pb::ScalarFuncSig::CastRealAsDecimal,
    ] {
        for name in [
            "abs",
            "plus",
            "minus",
            "mul",
            "mod",
            "cast_decimal",
            "cast_json",
            "regexp_like",
            "__renamed_pb__",
        ] {
            for (ti, target) in fields.iter().enumerate() {
                for (si, source) in fields.iter().enumerate() {
                    for arity in 0..4 {
                        for kind in 0..3 {
                            let suite = EvaluatorSuite::new(
                                vec![sealed_pb_root(signature, name, target, source, arity, kind)],
                                false,
                            );
                            for enabled in [false, true] {
                                for (selection_case, selection) in [
                                    vec![],
                                    vec![1],
                                    vec![0, 1, 2],
                                    vec![2, 0, 2],
                                    vec![usize::MAX],
                                ]
                                .into_iter()
                                .enumerate()
                                {
                                    let context = leaf::Context::new(0, Datum::Int(7));
                                    context.enabled.set(enabled);
                                    let mut input = chunk(
                                        &[ty()],
                                        &[
                                            vec![Datum::Int(7)],
                                            vec![Datum::Null],
                                            vec![Datum::Int(-3)],
                                        ],
                                    );
                                    input.set_sel(Some(selection));
                                    records.push(format!("sealed-pb/{signature:?}/{name}/{ti}/{si}/{arity}/{kind}/{enabled}/{selection_case}|{}",observe(&suite,&context,&mut input,target,&mut evaluate,&[ty()])));
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    records
}
#[test]
fn routing_original_sealed_pb_capture() {
    let a = (|| sealed_pb_records(|s, c, i, o, _| s.run(c, i, o)))();
    let b = (|| sealed_pb_records(|s, c, i, o, _| s.run(c, i, o)))();
    assert_eq!(a, b);
    eprintln!("N62 sealed PB original {} {:016X}", a.len(), digest(&a));
    assert_eq!((a.len(), digest(&a)), (158760, 0x739D_B141_9FCC_0361));
}
#[test]
fn routing_prepare_diagnostic() {
    let roots = [
        pb_root("abs", &ty(), &ty(), 0, 0),
        sealed_pb_root(
            db_pb::ScalarFuncSig::RegexpLikeSig,
            "abs",
            &real_ty(),
            &real_ty(),
            1,
            0,
        ),
        sealed_pb_root(
            db_pb::ScalarFuncSig::CastStringAsReal,
            "abs",
            &real_ty(),
            &real_ty(),
            1,
            0,
        ),
        call("abs", real_ty(), vec![typed_column(0, &real_ty())]),
    ];
    for (index, root) in roots.into_iter().enumerate() {
        let suite = EvaluatorSuite::new(vec![root], false);
        let result = PreparedBatchSuite::compile(
            &suite,
            &[ty()],
            true,
            0,
            BatchSourceLimits {
                tree: CompileLimits::default(),
                max_metadata_bytes: 1 << 20,
            },
            ExecutionLimits::default(),
            8 << 20,
        );
        match (index, &result) {
            (0 | 3, Err(error)) => assert_eq!(error.refusal(), BatchRefusal::Contract),
            (1 | 2, Ok(_)) => {}
            _ => panic!("unexpected preparation result at {index}"),
        }
        eprintln!(
            "N62 preparediagnostic {index} {}",
            match result {
                Ok(_) => "PREPARED".to_owned(),
                Err(error) => format!("{error:?}"),
            }
        );
    }
}

fn projection_values(field: &FieldType) -> [Datum; 3] {
    match field.code() {
        FieldTypeCode::Float => [Datum::Float32(1.25), Datum::Null, Datum::Float32(-2.5)],
        FieldTypeCode::Double => [Datum::Real(1.25), Datum::Null, Datum::Real(-2.5)],
        FieldTypeCode::NewDecimal => [
            Datum::Decimal(Decimal::from_literal("1.25")),
            Datum::Null,
            Datum::Decimal(Decimal::from_literal("-2.50")),
        ],
        FieldTypeCode::Varchar | FieldTypeCode::VarString | FieldTypeCode::String => [
            Datum::new_string("1.25"),
            Datum::Null,
            Datum::new_string("bad"),
        ],
        _ if field.is_unsigned() => [Datum::UInt(7), Datum::Null, Datum::UInt(u64::MAX)],
        _ => [Datum::Int(7), Datum::Null, Datum::Int(-3)],
    }
}

fn projection_pb_records(
    mut evaluate: impl FnMut(
        &EvaluatorSuite,
        &leaf::Context,
        &mut Chunk,
        &mut Chunk,
        &[FieldType],
    ) -> std::result::Result<(), EvaluatorError>,
) -> Vec<String> {
    let fields = declarations();
    let mut records = Vec::new();
    for signature in [
        db_pb::ScalarFuncSig::RegexpLikeSig,
        db_pb::ScalarFuncSig::CastStringAsReal,
        db_pb::ScalarFuncSig::CastRealAsDecimal,
    ] {
        for name in [
            "abs",
            "plus",
            "minus",
            "mul",
            "mod",
            "cast_decimal",
            "cast_json",
            "regexp_like",
            "__renamed_pb__",
        ] {
            for (ti, target) in fields.iter().enumerate() {
                for (si, source) in fields.iter().enumerate() {
                    for arity in 0..4 {
                        for kind in 0..3 {
                            let suite = EvaluatorSuite::new(
                                vec![sealed_pb_root(signature, name, target, source, arity, kind)],
                                false,
                            );
                            for enabled in [false, true] {
                                for (selection_case, selection) in
                                    [vec![], vec![1], vec![0, 1, 2], vec![2, 0, 2]]
                                        .into_iter()
                                        .enumerate()
                                {
                                    let context = leaf::Context::new(0, Datum::Int(7));
                                    context.enabled.set(enabled);
                                    let actual = if kind == 1 { source.clone() } else { ty() };
                                    let schema = std::slice::from_ref(&actual);
                                    let values = projection_values(&actual);
                                    let mut input = chunk(
                                        schema,
                                        &values.into_iter().map(|v| vec![v]).collect::<Vec<_>>(),
                                    );
                                    input.set_sel(Some(selection));
                                    records.push(format!("projection-pb/{signature:?}/{name}/{ti}/{si}/{arity}/{kind}/{enabled}/{selection_case}|{}",observe(&suite,&context,&mut input,target,&mut evaluate,schema)));
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    records
}
fn projection_schema_records(
    mut evaluate: impl FnMut(
        &EvaluatorSuite,
        &leaf::Context,
        &mut Chunk,
        &mut Chunk,
        &[FieldType],
    ) -> std::result::Result<(), EvaluatorError>,
) -> Vec<String> {
    let fields = declarations();
    let mut records = Vec::new();
    for (ai, actual) in fields.iter().enumerate() {
        for (di, declared) in fields.iter().enumerate() {
            if actual.code() != declared.code() || get_fixed_len(actual) != get_fixed_len(declared)
            {
                continue;
            }
            for name in [
                "abs",
                "plus",
                "cast_decimal",
                "cast_json",
                "upper",
                "coalesce",
            ] {
                let target = match name {
                    "cast_decimal" => fields[5].clone(),
                    "cast_json" => FieldType::new(FieldTypeCode::Json),
                    "upper" => string_ty(),
                    _ => declared.clone(),
                };
                let arg = typed_column(0, declared);
                let args = if matches!(name, "plus" | "coalesce") {
                    vec![arg, literal(Datum::Null, declared.clone())]
                } else {
                    vec![arg]
                };
                let suite = EvaluatorSuite::new(vec![call(name, target.clone(), args)], false);
                let values = match actual.code() {
                    FieldTypeCode::Float => {
                        [Datum::Float32(1.25), Datum::Null, Datum::Float32(-2.5)]
                    }
                    FieldTypeCode::Double => [Datum::Real(1.25), Datum::Null, Datum::Real(-2.5)],
                    FieldTypeCode::NewDecimal => [
                        Datum::Decimal(Decimal::from_literal("1.25")),
                        Datum::Null,
                        Datum::Decimal(Decimal::from_literal("-2.50")),
                    ],
                    FieldTypeCode::Varchar | FieldTypeCode::VarString | FieldTypeCode::String => [
                        Datum::new_string("1.25"),
                        Datum::Null,
                        Datum::new_string("bad"),
                    ],
                    _ => [Datum::Int(7), Datum::Null, Datum::Int(-3)],
                };
                for enabled in [false, true] {
                    for (selection_case, selection) in
                        [vec![], vec![1], vec![0, 1, 2], vec![2, 0, 2]]
                            .into_iter()
                            .enumerate()
                    {
                        let context = leaf::Context::new(0, Datum::Int(7));
                        context.enabled.set(enabled);
                        let schema = std::slice::from_ref(actual);
                        let mut input = chunk(
                            schema,
                            &values.iter().cloned().map(|v| vec![v]).collect::<Vec<_>>(),
                        );
                        input.set_sel(Some(selection));
                        records.push(format!(
                            "projection-schema/{ai}/{di}/{name}/{enabled}/{selection_case}|{}",
                            observe(&suite, &context, &mut input, &target, &mut evaluate, schema)
                        ));
                    }
                }
            }
        }
    }
    records
}
#[test]
fn routing_original_projection_contract_capture() {
    let a = (|| projection_pb_records(|s, c, i, o, _| s.run(c, i, o)))();
    let b = (|| projection_pb_records(|s, c, i, o, _| s.run(c, i, o)))();
    assert_eq!(a, b);
    eprintln!("N62 Projection PB original {} {:016X}", a.len(), digest(&a));
    assert_eq!((a.len(), digest(&a)), (127008, 0xC738_E3D0_F524_444E));
    let a = (|| projection_schema_records(|s, c, i, o, _| s.run(c, i, o)))();
    let b = (|| projection_schema_records(|s, c, i, o, _| s.run(c, i, o)))();
    assert_eq!(a, b);
    eprintln!(
        "N62 Projection schema original {} {:016X}",
        a.len(),
        digest(&a)
    );
    assert_eq!((a.len(), digest(&a)), (432, 0xE60E_F3AB_FBDA_A21B));
}

#[test]
fn routing_stale_pb_owner_is_terminal_binding_failure_without_sql_callbacks() {
    let mut count = 0;
    for field in declarations() {
        for arity in 0..4 {
            let root = pb_root("regexp_like", &field, &ty(), arity, 0);
            let suite = EvaluatorSuite::new(vec![root], false);
            let failure = PreparedBatchSuite::compile(
                &suite,
                &[ty()],
                true,
                0,
                BatchSourceLimits {
                    tree: CompileLimits::default(),
                    max_metadata_bytes: 1 << 20,
                },
                ExecutionLimits::default(),
                8 << 20,
            )
            .err()
            .expect("stale PB owner accepted");
            assert_eq!(
                failure.refusal(),
                BatchRefusal::Contract,
                "metadata seal is a binding contract"
            );
            let context = leaf::Context::new(0, Datum::Int(7));
            let mut input = chunk(&[ty()], &[vec![Datum::Int(7)]]);
            let mut output = output();
            for _ in 0..3 {
                assert!(suite
                    .run_with_tikv_batch(&context, &mut input, &mut output, &[ty()])
                    .is_err());
                let route = suite.batch_route_snapshot();
                assert_eq!((route.prepared, route.refusals, route.fallback), (0, 1, 0));
                assert_eq!(route.refusal, Some(BatchRefusal::Contract));
            }
            assert!(context.events.borrow().is_empty());
            assert_eq!(output.num_rows(), 0);
            count += 1;
        }
    }
    assert_eq!(count, 28);
    let mut root = sealed_pb_root(
        db_pb::ScalarFuncSig::RegexpLikeSig,
        "regexp_like",
        &ty(),
        &ty(),
        2,
        0,
    );
    let Expression::ScalarFunction(f) = &mut root else {
        unreachable!()
    };
    f.args.pop();
    let suite = EvaluatorSuite::new(vec![root], false);
    let failure = PreparedBatchSuite::compile(
        &suite,
        &[ty()],
        true,
        0,
        BatchSourceLimits {
            tree: CompileLimits::default(),
            max_metadata_bytes: 1 << 20,
        },
        ExecutionLimits::default(),
        8 << 20,
    )
    .err()
    .expect("stale child count accepted");
    assert_eq!(failure.refusal(), BatchRefusal::Contract);
}
#[test]
fn routing_invalid_projection_schema_and_coordinates_do_not_retry() {
    for root in [
        call("abs", real_ty(), vec![typed_column(0, &real_ty())]),
        call("abs", ty(), vec![typed_column(99, &ty())]),
    ] {
        let suite = EvaluatorSuite::new(vec![root], false);
        let context = leaf::Context::new(0, Datum::Int(7));
        let mut input = chunk(&[ty()], &[vec![Datum::Int(7)]]);
        let mut output = output();
        assert!(suite
            .run_with_tikv_batch(&context, &mut input, &mut output, &[ty()])
            .is_err());
        assert_eq!(suite.batch_route_snapshot().fallback, 0);
        assert!(context.events.borrow().is_empty());
    }
    let suite = EvaluatorSuite::new(
        vec![call("coalesce", ty(), vec![int(1), typed_column(0, &ty())])],
        false,
    );
    let context = leaf::Context::new(0, Datum::Int(7));
    let mut input = chunk(&[ty()], &[vec![Datum::Int(7)]]);
    input.set_sel(Some(vec![usize::MAX]));
    let mut output = output();
    assert!(suite
        .run_with_tikv_batch(&context, &mut input, &mut output, &[ty()])
        .is_err());
    assert_eq!(suite.batch_route_snapshot().fallback, 0);
    assert!(context.events.borrow().is_empty());
    assert_eq!(output.num_rows(), 0);
}

fn scope_records(original: bool) -> Vec<String> {
    use std::cell::RefCell;
    struct Context {
        cache: ReadyValueCache,
        events: RefCell<Vec<String>>,
        enabled: bool,
        value: Datum,
    }
    impl Columns for Context {
        fn get(&self, _: &[String]) -> Option<Datum> {
            panic!("no named row")
        }
        fn ready_value_cache(&self) -> Option<&ReadyValueCache> {
            self.events.borrow_mut().push("cache".into());
            Some(&self.cache)
        }
        fn enable_vectorized_expression(&self) -> bool {
            self.events.borrow_mut().push("flag".into());
            self.enabled
        }
        fn param_value(&self, order: usize) -> std::result::Result<Datum, EvalError> {
            self.events.borrow_mut().push(format!("param:{order}"));
            Ok(self.value.clone())
        }
    }
    let mut records = Vec::new();
    for name in [
        "isnull",
        "istrue",
        "isfalse",
        "isnottrue",
        "isnotfalse",
        "isunknown",
        "isnotunknown",
        "istrue_with_null",
        "not",
        "any_value",
        "lower",
        "upper",
        "ifnull",
        "coalesce",
    ] {
        for enabled in [false, true] {
            for poisoned in [false, true] {
                for selection in [vec![], vec![0], vec![1, 0, 1]] {
                    for null in [false, true] {
                        let context = Context {
                            cache: ReadyValueCache::new(),
                            events: RefCell::new(Vec::new()),
                            enabled,
                            value: if null {
                                Datum::Null
                            } else {
                                Datum::new_string("1tail")
                            },
                        };
                        if poisoned {
                            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                let _guard = context.cache.preparation_unwind_guard();
                                panic!("original scope poison");
                            }));
                        }
                        let string = matches!(
                            name,
                            "any_value" | "lower" | "upper" | "ifnull" | "coalesce"
                        );
                        let field = if string { string_ty() } else { ty() };
                        let mut parameter = Constant::new(Datum::Null, string_ty());
                        parameter.param_marker = Some(ParamMarker { order: 0 });
                        let mut args = vec![Expression::Constant(parameter)];
                        if matches!(name, "ifnull" | "coalesce") {
                            args.push(literal(Datum::new_string("fallback"), string_ty()));
                        }
                        let suite =
                            EvaluatorSuite::new(vec![call(name, field.clone(), args)], false);
                        for reuse in 0..2 {
                            let mut input =
                                chunk(&[ty()], &[vec![Datum::Int(1)], vec![Datum::Int(2)]]);
                            input.set_sel(Some(selection.clone()));
                            let mut output = Chunk::new_empty(std::slice::from_ref(&field));
                            output.append_null(0);
                            let result =
                                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                    if original {
                                        (|| suite.run(&context, &mut input, &mut output))()
                                    } else {
                                        suite.run_with_tikv_batch(
                                            &context,
                                            &mut input,
                                            &mut output,
                                            &[ty()],
                                        )
                                    }
                                }));
                            let status = match result {
                                Ok(v) => format!("{v:?}"),
                                Err(_) => "PANIC".into(),
                            };
                            let cells = (0..output.num_rows())
                                .map(|i| leaf::frame(&output.get_row(i).get_datum(0, &field)))
                                .collect::<Vec<_>>();
                            records.push(format!("{name}/{enabled}/{poisoned}/{selection:?}/{null}/{reuse}|{status}|{cells:?}|{:?}",context.events.borrow()));
                        }
                    }
                }
            }
        }
    }
    records
}
#[test]
fn routing_original_row_scope_capture() {
    let a = scope_records(true);
    let b = scope_records(true);
    assert_eq!(a, b);
    eprintln!("N62 scope original {} {:016X}", a.len(), digest(&a));
    assert_eq!((a.len(), digest(&a)), (672, 0x2B6B_C730_362F_A53D));
}
#[test]
fn routing_row_scopes_match_original_cache_binding_and_poison() {
    // Native Suite scope originals remain separate: this is the actual
    // pre-change Projection entry, including its already shared row profiles.
    let old: Vec<String> =
        serde_json::from_str(include_str!("batch_projection_scope_original.json")).unwrap();
    assert_eq!((old.len(), digest(&old)), (672, 0xEA60_639F_6C88_EF85));
    let new = scope_records(false);
    assert_eq!(old.len(), new.len());
    let mut differences = 0;
    for (a, b) in old.iter().zip(new) {
        if a != &b {
            if differences < 8 {
                eprintln!("N62 Projection scope difference\nOLD {a}\nNEW {b}");
            }
            differences += 1;
        }
    }
    assert_eq!(
        differences, 0,
        "actual pre-change Projection scope contract"
    );
}

#[test]
fn routing_projection_scope_entry_capture() {
    let a = scope_records(false);
    let b = scope_records(false);
    assert_eq!(a, b);
    eprintln!(
        "N62 Projection scope contract {} {:016X}",
        a.len(),
        digest(&a)
    );
    assert_eq!((a.len(), digest(&a)), (672, 0xEA60_639F_6C88_EF85));
}
