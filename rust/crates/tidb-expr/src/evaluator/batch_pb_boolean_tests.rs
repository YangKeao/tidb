// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Original selected Boolean PB raw demand, truth conversion and short circuit contracts.
use super::*;
fn specs() -> Vec<(Sig, usize)> {
    vec![
        (Sig::LogicalAnd, 2),
        (Sig::LogicalOr, 2),
        (Sig::IntIsNull, 1),
        (Sig::RealIsNull, 1),
        (Sig::DecimalIsNull, 1),
        (Sig::StringIsNull, 1),
        (Sig::TimeIsNull, 1),
        (Sig::DurationIsNull, 1),
        (Sig::VectorFloat32IsNull, 1),
        (Sig::UnaryNotInt, 1),
        (Sig::IntIsTrueWithNull, 1),
    ]
}
fn smoke(_: Sig, arity: usize) -> Vec<Datum> {
    let mut values = vec![Datum::Int(4), Datum::Int(2)];
    values.resize(arity, Datum::Int(4));
    values
}
fn make(
    sig: Sig,
    source: usize,
    result: i32,
    flags: Option<u32>,
    name: &str,
    shape: usize,
    values: &[Datum],
) -> EvaluatorSuite {
    let stored = Datum::Bytes(b"stored".to_vec());
    let mut p = pb_call(
        values
            .iter()
            .map(|value| {
                atom(
                    if shape == 0 { value } else { &stored },
                    Some(declaration(
                        [8, 253, 5, 246, 245, 12, 11, 16, 247, 248][source],
                        Some(0),
                    )),
                )
            })
            .collect(),
        Some(declaration(result, flags)),
    );
    p.sig = Some(sig as i32);
    let mut root = crate::distsql_builtin::pb_to_expr(&p, &[]).unwrap();
    let Expression::ScalarFunction(f) = &mut root else {
        panic!()
    };
    assert_eq!(f.pb_signature(), Some(sig));
    if name != "canonical" {
        f.func_name = CiString::new(name);
    }
    for (i, arg) in f.args.iter_mut().enumerate() {
        let Expression::Constant(c) = arg else {
            panic!()
        };
        match shape {
            0 => (),
            1 => c.param_marker = Some(crate::constant::ParamMarker { order: i as i64 }),
            3 => {
                c.deferred_expr = Some(Box::new(call(
                    "setvar",
                    string_ty(),
                    vec![
                        literal(Datum::new_string(format!("binary-{i}")), string_ty()),
                        parameter(i, string_ty()),
                    ],
                )))
            }
            _ => unreachable!(),
        }
    }
    suite(root)
}
fn fixed_clock(body: impl FnOnce()) {
    let now = chrono::DateTime::parse_from_rfc3339("2026-10-21T00:00:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc);
    tidb_datatype::with_test_conversion_clock(now, body)
}
fn case(
    owner: &EvaluatorSuite,
    actual: &EvaluatorSuite,
    old: &C,
    new: &C,
    selection: &Option<Vec<usize>>,
    compare: bool,
    label: &str,
    hash: &mut u64,
    count: &mut usize,
    gaps: &mut Vec<String>,
) {
    let ty = output_type(owner);
    let mut a = input(selection);
    let mut left = output_for(&ty);
    left.append_null(0);
    let expected = original_session_read_outcome(|| owner.run(old, &mut a, &mut left));
    if compare {
        let mut b = input(selection);
        let mut right = output_for(&ty);
        right.append_null(0);
        let got = session_read_outcome(|| {
            actual.run_with_tikv_batch(new, &mut b, &mut right, &[string_ty()])
        });
        if got != expected || right.column(0) != left.column(0) || new.snapshot() != old.snapshot()
        {
            eprintln!("BOOLEAN DIFFERENCE {label} old={expected} new={got} oldcol={:?} newcol={:?} oldctx={} newctx={}",left.column(0),right.column(0),old.snapshot(),new.snapshot());
        }
        assert_eq!(got, expected, "{label}");
        assert_eq!(right.column(0), left.column(0), "{label}");
        assert_eq!(new.snapshot(), old.snapshot(), "{label}");
        let route = actual.batch_route_snapshot();
        if (route.prepared == 0 || route.refusals != 0 || route.fallback != 0) && gaps.len() < 8 {
            gaps.push(format!("{label}:{route:?}"));
        }
    }
    observe(
        hash,
        &format!("{label}|{expected}|{:?}|{}", left.column(0), old.snapshot()),
    );
    *count += 1;
}
#[test]
fn pb_boolean_source_original_phases() {
    if isolate_common_corpus(
        "stateful_tests::pb_regexp_tests::eager_tests::boolean_tests::pb_boolean_source_original_phases",
    ) {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    matrix(false, false);
}
#[test]
fn pb_boolean_source_actual_phases() {
    if isolate_common_corpus(
        "stateful_tests::pb_regexp_tests::eager_tests::boolean_tests::pb_boolean_source_actual_phases",
    ) {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    matrix(true, false);
}
#[test]
fn pb_boolean_source_original_full() {
    if isolate_common_corpus(
        "stateful_tests::pb_regexp_tests::eager_tests::boolean_tests::pb_boolean_source_original_full",
    ) {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    matrix(false, true);
}
#[test]
fn pb_boolean_source_actual_full() {
    if isolate_common_corpus(
        "stateful_tests::pb_regexp_tests::eager_tests::boolean_tests::pb_boolean_source_actual_full",
    ) {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    matrix(true, true);
}
fn matrix(compare: bool, full: bool) {
    fixed_clock(|| {
        let mut count = 0;
        let mut hash = 0xcbf29ce484222325;
        let mut gaps = vec![];
        for (sig, standard) in specs() {
            for arity in if full {
                vec![standard]
            } else {
                vec![0, 1, 2, 3, 9]
            } {
                let base = smoke(sig, arity);
                let mut samples = vec![(0, base.clone())];
                if full {
                    samples.clear();
                    for position in 0..arity {
                        for (vi, value) in crate::tikv::byte_unary_value_tests::values()
                            .into_iter()
                            .enumerate()
                        {
                            let mut values = base.clone();
                            values[position] = value;
                            samples.push((1 + 149 * position + vi, values));
                        }
                    }
                } else if arity > 0 {
                    for sample in 1..7 {
                        let mut values = base.clone();
                        let index = if sample == 2 { arity - 1 } else { 0 };
                        values[index] = if sample <= 2 {
                            Datum::Null
                        } else if sample == 3 {
                            Datum::Bytes(vec![255])
                        } else if sample == 4 {
                            Datum::Int(0)
                        } else if sample == 5 {
                            Datum::Int(1)
                        } else {
                            Datum::new_string("invalid number")
                        };
                        samples.push((sample, values));
                    }
                }
                for source in 0..2 {
                    for result in if full {
                        vec![8, 245, 12]
                    } else {
                        vec![8, 5, 246, 245, 253, 6, 13, 16, 247, 248, 12, 11]
                    } {
                        for flags in [None, Some(32)] {
                            for name in if full {
                                vec!["wire display", "char_length"]
                            } else {
                                vec![
                                    "canonical",
                                    "wire display",
                                    "coalesce",
                                    "char_length",
                                    "plus",
                                    "cast_json",
                                    "cast_decimal",
                                    "abs",
                                    "not",
                                    "isnull",
                                    "and",
                                    "or",
                                    "case",
                                    "if",
                                    "ifnull",
                                ]
                            } {
                                for shape in if full { vec![1] } else { vec![0, 1, 3] } {
                                    for (vi, values) in &samples {
                                        for profile in if full { vec![0] } else { vec![0, 3, 7] } {
                                            for enabled in [false, true] {
                                                for selection in if full {
                                                    vec![None, Some(vec![2, 0, 2]), Some(vec![])]
                                                } else {
                                                    vec![None, Some(vec![])]
                                                } {
                                                    let owner = make(
                                                        sig, source, result, flags, name, shape,
                                                        values,
                                                    );
                                                    let actual = make(
                                                        sig, source, result, flags, name, shape,
                                                        values,
                                                    );
                                                    let old =
                                                        C::new(enabled, values.clone(), profile);
                                                    let new =
                                                        C::new(enabled, values.clone(), profile);
                                                    for reuse in 0..if full { 2 } else { 1 } {
                                                        let label=format!("pb-boolean/{full}/{sig:?}/{arity}/{source}/{result}/{flags:?}/{name}/{shape}/{vi}/{profile}/{enabled}/{selection:?}/{reuse}");
                                                        case(
                                                            &owner, &actual, &old, &new,
                                                            &selection, compare, &label, &mut hash,
                                                            &mut count, &mut gaps,
                                                        );
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        eprintln!("PB BOOLEAN ORIGINAL full={full} {count}/{hash:016X}");
        assert_eq!(
            (count, hash),
            if full {
                (557856, 0xB32D19558ECCC945)
            } else {
                (8268480, 0x422ED1E676E8C1D7)
            }
        );
        if !gaps.is_empty() {
            eprintln!("PB BOOLEAN SOURCE RED {gaps:?}");
        }
        assert!(gaps.is_empty());
    });
}
#[test]
fn pb_boolean_source_original_long() {
    if isolate_common_corpus(
        "stateful_tests::pb_regexp_tests::eager_tests::boolean_tests::pb_boolean_source_original_long",
    ) {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    long(false);
}
#[test]
fn pb_boolean_source_actual_long() {
    if isolate_common_corpus(
        "stateful_tests::pb_regexp_tests::eager_tests::boolean_tests::pb_boolean_source_actual_long",
    ) {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    long(true);
}
fn long(compare: bool) {
    fixed_clock(|| {
        let mut count = 0;
        let mut hash = 0xcbf29ce484222325;
        let mut gaps = vec![];
        for (sig, _) in specs() {
            let arity = 2; // Genuine ignored RHS on unary owners, never a synthetic signature.
            for result in [8, 245] {
                for name in ["canonical", "coalesce"] {
                    for shape in [1, 3] {
                        for profile in [0, 3, 7] {
                            for n in [0usize, 1, 3, 1026, 2051] {
                                for selected in [false, true] {
                                    let values = smoke(sig, arity);
                                    let owner = make(sig, 0, result, None, name, shape, &values);
                                    let actual = make(sig, 0, result, None, name, shape, &values);
                                    let ty = output_type(&owner);
                                    let mut old = C::new(false, values.clone(), profile);
                                    let mut new = C::new(false, values.clone(), profile);
                                    for reuse in 0..2 {
                                        for (round, enabled) in
                                            [false, true, true, false, true, false, true]
                                                .into_iter()
                                                .enumerate()
                                        {
                                            let mut values = values.clone();
                                            match round {
                                                1 => {
                                                    values[0] = Datum::Int(i64::MIN);
                                                    values[1] = Datum::Int(-1);
                                                }
                                                2 => values[1] = Datum::Int(0),
                                                3 => values[0] = Datum::Null,
                                                4 => {
                                                    values[0] = Datum::Int(0);
                                                    values[1] = Datum::Bytes(vec![255]);
                                                }
                                                5 => {
                                                    values[0] = Datum::Int(1);
                                                    values[1] = Datum::new_string("invalid number");
                                                }
                                                6 => {
                                                    values[0] = Datum::Null;
                                                    values[1] = Datum::Int(0);
                                                }
                                                _ => (),
                                            }
                                            old.values = values.clone();
                                            new.values = values;
                                            old.enabled.set(enabled);
                                            new.enabled.set(enabled);
                                            let id = if round < 2 { 17 } else { 18 };
                                            old.id.set(id);
                                            new.id.set(id);
                                            let build = || {
                                                let mut chunk = Chunk::new_with_capacity(
                                                    &[string_ty()],
                                                    n.max(1),
                                                );
                                                for i in 0..n {
                                                    chunk.append_datum(
                                                        0,
                                                        &Datum::new_string(format!("row-{i}")),
                                                    );
                                                }
                                                if selected {
                                                    chunk.set_sel(Some(if n == 0 {
                                                        vec![]
                                                    } else {
                                                        vec![n - 1, 0, n - 1]
                                                    }));
                                                }
                                                chunk
                                            };
                                            let label=format!("pb-boolean-long/{sig:?}/{result}/{name}/{shape}/{profile}/{n}/{selected}/{reuse}/{round}/{enabled}/{id}");
                                            let mut a = build();
                                            let mut left = output_for(&ty);
                                            left.append_null(0);
                                            let expected = original_session_read_outcome(|| {
                                                owner.run(&old, &mut a, &mut left)
                                            });
                                            if compare {
                                                let mut b = build();
                                                let mut right = output_for(&ty);
                                                right.append_null(0);
                                                let got = session_read_outcome(|| {
                                                    actual.run_with_tikv_batch(
                                                        &new,
                                                        &mut b,
                                                        &mut right,
                                                        &[string_ty()],
                                                    )
                                                });
                                                if got != expected
                                                    || right.column(0) != left.column(0)
                                                    || new.snapshot() != old.snapshot()
                                                {
                                                    eprintln!("BINARY LONG DIFFERENCE {label} old={expected} new={got} oldctx={} newctx={}",old.snapshot(),new.snapshot());
                                                }
                                                assert_eq!(got, expected, "{label}");
                                                assert_eq!(
                                                    right.column(0),
                                                    left.column(0),
                                                    "{label}"
                                                );
                                                assert_eq!(
                                                    new.snapshot(),
                                                    old.snapshot(),
                                                    "{label}"
                                                );
                                                let route = actual.batch_route_snapshot();
                                                if (route.prepared == 0
                                                    || route.fallback != 0
                                                    || route.refusals != 0)
                                                    && gaps.len() < 8
                                                {
                                                    gaps.push(format!("{label}:{route:?}"));
                                                }
                                            }
                                            observe(
                                                &mut hash,
                                                &format!(
                                                    "{label}|{expected}|{:?}|{}",
                                                    left.column(0),
                                                    old.snapshot()
                                                ),
                                            );
                                            count += 1;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        eprintln!("PB BOOLEAN LONG ORIGINAL {count}/{hash:016X}");
        assert_eq!((count, hash), (36960, 0x3437114FD0BF3C6D));
        if !gaps.is_empty() {
            eprintln!("PB BOOLEAN LONG SOURCE RED {gaps:?}");
        }
        assert!(gaps.is_empty());
    });
}
#[test]
fn pb_boolean_source_original_physical() {
    if isolate_common_corpus("stateful_tests::pb_regexp_tests::eager_tests::boolean_tests::pb_boolean_source_original_physical"){return;}
    std::panic::set_hook(Box::new(|_| {}));
    physical(false);
}
#[test]
fn pb_boolean_source_actual_physical() {
    if isolate_common_corpus(
        "stateful_tests::pb_regexp_tests::eager_tests::boolean_tests::pb_boolean_source_actual_physical",
    ) {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    physical(true);
}
fn physical(compare: bool) {
    fixed_clock(|| {
        let mut count = 0;
        let mut hash = 0xcbf29ce484222325;
        let mut gaps = vec![];
        for (sig, arity) in specs() {
            for result in [8, 246, 245, 16] {
                for flags in [None, Some(32)] {
                    for name in ["canonical", "coalesce", "char_length", "plus", "cast_json"] {
                        for nested in [false, true] {
                            for sample in 0..4 {
                                for enabled in [false, true] {
                                    for n in [0usize, 3, 1026] {
                                        for selection in 0..3 {
                                            let schema = vec![string_ty(); arity];
                                            let build_owner = || {
                                                let mut p = pb_call(
                                                    (0..arity)
                                                        .map(|i| {
                                                            let mut index = vec![];
                                                            tidb_codec::encode_int(
                                                                &mut index, i as i64,
                                                            );
                                                            let argument = wire::Expr {
                                                                tp: Some(
                                                                    wire::ExprType::ColumnRef
                                                                        as i32,
                                                                ),
                                                                val: Some(index),
                                                                field_type: Some(declaration(
                                                                    253,
                                                                    Some(0),
                                                                )),
                                                                ..Default::default()
                                                            };
                                                            if nested && i == arity - 1 {
                                                                wire::Expr {
                                                                    tp: Some(
                                                                        wire::ExprType::ScalarFunc
                                                                            as i32,
                                                                    ),
                                                                    sig: Some(
                                                                        Sig::UpperUtf8 as i32,
                                                                    ),
                                                                    children: vec![argument],
                                                                    field_type: Some(declaration(
                                                                        253,
                                                                        Some(0),
                                                                    )),
                                                                    ..Default::default()
                                                                }
                                                            } else {
                                                                argument
                                                            }
                                                        })
                                                        .collect(),
                                                    Some(declaration(result, flags)),
                                                );
                                                p.sig = Some(sig as i32);
                                                let mut root =
                                                    crate::distsql_builtin::pb_to_expr(&p, &schema)
                                                        .unwrap();
                                                let Expression::ScalarFunction(f) = &mut root
                                                else {
                                                    panic!()
                                                };
                                                if name != "canonical" {
                                                    f.func_name = CiString::new(name);
                                                }
                                                suite(root)
                                            };
                                            let owner = build_owner();
                                            let actual = build_owner();
                                            let ty = output_type(&owner);
                                            let old = C::new(enabled, vec![], 0);
                                            let new = C::new(enabled, vec![], 0);
                                            let build = || {
                                                let mut chunk =
                                                    Chunk::new_with_capacity(&schema, n.max(1));
                                                for row in 0..n {
                                                    let mut values = smoke(sig, arity);
                                                    for i in 0..arity {
                                                        let value = if (sample == 1
                                                            && i == 0
                                                            && row % 3 == 1)
                                                            || (sample == 2
                                                                && i == arity - 1
                                                                && row % 3 == 2)
                                                        {
                                                            Datum::Null
                                                        } else if (sample == 3
                                                            && i == 0
                                                            && row % 3 == 1)
                                                            || (sample == 4
                                                                && i == arity - 1
                                                                && row % 3 == 2)
                                                        {
                                                            Datum::Bytes(vec![255])
                                                        } else if sample == 5
                                                            && i == 1
                                                            && row % 3 == 1
                                                        {
                                                            Datum::Bytes(b"invalid number".to_vec())
                                                        } else {
                                                            match std::mem::replace(
                                                                &mut values[i],
                                                                Datum::Null,
                                                            ) {
                                                                Datum::Int(value) => Datum::Bytes(
                                                                    value.to_string().into_bytes(),
                                                                ),
                                                                Datum::String(value) => {
                                                                    Datum::Bytes(
                                                                        value.bytes().to_vec(),
                                                                    )
                                                                }
                                                                other => other,
                                                            }
                                                        };
                                                        chunk.append_datum(i, &value);
                                                    }
                                                }
                                                match selection {
                                                    1 => chunk.set_sel(Some(if n == 0 {
                                                        vec![]
                                                    } else {
                                                        vec![n - 1, 0, n - 1]
                                                    })),
                                                    2 => chunk.set_sel(Some(vec![])),
                                                    _ => (),
                                                };
                                                chunk
                                            };
                                            for reuse in 0..2 {
                                                let label=format!("pb-boolean-physical/{sig:?}/{result}/{flags:?}/{name}/{nested}/{sample}/{enabled}/{n}/{selection}/{reuse}");
                                                let mut a = build();
                                                let mut left = output_for(&ty);
                                                left.append_null(0);
                                                let expected =
                                                    original_session_read_outcome(|| {
                                                        owner.run(&old, &mut a, &mut left)
                                                    });
                                                if compare {
                                                    let mut b = build();
                                                    let mut right = output_for(&ty);
                                                    right.append_null(0);
                                                    let got = session_read_outcome(|| {
                                                        actual.run_with_tikv_batch(
                                                            &new, &mut b, &mut right, &schema,
                                                        )
                                                    });
                                                    if got != expected
                                                        || right.column(0) != left.column(0)
                                                        || new.snapshot() != old.snapshot()
                                                    {
                                                        eprintln!("BINARY PHYSICAL DIFFERENCE {label} old={expected} new={got} oldctx={} newctx={}",old.snapshot(),new.snapshot());
                                                    }
                                                    assert_eq!(got, expected, "{label}");
                                                    assert_eq!(
                                                        right.column(0),
                                                        left.column(0),
                                                        "{label}"
                                                    );
                                                    assert_eq!(
                                                        new.snapshot(),
                                                        old.snapshot(),
                                                        "{label}"
                                                    );
                                                    let route = actual.batch_route_snapshot();
                                                    if (route.prepared == 0
                                                        || route.fallback != 0
                                                        || route.refusals != 0)
                                                        && gaps.len() < 8
                                                    {
                                                        gaps.push(format!("{label}:{route:?}"));
                                                    }
                                                }
                                                observe(
                                                    &mut hash,
                                                    &format!(
                                                        "{label}|{expected}|{:?}|{}",
                                                        left.column(0),
                                                        old.snapshot()
                                                    ),
                                                );
                                                count += 1;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        eprintln!("PB BOOLEAN PHYSICAL ORIGINAL {count}/{hash:016X}");
        assert_eq!((count, hash), (126720, 0x5A7FAB439B4E7FBD));
        if !gaps.is_empty() {
            eprintln!("PB BOOLEAN PHYSICAL SOURCE RED {gaps:?}");
        }
        assert!(gaps.is_empty());
    });
}

#[test]
fn pb_boolean_source_original_literal_flags() {
    if isolate_common_corpus("stateful_tests::pb_regexp_tests::eager_tests::boolean_tests::pb_boolean_source_original_literal_flags"){return;}
    std::panic::set_hook(Box::new(|_| {}));
    literal_flags(false);
}
#[test]
fn pb_boolean_source_actual_literal_flags() {
    if isolate_common_corpus("stateful_tests::pb_regexp_tests::eager_tests::boolean_tests::pb_boolean_source_actual_literal_flags"){return;}
    std::panic::set_hook(Box::new(|_| {}));
    literal_flags(true);
}
fn literal_flags(compare: bool) {
    fixed_clock(|| {
        let mut count = 0;
        let mut hash = 0xcbf29ce484222325;
        let mut gaps = vec![];
        for (sig, _) in specs() {
            for source in [8, 253, 16] {
                for flags in 0usize..8 {
                    for result in [8, 245, 16] {
                        for name in ["canonical", "wire display"] {
                            for (sample, pair) in [
                                (0, [Some(4), Some(2)]),
                                (1, [Some(u64::MAX), Some(2)]),
                                (2, [Some(0), Some(0)]),
                                (3, [None, Some(2)]),
                            ] {
                                for profile in [0, 2] {
                                    for enabled in [false, true] {
                                        for selection in [None, Some(vec![2, 0, 2]), Some(vec![])] {
                                            // This is the real selected-builtin constructor used by the decoder,
                                            // with real BinaryLiteral constant nodes. The wire literal decoder
                                            // does not support a binary-literal variant; do not forge one.
                                            let build = || {
                                                let args=pair.iter().enumerate().map(|(i,value)|{let field=crate::distsql_builtin::pb_type_to_field_type(&declaration(source,Some(if flags&(2<<i)!=0{32}else{0})));literal(value.map_or(Datum::Null,|value|Datum::BinaryLiteral(tidb_datatype::BinaryLiteral::from_uint(value,None))),field)}).collect();
                                                let field =
                                                    crate::distsql_builtin::pb_type_to_field_type(
                                                        &declaration(
                                                            result,
                                                            if flags & 1 != 0 {
                                                                Some(32)
                                                            } else {
                                                                None
                                                            },
                                                        ),
                                                    );
                                                let mut function = ScalarFunction::from_pb(
                                                    crate::scalar_function::PbBuiltin::new(sig)
                                                        .unwrap(),
                                                    field,
                                                    args,
                                                );
                                                if name != "canonical" {
                                                    function.func_name = CiString::new(name);
                                                }
                                                suite(Expression::ScalarFunction(function))
                                            };
                                            let owner = build();
                                            let actual = build();
                                            let old = C::new(enabled, vec![], profile);
                                            let new = C::new(enabled, vec![], profile);
                                            for reuse in 0..2 {
                                                let label=format!("pb-boolean-literal/{sig:?}/{source}/{flags}/{result}/{name}/{sample}/{profile}/{enabled}/{selection:?}/{reuse}");
                                                case(
                                                    &owner, &actual, &old, &new, &selection,
                                                    compare, &label, &mut hash, &mut count,
                                                    &mut gaps,
                                                );
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        eprintln!("PB BOOLEAN LITERAL ORIGINAL {count}/{hash:016X}");
        assert_eq!((count, hash), (152064, 0x3C9D98FD0ED6EFE5));
        if !gaps.is_empty() {
            eprintln!("PB BOOLEAN LITERAL SOURCE RED {gaps:?}");
        }
        assert!(gaps.is_empty());
    });
}
#[test]
fn pb_boolean_source_original_source_families() {
    if isolate_common_corpus("stateful_tests::pb_regexp_tests::eager_tests::boolean_tests::pb_boolean_source_original_source_families"){return;}
    std::panic::set_hook(Box::new(|_| {}));
    source_families(false);
}
#[test]
fn pb_boolean_source_actual_source_families() {
    if isolate_common_corpus("stateful_tests::pb_regexp_tests::eager_tests::boolean_tests::pb_boolean_source_actual_source_families"){return;}
    std::panic::set_hook(Box::new(|_| {}));
    source_families(true);
}
fn source_families(compare: bool) {
    fixed_clock(|| {
        let mut count = 0;
        let mut hash = 0xcbf29ce484222325;
        let mut gaps = vec![];
        for (sig, _) in specs() {
            for source in 0..10 {
                for sample in 0..2 {
                    for result in [8, 245, 246] {
                        for name in ["canonical", "wire display"] {
                            for profile in [0, 2, 7] {
                                for enabled in [false, true] {
                                    for selection in [None, Some(vec![2, 0, 2]), Some(vec![])] {
                                        let values = if sample == 0 {
                                            vec![Datum::Int(4), Datum::Int(2)]
                                        } else {
                                            vec![Datum::new_string("4.5"), Datum::new_string("2")]
                                        };
                                        let owner =
                                            make(sig, source, result, None, name, 1, &values);
                                        let actual =
                                            make(sig, source, result, None, name, 1, &values);
                                        let old = C::new(enabled, values.clone(), profile);
                                        let new = C::new(enabled, values.clone(), profile);
                                        for reuse in 0..2 {
                                            let label=format!("pb-boolean-families/{sig:?}/{source}/{sample}/{result}/{name}/{profile}/{enabled}/{selection:?}/{reuse}");
                                            case(
                                                &owner, &actual, &old, &new, &selection, compare,
                                                &label, &mut hash, &mut count, &mut gaps,
                                            );
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        eprintln!("PB BOOLEAN FAMILIES ORIGINAL {count}/{hash:016X}");
        assert_eq!((count, hash), (47520, 0xAB6F94138BD915C5));
        if !gaps.is_empty() {
            eprintln!("PB BOOLEAN FAMILIES SOURCE RED {gaps:?}");
        }
        assert!(gaps.is_empty());
    });
}

#[test]
fn pb_boolean_source_original_optional_declarations() {
    if isolate_common_corpus("stateful_tests::pb_regexp_tests::eager_tests::boolean_tests::pb_boolean_source_original_optional_declarations") {return;}
    std::panic::set_hook(Box::new(|_| {}));
    optional_declarations(false);
}
#[test]
fn pb_boolean_source_actual_optional_declarations() {
    if isolate_common_corpus("stateful_tests::pb_regexp_tests::eager_tests::boolean_tests::pb_boolean_source_actual_optional_declarations") {return;}
    std::panic::set_hook(Box::new(|_| {}));
    optional_declarations(true);
}
fn optional_declarations(compare: bool) {
    fixed_clock(|| {
        let mut count = 0;
        let mut hash = 0xcbf29ce484222325;
        let mut gaps = vec![];
        for (sig, arity) in specs() {
            for absent in 0usize..(1 << (arity + 1)) {
                for source in [8, 253] {
                    for result in [8, 245, 16] {
                        for flags in [None, Some(32)] {
                            for name in ["canonical", "wire display", "coalesce", "plus", "abs"] {
                                for shape in [0, 1, 3] {
                                    for sample in 0..5 {
                                        let mut values = smoke(sig, arity);
                                        match sample {
                                            1 => values[0] = Datum::Null,
                                            2 => values[0] = Datum::Int(0),
                                            3 => values[0] = Datum::Bytes(vec![255]),
                                            4 => {
                                                values[arity - 1] = Datum::Null;
                                            }
                                            _ => (),
                                        }
                                        let build = || {
                                            let args=values.iter().enumerate().map(|(i,value)| {
                let mut c=crate::constant::Constant::new(if shape==0 {value.clone()} else {Datum::Bytes(b"stored".to_vec())},crate::distsql_builtin::pb_type_to_field_type(&declaration(source,Some(0))));
                if absent&(2<<i)!=0 {c.ret_type=None;}
                if shape==1 {c.param_marker=Some(crate::constant::ParamMarker{order:i as i64});}
                if shape==3 {c.deferred_expr=Some(Box::new(call("setvar",string_ty(),vec![literal(Datum::new_string(format!("boolean-{i}")),string_ty()),parameter(i,string_ty())])));}
                Expression::Constant(c)
            }).collect();
                                            let mut f = ScalarFunction::from_pb(
                                                crate::scalar_function::PbBuiltin::new(sig)
                                                    .unwrap(),
                                                crate::distsql_builtin::pb_type_to_field_type(
                                                    &declaration(result, flags),
                                                ),
                                                args,
                                            );
                                            if absent & 1 != 0 {
                                                f.ret_type = None;
                                            }
                                            if name != "canonical" {
                                                f.func_name = CiString::new(name);
                                            }
                                            let json = FieldType::new(FieldTypeCode::Json);
                                            suite(call(
                                                "coalesce",
                                                json.clone(),
                                                vec![
                                                    Expression::ScalarFunction(f),
                                                    literal(Datum::Null, json),
                                                ],
                                            ))
                                        };
                                        let owner = build();
                                        let actual = build();
                                        for enabled in [false, true] {
                                            for selection in
                                                [None, Some(vec![2, 0, 2]), Some(vec![])]
                                            {
                                                let old = C::new(enabled, values.clone(), 0);
                                                let new = C::new(enabled, values.clone(), 0);
                                                for reuse in 0..2 {
                                                    let label=format!("pb-boolean-optional/{sig:?}/{absent}/{source}/{result}/{flags:?}/{name}/{shape}/{sample}/{enabled}/{selection:?}/{reuse}");
                                                    case(
                                                        &owner, &actual, &old, &new, &selection,
                                                        compare, &label, &mut hash, &mut count,
                                                        &mut gaps,
                                                    );
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        eprintln!("PB BOOLEAN OPTIONAL ORIGINAL {count}/{hash:016X}");
        assert_eq!((count, hash), (561600, 0x7EA0B2B3F1A97B05));
        if !gaps.is_empty() {
            eprintln!("PB BOOLEAN OPTIONAL SOURCE RED {gaps:?}");
        }
        assert!(gaps.is_empty());
    });
}

#[test]
fn pb_boolean_source_direct_consumers_and_fixed_cap_recovery() {
    for (sig, arity) in specs() {
        let values = smoke(sig, arity);
        for name in [
            "wire display",
            "char_length",
            "plus",
            "abs",
            "cast_json",
            "cast_decimal",
        ] {
            let owner = make(sig, 1, 8, None, name, 1, &values);
            let ty = output_type(&owner);
            let mut direct = PreparedBatch::compile(
                &owner,
                &[string_ty()],
                true,
                944,
                limits(),
                ExecutionLimits::default(),
                1 << 20,
            )
            .unwrap();
            let candidate =
                crate::scalar_function::static_batch_target(&owner.program.calculated[0]);
            for enabled in [true] {
                if candidate.is_none() {
                    let ctx = C::new(enabled, values.clone(), 0);
                    let mut b = input(&None);
                    let mut right = output_for(&ty);
                    right.append_null(0);
                    let failure = owner
                        .run_batch(
                            &ctx,
                            &mut EvalContext::default(),
                            &mut direct,
                            &mut b,
                            &mut right,
                            &[string_ty()],
                        )
                        .unwrap_err();
                    assert_eq!(failure.refusal(), BatchRefusal::Admission);
                    assert_eq!(right.num_rows(), 1);
                    continue;
                }
                let old = C::new(enabled, values.clone(), 0);
                let new = C::new(enabled, values.clone(), 0);
                let mut a = input(&Some(vec![2, 0, 2]));
                let mut left = output_for(&ty);
                left.append_null(0);
                let expected = original_session_read_outcome(|| {
                    owner.run_with_consumer(
                        &old,
                        &mut a,
                        &mut left,
                        &mut crate::evaluator::NativeBatchConsumer,
                    )
                });
                let mut b = input(&Some(vec![2, 0, 2]));
                let mut right = output_for(&ty);
                right.append_null(0);
                let got = session_read_outcome(|| {
                    owner
                        .run_batch(
                            &new,
                            &mut EvalContext::default(),
                            &mut direct,
                            &mut b,
                            &mut right,
                            &[string_ty()],
                        )
                        .map_err(BatchFailure::into_evaluator_error)
                });
                assert_eq!(
                    got, expected,
                    "direct/{sig:?}/{name}/{enabled}/{candidate:?}"
                );
                assert_eq!(right.column(0), left.column(0));
                assert_eq!(
                    new.snapshot(),
                    old.snapshot(),
                    "direct/{sig:?}/{name}/{enabled}"
                );
            }
        }
        let owner = make(sig, 1, 8, None, "wire display", 1, &values);
        let ty = output_type(&owner);
        let mut prepared = PreparedBatchSuite::compile(
            &owner,
            &[string_ty()],
            true,
            944,
            limits(),
            ExecutionLimits::default(),
            1 << 20,
        )
        .unwrap();
        prepared.workers[0].max_materialization_retained_bytes = 64 * 1024;
        let mut first = input(&Some(vec![0; 8193]));
        let mut output = output_for(&ty);
        output.append_null(0);
        let ctx = C::new(true, values.clone(), 0);
        let failure = prepared
            .run(
                &owner,
                &ctx,
                &mut EvalContext::default(),
                &mut first,
                &mut output,
                &[string_ty()],
            )
            .unwrap_err();
        assert!(
            matches!(failure.local_error(), Some(LocalError::ResourceLimit(_))),
            "{sig:?}/{failure:?}"
        );
        assert_eq!(output.num_rows(), 1);
        for null in [true, false, true] {
            let mut values = values.clone();
            if null && !values.is_empty() {
                values[0] = Datum::Null;
            }
            let old = C::new(true, values.clone(), 0);
            let new = C::new(true, values.clone(), 0);
            let mut a = input(&None);
            let mut left = output_for(&ty);
            left.append_null(0);
            let expected = original_session_read_outcome(|| owner.run(&old, &mut a, &mut left));
            let mut b = input(&None);
            let mut right = output_for(&ty);
            right.append_null(0);
            let got = session_read_outcome(|| {
                prepared
                    .run(
                        &owner,
                        &new,
                        &mut EvalContext::default(),
                        &mut b,
                        &mut right,
                        &[string_ty()],
                    )
                    .map_err(BatchFailure::into_evaluator_error)
            });
            assert_eq!(got, expected, "recovery/{sig:?}/{null}");
            assert_eq!(right.column(0), left.column(0));
            assert_eq!(new.snapshot(), old.snapshot());
            assert_eq!(
                prepared.workers[0].max_materialization_retained_bytes,
                64 * 1024
            );
        }
    }
}

#[test]
fn pb_boolean_source_authenticates_both_truth_phases_and_callback_epochs() {
    use super::super::super::hosts::{NativeBatchHosts, NativeHostMetadata, NativePbBoolean};
    use tidb_query_expr::local::{
        HostArgReply, HostInvocation, HostStart, HostStep, HostTaskId, LocalHostServices,
    };
    struct Context<'a> {
        store: &'a RefCell<NativeValueStore>,
        reset: Cell<bool>,
        panic: Cell<bool>,
        reads: Cell<usize>,
    }
    impl Columns for Context<'_> {
        fn get(&self, _: &[String]) -> Option<Datum> {
            panic!("no expression callback")
        }
        fn handle_truncate(&self, _: &str) -> std::result::Result<(), EvalError> {
            drop(self.store.borrow_mut());
            self.reads.set(self.reads.get() + 1);
            if self.reset.replace(false) {
                self.store.borrow_mut().begin_window().unwrap();
            }
            assert!(!self.panic.replace(false), "original PB ready callback");
            Ok(())
        }
    }
    let owner = make(
        Sig::LogicalOr,
        1,
        8,
        None,
        "wire display",
        1,
        &smoke(Sig::LogicalOr, 2),
    );
    let Expression::ScalarFunction(function) = &owner.program.calculated[0] else {
        panic!()
    };
    let field = projected(&native_bytes_type(), true).unwrap();
    let args = vec![field.clone(); 2];
    let catalog = HostCatalog::for_sql_batch(vec![(
        SqlBatchHostOperation::EagerValuesNative,
        HostSignature {
            arg_types: args.clone().into(),
            return_type: field.clone(),
        },
    )])
    .unwrap();
    let slot = catalog.slot(0).unwrap();
    let row = InputRow {
        occurrence: 0,
        input_row: 2,
    };
    let metadata = [NativeHostMetadata::PbBoolean(Box::new(NativePbBoolean {
        arguments: vec![3, 4].into(),
        kernel: function.pb_boolean_values().unwrap(),
        logical_arity: 2,
        name: "wire display".into(),
        result_type: function.ret_type.clone(),
        integer_batch_result: false,
        output_node: 2,
        output_role: NativeValueRole::Output,
    }))];
    let store = RefCell::new(NativeValueStore::new(256 << 10).unwrap());
    store.borrow_mut().begin_window().unwrap();
    let insert = |value: &Datum, role, node, row| {
        store
            .borrow_mut()
            .insert_clone(value, role, NativeValueOrigin { node, row })
            .unwrap()
    };
    let stale = insert(&Datum::Int(0), NativeValueRole::CharInput, 3, row);
    store.borrow_mut().begin_window().unwrap();
    let context = Context {
        store: &store,
        reset: Cell::new(false),
        panic: Cell::new(false),
        reads: Cell::new(0),
    };
    let mut provider = NativeBatchHosts::new(catalog, &metadata, &context, 16 << 10)
        .with_native_values(Some(&store));
    let mut eval = EvalContext::default();
    let begin = |p: &mut NativeBatchHosts<'_>, e: &mut EvalContext| {
        let HostStart::Pending { task, .. } = p
            .start(
                e,
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
        task
    };
    let reply =
        |p: &mut NativeBatchHosts<'_>, e: &mut EvalContext, t: &HostTaskId, index, token| {
            p.resume(
                e,
                t,
                HostArgReply {
                    index,
                    field_type: &field,
                    values: &VectorValue::from_scalar(&ScalarValue::Bytes(token), 1),
                },
            )
        };
    for index in 0..2 {
        for kind in 0..8 {
            store.borrow_mut().begin_window().unwrap();
            let task = begin(&mut provider, &mut eval);
            if index == 1 {
                assert!(matches!(
                    reply(
                        &mut provider,
                        &mut eval,
                        &task,
                        0,
                        Some(insert(&Datum::Int(0), NativeValueRole::CharInput, 3, row))
                    ),
                    Ok(HostStep::NeedArg(_))
                ));
            }
            let bad = match kind {
                0 => None,
                1 => Some(stale.clone()),
                2 => Some(vec![0; 8]),
                3 => Some(insert(
                    &Datum::Int(3),
                    NativeValueRole::Output,
                    3 + index,
                    row,
                )),
                4 => Some(insert(&Datum::Int(3), NativeValueRole::CharInput, 99, row)),
                5 => Some(insert(
                    &Datum::Int(3),
                    NativeValueRole::CharInput,
                    3 + index,
                    InputRow {
                        occurrence: 1,
                        ..row
                    },
                )),
                6 => Some(insert(
                    &Datum::Int(3),
                    NativeValueRole::CharInput,
                    3 + index,
                    InputRow {
                        input_row: 1,
                        ..row
                    },
                )),
                _ => {
                    store.borrow_mut().begin_window().unwrap();
                    Some(insert(
                        &Datum::Int(3),
                        NativeValueRole::CharInput,
                        3 + index,
                        row,
                    ))
                }
            };
            if kind == 7 && index == 0 {
                provider.cancel(&task);
                continue;
            }
            let before = context.reads.get();
            assert!(matches!(
                reply(&mut provider, &mut eval, &task, index, bad),
                Err(LocalError::BindingContract(_))
            ));
            assert_eq!(context.reads.get(), before);
            provider.cancel(&task);
            provider.cancel(&task);
        }
    }
    for stage in 0..2 {
        for behavior in 0..3 {
            store.borrow_mut().begin_window().unwrap();
            let task = begin(&mut provider, &mut eval);
            if stage == 1 {
                assert!(matches!(
                    reply(
                        &mut provider,
                        &mut eval,
                        &task,
                        0,
                        Some(insert(&Datum::Int(0), NativeValueRole::CharInput, 3, row))
                    ),
                    Ok(HostStep::NeedArg(_))
                ));
            }
            context.reset.set(behavior == 1);
            context.panic.set(behavior == 2);
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                reply(
                    &mut provider,
                    &mut eval,
                    &task,
                    stage,
                    Some(insert(
                        &Datum::Bytes(b"invalid number".to_vec()),
                        NativeValueRole::CharInput,
                        3 + stage,
                        row,
                    )),
                )
            }));
            match behavior {
                0 if stage == 0 => assert!(matches!(result, Ok(Ok(HostStep::NeedArg(_))))),
                0 => assert!(matches!(result, Ok(Ok(HostStep::Ready(_))))),
                1 => assert!(matches!(result, Ok(Err(LocalError::BindingContract(_))))),
                _ => assert!(result.is_err()),
            }
            let before = context.reads.get();
            assert!(reply(&mut provider, &mut eval, &task, stage, None).is_err());
            assert_eq!(context.reads.get(), before);
            provider.cancel(&task);
            provider.cancel(&task);
        }
    }
    for _ in 0..64 {
        store.borrow_mut().begin_window().unwrap();
        let task = begin(&mut provider, &mut eval);
        reply(
            &mut provider,
            &mut eval,
            &task,
            0,
            Some(insert(&Datum::Int(0), NativeValueRole::CharInput, 3, row)),
        )
        .unwrap();
        provider.cancel(&task);
    }
}

#[test]
fn pb_boolean_source_rejects_stale_optional_declarations_and_selected_origins() {
    for (sig, arity) in specs() {
        for bad in 0..6 {
            let mut owner = make(sig, 1, 8, None, "coalesce", 0, &smoke(sig, arity));
            let root = Arc::get_mut(&mut owner.program)
                .unwrap()
                .calculated
                .get_mut(0)
                .unwrap();
            let Expression::ScalarFunction(function) = root else {
                panic!()
            };
            match bad {
                0 => function.ret_type = None,
                1 => function.ret_type = Some(FieldType::new(FieldTypeCode::Double)),
                2 => function.args.push(literal(Datum::Null, string_ty())),
                3 => {
                    let other_sig = if sig == Sig::Cos {
                        Sig::UpperUtf8
                    } else {
                        Sig::Cos
                    };
                    let other = make(other_sig, 1, 8, None, "coalesce", 0, &smoke(other_sig, 1));
                    let Expression::ScalarFunction(other) = &other.program.calculated[0] else {
                        panic!()
                    };
                    function.pb_origin = other.pb_origin.clone();
                }
                4 | 5 => {
                    let Some(Expression::Constant(c)) = function.args.first_mut() else {
                        continue;
                    };
                    c.ret_type = if bad == 4 {
                        None
                    } else {
                        Some(FieldType::new(FieldTypeCode::Double))
                    };
                }
                _ => unreachable!(),
            }
            let result = PreparedBatchSuite::compile(
                &owner,
                &[string_ty()],
                true,
                944,
                limits(),
                ExecutionLimits::default(),
                1 << 20,
            );
            assert!(result.is_err(), "stale origin admitted: {sig:?}/{bad}");
            assert_eq!(
                result.err().unwrap().refusal(),
                BatchRefusal::Contract,
                "{sig:?}/{bad}"
            );
        }
    }
}

#[test]
fn pb_boolean_source_presence_does_not_copy_payload_and_suffix_is_not_compiled() {
    use super::super::super::hosts::{NativeBatchHosts, NativeHostMetadata, NativePbBoolean};
    use tidb_query_datatype::codec::data_type::ScalarValueRef;
    use tidb_query_expr::local::{
        HostArgReply, HostInvocation, HostStart, HostStep, LocalHostServices,
    };
    struct NoTruth;
    impl Columns for NoTruth {
        fn get(&self, _: &[String]) -> Option<Datum> {
            panic!("no expression callback")
        }
        fn handle_truncate(&self, _: &str) -> std::result::Result<(), EvalError> {
            panic!("IS NULL does not convert truth")
        }
    }
    let owner = make(
        Sig::IntIsNull,
        1,
        8,
        None,
        "wire display",
        1,
        &[Datum::Int(4)],
    );
    let Expression::ScalarFunction(function) = &owner.program.calculated[0] else {
        panic!()
    };
    let field = projected(&native_bytes_type(), true).unwrap();
    let args = vec![field.clone()];
    let catalog = HostCatalog::for_sql_batch(vec![(
        SqlBatchHostOperation::EagerValuesNative,
        tidb_query_expr::local::HostSignature {
            arg_types: args.clone().into(),
            return_type: field.clone(),
        },
    )])
    .unwrap();
    let slot = catalog.slot(0).unwrap();
    let row = InputRow {
        occurrence: 0,
        input_row: 2,
    };
    let metadata = [NativeHostMetadata::PbBoolean(Box::new(NativePbBoolean {
        kernel: function.pb_boolean_values().unwrap(),
        arguments: vec![3].into(),
        logical_arity: 1,
        name: "wire display".into(),
        result_type: function.ret_type.clone(),
        integer_batch_result: false,
        output_node: 2,
        output_role: NativeValueRole::Output,
    }))];
    let store = RefCell::new(NativeValueStore::new(256 << 10).unwrap());
    let mut provider = NativeBatchHosts::new(catalog, &metadata, &NoTruth, 16 << 10)
        .with_native_values(Some(&store));
    let mut eval = EvalContext::default();
    for value in [
        Datum::Bytes(vec![b'x'; 32768]),
        Datum::Null,
        Datum::Bytes(vec![255; 32768]),
    ] {
        store.borrow_mut().begin_window().unwrap();
        let HostStart::Pending { task, request } = provider
            .start(
                &mut eval,
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
        assert_eq!(request.index, 0);
        let token = store
            .borrow_mut()
            .insert_clone(
                &value,
                NativeValueRole::CharInput,
                NativeValueOrigin { node: 3, row },
            )
            .unwrap();
        let HostStep::Ready(output) = provider
            .resume(
                &mut eval,
                &task,
                HostArgReply {
                    index: 0,
                    field_type: &field,
                    values: &VectorValue::from_scalar(&ScalarValue::Bytes(Some(token)), 1),
                },
            )
            .unwrap()
        else {
            panic!()
        };
        let ScalarValueRef::Bytes(Some(token)) = output.get_scalar_ref(0) else {
            panic!()
        };
        assert_eq!(
            store
                .borrow()
                .get(
                    token,
                    NativeValueRole::Output,
                    NativeValueOrigin { node: 2, row }
                )
                .unwrap(),
            &Datum::Int(i64::from(value.is_null()))
        );
        provider.cancel(&task);
        provider.cancel(&task);
    }
    // The real original PB unary owner ignores every suffix. A genuinely
    // unsupported producer in that suffix must neither run nor need a binding.
    for name in ["canonical", "wire display", "abs", "plus", "cast_json"] {
        let values = vec![Datum::Int(4), Datum::Bytes(vec![255]), Datum::Null];
        let build = || {
            let base = make(Sig::UnaryNotInt, 0, 8, None, name, 1, &values);
            let mut root = base.program.calculated[0].clone();
            let Expression::ScalarFunction(f) = &mut root else {
                panic!()
            };
            f.args[2] = call(
                "unregistered_ignored_boolean_child",
                string_ty(),
                vec![parameter(2, string_ty())],
            );
            suite(root)
        };
        let owner = build();
        let actual = build();
        for enabled in [false, true] {
            for selection in [None, Some(vec![2, 0, 2]), Some(vec![])] {
                let old = C::new(enabled, values.clone(), 0);
                let new = C::new(enabled, values.clone(), 0);
                let mut count = 0;
                let mut hash = 0xcbf29ce484222325;
                let mut gaps = vec![];
                case(
                    &owner,
                    &actual,
                    &old,
                    &new,
                    &selection,
                    true,
                    &format!("ignored/{name}/{enabled}/{selection:?}"),
                    &mut hash,
                    &mut count,
                    &mut gaps,
                );
                assert!(gaps.is_empty());
                assert_eq!(count, 1);
            }
        }
    }
}
