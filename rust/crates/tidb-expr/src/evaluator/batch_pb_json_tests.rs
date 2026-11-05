// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Original contracts for all selected JSON PB value bodies and live caches.
use super::*;
fn specs() -> [(Sig, usize); 4] {
    [
        (Sig::JsonMemberOfSig, 2),
        (Sig::JsonReplaceSig, 3),
        (Sig::JsonArrayAppendSig, 3),
        (Sig::JsonMergePatchSig, 2),
    ]
}
fn smoke(sig: Sig, arity: usize) -> Vec<Datum> {
    let s = Datum::new_string;
    let mut values = match sig {
        Sig::JsonMemberOfSig => vec![s("1"), s("[1,2]")],
        Sig::JsonReplaceSig => vec![s(r#"{"a":1,"b":2}"#), s("$.a"), Datum::Int(3)],
        Sig::JsonArrayAppendSig => vec![s(r#"{"a":[1],"b":[2]}"#), s("$.a"), Datum::Int(3)],
        Sig::JsonMergePatchSig => vec![s(r#"{"a":1}"#), s(r#"{"b":2}"#)],
        _ => unreachable!(),
    };
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
            .enumerate()
            .map(|(i, value)| {
                atom(
                    if shape == 0 { value } else { &stored },
                    Some(declaration(
                        match source {
                            0 => 253,
                            1 => 245,
                            _ => [8, 253, 245][i % 3],
                        },
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
                        literal(Datum::new_string(format!("json-{i}")), string_ty()),
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
            eprintln!("JSON DIFFERENCE {label} old={expected} new={got} oldcol={:?} newcol={:?} oldctx={} newctx={}",left.column(0),right.column(0),old.snapshot(),new.snapshot());
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
fn pb_json_source_original_phases() {
    if isolate_common_corpus(
        "stateful_tests::pb_regexp_tests::eager_tests::json_tests::pb_json_source_original_phases",
    ) {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    matrix(false, false);
}
#[test]
fn pb_json_source_actual_phases() {
    if isolate_common_corpus(
        "stateful_tests::pb_regexp_tests::eager_tests::json_tests::pb_json_source_actual_phases",
    ) {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    matrix(true, false);
}
#[test]
fn pb_json_source_original_full() {
    if isolate_common_corpus(
        "stateful_tests::pb_regexp_tests::eager_tests::json_tests::pb_json_source_original_full",
    ) {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    matrix(false, true);
}
#[test]
fn pb_json_source_actual_full() {
    if isolate_common_corpus(
        "stateful_tests::pb_regexp_tests::eager_tests::json_tests::pb_json_source_actual_full",
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
                vec![0, 1, 2, 3, 4, 5, 9]
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
                    for sample in 1..6 {
                        let mut values = base.clone();
                        let index = if matches!(sample, 2 | 4) {
                            arity - 1
                        } else if sample == 5 {
                            1.min(arity - 1)
                        } else {
                            0
                        };
                        values[index] = match sample {
                            1 | 2 => Datum::Null,
                            3 | 4 => Datum::Bytes(vec![255]),
                            _ => Datum::new_string("invalid path"),
                        };
                        samples.push((sample, values));
                    }
                }
                for source in 0..3 {
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
                                ]
                            } {
                                for shape in if full { vec![1] } else { vec![0, 1, 3] } {
                                    for (vi, values) in &samples {
                                        for profile in if full { vec![0] } else { vec![0, 2, 3, 7] }
                                        {
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
                                                        let label=format!("pb-json/{full}/{sig:?}/{arity}/{source}/{result}/{flags:?}/{name}/{shape}/{vi}/{profile}/{enabled}/{selection:?}/{reuse}");
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
        eprintln!("PB JSON ORIGINAL full={full} {count}/{hash:016X}");
        assert_eq!(
            (count, hash),
            if full {
                (643680, 0xC01D3EC8C49C7815)
            } else {
                (3068928, 0x22458E71C1756ADD)
            }
        );
        if !gaps.is_empty() {
            eprintln!("PB JSON SOURCE RED {gaps:?}");
        }
        assert!(gaps.is_empty());
    });
}
#[test]
fn pb_json_source_original_long() {
    if isolate_common_corpus(
        "stateful_tests::pb_regexp_tests::eager_tests::json_tests::pb_json_source_original_long",
    ) {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    long(false);
}
#[test]
fn pb_json_source_actual_long() {
    if isolate_common_corpus(
        "stateful_tests::pb_regexp_tests::eager_tests::json_tests::pb_json_source_actual_long",
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
        for (sig, arity) in specs() {
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
                                            [false, true, true, false].into_iter().enumerate()
                                        {
                                            let mut values = values.clone();
                                            if round > 0 {
                                                match sig {
                                                    Sig::JsonMemberOfSig => {
                                                        values[0] =
                                                            Datum::new_string(if round == 1 {
                                                                "2"
                                                            } else {
                                                                "3"
                                                            })
                                                    }
                                                    Sig::JsonReplaceSig
                                                    | Sig::JsonArrayAppendSig => {
                                                        values[1] = if round == 1 {
                                                            Datum::new_string("$.b")
                                                        } else if round == 2 {
                                                            Datum::new_string("invalid path")
                                                        } else {
                                                            Datum::Null
                                                        }
                                                    }
                                                    Sig::JsonMergePatchSig => {
                                                        values[1] = if round == 3 {
                                                            Datum::Null
                                                        } else {
                                                            Datum::new_string(r#"{"b":3}"#)
                                                        }
                                                    }
                                                    _ => unreachable!(),
                                                }
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
                                            let label=format!("pb-json-long/{sig:?}/{result}/{name}/{shape}/{profile}/{n}/{selected}/{reuse}/{round}/{enabled}/{id}");
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
                                                    eprintln!("JSON LONG DIFFERENCE {label} old={expected} new={got} oldctx={} newctx={}",old.snapshot(),new.snapshot());
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
        eprintln!("PB JSON LONG ORIGINAL {count}/{hash:016X}");
        assert_eq!((count, hash), (7680, 0xC235536AFFBF7269));
        if !gaps.is_empty() {
            eprintln!("PB JSON LONG SOURCE RED {gaps:?}");
        }
        assert!(gaps.is_empty());
    });
}
#[test]
fn pb_json_source_original_physical() {
    if isolate_common_corpus("stateful_tests::pb_regexp_tests::eager_tests::json_tests::pb_json_source_original_physical"){return;}
    std::panic::set_hook(Box::new(|_| {}));
    physical(false);
}
#[test]
fn pb_json_source_actual_physical() {
    if isolate_common_corpus(
        "stateful_tests::pb_regexp_tests::eager_tests::json_tests::pb_json_source_actual_physical",
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
            for result in [8, 5, 246, 245, 253, 6, 13, 16, 247, 248, 12, 11] {
                for flags in [None, Some(32)] {
                    for name in ["canonical", "coalesce", "char_length", "plus", "cast_json"] {
                        for nested in [false, true] {
                            for sample in 0..6 {
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
                                                            Datum::Bytes(b"invalid path".to_vec())
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
                                                let label=format!("pb-json-physical/{sig:?}/{result}/{flags:?}/{name}/{nested}/{sample}/{enabled}/{n}/{selection}/{reuse}");
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
                                                        eprintln!("JSON PHYSICAL DIFFERENCE {label} old={expected} new={got} oldctx={} newctx={}",old.snapshot(),new.snapshot());
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
        eprintln!("PB JSON PHYSICAL ORIGINAL {count}/{hash:016X}");
        assert_eq!((count, hash), (207360, 0x18C4F5C7B906E8A3));
        if !gaps.is_empty() {
            eprintln!("PB JSON PHYSICAL SOURCE RED {gaps:?}");
        }
        assert!(gaps.is_empty());
    });
}

#[test]
fn pb_json_source_live_cache_identity_and_fixed_cap_recovery() {
    let sig = Sig::JsonReplaceSig;
    let values = smoke(sig, 3);
    let old_owner = make(sig, 0, 245, None, "wire display", 1, &values);
    let actual = make(sig, 0, 245, None, "wire display", 1, &values);
    let ty = output_type(&old_owner);
    let mut old = C::new(false, values.clone(), 0);
    let mut new = C::new(false, values.clone(), 0);
    let single = || {
        let mut input = input(&None);
        input.set_sel(Some(vec![0]));
        input
    };
    // Warm each genuine owner through the old public caller before compilation.
    for (owner, context) in [(&old_owner, &old), (&actual, &new)] {
        let mut input = single();
        let mut output = output_for(&ty);
        owner.run(context, &mut input, &mut output).unwrap();
    }
    assert_eq!(old.snapshot(), new.snapshot());
    let snapshot = new.snapshot();
    let mut prepared = PreparedBatchSuite::compile(
        &actual,
        &[string_ty()],
        true,
        945,
        limits(),
        ExecutionLimits::default(),
        1 << 20,
    )
    .unwrap();
    assert_eq!(new.snapshot(), snapshot);
    assert!(
        matches!(&prepared.workers[0].source.host_metadata[0],NativeHostMetadata::PbEagerValues(meta) if matches!(&meta.kernel,super::super::super::super::hosts::PbValueKernel::Json(_)))
    );
    let mut previous = None;
    for (round, id, path) in [
        (0, 17, Some("$.b")),
        (1, 18, Some("$.b")),
        (2, 18, None),
        (3, 19, None),
        (4, 19, Some("$.a")),
    ] {
        old.id.set(id);
        new.id.set(id);
        old.values[1] = path.map_or(Datum::Null, Datum::new_string);
        new.values[1] = old.values[1].clone();
        old.values[2] = Datum::Int(9);
        new.values[2] = Datum::Int(9);
        let mut a = single();
        let mut left = output_for(&ty);
        let expected = original_session_read_outcome(|| old_owner.run(&old, &mut a, &mut left));
        let mut b = single();
        let mut right = output_for(&ty);
        let got = session_read_outcome(|| {
            prepared
                .run(
                    &actual,
                    &new,
                    &mut EvalContext::default(),
                    &mut b,
                    &mut right,
                    &[string_ty()],
                )
                .map_err(BatchFailure::into_evaluator_error)
        });
        assert_eq!(got, expected, "warm{round}");
        assert_eq!(right.column(0), left.column(0));
        assert_eq!(new.snapshot(), old.snapshot());
        assert_eq!(expected, "Ok(())");
        if round == 0 {
            let clone = suite(old_owner.program.calculated[0].clone());
            let context = C::new(false, old.values.clone(), 0);
            let mut input = single();
            let mut reset = output_for(&ty);
            clone.run(&context, &mut input, &mut reset).unwrap();
            assert_ne!(
                reset.column(0),
                left.column(0),
                "owner cloning resets paths; invocation capture must share the warmed path"
            );
        }
        if round == 1 {
            previous = Some(format!("{:?}", left.column(0)));
        }
        if round == 2 {
            assert_eq!(
                Some(format!("{:?}", left.column(0))),
                previous,
                "cached path ignores a later raw NULL path, after all children are read"
            );
        }
        if round >= 3 {
            assert!(
                left.get_row(0).is_null(0),
                "cached NULL path persists in the same context"
            );
        }
    }
    for (sig, arity) in specs() {
        let values = smoke(sig, arity);
        let owner = make(sig, 0, 245, None, "wire display", 1, &values);
        let ty = output_type(&owner);
        let mut prepared = PreparedBatchSuite::compile(
            &owner,
            &[string_ty()],
            true,
            945,
            limits(),
            ExecutionLimits::default(),
            1 << 20,
        )
        .unwrap();

        let mut huge = values.clone();
        huge[0] = Datum::new_string(format!(r#"{{"a":"{}"}}"#, "x".repeat(32768)));
        let context = C::new(true, huge.clone(), 0);
        let native = C::new(true, huge, 0);
        let mut actual_input = input(&None);
        let mut output = output_for(&ty);
        output.append_null(0);
        let mut original_input = input(&None);
        let mut original_output = output_for(&ty);
        original_output.append_null(0);
        let expected = original_session_read_outcome(|| {
            owner.run(&native, &mut original_input, &mut original_output)
        });
        let got = session_read_outcome(|| {
            prepared
                .run(
                    &owner,
                    &context,
                    &mut EvalContext::default(),
                    &mut actual_input,
                    &mut output,
                    &[string_ty()],
                )
                .map_err(BatchFailure::into_evaluator_error)
        });
        assert_eq!(got, expected, "large JSON/{sig:?}");
        assert_eq!(output.column(0), original_output.column(0));
        assert_eq!(context.snapshot(), native.snapshot());
        let mut values = values;
        values[0] = Datum::Null;
        let old = C::new(false, values.clone(), 0);
        let new = C::new(false, values, 0);
        let mut a = single();
        let mut left = output_for(&ty);
        left.append_null(0);
        let expected = original_session_read_outcome(|| owner.run(&old, &mut a, &mut left));
        let mut b = single();
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
        assert_eq!(got, expected, "{sig:?}");
        assert_eq!(right.column(0), left.column(0));
        assert_eq!(new.snapshot(), old.snapshot());
    }
}
#[test]
fn pb_json_source_actual_legacy_consumers_and_stale_origins() {
    for (sig, _) in specs() {
        for arity in [1, 2, 3] {
            for result in [8, 246, 245] {
                for name in ["abs", "cast_decimal", "cast_json", "plus", "char_length"] {
                    let values = smoke(sig, arity);
                    let owner = make(sig, 0, result, None, name, 1, &values);
                    let ty = output_type(&owner);
                    let mut direct = PreparedBatch::compile(
                        &owner,
                        &[string_ty()],
                        true,
                        945,
                        limits(),
                        ExecutionLimits::default(),
                        1 << 20,
                    )
                    .unwrap();
                    let candidate =
                        crate::scalar_function::static_batch_target(&owner.program.calculated[0]);
                    let old = C::new(true, values.clone(), 0);
                    let new = C::new(true, values.clone(), 0);
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
                    if candidate.is_some() {
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
                        assert_eq!(got, expected, "{sig:?}/{arity}/{result}/{name}");
                        assert_eq!(right.column(0), left.column(0));
                        assert_eq!(new.snapshot(), old.snapshot());
                    } else {
                        let error = owner
                            .run_batch(
                                &new,
                                &mut EvalContext::default(),
                                &mut direct,
                                &mut b,
                                &mut right,
                                &[string_ty()],
                            )
                            .unwrap_err();
                        assert_eq!(error.refusal(), BatchRefusal::Admission);
                        assert_eq!(right.num_rows(), 1);
                    }
                }
            }
        }
    }
    for (sig, arity) in specs() {
        for bad in 0..6 {
            let mut owner = make(sig, 0, 245, None, "coalesce", 0, &smoke(sig, arity));
            let Expression::ScalarFunction(function) = Arc::get_mut(&mut owner.program)
                .unwrap()
                .calculated
                .get_mut(0)
                .unwrap()
            else {
                panic!()
            };
            match bad {
                0 => function.ret_type = None,
                1 => function.ret_type = Some(FieldType::new(FieldTypeCode::Double)),
                2 => function.args.push(literal(Datum::Null, string_ty())),
                3 => {
                    let other = super::make(
                        Sig::UpperUtf8,
                        253,
                        245,
                        None,
                        "coalesce",
                        0,
                        &[Datum::new_string("a")],
                    );
                    let Expression::ScalarFunction(other) = &other.program.calculated[0] else {
                        panic!()
                    };
                    function.pb_origin = other.pb_origin.clone();
                }
                4 | 5 => {
                    let Expression::Constant(c) = &mut function.args[0] else {
                        panic!()
                    };
                    c.ret_type = if bad == 4 {
                        None
                    } else {
                        Some(FieldType::new(FieldTypeCode::Double))
                    };
                }
                _ => unreachable!(),
            };
            let result = PreparedBatchSuite::compile(
                &owner,
                &[string_ty()],
                true,
                945,
                limits(),
                ExecutionLimits::default(),
                1 << 20,
            );
            assert!(result.is_err(), "{sig:?}/{bad}");
            assert_eq!(result.err().unwrap().refusal(), BatchRefusal::Contract);
        }
    }
}
#[test]
fn pb_json_values_authenticate_all_raw_arguments_and_callback_epochs() {
    use super::super::super::super::hosts::{
        NativeBatchHosts, NativeHostMetadata, NativePbEagerValues, PbValueKernel,
    };
    use tidb_query_expr::local::{
        HostArgReply, HostInvocation, HostStart, HostStep, HostTaskId, LocalHostServices,
    };
    struct Context<'a> {
        store: &'a RefCell<NativeValueStore>,
        ready: crate::ReadyValueCache,
        ready_reads: Cell<usize>,
        id_reads: Cell<usize>,
        mode: Cell<usize>,
    }
    impl Columns for Context<'_> {
        fn get(&self, _: &[String]) -> Option<Datum> {
            panic!("no JSON expression callback")
        }
        fn ready_value_cache(&self) -> Option<&crate::ReadyValueCache> {
            drop(self.store.borrow_mut());
            self.ready_reads.set(self.ready_reads.get() + 1);
            match self.mode.replace(0) {
                1 => {
                    self.store.borrow_mut().begin_window().unwrap();
                }
                3 => panic!("original JSON ready callback"),
                value => self.mode.set(value),
            }
            Some(&self.ready)
        }
        fn context_id(&self) -> u64 {
            drop(self.store.borrow_mut());
            self.id_reads.set(self.id_reads.get() + 1);
            if self.mode.replace(0) == 2 {
                self.store.borrow_mut().begin_window().unwrap();
            }
            17
        }
    }
    let owner = make(
        Sig::JsonReplaceSig,
        0,
        245,
        None,
        "wire display",
        1,
        &smoke(Sig::JsonReplaceSig, 3),
    );
    let Expression::ScalarFunction(function) = &owner.program.calculated[0] else {
        panic!()
    };
    let field = projected(&native_bytes_type(), true).unwrap();
    let args = vec![field.clone(); 3];
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
    let metadata = [NativeHostMetadata::PbEagerValues(Box::new(
        NativePbEagerValues {
            arguments: vec![3, 4, 5].into(),
            kernel: PbValueKernel::Json(Box::new(function.pb_json_values().unwrap())),
            name: "wire display".into(),
            result_type: function.ret_type.clone(),
            integer_batch_result: false,
            output_node: 2,
            output_role: NativeValueRole::Output,
        },
    ))];
    let store = RefCell::new(NativeValueStore::new().unwrap());
    store.borrow_mut().begin_window().unwrap();
    let insert = |value: &Datum, role, node, row| {
        store
            .borrow_mut()
            .insert_clone(value, role, NativeValueOrigin { node, row })
            .unwrap()
    };
    let stale = insert(&Datum::Null, NativeValueRole::CharInput, 3, row);
    store.borrow_mut().begin_window().unwrap();
    let context = Context {
        store: &store,
        ready: crate::ReadyValueCache::new(),
        ready_reads: Cell::new(0),
        id_reads: Cell::new(0),
        mode: Cell::new(0),
    };
    let mut provider =
        NativeBatchHosts::new(catalog, &metadata, &context).with_native_values(Some(&store));
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
    for index in 0..3 {
        for kind in 0..8 {
            store.borrow_mut().begin_window().unwrap();
            let task = begin(&mut provider, &mut eval);
            let before = context.ready_reads.get();
            for prefix in 0..index {
                assert!(matches!(
                    reply(
                        &mut provider,
                        &mut eval,
                        &task,
                        prefix,
                        Some(
                            insert(&Datum::Null, NativeValueRole::CharInput, 3 + prefix, row)
                                .to_vec()
                        )
                    ),
                    Ok(HostStep::NeedArg(_))
                ));
            }
            let bad = match kind {
                0 => None,
                1 => Some((stale.clone()).to_vec()),
                2 => Some((vec![0; 8]).to_vec()),
                3 => Some((insert(&Datum::Null, NativeValueRole::Output, 3 + index, row)).to_vec()),
                4 => Some((insert(&Datum::Null, NativeValueRole::CharInput, 99, row)).to_vec()),
                5 => Some(
                    (insert(
                        &Datum::Null,
                        NativeValueRole::CharInput,
                        3 + index,
                        InputRow {
                            occurrence: 1,
                            ..row
                        },
                    ))
                    .to_vec(),
                ),
                6 => Some(
                    (insert(
                        &Datum::Null,
                        NativeValueRole::CharInput,
                        3 + index,
                        InputRow {
                            input_row: 1,
                            ..row
                        },
                    ))
                    .to_vec(),
                ),
                _ => {
                    store.borrow_mut().begin_window().unwrap();
                    Some(
                        (insert(&Datum::Null, NativeValueRole::CharInput, 3 + index, row)).to_vec(),
                    )
                }
            };
            if index == 0 && kind == 7 {
                provider.cancel(&task);
                continue;
            }
            assert!(matches!(
                reply(&mut provider, &mut eval, &task, index, bad),
                Err(LocalError::BindingContract(_))
            ));
            assert_eq!(context.ready_reads.get(), before);
            provider.cancel(&task);
            provider.cancel(&task);
        }
    }
    // A real NULL document still demands both later raw arguments first.
    let task = begin(&mut provider, &mut eval);
    let before = context.ready_reads.get();
    for index in 0..2 {
        assert!(matches!(
            reply(
                &mut provider,
                &mut eval,
                &task,
                index,
                Some(insert(&Datum::Null, NativeValueRole::CharInput, 3 + index, row).to_vec())
            ),
            Ok(HostStep::NeedArg(_))
        ));
        assert_eq!(context.ready_reads.get(), before);
    }
    assert!(matches!(
        reply(
            &mut provider,
            &mut eval,
            &task,
            2,
            Some(insert(&Datum::Null, NativeValueRole::CharInput, 5, row).to_vec())
        ),
        Ok(HostStep::Ready(_))
    ));
    assert_eq!(context.id_reads.get(), 0);
    provider.cancel(&task);
    for mode in [0usize, 1, 2, 3, 0] {
        store.borrow_mut().begin_window().unwrap();
        let task = begin(&mut provider, &mut eval);
        let before = context.ready_reads.get();
        for (index, value) in [Datum::new_string(r#"{"a":1}"#), Datum::new_string("$.a")]
            .iter()
            .enumerate()
        {
            assert!(matches!(
                reply(
                    &mut provider,
                    &mut eval,
                    &task,
                    index,
                    Some(insert(value, NativeValueRole::CharInput, 3 + index, row).to_vec())
                ),
                Ok(HostStep::NeedArg(_))
            ));
            assert_eq!(context.ready_reads.get(), before);
        }
        context.mode.set(mode);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            reply(
                &mut provider,
                &mut eval,
                &task,
                2,
                Some(insert(&Datum::Int(9), NativeValueRole::CharInput, 5, row).to_vec()),
            )
        }));
        match mode {
            0 => assert!(
                matches!(&result, Ok(Ok(HostStep::Ready(_)))),
                "{result:?}; native={:?}",
                provider.take_native_error()
            ),
            1 | 2 => assert!(matches!(result, Ok(Err(LocalError::BindingContract(_))))),
            _ => assert!(result.is_err()),
        };
        provider.cancel(&task);
        provider.cancel(&task);
        assert!(reply(&mut provider, &mut eval, &task, 2, None).is_err());
    }
    for prefix in 0..3 {
        for _ in 0..64 {
            store.borrow_mut().begin_window().unwrap();
            let task = begin(&mut provider, &mut eval);
            let before = context.ready_reads.get();
            for index in 0..prefix {
                reply(
                    &mut provider,
                    &mut eval,
                    &task,
                    index,
                    Some(insert(&Datum::Null, NativeValueRole::CharInput, 3 + index, row).to_vec()),
                )
                .unwrap();
            }
            provider.cancel(&task);
            assert_eq!(context.ready_reads.get(), before);
        }
    }
}

#[test]
fn pb_json_source_keeps_actual_binary_value_field_attributes() {
    // SQL binary value arguments use the original typed JSON opaque route;
    // byte payload alone cannot substitute for the decoded source declaration.
    let probes = vec![
        Datum::new_string("x"),
        Datum::Bytes(vec![0, 255, b'a']),
        Datum::Int(7),
        Datum::UInt(u64::MAX),
        Datum::BinaryLiteral(tidb_datatype::BinaryLiteral::from_uint(255, None)),
        Datum::Null,
    ];
    for (sig, arity) in specs() {
        for domain in 0..4 {
            for result in [8, 245] {
                for flags in [None, Some(32)] {
                    for enabled in [false, true] {
                        for probe in &probes {
                            for selection in [None, Some(vec![2, 0, 2]), Some(vec![])] {
                                let mut values = smoke(sig, arity);
                                values[arity - 1] = probe.clone();
                                let build = || {
                                    let mut p = pb_call(
                                        (0..arity)
                                            .map(|_| {
                                                let mut source = declaration(
                                                    match domain {
                                                        2 => 252,
                                                        3 => 254,
                                                        _ => 253,
                                                    },
                                                    Some(if domain == 0 { 0 } else { 128 }),
                                                );
                                                source.charset = Some(
                                                    if domain == 0 { "utf8mb4" } else { "binary" }
                                                        .into(),
                                                );
                                                source.collate =
                                                    Some(if domain == 0 { -46 } else { 63 });
                                                source.flen =
                                                    Some(if domain == 3 { 4 } else { 128 });
                                                atom(
                                                    &Datum::Bytes(b"stored".to_vec()),
                                                    Some(source),
                                                )
                                            })
                                            .collect(),
                                        Some(declaration(result, flags)),
                                    );
                                    p.sig = Some(sig as i32);
                                    let mut root =
                                        crate::distsql_builtin::pb_to_expr(&p, &[]).unwrap();
                                    let Expression::ScalarFunction(function) = &mut root else {
                                        panic!()
                                    };
                                    for (i, arg) in function.args.iter_mut().enumerate() {
                                        let Expression::Constant(c) = arg else {
                                            panic!()
                                        };
                                        c.param_marker =
                                            Some(crate::constant::ParamMarker { order: i as i64 });
                                    }
                                    suite(root)
                                };
                                let owner = build();
                                let actual = build();
                                let ty = output_type(&owner);
                                let old = C::new(enabled, values.clone(), 0);
                                let new = C::new(enabled, values.clone(), 0);
                                for reuse in 0..2 {
                                    let label=format!("{sig:?}/{domain}/{result}/{flags:?}/{enabled}/{probe:?}/{selection:?}/{reuse}");
                                    let mut a = input(&selection);
                                    let mut left = output_for(&ty);
                                    left.append_null(0);
                                    let expected = original_session_read_outcome(|| {
                                        owner.run(&old, &mut a, &mut left)
                                    });
                                    let mut b = input(&selection);
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
                                    assert_eq!(got, expected, "{label}");
                                    assert_eq!(right.column(0), left.column(0), "{label}");
                                    assert_eq!(new.snapshot(), old.snapshot(), "{label}");
                                    let route = actual.batch_route_snapshot();
                                    assert!(route.prepared > 0);
                                    assert_eq!(
                                        (route.refusals, route.fallback),
                                        (0, 0),
                                        "{label}:{route:?}"
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
