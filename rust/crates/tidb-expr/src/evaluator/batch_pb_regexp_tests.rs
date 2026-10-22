// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Genuine decoded protobuf owners, independent of their display names.
use super::*;
use tidb_proto::tipb as wire;

fn field(code: i32, collate: i32) -> wire::FieldType {
    wire::FieldType {
        tp: Some(code),
        flag: Some(0),
        flen: Some(128),
        decimal: Some(-1),
        charset: Some("utf8mb4".into()),
        collate: Some(collate),
        ..Default::default()
    }
}
fn atom(value: &Datum, field: Option<wire::FieldType>) -> wire::Expr {
    let (kind, bytes) = match value {
        Datum::Null => (wire::ExprType::Null, vec![]),
        Datum::Int(value) => {
            let mut bytes = vec![];
            tidb_codec::encode_int(&mut bytes, *value);
            (wire::ExprType::Int64, bytes)
        }
        Datum::Bytes(value) => (wire::ExprType::Bytes, value.clone()),
        Datum::String(value) => (wire::ExprType::String, value.bytes().to_vec()),
        _ => panic!("phase fixture must be an actual decodable wire literal"),
    };
    wire::Expr {
        tp: Some(kind as i32),
        val: Some(bytes),
        field_type: field,
        ..Default::default()
    }
}
fn pb_call(args: Vec<wire::Expr>, field: Option<wire::FieldType>) -> wire::Expr {
    wire::Expr {
        tp: Some(wire::ExprType::ScalarFunc as i32),
        sig: Some(wire::ScalarFuncSig::RegexpLikeSig as i32),
        children: args,
        field_type: field,
        ..Default::default()
    }
}
fn owner(
    arity: usize,
    source: i32,
    result: i32,
    collate: i32,
    mask: usize,
    shape: usize,
    display: &str,
    values: &[Datum; 3],
) -> EvaluatorSuite {
    let mut args = vec![];
    for i in 0..arity {
        let ft = (mask & (2 << i) == 0).then(|| field(source, collate));
        let mut arg = atom(
            if matches!(shape, 0 | 1 | 4) {
                &values[i % 3]
            } else {
                &Datum::Null
            },
            ft.clone(),
        );
        if shape == 1 && i == 1 {
            let mut offset = vec![];
            tidb_codec::encode_int(&mut offset, 0);
            arg = wire::Expr {
                tp: Some(wire::ExprType::ColumnRef as i32),
                val: Some(offset),
                field_type: ft.clone(),
                ..Default::default()
            };
        }
        if shape == 4 && i == 0 {
            arg = pb_call(
                vec![
                    atom(&values[0], Some(field(source, collate))),
                    atom(&Datum::new_string("."), Some(field(253, collate))),
                ],
                ft.clone(),
            );
        }
        // Contextual sources retain genuine wire provenance and their real
        // decoded declarations; only the actual public producer is installed.
        if shape == 2 || shape == 3 {
            arg = atom(&Datum::new_string("wire stored value"), ft);
        }
        args.push(arg);
    }
    let pb = pb_call(args, (mask & 1 == 0).then(|| field(result, collate)));
    let mut root = crate::distsql_builtin::pb_to_expr(&pb, &[string_ty()]).unwrap();
    let Expression::ScalarFunction(function) = &mut root else {
        unreachable!()
    };
    assert_eq!(
        function.pb_signature(),
        Some(wire::ScalarFuncSig::RegexpLikeSig)
    );
    function.func_name = CiString::new(display);
    for (i, arg) in function.args.iter_mut().enumerate() {
        if shape == 2 || shape == 3 {
            let Expression::Constant(c) = arg else {
                unreachable!()
            };
            if shape == 2 {
                c.param_marker = Some(crate::constant::ParamMarker {
                    order: (i % 3) as i64,
                });
            } else {
                c.deferred_expr = Some(Box::new(call(
                    "setvar",
                    string_ty(),
                    vec![
                        literal(Datum::new_string(format!("pb-{i}")), string_ty()),
                        parameter(i % 3, string_ty()),
                    ],
                )));
            }
        }
        if let Expression::ScalarFunction(child) = arg {
            child.func_name = CiString::new("pb child display only");
        }
    }
    suite(root)
}
fn output_type(owner: &EvaluatorSuite) -> FieldType {
    snapshot_field_type(owner.program.calculated[0].static_type().unwrap())
}
fn input(selection: &Option<Vec<usize>>) -> Chunk {
    let mut c = chunk(
        &[string_ty()],
        &[
            vec![Datum::new_string("^a")],
            vec![Datum::new_string("(")],
            vec![Datum::Null],
        ],
    );
    c.set_sel(selection.clone());
    c
}
fn observe(hash: &mut u64, s: &str) {
    for byte in s.bytes() {
        *hash = (*hash ^ u64::from(byte)).wrapping_mul(0x100000001b3);
    }
}
#[test]
fn pb_regexp_source_original_phases() {
    if isolate_common_corpus("stateful_tests::pb_regexp_tests::pb_regexp_source_original_phases") {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    matrix(false, false);
}
#[test]
fn pb_regexp_source_actual_phases() {
    if isolate_common_corpus("stateful_tests::pb_regexp_tests::pb_regexp_source_actual_phases") {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    matrix(true, false);
}
#[test]
fn pb_regexp_source_original_full() {
    if isolate_common_corpus("stateful_tests::pb_regexp_tests::pb_regexp_source_original_full") {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    matrix(false, true);
}
#[test]
fn pb_regexp_source_actual_full() {
    if isolate_common_corpus("stateful_tests::pb_regexp_tests::pb_regexp_source_actual_full") {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    matrix(true, true);
}
fn matrix(compare: bool, full: bool) {
    let now = chrono::DateTime::parse_from_rfc3339("2026-10-21T00:00:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc);
    tidb_datatype::with_test_conversion_clock(now, || {
        let fields = if full {
            vec![8, 253, 5, 246, 12, 11, 245, 6]
        } else {
            vec![8, 253, 5]
        };
        let results = if full { vec![8] } else { vec![8, 253, 245] };
        let displays = if full {
            vec!["wire display", "getvar"]
        } else {
            vec!["wire display", "regexp_like", "getvar", "ifnull"]
        };
        let samples: Vec<_> = if full {
            crate::tikv::byte_unary_value_tests::values()
                .into_iter()
                .enumerate()
                .flat_map(|(vi, value)| {
                    (0..3).map(move |position| {
                        let mut v = [
                            Datum::new_string("abc"),
                            Datum::new_string("^a"),
                            Datum::new_string(""),
                        ];
                        v[position] = value.clone();
                        (vi * 3 + position, position, v)
                    })
                })
                .collect()
        } else {
            vec![
                (
                    0,
                    0,
                    [
                        Datum::Null,
                        Datum::Bytes(vec![255]),
                        Datum::Bytes(vec![255]),
                    ],
                ),
                (
                    1,
                    0,
                    [
                        Datum::Bytes(vec![255]),
                        Datum::Null,
                        Datum::Bytes(vec![255]),
                    ],
                ),
                (
                    2,
                    0,
                    [
                        Datum::new_string("abc"),
                        Datum::new_string("^a"),
                        Datum::new_string(""),
                    ],
                ),
                (
                    3,
                    0,
                    [
                        Datum::new_string("a\nb"),
                        Datum::new_string("^b"),
                        Datum::new_string("m"),
                    ],
                ),
                (
                    4,
                    0,
                    [
                        Datum::new_string("abc"),
                        Datum::new_string("("),
                        Datum::new_string("i"),
                    ],
                ),
                (
                    5,
                    0,
                    [
                        Datum::Int(123),
                        Datum::new_string("1"),
                        Datum::new_string("c"),
                    ],
                ),
                (
                    6,
                    0,
                    [
                        Datum::new_string("ABC"),
                        Datum::new_string("a"),
                        Datum::new_string("z"),
                    ],
                ),
            ]
        };
        let (mut count, mut hash) = (0usize, 0xcbf29ce484222325u64);
        let mut gaps = vec![];
        for arity in if full {
            vec![2, 3]
        } else {
            vec![0, 1, 2, 3, 4, 9]
        } {
            for &source in &fields {
                for &result in &results {
                    for collate in if full { vec![-46] } else { vec![46, -45] } {
                        let masks = if full || !matches!(arity, 2 | 3) {
                            vec![0, (1 << (arity + 1)) - 1]
                        } else {
                            (0..1 << (arity + 1)).collect()
                        };
                        for mask in masks {
                            for &display in &displays {
                                for shape in if full { vec![2] } else { vec![0, 1, 2, 3, 4] } {
                                    for (vi, position, values) in &samples {
                                        if full && *position >= arity {
                                            continue;
                                        }
                                        for profile in if full { vec![0] } else { vec![0, 2, 6, 7] }
                                        {
                                            for enabled in [false, true] {
                                                for selection in
                                                    [None, Some(vec![2, 0, 2]), Some(vec![])]
                                                {
                                                    let original = owner(
                                                        arity, source, result, collate, mask,
                                                        shape, display, values,
                                                    );
                                                    let actual = owner(
                                                        arity, source, result, collate, mask,
                                                        shape, display, values,
                                                    );
                                                    let ty = output_type(&original);
                                                    let old = RegexpSourceContext::new(
                                                        enabled,
                                                        values.clone(),
                                                        profile,
                                                    );
                                                    let new = RegexpSourceContext::new(
                                                        enabled,
                                                        values.clone(),
                                                        profile,
                                                    );
                                                    for reuse in 0..2 {
                                                        let mut a = input(&selection);
                                                        let mut left = output_for(&ty);
                                                        left.append_null(0);
                                                        let expected = session_read_outcome(|| {
                                                            original.run(&old, &mut a, &mut left)
                                                        });
                                                        let label=format!("pb-regexp/{full}/{arity}/{source}/{result}/{collate}/{mask}/{display}/{shape}/{vi}/{profile}/{enabled}/{selection:?}/{reuse}");
                                                        if compare {
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
                                                            if got != expected
                                                                || left.column(0) != right.column(0)
                                                                || old.snapshot() != new.snapshot()
                                                            {
                                                                eprintln!("PB REGEXP DIFFERENCE {label} old={expected:?} new={got:?} oldcol={:?} newcol={:?} oldevents={} newevents={}",left.column(0),right.column(0),old.snapshot(),new.snapshot());
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
                                                            let route =
                                                                actual.batch_route_snapshot();
                                                            if (route.fallback != 0
                                                                || route.refusals != 0)
                                                                && gaps.len() < 12
                                                            {
                                                                gaps.push(format!(
                                                                    "{label}:{route:?}"
                                                                ));
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
            }
        }
        eprintln!("PB REGEXP ORIGINAL full={full} {count}/{hash:016X}");
        assert_eq!(
            (count, hash),
            if full {
                (286080, 0x001C38829809F571)
            } else {
                (3870720, 0x0259457ADE53B559)
            }
        );
        if !gaps.is_empty() {
            eprintln!("PB REGEXP SOURCE RED {gaps:?}");
        }
        assert!(gaps.is_empty());
    });
}
#[test]
fn pb_regexp_source_original_long() {
    if isolate_common_corpus("stateful_tests::pb_regexp_tests::pb_regexp_source_original_long") {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    long(false);
}
#[test]
fn pb_regexp_source_actual_long() {
    if isolate_common_corpus("stateful_tests::pb_regexp_tests::pb_regexp_source_actual_long") {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    long(true);
}
fn long(compare: bool) {
    let (mut count, mut hash) = (0usize, 0xcbf29ce484222325u64);
    for arity in [2, 3] {
        for display in ["wire display", "regexp_like", "getvar", "ifnull"] {
            for profile in 0..8 {
                for rows in [0usize, 1, 3, 1026, 2051] {
                    for first in [false, true] {
                        let values = [
                            Datum::new_string("abc"),
                            Datum::new_string("^a"),
                            Datum::new_string(""),
                        ];
                        let original = owner(arity, 8, 8, -46, 0, 2, display, &values);
                        let actual = owner(arity, 8, 8, -46, 0, 2, display, &values);
                        let mut old = RegexpSourceContext::new(first, values.clone(), profile);
                        let mut new = RegexpSourceContext::new(first, values, profile);
                        for (round, enabled) in
                            [first, !first, first, !first].into_iter().enumerate()
                        {
                            old.base.enabled.set(enabled);
                            new.base.enabled.set(enabled);
                            old.id.set(if round == 2 { 18 } else { 17 });
                            new.id.set(old.id.get());
                            old.base.values[1] =
                                Datum::new_string(if round == 1 { "[" } else { "^a" });
                            new.base.values[1] = old.base.values[1].clone();
                            let selection = Some((0..rows).map(|i| [2, 0, 2][i % 3]).collect());
                            let mut a = input(&selection);
                            let mut left = output_for(&ty());
                            left.append_null(0);
                            let expected =
                                session_read_outcome(|| original.run(&old, &mut a, &mut left));
                            let label = format!(
                                "pb-long/{arity}/{display}/{profile}/{rows}/{first}/{round}"
                            );
                            if compare {
                                let mut b = input(&selection);
                                let mut right = output_for(&ty());
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
                                    || left.column(0) != right.column(0)
                                    || old.snapshot() != new.snapshot()
                                {
                                    eprintln!("PB REGEXP LONG DIFFERENCE {label} old={expected:?} new={got:?} oldcol={:?} newcol={:?} oldevents={} newevents={}",left.column(0),right.column(0),old.snapshot(),new.snapshot());
                                }
                                assert_eq!(got, expected, "{label}");
                                assert_eq!(right.column(0), left.column(0), "{label}");
                                assert_eq!(new.snapshot(), old.snapshot(), "{label}");
                                let route = actual.batch_route_snapshot();
                                assert_eq!(
                                    (route.fallback, route.refusals),
                                    (0, 0),
                                    "{label}/{route:?}"
                                );
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
    eprintln!("PB REGEXP LONG ORIGINAL {count}/{hash:016X}");
    assert_eq!((count, hash), (2560, 0x40176B6F9BC54430));
}

#[test]
fn pb_regexp_source_wire_attributes_keep_effective_declarations() {
    if isolate_common_corpus("stateful_tests::pb_regexp_tests::pb_regexp_source_wire_attributes_keep_effective_declarations"){return;}
    std::panic::set_hook(Box::new(|_| {}));
    let mut count = 0;
    for arity in [2, 3] {
        for source in [8, 253, 5, 245] {
            for source_flags in [None, Some(0), Some(32), Some(33)] {
                for result_flags in [None, Some(0), Some(32), Some(33)] {
                    for display in ["wire display", "regexp_like", "getvar", "ifnull"] {
                        for values in [
                            [
                                Datum::new_string("abc"),
                                Datum::new_string("^a"),
                                Datum::new_string(""),
                            ],
                            [
                                Datum::Null,
                                Datum::Bytes(vec![255]),
                                Datum::Bytes(vec![255]),
                            ],
                            [
                                Datum::Int(123),
                                Datum::new_string("1"),
                                Datum::new_string("c"),
                            ],
                            [
                                Datum::Bytes(vec![255]),
                                Datum::new_string("^a"),
                                Datum::new_string(""),
                            ],
                        ] {
                            for enabled in [false, true] {
                                for selection in [None, Some(vec![2, 0, 2]), Some(vec![])] {
                                    let make = || {
                                        let mut result = field(8, -46);
                                        result.flag = result_flags;
                                        let args = (0..arity)
                                            .map(|_| {
                                                let mut source = field(source, -45);
                                                source.flag = source_flags;
                                                atom(
                                                    &Datum::new_string("actual wire"),
                                                    Some(source),
                                                )
                                            })
                                            .collect();
                                        let pb = pb_call(args, Some(result));
                                        let mut root =
                                            crate::distsql_builtin::pb_to_expr(&pb, &[]).unwrap();
                                        let Expression::ScalarFunction(f) = &mut root else {
                                            unreachable!()
                                        };
                                        f.func_name = CiString::new(display);
                                        for (i, arg) in f.args.iter_mut().enumerate() {
                                            let Expression::Constant(c) = arg else {
                                                unreachable!()
                                            };
                                            c.param_marker = Some(crate::constant::ParamMarker {
                                                order: i as i64,
                                            });
                                        }
                                        suite(root)
                                    };
                                    let original = make();
                                    let actual = make();
                                    let ty = output_type(&original);
                                    assert_eq!(
                                        ty.is_unsigned(),
                                        result_flags.is_some_and(|flag| flag & 32 != 0)
                                    );
                                    let old = RegexpSourceContext::new(enabled, values.clone(), 0);
                                    let new = RegexpSourceContext::new(enabled, values.clone(), 0);
                                    let mut a = input(&selection);
                                    let mut left = output_for(&ty);
                                    left.append_null(0);
                                    let expected = session_read_outcome(|| {
                                        original.run(&old, &mut a, &mut left)
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
                                    let label=format!("{arity}/{source}/{source_flags:?}/{result_flags:?}/{display}/{enabled}/{selection:?}");
                                    assert_eq!(got, expected, "{label}");
                                    assert_eq!(right.column(0), left.column(0), "{label}");
                                    assert_eq!(new.snapshot(), old.snapshot(), "{label}");
                                    let route = actual.batch_route_snapshot();
                                    assert_eq!(
                                        (route.fallback, route.refusals),
                                        (0, 0),
                                        "{label}/{route:?}"
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
    assert_eq!(count, 12288);
}

#[test]
fn pb_regexp_source_provenance_and_legacy_precedence_guards() {
    let make = |arity| {
        crate::distsql_builtin::pb_to_expr(
            &pb_call(
                (0..arity)
                    .map(|_| atom(&Datum::Int(4), Some(field(8, -46))))
                    .collect(),
                Some(field(8, -46)),
            ),
            &[],
        )
        .unwrap()
    };
    for bad in 0..6 {
        let mut root = make(if bad == 2 { 1 } else { 2 });
        let Expression::ScalarFunction(f) = &mut root else {
            unreachable!()
        };
        match bad {
            0 => f.ret_type = Some(FieldType::new(FieldTypeCode::Double)),
            1 => {
                let Expression::Constant(c) = &mut f.args[0] else {
                    unreachable!()
                };
                c.ret_type = Some(FieldType::new(FieldTypeCode::Double));
            }
            2 => f.ret_type = None,
            3 => {
                f.args.pop();
            }
            4 => {
                let mut other = pb_call(
                    vec![atom(&Datum::Int(1), Some(field(8, -46)))],
                    Some(field(8, -46)),
                );
                other.sig = Some(wire::ScalarFuncSig::UpperUtf8 as i32);
                let Expression::ScalarFunction(other) =
                    crate::distsql_builtin::pb_to_expr(&other, &[]).unwrap()
                else {
                    unreachable!()
                };
                f.pb_origin = other.pb_origin;
            }
            _ => {
                let Expression::Constant(other) = crate::distsql_builtin::pb_to_expr(
                    &atom(&Datum::Int(1), Some(field(8, -46))),
                    &[],
                )
                .unwrap() else {
                    unreachable!()
                };
                f.pb_origin = other.pb_origin;
            }
        }
        let owner = suite(root);
        let failure = PreparedBatchSuite::compile(
            &owner,
            &[string_ty()],
            true,
            940,
            limits(),
            ExecutionLimits::default(),
            1 << 20,
        )
        .err()
        .expect("stale provenance must be refused");
        assert_eq!(failure.refusal(), BatchRefusal::Admission, "bad={bad}");
    }
    // A different genuine selected signature is still outside this closure.
    let mut other = pb_call(
        vec![atom(&Datum::new_string("a"), Some(field(253, -46)))],
        Some(field(253, -46)),
    );
    other.sig = Some(wire::ScalarFuncSig::UpperUtf8 as i32);
    let owner = suite(crate::distsql_builtin::pb_to_expr(&other, &[]).unwrap());
    assert!(PreparedBatchSuite::compile(
        &owner,
        &[],
        true,
        940,
        limits(),
        ExecutionLimits::default(),
        1 << 20
    )
    .is_err());
    // The arithmetic root remains an explicit gap. The three unary roots now
    // retain both their original bulk body and scalar PB dispatch.
    for (name, arity, result) in [
        ("plus", 2, 8),
        ("abs", 1, 8),
        ("cast_decimal", 1, 246),
        ("cast_json", 1, 245),
    ] {
        let make = || {
            let pb = pb_call(
                (0..arity)
                    .map(|_| atom(&Datum::Int(4), Some(field(8, -46))))
                    .collect(),
                Some(field(result, -46)),
            );
            let mut root = crate::distsql_builtin::pb_to_expr(&pb, &[]).unwrap();
            let Expression::ScalarFunction(f) = &mut root else {
                unreachable!()
            };
            f.func_name = CiString::new(name);
            assert!(
                crate::scalar_function::static_batch_target(&root).is_some(),
                "{name}"
            );
            suite(root)
        };
        for enabled in [false, true] {
            let original = make();
            let actual = make();
            let ty = output_type(&original);
            let values = [Datum::Null, Datum::Null, Datum::Null];
            let old = RegexpSourceContext::new(enabled, values.clone(), 0);
            let new = RegexpSourceContext::new(enabled, values.clone(), 0);
            let mut a = input(&None);
            let mut left = output_for(&ty);
            let expected = session_read_outcome(|| original.run(&old, &mut a, &mut left));
            let mut b = input(&None);
            let mut right = output_for(&ty);
            let got = session_read_outcome(|| {
                actual.run_with_tikv_batch(&new, &mut b, &mut right, &[string_ty()])
            });
            assert_eq!(got, expected, "{name}/{enabled}");
            assert_eq!(right.column(0), left.column(0));
            assert_eq!(new.snapshot(), old.snapshot());
            let route = actual.batch_route_snapshot();
            assert_eq!(
                (route.prepared, route.fallback, route.refusals),
                if name == "plus" { (0, 1, 1) } else { (1, 0, 0) },
                "{name}/{enabled}/{route:?}"
            );
        }
    }
}

#[test]
fn pb_regexp_source_cache_identity_and_same_cap_recovery() {
    if isolate_common_corpus(
        "stateful_tests::pb_regexp_tests::pb_regexp_source_cache_identity_and_same_cap_recovery",
    ) {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    let values = [
        Datum::new_string("abc"),
        Datum::new_string("^a"),
        Datum::new_string(""),
    ];
    let owner = owner(2, 253, 8, -46, 0, 0, "getvar", &values);
    let mut prepared = PreparedBatchSuite::compile(
        &owner,
        &[string_ty()],
        true,
        940,
        limits(),
        ExecutionLimits::default(),
        1 << 20,
    )
    .unwrap();
    let NativeHostMetadata::RegexpValues(meta) = &prepared.workers[0].source.host_metadata[0]
    else {
        panic!("actual PB regexp metadata")
    };
    assert!(!meta.operator);
    assert!(meta
        .invocation
        .with_context_id(17)
        .prepare_like("(", "")
        .execute("abc")
        .is_err());
    let old = RegexpSourceContext::new(false, values.clone(), 0);
    let new = RegexpSourceContext::new(false, values.clone(), 0);
    for id in [17, 18, 17] {
        old.id.set(id);
        new.id.set(id);
        let mut a = input(&None);
        let mut left = output_for(&ty());
        left.append_null(0);
        let expected = session_read_outcome(|| owner.run(&old, &mut a, &mut left));
        let mut b = input(&None);
        let mut right = output_for(&ty());
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
        assert_eq!(got, expected);
        assert_eq!(right.column(0), left.column(0));
        assert_eq!(new.snapshot(), old.snapshot());
        if id == 18 {
            assert_eq!(right.num_rows(), 4);
        }
    }
    for arity in [2, 3] {
        let owner = self::owner(arity, 253, 8, -46, 0, 2, "wire display", &values);
        let mut prepared = PreparedBatchSuite::compile(
            &owner,
            &[string_ty()],
            true,
            940,
            limits(),
            ExecutionLimits::default(),
            1 << 20,
        )
        .unwrap();
        prepared.workers[0].max_materialization_retained_bytes = 16 * 1024;
        let mut ctx = RegexpSourceContext::new(
            true,
            [
                Datum::new_string("x".repeat(32768)),
                Datum::new_string("x"),
                Datum::new_string(""),
            ],
            0,
        );
        let mut first = input(&None);
        let mut output = output_for(&ty());
        output.append_null(0);
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
        assert!(matches!(
            failure.local_error(),
            Some(LocalError::ResourceLimit(_))
        ));
        assert_eq!(output.num_rows(), 1);
        ctx.base.values[0] = Datum::Null;
        ctx.base.enabled.set(false);
        ctx.base.events.borrow_mut().clear();
        for n in &ctx.base.reads {
            n.set(0);
        }
        ctx.context_reads.set(0);
        let old = RegexpSourceContext::new(false, ctx.base.values.clone(), 0);
        let mut a = input(&None);
        let mut left = output_for(&ty());
        left.append_null(0);
        let expected = session_read_outcome(|| owner.run(&old, &mut a, &mut left));
        let mut b = input(&None);
        let mut right = output_for(&ty());
        right.append_null(0);
        let got = session_read_outcome(|| {
            prepared
                .run(
                    &owner,
                    &ctx,
                    &mut EvalContext::default(),
                    &mut b,
                    &mut right,
                    &[string_ty()],
                )
                .map_err(BatchFailure::into_evaluator_error)
        });
        assert_eq!(got, expected);
        assert_eq!(right.column(0), left.column(0));
        assert_eq!(ctx.snapshot(), old.snapshot());
        assert_eq!(
            prepared.workers[0].max_materialization_retained_bytes,
            16 * 1024
        );
    }
}

#[path = "batch_pb_result_tests.rs"]
mod result_tests;

#[path = "batch_pb_unary_tests.rs"]
mod unary_tests;
