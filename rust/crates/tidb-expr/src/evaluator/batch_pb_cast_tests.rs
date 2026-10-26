// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Original selected PB CAST typed-source, header and value contracts.
use super::*;
fn specs() -> Vec<(Sig, usize)> {
    vec![
        (Sig::CastDecimalAsDuration, 1),
        (Sig::CastDecimalAsInt, 1),
        (Sig::CastDecimalAsJson, 1),
        (Sig::CastDecimalAsReal, 1),
        (Sig::CastDecimalAsString, 1),
        (Sig::CastDecimalAsTime, 1),
        (Sig::CastDurationAsDecimal, 1),
        (Sig::CastDurationAsInt, 1),
        (Sig::CastDurationAsJson, 1),
        (Sig::CastDurationAsReal, 1),
        (Sig::CastDurationAsString, 1),
        (Sig::CastDurationAsTime, 1),
        (Sig::CastIntAsDecimal, 1),
        (Sig::CastIntAsDuration, 1),
        (Sig::CastIntAsJson, 1),
        (Sig::CastIntAsReal, 1),
        (Sig::CastIntAsString, 1),
        (Sig::CastIntAsTime, 1),
        (Sig::CastJsonAsDecimal, 1),
        (Sig::CastJsonAsDuration, 1),
        (Sig::CastJsonAsInt, 1),
        (Sig::CastJsonAsReal, 1),
        (Sig::CastJsonAsString, 1),
        (Sig::CastJsonAsTime, 1),
        (Sig::CastRealAsDecimal, 1),
        (Sig::CastRealAsDuration, 1),
        (Sig::CastRealAsInt, 1),
        (Sig::CastRealAsJson, 1),
        (Sig::CastRealAsString, 1),
        (Sig::CastRealAsTime, 1),
        (Sig::CastStringAsDecimal, 1),
        (Sig::CastStringAsDuration, 1),
        (Sig::CastStringAsInt, 1),
        (Sig::CastStringAsJson, 1),
        (Sig::CastStringAsReal, 1),
        (Sig::CastStringAsTime, 1),
        (Sig::CastTimeAsDecimal, 1),
        (Sig::CastTimeAsDuration, 1),
        (Sig::CastTimeAsInt, 1),
        (Sig::CastTimeAsJson, 1),
        (Sig::CastTimeAsReal, 1),
        (Sig::CastTimeAsString, 1),
        (Sig::CastTimeAsTime, 1),
    ]
}
fn result_code(sig: Sig) -> i32 {
    match sig {
        Sig::CastDecimalAsDuration => 11,
        Sig::CastDecimalAsInt => 8,
        Sig::CastDecimalAsJson => 245,
        Sig::CastDecimalAsReal => 5,
        Sig::CastDecimalAsString => 253,
        Sig::CastDecimalAsTime => 12,
        Sig::CastDurationAsDecimal => 246,
        Sig::CastDurationAsInt => 8,
        Sig::CastDurationAsJson => 245,
        Sig::CastDurationAsReal => 5,
        Sig::CastDurationAsString => 253,
        Sig::CastDurationAsTime => 12,
        Sig::CastIntAsDecimal => 246,
        Sig::CastIntAsDuration => 11,
        Sig::CastIntAsJson => 245,
        Sig::CastIntAsReal => 5,
        Sig::CastIntAsString => 253,
        Sig::CastIntAsTime => 12,
        Sig::CastJsonAsDecimal => 246,
        Sig::CastJsonAsDuration => 11,
        Sig::CastJsonAsInt => 8,
        Sig::CastJsonAsReal => 5,
        Sig::CastJsonAsString => 253,
        Sig::CastJsonAsTime => 12,
        Sig::CastRealAsDecimal => 246,
        Sig::CastRealAsDuration => 11,
        Sig::CastRealAsInt => 8,
        Sig::CastRealAsJson => 245,
        Sig::CastRealAsString => 253,
        Sig::CastRealAsTime => 12,
        Sig::CastStringAsDecimal => 246,
        Sig::CastStringAsDuration => 11,
        Sig::CastStringAsInt => 8,
        Sig::CastStringAsJson => 245,
        Sig::CastStringAsReal => 5,
        Sig::CastStringAsTime => 12,
        Sig::CastTimeAsDecimal => 246,
        Sig::CastTimeAsDuration => 11,
        Sig::CastTimeAsInt => 8,
        Sig::CastTimeAsJson => 245,
        Sig::CastTimeAsReal => 5,
        Sig::CastTimeAsString => 253,
        Sig::CastTimeAsTime => 12,
        _ => unreachable!("captured actual cast table"),
    }
}
fn smoke(_sig: Sig, arity: usize) -> Vec<Datum> {
    vec![Datum::Int(4); arity]
}
fn variation(_sig: Sig, values: &mut [Datum], sample: usize) {
    if values.is_empty() {
        return;
    }
    values[0] = match sample {
        1 => Datum::Null,
        2 => Datum::Int(-4),
        3 => Datum::UInt(u64::MAX),
        4 => Datum::Real(1.5),
        5 => Datum::new_string("1bad"),
        6 => Datum::Bytes(vec![255]),
        7 => Datum::new_string("2026-10-21 12:34:56.123456"),
        8 => Datum::Decimal(tidb_datatype::Decimal::from_literal("2.500")),
        _ => Datum::Int(4),
    };
}
fn cast_atom(value: &Datum, field: Option<wire::FieldType>) -> wire::Expr {
    let mut bytes = vec![];
    let kind = match value {
        Datum::UInt(value) => {
            tidb_codec::encode_uint(&mut bytes, *value);
            wire::ExprType::Uint64
        }
        Datum::Real(value) => {
            tidb_codec::encode_float(&mut bytes, *value);
            wire::ExprType::Float64
        }
        Datum::Decimal(value) => {
            let (precision, scale) = value.storage_shape();
            tidb_codec::encode_decimal_fixed(&mut bytes, value, precision, scale).unwrap();
            wire::ExprType::MysqlDecimal
        }
        _ => return atom(value, field),
    };
    wire::Expr {
        tp: Some(kind as i32),
        val: Some(bytes),
        field_type: field,
        ..Default::default()
    }
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
                cast_atom(
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
                        literal(Datum::new_string(format!("cast-{i}")), string_ty()),
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
    let expected = session_read_outcome(|| owner.run(old, &mut a, &mut left));
    if compare {
        let mut b = input(selection);
        let mut right = output_for(&ty);
        right.append_null(0);
        let got = session_read_outcome(|| {
            actual.run_with_tikv_batch(new, &mut b, &mut right, &[string_ty()])
        });
        if got != expected || right.column(0) != left.column(0) || new.snapshot() != old.snapshot()
        {
            eprintln!("CAST DIFFERENCE {label} old={expected} new={got} oldcol={:?} newcol={:?} oldctx={} newctx={}",left.column(0),right.column(0),old.snapshot(),new.snapshot());
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
fn pb_cast_source_original_phases() {
    if isolate_common_corpus(
        "stateful_tests::pb_regexp_tests::eager_tests::cast_tests::pb_cast_source_original_phases",
    ) {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    matrix(false, false);
}
#[test]
fn pb_cast_source_actual_phases() {
    if isolate_common_corpus(
        "stateful_tests::pb_regexp_tests::eager_tests::cast_tests::pb_cast_source_actual_phases",
    ) {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    matrix(true, false);
}
#[test]
fn pb_cast_source_original_full() {
    if isolate_common_corpus(
        "stateful_tests::pb_regexp_tests::eager_tests::cast_tests::pb_cast_source_original_full",
    ) {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    matrix(false, true);
}
#[test]
fn pb_cast_source_actual_full() {
    if isolate_common_corpus(
        "stateful_tests::pb_regexp_tests::eager_tests::cast_tests::pb_cast_source_actual_full",
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
                vec![0, 1, 2, 9]
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
                    for sample in 1..9 {
                        let mut values = base.clone();
                        variation(sig, &mut values, sample);
                        samples.push((sample, values));
                    }
                }
                for source in if full { vec![0, 1, 2, 4] } else { vec![0, 1] } {
                    for result in if full {
                        vec![8, 5, 246, 245, 253, 12, 11]
                    } else {
                        vec![8, 5, 246, 245, 253, 12, 11, 16]
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
                                ]
                            } {
                                for shape in if full { vec![1] } else { vec![0, 1] } {
                                    for (vi, values) in &samples {
                                        for profile in if full { vec![0] } else { vec![0] } {
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
                                                        let label=format!("pb-cast/{full}/{sig:?}/{arity}/{source}/{result}/{flags:?}/{name}/{shape}/{vi}/{profile}/{enabled}/{selection:?}/{reuse}");
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
        eprintln!("PB CAST ORIGINAL full={full} {count}/{hash:016X}");
        // n49 immutable original observations, captured before production.
        assert_eq!(
            (count, hash),
            if full {
                (8611008usize, 0x0B214D6A23760DFDu64)
            } else {
                (2465792usize, 0x1F3CA1870CD1D5FBu64)
            }
        );
        if !gaps.is_empty() {
            eprintln!("PB CAST SOURCE RED {gaps:?}");
        }
        assert!(gaps.is_empty());
    });
}
#[test]
fn pb_cast_source_original_long() {
    if isolate_common_corpus(
        "stateful_tests::pb_regexp_tests::eager_tests::cast_tests::pb_cast_source_original_long",
    ) {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    long(false);
}
#[test]
fn pb_cast_source_actual_long() {
    if isolate_common_corpus(
        "stateful_tests::pb_regexp_tests::eager_tests::cast_tests::pb_cast_source_actual_long",
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
                                            [false, true, true, false, true, false, true]
                                                .into_iter()
                                                .enumerate()
                                        {
                                            let mut values = values.clone();
                                            variation(
                                                sig,
                                                &mut values,
                                                [0, 4, 1, 7, 8, 9, 11][round],
                                            );
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
                                            let label=format!("pb-cast-long/{sig:?}/{result}/{name}/{shape}/{profile}/{n}/{selected}/{reuse}/{round}/{enabled}/{id}");
                                            let mut a = build();
                                            let mut left = output_for(&ty);
                                            left.append_null(0);
                                            let expected = session_read_outcome(|| {
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
        eprintln!("PB CAST LONG ORIGINAL {count}/{hash:016X}");
        assert_eq!((count, hash), (144480usize, 0xF5CE92D5D77D368Fu64));
        if !gaps.is_empty() {
            eprintln!("PB CAST LONG SOURCE RED {gaps:?}");
        }
        assert!(gaps.is_empty());
    });
}
#[test]
fn pb_cast_source_original_physical() {
    if isolate_common_corpus("stateful_tests::pb_regexp_tests::eager_tests::cast_tests::pb_cast_source_original_physical"){return;}
    std::panic::set_hook(Box::new(|_| {}));
    physical(false);
}
#[test]
fn pb_cast_source_actual_physical() {
    if isolate_common_corpus(
        "stateful_tests::pb_regexp_tests::eager_tests::cast_tests::pb_cast_source_actual_physical",
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
                                                let label=format!("pb-cast-physical/{sig:?}/{result}/{flags:?}/{name}/{nested}/{sample}/{enabled}/{n}/{selection}/{reuse}");
                                                let mut a = build();
                                                let mut left = output_for(&ty);
                                                left.append_null(0);
                                                let expected = session_read_outcome(|| {
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
        eprintln!("PB CAST PHYSICAL ORIGINAL {count}/{hash:016X}");
        assert_eq!((count, hash), (495360usize, 0xBFF076D62C9F3A1Du64));
        if !gaps.is_empty() {
            eprintln!("PB CAST PHYSICAL SOURCE RED {gaps:?}");
        }
        assert!(gaps.is_empty());
    });
}

#[test]
fn pb_cast_source_original_literal_flags() {
    if isolate_common_corpus("stateful_tests::pb_regexp_tests::eager_tests::cast_tests::pb_cast_source_original_literal_flags"){return;}
    std::panic::set_hook(Box::new(|_| {}));
    literal_flags(false);
}
#[test]
fn pb_cast_source_actual_literal_flags() {
    if isolate_common_corpus("stateful_tests::pb_regexp_tests::eager_tests::cast_tests::pb_cast_source_actual_literal_flags"){return;}
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
                for flags in 0usize..4 {
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
                                                let args=pair.iter().take(1).enumerate().map(|(i,value)|{let field=crate::distsql_builtin::pb_type_to_field_type(&declaration(source,Some(if flags&(2<<i)!=0{32}else{0})));literal(value.map_or(Datum::Null,|value|Datum::BinaryLiteral(tidb_datatype::BinaryLiteral::from_uint(value,None))),field)}).collect();
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
                                                let label=format!("pb-cast-literal/{sig:?}/{source}/{flags}/{result}/{name}/{sample}/{profile}/{enabled}/{selection:?}/{reuse}");
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
        eprintln!("PB CAST LITERAL ORIGINAL {count}/{hash:016X}");
        assert_eq!((count, hash), (297216usize, 0x0D4706B892EE41F5u64));
        if !gaps.is_empty() {
            eprintln!("PB CAST LITERAL SOURCE RED {gaps:?}");
        }
        assert!(gaps.is_empty());
    });
}
#[test]
fn pb_cast_source_original_source_families() {
    if isolate_common_corpus("stateful_tests::pb_regexp_tests::eager_tests::cast_tests::pb_cast_source_original_source_families"){return;}
    std::panic::set_hook(Box::new(|_| {}));
    source_families(false);
}
#[test]
fn pb_cast_source_actual_source_families() {
    if isolate_common_corpus("stateful_tests::pb_regexp_tests::eager_tests::cast_tests::pb_cast_source_actual_source_families"){return;}
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
                                        let mut values = if sample == 0 {
                                            vec![Datum::Int(4), Datum::Int(2)]
                                        } else {
                                            vec![Datum::new_string("4.5"), Datum::new_string("2")]
                                        };
                                        values.truncate(1);
                                        let owner =
                                            make(sig, source, result, None, name, 1, &values);
                                        let actual =
                                            make(sig, source, result, None, name, 1, &values);
                                        let old = C::new(enabled, values.clone(), profile);
                                        let new = C::new(enabled, values.clone(), profile);
                                        for reuse in 0..2 {
                                            let label=format!("pb-cast-families/{sig:?}/{source}/{sample}/{result}/{name}/{profile}/{enabled}/{selection:?}/{reuse}");
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
        eprintln!("PB CAST FAMILIES ORIGINAL {count}/{hash:016X}");
        assert_eq!((count, hash), (185760usize, 0x92E63C2709F42841u64));
        if !gaps.is_empty() {
            eprintln!("PB CAST FAMILIES SOURCE RED {gaps:?}");
        }
        assert!(gaps.is_empty());
    });
}

#[test]
fn pb_cast_source_original_optional_declarations() {
    if isolate_common_corpus("stateful_tests::pb_regexp_tests::eager_tests::cast_tests::pb_cast_source_original_optional_declarations") {return;}
    std::panic::set_hook(Box::new(|_| {}));
    optional_declarations(false);
}
#[test]
fn pb_cast_source_actual_optional_declarations() {
    if isolate_common_corpus("stateful_tests::pb_regexp_tests::eager_tests::cast_tests::pb_cast_source_actual_optional_declarations") {return;}
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
                                for shape in [0, 1, 2, 3, 4] {
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
                let field=crate::distsql_builtin::pb_type_to_field_type(&declaration(source,Some(0)));
                if shape==2 {let mut col=crate::column::Column::new(990+i as i64,field);if absent&(2<<i)!=0{col.ret_type=None;}return Expression::CorrelatedColumn(crate::column::CorrelatedColumn::with_value(col,value.clone()));}
                if shape==4 {let mut nested=ScalarFunction::new(CiString::new("coalesce"),field,vec![parameter(i,string_ty()),literal(Datum::Null,string_ty())]);if absent&(2<<i)!=0{nested.ret_type=None;}return Expression::ScalarFunction(nested);}
                let mut c=crate::constant::Constant::new(if shape==0 {value.clone()} else {Datum::Bytes(b"stored".to_vec())},field);
                if absent&(2<<i)!=0 {c.ret_type=None;}
                if shape==1 {c.param_marker=Some(crate::constant::ParamMarker{order:i as i64});}
                if shape==3 {c.deferred_expr=Some(Box::new(call("setvar",string_ty(),vec![literal(Datum::new_string(format!("cast-optional-{i}")),string_ty()),parameter(i,string_ty())])));}
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
                                                    let label=format!("pb-cast-optional/{sig:?}/{absent}/{source}/{result}/{flags:?}/{name}/{shape}/{sample}/{enabled}/{selection:?}/{reuse}");
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
        eprintln!("PB CAST OPTIONAL ORIGINAL {count}/{hash:016X}");
        assert_eq!((count, hash), (3096000usize, 0x6538DC12A562DC4Du64));
        if !gaps.is_empty() {
            eprintln!("PB CAST OPTIONAL SOURCE RED {gaps:?}");
        }
        assert!(gaps.is_empty());
    });
}

fn with_declared_fields(
    sig: Sig,
    source: Option<FieldType>,
    result: Option<FieldType>,
    shape: usize,
    value: &Datum,
) -> EvaluatorSuite {
    let field = source.clone();
    let arg = match shape {
        2 => {
            let mut column = crate::column::Column::new(991, string_ty());
            column.ret_type = field;
            Expression::CorrelatedColumn(crate::column::CorrelatedColumn::with_value(
                column,
                value.clone(),
            ))
        }
        4 => {
            let mut nested = ScalarFunction::new(
                CiString::new("coalesce"),
                string_ty(),
                vec![parameter(0, string_ty()), literal(Datum::Null, string_ty())],
            );
            nested.ret_type = field;
            Expression::ScalarFunction(nested)
        }
        _ => {
            let mut c = crate::constant::Constant::new(
                if shape == 0 {
                    value.clone()
                } else {
                    Datum::Bytes(b"stored".to_vec())
                },
                string_ty(),
            );
            c.ret_type = field;
            if shape == 1 {
                c.param_marker = Some(crate::constant::ParamMarker { order: 0 });
            }
            if shape == 3 {
                c.deferred_expr = Some(Box::new(call(
                    "setvar",
                    string_ty(),
                    vec![
                        literal(Datum::new_string("cast-attrs"), string_ty()),
                        parameter(0, string_ty()),
                    ],
                )));
            }
            Expression::Constant(c)
        }
    };
    // from_pb is the real selected-owner constructor. Its final metadata is
    // exactly the supplied Some/None declaration; no field is fed to the driver
    // on behalf of a missing declaration.
    let mut f = ScalarFunction::from_pb(
        crate::scalar_function::PbBuiltin::new(sig).unwrap(),
        string_ty(),
        vec![arg],
    );
    f.ret_type = result;
    let json = FieldType::new(FieldTypeCode::Json);
    suite(call(
        "coalesce",
        json.clone(),
        vec![Expression::ScalarFunction(f), literal(Datum::Null, json)],
    ))
}
#[test]
fn pb_cast_source_original_attributes() {
    if isolate_common_corpus("stateful_tests::pb_regexp_tests::eager_tests::cast_tests::pb_cast_source_original_attributes"){return;}
    std::panic::set_hook(Box::new(|_| {}));
    attributes(false);
}
#[test]
fn pb_cast_source_actual_attributes() {
    if isolate_common_corpus("stateful_tests::pb_regexp_tests::eager_tests::cast_tests::pb_cast_source_actual_attributes"){return;}
    std::panic::set_hook(Box::new(|_| {}));
    attributes(true);
}
fn attributes(compare: bool) {
    fixed_clock(|| {
        let mut count = 0;
        let mut hash = 0xcbf29ce484222325;
        let mut gaps = vec![];
        for (sig, _) in specs() {
            for source in [8, 253, 5, 246, 245, 12, 11, 16, 247, 248] {
                for attr in 0..9 {
                    let mut ft = declaration(result_code(sig), None);
                    match attr {
                        1 => {
                            ft.flen = Some(2);
                            ft.decimal = Some(0);
                        }
                        2 => {
                            ft.flen = Some(20);
                            ft.decimal = Some(6);
                        }
                        3 => {
                            ft.flen = Some(-1);
                            ft.decimal = Some(-1);
                        }
                        4 => {
                            ft.flen = Some(2);
                            ft.decimal = Some(8);
                        }
                        5 => {
                            ft.flen = Some(2);
                            ft.decimal = Some(0);
                            ft.flag = Some(128);
                            ft.charset = Some("binary".into());
                            ft.collate = Some(63);
                        }
                        6 => ft = declaration(6, None),
                        7 => ft = declaration(16, Some(32)),
                        8 => ft = declaration(247, Some(0)),
                        _ => (),
                    }
                    for shape in 0..5 {
                        for sample in 0..5 {
                            let mut v = smoke(sig, 1);
                            variation(sig, &mut v, [0, 1, 6, 7, 8][sample]);
                            for profile in [0, 3, 7] {
                                for unsigned in [false, true] {
                                    let source_code = source;
                                    let source = crate::distsql_builtin::pb_type_to_field_type(
                                        &declaration(source, Some(if unsigned { 32 } else { 0 })),
                                    );
                                    let result = crate::distsql_builtin::pb_type_to_field_type(&ft);
                                    let owner = with_declared_fields(
                                        sig,
                                        Some(source.clone()),
                                        Some(result.clone()),
                                        shape,
                                        &v[0],
                                    );
                                    let actual = with_declared_fields(
                                        sig,
                                        Some(source),
                                        Some(result),
                                        shape,
                                        &v[0],
                                    );
                                    for enabled in [false, true] {
                                        for selection in [None, Some(vec![2, 0, 2]), Some(vec![])] {
                                            let old = C::new(enabled, v.clone(), profile);
                                            let new = C::new(enabled, v.clone(), profile);
                                            for reuse in 0..2 {
                                                let label=format!("pb-cast-attrs/{sig:?}/{source_code}/{attr}/{shape}/{sample}/{profile}/{unsigned}/{enabled}/{selection:?}/{reuse}");
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
        eprintln!("PB CAST ATTRIBUTES ORIGINAL {count}/{hash:016X}");
        assert_eq!((count, hash), (6966000usize, 0x9E0479763639F749u64));
        if !gaps.is_empty() {
            eprintln!("PB CAST ATTRIBUTES SOURCE RED {gaps:?}");
        }
        assert!(gaps.is_empty());
    });
}
#[test]
fn pb_cast_source_original_correlated() {
    if isolate_common_corpus("stateful_tests::pb_regexp_tests::eager_tests::cast_tests::pb_cast_source_original_correlated"){return;}
    std::panic::set_hook(Box::new(|_| {}));
    correlated(false);
}
#[test]
fn pb_cast_source_actual_correlated() {
    if isolate_common_corpus("stateful_tests::pb_regexp_tests::eager_tests::cast_tests::pb_cast_source_actual_correlated"){return;}
    std::panic::set_hook(Box::new(|_| {}));
    correlated(true);
}
fn correlated(compare: bool) {
    fixed_clock(|| {
        let mut count = 0;
        let mut hash = 0xcbf29ce484222325;
        let mut gaps = vec![];
        for (sig, _) in specs() {
            for source in [None, Some(8), Some(253)] {
                for result in [None, Some(result_code(sig)), Some(245)] {
                    for (vi, value) in crate::tikv::byte_unary_value_tests::values()
                        .into_iter()
                        .enumerate()
                    {
                        let source_code = source;
                        let return_code = result;
                        let source = source.map(|code| {
                            crate::distsql_builtin::pb_type_to_field_type(&declaration(code, None))
                        });
                        let result = result.map(|code| {
                            crate::distsql_builtin::pb_type_to_field_type(&declaration(code, None))
                        });
                        let owner =
                            with_declared_fields(sig, source.clone(), result.clone(), 2, &value);
                        let actual = with_declared_fields(sig, source, result, 2, &value);
                        for enabled in [false, true] {
                            for selection in [None, Some(vec![2, 0, 2]), Some(vec![])] {
                                let old = C::new(enabled, vec![], 0);
                                let new = C::new(enabled, vec![], 0);
                                for reuse in 0..2 {
                                    let label=format!("pb-cast-correlated/{sig:?}/{source_code:?}/{return_code:?}/{vi}/{enabled}/{selection:?}/{reuse}");
                                    case(
                                        &owner, &actual, &old, &new, &selection, compare, &label,
                                        &mut hash, &mut count, &mut gaps,
                                    );
                                }
                            }
                        }
                    }
                }
            }
        }
        eprintln!("PB CAST CORRELATED ORIGINAL {count}/{hash:016X}");
        assert_eq!((count, hash), (691956usize, 0x3AACD14D71C42B0Eu64));
        if !gaps.is_empty() {
            eprintln!("PB CAST CORRELATED SOURCE RED {gaps:?}");
        }
        assert!(gaps.is_empty());
    });
}

#[test]
fn pb_cast_source_original_physical_missing() {
    if isolate_common_corpus("stateful_tests::pb_regexp_tests::eager_tests::cast_tests::pb_cast_source_original_physical_missing") {return;}
    std::panic::set_hook(Box::new(|_| {}));
    physical_missing(false);
}
#[test]
fn pb_cast_source_actual_physical_missing() {
    if isolate_common_corpus("stateful_tests::pb_regexp_tests::eager_tests::cast_tests::pb_cast_source_actual_physical_missing") {return;}
    std::panic::set_hook(Box::new(|_| {}));
    physical_missing(true);
}
fn physical_missing(compare: bool) {
    fixed_clock(|| {
        let mut count = 0;
        let mut hash = 0xcbf29ce484222325;
        let mut gaps = vec![];
        for (sig, _) in specs() {
            for declared in [false, true] {
                for result in [None, Some(result_code(sig)), Some(245)] {
                    for arity in [0, 1, 2] {
                        let build = || {
                            let mut column = crate::column::Column::new(42, string_ty());
                            column.index = 0;
                            if !declared {
                                column.ret_type = None;
                            }
                            let mut args = vec![];
                            if arity > 0 {
                                args.push(Expression::Column(column));
                            }
                            if arity > 1 {
                                args.push(parameter(99, string_ty()));
                            }
                            let mut f = ScalarFunction::from_pb(
                                crate::scalar_function::PbBuiltin::new(sig).unwrap(),
                                string_ty(),
                                args,
                            );
                            f.ret_type = result.map(|code| {
                                crate::distsql_builtin::pb_type_to_field_type(&declaration(
                                    code, None,
                                ))
                            });
                            let json = FieldType::new(FieldTypeCode::Json);
                            suite(call(
                                "coalesce",
                                json.clone(),
                                vec![Expression::ScalarFunction(f), literal(Datum::Null, json)],
                            ))
                        };
                        let owner = build();
                        let actual = build();
                        for enabled in [false, true] {
                            for selection in [None, Some(vec![2, 0, 2]), Some(vec![])] {
                                let old = C::new(enabled, vec![], 0);
                                let new = C::new(enabled, vec![], 0);
                                for reuse in 0..2 {
                                    let label=format!("pb-cast-physical-missing/{sig:?}/{declared}/{result:?}/{arity}/{enabled}/{selection:?}/{reuse}");
                                    case(
                                        &owner, &actual, &old, &new, &selection, compare, &label,
                                        &mut hash, &mut count, &mut gaps,
                                    );
                                }
                            }
                        }
                    }
                }
            }
        }
        eprintln!("PB CAST PHYSICAL MISSING ORIGINAL {count}/{hash:016X}");
        assert_eq!((count, hash), (9288usize, 0xC02AAAACD0B2CECBu64));
        if !gaps.is_empty() {
            eprintln!("PB CAST PHYSICAL MISSING SOURCE RED {gaps:?}");
        }
        assert!(gaps.is_empty());
    });
}

#[test]
fn pb_cast_source_direct_consumers_and_fixed_cap_recovery() {
    fixed_clock(|| {
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
                    let expected = session_read_outcome(|| {
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
            // Match this real caller result to the selected target for fixed-cap recovery.
            // A separately tested mismatched return intentionally needs formatter allowance.
            let owner = make(sig, 1, result_code(sig), None, "wire display", 1, &values);
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
                let expected = session_read_outcome(|| owner.run(&old, &mut a, &mut left));
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
    });
}

#[test]
fn pb_cast_source_rejects_stale_optional_declarations_and_selected_origins() {
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
                BatchRefusal::Admission,
                "{sig:?}/{bad}"
            );
        }
    }
}

#[test]
fn pb_cast_source_missing_literal_type_precedes_large_value_copy() {
    fixed_clock(|| {
        for (sig, _) in specs() {
            for value in [Datum::Null, Datum::Bytes(vec![b'x'; 2 * 1024 * 1024])] {
                let owner = with_declared_fields(
                    sig,
                    None,
                    Some(FieldType::new(FieldTypeCode::LongLong)),
                    0,
                    &value,
                );
                let old = C::new(true, vec![], 0);
                let new = C::new(true, vec![], 0);
                let ty = output_type(&owner);
                let mut a = input(&None);
                let mut left = output_for(&ty);
                left.append_null(0);
                let expected = session_read_outcome(|| owner.run(&old, &mut a, &mut left));
                assert!(
                    expected.contains("constant has no result type"),
                    "{sig:?}/{expected}"
                );
                let mut policy = limits();
                policy.max_metadata_bytes = 64 * 1024;
                let mut prepared = PreparedBatchSuite::compile(
                    &owner,
                    &[string_ty()],
                    true,
                    944,
                    policy,
                    ExecutionLimits::default(),
                    1 << 20,
                )
                .unwrap();
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
                assert_eq!(got, expected, "{sig:?}");
                assert_eq!(right.column(0), left.column(0));
                assert_eq!(new.snapshot(), old.snapshot());
            }
        }
    });
}

#[test]
fn pb_cast_source_fixed_cap_matches_original_mismatched_return_guard() {
    fixed_clock(|| {
        let values = vec![Datum::Int(4)];
        let owner = make(
            Sig::CastDecimalAsDuration,
            1,
            8,
            None,
            "wire display",
            1,
            &values,
        );
        let Expression::ScalarFunction(function) = &owner.program.calculated[0] else {
            panic!()
        };
        let raw_ctx = C::new(true, values.clone(), 0);
        let source = input(&None);
        let raw = function
            .original_pb_cast_value_for_test(&raw_ctx, source.physical_row(0))
            .unwrap();
        assert!(matches!(raw, Datum::Duration(_)));
        // Original raw value plus the existing bounded return guard is independent
        // of the new caller's materialization ledger. This is a finite policy test,
        // not an assertion of success equivalence with an uncapped original caller.
        let rejected = crate::tikv::identity_actual::coerce_result(
            function.func_name.lowercase(),
            function.get_static_type().unwrap(),
            raw,
            64 * 1024,
        )
        .unwrap_err();
        assert!(
            matches!(rejected,EvalError::ExpressionRuntimeFailure(e) if e.class()==crate::ExpressionRuntimeFailureClass::ResourceLimit)
        );
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
        for null in [false, true, false, true] {
            let values = if null {
                vec![Datum::Null]
            } else {
                values.clone()
            };
            let ctx = C::new(true, values, 0);
            let mut i = input(&None);
            let ty = output_type(&owner);
            let mut o = output_for(&ty);
            o.append_null(0);
            let got = prepared.run(
                &owner,
                &ctx,
                &mut EvalContext::default(),
                &mut i,
                &mut o,
                &[string_ty()],
            );
            if null {
                assert!(got.is_ok());
                assert_eq!(o.num_rows(), 4);
            } else {
                assert_eq!(got.unwrap_err().refusal(), BatchRefusal::Resource);
                assert_eq!(o.num_rows(), 1);
            }
            assert_eq!(
                prepared.workers[0].max_materialization_retained_bytes,
                64 * 1024
            );
        }
    });
}
