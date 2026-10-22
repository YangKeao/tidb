// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Actual PB scalar signatures and original legacy arithmetic consumers.
use super::*;
const NAMES: [&str; 6] = ["plus", "minus", "mul", "mod", "div", "intdiv"];
fn declaration(code: i32, unsigned: bool) -> wire::FieldType {
    let mut ft = field(code, -46);
    ft.flag = Some(if unsigned { 32 } else { 0 });
    match code {
        8 => {
            ft.flen = Some(20);
            ft.decimal = Some(0);
        }
        246 => {
            ft.flen = Some(65);
            ft.decimal = Some(5);
        }
        13 => {
            ft.flen = Some(4);
            ft.decimal = Some(0);
        }
        16 => ft.flen = Some(8),
        247 | 248 => ft.elems = vec!["zero".into(), "one".into()],
        _ => (),
    };
    ft
}
fn make(
    name: &str,
    sources: [i32; 2],
    result: i32,
    flags: usize,
    shape: usize,
    values: &[Datum; 3],
) -> EvaluatorSuite {
    let stored = Datum::Bytes(b"stored".to_vec());
    let args = (0..2)
        .map(|i| {
            let ft = Some(declaration(sources[i], flags & (2 << i) != 0));
            if shape == 2 && i == 0 {
                let mut v = vec![];
                tidb_codec::encode_int(&mut v, 0);
                wire::Expr {
                    tp: Some(wire::ExprType::ColumnRef as i32),
                    val: Some(v),
                    field_type: ft,
                    ..Default::default()
                }
            } else if shape == 4 && i == 0 {
                pb_call(
                    vec![
                        atom(&Datum::Int(-4), Some(declaration(8, false))),
                        atom(&Datum::Int(2), Some(declaration(8, false))),
                    ],
                    ft,
                )
            } else {
                atom(if shape == 0 { &values[i] } else { &stored }, ft)
            }
        })
        .collect();
    let mut root = crate::distsql_builtin::pb_to_expr(
        &pb_call(args, Some(declaration(result, flags & 1 != 0))),
        &[string_ty()],
    )
    .unwrap();
    let Expression::ScalarFunction(f) = &mut root else {
        unreachable!()
    };
    f.func_name = CiString::new(name);
    for (i, arg) in f.args.iter_mut().enumerate() {
        if shape == 1 || shape == 3 {
            let Expression::Constant(c) = arg else {
                unreachable!()
            };
            if shape == 1 {
                c.param_marker = Some(crate::constant::ParamMarker { order: i as i64 });
            } else {
                c.deferred_expr = Some(Box::new(call(
                    "setvar",
                    string_ty(),
                    vec![
                        literal(Datum::new_string(format!("arithmetic-{i}")), string_ty()),
                        parameter(i, string_ty()),
                    ],
                )));
            }
        }
        if shape == 4 && i == 0 {
            let Expression::ScalarFunction(f) = arg else {
                unreachable!()
            };
            f.func_name = CiString::new("plus");
        }
    }
    suite(root)
}
struct C {
    base: RegexpSourceContext,
}
impl C {
    fn new(enabled: bool, values: [Datum; 3], profile: usize) -> Self {
        Self {
            base: RegexpSourceContext::new(enabled, values, profile),
        }
    }
    fn snapshot(&self) -> String {
        self.base.snapshot()
    }
}
impl Columns for C {
    fn get(&self, _: &[String]) -> Option<Datum> {
        panic!("no expression callback")
    }
    fn ready_value_cache(&self) -> Option<&crate::ReadyValueCache> {
        self.base.ready_value_cache()
    }
    fn context_id(&self) -> u64 {
        self.base.context_id()
    }
    fn enable_vectorized_expression(&self) -> bool {
        self.base.enable_vectorized_expression()
    }
    fn param_value(&self, i: usize) -> std::result::Result<Datum, EvalError> {
        self.base.param_value(i)
    }
    fn append_warning(&self, c: u16, m: &str) {
        self.base.append_warning(c, m)
    }
    fn type_flags(&self) -> tidb_datatype::ConversionFlags {
        self.base.type_flags()
    }
    fn truncate_level(&self) -> crate::ErrorLevel {
        self.base.truncate_level()
    }
    fn time_zone(&self) -> SessionTimeZone {
        self.base.time_zone()
    }
    fn connection_charset_info(&self) -> (&str, &str) {
        self.base.connection_charset_info()
    }
    fn set_uservar(&self, n: &str, v: Datum) {
        self.base.set_uservar(n, v)
    }
    fn no_unsigned_subtraction(&self) -> bool {
        let v = self.base.profile == 2;
        self.base.event(format!("unsigned-subtraction:{v}"));
        v
    }
    fn div_precision_increment(&self) -> u32 {
        let v = match self.base.profile {
            2 => 30,
            7 => 0,
            _ => 4,
        };
        self.base.event(format!("division-precision:{v}"));
        v
    }
    fn division_by_zero_level(&self) -> crate::ErrorLevel {
        let v = match self.base.profile {
            2 => crate::ErrorLevel::Error,
            7 => crate::ErrorLevel::Ignore,
            _ => crate::ErrorLevel::Warn,
        };
        self.base.event(format!("division-level:{v:?}"));
        v
    }
}
#[test]
fn pb_arithmetic_source_original_phases() {
    if isolate_common_corpus(
        "stateful_tests::pb_regexp_tests::arithmetic_tests::pb_arithmetic_source_original_phases",
    ) {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    matrix(false, false);
}
#[test]
fn pb_arithmetic_source_actual_phases() {
    if isolate_common_corpus(
        "stateful_tests::pb_regexp_tests::arithmetic_tests::pb_arithmetic_source_actual_phases",
    ) {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    matrix(true, false);
}
#[test]
fn pb_arithmetic_source_original_full() {
    if isolate_common_corpus(
        "stateful_tests::pb_regexp_tests::arithmetic_tests::pb_arithmetic_source_original_full",
    ) {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    matrix(false, true);
}
#[test]
fn pb_arithmetic_source_actual_full() {
    if isolate_common_corpus(
        "stateful_tests::pb_regexp_tests::arithmetic_tests::pb_arithmetic_source_actual_full",
    ) {
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
        let samples: Vec<(usize, [Datum; 3])> = if full {
            crate::tikv::byte_unary_value_tests::values()
                .into_iter()
                .enumerate()
                .flat_map(|(vi, v)| {
                    (0..2).map(move |pos| {
                        let mut a = [
                            Datum::new_string("abc"),
                            Datum::new_string("^a"),
                            Datum::Null,
                        ];
                        a[pos] = v.clone();
                        (vi * 2 + pos, a)
                    })
                })
                .collect()
        } else {
            vec![
                (0, [Datum::Int(4), Datum::Int(2), Datum::Null]),
                (1, [Datum::Int(-4), Datum::Int(0), Datum::Null]),
                (2, [Datum::Null, Datum::Bytes(vec![255]), Datum::Null]),
                (3, [Datum::Int(i64::MIN), Datum::Int(-1), Datum::Null]),
                (
                    4,
                    [
                        Datum::new_string("abc"),
                        Datum::new_string("^a"),
                        Datum::Null,
                    ],
                ),
                (
                    5,
                    [
                        Datum::new_string("4.5"),
                        Datum::new_string("2"),
                        Datum::Null,
                    ],
                ),
                (6, [Datum::Bytes(vec![255]), Datum::Int(1), Datum::Null]),
                (7, [Datum::Int(4), Datum::Null, Datum::Null]),
            ]
        };
        let pairs = if full {
            vec![[8, 8], [253, 8], [8, 5]]
        } else {
            vec![
                [8, 8],
                [8, 5],
                [5, 246],
                [246, 8],
                [253, 8],
                [245, 8],
                [12, 8],
                [11, 8],
            ]
        };
        let results = if full {
            vec![8, 5, 246]
        } else {
            vec![8, 5, 246, 245, 253, 6, 13, 16, 247, 248]
        };
        let (mut count, mut hash) = (0usize, 0xcbf29ce484222325u64);
        let mut gaps = vec![];
        for name in NAMES {
            for &sources in &pairs {
                for &result in &results {
                    for flags in if full {
                        vec![0, 7]
                    } else {
                        vec![0, 1, 2, 4, 7]
                    } {
                        for shape in if full { vec![1] } else { vec![0, 1, 2, 3, 4] } {
                            if shape == 2 && sources[0] != 253 {
                                continue;
                            }
                            for (vi, values) in &samples {
                                for profile in if full { vec![0] } else { vec![0, 2, 3, 6, 7] } {
                                    for enabled in [false, true] {
                                        for selection in [None, Some(vec![2, 0, 2]), Some(vec![])] {
                                            let original =
                                                make(name, sources, result, flags, shape, values);
                                            let actual =
                                                make(name, sources, result, flags, shape, values);
                                            let ty = output_type(&original);
                                            let old = C::new(enabled, values.clone(), profile);
                                            let new = C::new(enabled, values.clone(), profile);
                                            for reuse in 0..2 {
                                                let label=format!("pb-arithmetic/{full}/{name}/{sources:?}/{result}/{flags}/{shape}/{vi}/{profile}/{enabled}/{selection:?}/{reuse}");
                                                let mut a = input(&selection);
                                                let mut left = output_for(&ty);
                                                left.append_null(0);
                                                let expected = session_read_outcome(|| {
                                                    original.run(&old, &mut a, &mut left)
                                                });
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
                                                        eprintln!("ARITHMETIC DIFFERENCE {label} old={expected:?} new={got:?} left={:?} right={:?} oldevents={} newevents={}",left.column(0),right.column(0),old.snapshot(),new.snapshot());
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
        eprintln!("PB ARITHMETIC ORIGINAL full={full} {count}/{hash:016X}");
        assert_eq!(
            (count, hash),
            if full {
                (386208, 0xF179850F4FA9F719)
            } else {
                (4752000, 0x3227AB25DBF1A2CB)
            }
        );
        if !gaps.is_empty() {
            eprintln!("PB ARITHMETIC SOURCE RED {gaps:?}");
        }
        assert!(gaps.is_empty());
    });
}
#[test]
fn pb_arithmetic_source_original_long() {
    if isolate_common_corpus(
        "stateful_tests::pb_regexp_tests::arithmetic_tests::pb_arithmetic_source_original_long",
    ) {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    long(false);
}
#[test]
fn pb_arithmetic_source_actual_long() {
    if isolate_common_corpus(
        "stateful_tests::pb_regexp_tests::arithmetic_tests::pb_arithmetic_source_actual_long",
    ) {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    long(true);
}
fn long(compare: bool) {
    let (mut count, mut hash) = (0usize, 0xcbf29ce484222325u64);
    let mut gaps = vec![];
    for name in NAMES {
        for result in [8, 246] {
            for shape in [1, 3, 4] {
                for flags in [0, 7] {
                    for profile in [0, 3, 5, 7] {
                        let args = [Datum::Int(4), Datum::Int(2), Datum::Null];
                        let original = make(name, [8, 8], result, flags, shape, &args);
                        let actual = make(name, [8, 8], result, flags, shape, &args);
                        let ty = output_type(&original);
                        let old = C::new(false, args.clone(), profile);
                        let new = C::new(false, args, profile);
                        for rows in [0, 1, 3, 1026, 2051] {
                            for (round, enabled) in
                                [false, true, true, false].into_iter().enumerate()
                            {
                                old.base.base.enabled.set(enabled);
                                new.base.base.enabled.set(enabled);
                                old.base.id.set(if round == 2 { 18 } else { 17 });
                                new.base.id.set(old.base.id.get());
                                let selected = if rows > 1 {
                                    Some(
                                        (0..rows)
                                            .map(|i| (rows - 1 - i / 2) % rows)
                                            .collect::<Vec<_>>(),
                                    )
                                } else {
                                    None
                                };
                                let data = (0..rows)
                                    .map(|_| vec![Datum::new_string("4")])
                                    .collect::<Vec<_>>();
                                for reuse in 0..2 {
                                    let label=format!("pb-arithmetic-long/{name}/{result}/{shape}/{flags}/{profile}/{rows}/{round}/{reuse}");
                                    let mut a = chunk(&[string_ty()], &data);
                                    a.set_sel(selected.clone());
                                    let mut left = output_for(&ty);
                                    left.append_null(0);
                                    let expected = session_read_outcome(|| {
                                        original.run(&old, &mut a, &mut left)
                                    });
                                    if compare {
                                        let mut b = chunk(&[string_ty()], &data);
                                        b.set_sel(selected.clone());
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
                                            eprintln!("ARITHMETIC LONG DIFFERENCE {label} old={expected:?} new={got:?} oldevents={} newevents={}",old.snapshot(),new.snapshot());
                                        }
                                        assert_eq!(got, expected, "{label}");
                                        assert_eq!(right.column(0), left.column(0), "{label}");
                                        assert_eq!(new.snapshot(), old.snapshot(), "{label}");
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
    eprintln!("PB ARITHMETIC LONG ORIGINAL {count}/{hash:016X}");
    assert_eq!((count, hash), (11520, 0xAA6B736726DD9D61));
    if !gaps.is_empty() {
        eprintln!("PB ARITHMETIC LONG SOURCE RED {gaps:?}");
    }
    assert!(gaps.is_empty());
}

#[test]
fn pb_arithmetic_source_direct_profiles_and_fixed_cap_recovery() {
    for name in NAMES {
        let owner = make(
            name,
            [8, 8],
            8,
            0,
            1,
            &[Datum::Int(4), Datum::Int(2), Datum::Null],
        );
        let ty = output_type(&owner);
        let mut direct = PreparedBatch::compile(
            &owner,
            &[string_ty()],
            true,
            943,
            limits(),
            ExecutionLimits::default(),
            1 << 20,
        )
        .unwrap();
        let candidate = crate::scalar_function::static_batch_target(&owner.program.calculated[0]);
        assert_eq!(
            direct.row_profile.is_some(),
            candidate.is_some(),
            "{name}/{candidate:?}"
        );
        if candidate.is_some() {
            let args = [Datum::Int(4), Datum::Int(2), Datum::Null];
            let old = C::new(true, args.clone(), 0);
            let new = C::new(true, args, 0);
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
            assert_eq!(got, expected, "{name}/direct");
            assert_eq!(right.column(0), left.column(0));
            assert_eq!(new.snapshot(), old.snapshot(), "{name}/direct");
        } else {
            // The direct mandatory batch consumer keeps its original refusal
            // when this actual result declaration has no native candidate.
            let ctx = C::new(true, [Datum::Int(4), Datum::Int(2), Datum::Null], 0);
            let mut input = input(&None);
            let mut output = output_for(&ty);
            output.append_null(0);
            let failure = owner
                .run_batch(
                    &ctx,
                    &mut EvalContext::default(),
                    &mut direct,
                    &mut input,
                    &mut output,
                    &[string_ty()],
                )
                .unwrap_err();
            assert_eq!(failure.refusal(), BatchRefusal::Admission);
            assert_eq!(output.num_rows(), 1);
        }
        let mut prepared = PreparedBatchSuite::compile(
            &owner,
            &[string_ty()],
            true,
            943,
            limits(),
            ExecutionLimits::default(),
            1 << 20,
        )
        .unwrap();
        prepared.workers[0].max_materialization_retained_bytes = 64 * 1024;
        let mut ctx = C::new(true, [Datum::Int(4), Datum::Int(2), Datum::Null], 0);
        let mut a = input(&Some(vec![0; 8193]));
        let mut output = output_for(&ty);
        output.append_null(0);
        let failure = prepared
            .run(
                &owner,
                &ctx,
                &mut EvalContext::default(),
                &mut a,
                &mut output,
                &[string_ty()],
            )
            .unwrap_err();
        assert!(matches!(
            failure.local_error(),
            Some(LocalError::ResourceLimit(_))
        ));
        assert_eq!(output.num_rows(), 1);
        assert_eq!(prepared.workers[0].selection.capacity(), 0);
        for value in [Datum::Null, Datum::Int(4)] {
            ctx.base.base.values[0] = value;
            ctx.base.base.events.borrow_mut().clear();
            ctx.base.context_reads.set(0);
            for n in &ctx.base.base.reads {
                n.set(0);
            }
            let old = C::new(true, ctx.base.base.values.clone(), 0);
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
                        &ctx,
                        &mut EvalContext::default(),
                        &mut b,
                        &mut right,
                        &[string_ty()],
                    )
                    .map_err(BatchFailure::into_evaluator_error)
            });
            assert_eq!(got, expected, "{name}/recovery");
            assert_eq!(right.column(0), left.column(0));
            assert_eq!(ctx.snapshot(), old.snapshot(), "{name}/recovery");
            assert_eq!(
                prepared.workers[0].max_materialization_retained_bytes,
                64 * 1024
            );
        }
    }
}

#[test]
fn pb_arithmetic_source_late_precision_panic_precedes_integer_collection_and_recovers() {
    struct Late {
        base: C,
        reads: Cell<usize>,
    }
    impl Columns for Late {
        fn get(&self, _: &[String]) -> Option<Datum> {
            panic!("no expression callback")
        }
        fn ready_value_cache(&self) -> Option<&crate::ReadyValueCache> {
            self.base.ready_value_cache()
        }
        fn context_id(&self) -> u64 {
            self.base.context_id()
        }
        fn enable_vectorized_expression(&self) -> bool {
            self.base.enable_vectorized_expression()
        }
        fn param_value(&self, i: usize) -> std::result::Result<Datum, EvalError> {
            self.base.param_value(i)
        }
        fn append_warning(&self, c: u16, m: &str) {
            self.base.append_warning(c, m)
        }
        fn type_flags(&self) -> tidb_datatype::ConversionFlags {
            self.base.type_flags()
        }
        fn truncate_level(&self) -> crate::ErrorLevel {
            self.base.truncate_level()
        }
        fn time_zone(&self) -> SessionTimeZone {
            self.base.time_zone()
        }
        fn connection_charset_info(&self) -> (&str, &str) {
            self.base.connection_charset_info()
        }
        fn set_uservar(&self, n: &str, v: Datum) {
            self.base.set_uservar(n, v)
        }
        fn no_unsigned_subtraction(&self) -> bool {
            self.base.no_unsigned_subtraction()
        }
        fn division_by_zero_level(&self) -> crate::ErrorLevel {
            self.base.division_by_zero_level()
        }
        fn div_precision_increment(&self) -> u32 {
            let n = self.reads.get() + 1;
            self.reads.set(n);
            self.base.base.event(format!("late-precision:{n}"));
            if n == 3 {
                panic!("original late arithmetic precision");
            }
            self.base.div_precision_increment()
        }
    }
    let values = [
        Datum::new_string("4.5"),
        Datum::new_string("2"),
        Datum::Null,
    ];
    let original = make("intdiv", [8, 5], 16, 0, 1, &values);
    let actual = make("intdiv", [8, 5], 16, 0, 1, &values);
    let ty = output_type(&original);
    let old = Late {
        base: C::new(true, values.clone(), 0),
        reads: Cell::new(0),
    };
    let new = Late {
        base: C::new(true, values, 0),
        reads: Cell::new(0),
    };
    for (round, enabled) in [true, true, false].into_iter().enumerate() {
        old.base.base.base.enabled.set(enabled);
        new.base.base.base.enabled.set(enabled);
        let mut a = input(&None);
        let mut left = output_for(&ty);
        left.append_null(0);
        let expected = session_read_outcome(|| original.run(&old, &mut a, &mut left));
        let mut b = input(&None);
        let mut right = output_for(&ty);
        right.append_null(0);
        let got = session_read_outcome(|| {
            actual.run_with_tikv_batch(&new, &mut b, &mut right, &[string_ty()])
        });
        assert_eq!(got, expected, "round{round}");
        assert_eq!(right.column(0), left.column(0));
        assert_eq!(new.base.snapshot(), old.base.snapshot(), "round{round}");
        assert_eq!(new.reads.get(), old.reads.get());
        if round == 0 {
            assert!(expected.contains("original late arithmetic precision"));
            assert_eq!(old.reads.get(), 3);
        }
        let route = actual.batch_route_snapshot();
        assert_eq!((route.refusals, route.fallback), (0, 0));
    }
}
