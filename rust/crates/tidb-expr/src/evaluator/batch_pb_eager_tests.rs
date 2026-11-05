// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Independent original contracts for the complete non-JSON eager PB owner.
use super::*;
use wire::ScalarFuncSig as Sig;
fn catalog() -> Vec<(Sig, usize)> {
    vec![
        (Sig::CharLength, 1),
        (Sig::CharLengthUtf8, 1),
        (Sig::Upper, 1),
        (Sig::UpperUtf8, 1),
        (Sig::Lower, 1),
        (Sig::LowerUtf8, 1),
        (Sig::Substring2Args, 2),
        (Sig::Substring3Args, 3),
        (Sig::Substring2ArgsUtf8, 2),
        (Sig::Substring3ArgsUtf8, 3),
        (Sig::Acos, 1),
        (Sig::Asin, 1),
        (Sig::Atan1Arg, 1),
        (Sig::Atan2Args, 2),
        (Sig::Cos, 1),
        (Sig::Cot, 1),
        (Sig::Sin, 1),
        (Sig::Pow, 2),
        (Sig::Pi, 0),
        (Sig::Conv, 3),
        (Sig::RoundInt, 2),
        (Sig::RoundReal, 2),
        (Sig::RoundDec, 2),
        (Sig::Date, 1),
        (Sig::DateDiff, 2),
        (Sig::DateFormatSig, 2),
        (Sig::Hour, 1),
        (Sig::Minute, 1),
        (Sig::Second, 1),
        (Sig::MicroSecond, 1),
        (Sig::Month, 1),
        (Sig::WeekWithoutMode, 1),
        (Sig::TimestampDiff, 3),
        (Sig::UnixTimestampInt, 1),
        (Sig::UnixTimestampDec, 1),
        (Sig::FromUnixTime1Arg, 1),
        (Sig::FromUnixTime2Arg, 2),
    ]
}
fn smoke(sig: Sig, arity: usize) -> Vec<Datum> {
    let s = Datum::new_string;
    let mut v = match sig {
        Sig::CharLength
        | Sig::CharLengthUtf8
        | Sig::Upper
        | Sig::UpperUtf8
        | Sig::Lower
        | Sig::LowerUtf8 => vec![s("aBß")],
        Sig::Substring2Args | Sig::Substring2ArgsUtf8 => vec![s("abcd"), Datum::Int(2)],
        Sig::Substring3Args | Sig::Substring3ArgsUtf8 => {
            vec![s("abcd"), Datum::Int(2), Datum::Int(2)]
        }
        Sig::Acos | Sig::Asin | Sig::Atan1Arg | Sig::Cos | Sig::Cot | Sig::Sin => vec![s("0.5")],
        Sig::Atan2Args => vec![Datum::Int(1), Datum::Int(2)],
        Sig::Pow => vec![Datum::Int(2), Datum::Int(3)],
        Sig::Conv => vec![s("101"), Datum::Int(2), Datum::Int(10)],
        Sig::RoundInt | Sig::RoundReal | Sig::RoundDec => vec![s("1.25"), Datum::Int(1)],
        Sig::Date
        | Sig::Month
        | Sig::WeekWithoutMode
        | Sig::UnixTimestampInt
        | Sig::UnixTimestampDec => vec![s("2026-10-21 12:34:56.123")],
        Sig::DateDiff => vec![s("2026-10-21"), s("2026-10-20")],
        Sig::DateFormatSig => vec![s("2026-10-21 12:34:56.123"), s("%Y-%m")],
        Sig::Hour | Sig::Minute | Sig::Second | Sig::MicroSecond => vec![s("12:34:56.123456")],
        Sig::TimestampDiff => vec![s("DAY"), s("2026-10-20"), s("2026-10-21")],
        Sig::FromUnixTime1Arg => vec![Datum::Int(0)],
        Sig::FromUnixTime2Arg => vec![Datum::Int(0), s("%Y-%m")],
        Sig::Pi => vec![],
        _ => unreachable!(),
    };
    v.resize(arity, Datum::Int(4));
    v
}
fn declaration(code: i32, flags: Option<u32>) -> wire::FieldType {
    let mut f = field(code, -46);
    f.flag = flags;
    match code {
        8 => {
            f.flen = Some(20);
            f.decimal = Some(0);
        }
        246 => {
            f.flen = Some(65);
            f.decimal = Some(5);
        }
        12 => {
            f.flen = Some(26);
            f.decimal = Some(3);
        }
        11 => {
            f.flen = Some(16);
            f.decimal = Some(3);
        }
        13 => {
            f.flen = Some(4);
            f.decimal = Some(0);
        }
        16 => f.flen = Some(8),
        247 | 248 => {
            f.elems = vec![
                "zero".into(),
                "one".into(),
                "two".into(),
                "three".into(),
                "four".into(),
            ]
        }
        _ => (),
    };
    f
}
fn make(
    sig: Sig,
    source: i32,
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
            .map(|v| {
                atom(
                    if shape == 0 { v } else { &stored },
                    Some(declaration(source, Some(0))),
                )
            })
            .collect(),
        Some(declaration(result, flags)),
    );
    p.sig = Some(sig as i32);
    let mut root = crate::distsql_builtin::pb_to_expr(&p, &[]).unwrap();
    let Expression::ScalarFunction(f) = &mut root else {
        unreachable!()
    };
    assert_eq!(f.pb_signature(), Some(sig));
    if name != "canonical" {
        f.func_name = CiString::new(name);
    }
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
                        literal(Datum::new_string(format!("eager-{i}")), string_ty()),
                        parameter(i, string_ty()),
                    ],
                )));
            }
        }
    }
    suite(root)
}
struct C {
    ready: crate::ReadyValueCache,
    values: Vec<Datum>,
    reads: Vec<Cell<usize>>,
    enabled: Cell<bool>,
    profile: usize,
    id: Cell<u64>,
    ids: Cell<usize>,
    ready_reads: Cell<usize>,
    events: RefCell<Vec<String>>,
}
impl C {
    fn new(enabled: bool, values: Vec<Datum>, profile: usize) -> Self {
        Self {
            ready: crate::ReadyValueCache::new(),
            reads: (0..values.len()).map(|_| Cell::new(0)).collect(),
            values,
            enabled: Cell::new(enabled),
            profile,
            id: Cell::new(17),
            ids: Cell::new(0),
            ready_reads: Cell::new(0),
            events: RefCell::new(vec![]),
        }
    }
    fn event(&self, s: String) {
        self.events.borrow_mut().push(s);
    }
    fn snapshot(&self) -> String {
        format!(
            "{:?}|{}:{}:{}|{:?}",
            self.reads.iter().map(Cell::get).collect::<Vec<_>>(),
            self.id.get(),
            self.ids.get(),
            self.ready_reads.get(),
            self.events.borrow()
        )
    }
}
impl Columns for C {
    fn get(&self, _: &[String]) -> Option<Datum> {
        panic!("no expression or named-column callback")
    }
    fn ready_value_cache(&self) -> Option<&crate::ReadyValueCache> {
        let n = self.ready_reads.get();
        self.ready_reads.set(n + 1);
        self.event(format!("ready:{n}"));
        if self.profile == 7 {
            None
        } else {
            Some(&self.ready)
        }
    }
    fn enable_vectorized_expression(&self) -> bool {
        let v = self.enabled.get();
        self.event(format!("flag:{v}"));
        v
    }
    fn param_value(&self, i: usize) -> std::result::Result<Datum, EvalError> {
        let n = self
            .reads
            .get(i)
            .ok_or(EvalError::Unsupported("eager parameter index"))?
            .get();
        self.reads[i].set(n + 1);
        self.event(format!("param:{i}:{n}"));
        if self.profile == 3 && i == 0 && n == 3 {
            panic!("original eager parameter callback");
        }
        Ok(self.values[i].clone())
    }
    fn context_id(&self) -> u64 {
        let n = self.ids.get();
        self.ids.set(n + 1);
        let id = if self.profile == 2 {
            17 + ((n / 2) % 2) as u64
        } else {
            self.id.get()
        };
        self.event(format!("context:{n}:{id}"));
        id
    }
    fn time_zone(&self) -> SessionTimeZone {
        self.event("zone".into());
        SessionTimeZone::utc()
    }
    fn type_flags(&self) -> tidb_datatype::ConversionFlags {
        self.event("flags".into());
        tidb_datatype::DEFAULT_STATEMENT_FLAGS.with_ignore_truncate_err(true)
    }
    fn truncate_level(&self) -> crate::ErrorLevel {
        self.event("truncate-level".into());
        crate::ErrorLevel::Warn
    }
    fn append_warning(&self, c: u16, m: &str) {
        self.event(format!("warning:{c}:{m}"));
    }
    fn connection_charset_info(&self) -> (&str, &str) {
        self.event("connection".into());
        ("utf8mb4", "utf8mb4_bin")
    }
    fn set_uservar(&self, n: &str, v: Datum) {
        self.event(format!("set:{n}:{}", leaf::frame(&v)));
    }
    fn now(&self) -> Option<(i64, u32, i32)> {
        self.event("now".into());
        Some((1792540800, 123456789, 0))
    }
    fn default_week_format(&self) -> i64 {
        let v = if self.profile == 2 { 3 } else { 0 };
        self.event(format!("week-mode:{v}"));
        v
    }
    fn div_precision_increment(&self) -> u32 {
        self.event("division-precision:4".into());
        4
    }
    fn no_unsigned_subtraction(&self) -> bool {
        let v = self.profile == 2;
        self.event(format!("unsigned-subtraction:{v}"));
        v
    }
    fn max_allowed_packet(&self) -> u64 {
        self.event("packet".into());
        u64::MAX
    }
}
#[test]
fn pb_eager_source_original_phases() {
    if isolate_common_corpus(
        "stateful_tests::pb_regexp_tests::eager_tests::pb_eager_source_original_phases",
    ) {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    matrix(false, false);
}
#[test]
fn pb_eager_source_actual_phases() {
    if isolate_common_corpus(
        "stateful_tests::pb_regexp_tests::eager_tests::pb_eager_source_actual_phases",
    ) {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    matrix(true, false);
}
#[test]
fn pb_eager_source_original_full() {
    if isolate_common_corpus(
        "stateful_tests::pb_regexp_tests::eager_tests::pb_eager_source_original_full",
    ) {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    matrix(false, true);
}
#[test]
fn pb_eager_source_actual_full() {
    if isolate_common_corpus(
        "stateful_tests::pb_regexp_tests::eager_tests::pb_eager_source_actual_full",
    ) {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    matrix(true, true);
}
fn matrix(compare: bool, full: bool) {
    // Independent pre-policy records retain the historical golden; every changed
    // malformed Decimal must first match its exact original-native outcome.
    let policy_records: Vec<(usize, String, String)> = if full {
        serde_json::from_str(include_str!("batch_pb_eager_policy.json")).unwrap()
    } else {
        Vec::new()
    };
    let mut policy_records = policy_records.iter().peekable();
    let now = chrono::DateTime::parse_from_rfc3339("2026-10-21T00:00:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc);
    tidb_datatype::with_test_conversion_clock(now, || {
        let specs = catalog();
        assert_eq!(specs.len(), 37);
        let mut count = 0usize;
        let mut hash = 0xcbf29ce484222325u64;
        let mut gaps = vec![];
        for (sig, standard) in specs {
            for arity in if full {
                vec![standard]
            } else {
                vec![0, 1, 2, 3, 9]
            } {
                let base = smoke(sig, arity);
                let samples = if full {
                    crate::tikv::byte_unary_value_tests::values()
                        .into_iter()
                        .enumerate()
                        .flat_map(|(vi, v)| {
                            (0..arity).map({
                                let base = &base;
                                move |pos| {
                                    let mut a = base.clone();
                                    a[pos] = v.clone();
                                    (vi * arity + pos, a)
                                }
                            })
                        })
                        .collect::<Vec<_>>()
                } else {
                    (0..4)
                        .map(|vi| {
                            let mut a = base.clone();
                            if arity > 0 {
                                match vi {
                                    1 => a[0] = Datum::Null,
                                    2 => a[arity - 1] = Datum::Null,
                                    3 => a[0] = Datum::Bytes(vec![255]),
                                    _ => (),
                                }
                            }
                            (vi, a)
                        })
                        .collect()
                };
                for source in [8, 253] {
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
                                        for profile in if full { vec![0] } else { vec![0, 3, 7] } {
                                            for enabled in [false, true] {
                                                for selection in if full {
                                                    vec![None, Some(vec![2, 0, 2]), Some(vec![])]
                                                } else {
                                                    vec![None, Some(vec![])]
                                                } {
                                                    let original = make(
                                                        sig, source, result, flags, name, shape,
                                                        values,
                                                    );
                                                    let actual = make(
                                                        sig, source, result, flags, name, shape,
                                                        values,
                                                    );
                                                    let ty = output_type(&original);
                                                    let old =
                                                        C::new(enabled, values.clone(), profile);
                                                    let new =
                                                        C::new(enabled, values.clone(), profile);
                                                    for reuse in 0..if full { 2 } else { 1 } {
                                                        let label=format!("pb-eager/{full}/{sig:?}/{arity}/{source}/{result}/{flags:?}/{name}/{shape}/{vi}/{profile}/{enabled}/{selection:?}/{reuse}");
                                                        let mut a = input(&selection);
                                                        let mut left = output_for(&ty);
                                                        left.append_null(0);
                                                        let expected =
                                                            original_session_read_outcome(|| {
                                                                original
                                                                    .run(&old, &mut a, &mut left)
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
                                                                eprintln!("EAGER DIFFERENCE {label} old={expected:?} new={got:?} left={:?} right={:?} oldevents={} newevents={}",left.column(0),right.column(0),old.snapshot(),new.snapshot());
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
                                                            if (route.prepared == 0
                                                                || route.refusals != 0
                                                                || route.fallback != 0)
                                                                && gaps.len() < 8
                                                            {
                                                                gaps.push(format!(
                                                                    "{label}:{route:?}"
                                                                ));
                                                            }
                                                        }
                                                        let record = format!(
                                                            "{label}|{expected}|{:?}|{}",
                                                            left.column(0),
                                                            old.snapshot()
                                                        );
                                                        if policy_records
                                                            .peek()
                                                            .is_some_and(|item| item.0 == count)
                                                        {
                                                            let (_, before, after) =
                                                                policy_records.next().unwrap();
                                                            assert_eq!(
                                                                &record, after,
                                                                "policy outcome {label}"
                                                            );
                                                            observe(&mut hash, before);
                                                        } else {
                                                            observe(&mut hash, &record);
                                                        }
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
        assert!(
            policy_records.next().is_none(),
            "unvisited policy observation"
        );
        eprintln!("PB EAGER ORIGINAL full={full} {count}/{hash:016X}");
        assert_eq!(
            (count, hash),
            if full {
                (2317248, 0x529CC9B973792E9D)
            } else {
                (7672320, 0xF82657AC72E2284D)
            }
        );
        if !gaps.is_empty() {
            eprintln!("PB EAGER SOURCE RED {gaps:?}");
        }
        assert!(gaps.is_empty());
    });
}
#[test]
fn pb_eager_source_original_long() {
    if isolate_common_corpus(
        "stateful_tests::pb_regexp_tests::eager_tests::pb_eager_source_original_long",
    ) {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    long(false);
}
#[test]
fn pb_eager_source_actual_long() {
    if isolate_common_corpus(
        "stateful_tests::pb_regexp_tests::eager_tests::pb_eager_source_actual_long",
    ) {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    long(true);
}
fn long(compare: bool) {
    let now = chrono::DateTime::parse_from_rfc3339("2026-10-21T00:00:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc);
    tidb_datatype::with_test_conversion_clock(now, || {
        let (mut count, mut hash) = (0usize, 0xcbf29ce484222325u64);
        let mut gaps = vec![];
        for (sig, arity) in [
            (Sig::Pi, 0),
            (Sig::UpperUtf8, 1),
            (Sig::Substring3ArgsUtf8, 3),
            (Sig::Pow, 2),
            (Sig::Conv, 3),
            (Sig::RoundDec, 2),
            (Sig::DateFormatSig, 2),
            (Sig::UnixTimestampDec, 1),
            (Sig::FromUnixTime1Arg, 1),
        ] {
            for result in [8, 12] {
                for flags in [None, Some(32)] {
                    for name in ["canonical", "coalesce"] {
                        for shape in [1, 3] {
                            for profile in [0, 3, 7] {
                                let values = smoke(sig, arity);
                                let original = make(sig, 253, result, flags, name, shape, &values);
                                let actual = make(sig, 253, result, flags, name, shape, &values);
                                let ty = output_type(&original);
                                let old = C::new(false, values.clone(), profile);
                                let new = C::new(false, values, profile);
                                for rows in [0, 1, 3, 1026, 2051] {
                                    for (round, enabled) in
                                        [false, true, true, false].into_iter().enumerate()
                                    {
                                        old.enabled.set(enabled);
                                        new.enabled.set(enabled);
                                        old.id.set(if round == 2 { 18 } else { 17 });
                                        new.id.set(old.id.get());
                                        let selection = if rows > 1 {
                                            Some(
                                                (0..rows)
                                                    .map(|i| (rows - 1 - i / 2) % rows)
                                                    .collect::<Vec<_>>(),
                                            )
                                        } else {
                                            None
                                        };
                                        let data = (0..rows)
                                            .map(|_| vec![Datum::new_string("a")])
                                            .collect::<Vec<_>>();
                                        for reuse in 0..2 {
                                            let label=format!("pb-eager-long/{sig:?}/{result}/{flags:?}/{name}/{shape}/{profile}/{rows}/{round}/{reuse}");
                                            let mut a = chunk(&[string_ty()], &data);
                                            a.set_sel(selection.clone());
                                            let mut left = output_for(&ty);
                                            left.append_null(0);
                                            let expected = original_session_read_outcome(|| {
                                                original.run(&old, &mut a, &mut left)
                                            });
                                            if compare {
                                                let mut b = chunk(&[string_ty()], &data);
                                                b.set_sel(selection.clone());
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
                                                    eprintln!("EAGER LONG DIFFERENCE {label} old={expected:?} new={got:?} oldevents={} newevents={}",old.snapshot(),new.snapshot());
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
                                                    || route.refusals != 0
                                                    || route.fallback != 0)
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
        eprintln!("PB EAGER LONG ORIGINAL {count}/{hash:016X}");
        assert_eq!((count, hash), (17280, 0xF4427E1341D6858D));
        if !gaps.is_empty() {
            eprintln!("PB EAGER LONG SOURCE RED {gaps:?}");
        }
        assert!(gaps.is_empty());
    });
}

#[test]
fn pb_eager_source_direct_consumers_and_fixed_cap_dispositions() {
    for (sig, arity) in catalog() {
        let values = smoke(sig, arity);
        for name in [
            "wire display",
            "char_length",
            "plus",
            "abs",
            "cast_json",
            "cast_decimal",
        ] {
            let owner = make(sig, 253, 8, None, name, 1, &values);
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
        let owner = make(sig, 253, 8, None, "wire display", 1, &values);
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

        let mut first = input(&Some(vec![0; 8193]));
        let mut output = output_for(&ty);
        output.append_null(0);
        let ctx = C::new(true, values.clone(), 0);
        let native = C::new(true, values.clone(), 0);
        let mut original_input = input(&Some(vec![0; 8193]));
        let mut original_output = output_for(&ty);
        original_output.append_null(0);
        let expected = original_session_read_outcome(|| {
            owner.run(&native, &mut original_input, &mut original_output)
        });
        let got = session_read_outcome(|| {
            prepared
                .run(
                    &owner,
                    &ctx,
                    &mut EvalContext::default(),
                    &mut first,
                    &mut output,
                    &[string_ty()],
                )
                .map_err(BatchFailure::into_evaluator_error)
        });
        assert_eq!(got, expected, "large PB source/{sig:?}");
        assert_eq!(output.column(0), original_output.column(0));
        assert_eq!(ctx.snapshot(), native.snapshot());
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
        }
    }
}

#[test]
fn pb_eager_values_authenticate_every_raw_phase_and_callback_epoch() {
    use super::super::super::hosts::{NativeBatchHosts, NativeHostMetadata, NativePbEagerValues};
    use tidb_query_expr::local::{
        HostArgReply, HostInvocation, HostStart, HostStep, HostTaskId, LocalHostServices,
    };
    struct Context<'a> {
        store: &'a RefCell<NativeValueStore>,
        ready: crate::ReadyValueCache,
        reset: Cell<bool>,
        panic: Cell<bool>,
        reads: Cell<usize>,
    }
    impl Columns for Context<'_> {
        fn get(&self, _: &[String]) -> Option<Datum> {
            panic!("no expression callback")
        }
        fn ready_value_cache(&self) -> Option<&crate::ReadyValueCache> {
            drop(self.store.borrow_mut());
            self.reads.set(self.reads.get() + 1);
            if self.reset.replace(false) {
                self.store.borrow_mut().begin_window().unwrap();
            }
            assert!(!self.panic.replace(false), "original PB ready callback");
            Some(&self.ready)
        }
    }
    let owner = make(
        Sig::Pow,
        253,
        5,
        None,
        "wire display",
        1,
        &smoke(Sig::Pow, 2),
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
    let metadata = [NativeHostMetadata::PbEagerValues(Box::new(
        NativePbEagerValues {
            arguments: vec![3, 4].into(),
            kernel: super::super::super::hosts::PbValueKernel::Eager(
                function.pb_eager_values().unwrap(),
            ),
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
    let stale = insert(&Datum::Int(2), NativeValueRole::CharInput, 3, row);
    store.borrow_mut().begin_window().unwrap();
    let context = Context {
        store: &store,
        ready: crate::ReadyValueCache::new(),
        reset: Cell::new(false),
        panic: Cell::new(false),
        reads: Cell::new(0),
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
                        Some(insert(&Datum::Int(2), NativeValueRole::CharInput, 3, row).to_vec())
                    ),
                    Ok(HostStep::NeedArg(_))
                ));
            }
            let bad = match kind {
                0 => None,
                1 => Some((stale.clone()).to_vec()),
                2 => Some((vec![0; 8]).to_vec()),
                3 => {
                    Some((insert(&Datum::Int(3), NativeValueRole::Output, 3 + index, row)).to_vec())
                }
                4 => Some((insert(&Datum::Int(3), NativeValueRole::CharInput, 99, row)).to_vec()),
                5 => Some(
                    (insert(
                        &Datum::Int(3),
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
                        &Datum::Int(3),
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
                        (insert(&Datum::Int(3), NativeValueRole::CharInput, 3 + index, row))
                            .to_vec(),
                    )
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
    // NULL never coerces or clones the retained, invalid numeric prefix.
    store.borrow_mut().begin_window().unwrap();
    let task = begin(&mut provider, &mut eval);
    reply(
        &mut provider,
        &mut eval,
        &task,
        0,
        Some(
            insert(
                &Datum::Bytes(vec![b'x'; 32768]),
                NativeValueRole::CharInput,
                3,
                row,
            )
            .to_vec(),
        ),
    )
    .unwrap();
    assert!(matches!(
        reply(
            &mut provider,
            &mut eval,
            &task,
            1,
            Some(insert(&Datum::Null, NativeValueRole::CharInput, 4, row).to_vec())
        ),
        Ok(HostStep::Ready(_))
    ));
    provider.cancel(&task);
    for behavior in 0..3 {
        store.borrow_mut().begin_window().unwrap();
        let task = begin(&mut provider, &mut eval);
        reply(
            &mut provider,
            &mut eval,
            &task,
            0,
            Some(insert(&Datum::Int(2), NativeValueRole::CharInput, 3, row).to_vec()),
        )
        .unwrap();
        context.reset.set(behavior == 1);
        context.panic.set(behavior == 2);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            reply(
                &mut provider,
                &mut eval,
                &task,
                1,
                Some(insert(&Datum::Int(3), NativeValueRole::CharInput, 4, row).to_vec()),
            )
        }));
        match behavior {
            0 => assert!(
                matches!(&result, Ok(Ok(HostStep::Ready(_)))),
                "result={result:?}; native={:?}",
                provider.take_native_error()
            ),
            1 => assert!(matches!(result, Ok(Err(LocalError::BindingContract(_))))),
            _ => assert!(result.is_err()),
        };
        provider.cancel(&task);
        provider.cancel(&task);
        assert!(reply(&mut provider, &mut eval, &task, 1, None).is_err());
    }
    for _ in 0..64 {
        store.borrow_mut().begin_window().unwrap();
        let task = begin(&mut provider, &mut eval);
        reply(
            &mut provider,
            &mut eval,
            &task,
            0,
            Some(insert(&Datum::Int(2), NativeValueRole::CharInput, 3, row).to_vec()),
        )
        .unwrap();
        provider.cancel(&task);
    }
}

#[test]
fn pb_eager_source_rejects_stale_optional_declarations_and_selected_origins() {
    for (sig, arity) in catalog() {
        for bad in 0..6 {
            let mut owner = make(sig, 253, 8, None, "coalesce", 0, &smoke(sig, arity));
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
                    let other = make(other_sig, 253, 8, None, "coalesce", 0, &smoke(other_sig, 1));
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

#[path = "batch_pb_json_tests.rs"]
mod json_tests;

#[path = "batch_pb_binary_tests.rs"]
mod binary_tests;

#[path = "batch_pb_boolean_tests.rs"]
mod boolean_tests;

#[path = "batch_pb_lazy_tests.rs"]
mod lazy_tests;

#[path = "batch_pb_cast_tests.rs"]
mod cast_tests;
