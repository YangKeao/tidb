// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Original RAND occurrence, owner-property and lazy AES observations.
use super::*;
use crate::tikv::dynamic_leaf_tests as leaf;
use crate::BlockEncryptionMode as M;
use tidb_query_datatype::codec::data_type::ScalarValueRef;
const NAMES: [&str; 4] = ["rand", "tidb_is_ddl_owner", "aes_encrypt", "aes_decrypt"];
const MODES: [M; 12] = [
    M::Aes128Ecb,
    M::Aes192Ecb,
    M::Aes256Ecb,
    M::Aes128Cbc,
    M::Aes192Cbc,
    M::Aes256Cbc,
    M::Aes128Ofb,
    M::Aes192Ofb,
    M::Aes256Ofb,
    M::Aes128Cfb,
    M::Aes192Cfb,
    M::Aes256Cfb,
];
struct C {
    ready: crate::ReadyValueCache,
    values: [Datum; 3],
    profile: usize,
    fail_at: usize,
    enabled: bool,
    mode: M,
    reads: [Cell<usize>; 3],
    rng_calls: Cell<usize>,
    owner_calls: Cell<usize>,
    mode_calls: Cell<usize>,
    session: crate::MysqlRng,
    seeded: RefCell<std::collections::HashMap<usize, crate::MysqlRng>>,
    keys: RefCell<Vec<usize>>,
    events: RefCell<Vec<String>>,
}
impl C {
    fn new(values: [Datum; 3], profile: usize, enabled: bool, mode: M) -> Self {
        Self {
            ready: crate::ReadyValueCache::new(),
            values,
            profile,
            fail_at: 1,
            enabled,
            mode,
            reads: [Cell::new(0), Cell::new(0), Cell::new(0)],
            rng_calls: Cell::new(0),
            owner_calls: Cell::new(0),
            mode_calls: Cell::new(0),
            session: crate::MysqlRng::new_with_seed(73),
            seeded: RefCell::new(std::collections::HashMap::new()),
            keys: RefCell::new(vec![]),
            events: RefCell::new(vec![]),
        }
    }
    fn snapshot(&self) -> String {
        format!(
            "{}:{}:{}|{}:{}:{}|{:?}",
            self.reads[0].get(),
            self.reads[1].get(),
            self.reads[2].get(),
            self.rng_calls.get(),
            self.owner_calls.get(),
            self.mode_calls.get(),
            self.events.borrow()
        )
    }
    fn rng_available(&self, n: usize) -> bool {
        if self.profile == 3 && n == self.fail_at {
            panic!("late RNG callback")
        }
        self.profile != 1 && !(self.profile == 2 && n == self.fail_at)
    }
}
impl Columns for C {
    fn ready_value_cache(&self) -> Option<&crate::ReadyValueCache> {
        Some(&self.ready)
    }
    fn get(&self, _: &[String]) -> Option<Datum> {
        panic!("no expression callback")
    }
    fn enable_vectorized_expression(&self) -> bool {
        self.events
            .borrow_mut()
            .push(format!("flag:{}", self.enabled));
        self.enabled
    }
    fn param_value(&self, index: usize) -> std::result::Result<Datum, EvalError> {
        let n = self.reads[index].get();
        self.reads[index].set(n + 1);
        self.events.borrow_mut().push(format!("param:{index}:{n}"));
        if self.profile == 5 && index == 1 && n == self.fail_at {
            return Err(EvalError::Unsupported("late final-state child failure"));
        }
        Ok(self.values[index].clone())
    }
    fn rand_next(&self) -> Option<f64> {
        let n = self.rng_calls.get();
        self.rng_calls.set(n + 1);
        self.events.borrow_mut().push(format!("rand:{n}"));
        self.rng_available(n).then(|| self.session.gen())
    }
    fn rand_seeded_next(&self, key: usize, seed: i64) -> Option<f64> {
        // Only test observations normalize identities. Runtime state is keyed
        // by the actual original occurrence pointer supplied by the caller.
        let class = {
            let mut keys = self.keys.borrow_mut();
            if let Some(i) = keys.iter().position(|x| *x == key) {
                i
            } else {
                keys.push(key);
                keys.len() - 1
            }
        };
        let n = self.rng_calls.get();
        self.rng_calls.set(n + 1);
        self.events
            .borrow_mut()
            .push(format!("seeded:{class}:{seed}:{n}"));
        self.rng_available(n).then(|| {
            self.seeded
                .borrow_mut()
                .entry(key)
                .or_insert_with(|| crate::MysqlRng::new_with_seed(seed))
                .gen()
        })
    }
    fn ddl_owner_info(&self) -> std::result::Result<bool, EvalError> {
        let n = self.owner_calls.get();
        self.owner_calls.set(n + 1);
        self.events.borrow_mut().push(format!("owner:{n}"));
        if self.profile == 1 {
            return Err(EvalError::Unsupported(
                "optional property: 'OptPropDDLOwnerInfo' not exists in EvalContext",
            ));
        }
        if self.profile == 2 && n == self.fail_at {
            return Err(EvalError::Unsupported("late owner provider failure"));
        }
        if self.profile == 3 && n == self.fail_at {
            panic!("late owner callback")
        }
        Ok(n % 2 == 0)
    }
    fn block_encryption_mode(&self) -> M {
        let n = self.mode_calls.get();
        self.mode_calls.set(n + 1);
        let mode = if self.profile == 4 && n > 0 {
            if self.mode.iv_required() {
                M::Aes128Ecb
            } else {
                M::Aes128Cbc
            }
        } else {
            self.mode
        };
        self.events.borrow_mut().push(format!("mode:{mode:?}:{n}"));
        if self.profile == 3 && n == self.fail_at {
            panic!("late encryption-mode callback")
        }
        mode
    }
    fn append_warning(&self, code: u16, message: &str) {
        self.events
            .borrow_mut()
            .push(format!("warning:{code}:{message}"));
    }
    fn time_zone(&self) -> SessionTimeZone {
        self.events.borrow_mut().push("zone".into());
        SessionTimeZone::utc()
    }
    fn type_flags(&self) -> tidb_datatype::ConversionFlags {
        self.events.borrow_mut().push("flags".into());
        tidb_datatype::DEFAULT_STATEMENT_FLAGS.with_ignore_truncate_err(true)
    }
    fn set_uservar(&self, name: &str, value: Datum) {
        self.events
            .borrow_mut()
            .push(format!("set:{name}:{}", leaf::frame(&value)));
    }
}
fn parameter(index: usize) -> Expression {
    let mut c = Constant::new(Datum::new_string("stored header"), string_ty());
    c.param_marker = Some(ParamMarker {
        order: index as i64,
    });
    Expression::Constant(c)
}
fn poison() -> Datum {
    Datum::Decimal(
        Decimal::from_raw_parts(true, vec![0, 255, b'0'], u32::MAX, 3)
            .with_declared_shape(-1, i64::MAX),
    )
}
fn root(name: &str, shape: usize) -> Expression {
    let p = parameter;
    let bad = || {
        let mut c = Constant::new(poison(), string_ty());
        c.param_marker = Some(ParamMarker { order: 0 });
        Expression::Constant(c)
    };
    let deferred = || {
        let mut c = Constant::new(Datum::new_string("stored deferred"), string_ty());
        c.deferred_expr = Some(Box::new(leaf::assignment("final", p(0), &string_ty())));
        Expression::Constant(c)
    };
    let args = if name == "rand" {
        match shape {
            0 => vec![],
            1 => vec![int(1)],
            2 => vec![p(0)],
            3 => vec![string_column(0)],
            4 => vec![deferred()],
            5 => vec![p(0), p(1)],
            _ => vec![bad()],
        }
    } else if name == "tidb_is_ddl_owner" {
        match shape {
            0 => vec![],
            1 => vec![p(0)],
            2 => vec![literal(Datum::Bytes(vec![0xff]), string_ty())],
            3 => vec![literal(poison(), string_ty())],
            4 => vec![bad()],
            5 => vec![string_column(0)],
            _ => vec![int(1), p(1)],
        }
    } else {
        match shape {
            0 => vec![p(0), p(1)],
            1 => vec![p(0), p(1), p(2)],
            2 => vec![p(0)],
            3 => vec![p(0), p(1), p(2), p(1)],
            4 => vec![],
            5 => vec![string_column(0), p(1), p(2)],
            _ => vec![bad(), p(1), p(2)],
        }
    };
    call(
        name,
        if name == "rand" {
            real_ty()
        } else if name == "tidb_is_ddl_owner" {
            ty()
        } else {
            string_ty()
        },
        args,
    )
}
fn values() -> Vec<[Datum; 3]> {
    vec![
        [
            Datum::new_string("hello"),
            Datum::new_string("key"),
            Datum::new_string("1234567890123456"),
        ],
        [
            Datum::Null,
            Datum::new_string("key"),
            Datum::new_string("1234567890123456"),
        ],
        [
            Datum::new_string("abc"),
            Datum::Null,
            Datum::new_string("1234567890123456"),
        ],
        [
            Datum::Bytes(vec![0xff, 0]),
            Datum::new_string("pass"),
            Datum::Null,
        ],
        [
            Datum::Int(3),
            Datum::UInt(u64::MAX),
            Datum::new_string("short"),
        ],
        [
            Datum::new_string("2bad"),
            Datum::new_string(""),
            Datum::Bytes(vec![]),
        ],
    ]
}
#[test]
fn final_state_original_identity_modes_demands_and_publication() {
    if isolate_common_corpus(
        "final_state_tests::final_state_original_identity_modes_demands_and_publication",
    ) {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    let (mut count, mut hash) = (0usize, 0xcbf29ce484222325u64);
    for name in NAMES {
        for shape in 0..7 {
            let owner = suite(root(name, shape));
            let actual = suite(root(name, shape));
            let field = if name == "rand" {
                real_ty()
            } else if name == "tidb_is_ddl_owner" {
                ty()
            } else {
                string_ty()
            };
            let modes = if name.starts_with("aes_") {
                &MODES[..]
            } else {
                &MODES[..1]
            };
            for mode in modes {
                for profile in 0..6 {
                    for enabled in [false, true] {
                        for (vi, values) in values().into_iter().enumerate() {
                            for selection in [None, Some(vec![2, 0, 2]), Some(vec![])] {
                                let c = C::new(values.clone(), profile, enabled, *mode);
                                // The chunk has a string column; parameter 0
                                // independently retains its original Int datum.
                                let column = match &values[0] {
                                    Datum::Int(value) => Datum::new_string(value.to_string()),
                                    value => value.clone(),
                                };
                                let mut input = chunk(&[string_ty()], &vec![vec![column]; 3]);
                                input.set_sel(selection.clone());
                                let mut output = output_for(&field);
                                output.append_null(0);
                                let result =
                                    session_read_outcome(|| owner.run(&c, &mut input, &mut output));
                                let label=format!("{name}/{shape}/{mode:?}/{profile}/{enabled}/{vi}/{selection:?}");
                                {
                                    let new = C::new(values.clone(), profile, enabled, *mode);
                                    let column = match &values[0] {
                                        Datum::Int(v) => Datum::new_string(v.to_string()),
                                        v => v.clone(),
                                    };
                                    let mut right_input =
                                        chunk(&[string_ty()], &vec![vec![column]; 3]);
                                    right_input.set_sel(selection.clone());
                                    let mut right = output_for(&field);
                                    right.append_null(0);
                                    let got = session_read_outcome(|| {
                                        actual.run_with_tikv_batch(
                                            &new,
                                            &mut right_input,
                                            &mut right,
                                            &[string_ty()],
                                        )
                                    });
                                    if got != result
                                        || format!("{:?}", right.column(0))
                                            != format!("{:?}", output.column(0))
                                        || new.snapshot() != c.snapshot()
                                    {
                                        eprintln!("FINAL STATE DIFFERENCE {label} old={result:?} new={got:?} oldcol={:?} newcol={:?} oldeffects={} neweffects={}",output.column(0),right.column(0),c.snapshot(),new.snapshot());
                                    }
                                    assert_eq!(got, result, "outcome {label}");
                                    assert_eq!(
                                        format!("{:?}", right.column(0)),
                                        format!("{:?}", output.column(0)),
                                        "column {label}"
                                    );
                                    assert_eq!(new.snapshot(), c.snapshot(), "effects {label}");
                                    let route = actual.batch_route_snapshot();
                                    assert_eq!(
                                        (route.fallback, route.refusals),
                                        (0, 0),
                                        "source {label}: {route:?}"
                                    );
                                }

                                for byte in format!(
                                    "{label}|{result}|{:?}|{}",
                                    output.column(0),
                                    c.snapshot()
                                )
                                .bytes()
                                {
                                    hash = (hash ^ u64::from(byte)).wrapping_mul(0x100000001b3);
                                }
                                count += 1;
                            }
                        }
                    }
                }
            }
        }
    }
    eprintln!("FINAL STATE ORIGINAL {count}/{hash:016X}");
    assert_eq!((count, hash), (39312, 0x139A40582F6B294D));
}
#[test]
fn final_state_actual_sources_do_not_fallback() {
    let mut gaps = vec![];
    for name in NAMES {
        for shape in [0, 1, 2, 5, 6] {
            let owner = suite(root(name, shape));
            let field = if name == "rand" {
                real_ty()
            } else if name == "tidb_is_ddl_owner" {
                ty()
            } else {
                string_ty()
            };
            let values = values().remove(0);
            let c = C::new(values.clone(), 0, true, M::Aes128Ecb);
            let mut input = chunk(&[string_ty()], &[vec![values[0].clone()]]);
            let mut output = output_for(&field);
            let _ = session_read_outcome(|| {
                owner.run_with_tikv_batch(&c, &mut input, &mut output, &[string_ty()])
            });
            let route = owner.batch_route_snapshot();
            if (route.refusals, route.fallback) != (0, 0) {
                gaps.push((name, shape, route));
            }
        }
    }
    assert!(gaps.is_empty(), "final state source gaps: {gaps:?}");
}

#[test]
fn final_state_rand_uses_original_occurrence_during_public_source_alternation() {
    for shape in [1, 2, 4] {
        for enabled in [false, true] {
            let owner = suite(root("rand", shape));
            let Expression::ScalarFunction(function) = &owner.program.calculated[0] else {
                unreachable!()
            };
            let key = std::ptr::from_ref(function) as usize;
            let mut c = C::new(
                [Datum::Int(7), Datum::Null, Datum::Null],
                0,
                enabled,
                M::Aes128Ecb,
            );
            let expected = crate::MysqlRng::new_with_seed(if shape == 1 { 1 } else { 7 });
            for cycle in 0..12 {
                c.values[0] = Datum::Int(7 + cycle);
                let mut input = chunk(&[string_ty()], &vec![vec![Datum::new_string("ignored")]; 3]);
                input.set_sel(Some(vec![2, 0, 2]));
                let mut output = output_for(&real_ty());
                if cycle % 2 == 0 {
                    owner
                        .run_with_tikv_batch(&c, &mut input, &mut output, &[string_ty()])
                        .unwrap();
                } else {
                    owner.run(&c, &mut input, &mut output).unwrap();
                }
                for actual in values_for(&output, &real_ty()) {
                    assert_eq!(
                        actual,
                        Datum::Real(expected.gen()),
                        "shape={shape}, cycle={cycle}"
                    );
                }
                assert_eq!(
                    c.keys.borrow().as_slice(),
                    &[key],
                    "true original occurrence"
                );
                let route = owner.batch_route_snapshot();
                assert_eq!((route.fallback, route.refusals), (0, 0));
            }
        }
    }
}

#[test]
fn final_state_aes_preparation_panic_preserves_effective_ready_lane() {
    let original = suite(root("aes_encrypt", 1));
    let actual = suite(root("aes_encrypt", 1));
    let left = C::new(values().remove(0), 3, false, M::Aes128Ecb);
    let right = C::new(values().remove(0), 3, false, M::Aes128Ecb);
    let input = || chunk(&[string_ty()], &vec![vec![Datum::new_string("unused")]; 3]);
    let mut old_input = input();
    let mut new_input = input();
    let mut old_output = output_for(&string_ty());
    let mut new_output = output_for(&string_ty());
    let old = session_read_outcome(|| original.run(&left, &mut old_input, &mut old_output));
    let new = session_read_outcome(|| {
        actual.run_with_tikv_batch(&right, &mut new_input, &mut new_output, &[string_ty()])
    });
    assert_eq!(old, new);
    assert_eq!(left.snapshot(), right.snapshot());
    assert_eq!(
        format!("{:?}", old_output.column(0)),
        format!("{:?}", new_output.column(0))
    );
    assert_eq!(left.ready.prepared_worker_count(), 0);
    assert_eq!(
        right.ready.prepared_worker_count(),
        left.ready.prepared_worker_count(),
        "unwind clears the same effective lane"
    );
    let probe = |ctx: &C| {
        crate::tikv::evaluate_args_in(
            crate::tikv::EvaluatedBytesOp::AesNullNative,
            ctx,
            || Ok(crate::tikv::EvaluatedArgs::NullWitness(None)),
            |value| value.into_bytes(),
        )
    };
    assert_eq!(
        format!("{:?}", probe(&left)),
        format!("{:?}", probe(&right)),
        "subsequent real worker demand observes the same poisoned lane"
    );
}

#[test]
fn final_state_actual_datums_and_return_types_match_original_before_retirement() {
    if isolate_common_corpus("final_state_tests::final_state_actual_datums_and_return_types_match_original_before_retirement") { return; }
    std::panic::set_hook(Box::new(|_| {}));
    let fields = return_fields();
    let (mut count, mut hash) = (0usize, 0xcbf29ce484222325u64);
    for name in NAMES {
        for (fi, field) in fields.iter().enumerate() {
            for (vi, value) in crate::tikv::byte_unary_value_tests::values()
                .into_iter()
                .enumerate()
            {
                let positions = if name.starts_with("aes_") { 3 } else { 1 };
                for index in 0..positions {
                    let modes = if name.starts_with("aes_") {
                        &MODES[..]
                    } else {
                        &MODES[..1]
                    };
                    for mode in modes {
                        let mut args = if name.starts_with("aes_") {
                            vec![parameter(0), parameter(1), parameter(2)]
                        } else {
                            vec![parameter(0)]
                        };
                        let mut p = Constant::new(
                            Datum::Int(99),
                            crate::tikv::misc_identity_value_tests::field(&value),
                        );
                        p.param_marker = Some(ParamMarker {
                            order: index as i64,
                        });
                        args[index] = Expression::Constant(p);
                        let expr = call(name, field.clone(), args);
                        let original = suite(expr.clone());
                        let actual = suite(expr);
                        for enabled in [false, true] {
                            for selection in [None, Some(vec![2, 0, 2]), Some(vec![])] {
                                let mut values = values().remove(0);
                                values[index] = value.clone();
                                let old = C::new(values.clone(), 0, enabled, *mode);
                                let new = C::new(values, 0, enabled, *mode);
                                let input = || {
                                    let mut c = chunk(
                                        &[string_ty()],
                                        &vec![vec![Datum::new_string("unread")]; 3],
                                    );
                                    c.set_sel(selection.clone());
                                    c
                                };
                                let (mut left_input, mut right_input) = (input(), input());
                                let (mut left, mut right) = (output_for(field), output_for(field));
                                left.append_null(0);
                                right.append_null(0);
                                let expected = original_session_read_outcome(|| {
                                    original.run(&old, &mut left_input, &mut left)
                                });
                                let got = session_read_outcome(|| {
                                    actual.run_with_tikv_batch(
                                        &new,
                                        &mut right_input,
                                        &mut right,
                                        &[string_ty()],
                                    )
                                });
                                let label = format!(
                                    "{name}/{fi}/{vi}/{index}/{mode:?}/{enabled}/{selection:?}"
                                );
                                if got != expected
                                    || right.column(0) != left.column(0)
                                    || new.snapshot() != old.snapshot()
                                {
                                    eprintln!("FINAL STATE FULL DIFFERENCE {label} input={} old={expected:?} new={got:?} oldcol={:?} newcol={:?} oldeffects={} neweffects={}",leaf::frame(&value),left.column(0),right.column(0),old.snapshot(),new.snapshot());
                                }
                                assert_eq!(got, expected, "result {label}");
                                assert_eq!(right.column(0), left.column(0), "column {label}");
                                assert_eq!(new.snapshot(), old.snapshot(), "effects {label}");
                                let route = actual.batch_route_snapshot();
                                assert_eq!(
                                    (route.fallback, route.refusals),
                                    (0, 0),
                                    "source {label}: {route:?}"
                                );
                                for byte in format!(
                                    "{label}|{expected}|{:?}|{}",
                                    left.column(0),
                                    old.snapshot()
                                )
                                .bytes()
                                {
                                    hash = (hash ^ u64::from(byte)).wrapping_mul(0x100000001b3);
                                }
                                count += 1;
                            }
                        }
                    }
                }
            }
        }
    }
    eprintln!("FINAL STATE FULL ORIGINAL {count}/{hash:016X}");
    assert_eq!((count, hash), (727716, 0x092E0C47BF46492F));
}

#[test]
fn final_state_aes_authenticates_after_warning_before_worker_and_recovers_after_cancel() {
    use tidb_query_expr::local::{
        HostArgReply, HostInvocation, HostSignature, HostStart, HostStep, HostTaskId,
        LocalHostServices,
    };
    struct D<'a> {
        store: &'a RefCell<NativeValueStore>,
        ready: crate::ReadyValueCache,
        action: Cell<u8>,
        warnings: Cell<usize>,
    }
    impl Columns for D<'_> {
        fn get(&self, _: &[String]) -> Option<Datum> {
            panic!("no expression callback")
        }
        fn ready_value_cache(&self) -> Option<&crate::ReadyValueCache> {
            Some(&self.ready)
        }
        fn block_encryption_mode(&self) -> M {
            let _exclusive = self.store.borrow_mut();
            M::Aes128Ecb
        }
        fn append_warning(&self, code: u16, message: &str) {
            assert_eq!((code, message), (1618, "<IV> option ignored"));
            self.warnings.set(self.warnings.get() + 1);
            let mut store = self.store.borrow_mut();
            match self.action.get() {
                1 => panic!("AES warning callback"),
                2 => store.begin_window().unwrap(),
                _ => {}
            }
        }
    }
    for optional in [false, true] {
        let Expression::ScalarFunction(mut function) = root("aes_encrypt", 1) else {
            unreachable!()
        };
        if optional {
            function.ret_type = None;
        }
        let wire = projected(&native_bytes_type(), true).unwrap();
        let args = vec![wire.clone(); 3];
        let catalog = HostCatalog::for_sql_batch(vec![(
            SqlBatchHostOperation::EagerValuesNative,
            HostSignature {
                arg_types: args.clone().into(),
                return_type: wire.clone(),
            },
        )])
        .unwrap();
        let slot = catalog.slot(0).unwrap();
        let metadata = [hosts::NativeHostMetadata::FinalState(Box::new(
            hosts::NativeFinalState {
                arguments: vec![3, 4, 5].into(),
                name: "aes_encrypt",
                function_key: std::ptr::from_ref(&function) as usize,
                constant_seed: true,
                result_type: (!optional).then(string_ty),
                output_node: 2,
                output_role: NativeValueRole::Output,
            },
        ))];
        let store = RefCell::new(NativeValueStore::new(8 << 20).unwrap());
        store.borrow_mut().begin_window().unwrap();
        let ctx = D {
            store: &store,
            ready: crate::ReadyValueCache::new(),
            action: Cell::new(0),
            warnings: Cell::new(0),
        };
        let mut p = hosts::NativeBatchHosts::new(catalog, &metadata, &ctx, 16384)
            .with_native_values(Some(&store));
        let mut eval = EvalContext::default();
        let row = InputRow {
            occurrence: 2,
            input_row: 7,
        };
        let origin = |node| NativeValueOrigin { node, row };
        let insert = |value: &Datum, role, node| {
            store
                .borrow_mut()
                .insert_clone(value, role, origin(node))
                .unwrap()
        };
        let begin = |p: &mut hosts::NativeBatchHosts<'_>, eval: &mut EvalContext| {
            let HostStart::Pending { task, request } = p
                .start(
                    eval,
                    HostInvocation {
                        slot,
                        row,
                        arg_types: &args,
                        return_type: &wire,
                    },
                )
                .unwrap()
            else {
                panic!()
            };
            assert_eq!(request.index, 0);
            task
        };
        let reply = |p: &mut hosts::NativeBatchHosts<'_>,
                     eval: &mut EvalContext,
                     task: &HostTaskId,
                     index,
                     token| {
            p.resume(
                eval,
                task,
                HostArgReply {
                    index,
                    field_type: &wire,
                    values: &VectorValue::from_scalar(&ScalarValue::Bytes(token), 1),
                },
            )
        };
        let original = insert(&Datum::Null, NativeValueRole::CharInput, 3);
        let mut bad = original.clone();
        bad[0] ^= 1;
        let mut short = original.clone();
        short.pop();
        let mut long = original.clone();
        long.push(0);
        store.borrow_mut().begin_window().unwrap();
        let mut foreign = NativeValueStore::new(4096).unwrap();
        foreign.begin_window().unwrap();
        let foreign = foreign
            .insert_clone(&Datum::Null, NativeValueRole::CharInput, origin(3))
            .unwrap();
        let wrong_row = store
            .borrow_mut()
            .insert_clone(
                &Datum::Null,
                NativeValueRole::CharInput,
                NativeValueOrigin {
                    node: 3,
                    row: InputRow {
                        occurrence: 3,
                        ..row
                    },
                },
            )
            .unwrap();
        let wrong_physical = store
            .borrow_mut()
            .insert_clone(
                &Datum::Null,
                NativeValueRole::CharInput,
                NativeValueOrigin {
                    node: 3,
                    row: InputRow {
                        input_row: 8,
                        ..row
                    },
                },
            )
            .unwrap();
        for token in [
            None,
            Some(original),
            Some(bad),
            Some(short),
            Some(long),
            Some(foreign),
            Some(wrong_row),
            Some(wrong_physical),
            Some(insert(&Datum::Null, NativeValueRole::Output, 3)),
            Some(insert(&Datum::Null, NativeValueRole::CharInput, 4)),
        ] {
            let task = begin(&mut p, &mut eval);
            assert!(matches!(
                reply(&mut p, &mut eval, &task, 0, token),
                Err(LocalError::BindingContract(_))
            ));
            assert_eq!(ctx.warnings.get(), 0);
            assert_eq!(ctx.ready.prepared_worker_count(), 0);
            p.cancel(&task);
        }
        let task = begin(&mut p, &mut eval);
        let input = insert(&Datum::new_string("abc"), NativeValueRole::CharInput, 3);
        assert!(matches!(
            reply(&mut p, &mut eval, &task, 0, Some(input)),
            Ok(HostStep::NeedArg(_))
        ));
        ctx.action.set(2);
        let key = insert(&Datum::new_string("key"), NativeValueRole::CharInput, 4);
        assert!(matches!(
            reply(&mut p, &mut eval, &task, 1, Some(key)),
            Err(LocalError::BindingContract(_))
        ));
        assert_eq!(ctx.warnings.get(), 1);
        assert_eq!(
            ctx.ready.prepared_worker_count(),
            0,
            "invalidated inputs must not reach the cipher worker"
        );
        p.cancel(&task);
        ctx.action.set(0);
        for _ in 0..128 {
            store.borrow_mut().begin_window().unwrap();
            let abandoned = begin(&mut p, &mut eval);
            let input = insert(&Datum::new_string("abc"), NativeValueRole::CharInput, 3);
            assert!(matches!(
                reply(&mut p, &mut eval, &abandoned, 0, Some(input)),
                Ok(HostStep::NeedArg(_))
            ));
            p.cancel(&abandoned);
            let current = begin(&mut p, &mut eval);
            p.cancel(&abandoned);
            let stale = insert(&Datum::new_string("key"), NativeValueRole::CharInput, 4);
            assert!(reply(&mut p, &mut eval, &abandoned, 1, Some(stale)).is_err());
            let null = insert(&Datum::Null, NativeValueRole::CharInput, 3);
            let HostStep::Ready(values) =
                reply(&mut p, &mut eval, &current, 0, Some(null)).unwrap()
            else {
                panic!()
            };
            let ScalarValueRef::Bytes(Some(token)) = values.get_scalar_ref(0) else {
                panic!()
            };
            assert_eq!(
                store
                    .borrow()
                    .get(token, NativeValueRole::Output, origin(2))
                    .unwrap(),
                &Datum::Null
            );
            assert_eq!(ctx.warnings.get(), 1);
            assert_eq!(
                ctx.ready.prepared_worker_count(),
                1,
                "stale cancel must retain the current actual cache binding"
            );
            p.cancel(&current);
        }
    }
}

fn return_fields() -> [FieldType; 11] {
    [
        FieldType::new(FieldTypeCode::LongLong),
        FieldType::new(FieldTypeCode::LongLong).with_unsigned(true),
        FieldType::new(FieldTypeCode::Float),
        FieldType::new(FieldTypeCode::Double),
        FieldType::new(FieldTypeCode::NewDecimal)
            .with_flen(65)
            .with_decimal(6),
        FieldType::new(FieldTypeCode::VarString),
        FieldType::new(FieldTypeCode::Date),
        FieldType::new(FieldTypeCode::Datetime).with_decimal(6),
        FieldType::new(FieldTypeCode::Duration).with_decimal(6),
        FieldType::new(FieldTypeCode::Json),
        FieldType::new(FieldTypeCode::VectorFloat32),
    ]
}

#[test]
fn final_state_shared_value_phases_match_preserved_original_controls() {
    if isolate_common_corpus(
        "final_state_tests::final_state_shared_value_phases_match_preserved_original_controls",
    ) {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    let (mut count, mut hash) = (0usize, 0xcbf29ce484222325u64);
    let input = chunk(&[string_ty()], &[vec![Datum::new_string("unused")]]);
    for name in NAMES {
        for (fi, field) in return_fields().iter().enumerate() {
            for (vi, value) in crate::tikv::byte_unary_value_tests::values()
                .into_iter()
                .enumerate()
            {
                for index in 0..if name.starts_with("aes_") { 3 } else { 1 } {
                    let modes = if name.starts_with("aes_") {
                        &MODES[..]
                    } else {
                        &MODES[..1]
                    };
                    for mode in modes {
                        let mut args = if name.starts_with("aes_") {
                            vec![parameter(0), parameter(1), parameter(2)]
                        } else {
                            vec![parameter(0)]
                        };
                        let mut p = Constant::new(
                            Datum::Int(99),
                            crate::tikv::misc_identity_value_tests::field(&value),
                        );
                        p.param_marker = Some(ParamMarker {
                            order: index as i64,
                        });
                        args[index] = Expression::Constant(p);
                        let Expression::ScalarFunction(function) = call(name, field.clone(), args)
                        else {
                            unreachable!()
                        };
                        let mut values = values().remove(0);
                        values[index] = value.clone();
                        let old = C::new(values.clone(), 0, false, *mode);
                        let new = C::new(values, 0, false, *mode);
                        let expected = original_session_read_outcome(|| {
                            old.ready
                                .with_columns(&old, |ctx| function.eval(ctx, input.get_row(0)))
                        });
                        let got = session_read_outcome(|| {
                            new.ready
                                .with_columns(&new, |ctx| function.eval(ctx, input.get_row(0)))
                        });
                        let label = format!("{name}/{fi}/{vi}/{index}/{mode:?}");
                        if got != expected || new.snapshot() != old.snapshot() {
                            eprintln!("FINAL STATE INDEPENDENT DIFFERENCE {label} old={expected:?} new={got:?} oldeffects={} neweffects={}",old.snapshot(),new.snapshot());
                        }
                        assert_eq!(got, expected, "result {label}");
                        assert_eq!(new.snapshot(), old.snapshot(), "effects {label}");
                        for byte in format!("{label}|{expected}|{}", old.snapshot()).bytes() {
                            hash = (hash ^ u64::from(byte)).wrapping_mul(0x100000001b3);
                        }
                        count += 1;
                    }
                }
            }
        }
    }
    eprintln!("FINAL STATE INDEPENDENT ORIGINAL {count}/{hash:016X}");
    assert_eq!((count, hash), (121286, 0x286849C89B032D6E));
}

#[test]
fn final_state_owner_all_return_types_keep_late_callbacks_and_prefixes() {
    if isolate_common_corpus(
        "final_state_tests::final_state_owner_all_return_types_keep_late_callbacks_and_prefixes",
    ) {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    for field in return_fields() {
        for profile in 0..6 {
            for enabled in [false, true] {
                for selection in [None, Some(vec![2, 0, 2]), Some(vec![])] {
                    let expr = call("tidb_is_ddl_owner", field.clone(), vec![]);
                    let original = suite(expr.clone());
                    let actual = suite(expr);
                    let old = C::new(values().remove(0), profile, enabled, M::Aes128Ecb);
                    let new = C::new(values().remove(0), profile, enabled, M::Aes128Ecb);
                    let input = || {
                        let mut c =
                            chunk(&[string_ty()], &vec![vec![Datum::new_string("unused")]; 3]);
                        c.set_sel(selection.clone());
                        c
                    };
                    let (mut a, mut b) = (input(), input());
                    let (mut left, mut right) = (output_for(&field), output_for(&field));
                    left.append_null(0);
                    right.append_null(0);
                    let expected =
                        original_session_read_outcome(|| original.run(&old, &mut a, &mut left));
                    let got = session_read_outcome(|| {
                        actual.run_with_tikv_batch(&new, &mut b, &mut right, &[string_ty()])
                    });
                    assert_eq!(got, expected);
                    assert_eq!(left.column(0), right.column(0));
                    assert_eq!(old.snapshot(), new.snapshot());
                    let route = actual.batch_route_snapshot();
                    assert_eq!((route.fallback, route.refusals), (0, 0));
                    // Preserve the actual old owner branch independently of the
                    // shared public helper, including every returned type.
                    let Expression::ScalarFunction(f) =
                        call("tidb_is_ddl_owner", field.clone(), vec![])
                    else {
                        unreachable!()
                    };
                    let x = C::new(values().remove(0), profile, enabled, M::Aes128Ecb);
                    let y = C::new(values().remove(0), profile, enabled, M::Aes128Ecb);
                    assert_eq!(
                        session_read_outcome(|| f.eval(&x, a.physical_row(0))),
                        session_read_outcome(|| f.eval(&y, b.physical_row(0)))
                    );
                    assert_eq!(x.snapshot(), y.snapshot());
                }
            }
        }
    }
}

#[test]
fn final_state_long_selections_and_nested_fresh_keep_original_state_and_prefixes() {
    if isolate_common_corpus("final_state_tests::final_state_long_selections_and_nested_fresh_keep_original_state_and_prefixes") {return;}
    std::panic::set_hook(Box::new(|_| {}));
    for (name, shape) in [
        ("rand", 0),
        ("rand", 2),
        ("rand", 3),
        ("rand", 5),
        ("tidb_is_ddl_owner", 0),
        ("tidb_is_ddl_owner", 1),
        ("aes_encrypt", 1),
        ("aes_decrypt", 1),
        ("aes_encrypt", 5),
    ] {
        let field = if name == "rand" {
            real_ty()
        } else if name == "tidb_is_ddl_owner" {
            ty()
        } else {
            string_ty()
        };
        for nested in [false, true] {
            let expr = if nested {
                call(
                    "benchmark",
                    ty(),
                    vec![
                        int(3),
                        call("name_const", field.clone(), vec![int(1), root(name, shape)]),
                    ],
                )
            } else {
                root(name, shape)
            };
            let out = if nested { ty() } else { field.clone() };
            for n in [2051usize, 2057] {
                if nested && n == 2051 {
                    continue;
                }
                for profile in [0, 2, 5] {
                    if (n == 2051 || nested) && profile != 0 {
                        continue;
                    }
                    for enabled in [false, true] {
                        let selections = if nested {
                            vec![Some((0..n).rev().chain([0, n - 1, 0]).collect::<Vec<_>>())]
                        } else {
                            vec![
                                None,
                                Some((0..n).rev().chain([0, n - 1, 0]).collect::<Vec<_>>()),
                                Some(vec![]),
                            ]
                        };
                        for selection in selections {
                            let original = suite(expr.clone());
                            let actual = suite(expr.clone());
                            let mut old =
                                C::new(values().remove(0), profile, enabled, M::Aes128Ecb);
                            let mut new =
                                C::new(values().remove(0), profile, enabled, M::Aes128Ecb);
                            old.fail_at = 1026;
                            new.fail_at = 1026;
                            for cycle in 0..3 {
                                let input = || {
                                    let mut c = chunk(
                                        &[string_ty()],
                                        &vec![vec![Datum::new_string("7")]; n],
                                    );
                                    c.set_sel(selection.clone());
                                    c
                                };
                                let (mut a, mut b) = (input(), input());
                                let (mut left, mut right) = (output_for(&out), output_for(&out));
                                left.append_null(0);
                                right.append_null(0);
                                let expected = original_session_read_outcome(|| {
                                    original.run(&old, &mut a, &mut left)
                                });
                                let got = session_read_outcome(|| {
                                    if cycle == 1 {
                                        actual.run(&new, &mut b, &mut right)
                                    } else {
                                        actual.run_with_tikv_batch(
                                            &new,
                                            &mut b,
                                            &mut right,
                                            &[string_ty()],
                                        )
                                    }
                                });
                                let label=format!("{name}/{shape}/{nested}/{n}/{profile}/{enabled}/{cycle}/selected={:?}",selection.as_ref().map(Vec::len));
                                if got != expected
                                    || left.column(0) != right.column(0)
                                    || old.snapshot() != new.snapshot()
                                {
                                    eprintln!("FINAL STATE LONG DIFFERENCE {label} old={expected:?} new={got:?}");
                                }
                                assert_eq!(got, expected, "result {label}");
                                assert_eq!(left.column(0), right.column(0), "column {label}");
                                assert_eq!(old.snapshot(), new.snapshot(), "effects {label}");
                                let route = actual.batch_route_snapshot();
                                assert_eq!(
                                    (route.fallback, route.refusals),
                                    (0, 0),
                                    "source {label}: {route:?}"
                                );
                                assert_eq!(a.sel(), b.sel());
                            }
                        }
                    }
                }
            }
        }
    }
}

#[test]
fn final_state_unused_large_values_and_real_null_recovery_do_not_clone_extra_payloads() {
    let vector =
        || Datum::VectorFloat32(VectorFloat32::init((2 << 20) / std::mem::size_of::<f32>()));
    for (name, shape, mode, index) in [
        ("aes_encrypt", 1, M::Aes128Ecb, 2),
        ("aes_decrypt", 1, M::Aes128Ecb, 2),
        ("aes_encrypt", 1, M::Aes128Cbc, 2),
        ("rand", 5, M::Aes128Ecb, 0),
    ] {
        for enabled in [false, true] {
            let original = suite(root(name, shape));
            let actual = suite(root(name, shape));
            let mut values = values().remove(0);
            values[index] = vector();
            if mode.iv_required() && index == 2 {
                values[1] = Datum::Null;
            }
            let mut old = C::new(values.clone(), 0, enabled, mode);
            let mut new = C::new(values, 0, enabled, mode);
            let field = if name == "rand" {
                real_ty()
            } else {
                string_ty()
            };
            for recovery in [false, true] {
                if recovery {
                    old.values[0] = Datum::Null;
                    new.values[0] = Datum::Null;
                }
                let input = || chunk(&[string_ty()], &vec![vec![Datum::new_string("unused")]; 3]);
                let (mut a, mut b) = (input(), input());
                let (mut left, mut right) = (output_for(&field), output_for(&field));
                let expected =
                    original_session_read_outcome(|| original.run(&old, &mut a, &mut left));
                let got = session_read_outcome(|| {
                    actual.run_with_tikv_batch(&new, &mut b, &mut right, &[string_ty()])
                });
                assert_eq!(got, expected, "{name}/{index}/{recovery}");
                assert_eq!(left.column(0), right.column(0));
                assert_eq!(old.snapshot(), new.snapshot());
                if index == 2 {
                    assert_eq!(old.reads[2].get(), 0);
                    assert_eq!(new.reads[2].get(), 0);
                }
                let route = actual.batch_route_snapshot();
                assert_eq!((route.fallback, route.refusals), (0, 0));
            }
        }
    }
}

#[test]
fn final_state_demanded_vector_formatting_obeys_existing_work_limit_and_null_recovers() {
    // A demanded vector password genuinely formats every component. The existing
    // shared formatter budgets 512 work units/component; this exceeds the
    // unchanged finite source allowance, unlike the ignored large IV cases.
    let actual = suite(root("aes_encrypt", 1));
    let mut values = values().remove(0);
    values[1] = Datum::VectorFloat32(VectorFloat32::init((2 << 20) / std::mem::size_of::<f32>()));
    let mut c = C::new(values, 0, true, M::Aes128Ecb);
    let mut input = chunk(&[string_ty()], &[vec![Datum::new_string("unread")]]);
    let mut output = output_for(&string_ty());
    let error = actual
        .run_with_tikv_batch(&c, &mut input, &mut output, &[string_ty()])
        .unwrap_err();
    assert!(
        matches!(error,crate::evaluator::EvaluatorError::Eval(EvalError::ExpressionRuntimeFailure(ref failure)) if failure.class()==crate::tikv::ExpressionRuntimeFailureClass::ResourceLimit)
    );
    assert_eq!(c.reads[1].get(), 1);
    assert_eq!(c.ready.prepared_worker_count(), 0);
    assert_eq!(output.num_rows(), 0);
    c.values[0] = Datum::Null;
    let mut input = chunk(&[string_ty()], &vec![vec![Datum::new_string("unread")]; 3]);
    actual
        .run_with_tikv_batch(&c, &mut input, &mut output, &[string_ty()])
        .unwrap();
    assert_eq!(output.num_rows(), 3);
    for row in 0..3 {
        assert!(output.get_row(row).is_null(0));
    }
    assert_eq!(c.reads[0].get(), 4);
    assert_eq!(c.reads[1].get(), 1);
    assert!(c
        .events
        .borrow()
        .iter()
        .all(|event| !event.starts_with("warning:")));
    let route = actual.batch_route_snapshot();
    assert_eq!((route.fallback, route.refusals), (0, 0));
}

#[test]
fn final_state_niladic_services_use_actual_context_over_virtual_rows_without_columns() {
    if isolate_common_corpus("final_state_tests::final_state_niladic_services_use_actual_context_over_virtual_rows_without_columns") {return;}
    std::panic::set_hook(Box::new(|_| {}));
    for name in ["rand", "tidb_is_ddl_owner"] {
        let field = if name == "rand" { real_ty() } else { ty() };
        for profile in 0..4 {
            for enabled in [false, true] {
                for selection in [None, Some(vec![2050, 0, 2050]), Some(vec![])] {
                    let original = suite(root(name, 0));
                    let actual = suite(root(name, 0));
                    let old = C::new(values().remove(0), profile, enabled, M::Aes128Ecb);
                    let new = C::new(values().remove(0), profile, enabled, M::Aes128Ecb);
                    let input = || {
                        let mut c = Chunk::new_with_capacity(&[], 2051);
                        c.set_num_virtual_rows(2051);
                        c.set_sel(selection.clone());
                        c
                    };
                    let (mut a, mut b) = (input(), input());
                    let (mut left, mut right) = (output_for(&field), output_for(&field));
                    left.append_null(0);
                    right.append_null(0);
                    let expected =
                        original_session_read_outcome(|| original.run(&old, &mut a, &mut left));
                    let got = session_read_outcome(|| {
                        actual.run_with_tikv_batch(&new, &mut b, &mut right, &[])
                    });
                    assert_eq!(got, expected, "{name}/{profile}/{enabled}");
                    assert_eq!(left.column(0), right.column(0));
                    assert_eq!(old.snapshot(), new.snapshot());
                    let route = actual.batch_route_snapshot();
                    assert_eq!((route.fallback, route.refusals), (0, 0));
                }
            }
        }
    }
}

#[test]
fn optional_phase_final_state_original_and_source() {
    if isolate_common_corpus("final_state_tests::optional_phase_final_state_original_and_source") {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    let mut observed = (0usize, 0xcbf29ce484222325u64, Vec::new());
    for name in NAMES {
        for shape in 0..7 {
            for mask in 0..8 {
                for mode in [M::Aes128Ecb, M::Aes128Cbc] {
                    for profile in 0..6 {
                        for enabled in [false, true] {
                            for (vi, values) in values().into_iter().enumerate() {
                                for selection in [None, Some(vec![2, 0, 2]), Some(vec![])] {
                                    let make = || {
                                        let mut e = root(name, shape);
                                        erase_optional_call_declarations(&mut e, mask);
                                        suite(e)
                                    };
                                    let owner = make();
                                    let actual = make();
                                    let old = C::new(values.clone(), profile, enabled, mode);
                                    let new = C::new(values.clone(), profile, enabled, mode);
                                    let field = if name == "rand" {
                                        real_ty()
                                    } else if name == "tidb_is_ddl_owner" {
                                        ty()
                                    } else {
                                        string_ty()
                                    };
                                    let schema = [string_ty()];
                                    let input = || {
                                        let mut c = chunk(
                                            &schema,
                                            &vec![
                                                vec![match &values[0] {
                                                    Datum::Int(v) =>
                                                        Datum::new_string(v.to_string()),
                                                    v => v.clone(),
                                                }];
                                                3
                                            ],
                                        );
                                        c.set_sel(selection.clone());
                                        c
                                    };
                                    let label = format!("final_state/{name}/{shape}/{mask}/{mode:?}/{profile}/{enabled}/{vi}/{selection:?}");
                                    observe_optional_phase_case(
                                        &label,
                                        [&owner, &actual],
                                        [&old, &new],
                                        |c| c.snapshot(),
                                        input,
                                        (&field, &schema),
                                        &mut observed,
                                    );
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    eprintln!(
        "OPTIONAL PHASE final_state ORIGINAL {}/{:016X}",
        observed.0, observed.1
    );
    assert_eq!((observed.0, observed.1), (193536, 0x687909A43FB9D9D5));
    if !observed.2.is_empty() {
        eprintln!("OPTIONAL PHASE final_state ADMISSION RED {:?}", observed.2);
    }
    assert!(observed.2.is_empty());
}
