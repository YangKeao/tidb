// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Genuine Arc-owned roots across live contexts before request-lane caching.
use super::*;
use crate::tikv::dynamic_leaf_tests as leaf;
use std::sync::{Arc, RwLock};
use tidb_chunk::row::Row;

fn root(kind: usize, optional: bool, outer: usize, binding: Arc<RwLock<Datum>>) -> Arc<Expression> {
    let declared = (!optional).then(ty);
    let mut value = match kind {
        0 => column(0),
        1 => leaf::expression("param", 0, &Datum::Int(7), declared.as_ref(), None),
        2 => leaf::expression("deferred_row", 0, &Datum::Int(7), declared.as_ref(), None),
        3 => leaf::expression("deferred_value", 0, &Datum::Int(7), declared.as_ref(), None),
        4 => leaf::expression(
            "correlated",
            0,
            &Datum::Int(7),
            declared.as_ref(),
            Some(binding),
        ),
        5 => leaf::expression(
            "correlated_absent",
            0,
            &Datum::Int(7),
            declared.as_ref(),
            None,
        ),
        6 => leaf::expression(
            "param_effect_deferred",
            0,
            &Datum::Int(7),
            declared.as_ref(),
            None,
        ),
        7 => leaf::assignment(
            "owned-v",
            leaf::expression("param", 0, &Datum::Int(7), declared.as_ref(), None),
            &ty(),
        ),
        _ => unreachable!(),
    };
    if optional {
        match &mut value {
            Expression::Column(c) => c.ret_type = None,
            Expression::ScalarFunction(f) => f.ret_type = None,
            _ => {}
        }
    }
    Arc::new(match outer {
        0 => value,
        1 => plus(value, int(2)),
        2 => call("if", ty(), vec![value, int(11), int(22)]),
        3 => call(
            "concat",
            string_ty(),
            vec![value, literal(Datum::new_string("!"), string_ty())],
        ),
        _ => unreachable!(),
    })
}
fn records(cached: bool) -> Vec<String> {
    let mut records = vec![];
    let one = chunk(
        &[ty()],
        &[
            vec![Datum::Int(10)],
            vec![Datum::Null],
            vec![Datum::Int(-3)],
        ],
    );
    let two = chunk(
        &[ty(), ty()],
        &[
            vec![Datum::Int(19), Datum::Int(3)],
            vec![Datum::Null, Datum::Int(-9)],
        ],
    );
    for kind in 0..8 {
        for optional in [false, true] {
            for outer in 0..4 {
                let binding = Arc::new(RwLock::new(Datum::Int(7)));
                let root = root(kind, optional, outer, Arc::clone(&binding));
                let cache = crate::ReadyValueCache::new();
                for (vi, value) in [
                    Datum::Null,
                    Datum::Int(-1),
                    Datum::Int(7),
                    Datum::UInt(u64::MAX),
                    Datum::Real(1.5),
                    Datum::new_string("12"),
                    Datum::Bytes(vec![0xff]),
                ]
                .into_iter()
                .enumerate()
                {
                    for profile in [0, 1, 3] {
                        for sequential in [false, true] {
                            for enabled in [false, true] {
                                for (ri, row) in [
                                    Row::empty(),
                                    one.physical_row(0),
                                    one.physical_row(1),
                                    one.physical_row(2),
                                    two.physical_row(0),
                                    two.physical_row(1),
                                ]
                                .into_iter()
                                .enumerate()
                                {
                                    *binding.write().unwrap() = value.clone();
                                    let mut context = leaf::Context::new(profile, value.clone());
                                    context.sequential = sequential;
                                    context.enabled.set(enabled);
                                    context.binding = Some(Arc::clone(&binding));
                                    let observed = leaf::observe(
                                        || {
                                            cache.with_columns(&context, |bound| {
                                                let value = if cached {
                                                    cache.eval_owned_expression(&root, bound, row)
                                                } else {
                                                    root.eval(bound, row)
                                                };
                                                value.map_err(|error| (Datum::Null, error))
                                            })
                                        },
                                        &context,
                                    );
                                    records.push(format!("{kind}/{optional}/{outer}/{vi}/{profile}/{sequential}/{enabled}/{ri}|{observed}"));
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
fn digest(records: &[String]) -> u64 {
    let mut hash = 0xcbf29ce484222325u64;
    for record in records {
        for byte in record.bytes().chain([b'\n']) {
            hash = (hash ^ u64::from(byte)).wrapping_mul(0x100000001b3);
        }
    }
    hash
}
#[test]
fn owned_uncached_original_observations() {
    let records = crate::test_original_eval::with_original(|| records(false));
    println!(
        "N56 owned original count={} digest={:016X}",
        records.len(),
        digest(&records)
    );
    assert_eq!(records.len(), 32_256);
    assert_eq!(digest(&records), 0x461C9558DA525FA9);
}
#[test]
fn owned_forwarding_matches_independent_original_and_uncached_shared() {
    let expected = crate::test_original_eval::with_original(|| records(false));
    let actual = records(false);
    let forwarded = records(true);
    assert_eq!(expected.len(), actual.len());
    for (i, ((old, shared), cached)) in expected.iter().zip(&actual).zip(&forwarded).enumerate() {
        assert_eq!(old, shared, "original/shared owned record {i}");
        assert_eq!(old, cached, "original/owned-cache record {i}");
    }
}
#[test]
fn owned_cache_prepares_once_and_keeps_genuine_root_alive() {
    let cache = crate::ReadyValueCache::new();
    let root = Arc::new(plus(column(0), int(2)));
    let input = chunk(&[ty()], &[vec![Datum::Int(5)]]);
    let before = crate::test_original_eval::scalar_preparations();
    for _ in 0..8 {
        assert_eq!(
            cache
                .eval_owned_expression(&root, &crate::NoColumns, input.physical_row(0))
                .unwrap(),
            Datum::Int(7)
        );
    }
    assert_eq!(crate::test_original_eval::scalar_preparations() - before, 1);
    assert_eq!(
        Arc::strong_count(&root),
        2,
        "cache must retain the actual root, not a cloned tree"
    );
    let weak = Arc::downgrade(&root);
    drop(root);
    assert!(
        weak.upgrade().is_some(),
        "metadata outlived its genuine owner"
    );
    drop(cache);
    assert!(weak.upgrade().is_none());
}
#[test]
fn owned_cache_width_and_owner_changes_evict_without_stale_bindings() {
    let cache = crate::ReadyValueCache::new();
    let a = Arc::new(int(7));
    let b = Arc::new(a.as_ref().clone());
    assert!(!Arc::ptr_eq(&a, &b));
    let one = chunk(&[ty()], &[vec![Datum::Int(1)]]);
    let two = chunk(&[ty(), ty()], &[vec![Datum::Int(2), Datum::Int(3)]]);
    let mut before = crate::test_original_eval::scalar_preparations();
    for row in [one.physical_row(0), one.physical_row(0)] {
        assert_eq!(
            cache
                .eval_owned_expression(&a, &crate::NoColumns, row)
                .unwrap(),
            Datum::Int(7)
        );
    }
    assert_eq!(crate::test_original_eval::scalar_preparations() - before, 1);
    before = crate::test_original_eval::scalar_preparations();
    assert_eq!(
        cache
            .eval_owned_expression(&a, &crate::NoColumns, two.physical_row(0))
            .unwrap(),
        Datum::Int(7)
    );
    assert_eq!(crate::test_original_eval::scalar_preparations() - before, 1);
    assert_eq!(
        cache
            .eval_owned_expression(&b, &crate::NoColumns, two.physical_row(0))
            .unwrap(),
        Datum::Int(7)
    );
    assert_eq!(Arc::strong_count(&a), 1, "old root retained after eviction");
    assert_eq!(Arc::strong_count(&b), 2);
}

fn recovery_records(cached: bool) -> Vec<String> {
    let cache = crate::ReadyValueCache::new();
    let root = Arc::new(plus(
        column(0),
        leaf::expression("param", 0, &Datum::Null, Some(&ty()), None),
    ));
    let input = chunk(&[ty()], &[vec![Datum::Int(5)]]);
    let mut records = vec![];
    for profile in [1, 2, 0, 3] {
        let context = leaf::Context::new(profile, Datum::Int(3));
        records.push(leaf::observe(
            || {
                cache.with_columns(&context, |bound| {
                    let value = if cached {
                        cache.eval_owned_expression(&root, bound, input.physical_row(0))
                    } else {
                        root.eval(bound, input.physical_row(0))
                    };
                    value.map_err(|error| (Datum::Null, error))
                })
            },
            &context,
        ));
    }
    assert!(
        cache.original_scope_status().is_err(),
        "callback unwind did not retain the original lane poison"
    );
    let oversized = Arc::new(call(
        "coalesce",
        ty(),
        (0..CompileLimits::default().max_nodes)
            .map(|_| leaf::expression("param", 0, &Datum::Null, Some(&ty()), None))
            .collect(),
    ));
    let context = leaf::Context::new(2, Datum::Int(7));
    let error = cache
        .with_columns(&context, |bound| {
            if cached {
                cache.eval_owned_expression(&oversized, bound, Row::empty())
            } else {
                oversized.eval(bound, Row::empty())
            }
        })
        .unwrap_err();
    assert!(matches!(error, EvalError::ExpressionRuntimeFailure(error)
        if error.class() == crate::ExpressionRuntimeFailureClass::ResourceLimit));
    assert_eq!(context.calls.get(), 0, "metadata work demanded a parameter");
    let constant = Arc::new(int(7));
    let result = cache.with_columns(&context, |bound| {
        if cached {
            cache.eval_owned_expression(&constant, bound, Row::empty())
        } else {
            constant.eval(bound, Row::empty())
        }
    });
    assert_eq!(
        result.unwrap(),
        Datum::Int(7),
        "pure metadata lookup checked a ready capability too early"
    );
    records
}
#[test]
fn owned_cache_error_unwind_and_poison_keep_original_demand_points() {
    assert_eq!(recovery_records(true), recovery_records(false));
}

#[test]
fn owned_cache_reentry_uses_the_same_driver_without_borrowing_its_slot() {
    struct Reentrant<'a> {
        cache: &'a crate::ReadyValueCache,
        nested: Arc<Expression>,
        cached: bool,
        calls: Cell<usize>,
    }
    impl Columns for Reentrant<'_> {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }
        fn param_value(&self, order: usize) -> std::result::Result<Datum, EvalError> {
            assert_eq!(order, 0);
            self.calls.set(self.calls.get() + 1);
            if self.cached {
                self.cache
                    .eval_owned_expression(&self.nested, self, Row::empty())
            } else {
                self.nested.eval(self, Row::empty())
            }
        }
    }
    let evaluate = |cached| {
        let cache = crate::ReadyValueCache::new();
        let context = Reentrant {
            cache: &cache,
            nested: Arc::new(int(3)),
            cached,
            calls: Cell::new(0),
        };
        let root = Arc::new(plus(
            column(0),
            leaf::expression("param", 0, &Datum::Null, Some(&ty()), None),
        ));
        let input = chunk(&[ty()], &[vec![Datum::Int(5)]]);
        let mut values = vec![];
        for _ in 0..4 {
            let value = cache
                .with_columns(&context, |bound| {
                    if cached {
                        cache.eval_owned_expression(&root, bound, input.physical_row(0))
                    } else {
                        root.eval(bound, input.physical_row(0))
                    }
                })
                .unwrap();
            values.push(value);
        }
        assert_eq!(values, vec![Datum::Int(8); 4]);
        (values, context.calls.get())
    };
    assert_eq!(evaluate(true), evaluate(false));
}

#[test]
fn owned_cache_never_retains_a_live_context_or_input_row() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct Context(Arc<AtomicUsize>);
    impl Columns for Context {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }
        fn param_value(&self, _: usize) -> std::result::Result<Datum, EvalError> {
            Ok(Datum::Int(7))
        }
    }
    impl Drop for Context {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    let drops = Arc::new(AtomicUsize::new(0));
    let context = Arc::new(Context(Arc::clone(&drops)));
    let weak = Arc::downgrade(&context);
    let cache = crate::ReadyValueCache::new();
    let root = Arc::new(leaf::expression(
        "param",
        0,
        &Datum::Null,
        Some(&ty()),
        None,
    ));
    assert_eq!(
        cache
            .eval_owned_expression(&root, context.as_ref(), Row::empty())
            .unwrap(),
        Datum::Int(7)
    );
    drop(context);
    assert!(weak.upgrade().is_none());
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    // A new context supplies a fresh value to the same genuine root.
    let context = leaf::Context::new(0, Datum::Int(19));
    assert_eq!(
        cache
            .eval_owned_expression(&root, &context, Row::empty())
            .unwrap(),
        Datum::Int(19)
    );
}

struct RandOwnerContext {
    inner: leaf::Context,
    expected: usize,
}
impl Columns for RandOwnerContext {
    fn get(&self, _: &[String]) -> Option<Datum> {
        panic!("no invented named input")
    }
    fn param_value(&self, order: usize) -> std::result::Result<Datum, EvalError> {
        self.inner.param_value(order)
    }
    fn enable_vectorized_expression(&self) -> bool {
        panic!("scalar plan lookup has no vector getter")
    }
    fn rand_next(&self) -> Option<f64> {
        self.inner.events.borrow_mut().push("rand-next".into());
        Some(0.25)
    }
    fn rand_seeded_next(&self, owner: usize, seed: i64) -> Option<f64> {
        assert_eq!(
            owner, self.expected,
            "RAND must keep the actual function owner"
        );
        self.inner
            .events
            .borrow_mut()
            .push(format!("seed:{seed}:actual-owner"));
        Some(0.5)
    }
    fn context_id(&self) -> u64 {
        self.inner.events.borrow_mut().push("context-id".into());
        17
    }
}
fn rand_records(cached: bool) -> Vec<String> {
    let mut records = vec![];
    let input = chunk(&[ty()], &[vec![Datum::Int(7)]]);
    for shape in 0..5 {
        let args = match shape {
            0 => vec![],
            1 => vec![int(7)],
            2 => vec![leaf::expression(
                "param",
                0,
                &Datum::Int(7),
                Some(&ty()),
                None,
            )],
            3 => vec![column(0)],
            4 => vec![int(7), int(8)],
            _ => unreachable!(),
        };
        let original = Arc::new(call("rand", real_ty(), args));
        let alias = Arc::clone(&original);
        let cloned = Arc::new(original.as_ref().clone());
        assert!(Arc::ptr_eq(&original, &alias));
        assert!(!Arc::ptr_eq(&original, &cloned));
        let cache = crate::ReadyValueCache::new();
        for (owner, root) in [original, alias, cloned].into_iter().enumerate() {
            let Expression::ScalarFunction(function) = root.as_ref() else {
                unreachable!()
            };
            let context = RandOwnerContext {
                inner: leaf::Context::new(0, Datum::Int(7)),
                expected: std::ptr::from_ref(function) as usize,
            };
            for occurrence in 0..3 {
                let observed = leaf::observe(
                    || {
                        cache.with_columns(&context, |bound| {
                            let value = if cached {
                                cache.eval_owned_expression(&root, bound, input.physical_row(0))
                            } else {
                                root.eval(bound, input.physical_row(0))
                            };
                            value.map_err(|error| (Datum::Null, error))
                        })
                    },
                    &context.inner,
                );
                records.push(format!("{shape}/{owner}/{occurrence}|{observed}"));
            }
        }
    }
    records
}
#[test]
fn owned_rand_keeps_real_owner_for_arc_alias_and_expression_clone() {
    let expected = crate::test_original_eval::with_original(|| rand_records(false));
    assert_eq!(expected.len(), 45);
    assert_eq!(rand_records(false), expected);
    assert_eq!(rand_records(true), expected);
}

#[test]
fn owned_plan_lookup_does_not_check_an_ineffective_ready_lane() {
    struct Context<'a> {
        effective: &'a crate::ReadyValueCache,
        panic: bool,
        reads: Cell<usize>,
    }
    impl Columns for Context<'_> {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }
        fn ready_value_cache(&self) -> Option<&crate::ReadyValueCache> {
            Some(self.effective)
        }
        fn param_value(&self, _: usize) -> std::result::Result<Datum, EvalError> {
            self.reads.set(self.reads.get() + 1);
            assert!(!self.panic, "effective lane callback");
            Ok(Datum::Int(7))
        }
    }
    for cached in [false, true] {
        let metadata = crate::ReadyValueCache::new();
        let effective = crate::ReadyValueCache::new();
        let context = Context {
            effective: &effective,
            panic: true,
            reads: Cell::new(0),
        };
        let root = Arc::new(leaf::expression(
            "param",
            0,
            &Datum::Null,
            Some(&ty()),
            None,
        ));
        let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            metadata.with_columns(&context, |bound| {
                if cached {
                    metadata.eval_owned_expression(&root, bound, Row::empty())
                } else {
                    root.eval(bound, Row::empty())
                }
            })
        }));
        assert!(failed.is_err());
        assert!(effective.original_scope_status().is_err());
        assert!(
            metadata.original_scope_status().is_ok(),
            "unwind poisoned the metadata cache instead of the effective lane"
        );
        let constant = Arc::new(int(7));
        assert_eq!(
            metadata
                .with_columns(&context, |bound| if cached {
                    metadata.eval_owned_expression(&constant, bound, Row::empty())
                } else {
                    constant.eval(bound, Row::empty())
                })
                .unwrap(),
            Datum::Int(7)
        );
    }
}
