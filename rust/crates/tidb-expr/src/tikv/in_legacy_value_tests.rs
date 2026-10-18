// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Independent original legacy-IN return/demand/source-error observations.
use super::*;
use crate::{Columns, Datum, EvalError};
use std::{
    cell::{Cell, RefCell},
    sync::Arc,
};

struct Context {
    cache: Option<ReadyValueCache>,
    reject_cache: bool,
    reads: Cell<u32>,
    events: RefCell<Vec<String>>,
    warnings: RefCell<Vec<(u16, String)>>,
}
impl Context {
    fn new(cached: bool) -> Self {
        Self {
            cache: cached.then(ReadyValueCache::new),
            reject_cache: false,
            reads: Cell::new(0),
            events: RefCell::new(vec![]),
            warnings: RefCell::new(vec![]),
        }
    }
    fn reset(&self) {
        self.reads.set(0);
        self.events.borrow_mut().clear();
        self.warnings.borrow_mut().clear();
    }
    fn event(&self, index: usize, selected: &dyn Columns) {
        assert_eq!(selected.context_id(), 8127);
        let precision = selected.div_precision_increment();
        self.events
            .borrow_mut()
            .push(format!("arg:{index}:{precision}"));
        selected.append_warning(60001, &format!("arg:{index}"));
    }
}
impl Columns for Context {
    fn get(&self, _: &[String]) -> Option<Datum> {
        panic!("legacy IN must use actual channels")
    }
    fn context_id(&self) -> u64 {
        8127
    }
    fn ready_value_cache(&self) -> Option<&ReadyValueCache> {
        assert!(
            !self.reject_cache,
            "legacy IN attempted retired Ready worker lookup"
        );
        self.cache.as_ref()
    }
    fn div_precision_increment(&self) -> u32 {
        let read = self.reads.get();
        self.reads.set(read + 1);
        4 + read
    }
    fn append_warning(&self, code: u16, message: &str) {
        self.warnings.borrow_mut().push((code, message.to_owned()));
    }
    fn time_zone(&self) -> tidb_datatype::SessionTimeZone {
        panic!("already typed legacy IN must not cast")
    }
    fn truncate_level(&self) -> crate::ErrorLevel {
        panic!("already typed legacy IN must not coerce")
    }
}
#[derive(Debug)]
enum Fault {
    Source(Arc<EvalError>),
    Adapter(EvalError),
}
impl From<EvalError> for Fault {
    fn from(error: EvalError) -> Self {
        Self::Adapter(error)
    }
}
fn observe(hash: &mut u64, result: &Result<Option<i128>, Fault>, ctx: &Context) {
    let text = match result {
        Ok(value) => format!("{value:?}"),
        Err(Fault::Source(value)) => format!("Source({value:?})"),
        Err(Fault::Adapter(value)) => panic!("unexpected adapter failure: {value:?}"),
    };
    for byte in format!(
        "{text}|{:?}|{:?}\n",
        ctx.events.borrow(),
        ctx.warnings.borrow()
    )
    .bytes()
    {
        *hash = (*hash ^ u64::from(byte)).wrapping_mul(0x100000001b3);
    }
    assert_eq!(ctx.events.borrow().len(), ctx.reads.get() as usize);
    assert_eq!(ctx.warnings.borrow().len(), ctx.reads.get() as usize);
    for (ordinal, event) in ctx.events.borrow().iter().enumerate() {
        assert_eq!(event, &format!("arg:{ordinal}:{}", ordinal + 4));
    }
}
fn direct_result(
    value: Result<Option<bool>, tidb_query_expr::NativeInLegacyError<Fault>>,
) -> Result<Option<i128>, Fault> {
    match value {
        Ok(value) => Ok(value.map(i128::from)),
        Err(tidb_query_expr::NativeInLegacyError::Source(error)) => Err(error),
        Err(tidb_query_expr::NativeInLegacyError::ResourceLimit(reason)) => Err(Fault::Adapter(
            EvalError::ExpressionRuntimeFailure(ExpressionRuntimeFailure::from_local_eval(
                LocalError::ResourceLimit(reason.into()),
                None,
            )),
        )),
    }
}
fn direct_int(
    ctx: &Context,
    count: usize,
    mut read: impl FnMut(usize, &dyn Columns) -> Result<Option<i128>, Fault>,
) -> Result<Option<i128>, Fault> {
    direct_result(tidb_query_expr::native_in_legacy_i128_values(count, |i| {
        read(i, ctx)
    }))
}
fn direct_bytes(
    ctx: &Context,
    count: usize,
    id: i32,
    mut read: impl FnMut(usize, &dyn Columns) -> Result<Option<Vec<u8>>, Fault>,
) -> Result<Option<i128>, Fault> {
    direct_result(tidb_query_expr::native_in_legacy_bytes_values(
        count,
        tidb_query_expr::local::ExecutionLimits::default().max_retained_bytes,
        |i| read(i, ctx),
        || {
            tidb_datatype::get_collator_by_id(id)
                .new_collation()
                .map_or(
                    NativeCollation::Binary,
                    tidb_datatype::Collation::native_policy,
                )
        },
    ))
}
fn independent(
    old: &Result<Option<i128>, Fault>,
    original: &Context,
    run: impl FnOnce(&Context) -> Result<Option<i128>, Fault>,
) {
    let ctx = Context::new(false);
    let direct = run(&ctx);
    assert_eq!(
        format!("{old:?}"),
        format!("{direct:?}"),
        "independent OLD head/step vs borrowed value service"
    );
    if let (Err(Fault::Source(a)), Err(Fault::Source(b))) = (old, &direct) {
        assert!(Arc::ptr_eq(a, b));
    }
    assert_eq!(*original.events.borrow(), *ctx.events.borrow());
    assert_eq!(*original.warnings.borrow(), *ctx.warnings.borrow());
}

fn layouts<T: Clone>(left: Option<T>, right: Option<T>) -> Vec<Vec<Option<T>>> {
    vec![
        vec![],
        vec![left.clone()],
        vec![left.clone(), right.clone()],
        vec![left.clone(), None, right.clone()],
        vec![left.clone(), right.clone(), None],
        vec![left.clone(), left.clone(), right.clone()],
        {
            let mut values = vec![None; 33];
            values[0] = left;
            values[32] = right;
            values
        },
    ]
}
#[test]
fn original_legacy_in_return_demand_and_context_witness() {
    assert!(
        tidb_datatype::new_collation_enabled(),
        "fixed normal-suite mode; mutable mode test is process-isolated"
    );
    let ints = [
        None,
        Some(i128::MIN),
        Some(i128::MAX),
        Some(i128::from(i64::MIN)),
        Some(i128::from(i64::MAX)),
        Some(i128::from(u64::MAX)),
        Some(-(1i128 << 64)),
        Some(0),
        Some(1),
        Some(-1),
    ];
    let bytes = [
        None,
        Some(b"".to_vec()),
        Some(b"A".to_vec()),
        Some(b"a ".to_vec()),
        Some("äß".as_bytes().to_vec()),
        Some(vec![255, 0]),
        Some("𠀀".as_bytes().to_vec()),
        Some(b"a\0b".to_vec()),
        Some("é".as_bytes().to_vec()),
        Some(vec![128]),
    ];
    let mut hash = 0xcbf29ce484222325;
    let mut cases = 0;
    for mode in 0..3 {
        let reused = Context::new(true);
        for left in ints {
            for right in ints {
                for values in layouts(left, right) {
                    let fresh = Context::new(mode == 1);
                    let ctx = if mode == 2 { &reused } else { &fresh };
                    ctx.reset();
                    let result =
                        eval_legacy_in_int_in::<Fault>(ctx, values.len(), |index, selected| {
                            ctx.event(index, selected);
                            Ok(values.get(index).copied().flatten())
                        });
                    independent(&result, ctx, |next| {
                        direct_int(next, values.len(), |index, selected| {
                            next.event(index, selected);
                            Ok(values.get(index).copied().flatten())
                        })
                    });
                    observe(&mut hash, &result, ctx);
                    cases += 1;
                }
            }
        }
        for id in [
            63,
            46,
            45,
            224,
            -46,
            -45,
            i32::MIN,
            i32::MAX,
            0,
            28,
            87,
            248,
        ] {
            for left in &bytes {
                for right in &bytes {
                    for values in layouts(left.clone(), right.clone()) {
                        let fresh = Context::new(mode == 1);
                        let ctx = if mode == 2 { &reused } else { &fresh };
                        ctx.reset();
                        let result = eval_legacy_in_bytes_in::<Fault>(
                            ctx,
                            values.len(),
                            id,
                            |index, selected| {
                                ctx.event(index, selected);
                                Ok(values.get(index).cloned().flatten())
                            },
                        );
                        independent(&result, ctx, |next| {
                            direct_bytes(next, values.len(), id, |index, selected| {
                                next.event(index, selected);
                                Ok(values.get(index).cloned().flatten())
                            })
                        });
                        observe(&mut hash, &result, ctx);
                        cases += 1;
                    }
                }
            }
        }
        // Even a deliberately non-NULL reply for an absent operand0 is kept.
        for count in [0, 1] {
            for left in ints {
                let fresh = Context::new(mode == 1);
                let ctx = if mode == 2 { &reused } else { &fresh };
                ctx.reset();
                let result = eval_legacy_in_int_in::<Fault>(ctx, count, |index, selected| {
                    assert_eq!(index, 0);
                    ctx.event(index, selected);
                    Ok(left)
                });
                assert!(
                    matches!(result, Ok(None) if left.is_none())
                        || matches!(result, Ok(Some(0)) if left.is_some())
                );
                independent(&result, ctx, |next| {
                    direct_int(next, count, |index, selected| {
                        next.event(index, selected);
                        Ok(left)
                    })
                });
                observe(&mut hash, &result, ctx);
                cases += 1;
            }
        }
    }
    assert_eq!(cases, 27360);
    // Captured from actual old head/step workers at762c0152/SDKe5da224.
    // Never regenerate this or the two other OLD hashes from new value code.
    assert_eq!(
        hash, 0x03fe1f325a53c789,
        "OLD legacy IN return/demand/context"
    );
}
#[test]
fn original_legacy_in_source_failure_identity_and_reuse_witness() {
    let mut hash = 0xcbf29ce484222325;
    let mut cases = 0;
    for mode in 0..3 {
        let reused = Context::new(true);
        for resource in [false, true] {
            for bytes in [false, true] {
                for base_null in [false, true] {
                    for count in [0, 1, 2, 5, usize::MAX] {
                        for failure in 0..count.min(5).max(1) {
                            let fresh = Context::new(mode == 1);
                            let ctx = if mode == 2 { &reused } else { &fresh };
                            ctx.reset();
                            let error = Arc::new(if resource {
                                EvalError::ExpressionRuntimeFailure(
                                    ExpressionRuntimeFailure::from_local_eval(
                                        LocalError::ResourceLimit(
                                            "legacy IN source denied retained budget".into(),
                                        ),
                                        None,
                                    ),
                                )
                            } else {
                                EvalError::Unsupported("legacy IN source SQL failure")
                            });
                            let input = |index: usize, selected: &dyn Columns| {
                                ctx.event(index, selected);
                                if index == failure {
                                    Err(Fault::Source(Arc::clone(&error)))
                                } else if index == 0 && base_null {
                                    Ok(None)
                                } else {
                                    Ok(Some(index as i128))
                                }
                            };
                            let result = if bytes {
                                let mut input = input;
                                eval_legacy_in_bytes_in::<Fault>(ctx, count, -45, |i, selected| {
                                    input(i, selected).map(|v| v.map(|v| v.to_le_bytes().to_vec()))
                                })
                            } else {
                                eval_legacy_in_int_in::<Fault>(ctx, count, input)
                            };
                            let Err(Fault::Source(actual)) = &result else {
                                panic!("source E folded/lost: {result:?}")
                            };
                            assert!(Arc::ptr_eq(actual, &error));
                            independent(&result, ctx, |next| {
                                let input = |index: usize, selected: &dyn Columns| {
                                    next.event(index, selected);
                                    if index == failure {
                                        Err(Fault::Source(Arc::clone(&error)))
                                    } else if index == 0 && base_null {
                                        Ok(None)
                                    } else {
                                        Ok(Some(index as i128))
                                    }
                                };
                                if bytes {
                                    let mut input = input;
                                    direct_bytes(next, count, -45, |i, selected| {
                                        input(i, selected)
                                            .map(|v| v.map(|v| v.to_le_bytes().to_vec()))
                                    })
                                } else {
                                    direct_int(next, count, input)
                                }
                            });
                            observe(&mut hash, &result, ctx);
                            cases += 1;
                            ctx.reset();
                            let retry =
                                eval_legacy_in_int_in::<Fault>(ctx, 3, |index, selected| {
                                    ctx.event(index, selected);
                                    Ok(Some(i128::MAX))
                                });
                            assert!(matches!(retry, Ok(Some(1))));
                            assert_eq!(ctx.reads.get(), 2);
                        }
                    }
                }
            }
        }
    }
    assert_eq!(cases, 336);
    assert_eq!(hash, 0x7643f3333ea24bfd, "OLD legacy IN source E/trace");
}
fn reject_worker(bytes: bool) {
    let mut ctx = Context::new(false);
    ctx.reject_cache = true;
    let result = if bytes {
        eval_legacy_in_bytes_in::<Fault>(&ctx, 2, 45, |_, _| Ok(Some(b"a".to_vec())))
    } else {
        eval_legacy_in_int_in::<Fault>(&ctx, 2, |_, _| Ok(Some(i128::MAX)))
    };
    assert!(matches!(result, Ok(Some(1))));
}
#[test]
fn legacy_integer_in_worker_route_must_be_retired() {
    reject_worker(false);
}
#[test]
fn legacy_bytes_in_worker_route_must_be_retired() {
    reject_worker(true);
}

#[test]
fn legacy_bytes_in_resolves_mutable_collation_after_actual_candidate() {
    const CHILD: &str = "TIDB_N6W_IN_COLLATION_CHILD";
    const NAME: &str = "tikv::in_legacy_value_tests::legacy_bytes_in_resolves_mutable_collation_after_actual_candidate";
    if std::env::var_os(CHILD).is_none() {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", NAME, "--nocapture"])
            .env(CHILD, "1")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "isolated late-collation test failed: {}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        eprint!("{}", String::from_utf8_lossy(&output.stderr));
        return;
    }
    // The child process runs this single test only. No mode mutation can race
    // the normal parallel suite; no ignored-test escape is required.
    let mut hash = 0xcbf29ce484222325;
    let mut cases = 0;
    for initial in [false, true] {
        for after_candidate in [false, true] {
            for id in [45, -45, 46, -46, 63, 0, i32::MIN, i32::MAX, 87, 248] {
                for null_base in [false, true] {
                    for null_candidate in [false, true] {
                        tidb_datatype::set_new_collation_enabled(initial);
                        let ctx = Context::new(true);
                        let result =
                            eval_legacy_in_bytes_in::<Fault>(&ctx, 2, id, |index, selected| {
                                ctx.event(index, selected);
                                if index == 0 {
                                    Ok((!null_base).then(|| b"a".to_vec()))
                                } else {
                                    tidb_datatype::set_new_collation_enabled(after_candidate);
                                    Ok((!null_candidate).then(|| b"A ".to_vec()))
                                }
                            });
                        let expected = if null_base || null_candidate {
                            None
                        } else {
                            let collator = tidb_datatype::get_collator_by_id(id)
                                .new_collation()
                                .map_or(
                                    NativeCollation::Binary,
                                    tidb_datatype::Collation::native_policy,
                                );
                            Some(i128::from(collator.compare(b"a", b"A ").unwrap().is_eq()))
                        };
                        assert!(matches!(result, Ok(value) if value == expected));
                        independent(&result, &ctx, |next| {
                            tidb_datatype::set_new_collation_enabled(initial);
                            direct_bytes(next, 2, id, |index, selected| {
                                next.event(index, selected);
                                if index == 0 {
                                    Ok((!null_base).then(|| b"a".to_vec()))
                                } else {
                                    tidb_datatype::set_new_collation_enabled(after_candidate);
                                    Ok((!null_candidate).then(|| b"A ".to_vec()))
                                }
                            })
                        });
                        observe(&mut hash, &result, &ctx);
                        cases += 1;
                    }
                }
            }
        }
    }
    tidb_datatype::set_new_collation_enabled(true);
    assert_eq!(cases, 160);
    assert_eq!(
        hash, 0x86adc5e714cb9209,
        "OLD legacy IN late mutable collation"
    );
}
