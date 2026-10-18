// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Actual old control values and source effects before argument-program retirement.
use super::*;
use crate::{Columns, Datum, EvalError};
use std::cell::{Cell, RefCell};
use tidb_datatype::{Collation, Decimal, MySqlDuration, MysqlEnum, MysqlSet, VectorFloat32};
struct Context {
    cache: Option<ReadyValueCache>,
    reject_cache: bool,
    reads: Cell<u32>,
    events: RefCell<Vec<String>>,
    warnings: RefCell<Vec<(u16, String)>>,
    failure: RefCell<Option<(String, EvalError)>>,
}
impl Context {
    fn new(cached: bool) -> Self {
        Self {
            cache: cached.then(ReadyValueCache::new),
            reject_cache: false,
            reads: Cell::new(0),
            events: RefCell::new(vec![]),
            warnings: RefCell::new(vec![]),
            failure: RefCell::new(None),
        }
    }
    fn reset(&self) {
        self.reads.set(0);
        self.events.borrow_mut().clear();
        self.warnings.borrow_mut().clear();
        self.failure.borrow_mut().take();
    }
    fn event(&self, label: &str, selected: &dyn Columns) -> Result<(), EvalError> {
        assert_eq!(selected.context_id(), 1307);
        let read = selected.div_precision_increment();
        self.events.borrow_mut().push(format!("{label}:{read}"));
        selected.append_warning(60000, label);
        if let Some((fault, error)) = self.failure.borrow().as_ref() {
            if label == fault {
                return Err(error.clone());
            }
        }
        Ok(())
    }
}
impl Columns for Context {
    fn get(&self, _: &[String]) -> Option<Datum> {
        panic!("control selector must not invent source lookups")
    }
    fn context_id(&self) -> u64 {
        1307
    }
    fn ready_value_cache(&self) -> Option<&ReadyValueCache> {
        assert!(
            !self.reject_cache,
            "control attempted retired Ready worker lookup"
        );
        self.cache.as_ref()
    }
    fn div_precision_increment(&self) -> u32 {
        let read = self.reads.get();
        self.reads.set(read + 1);
        4 + read
    }
    fn append_warning(&self, code: u16, text: &str) {
        self.warnings.borrow_mut().push((code, text.to_owned()));
    }
    fn truncate_level(&self) -> crate::ErrorLevel {
        panic!("control selector must not redo coercion")
    }
}
pub(super) fn fixtures() -> Vec<Datum> {
    let mut values = crate::ops::arithmetic::tests::fixtures();
    let mut vector = VectorFloat32::must_create(vec![0.0, 0.0]);
    vector
        .elements_mut()
        .copy_from_slice(&[f32::from_bits(0x7fc01234), f32::NEG_INFINITY]);
    values.extend([
        Datum::Decimal(
            Decimal::from_raw_parts(true, vec![255, 0, b'0'], u32::MAX, 3)
                .with_declared_shape(-1, i64::MAX),
        ),
        Datum::Decimal(Decimal::from_raw_parts(false, vec![b'1'], 1_000_000, 0)),
        Datum::Float32(f64::from_bits(0x7ff8000012345678)),
        Datum::Real(f64::from_bits(0xfff0000000000001)),
        Datum::Duration(MySqlDuration::from_raw_parts(i64::MIN, i64::MAX)),
        Datum::Enum(
            MysqlEnum::new(vec![255, 0], u64::MAX),
            Collation::Utf8Mb4ZhPinyinTiDbAsCs,
        ),
        Datum::Set(MysqlSet::new(vec![255, 0], u64::MAX), Collation::Gb18030Bin),
        Datum::VectorFloat32(vector),
    ]);
    assert_eq!(values.len(), 40);
    values
}
fn snapshot(value: &Datum) -> String {
    match value {
        Datum::Decimal(v) => format!(
            "Decimal({:?},{},{},{},{:?})",
            v.coefficient_bytes(),
            v.is_negative(),
            v.scale(),
            v.storage_scale(),
            v.declared_shape()
        ),
        Datum::Real(v) => format!("Real({:016x})", v.to_bits()),
        Datum::Float32(v) => format!("Float32({:016x})", v.to_bits()),
        Datum::VectorFloat32(v) => format!(
            "Vector({:?})",
            v.elements().iter().map(|v| v.to_bits()).collect::<Vec<_>>()
        ),
        Datum::Duration(v) => format!("Duration({},{})", v.nanoseconds(), v.fsp()),
        Datum::Time(v) => format!("Time({},{:?},{})", v.core_time().raw(), v.kind(), v.fsp()),
        Datum::String(v) => format!("String({:?},{:?})", v.bytes(), v.collation()),
        Datum::Enum(v, c) => format!("Enum({:?},{},{c:?})", v.name_bytes(), v.value()),
        Datum::Set(v, c) => format!("Set({:?},{},{c:?})", v.name_bytes(), v.value()),
        Datum::Json(v) => format!("Json({},{:?})", v.type_code(), v.value()),
        _ => format!("{value:?}"),
    }
}
fn result_snapshot(value: &Result<Datum, EvalError>) -> String {
    match value {
        Ok(v) => snapshot(v),
        Err(e) => format!("Err({e:?})"),
    }
}
// Captured from independently active old control programs before deletion.
// Reproduce with N6u's test-only patch at734d3745/SDK6a36ba0; never regenerate
// either fixed result/trace constant from these new direct implementations.
fn witness(hash: &mut u64, result: &Result<Datum, EvalError>, ctx: &Context) {
    for byte in format!(
        "{}|{:?}|{:?}\n",
        result_snapshot(result),
        ctx.events.borrow(),
        ctx.warnings.borrow()
    )
    .bytes()
    {
        *hash = (*hash ^ u64::from(byte)).wrapping_mul(0x100000001b3);
    }
}
fn assert_events(ctx: &Context, labels: &[String]) {
    assert_eq!(ctx.reads.get() as usize, labels.len());
    assert_eq!(
        *ctx.events.borrow(),
        labels
            .iter()
            .enumerate()
            .map(|(i, label)| format!("{label}:{}", i + 4))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        *ctx.warnings.borrow(),
        labels
            .iter()
            .map(|label| (60000, label.clone()))
            .collect::<Vec<_>>()
    );
}
fn check(
    mode: usize,
    reused: &Context,
    expected: &Datum,
    labels: Vec<String>,
    hash: &mut u64,
    run: impl FnOnce(&Context) -> Result<Datum, EvalError>,
) {
    let fresh = Context::new(mode == 1);
    let ctx = if mode == 2 { reused } else { &fresh };
    ctx.reset();
    let actual = run(ctx);
    assert_eq!(result_snapshot(&actual), snapshot(expected));
    assert_events(ctx, &labels);
    witness(hash, &actual, ctx);
}
#[test]
fn original_control_value_and_demand_witness() {
    let values = fixtures();
    let mut hash = 0xcbf29ce484222325;
    let mut cases = 0;
    for mode in 0..3 {
        let reused = Context::new(true);
        for (i, value) in values.iter().enumerate() {
            let other = &values[(i + 1) % values.len()];
            for condition in [None, Some(false), Some(true)] {
                let choose = tidb_query_expr::native_if_choose_branch(condition)
                    == tidb_query_expr::NativeIfBranch::Then;
                check(
                    mode,
                    &reused,
                    if choose { value } else { other },
                    vec![
                        "condition".into(),
                        if choose { "then".into() } else { "else".into() },
                    ],
                    &mut hash,
                    |ctx| {
                        eval_if_in(
                            ctx,
                            |selected| {
                                ctx.event("condition", selected)?;
                                Ok(condition)
                            },
                            |selected| {
                                ctx.event("then", selected)?;
                                Ok(value.clone())
                            },
                            |selected| {
                                ctx.event("else", selected)?;
                                Ok(other.clone())
                            },
                        )
                    },
                );
                cases += 1;
            }
            for second in &values {
                let choose = matches!(
                    tidb_query_expr::native_if_null_choose_first(
                        (!value.is_null()).then_some(value)
                    ),
                    tidb_query_expr::NativeIfNullChoice::Done(_)
                );
                let mut labels = vec!["first".into()];
                if !choose {
                    labels.push("second".into());
                }
                check(
                    mode,
                    &reused,
                    if choose { value } else { second },
                    labels,
                    &mut hash,
                    |ctx| {
                        eval_if_null_in(
                            ctx,
                            |selected| {
                                ctx.event("first", selected)?;
                                Ok(value.clone())
                            },
                            |selected| {
                                ctx.event("second", selected)?;
                                Ok(second.clone())
                            },
                        )
                    },
                );
                cases += 1;
            }
            let mut long = vec![Datum::Null; 2050];
            long.push(value.clone());
            for sequence in [
                vec![],
                vec![Datum::Null],
                vec![Datum::Null, value.clone(), other.clone()],
                vec![value.clone(), other.clone()],
                vec![Datum::Null, Datum::Null, value.clone()],
                long,
            ] {
                let mut labels = Vec::new();
                let mut selected = &Datum::Null;
                for (index, candidate) in sequence.iter().enumerate() {
                    labels.push(format!("arg:{index}"));
                    if let tidb_query_expr::NativeIfNullChoice::Done(v) =
                        tidb_query_expr::native_if_null_choose_first(
                            (!candidate.is_null()).then_some(candidate),
                        )
                    {
                        selected = v;
                        break;
                    }
                }
                check(mode, &reused, selected, labels, &mut hash, |ctx| {
                    eval_coalesce_in(ctx, sequence.len(), |index, selected| {
                        ctx.event(&format!("arg:{index}"), selected)?;
                        Ok(&sequence[index])
                    })
                });
                cases += 1;
            }
            for conditions in [
                vec![],
                vec![Some(true)],
                vec![Some(false)],
                vec![None, Some(true)],
                vec![Some(false), None, Some(false)],
                vec![None, Some(false), Some(true), Some(false)],
            ] {
                for has_else in [false, true] {
                    let mut labels = vec!["initialize".into()];
                    let mut selected = None;
                    for (index, condition) in conditions.iter().enumerate() {
                        labels.push(format!("condition:{index}"));
                        if tidb_query_expr::native_if_choose_branch(*condition)
                            == tidb_query_expr::NativeIfBranch::Then
                        {
                            selected = Some(index);
                            break;
                        }
                    }
                    if selected.is_none() && has_else {
                        selected = Some(conditions.len());
                    }
                    if let Some(index) = selected {
                        labels.push(format!("value:{index}"));
                    }
                    let expected =
                        selected.map_or(&Datum::Null, |index| &values[(i + index) % values.len()]);
                    check(mode, &reused, expected, labels, &mut hash, |ctx| {
                        eval_case_in(
                            ctx,
                            conditions.len(),
                            has_else,
                            |selected| {
                                ctx.event("initialize", selected)?;
                                Ok(selected.context_id())
                            },
                            |index, state, selected| {
                                assert_eq!(*state, 1307);
                                ctx.event(&format!("condition:{index}"), selected)?;
                                Ok(conditions[index])
                            },
                            |index, selected| {
                                ctx.event(&format!("value:{index}"), selected)?;
                                Ok(values[(i + index) % values.len()].clone())
                            },
                        )
                    });
                    cases += 1;
                }
            }
            for comparison in [None, Some(0), Some(1)] {
                let choose = tidb_query_expr::native_if_choose_branch(comparison.map(|v| v == 1))
                    == tidb_query_expr::NativeIfBranch::Then;
                check(
                    mode,
                    &reused,
                    if choose { &Datum::Null } else { value },
                    vec!["comparison".into()],
                    &mut hash,
                    |ctx| {
                        eval_null_if_in(ctx, value, |selected| {
                            ctx.event("comparison", selected)?;
                            Ok(comparison.map_or(Datum::Null, Datum::Int))
                        })
                    },
                );
                cases += 1;
            }
        }
    }
    assert_eq!(cases, 7680);
    assert_eq!(
        hash, 0x8addb0a0f0899f79,
        "original control representation/demand witness"
    );
}

fn run_failure(index: usize, ctx: &Context) -> Result<Datum, EvalError> {
    match index {
        0..=2 => eval_if_in(
            ctx,
            |s| {
                ctx.event("condition", s)?;
                Ok(Some(index != 2))
            },
            |s| {
                ctx.event("then", s)?;
                Ok(Datum::Int(1))
            },
            |s| {
                ctx.event("else", s)?;
                Ok(Datum::Int(2))
            },
        ),
        3..=4 => eval_if_null_in(
            ctx,
            |s| {
                ctx.event("first", s)?;
                Ok(Datum::Null)
            },
            |s| {
                ctx.event("second", s)?;
                Ok(Datum::Int(1))
            },
        ),
        5..=6 => eval_coalesce_in(ctx, 2, |i, s| {
            ctx.event(&format!("arg:{i}"), s)?;
            Ok(if i == 0 { Datum::Null } else { Datum::Int(1) })
        }),
        7..=11 => eval_case_in(
            ctx,
            if index == 9 { 2 } else { 1 },
            index == 11,
            |s| {
                ctx.event("initialize", s)?;
                Ok(())
            },
            |i, _, s| {
                ctx.event(&format!("condition:{i}"), s)?;
                Ok(Some(index == 10))
            },
            |i, s| {
                ctx.event(&format!("value:{i}"), s)?;
                Ok(Datum::Int(1))
            },
        ),
        12 => eval_null_if_in(ctx, &Datum::Int(7), |s| {
            ctx.event("comparison", s)?;
            Ok(Datum::Int(0))
        }),
        _ => unreachable!(),
    }
}
#[test]
fn original_control_source_failures_keep_identity_no_replay_and_warnings() {
    let labels: Vec<Vec<String>> = vec![
        vec!["condition"],
        vec!["condition", "then"],
        vec!["condition", "else"],
        vec!["first"],
        vec!["first", "second"],
        vec!["arg:0"],
        vec!["arg:0", "arg:1"],
        vec!["initialize"],
        vec!["initialize", "condition:0"],
        vec!["initialize", "condition:0", "condition:1"],
        vec!["initialize", "condition:0", "value:0"],
        vec!["initialize", "condition:0", "value:1"],
        vec!["comparison"],
    ]
    .into_iter()
    .map(|v| v.into_iter().map(str::to_owned).collect())
    .collect();
    let mut hash = 0xcbf29ce484222325;
    let mut cases = 0;
    for mode in 0..3 {
        let reused = Context::new(true);
        for resource in [false, true] {
            for (index, labels) in labels.iter().enumerate() {
                let error = if resource {
                    EvalError::ExpressionRuntimeFailure(ExpressionRuntimeFailure::from_local_eval(
                        LocalError::ResourceLimit("control source denied retained budget".into()),
                        None,
                    ))
                } else {
                    EvalError::Unsupported("control source SQL failure")
                };
                let fresh = Context::new(mode == 1);
                let ctx = if mode == 2 { &reused } else { &fresh };
                ctx.reset();
                *ctx.failure.borrow_mut() = Some((labels.last().unwrap().clone(), error.clone()));
                let actual = run_failure(index, ctx);
                assert_eq!(actual, Err(error), "source failure identity stage{index}");
                assert_events(ctx, labels);
                witness(&mut hash, &actual, ctx);
                cases += 1;
                // A later control call can still select normally after failure;
                // it must not inherit a stale head/report or poisoned selection.
                ctx.reset();
                assert_eq!(
                    eval_if_in(
                        ctx,
                        |_| Ok(Some(true)),
                        |_| Ok(Datum::Int(9)),
                        |_| panic!("dead branch after source failure")
                    ),
                    Ok(Datum::Int(9))
                );
            }
        }
    }
    assert_eq!(cases, 78);
    assert_eq!(
        hash, 0x4576b55725e08f88,
        "original source failure identity/trace witness"
    );
}
fn reject_worker_route(index: usize) {
    let mut ctx = Context::new(false);
    ctx.reject_cache = true;
    let actual = match index {
        0 => eval_if_in(
            &ctx,
            |_| Ok(Some(true)),
            |_| Ok(Datum::Int(1)),
            |_| panic!("dead IF branch"),
        ),
        1 => eval_if_null_in(&ctx, |_| Ok(Datum::Int(1)), |_| panic!("dead IFNULL rhs")),
        2 => eval_coalesce_in(&ctx, 1, |_, _| Ok(Datum::Int(1))),
        3 => eval_case_in(
            &ctx,
            1,
            false,
            |_| Ok(()),
            |_, _, _| Ok(Some(true)),
            |_, _| Ok(Datum::Int(1)),
        ),
        4 => eval_null_if_in(&ctx, &Datum::Int(1), |_| Ok(Datum::Int(0))),
        _ => unreachable!(),
    };
    assert_eq!(actual, Ok(Datum::Int(1)));
}
#[test]
fn control_worker_route_if() {
    reject_worker_route(0)
}
#[test]
fn control_worker_route_ifnull() {
    reject_worker_route(1)
}
#[test]
fn control_worker_route_coalesce() {
    reject_worker_route(2)
}
#[test]
fn control_worker_route_case() {
    reject_worker_route(3)
}
#[test]
fn control_worker_route_nullif() {
    reject_worker_route(4)
}

#[test]
fn selected_control_ownership_enforces_payload_budget_without_formatting_or_frames() {
    use super::control_value::{copy, owned};
    use crate::ExpressionRuntimeFailureClass as Class;
    let denied = |result: Result<Datum, EvalError>| {
        let Err(EvalError::ExpressionRuntimeFailure(failure)) = result else {
            panic!("selected ownership must retain terminal budget failure")
        };
        assert_eq!(failure.class(), Class::ResourceLimit);
        assert_eq!(failure.phase(), None);
    };
    for value in fixtures() {
        let expected = snapshot(&value);
        assert_eq!(snapshot(&owned(value.clone(), 8192).unwrap()), expected);
        assert_eq!(snapshot(&copy(&value, 8192).unwrap()), expected);
        denied(owned(value.clone(), usize::MAX));
        denied(copy(&value, usize::MAX));
    }
    for value in [
        Datum::Raw(vec![1, 2, 3]),
        Datum::Bytes(vec![255]),
        Datum::Decimal(Decimal::from_raw_parts(
            true,
            vec![255, 0, b'0'],
            u32::MAX,
            3,
        )),
        Datum::VectorFloat32(VectorFloat32::must_create(vec![1.0, 2.0])),
    ] {
        denied(owned(value.clone(), 0));
        denied(copy(&value, 0));
    }
    let wide = Datum::Decimal(
        Decimal::from_raw_parts(false, vec![b'1'], 1_000_000, 0).with_declared_shape(-1, i64::MAX),
    );
    // Only one actual coefficient byte is retained/copied, without formatting
    // a million fractional digits or allocating the old representation frame.
    assert_eq!(snapshot(&owned(wide.clone(), 1).unwrap()), snapshot(&wide));
    assert_eq!(snapshot(&copy(&wide, 2).unwrap()), snapshot(&wide));
    denied(owned(wide.clone(), 0));
    denied(copy(&wide, 1));
    assert_eq!(owned(Datum::Null, 0), Ok(Datum::Null));
    assert_eq!(copy(&Datum::Int(9), 0), Ok(Datum::Int(9)));
}
