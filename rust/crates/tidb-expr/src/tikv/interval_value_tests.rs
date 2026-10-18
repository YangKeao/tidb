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

//! Actual old eager/lazy INTERVAL observations before transport replacement.
use super::*;
use crate::{Columns, Datum, EvalError};
use std::cell::{Cell, RefCell};
use tidb_datatype::{EvalType, FieldType, FieldTypeCode};
struct Context {
    cache: Option<ReadyValueCache>,
    reject_cache: bool,
    level: crate::ErrorLevel,
    reads: RefCell<Vec<usize>>,
    events: RefCell<Vec<String>>,
    warnings: RefCell<Vec<(u16, String)>>,
    failure: RefCell<Option<(usize, EvalError)>>,
}
impl Context {
    fn new(cache: bool, level: crate::ErrorLevel) -> Self {
        Self {
            cache: cache.then(ReadyValueCache::new),
            reject_cache: false,
            level,
            reads: RefCell::new(vec![]),
            events: RefCell::new(vec![]),
            warnings: RefCell::new(vec![]),
            failure: RefCell::new(None),
        }
    }
    fn reset(&self) {
        self.reads.borrow_mut().clear();
        self.events.borrow_mut().clear();
        self.warnings.borrow_mut().clear();
        self.failure.borrow_mut().take();
    }
    fn read(&self, index: usize, values: &[Datum]) -> Result<Datum, EvalError> {
        self.reads.borrow_mut().push(index);
        self.events.borrow_mut().push(format!("read:{index}"));
        self.append_warning(60006, &format!("read:{index}"));
        if let Some((at, error)) = self.failure.borrow().as_ref() {
            if *at == index {
                return Err(error.clone());
            }
        }
        Ok(values[index].clone())
    }
}
impl Columns for Context {
    fn get(&self, _: &[String]) -> Option<Datum> {
        panic!("INTERVAL must not invent source lookups")
    }
    fn ready_value_cache(&self) -> Option<&ReadyValueCache> {
        assert!(
            !self.reject_cache,
            "INTERVAL attempted retired Ready worker lookup"
        );
        self.cache.as_ref()
    }
    fn context_id(&self) -> u64 {
        8173
    }
    fn truncate_level(&self) -> crate::ErrorLevel {
        self.events
            .borrow_mut()
            .push(format!("truncate:{:?}", self.level));
        self.level
    }
    fn time_zone(&self) -> tidb_datatype::SessionTimeZone {
        self.events.borrow_mut().push("zone".into());
        tidb_datatype::SessionTimeZone::utc()
    }
    fn div_precision_increment(&self) -> u32 {
        self.events.borrow_mut().push("precision".into());
        4
    }
    fn append_warning(&self, code: u16, message: &str) {
        self.warnings.borrow_mut().push((code, message.to_owned()));
    }
}
fn witness(hash: &mut u64, value: &Result<Datum, EvalError>, ctx: &Context) {
    for byte in format!(
        "{value:?}|{:?}|{:?}\n",
        ctx.events.borrow(),
        ctx.warnings.borrow()
    )
    .bytes()
    {
        *hash = (*hash ^ u64::from(byte)).wrapping_mul(0x100000001b3);
    }
}
fn value_error(error: tidb_query_expr::NativeIntervalValueError) -> EvalError {
    match error {
        tidb_query_expr::NativeIntervalValueError::Arity { .. } => {
            EvalError::ExpressionRuntimeFailure(ExpressionRuntimeFailure::from_local_eval(
                LocalError::evaluated_argument_shape_mismatch(),
                Some(crate::ExpressionRuntimeFailurePhase::Invoke),
            ))
        }
        tidb_query_expr::NativeIntervalValueError::Invalid => {
            EvalError::ExpressionAdapterFailure(ExpressionAdapterFailure::from_scope(
                super::adapter_failure::ScopeFailureKind::Contract,
                "invalid native INTERVAL report",
            ))
        }
        tidb_query_expr::NativeIntervalValueError::ResourceLimit(reason) => {
            EvalError::ExpressionRuntimeFailure(ExpressionRuntimeFailure::from_local_eval(
                LocalError::ResourceLimit(reason.into()),
                None,
            ))
        }
    }
}
fn number(
    ctx: &Context,
    value: &Datum,
    field: Option<&FieldType>,
    cast: tidb_query_expr::NativeIntervalCast,
) -> Result<Option<tidb_query_expr::NativeIntervalNumber>, EvalError> {
    use tidb_query_expr::{NativeIntervalCast as C, NativeIntervalNumber as N};
    match cast {
        C::Real => {
            if value.is_null() {
                Ok(None)
            } else {
                crate::ops::to_f64_with_mysql_string(value, ctx)
                    .map(|value| Some(N::Real(value.to_bits())))
            }
        }
        C::Int => match crate::cast::cast_arg_as_int(value, field, ctx)? {
            Datum::Null => Ok(None),
            Datum::Int(value) => Ok(Some(N::Signed(value))),
            Datum::UInt(value) => Ok(Some(N::Unsigned(value))),
            _ => Err(value_error(
                tidb_query_expr::NativeIntervalValueError::Invalid,
            )),
        },
    }
}
fn metadata(field: &FieldType) -> tidb_query_expr::NativeIntervalFieldType {
    use tidb_query_expr::NativeIntervalEvalType as T;
    tidb_query_expr::NativeIntervalFieldType {
        eval_type: match field.eval_type() {
            EvalType::Int => T::Int,
            EvalType::Real => T::Real,
            EvalType::Decimal => T::Decimal,
            EvalType::String => T::String,
            EvalType::Datetime => T::Datetime,
            EvalType::Timestamp => T::Timestamp,
            EvalType::Duration => T::Duration,
            EvalType::Json => T::Json,
            EvalType::VectorFloat32 => T::VectorFloat32,
        },
        flags: field.flags(),
    }
}
fn direct_eager(ctx: &Context, values: &[Datum]) -> Result<Datum, EvalError> {
    use tidb_query_expr::{
        NativeIntervalEagerInput as V, NativeIntervalEagerStart as H, NativeIntervalValueStep as S,
    };
    let views = values
        .iter()
        .map(|value| match value {
            Datum::Null => V::Null,
            Datum::Int(value) => V::Int(*value),
            Datum::UInt(value) => V::UInt(*value),
            Datum::MinNotNull | Datum::MaxValue => V::Sentinel,
            _ => V::Other,
        })
        .collect::<Vec<_>>();
    match tidb_query_expr::native_interval_eager_values(
        &views,
        tidb_query_expr::local::ExecutionLimits::default().max_retained_bytes,
    )
    .map_err(value_error)?
    {
        H::Index(index) => Ok(Datum::Int(index)),
        H::Sentinels => Err(EvalError::Unsupported("range sentinel INTERVAL argument")),
        H::Real(mut state) => loop {
            match state.step() {
                S::Index(index) => break Ok(Datum::Int(index)),
                S::Probe { index, .. } => {
                    let value = crate::ops::to_f64_with_mysql_string(&values[index], ctx)?;
                    state.accept_real(value).map_err(value_error)?;
                }
            }
        },
    }
}
fn direct_lazy(
    ctx: &Context,
    types: &[Option<FieldType>],
    mut read: impl FnMut(usize) -> Result<Datum, EvalError>,
) -> Result<Datum, EvalError> {
    use tidb_query_expr::NativeIntervalValueStep as S;
    let mut state = tidb_query_expr::NativeIntervalSearch::new(
        types.iter().map(|field| field.as_ref().map(metadata)),
    )
    .map_err(value_error)?;
    loop {
        match state.step() {
            S::Index(index) => break Ok(Datum::Int(index)),
            S::Probe { index, cast } => {
                let value = read(index)?;
                let value = number(ctx, &value, types[index].as_ref(), cast)?;
                state.accept(value).map_err(value_error)?;
            }
        }
    }
}
fn independent(
    original: &Result<Datum, EvalError>,
    old: &Context,
    run: impl FnOnce(&Context) -> Result<Datum, EvalError>,
) {
    let next = Context::new(false, old.level);
    *next.failure.borrow_mut() = old.failure.borrow().clone();
    let actual = run(&next);
    assert_eq!(
        format!("{original:?}"),
        format!("{actual:?}"),
        "independent OLD argument programs vs actual typed SDK state"
    );
    if let (
        Err(EvalError::ExpressionRuntimeFailure(a)),
        Err(EvalError::ExpressionRuntimeFailure(b)),
    ) = (original, &actual)
    {
        assert_eq!(
            a.local_error().to_string(),
            b.local_error().to_string(),
            "original structured cause contents changed"
        );
        assert_eq!(a.phase(), b.phase());
    }
    assert_eq!(*old.events.borrow(), *next.events.borrow());
    assert_eq!(*old.warnings.borrow(), *next.warnings.borrow());
    if old.failure.borrow().is_some() {
        assert_eq!(original, &actual, "source capture identity changed");
    }
}

fn layouts(left: &Datum, right: &Datum) -> Vec<Vec<Datum>> {
    vec![
        vec![],
        vec![left.clone()],
        vec![left.clone(), right.clone()],
        vec![left.clone(), Datum::Null, right.clone()],
        vec![left.clone(), right.clone(), Datum::Null],
        vec![left.clone(), left.clone(), right.clone()],
        vec![
            left.clone(),
            Datum::Int(0),
            right.clone(),
            Datum::UInt(u64::MAX),
        ],
        {
            let mut long = vec![Datum::Null; 33];
            long[0] = left.clone();
            long[32] = right.clone();
            long
        },
    ]
}
#[test]
fn original_interval_eager_value_cast_context_and_precedence_witness() {
    let values = crate::ops::arithmetic::tests::fixtures();
    let mut hash = 0xcbf29ce484222325;
    let mut cases = 0;
    for level in [
        crate::ErrorLevel::Warn,
        crate::ErrorLevel::Error,
        crate::ErrorLevel::Ignore,
    ] {
        for mode in 0..3 {
            let reused = Context::new(true, level);
            for first in &values {
                for other in &values {
                    for input in layouts(first, other) {
                        let fresh = Context::new(mode == 1, level);
                        let ctx = if mode == 2 { &reused } else { &fresh };
                        ctx.reset();
                        let actual = eval_interval_in(ctx, &input);
                        assert!(ctx.reads.borrow().is_empty());
                        independent(&actual, ctx, |next| direct_eager(next, &input));
                        witness(&mut hash, &actual, ctx);
                        cases += 1;
                    }
                }
            }
        }
    }
    assert_eq!(cases, 73728);
    assert_eq!(
        hash, 0x319a9e8733ea8257,
        "OLD eager INTERVAL value/precedence/context"
    );
}
fn fields(mode: usize, count: usize) -> Vec<Option<FieldType>> {
    let int = || FieldType::new(FieldTypeCode::LongLong);
    match mode {
        0 => vec![None; count],
        1 => vec![Some(int()); count],
        2 => vec![Some(int().with_flags(1)); count],
        3 => vec![Some(int().with_flags(1).with_unsigned(true)); count],
        4 => vec![Some(FieldType::new(FieldTypeCode::Double).with_flags(1)); count],
        5 => vec![Some(FieldType::new(FieldTypeCode::Double)); count],
        6 => (0..count)
            .map(|i| {
                Some(if i % 2 == 0 {
                    int().with_flags(1)
                } else {
                    FieldType::new(FieldTypeCode::Varchar).with_flags(u32::MAX - 1)
                })
            })
            .collect(),
        7 => vec![Some(FieldType::new(FieldTypeCode::Json).with_flags(1)); count],
        _ => unreachable!(),
    }
}
#[test]
fn original_interval_lazy_value_read_cast_metadata_and_context_witness() {
    let values = crate::ops::arithmetic::tests::fixtures();
    let mut hash = 0xcbf29ce484222325;
    let mut cases = 0;
    for level in [
        crate::ErrorLevel::Warn,
        crate::ErrorLevel::Error,
        crate::ErrorLevel::Ignore,
    ] {
        for mode in 0..3 {
            let reused = Context::new(true, level);
            for profile in 0..8 {
                for first in &values {
                    for other in &values {
                        for input in [
                            vec![first.clone(), other.clone()],
                            vec![first.clone(), Datum::Null, other.clone()],
                            vec![
                                first.clone(),
                                Datum::Int(0),
                                other.clone(),
                                Datum::UInt(u64::MAX),
                            ],
                            vec![
                                first.clone(),
                                Datum::Int(100),
                                Datum::Null,
                                other.clone(),
                                Datum::Int(-1),
                            ],
                        ] {
                            let fresh = Context::new(mode == 1, level);
                            let ctx = if mode == 2 { &reused } else { &fresh };
                            ctx.reset();
                            let types = fields(profile, input.len());
                            let actual =
                                eval_interval_lazy_in(ctx, &types, |index| ctx.read(index, &input));
                            independent(&actual, ctx, |next| {
                                direct_lazy(next, &types, |i| next.read(i, &input))
                            });
                            witness(&mut hash, &actual, ctx);
                            cases += 1;
                        }
                    }
                }
            }
        }
    }
    assert_eq!(cases, 294912);
    assert_eq!(
        hash, 0x242c1ac24fae8178,
        "OLD lazy INTERVAL value/read/cast/context"
    );
}
#[test]
fn original_interval_lazy_source_failure_identity_skips_and_reuse_witness() {
    let mut hash = 0xcbf29ce484222325;
    let mut cases = 0;
    for mode in 0..3 {
        let reused = Context::new(true, crate::ErrorLevel::Warn);
        for profile in 0..8 {
            for resource in [false, true] {
                for first in [Datum::Null, Datum::Int(-1), Datum::Int(2), Datum::Int(100)] {
                    let input = vec![
                        first,
                        Datum::Int(0),
                        Datum::Int(1),
                        Datum::Int(3),
                        Datum::Int(5),
                    ];
                    let types = fields(profile, input.len());
                    let fresh = Context::new(mode == 1, crate::ErrorLevel::Warn);
                    let ctx = if mode == 2 { &reused } else { &fresh };
                    ctx.reset();
                    let expected = eval_interval_lazy_in(ctx, &types, |i| ctx.read(i, &input));
                    assert!(expected.is_ok());
                    let demanded = ctx.reads.borrow().clone();
                    for at in 0..input.len() {
                        ctx.reset();
                        let error = if resource {
                            EvalError::ExpressionRuntimeFailure(
                                ExpressionRuntimeFailure::from_local_eval(
                                    LocalError::ResourceLimit(
                                        "INTERVAL actual source denied retained budget".into(),
                                    ),
                                    None,
                                ),
                            )
                        } else {
                            EvalError::Unsupported("INTERVAL actual source SQL failure")
                        };
                        *ctx.failure.borrow_mut() = Some((at, error.clone()));
                        let actual = eval_interval_lazy_in(ctx, &types, |i| ctx.read(i, &input));
                        if let Some(position) = demanded.iter().position(|i| *i == at) {
                            assert_eq!(actual, Err(error));
                            assert_eq!(*ctx.reads.borrow(), demanded[..=position]);
                        } else {
                            assert_eq!(actual, expected);
                            assert_eq!(*ctx.reads.borrow(), demanded);
                        }
                        independent(&actual, ctx, |next| {
                            direct_lazy(next, &types, |i| next.read(i, &input))
                        });
                        witness(&mut hash, &actual, ctx);
                        cases += 1;
                        ctx.reset();
                        assert_eq!(
                            eval_interval_lazy_in(ctx, &types, |i| ctx.read(i, &input)),
                            expected
                        );
                    }
                }
            }
        }
    }
    assert_eq!(cases, 960);
    assert_eq!(
        hash, 0x2bb943c97c73bb21,
        "OLD lazy INTERVAL source identity/skips/reuse"
    );
}
fn reject_worker(lazy: bool, real: bool) {
    let mut ctx = Context::new(false, crate::ErrorLevel::Warn);
    ctx.reject_cache = true;
    let values = if real {
        vec![Datum::Real(1.0), Datum::Real(2.0)]
    } else {
        vec![Datum::Int(1), Datum::Int(2)]
    };
    let actual = if lazy {
        eval_interval_lazy_in(&ctx, &fields(2, 2), |i| Ok(values[i].clone()))
    } else {
        eval_interval_in(&ctx, &values)
    };
    assert_eq!(actual, Ok(Datum::Int(0)));
}
#[test]
fn interval_eager_integer_worker_route_must_be_retired() {
    reject_worker(false, false);
}
#[test]
fn interval_eager_real_worker_route_must_be_retired() {
    reject_worker(false, true);
}
#[test]
fn interval_lazy_worker_route_must_be_retired() {
    reject_worker(true, false);
}
