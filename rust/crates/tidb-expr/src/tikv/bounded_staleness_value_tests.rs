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

//! Original endpoint, warning, SafeTS demand and raw identity observations.
use super::*;
use crate::{Columns, Datum, ErrorLevel};
use std::cell::RefCell;
use tidb_datatype::{CoreTime, Time, TimeType};

struct Context {
    cache: Option<ReadyValueCache>,
    reject: bool,
    safe: Option<Time>,
    level: ErrorLevel,
    events: RefCell<Vec<String>>,
}
impl Columns for Context {
    fn get(&self, _: &[String]) -> Option<Datum> {
        panic!("no invented endpoint sources")
    }
    fn ready_value_cache(&self) -> Option<&ReadyValueCache> {
        assert!(
            !self.reject,
            "bounded staleness attempted retired Ready lookup"
        );
        self.cache.as_ref()
    }
    fn bounded_staleness_safe_time(&self) -> Option<Time> {
        self.events.borrow_mut().push("safe".into());
        self.safe
    }
    fn truncate_level(&self) -> ErrorLevel {
        self.level
    }
    fn append_warning(&self, code: u16, text: &str) {
        self.events.borrow_mut().push(format!("warn:{code}:{text}"));
    }
    fn date_modes(&self) -> tidb_datatype::DateModes {
        panic!("no date modes")
    }
    fn time_zone(&self) -> tidb_datatype::SessionTimeZone {
        panic!("no time zone")
    }
}
fn times() -> Vec<Time> {
    use tidb_query_datatype::codec::mysql::Time as Shared;
    let mut values = Vec::new();
    for (y, m, d, h, i, s, u, low) in [
        (0, 0, 0, 0, 0, 0, 0, 0),
        (2020, 0, 1, 0, 0, 0, 0, 1),
        (2020, 1, 0, 0, 0, 0, 0, 2),
        (2020, 1, 1, 2, 3, 4, 123456, 3),
        (2022, 1, 1, 2, 3, 4, 654321, 4),
        (0, 13, 31, 31, 63, 63, 1048575, 5),
        (2021, 2, 30, 5, 6, 7, 123499, 15),
    ] {
        let raw = Shared::native_core_from_fields(y, m, d, h, i, s, u) | low;
        for kind in [TimeType::Date, TimeType::DateTime, TimeType::Timestamp] {
            for fsp in [0, 6] {
                values.push(Time::from_raw_parts(CoreTime::from_raw(raw), kind, fsp));
            }
        }
    }
    for kind in [TimeType::Date, TimeType::Timestamp] {
        let raw = Shared::native_core_from_fields(2020, 1, 1, 2, 3, 4, 123456) | 7;
        values.push(Time::from_raw_parts(CoreTime::from_raw(raw), kind, 255));
    }
    values
}
fn context(profile: usize, level: usize, safe: Option<Time>) -> Context {
    Context {
        cache: (profile != 0).then(ReadyValueCache::new),
        reject: false,
        safe,
        level: match level {
            0 => ErrorLevel::Warn,
            1 => ErrorLevel::Error,
            _ => ErrorLevel::Ignore,
        },
        events: RefCell::new(vec![]),
    }
}
fn observe(result: &Result<Datum, crate::EvalError>, ctx: &Context) -> String {
    let raw = result
        .as_ref()
        .ok()
        .map(|v| identity_value::encode(v).unwrap());
    format!("{result:?}|{raw:?}|{:?}\n", ctx.events.borrow())
}
#[test]
fn original_bounded_staleness_raw_endpoints_priority_and_safe_demand() {
    let mut inputs = crate::ops::arithmetic::tests::fixtures();
    let times = times();
    inputs.extend(times.iter().copied().map(Datum::Time));
    let safe: Vec<_> = std::iter::once(None)
        .chain(times.iter().step_by(6).copied().map(Some))
        .collect();
    let mut hash = 0xcbf29ce484222325u64;
    let mut count = 0;
    for profile in 0..3 {
        for level in 0..3 {
            for safe in &safe {
                let mut ctx = context(profile, level, *safe);
                for left in &inputs {
                    for right in &inputs {
                        if profile == 2 {
                            ctx = context(profile, level, *safe)
                        }
                        ctx.events.borrow_mut().clear();
                        let result =
                            eval_bounded_staleness_in(&ctx, &[left.clone(), right.clone()]);
                        for byte in observe(&result, &ctx).bytes() {
                            hash = (hash ^ u64::from(byte)).wrapping_mul(0x100000001b3)
                        }
                        count += 1;
                    }
                }
                for vals in [vec![], vec![Datum::Null], vec![Datum::Null; 3]] {
                    ctx.events.borrow_mut().clear();
                    let result = eval_bounded_staleness_in(&ctx, &vals);
                    for byte in observe(&result, &ctx).bytes() {
                        hash = (hash ^ u64::from(byte)).wrapping_mul(0x100000001b3)
                    }
                    count += 1;
                }
            }
        }
    }
    eprintln!("ORIGINAL bounded staleness count={count} hash={hash:016x}");
    assert_eq!(count, 468099);
    assert_eq!(hash, 0x58044483da932cf4);
}
#[test]
fn bounded_staleness_actual_values_never_lookup_retired_ready_workers() {
    let mut ctx = context(0, 0, None);
    ctx.reject = true;
    assert_eq!(
        eval_bounded_staleness_in(&ctx, &[Datum::Null, Datum::Int(1)]).unwrap(),
        Datum::Null
    );
}

#[test]
fn bounded_staleness_finite_work_precedes_warnings_and_safe_callbacks() {
    let mut ctx = context(0, 0, None);
    ctx.reject = true;
    let t = times();
    let invalid = Datum::Time(t[6]);
    let left = Datum::Time(t[18]);
    let right = Datum::Time(t[24]);
    for (vals, budget) in [
        ([invalid.clone(), right.clone()], 127),
        ([left.clone(), right.clone()], 0),
    ] {
        let error =
            super::bounded_staleness::bounded_staleness_bounded(&ctx, &vals, budget).unwrap_err();
        assert!(
            matches!(error,crate::EvalError::ExpressionRuntimeFailure(ref e) if matches!(e.local_error(),tidb_query_expr::local::LocalError::ResourceLimit(_)))
        );
        assert!(ctx.events.borrow().is_empty());
    }
    assert_eq!(
        super::bounded_staleness::bounded_staleness_bounded(
            &ctx,
            &[right.clone(), left.clone()],
            0
        )
        .unwrap(),
        Datum::Null
    );
    assert_eq!(
        super::bounded_staleness::bounded_staleness_bounded(&ctx, &[Datum::Null, right.clone()], 0)
            .unwrap(),
        Datum::Null
    );
    assert!(ctx.events.borrow().is_empty());
    assert_eq!(
        super::bounded_staleness::bounded_staleness_bounded(&ctx, &[invalid, right.clone()], 128)
            .unwrap(),
        Datum::Null
    );
    assert_eq!(ctx.events.borrow().len(), 1);
    assert!(ctx.events.borrow()[0].starts_with("warn:1292:Incorrect datetime value: '2020-00-01'"));
    ctx.events.borrow_mut().clear();
    let value =
        super::bounded_staleness::bounded_staleness_bounded(&ctx, &[left, right], 128).unwrap();
    let Datum::Time(value) = value else { panic!() };
    assert_eq!(value.core_time().raw(), t[18].core_time().raw());
    assert_eq!(value.kind(), TimeType::DateTime);
    assert_eq!(value.fsp(), 3);
    assert_eq!(&*ctx.events.borrow(), &["safe".to_string()]);
}
