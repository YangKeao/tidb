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

//! Immutable original text/core TIMESTAMPDIFF observations before retirement.
use super::*;
use crate::{Columns, Datum, EvalError};
use std::cell::Cell;
use tidb_datatype::CoreTime;
struct Context {
    cache: Option<ReadyValueCache>,
    reject: bool,
    calls: Cell<usize>,
}
impl Context {
    fn new(cache: bool) -> Self {
        Self {
            cache: cache.then(ReadyValueCache::new),
            reject: false,
            calls: Cell::new(0),
        }
    }
}
impl Columns for Context {
    fn get(&self, _: &[String]) -> Option<Datum> {
        panic!("timestamp diff must not invent sources")
    }
    fn ready_value_cache(&self) -> Option<&ReadyValueCache> {
        assert!(
            !self.reject,
            "timestamp diff attempted retired Ready lookup"
        );
        self.cache.as_ref()
    }
    fn time_zone(&self) -> tidb_datatype::SessionTimeZone {
        self.calls.set(self.calls.get() + 1);
        tidb_datatype::SessionTimeZone::utc()
    }
    fn date_modes(&self) -> tidb_datatype::DateModes {
        panic!("timestamp diff must not read date modes")
    }
    fn append_warning(&self, _: u16, _: &str) {
        panic!("timestamp diff must not append warnings")
    }
}
fn endpoints() -> Vec<Datum> {
    let mut v = crate::ops::arithmetic::tests::fixtures();
    v.extend(
        [
            "",
            "bad",
            "0000-00-00",
            "0000-01-01",
            "0000-03-01",
            "0001-01-01",
            "9999-12-31",
            "2020-01-31",
            "2020-02-29",
            "2021-02-29",
            "2020-01-15 12:00:00.123455",
            "2020-02-15 12:00:00.123456",
            "2020-02-15 12:00:00.123457",
            "2020-01-01 00:00:00.900000",
            "2020-01-01 00:00:00.100000",
            "2020-13-01",
            "2020-01-00",
            "12:34:56",
            "20200101",
            " 一😂 ",
        ]
        .map(Datum::new_string),
    );
    v
}
fn units() -> Vec<Datum> {
    let mut v = crate::ops::arithmetic::tests::fixtures();
    v.extend(
        [
            "YEAR",
            "QUARTER",
            "MONTH",
            "WEEK",
            "DAY",
            "HOUR",
            "MINUTE",
            "SECOND",
            "MICROSECOND",
            "day",
            "month",
            " DAY",
            "DAY ",
            "",
            "unknown",
            "一😂",
        ]
        .map(Datum::new_string),
    );
    v
}
fn observation(result: &Result<Datum, EvalError>, ctx: &Context) -> String {
    let identity = result
        .as_ref()
        .ok()
        .map(|v| identity_value::encode(v).unwrap());
    let cause = match result {
        Err(EvalError::ExpressionRuntimeFailure(e)) => {
            Some((e.local_error().to_string(), e.phase()))
        }
        _ => None,
    };
    format!("{result:?}|{identity:?}|{cause:?}|{}\n", ctx.calls.get())
}
#[test]
fn original_timestamp_diff_text_core_coercion_and_null_witness() {
    let mut hash = 0xcbf29ce484222325u64;
    let mut count = 0usize;
    let mut add = |s: String| {
        for b in s.bytes() {
            hash = (hash ^ u64::from(b)).wrapping_mul(0x100000001b3)
        }
        count += 1;
    };
    for profile in 0..3 {
        let mut ctx = Context::new(profile != 0);
        for unit in units() {
            for left in endpoints() {
                for right in endpoints() {
                    if profile == 2 {
                        ctx = Context::new(true)
                    }
                    ctx.calls.set(0);
                    let args = [unit.clone(), left.clone(), right.clone()];
                    let result = eval_timestamp_diff_in(&ctx, &args);
                    let independent =
                        super::timestamp_diff::timestamp_diff_bounded(&args, 64 << 20);
                    assert_eq!(
                        observation(&result, &ctx),
                        observation(&independent, &ctx),
                        "original text vs actual SDK values: {args:?}"
                    );
                    add(observation(&result, &ctx));
                }
            }
        }
        for args in [
            vec![],
            vec![Datum::Null],
            vec![Datum::Null, Datum::Null],
            vec![Datum::Null; 4],
        ] {
            add(observation(&eval_timestamp_diff_in(&ctx, &args), &ctx));
        }
        let raw = [
            None,
            Some(0),
            Some(1),
            Some(15),
            Some(CoreTime::from_date(0, 1, 1, 0, 0, 0, 0).raw()),
            Some(CoreTime::from_date(0, 3, 1, 0, 0, 0, 0).raw()),
            Some(CoreTime::from_date(2020, 1, 31, 0, 0, 0, 0).raw()),
            Some(CoreTime::from_date(2020, 2, 29, 0, 0, 0, 0).raw()),
            Some(CoreTime::from_date(9999, 12, 31, 0, 0, 0, 0).raw()),
        ];
        for unit in [
            b"YEAR".as_slice(),
            b"QUARTER",
            b"MONTH",
            b"WEEK",
            b"DAY",
            b"HOUR",
            b"MINUTE",
            b"SECOND",
            b"MICROSECOND",
            b"day",
            b" DAY",
            b"unknown",
            b"\xff",
        ] {
            for left in raw {
                for right in raw {
                    if profile == 2 {
                        ctx = Context::new(true)
                    }
                    ctx.calls.set(0);
                    let result = eval_legacy_timestamp_diff_in(
                        LegacyTimestampDiffArgs::Values {
                            unit: unit.to_vec(),
                            left: left.map(CoreTime::from_raw),
                            right: right.map(CoreTime::from_raw),
                        },
                        &ctx,
                    )
                    .map(|v| v.map_or(Datum::Null, Datum::Int));
                    let independent = Ok(tidb_query_expr::native_timestamp_diff_core_values(
                        unit, left, right,
                    )
                    .map_or(Datum::Null, Datum::Int));
                    assert_eq!(observation(&result,&ctx),observation(&independent,&ctx),"original core vs actual SDK bits unit={unit:?} left={left:?} right={right:?}");
                    add(observation(&result, &ctx));
                }
            }
        }
        for value in [None, Some(())] {
            add(observation(
                &eval_legacy_timestamp_diff_in(LegacyTimestampDiffArgs::NullWitness(value), &ctx)
                    .map(|v| v.map_or(Datum::Null, Datum::Int)),
                &ctx,
            ));
        }
        add(observation(&eval_timestamp_diff_null_in(&ctx), &ctx));
    }
    eprintln!("OLD TIMESTAMPDIFF count={count} hash={hash:016x}");
    assert_eq!(
        hash, 0xb4faecb69db19986,
        "capture original only before body switch"
    );
}
#[test]
fn timestamp_diff_actual_values_never_lookup_retired_ready_workers() {
    let mut ctx = Context::new(true);
    ctx.reject = true;
    assert_eq!(
        eval_timestamp_diff_in(
            &ctx,
            &[
                Datum::new_string("DAY"),
                Datum::new_string("2020-01-01"),
                Datum::new_string("2020-01-02")
            ]
        )
        .unwrap(),
        Datum::Int(1)
    );
    assert_eq!(eval_timestamp_diff_null_in(&ctx).unwrap(), Datum::Null);
    assert_eq!(
        eval_legacy_timestamp_diff_in(LegacyTimestampDiffArgs::NullWitness(None), &ctx).unwrap(),
        None
    );
    assert_eq!(
        eval_legacy_timestamp_diff_in(
            LegacyTimestampDiffArgs::Values {
                unit: b"DAY".to_vec(),
                left: Some(CoreTime::from_date(2020, 1, 1, 0, 0, 0, 0)),
                right: Some(CoreTime::from_date(2020, 1, 2, 0, 0, 0, 0))
            },
            &ctx
        )
        .unwrap(),
        Some(1)
    );
}

#[test]
fn timestamp_diff_year_unsigned_fallback_is_metered_before_context() {
    let fields = [
        tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::VarString),
        tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::Year),
        tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::Year),
    ];
    let ctx = Context::new(false);
    let mut values = [Datum::Null, Datum::UInt(u64::MAX), Datum::Null];
    let error =
        super::timestamp_diff::timestamp_diff_cast_arguments_bounded(&ctx, &mut values, &fields, 0)
            .unwrap_err();
    let EvalError::ExpressionRuntimeFailure(error) = error else {
        panic!("{error:?}")
    };
    assert!(matches!(
        error.local_error(),
        tidb_query_expr::local::LocalError::ResourceLimit(_)
    ));
    assert_eq!(error.phase(), None);
    assert_eq!(ctx.calls.get(), 0);
    // Genuine i64 YEAR construction and NULL passthrough do not allocate text
    // or read modes/zone, and therefore succeed even with zero work allowance.
    let mut values = [Datum::Null, Datum::UInt(69), Datum::Int(70)];
    super::timestamp_diff::timestamp_diff_cast_arguments_bounded(&ctx, &mut values, &fields, 0)
        .unwrap();
    let Datum::Time(left) = values[1] else {
        panic!()
    };
    let Datum::Time(right) = values[2] else {
        panic!()
    };
    assert_eq!(left.core_time().year(), 69);
    assert_eq!(right.core_time().year(), 70);
    for source in [Datum::UInt(i64::MAX as u64), Datum::Int(-1)] {
        let mut values = [Datum::Null, source, Datum::Null];
        assert_eq!(
            super::timestamp_diff::timestamp_diff_cast_arguments_bounded(
                &ctx,
                &mut values,
                &fields,
                0
            ),
            Err(EvalError::Unsupported(
                "a YEAR value outside the year range"
            ))
        );
    }
    assert_eq!(ctx.calls.get(), 0);
}
