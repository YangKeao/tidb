// Copyright 2026 PingCAP, Inc.
// Licensed under the Apache License, Version 2.0.
//! Original EXTRACT source selection, actual casts, mode demand and composites.
use super::extract::eval_extract_values_bounded;
use super::*;
use crate::{Columns, Datum, ErrorLevel, EvalError};
use std::cell::{Cell, RefCell};
use tidb_datatype::{FieldType, FieldTypeCode, SessionTimeZone};
pub(crate) struct Context {
    cache: Option<ReadyValueCache>,
    pub(crate) reject: bool,
    pub(crate) enabled: Cell<bool>,
    seed: usize,
    level: Cell<usize>,
    modes: Cell<usize>,
    zones: Cell<usize>,
    pub(crate) events: RefCell<Vec<String>>,
}
impl Columns for Context {
    fn get(&self, _: &[String]) -> Option<Datum> {
        panic!("no fabricated EXTRACT sources")
    }
    fn ready_value_cache(&self) -> Option<&ReadyValueCache> {
        assert!(!self.reject, "EXTRACT attempted retired Ready lookup");
        self.cache.as_ref()
    }
    fn enable_vectorized_expression(&self) -> bool {
        self.enabled.get()
    }
    fn context_id(&self) -> u64 {
        8501
    }
    fn date_modes(&self) -> tidb_datatype::DateModes {
        let n = self.modes.get();
        self.modes.set(n + 1);
        let b = (self.seed + n) % 8;
        self.events.borrow_mut().push(format!("modes:{n}:{b}"));
        tidb_datatype::DateModes {
            no_zero_date: b & 1 != 0,
            no_zero_in_date: b & 2 != 0,
            allow_invalid_dates: b & 4 != 0,
        }
    }
    fn time_zone(&self) -> SessionTimeZone {
        let n = self.zones.get();
        self.zones.set(n + 1);
        self.events.borrow_mut().push(format!("zone:{n}"));
        match n % 3 {
            0 => SessionTimeZone::utc(),
            1 => SessionTimeZone::Named(chrono_tz::America::Los_Angeles),
            _ => SessionTimeZone::Fixed {
                name: "+08".into(),
                offset_secs: 28800,
            },
        }
    }
    fn now(&self) -> Option<(i64, u32, i32)> {
        self.events.borrow_mut().push("now".into());
        (self.seed % 3 != 0).then_some((1_600_000_000, 123456789, 28800))
    }
    fn truncate_level(&self) -> ErrorLevel {
        let v = self.level.get();
        self.events.borrow_mut().push(format!("level:{v}"));
        match v {
            0 => ErrorLevel::Warn,
            1 => ErrorLevel::Error,
            _ => ErrorLevel::Ignore,
        }
    }
    fn append_warning(&self, code: u16, text: &str) {
        self.events.borrow_mut().push(format!("warn:{code}:{text}"))
    }
}
pub(crate) fn context(profile: usize, seed: usize, level: usize) -> Context {
    Context {
        cache: (profile != 0).then(ReadyValueCache::new),
        reject: false,
        enabled: Cell::new(crate::NoColumns.enable_vectorized_expression()),
        seed,
        level: Cell::new(level),
        modes: Cell::new(0),
        zones: Cell::new(0),
        events: RefCell::new(vec![]),
    }
}
impl Context {
    fn reset(&self, level: usize) {
        self.level.set(level);
        self.modes.set(0);
        self.zones.set(0);
        self.events.borrow_mut().clear();
    }
}
fn values() -> Vec<Datum> {
    let mut v = crate::ops::arithmetic::tests::fixtures();
    v.extend(
        [
            "2020-01-02 03:04:05.123456",
            "2020-02-31 03:04:05",
            "0000-00-00",
            "2020-00-01",
            "2011-03-13 02:30:00",
            "2011-11-06 01:30:00",
            "1:02",
            "-01:02:03.1234567",
            "838:59:59.1",
            "4294967295:00:00",
            "4294967295-01-02",
            "2020-01-02 nonsense",
            " 12 ",
            "一😂",
        ]
        .map(Datum::new_string),
    );
    v
}
fn sources() -> Vec<Option<FieldType>> {
    vec![
        None,
        Some(FieldType::new(FieldTypeCode::Date)),
        Some(FieldType::new(FieldTypeCode::Datetime).with_decimal(6)),
        Some(FieldType::new(FieldTypeCode::Timestamp).with_decimal(3)),
        Some(FieldType::new(FieldTypeCode::Duration).with_decimal(2)),
        Some(FieldType::new(FieldTypeCode::Year)),
        Some(FieldType::new(FieldTypeCode::Unknown(12)).with_decimal(6)),
        Some(FieldType::new(FieldTypeCode::LongLong).with_unsigned(true)),
        Some(FieldType::new(FieldTypeCode::Double).with_decimal(6)),
        Some(FieldType::new(FieldTypeCode::Varchar)),
        Some(FieldType::new(FieldTypeCode::Json)),
        Some(FieldType::new(FieldTypeCode::Null)),
    ]
}
fn units() -> [&'static str; 25] {
    [
        "MICROSECOND",
        "SECOND",
        "MINUTE",
        "HOUR",
        "DAY",
        "WEEK",
        "MONTH",
        "QUARTER",
        "YEAR",
        "YEAR_MONTH",
        "DAY_HOUR",
        "DAY_MINUTE",
        "DAY_SECOND",
        "DAY_MICROSECOND",
        "HOUR_MINUTE",
        "HOUR_SECOND",
        "HOUR_MICROSECOND",
        "MINUTE_SECOND",
        "MINUTE_MICROSECOND",
        "SECOND_MICROSECOND",
        "day_second",
        "hOuR",
        "HOUR ",
        "一😂",
        "",
    ]
}
fn observe(result: &Result<Datum, EvalError>, ctx: &Context) -> String {
    let raw = result
        .as_ref()
        .ok()
        .map(|v| identity_value::encode(v).unwrap());
    format!("{result:?}|{raw:?}|{:?}\n", ctx.events.borrow())
}
fn scalar(
    ctx: &Context,
    unit: &Datum,
    value: &Datum,
    source: &FieldType,
) -> Result<Datum, EvalError> {
    use crate::expression::{Constant, Expression, ScalarFunction};
    let root = Expression::ScalarFunction(ScalarFunction::new(
        tidb_ast::CiString::new("extract"),
        FieldType::new(FieldTypeCode::LongLong),
        vec![
            Expression::Constant(Constant::new(
                unit.clone(),
                FieldType::new(FieldTypeCode::Varchar),
            )),
            Expression::Constant(Constant::new(value.clone(), source.clone())),
        ],
    ));
    let row = tidb_chunk::mutrow::MutRow::from_datums(&[]);
    root.eval(ctx, row.to_row())
}
#[test]
fn original_extract_selection_casts_mixed_modes_composite_and_unit_normalization() {
    let values = values();
    let sources = sources();
    let mut count = 0;
    let mut hash = 0xcbf29ce484222325u64;
    let mut record = |result: Result<Datum, EvalError>, ctx: &Context| {
        for b in observe(&result, ctx).bytes() {
            hash = (hash ^ u64::from(b)).wrapping_mul(0x100000001b3);
        }
        count += 1;
    };
    for profile in 0..3 {
        for seed in 0..8 {
            for level in 0..3 {
                let mut ctx = context(profile, seed, level);
                for unit in units() {
                    for value in &values {
                        for source in &sources {
                            if profile == 2 {
                                ctx = context(profile, seed, level)
                            }
                            ctx.reset(level);
                            record(eval_extract_in(&ctx, unit, value, source.as_ref()), &ctx);
                        }
                        if profile == 2 {
                            ctx = context(profile, seed, level)
                        }
                        ctx.reset(level);
                        record(
                            eval_extract_composite_in(&ctx, unit, std::slice::from_ref(value)),
                            &ctx,
                        );
                    }
                    for arity in [0, 2] {
                        if profile == 2 {
                            ctx = context(profile, seed, level)
                        }
                        ctx.reset(level);
                        record(
                            eval_extract_composite_in(&ctx, unit, &vec![Datum::Null; arity]),
                            &ctx,
                        );
                    }
                }
                if profile == 2 {
                    ctx = context(profile, seed, level)
                }
                ctx.reset(level);
                record(
                    eval_extract_scalar_values_bounded(&ctx, &Datum::Null, &Datum::Null, None, 0),
                    &ctx,
                );
                for unit in &values[..32] {
                    for value in &values[32..38] {
                        for source in [
                            FieldType::new(FieldTypeCode::Varchar),
                            FieldType::new(FieldTypeCode::Unknown(12)),
                        ] {
                            if profile == 2 {
                                ctx = context(profile, seed, level)
                            }
                            ctx.reset(level);
                            record(scalar(&ctx, unit, value, &source), &ctx);
                        }
                    }
                }
            }
        }
    }
    assert_eq!(count, 1107720);
    assert_eq!(hash, 0x9ec51a59f6c44c40);
}

#[test]
fn extract_all_actual_routes_never_lookup_ready_workers() {
    let mut ctx = context(0, 4, 0);
    ctx.reject = true;
    let mut panicked = vec![];
    for case in 0..5 {
        let run = || match case {
            0 => eval_extract_in(&ctx, "DAY", &Datum::new_string("2020-01-02"), None),
            1 => eval_extract_in(&ctx, "HOUR", &Datum::new_string("1:02:03"), None),
            2 => eval_extract_in(
                &ctx,
                "DAY_SECOND",
                &Datum::new_string("2020-02-31 03:04:05"),
                None,
            ),
            3 => eval_extract_composite_in(&ctx, "DAY_SECOND", &[Datum::new_string("1:02")]),
            _ => eval_extract_scalar_values_bounded(&ctx, &Datum::Null, &Datum::Null, None, 0),
        };
        if std::panic::catch_unwind(std::panic::AssertUnwindSafe(run)).is_err() {
            panicked.push(case);
        }
    }
    assert!(
        panicked.is_empty(),
        "retired EXTRACT Ready lookups in {panicked:?}"
    );
}

#[test]
fn extract_actual_budget_precedes_callbacks_and_preserves_suppression_and_errors() {
    let mut ctx = context(0, 0, 0);
    ctx.reject = true;
    let resource = |v: Result<Datum, EvalError>| {
        assert!(
            matches!(v, Err(EvalError::ExpressionRuntimeFailure(_))),
            "{v:?}"
        )
    };
    for maximum in [0, 128, 8192] {
        for (unit, source) in [
            ("DAY_SECOND", None),
            ("DAY", Some(FieldType::new(FieldTypeCode::Datetime))),
            ("HOUR", None),
        ] {
            ctx.reset(0);
            resource(eval_extract_values_bounded(
                &ctx,
                unit,
                &Datum::Bytes(vec![b'1'; 16384]),
                source.as_ref(),
                maximum,
            ));
            assert!(ctx.events.borrow().is_empty());
        }
        ctx.reset(0);
        resource(eval_extract_composite_values_bounded(
            &ctx,
            "DAY_SECOND",
            &[Datum::Bytes(vec![b'1'; 16384])],
            maximum,
        ));
        assert!(ctx.events.borrow().is_empty());
        resource(eval_extract_scalar_values_bounded(
            &ctx,
            &Datum::Bytes(vec![b'1'; 16384]),
            &Datum::Null,
            None,
            maximum,
        ));
        assert!(ctx.events.borrow().is_empty());
    }
    // NULL unit suppresses coercion of the already evaluated value, not its source.
    assert_eq!(
        eval_extract_scalar_values_bounded(&ctx, &Datum::Null, &Datum::Bytes(vec![0xff]), None, 0)
            .unwrap(),
        Datum::Null
    );
    assert_eq!(
        eval_extract_composite_values_bounded(&ctx, &"X".repeat(16384), &[Datum::Null], 0).unwrap(),
        Datum::Null
    );
    assert!(ctx.events.borrow().is_empty());
    let year = FieldType::new(FieldTypeCode::Year);
    ctx.reset(0);
    let result = eval_extract_values_bounded(&ctx, "DAY", &Datum::Int(2020), Some(&year), 8192);
    assert!(!matches!(
        result,
        Err(EvalError::ExpressionRuntimeFailure(_))
    ));
    ctx.reset(0);
    resource(eval_extract_values_bounded(
        &ctx,
        "DAY",
        &Datum::UInt(u64::MAX),
        Some(&year),
        128,
    ));
    assert!(ctx.events.borrow().is_empty());
    // SQL duration passthrough never formats the raw duration's huge FSP.
    ctx.reset(0);
    assert_eq!(
        eval_extract_values_bounded(
            &ctx,
            "HOUR",
            &Datum::new_duration(tidb_datatype::MySqlDuration::from_raw_parts(
                3_600_000_000_000,
                i64::MAX
            )),
            None,
            8192
        )
        .unwrap(),
        Datum::Int(1)
    );
    assert!(ctx.events.borrow().is_empty());
    for level in 0..3 {
        ctx.reset(level);
        let result =
            eval_extract_values_bounded(&ctx, "DAY_SECOND", &Datum::new_string("1x"), None, 8192);
        assert!(matches!(result, Err(EvalError::Conversion(_))));
        assert_eq!(ctx.events.borrow().len(), 1); // first mode only, no truncation callback
        ctx.reset(level);
        assert_eq!(
            eval_extract_composite_values_bounded(
                &ctx,
                "DAY_SECOND",
                &[Datum::new_string("bad")],
                8192
            )
            .unwrap(),
            Datum::Null
        );
        assert_eq!(ctx.events.borrow().len(), 1);
        assert!(ctx.events.borrow()[0].starts_with("warn:1292:"));
    }
    // Invalid UTF-8 in a unit is lossy normalized and reaches the original invalid-unit error.
    ctx.reset(0);
    let time = Datum::Time(
        tidb_datatype::parse_datetime("2020-01-02", &chrono_tz::UTC, true, true)
            .unwrap()
            .time,
    );
    assert!(matches!(
        eval_extract_scalar_values_bounded(&ctx, &Datum::Bytes(vec![0xff]), &time, None, 8192),
        Err(EvalError::Conversion(_))
    ));
    assert!(ctx.events.borrow().is_empty());
}
