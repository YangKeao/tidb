// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Original GROUPING and temporal literal observations before Ready retirement.
use crate::grouping::{GroupingMetadata, GroupingMode};
use crate::{Columns, Datum, EvalError};
use std::collections::BTreeSet;
use tidb_datatype::{DateModes, FieldType, SessionTimeZone, Time};
pub(crate) fn grouping_cases() -> Vec<GroupingMetadata> {
    let mut out = vec![];
    for mode in [
        GroupingMode::BitAnd,
        GroupingMode::NumericCmp,
        GroupingMode::NumericSet,
    ] {
        for n in [0, 1, 2, 3, 7, 32, 63, 64, 65, 127] {
            let marks = (0..n)
                .map(|i| {
                    if mode == GroupingMode::NumericSet {
                        match i % 4 {
                            0 => BTreeSet::new(),
                            1 => BTreeSet::from([0, u64::MAX]),
                            2 => (0..16).collect(),
                            _ => BTreeSet::from([i as u64, 1u64 << (i % 64)]),
                        }
                    } else {
                        BTreeSet::from([if i % 7 == 0 {
                            u64::MAX
                        } else {
                            1u64 << (i % 64)
                        }])
                    }
                })
                .collect();
            out.push(GroupingMetadata::new(mode, marks).unwrap());
        }
    }
    out
}
pub(crate) fn ids() -> Vec<Datum> {
    let mut v = super::duration_construction_value_tests::values();
    v.extend([
        Datum::UInt(0),
        Datum::UInt(1),
        Datum::UInt(1 << 63),
        Datum::UInt(u64::MAX),
        Datum::Int(-1),
        Datum::Int(i64::MIN),
    ]);
    v
}
pub(crate) fn original_grouping(
    value: &Datum,
    metadata: Option<&GroupingMetadata>,
    ctx: &dyn Columns,
) -> Result<Datum, EvalError> {
    let Some(id) = crate::arg_eval_type::eval_int(value)? else {
        return crate::grouping::eval_grouping_null_in(ctx);
    };
    let metadata = metadata.ok_or(EvalError::Unsupported("Meta data is not initialized"))?;
    crate::grouping::eval_grouping_in(id as u64, metadata, ctx)
}
pub(crate) fn zones() -> Vec<SessionTimeZone> {
    vec![
        SessionTimeZone::utc(),
        SessionTimeZone::Named(chrono_tz::America::Los_Angeles),
        SessionTimeZone::Named(chrono_tz::America::New_York),
        SessionTimeZone::Fixed {
            name: "raw+08".into(),
            offset_secs: 28800,
        },
        SessionTimeZone::Fixed {
            name: "raw+24".into(),
            offset_secs: 86400,
        },
        SessionTimeZone::Fixed {
            name: "rawMAX".into(),
            offset_secs: i32::MAX,
        },
    ]
}
pub(crate) fn texts() -> Vec<String> {
    let mut out = [
        "",
        "junk",
        "2024-01-01",
        "20240101",
        "240101",
        "2024-02-29",
        "2023-02-29",
        "2020-02-30",
        "0000-00-00",
        "2007-10-00",
        "2007-00-10",
        "2024-01-01 01:02:03",
        "2024-01-01 01:02",
        "2024-01-01 01",
        "2024-01-01 01:02:03.010",
        "2024-01-01 01:02:03.123456789",
        "2011-03-13 01:59:59.9999999",
        "2011-11-06 01:59:59.9999999",
        "2024-01-01 14:00:00+02:00",
        "2024-01-01 14:00:00+14:01",
        "2024-01-01 14:00:00+24:00",
        "2024-01-01 14:00:00-00:00",
        "2024-01-01 14:00:00Z",
        "9999-12-31 23:59:59.9999999",
        "0000-01-01 00:00:00",
        "10000-01-01",
        " 2024/1/2 ",
        "\t2024-01-02\n",
        "２０２４-01-01",
        "2024-01-01\0",
        "2024-01-01 01:02:03.",
        "-2024-01-01",
        "2024-1-1",
        "2024年1月1",
        "2024-01-01 01:02:03+01:٢",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect::<Vec<_>>();
    for n in [1, 6, 7, 32, 256] {
        out.push(format!("2024-01-01 01:02:03.{}", "9".repeat(n)));
        out.push(format!("{}2024-01-01", "0".repeat(n)));
    }
    out
}
pub(crate) fn modes(bits: usize) -> DateModes {
    DateModes {
        allow_invalid_dates: bits & 1 != 0,
        no_zero_date: bits & 2 != 0,
        no_zero_in_date: bits & 4 != 0,
    }
}
pub(crate) fn original_literal(
    timestamp: bool,
    text: &str,
    zone: &SessionTimeZone,
    modes: DateModes,
    ctx: &dyn Columns,
) -> Result<(Time, FieldType), EvalError> {
    if timestamp {
        crate::time_literal::timestamp_literal_in(text, zone, modes, ctx)
    } else {
        crate::time_literal::date_literal_in(text, zone, modes, ctx)
    }
}
pub(crate) fn literal_observation(
    f: impl FnOnce() -> Result<(Time, FieldType), EvalError>,
    ctx: &super::calendar_value_tests::Context,
) -> String {
    let out = std::panic::catch_unwind(std::panic::AssertUnwindSafe(f));
    let r = match out {
        Ok(Ok((t, f))) => format!(
            "VALUE/{:016X}/{:?}/{}/{f:?}",
            t.core_time().raw(),
            t.kind(),
            t.fsp()
        ),
        Ok(Err(e)) => format!("ERROR/{e:?}"),
        Err(p) => format!(
            "PANIC/{:?}",
            p.downcast_ref::<String>()
                .map(String::as_str)
                .or_else(|| p.downcast_ref::<&str>().copied())
        ),
    };
    format!("{r}/{:?}", ctx.events.borrow())
}
#[test]
fn metadata_grouping_original_observations() {
    let (mut count, mut hash) = (0usize, 0xcbf29ce484222325u64);
    let cases = grouping_cases();
    for profile in 0..3 {
        for metadata in std::iter::once(None).chain(cases.iter().map(Some)) {
            for value in ids() {
                let ctx = super::calendar_value_tests::context(profile, 0, 0);
                let result = super::duration_construction_value_tests::observation(
                    || original_grouping(&value, metadata, &ctx),
                    &ctx,
                );
                for b in result.bytes() {
                    hash = (hash ^ u64::from(b)).wrapping_mul(0x100000001b3);
                }
                count += 1;
            }
        }
    }
    eprintln!("METADATA GROUPING ORIGINAL {count} {hash:016X}");
    assert_eq!((count, hash), (10416, 0xA5CDD369E6A88378));
}
#[test]
fn metadata_literal_original_observations() {
    let (mut count, mut hash) = (0usize, 0xcbf29ce484222325u64);
    for profile in 0..3 {
        for timestamp in [false, true] {
            for zone in zones() {
                for bits in 0..8 {
                    for text in texts() {
                        let ctx = super::calendar_value_tests::context(profile, 0, 0);
                        let result = literal_observation(
                            || original_literal(timestamp, &text, &zone, modes(bits), &ctx),
                            &ctx,
                        );
                        for b in result.bytes() {
                            hash = (hash ^ u64::from(b)).wrapping_mul(0x100000001b3);
                        }
                        count += 1;
                    }
                }
            }
        }
    }
    eprintln!("METADATA LITERAL ORIGINAL {count} {hash:016X}");
    assert_eq!((count, hash), (12960, 0x80D479F72878FD19));
}
#[test]
fn metadata_never_requests_ready() {
    let mut failures = vec![];
    for metadata in grouping_cases()
        .iter()
        .take(1)
        .chain(grouping_cases().iter().skip(10).take(1))
        .chain(grouping_cases().iter().skip(20).take(1))
    {
        let mut ctx = super::calendar_value_tests::context(1, 0, 0);
        ctx.reject = true;
        if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            original_grouping(&Datum::Int(1), Some(metadata), &ctx)
        }))
        .is_err()
        {
            failures.push(format!("grouping {:?}", metadata.mode()));
        }
    }
    let mut ctx = super::calendar_value_tests::context(1, 0, 0);
    ctx.reject = true;
    if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        original_grouping(&Datum::Null, None, &ctx)
    }))
    .is_err()
    {
        failures.push("grouping NULL".into());
    }
    for timestamp in [false, true] {
        if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            original_literal(
                timestamp,
                "2024-01-01 01:02:03",
                &SessionTimeZone::utc(),
                modes(0),
                &ctx,
            )
        }))
        .is_err()
        {
            failures.push(format!("literal {timestamp}"));
        }
    }
    assert!(failures.is_empty(), "metadata Ready {failures:?}");
}

#[test]
fn metadata_grouping_shares_immutable_marks_and_preserves_hash_replacement() {
    use crate::{constant::Constant, expression::Expression, scalar_function::ScalarFunction};
    use tidb_datatype::FieldTypeCode;
    let ty = FieldType::new(FieldTypeCode::LongLong);
    let mut f = ScalarFunction::new(
        tidb_ast::CiString::new("grouping"),
        ty.clone(),
        vec![Expression::Constant(Constant::new(Datum::Int(1), ty))],
    );
    f.set_grouping_metadata(GroupingMode::BitAnd, vec![BTreeSet::from([1])])
        .unwrap();
    let hash = f.hash_code().to_vec();
    let h64 = f.hash64();
    let snapshot = f.shared_grouping_metadata().unwrap();
    let mut cloned = f.clone();
    assert!(std::sync::Arc::ptr_eq(
        &snapshot,
        &cloned.shared_grouping_metadata().unwrap()
    ));
    assert!(f.equals(&cloned));
    assert_eq!(cloned.hash_code(), hash);
    assert_eq!(cloned.hash64(), h64);
    cloned
        .set_grouping_metadata(GroupingMode::NumericCmp, vec![BTreeSet::from([1])])
        .unwrap();
    // Original Equals/Hash64 deliberately ignore planner metadata; HashCode
    // includes GROUPING marks. Preserve both distinct public contracts.
    assert!(f.equals(&cloned));
    assert_eq!(cloned.hash64(), h64);
    assert_ne!(cloned.hash_code(), hash);
    assert_eq!(f.hash_code(), hash);
    assert_eq!(snapshot.mode(), GroupingMode::BitAnd);
    assert!(cloned
        .set_grouping_metadata(GroupingMode::BitAnd, vec![BTreeSet::new()])
        .is_err());
    assert!(cloned.grouping_metadata().is_err());
    assert!(cloned.shared_grouping_metadata().is_none());
    assert_eq!(f.hash_code(), hash);
}
#[test]
fn metadata_literal_rewriter_captures_zone_modes_once_and_never_reads_execution_columns() {
    use crate::rewriter::{rewrite_expr_resolved, ColumnResolver};
    use std::cell::RefCell;
    struct R {
        events: RefCell<Vec<&'static str>>,
    }
    impl Columns for R {
        fn get(&self, _: &[String]) -> Option<Datum> {
            panic!("literal read execution column")
        }
        fn ready_value_cache(&self) -> Option<&crate::ReadyValueCache> {
            panic!("literal entered Ready")
        }
        fn time_zone(&self) -> SessionTimeZone {
            panic!("literal refetched execution zone")
        }
        fn date_modes(&self) -> DateModes {
            panic!("literal refetched execution modes")
        }
    }
    impl ColumnResolver for R {
        fn resolve(&self, _: &[String]) -> Option<(usize, FieldType, i64)> {
            None
        }
        fn time_zone(&self) -> SessionTimeZone {
            self.events.borrow_mut().push("zone");
            SessionTimeZone::Named(chrono_tz::America::Los_Angeles)
        }
        fn date_modes(&self) -> DateModes {
            self.events.borrow_mut().push("modes");
            modes(0)
        }
        fn literal_execution_context(&self) -> Option<&dyn Columns> {
            self.events.borrow_mut().push("execution");
            Some(self)
        }
    }
    for timestamp in [false, true] {
        let r = R {
            events: RefCell::new(vec![]),
        };
        let expr = tidb_ast::Expr::Cast(tidb_ast::CastExpr {
            expr: Box::new(tidb_ast::Expr::String(
                if timestamp {
                    "2011-03-13 01:59:59.9999999"
                } else {
                    "2024-02-29"
                }
                .into(),
            )),
            cast_type: if timestamp {
                tidb_ast::CastType::DateTime { fsp: None }
            } else {
                tidb_ast::CastType::Date
            },
            style: if timestamp {
                tidb_ast::CastStyle::TimestampLiteral
            } else {
                tidb_ast::CastStyle::DateLiteral
            },
            array: false,
        });
        let folded = rewrite_expr_resolved(&expr, &r).unwrap();
        assert_eq!(*r.events.borrow(), vec!["zone", "modes", "execution"]);
        let field = folded.static_type().unwrap();
        assert_eq!(field.decimal(), if timestamp { 6 } else { 0 });
        let crate::expression::Expression::Constant(c) = folded else {
            panic!()
        };
        let Datum::Time(t) = c.value else { panic!() };
        assert_eq!(
            t.to_string(),
            if timestamp {
                "2011-03-13 03:00:00.000000"
            } else {
                "2024-02-29"
            }
        );
    }
}

#[test]
fn metadata_grouping_empty_sets_do_not_pay_maximum_tree_search_work() {
    let metadata =
        GroupingMetadata::new(GroupingMode::NumericSet, vec![BTreeSet::new(); 3000]).unwrap();
    assert_eq!(metadata.eval(1), u64::MAX);
    assert_eq!(
        super::grouping_value::eval(&Datum::UInt(1), Some(&metadata), 500000),
        Ok(Datum::UInt(u64::MAX))
    );
}
