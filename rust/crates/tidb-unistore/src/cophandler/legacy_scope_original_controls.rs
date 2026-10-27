// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Actual old legacy scope behavior with absent/retired cache and Shared trees.
use super::*;
use std::{
    cell::RefCell,
    io::Write,
    panic::{catch_unwind, AssertUnwindSafe},
};
use tidb_datatype::{Datum, Decimal, FieldType, FieldTypeCode};
use tidb_expr::Columns;

pub(super) struct ScopeContext {
    cache: tidb_expr::ReadyValueCache,
    events: RefCell<Vec<String>>,
    cache_mode: usize,
    failure: usize,
    text: bool,
}

impl Columns for ScopeContext {
    fn get(&self, path: &[String]) -> Option<Datum> {
        self.events.borrow_mut().push(format!("get:{path:?}"));
        None
    }
    fn ready_value_cache(&self) -> Option<&tidb_expr::ReadyValueCache> {
        self.events.borrow_mut().push("cache".into());
        match self.cache_mode {
            0 => Some(&self.cache),
            1 => None,
            2 => panic!("actual retired legacy cache capability"),
            _ => unreachable!(),
        }
    }
    fn param_value(&self, order: usize) -> Result<Datum, tidb_expr::EvalError> {
        self.events.borrow_mut().push(format!("param:{order}"));
        match (self.failure, order) {
            (1, 0) => Ok(Datum::Null),
            (2, 0) => Err(tidb_expr::EvalError::Unsupported(
                "actual scope parameter SQL failure",
            )),
            (3, 0) => panic!("actual scope parameter panic"),
            _ => Ok(if self.text {
                Datum::new_string("éx")
            } else {
                Datum::Int(if order == 0 { 5 } else { 2 })
            }),
        }
    }
    fn append_warning(&self, code: u16, message: &str) {
        self.events
            .borrow_mut()
            .push(format!("warn:{code}:{message}"));
    }
    fn handle_truncate(&self, message: &str) -> Result<(), tidb_expr::EvalError> {
        self.events.borrow_mut().push(format!("truncate:{message}"));
        Err(tidb_expr::EvalError::Unsupported("actual scope truncation"))
    }
    fn time_zone(&self) -> tidb_datatype::SessionTimeZone {
        self.events.borrow_mut().push("zone".into());
        tidb_datatype::SessionTimeZone::utc()
    }
    fn date_modes(&self) -> tidb_datatype::DateModes {
        self.events.borrow_mut().push("date-modes".into());
        tidb_datatype::DateModes::default()
    }
}
fn field(text: bool) -> FieldType {
    if text {
        FieldType::new(FieldTypeCode::Varchar).with_flen(128)
    } else {
        FieldType::new(FieldTypeCode::LongLong)
    }
}
fn parameter(order: i64, text: bool) -> tidb_expr::expression::Expression {
    let mut c = tidb_expr::constant::Constant::new(
        if text {
            Datum::new_string("planning")
        } else {
            Datum::Int(7)
        },
        field(text),
    );
    c.param_marker = Some(tidb_expr::constant::ParamMarker { order });
    tidb_expr::expression::Expression::Constant(c)
}
fn function(
    name: &str,
    text: bool,
    args: Vec<tidb_expr::expression::Expression>,
) -> tidb_expr::expression::Expression {
    tidb_expr::expression::Expression::ScalarFunction(
        tidb_expr::scalar_function::ScalarFunction::new(
            tidb_ast::CiString::new(name),
            field(text),
            args,
        ),
    )
}
fn shared(expr: tidb_expr::expression::Expression) -> SimpleExpr {
    SimpleExpr::Shared(Arc::new(SharedExpression {
        expression: Arc::new(expr),
        context: Arc::new(RequestEvalContext::new(
            tidb_datatype::SessionTimeZone::utc(),
            4,
            0,
        )),
    }))
}
pub(super) fn roots(text: bool) -> Vec<(String, SimpleExpr)> {
    let mut deferred = tidb_expr::constant::Constant::new(Datum::Null, field(text));
    deferred.deferred_expr = Some(Box::new(parameter(0, text)));
    let mut column = tidb_expr::column::Column::new(1, field(false));
    column.index = 99;
    let mut deep = parameter(0, false);
    for _ in 0..1100 {
        deep = function("abs", false, vec![deep]);
    }
    let sources = vec![
        ("parameter", shared(parameter(0, text))),
        (
            "deferred",
            shared(tidb_expr::expression::Expression::Constant(deferred)),
        ),
        (
            "native-plus",
            shared(function(
                "plus",
                false,
                vec![parameter(0, false), parameter(1, false)],
            )),
        ),
        (
            "native-if",
            shared(function(
                "if",
                false,
                vec![
                    tidb_expr::expression::Expression::Constant(
                        tidb_expr::constant::Constant::new(Datum::Int(0), field(false)),
                    ),
                    parameter(0, false),
                    parameter(1, false),
                ],
            )),
        ),
        (
            "native-column-refusal",
            shared(tidb_expr::expression::Expression::Column(column)),
        ),
        ("native-resource-refusal", shared(deep)),
    ];
    let mut out = vec![
        ("null".into(), SimpleExpr::Null),
        ("int-leaf".into(), SimpleExpr::Int(7)),
        (
            "decimal-leaf".into(),
            SimpleExpr::Decimal(Decimal::parse_mysql("1.25").0),
        ),
        ("column-missing".into(), SimpleExpr::Column(99)),
        (
            "decimal-plus".into(),
            SimpleExpr::Func(
                SimpleSig::PlusDecimal,
                vec![
                    SimpleExpr::Decimal(Decimal::parse_mysql("1.25").0),
                    SimpleExpr::Decimal(Decimal::parse_mysql("2.5").0),
                ],
            ),
        ),
        (
            "time-cast".into(),
            SimpleExpr::Func(
                SimpleSig::CastStringAsTime,
                vec![SimpleExpr::Bytes(b"2020-01-31".to_vec())],
            ),
        ),
        (
            "date-add".into(),
            SimpleExpr::Func(
                SimpleSig::AddSubDate {
                    subtract: false,
                    date: DateArithArg::String,
                    interval: IntervalArg::Int,
                    datetime_result: false,
                },
                vec![
                    SimpleExpr::Bytes(b"2020-01-31".to_vec()),
                    SimpleExpr::Int(1),
                    SimpleExpr::Bytes(b"MONTH".to_vec()),
                ],
            ),
        ),
    ];
    for (name, source) in sources {
        out.push((name.into(), source.clone()));
        for sig in [
            SimpleSig::PlusInt,
            SimpleSig::IntDivideInt,
            SimpleSig::LogicalAnd,
            SimpleSig::InInt,
        ] {
            out.push((
                format!("{sig:?}/{name}"),
                SimpleExpr::Func(sig, vec![source.clone(), shared(parameter(1, false))]),
            ));
        }
        out.push((
            format!("format/{name}"),
            SimpleExpr::Func(
                SimpleSig::FromUnixTime2Arg,
                vec![SimpleExpr::Decimal(Decimal::parse_mysql("1.25").0), source],
            ),
        ));
    }
    out
}
fn observed(e: &LegacyEvaluator<'_>, root: &SimpleExpr, view: usize) -> String {
    match catch_unwind(AssertUnwindSafe(|| match view {
        0 => format!("{:?}", e.eval_expr_original(root)),
        1 => format!("{:?}", e.eval_decimal_original(Some(root))),
        2 => format!("{:?}", e.eval_json_original(Some(root))),
        3 => format!(
            "{:?}",
            e.eval_real_original(Some(root))
                .map(|v| v.map(f64::to_bits))
        ),
        4 => format!("{:?}", e.eval_duration_original(Some(root))),
        5 => format!("{:?}", e.eval_time_original(Some(root))),
        6 => format!("{:?}", e.eval_bytes_original(Some(root))),
        7 => format!("{:?}", e.eval_datum_original(root)),
        _ => unreachable!(),
    })) {
        Ok(value) => value,
        Err(panic) => format!(
            "panic:{:?}",
            panic
                .downcast_ref::<String>()
                .map(String::as_str)
                .or_else(|| panic.downcast_ref::<&str>().copied())
                .unwrap_or("non-string panic")
        ),
    }
}
fn scope_records(mut observe: impl FnMut(&str)) -> usize {
    let mut count = 0;
    let row = [Datum::Int(9)];
    let zone = tidb_datatype::SessionTimeZone::Named(chrono_tz::America::New_York);
    for text in [false, true] {
        for (label, root) in roots(text) {
            for cache_mode in 0..3 {
                for failure in 0..4 {
                    for precision in [-1, 0, 4, 30] {
                        for view in 0..8 {
                            let context = ScopeContext {
                                cache: tidb_expr::ReadyValueCache::new(),
                                events: RefCell::new(vec![]),
                                cache_mode,
                                failure,
                                text,
                            };
                            let mut e = LegacyEvaluator::new(&row, precision, &zone);
                            e.raw_columns = &context;
                            e.shared_override = Some(&context);
                            for reuse in 0..2 {
                                let value = observed(&e, &root, view);
                                observe(&format!("{text}/{label}/{cache_mode}/{failure}/{precision}/{view}/{reuse}|{value}|{:?}",context.events.borrow()));
                                count += 1;
                            }
                        }
                    }
                }
            }
        }
    }
    count
}
fn digest(destination: Option<&std::path::Path>) -> (usize, u64) {
    let mut file = destination
        .map(|p| {
            std::fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(p)
                .unwrap()
        })
        .map(std::io::BufWriter::new);
    let mut hash = 0xcbf29ce484222325u64;
    let count = scope_records(|record| {
        for byte in record.bytes().chain(std::iter::once(b'\n')) {
            hash = (hash ^ u64::from(byte)).wrapping_mul(0x100000001b3);
        }
        if let Some(f) = file.as_mut() {
            writeln!(f, "{record}").unwrap();
        }
    });
    if let Some(f) = file.as_mut() {
        f.flush().unwrap();
    }
    (count, hash)
}

#[test]
fn retained_original_digest_is_unchanged() {
    assert_eq!(digest(None), (66048, 0x3F895141D852AA15));
}
