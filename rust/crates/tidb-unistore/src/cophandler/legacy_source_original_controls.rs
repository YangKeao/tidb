// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Independent genuine public SimpleExpr contracts before typed-tree retirement.
use super::*;
use std::{
    cell::RefCell,
    io::Write,
    panic::{catch_unwind, AssertUnwindSafe},
};
use tidb_datatype::{BinaryJSON, Datum, Decimal, MySqlDuration};

pub(super) fn signatures() -> Vec<SimpleSig> {
    use SimpleSig::*;
    let mut out = vec![
        PlusDecimal,
        MinusDecimal,
        MultiplyDecimal,
        ModDecimal,
        ModIntUnsignedUnsigned,
        ModIntUnsignedSigned,
        ModIntSignedUnsigned,
        ModIntSignedSigned,
        PlusInt,
        MinusInt,
        MultiplyInt,
        MultiplyIntUnsigned,
        PlusIntUnsignedUnsigned,
        PlusIntUnsignedSigned,
        PlusIntSignedUnsigned,
        PlusIntSignedSigned,
        MinusIntUnsignedUnsigned,
        MinusIntUnsignedSigned,
        MinusIntSignedUnsigned,
        MinusIntSignedSigned,
        MinusIntForcedUnsignedUnsigned,
        MinusIntForcedUnsignedSigned,
        MinusIntForcedSignedUnsigned,
        RoundReal,
        RoundInt,
        RoundDec,
        Pow,
        Acos,
        Asin,
        Atan1Arg,
        Atan2Args,
        Cos,
        Cot,
        Pi,
        Sin,
        CharLengthUtf8,
        CharLength,
        LowerUtf8,
        Lower,
        DateFormatSig,
        Conv,
        ModReal,
        TimestampDiff,
        UpperUtf8,
        Upper,
        Substring2ArgsUtf8,
        Substring3ArgsUtf8,
        Substring2Args,
        Substring3Args,
        DivideDecimal,
        LtReal,
        LeReal,
        GtReal,
        GeReal,
        EqReal,
        NeReal,
        IntDivideInt,
        IntDivideIntUnsignedUnsigned,
        IntDivideIntUnsignedSigned,
        IntDivideIntSignedSigned,
        IntDivideIntSignedUnsigned,
        IntDivideDecimal,
        PlusReal,
        MinusReal,
        MultiplyReal,
        DivideReal,
        LtInt,
        LeInt,
        GtInt,
        GeInt,
        EqInt,
        NeInt,
        LtDecimal,
        LeDecimal,
        GtDecimal,
        GeDecimal,
        EqDecimal,
        NeDecimal,
        LtTime,
        LeTime,
        GtTime,
        GeTime,
        EqTime,
        NeTime,
        LogicalAnd,
        LogicalOr,
        UnaryNot,
        IntIsNull,
        DecimalIsNull,
        DurationIsNull,
        RealIsNull,
        StringIsNull,
        TimeIsNull,
        JsonMemberOfSig,
        Date,
        Hour,
        Minute,
        Second,
        MicroSecond,
        Month,
        DateDiff,
        CastTimeAsDuration,
        WeekWithoutMode,
        VectorFloat32IsNull,
        CastIntAsInt,
        CastRealAsInt,
        CastDecimalAsInt,
        CastStringAsInt,
        CastStringAsReal,
        CastIntAsDecimal,
        CastRealAsDecimal,
        CastDecimalAsDecimal,
        CastStringAsDecimal,
        CastTimeAsDecimal,
        CastDurationAsDecimal,
        CastIntAsString,
        CastRealAsString,
        CastDecimalAsString,
        CastStringAsString,
        CastTimeAsString,
        CastDurationAsString,
        CastIntAsTime,
        CastRealAsTime,
        CastDecimalAsTime,
        CastStringAsTime,
        CastTimeAsTime,
        CastIntAsDuration,
        CastRealAsDuration,
        CastDecimalAsDuration,
        CastStringAsDuration,
        CastDurationAsDuration,
        CastIntAsJson,
        CastRealAsJson,
        CastDecimalAsJson,
        CastStringAsJson,
        CastTimeAsJson,
        CastDurationAsJson,
        CastJsonAsJson,
        CastJsonAsInt,
        CastJsonAsReal,
        CastJsonAsTime,
        CastJsonAsDuration,
        JsonReplaceSig,
        JsonArrayAppendSig,
        JsonMergePatchSig,
        UnixTimestampInt,
        UnixTimestampDec,
        FromUnixTime1Arg,
        FromUnixTime2Arg,
        CastIntAsReal,
        CastRealAsReal,
        CastDecimalAsReal,
        InInt,
    ];
    for collation in [46, 45, 63, 9999] {
        out.extend([
            LtString(collation),
            LeString(collation),
            GtString(collation),
            GeString(collation),
            EqString(collation),
            NeString(collation),
            InString(collation),
            Like(collation),
            RegexpLike(collation),
        ]);
    }
    for subtract in [false, true] {
        for date in [
            DateArithArg::String,
            DateArithArg::Int,
            DateArithArg::Real,
            DateArithArg::Decimal,
            DateArithArg::Datetime,
            DateArithArg::Duration,
        ] {
            for interval in [
                IntervalArg::String,
                IntervalArg::Int,
                IntervalArg::Real,
                IntervalArg::Decimal,
            ] {
                for datetime_result in [false, true] {
                    out.push(AddSubDate {
                        subtract,
                        date,
                        interval,
                        datetime_result,
                    });
                }
            }
        }
    }
    out
}
pub(super) fn time() -> tidb_datatype::Time {
    tidb_datatype::parse_datetime("2020-01-31 12:34:56.123456", &chrono_tz::UTC, true, false)
        .unwrap()
        .time
}
pub(super) fn rows() -> Vec<Vec<Datum>> {
    vec![
        vec![],
        vec![Datum::Null; 4],
        vec![Datum::Int(-7), Datum::Int(2), Datum::Int(1), Datum::Int(99)],
        vec![
            Datum::UInt(u64::MAX),
            Datum::UInt(1),
            Datum::UInt(0),
            Datum::UInt(9),
        ],
        vec![
            Datum::Real(-0.0),
            Datum::Real(2.5),
            Datum::Real(f64::NAN),
            Datum::Real(f64::INFINITY),
        ],
        vec![
            Datum::Decimal(Decimal::parse_mysql("20200131.123456").0),
            Datum::Decimal(Decimal::parse_mysql("1.25").0),
            Datum::Null,
            Datum::Int(3),
        ],
        vec![
            Datum::new_string("2020-01-31"),
            Datum::new_string("1"),
            Datum::new_string("MONTH"),
            Datum::Bytes(vec![0xff]),
        ],
        vec![
            Datum::Time(time()),
            Datum::new_duration(MySqlDuration::from_raw_parts(-12_345_678_000, 6)),
            Datum::new_string("%Y-%m-%d %H:%i:%s.%f"),
            Datum::Int(2),
        ],
        vec![
            Datum::Json(BinaryJSON::parse("[1,2,3]").unwrap()),
            Datum::Json(BinaryJSON::parse("2").unwrap()),
            Datum::new_string("$[0]"),
            Datum::Null,
        ],
    ]
}
pub(super) struct Context {
    cache: tidb_expr::ReadyValueCache,
    events: RefCell<Vec<String>>,
    mode: usize,
    value: Datum,
    infrastructure: Option<tidb_expr::EvalError>,
}

impl tidb_expr::Columns for Context {
    fn get(&self, path: &[String]) -> Option<Datum> {
        self.events.borrow_mut().push(format!("get:{path:?}"));
        None
    }
    fn ready_value_cache(&self) -> Option<&tidb_expr::ReadyValueCache> {
        self.events.borrow_mut().push("cache".into());
        Some(&self.cache)
    }
    fn param_value(&self, order: usize) -> Result<Datum, tidb_expr::EvalError> {
        self.events.borrow_mut().push(format!("param:{order}"));
        match (self.mode, order) {
            (1, 0) => Ok(Datum::Null),
            (2, 0) | (5, 1) => Err(tidb_expr::EvalError::Unsupported(
                "actual legacy shared SQL error",
            )),
            (3, 0) => Err(self.infrastructure.as_ref().unwrap().clone()),
            (4, 0) | (6, 1) => panic!("actual legacy shared parameter panic"),
            _ => Ok(self.value.clone()),
        }
    }
    fn append_warning(&self, code: u16, message: &str) {
        self.events
            .borrow_mut()
            .push(format!("warn:{code}:{message}"));
    }
    fn handle_truncate(&self, message: &str) -> Result<(), tidb_expr::EvalError> {
        self.events.borrow_mut().push(format!("truncate:{message}"));
        Err(tidb_expr::EvalError::Unsupported(
            "legacy original truncate callback",
        ))
    }
    fn time_zone(&self) -> tidb_datatype::SessionTimeZone {
        self.events.borrow_mut().push("zone".into());
        tidb_datatype::SessionTimeZone::utc()
    }
    fn div_precision_increment(&self) -> u32 {
        self.events.borrow_mut().push("precision".into());
        4
    }
}
pub(super) fn children(shape: usize) -> Vec<SimpleExpr> {
    use SimpleExpr::*;
    match shape {
        0 => vec![],
        1 => vec![Null],
        2 => vec![Int(3), Int(2), Int(1), Int(99)],
        3 => (0..4).map(Column).collect(),
        4 => vec![
            Func(SimpleSig::PlusInt, vec![Column(0), Int(1)]),
            Column(1),
            Column(2),
            Column(3),
        ],
        5 => vec![
            Bytes(b"2020-01-31".to_vec()),
            Bytes(b"1".to_vec()),
            Bytes(b"MONTH".to_vec()),
        ],
        _ => unreachable!(),
    }
}
fn result(e: &LegacyEvaluator<'_>, root: &SimpleExpr, view: usize) -> String {
    let value = catch_unwind(AssertUnwindSafe(|| match view {
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
        7 => match e.eval_datum_original(root) {
            Ok(Datum::Real(v)) => format!("Ok(RealBits({:016X}))", v.to_bits()),
            v => format!("{v:?}"),
        },
        8 => format!(
            "{:?}",
            LegacyEvaluator::new(e.row, e.div_precision_increment, e.time_zone)
                .eval_expr_original(root)
                .map_err(LegacyEvalError::into_message)
        ),
        _ => unreachable!(),
    }));
    match value {
        Ok(v) => v,
        Err(p) => format!(
            "panic:{:?}",
            p.downcast_ref::<String>()
                .map(String::as_str)
                .or_else(|| p.downcast_ref::<&str>().copied())
                .unwrap_or("non-string panic")
        ),
    }
}
fn legacy_records(mut observe: impl FnMut(&str)) -> usize {
    let mut count = 0;
    let signatures = signatures();
    for (si, sig) in signatures.iter().enumerate() {
        for shape in 0..6 {
            let root = SimpleExpr::Func(*sig, children(shape));
            for (ri, row) in rows().iter().enumerate() {
                for (profile, (precision, zone)) in [
                    (0, tidb_datatype::SessionTimeZone::utc()),
                    (
                        4,
                        tidb_datatype::SessionTimeZone::Named(chrono_tz::America::New_York),
                    ),
                    (
                        30,
                        tidb_datatype::SessionTimeZone::Named(chrono_tz::Asia::Shanghai),
                    ),
                ]
                .into_iter()
                .enumerate()
                {
                    for view in 0..9 {
                        let context = Context {
                            cache: tidb_expr::ReadyValueCache::new(),
                            events: RefCell::new(vec![]),
                            mode: 0,
                            value: Datum::Int(5),
                            infrastructure: None,
                        };
                        let mut e = LegacyEvaluator::new(row, precision, &zone);
                        e.raw_columns = &context;
                        for reuse in 0..2 {
                            let value = result(&e, &root, view);
                            observe(&format!(
                                "{si}:{sig:?}/{shape}/{ri}/{profile}/{view}/{reuse}|{value}|{:?}",
                                context.events.borrow()
                            ));
                            count += 1;
                        }
                    }
                }
            }
        }
    }
    count + leaf_and_shared_records(observe)
}
fn leaf_and_shared_records(mut observe: impl FnMut(&str)) -> usize {
    use tidb_datatype::{FieldType, FieldTypeCode};
    let zone = tidb_datatype::SessionTimeZone::utc();
    let mut count = 0;
    let leaves = vec![
        SimpleExpr::Null,
        SimpleExpr::Int(i64::MIN),
        SimpleExpr::Column(0),
        SimpleExpr::Column(99),
        SimpleExpr::Bytes(vec![0xff, 0]),
        SimpleExpr::Decimal(Decimal::parse_mysql("-1.2500").0),
        SimpleExpr::Json(BinaryJSON::parse("[-1,2]").unwrap()),
        SimpleExpr::Real(f64::from_bits(0x7ff8000000000123)),
        SimpleExpr::Time(time()),
    ];
    for (li, root) in leaves.iter().enumerate() {
        for (ri, row) in rows().iter().enumerate() {
            for view in 0..9 {
                let context = Context {
                    cache: tidb_expr::ReadyValueCache::new(),
                    events: RefCell::new(vec![]),
                    mode: 0,
                    value: Datum::Int(5),
                    infrastructure: None,
                };
                let mut e = LegacyEvaluator::new(row, 4, &zone);
                e.raw_columns = &context;
                for reuse in 0..2 {
                    observe(&format!(
                        "leaf/{li}/{ri}/{view}/{reuse}|{}|{:?}",
                        result(&e, root, view),
                        context.events.borrow()
                    ));
                    count += 1;
                }
            }
        }
    }
    // Obtain a real scope failure through public APIs, retaining its identity.
    let poisoned = tidb_expr::ReadyValueCache::new();
    assert!(catch_unwind(AssertUnwindSafe(|| poisoned
        .with_columns(&tidb_expr::NoColumns, |_| panic!(
            "original scope producer unwind"
        ))))
    .is_err());
    let infrastructure = poisoned
        .with_columns(&tidb_expr::NoColumns, |bound| tidb_expr::eval_pi_in(bound))
        .unwrap_err();
    assert!(
        matches!(
            infrastructure,
            tidb_expr::EvalError::ExpressionRuntimeFailure(_)
                | tidb_expr::EvalError::ExpressionAdapterFailure(_)
        ),
        "actual infrastructure: {infrastructure:?}"
    );
    for (family, (field, value)) in [
        (FieldType::new(FieldTypeCode::LongLong), Datum::Int(5)),
        (
            FieldType::new(FieldTypeCode::NewDecimal)
                .with_flen(20)
                .with_decimal(2),
            Datum::Decimal(Decimal::parse_mysql("5.25").0),
        ),
        (
            FieldType::new(FieldTypeCode::Varchar).with_flen(128),
            Datum::new_string("2020-01-31"),
        ),
    ]
    .into_iter()
    .enumerate()
    {
        let args: Vec<_> = (0..2)
            .map(|order| {
                let mut constant = tidb_expr::constant::Constant::new(value.clone(), field.clone());
                constant.param_marker = Some(tidb_expr::constant::ParamMarker { order });
                SimpleExpr::Shared(Arc::new(SharedExpression {
                    expression: Arc::new(tidb_expr::expression::Expression::Constant(constant)),
                    context: Arc::new(RequestEvalContext::new(zone.clone(), 4, 0)),
                }))
            })
            .collect();
        for (si, sig) in signatures().iter().enumerate() {
            let root = SimpleExpr::Func(*sig, args.clone());
            for mode in 0..7 {
                for view in 0..9 {
                    let context = Context {
                        cache: tidb_expr::ReadyValueCache::new(),
                        events: RefCell::new(vec![]),
                        mode,
                        value: value.clone(),
                        infrastructure: Some(infrastructure.clone()),
                    };
                    let mut e = LegacyEvaluator::new(&[], 4, &zone);
                    e.raw_columns = &context;
                    e.shared_override = Some(&context);
                    for reuse in 0..2 {
                        observe(&format!(
                            "shared/{family}/{si}:{sig:?}/{mode}/{view}/{reuse}|{}|{:?}",
                            result(&e, &root, view),
                            context.events.borrow()
                        ));
                        count += 1;
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
    let count = legacy_records(|record| {
        for b in record.bytes().chain(std::iter::once(b'\n')) {
            hash = (hash ^ u64::from(b)).wrapping_mul(0x100000001b3);
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
    assert_eq!(digest(None), (940248, 0xB02C0B8B9BDDD00C));
}
