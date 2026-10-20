// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Original variable/session side effects and row-major projection observations.
use crate::{
    constant::Constant, expression::Expression, scalar_function::ScalarFunction, Columns, Datum,
    EvalError,
};
use std::{
    cell::{Cell, RefCell},
    collections::BTreeMap,
};
use tidb_ast::CiString;
use tidb_chunk::{chunk::Chunk, row::Row};
use tidb_datatype::{DateModes, FieldType, FieldTypeCode, SessionTimeZone};
pub(crate) const KINDS: [&str; 9] = [
    "int", "uint", "real", "decimal", "string", "time", "duration", "json", "unknown",
];
pub(crate) struct Context {
    pub(crate) profile: usize,
    pub(crate) enabled: Cell<bool>,
    pub(crate) events: RefCell<Vec<String>>,
    pub(crate) vars: RefCell<BTreeMap<String, Datum>>,
    last: Cell<Option<u64>>,
}
fn frame(value: &Datum) -> String {
    match super::identity_value::encode(value) {
        Ok(Some(bytes)) => bytes.iter().map(|b| format!("{b:02X}")).collect(),
        other => format!("FRAME:{other:?}"),
    }
}
impl Context {
    pub(crate) fn new(profile: usize, value: Datum) -> Self {
        let mut vars = BTreeMap::new();
        if profile != 0 {
            vars.insert("x".into(), value);
        }
        vars.insert("y".into(), Datum::Int(9));
        Self {
            profile,
            enabled: Cell::new(false),
            events: RefCell::new(vec![]),
            vars: RefCell::new(vars),
            last: Cell::new((profile != 0).then_some(u64::MAX)),
        }
    }
    fn event(&self, value: String) {
        self.events.borrow_mut().push(value);
    }
    pub(crate) fn snapshot(&self) -> String {
        let vars = self
            .vars
            .borrow()
            .iter()
            .map(|(k, v)| format!("{k:?}:{}", frame(v)))
            .collect::<Vec<_>>();
        format!(
            "{:?}|{:?}|last:{:?}|flag:{}",
            *self.events.borrow(),
            vars,
            self.last.get(),
            self.enabled.get()
        )
    }
    fn flip(&self) {
        if self.profile == 2 {
            self.enabled.set(!self.enabled.get());
        }
    }
}
impl Columns for Context {
    fn get(&self, _: &[String]) -> Option<Datum> {
        panic!("no invented user-variable column")
    }
    fn enable_vectorized_expression(&self) -> bool {
        let value = self.enabled.get();
        self.event(format!("flag:{value}"));
        value
    }
    fn get_uservar(&self, name: &str) -> Option<Datum> {
        let value = self.vars.borrow().get(name).cloned();
        self.event(format!("get:{name:?}:{:?}", value.as_ref().map(frame)));
        value
    }
    fn set_uservar(&self, name: &str, value: Datum) {
        self.event(format!("set:{name:?}:{}", frame(&value)));
        self.vars.borrow_mut().insert(name.into(), value);
        self.flip();
    }
    fn last_insert_id(&self) -> Option<u64> {
        self.event("last:get".into());
        self.last.get()
    }
    fn set_last_insert_id(&self, value: u64) {
        self.event(format!("last:set:{value}"));
        self.last.set(Some(value));
        self.flip();
    }
    fn truncate_level(&self) -> crate::context::ErrorLevel {
        self.event(format!("truncate:{}", self.profile));
        match self.profile {
            0 => crate::context::ErrorLevel::Ignore,
            1 => crate::context::ErrorLevel::Warn,
            _ => crate::context::ErrorLevel::Error,
        }
    }
    fn append_warning(&self, code: u16, message: &str) {
        self.event(format!("warning:{code}:{message}"));
    }
    fn date_modes(&self) -> DateModes {
        self.event(format!("modes:{}", self.profile));
        DateModes {
            no_zero_date: self.profile == 2,
            no_zero_in_date: self.profile == 2,
            allow_invalid_dates: self.profile == 1,
        }
    }
    fn time_zone(&self) -> SessionTimeZone {
        self.event(format!("zone:{}", self.profile));
        SessionTimeZone::Fixed {
            name: "fixture".into(),
            offset_secs: (self.profile as i32) * 3600,
        }
    }
    fn now(&self) -> Option<(i64, u32, i32)> {
        self.event("now".into());
        Some((1700000000, 123456000, (self.profile as i32) * 3600))
    }
}
pub(crate) fn int_type() -> FieldType {
    FieldType::new(FieldTypeCode::LongLong)
}
pub(crate) fn string_type() -> FieldType {
    FieldType::new(FieldTypeCode::VarString)
}
pub(crate) fn literal(value: Datum) -> Expression {
    let field = super::misc_identity_value_tests::field(&value);
    Expression::Constant(Constant::new(value, field))
}
pub(crate) fn call(name: &str, ret: FieldType, args: Vec<Expression>) -> Expression {
    Expression::ScalarFunction(ScalarFunction::new(CiString::new(name), ret, args))
}
pub(crate) fn headers() -> Vec<Datum> {
    vec![
        Datum::new_string("x"),
        Datum::new_string("X"),
        Datum::new_string("missing"),
        Datum::Null,
        Datum::UInt(u64::MAX),
        Datum::Bytes(vec![255]),
        Datum::Decimal(tidb_datatype::Decimal::from_literal("12.50")),
        Datum::Decimal(tidb_datatype::Decimal::from_raw_parts(
            false,
            vec![255],
            u32::MAX,
            1,
        )),
        Datum::Float32(f64::NAN),
    ]
}
pub(crate) fn scalar_function(
    name: &str,
    ret: &FieldType,
    header: &Datum,
    value: &Datum,
) -> ScalarFunction {
    let args = if name == "last_insert_id" {
        vec![literal(value.clone())]
    } else if name == "setvar" {
        vec![literal(header.clone()), literal(value.clone())]
    } else {
        vec![literal(header.clone())]
    };
    ScalarFunction::new(CiString::new(name), ret.clone(), args)
}
pub(crate) fn observe(f: impl FnOnce() -> Result<Datum, EvalError>, ctx: &Context) -> Vec<u8> {
    let value = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)) {
        Ok(Ok(value)) => format!("VALUE:{}", frame(&value)),
        Ok(Err(e)) => format!("ERROR:{e:?}"),
        Err(e) => format!(
            "PANIC:{:?}",
            e.downcast_ref::<String>()
                .map(String::as_str)
                .or_else(|| e.downcast_ref::<&str>().copied())
        ),
    };
    format!("{value}|{}", ctx.snapshot()).into_bytes()
}
fn fold(hash: &mut u64, bytes: &[u8]) {
    for &b in bytes {
        *hash = (*hash ^ u64::from(b)).wrapping_mul(0x100000001b3);
    }
}
fn isolated(which: &str) -> bool {
    const KEY: &str = "TIDB_USER_VARIABLE_ORIGINAL";
    if std::env::var(KEY).as_deref() == Ok(which) {
        return false;
    }
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            &format!("tikv::user_variable_tests::{which}"),
            "--nocapture",
        ])
        .env(KEY, which)
        .env("TZ", "UTC")
        .output()
        .unwrap();
    eprint!("{}", String::from_utf8_lossy(&output.stderr));
    eprintln!("{}", String::from_utf8_lossy(&output.stdout));
    assert!(output.status.success());
    true
}
#[test]
fn user_variable_original_scalar_observations() {
    if isolated("user_variable_original_scalar_observations") {
        return;
    }
    let now = chrono::DateTime::parse_from_rfc3339("2026-10-20T00:00:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc);
    tidb_datatype::with_test_conversion_clock(now, || {
        let (mut count, mut hash) = (0usize, 0xcbf29ce484222325u64);
        for profile in 0..3 {
            for ret in super::eager_scalar_value_tests::types() {
                for (vi, value) in super::session_read_tests::values().into_iter().enumerate() {
                    for (hi, header) in headers().into_iter().enumerate() {
                        for name in KINDS
                            .iter()
                            .map(|kind| format!("getvar_{kind}"))
                            .chain(std::iter::once("setvar".into()))
                        {
                            let ctx = Context::new(
                                profile,
                                if name == "setvar" {
                                    Datum::Int(42)
                                } else {
                                    value.clone()
                                },
                            );
                            let f = scalar_function(&name, &ret, &header, &value);
                            fold(
                                &mut hash,
                                format!("{name}|{profile}|{ret:?}|{vi}|{hi}|").as_bytes(),
                            );
                            fold(&mut hash, &observe(|| f.eval(&ctx, Row::empty()), &ctx));
                            count += 1;
                        }
                    }
                    let ctx = Context::new(profile, Datum::Int(42));
                    let f = scalar_function("last_insert_id", &ret, &Datum::Null, &value);
                    fold(
                        &mut hash,
                        format!("last_insert_id|{profile}|{ret:?}|{vi}|").as_bytes(),
                    );
                    fold(&mut hash, &observe(|| f.eval(&ctx, Row::empty()), &ctx));
                    count += 1;
                }
            }
        }
        assert_eq!((count, hash), (57057, 0xDF2895EE82065D7E));
        eprintln!("USER VARIABLE SCALAR ORIGINAL {count} {hash:016X}");
    });
}
pub(crate) fn input_column() -> Expression {
    let mut c = crate::column::Column::new(1, int_type());
    c.index = 0;
    c.orig_name = "input".into();
    Expression::Column(c)
}
pub(crate) fn program(which: usize) -> (Vec<Expression>, Vec<FieldType>) {
    let get = || {
        call(
            "getvar_int",
            int_type(),
            vec![literal(Datum::new_string("x"))],
        )
    };
    let set = |v, ret| call("setvar", ret, vec![literal(Datum::new_string("x")), v]);
    let last = || call("last_insert_id", int_type(), vec![]);
    let writer = || call("last_insert_id", int_type(), vec![input_column()]);
    let sum = || call("plus", int_type(), vec![get(), input_column()]);
    let bad = || {
        call(
            "if",
            string_type(),
            vec![
                call(
                    "eq",
                    int_type(),
                    vec![input_column(), literal(Datum::Int(2))],
                ),
                literal(Datum::new_string("bad")),
                literal(Datum::new_string("7")),
            ],
        )
    };
    let roots = match which {
        0 => vec![set(sum(), int_type()), get()],
        1 => vec![get(), set(input_column(), int_type()), get()],
        2 => vec![set(literal(Datum::Null), int_type()), get()],
        3 => vec![set(literal(Datum::new_string("7")), string_type()), get()],
        4 => vec![set(bad(), string_type()), get()],
        5 => vec![writer(), last()],
        6 => vec![set(writer(), int_type()), get(), last()],
        7 => vec![
            set(sum(), int_type()),
            get(),
            input_column(),
            input_column(),
        ],
        8 => vec![
            set(
                call(
                    "if",
                    int_type(),
                    vec![
                        call(
                            "eq",
                            int_type(),
                            vec![input_column(), literal(Datum::Int(2))],
                        ),
                        literal(Datum::Null),
                        input_column(),
                    ],
                ),
                int_type(),
            ),
            get(),
        ],
        _ => unreachable!(),
    };
    let fields = roots
        .iter()
        .map(|r| r.static_type().unwrap().clone())
        .collect();
    (roots, fields)
}
pub(crate) fn input(nullable: bool) -> Chunk {
    let mut chunk = Chunk::new_with_capacity(&[int_type()], 3);
    for i in 1..=3 {
        if nullable && i == 2 {
            chunk.append_null(0);
        } else {
            chunk.append_int64(0, i);
        }
    }
    chunk
}
pub(crate) fn chunk_frame(output: &Chunk) -> String {
    let columns = (0..output.num_cols())
        .map(|i| {
            let c = output.column(i);
            (0..c.rows())
                .map(|r| {
                    if c.is_null(r) {
                        "NULL".to_owned()
                    } else {
                        c.get_raw(r)
                            .as_ref()
                            .iter()
                            .map(|b| format!("{b:02X}"))
                            .collect()
                    }
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    format!("{columns:?}")
}
#[test]
fn user_variable_original_ordered_projection_observations() {
    if isolated("user_variable_original_ordered_projection_observations") {
        return;
    }
    let (mut count, mut hash) = (0usize, 0xcbf29ce484222325u64);
    for which in 0..9 {
        for profile in 0..3 {
            for nullable in [false, true] {
                for (selection_id, selection) in
                    [None, Some(vec![2, 0, 2]), Some(vec![1, 0, 2]), Some(vec![])]
                        .into_iter()
                        .enumerate()
                {
                    for enabled in [false, true] {
                        let (roots, fields) = program(which);
                        // Selected direct outputs use the working general row evaluator;
                        // its optimized legacy swap refuses a non-nil selection vector.
                        let avoid = which == 7 && selection.is_some();
                        let suite = crate::evaluator::EvaluatorSuite::new(roots, avoid);
                        let ctx = Context::new(profile, Datum::Int(0));
                        ctx.enabled.set(enabled);
                        let mut input = input(nullable);
                        input.set_sel(selection.clone());
                        let mut output = Chunk::new_with_capacity(&fields, 0);
                        let result =
                            match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                suite.run(&ctx, &mut input, &mut output)
                            })) {
                                Ok(v) => format!("{v:?}"),
                                Err(e) => format!(
                                    "PANIC:{:?}",
                                    e.downcast_ref::<String>()
                                        .map(String::as_str)
                                        .or_else(|| e.downcast_ref::<&str>().copied())
                                ),
                            };
                        fold(&mut hash,format!("{which}|{profile}|{nullable}|{selection_id}|{enabled}|{avoid}|{result}|{}|{}",chunk_frame(&output),ctx.snapshot()).as_bytes());
                        count += 1;
                    }
                }
            }
        }
    }
    assert_eq!((count, hash), (432, 0x695ADB35A928374D));
    eprintln!("USER VARIABLE ORDERED ORIGINAL {count} {hash:016X}");
}

// Test-only demanded-argument harness. Production selectors above never hold
// an Expression or a callback; the SDK driver supplies those actual values.
fn candidate_scalar(function: &ScalarFunction, ctx: &Context) -> Result<Datum, EvalError> {
    use crate::scalar_function::user_variable::{record_last_insert_id, UserVariableAction};
    let name = function.func_name.lowercase();
    let stored_name = function
        .identity_constant_probe()
        .and_then(|value| value.sql_string().ok());
    let value = if name == "last_insert_id" {
        record_last_insert_id(
            function.args[0].eval(ctx, Row::empty())?,
            function.args[0].static_type(),
            ctx,
        )
    } else if let (Some(action), Some(variable)) =
        (UserVariableAction::for_function(function), stored_name)
    {
        match action {
            UserVariableAction::Get(_) => action.read(ctx, &variable),
            UserVariableAction::Set => {
                action.write(ctx, &variable, function.args[1].eval(ctx, Row::empty())?)
            }
        }
    } else {
        let values = function
            .args
            .iter()
            .map(|arg| arg.eval(ctx, Row::empty()))
            .collect::<Result<Vec<_>, _>>()?;
        crate::scalar_function::eager_values::EagerValueKernel::capture(function).eval_in(
            ctx,
            values,
            std::convert::identity,
        )
    }?;
    crate::scalar_function::coerce_function_result(name, function.get_static_type(), value)
}
#[test]
fn user_variable_shared_scalar_frozen_regression() {
    if isolated("user_variable_shared_scalar_frozen_regression") {
        return;
    }
    let now = chrono::DateTime::parse_from_rfc3339("2026-10-20T00:00:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc);
    tidb_datatype::with_test_conversion_clock(now, || {
        let (mut count, mut hash) = (0usize, 0xcbf29ce484222325u64);
        for profile in 0..3 {
            for ret in super::eager_scalar_value_tests::types() {
                for (vi, value) in super::session_read_tests::values().into_iter().enumerate() {
                    for (hi, header) in headers().into_iter().enumerate() {
                        for name in KINDS
                            .iter()
                            .map(|kind| format!("getvar_{kind}"))
                            .chain(std::iter::once("setvar".into()))
                        {
                            let initial = if name == "setvar" {
                                Datum::Int(42)
                            } else {
                                value.clone()
                            };
                            let old = Context::new(profile, initial.clone());
                            let new = Context::new(profile, initial);
                            let f = scalar_function(&name, &ret, &header, &value);
                            let expected = observe(|| f.eval(&old, Row::empty()), &old);
                            let actual = observe(|| candidate_scalar(&f, &new), &new);
                            assert_eq!(
                                actual, expected,
                                "{name} profile={profile} ret={ret:?} vi={vi} hi={hi}"
                            );
                            fold(
                                &mut hash,
                                format!("{name}|{profile}|{ret:?}|{vi}|{hi}|").as_bytes(),
                            );
                            fold(&mut hash, &expected);
                            count += 1;
                        }
                    }
                    let old = Context::new(profile, Datum::Int(42));
                    let new = Context::new(profile, Datum::Int(42));
                    let f = scalar_function("last_insert_id", &ret, &Datum::Null, &value);
                    let expected = observe(|| f.eval(&old, Row::empty()), &old);
                    let actual = observe(|| candidate_scalar(&f, &new), &new);
                    assert_eq!(
                        actual, expected,
                        "last_insert_id profile={profile} ret={ret:?} vi={vi}"
                    );
                    fold(
                        &mut hash,
                        format!("last_insert_id|{profile}|{ret:?}|{vi}|").as_bytes(),
                    );
                    fold(&mut hash, &expected);
                    count += 1;
                }
            }
        }
        assert_eq!((count, hash), (57057, 0xDF2895EE82065D7E));
        eprintln!("USER VARIABLE SHARED SCALAR FROZEN REGRESSION {count} {hash:016X}");
    });
}
