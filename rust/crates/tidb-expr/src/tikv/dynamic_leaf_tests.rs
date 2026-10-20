// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Original live parameter/deferred/correlated values, error Datums and phases.
use crate::{
    column::{Column, CorrelatedColumn},
    constant::{Constant, ParamMarker},
    expression::Expression,
    scalar_function::ScalarFunction,
    Columns, Datum, EvalError,
};
use std::{
    cell::{Cell, RefCell},
    collections::BTreeMap,
    sync::{Arc, RwLock},
};
use tidb_chunk::{chunk::Chunk, row::Row};
use tidb_datatype::{ConversionFlags, FieldType, FieldTypeCode, SessionTimeZone};
pub(crate) const KINDS: [&str; 9] = [
    "param",
    "param_deferred",
    "param_effect_deferred",
    "deferred_value",
    "deferred_row",
    "deferred_effect",
    "deferred_error",
    "correlated",
    "correlated_absent",
];
pub(crate) struct Context {
    pub(crate) profile: usize,
    pub(crate) value: Datum,
    pub(crate) sequential: bool,
    pub(crate) calls: Cell<usize>,
    pub(crate) enabled: Cell<bool>,
    pub(crate) events: RefCell<Vec<String>>,
    pub(crate) vars: RefCell<BTreeMap<String, Datum>>,
    pub(crate) binding: Option<Arc<RwLock<Datum>>>,
}
pub(crate) fn frame(value: &Datum) -> String {
    match super::identity_value::encode(value) {
        Ok(Some(bytes)) => bytes.iter().map(|b| format!("{b:02X}")).collect(),
        other => format!("FRAME:{other:?}"),
    }
}
impl Context {
    pub(crate) fn new(profile: usize, value: Datum) -> Self {
        Self {
            profile,
            value,
            sequential: false,
            calls: Cell::new(0),
            enabled: Cell::new(false),
            events: RefCell::new(vec![]),
            vars: RefCell::new(BTreeMap::new()),
            binding: None,
        }
    }
    fn event(&self, event: String) {
        self.events.borrow_mut().push(event);
    }
    pub(crate) fn snapshot(&self) -> String {
        format!(
            "{:?}|calls:{}|flag:{}|vars:{:?}|binding:{:?}",
            *self.events.borrow(),
            self.calls.get(),
            self.enabled.get(),
            self.vars
                .borrow()
                .iter()
                .map(|(k, v)| (k.clone(), frame(v)))
                .collect::<Vec<_>>(),
            self.binding
                .as_ref()
                .map(|b| frame(&b.read().unwrap_or_else(std::sync::PoisonError::into_inner)))
        )
    }
}
impl Columns for Context {
    fn get(&self, _: &[String]) -> Option<Datum> {
        panic!("no invented dynamic leaf input")
    }
    fn param_value(&self, order: usize) -> Result<Datum, EvalError> {
        let n = self.calls.get();
        self.calls.set(n + 1);
        self.event(format!("param:{order}:{n}"));
        if order != 0 {
            return Err(EvalError::Unsupported("dynamic parameter not supplied"));
        }
        if self.profile == 1 {
            return Err(EvalError::Unsupported("dynamic parameter unavailable"));
        }
        if self.profile == 2 {
            panic!("dynamic parameter callback");
        }
        let value = if self.sequential {
            Datum::Int((n + 1) as i64)
        } else {
            self.value.clone()
        };
        if let Some(binding) = &self.binding {
            *binding
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = value.clone();
        }
        Ok(value)
    }
    fn enable_vectorized_expression(&self) -> bool {
        let flag = self.enabled.get();
        self.event(format!("flag:{flag}"));
        flag
    }
    fn time_zone(&self) -> SessionTimeZone {
        self.event("zone".into());
        if self.profile == 3 {
            SessionTimeZone::Named(chrono_tz::America::New_York)
        } else {
            SessionTimeZone::utc()
        }
    }
    fn type_flags(&self) -> ConversionFlags {
        self.event("flags".into());
        tidb_datatype::DEFAULT_STATEMENT_FLAGS
            .with_ignore_truncate_err(self.profile % 3 == 0)
            .with_truncate_as_warning(self.profile % 3 == 1)
            .with_ignore_zero_in_date_err(true)
            .with_ignore_invalid_date_err(self.profile % 3 != 2)
    }
    fn append_warning(&self, code: u16, message: &str) {
        self.event(format!("warning:{code}:{message}"));
    }
    fn get_uservar(&self, name: &str) -> Option<Datum> {
        self.event(format!("get:{name}"));
        self.vars.borrow().get(name).cloned()
    }
    fn set_uservar(&self, name: &str, value: Datum) {
        self.event(format!("set:{name}:{}", frame(&value)));
        self.vars.borrow_mut().insert(name.into(), value);
        if self.profile == 3 {
            self.enabled.set(!self.enabled.get());
        }
    }
}
pub(crate) fn int_type() -> FieldType {
    FieldType::new(FieldTypeCode::LongLong)
}
pub(crate) fn literal(value: Datum, field: FieldType) -> Expression {
    Expression::Constant(Constant::new(value, field))
}
pub(crate) fn call(name: &str, field: FieldType, args: Vec<Expression>) -> Expression {
    Expression::ScalarFunction(ScalarFunction::new(
        tidb_ast::CiString::new(name),
        field,
        args,
    ))
}
pub(crate) fn column() -> Expression {
    let mut c = Column::new(1, int_type());
    c.index = 0;
    Expression::Column(c)
}
pub(crate) fn assignment(name: &str, value: Expression, field: &FieldType) -> Expression {
    call(
        "setvar",
        field.clone(),
        vec![
            literal(
                Datum::new_string(name),
                FieldType::new(FieldTypeCode::VarString),
            ),
            value,
        ],
    )
}
pub(crate) fn expression(
    kind: &str,
    order: i64,
    value: &Datum,
    ret: Option<&FieldType>,
    binding: Option<Arc<RwLock<Datum>>>,
) -> Expression {
    if kind.starts_with("correlated") {
        let mut column = Column::new(1, int_type());
        column.ret_type = ret.cloned();
        return Expression::CorrelatedColumn(CorrelatedColumn {
            column,
            data: if kind == "correlated_absent" {
                None
            } else {
                binding
            },
        });
    }
    let mut c = Constant::new(Datum::new_string("saved planning value"), int_type());
    c.ret_type = ret.cloned();
    let plain = || {
        let field = super::misc_identity_value_tests::field(value);
        literal(value.clone(), field)
    };
    if kind.starts_with("param") {
        c.param_marker = Some(ParamMarker { order });
    }
    c.deferred_expr = match kind {
        "param" => None,
        "param_deferred" | "deferred_value" => Some(Box::new(plain())),
        "param_effect_deferred" | "deferred_effect" => {
            let field = super::misc_identity_value_tests::field(value);
            Some(Box::new(assignment("child", plain(), &field)))
        }
        "deferred_row" => Some(Box::new(column())),
        "deferred_error" => Some(Box::new(call(
            "__unregistered_deferred__",
            int_type(),
            vec![plain()],
        ))),
        _ => unreachable!(),
    };
    Expression::Constant(c)
}
pub(crate) fn input(nullable: bool) -> Chunk {
    let mut input = Chunk::new_with_capacity(&[int_type()], 3);
    for value in [10, 20, 30] {
        if nullable && value == 20 {
            input.append_null(0);
        } else {
            input.append_int64(0, value);
        }
    }
    input
}
pub(crate) fn observe(
    f: impl FnOnce() -> Result<Datum, (Datum, EvalError)>,
    ctx: &Context,
) -> String {
    let result = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)) {
        Ok(Ok(v)) => format!("VALUE:{}", frame(&v)),
        Ok(Err((v, e))) => format!("ERROR:{}:{e:?}", frame(&v)),
        Err(p) => format!(
            "PANIC:{:?}",
            p.downcast_ref::<String>()
                .map(String::as_str)
                .or_else(|| p.downcast_ref::<&str>().copied())
        ),
    };
    format!("{result}|{}", ctx.snapshot())
}
fn fold(hash: &mut u64, value: &str) {
    for byte in value.bytes() {
        *hash = (*hash ^ u64::from(byte)).wrapping_mul(0x100000001b3);
    }
}
fn isolated(which: &str) -> bool {
    const KEY: &str = "TIDB_DYNAMIC_LEAF_ORIGINAL";
    if std::env::var(KEY).as_deref() == Ok(which) {
        return false;
    }
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            &format!("tikv::dynamic_leaf_tests::{which}"),
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
fn dynamic_leaf_original_values_error_datums_and_context() {
    if isolated("dynamic_leaf_original_values_error_datums_and_context") {
        return;
    }
    let now = chrono::DateTime::parse_from_rfc3339("2026-10-20T00:00:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc);
    tidb_datatype::with_test_conversion_clock(now, || {
        let (mut count, mut hash) = (0usize, 0xcbf29ce484222325u64);
        for kind in KINDS {
            for ret in super::eager_scalar_value_tests::types()
                .into_iter()
                .map(Some)
                .chain(std::iter::once(None))
            {
                for profile in 0..4 {
                    for (vi, value) in super::session_read_tests::values().into_iter().enumerate() {
                        for order in if kind.starts_with("param") {
                            vec![-1, 0, 1, i64::MAX]
                        } else {
                            vec![0]
                        } {
                            let binding = Arc::new(RwLock::new(value.clone()));
                            let expr = expression(kind, order, &value, ret.as_ref(), Some(binding));
                            let ctx = Context::new(profile, value.clone());
                            let input = input(false);
                            let outcome = observe(
                                || expr.eval_with_error_value(&ctx, input.get_row(2)),
                                &ctx,
                            );
                            fold(
                                &mut hash,
                                &format!("{kind}|{ret:?}|{profile}|{vi}|{order}|{outcome}"),
                            );
                            count += 1;
                        }
                    }
                }
            }
        }
        assert_eq!((count, hash), (16416, 0xE7DC6852B12EF799));
        eprintln!("DYNAMIC LEAF ORIGINAL {count} {hash:016X}");
    });
}

pub(crate) fn phase_program(
    which: usize,
    binding: Arc<RwLock<Datum>>,
) -> (Vec<Expression>, Vec<FieldType>) {
    let int = int_type();
    let param = || expression("param", 0, &Datum::Int(0), Some(&int), None);
    let correlated = || {
        expression(
            "correlated",
            0,
            &Datum::Null,
            Some(&int),
            Some(Arc::clone(&binding)),
        )
    };
    let deferred_param = || {
        let mut c = Constant::new(Datum::new_string("saved"), int.clone());
        c.deferred_expr = Some(Box::new(param()));
        Expression::Constant(c)
    };
    let roots = match which {
        0 => vec![param()],
        1 => vec![param(), param()],
        2 => vec![call("plus", int.clone(), vec![param(), param()])],
        3 => vec![
            assignment("x", param(), &int),
            call(
                "getvar_int",
                int.clone(),
                vec![literal(
                    Datum::new_string("x"),
                    FieldType::new(FieldTypeCode::VarString),
                )],
            ),
            param(),
        ],
        4 => vec![deferred_param(), param()],
        5 => vec![expression(
            "param_effect_deferred",
            0,
            &Datum::Int(9),
            Some(&int),
            None,
        )],
        6 => vec![expression(
            "deferred_row",
            0,
            &Datum::Int(0),
            Some(&int),
            None,
        )],
        7 => vec![param(), correlated(), correlated()],
        8 => vec![
            assignment("x", param(), &int),
            correlated(),
            correlated(),
            param(),
        ],
        9 => vec![expression("param", 0, &Datum::Int(0), None, None)],
        _ => unreachable!(),
    };
    let fields = vec![int; roots.len()];
    (roots, fields)
}
#[test]
fn dynamic_leaf_original_projection_phases_and_live_bindings() {
    if isolated("dynamic_leaf_original_projection_phases_and_live_bindings") {
        return;
    }
    let (mut count, mut hash) = (0usize, 0xcbf29ce484222325u64);
    for which in 0..10 {
        for profile in 0..4 {
            for nullable in [false, true] {
                for (selection_id, selection) in
                    [None, Some(vec![2, 0, 2]), Some(vec![1, 0, 2]), Some(vec![])]
                        .into_iter()
                        .enumerate()
                {
                    for enabled in [false, true] {
                        let binding = Arc::new(RwLock::new(Datum::Int(900)));
                        let (roots, fields) = phase_program(which, Arc::clone(&binding));
                        let owner = crate::evaluator::EvaluatorSuite::new(roots, false);
                        let mut ctx = Context::new(profile, Datum::Int(0));
                        ctx.sequential = true;
                        ctx.binding = Some(binding);
                        ctx.enabled.set(enabled);
                        let mut input = input(nullable);
                        input.set_sel(selection.clone());
                        let mut output = Chunk::new_with_capacity(&fields, 0);
                        let result =
                            match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                owner.run(&ctx, &mut input, &mut output)
                            })) {
                                Ok(v) => format!("{v:?}"),
                                Err(p) => format!(
                                    "PANIC:{:?}",
                                    p.downcast_ref::<String>()
                                        .map(String::as_str)
                                        .or_else(|| p.downcast_ref::<&str>().copied())
                                ),
                            };
                        fold(&mut hash,&format!("{which}|{profile}|{nullable}|{selection_id}|{enabled}|{result}|{}|{}",super::user_variable_tests::chunk_frame(&output),ctx.snapshot()));
                        count += 1;
                    }
                }
            }
        }
    }
    assert_eq!((count, hash), (640, 0x702A47B62B6F8FC5));
    eprintln!("DYNAMIC LEAF ORIGINAL PHASES {count} {hash:016X}");
}
#[test]
fn dynamic_leaf_original_empty_row_error_precedence() {
    if isolated("dynamic_leaf_original_empty_row_error_precedence") {
        return;
    }
    let (mut count, mut hash) = (0usize, 0xcbf29ce484222325u64);
    for missing in [false, true] {
        for index in [-1, 0, 3] {
            for parent in [false, true] {
                for profile in 0..4 {
                    let mut c = Column::new(1, int_type());
                    c.index = index;
                    if missing {
                        c.ret_type = None;
                    }
                    let child = Expression::Column(c);
                    let expr = if parent {
                        let mut c = Constant::new(Datum::new_string("saved"), int_type());
                        c.deferred_expr = Some(Box::new(child));
                        Expression::Constant(c)
                    } else {
                        child
                    };
                    let ctx = Context::new(profile, Datum::Int(0));
                    let outcome = observe(|| expr.eval_with_error_value(&ctx, Row::empty()), &ctx);
                    fold(
                        &mut hash,
                        &format!("{missing}|{index}|{parent}|{profile}|{outcome}"),
                    );
                    count += 1;
                }
            }
        }
    }
    assert_eq!((count, hash), (48, 0xF3F48F4224A0ECED));
    eprintln!("DYNAMIC LEAF ORIGINAL EMPTY ROW {count} {hash:016X}");
}

#[test]
fn dynamic_leaf_original_long_selection_broadcast_and_row_demand() {
    if isolated("dynamic_leaf_original_long_selection_broadcast_and_row_demand") {
        return;
    }
    let (mut count, mut hash) = (0usize, 0xcbf29ce484222325u64);
    for which in 0..10 {
        for profile in [0, 3] {
            for enabled in [false, true] {
                let binding = Arc::new(RwLock::new(Datum::Int(900)));
                let (roots, fields) = phase_program(which, Arc::clone(&binding));
                let owner = crate::evaluator::EvaluatorSuite::new(roots, false);
                let mut ctx = Context::new(profile, Datum::Int(0));
                ctx.sequential = true;
                ctx.binding = Some(binding);
                ctx.enabled.set(enabled);
                let mut input = input(true);
                input.set_sel(Some((0..2050).map(|i| [2, 0, 2][i % 3]).collect()));
                let mut output = Chunk::new_with_capacity(&fields, 0);
                let outcome = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    owner.run(&ctx, &mut input, &mut output)
                })) {
                    Ok(v) => format!("{v:?}"),
                    Err(p) => format!(
                        "PANIC:{:?}",
                        p.downcast_ref::<String>()
                            .map(String::as_str)
                            .or_else(|| p.downcast_ref::<&str>().copied())
                    ),
                };
                if which == 0 {
                    assert_eq!(ctx.calls.get(), 1);
                    assert_eq!(output.num_rows(), 2050);
                }
                if which == 1 {
                    assert_eq!(ctx.calls.get(), 2);
                    assert_eq!(output.num_rows(), 2050);
                }
                fold(
                    &mut hash,
                    &format!(
                        "{which}|{profile}|{enabled}|{outcome}|{}|{}",
                        super::user_variable_tests::chunk_frame(&output),
                        ctx.snapshot()
                    ),
                );
                count += 1;
            }
        }
    }
    assert_eq!((count, hash), (40, 0x9EFE344B4321EE4F));
    eprintln!("DYNAMIC LEAF ORIGINAL LONG SELECTION {count} {hash:016X}");
}

// This demanded-child harness is test-only; production descriptors never retain
// an expression or evaluator callback. Use distinct live context/binding owners.
fn candidate(
    expression: &Expression,
    ctx: &Context,
    row: Row<'_>,
) -> Result<Datum, (Datum, EvalError)> {
    let descriptor = crate::context_value::ContextValue::capture(expression).unwrap();
    let raw = if descriptor.demands_child() {
        let Expression::Constant(c) = expression else {
            unreachable!()
        };
        c.deferred_expr
            .as_deref()
            .unwrap()
            .eval(ctx, row)
            .map_err(|error| (c.value.clone(), error))?
    } else {
        descriptor
            .read_context(ctx)
            .map_err(|error| match expression {
                Expression::Constant(c) => (c.value.clone(), error),
                _ => (Datum::Null, error),
            })?
    };
    descriptor.finish_expression(raw, ctx)
}
#[test]
fn dynamic_leaf_shared_frozen_values_regression() {
    if isolated("dynamic_leaf_shared_frozen_values_regression") {
        return;
    }
    let now = chrono::DateTime::parse_from_rfc3339("2026-10-20T00:00:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc);
    tidb_datatype::with_test_conversion_clock(now, || {
        let (mut count, mut hash) = (0usize, 0xcbf29ce484222325u64);
        for kind in KINDS {
            for ret in super::eager_scalar_value_tests::types()
                .into_iter()
                .map(Some)
                .chain(std::iter::once(None))
            {
                for profile in 0..4 {
                    for (vi, value) in super::session_read_tests::values().into_iter().enumerate() {
                        for order in if kind.starts_with("param") {
                            vec![-1, 0, 1, i64::MAX]
                        } else {
                            vec![0]
                        } {
                            let oldbinding = Arc::new(RwLock::new(value.clone()));
                            let newbinding = Arc::new(RwLock::new(value.clone()));
                            let original =
                                expression(kind, order, &value, ret.as_ref(), Some(oldbinding));
                            let separate =
                                expression(kind, order, &value, ret.as_ref(), Some(newbinding));
                            let old = Context::new(profile, value.clone());
                            let new = Context::new(profile, value.clone());
                            let input = input(false);
                            let expected = observe(
                                || original.eval_with_error_value(&old, input.get_row(2)),
                                &old,
                            );
                            let actual =
                                observe(|| candidate(&separate, &new, input.get_row(2)), &new);
                            assert_eq!(
                                actual, expected,
                                "{kind} ret={ret:?} p={profile} vi={vi} order={order}"
                            );
                            fold(
                                &mut hash,
                                &format!("{kind}|{ret:?}|{profile}|{vi}|{order}|{expected}"),
                            );
                            count += 1;
                        }
                    }
                }
            }
        }
        assert_eq!((count, hash), (16416, 0xE7DC6852B12EF799));
        eprintln!("DYNAMIC LEAF SHARED FROZEN VALUES {count} {hash:016X}");
    });
}

#[test]
fn dynamic_leaf_shared_frozen_hybrid_generic_and_root_shortcuts() {
    if isolated("dynamic_leaf_shared_frozen_hybrid_generic_and_root_shortcuts") {
        return;
    }
    let compare = true;
    let (mut count, mut hash) = (0usize, 0xcbf29ce484222325u64);
    for code in [FieldTypeCode::Enum, FieldTypeCode::Set, FieldTypeCode::Bit] {
        for flags in [0, tidb_datatype::FieldTypeFlags::ENUM_SET_AS_INT] {
            let field = FieldType::new(code).with_flags(flags);
            for kind in ["literal", "param", "param_deferred", "correlated"] {
                for profile in 0..4 {
                    for (vi, value) in super::session_read_tests::values().into_iter().enumerate() {
                        let make = || {
                            if kind == "literal" {
                                literal(value.clone(), field.clone())
                            } else {
                                expression(
                                    kind,
                                    0,
                                    &value,
                                    Some(&field),
                                    Some(Arc::new(RwLock::new(value.clone()))),
                                )
                            }
                        };
                        let original = make();
                        let ctx = Context::new(profile, value.clone());
                        let input = input(false);
                        let generic = observe(
                            || original.eval_with_error_value(&ctx, input.get_row(0)),
                            &ctx,
                        );
                        if compare && kind != "literal" {
                            let new = Context::new(profile, value.clone());
                            let separate = make();
                            assert_eq!(
                                observe(|| candidate(&separate, &new, input.get_row(0)), &new),
                                generic,
                                "{kind} {code:?} flags={flags} p={profile} vi={vi}"
                            );
                        }
                        let ctx = Context::new(profile, value);
                        let owner = crate::evaluator::EvaluatorSuite::new(vec![original], false);
                        let mut input = input;
                        let mut output = Chunk::new_with_capacity(std::slice::from_ref(&field), 0);
                        let root =
                            match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                owner.run(&ctx, &mut input, &mut output)
                            })) {
                                Ok(v) => format!("{v:?}"),
                                Err(p) => format!(
                                    "PANIC:{:?}",
                                    p.downcast_ref::<String>()
                                        .map(String::as_str)
                                        .or_else(|| p.downcast_ref::<&str>().copied())
                                ),
                            };
                        fold(&mut hash,&format!("{code:?}|{flags}|{kind}|{profile}|{vi}|generic:{generic}|root:{root}|{}|{}",super::user_variable_tests::chunk_frame(&output),ctx.snapshot()));
                        count += 1;
                    }
                }
            }
        }
    }
    assert_eq!((count, hash), (1824, 0x53D70540CA7421FF));
    eprintln!("DYNAMIC LEAF SHARED FROZEN HYBRID ROOT {count} {hash:016X} compare={compare}");
}
