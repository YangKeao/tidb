// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Original typed context reads and runtime diagnostic re-demand.
use super::dynamic_leaf_tests as proof;
use crate::{constant::Constant, expression::Expression, Columns, Datum, EvalError};
use std::sync::{Arc, RwLock};
use tidb_datatype::{EvalType, FieldType, FieldTypeCode};

pub(crate) struct Context {
    pub(crate) inner: proof::Context,
    pub(crate) script: Vec<Datum>,
}
impl Context {
    pub(crate) fn new(profile: usize, script: Vec<Datum>) -> Self {
        Self {
            inner: proof::Context::new(profile, script[0].clone()),
            script,
        }
    }
    pub(crate) fn snapshot(&self) -> String {
        self.inner.snapshot()
    }
}
impl Columns for Context {
    fn get(&self, _: &[String]) -> Option<Datum> {
        panic!("no invented typed context input")
    }
    fn param_value(&self, order: usize) -> Result<Datum, EvalError> {
        let n = self.inner.calls.get();
        let _ = self.inner.param_value(order)?;
        let value = self.script[n.min(self.script.len() - 1)].clone();
        if let Some(binding) = &self.inner.binding {
            *binding
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = value.clone();
        }
        Ok(value)
    }
    fn enable_vectorized_expression(&self) -> bool {
        self.inner.enable_vectorized_expression()
    }
    fn time_zone(&self) -> tidb_datatype::SessionTimeZone {
        self.inner.time_zone()
    }
    fn type_flags(&self) -> tidb_datatype::ConversionFlags {
        self.inner.type_flags()
    }
    fn append_warning(&self, code: u16, message: &str) {
        self.inner.append_warning(code, message);
    }
    fn get_uservar(&self, name: &str) -> Option<Datum> {
        self.inner.get_uservar(name)
    }
    fn set_uservar(&self, name: &str, value: Datum) {
        self.inner.set_uservar(name, value);
    }
}
pub(crate) fn outcome(f: impl FnOnce() -> Result<Datum, EvalError>, ctx: &Context) -> String {
    let result = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)) {
        Ok(Ok(v)) => format!("VALUE:{}", proof::frame(&v)),
        Ok(Err(e)) => format!("ERROR:{e:?}"),
        Err(p) => format!(
            "PANIC:{:?}",
            p.downcast_ref::<String>()
                .map(String::as_str)
                .or_else(|| p.downcast_ref::<&str>().copied())
        ),
    };
    format!("{result}|{}", ctx.snapshot())
}
fn fold(hash: &mut u64, s: &str) {
    for b in s.bytes() {
        *hash = (*hash ^ u64::from(b)).wrapping_mul(0x100000001b3);
    }
}
fn isolated(which: &str) -> bool {
    const KEY: &str = "TIDB_TYPED_DYNAMIC_ORIGINAL";
    if std::env::var(KEY).as_deref() == Ok(which) {
        return false;
    }
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            &format!("tikv::typed_dynamic_tests::{which}"),
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
pub(crate) fn targets() -> [EvalType; 9] {
    [
        EvalType::Int,
        EvalType::Real,
        EvalType::Decimal,
        EvalType::String,
        EvalType::Datetime,
        EvalType::Timestamp,
        EvalType::Duration,
        EvalType::Json,
        EvalType::VectorFloat32,
    ]
}
pub(crate) fn scripts() -> Vec<Vec<Datum>> {
    let mut scripts = super::session_read_tests::values()
        .into_iter()
        .map(|v| vec![v])
        .collect::<Vec<_>>();
    scripts.extend([
        vec![Datum::Null, Datum::Int(7), Datum::new_string("9")],
        vec![Datum::Int(7), Datum::new_string("12tail"), Datum::Null],
        vec![Datum::new_string("7"), Datum::Int(8), Datum::Real(9.0)],
        vec![Datum::Int(i64::MAX), Datum::Int(i64::MAX), Datum::Int(42)],
        vec![
            Datum::Int(i64::MIN),
            Datum::Int(i64::MIN),
            Datum::new_string("after"),
        ],
    ]);
    scripts
}
#[test]
fn typed_dynamic_original_public_requested_domains_and_type_reads() {
    if isolated("typed_dynamic_original_public_requested_domains_and_type_reads") {
        return;
    }
    let now = chrono::DateTime::parse_from_rfc3339("2026-10-20T00:00:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc);
    tidb_datatype::with_test_conversion_clock(now, || {
        let (mut count, mut hash) = (0usize, 0xcbf29ce484222325u64);
        for kind in proof::KINDS {
            for ret in super::eager_scalar_value_tests::types()
                .into_iter()
                .map(Some)
                .chain(std::iter::once(None))
            {
                for profile in 0..4 {
                    for (si, script) in scripts().into_iter().enumerate() {
                        for order in if kind.starts_with("param") {
                            vec![-1, 0, 1, i64::MAX]
                        } else {
                            vec![0]
                        } {
                            for target in targets() {
                                let value = &script[0];
                                let expr = proof::expression(
                                    kind,
                                    order,
                                    value,
                                    ret.as_ref(),
                                    Some(Arc::new(RwLock::new(value.clone()))),
                                );
                                let ctx = Context::new(profile, script.clone());
                                let input = proof::input(true);
                                let result = outcome(
                                    || match &expr {
                                        Expression::Constant(c) => {
                                            c.eval_typed_as_on_row(&ctx, input.get_row(2), target)
                                        }
                                        Expression::CorrelatedColumn(c) => {
                                            c.eval_typed(&ctx, target)
                                        }
                                        _ => unreachable!(),
                                    },
                                    &ctx,
                                );
                                fold(
                                    &mut hash,
                                    &format!(
                                        "{kind}|{ret:?}|{profile}|{si}|{order}|{target:?}|{result}"
                                    ),
                                );
                                count += 1;
                            }
                        }
                    }
                }
            }
        }
        assert_eq!((count, hash), (186624, 0x1F8A2B96768DA8AB));
        eprintln!("TYPED DYNAMIC ORIGINAL PUBLIC {count} {hash:016X}");
    });
}

pub(crate) fn root(which: usize, value: &Datum, binding: Arc<RwLock<Datum>>) -> Expression {
    let int = proof::int_type();
    let param = || proof::expression("param", 0, value, Some(&int), None);
    let corr = || {
        proof::expression(
            "correlated",
            0,
            value,
            Some(&int),
            Some(Arc::clone(&binding)),
        )
    };
    let one = || proof::literal(Datum::Int(1), int.clone());
    let plus = || proof::call("plus", int.clone(), vec![param(), one()]);
    match which {
        0 => plus(),
        1 => proof::call("plus", int.clone(), vec![param(), param()]),
        2 => proof::call("plus", int.clone(), vec![param(), corr()]),
        3 => proof::call("abs", int.clone(), vec![param()]),
        4 => proof::call("unaryminus", int.clone(), vec![param()]),
        5 => proof::call(
            "cast_decimal",
            FieldType::new(FieldTypeCode::NewDecimal),
            vec![param()],
        ),
        6 => proof::call(
            "cast_json",
            FieldType::new(FieldTypeCode::Json),
            vec![param()],
        ),
        7 => proof::call("char_length", int.clone(), vec![param()]),
        8 => proof::call(
            "lower",
            FieldType::new(FieldTypeCode::VarString),
            vec![param()],
        ),
        9..=13 => {
            let mut c = Constant::new(Datum::Int(99), int.clone());
            c.deferred_expr = Some(Box::new(plus()));
            if which >= 10 {
                c.param_marker = Some(crate::constant::ParamMarker {
                    order: if which == 11 { -1 } else { 0 },
                });
            }
            if which == 12 {
                c.ret_type = Some(FieldType::new(FieldTypeCode::VarString));
            }
            if which == 13 {
                let mut outer = Constant::new(Datum::Int(77), int);
                outer.deferred_expr = Some(Box::new(Expression::Constant(c)));
                return Expression::Constant(outer);
            }
            Expression::Constant(c)
        }
        14 => proof::assignment("typed", plus(), &int),
        15 => proof::call("exp", FieldType::new(FieldTypeCode::Double), vec![param()]),
        16 => proof::call(
            "pow",
            FieldType::new(FieldTypeCode::Double),
            vec![
                param(),
                proof::literal(Datum::Real(0.5), FieldType::new(FieldTypeCode::Double)),
            ],
        ),
        _ => unreachable!(),
    }
}
#[test]
fn typed_dynamic_original_root_admission_bulk_and_diagnostic_phases() {
    if isolated("typed_dynamic_original_root_admission_bulk_and_diagnostic_phases") {
        return;
    }
    let (mut count, mut hash) = (0usize, 0xcbf29ce484222325u64);
    for which in 0..17 {
        for profile in 0..4 {
            for (si, script) in scripts().into_iter().enumerate() {
                for (sel_id, selection) in [None, Some(vec![2, 0, 2]), Some(vec![])]
                    .into_iter()
                    .enumerate()
                {
                    for enabled in [false, true] {
                        let binding = Arc::new(RwLock::new(Datum::Int(900)));
                        let root = root(which, &script[0], Arc::clone(&binding));
                        let field = root.static_type().unwrap().clone();
                        let owner = crate::evaluator::EvaluatorSuite::new(vec![root], false);
                        let mut ctx = Context::new(profile, script.clone());
                        ctx.inner.binding = Some(binding);
                        ctx.inner.enabled.set(enabled);
                        let mut input = proof::input(true);
                        input.set_sel(selection.clone());
                        let mut output = tidb_chunk::chunk::Chunk::new_with_capacity(&[field], 0);
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
                        fold(
                            &mut hash,
                            &format!(
                                "{which}|{profile}|{si}|{sel_id}|{enabled}|{result}|{}|{}",
                                super::user_variable_tests::chunk_frame(&output),
                                ctx.snapshot()
                            ),
                        );
                        count += 1;
                    }
                }
            }
        }
    }
    assert_eq!((count, hash), (9792, 0xE4C86226EC383F1D));
    eprintln!("TYPED DYNAMIC ORIGINAL ROOT PHASES {count} {hash:016X}");
}

#[test]
fn typed_dynamic_original_auto_domain_has_separate_type_reads() {
    if isolated("typed_dynamic_original_auto_domain_has_separate_type_reads") {
        return;
    }
    let (mut count, mut hash) = (0usize, 0xcbf29ce484222325u64);
    for kind in proof::KINDS
        .into_iter()
        .filter(|kind| !kind.starts_with("correlated"))
    {
        for ret in super::eager_scalar_value_tests::types()
            .into_iter()
            .map(Some)
            .chain(std::iter::once(None))
        {
            for profile in 0..4 {
                for (si, script) in scripts().into_iter().enumerate() {
                    for order in if kind.starts_with("param") {
                        vec![-1, 0, 1, i64::MAX]
                    } else {
                        vec![0]
                    } {
                        let expr = proof::expression(kind, order, &script[0], ret.as_ref(), None);
                        let Expression::Constant(c) = expr else {
                            unreachable!()
                        };
                        let ctx = Context::new(profile, script.clone());
                        let input = proof::input(true);
                        let result = outcome(|| c.eval_typed_on_row(&ctx, input.get_row(2)), &ctx);
                        fold(
                            &mut hash,
                            &format!("{kind}|{ret:?}|{profile}|{si}|{order}|{result}"),
                        );
                        count += 1;
                    }
                }
            }
        }
    }
    assert_eq!((count, hash), (18432, 0x015B674ABA041ABB));
    eprintln!("TYPED DYNAMIC ORIGINAL AUTO {count} {hash:016X}");
}

pub(crate) fn diagnostic_expression(which: usize, value: &Datum) -> Expression {
    let field = super::misc_identity_value_tests::field(value);
    let param = || proof::expression("param", 0, value, Some(&field), None);
    let effect = || proof::expression("deferred_effect", 0, value, Some(&field), None);
    let column = |index, missing| {
        let mut c = crate::column::Column::new(1, proof::int_type());
        c.index = index;
        if missing {
            c.ret_type = None;
        }
        Expression::Column(c)
    };
    let deferred = |child| {
        let mut c = Constant::new(Datum::new_string("saved diagnostic"), field.clone());
        c.deferred_expr = Some(Box::new(child));
        Expression::Constant(c)
    };
    match which {
        0 => param(),
        1 => deferred(column(0, false)),
        2 => deferred(column(0, true)),
        3 => deferred(column(-1, false)),
        4 => effect(),
        5 => proof::expression("param_effect_deferred", 0, value, Some(&field), None),
        6 => proof::call("__unregistered_diagnostic__", field.clone(), vec![param()]),
        7 => proof::call("plus", field.clone(), vec![param(), effect()]),
        8 => proof::call(
            "cast_json",
            FieldType::new(FieldTypeCode::Json),
            vec![effect()],
        ),
        9 => {
            let mut c = crate::column::Column::new(2, field.clone());
            c.is_hidden = true;
            c.virtual_expr = Some(Box::new(effect()));
            Expression::Column(c)
        }
        10 => column(0, false),
        11 => proof::expression(
            "correlated",
            0,
            value,
            Some(&field),
            Some(Arc::new(RwLock::new(value.clone()))),
        ),
        12 => proof::call(
            "plus",
            field.clone(),
            vec![
                proof::call("__unregistered_diagnostic__", field.clone(), vec![param()]),
                effect(),
            ],
        ),
        13 => proof::call(
            "plus",
            field.clone(),
            vec![deferred(column(0, false)), effect()],
        ),
        14 => {
            let mut f = crate::scalar_function::ScalarFunction::new(
                tidb_ast::CiString::new("cast_json"),
                FieldType::new(FieldTypeCode::Json),
                vec![effect()],
            );
            f.ret_type = None;
            Expression::ScalarFunction(f)
        }
        _ => unreachable!(),
    }
}
#[test]
fn typed_dynamic_original_diagnostic_empty_row_effects_and_short_circuits() {
    if isolated("typed_dynamic_original_diagnostic_empty_row_effects_and_short_circuits") {
        return;
    }
    let (mut count, mut hash) = (0usize, 0xcbf29ce484222325u64);
    for which in 0..15 {
        for profile in 0..4 {
            for (si, script) in scripts().into_iter().enumerate() {
                for float in [false, true] {
                    let expr = diagnostic_expression(which, &script[0]);
                    let ctx = Context::new(profile, script.clone());
                    let result =
                        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            crate::scalar_function::numeric_expression_text(&expr, float, &ctx)
                        })) {
                            Ok(v) => format!("{v:?}"),
                            Err(p) => format!(
                                "PANIC:{:?}",
                                p.downcast_ref::<String>()
                                    .map(String::as_str)
                                    .or_else(|| p.downcast_ref::<&str>().copied())
                            ),
                        };
                    fold(
                        &mut hash,
                        &format!(
                            "numeric|{which}|{profile}|{si}|{float}|{result}|{}",
                            ctx.snapshot()
                        ),
                    );
                    count += 1;
                }
                for name in ["abs", "exp", "cot"] {
                    let expr = diagnostic_expression(which, &script[0]);
                    let field = if name == "abs" {
                        proof::int_type()
                    } else {
                        FieldType::new(FieldTypeCode::Double)
                    };
                    let root = proof::call(name, field, vec![expr]);
                    let ctx = Context::new(profile, script.clone());
                    let input = proof::input(false);
                    let result = outcome(|| root.eval(&ctx, input.get_row(0)), &ctx);
                    fold(
                        &mut hash,
                        &format!("math|{which}|{profile}|{si}|{name}|{result}"),
                    );
                    count += 1;
                }
            }
        }
    }
    assert_eq!((count, hash), (7200, 0xA4F6DB5E8D40E458));
    eprintln!("TYPED DYNAMIC ORIGINAL DIAGNOSTIC {count} {hash:016X}");
}

fn candidate(
    expression: &Expression,
    ctx: &Context,
    row: tidb_chunk::row::Row<'_>,
    target: Option<EvalType>,
) -> Result<Datum, EvalError> {
    let descriptor = crate::typed_context_value::TypedContextValue::capture(expression).unwrap();
    match descriptor.begin(ctx, target)? {
        crate::typed_context_value::TypedRead::Ready(value) => Ok(value),
        crate::typed_context_value::TypedRead::Child(state) => {
            let Expression::Constant(c) = expression else {
                unreachable!()
            };
            state.finish(c.deferred_expr.as_deref().unwrap().eval(ctx, row)?, ctx)
        }
    }
}
#[test]
fn typed_dynamic_shared_frozen_public_values() {
    if isolated("typed_dynamic_shared_frozen_public_values") {
        return;
    }
    let now = chrono::DateTime::parse_from_rfc3339("2026-10-20T00:00:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc);
    tidb_datatype::with_test_conversion_clock(now, || {
        let (mut count, mut hash) = (0usize, 0xcbf29ce484222325u64);
        for kind in proof::KINDS {
            for ret in super::eager_scalar_value_tests::types()
                .into_iter()
                .map(Some)
                .chain(std::iter::once(None))
            {
                for profile in 0..4 {
                    for (si, script) in scripts().into_iter().enumerate() {
                        for order in if kind.starts_with("param") {
                            vec![-1, 0, 1, i64::MAX]
                        } else {
                            vec![0]
                        } {
                            for target in targets() {
                                let make = || {
                                    proof::expression(
                                        kind,
                                        order,
                                        &script[0],
                                        ret.as_ref(),
                                        Some(Arc::new(RwLock::new(script[0].clone()))),
                                    )
                                };
                                let original = make();
                                let separate = make();
                                let old = Context::new(profile, script.clone());
                                let new = Context::new(profile, script.clone());
                                let input = proof::input(true);
                                let expected = outcome(
                                    || match &original {
                                        Expression::Constant(c) => {
                                            c.eval_typed_as_on_row(&old, input.get_row(2), target)
                                        }
                                        Expression::CorrelatedColumn(c) => {
                                            c.eval_typed(&old, target)
                                        }
                                        _ => unreachable!(),
                                    },
                                    &old,
                                );
                                let got = outcome(
                                    || candidate(&separate, &new, input.get_row(2), Some(target)),
                                    &new,
                                );
                                assert_eq!(got,expected,"{kind} {ret:?} p={profile} script={si} order={order} target={target:?}");
                                fold(&mut hash,&format!("{kind}|{ret:?}|{profile}|{si}|{order}|{target:?}|{expected}"));
                                count += 1;
                            }
                        }
                    }
                }
            }
        }
        assert_eq!((count, hash), (186624, 0x1F8A2B96768DA8AB));
        eprintln!("TYPED DYNAMIC SHARED FROZEN PUBLIC {count} {hash:016X}");
    });
}
#[test]
fn typed_dynamic_shared_frozen_auto_values() {
    if isolated("typed_dynamic_shared_frozen_auto_values") {
        return;
    }
    let (mut count, mut hash) = (0usize, 0xcbf29ce484222325u64);
    for kind in proof::KINDS
        .into_iter()
        .filter(|kind| !kind.starts_with("correlated"))
    {
        for ret in super::eager_scalar_value_tests::types()
            .into_iter()
            .map(Some)
            .chain(std::iter::once(None))
        {
            for profile in 0..4 {
                for (si, script) in scripts().into_iter().enumerate() {
                    for order in if kind.starts_with("param") {
                        vec![-1, 0, 1, i64::MAX]
                    } else {
                        vec![0]
                    } {
                        let original =
                            proof::expression(kind, order, &script[0], ret.as_ref(), None);
                        let separate =
                            proof::expression(kind, order, &script[0], ret.as_ref(), None);
                        let Expression::Constant(c) = &original else {
                            unreachable!()
                        };
                        let old = Context::new(profile, script.clone());
                        let new = Context::new(profile, script.clone());
                        let input = proof::input(true);
                        let expected =
                            outcome(|| c.eval_typed_on_row(&old, input.get_row(2)), &old);
                        let actual =
                            outcome(|| candidate(&separate, &new, input.get_row(2), None), &new);
                        assert_eq!(
                            actual, expected,
                            "{kind} {ret:?} p={profile} script={si} order={order}"
                        );
                        fold(
                            &mut hash,
                            &format!("{kind}|{ret:?}|{profile}|{si}|{order}|{expected}"),
                        );
                        count += 1;
                    }
                }
            }
        }
    }
    assert_eq!((count, hash), (18432, 0x015B674ABA041ABB));
    eprintln!("TYPED DYNAMIC SHARED FROZEN AUTO {count} {hash:016X}");
}

#[test]
fn typed_dynamic_original_math_renderer_demand_before_retirement() {
    if isolated("typed_dynamic_original_math_renderer_demand_before_retirement") {
        return;
    }
    let (mut count, mut hash) = (0usize, 0xcbf29ce484222325u64);
    for which in 0..15 {
        for profile in 0..4 {
            for (si, script) in scripts().into_iter().enumerate() {
                for name in ["abs", "exp", "cot"] {
                    let root = proof::call(
                        name,
                        FieldType::new(FieldTypeCode::Double),
                        vec![diagnostic_expression(which, &script[0])],
                    );
                    let Expression::ScalarFunction(f) = root else {
                        unreachable!()
                    };
                    let ctx = Context::new(profile, script.clone());
                    let result =
                        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            crate::scalar_function::dynamic_math_diagnostic_for_test(&f, &ctx)
                        })) {
                            Ok(v) => format!("{v:?}"),
                            Err(p) => format!(
                                "PANIC:{:?}",
                                p.downcast_ref::<String>()
                                    .map(String::as_str)
                                    .or_else(|| p.downcast_ref::<&str>().copied())
                            ),
                        };
                    fold(
                        &mut hash,
                        &format!("{which}|{profile}|{si}|{name}|{result}|{}", ctx.snapshot()),
                    );
                    count += 1;
                }
            }
        }
    }
    assert_eq!((count, hash), (4320, 0xBC1F79F0E8CB417D));
    eprintln!("TYPED DYNAMIC ORIGINAL MATH RENDER {count} {hash:016X}");
}

fn plan_outcome(
    plan: &crate::scalar_function::dynamic_diagnostic::DynamicDiagnostic,
    bindings: &[&Expression],
    ctx: &Context,
) -> String {
    use crate::scalar_function::dynamic_diagnostic::DiagnosticStep;
    let result = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut state = plan.state();
        let mut step = plan.step(&mut state).unwrap();
        loop {
            match step {
                DiagnosticStep::Done(text) => return text,
                DiagnosticStep::NeedConstant(index) => {
                    let Expression::Constant(c) = bindings[index] else {
                        unreachable!()
                    };
                    let value = c.eval_in(ctx).ok();
                    step = plan.resume(&mut state, index, value.as_ref()).unwrap();
                }
            }
        }
    })) {
        Ok(v) => format!("{v:?}"),
        Err(p) => format!(
            "PANIC:{:?}",
            p.downcast_ref::<String>()
                .map(String::as_str)
                .or_else(|| p.downcast_ref::<&str>().copied())
        ),
    };
    format!("{result}|{}", ctx.snapshot())
}
#[test]
fn typed_dynamic_independent_diagnostic_plans_before_retirement() {
    if isolated("typed_dynamic_independent_diagnostic_plans_before_retirement") {
        return;
    }
    use crate::scalar_function::dynamic_diagnostic::DynamicDiagnostic;
    let (mut count, mut math_hash) = (0usize, 0xcbf29ce484222325u64);
    for which in 0..15 {
        for profile in 0..4 {
            for (si, script) in scripts().into_iter().enumerate() {
                for float in [false, true] {
                    let original = diagnostic_expression(which, &script[0]);
                    let separate = diagnostic_expression(which, &script[0]);
                    let old = Context::new(profile, script.clone());
                    let new = Context::new(profile, script.clone());
                    let expected =
                        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            crate::scalar_function::numeric_expression_text(&original, float, &old)
                        })) {
                            Ok(v) => format!("{v:?}"),
                            Err(p) => format!(
                                "PANIC:{:?}",
                                p.downcast_ref::<String>()
                                    .map(String::as_str)
                                    .or_else(|| p.downcast_ref::<&str>().copied())
                            ),
                        };
                    let (plan, bindings) =
                        DynamicDiagnostic::capture_numeric(&separate, float, 4096, 4 << 20)
                            .unwrap();
                    let got = plan_outcome(&plan, &bindings, &new);
                    assert_eq!(
                        got,
                        format!("{expected}|{}", old.snapshot()),
                        "numeric {which} p={profile} s={si} float={float}"
                    );
                    count += 1;
                }
                for name in ["abs", "exp", "cot"] {
                    let make = || {
                        proof::call(
                            name,
                            FieldType::new(FieldTypeCode::Double),
                            vec![diagnostic_expression(which, &script[0])],
                        )
                    };
                    let Expression::ScalarFunction(original) = make() else {
                        unreachable!()
                    };
                    let Expression::ScalarFunction(separate) = make() else {
                        unreachable!()
                    };
                    let old = Context::new(profile, script.clone());
                    let new = Context::new(profile, script.clone());
                    let expected =
                        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            crate::scalar_function::dynamic_math_diagnostic_for_test(
                                &original, &old,
                            )
                        })) {
                            Ok(v) => format!("{v:?}"),
                            Err(p) => format!(
                                "PANIC:{:?}",
                                p.downcast_ref::<String>()
                                    .map(String::as_str)
                                    .or_else(|| p.downcast_ref::<&str>().copied())
                            ),
                        };
                    let (plan, bindings) =
                        DynamicDiagnostic::capture_math(&separate, 4096, 4 << 20).unwrap();
                    assert_eq!(
                        plan_outcome(&plan, &bindings, &new),
                        format!("{expected}|{}", old.snapshot()),
                        "math {name} {which} p={profile} s={si}"
                    );
                    fold(
                        &mut math_hash,
                        &format!(
                            "{which}|{profile}|{si}|{name}|{expected}|{}",
                            old.snapshot()
                        ),
                    );
                    count += 1;
                }
            }
        }
    }
    assert_eq!(count, 7200);
    assert_eq!(math_hash, 0xBC1F79F0E8CB417D);
    eprintln!(
        "TYPED DYNAMIC INDEPENDENT DIAGNOSTIC BEFORE RETIREMENT {count} math4320/{math_hash:016X}"
    );
}

pub(crate) fn optional_root(kind: &str, value: &Datum) -> Expression {
    if kind == "literal" {
        let mut c = Constant::new(value.clone(), proof::int_type());
        c.ret_type = None;
        Expression::Constant(c)
    } else {
        proof::expression(
            kind,
            0,
            value,
            None,
            Some(Arc::new(RwLock::new(value.clone()))),
        )
    }
}
#[test]
fn typed_dynamic_original_optional_root_declarations_and_caller_layouts() {
    if isolated("typed_dynamic_original_optional_root_declarations_and_caller_layouts") {
        return;
    }
    let (mut count, mut hash) = (0usize, 0xcbf29ce484222325u64);
    for kind in std::iter::once("literal").chain(proof::KINDS) {
        for output_field in super::eager_scalar_value_tests::types() {
            for profile in 0..4 {
                for (vi, value) in super::session_read_tests::values().into_iter().enumerate() {
                    for (sel_id, selection) in [None, Some(vec![2, 0, 2]), Some(vec![])]
                        .into_iter()
                        .enumerate()
                    {
                        for enabled in [false, true] {
                            let owner = crate::evaluator::EvaluatorSuite::new(
                                vec![optional_root(kind, &value)],
                                false,
                            );
                            let ctx = Context::new(profile, vec![value.clone()]);
                            ctx.inner.enabled.set(enabled);
                            let mut input = proof::input(true);
                            input.set_sel(selection.clone());
                            let mut output = tidb_chunk::chunk::Chunk::new_with_capacity(
                                std::slice::from_ref(&output_field),
                                0,
                            );
                            output.append_null(0);
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
                            fold(&mut hash,&format!("{kind}|{output_field:?}|{profile}|{vi}|{sel_id}|{enabled}|{result}|{}|{}",format!("{:?}|rows:{}", output.column(0), output.num_rows()),ctx.snapshot()));
                            count += 1;
                        }
                    }
                }
            }
        }
    }
    assert_eq!((count, hash), (50160, 0xA9F134CB35861E1F));
    eprintln!("TYPED DYNAMIC ORIGINAL OPTIONAL ROOT {count} {hash:016X}");
}

#[test]
fn typed_dynamic_shared_frozen_column_index() {
    let mut cases = 0;
    for columns in [0, 1, 3] {
        for nullable in [false, true] {
            let fields = vec![proof::int_type(); columns];
            let mut input = tidb_chunk::chunk::Chunk::new_with_capacity(&fields, 3);
            if columns == 0 {
                input.set_num_virtual_rows(3);
            } else {
                for r in 0..3 {
                    for c in 0..columns {
                        if nullable && r == 1 {
                            input.append_null(c);
                        } else {
                            input.append_int64(c, (r * 10 + c) as i64);
                        }
                    }
                }
            }
            for row in std::iter::once(tidb_chunk::row::Row::empty())
                .chain((0..3).map(|i| input.get_row(i)))
            {
                for missing in [false, true] {
                    for index in [-1, 0, 1, 3, i64::MAX] {
                        let mut c = crate::column::Column::new(1, proof::int_type());
                        c.index = index;
                        if missing {
                            c.ret_type = None;
                        }
                        let original = c.eval(row).map(|_| usize::try_from(index).unwrap());
                        let candidate =
                            crate::column::column_read_index(!missing, index, row.len());
                        assert_eq!(
                            candidate, original,
                            "columns={columns} nullable={nullable} missing={missing} index={index}"
                        );
                        cases += 1;
                    }
                }
            }
        }
    }
    assert_eq!(cases, 240);
    eprintln!("TYPED DYNAMIC COLUMN INDEX SHARED FROZEN {cases}");
}
