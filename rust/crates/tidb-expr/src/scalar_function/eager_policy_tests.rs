// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Runtime observation of the original early/common-body choice, before broad source admission.
use super::*;
use std::collections::BTreeSet;

pub(crate) fn names() -> BTreeSet<String> {
    let mut names = crate::builtin_registry::builtin_list()
        .into_iter()
        .collect::<BTreeSet<_>>();
    for name in [
        "__unregistered_eager__",
        "unknown_in_union",
        "cast_unknown",
        "cast_unknown_in_union",
        "date_add_invalid_unit",
        "date_sub_invalid_unit",
        "ltrim_with",
        "rtrim_with",
        "getvar_unknown",
        "getvar_int",
        "getvar_real",
        "getvar_decimal",
        "getvar_string",
        "getvar_time",
        "getvar_duration",
        "getvar_json",
        "unaryplus",
        "unaryminus",
        "bitneg",
        "logicand",
        "logicor",
        "logicxor",
        "plus",
        "minus",
        "mul",
        "div",
        "intdiv",
        "mod",
        "eq",
        "ne",
        "lt",
        "le",
        "gt",
        "ge",
        "nulleq",
        "isnull",
        "isnotnull",
        "istrue",
        "isfalse",
        "isunknown",
        "isnotunknown",
        "istrue_with_null",
        "coercibility",
        "name_const",
        "timestampdiff",
        "sig_timestampdiff",
    ] {
        names.insert(name.to_string());
    }
    names
}
pub(crate) fn arities(name: &str) -> BTreeSet<usize> {
    let mut values = (0..=6).collect::<BTreeSet<_>>();
    if let Some((_, a)) = crate::builtin_registry::function_class(name) {
        for n in [
            a.min_args,
            a.min_args.saturating_add(1),
            a.max_args.unwrap_or(a.min_args + 3),
            a.max_args.unwrap_or(a.min_args + 3).saturating_add(1),
        ] {
            values.insert(n.min(8));
        }
    }
    values
}
pub(crate) fn function(name: &str, n: usize) -> ScalarFunction {
    ScalarFunction::new(
        tidb_ast::CiString::new(name),
        FieldType::new(tidb_datatype::FieldTypeCode::VarString),
        (0..n)
            .map(|_| {
                Expression::Constant(crate::constant::Constant::new(
                    Datum::Null,
                    FieldType::new(tidb_datatype::FieldTypeCode::Null),
                ))
            })
            .collect(),
    )
}
pub(crate) fn original_common(name: &str, n: usize) -> bool {
    let f = function(name, n);
    let before = eager_value_phases_for_test();
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        f.eval(&crate::NoColumns, Row::empty())
    }));
    let count = eager_value_phases_for_test() - before;
    assert!(
        count <= 1,
        "nested common-body execution for {name}/{n}: {count}"
    );
    count == 1
}
fn isolated(which: &str) -> bool {
    const KEY: &str = "TIDB_EAGER_POLICY_ORACLE";
    if std::env::var(KEY).as_deref() == Ok(which) {
        return false;
    }
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            &format!("scalar_function::eager_policy_tests::{which}"),
            "--nocapture",
        ])
        .env(KEY, which)
        .output()
        .unwrap();
    eprintln!("{}", String::from_utf8_lossy(&output.stderr));
    eprintln!("{}", String::from_utf8_lossy(&output.stdout));
    assert!(output.status.success());
    true
}
#[test]
fn eager_policy_original_registry_phase_observations() {
    if isolated("eager_policy_original_registry_phase_observations") {
        return;
    }

    let (mut count, mut common, mut hash) = (0usize, 0usize, 0xcbf29ce484222325u64);
    let names = names();
    for name in &names {
        for n in arities(name) {
            let reached = original_common(name, n);
            common += usize::from(reached);
            let record = format!("{name}|{n}|{reached}\n");
            for b in record.bytes() {
                hash = (hash ^ u64::from(b)).wrapping_mul(0x100000001b3);
            }
            count += 1;
        }
    }
    assert_eq!(
        (count, common, names.len(), hash),
        (2308, 2036, 328, 0xBC3F1FF91A278C16)
    );
    eprintln!(
        "EAGER POLICY ORIGINAL count={count} common={common} names={} {hash:016X}",
        names.len()
    );
}

#[test]
fn eager_policy_candidate_matches_executed_original_registry_and_bad_arities() {
    if isolated("eager_policy_candidate_matches_executed_original_registry_and_bad_arities") {
        return;
    }
    let mut pairs = 0;
    for name in names() {
        for n in arities(&name) {
            let actual = argument_policy::classify(&function(&name, n));
            let original = original_common(&name, n);
            assert_eq!(
                actual == argument_policy::ArgumentPolicy::CommonEager,
                original,
                "{name}/{n}: {actual:?}"
            );
            pairs += 1;
        }
    }
    assert_eq!(pairs, 2308);
    eprintln!("EAGER POLICY INDEPENDENT ORIGINAL CLASSIFICATION PAIRS {pairs}");
}
