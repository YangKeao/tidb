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

#[test]
fn eager_policy_registry_and_bad_arities_match_frozen_results() {
    let (mut count, mut common, mut hash) = (0usize, 0usize, 0xcbf29ce484222325u64);
    let names = names();
    for name in &names {
        for n in arities(name) {
            let reached = argument_policy::classify(&function(name, n))
                == argument_policy::ArgumentPolicy::CommonEager;
            common += usize::from(reached);
            for b in format!("{name}|{n}|{reached}\n").bytes() {
                hash = (hash ^ u64::from(b)).wrapping_mul(0x100000001b3);
            }
            count += 1;
        }
    }
    assert_eq!(
        (count, common, names.len(), hash),
        (2308, 2036, 328, 0xBC3F1FF91A278C16)
    );
}
