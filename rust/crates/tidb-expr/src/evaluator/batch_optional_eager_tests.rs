// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Actual absent declarations are retained; transport bytes are not SQL types.
use super::*;
use crate::tikv::dynamic_leaf_tests as leaf;
use std::sync::RwLock;
const NAMES: [&str; 4] = ["reverse", "bin", "json_array", "json_object"];
const KINDS: [&str; 6] = [
    "literal",
    "param",
    "deferred_effect",
    "correlated",
    "correlated_absent",
    "column_missing_type",
];
fn output_field(name: &str) -> FieldType {
    if name.starts_with("json_") {
        FieldType::new(FieldTypeCode::Json)
    } else {
        string_ty()
    }
}
fn make(
    name: &str,
    mask: u8,
    kind: &str,
    value: &Datum,
    profile: usize,
    enabled: bool,
) -> (EvaluatorSuite, leaf::Context) {
    let mut context = leaf::Context::new(profile, value.clone());
    context.enabled.set(enabled);
    if kind.starts_with("correlated") {
        context.binding = Some(Arc::new(RwLock::new(value.clone())));
    }
    let field = crate::tikv::misc_identity_value_tests::field(value);
    let declared = (mask & 6 == 0).then_some(&field);
    let arg = match kind {
        "literal" => {
            let mut c = Constant::new(value.clone(), field.clone());
            c.ret_type = declared.cloned();
            Expression::Constant(c)
        }
        "column_missing_type" => {
            let mut c = Column::new(1, ty());
            c.index = 0;
            c.ret_type = None;
            Expression::Column(c)
        }
        _ => leaf::expression(kind, 0, value, declared, context.binding.clone()),
    };
    let args = if name == "json_object" {
        let mut c = Constant::new(Datum::new_string("k"), string_ty());
        if mask & 4 != 0 {
            c.ret_type = None;
        }
        vec![Expression::Constant(c), arg]
    } else {
        vec![arg]
    };
    let mut f = ScalarFunction::new(CiString::new(name), output_field(name), args);
    if mask & 1 != 0 {
        f.ret_type = None;
    }
    (suite(Expression::ScalarFunction(f)), context)
}
#[test]
fn optional_eager_original_metadata_demands_and_prefixes() {
    if isolate_common_corpus(
        "optional_eager_tests::optional_eager_original_metadata_demands_and_prefixes",
    ) {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    let (mut count, mut hash) = (0usize, 0xcbf29ce484222325u64);
    for name in NAMES {
        for mask in 0..8 {
            for kind in KINDS {
                for (vi, value) in crate::tikv::byte_unary_value_tests::values()
                    .into_iter()
                    .enumerate()
                {
                    for profile in 0..4 {
                        for enabled in [false, true] {
                            for selection in [None, Some(vec![2, 0, 2]), Some(vec![])] {
                                let (owner, context) =
                                    make(name, mask, kind, &value, profile, enabled);
                                let input = || {
                                    let mut c = chunk(
                                        &[ty()],
                                        &[
                                            vec![Datum::Int(1)],
                                            vec![Datum::Int(2)],
                                            vec![Datum::Int(3)],
                                        ],
                                    );
                                    c.set_sel(selection.clone());
                                    c
                                };
                                let mut a = input();
                                let mut left = output_for(&output_field(name));
                                left.append_null(0);
                                let expected =
                                    session_read_outcome(|| owner.run(&context, &mut a, &mut left));
                                let label = format!(
                                    "{name}/{mask}/{kind}/{vi}/{profile}/{enabled}/{selection:?}"
                                );
                                {
                                    let (actual, new) =
                                        make(name, mask, kind, &value, profile, enabled);
                                    let mut b = input();
                                    let mut right = output_for(&output_field(name));
                                    right.append_null(0);
                                    let got = session_read_outcome(|| {
                                        actual.run_with_tikv_batch(
                                            &new,
                                            &mut b,
                                            &mut right,
                                            &[ty()],
                                        )
                                    });
                                    if got != expected
                                        || right.column(0) != left.column(0)
                                        || new.snapshot() != context.snapshot()
                                    {
                                        eprintln!("OPTIONAL EAGER DIFFERENCE {label} old={expected:?} new={got:?} oldeffects={} neweffects={}",context.snapshot(),new.snapshot());
                                    }
                                    assert_eq!(got, expected, "outcome {label}");
                                    assert_eq!(right.column(0), left.column(0), "column {label}");
                                    assert_eq!(
                                        new.snapshot(),
                                        context.snapshot(),
                                        "effects {label}"
                                    );
                                    let route = actual.batch_route_snapshot();
                                    assert_eq!(
                                        (route.fallback, route.refusals),
                                        (0, 0),
                                        "source {label}: {route:?}"
                                    );
                                }
                                for byte in format!(
                                    "{label}|{expected}|{:?}|{}",
                                    left.column(0),
                                    context.snapshot()
                                )
                                .bytes()
                                {
                                    hash = (hash ^ u64::from(byte)).wrapping_mul(0x100000001b3);
                                }
                                count += 1;
                            }
                        }
                    }
                }
            }
        }
    }
    eprintln!("OPTIONAL EAGER ORIGINAL {count}/{hash:016X}");
    assert_eq!((count, hash), (686592, 0x5EC04B9A028B7011));
}
#[test]
fn optional_eager_actual_absence_does_not_fallback_or_panic_in_preparation() {
    let mut gaps = vec![];
    for name in NAMES {
        for mask in [1, 2, 3, 4, 7] {
            for kind in ["literal", "param", "column_missing_type"] {
                let (actual, context) = make(name, mask, kind, &Datum::new_string("abc"), 0, true);
                let mut input = chunk(&[ty()], &[vec![Datum::Int(1)]]);
                let mut output = output_for(&output_field(name));
                let outcome = session_read_outcome(|| {
                    actual.run_with_tikv_batch(&context, &mut input, &mut output, &[ty()])
                });
                let route = actual.batch_route_snapshot();
                if route.prepared != 1 || route.fallback != 0 || route.refusals != 0 {
                    gaps.push((name, mask, kind, route, outcome));
                }
            }
        }
    }
    assert!(gaps.is_empty(), "optional eager source gaps: {gaps:?}");
}

fn registry_output(name: &str) -> FieldType {
    if matches!(
        name,
        "bit_length"
            | "json_depth"
            | "json_valid"
            | "json_storage_size"
            | "json_storage_free"
            | "json_length"
            | "json_contains"
            | "json_contains_path"
    ) {
        ty()
    } else if name.starts_with("json_")
        && !matches!(
            name,
            "json_quote" | "json_unquote" | "json_pretty" | "json_type"
        )
    {
        FieldType::new(FieldTypeCode::Json)
    } else {
        string_ty()
    }
}
#[test]
fn optional_eager_preferred_registry_arity_and_original_metadata_match() {
    if isolate_common_corpus(
        "optional_eager_tests::optional_eager_preferred_registry_arity_and_original_metadata_match",
    ) {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    use crate::scalar_function::{eager_policy_tests as policy, eager_values};
    let (mut count, mut hash, mut families) = (0usize, 0xcbf29ce484222325u64, 0usize);
    for name in policy::names() {
        if !eager_values::prefers_common_body(&policy::function(&name, 0)) {
            continue;
        }
        families += 1;
        for arity in policy::arities(&name) {
            for mask in [1, 2, 3, 7] {
                for dynamic in [false, true] {
                    let mut args = vec![];
                    for _ in 0..arity {
                        let mut c = Constant::new(
                            if dynamic {
                                Datum::new_string("stored")
                            } else {
                                Datum::Null
                            },
                            string_ty(),
                        );
                        if dynamic {
                            c.param_marker = Some(ParamMarker { order: 0 });
                        }
                        if mask & 6 != 0 {
                            c.ret_type = None;
                        }
                        args.push(Expression::Constant(c));
                    }
                    let field = registry_output(&name);
                    let mut f = ScalarFunction::new(CiString::new(&name), field.clone(), args);
                    if mask & 1 != 0 {
                        f.ret_type = None;
                    }
                    let original = suite(Expression::ScalarFunction(f.clone()));
                    let actual = suite(Expression::ScalarFunction(f));
                    for enabled in [false, true] {
                        for selection in [None, Some(vec![2, 0, 2]), Some(vec![])] {
                            let old = leaf::Context::new(0, Datum::new_string("abc"));
                            old.enabled.set(enabled);
                            let new = leaf::Context::new(0, Datum::new_string("abc"));
                            new.enabled.set(enabled);
                            let input = || {
                                let mut c = chunk(
                                    &[ty()],
                                    &[
                                        vec![Datum::Int(1)],
                                        vec![Datum::Int(2)],
                                        vec![Datum::Int(3)],
                                    ],
                                );
                                c.set_sel(selection.clone());
                                c
                            };
                            let (mut a, mut b) = (input(), input());
                            let (mut left, mut right) = (output_for(&field), output_for(&field));
                            left.append_null(0);
                            right.append_null(0);
                            let expected =
                                session_read_outcome(|| original.run(&old, &mut a, &mut left));
                            let got = session_read_outcome(|| {
                                actual.run_with_tikv_batch(&new, &mut b, &mut right, &[ty()])
                            });
                            let label =
                                format!("{name}/{arity}/{mask}/{dynamic}/{enabled}/{selection:?}");
                            if got != expected
                                || left.column(0) != right.column(0)
                                || old.snapshot() != new.snapshot()
                            {
                                eprintln!("OPTIONAL REGISTRY DIFFERENCE {label} old={expected:?} new={got:?} oldeffects={} neweffects={}",old.snapshot(),new.snapshot());
                            }
                            assert_eq!(got, expected, "outcome {label}");
                            assert_eq!(left.column(0), right.column(0), "column {label}");
                            assert_eq!(old.snapshot(), new.snapshot(), "effects {label}");
                            let route = actual.batch_route_snapshot();
                            assert_eq!(
                                (route.fallback, route.refusals),
                                (0, 0),
                                "source {label}: {route:?}"
                            );
                            for byte in format!(
                                "{label}|{expected}|{:?}|{}",
                                left.column(0),
                                old.snapshot()
                            )
                            .bytes()
                            {
                                hash = (hash ^ u64::from(byte)).wrapping_mul(0x100000001b3);
                            }
                            count += 1;
                        }
                    }
                }
            }
        }
    }
    eprintln!("OPTIONAL REGISTRY ORIGINAL {families}/{count}/{hash:016X}");
    assert_eq!((families, count, hash), (52, 17808, 0x96FBFCB7F4B38305));
}

#[test]
fn optional_common_profiles_record_original_and_actual_admission() {
    if isolate_common_corpus(
        "optional_eager_tests::optional_common_profiles_record_original_and_actual_admission",
    ) {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    use crate::scalar_function::{eager_policy_tests as policy, eager_values};
    let (mut count, mut hash, mut profiles) = (0usize, 0xcbf29ce484222325u64, 0usize);
    let mut gaps = vec![];
    for name in policy::names() {
        for arity in policy::arities(&name) {
            let signature = policy::function(&name, arity);
            if !eager_values::supports(&signature) || eager_values::prefers_common_body(&signature)
            {
                continue;
            }
            // Entropy-producing valid signatures need a separate contract
            // oracle; two real OS draws cannot be compared byte-for-byte.
            if (matches!(name.as_str(), "uuid" | "uuid_v4" | "uuid_v7") && arity == 0)
                || (name == "random_bytes" && arity == 1)
            {
                continue;
            }
            profiles += 1;
            for mask in [1, 2, 3, 7] {
                for (vi, value) in [
                    Datum::Null,
                    Datum::Int(3),
                    Datum::new_string("abc"),
                    Datum::new_string("[1]"),
                ]
                .into_iter()
                .enumerate()
                {
                    for kind in ["literal", "param", "deferred_effect", "correlated"] {
                        for enabled in [false, true] {
                            for selection in [None, Some(vec![2, 0, 2]), Some(vec![])] {
                                let make = || {
                                    let mut ctx = leaf::Context::new(0, value.clone());
                                    ctx.enabled.set(enabled);
                                    ctx.binding = Some(Arc::new(RwLock::new(value.clone())));
                                    let actual_field =
                                        crate::tikv::misc_identity_value_tests::field(&value);
                                    let declared = (mask & 6 == 0).then_some(&actual_field);
                                    let args = (0..arity)
                                        .map(|_| {
                                            if kind == "literal" {
                                                let mut c = Constant::new(
                                                    value.clone(),
                                                    actual_field.clone(),
                                                );
                                                c.ret_type = declared.cloned();
                                                Expression::Constant(c)
                                            } else {
                                                leaf::expression(
                                                    kind,
                                                    0,
                                                    &value,
                                                    declared,
                                                    ctx.binding.clone(),
                                                )
                                            }
                                        })
                                        .collect();
                                    let field = registry_output(&name);
                                    let mut f = ScalarFunction::new(
                                        CiString::new(&name),
                                        field.clone(),
                                        args,
                                    );
                                    if mask & 1 != 0 {
                                        f.ret_type = None;
                                    }
                                    (suite(Expression::ScalarFunction(f)), ctx, field)
                                };
                                let input = || {
                                    let mut c = chunk(
                                        &[ty()],
                                        &[
                                            vec![Datum::Int(1)],
                                            vec![Datum::Int(2)],
                                            vec![Datum::Int(3)],
                                        ],
                                    );
                                    c.set_sel(selection.clone());
                                    c
                                };
                                let (owner, old, field) = make();
                                let mut left = output_for(&field);
                                left.append_null(0);
                                let expected = session_read_outcome(|| {
                                    owner.run(&old, &mut input(), &mut left)
                                });
                                let label = format!(
                                    "{name}/{arity}/{mask}/{vi}/{kind}/{enabled}/{selection:?}"
                                );
                                {
                                    let (actual, new, _) = make();
                                    let mut right = output_for(&field);
                                    right.append_null(0);
                                    let got = session_read_outcome(|| {
                                        actual.run_with_tikv_batch(
                                            &new,
                                            &mut input(),
                                            &mut right,
                                            &[ty()],
                                        )
                                    });
                                    if got != expected
                                        || right.column(0) != left.column(0)
                                        || new.snapshot() != old.snapshot()
                                    {
                                        eprintln!("OPTIONAL COMMON DIFFERENCE {label} old={expected:?} new={got:?} oldcolumn={:?} newcolumn={:?} oldeffects={} neweffects={}",left.column(0),right.column(0),old.snapshot(),new.snapshot());
                                    }
                                    assert_eq!(got, expected, "outcome {label}");
                                    assert_eq!(right.column(0), left.column(0), "column {label}");
                                    assert_eq!(new.snapshot(), old.snapshot(), "effects {label}");
                                    let route = actual.batch_route_snapshot();
                                    if route.prepared != 1
                                        || route.fallback != 0
                                        || route.refusals != 0
                                    {
                                        if gaps.len() < 30 {
                                            gaps.push(format!("{label}: {route:?}"));
                                        }
                                    }
                                }
                                for byte in format!(
                                    "{label}|{expected}|{:?}|{}",
                                    left.column(0),
                                    old.snapshot()
                                )
                                .bytes()
                                {
                                    hash = (hash ^ u64::from(byte)).wrapping_mul(0x100000001b3);
                                }
                                count += 1;
                            }
                        }
                    }
                }
            }
        }
    }
    eprintln!("OPTIONAL COMMON ORIGINAL {profiles}/{count}/{hash:016X}");
    assert_eq!((profiles, count, hash), (1713, 657792, 0xA94C463994D2D98D));
    if !gaps.is_empty() {
        eprintln!("OPTIONAL COMMON ADMISSION RED {gaps:?}");
    }
    assert!(gaps.is_empty(), "optional common admission gaps: {gaps:?}");
}

#[test]
fn optional_common_entropy_keeps_actual_generation_and_error_contracts() {
    let mut gaps = 0;
    let mut cases = 0;
    for name in ["uuid", "uuid_v4", "uuid_v7", "random_bytes"] {
        for mask in [1, 2, 3, 7] {
            for value in [
                Datum::Null,
                Datum::Int(-1),
                Datum::Int(0),
                Datum::Int(32),
                Datum::Int(1024),
                Datum::Int(1025),
            ] {
                for dynamic in [false, true] {
                    for enabled in [false, true] {
                        for selection in [None, Some(vec![2, 0, 2]), Some(vec![])] {
                            let make = || {
                                let context = leaf::Context::new(0, value.clone());
                                context.enabled.set(enabled);
                                let mut arg = Constant::new(value.clone(), ty());
                                if dynamic {
                                    arg.param_marker = Some(ParamMarker { order: 0 });
                                }
                                if mask & 6 != 0 {
                                    arg.ret_type = None;
                                }
                                let args = if name == "random_bytes" {
                                    vec![Expression::Constant(arg)]
                                } else {
                                    vec![]
                                };
                                let mut f =
                                    ScalarFunction::new(CiString::new(name), string_ty(), args);
                                if mask & 1 != 0 {
                                    f.ret_type = None;
                                }
                                (suite(Expression::ScalarFunction(f)), context)
                            };
                            let input = || {
                                let mut c = chunk(
                                    &[ty()],
                                    &[
                                        vec![Datum::Int(1)],
                                        vec![Datum::Int(2)],
                                        vec![Datum::Int(3)],
                                    ],
                                );
                                c.set_sel(selection.clone());
                                c
                            };
                            let label = format!(
                                "{name}/{mask}/{value:?}/{dynamic}/{enabled}/{selection:?}"
                            );
                            let validate =
                                |output: &Chunk, outcome: &std::result::Result<(), EvaluatorError>| {
                                    assert!(output.column(0).is_null(0), "prefix {label}");
                                    let empty = selection.as_ref().is_some_and(Vec::is_empty);
                                    let random_null = name == "random_bytes" && value.is_null();
                                    let good_random = matches!(value, Datum::Int(32 | 1024));
                                    let success = empty
                                        || name != "random_bytes"
                                        || random_null
                                        || good_random;
                                    assert_eq!(outcome.is_ok(), success, "{label}: {outcome:?}");
                                    assert_eq!(
                                        output.num_rows(),
                                        if !empty && success { 4 } else { 1 },
                                        "{label}"
                                    );
                                    let mut actual_values = std::collections::HashSet::new();
                                    for row in 1..output.num_rows() {
                                        assert_eq!(
                                            output.column(0).is_null(row),
                                            random_null,
                                            "{label}"
                                        );
                                        if random_null {
                                            continue;
                                        }
                                        let column = output.column(0);
                                        let bytes = column.get_bytes(row);
                                        let bytes = bytes.as_ref();
                                        if name == "random_bytes" {
                                            let Datum::Int(length) = value else { panic!() };
                                            assert_eq!(bytes.len(), length as usize, "{label}");
                                        } else {
                                            assert_eq!(bytes.len(), 36, "{label}");
                                            for i in 0..36 {
                                                if [8, 13, 18, 23].contains(&i) {
                                                    assert_eq!(bytes[i], b'-', "{label}");
                                                } else {
                                                    assert!(
                                                        bytes[i].is_ascii_hexdigit(),
                                                        "{label}"
                                                    );
                                                }
                                            }
                                            assert_eq!(
                                                bytes[14],
                                                match name {
                                                    "uuid" => b'1',
                                                    "uuid_v4" => b'4',
                                                    _ => b'7',
                                                },
                                                "{label}"
                                            );
                                            assert!(
                                                matches!(bytes[19], b'8' | b'9' | b'a' | b'b'),
                                                "{label}"
                                            );
                                        }
                                        // Reverse/duplicate physical selections still
                                        // invoke the genuine generator per occurrence.
                                        assert!(
                                            actual_values.insert(bytes.to_vec()),
                                            "reused entropy {label}"
                                        );
                                    }
                                };
                            let (owner, old) = make();
                            let mut left = output_for(&string_ty());
                            left.append_null(0);
                            let expected = owner.run(&old, &mut input(), &mut left);
                            validate(&left, &expected);
                            {
                                let (actual, new) = make();
                                let mut right = output_for(&string_ty());
                                right.append_null(0);
                                let got = actual.run_with_tikv_batch(
                                    &new,
                                    &mut input(),
                                    &mut right,
                                    &[ty()],
                                );
                                validate(&right, &got);
                                assert_eq!(got, expected, "{label}");
                                assert_eq!(new.snapshot(), old.snapshot(), "{label}");
                                let route = actual.batch_route_snapshot();
                                if route.prepared != 1 || route.fallback != 0 || route.refusals != 0
                                {
                                    gaps += 1;
                                }
                            }
                            cases += 1;
                        }
                    }
                }
            }
        }
    }
    eprintln!("OPTIONAL COMMON ACTUAL ENTROPY {cases}, admission gaps {gaps}");
    assert_eq!(cases, 1152);
    assert_eq!(gaps, 0);
}
