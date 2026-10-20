// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Original early string demand, type signatures and collation observations.
use crate::{
    constant::Constant, expression::Expression, scalar_function::ScalarFunction, Datum, EvalError,
};
use tidb_chunk::row::Row;
use tidb_datatype::{Collation, FieldType, FieldTypeCode};
pub(crate) fn recipes() -> Vec<(&'static str, usize)> {
    let mut all = vec![
        ("length", 1),
        ("octet_length", 1),
        ("char_length", 1),
        ("character_length", 1),
        ("trim", 2),
        ("locate", 2),
        ("locate", 3),
        ("instr", 2),
        ("strcmp", 2),
        ("field", 2),
        ("field", 3),
    ];
    for name in ["ltrim_with", "rtrim_with"] {
        for n in 0..=3 {
            all.push((name, n));
        }
    }
    all
}
pub(crate) fn declarations() -> Vec<FieldType> {
    vec![
        FieldType::new(FieldTypeCode::VarString).with_collation(Collation::Binary),
        FieldType::new(FieldTypeCode::VarString).with_collation(Collation::Utf8Mb4Bin),
        FieldType::new(FieldTypeCode::VarString).with_collation(Collation::Utf8Mb4GeneralCi),
        FieldType::new(FieldTypeCode::LongLong).with_unsigned(true),
        FieldType::new(FieldTypeCode::NewDecimal)
            .with_flen(65)
            .with_decimal(6),
    ]
}
pub(crate) fn inputs(name: &str, n: usize) -> Vec<Vec<Datum>> {
    let text = Datum::new_string;
    let base = match name {
        "length" | "octet_length" | "char_length" | "character_length" => vec![text("éA中")],
        "trim" | "ltrim_with" | "rtrim_with" => (0..n)
            .map(|i| if i == 0 { text(" éAé ") } else { text(" é") })
            .collect(),
        "locate" if n == 3 => vec![text("a"), text("Aéa"), Datum::Int(2)],
        "locate" => vec![text("a"), text("Aéa")],
        "instr" => vec![text("Aéa"), text("a")],
        "strcmp" => vec![text("A"), text("a")],
        "field" => (0..n)
            .map(|i| {
                text(if i == 0 {
                    "a"
                } else if i == 1 {
                    "A"
                } else {
                    "é"
                })
            })
            .collect(),
        _ => unreachable!(),
    };
    let mut values = super::session_read_tests::values();
    values.extend([
        text(" a "),
        text("a,A,é"),
        text("12junk"),
        text("e\u{301}"),
        text("a\0b"),
    ]);
    let mut inputs = vec![base.clone()];
    for index in 0..n {
        for value in &values {
            let mut args = base.clone();
            args[index] = value.clone();
            inputs.push(args);
        }
    }
    inputs
}
pub(crate) fn function(
    name: &str,
    values: &[Datum],
    decl: &FieldType,
    ret: &FieldType,
) -> ScalarFunction {
    ScalarFunction::new(
        tidb_ast::CiString::new(name),
        ret.clone(),
        values
            .iter()
            .cloned()
            .map(|v| Expression::Constant(Constant::new(v, decl.clone())))
            .collect(),
    )
}
pub(crate) fn observe(
    f: impl FnOnce() -> Result<Datum, EvalError>,
    ctx: &super::user_variable_tests::Context,
) -> Vec<u8> {
    super::user_variable_tests::observe(f, ctx)
}
fn fold(hash: &mut u64, bytes: &[u8]) {
    for &b in bytes {
        *hash = (*hash ^ u64::from(b)).wrapping_mul(0x100000001b3);
    }
}
pub(crate) fn isolated(which: &str) -> bool {
    const KEY: &str = "TIDB_EARLY_STRING_ORIGINAL";
    if std::env::var(KEY).as_deref() == Ok(which) {
        return false;
    }
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            &format!("tikv::early_string_tests::{which}"),
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
fn early_string_original_observations() {
    if isolated("early_string_original_observations") {
        return;
    }
    let now = chrono::DateTime::parse_from_rfc3339("2026-10-20T00:00:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc);
    tidb_datatype::with_test_conversion_clock(now, || {
        let (mut count, mut hash) = (0usize, 0xcbf29ce484222325u64);
        for (name, n) in recipes() {
            for decl in declarations() {
                for ret in super::eager_scalar_value_tests::types() {
                    for profile in 0..3 {
                        for (index, values) in inputs(name, n).into_iter().enumerate() {
                            let ctx =
                                super::user_variable_tests::Context::new(profile, Datum::Int(0));
                            let f = function(name, &values, &decl, &ret);
                            let result = observe(|| f.eval(&ctx, Row::empty()), &ctx);
                            fold(
                                &mut hash,
                                format!("{name}|{n}|{decl:?}|{ret:?}|{profile}|{index}|")
                                    .as_bytes(),
                            );
                            fold(&mut hash, &result);
                            count += 1;
                        }
                    }
                }
            }
        }
        assert_eq!((count, hash), (129855, 0xE1A91F4419B7C00E));
        eprintln!("EARLY STRING ORIGINAL {count} {hash:016X}");
    });
}

// Test-only original-demand harness; production metadata has no expression callback.
fn candidate(
    function: &ScalarFunction,
    ctx: &super::user_variable_tests::Context,
) -> Result<Datum, EvalError> {
    let _ = function
        .identity_constant_probe()
        .and_then(|value| value.sql_string().ok());
    let selected =
        crate::scalar_function::early_string::EarlyString::for_function(function).unwrap();
    let count = selected.demanded_arguments(function.args.len());
    let values = function.args[..count]
        .iter()
        .map(|arg| arg.eval(ctx, Row::empty()))
        .collect::<Result<Vec<_>, _>>()?;
    let value = selected.eval(ctx, values)?;
    crate::scalar_function::coerce_function_result(
        function.func_name.lowercase(),
        function.get_static_type(),
        value,
    )
}
#[test]
fn early_string_shared_frozen_regression() {
    if isolated("early_string_shared_frozen_regression") {
        return;
    }
    let now = chrono::DateTime::parse_from_rfc3339("2026-10-20T00:00:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc);
    tidb_datatype::with_test_conversion_clock(now, || {
        let (mut count, mut hash) = (0usize, 0xcbf29ce484222325u64);
        for (name, n) in recipes() {
            for decl in declarations() {
                for ret in super::eager_scalar_value_tests::types() {
                    for profile in 0..3 {
                        for (index, values) in inputs(name, n).into_iter().enumerate() {
                            let old =
                                super::user_variable_tests::Context::new(profile, Datum::Int(0));
                            let new =
                                super::user_variable_tests::Context::new(profile, Datum::Int(0));
                            let f = function(name, &values, &decl, &ret);
                            let expected = observe(|| f.eval(&old, Row::empty()), &old);
                            let actual = observe(|| candidate(&f, &new), &new);
                            assert_eq!(
                                actual, expected,
                                "{name}/{n} decl={decl:?} ret={ret:?} p={profile} i={index}"
                            );
                            fold(
                                &mut hash,
                                format!("{name}|{n}|{decl:?}|{ret:?}|{profile}|{index}|")
                                    .as_bytes(),
                            );
                            fold(&mut hash, &expected);
                            count += 1;
                        }
                    }
                }
            }
        }
        assert_eq!((count, hash), (129855, 0xE1A91F4419B7C00E));
        eprintln!("EARLY STRING SHARED FROZEN REGRESSION {count} {hash:016X}");
    });
}

pub(crate) fn demand_cases() -> Vec<ScalarFunction> {
    let string =
        FieldType::new(FieldTypeCode::VarString).with_collation(Collation::Utf8Mb4GeneralCi);
    let int = FieldType::new(FieldTypeCode::LongLong).with_collation(Collation::Utf8Mb4GeneralCi);
    let literal = |v: Datum, t: &FieldType| Expression::Constant(Constant::new(v, t.clone()));
    let mark = |key: &str, value: Datum| {
        Expression::ScalarFunction(ScalarFunction::new(
            tidb_ast::CiString::new("setvar"),
            string.clone(),
            vec![
                literal(Datum::new_string(key), &string),
                literal(value, &string),
            ],
        ))
    };
    let call = |name: &str, ret: &FieldType, args: Vec<Expression>| {
        ScalarFunction::new(tidb_ast::CiString::new(name), ret.clone(), args)
    };
    vec![
        call(
            "strcmp",
            &int,
            vec![
                mark("left", Datum::new_string("A")),
                mark("right", Datum::new_string("a")),
            ],
        ),
        call(
            "locate",
            &int,
            vec![
                literal(Datum::Null, &string),
                mark("text", Datum::Bytes(vec![255])),
                mark("pos", Datum::new_string("1junk")),
            ],
        ),
        call(
            "ltrim_with",
            &string,
            vec![
                mark("left", Datum::new_string(" A ")),
                mark("remove", Datum::new_string(" ")),
                mark("ignored", Datum::new_string("bad")),
            ],
        ),
        call(
            "rtrim_with",
            &string,
            vec![
                literal(Datum::Null, &string),
                mark("remove", Datum::new_string(" ")),
                Expression::ScalarFunction(call("__unregistered_ignored__", &string, vec![])),
            ],
        ),
        call("ltrim_with", &string, vec![]),
        call(
            "rtrim_with",
            &string,
            vec![mark("first", Datum::new_string(" a "))],
        ),
        call(
            "field",
            &int,
            vec![
                literal(Datum::Null, &string),
                mark("second", Datum::new_string("A")),
                mark("third", Datum::new_string("a")),
            ],
        ),
        call("length", &int, vec![mark("text", Datum::new_string("é中"))]),
    ]
}
#[test]
fn early_string_supplement_frozen_collation_and_child_demand() {
    if isolated("early_string_supplement_frozen_collation_and_child_demand") {
        return;
    }
    let compare = true;
    let (mut count, mut hash) = (0usize, 0xcbf29ce484222325u64);
    for collation in super::wildcard_value_tests::collations() {
        let decl = FieldType::new(FieldTypeCode::VarString).with_collation(collation);
        let ret = FieldType::new(FieldTypeCode::LongLong).with_collation(collation);
        for (name, n) in [
            ("locate", 2),
            ("locate", 3),
            ("instr", 2),
            ("strcmp", 2),
            ("field", 3),
        ] {
            for profile in 0..3 {
                for (index, values) in inputs(name, n).into_iter().enumerate() {
                    let old = super::user_variable_tests::Context::new(profile, Datum::Int(0));
                    let f = function(name, &values, &decl, &ret);
                    let expected = observe(|| f.eval(&old, Row::empty()), &old);
                    if compare {
                        let new = super::user_variable_tests::Context::new(profile, Datum::Int(0));
                        let actual = observe(|| candidate(&f, &new), &new);
                        assert_eq!(
                            actual, expected,
                            "{name}/{n} coll={collation:?} p={profile} i={index}"
                        );
                    }
                    fold(
                        &mut hash,
                        format!("{collation:?}|{name}|{n}|{profile}|{index}|").as_bytes(),
                    );
                    fold(&mut hash, &expected);
                    count += 1;
                }
            }
        }
    }
    for (index, f) in demand_cases().into_iter().enumerate() {
        for profile in 0..3 {
            let old = super::user_variable_tests::Context::new(profile, Datum::Int(0));
            let expected = observe(|| f.eval(&old, Row::empty()), &old);
            if compare {
                let new = super::user_variable_tests::Context::new(profile, Datum::Int(0));
                let actual = observe(|| candidate(&f, &new), &new);
                assert_eq!(actual, expected, "demand={index} p={profile}");
            }
            fold(&mut hash, format!("demand|{index}|{profile}|").as_bytes());
            fold(&mut hash, &expected);
            count += 1;
        }
    }
    assert_eq!((count, hash), (3540, 0xF069242CF30A6838));
    eprintln!("EARLY STRING SUPPLEMENT SHARED FROZEN {count} {hash:016X} compare={compare}");
}
