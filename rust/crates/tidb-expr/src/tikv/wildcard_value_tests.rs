// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Original LIKE/ILIKE utility, scalar, AST, legacy and live-cache observations.
use crate::{constant::Constant, expression::Expression, scalar_function::ScalarFunction};
use crate::{Columns, Datum, EvalError};
use std::cell::Cell;
use tidb_datatype::{Collation, DateModes, FieldType, FieldTypeCode, SessionTimeZone};
pub(crate) struct C {
    pub(crate) inner: super::calendar_value_tests::Context,
    pub(crate) id: Cell<u64>,
}
pub(crate) fn context(profile: usize, seed: usize, level: usize) -> C {
    C {
        inner: super::calendar_value_tests::context(profile, seed, level),
        id: Cell::new(8901),
    }
}
impl Columns for C {
    fn get(&self, _: &[String]) -> Option<Datum> {
        None
    }
    fn ready_value_cache(&self) -> Option<&crate::ReadyValueCache> {
        self.inner.ready_value_cache()
    }
    fn context_id(&self) -> u64 {
        let id = self.id.get();
        self.inner.events.borrow_mut().push(format!("context:{id}"));
        id
    }
    fn truncate_level(&self) -> crate::ErrorLevel {
        self.inner.truncate_level()
    }
    fn append_warning(&self, code: u16, message: &str) {
        self.inner.append_warning(code, message)
    }
    fn date_modes(&self) -> DateModes {
        self.inner.date_modes()
    }
    fn time_zone(&self) -> SessionTimeZone {
        self.inner.time_zone()
    }
    fn now(&self) -> Option<(i64, u32, i32)> {
        self.inner.now()
    }
}
pub(crate) fn collations() -> Vec<Collation> {
    vec![
        Collation::Binary,
        Collation::Utf8Mb4Bin,
        Collation::Utf8Mb4GeneralCi,
        Collation::Utf8Mb4UnicodeCi,
    ]
}
pub(crate) fn words() -> Vec<Vec<u8>> {
    [
        b"".as_slice(),
        b"abc",
        b"AbC",
        b"ab%",
        b"_",
        b"%",
        b"a%_",
        b"a\\%",
        b"a\\",
        b"a\0b",
        b"\xff",
        b"\xfe",
        b"\xc3",
        b"a\xffb",
        "é".as_bytes(),
        "e\u{301}".as_bytes(),
        "中".as_bytes(),
        "İ".as_bytes(),
        "ß".as_bytes(),
        b"a_a",
        b"a%a%z",
    ]
    .into_iter()
    .map(<[u8]>::to_vec)
    .collect()
}
pub(crate) fn escapes() -> [u8; 8] {
    [b'\\', 0, b'%', b'_', b'a', b'A', b'x', 255]
}
fn fold(hash: &mut u64, value: &str) {
    for b in value.bytes() {
        *hash = (*hash ^ u64::from(b)).wrapping_mul(0x100000001b3);
    }
}
fn observe(f: impl FnOnce() -> Result<Datum, EvalError>, ctx: &C) -> String {
    super::duration_construction_value_tests::observation(f, &ctx.inner)
}
pub(crate) fn scalar(
    ilike: bool,
    values: Vec<Datum>,
    escape_type: &FieldType,
    collation: Collation,
) -> ScalarFunction {
    let string = FieldType::new(FieldTypeCode::VarString).with_collation(collation);
    let args = values
        .into_iter()
        .enumerate()
        .map(|(i, v)| {
            Expression::Constant(Constant::new(
                v,
                if i == 2 {
                    escape_type.clone()
                } else {
                    string.clone()
                },
            ))
        })
        .collect();
    ScalarFunction::new(
        tidb_ast::CiString::new(if ilike { "ilike" } else { "like" }),
        FieldType::new(FieldTypeCode::LongLong),
        args,
    )
}
#[test]
fn wildcard_original_kernel_observations() {
    let (mut count, mut hash) = (0usize, 0xcbf29ce484222325u64);
    for profile in 0..3 {
        for collation in collations() {
            for text in words() {
                for pattern in words() {
                    for escape in escapes() {
                        let ctx = context(profile, 0, 0);
                        let result = observe(
                            || {
                                crate::like::like_match_with_collation_in(
                                    &text,
                                    &pattern,
                                    Some(escape),
                                    collation,
                                    &ctx,
                                )
                                .map(|v| Datum::Int(i64::from(v)))
                            },
                            &ctx,
                        );
                        fold(&mut hash, &result);
                        count += 1;
                    }
                }
            }
        }
    }
    for profile in 0..3 {
        for ci in [false, true] {
            for text in words() {
                for pattern in words() {
                    for escape in escapes() {
                        let ctx = context(profile, 0, 0);
                        let result = observe(
                            || {
                                super::eval_legacy_like_in(
                                    ci,
                                    super::LegacyLikeArgs::Values {
                                        target: text.clone(),
                                        pattern: pattern.clone(),
                                        escape,
                                    },
                                    &ctx,
                                )
                                .map(|v| v.map_or(Datum::Null, |v| Datum::Int(v as i64)))
                            },
                            &ctx,
                        );
                        fold(&mut hash, &result);
                        count += 1;
                    }
                }
            }
        }
    }
    for profile in 0..3 {
        let ctx = context(profile, 0, 0);
        for value in [None, Some(1)] {
            let result = observe(
                || {
                    super::eval_legacy_like_in(
                        false,
                        super::LegacyLikeArgs::NullWitness(value),
                        &ctx,
                    )
                    .map(|v| v.map_or(Datum::Null, |v| Datum::Int(v as i64)))
                },
                &ctx,
            );
            fold(&mut hash, &result);
            count += 1;
        }
        let result = observe(
            || {
                super::eval_legacy_like_in(false, super::LegacyLikeArgs::Missing, &ctx)
                    .map(|v| v.map_or(Datum::Null, |v| Datum::Int(v as i64)))
            },
            &ctx,
        );
        fold(&mut hash, &result);
        count += 1;
    }
    eprintln!("WILDCARD KERNEL ORIGINAL {count} {hash:016X}");
    assert_eq!(count, 63513);
    assert_eq!(hash, 0x1952BB7F6D8F1E9B);
}
#[test]
fn wildcard_original_scalar_observations() {
    let (mut count, mut hash) = (0usize, 0xcbf29ce484222325u64);
    let types = [
        FieldType::new(FieldTypeCode::LongLong),
        FieldType::new(FieldTypeCode::LongLong).with_unsigned(true),
        FieldType::new(FieldTypeCode::VarString),
        FieldType::new(FieldTypeCode::Year),
        FieldType::new(FieldTypeCode::Datetime).with_decimal(6),
        FieldType::new(FieldTypeCode::Duration).with_decimal(6),
    ];
    for profile in 0..3 {
        for level in 0..3 {
            for ilike in [false, true] {
                for index in 0..3 {
                    for ty in &types {
                        for value in super::duration_construction_value_tests::values() {
                            let ctx = context(profile, 1, level);
                            let mut values = vec![
                                Datum::new_string("Abcé"),
                                Datum::new_string("a%"),
                                Datum::Int(92),
                            ];
                            values[index] = value;
                            let f = scalar(ilike, values, ty, Collation::Utf8Mb4GeneralCi);
                            let input = tidb_chunk::chunk::Chunk::new_with_capacity(&[], 1);
                            let result = observe(|| f.eval(&ctx, input.get_row(0)), &ctx);
                            fold(&mut hash, &result);
                            count += 1;
                        }
                    }
                }
            }
        }
    }
    eprintln!("WILDCARD SCALAR ORIGINAL {count} {hash:016X}");
    assert_eq!(count, 34344);
    assert_eq!(hash, 0xC49CB830518858A9);
}
#[test]
fn wildcard_original_cache_and_ast_observations() {
    let (mut count, mut hash) = (0usize, 0xcbf29ce484222325u64);
    let escape = FieldType::new(FieldTypeCode::LongLong);
    for profile in 0..3 {
        for ilike in [false, true] {
            let mut f = scalar(
                ilike,
                vec![
                    Datum::new_string("abc"),
                    Datum::new_string("a%"),
                    Datum::Int(92),
                ],
                &escape,
                Collation::Utf8Mb4GeneralCi,
            );
            for (id, text, pattern, esc) in [
                (7, "abc", "a%", 92),
                (7, "abc", "z%", 0),
                (8, "abc", "z%", 0),
                (8, "zxc", "a%", 92),
                (7, "abc", "a%", 92),
            ] {
                f.args[0] = Expression::Constant(Constant::new(
                    Datum::new_string(text),
                    FieldType::new(FieldTypeCode::VarString),
                ));
                f.args[1] = Expression::Constant(Constant::new(
                    Datum::new_string(pattern),
                    FieldType::new(FieldTypeCode::VarString),
                ));
                f.args[2] = Expression::Constant(Constant::new(Datum::Int(esc), escape.clone()));
                let ctx = context(profile, 0, 0);
                ctx.id.set(id);
                let input = tidb_chunk::chunk::Chunk::new_with_capacity(&[], 1);
                let result = observe(|| f.eval(&ctx, input.get_row(0)), &ctx);
                fold(&mut hash, &result);
                count += 1;
            }
        }
    }
    for profile in 0..3 {
        for ilike in [false, true] {
            for not in [false, true] {
                for (left, right) in [
                    (tidb_ast::Expr::Null, tidb_ast::Expr::String("%".into())),
                    (tidb_ast::Expr::String("abc".into()), tidb_ast::Expr::Null),
                    (
                        tidb_ast::Expr::String("AbC".into()),
                        tidb_ast::Expr::String("a%".into()),
                    ),
                    (
                        tidb_ast::Expr::Decimal("12.50".into()),
                        tidb_ast::Expr::String("12.5".into()),
                    ),
                ] {
                    let ctx = context(profile, 0, 0);
                    let expr = tidb_ast::Expr::Like {
                        expr: Box::new(left),
                        pattern: Box::new(right),
                        not,
                        ilike,
                        escape: Some(0),
                    };
                    let result = observe(|| crate::eval_in(&expr, &ctx), &ctx);
                    fold(&mut hash, &result);
                    count += 1;
                }
            }
        }
    }
    eprintln!("WILDCARD HISTORY AST ORIGINAL {count} {hash:016X}");
    assert_eq!(count, 78);
    assert_eq!(hash, 0x94085E5905346BCD);
}
#[test]
fn wildcard_never_requests_ready() {
    let mut failures = vec![];
    for ilike in [false, true] {
        let mut ctx = context(1, 0, 0);
        ctx.inner.reject = true;
        let f = scalar(
            ilike,
            vec![
                Datum::new_string("abc"),
                Datum::new_string("a%"),
                Datum::Int(92),
            ],
            &FieldType::new(FieldTypeCode::LongLong),
            Collation::Utf8Mb4Bin,
        );
        let input = tidb_chunk::chunk::Chunk::new_with_capacity(&[], 1);
        if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            f.eval(&ctx, input.get_row(0))
        }))
        .is_err()
        {
            failures.push(format!("scalar {ilike}"));
        }
    }
    let mut ctx = context(1, 0, 0);
    ctx.inner.reject = true;
    if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        crate::like::like_match_with_collation_in(b"abc", b"a%", None, Collation::Utf8Mb4Bin, &ctx)
    }))
    .is_err()
    {
        failures.push("utility".into());
    }
    if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        crate::like::like_null_in(&ctx)
    }))
    .is_err()
    {
        failures.push("null".into());
    }
    for args in [
        super::LegacyLikeArgs::Missing,
        super::LegacyLikeArgs::NullWitness(None),
        super::LegacyLikeArgs::Values {
            target: b"abc".to_vec(),
            pattern: b"a%".to_vec(),
            escape: 92,
        },
    ] {
        if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            super::eval_legacy_like_in(false, args, &ctx)
        }))
        .is_err()
        {
            failures.push("legacy".into());
        }
    }
    assert!(failures.is_empty(), "wildcard Ready {failures:?}");
}

#[test]
fn wildcard_ast_keeps_eager_children_and_nullable_byte_conversion_barrier() {
    let mut ctx = context(1, 0, 0);
    ctx.inner.reject = true;
    let bad = tidb_ast::Expr::Func {
        name: "abs".into(),
        args: vec![tidb_ast::Expr::Int(i64::MIN.to_string())],
        origin_position: 0,
    };
    for ilike in [false, true] {
        for not in [false, true] {
            let expected = observe(|| crate::eval_in(&bad, &ctx), &ctx);
            let expr = tidb_ast::Expr::Like {
                expr: Box::new(tidb_ast::Expr::Null),
                pattern: Box::new(bad.clone()),
                not,
                ilike,
                escape: None,
            };
            assert_eq!(observe(|| crate::eval_in(&expr, &ctx), &ctx), expected);
            assert_eq!(
                super::wildcard_value::ast(ilike, &Datum::MinNotNull, &Datum::Null, None, 8192)
                    .unwrap(),
                Datum::Null
            );
            assert_eq!(
                super::wildcard_value::ast(ilike, &Datum::Null, &Datum::MaxValue, Some(0), 8192)
                    .unwrap(),
                Datum::Null
            );
        }
    }
}
