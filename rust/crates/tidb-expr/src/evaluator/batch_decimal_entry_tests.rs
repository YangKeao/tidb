// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Frozen genuine Decimal-priority entry observations, before retirement.
use super::*;
use crate::tikv::dynamic_leaf_tests as leaf;
use crate::ReadyValueCache;
fn decimal() -> FieldType {
    FieldType::new(FieldTypeCode::NewDecimal)
        .with_flen(65)
        .with_decimal(6)
}
fn declarations() -> Vec<Option<FieldType>> {
    vec![
        None,
        Some(ty()),
        Some(decimal()),
        Some(decimal().with_flen(2).with_decimal(1)),
        Some(decimal().with_unsigned(true)),
        Some(decimal().with_flen(-1).with_decimal(-1)),
    ]
}
fn value(which: usize) -> Expression {
    let d = decimal();
    match which {
        0 => typed_column(0, &ty()),
        1 => typed_column(0, &ty().with_unsigned(true)),
        2 => typed_column(1, &d),
        3 => typed_column(2, &d),
        4 => typed_column(99, &d),
        5 => literal(Datum::Null, d),
        6 => literal(Datum::Int(-1), ty()),
        7 => literal(Datum::UInt(u64::MAX), ty()),
        8 => literal(Datum::Int(-1), ty().with_unsigned(true)),
        9 => literal(Datum::Decimal(Decimal::from_literal("-1.250000")), d),
        10 => literal(
            Datum::Decimal(Decimal::from_literal(
                "9999999999999999999999999999999999999999",
            )),
            d,
        ),
        11 => literal(Datum::new_string("12"), d),
        12 => literal(Datum::Real(1.25), real_ty()),
        13 => leaf::expression("param", 0, &Datum::Int(7), Some(&d), None),
        14 => leaf::expression("deferred_value", 0, &Datum::Int(7), Some(&d), None),
        15..=19 => call(
            "cast_decimal",
            d.with_decimal([0, 6, 30, 38, 39][which - 15]),
            vec![typed_column(0, &ty())],
        ),
        20 => call(
            "plus",
            d,
            vec![typed_column(1, &decimal()), typed_column(0, &ty())],
        ),
        21 => call("mul", d, vec![value(10), value(10)]),
        22 => call("plus", d, vec![value(11), value(4)]),
        23 => call("plus", d, vec![value(4), value(11)]),
        _ => unreachable!(),
    }
}
fn function(
    which: usize,
    field: Option<FieldType>,
    left: Expression,
    right: Expression,
) -> ScalarFunction {
    let (name, args) = match which {
        0 => ("plus", vec![left, right]),
        1 => ("minus", vec![left, right]),
        2 => ("mul", vec![left, right]),
        3 => ("PLUS", vec![left, right]),
        4 => ("cast_decimal", vec![left]),
        5 => ("cast_decimal", vec![left, right]),
        6 => ("div", vec![left, right]),
        7 => ("__decimal_entry_unknown__", vec![left, right]),
        8 => ("plus", vec![left]),
        9 => ("plus", vec![left, right, int(1)]),
        _ => unreachable!(),
    };
    let mut f = ScalarFunction::new(tidb_ast::CiString::new(name), decimal(), args);
    f.ret_type = field;
    f
}
fn observe(
    f: &ScalarFunction,
    input: &Chunk,
    profile: usize,
    eval: &mut impl FnMut(
        &ScalarFunction,
        &leaf::Context,
        &Chunk,
        &mut Chunk,
    ) -> std::result::Result<bool, EvalError>,
) -> String {
    let context = leaf::Context::new(profile, Datum::Int(7));
    let mut output = chunk(
        &[decimal()],
        &[vec![Datum::Decimal(Decimal::from_literal("8.750000"))]],
    );
    let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        eval(f, &context, input, &mut output)
    }));
    let status = match r {
        Ok(Ok(value)) => format!("VALUE:{value}"),
        Ok(Err(e)) => format!("ERROR:{e:?}"),
        Err(p) => format!(
            "PANIC:{}",
            p.downcast_ref::<String>()
                .cloned()
                .or_else(|| p.downcast_ref::<&str>().map(|s| s.to_string()))
                .unwrap_or_else(|| "opaque native panic".into())
        ),
    };
    let cells = (0..output.num_rows())
        .map(|r| {
            if output.column(0).is_null(r) {
                "NULL".to_string()
            } else {
                format!("{:02X?}", output.column(0).get_raw(r).as_ref())
            }
        })
        .collect::<Vec<_>>();
    format!("{status}|cells:{cells:?}|{}", context.snapshot())
}
fn records(
    mut eval: impl FnMut(
        &ScalarFunction,
        &leaf::Context,
        &Chunk,
        &mut Chunk,
    ) -> std::result::Result<bool, EvalError>,
) -> Vec<String> {
    let schema = [ty(), decimal(), string_ty()];
    let rows = [
        vec![
            Datum::Int(i64::MAX),
            Datum::Decimal(Decimal::from_literal("1.250000")),
            Datum::new_string("raw"),
        ],
        vec![Datum::Null, Datum::Null, Datum::Null],
        vec![
            Datum::Int(-3),
            Datum::Decimal(Decimal::from_literal(
                "9999999999999999999999999999999999999999",
            )),
            Datum::new_string("x"),
        ],
    ];
    let mut input = chunk(&schema, &rows);
    let mut records = Vec::new();
    for (fi, field) in declarations().into_iter().enumerate() {
        for op in 0..10 {
            for left in 0..24 {
                for right in 0..24 {
                    let f = function(op, field.clone(), value(left), value(right));
                    for (si, selection) in [
                        vec![],
                        vec![1],
                        vec![0, 1, 2],
                        vec![2, 0, 2],
                        vec![usize::MAX],
                        vec![2, usize::MAX],
                    ]
                    .into_iter()
                    .enumerate()
                    {
                        input.set_sel(Some(selection));
                        records.push(format!(
                            "{fi}/{op}/{left}/{right}/{si}|{}",
                            observe(&f, &input, 0, &mut eval)
                        ));
                    }
                }
            }
        }
    }
    records
}
fn digest(records: &[String]) -> u64 {
    let mut digest = 0xcbf29ce484222325u64;
    for record in records {
        for byte in (record.len() as u64)
            .to_le_bytes()
            .iter()
            .copied()
            .chain(record.bytes())
        {
            digest ^= u64::from(byte);
            digest = digest.wrapping_mul(0x100000001b3);
        }
    }
    digest
}
fn long_records(
    mut eval: impl FnMut(
        &ScalarFunction,
        &leaf::Context,
        &Chunk,
        &mut Chunk,
    ) -> std::result::Result<bool, EvalError>,
) -> Vec<String> {
    let schema = [ty(), decimal(), string_ty()];
    let rows = [
        vec![
            Datum::Int(i64::MIN),
            Datum::Decimal(Decimal::from_literal("-1.250000")),
            Datum::new_string("raw"),
        ],
        vec![Datum::Null, Datum::Null, Datum::Null],
        vec![
            Datum::Int(i64::MAX),
            Datum::Decimal(Decimal::from_literal(
                "9999999999999999999999999999999999999999",
            )),
            Datum::new_string("x"),
        ],
    ];
    let mut input = chunk(&schema, &rows);
    let mut records = Vec::new();
    for left in [0, 1, 2, 10, 15, 19, 20, 22, 23] {
        for op in [0, 2, 4] {
            let f = function(op, Some(decimal()), value(left), value(2));
            for len in [0, 1, 1024, 1025, 2050] {
                input.set_sel(Some((0..len).map(|i| [0, 1, 0][i % 3]).collect()));
                records.push(format!(
                    "long/{left}/{op}/{len}|{}",
                    observe(&f, &input, 0, &mut eval)
                ));
            }
        }
    }
    records
}
#[test]
fn decimal_entry_public_matches_frozen_results() {
    let actual = records(|f, c, i, o| f.vec_eval_decimal_arithmetic_in(c, i, o, 0));
    assert_eq!(
        (actual.len(), digest(&actual)),
        (207360, 0xC538_B0CF_8411_E721)
    );
    let actual = long_records(|f, c, i, o| f.vec_eval_decimal_arithmetic_in(c, i, o, 0));
    assert_eq!(
        (actual.len(), digest(&actual)),
        (135, 0xB96F_12A4_9984_1AAB)
    );
}

fn suite_records() -> Vec<String> {
    let schema = [ty(), decimal(), string_ty()];
    let data = [
        vec![
            Datum::Int(7),
            Datum::Decimal(Decimal::from_literal("1.250000")),
            Datum::new_string("raw"),
        ],
        vec![Datum::Null, Datum::Null, Datum::Null],
        vec![
            Datum::Int(-3),
            Datum::Decimal(Decimal::from_literal(
                "9999999999999999999999999999999999999999",
            )),
            Datum::new_string("x"),
        ],
    ];
    let mut records = Vec::new();
    for left in [0, 2, 10, 11, 13, 15, 19, 20, 22, 23] {
        let root = Expression::ScalarFunction(function(
            0,
            Some(decimal().with_flen(2).with_decimal(1)),
            value(left),
            value(2),
        ));
        let suite = EvaluatorSuite::new(vec![root], false);
        for (si, selection) in [
            vec![],
            vec![0],
            vec![1],
            vec![2, 0, 2],
            vec![usize::MAX],
            (0..1025).map(|i| [0, 1, 0][i % 3]).collect(),
        ]
        .into_iter()
        .enumerate()
        {
            for enabled in [false, true] {
                for profile in 0..4 {
                    let context = leaf::Context::new(profile, Datum::Int(9));
                    context.enabled.set(enabled);
                    let mut input = chunk(&schema, &data);
                    input.set_sel(Some(selection.clone()));
                    let mut output = chunk(
                        &[decimal()],
                        &[vec![Datum::Decimal(Decimal::from_literal("8.750000"))]],
                    );
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        suite.run(&context, &mut input, &mut output)
                    }));
                    let status = match result {
                        Ok(Ok(())) => "OK".to_owned(),
                        Ok(Err(e)) => format!("ERROR:{e:?}"),
                        Err(p) => format!(
                            "PANIC:{}",
                            p.downcast_ref::<String>()
                                .cloned()
                                .or_else(|| p.downcast_ref::<&str>().map(|s| s.to_string()))
                                .unwrap_or_else(|| "opaque native panic".into())
                        ),
                    };
                    let cells = (0..output.num_rows())
                        .map(|i| {
                            if output.column(0).is_null(i) {
                                "NULL".to_owned()
                            } else {
                                format!("{:02X?}", output.column(0).get_raw(i).as_ref())
                            }
                        })
                        .collect::<Vec<_>>();
                    records.push(format!(
                        "{left}/{si}/{enabled}/{profile}|{status}|{cells:?}|{}",
                        context.snapshot()
                    ));
                }
            }
        }
    }
    records
}
#[test]
fn decimal_entry_finite_policies_stage_output_and_recover() {
    let f = function(0, Some(decimal()), value(0), value(2));
    let input = chunk(
        &[ty(), decimal()],
        &[
            vec![Datum::Int(7), Datum::Decimal(Decimal::from_literal("1.25"))],
            vec![Datum::Null, Datum::Null],
        ],
    );
    let sentinel = || {
        chunk(
            &[decimal()],
            &[vec![Datum::Decimal(Decimal::from_literal("8.75"))]],
        )
    };
    let context = leaf::Context::new(0, Datum::Int(7));
    let (limits, execution) = public_scalar_limits();
    for phase in 0..3 {
        let mut compile = limits;
        let mut work = execution;
        match phase {
            0 => compile.max_metadata_bytes = 1,
            1 => work.max_steps = 0,
            2 => work.max_retained_bytes = 1,
            _ => unreachable!(),
        }
        let mut output = sentinel();
        assert!(decimal_fast_source::eval_function_with_policies(
            &f,
            &context,
            &input,
            &mut output,
            0,
            compile,
            work
        )
        .unwrap());
        let mut recovered = sentinel();
        assert!(
            decimal_fast_source::eval_function(&f, &context, &input, &mut recovered, 0).unwrap()
        );
        let mut original = sentinel();
        assert!(
            (|| f.vec_eval_decimal_arithmetic_in(&context, &input, &mut original, 0))().unwrap()
        );
        assert_eq!(output.num_rows(), original.num_rows());
        for row in 0..original.num_rows() {
            assert_eq!(
                output.column(0).is_null(row),
                original.column(0).is_null(row)
            );
            assert_eq!(
                output.column(0).get_raw(row).as_ref(),
                original.column(0).get_raw(row).as_ref()
            );
        }
        for row in 0..original.num_rows() {
            assert_eq!(
                recovered.column(0).is_null(row),
                original.column(0).is_null(row)
            );
            assert_eq!(
                recovered.column(0).get_raw(row).as_ref(),
                original.column(0).get_raw(row).as_ref()
            );
        }
    }
}

fn without_decimal_tree<T>(body: impl FnOnce() -> T) -> T {
    let before = crate::test_evaluation::shared_entries();
    let value = body();
    assert_eq!(
        crate::test_evaluation::shared_entries(),
        before,
        "Decimal batch nested a public scalar driver"
    );
    value
}

#[test]
fn decimal_entry_public_suite_matches_frozen_results() {
    let actual = suite_records();
    assert_eq!(
        (actual.len(), digest(&actual)),
        (480, 0xF2BA_883A_3C3A_F0B3)
    );
}

#[test]
fn decimal_entry_native_scope_keeps_null_refusal_and_demand_precedence() {
    struct ScopeContext {
        cache: ReadyValueCache,
        calls: std::cell::Cell<usize>,
    }
    impl Columns for ScopeContext {
        fn get(&self, _: &[String]) -> Option<Datum> {
            panic!("no invented Decimal row")
        }
        fn ready_value_cache(&self) -> Option<&ReadyValueCache> {
            self.calls.set(self.calls.get() + 1);
            Some(&self.cache)
        }
        fn enable_vectorized_expression(&self) -> bool {
            panic!("Decimal priority precedes vector getter")
        }
    }
    let roots = [
        function(0, Some(decimal()), value(0), value(2)),
        function(0, Some(decimal()), value(20), value(2)),
        function(4, Some(decimal()), value(0), value(2)),
        function(0, Some(decimal()), value(11), value(2)),
        function(0, Some(decimal()), value(0), value(6)),
    ];
    let rows = [
        vec![Datum::Int(7), Datum::Decimal(Decimal::from_literal("1.25"))],
        vec![Datum::Null, Datum::Null],
    ];
    for (which, f) in roots.iter().enumerate() {
        for selection in [
            vec![],
            vec![0],
            vec![1],
            vec![1, 0, 1],
            vec![usize::MAX],
            vec![0, usize::MAX],
        ] {
            for poisoned in [false, true] {
                let run = |original| {
                    let context = ScopeContext {
                        cache: ReadyValueCache::new(),
                        calls: std::cell::Cell::new(0),
                    };
                    if poisoned {
                        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            let _guard = context.cache.preparation_unwind_guard();
                            panic!("original scope poison fixture");
                        }));
                    }
                    let mut input = chunk(&[ty(), decimal()], &rows);
                    input.set_sel(Some(selection.clone()));
                    let mut output = chunk(
                        &[decimal()],
                        &[vec![Datum::Decimal(Decimal::from_literal("8.75"))]],
                    );
                    let mut body = || {
                        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            f.vec_eval_decimal_arithmetic_in(&context, &input, &mut output, 0)
                        }))
                    };
                    let result = if original {
                        body()
                    } else {
                        without_decimal_tree(body)
                    };
                    let status = match result {
                        Ok(value) => format!("{value:?}"),
                        Err(p) => format!(
                            "PANIC:{}",
                            p.downcast_ref::<String>()
                                .cloned()
                                .or_else(|| p.downcast_ref::<&str>().map(|s| s.to_string()))
                                .unwrap_or_else(|| "opaque panic".into())
                        ),
                    };
                    let cells = (0..output.num_rows())
                        .map(|i| {
                            if output.column(0).is_null(i) {
                                "NULL".to_owned()
                            } else {
                                format!("{:02X?}", output.column(0).get_raw(i).as_ref())
                            }
                        })
                        .collect::<Vec<_>>();
                    (status, context.calls.get(), cells)
                };
                assert_eq!(
                    run(true),
                    run(false),
                    "root{which} selected{selection:?} poison{poisoned}"
                );
            }
        }
    }
}
