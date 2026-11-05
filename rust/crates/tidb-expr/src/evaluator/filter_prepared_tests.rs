// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Real filter-major observations captured before borrowed-plan reuse.
use super::*;
use crate::tikv::dynamic_leaf_tests as leaf;
use tidb_datatype::{FieldType, FieldTypeCode};

fn ty() -> FieldType {
    FieldType::new(FieldTypeCode::LongLong)
}
fn int(value: i64) -> Expression {
    leaf::literal(Datum::Int(value), ty())
}
fn call(name: &str, args: Vec<Expression>) -> Expression {
    leaf::call(name, ty(), args)
}
fn parameter() -> Expression {
    leaf::expression("param", 0, &Datum::Null, Some(&ty()), None)
}
fn filters(case: usize, value: &Datum) -> Vec<Expression> {
    let column = leaf::column;
    let assignment = || leaf::assignment("filter-v", parameter(), &ty());
    match case {
        0 => vec![column()],
        1 => vec![call("plus", vec![column(), parameter()])],
        2 => vec![parameter()],
        3 => vec![assignment()],
        4 => vec![column(), assignment()],
        5 => {
            let mut c = column();
            if let Expression::Column(c) = &mut c {
                c.in_operand = true;
            }
            vec![call("eq", vec![c, int(1)]), parameter()]
        }
        6 => vec![int(0), parameter()],
        7 => vec![call("if", vec![column(), parameter(), int(0)])],
        8 => vec![call("coalesce", vec![parameter(), column()])],
        9 => vec![leaf::call(
            "concat",
            FieldType::new(FieldTypeCode::VarString),
            vec![
                parameter(),
                leaf::literal(
                    Datum::new_string(""),
                    FieldType::new(FieldTypeCode::VarString),
                ),
            ],
        )],
        10 => vec![call("abs", vec![column()])],
        11 => vec![call(
            "lt",
            vec![
                leaf::call("rand", FieldType::new(FieldTypeCode::Double), vec![int(17)]),
                leaf::literal(Datum::Real(0.5), FieldType::new(FieldTypeCode::Double)),
            ],
        )],
        12 => {
            let mut c = column();
            if let Expression::Column(c) = &mut c {
                c.ret_type = None;
            }
            vec![c]
        }
        13 => vec![leaf::expression(
            "deferred_row",
            0,
            value,
            Some(&ty()),
            None,
        )],
        14 => {
            let mut c = parameter();
            if let Expression::Constant(c) = &mut c {
                c.ret_type = None;
            }
            vec![c]
        }
        _ => unreachable!(),
    }
}
fn input(rows: usize, selection: usize) -> Chunk {
    let mut chunk = Chunk::new_with_capacity(&[ty()], rows.max(1));
    for i in 0..rows {
        match i % 7 {
            0 => chunk.append_null(0),
            1 | 6 => chunk.append_int64(0, 0),
            2 => chunk.append_int64(0, 1),
            3 => chunk.append_int64(0, -1),
            4 => chunk.append_int64(0, 2),
            _ => chunk.append_int64(0, 7),
        }
    }
    match selection {
        0 => {}
        1 => chunk.set_sel(Some(vec![])),
        2 => chunk.set_sel(Some((0..rows).rev().filter(|i| i % 2 == 0).collect())),
        3 => chunk.set_sel(Some(vec![rows + 3, 0, 0])),
        _ => unreachable!(),
    }
    chunk
}
fn evaluate(
    entry: usize,
    context: &leaf::Context,
    filters: &[Expression],
    input: &Chunk,
) -> std::result::Result<(Vec<bool>, Vec<bool>), EvalError> {
    let selected = vec![false, true, false, true, true];
    let nulls = vec![true, false, true];
    match entry {
        0 | 1 => {
            vectorized_filter_consider_null(context, entry == 1, filters, input, selected, nulls)
        }
        2 | 3 => vec_eval_bool(context, entry == 3, filters, input, selected, nulls),
        _ => unreachable!(),
    }
}
fn records() -> Vec<String> {
    let mut records = vec![];
    for case in 0..15 {
        for (vi, value) in [
            Datum::Null,
            Datum::Int(0),
            Datum::Int(1),
            Datum::Int(-1),
            Datum::new_string("12"),
            Datum::Bytes(vec![0xff]),
        ]
        .into_iter()
        .enumerate()
        {
            for profile in [0, 1, 3] {
                for sequential in [false, true] {
                    for rows in [0, 1, 7] {
                        for selection in 0..4 {
                            for entry in 0..4 {
                                let filters = filters(case, &value);
                                let input = input(rows, selection);
                                let mut context = leaf::Context::new(profile, value.clone());
                                context.sequential = sequential;
                                let outcome =
                                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                        evaluate(entry, &context, &filters, &input)
                                    }));
                                let result = match outcome {
                                    Ok(value) => format!("{value:?}"),
                                    Err(payload) => format!(
                                        "PANIC:{}",
                                        payload
                                            .downcast_ref::<String>()
                                            .map(String::as_str)
                                            .or_else(|| payload.downcast_ref::<&str>().copied())
                                            .unwrap_or("non-text payload")
                                    ),
                                };
                                records.push(format!("{case}/{vi}/{profile}/{sequential}/{rows}/{selection}/{entry}|{result}|{}|sel:{:?}", context.snapshot(), input.sel()));
                            }
                        }
                    }
                }
            }
        }
    }
    records
}
fn digest(records: &[String]) -> u64 {
    let mut hash = 0xcbf29ce484222325u64;
    for record in records {
        for byte in record.bytes().chain([b'\n']) {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(0x100000001b3);
        }
    }
    hash
}
#[test]
fn filter_public_observations_match_frozen_results() {
    let actual = records();
    assert_eq!(
        (actual.len(), digest(&actual)),
        (25920, 0x9C47_E878_2A22_C4DF)
    );
}
#[test]
fn filter_prepares_each_demanded_scalar_root_once() {
    for entry in 0..4 {
        let context = leaf::Context::new(0, Datum::Int(1));
        let mut chunk = Chunk::new_with_capacity(&[ty()], 8);
        for _ in 0..8 {
            chunk.append_int64(0, 1);
        }
        let filters = vec![call("plus", vec![leaf::column(), parameter()])];
        let before = crate::test_evaluation::scalar_preparations();
        assert_eq!(
            evaluate(entry, &context, &filters, &chunk).unwrap().0,
            vec![true; 8]
        );
        // The original integer probe and general demand each read the
        // live parameter. Preparation reuse must preserve both reads.
        assert_eq!(context.calls.get(), 16);
        assert_eq!(
            crate::test_evaluation::scalar_preparations() - before,
            1,
            "scalar filter recompiled its genuine root for every live row"
        );
    }
}

#[test]
fn filter_only_prepares_demanded_roots_and_resource_never_replays() {
    let count = 512;
    let oversized = || call("coalesce", (0..count).map(|_| parameter()).collect());
    for entry in 0..4 {
        let context = leaf::Context::new(2, Datum::Int(1)); // Panics on any live parameter read.
        let empty = input(0, 0);
        let before = crate::test_evaluation::scalar_preparations();
        assert_eq!(
            evaluate(entry, &context, &[oversized()], &empty).unwrap(),
            (vec![], vec![])
        );
        assert_eq!(crate::test_evaluation::scalar_preparations(), before);
        assert_eq!(context.calls.get(), 0);
        let chunk = input(7, 0);
        let suppressed = vec![int(0), oversized()];
        let before = crate::test_evaluation::scalar_preparations();
        assert_eq!(
            evaluate(entry, &context, &suppressed, &chunk).unwrap().0,
            vec![false; 7]
        );
        assert_eq!(
            crate::test_evaluation::scalar_preparations() - before,
            usize::from(entry == 0 || entry == 2)
        );
        assert_eq!(context.calls.get(), 0);
        let demanded = vec![int(1), oversized()];
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| evaluate(
                entry, &context, &demanded, &chunk
            )))
            .is_err()
        );
        assert_eq!(
            context.calls.get(),
            1,
            "demanded callback must not replay after unwind"
        );
    }
}

#[test]
fn filter_reuse_does_not_cache_failed_contexts_or_mutated_roots() {
    let mut chunk = Chunk::new_with_capacity(&[ty()], 8);
    for _ in 0..8 {
        chunk.append_int64(0, 1);
    }
    for entry in 0..4 {
        let mut filters = vec![call("plus", vec![leaf::column(), parameter()])];
        for profile in [1, 2, 0, 3] {
            let observe = |context: &leaf::Context| {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    evaluate(entry, context, &filters, &chunk)
                }));
                let outcome = match result {
                    Ok(value) => format!("{value:?}"),
                    Err(payload) => format!(
                        "PANIC:{}",
                        payload
                            .downcast_ref::<String>()
                            .map(String::as_str)
                            .or_else(|| payload.downcast_ref::<&str>().copied())
                            .unwrap_or("non-text payload")
                    ),
                };
                (outcome, context.snapshot())
            };
            let original = leaf::Context::new(profile, Datum::Int(1));
            let expected = (|| observe(&original))();
            let actual = leaf::Context::new(profile, Datum::Int(1));
            let before = crate::test_evaluation::scalar_preparations();
            assert_eq!(observe(&actual), expected);
            assert_eq!(crate::test_evaluation::scalar_preparations() - before, 1);
        }
        // The previous call's borrow is over: public AST fields may now change.
        let Expression::ScalarFunction(function) = &mut filters[0] else {
            unreachable!()
        };
        function.args[1] = int(4);
        let actual = leaf::Context::new(2, Datum::Int(1));
        let before = crate::test_evaluation::scalar_preparations();
        assert_eq!(
            evaluate(entry, &actual, &filters, &chunk).unwrap().0,
            vec![true; 8]
        );
        assert_eq!(actual.calls.get(), 0);
        assert_eq!(crate::test_evaluation::scalar_preparations() - before, 1);
    }
}

#[test]
fn filter_reuse_matches_public_per_row_execution_and_reports_timing() {
    fn per_row(root: &Expression, input: &Chunk) -> (Vec<bool>, Vec<bool>) {
        let mut selected = Vec::with_capacity(input.physical_rows());
        let mut nulls = Vec::with_capacity(input.physical_rows());
        for physical in 0..input.physical_rows() {
            let value = root
                .eval(&crate::NoColumns, input.physical_row(physical))
                .unwrap();
            let truth = crate::truthy_of(&value).unwrap();
            selected.push(truth == Some(true));
            nulls.push(truth.is_none()); // The genuine root has an integer declaration.
        }
        (selected, nulls)
    }
    for rows in [32, 1024, 8192] {
        let root = call("plus", vec![leaf::column(), int(2)]);
        let filters = std::slice::from_ref(&root);
        let chunk = input(rows, 0);
        let expected = per_row(&root, &chunk);
        let prepared = || {
            vectorized_filter_consider_null(
                &crate::NoColumns,
                false,
                filters,
                &chunk,
                Vec::new(),
                Vec::new(),
            )
            .unwrap()
        };
        assert_eq!(prepared(), expected);
        let before = crate::test_evaluation::scalar_preparations();
        assert_eq!(per_row(&root, &chunk), expected);
        assert_eq!(crate::test_evaluation::scalar_preparations() - before, rows);
        let before = crate::test_evaluation::scalar_preparations();
        assert_eq!(prepared(), expected);
        assert_eq!(crate::test_evaluation::scalar_preparations() - before, 1);
        // Same current shared compiler/SDK and real row on both sides. The
        // public-entry baseline prepares for each row, as before this change.
        // Timing is descriptive; correctness and compilation counts decide pass.
        let mut baseline_ns = vec![];
        let mut prepared_ns = vec![];
        for round in 0..7 {
            for optimized in if round % 2 == 0 {
                [false, true]
            } else {
                [true, false]
            } {
                let start = std::time::Instant::now();
                let actual = if optimized {
                    prepared()
                } else {
                    per_row(&root, &chunk)
                };
                std::hint::black_box(&actual);
                let elapsed = start.elapsed().as_nanos();
                assert_eq!(actual, expected);
                if optimized {
                    prepared_ns.push(elapsed);
                } else {
                    baseline_ns.push(elapsed);
                }
            }
        }
        println!(
            "N55 shared filter rows={rows} baseline_ns={baseline_ns:?} prepared_ns={prepared_ns:?}"
        );
    }
}
