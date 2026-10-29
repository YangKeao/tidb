// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Planner annotations do not replace genuine physical Column value demand.
use super::*;
use crate::tikv::dynamic_leaf_tests as leaf;

fn annotated(mask: usize, optional: bool) -> Expression {
    let Expression::Column(mut c) = column(0) else {
        unreachable!()
    };
    if mask & 1 != 0 {
        // This would fail or mutate the context if it were ever evaluated.
        c.virtual_expr = Some(Box::new(call(
            "__planner_only_must_not_run__",
            ty(),
            vec![leaf::assignment("virtual", int(99), &ty())],
        )));
    }
    if mask & 2 != 0 {
        c.correlated_col_unique_id = 917;
    }
    if optional {
        c.ret_type = None;
    }
    Expression::Column(c)
}
fn root(shape: usize, arg: Expression) -> (Expression, FieldType) {
    match shape {
        0 => (arg, ty()),
        1 => (plus(arg, int(2)), ty()),
        2 => (abs(arg), ty()),
        3 => (call("eq", ty(), vec![arg, int(7)]), ty()),
        4 => (call("if", ty(), vec![arg, int(11), int(22)]), ty()),
        5 => (call("coalesce", ty(), vec![arg, int(33)]), ty()),
        6 => (call("cast_char", string_ty(), vec![arg]), string_ty()),
        7 => (
            call(
                "concat",
                string_ty(),
                vec![arg, literal(Datum::new_string("!"), string_ty())],
            ),
            string_ty(),
        ),
        8 => (leaf::assignment("actual", arg, &ty()), ty()),
        _ => unreachable!(),
    }
}
fn observed(
    s: &EvaluatorSuite,
    context: &leaf::Context,
    input: &mut Chunk,
    field: &FieldType,
    shared: bool,
) -> String {
    let mut output = output_for(field);
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        if shared {
            s.run_with_tikv_batch(context, input, &mut output, &[ty()])
        } else {
            (|| s.run(context, input, &mut output))()
        }
    }));
    let status = match outcome {
        Ok(result) => format!("{result:?}"),
        Err(payload) => {
            let text = payload
                .downcast_ref::<String>()
                .map(String::as_str)
                .or_else(|| payload.downcast_ref::<&str>().copied())
                .unwrap_or("opaque");
            format!("PANIC:{text}")
        }
    };
    let values = (0..output.num_rows())
        .map(|i| leaf::frame(&output.get_row(i).get_datum(0, field)))
        .collect::<Vec<_>>();
    format!(
        "{status}|output:{values:?}|input:{:?}|{}",
        input.column(0),
        context.snapshot()
    )
}
fn records(shared: bool, require_prepared: bool) -> Vec<String> {
    let mut records = vec![];
    let mut refused = vec![];
    let mut retained_optional_cast = 0;
    for mask in 0..4 {
        for optional in [false, true] {
            for shape in 0..9 {
                for enabled in [false, true] {
                    for profile in [0, 1, 3] {
                        for selection in 0..4 {
                            let (expression, field) = root(shape, annotated(mask, optional));
                            let s = suite(expression);
                            let context = leaf::Context::new(profile, Datum::Int(77));
                            context.enabled.set(enabled);
                            let mut input = chunk(
                                &[ty()],
                                &[
                                    vec![Datum::Int(7)],
                                    vec![Datum::Null],
                                    vec![Datum::Int(-3)],
                                    vec![Datum::Int(0)],
                                ],
                            );
                            input.set_sel(match selection {
                                0 => None,
                                1 => Some(vec![3, 0, 3, 2, 1]),
                                2 => Some(vec![]),
                                _ => Some(vec![1, 1, 0]),
                            });
                            let label = format!(
                                "{mask}/{optional}/{shape}/{enabled}/{profile}/{selection}"
                            );
                            let observation = observed(&s, &context, &mut input, &field, shared);
                            records.push(format!("{label}|{observation}"));
                            if shared {
                                let route = s.batch_route_snapshot();
                                if optional && shape == 6 {
                                    // N58 closes the independently recorded missing-type CAST
                                    // gap, including the original mask-zero planner case.
                                    assert_eq!(
                                        (
                                            route.prepared,
                                            route.fallback,
                                            route.refusals,
                                            route.refusal
                                        ),
                                        (1, 0, 0, None)
                                    );
                                    retained_optional_cast += 1;
                                } else if route.fallback != 0
                                    || route.refusals != 0
                                    || route.prepared != 1
                                {
                                    refused.push(format!("{label}:{route:?}"));
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    assert!(!shared || retained_optional_cast == 96);
    if require_prepared {
        assert!(
            refused.is_empty(),
            "{} refused; first {:?}",
            refused.len(),
            refused.iter().take(8).collect::<Vec<_>>()
        );
    }
    records
}
fn digest(records: &[String]) -> u64 {
    let mut hash = 0xcbf29ce484222325u64;
    for record in records {
        for byte in record.len().to_le_bytes().into_iter().chain(record.bytes()) {
            hash = (hash ^ u64::from(byte)).wrapping_mul(0x100000001b3);
        }
    }
    hash
}
#[test]
fn projection_planner_columns_keep_original_phases_and_physical_values() {
    let original = records(false, false);
    eprintln!("N57 ORIGINAL {} {:016X}", original.len(), digest(&original));
    // Captured before planner-marker admission changes.
    assert_eq!(
        (original.len(), digest(&original)),
        (1728, 0x4EAD65319048E375)
    );
    let current = records(true, false);
    eprintln!(
        "N57 CURRENT SHARED {} {:016X}",
        current.len(),
        digest(&current)
    );
    let mut selected_direct = 0;
    let mut retained_cast = 0;
    // N57's pre-change shared535CA5CB231F073D is retained in its raw
    // evidence and receipt. N58 fixes its CAST error gap; original records
    // remain unchanged and now directly authenticate those corrected cases.
    assert_eq!(current.len(), 1728);
    for (i, (old, new)) in original.iter().zip(&current).enumerate() {
        let label = old
            .split('|')
            .next()
            .unwrap()
            .split('/')
            .collect::<Vec<_>>();
        if label[2] == "0" && label[5] != "0" {
            // The existing unified Projection supports selected direct-column
            // outputs; original Suite.run only has the unselected owner swap.
            // Preserve this already established behavior with its own full
            // pre-promotion current-shared pin, never relabel it as original.
            assert!(old.contains("The selection vector of Chunk is not nil"));
            assert!(new.contains("|Ok(())|"));
            selected_direct += 1;
        } else if label[1] == "true" && label[2] == "6" {
            // The previously recorded96 missing-type CAST cases now match
            // the unchanged independent original, including empty selections.
            assert_eq!(old, new, "corrected CAST original/candidate record {i}");
            retained_cast += 1;
        } else {
            assert_eq!(old, new, "original/candidate record {i}");
        }
    }
    assert_eq!((selected_direct, retained_cast), (144, 96));
}
#[test]
fn projection_planner_columns_prepare_without_generic_fallback() {
    records(true, true);
}

fn edge_records(shared: bool, require_prepared: bool) -> Vec<String> {
    let mut records = vec![];
    let mut refused = vec![];
    for mask in 0..4 {
        for shape in 0..5 {
            for enabled in [false, true] {
                for profile in [0, 1, 3] {
                    for hidden in [false, true] {
                        let mut arg = annotated(mask, false);
                        let Expression::Column(c) = &mut arg else {
                            unreachable!()
                        };
                        c.is_hidden = hidden;
                        let (expression, field, schema, rows) = if shape < 3 {
                            let (e, f) = root(shape + 1, arg);
                            (
                                e,
                                f,
                                ty(),
                                vec![
                                    vec![Datum::Int(i64::MAX)],
                                    vec![Datum::Int(i64::MIN)],
                                    vec![Datum::Null],
                                    vec![Datum::Int(7)],
                                ],
                            )
                        } else {
                            let Expression::Column(c) = &mut arg else {
                                unreachable!()
                            };
                            c.ret_type = Some(string_ty());
                            let parameter = leaf::expression(
                                "param",
                                0,
                                &Datum::Null,
                                Some(&string_ty()),
                                None,
                            );
                            let e = if shape == 3 {
                                call("concat", string_ty(), vec![arg, parameter])
                            } else {
                                call("coalesce", string_ty(), vec![arg, parameter])
                            };
                            (
                                e,
                                string_ty(),
                                string_ty(),
                                vec![
                                    vec![Datum::new_string("a")],
                                    vec![Datum::Null],
                                    vec![Datum::new_string("b")],
                                    vec![Datum::Null],
                                ],
                            )
                        };
                        let suite = suite(expression);
                        let mut context = leaf::Context::new(profile, Datum::new_string("live"));
                        context.enabled.set(enabled);
                        context.sequential = true;
                        let mut input = chunk(&[schema.clone()], &rows);
                        input.set_sel(Some(vec![0, 3, 2, 1, 3]));
                        let label = format!("{mask}/{shape}/{enabled}/{profile}/{hidden}");
                        let mut output = output_for(&field);
                        let outcome = if shared {
                            suite.run_with_tikv_batch(&context, &mut input, &mut output, &[schema])
                        } else {
                            (|| suite.run(&context, &mut input, &mut output))()
                        };
                        let values = (0..output.num_rows())
                            .map(|i| leaf::frame(&output.get_row(i).get_datum(0, &field)))
                            .collect::<Vec<_>>();
                        records.push(format!(
                            "{label}|{outcome:?}|{values:?}|{}",
                            context.snapshot()
                        ));
                        let route = suite.batch_route_snapshot();
                        if shared
                            && (route.prepared != 1 || route.fallback != 0 || route.refusals != 0)
                        {
                            refused.push(format!("{label}:{route:?}"));
                        }
                    }
                }
            }
        }
    }
    if require_prepared {
        assert!(
            refused.is_empty(),
            "{} edge refusals; {:?}",
            refused.len(),
            refused.iter().take(8).collect::<Vec<_>>()
        );
    }
    records
}
#[test]
fn planner_columns_keep_overflow_diagnostics_and_live_typed_string_phases() {
    let original = edge_records(false, false);
    eprintln!(
        "N57 EDGE ORIGINAL {} {:016X}",
        original.len(),
        digest(&original)
    );
    assert_eq!(
        (original.len(), digest(&original)),
        (240, 0xE68166457C10F6D9)
    );
    let current = edge_records(true, false);
    for (i, (old, new)) in original.iter().zip(&current).enumerate() {
        assert_eq!(old, new, "edge original/candidate record {i}");
    }
}
#[test]
fn planner_columns_prepare_typed_string_and_hidden_diagnostic_profiles() {
    edge_records(true, true);
}

fn diagnostic_records(shared: bool, require_prepared: bool) -> Vec<String> {
    let mut records = vec![];
    for virtual_kind in ["param", "param_effect_deferred", "deferred_row"] {
        for hidden in [false, true] {
            for shape in 0..4 {
                for enabled in [false, true] {
                    for profile in [0, 1, 3] {
                        let Expression::Column(mut c) = column(0) else {
                            unreachable!()
                        };
                        c.virtual_expr = Some(Box::new(leaf::expression(
                            virtual_kind,
                            0,
                            &Datum::Int(9),
                            Some(&ty()),
                            None,
                        )));
                        c.correlated_col_unique_id = 917;
                        c.is_hidden = hidden;
                        let arg = Expression::Column(c);
                        let e = match shape {
                            0 => plus(arg, int(2)),
                            1 => minus(arg, int(2)),
                            2 => mul(arg, int(2)),
                            _ => abs(arg),
                        };
                        let suite = suite(e);
                        let mut context = leaf::Context::new(profile, Datum::Int(77));
                        context.enabled.set(enabled);
                        context.sequential = true;
                        let mut input = chunk(
                            &[ty()],
                            &[vec![Datum::Int(i64::MAX)], vec![Datum::Int(i64::MIN)]],
                        );
                        if shape == 1 || shape == 3 {
                            input.set_sel(Some(vec![1, 0]));
                        }
                        let mut output = output_for(&ty());
                        let outcome = if shared {
                            suite.run_with_tikv_batch(&context, &mut input, &mut output, &[ty()])
                        } else {
                            (|| suite.run(&context, &mut input, &mut output))()
                        };
                        let values = (0..output.num_rows())
                            .map(|i| leaf::frame(&output.get_row(i).get_datum(0, &ty())))
                            .collect::<Vec<_>>();
                        records.push(format!("{virtual_kind}/{hidden}/{shape}/{enabled}/{profile}|{outcome:?}|{values:?}|{}",context.snapshot()));
                        if require_prepared {
                            let r = suite.batch_route_snapshot();
                            assert_eq!((r.prepared, r.fallback, r.refusals), (1, 0, 0));
                        }
                    }
                }
            }
        }
    }
    records
}
#[test]
fn hidden_planner_columns_keep_demanded_live_overflow_metadata() {
    let original = diagnostic_records(false, false);
    eprintln!(
        "N57 DIAGNOSTIC ORIGINAL {} {:016X}",
        original.len(),
        digest(&original)
    );
    assert_eq!(
        (original.len(), digest(&original)),
        (144, 0x6D36FF5166A1AD47)
    );
    let current = diagnostic_records(true, false);
    for (i, (old, new)) in original.iter().zip(&current).enumerate() {
        assert_eq!(old, new, "hidden diagnostic original/candidate record {i}");
    }
}
#[test]
fn hidden_planner_column_diagnostics_use_same_prepared_driver() {
    diagnostic_records(true, true);
}

#[test]
fn planner_annotations_preserve_source_budgets_and_real_storage_declarations() {
    let owner = suite(plus(annotated(3, false), int(2)));
    let compile = |schema: &[FieldType], source, maximum| {
        PreparedBatchSuite::compile(
            &owner,
            schema,
            true,
            0,
            source,
            ExecutionLimits::default(),
            maximum,
        )
    };
    for source in [
        BatchSourceLimits {
            max_metadata_bytes: 1,
            ..limits()
        },
        BatchSourceLimits {
            tree: CompileLimits {
                max_nodes: 1,
                ..CompileLimits::default()
            },
            ..limits()
        },
        BatchSourceLimits {
            tree: CompileLimits {
                max_depth: 1,
                ..CompileLimits::default()
            },
            ..limits()
        },
    ] {
        assert!(
            matches!(compile(&[ty()], source, 8 << 20), Err(error) if error.refusal() == BatchRefusal::Resource)
        );
    }
    // Materialization is checked on demand, rather than during pure compile.
    let mut zero_materialization = compile(&[ty()], limits(), 0).unwrap();
    let mut input = chunk(&[ty()], &[vec![Datum::Int(7)]]);
    let mut output = output_for(&ty());
    assert!(
        matches!(zero_materialization.run(&owner, &crate::NoColumns, &mut EvalContext::default(), &mut input, &mut output, &[ty()]), Err(error) if error.refusal() == BatchRefusal::Resource)
    );
    assert_eq!(output.num_rows(), 0);
    // Ignoring planner flags never permits decoding a different storage code.
    assert!(
        matches!(compile(&[real_ty()], limits(), 8 << 20), Err(error) if error.refusal() == BatchRefusal::Contract)
    );
    assert!(compile(&[ty()], limits(), 8 << 20).is_ok());
}
