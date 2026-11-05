// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Independent genuine native-context contracts before remaining guard closure.
use super::*;
use crate::ReadyValueCache;
use std::cell::RefCell;

struct Spec {
    name: &'static str,
    ret: FieldType,
    args: Vec<(FieldType, Datum)>,
}
fn specs() -> Vec<Spec> {
    let text = string_ty();
    let integer = ty();
    let mut out = Vec::new();
    for name in [
        "char_length",
        "character_length",
        "length",
        "octet_length",
        "bit_length",
        "ascii",
        "crc32",
    ] {
        out.push(Spec {
            name,
            ret: integer.clone(),
            args: vec![(text.clone(), Datum::new_string("éx"))],
        });
    }
    for name in ["substr", "substring", "mid"] {
        for arity in [2, 3] {
            let mut args = vec![
                (text.clone(), Datum::new_string("éxyz")),
                (integer.clone(), Datum::Int(2)),
            ];
            if arity == 3 {
                args.push((integer.clone(), Datum::Int(2)));
            }
            out.push(Spec {
                name,
                ret: text.clone(),
                args,
            });
        }
    }
    let families = vec![
        (integer.clone(), Datum::Int(-4)),
        (integer.clone().with_unsigned(true), Datum::UInt(u64::MAX)),
        (real_ty(), Datum::Real(-1.25)),
        (
            FieldType::new(FieldTypeCode::NewDecimal)
                .with_flen(20)
                .with_decimal(2),
            Datum::Decimal(Decimal::from_literal("-1.25")),
        ),
        (text.clone(), Datum::new_string("éx")),
        (
            FieldType::new(FieldTypeCode::Datetime).with_decimal(6),
            Datum::Time(Time::from_raw_parts(
                tidb_datatype::CoreTime::from_raw(0x1fa0bb8b8b800000),
                tidb_datatype::TimeType::DateTime,
                6,
            )),
        ),
        (
            FieldType::new(FieldTypeCode::Duration).with_decimal(6),
            Datum::Duration(MySqlDuration::from_nanoseconds(-45678900000000, 6).unwrap()),
        ),
        (
            FieldType::new(FieldTypeCode::Json),
            Datum::Json(BinaryJSON::parse("{\"x\":2}").unwrap()),
        ),
        (
            FieldType::new(FieldTypeCode::VectorFloat32),
            Datum::VectorFloat32(VectorFloat32::must_create(vec![1.0, -0.0])),
        ),
    ];
    for (field, value) in families {
        for name in ["any_value", "ifnull", "coalesce"] {
            let arity = if name == "any_value" {
                1
            } else if name == "ifnull" {
                2
            } else {
                3
            };
            out.push(Spec {
                name,
                ret: field.clone(),
                args: (0..arity).map(|_| (field.clone(), value.clone())).collect(),
            });
        }
        for name in ["isnull", "istrue", "isfalse", "not"] {
            out.push(Spec {
                name,
                ret: integer.clone(),
                args: vec![(field.clone(), value.clone())],
            });
        }
    }
    out
}
struct Context {
    cache: ReadyValueCache,
    values: Vec<Datum>,
    events: RefCell<Vec<String>>,
    enabled: bool,
    profile: usize,
}
impl Columns for Context {
    fn get(&self, _: &[String]) -> Option<Datum> {
        panic!("no invented context row")
    }
    fn ready_value_cache(&self) -> Option<&ReadyValueCache> {
        self.events.borrow_mut().push("cache".into());
        Some(&self.cache)
    }
    fn enable_vectorized_expression(&self) -> bool {
        self.events.borrow_mut().push("flag".into());
        self.enabled
    }
    fn param_value(&self, order: usize) -> std::result::Result<Datum, EvalError> {
        self.events.borrow_mut().push(format!("param:{order}"));
        if order == 0 && self.profile == 1 {
            return Err(EvalError::Unsupported(
                "native context parameter unavailable",
            ));
        }
        if order == 0 && self.profile == 2 {
            panic!("native context parameter panic");
        }
        Ok(self.values[order].clone())
    }
}
fn expression(spec: &Spec, shape: usize) -> Expression {
    let args = spec
        .args
        .iter()
        .enumerate()
        .map(|(order, (field, value))| {
            if shape == 0 {
                return literal(value.clone(), field.clone());
            }
            let mut p = Constant::new(value.clone(), field.clone());
            p.param_marker = Some(ParamMarker {
                order: order as i64,
            });
            let p = Expression::Constant(p);
            if shape == 1 {
                p
            } else {
                let mut wrapper = Constant::new(Datum::Null, field.clone());
                wrapper.deferred_expr = Some(Box::new(p));
                Expression::Constant(wrapper)
            }
        })
        .collect();
    call(spec.name, spec.ret.clone(), args)
}
fn context_records(projection: bool) -> (Vec<String>, Vec<String>) {
    let mut records = Vec::new();
    let mut routes = Vec::new();
    let mut eligible = 0;
    for (index, spec) in specs().iter().enumerate() {
        for shape in 0..3 {
            let root = expression(spec, shape);
            if crate::scalar_function::static_batch_target(&root).is_some() {
                eligible += 1;
            }
            for enabled in [false, true] {
                for null in [false, true] {
                    for profile in 0..3 {
                        for selection in [None, Some(vec![2, 0, 2]), Some(vec![])] {
                            let context = Context {
                                cache: ReadyValueCache::new(),
                                values: spec
                                    .args
                                    .iter()
                                    .enumerate()
                                    .map(|(i, (_, v))| {
                                        if null && i == 0 {
                                            Datum::Null
                                        } else {
                                            v.clone()
                                        }
                                    })
                                    .collect(),
                                events: RefCell::new(Vec::new()),
                                enabled,
                                profile,
                            };
                            let suite = EvaluatorSuite::new(vec![root.clone()], false);
                            for reuse in 0..2 {
                                let mut input = chunk(
                                    &[ty()],
                                    &[
                                        vec![Datum::Int(1)],
                                        vec![Datum::Int(2)],
                                        vec![Datum::Int(3)],
                                    ],
                                );
                                input.set_sel(selection.clone());
                                let mut output = Chunk::new_empty(std::slice::from_ref(&spec.ret));
                                output.append_null(0);
                                let result =
                                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                        if projection {
                                            suite.run_with_tikv_batch(
                                                &context,
                                                &mut input,
                                                &mut output,
                                                &[ty()],
                                            )
                                        } else {
                                            (|| suite.run(&context, &mut input, &mut output))()
                                        }
                                    }));
                                let status = match result {
                                    Ok(v) => format!("{v:?}"),
                                    Err(p) => format!(
                                        "PANIC:{:?}",
                                        p.downcast_ref::<String>()
                                            .map(String::as_str)
                                            .or_else(|| p.downcast_ref::<&str>().copied())
                                    ),
                                };
                                let label=format!("{index}/{}/{:?}/{shape}/{enabled}/{null}/{profile}/{selection:?}/{reuse}",spec.name,spec.ret);
                                records.push(format!(
                                    "{label}|{status}|{:?}|{:?}",
                                    output.column(0),
                                    context.events.borrow()
                                ));
                                routes.push(format!("{label}|{:?}", suite.batch_route_snapshot()));
                            }
                        }
                    }
                }
            }
        }
    }
    eprintln!("N63 genuine native eligible source shapes {eligible}");
    (records, routes)
}
fn digest(records: &[String]) -> u64 {
    records
        .iter()
        .flat_map(|s| s.bytes())
        .fold(0xcbf29ce484222325, |h, b| {
            (h ^ u64::from(b)).wrapping_mul(0x100000001b3)
        })
}
#[test]
fn context_native_original_capture() {
    let (a, _) = context_records(false);
    let (b, _) = context_records(false);
    assert_eq!(a, b);
    eprintln!("N63 native original {} {:016X}", a.len(), digest(&a));
    assert_eq!((a.len(), digest(&a)), (16416, 0x8A44_7106_7FCF_4CED));
}
#[test]
fn context_projection_original_capture() {
    let (a, _) = context_records(true);
    let (b, _) = context_records(true);
    assert_eq!(a, b);
    eprintln!("N63 Projection contract {} {:016X}", a.len(), digest(&a));
    assert_eq!((a.len(), digest(&a)), (16416, 0x58F7_ED71_8027_3A11));
}
#[test]
fn context_projection_actual_preserves_original_and_retires_admissions() {
    let old: Vec<String> =
        serde_json::from_str(include_str!("batch_context_native_original.json")).unwrap();
    assert_eq!((old.len(), digest(&old)), (16416, 0x58F7_ED71_8027_3A11));
    let scalar_before = crate::test_evaluation::shared_entries();
    let (actual, routes) = context_records(true);
    assert_eq!(
        scalar_before,
        crate::test_evaluation::shared_entries(),
        "shared context profile entered old scalar tree"
    );
    assert_eq!(old.len(), actual.len());
    let mut differences = 0;
    let mut refused = 0;
    let mut replayed = 0;
    for ((old, new), route) in old.iter().zip(&actual).zip(routes) {
        if old != new {
            if differences < 12 {
                eprintln!("N63 context difference\nOLD {old}\nNEW {new}\nROUTE {route}");
            }
            differences += 1;
        }
        if !route.contains("refusals: 0,") {
            refused += 1;
        }
        if !route.contains("fallback: 0,") {
            replayed += 1;
        }
    }
    eprintln!("N63 actual records={} differences={differences} refused_records={refused} fallback_records={replayed}",actual.len());
    assert_eq!((differences, refused, replayed), (0, 0, 0));
}

#[test]
fn context_native_profiles_keep_source_limits_owner_and_error_recovery() {
    let root = expression(&specs()[0], 1);
    let suite = EvaluatorSuite::new(vec![root.clone()], false);
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
        let _prepared = PreparedBatchSuite::compile(
            &suite,
            &[ty()],
            true,
            0,
            source,
            ExecutionLimits::default(),
            8 << 20,
        )
        .unwrap();
    }
    let mut prepared = PreparedBatchSuite::compile(
        &suite,
        &[ty()],
        true,
        0,
        limits(),
        ExecutionLimits::default(),
        8 << 20,
    )
    .unwrap();
    let other = EvaluatorSuite::new(vec![root], false);
    let mut context = Context {
        cache: ReadyValueCache::new(),
        values: vec![Datum::new_string("éx")],
        events: RefCell::new(Vec::new()),
        enabled: true,
        profile: 0,
    };
    let mut input = chunk(&[ty()], &[vec![Datum::Int(1)]]);
    let mut output = output();
    let error = prepared
        .run(
            &other,
            &context,
            &mut EvalContext::default(),
            &mut input,
            &mut output,
            &[ty()],
        )
        .unwrap_err();
    assert_eq!(error.refusal(), BatchRefusal::Contract);
    assert!(context.events.borrow().is_empty());
    assert_eq!(output.num_rows(), 0);
    context.profile = 1;
    assert!(suite
        .run_with_tikv_batch(&context, &mut input, &mut output, &[ty()])
        .is_err());
    assert_eq!(output.num_rows(), 0);
    assert_eq!(suite.batch_route_snapshot().prepared, 1);
    context.profile = 0;
    context.events.borrow_mut().clear();
    suite
        .run_with_tikv_batch(&context, &mut input, &mut output, &[ty()])
        .unwrap();
    assert_eq!(output.column(0).get_int64(0), 2);
    let route = suite.batch_route_snapshot();
    assert_eq!((route.prepared, route.refusals, route.fallback), (2, 0, 0));
}
