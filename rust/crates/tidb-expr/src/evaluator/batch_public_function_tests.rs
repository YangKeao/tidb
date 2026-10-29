// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Genuine direct ScalarFunction callers, before borrowed-root compilation.
use super::*;
use std::sync::{Arc, RwLock};
use tidb_datatype::{MysqlEnum, MysqlSet};

fn samples() -> Vec<(FieldType, Datum)> {
    vec![
        (
            FieldType::new(FieldTypeCode::Enum).with_elems(["a", "b"]),
            Datum::new_enum(MysqlEnum::new("b", 2), Collation::Binary),
        ),
        (
            FieldType::new(FieldTypeCode::Set).with_elems(["a", "b"]),
            Datum::new_set(MysqlSet::new("a,b", 3), Collation::Binary),
        ),
        (
            FieldType::new(FieldTypeCode::Bit).with_flen(8),
            Datum::Bit(tidb_datatype::BinaryLiteral::from(vec![0xa5])),
        ),
        (ty(), Datum::Int(7)),
        (string_ty(), Datum::new_string("7")),
    ]
}
fn argument(kind: usize, value: &Datum, field: Option<&FieldType>) -> Expression {
    let column = || {
        let mut column = Column::default();
        column.index = 0;
        column.unique_id = 1;
        column.ret_type = field.cloned();
        column.orig_name = "t.actual".into();
        column
    };
    if kind == 1 {
        return Expression::Column(column());
    }
    if kind == 3 {
        return Expression::CorrelatedColumn(crate::column::CorrelatedColumn {
            column: column(),
            data: Some(Arc::new(RwLock::new(value.clone()))),
        });
    }
    let mut constant = Constant::default();
    constant.value = if kind == 0 {
        value.clone()
    } else {
        Datum::new_string("actual cached planning value")
    };
    constant.ret_type = field.cloned();
    if kind == 2 {
        constant.param_marker = Some(ParamMarker { order: 0 });
    }
    if kind == 4 {
        constant.deferred_expr = Some(Box::new(Expression::Column(column())));
    }
    Expression::Constant(constant)
}
fn function(name: &str, target: Option<FieldType>, argument: Expression) -> ScalarFunction {
    let mut function = ScalarFunction::default();
    function.func_name = CiString::new(name);
    function.ret_type = target;
    function.args = match name {
        "any_value" => vec![argument],
        "name_const" => vec![literal(Datum::new_string("label"), string_ty()), argument],
        "coalesce" => vec![argument, null()],
        "if" => vec![int(1), argument, null()],
        "concat" => vec![argument, literal(Datum::new_string("!"), string_ty())],
        _ => unreachable!(),
    };
    function
}
fn function_records(
    mut evaluate: impl FnMut(
        &ScalarFunction,
        &leaf::Context,
        Row<'_>,
    ) -> std::result::Result<Datum, EvalError>,
) -> Vec<String> {
    let returns = samples()
        .into_iter()
        .map(|(field, _)| Some(field))
        .chain(std::iter::once(None))
        .collect::<Vec<_>>();
    let mut out = Vec::new();
    for (si, (field, value)) in samples().into_iter().enumerate() {
        for missing in [false, true] {
            let flags =
                if !missing && matches!(field.code(), FieldTypeCode::Enum | FieldTypeCode::Set) {
                    vec![0, FieldTypeFlags::ENUM_SET_AS_INT]
                } else {
                    vec![0]
                };
            for source_flags in flags {
                let field = field.clone().with_flags(source_flags);
                for is_null in [false, true] {
                    let value = if is_null { Datum::Null } else { value.clone() };
                    let input = chunk(std::slice::from_ref(&field), &[vec![value.clone()]]);
                    for kind in 0..5 {
                        for (ri, target) in returns.iter().enumerate() {
                            let flags = if target.is_some() {
                                vec![
                                    0,
                                    FieldTypeFlags::ENUM_SET_AS_INT,
                                    FieldTypeFlags::UNSIGNED,
                                    FieldTypeFlags::ENUM_SET_AS_INT | FieldTypeFlags::UNSIGNED,
                                ]
                            } else {
                                vec![0]
                            };
                            for flags in flags {
                                for name in ["any_value", "name_const", "coalesce", "if", "concat"]
                                {
                                    let actual = Box::new(function(
                                        name,
                                        target.clone().map(|field| field.with_flags(flags)),
                                        argument(kind, &value, (!missing).then_some(&field)),
                                    ));
                                    for profile in [0, 1, 3] {
                                        for row in [Row::empty(), input.physical_row(0)] {
                                            let context =
                                                leaf::Context::new(profile, value.clone());
                                            let observed = leaf::observe(
                                                || {
                                                    evaluate(&actual, &context, row)
                                                        .map_err(|error| (Datum::Null, error))
                                                },
                                                &context,
                                            );
                                            out.push(format!("{si}/{missing}/{source_flags}/{is_null}/{kind}/{ri}/{flags}/{name}/{profile}/{}|{observed}",row.is_empty()));
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    out
}
#[test]
fn direct_function_original_return_views_and_optional_fields() {
    let records = function_records(|function, context, row| function.eval(context, row));
    eprintln!(
        "N52 original function return views {} {:016X}",
        records.len(),
        digest(&records)
    );
    assert_eq!(
        (records.len(), digest(&records)),
        (75600, 0x9D7254FFD2481AF5)
    );
    let (field, value) = samples().remove(0);
    let function = function(
        "any_value",
        Some(
            field
                .clone()
                .with_added_flags(FieldTypeFlags::ENUM_SET_AS_INT),
        ),
        literal(value.clone(), field.clone()),
    );
    let value = function.eval(&crate::NoColumns, Row::empty()).unwrap();
    assert!(matches!(value, Datum::Enum(..)));
    assert_eq!(
        crate::expression::apply_enum_set_integer_view(value, function.ret_type.as_ref()),
        Datum::UInt(2)
    );
}

fn wire_field(code: i32, unsigned: bool) -> db_pb::FieldType {
    db_pb::FieldType {
        tp: Some(code),
        flag: Some(if unsigned { 32 } else { 0 }),
        flen: Some(if code == 246 { 65 } else { 128 }),
        decimal: Some(if matches!(code, 11 | 12) {
            3
        } else if code == 246 {
            5
        } else if code == 8 {
            0
        } else {
            -1
        }),
        charset: Some("binary".into()),
        collate: Some(-63),
        ..Default::default()
    }
}
fn selected_function(
    signature: db_pb::ScalarFuncSig,
    code: i32,
    arity: usize,
    value: Option<i64>,
    unsigned: bool,
) -> ScalarFunction {
    let mut bytes = vec![];
    if let Some(value) = value {
        tidb_codec::encode_int(&mut bytes, value);
    }
    let child = db_pb::Expr {
        tp: Some(if value.is_some() {
            db_pb::ExprType::Int64
        } else {
            db_pb::ExprType::Null
        } as i32),
        val: Some(bytes),
        field_type: Some(wire_field(8, unsigned)),
        ..Default::default()
    };
    let wire = db_pb::Expr {
        tp: Some(db_pb::ExprType::ScalarFunc as i32),
        sig: Some(signature as i32),
        children: vec![child; arity],
        field_type: Some(wire_field(code, unsigned)),
        ..Default::default()
    };
    let Expression::ScalarFunction(function) = pb_to_expr(&wire, &[]).unwrap() else {
        panic!("real selected function")
    };
    function
}
fn selected_records(
    mut evaluate: impl FnMut(
        &ScalarFunction,
        &leaf::Context,
        Row<'_>,
    ) -> std::result::Result<Datum, EvalError>,
) -> Vec<String> {
    use db_pb::ScalarFuncSig as Sig;
    let mut out = vec![];
    for (sig, code) in [
        (Sig::CastIntAsString, 253),
        (Sig::CastIntAsReal, 5),
        (Sig::CastIntAsDecimal, 246),
        (Sig::CastIntAsJson, 245),
        (Sig::CastIntAsDuration, 11),
        (Sig::CastIntAsTime, 12),
    ] {
        for unsigned in [false, true] {
            for arity in [0, 1, 2] {
                for value in [None, Some(-1), Some(0), Some(7), Some(20201021)] {
                    let function = Box::new(selected_function(sig, code, arity, value, unsigned));
                    for profile in [0, 1, 3] {
                        let context = leaf::Context::new(profile, Datum::Null);
                        let observed = leaf::observe(
                            || {
                                evaluate(&function, &context, Row::empty())
                                    .map_err(|error| (Datum::Null, error))
                            },
                            &context,
                        );
                        out.push(format!(
                            "{sig:?}/{unsigned}/{arity}/{value:?}/{profile}|{observed}"
                        ));
                    }
                }
            }
        }
    }
    out
}
#[test]
fn direct_function_original_selected_pb_owner_header_and_values() {
    let records = selected_records(|function, context, row| function.eval(context, row));
    eprintln!(
        "N52 original selected function {} {:016X}",
        records.len(),
        digest(&records)
    );
    assert_eq!((records.len(), digest(&records)), (540, 0x4CDD027B2A300F8D));
}

struct OwnerContext {
    inner: leaf::Context,
    expected: usize,
}
impl Columns for OwnerContext {
    fn get(&self, _: &[String]) -> Option<Datum> {
        panic!("no named lookup")
    }
    fn param_value(&self, order: usize) -> std::result::Result<Datum, EvalError> {
        self.inner.param_value(order)
    }
    fn enable_vectorized_expression(&self) -> bool {
        panic!("direct function has no vector getter")
    }
    fn rand_next(&self) -> Option<f64> {
        self.inner.events.borrow_mut().push("rand-next".into());
        Some(0.25)
    }
    fn rand_seeded_next(&self, key: usize, seed: i64) -> Option<f64> {
        assert_eq!(key, self.expected, "genuine function owner");
        self.inner
            .events
            .borrow_mut()
            .push(format!("seed:{seed}:real-owner"));
        Some(0.5)
    }
    fn context_id(&self) -> u64 {
        self.inner.events.borrow_mut().push("context-id".into());
        17
    }
}
fn owner_records(
    mut evaluate: impl FnMut(
        &ScalarFunction,
        &OwnerContext,
        Row<'_>,
    ) -> std::result::Result<Datum, EvalError>,
) -> Vec<String> {
    let input = chunk(&[ty()], &[vec![Datum::Int(7)]]);
    let mut out = vec![];
    for kind in 0..5 {
        let mut function = ScalarFunction::default();
        function.func_name = CiString::new("rand");
        function.ret_type = Some(real_ty());
        function.args = match kind {
            0 => vec![],
            1 => vec![int(7)],
            2 => vec![argument(2, &Datum::Int(7), Some(&ty()))],
            3 => vec![column(0)],
            4 => vec![int(7), int(8)],
            _ => unreachable!(),
        };
        let original = Box::new(function);
        let cloned = Box::new(original.as_ref().clone());
        assert_ne!(
            std::ptr::from_ref(original.as_ref()),
            std::ptr::from_ref(cloned.as_ref())
        );
        for (owner, function) in [&original, &cloned].into_iter().enumerate() {
            let context = OwnerContext {
                inner: leaf::Context::new(0, Datum::Int(7)),
                expected: std::ptr::from_ref(function.as_ref()) as usize,
            };
            for occurrence in 0..3 {
                let observed = leaf::observe(
                    || {
                        evaluate(function, &context, input.physical_row(0))
                            .map_err(|error| (Datum::Null, error))
                    },
                    &context.inner,
                );
                out.push(format!("{kind}/{owner}/{occurrence}|{observed}"));
            }
        }
    }
    out
}
#[test]
fn direct_function_original_rand_and_clone_owner_identity() {
    let records = owner_records(|function, context, row| function.eval(context, row));
    eprintln!(
        "N52 original function owner {} {:016X}",
        records.len(),
        digest(&records)
    );
    assert_eq!((records.len(), digest(&records)), (30, 0xE5DCB211D42975EC));
}

struct CacheContext {
    inner: leaf::Context,
    cache: crate::ReadyValueCache,
    id: Cell<u64>,
}
impl Columns for CacheContext {
    fn get(&self, _: &[String]) -> Option<Datum> {
        panic!("no named lookup")
    }
    fn param_value(&self, order: usize) -> std::result::Result<Datum, EvalError> {
        self.inner.param_value(order)
    }
    fn enable_vectorized_expression(&self) -> bool {
        panic!("direct function has no vector getter")
    }
    fn ready_value_cache(&self) -> Option<&crate::ReadyValueCache> {
        self.inner.events.borrow_mut().push("ready-cache".into());
        Some(&self.cache)
    }
    fn context_id(&self) -> u64 {
        self.inner
            .events
            .borrow_mut()
            .push(format!("context-id:{}", self.id.get()));
        self.id.get()
    }
    fn time_zone(&self) -> SessionTimeZone {
        self.inner.time_zone()
    }
    fn type_flags(&self) -> tidb_datatype::ConversionFlags {
        self.inner.type_flags()
    }
    fn append_warning(&self, code: u16, message: &str) {
        self.inner.append_warning(code, message)
    }
}
fn cache_records(
    mut evaluate: impl FnMut(
        &ScalarFunction,
        &CacheContext,
        Row<'_>,
    ) -> std::result::Result<Datum, EvalError>,
) -> Vec<String> {
    let mut parameter = Constant::new(Datum::new_string("actual stored pattern"), string_ty());
    parameter.param_marker = Some(ParamMarker { order: 0 });
    let function = Box::new(ScalarFunction::new(
        CiString::new("regexp_like"),
        ty(),
        vec![
            literal(Datum::new_string("abc"), string_ty()),
            Expression::Constant(parameter),
        ],
    ));
    let cloned = Box::new(function.as_ref().clone());
    let mut out = vec![];
    for (owner, actual) in [function.as_ref(), cloned.as_ref()].into_iter().enumerate() {
        let mut context = CacheContext {
            inner: leaf::Context::new(0, Datum::new_string("^a")),
            cache: crate::ReadyValueCache::new(),
            id: Cell::new(17),
        };
        for (step, (id, pattern)) in [(17, "^a"), (17, "^b"), (18, "^b"), (19, "["), (19, "^a")]
            .into_iter()
            .enumerate()
        {
            context.id.set(id);
            context.inner.value = Datum::new_string(pattern);
            let observed = leaf::observe(
                || evaluate(actual, &context, Row::empty()).map_err(|error| (Datum::Null, error)),
                &context.inner,
            );
            out.push(format!("{owner}/{step}|{observed}"));
            assert!(context.cache.original_scope_status().is_ok());
        }
    }
    out
}
#[test]
fn direct_function_original_cache_reuse_context_changes_and_failed_initializers() {
    let records = cache_records(|function, context, row| function.eval(context, row));
    eprintln!(
        "N52 original function cache {} {:016X}",
        records.len(),
        digest(&records)
    );
    assert_eq!((records.len(), digest(&records)), (10, 0x05308B90353B745C));
}

fn shared_function(
    function: &ScalarFunction,
    context: &dyn Columns,
    row: Row<'_>,
) -> std::result::Result<Datum, EvalError> {
    super::super::super::scalar_source::BorrowedExpression::compile_function(
        function,
        row.len(),
        true,
        limits(),
        ExecutionLimits::default(),
        1 << 20,
    )
    .and_then(|mut program| program.eval(context, row))
    .map_err(|error| match error.into_evaluator_error() {
        EvaluatorError::Eval(error) => error,
        other => panic!("unexpected genuine function error {other:?}"),
    })
}
fn same_records(original: &[String], actual: &[String], count: usize, pin: u64) {
    assert_eq!((original.len(), digest(original)), (count, pin));
    assert_eq!(actual.len(), count);
    for (index, (want, got)) in original.iter().zip(actual).enumerate() {
        assert_eq!(got, want, "genuine function root record {index}");
    }
    assert_eq!((actual.len(), digest(actual)), (count, pin));
}
#[test]
fn direct_function_shared_return_views_and_optional_fields() {
    let original = function_records(|function, context, row| function.eval(context, row));
    let actual = function_records(|function, context, row| shared_function(function, context, row));
    same_records(&original, &actual, 75600, 0x9D7254FFD2481AF5);
}
#[test]
fn direct_function_shared_selected_pb_owner_header_and_values() {
    let original = selected_records(|function, context, row| function.eval(context, row));
    let actual = selected_records(|function, context, row| shared_function(function, context, row));
    same_records(&original, &actual, 540, 0x4CDD027B2A300F8D);
}
#[test]
fn direct_function_shared_rand_and_clone_owner_identity() {
    let original = owner_records(|function, context, row| function.eval(context, row));
    let actual = owner_records(|function, context, row| shared_function(function, context, row));
    same_records(&original, &actual, 30, 0xE5DCB211D42975EC);
}
#[test]
fn direct_function_shared_cache_reuse_context_changes_and_failed_initializers() {
    let original = cache_records(|function, context, row| function.eval(context, row));
    let actual = cache_records(|function, context, row| shared_function(function, context, row));
    same_records(&original, &actual, 10, 0x05308B90353B745C);
}
#[test]
fn direct_function_shared_keeps_the_prior_function_caller_pin() {
    let actual = records(
        |expression, context, row| {
            let Expression::ScalarFunction(function) = expression else {
                unreachable!()
            };
            shared_function(function, context, row).map_err(|error| (Datum::Null, error))
        },
        true,
    );
    assert_eq!((actual.len(), digest(&actual)), (5880, 0x2550BA35CB389B63));
}
#[test]
fn direct_function_result_role_cannot_replace_an_expression_result_view() {
    let (field, value) = samples().remove(0);
    let function = function(
        "any_value",
        Some(
            field
                .clone()
                .with_added_flags(FieldTypeFlags::ENUM_SET_AS_INT),
        ),
        literal(value.clone(), field),
    );
    let expression = int(7);
    assert_eq!(
        lower_source_plan(
            SourceNodeRef::from(&function),
            SourceInputs::PhysicalRow { width: 0 },
            true,
            1,
            limits(),
            Some(NativeView::RootValue),
            SourceConsumer::Scalar
        )
        .err()
        .unwrap()
        .refusal(),
        BatchRefusal::Contract
    );
    assert_eq!(
        lower_source_plan(
            &expression,
            SourceInputs::PhysicalRow { width: 0 },
            true,
            1,
            limits(),
            Some(NativeView::FunctionRoot),
            SourceConsumer::Scalar
        )
        .err()
        .unwrap()
        .refusal(),
        BatchRefusal::Contract
    );
    assert_eq!(
        shared_function(&function, &crate::NoColumns, Row::empty()).unwrap(),
        value
    );
    let mut store = NativeValueStore::new(64 << 10).unwrap();
    store.begin_window().unwrap();
    let origin = NativeValueOrigin {
        node: 0,
        row: InputRow {
            occurrence: 0,
            input_row: 0,
        },
    };
    let token = store
        .insert_clone(&value, NativeValueRole::FunctionOutput, origin)
        .unwrap();
    assert!(store.get(&token, NativeValueRole::Output, origin).is_err());
    assert!(store
        .get(&token, NativeValueRole::CharInput, origin)
        .is_err());
    assert_eq!(
        *store
            .get(&token, NativeValueRole::FunctionOutput, origin)
            .unwrap(),
        value
    );
}

#[test]
fn direct_function_same_plan_keeps_numeric_error_diagnostics_and_live_contexts() {
    use super::super::super::scalar_source::BorrowedExpression;
    use crate::tikv::typed_dynamic_tests::Context as Script;
    for name in ["abs", "plus", "minus", "mul", "cot", "exp", "pow"] {
        let integer = matches!(name, "abs" | "plus" | "minus" | "mul");
        let field = if integer { ty() } else { real_ty() };
        for declared in [false, true] {
            let mut function = ScalarFunction::default();
            function.func_name = CiString::new(name);
            function.ret_type = declared.then_some(field.clone());
            function.args = vec![argument(2, &Datum::Null, Some(&field))];
            if matches!(name, "plus" | "minus" | "mul" | "pow") {
                function.args.push(literal(
                    if integer {
                        Datum::Int(1)
                    } else {
                        Datum::Real(2.0)
                    },
                    field.clone(),
                ));
            }
            let mut plan = BorrowedExpression::compile_function(
                &function,
                0,
                true,
                limits(),
                ExecutionLimits::default(),
                1 << 20,
            )
            .unwrap();
            for value in [
                Datum::Null,
                Datum::Int(7),
                Datum::Int(i64::MIN),
                Datum::Int(i64::MAX),
                Datum::Real(0.0),
                Datum::Real(10000.0),
                Datum::new_string("bad"),
            ] {
                for profile in [0, 1, 3] {
                    let old = Script::new(profile, vec![value.clone(); 32]);
                    let new = Script::new(profile, vec![value.clone(); 32]);
                    let want = leaf::observe(
                        || {
                            function
                                .eval(&old, Row::empty())
                                .map_err(|e| (Datum::Null, e))
                        },
                        &old.inner,
                    );
                    let got = leaf::observe(
                        || {
                            plan.eval(&new, Row::empty()).map_err(|e| {
                                match e.into_evaluator_error() {
                                    EvaluatorError::Eval(e) => (Datum::Null, e),
                                    other => panic!("unexpected {other:?}"),
                                }
                            })
                        },
                        &new.inner,
                    );
                    assert_eq!(got, want, "{name}/{declared}/{profile}/{value:?}");
                }
            }
        }
    }
}

#[test]
fn direct_function_same_budget_recovers_after_resource_refusal_and_bad_row_binding() {
    use super::super::super::scalar_source::BorrowedExpression;
    let function = function("coalesce", None, argument(2, &Datum::Null, None));
    let mut plan = BorrowedExpression::compile_function(
        &function,
        0,
        true,
        limits(),
        ExecutionLimits::default(),
        64 << 10,
    )
    .unwrap();
    let huge = leaf::Context::new(0, Datum::Bytes(vec![b'x'; 2 << 20]));
    assert_eq!(
        plan.eval(&huge, Row::empty()).unwrap_err().refusal(),
        BatchRefusal::Resource
    );
    let empty = leaf::Context::new(0, Datum::Null);
    assert_eq!(plan.eval(&empty, Row::empty()).unwrap(), Datum::Null);
    let small = leaf::Context::new(0, Datum::Int(7));
    assert_eq!(plan.eval(&small, Row::empty()).unwrap(), Datum::Int(7));
    let input = chunk(&[ty()], &[vec![Datum::Int(0)]]);
    let untouched = leaf::Context::new(0, Datum::Int(8));
    assert_eq!(
        plan.eval(&untouched, input.physical_row(0))
            .unwrap_err()
            .refusal(),
        BatchRefusal::Contract
    );
    assert_eq!(untouched.calls.get(), 0);
    assert_eq!(plan.eval(&untouched, Row::empty()).unwrap(), Datum::Int(8));
}

#[test]
fn expression_preflight_keeps_original_reference_storage_budget() {
    // The existing heterogeneous Projection stays within a fixed 1 MiB
    // source budget. Adding a distinct function root must not widen every
    // ordinary Expression entry in its temporary source arrays.
    let expression = literal(Datum::Int(1), ty());
    let checked = preflight(&expression, &[], limits()).unwrap();
    assert_eq!(
        std::mem::size_of_val(&checked[0].node),
        std::mem::size_of::<&Expression>()
    );
}

#[test]
fn public_function_contracts_match_frozen_results() {
    let returns = (|| function_records(|f, c, r| f.eval(c, r)))();
    assert_eq!(
        (returns.len(), digest(&returns)),
        (75600, 0x9D7254FFD2481AF5)
    );
    let pb = (|| selected_records(|f, c, r| f.eval(c, r)))();
    assert_eq!((pb.len(), digest(&pb)), (540, 0x4CDD027B2A300F8D));
    let owner = (|| owner_records(|f, c, r| f.eval(c, r)))();
    assert_eq!((owner.len(), digest(&owner)), (30, 0xE5DCB211D42975EC));
    let cache = (|| cache_records(|f, c, r| f.eval(c, r)))();
    assert_eq!((cache.len(), digest(&cache)), (10, 0x05308B90353B745C));
}
