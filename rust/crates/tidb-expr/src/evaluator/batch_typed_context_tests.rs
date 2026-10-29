// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Actual staged typed producer, distinct row reads and invocation broadcasts.
use super::super::hosts::{NativeBatchHosts, NativeHostMetadata, NativeTypedContext};
use super::*;
use crate::tikv::{dynamic_leaf_tests as leaf, typed_dynamic_tests as proof};
use tidb_query_datatype::codec::data_type::ScalarValueRef;
use tidb_query_expr::local::{
    HostArgReply, HostInvocation, HostSignature, HostSlot, HostStart, HostStep, LocalHostServices,
};

fn native_error(provider: &mut NativeBatchHosts<'_>, error: LocalError) -> EvalError {
    provider.take_native_error().unwrap_or_else(|| {
        EvalError::ExpressionRuntimeFailure(crate::ExpressionRuntimeFailure::from_local_eval(
            error, None,
        ))
    })
}
fn typed_invoke(
    provider: &mut NativeBatchHosts<'_>,
    ctx: &dyn crate::Columns,
    expression: &Expression,
    input: &Chunk,
    store: &RefCell<NativeValueStore>,
    slot: HostSlot,
    args: &[tipb::FieldType],
    field: &tipb::FieldType,
    row: InputRow,
) -> std::result::Result<Datum, EvalError> {
    let mut eval = EvalContext::default();
    let start = match provider.start(
        &mut eval,
        HostInvocation {
            slot,
            row,
            arg_types: args,
            return_type: field,
        },
    ) {
        Ok(start) => start,
        Err(error) => return Err(native_error(provider, error)),
    };
    let values = match start {
        HostStart::Ready(values) => values,
        HostStart::Pending { task, request } => {
            assert_eq!(request.index, 0);
            let Expression::Constant(constant) = expression else {
                panic!("only a deferred source demands its child")
            };
            let value = match constant
                .deferred_expr
                .as_ref()
                .unwrap()
                .eval(ctx, input.get_row(row.input_row))
            {
                Ok(value) => value,
                Err(error) => {
                    provider.cancel(&task);
                    return Err(error);
                }
            };
            let token = store
                .borrow_mut()
                .insert_clone(
                    &value,
                    NativeValueRole::CharInput,
                    NativeValueOrigin { node: 1, row },
                )
                .unwrap();
            let child = VectorValue::from_scalar(&ScalarValue::Bytes(Some(token.to_vec())), 1);
            match provider.resume(
                &mut eval,
                &task,
                HostArgReply {
                    index: 0,
                    field_type: field,
                    values: &child,
                },
            ) {
                Ok(HostStep::Ready(values)) => values,
                Ok(HostStep::NeedArg(_) | HostStep::NeedArgRow { .. }) => {
                    panic!("typed producer has exactly one child phase")
                }
                Err(error) => {
                    provider.cancel(&task);
                    return Err(native_error(provider, error));
                }
            }
        }
    };
    let ScalarValueRef::Bytes(Some(token)) = values.get_scalar_ref(0) else {
        panic!("typed values use genuine capabilities")
    };
    Ok(store
        .borrow()
        .get(
            token,
            NativeValueRole::CharInput,
            NativeValueOrigin { node: 0, row },
        )
        .unwrap()
        .clone())
}
fn observe_typed(body: impl FnOnce() -> std::result::Result<Datum, EvalError>) -> String {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(body)) {
        Ok(Ok(value)) => format!("VALUE:{}", leaf::frame(&value)),
        Ok(Err(error)) => format!("ERROR:{error:?}"),
        Err(p) => format!(
            "PANIC:{:?}",
            p.downcast_ref::<String>()
                .map(String::as_str)
                .or_else(|| p.downcast_ref::<&str>().copied())
        ),
    }
}
#[test]
fn typed_context_runtime_preserves_public_domains_deferred_priority_and_callbacks() {
    if isolate_common_corpus("typed_context_tests::typed_context_runtime_preserves_public_domains_deferred_priority_and_callbacks") {return;}
    let mut cases = 0;
    for kind in leaf::KINDS {
        for declaration in crate::tikv::eager_scalar_value_tests::types()
            .into_iter()
            .map(Some)
            .chain(std::iter::once(None))
        {
            for value in crate::tikv::session_read_tests::values() {
                for profile in 0..4 {
                    for target in proof::targets() {
                        let make = || {
                            leaf::expression(
                                kind,
                                0,
                                &value,
                                declaration.as_ref(),
                                Some(std::sync::Arc::new(std::sync::RwLock::new(value.clone()))),
                            )
                        };
                        let actual = make();
                        let reference = make();
                        let descriptor =
                            crate::typed_context_value::TypedContextValue::capture(&actual)
                                .unwrap();
                        let arity = usize::from(descriptor.demands_child());
                        let field = projected(&native_bytes_type(), true).unwrap();
                        let args = vec![field.clone(); arity];
                        let catalog = HostCatalog::for_sql_batch(vec![(
                            SqlBatchHostOperation::EagerValuesNative,
                            HostSignature {
                                arg_types: args.clone().into(),
                                return_type: field.clone(),
                            },
                        )])
                        .unwrap();
                        let slot = catalog.slot(0).unwrap();
                        let metadata = [NativeHostMetadata::TypedContext(NativeTypedContext {
                            descriptor: Some(Box::new(descriptor.into())),
                            target,
                            cache_slot: None,
                            argument: (arity == 1).then_some(1),
                            output_node: 0,
                            output_role: NativeValueRole::CharInput,
                        })];
                        let ctx = proof::Context::new(profile, vec![value.clone()]);
                        let old = proof::Context::new(profile, vec![value.clone()]);
                        let store = RefCell::new(NativeValueStore::new(1 << 20).unwrap());
                        store.borrow_mut().begin_window().unwrap();
                        let mut provider = NativeBatchHosts::new(catalog, &metadata, &ctx, 1 << 20)
                            .with_native_values(Some(&store));
                        let input = leaf::input(true);
                        let row = InputRow {
                            occurrence: 4,
                            input_row: 2,
                        };
                        let got = observe_typed(|| {
                            typed_invoke(
                                &mut provider,
                                &ctx,
                                &actual,
                                &input,
                                &store,
                                slot,
                                &args,
                                &field,
                                row,
                            )
                        });
                        let want = observe_typed(|| match &reference {
                            Expression::Constant(c) => {
                                c.eval_typed_as_on_row(&old, input.get_row(2), target)
                            }
                            Expression::CorrelatedColumn(c) => c.eval_typed(&old, target),
                            _ => unreachable!(),
                        });
                        let label =
                            format!("{kind} {declaration:?} {value:?} p={profile} {target:?}");
                        assert_eq!(got, want, "typed runtime {label}");
                        assert_eq!(
                            ctx.snapshot(),
                            old.snapshot(),
                            "typed runtime effects {label}"
                        );
                        assert!(tidb_query_expr::local::value_kernel_work_remaining().is_none());
                        cases += 1;
                    }
                }
            }
        }
    }
    assert_eq!(cases, 73872);
    eprintln!("TYPED CONTEXT ACTUAL RUNTIME {cases}");
}
#[test]
fn typed_context_runtime_caches_each_bulk_source_across_windows_and_keeps_row_reads_live() {
    for cached in [false, true] {
        let expression = leaf::expression("param_deferred", 0, &Datum::Int(99), Some(&ty()), None);
        let field = projected(&native_bytes_type(), true).unwrap();
        let catalog = HostCatalog::for_sql_batch(vec![(
            SqlBatchHostOperation::EagerValuesNative,
            HostSignature {
                arg_types: vec![].into(),
                return_type: field.clone(),
            },
        )])
        .unwrap();
        let slot = catalog.slot(0).unwrap();
        let metadata = [NativeHostMetadata::TypedContext(NativeTypedContext {
            descriptor: Some(Box::new(
                crate::typed_context_value::TypedContextValue::capture(&expression)
                    .unwrap()
                    .into(),
            )),
            target: NativeEvalType::Int,
            cache_slot: cached.then_some(0),
            argument: None,
            output_node: 0,
            output_role: NativeValueRole::CharInput,
        })];
        let script = vec![Datum::Int(7), Datum::Int(9), Datum::Int(11)];
        let ctx = proof::Context::new(0, script.clone());
        let old = proof::Context::new(0, script);
        let store = RefCell::new(NativeValueStore::new(65536).unwrap());
        let mut provider = NativeBatchHosts::new(catalog, &metadata, &ctx, 65536)
            .with_native_values(Some(&store))
            .with_typed_cache_slots(usize::from(cached))
            .unwrap();
        let input = leaf::input(false);
        let Expression::Constant(c) = &expression else {
            unreachable!()
        };
        let mut saved: Option<Datum> = None;
        for (occurrence, input_row) in [2, 0, 2].into_iter().enumerate() {
            store.borrow_mut().begin_window().unwrap();
            let expected = if let Some(value) = &saved {
                value.clone()
            } else {
                let value = c
                    .eval_typed_as_on_row(&old, input.get_row(0), NativeEvalType::Int)
                    .unwrap();
                if cached {
                    saved = Some(value.clone());
                }
                value
            };
            let value = typed_invoke(
                &mut provider,
                &ctx,
                &expression,
                &input,
                &store,
                slot,
                &[],
                &field,
                InputRow {
                    occurrence: occurrence + 5,
                    input_row,
                },
            )
            .unwrap();
            assert_eq!(value, expected);
            assert_eq!(ctx.snapshot(), old.snapshot());
        }
        assert_eq!(ctx.inner.calls.get(), if cached { 2 } else { 6 });
    }
}

#[test]
fn typed_context_runtime_authenticates_consumes_callbacks_and_recovers() {
    struct C<'a> {
        store: &'a RefCell<NativeValueStore>,
        action: Cell<u8>,
        events: RefCell<Vec<&'static str>>,
    }
    impl crate::Columns for C<'_> {
        fn get(&self, _: &[String]) -> Option<Datum> {
            panic!("no invented input")
        }
        fn time_zone(&self) -> tidb_datatype::SessionTimeZone {
            let mut store = self.store.borrow_mut();
            self.events.borrow_mut().push("zone");
            match self.action.replace(0) {
                1 => panic!("typed conversion callback"),
                2 => {
                    store.begin_window().unwrap();
                }
                _ => (),
            }
            tidb_datatype::SessionTimeZone::utc()
        }
        fn type_flags(&self) -> tidb_datatype::ConversionFlags {
            let _exclusive = self.store.borrow_mut();
            self.events.borrow_mut().push("flags");
            tidb_datatype::DEFAULT_STATEMENT_FLAGS
        }
    }
    let expression = leaf::expression("deferred_value", 0, &Datum::Int(7), Some(&ty()), None);
    let field = projected(&native_bytes_type(), true).unwrap();
    let args = [field.clone()];
    let catalog = HostCatalog::for_sql_batch(vec![(
        SqlBatchHostOperation::EagerValuesNative,
        HostSignature {
            arg_types: args.to_vec().into(),
            return_type: field.clone(),
        },
    )])
    .unwrap();
    let slot = catalog.slot(0).unwrap();
    let metadata = [NativeHostMetadata::TypedContext(NativeTypedContext {
        descriptor: Some(Box::new(
            crate::typed_context_value::TypedContextValue::capture(&expression)
                .unwrap()
                .into(),
        )),
        target: NativeEvalType::Int,
        cache_slot: None,
        argument: Some(1),
        output_node: 0,
        output_role: NativeValueRole::CharInput,
    })];
    let store = RefCell::new(NativeValueStore::new(1 << 20).unwrap());
    store.borrow_mut().begin_window().unwrap();
    let ctx = C {
        store: &store,
        action: Cell::new(0),
        events: RefCell::new(vec![]),
    };
    let mut provider =
        NativeBatchHosts::new(catalog, &metadata, &ctx, 65536).with_native_values(Some(&store));
    let row = InputRow {
        occurrence: 7,
        input_row: 2,
    };
    let mut eval = EvalContext::default();
    let start = |provider: &mut NativeBatchHosts<'_>, eval: &mut EvalContext| {
        let HostStart::Pending { task, request } = provider
            .start(
                eval,
                HostInvocation {
                    slot,
                    row,
                    arg_types: &args,
                    return_type: &field,
                },
            )
            .unwrap()
        else {
            panic!()
        };
        assert_eq!(request.index, 0);
        task
    };
    let insert = |node, row, role, value: &Datum| {
        store
            .borrow_mut()
            .insert_clone(value, role, NativeValueOrigin { node, row })
            .unwrap()
    };
    let task = start(&mut provider, &mut eval);
    let valid = insert(1, row, NativeValueRole::CharInput, &Datum::Int(7));
    let mut foreign = NativeValueStore::new(4096).unwrap();
    foreign.begin_window().unwrap();
    let foreign = foreign
        .insert_clone(
            &Datum::Int(7),
            NativeValueRole::CharInput,
            NativeValueOrigin { node: 1, row },
        )
        .unwrap();
    for token in [
        foreign,
        insert(1, row, NativeValueRole::Output, &Datum::Int(7)),
        insert(2, row, NativeValueRole::CharInput, &Datum::Int(7)),
        insert(
            1,
            InputRow {
                occurrence: 8,
                ..row
            },
            NativeValueRole::CharInput,
            &Datum::Int(7),
        ),
        insert(
            1,
            InputRow {
                input_row: 0,
                ..row
            },
            NativeValueRole::CharInput,
            &Datum::Int(7),
        ),
    ] {
        let values = VectorValue::from_scalar(&ScalarValue::Bytes(Some(token)), 1);
        assert!(provider
            .resume(
                &mut eval,
                &task,
                HostArgReply {
                    index: 0,
                    field_type: &field,
                    values: &values
                }
            )
            .is_err());
        assert!(ctx.events.borrow().is_empty());
    }
    let wire_null = VectorValue::from_scalar(&ScalarValue::Bytes(None), 1);
    assert!(provider
        .resume(
            &mut eval,
            &task,
            HostArgReply {
                index: 0,
                field_type: &field,
                values: &wire_null
            }
        )
        .is_err());
    let values = VectorValue::from_scalar(&ScalarValue::Bytes(Some(valid)), 1);
    let HostStep::Ready(_) = provider
        .resume(
            &mut eval,
            &task,
            HostArgReply {
                index: 0,
                field_type: &field,
                values: &values,
            },
        )
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(*ctx.events.borrow(), ["zone", "flags"]);
    assert!(provider
        .resume(
            &mut eval,
            &task,
            HostArgReply {
                index: 0,
                field_type: &field,
                values: &values
            }
        )
        .is_err());
    for action in [1, 2] {
        ctx.events.borrow_mut().clear();
        ctx.action.set(action);
        let task = start(&mut provider, &mut eval);
        let token = insert(1, row, NativeValueRole::CharInput, &Datum::Int(7));
        let values = VectorValue::from_scalar(&ScalarValue::Bytes(Some(token)), 1);
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            provider.resume(
                &mut eval,
                &task,
                HostArgReply {
                    index: 0,
                    field_type: &field,
                    values: &values,
                },
            )
        }));
        if action == 1 {
            assert!(outcome.is_err());
            assert_eq!(*ctx.events.borrow(), ["zone"]);
        } else {
            assert!(outcome.unwrap().is_err());
            assert_eq!(*ctx.events.borrow(), ["zone", "flags"]);
        }
        let count = ctx.events.borrow().len();
        assert!(provider
            .resume(
                &mut eval,
                &task,
                HostArgReply {
                    index: 0,
                    field_type: &field,
                    values: &values
                }
            )
            .is_err());
        assert_eq!(ctx.events.borrow().len(), count);
        provider.cancel(&task);
        let next = start(&mut provider, &mut eval);
        let token = insert(1, row, NativeValueRole::CharInput, &Datum::Null);
        let values = VectorValue::from_scalar(&ScalarValue::Bytes(Some(token)), 1);
        let HostStep::Ready(values) = provider
            .resume(
                &mut eval,
                &next,
                HostArgReply {
                    index: 0,
                    field_type: &field,
                    values: &values,
                },
            )
            .unwrap()
        else {
            panic!()
        };
        let ScalarValueRef::Bytes(Some(token)) = values.get_scalar_ref(0) else {
            panic!()
        };
        assert_eq!(
            store
                .borrow()
                .get(
                    token,
                    NativeValueRole::CharInput,
                    NativeValueOrigin { node: 0, row }
                )
                .unwrap(),
            &Datum::Null
        );
        assert_eq!(ctx.events.borrow().len(), count);
    }
    assert!(provider.take_native_error().is_none());
    assert!(tidb_query_expr::local::value_kernel_work_remaining().is_none());
}

#[test]
fn typed_context_runtime_forward_ignores_wrapper_type_and_parameter_and_keeps_real_value() {
    let field = projected(&native_bytes_type(), true).unwrap();
    let args = [field.clone()];
    let catalog = HostCatalog::for_sql_batch(vec![(
        SqlBatchHostOperation::EagerValuesNative,
        HostSignature {
            arg_types: args.to_vec().into(),
            return_type: field.clone(),
        },
    )])
    .unwrap();
    let slot = catalog.slot(0).unwrap();
    let metadata = [NativeHostMetadata::TypedContext(NativeTypedContext {
        descriptor: None,
        target: NativeEvalType::Int,
        cache_slot: None,
        argument: Some(1),
        output_node: 0,
        output_role: NativeValueRole::CharInput,
    })];
    let ctx = leaf::Context::new(2, Datum::Int(99));
    let store = RefCell::new(NativeValueStore::new(65536).unwrap());
    store.borrow_mut().begin_window().unwrap();
    let row = InputRow {
        occurrence: 5,
        input_row: 2,
    };
    let mut provider =
        NativeBatchHosts::new(catalog, &metadata, &ctx, 65536).with_native_values(Some(&store));
    let mut eval = EvalContext::default();
    let HostStart::Pending { task, .. } = provider
        .start(
            &mut eval,
            HostInvocation {
                slot,
                row,
                arg_types: &args,
                return_type: &field,
            },
        )
        .unwrap()
    else {
        panic!()
    };
    let token = store
        .borrow_mut()
        .insert_clone(
            &Datum::Int(7),
            NativeValueRole::CharInput,
            NativeValueOrigin { node: 1, row },
        )
        .unwrap();
    let values = VectorValue::from_scalar(&ScalarValue::Bytes(Some(token)), 1);
    let HostStep::Ready(values) = provider
        .resume(
            &mut eval,
            &task,
            HostArgReply {
                index: 0,
                field_type: &field,
                values: &values,
            },
        )
        .unwrap()
    else {
        panic!()
    };
    let ScalarValueRef::Bytes(Some(token)) = values.get_scalar_ref(0) else {
        panic!()
    };
    assert_eq!(
        store
            .borrow()
            .get(
                token,
                NativeValueRole::CharInput,
                NativeValueOrigin { node: 0, row }
            )
            .unwrap(),
        &Datum::Int(7)
    );
    assert!(ctx.events.borrow().is_empty());
}

fn prepared_batch_observation(
    f: impl FnOnce() -> std::result::Result<Vec<Datum>, EvalError>,
    ctx: &proof::Context,
) -> String {
    let seen = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)) {
        Ok(Ok(values)) => format!(
            "VALUES:{:?}",
            values.iter().map(leaf::frame).collect::<Vec<_>>()
        ),
        Ok(Err(error)) => format!("ERROR:{error:?}"),
        Err(p) => format!(
            "PANIC:{:?}",
            p.downcast_ref::<String>()
                .map(String::as_str)
                .or_else(|| p.downcast_ref::<&str>().copied())
        ),
    };
    format!("{seen}|{}", ctx.snapshot())
}
fn prepared_batch_values(
    descriptor: super::super::hosts::NativeTypedDescriptor,
    target: NativeEvalType,
    cache: bool,
    ctx: &dyn Columns,
    input: &Chunk,
) -> std::result::Result<Vec<Datum>, EvalError> {
    let field = projected(&native_bytes_type(), true).unwrap();
    let catalog = HostCatalog::for_sql_batch(vec![(
        SqlBatchHostOperation::EagerValuesNative,
        HostSignature {
            arg_types: Box::new([]),
            return_type: field.clone(),
        },
    )])
    .unwrap();
    let slot = catalog.slot(0).unwrap();
    let metadata = [NativeHostMetadata::TypedContext(NativeTypedContext {
        descriptor: Some(Box::new(descriptor)),
        target,
        cache_slot: cache.then_some(0),
        argument: None,
        output_node: 0,
        output_role: NativeValueRole::CharInput,
    })];
    let store = RefCell::new(NativeValueStore::new(1 << 24).unwrap());
    let mut provider = NativeBatchHosts::new(catalog, &metadata, ctx, 1 << 20)
        .with_native_values(Some(&store))
        .with_input(input)
        .with_typed_cache_slots(usize::from(cache))
        .unwrap();
    let mut eval = EvalContext::default();
    let mut result = vec![];
    for occurrence in 0..input.num_rows() {
        // Cache values must survive a new native capability generation; each
        // result still carries its own occurrence and physical row identities.
        store.borrow_mut().begin_window().unwrap();
        let row = InputRow {
            occurrence,
            input_row: input.get_row(occurrence).idx(),
        };
        let output = match provider.start(
            &mut eval,
            HostInvocation {
                slot,
                row,
                arg_types: &[],
                return_type: &field,
            },
        ) {
            Ok(HostStart::Ready(value)) => value,
            Ok(HostStart::Pending { .. }) => panic!("prepared leaf has no child"),
            Err(error) => {
                return Err(provider
                    .take_native_error()
                    .unwrap_or_else(|| panic!("unexpected prepared leaf contract: {error:?}")))
            }
        };
        let ScalarValueRef::Bytes(Some(token)) = output.get_scalar_ref(0) else {
            panic!("SQL NULL must retain a genuine capability")
        };
        let value = store
            .borrow()
            .get(
                token,
                NativeValueRole::CharInput,
                NativeValueOrigin { node: 0, row },
            )
            .unwrap()
            .clone();
        // Integer batch transport owns bits, independent of the forwarding
        // wrapper's unsigned declaration. Compare that original bit domain.
        result.push(if target == NativeEvalType::Int {
            crate::scalar_function::native_batch_integer_value(
                crate::scalar_function::native_batch_integer_bits(value)?,
                false,
            )
        } else {
            value
        });
    }
    Ok(result)
}
#[test]
fn typed_prepared_column_matches_public_row_guards_and_effects() {
    if isolate_common_corpus(
        "typed_context_tests::typed_prepared_column_matches_public_row_guards_and_effects",
    ) {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    use super::super::hosts::NativeTypedDescriptor;
    let mut fields = crate::tikv::eager_scalar_value_tests::types()
        .into_iter()
        .map(Some)
        .collect::<Vec<_>>();
    for code in [FieldTypeCode::Bit, FieldTypeCode::Enum, FieldTypeCode::Set] {
        for flags in [0, tidb_datatype::FieldTypeFlags::ENUM_SET_AS_INT] {
            fields.push(Some(FieldType::new(code).with_flags(flags)));
        }
    }
    fields.push(None);
    let mut count = 0;
    for (fi, field) in fields.iter().enumerate() {
        for target in proof::targets() {
            for index in [-1, 0, 1, 3, i64::MAX] {
                for selection in [None, Some(vec![2, 0, 2])] {
                    for profile in 0..4 {
                        let mut input = leaf::input(true);
                        input.set_sel(selection.clone());
                        let mut column = crate::column::Column::new(1, ty());
                        column.ret_type = field.clone();
                        column.index = index;
                        let expr = Expression::Column(column);
                        let (ctx, old) = (
                            proof::Context::new(profile, vec![Datum::Null]),
                            proof::Context::new(profile, vec![Datum::Null]),
                        );
                        let want = prepared_batch_observation(
                            || {
                                (0..input.num_rows())
                                    .map(|row| {
                                        let Expression::Column(column) = &expr else {
                                            unreachable!()
                                        };
                                        let value = if target == NativeEvalType::Int
                                            && !field.as_ref().is_some_and(FieldType::is_hybrid)
                                        {
                                            let index = usize::try_from(index).map_err(|_| {
                                                EvalError::Unsupported(
                                                    "invalid integer column index",
                                                )
                                            })?;
                                            if index >= input.num_cols() {
                                                return Err(EvalError::Unsupported(
                                                    "invalid integer column index",
                                                ));
                                            }
                                            let physical = input.get_row(row).idx();
                                            if input.column(index).is_null(physical) {
                                                Datum::Null
                                            } else {
                                                Datum::Int(input.column(index).get_int64(physical))
                                            }
                                        } else {
                                            crate::scalar_function::normalize_numeric_row_value(
                                                expr.eval(&old, input.get_row(row))?,
                                                field.as_ref(),
                                                target,
                                                &old,
                                            )?
                                        };
                                        Ok(value)
                                    })
                                    .collect()
                            },
                            &old,
                        );
                        let got = prepared_batch_observation(
                            || {
                                prepared_batch_values(
                                    NativeTypedDescriptor::Column {
                                        index,
                                        field: field.clone(),
                                    },
                                    target,
                                    false,
                                    &ctx,
                                    &input,
                                )
                            },
                            &ctx,
                        );
                        if got != want {
                            eprintln!("column {fi} {target:?} index={index} sel={selection:?} p={profile}: public={want} prepared={got}");
                        }
                        assert_eq!(
                            got, want,
                            "column {fi} {target:?} index={index} sel={selection:?} p={profile}"
                        );
                        assert_eq!(input.sel(), selection.as_deref());
                        count += 1;
                    }
                }
            }
        }
    }
    assert_eq!(count, 6480);
    eprintln!("PREPARED COLUMN INDEPENDENT {count}");
}
#[test]
fn typed_prepared_literal_matches_direct_conversion_and_broadcast() {
    if isolate_common_corpus(
        "typed_context_tests::typed_prepared_literal_matches_direct_conversion_and_broadcast",
    ) {
        return;
    }
    std::panic::set_hook(Box::new(|_| {}));
    use super::super::hosts::NativeTypedDescriptor;
    let mut fields = crate::tikv::eager_scalar_value_tests::types()
        .into_iter()
        .map(Some)
        .collect::<Vec<_>>();
    for code in [FieldTypeCode::Bit, FieldTypeCode::Enum, FieldTypeCode::Set] {
        for flags in [0, tidb_datatype::FieldTypeFlags::ENUM_SET_AS_INT] {
            fields.push(Some(FieldType::new(code).with_flags(flags)));
        }
    }
    fields.push(None);
    let mut values = crate::tikv::session_read_tests::values();
    values.extend([
        Datum::Enum(
            tidb_datatype::MysqlEnum::new("large", 9_007_199_254_740_993),
            tidb_datatype::Collation::DEFAULT,
        ),
        Datum::Enum(
            tidb_datatype::MysqlEnum::new("largest", u64::MAX),
            tidb_datatype::Collation::DEFAULT,
        ),
        Datum::Set(
            tidb_datatype::MysqlSet::new("largest", u64::MAX),
            tidb_datatype::Collation::DEFAULT,
        ),
    ]);
    let mut count = 0;
    for (fi, field) in fields.iter().enumerate() {
        for target in proof::targets() {
            for (vi, value) in values.iter().enumerate() {
                for profile in 0..4 {
                    let mut input = leaf::input(true);
                    input.set_sel(Some(vec![2, 0, 2]));
                    let mut c = crate::constant::Constant::new(value.clone(), ty());
                    c.ret_type = field.clone();
                    let expr = Expression::Constant(c);
                    let (ctx, old) = (
                        proof::Context::new(profile, vec![Datum::Null]),
                        proof::Context::new(profile, vec![Datum::Null]),
                    );
                    let want = prepared_batch_observation(
                        || {
                            let Expression::Constant(constant) = &expr else {
                                unreachable!()
                            };
                            let value =
                                constant.eval_typed_as_on_row(&old, input.get_row(0), target)?;
                            let value = if target == NativeEvalType::Int {
                                crate::scalar_function::native_batch_integer_value(
                                    crate::scalar_function::native_batch_integer_bits(value)?,
                                    false,
                                )
                            } else {
                                value
                            };
                            Ok(vec![value; input.num_rows()])
                        },
                        &old,
                    );
                    let got = prepared_batch_observation(
                        || {
                            prepared_batch_values(
                                NativeTypedDescriptor::Literal {
                                    value: value.clone(),
                                    field: field.clone(),
                                },
                                target,
                                true,
                                &ctx,
                                &input,
                            )
                        },
                        &ctx,
                    );
                    assert_eq!(got, want, "literal {fi} {target:?} value={vi} p={profile}");
                    count += 1;
                }
            }
        }
    }
    assert_eq!(count, 14256);
    eprintln!("PREPARED LITERAL INDEPENDENT {count}");
}
#[test]
fn numeric_operand_independent_preparation_matches_original_before_owner_retirement() {
    if isolate_common_corpus("typed_context_tests::numeric_operand_independent_preparation_matches_original_before_owner_retirement") {return;}
    std::panic::set_hook(Box::new(|_| {}));
    use crate::scalar_function::{NativeArithmeticInputMode as Mode, NumericOperandStep};
    let mut fields = crate::tikv::eager_scalar_value_tests::types();
    for code in [FieldTypeCode::Bit, FieldTypeCode::Enum, FieldTypeCode::Set] {
        for flags in [0, tidb_datatype::FieldTypeFlags::ENUM_SET_AS_INT] {
            fields.push(FieldType::new(code).with_flags(flags));
        }
    }
    let mut values = crate::tikv::session_read_tests::values();
    values.extend([
        Datum::Enum(
            tidb_datatype::MysqlEnum::new("large", 9_007_199_254_740_993),
            tidb_datatype::Collation::DEFAULT,
        ),
        Datum::Enum(
            tidb_datatype::MysqlEnum::new("largest", u64::MAX),
            tidb_datatype::Collation::DEFAULT,
        ),
        Datum::Set(
            tidb_datatype::MysqlSet::new("largest", u64::MAX),
            tidb_datatype::Collation::DEFAULT,
        ),
    ]);
    let (mut count, mut hash) = (0usize, 0xcbf29ce484222325u64);
    for (fi, field) in fields.iter().enumerate() {
        for (vi, value) in values.iter().enumerate() {
            for literal in [false, true] {
                for binary in [false, true] {
                    for mode in [
                        Mode::Raw,
                        Mode::TypedInteger,
                        Mode::Numeric(NativeEvalType::Int),
                        Mode::Numeric(NativeEvalType::Real),
                        Mode::Numeric(NativeEvalType::Decimal),
                    ] {
                        for profile in 0..4 {
                            let (ctx, old) = (
                                proof::Context::new(profile, vec![Datum::Null]),
                                proof::Context::new(profile, vec![Datum::Null]),
                            );
                            let want = proof::outcome(
                                || {
                                    crate::scalar_function::native_numeric_operand(
                                        field,
                                        literal,
                                        binary,
                                        value.clone(),
                                        mode,
                                        &old,
                                        Some("original subject"),
                                        None,
                                    )
                                },
                                &old,
                            );
                            let got = proof::outcome(
                                || match crate::scalar_function::prepare_native_numeric_operand(
                                    field,
                                    literal,
                                    false,
                                    binary,
                                    value.clone(),
                                    mode,
                                    &ctx,
                                )? {
                                    NumericOperandStep::Ready(value) => Ok(value),
                                    NumericOperandStep::Subject(pending) => {
                                        pending.finish(field, Some("original subject"), &ctx)
                                    }
                                },
                                &ctx,
                            );
                            assert_eq!(got,want,"field={fi} value={vi} literal={literal} binary={binary} mode={mode:?} p={profile}");
                            for b in want.bytes() {
                                hash = (hash ^ u64::from(b)).wrapping_mul(0x100000001b3);
                            }
                            count += 1;
                        }
                    }
                }
            }
        }
    }
    eprintln!("NUMERIC OPERAND INDEPENDENT {count}/{hash:016X}");
    assert_eq!((count, hash), (29920, 0x6D9435492158AE78));
}
