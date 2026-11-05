// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! A prepared deferred wrapper forwards its complete original child program.
//! It performs no SQL conversion, type read, callback or extra row scope.
use super::*;
pub(super) struct TypedWindowTask {
    rows: Vec<usize>,
}
impl TypedWindowTask {
    #[cfg(test)]
    pub(super) fn retained_bytes(&self) -> LocalResult<usize> {
        std::mem::size_of::<Self>()
            .checked_add(array_bytes::<usize>(self.rows.capacity())?)
            .ok_or_else(|| resource("typed forward row map extent overflow"))
    }
}
impl NativeBatchHosts<'_> {
    pub(super) fn start_typed_window(
        &mut self,
        invocation: HostWindowInvocation<'_>,
    ) -> LocalResult<HostStart> {
        let signature = self.catalog.signature(invocation.slot)?;
        let Some(NativeHostMetadata::TypedContext(meta)) =
            self.metadata.get(invocation.slot.index())
        else {
            return Err(binding("typed forward window metadata absent"));
        };
        if self.catalog.sql_batch_operation(invocation.slot)?
            != SqlBatchHostOperation::EagerValuesWindowNative
            || signature.arg_types.as_ref() != invocation.arg_types
            || &signature.return_type != invocation.return_type
            || invocation.arg_types.len() != 1
            || invocation.physical_rows.is_empty()
            || invocation
                .occurrence_start
                .checked_add(invocation.physical_rows.len() - 1)
                .is_none()
            || self.native_values.is_none()
            || meta.argument.is_none()
            || !meta.window_forward()
            || meta.cache_slot.is_some()
            || !matches!(
                meta.output_role,
                NativeValueRole::CharInput | NativeValueRole::Output
            )
        {
            return Err(binding("typed forward window declaration differs"));
        }

        let mut rows = Vec::new();
        rows.try_reserve_exact(invocation.physical_rows.len())
            .map_err(|e| LocalError::ResourceLimit(e.to_string()))?;
        rows.extend_from_slice(invocation.physical_rows);
        let row = InputRow {
            occurrence: invocation.occurrence_start,
            input_row: rows[0],
        };
        self.install_task(
            invocation.slot,
            row,
            1,
            SqlDemand::TypedWindow(Box::new(TypedWindowTask { rows })),
        )
    }
    pub(super) fn resume_typed_window(
        &mut self,
        id: &HostTaskId,
        reply: HostWindowArgReply<'_>,
    ) -> LocalResult<HostStep> {
        let task = self.task(id)?;
        let SqlDemand::TypedWindow(state) = &task.demand else {
            return Err(binding("typed forward window phase consumed"));
        };
        let (slot, start) = (task.slot, task.row.occurrence);
        if reply.index != 0
            || reply.occurrence_start != start
            || reply.physical_rows != state.rows
            || reply.values.len() != state.rows.len()
            || reply.values.eval_type() != TikvEvalType::Bytes
            || self.catalog.signature(slot)?.arg_types.first() != Some(reply.field_type)
        {
            return Err(binding("typed forward window reply differs"));
        }
        let Some(NativeHostMetadata::TypedContext(meta)) = self.metadata.get(slot.index()) else {
            return Err(binding("typed forward window metadata absent"));
        };
        let node = meta
            .argument
            .ok_or_else(|| binding("typed forward child absent"))?;
        let store = self
            .native_values
            .ok_or_else(|| binding("typed forward store absent"))?;
        // Authenticate the complete original scope before consuming this task.
        for position in 0..reply.values.len() {
            let ScalarValueRef::Bytes(Some(token)) = reply.values.get_scalar_ref(position) else {
                return Err(binding("typed forward cannot accept wire NULL"));
            };
            store.borrow().get(
                token,
                NativeValueRole::CharInput,
                NativeValueOrigin {
                    node,
                    row: InputRow {
                        occurrence: start + position,
                        input_row: state.rows[position],
                    },
                },
            )?;
        }
        let count = state.rows.len();

        let demand = std::mem::replace(
            &mut self.slots[id.slot]
                .task
                .as_mut()
                .expect("authenticated typed window")
                .demand,
            SqlDemand::Consumed,
        );
        let SqlDemand::TypedWindow(state) = demand else {
            unreachable!()
        };
        let mut output =
            ChunkedVecBytes::try_with_capacities(count, count * native_values::HANDLE_BYTES)
                .map_err(|e| LocalError::ResourceLimit(e.to_string()))?;
        for (position, &input_row) in state.rows.iter().enumerate() {
            let ScalarValueRef::Bytes(Some(token)) = reply.values.get_scalar_ref(position) else {
                unreachable!()
            };
            let row = InputRow {
                occurrence: start + position,
                input_row,
            };
            let origin = NativeValueOrigin { node, row };
            store
                .borrow()
                .get(token, NativeValueRole::CharInput, origin)?;

            let value =
                store
                    .borrow()
                    .clone_for_kernel(token, NativeValueRole::CharInput, origin)?;
            let value = if let Some(NativeTypedDescriptor::RootForward { unsigned }) =
                meta.descriptor.as_deref()
            {
                crate::scalar_function::native_batch_integer_value(
                    crate::scalar_function::native_batch_integer_bits(value).map_err(|_| {
                        binding("prepared integer forward received non-integer bits")
                    })?,
                    *unsigned,
                )
            } else {
                value
            };
            let token = store.borrow_mut().insert_clone(
                &value,
                meta.output_role,
                NativeValueOrigin {
                    node: meta.output_node,
                    row,
                },
            )?;
            output.push_ref(Some(&token));
        }
        self.slots[id.slot].task = None;
        Ok(HostStep::Ready(VectorValue::Bytes(output)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_query_expr::local::HostSignature;
    struct NoCallbacks;
    impl Columns for NoCallbacks {
        fn get(&self, _: &[String]) -> Option<Datum> {
            panic!("forward must not evaluate SQL")
        }
        fn param_value(&self, _: usize) -> std::result::Result<Datum, EvalError> {
            panic!("forward must not read parameters")
        }
        fn enable_vectorized_expression(&self) -> bool {
            panic!("forward must not read mode")
        }
        fn time_zone(&self) -> tidb_datatype::SessionTimeZone {
            panic!("forward must not convert")
        }
        fn type_flags(&self) -> tidb_datatype::ConversionFlags {
            panic!("forward must not convert")
        }
        fn truncate_level(&self) -> crate::ErrorLevel {
            panic!("forward must not convert")
        }
        fn div_precision_increment(&self) -> u32 {
            panic!("forward must not compute")
        }
    }
    fn metadata() -> [NativeHostMetadata; 1] {
        [NativeHostMetadata::TypedContext(NativeTypedContext {
            descriptor: None,
            target: NativeEvalType::String,
            cache_slot: None,
            argument: Some(1),
            output_node: 0,
            output_role: NativeValueRole::Output,
        })]
    }
    fn catalog(field: &tipb::FieldType) -> HostCatalog {
        HostCatalog::for_sql_batch(vec![(
            SqlBatchHostOperation::EagerValuesWindowNative,
            HostSignature {
                arg_types: vec![field.clone()].into(),
                return_type: field.clone(),
            },
        )])
        .unwrap()
    }
    fn input(
        store: &RefCell<NativeValueStore>,
        rows: &[usize],
        start: usize,
        large: bool,
    ) -> VectorValue {
        let mut values = ChunkedVecBytes::try_with_capacities(
            rows.len(),
            rows.len() * native_values::HANDLE_BYTES,
        )
        .unwrap();
        for (p, &input_row) in rows.iter().enumerate() {
            let value = if large {
                Datum::new_string("x".repeat(4096))
            } else {
                datum(p)
            };
            let token = store
                .borrow_mut()
                .insert_clone(
                    &value,
                    NativeValueRole::CharInput,
                    NativeValueOrigin {
                        node: 1,
                        row: InputRow {
                            occurrence: start + p,
                            input_row,
                        },
                    },
                )
                .unwrap();
            values.push_ref(Some(&token));
        }
        VectorValue::Bytes(values)
    }
    fn datum(p: usize) -> Datum {
        match p % 7 {
            0 => Datum::Null,
            1 => Datum::Int(-7),
            2 => Datum::UInt(u64::MAX),
            3 => Datum::Real(f64::from_bits(0x7ff8000000000042)),
            4 => Datum::Decimal(tidb_datatype::Decimal::from_literal("1.2500")),
            5 => Datum::new_string("é\0x"),
            _ => Datum::VectorFloat32(VectorFloat32::init(2)),
        }
    }
    fn reply<'a>(
        field: &'a tipb::FieldType,
        rows: &'a [usize],
        start: usize,
        values: &'a VectorValue,
    ) -> HostWindowArgReply<'a> {
        HostWindowArgReply {
            index: 0,
            field_type: field,
            occurrence_start: start,
            physical_rows: rows,
            values,
        }
    }
    #[test]
    fn typed_window_forward_authenticates_full_original_scopes_without_context_or_value_conversion()
    {
        let field = projected(&native_bytes_type(), true).unwrap();
        let meta = metadata();
        let store = RefCell::new(NativeValueStore::new().unwrap());
        let mut hosts = NativeBatchHosts::new(catalog(&field), &meta, &NoCallbacks)
            .with_native_values(Some(&store));
        let mut eval = EvalContext::default();
        let args = [field.clone()];
        let start = 11;
        for count in [1, 3, 2050] {
            for _ in 0..2 {
                store.borrow_mut().begin_window().unwrap();
                let rows = (0..count)
                    .map(|p| (17 - p % 17) as usize)
                    .collect::<Vec<_>>();
                let slot = hosts.catalog.slot(0).unwrap();
                assert!(hosts
                    .start(
                        &mut eval,
                        HostInvocation {
                            slot,
                            row: InputRow {
                                occurrence: start,
                                input_row: rows[0]
                            },
                            arg_types: &args,
                            return_type: &field
                        }
                    )
                    .is_err());
                let HostStart::Pending { task, request } = hosts
                    .start_window(
                        &mut eval,
                        HostWindowInvocation {
                            slot,
                            occurrence_start: start,
                            physical_rows: &rows,
                            arg_types: &args,
                            return_type: &field,
                        },
                    )
                    .unwrap()
                else {
                    panic!()
                };
                assert_eq!(request.index, 0);
                let values = input(&store, &rows, start, false);
                assert!(hosts
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
                assert!(hosts
                    .resume_window(&mut eval, &task, reply(&field, &rows, start + 1, &values))
                    .is_err());
                let wrong_rows = rows.iter().map(|p| p + 1).collect::<Vec<_>>();
                assert!(hosts
                    .resume_window(&mut eval, &task, reply(&field, &wrong_rows, start, &values))
                    .is_err());
                let badfield = projected(&FieldType::new(FieldTypeCode::LongLong), true).unwrap();
                assert!(hosts
                    .resume_window(&mut eval, &task, reply(&badfield, &rows, start, &values))
                    .is_err());
                let last = InputRow {
                    occurrence: start + count - 1,
                    input_row: rows[count - 1],
                };
                let mut foreign = NativeValueStore::new().unwrap();
                foreign.begin_window().unwrap();
                let foreign = foreign
                    .insert_clone(
                        &Datum::Int(7),
                        NativeValueRole::CharInput,
                        NativeValueOrigin { node: 1, row: last },
                    )
                    .unwrap();
                let wrong_node = store
                    .borrow_mut()
                    .insert_clone(
                        &Datum::Int(7),
                        NativeValueRole::CharInput,
                        NativeValueOrigin { node: 2, row: last },
                    )
                    .unwrap();
                let wrong_role = store
                    .borrow_mut()
                    .insert_clone(
                        &Datum::Int(7),
                        NativeValueRole::Output,
                        NativeValueOrigin { node: 1, row: last },
                    )
                    .unwrap();
                let wrong_occurrence = store
                    .borrow_mut()
                    .insert_clone(
                        &Datum::Int(7),
                        NativeValueRole::CharInput,
                        NativeValueOrigin {
                            node: 1,
                            row: InputRow {
                                occurrence: last.occurrence + 1,
                                ..last
                            },
                        },
                    )
                    .unwrap();
                let wrong_physical = store
                    .borrow_mut()
                    .insert_clone(
                        &Datum::Int(7),
                        NativeValueRole::CharInput,
                        NativeValueOrigin {
                            node: 1,
                            row: InputRow {
                                input_row: last.input_row + 1,
                                ..last
                            },
                        },
                    )
                    .unwrap();
                let text = store
                    .borrow_mut()
                    .insert_diagnostic_text(
                        "SQL text".into(),
                        NativeValueOrigin { node: 1, row: last },
                    )
                    .unwrap();
                let unavailable = store
                    .borrow_mut()
                    .insert_diagnostic_unavailable(NativeValueOrigin { node: 1, row: last })
                    .unwrap();
                for token in [
                    Some(foreign.to_vec()),
                    Some(wrong_node.to_vec()),
                    Some(wrong_role.to_vec()),
                    Some(wrong_occurrence.to_vec()),
                    Some(wrong_physical.to_vec()),
                    Some(text.to_vec()),
                    Some(unavailable.to_vec()),
                    Some(vec![0]),
                    None,
                ] {
                    let mut bad = ChunkedVecBytes::try_with_capacities(
                        count,
                        count * native_values::HANDLE_BYTES,
                    )
                    .unwrap();
                    for p in 0..count {
                        let ScalarValueRef::Bytes(value) = values.get_scalar_ref(p) else {
                            panic!()
                        };
                        bad.push_ref(if p + 1 == count {
                            token.as_deref()
                        } else {
                            value
                        });
                    }
                    assert!(hosts
                        .resume_window(
                            &mut eval,
                            &task,
                            reply(&field, &rows, start, &VectorValue::Bytes(bad))
                        )
                        .is_err());
                }
                assert!(hosts.take_native_error().is_none());
                let HostStep::Ready(output) = hosts
                    .resume_window(&mut eval, &task, reply(&field, &rows, start, &values))
                    .unwrap()
                else {
                    panic!()
                };
                assert_eq!(output.len(), count);
                for (p, &input_row) in rows.iter().enumerate() {
                    let ScalarValueRef::Bytes(Some(token)) = output.get_scalar_ref(p) else {
                        panic!()
                    };
                    let got = store
                        .borrow()
                        .get(
                            token,
                            NativeValueRole::Output,
                            NativeValueOrigin {
                                node: 0,
                                row: InputRow {
                                    occurrence: start + p,
                                    input_row,
                                },
                            },
                        )
                        .unwrap()
                        .clone();
                    assert_eq!(
                        crate::tikv::dynamic_leaf_tests::frame(&got),
                        crate::tikv::dynamic_leaf_tests::frame(&datum(p))
                    );
                }
                assert!(hosts
                    .resume_window(&mut eval, &task, reply(&field, &rows, start, &values))
                    .is_err());
                hosts.cancel(&task);
            }
        }
    }
    #[test]
    fn typed_window_forward_resource_refusal_is_consuming_and_same_provider_recovers() {
        let field = projected(&native_bytes_type(), true).unwrap();
        let meta = metadata();
        let store = RefCell::new(NativeValueStore::new().unwrap());
        let mut hosts = NativeBatchHosts::new(catalog(&field), &meta, &NoCallbacks)
            .with_native_values(Some(&store));
        let mut eval = EvalContext::default();
        let args = [field.clone()];
        let rows = [2, 0, 2];
        let start = 11;
        for large in [true, false] {
            store.borrow_mut().begin_window().unwrap();
            let values = input(&store, &rows, start, large);
            let HostStart::Pending { task, .. } = hosts
                .start_window(
                    &mut eval,
                    HostWindowInvocation {
                        slot: hosts.catalog.slot(0).unwrap(),
                        occurrence_start: start,
                        physical_rows: &rows,
                        arg_types: &args,
                        return_type: &field,
                    },
                )
                .unwrap()
            else {
                panic!()
            };
            let result =
                hosts.resume_window(&mut eval, &task, reply(&field, &rows, start, &values));
            let HostStep::Ready(output) = result.unwrap() else {
                panic!("expected forwarded values")
            };
            assert_eq!(output.len(), rows.len());
            for (p, &physical_row) in rows.iter().enumerate() {
                let ScalarValueRef::Bytes(Some(token)) = output.get_scalar_ref(p) else {
                    panic!("native output handle")
                };
                let expected = if large {
                    Datum::new_string("x".repeat(4096))
                } else {
                    datum(p)
                };
                assert_eq!(
                    *store
                        .borrow()
                        .get(
                            token,
                            NativeValueRole::Output,
                            NativeValueOrigin {
                                node: 0,
                                row: InputRow {
                                    occurrence: start + p,
                                    input_row: physical_row
                                }
                            }
                        )
                        .unwrap(),
                    expected
                );
            }
            assert!(hosts.take_native_error().is_none());
            assert!(hosts
                .resume_window(&mut eval, &task, reply(&field, &rows, start, &values))
                .is_err());
            hosts.cancel(&task);
            assert!(tidb_query_expr::local::value_kernel_work_remaining().is_none());
        }
    }
}
