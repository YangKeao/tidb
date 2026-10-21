// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Raw native operands: both binary arguments precede value conversion, even
//! when the first is NULL. No expression callback or ready-argument program.
use super::*;

// Invalid coefficient UTF-8 is rejected by the original value helper before
// any scale padding. Budget only its bounded storage here so that validation
// still occurs in that helper, after authentication and task consumption.
fn raw_conversion_work_bound(value: &Datum, multiplier: usize) -> LocalResult<usize> {
    if matches!(value, Datum::Decimal(decimal) if std::str::from_utf8(decimal.coefficient_bytes()).is_err())
    {
        native_payload_bytes(value)?
            .checked_mul(multiplier)
            .and_then(|bytes| bytes.checked_add(1024))
            .ok_or_else(|| resource("raw coefficient validation allowance overflow"))
    } else {
        window_values::conversion_work_bound(value, multiplier)
    }
}
impl NativeBatchHosts<'_> {
    pub(super) fn resume_raw_values(
        &mut self,
        id: &HostTaskId,
        token: &[u8],
    ) -> LocalResult<HostStep> {
        let task = self.task(id)?;
        let SqlDemand::RawValues { left_token } = task.demand else {
            return Err(binding("raw value resume has another task kind"));
        };
        let Some(NativeHostMetadata::RawValues {
            operation,
            name,
            result_type,
            left_node,
            right_node,
            output_node,
            output_role,
        }) = self.metadata.get(task.slot.index())
        else {
            return Err(binding("raw value descriptor missing"));
        };
        let row = task.row;
        let store = self
            .native_values
            .ok_or_else(|| binding("raw value store missing"))?;
        let left_origin = NativeValueOrigin {
            node: *left_node,
            row,
        };
        if left_token.is_none() && right_node.is_some() {
            store
                .borrow()
                .get(token, NativeValueRole::CharInput, left_origin)?;
            let left = token
                .try_into()
                .map_err(|_| binding("raw value token width differs"))?;
            let SqlDemand::RawValues { left_token } = &mut self.slots[id.slot]
                .task
                .as_mut()
                .expect("validated raw value task")
                .demand
            else {
                unreachable!()
            };
            *left_token = Some(left);
            return Ok(HostStep::NeedArg(HostArgRequest {
                index: 1,
                mode: ArgMode::Fresh,
            }));
        }
        let left_token = left_token.as_ref().map_or(token, |token| token.as_slice());
        let right_origin = right_node.map(|node| NativeValueOrigin { node, row });
        // Charge copies, numeric formatting and JSON text expansion before
        // cloning or entering the conversion/warning boundary. Decimal bounds
        // include visible scale padding, independently of coefficient length.
        let work = {
            let store = store.borrow();
            let bound = |token: &[u8], origin| -> LocalResult<usize> {
                let value = store.get(token, NativeValueRole::CharInput, origin)?;
                match operation {
                    NativeRawValueOperation::Bitwise { .. } => raw_conversion_work_bound(
                        value,
                        if matches!(value, Datum::Json(_)) {
                            128
                        } else {
                            16
                        },
                    ),
                    NativeRawValueOperation::UnarySign { sign, .. } => {
                        let identity = *sign == tidb_query_expr::impl_op::NativeUnarySign::Plus;
                        if matches!(value, Datum::Decimal(_))
                            || (!identity && matches!(value, Datum::String(_) | Datum::Bytes(_)))
                        {
                            raw_conversion_work_bound(value, 16)
                        } else {
                            native_payload_bytes(value)?
                                .checked_mul(if matches!(value, Datum::Json(_)) {
                                    128
                                } else {
                                    4
                                })
                                .and_then(|bytes| bytes.checked_add(1024))
                                .ok_or_else(|| resource("unary sign work allowance overflow"))
                        }
                    }
                    // XOR tests Decimal coefficient zero directly. It does not
                    // render scale padding or stringify JSON for a cast.
                    NativeRawValueOperation::LogicalXor => native_payload_bytes(value)?
                        .checked_mul(16)
                        .and_then(|bytes| bytes.checked_add(1024))
                        .ok_or_else(|| resource("XOR truth work allowance overflow")),
                }
            };
            let left = bound(left_token, left_origin)?;
            left.checked_add(match right_origin {
                Some(origin) => bound(token, origin)?,
                None => 0,
            })
            .ok_or_else(|| resource("raw value conversion allowance overflow"))?
        };
        let base = self
            .retained_bytes()?
            .checked_add(work)
            .ok_or_else(|| resource("raw value retained size overflow"))?;
        self.check_bytes(base)?;
        let (left, right) = {
            let store = store.borrow();
            (
                store.clone_for_kernel(left_token, NativeValueRole::CharInput, left_origin)?,
                match right_origin {
                    Some(origin) => {
                        Some(store.clone_for_kernel(token, NativeValueRole::CharInput, origin)?)
                    }
                    None => None,
                },
            )
        };
        // Authentication and copies precede consumption. A warning callback
        // can unwind, so it must never leave this same demand replayable.
        self.slots[id.slot]
            .task
            .as_mut()
            .expect("validated raw value task")
            .demand = SqlDemand::Consumed;
        let available = self
            .max_retained_bytes
            .checked_sub(base)
            .ok_or_else(|| resource("raw value work allowance exhausted"))?;
        let computed = match *operation {
            NativeRawValueOperation::Bitwise {
                operation,
                unsigned,
            } => crate::ops::eval_native_bitwise_values(
                operation,
                left,
                right,
                unsigned,
                self.context,
            ),
            NativeRawValueOperation::UnarySign {
                sign,
                unsigned,
                constant,
            } => crate::ops::eval_native_unary_sign_values(
                sign,
                left,
                unsigned,
                constant,
                self.context,
                available,
            ),
            NativeRawValueOperation::LogicalXor => crate::ops::eval_native_logical_xor_values(
                left,
                right.expect("XOR has both demanded raw values"),
                self.context,
            ),
        };
        // Context callbacks may invalidate native capabilities. No store borrow
        // crosses the callback; check both original inputs again before output.
        {
            let store = store.borrow();
            store.get(left_token, NativeValueRole::CharInput, left_origin)?;
            if let Some(origin) = right_origin {
                store.get(token, NativeValueRole::CharInput, origin)?;
            }
        }
        let result = match computed {
            Ok(value) => value,
            Err(error) => return self.native_failure(error),
        };
        let result = match crate::scalar_function::coerce_function_result(
            name,
            result_type.as_ref(),
            result,
        ) {
            Ok(value) => value,
            Err(error) => return self.native_failure(error),
        };
        self.finish_native_value(id, result, *output_node, *output_role, row, base)
            .map(HostStep::Ready)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;
    use tidb_datatype::Decimal;
    use tidb_query_expr::impl_op::NativeBitwiseOp as B;

    #[derive(Default)]
    struct Context {
        reads: Cell<usize>,
        warnings: Cell<usize>,
        reject: bool,
    }
    impl Columns for Context {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }
        fn div_precision_increment(&self) -> u32 {
            self.reads.set(self.reads.get() + 1);
            4
        }
        fn handle_truncate(&self, _: &str) -> std::result::Result<(), EvalError> {
            self.warnings.set(self.warnings.get() + 1);
            if self.reject {
                Err(EvalError::Unsupported("bitwise truncation rejected"))
            } else {
                Ok(())
            }
        }
    }
    #[test]
    fn native_raw_value_provider_keeps_resources_provenance_errors_and_reuse_terminal() {
        for declared in [true, false] {
            for operation in [B::And, B::Or, B::Xor, B::LeftShift, B::RightShift, B::Neg]
                .into_iter()
                .map(|operation| NativeRawValueOperation::Bitwise {
                    operation,
                    unsigned: [false; 2],
                })
                .chain(std::iter::once(NativeRawValueOperation::LogicalXor))
                .chain(
                    [
                        tidb_query_expr::impl_op::NativeUnarySign::Plus,
                        tidb_query_expr::impl_op::NativeUnarySign::Minus,
                    ]
                    .into_iter()
                    .map(|sign| NativeRawValueOperation::UnarySign {
                        sign,
                        unsigned: false,
                        constant: false,
                    }),
                )
            {
                let field: tipb::FieldType = tidb_query_datatype::FieldTypeTp::Blob.into();
                let args = vec![field.clone(); operation.arity()];
                let catalog = HostCatalog::for_sql_batch(vec![(
                    operation.sql_operation(),
                    HostSignature {
                        arg_types: args.clone().into_boxed_slice(),
                        return_type: field.clone(),
                    },
                )])
                .unwrap();
                let slot = catalog.slot(0).unwrap();
                let name = match operation.sql_operation() {
                    SqlBatchHostOperation::BitwiseNative(B::And) => "bitand",
                    SqlBatchHostOperation::BitwiseNative(B::Or) => "bitor",
                    SqlBatchHostOperation::BitwiseNative(B::Xor) => "bitxor",
                    SqlBatchHostOperation::BitwiseNative(B::LeftShift) => "leftshift",
                    SqlBatchHostOperation::BitwiseNative(B::RightShift) => "rightshift",
                    SqlBatchHostOperation::BitwiseNative(B::Neg) => "bitneg",
                    SqlBatchHostOperation::XorNative => "xor",
                    SqlBatchHostOperation::UnarySignNative(
                        tidb_query_expr::impl_op::NativeUnarySign::Plus,
                    ) => "unaryplus",
                    SqlBatchHostOperation::UnarySignNative(
                        tidb_query_expr::impl_op::NativeUnarySign::Minus,
                    ) => "unaryminus",
                    _ => unreachable!(),
                };
                let metadata = [NativeHostMetadata::RawValues {
                    operation,
                    name: name.into(),
                    result_type: declared.then(|| {
                        if matches!(
                            operation,
                            NativeRawValueOperation::UnarySign {
                                sign: tidb_query_expr::impl_op::NativeUnarySign::Plus,
                                ..
                            }
                        ) {
                            FieldType::new(FieldTypeCode::Varchar)
                        } else {
                            FieldType::new(FieldTypeCode::LongLong).with_unsigned(!matches!(
                                operation,
                                NativeRawValueOperation::LogicalXor
                            ))
                        }
                    }),
                    left_node: 3,
                    right_node: (operation.arity() == 2).then_some(8),
                    output_node: 2,
                    output_role: NativeValueRole::Output,
                }];
                let store = RefCell::new(NativeValueStore::new(1 << 24).unwrap());
                store.borrow_mut().begin_window().unwrap();
                let row = InputRow {
                    occurrence: 9,
                    input_row: 2,
                };
                let context = Context {
                    reject: true,
                    ..Context::default()
                };
                let mut provider = NativeBatchHosts::new(catalog, &metadata, &context, 16 * 1024)
                    .with_native_values(Some(&store));
                let mut ctx = EvalContext::default();
                // Each iteration begins from the same provider after explicit task
                // cancellation, so a rejected conversion cannot poison a later call.
                for case in 0..7 {
                    context.reads.set(0);
                    context.warnings.set(0);
                    let value = match case {
                        0 => Datum::Bytes(vec![b'1'; 16384]),
                        1 => Datum::Decimal(Decimal::from_raw_parts(
                            false,
                            b"1".to_vec(),
                            1_000_000,
                            0,
                        )),
                        5 => Datum::new_string("2tail"),
                        6 => Datum::Null,
                        _ => Datum::Int(3),
                    };
                    let origin = NativeValueOrigin {
                        node: 3,
                        row: if case == 4 {
                            InputRow {
                                occurrence: 10,
                                input_row: 2,
                            }
                        } else {
                            row
                        },
                    };
                    let token = store
                        .borrow_mut()
                        .insert_clone(
                            &value,
                            if case == 2 {
                                NativeValueRole::Output
                            } else {
                                NativeValueRole::CharInput
                            },
                            origin,
                        )
                        .unwrap();
                    if case == 3 {
                        store.borrow_mut().begin_window().unwrap();
                    }
                    let right = store
                        .borrow_mut()
                        .insert_clone(
                            &Datum::Int(1),
                            NativeValueRole::CharInput,
                            NativeValueOrigin { node: 8, row },
                        )
                        .unwrap();
                    let HostStart::Pending { task, request } = provider
                        .start(
                            &mut ctx,
                            HostInvocation {
                                slot,
                                row,
                                arg_types: &args,
                                return_type: &field,
                            },
                        )
                        .unwrap()
                    else {
                        panic!("expected raw operand demand")
                    };
                    assert_eq!(request.index, 0);
                    let left = VectorValue::from_scalar(&ScalarValue::Bytes(Some(token)), 1);
                    let mut result = provider.resume(
                        &mut ctx,
                        &task,
                        HostArgReply {
                            index: 0,
                            field_type: &field,
                            values: &left,
                        },
                    );
                    if operation.arity() == 2 && !matches!(case, 2..=4) {
                        assert!(matches!(
                            result,
                            Ok(HostStep::NeedArg(HostArgRequest {
                                index: 1,
                                mode: ArgMode::Fresh
                            }))
                        ));
                        assert_eq!((context.reads.get(), context.warnings.get()), (0, 0));
                        let right = VectorValue::from_scalar(&ScalarValue::Bytes(Some(right)), 1);
                        result = provider.resume(
                            &mut ctx,
                            &task,
                            HostArgReply {
                                index: 1,
                                field_type: &field,
                                values: &right,
                            },
                        );
                    }
                    match case {
                        5 if matches!(
                            operation,
                            NativeRawValueOperation::UnarySign {
                                sign: tidb_query_expr::impl_op::NativeUnarySign::Plus,
                                ..
                            }
                        ) =>
                        {
                            let HostStep::Ready(values) = result.unwrap() else {
                                panic!("unary plus must preserve actual string kind without conversion")
                            };
                            let ScalarValueRef::Bytes(Some(token)) = values.get_scalar_ref(0)
                            else {
                                panic!("expected native result")
                            };
                            assert_eq!(
                                *store
                                    .borrow()
                                    .get(
                                        token,
                                        NativeValueRole::Output,
                                        NativeValueOrigin { node: 2, row }
                                    )
                                    .unwrap(),
                                Datum::new_string("2tail")
                            );
                            assert_eq!((context.reads.get(), context.warnings.get()), (0, 0));
                            assert!(provider.take_native_error().is_none());
                        }

                        1 | 5 if matches!(operation, NativeRawValueOperation::LogicalXor) => {
                            let HostStep::Ready(values) = result.unwrap() else {
                                panic!(
                                "XOR must ignore warning probe errors and avoid Decimal rendering"
                            )
                            };
                            let ScalarValueRef::Bytes(Some(token)) = values.get_scalar_ref(0)
                            else {
                                panic!("expected native result")
                            };
                            assert_eq!(
                                *store
                                    .borrow()
                                    .get(
                                        token,
                                        NativeValueRole::Output,
                                        NativeValueOrigin { node: 2, row }
                                    )
                                    .unwrap(),
                                Datum::Int(0)
                            );
                            assert_eq!(
                                (context.reads.get(), context.warnings.get()),
                                (1, usize::from(case == 5))
                            );
                            assert!(provider.take_native_error().is_none());
                        }
                        0 | 1 => {
                            assert!(matches!(result, Err(LocalError::ResourceLimit(_))));
                            assert_eq!((context.reads.get(), context.warnings.get()), (0, 0));
                            assert!(provider.take_native_error().is_none());
                        }
                        2..=4 => {
                            assert!(matches!(result, Err(LocalError::BindingContract(_))));
                            assert_eq!((context.reads.get(), context.warnings.get()), (0, 0));
                            assert!(provider.take_native_error().is_none());
                        }
                        5 => {
                            assert!(result.is_err());
                            assert_eq!(
                                provider.take_native_error(),
                                Some(EvalError::Unsupported("bitwise truncation rejected"))
                            );
                            assert_eq!(
                                (context.reads.get(), context.warnings.get()),
                                (usize::from(operation.arity() == 2), 1)
                            );
                        }
                        6 => {
                            let HostStep::Ready(values) = result.unwrap() else {
                                panic!("NULL must finish after actual raw demand")
                            };
                            let ScalarValueRef::Bytes(Some(token)) = values.get_scalar_ref(0)
                            else {
                                panic!("expected native result")
                            };
                            assert_eq!(
                                *store
                                    .borrow()
                                    .get(
                                        token,
                                        NativeValueRole::Output,
                                        NativeValueOrigin { node: 2, row }
                                    )
                                    .unwrap(),
                                Datum::Null
                            );
                            assert_eq!(
                                (context.reads.get(), context.warnings.get()),
                                (usize::from(operation.arity() == 2), 0)
                            );
                        }
                        _ => unreachable!(),
                    }
                    provider.cancel(&task);
                    assert!(provider.task(&task).is_err());
                }
            }
        }
    }
    #[test]
    fn native_raw_unary_consumes_before_warning_callback_and_reauthenticates_after_it() {
        for declared in [true, false] {
            struct C<'a> {
                store: &'a RefCell<NativeValueStore>,
                action: Cell<u8>,
                calls: Cell<usize>,
            }
            impl Columns for C<'_> {
                fn get(&self, _: &[String]) -> Option<Datum> {
                    panic!("no invented input")
                }
                fn handle_truncate(&self, _: &str) -> std::result::Result<(), EvalError> {
                    let mut store = self.store.borrow_mut();
                    self.calls.set(self.calls.get() + 1);
                    match self.action.replace(0) {
                        1 => panic!("raw unary conversion callback"),
                        2 => {
                            store.begin_window().unwrap();
                        }
                        _ => (),
                    }
                    Ok(())
                }
            }
            let field: tipb::FieldType = tidb_query_datatype::FieldTypeTp::Blob.into();
            let args = [field.clone()];
            let operation = NativeRawValueOperation::UnarySign {
                sign: tidb_query_expr::impl_op::NativeUnarySign::Minus,
                unsigned: false,
                constant: true,
            };
            let catalog = HostCatalog::for_sql_batch(vec![(
                operation.sql_operation(),
                HostSignature {
                    arg_types: args.to_vec().into(),
                    return_type: field.clone(),
                },
            )])
            .unwrap();
            let slot = catalog.slot(0).unwrap();
            let metadata = [NativeHostMetadata::RawValues {
                operation,
                name: "unaryminus".into(),
                result_type: declared.then(|| FieldType::new(FieldTypeCode::Double)),
                left_node: 1,
                right_node: None,
                output_node: 0,
                output_role: NativeValueRole::Output,
            }];
            let store = RefCell::new(NativeValueStore::new(1 << 20).unwrap());
            store.borrow_mut().begin_window().unwrap();
            let ctx = C {
                store: &store,
                action: Cell::new(0),
                calls: Cell::new(0),
            };
            let mut provider = NativeBatchHosts::new(catalog, &metadata, &ctx, 65536)
                .with_native_values(Some(&store));
            let row = InputRow {
                occurrence: 7,
                input_row: 2,
            };
            let origin = NativeValueOrigin { node: 1, row };
            let mut eval = EvalContext::default();
            for action in [1, 2, 0] {
                ctx.calls.set(0);
                ctx.action.set(action);
                let HostStart::Pending { task, request } = provider
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
                    panic!("actual unary argument required")
                };
                assert_eq!(request.index, 0);
                let wrong = store
                    .borrow_mut()
                    .insert_clone(
                        &Datum::new_string("12tail"),
                        NativeValueRole::Output,
                        origin,
                    )
                    .unwrap();
                let wrong = VectorValue::from_scalar(&ScalarValue::Bytes(Some(wrong)), 1);
                assert!(provider
                    .resume(
                        &mut eval,
                        &task,
                        HostArgReply {
                            index: 0,
                            field_type: &field,
                            values: &wrong
                        }
                    )
                    .is_err());
                assert_eq!(ctx.calls.get(), 0);
                let token = store
                    .borrow_mut()
                    .insert_clone(
                        &Datum::new_string("12tail"),
                        NativeValueRole::CharInput,
                        origin,
                    )
                    .unwrap();
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
                assert_eq!(ctx.calls.get(), 1);
                match action {
                    1 => assert!(outcome.is_err(), "warning callback must unwind unchanged"),
                    2 => assert!(
                        matches!(outcome.unwrap(), Err(LocalError::BindingContract(_))),
                        "callback invalidated the original generation"
                    ),
                    _ => {
                        let HostStep::Ready(output) = outcome.unwrap().unwrap() else {
                            panic!("unary result required")
                        };
                        let ScalarValueRef::Bytes(Some(token)) = output.get_scalar_ref(0) else {
                            panic!()
                        };
                        assert_eq!(
                            *store
                                .borrow()
                                .get(
                                    token,
                                    NativeValueRole::Output,
                                    NativeValueOrigin { node: 0, row }
                                )
                                .unwrap(),
                            Datum::Real(-12.0)
                        );
                    }
                }
                assert!(
                    provider
                        .resume(
                            &mut eval,
                            &task,
                            HostArgReply {
                                index: 0,
                                field_type: &field,
                                values: &values
                            }
                        )
                        .is_err(),
                    "a consumed unary callback must not replay"
                );
                assert_eq!(
                    ctx.calls.get(),
                    1,
                    "replay must not call the warning handler again"
                );
                assert!(provider.take_native_error().is_none());
                provider.cancel(&task);
            }
        }
    }
    #[test]
    fn abs_context_math_consumes_warning_callback_and_reauthenticates_after_it() {
        struct C<'a> {
            store: &'a RefCell<NativeValueStore>,
            action: Cell<u8>,
            calls: Cell<usize>,
        }
        impl Columns for C<'_> {
            fn get(&self, _: &[String]) -> Option<Datum> {
                panic!("no invented input")
            }
            fn handle_truncate(&self, _: &str) -> std::result::Result<(), EvalError> {
                let mut store = self.store.borrow_mut();
                self.calls.set(self.calls.get() + 1);
                match self.action.replace(0) {
                    1 => panic!("ABS numeric callback"),
                    2 => {
                        store.begin_window().unwrap();
                    }
                    _ => (),
                }
                Ok(())
            }
        }
        let field: tipb::FieldType = tidb_query_datatype::FieldTypeTp::Blob.into();
        let args = [field.clone()];
        let catalog = HostCatalog::for_sql_batch(vec![(
            SqlBatchHostOperation::AbsNative,
            HostSignature {
                arg_types: args.to_vec().into(),
                return_type: field.clone(),
            },
        )])
        .unwrap();
        let slot = catalog.slot(0).unwrap();
        let metadata = [NativeHostMetadata::Math {
            name: "abs".into(),
            result_type: FieldType::new(FieldTypeCode::Double),
            argument_node: 1,
            output_node: 0,
            output_role: NativeValueRole::Output,
            overflow_expression: None,
        }];
        let store = RefCell::new(NativeValueStore::new(1 << 20).unwrap());
        store.borrow_mut().begin_window().unwrap();
        let ctx = C {
            store: &store,
            action: Cell::new(0),
            calls: Cell::new(0),
        };
        let mut provider =
            NativeBatchHosts::new(catalog, &metadata, &ctx, 65536).with_native_values(Some(&store));
        let row = InputRow {
            occurrence: 7,
            input_row: 2,
        };
        let origin = NativeValueOrigin { node: 1, row };
        let mut eval = EvalContext::default();
        for action in [1, 2, 0] {
            ctx.calls.set(0);
            ctx.action.set(action);
            let HostStart::Pending { task, request } = provider
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
                panic!("actual math argument required")
            };
            assert_eq!(request.index, 0);
            let wrong = store
                .borrow_mut()
                .insert_clone(
                    &Datum::new_string("12tail"),
                    NativeValueRole::Output,
                    origin,
                )
                .unwrap();
            let wrong = VectorValue::from_scalar(&ScalarValue::Bytes(Some(wrong)), 1);
            assert!(provider
                .resume(
                    &mut eval,
                    &task,
                    HostArgReply {
                        index: 0,
                        field_type: &field,
                        values: &wrong
                    }
                )
                .is_err());
            assert_eq!(ctx.calls.get(), 0);
            let token = store
                .borrow_mut()
                .insert_clone(
                    &Datum::new_string("12tail"),
                    NativeValueRole::CharInput,
                    origin,
                )
                .unwrap();
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
            assert_eq!(ctx.calls.get(), 1);
            match action {
                1 => assert!(outcome.is_err(), "warning callback must unwind unchanged"),
                2 => assert!(
                    matches!(outcome.unwrap(), Err(LocalError::BindingContract(_))),
                    "callback invalidated the original generation"
                ),
                _ => {
                    let HostStep::Ready(output) = outcome.unwrap().unwrap() else {
                        panic!("math result required")
                    };
                    let ScalarValueRef::Bytes(Some(token)) = output.get_scalar_ref(0) else {
                        panic!()
                    };
                    assert_eq!(
                        *store
                            .borrow()
                            .get(
                                token,
                                NativeValueRole::Output,
                                NativeValueOrigin { node: 0, row }
                            )
                            .unwrap(),
                        Datum::Real(12.0)
                    );
                }
            }
            assert!(
                provider
                    .resume(
                        &mut eval,
                        &task,
                        HostArgReply {
                            index: 0,
                            field_type: &field,
                            values: &values
                        }
                    )
                    .is_err(),
                "a consumed math callback must not replay"
            );
            assert_eq!(
                ctx.calls.get(),
                1,
                "replay must not call the warning handler again"
            );
            assert!(provider.take_native_error().is_none());
            provider.cancel(&task);
        }
    }
}
