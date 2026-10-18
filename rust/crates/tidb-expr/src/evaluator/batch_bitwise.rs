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

impl NativeBatchHosts<'_> {
    pub(super) fn resume_bitwise(
        &mut self,
        id: &HostTaskId,
        token: &[u8],
    ) -> LocalResult<HostStep> {
        let task = self.task(id)?;
        let SqlDemand::Bitwise { left_token } = task.demand else {
            return Err(binding("bitwise resume has another task kind"));
        };
        let Some(NativeHostMetadata::Bitwise {
            operation,
            name,
            result_type,
            left_node,
            right_node,
            unsigned,
            output_node,
            output_role,
        }) = self.metadata.get(task.slot.index())
        else {
            return Err(binding("bitwise descriptor missing"));
        };
        let row = task.row;
        let store = self
            .native_values
            .ok_or_else(|| binding("bitwise value store missing"))?;
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
                .map_err(|_| binding("bitwise token width differs"))?;
            let SqlDemand::Bitwise { left_token } = &mut self.slots[id.slot]
                .task
                .as_mut()
                .expect("validated bitwise task")
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
                window_values::conversion_work_bound(
                    value,
                    if matches!(value, Datum::Json(_)) {
                        128
                    } else {
                        16
                    },
                )
            };
            let left = bound(left_token, left_origin)?;
            left.checked_add(match right_origin {
                Some(origin) => bound(token, origin)?,
                None => 0,
            })
            .ok_or_else(|| resource("bitwise conversion allowance overflow"))?
        };
        let base = self
            .retained_bytes()?
            .checked_add(work)
            .ok_or_else(|| resource("bitwise retained size overflow"))?;
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
        let result = match crate::ops::eval_native_bitwise_values(
            *operation,
            left,
            right,
            *unsigned,
            self.context,
        ) {
            Ok(value) => value,
            Err(error) => return self.native_failure(error),
        };
        let result =
            match crate::scalar_function::coerce_function_result(name, Some(result_type), result) {
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
    fn native_bitwise_provider_keeps_resources_provenance_errors_and_reuse_terminal() {
        for operation in [B::And, B::Or, B::Xor, B::LeftShift, B::RightShift, B::Neg] {
            let field: tipb::FieldType = tidb_query_datatype::FieldTypeTp::Blob.into();
            let args = vec![field.clone(); operation.arity()];
            let catalog = HostCatalog::for_sql_batch(vec![(
                SqlBatchHostOperation::BitwiseNative(operation),
                HostSignature {
                    arg_types: args.clone().into_boxed_slice(),
                    return_type: field.clone(),
                },
            )])
            .unwrap();
            let slot = catalog.slot(0).unwrap();
            let name = match operation {
                B::And => "bitand",
                B::Or => "bitor",
                B::Xor => "bitxor",
                B::LeftShift => "leftshift",
                B::RightShift => "rightshift",
                B::Neg => "bitneg",
            };
            let metadata = [NativeHostMetadata::Bitwise {
                operation,
                name: name.into(),
                result_type: FieldType::new(FieldTypeCode::LongLong).with_unsigned(true),
                left_node: 3,
                right_node: (operation.arity() == 2).then_some(8),
                unsigned: [false; 2],
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
                    1 => {
                        Datum::Decimal(Decimal::from_raw_parts(false, b"1".to_vec(), 1_000_000, 0))
                    }
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
                            (usize::from(operation != B::Neg), 1)
                        );
                    }
                    6 => {
                        let HostStep::Ready(values) = result.unwrap() else {
                            panic!("NULL must finish after actual raw demand")
                        };
                        let ScalarValueRef::Bytes(Some(token)) = values.get_scalar_ref(0) else {
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
                            (usize::from(operation != B::Neg), 0)
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
