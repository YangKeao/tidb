// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! AST source adapter for the existing authenticated SDK HostFrame protocol.
use super::*;
use ast_source::demand::AstProgress;

impl NativeBatchHosts<'_> {
    pub(super) fn start_ast(&mut self, invocation: HostInvocation<'_>) -> LocalResult<HostStart> {
        let Some(NativeHostMetadata::Ast {
            node,
            arguments,
            raw_string,
        }) = self.metadata.get(invocation.slot.index())
        else {
            return Err(binding("AST host metadata absent"));
        };
        if self.ast_nodes.get(*node).is_none()
            || self.native_values.is_none()
            || arguments.len() != invocation.arg_types.len()
        {
            return Err(binding("AST source declaration differs"));
        }

        let HostStart::Pending { task, .. } = self.install_task(
            invocation.slot,
            invocation.row,
            arguments.len(),
            SqlDemand::Ast(Box::new(ast_source::AstTask::new(*raw_string))),
        )?
        else {
            unreachable!()
        };
        // Before returning its handle, this scope owns cancellation on unwind.
        struct StartGuard<'a, 'ctx> {
            hosts: &'a mut NativeBatchHosts<'ctx>,
            task: HostTaskId,
            armed: bool,
        }
        impl Drop for StartGuard<'_, '_> {
            fn drop(&mut self) {
                if self.armed {
                    self.hosts.cancel(&self.task);
                }
            }
        }
        let mut guard = StartGuard {
            hosts: self,
            task,
            armed: true,
        };
        let progress = guard.hosts.resume_ast(&guard.task, None)?;
        match progress {
            HostStep::Ready(value) => Ok(HostStart::Ready(value)),
            HostStep::NeedArg(request) => {
                // Only a live task may cross into the official shared driver.
                let task = guard.task;
                guard.armed = false;
                Ok(HostStart::Pending { task, request })
            }
            HostStep::NeedArgRow { .. } => Err(binding("AST cannot remap its logical coordinate")),
        }
    }
    pub(super) fn resume_ast(
        &mut self,
        id: &HostTaskId,
        token: Option<&[u8]>,
    ) -> LocalResult<HostStep> {
        let task = self.task(id)?;
        let (slot, row, arity) = (task.slot, task.row, task.arity);
        let SqlDemand::Ast(state) = &task.demand else {
            return Err(binding("AST phase consumed"));
        };
        let index = state.next;
        if index.is_some() != token.is_some() {
            return Err(binding("AST reply differs from its demand"));
        }
        let Some(NativeHostMetadata::Ast {
            node, arguments, ..
        }) = self.metadata.get(slot.index())
        else {
            return Err(binding("AST source metadata absent"));
        };
        let (node, arguments) = (*node, arguments.as_ref());
        let root = *self
            .ast_nodes
            .get(node)
            .ok_or_else(|| binding("AST genuine owner absent"))?;
        let store = self
            .native_values
            .ok_or_else(|| binding("AST storage absent"))?;
        let origin = if let Some(index) = index {
            Some(NativeValueOrigin {
                node: *arguments
                    .get(index)
                    .ok_or_else(|| binding("AST demanded child absent"))?,
                row,
            })
        } else {
            None
        };
        for (index, previous) in &state.tokens {
            store.borrow().get(
                previous,
                NativeValueRole::CharInput,
                NativeValueOrigin {
                    node: *arguments
                        .get(*index)
                        .ok_or_else(|| binding("AST previous child absent"))?,
                    row,
                },
            )?;
        }
        if let Some(token) = token {
            store.borrow().get(
                token,
                NativeValueRole::CharInput,
                origin.expect("actual demanded coordinate"),
            )?;
        }

        let incoming = if let Some(token) = token {
            Some(store.borrow().clone_for_kernel(
                token,
                NativeValueRole::CharInput,
                origin.expect("actual child"),
            )?)
        } else {
            None
        };
        // Consume the continuation BEFORE conversions or callbacks. A failed or
        // panicking phase can never be resumed with the same task generation.
        let SqlDemand::Ast(mut state) = std::mem::replace(
            &mut self.slots[id.slot]
                .task
                .as_mut()
                .expect("authenticated AST task")
                .demand,
            SqlDemand::Consumed,
        ) else {
            unreachable!()
        };
        state.reserve_reply()?;
        if let (Some(index), Some(token)) = (index, token) {
            state.tokens.push((
                index,
                token
                    .try_into()
                    .map_err(|_| binding("AST capability width differs"))?,
            ));
        }

        let mut progress = (|| state.advance(root, arity, incoming, self.context))();
        if let Ok(AstProgress::Prepare { aes }) = progress {
            // Match the original cache selection before preparation/children;
            // no caller reference is retained in the owned AST continuation.
            self.slots[id.slot].ready_preparation = self
                .context
                .ready_value_cache()
                .map(|cache| cache.preparation_unwind_guard());
            progress = (|| state.prepare(root, aes, self.context))();
        }
        for (index, previous) in &state.tokens {
            store.borrow().get(
                previous,
                NativeValueRole::CharInput,
                NativeValueOrigin {
                    node: *arguments
                        .get(*index)
                        .ok_or_else(|| binding("AST input declaration absent"))?,
                    row,
                },
            )?;
        }
        let progress = match progress {
            Ok(p) => p,
            Err(error) => return self.native_failure(error),
        };
        let progress = match progress {
            AstProgress::Cipher(operation, args) => {
                let cache = self.slots[id.slot]
                    .ready_preparation
                    .as_ref()
                    .map(|guard| guard.cache());
                let result = (|| {
                    crate::tikv::evaluate_closed_args(cache, operation, args)
                        .and_then(|result| result.into_bytes())
                        .map(|value| value.map_or(Datum::Null, Datum::new_string))
                })();
                match result {
                    Ok(v) => AstProgress::Ready(v),
                    Err(e) => return self.native_failure(e),
                }
            }
            AstProgress::Regexp(args) => {
                let cache = self.slots[id.slot]
                    .ready_preparation
                    .as_ref()
                    .map(|guard| guard.cache());
                let result = (|| crate::tikv::evaluate_closed_regexp_args(cache, args))();
                match result {
                    Ok(v) => AstProgress::Ready(v),
                    Err(e) => return self.native_failure(e),
                }
            }
            p => p,
        };
        for (index, previous) in &state.tokens {
            store.borrow().get(
                previous,
                NativeValueRole::CharInput,
                NativeValueOrigin {
                    node: *arguments
                        .get(*index)
                        .ok_or_else(|| binding("AST input declaration absent"))?,
                    row,
                },
            )?;
        }
        match progress {
            AstProgress::Need(index) => {
                if state.next != Some(index) || index >= arity {
                    return Err(binding("AST continuation demanded an undeclared child"));
                }

                self.slots[id.slot]
                    .task
                    .as_mut()
                    .expect("live AST task")
                    .demand = SqlDemand::Ast(state);
                Ok(HostStep::NeedArg(HostArgRequest {
                    index,
                    mode: ArgMode::Fresh,
                }))
            }
            AstProgress::Ready(value) => self
                .finish_native_value(
                    id,
                    value,
                    node,
                    if node == 0 {
                        NativeValueRole::Output
                    } else {
                        NativeValueRole::CharInput
                    },
                    row,
                )
                .map(HostStep::Ready),
            _ => Err(binding("AST preparation did not finish")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;
    use tidb_query_datatype::codec::data_type::ScalarValue;

    struct Source<'a> {
        values: &'a RefCell<NativeValueStore>,
        invalidate: Cell<bool>,
        writes: Cell<usize>,
        null_c: bool,
    }
    impl Columns for Source<'_> {
        fn get(&self, path: &[String]) -> Option<Datum> {
            path.last().map(|name| {
                if name == "c" && self.null_c {
                    Datum::Null
                } else {
                    Datum::Int(7)
                }
            })
        }
        fn set_uservar(&self, _: &str, _: Datum) {
            self.writes.set(self.writes.get() + 1);
            if self.invalidate.get() {
                self.values.borrow_mut().begin_window().unwrap();
            }
        }
    }
    fn source<'a>(store: &'a RefCell<NativeValueStore>, null_c: bool) -> Source<'a> {
        Source {
            values: store,
            invalidate: Cell::new(false),
            writes: Cell::new(0),
            null_c,
        }
    }
    fn invocation<'a>(catalog: &'a HostCatalog) -> HostInvocation<'a> {
        let slot = catalog.slot(0).unwrap();
        let signature = catalog.signature(slot).unwrap();
        HostInvocation {
            slot,
            row: InputRow {
                occurrence: 0,
                input_row: 0,
            },
            arg_types: &signature.arg_types,
            return_type: &signature.return_type,
        }
    }
    fn actual_token(
        plan: &ast_source::BorrowedAst<'_>,
        ctx: &Source<'_>,
        argument: usize,
    ) -> Vec<u8> {
        let (_, metadata, nodes) = plan.host_parts();
        let NativeHostMetadata::Ast { arguments, .. } = &metadata[0] else {
            unreachable!()
        };
        let node = arguments[argument];
        let tidb_ast::Expr::Column(path) = nodes[node] else {
            panic!("this protocol fixture demands a genuine named column");
        };
        let value = ctx.get(path).unwrap();
        ctx.values
            .borrow_mut()
            .insert_clone(
                &value,
                NativeValueRole::CharInput,
                NativeValueOrigin {
                    node,
                    row: InputRow {
                        occurrence: 0,
                        input_row: 0,
                    },
                },
            )
            .unwrap()
            .to_vec()
    }
    fn reply<'a>(
        catalog: &'a HostCatalog,
        index: usize,
        value: &'a VectorValue,
    ) -> HostArgReply<'a> {
        HostArgReply {
            index,
            field_type: &catalog
                .signature(catalog.slot(0).unwrap())
                .unwrap()
                .arg_types[index],
            values: value,
        }
    }
    fn vector(token: Vec<u8>) -> VectorValue {
        VectorValue::from_scalar(&ScalarValue::Bytes(Some(token)), 1)
    }
    #[test]
    fn genuine_ast_cancellation_stale_generation_and_wrong_origin_cannot_replay_demand() {
        let root = crate::ast_source_tests::parsed("IFNULL(c,d)");
        let plan = ast_source::BorrowedAst::compile(&root).unwrap();
        let (catalog, metadata, nodes) = plan.host_parts();
        let store = RefCell::new(NativeValueStore::new().unwrap());
        store.borrow_mut().begin_window().unwrap();
        let ctx = source(&store, true);
        let mut hosts = NativeBatchHosts::new(catalog.clone(), metadata, &ctx)
            .with_ast_nodes(nodes)
            .with_native_values(Some(&store));
        let mut sdk = EvalContext::default();
        let HostStart::Pending {
            task: first,
            request,
        } = hosts.start(&mut sdk, invocation(catalog)).unwrap()
        else {
            panic!("real first source demand");
        };
        assert_eq!(request.index, 0);
        let wrong = vector(actual_token(&plan, &ctx, 1));
        assert!(matches!(
            hosts.resume(&mut sdk, &first, reply(catalog, 0, &wrong)),
            Err(LocalError::BindingContract(_))
        ));
        let null = vector(actual_token(&plan, &ctx, 0));
        let HostStep::NeedArg(request) = hosts
            .resume(&mut sdk, &first, reply(catalog, 0, &null))
            .unwrap()
        else {
            panic!("NULL demands fallback");
        };
        assert_eq!(request.index, 1);
        hosts.cancel(&first);
        let right = vector(actual_token(&plan, &ctx, 1));
        assert!(hosts
            .resume(&mut sdk, &first, reply(catalog, 1, &right))
            .is_err());
        let HostStart::Pending { task: second, .. } =
            hosts.start(&mut sdk, invocation(catalog)).unwrap()
        else {
            panic!("fresh generation");
        };
        assert_eq!(first.slot, second.slot);
        assert_ne!(first.generation, second.generation);
        hosts.cancel(&first);
        assert!(hosts.task(&second).is_ok());
        assert!(matches!(
            hosts
                .resume(&mut sdk, &second, reply(catalog, 0, &null))
                .unwrap(),
            HostStep::NeedArg(_)
        ));
        let HostStep::Ready(VectorValue::Bytes(output)) = hosts
            .resume(&mut sdk, &second, reply(catalog, 1, &right))
            .unwrap()
        else {
            panic!("actual result");
        };
        assert_eq!(
            store
                .borrow()
                .get(
                    output.get(0).unwrap(),
                    NativeValueRole::Output,
                    NativeValueOrigin {
                        node: 0,
                        row: InputRow {
                            occurrence: 0,
                            input_row: 0
                        }
                    }
                )
                .unwrap(),
            &Datum::Int(7)
        );
        assert!(hosts.task(&second).is_err());
        assert_eq!(ctx.writes.get(), 0);
        assert!(hosts.take_native_error().is_none());
    }
    #[test]
    fn genuine_ast_setter_can_invalidate_storage_without_a_borrow_or_effect_replay() {
        let root = crate::ast_source_tests::parsed("@x:=c");
        let plan = ast_source::BorrowedAst::compile(&root).unwrap();
        let (catalog, metadata, nodes) = plan.host_parts();
        let store = RefCell::new(NativeValueStore::new().unwrap());
        store.borrow_mut().begin_window().unwrap();
        let ctx = source(&store, false);
        ctx.invalidate.set(true);
        let mut hosts = NativeBatchHosts::new(catalog.clone(), metadata, &ctx)
            .with_ast_nodes(nodes)
            .with_native_values(Some(&store));
        let mut sdk = EvalContext::default();
        let HostStart::Pending { task, .. } = hosts.start(&mut sdk, invocation(catalog)).unwrap()
        else {
            panic!("actual RHS demand");
        };
        let input = vector(actual_token(&plan, &ctx, 0));
        assert!(matches!(
            hosts.resume(&mut sdk, &task, reply(catalog, 0, &input)),
            Err(LocalError::BindingContract(_))
        ));
        assert_eq!(ctx.writes.get(), 1);
        assert!(hosts
            .resume(&mut sdk, &task, reply(catalog, 0, &input))
            .is_err());
        assert_eq!(ctx.writes.get(), 1);
        hosts.cancel(&task);
        ctx.invalidate.set(false);
        let HostStart::Pending { task: next, .. } =
            hosts.start(&mut sdk, invocation(catalog)).unwrap()
        else {
            panic!("fresh RHS demand");
        };
        let input = vector(actual_token(&plan, &ctx, 0));
        assert!(matches!(
            hosts
                .resume(&mut sdk, &next, reply(catalog, 0, &input))
                .unwrap(),
            HostStep::Ready(_)
        ));
        assert_eq!(ctx.writes.get(), 2);
        assert!(hosts.take_native_error().is_none());
    }
}
