// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Actual needle first, then a serialized optional empty-row list constructor.
use super::*;
use crate::builtin_ext::FindInSetLookup;
use tidb_query_expr::{NativeCacheAccess, NativeCacheInitialization, NativeContextCache};

pub(in super::super) struct NativeCachedList {
    pub arguments: [usize; 2],
    pub cacheable: bool,
    pub cache: NativeContextCache<FindInSetLookup>,
    pub collation: tidb_datatype::Collation,
    pub result_type: Option<FieldType>,
    pub output_node: usize,
    pub output_role: NativeValueRole,
}
pub(super) enum CachedListTask {
    Needle,
    List {
        needle: [u8; native_values::HANDLE_BYTES],
        initialization: Option<NativeCacheInitialization<FindInSetLookup>>,
    },
}
impl CachedListTask {
    pub(super) fn next_argument(&self) -> usize {
        match self {
            Self::Needle => 0,
            Self::List { .. } => 1,
        }
    }
    pub(super) fn retained_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
    }
}
impl NativeBatchHosts<'_> {
    pub(super) fn resume_cached_list(
        &mut self,
        id: &HostTaskId,
        token: &[u8],
    ) -> LocalResult<HostStep> {
        let task = self.task(id)?;
        let SqlDemand::CachedList(state) = &task.demand else {
            return Err(binding("cached list phase differs"));
        };
        let (slot, row, index) = (task.slot, task.row, state.next_argument());
        let Some(NativeHostMetadata::CachedList(meta)) = self.metadata.get(slot.index()) else {
            return Err(binding("cached list metadata absent"));
        };
        let store = self
            .native_values
            .ok_or_else(|| binding("cached list store absent"))?;
        let origin = NativeValueOrigin {
            node: meta.arguments[index],
            row,
        };
        store
            .borrow()
            .get(token, NativeValueRole::CharInput, origin)?;
        if let CachedListTask::List { needle, .. } = &**state {
            store.borrow().get(
                needle,
                NativeValueRole::CharInput,
                NativeValueOrigin {
                    node: meta.arguments[0],
                    row,
                },
            )?;
        }
        let base = self.retained_bytes()?;
        self.check_bytes(base)?;
        let maximum = self.max_retained_bytes - base;
        // Consume before context, cache services, formatting and callbacks.
        // The owned reservation below drops on every error or unwind.
        let demand = std::mem::replace(
            &mut self.slots[id.slot]
                .task
                .as_mut()
                .expect("authenticated cached-list task")
                .demand,
            SqlDemand::Consumed,
        );
        let SqlDemand::CachedList(mut state) = demand else {
            unreachable!()
        };
        let needle_origin = NativeValueOrigin {
            node: meta.arguments[0],
            row,
        };
        if matches!(&*state, CachedListTask::Needle) {
            let needle: [u8; native_values::HANDLE_BYTES] = token
                .try_into()
                .map_err(|_| binding("cached needle capability width differs"))?;
            let access = if meta.cacheable {
                let context = self.context.context_id();
                store
                    .borrow()
                    .get(&needle, NativeValueRole::CharInput, needle_origin)?;
                Some(meta.cache.begin_initialization(context))
            } else {
                None
            };
            if let Some(NativeCacheAccess::Hit(lookup)) = access {
                let result = tidb_query_expr::local::with_value_kernel_work_limit(
                    maximum,
                    || -> LocalResult<std::result::Result<Datum, EvalError>> {
                        let prepared = {
                            let values = store.borrow();
                            crate::builtin_ext::prepare_find_in_set_lookup_value(
                                values.get(&needle, NativeValueRole::CharInput, needle_origin)?,
                                &lookup,
                                meta.collation,
                            )
                        };
                        Ok(crate::scalar_function::eager_values::audit_work_result(
                            prepared.and_then(|args| {
                                crate::builtin_ext::eval_find_in_set_prepared_values(
                                    args,
                                    true,
                                    self.context,
                                )
                                .and_then(|value| {
                                    coerce_cached_list_result(value, meta.result_type.as_ref())
                                })
                            }),
                        ))
                    },
                )?;
                store
                    .borrow()
                    .get(&needle, NativeValueRole::CharInput, needle_origin)?;
                let value = match result {
                    Ok(v) => v,
                    Err(e) => return self.native_failure(e),
                };
                return self
                    .finish_native_value(id, value, meta.output_node, meta.output_role, row, base)
                    .map(HostStep::Ready);
            }
            let initialization = match access {
                Some(NativeCacheAccess::Initialize(lease)) => Some(lease),
                None => None,
                _ => unreachable!(),
            };
            *state = CachedListTask::List {
                needle,
                initialization,
            };
            self.slots[id.slot]
                .task
                .as_mut()
                .expect("live consumed cached-list slot")
                .demand = SqlDemand::CachedList(state);
            return Ok(HostStep::NeedArg(HostArgRequest {
                index: 1,
                mode: ArgMode::Fresh,
            }));
        }
        let CachedListTask::List {
            needle,
            initialization,
        } = *state
        else {
            unreachable!()
        };
        if initialization.is_some() != meta.cacheable {
            return Err(binding("cached list initializer declaration differs"));
        }
        let result = tidb_query_expr::local::with_value_kernel_work_limit(
            maximum,
            || -> LocalResult<std::result::Result<Datum, EvalError>> {
                let prepared = if let Some(initialization) = initialization {
                    let lookup = {
                        let values = store.borrow();
                        crate::scalar_function::eager_values::audit_work_result(
                            crate::builtin_ext::build_find_in_set_lookup(
                                values.get(token, NativeValueRole::CharInput, origin)?,
                                meta.collation,
                            ),
                        )
                    };
                    let lookup = match lookup {
                        Ok(value) => value,
                        Err(error) => return Ok(Err(error)),
                    };
                    // Complete all original constructor work and authentication
                    // before committing; lookup errors afterward still keep it warm.
                    store
                        .borrow()
                        .get(token, NativeValueRole::CharInput, origin)?;
                    store
                        .borrow()
                        .get(&needle, NativeValueRole::CharInput, needle_origin)?;
                    let lookup = initialization.commit(lookup);
                    let values = store.borrow();
                    crate::builtin_ext::prepare_find_in_set_lookup_value(
                        values.get(&needle, NativeValueRole::CharInput, needle_origin)?,
                        &lookup,
                        meta.collation,
                    )
                } else {
                    let values = store.borrow();
                    crate::builtin_ext::prepare_find_in_set_tuple_values(
                        values.get(&needle, NativeValueRole::CharInput, needle_origin)?,
                        values.get(token, NativeValueRole::CharInput, origin)?,
                        meta.collation,
                    )
                };
                // Both actual input borrows have ended before the Ready capability
                // and its direct prepared kernel can observe the caller's context.
                Ok(crate::scalar_function::eager_values::audit_work_result(
                    prepared.and_then(|args| {
                        crate::builtin_ext::eval_find_in_set_prepared_values(
                            args,
                            meta.cacheable,
                            self.context,
                        )
                        .and_then(|value| {
                            coerce_cached_list_result(value, meta.result_type.as_ref())
                        })
                    }),
                ))
            },
        )?;
        store
            .borrow()
            .get(token, NativeValueRole::CharInput, origin)?;
        store
            .borrow()
            .get(&needle, NativeValueRole::CharInput, needle_origin)?;
        let value = match result {
            Ok(v) => v,
            Err(e) => return self.native_failure(e),
        };
        self.finish_native_value(id, value, meta.output_node, meta.output_role, row, base)
            .map(HostStep::Ready)
    }
}

// ScalarFunction.Eval normalizes the returned family before Expression.Eval's
// enum/set view. Keep that original owner under the remaining finite allowance.
fn coerce_cached_list_result(
    value: Datum,
    field: Option<&FieldType>,
) -> std::result::Result<Datum, EvalError> {
    let maximum = tidb_query_expr::local::value_kernel_work_remaining()
        .expect("cached list return runs inside its lexical work scope");
    let retained = native_payload_bytes(&value)
        .and_then(|bytes| {
            bytes
                .checked_mul(2)
                .ok_or_else(|| resource("cached return size overflow"))
        })
        .map_err(|error| {
            EvalError::ExpressionRuntimeFailure(crate::ExpressionRuntimeFailure::from_local_eval(
                error, None,
            ))
        })?;
    let maximum = maximum.checked_sub(retained).ok_or_else(|| {
        EvalError::ExpressionRuntimeFailure(crate::ExpressionRuntimeFailure::from_local_eval(
            resource("cached return allowance exhausted"),
            None,
        ))
    })?;
    match field {
        Some(field) => crate::tikv::coerce_result_bounded("find_in_set", field, value, maximum),
        None => Ok(value),
    }
}
