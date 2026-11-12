// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Stateful phases suspend only through the existing HostFrame argument broker.
use super::*;
use crate::scalar_function::stateful::{Descriptor, Operation as O};
pub(in super::super) struct NativeStateful {
    pub arguments: Box<[usize]>,
    pub descriptor: Descriptor,
    pub repeated_subtree: Option<std::ops::Range<usize>>,
    pub result_type: Option<FieldType>,
    pub output_node: usize,
    pub output_role: NativeValueRole,
}
pub(super) enum StatefulTask {
    First,
    Repeat {
        count: [u8; native_values::HANDLE_BYTES],
        remaining: i64,
    },
    Timeout {
        name: String,
        source: [u8; native_values::HANDLE_BYTES],
    },
}
impl StatefulTask {
    pub(super) fn next_argument(&self) -> usize {
        match self {
            Self::First => 0,
            _ => 1,
        }
    }
    #[cfg(test)]
    pub(super) fn retained_bytes(&self) -> LocalResult<usize> {
        std::mem::size_of::<Self>()
            .checked_add(match self {
                Self::Timeout { name, .. } => name.capacity(),
                _ => 0,
            })
            .ok_or_else(|| resource("stateful task retained extent overflow"))
    }
}
enum Progress {
    Ready(Datum),
    More(StatefulTask),
}
impl NativeBatchHosts<'_> {
    pub(super) fn resume_stateful(
        &mut self,
        id: &HostTaskId,
        token: Option<&[u8]>,
    ) -> LocalResult<HostStep> {
        let task = self.task(id)?;
        let SqlDemand::Stateful(state) = &task.demand else {
            return Err(binding("stateful phase differs"));
        };
        let (slot, row, index) = (task.slot, task.row, state.next_argument());
        let Some(NativeHostMetadata::Stateful(meta)) = self.metadata.get(slot.index()) else {
            return Err(binding("stateful metadata absent"));
        };
        let store = self
            .native_values
            .ok_or_else(|| binding("stateful store absent"))?;
        let origin = |i| -> LocalResult<NativeValueOrigin> {
            Ok(NativeValueOrigin {
                node: *meta
                    .arguments
                    .get(i)
                    .ok_or_else(|| binding("stateful argument ordinal absent"))?,
                row,
            })
        };
        let previous = match &**state {
            StatefulTask::Repeat { count, .. } => Some((&count[..], origin(0)?)),
            StatefulTask::Timeout { source, .. } => Some((&source[..], origin(0)?)),
            _ => None,
        };
        if let Some(token) = token {
            store
                .borrow()
                .get(token, NativeValueRole::CharInput, origin(index)?)?;
        }
        if let Some((token, origin)) = previous {
            store
                .borrow()
                .get(token, NativeValueRole::CharInput, origin)?;
        }
        let borrowed_sleep = matches!(&**state, StatefulTask::First)
            && meta.descriptor.operation == O::Sleep
            && if let Some(token) = token {
                crate::scalar_function::stateful::borrowed_sleep_input(store.borrow().get(
                    token,
                    NativeValueRole::CharInput,
                    origin(index)?,
                )?)
            } else {
                false
            };

        // Payload ownership is transferred, never cloned here.

        let demand = std::mem::replace(
            &mut self.slots[id.slot]
                .task
                .as_mut()
                .expect("authenticated stateful task")
                .demand,
            SqlDemand::Consumed,
        );
        let SqlDemand::Stateful(state) = demand else {
            unreachable!()
        };
        let repeated = matches!(&*state, StatefulTask::Repeat { .. });
        let previous = match &*state {
            StatefulTask::Repeat { count, .. } => Some(*count),
            StatefulTask::Timeout { source, .. } => Some(*source),
            _ => None,
        };
        let value = if borrowed_sleep || matches!(&*state, StatefulTask::Repeat { .. }) {
            None
        } else {
            token
                .map(|token| {
                    native_values::NativeValueStore::lease_for_kernel(
                        store,
                        token,
                        NativeValueRole::CharInput,
                        origin(index)?,
                    )
                })
                .transpose()?
        };
        // Neither this owned input nor the pending task borrows the store while
        // conversion or the actual session service can call back into it.
        let result = (|| {
            let result = (|| -> std::result::Result<Progress, EvalError> {
                meta.descriptor.validate_arity()?;
                let progress = match *state {
                    StatefulTask::First => match meta.descriptor.operation {
                        O::Values | O::ReleaseAll => {
                            Progress::Ready(meta.descriptor.immediate(self.context)?)
                        }
                        O::Benchmark => match meta.descriptor.benchmark_count(
                            value.as_ref().expect("actual count reply").datum(),
                            self.context,
                        )? {
                            None => Progress::Ready(Datum::Null),
                            Some(0) => Progress::Ready(Datum::Int(0)),
                            Some(remaining) => Progress::More(StatefulTask::Repeat {
                                count: token
                                    .expect("count capability")
                                    .try_into()
                                    .expect("authenticated capability width"),
                                remaining,
                            }),
                        },
                        O::Sleep => {
                            if borrowed_sleep {
                                let seconds = {
                                    let values = store.borrow();
                                    let arg = values
                                        .get(
                                            token.expect("real sleep capability"),
                                            NativeValueRole::CharInput,
                                            origin(index).map_err(stateful_local_error)?,
                                        )
                                        .map_err(stateful_local_error)?;
                                    crate::scalar_function::stateful::prepare_sleep_nontext(arg)
                                        .expect("read-only classified nontext")
                                }?;
                                // All input borrows ended before the service.
                                Progress::Ready(
                                    meta.descriptor.sleep_after_seconds(seconds, self.context)?,
                                )
                            } else {
                                Progress::Ready(meta.descriptor.sleep(
                                    value.as_ref().expect("actual text reply").datum(),
                                    self.context,
                                )?)
                            }
                        }
                        operation => {
                            let name = meta.descriptor.lock_name(
                                value.as_ref().expect("actual name reply").datum(),
                                self.context,
                            )?;
                            if operation == O::GetLock {
                                Progress::More(StatefulTask::Timeout {
                                    name,
                                    source: token
                                        .expect("name capability")
                                        .try_into()
                                        .expect("authenticated capability width"),
                                })
                            } else {
                                Progress::Ready(meta.descriptor.lock_service(&name, self.context)?)
                            }
                        }
                    },
                    StatefulTask::Repeat { count, remaining } => {
                        // The broker has just executed this genuine child Fresh.
                        // Its result is ignored after capability validation.
                        if remaining == 1 {
                            Progress::Ready(Datum::Int(0))
                        } else {
                            Progress::More(StatefulTask::Repeat {
                                count,
                                remaining: remaining - 1,
                            })
                        }
                    }
                    StatefulTask::Timeout { name, .. } => {
                        Progress::Ready(meta.descriptor.acquire(
                            &name,
                            value.as_ref().expect("actual timeout reply").datum(),
                            self.context,
                        )?)
                    }
                };
                match progress {
                    Progress::Ready(value) => Ok(Progress::Ready(
                        crate::scalar_function::coerce_function_result(
                            meta.descriptor.operation_name(),
                            meta.result_type.as_ref(),
                            value,
                        )?,
                    )),
                    more => Ok(more),
                }
            })();
            result
        })();
        if let Some(token) = token {
            store
                .borrow()
                .authenticate(token, NativeValueRole::CharInput, origin(index)?)?;
        }
        if let Some(token) = previous {
            store
                .borrow()
                .get(&token, NativeValueRole::CharInput, origin(0)?)?;
        }
        drop(value); // Restore only after re-authentication; no callbacks remain.
        if repeated {
            let nodes = meta
                .repeated_subtree
                .clone()
                .ok_or_else(|| binding("benchmark subtree declaration absent"))?;
            store.borrow_mut().discard_subtree(row, nodes)?;
        }
        match result {
            Err(error) => self.native_failure(error),
            Ok(Progress::Ready(value)) => self
                .finish_native_value(id, value, meta.output_node, meta.output_role, row)
                .map(HostStep::Ready),
            Ok(Progress::More(state)) => {
                let next = state.next_argument();

                self.slots[id.slot]
                    .task
                    .as_mut()
                    .expect("live consumed stateful task")
                    .demand = SqlDemand::Stateful(Box::new(state));
                Ok(HostStep::NeedArg(HostArgRequest {
                    index: next,
                    mode: ArgMode::Fresh,
                }))
            }
        }
    }
}

fn stateful_local_error(error: LocalError) -> EvalError {
    EvalError::ExpressionRuntimeFailure(crate::ExpressionRuntimeFailure::from_local_eval(
        error, None,
    ))
}
