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

//! Closed context operations driven by TiKV's staged argument protocol.
//! This module owns no Expression, ScalarFunction or evaluation callback.
//! SQL algorithms and demanded-prefix rules stay in the shared core.

use super::*;
use tidb_query_datatype::codec::data_type::ScalarValueRef;
use tidb_query_expr::local::{
    ArgMode, ConcatDemand, ConcatDemandAction, ConcatKind, HostArgReply, HostArgRequest,
    HostCatalogKey, HostInvocation, HostSlot, HostStart, HostStep, HostTaskId, LocalHostServices,
};

struct ConcatTask {
    slot: HostSlot,
    kind: ConcatKind,
    arity: usize,
    demand: ConcatDemand,
    values: Vec<Option<Vec<u8>>>,
}
impl ConcatTask {
    fn retained_bytes(&self) -> LocalResult<usize> {
        self.values.iter().flatten().try_fold(
            array_bytes::<Option<Vec<u8>>>(self.values.capacity())?,
            |bytes, value| {
                bytes
                    .checked_add(value.capacity())
                    .ok_or_else(|| resource("SQL host prefix retained size overflow"))
            },
        )
    }
}
struct TaskSlot {
    generation: u64,
    task: Option<ConcatTask>,
}

pub(super) struct NativeBatchHosts<'a> {
    catalog: HostCatalog,
    context: &'a dyn Columns,
    slots: Vec<TaskSlot>,
    generation: u64,
    max_retained_bytes: usize,
    native_error: Option<EvalError>,
}
impl<'a> NativeBatchHosts<'a> {
    pub(super) fn new(
        catalog: HostCatalog,
        context: &'a dyn Columns,
        max_retained_bytes: usize,
    ) -> Self {
        Self {
            catalog,
            context,
            slots: Vec::new(),
            generation: 0,
            max_retained_bytes,
            native_error: None,
        }
    }
    pub(super) fn take_native_error(&mut self) -> Option<EvalError> {
        self.native_error.take()
    }
    fn retained_bytes(&self) -> LocalResult<usize> {
        self.slots
            .iter()
            .filter_map(|slot| slot.task.as_ref())
            .try_fold(
                array_bytes::<TaskSlot>(self.slots.capacity())?,
                |bytes, task| {
                    bytes
                        .checked_add(task.retained_bytes()?)
                        .ok_or_else(|| resource("SQL host retained size overflow"))
                },
            )
    }
    fn check_bytes(&self, bytes: usize) -> LocalResult<()> {
        if bytes > self.max_retained_bytes {
            Err(resource("SQL host retained byte cap exceeded"))
        } else {
            Ok(())
        }
    }
    fn task(&self, id: &HostTaskId) -> LocalResult<&ConcatTask> {
        self.slots
            .get(id.slot)
            .filter(|slot| slot.generation == id.generation && id.generation != 0)
            .and_then(|slot| slot.task.as_ref())
            .ok_or_else(|| LocalError::HostContract("SQL host task is stale or absent".into()))
    }
    fn finish(&mut self, id: &HostTaskId) -> LocalResult<VectorValue> {
        self.task(id)?;
        let task = self.slots[id.slot]
            .task
            .take()
            .expect("validated live task");
        let terminal = task.demand.finish_terminal()?;
        let other = self.retained_bytes()?;
        // Bound the overlap while the encoder consumes the owned prefix.
        let task_bytes = task.retained_bytes()?;
        let encode_cap = self
            .max_retained_bytes
            .checked_sub(other)
            .and_then(|bytes| bytes.checked_sub(task_bytes))
            .ok_or_else(|| resource("SQL host encoding overlap exceeds retained cap"))?;
        let prepared = tidb_query_expr::local::prepare_concat_args(
            task.kind,
            task.arity,
            task.values,
            terminal,
            encode_cap,
        )?;
        let output_cap = self
            .max_retained_bytes
            .checked_sub(other)
            .and_then(|bytes| bytes.checked_sub(prepared.retained_encoded_bytes()))
            .ok_or_else(|| resource("SQL host encoded transport exceeds retained cap"))?;
        let value = prepared.execute(output_cap)?;
        let len = value.as_ref().map_or(0, Vec::len);
        let external = value.as_ref().map_or(0, Vec::capacity);
        self.check_bytes(
            other
                .checked_add(external)
                .and_then(|bytes| bytes.checked_add(len))
                .and_then(|bytes| bytes.checked_add(std::mem::size_of::<usize>().saturating_mul(4)))
                .ok_or_else(|| resource("SQL host result overlap size overflow"))?,
        )?;
        let mut output = ChunkedVecBytes::try_with_capacities(1, len)
            .map_err(|error| LocalError::ResourceLimit(error.to_string()))?;
        output.push_ref(value.as_deref());
        self.check_bytes(
            other
                .checked_add(external)
                .and_then(|bytes| bytes.checked_add(output.retained_heap_bytes()?))
                .ok_or_else(|| resource("SQL host result retained size overflow"))?,
        )?;
        Ok(VectorValue::Bytes(output))
    }
}
impl LocalHostServices for NativeBatchHosts<'_> {
    fn catalog_key(&self) -> &HostCatalogKey {
        self.catalog.key()
    }
    fn start(
        &mut self,
        _ctx: &mut EvalContext,
        invocation: HostInvocation<'_>,
    ) -> LocalResult<HostStart> {
        let signature = self.catalog.signature(invocation.slot)?;
        if signature.arg_types.as_ref() != invocation.arg_types
            || &signature.return_type != invocation.return_type
        {
            return Err(LocalError::HostContract(
                "SQL host invocation differs from its catalog signature".into(),
            ));
        }
        let kind = match self.catalog.sql_batch_operation(invocation.slot)? {
            SqlBatchHostOperation::Concat => ConcatKind::Concat,
            SqlBatchHostOperation::ConcatWs => ConcatKind::ConcatWs,
        };
        let generation = self
            .generation
            .checked_add(1)
            .ok_or_else(|| resource("SQL host generation space exhausted"))?;
        let slot = match self.slots.iter().position(|slot| slot.task.is_none()) {
            Some(slot) => slot,
            None => {
                self.check_bytes(
                    self.retained_bytes()?
                        .checked_add(std::mem::size_of::<TaskSlot>())
                        .ok_or_else(|| resource("SQL host slot size overflow"))?,
                )?;
                self.slots
                    .try_reserve_exact(1)
                    .map_err(|error| LocalError::ResourceLimit(error.to_string()))?;
                self.slots.push(TaskSlot {
                    generation: 0,
                    task: None,
                });
                self.check_bytes(self.retained_bytes()?)?;
                self.slots.len() - 1
            }
        };
        let task = ConcatTask {
            slot: invocation.slot,
            kind,
            arity: invocation.arg_types.len(),
            demand: ConcatDemand::new(kind, invocation.arg_types.len())?,
            values: Vec::new(),
        };
        self.slots[slot] = TaskSlot {
            generation,
            task: Some(task),
        };
        self.generation = generation;
        Ok(HostStart::Pending {
            task: HostTaskId { slot, generation },
            request: HostArgRequest {
                index: 0,
                mode: ArgMode::Fresh,
            },
        })
    }
    fn resume(
        &mut self,
        _ctx: &mut EvalContext,
        id: &HostTaskId,
        reply: HostArgReply<'_>,
    ) -> LocalResult<HostStep> {
        let task = self.task(id)?;
        if task.demand.next_argument()? != Some(reply.index)
            || self
                .catalog
                .signature(task.slot)?
                .arg_types
                .get(reply.index)
                != Some(reply.field_type)
            || reply.values.len() != 1
            || reply.values.eval_type() != TikvEvalType::Bytes
        {
            return Err(LocalError::HostContract(
                "SQL host reply differs from its demanded argument".into(),
            ));
        }
        let ScalarValueRef::Bytes(bytes) = reply.values.get_scalar_ref(0) else {
            unreachable!("checked Bytes")
        };
        let len = bytes.map_or(0, <[u8]>::len);
        let old_capacity = task.values.capacity();
        let target = if task.values.len() == old_capacity {
            old_capacity.saturating_mul(2).max(4).min(task.arity)
        } else {
            old_capacity
        };
        let growth = array_bytes::<Option<Vec<u8>>>(target.saturating_sub(old_capacity))?;
        self.check_bytes(
            self.retained_bytes()?
                .checked_add(growth)
                .and_then(|size| size.checked_add(len))
                .ok_or_else(|| resource("SQL host prefix growth size overflow"))?,
        )?;
        let mut owned = None;
        if let Some(bytes) = bytes {
            let mut value = Vec::new();
            value
                .try_reserve_exact(bytes.len())
                .map_err(|error| LocalError::ResourceLimit(error.to_string()))?;
            value.extend_from_slice(bytes);
            owned = Some(value);
        }
        let action = {
            let task = self.slots[id.slot]
                .task
                .as_mut()
                .expect("validated live task");
            if target > old_capacity {
                task.values
                    .try_reserve_exact(target - task.values.len())
                    .map_err(|error| LocalError::ResourceLimit(error.to_string()))?;
            }
            task.values.push(owned);
            task.demand.observe(bytes.map(<[u8]>::len))?
        };
        self.check_bytes(self.retained_bytes()?)?;
        if action == ConcatDemandAction::ReadPacketLimit {
            let limit = self.context.max_allowed_packet();
            let task = self.slots[id.slot]
                .task
                .as_mut()
                .expect("validated live task");
            if task.demand.accept_packet_limit(limit)? {
                let name = match task.kind {
                    ConcatKind::Concat => "concat",
                    ConcatKind::ConcatWs => "concat_ws",
                };
                if let Err(error) = self.context.handle_allowed_packet_overflowed(name) {
                    // The caller owns this exact native error. No numeric/text
                    // classification or replay is needed at the frame boundary.
                    self.native_error = Some(error);
                    return Err(LocalError::HostContract(
                        "native SQL context operation failed".into(),
                    ));
                }
            }
        }
        match self.task(id)?.demand.next_argument()? {
            Some(index) => Ok(HostStep::NeedArg(HostArgRequest {
                index,
                mode: ArgMode::Fresh,
            })),
            None => self.finish(id).map(HostStep::Ready),
        }
    }
    fn cancel(&mut self, id: &HostTaskId) {
        if let Some(slot) = self.slots.get_mut(id.slot) {
            if slot.generation == id.generation {
                slot.task = None;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn prefix_heap_denial_and_stale_cancel_keep_task_generations_separate() {
        let field: tipb::FieldType = tidb_query_datatype::FieldTypeTp::VarString.into();
        let args = vec![field.clone(); 2];
        let catalog = HostCatalog::for_sql_batch(vec![(
            SqlBatchHostOperation::Concat,
            HostSignature {
                arg_types: args.clone().into_boxed_slice(),
                return_type: field.clone(),
            },
        )])
        .unwrap();
        let slot = catalog.slot(0).unwrap();
        let mut provider = NativeBatchHosts::new(catalog, &crate::NoColumns, 512);
        let mut ctx = EvalContext::default();
        let invocation = || HostInvocation {
            slot,
            row: InputRow {
                occurrence: 0,
                input_row: 0,
            },
            arg_types: &args,
            return_type: &field,
        };
        let HostStart::Pending { task: first, .. } =
            provider.start(&mut ctx, invocation()).unwrap()
        else {
            panic!("expected argument request")
        };
        let value = VectorValue::from_scalar(&ScalarValue::Bytes(Some(vec![b'x'; 700])), 1);
        assert!(matches!(
            provider.resume(
                &mut ctx,
                &first,
                HostArgReply {
                    index: 0,
                    field_type: &field,
                    values: &value
                }
            ),
            Err(LocalError::ResourceLimit(_))
        ));
        provider.cancel(&first);
        provider.cancel(&first);
        assert!(provider.task(&first).is_err());
        let HostStart::Pending { task: second, .. } =
            provider.start(&mut ctx, invocation()).unwrap()
        else {
            panic!("expected new argument request")
        };
        assert_eq!(first.slot, second.slot);
        assert_ne!(first.generation, second.generation);
        provider.cancel(&first);
        assert!(provider.task(&second).is_ok());
        provider.cancel(&second);
        assert!(provider.retained_bytes().unwrap() <= 512);
        assert!(provider.take_native_error().is_none());
    }
}
