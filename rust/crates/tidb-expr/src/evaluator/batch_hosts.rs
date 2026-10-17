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

pub(super) enum NativeHostMetadata {
    Plain,
    Regexp(tidb_query_expr::NativeRegexpInvocation),
    CharCast {
        charset: Box<str>,
        unsigned: bool,
    },
    CharNative {
        charset: Box<str>,
        source_type: FieldType,
        argument_node: usize,
    },
}
impl NativeHostMetadata {
    fn regexp(&self) -> Option<&tidb_query_expr::NativeRegexpInvocation> {
        match self {
            Self::Regexp(invocation) => Some(invocation),
            _ => None,
        }
    }
}

enum SqlDemand {
    CastCharNative {
        len: Option<u32>,
    },
    CastCharInt {
        len: Option<u32>,
    },
    Concat {
        kind: ConcatKind,
        demand: ConcatDemand,
    },
    RegexpLike {
        case_insensitive: bool,
        next: Option<usize>,
    },
}
struct NativeTask {
    slot: HostSlot,
    row: InputRow,
    arity: usize,
    demand: SqlDemand,
    values: Vec<Option<Vec<u8>>>,
}
impl NativeTask {
    fn next_argument(&self) -> LocalResult<Option<usize>> {
        match &self.demand {
            SqlDemand::Concat { demand, .. } => demand.next_argument(),
            SqlDemand::RegexpLike { next, .. } => Ok(*next),
            SqlDemand::CastCharInt { .. } | SqlDemand::CastCharNative { .. } => Ok(Some(0)),
        }
    }
    fn observe(&mut self, len: Option<usize>) -> LocalResult<ConcatDemandAction> {
        match &mut self.demand {
            SqlDemand::Concat { demand, .. } => demand.observe(len),
            SqlDemand::CastCharInt { .. } | SqlDemand::CastCharNative { .. } => Err(
                LocalError::HostContract("character cast cannot buffer a prefix".into()),
            ),
            SqlDemand::RegexpLike { next, .. } => {
                *next = if len.is_some() && self.values.len() < self.arity {
                    Some(self.values.len())
                } else {
                    None
                };
                Ok(ConcatDemandAction::Continue)
            }
        }
    }
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
struct CapturedWarnings {
    rows: Vec<(u16, String)>,
    base_bytes: usize,
    max_bytes: usize,
    failure: Option<LocalError>,
}
impl CapturedWarnings {
    fn retained_bytes(&self) -> LocalResult<usize> {
        self.rows.iter().try_fold(
            array_bytes::<(u16, String)>(self.rows.capacity())?,
            |bytes, (_, message)| {
                bytes
                    .checked_add(message.capacity())
                    .ok_or_else(|| resource("native warning retained size overflow"))
            },
        )
    }
    fn check(&self, extra: usize) -> LocalResult<()> {
        let bytes = self
            .base_bytes
            .checked_add(self.retained_bytes()?)
            .and_then(|bytes| bytes.checked_add(extra))
            .ok_or_else(|| resource("native warning capture size overflow"))?;
        if bytes > self.max_bytes {
            Err(resource("native warning capture policy exceeded"))
        } else {
            Ok(())
        }
    }
    fn capture(&mut self, code: u16, message: &str) {
        if self.failure.is_some() {
            return;
        }
        if let Err(error) = self.try_capture(code, message) {
            self.failure = Some(error);
        }
    }
    fn try_capture(&mut self, code: u16, message: &str) -> LocalResult<()> {
        let growth = if self.rows.len() == self.rows.capacity() {
            size_of::<(u16, String)>()
        } else {
            0
        };
        self.check(
            growth
                .checked_add(message.len())
                .ok_or_else(|| resource("native warning growth overflow"))?,
        )?;
        self.rows
            .try_reserve_exact(1)
            .map_err(|error| LocalError::ResourceLimit(error.to_string()))?;
        self.check(message.len())?;
        let mut owned = String::new();
        owned
            .try_reserve_exact(message.len())
            .map_err(|error| LocalError::ResourceLimit(error.to_string()))?;
        owned.push_str(message);
        self.check(owned.capacity())?;
        self.rows.push((code, owned));
        Ok(())
    }
}

struct TaskSlot {
    generation: u64,
    task: Option<NativeTask>,
}

pub(super) struct NativeBatchHosts<'a> {
    catalog: HostCatalog,
    context: &'a dyn Columns,
    metadata: &'a [NativeHostMetadata],
    native_values: Option<&'a RefCell<NativeValueStore>>,
    slots: Vec<TaskSlot>,
    generation: u64,
    max_retained_bytes: usize,
    native_error: Option<EvalError>,
}
impl<'a> NativeBatchHosts<'a> {
    pub(super) fn new(
        catalog: HostCatalog,
        metadata: &'a [NativeHostMetadata],
        context: &'a dyn Columns,
        max_retained_bytes: usize,
    ) -> Self {
        Self {
            catalog,
            metadata,
            native_values: None,
            context,
            slots: Vec::new(),
            generation: 0,
            max_retained_bytes,
            native_error: None,
        }
    }
    pub(super) fn with_native_values(
        mut self,
        values: Option<&'a RefCell<NativeValueStore>>,
    ) -> Self {
        self.native_values = values;
        self
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
    fn task(&self, id: &HostTaskId) -> LocalResult<&NativeTask> {
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
        if let SqlDemand::RegexpLike {
            case_insensitive,
            next,
        } = &task.demand
        {
            if next.is_some() {
                return Err(LocalError::HostContract(
                    "regexp task is not terminal".into(),
                ));
            }
            return self.finish_regexp(&task, *case_insensitive);
        }
        let SqlDemand::Concat { kind, demand } = &task.demand else {
            unreachable!()
        };
        let terminal = demand.finish_terminal()?;
        let other = self.retained_bytes()?;
        // Bound the overlap while the encoder consumes the owned prefix.
        let task_bytes = task.retained_bytes()?;
        let encode_cap = self
            .max_retained_bytes
            .checked_sub(other)
            .and_then(|bytes| bytes.checked_sub(task_bytes))
            .ok_or_else(|| resource("SQL host encoding overlap exceeds retained cap"))?;
        let prepared = tidb_query_expr::local::prepare_concat_args(
            *kind,
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
    fn finish_native_cast(
        &mut self,
        id: &HostTaskId,
        token: &[u8],
        len: Option<u32>,
    ) -> LocalResult<VectorValue> {
        let task = self.task(id)?;
        let Some(NativeHostMetadata::CharNative {
            charset,
            source_type,
            argument_node,
        }) = self.metadata.get(task.slot.index())
        else {
            return Err(binding("native character cast lost its source metadata"));
        };
        let store = self
            .native_values
            .ok_or_else(|| binding("native character cast has no value store"))?;
        let value = store.borrow().clone_for_kernel(
            token,
            NativeValueRole::CharInput,
            NativeValueOrigin {
                node: *argument_node,
                row: task.row,
            },
        )?;
        let input_charge = native_payload_bytes(&value)?
            .checked_mul(2)
            .ok_or_else(|| resource("native cast input allowance overflow"))?;
        let base_bytes = self
            .retained_bytes()?
            .checked_add(input_charge)
            .ok_or_else(|| resource("native cast live input size overflow"))?;
        self.check_bytes(base_bytes)?;
        let mut warnings = CapturedWarnings {
            rows: Vec::new(),
            base_bytes,
            max_bytes: self.max_retained_bytes,
            failure: None,
        };
        let result = if value.is_null() {
            Ok(Datum::Null)
        } else {
            crate::cast::validate_cast_value_admission(
                &value,
                tidb_query_expr::NativeCastAdmissionTarget::String,
            )
            .and_then(|()| {
                crate::tikv::eval_cast_char_reported_in(
                    self.context,
                    &value,
                    Some(source_type),
                    len,
                    Some(charset),
                    |code, message| warnings.capture(code, message),
                )
            })
        };
        if let Some(error) = warnings.failure.take() {
            return Err(error);
        }
        let result = match result {
            Ok(value) => value,
            Err(error) => {
                for (code, message) in &warnings.rows {
                    self.context.append_warning(*code, message);
                }
                return self.native_failure(error);
            }
        };
        let bytes = match result {
            Datum::Null => None,
            Datum::String(value) => Some(value.into_bytes()),
            Datum::Bytes(value) => Some(value),
            _ => return Err(binding("native CHAR kernel returned an unexpected kind")),
        };
        let length = bytes.as_ref().map_or(0, Vec::len);
        let capacity = bytes.as_ref().map_or(0, Vec::capacity);
        warnings.check(
            capacity
                .checked_add(length)
                .and_then(|bytes| bytes.checked_add(size_of::<usize>().saturating_mul(4)))
                .ok_or_else(|| resource("native CHAR output overlap overflow"))?,
        )?;
        let mut output = ChunkedVecBytes::try_with_capacities(1, length)
            .map_err(|error| LocalError::ResourceLimit(error.to_string()))?;
        output.push_ref(bytes.as_deref());
        warnings.check(
            output
                .retained_heap_bytes()
                .and_then(|size| size.checked_add(capacity))
                .ok_or_else(|| resource("native CHAR result size overflow"))?,
        )?;
        for (code, message) in &warnings.rows {
            self.context.append_warning(*code, message);
        }
        self.slots[id.slot].task = None;
        Ok(VectorValue::Bytes(output))
    }
    fn finish_int_cast(
        &mut self,
        id: &HostTaskId,
        scalar: ScalarValueRef<'_>,
        len: Option<u32>,
    ) -> LocalResult<VectorValue> {
        use tidb_query_datatype::codec::native_sql_string::{
            native_sql_string, NativeSqlStringInput,
        };
        use tidb_query_expr::{
            NativeCastStringError, NativeCastStringInput, NativeCastStringResult,
            NativeCastStringTarget,
        };
        let slot = self.task(id)?.slot;
        // Integer display is at most 20 bytes. Bound both temporary strings,
        // the output byte vector and its offsets before emitting any warning.
        self.check_bytes(
            self.retained_bytes()?
                .checked_add(256)
                .ok_or_else(|| resource("integer character cast retained size overflow"))?,
        )?;
        let Some(NativeHostMetadata::CharCast { charset, unsigned }) =
            self.metadata.get(slot.index())
        else {
            return Err(LocalError::HostContract(
                "character cast has no target metadata".into(),
            ));
        };
        let ScalarValueRef::Int(value) = scalar else {
            unreachable!("checked Int")
        };
        let value = if let Some(value) = value {
            let (input, display) = if *unsigned {
                (
                    NativeCastStringInput::UInt(*value as u64),
                    NativeSqlStringInput::UInt(*value as u64),
                )
            } else {
                (
                    NativeCastStringInput::Int(*value),
                    NativeSqlStringInput::Int(*value),
                )
            };
            let result = tidb_query_expr::native_cast_string(
                input,
                NativeCastStringTarget::Char {
                    len,
                    charset: Some(charset),
                },
                None,
                || self.context.connection_charset_info().0,
                || native_sql_string(display),
                || self.context.max_allowed_packet(),
                |name| self.context.handle_allowed_packet_overflowed(name),
                |code, message| self.context.append_warning(code, message),
            );
            match result {
                Ok(NativeCastStringResult::Text(value)) => Some(value.into_bytes()),
                Ok(NativeCastStringResult::Bytes(value)) => Some(value),
                Ok(NativeCastStringResult::Null) => None,
                Err(NativeCastStringError::Child(error)) => return self.native_failure(error),
                Err(NativeCastStringError::InvalidUtf8StringCoercion) => {
                    return self
                        .native_failure(EvalError::Unsupported("invalid UTF-8 string coercion"))
                }
            }
        } else {
            None
        };
        let mut output =
            ChunkedVecBytes::try_with_capacities(1, value.as_ref().map_or(0, Vec::len))
                .map_err(|error| LocalError::ResourceLimit(error.to_string()))?;
        output.push_ref(value.as_deref());
        self.check_bytes(
            self.retained_bytes()?
                .checked_add(
                    output
                        .retained_heap_bytes()
                        .ok_or_else(|| resource("character cast output size overflow"))?,
                )
                .and_then(|size| size.checked_add(value.as_ref().map_or(0, Vec::capacity)))
                .ok_or_else(|| resource("integer character cast result size overflow"))?,
        )?;
        self.slots[id.slot].task = None;
        Ok(VectorValue::Bytes(output))
    }
    fn native_failure<T>(&mut self, error: EvalError) -> LocalResult<T> {
        self.native_error = Some(error);
        Err(LocalError::HostContract(
            "native SQL context operation failed".into(),
        ))
    }
    fn finish_regexp(
        &mut self,
        task: &NativeTask,
        case_insensitive: bool,
    ) -> LocalResult<VectorValue> {
        let other = self.retained_bytes()?;
        let result = if task.values.iter().any(Option::is_none) {
            None
        } else {
            if task.values.len() != task.arity {
                return Err(LocalError::HostContract(
                    "regexp argument prefix is incomplete".into(),
                ));
            }
            let flags = task.values.get(2).and_then(Option::as_deref).unwrap_or(b"");
            let flags_len = flags
                .len()
                .checked_add(usize::from(case_insensitive))
                .ok_or_else(|| resource("regexp flags size overflow"))?;
            let prefix_bytes = other
                .checked_add(task.retained_bytes()?)
                .and_then(|size| size.checked_add(flags_len))
                .ok_or_else(|| resource("regexp prefix retained size overflow"))?;
            self.check_bytes(prefix_bytes)?;
            let mut prepared_flags = Vec::new();
            prepared_flags
                .try_reserve_exact(flags_len)
                .map_err(|error| LocalError::ResourceLimit(error.to_string()))?;
            if case_insensitive {
                prepared_flags.push(b'i');
            }
            prepared_flags.extend_from_slice(flags);
            let prefix_bytes = prefix_bytes
                .checked_add(prepared_flags.capacity().saturating_sub(flags_len))
                .ok_or_else(|| resource("regexp flags capacity overflow"))?;
            self.check_bytes(prefix_bytes)?;
            // Every demanded argument has already passed its original UTF-8 gate.
            let text = std::str::from_utf8(task.values[0].as_deref().expect("nonnull text"))
                .expect("checked UTF-8");
            let pattern = std::str::from_utf8(task.values[1].as_deref().expect("nonnull pattern"))
                .expect("checked UTF-8");
            let flags = std::str::from_utf8(&prepared_flags).expect("checked UTF-8 flags");
            let invocation = self
                .metadata
                .get(task.slot.index())
                .and_then(NativeHostMetadata::regexp)
                .ok_or_else(|| {
                    LocalError::HostContract("regexp slot has no native cache handles".into())
                })?;
            // Context lookup and compilation occur only after all non-NULL arguments.
            let prepared = invocation
                .with_context_id(self.context.context_id())
                .prepare_like(pattern, flags);
            self.check_bytes(
                prefix_bytes
                    .checked_add(
                        prepared
                            .known_cache_bytes()
                            .ok_or_else(|| resource("regexp cache retained size overflow"))?,
                    )
                    .ok_or_else(|| resource("regexp retained size overflow"))?,
            )?;
            match prepared.execute(text) {
                Ok(value) => Some(i64::from(value)),
                Err(error) => {
                    return self.native_failure(crate::regexp::native_regexp_error(&error))
                }
            }
        };
        self.check_bytes(
            other
                .checked_add(task.retained_bytes()?)
                .and_then(|size| size.checked_add(std::mem::size_of::<Option<i64>>()))
                .ok_or_else(|| resource("regexp output size overflow"))?,
        )?;
        Ok(VectorValue::from_scalar(&ScalarValue::Int(result), 1))
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
        let demand = match self.catalog.sql_batch_operation(invocation.slot)? {
            SqlBatchHostOperation::Concat => SqlDemand::Concat {
                kind: ConcatKind::Concat,
                demand: ConcatDemand::new(ConcatKind::Concat, invocation.arg_types.len())?,
            },
            SqlBatchHostOperation::ConcatWs => SqlDemand::Concat {
                kind: ConcatKind::ConcatWs,
                demand: ConcatDemand::new(ConcatKind::ConcatWs, invocation.arg_types.len())?,
            },
            SqlBatchHostOperation::CastCharNative { len } => {
                if self.native_values.is_none()
                    || !matches!(
                        self.metadata.get(invocation.slot.index()),
                        Some(NativeHostMetadata::CharNative { .. })
                    )
                {
                    return Err(LocalError::HostContract(
                        "native character cast lacks value/metadata capability".into(),
                    ));
                }
                SqlDemand::CastCharNative { len }
            }
            SqlBatchHostOperation::CastCharInt { len } => {
                if !matches!(
                    self.metadata.get(invocation.slot.index()),
                    Some(NativeHostMetadata::CharCast { .. })
                ) {
                    return Err(LocalError::HostContract(
                        "character cast has no target metadata".into(),
                    ));
                }
                SqlDemand::CastCharInt { len }
            }
            SqlBatchHostOperation::RegexpLike { case_insensitive } => {
                if self
                    .metadata
                    .get(invocation.slot.index())
                    .and_then(NativeHostMetadata::regexp)
                    .is_none()
                {
                    return Err(LocalError::HostContract(
                        "regexp slot has no native cache handles".into(),
                    ));
                }
                SqlDemand::RegexpLike {
                    case_insensitive,
                    next: Some(0),
                }
            }
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
        let task = NativeTask {
            slot: invocation.slot,
            row: invocation.row,
            arity: invocation.arg_types.len(),
            demand,
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
        if task.next_argument()? != Some(reply.index)
            || self
                .catalog
                .signature(task.slot)?
                .arg_types
                .get(reply.index)
                != Some(reply.field_type)
            || reply.values.len() != 1
            || reply.values.eval_type()
                != match &task.demand {
                    SqlDemand::CastCharInt { .. } => TikvEvalType::Int,
                    _ => TikvEvalType::Bytes,
                }
        {
            return Err(LocalError::HostContract(
                "SQL host reply differs from its demanded argument".into(),
            ));
        }
        if let SqlDemand::CastCharNative { len } = &task.demand {
            let ScalarValueRef::Bytes(Some(token)) = reply.values.get_scalar_ref(0) else {
                return Err(binding("native value handle cannot be wire NULL"));
            };
            return self
                .finish_native_cast(id, token, *len)
                .map(HostStep::Ready);
        }
        if let SqlDemand::CastCharInt { len } = &task.demand {
            return self
                .finish_int_cast(id, reply.values.get_scalar_ref(0), *len)
                .map(HostStep::Ready);
        }
        let ScalarValueRef::Bytes(bytes) = reply.values.get_scalar_ref(0) else {
            unreachable!("checked Bytes")
        };
        if matches!(&task.demand, SqlDemand::RegexpLike { .. })
            && bytes.is_some_and(|bytes| std::str::from_utf8(bytes).is_err())
        {
            return self
                .native_failure(EvalError::Unsupported("invalid UTF-8 REGEXP_LIKE argument"));
        }
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
            task.observe(bytes.map(<[u8]>::len))?
        };
        self.check_bytes(self.retained_bytes()?)?;
        if action == ConcatDemandAction::ReadPacketLimit {
            let limit = self.context.max_allowed_packet();
            let task = self.slots[id.slot]
                .task
                .as_mut()
                .expect("validated live task");
            let SqlDemand::Concat { kind, demand } = &mut task.demand else {
                unreachable!()
            };
            if demand.accept_packet_limit(limit)? {
                let name = match kind {
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
        match self.task(id)?.next_argument()? {
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
        let mut provider = NativeBatchHosts::new(catalog, &[], &crate::NoColumns, 512);
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

    #[test]
    fn integer_char_cast_resource_denial_precedes_native_warning() {
        struct Warnings(std::cell::Cell<usize>);
        impl Columns for Warnings {
            fn get(&self, _: &[String]) -> Option<Datum> {
                None
            }
            fn append_warning(&self, _: u16, _: &str) {
                self.0.set(self.0.get() + 1);
            }
        }
        let argument: tipb::FieldType = tidb_query_datatype::FieldTypeTp::LongLong.into();
        let output: tipb::FieldType = tidb_query_datatype::FieldTypeTp::VarString.into();
        let args = [argument.clone()];
        let catalog = HostCatalog::for_sql_batch(vec![(
            SqlBatchHostOperation::CastCharInt { len: Some(1) },
            HostSignature {
                arg_types: args.clone().into(),
                return_type: output.clone(),
            },
        )])
        .unwrap();
        let slot = catalog.slot(0).unwrap();
        let metadata = [NativeHostMetadata::CharCast {
            charset: "utf8mb4".into(),
            unsigned: false,
        }];
        let context = Warnings(std::cell::Cell::new(0));
        let mut provider = NativeBatchHosts::new(catalog, &metadata, &context, 256);
        let mut ctx = EvalContext::default();
        let HostStart::Pending { task, .. } = provider
            .start(
                &mut ctx,
                HostInvocation {
                    slot,
                    row: InputRow {
                        occurrence: 0,
                        input_row: 0,
                    },
                    arg_types: &args,
                    return_type: &output,
                },
            )
            .unwrap()
        else {
            panic!("expected argument request")
        };
        let value = VectorValue::from_scalar(&ScalarValue::Int(Some(12345)), 1);
        assert!(matches!(
            provider.resume(
                &mut ctx,
                &task,
                HostArgReply {
                    index: 0,
                    field_type: &argument,
                    values: &value,
                }
            ),
            Err(LocalError::ResourceLimit(_))
        ));
        assert_eq!(context.0.get(), 0);
        assert!(provider.take_native_error().is_none());
        provider.cancel(&task);
        assert!(provider.task(&task).is_err());
    }

    #[test]
    fn native_value_cast_warning_budget_and_forged_provenance_are_terminal() {
        struct Warnings(std::cell::Cell<usize>);
        impl Columns for Warnings {
            fn get(&self, _: &[String]) -> Option<Datum> {
                None
            }
            fn append_warning(&self, _: u16, _: &str) {
                self.0.set(self.0.get() + 1);
            }
        }
        let argument: tipb::FieldType = tidb_query_datatype::FieldTypeTp::Blob.into();
        let output: tipb::FieldType = tidb_query_datatype::FieldTypeTp::VarString.into();
        let args = [argument.clone()];
        let catalog = HostCatalog::for_sql_batch(vec![(
            SqlBatchHostOperation::CastCharNative { len: Some(1) },
            HostSignature {
                arg_types: args.clone().into(),
                return_type: output.clone(),
            },
        )])
        .unwrap();
        let slot = catalog.slot(0).unwrap();
        let metadata = [NativeHostMetadata::CharNative {
            charset: "gbk".into(),
            source_type: string_ty_for_test(),
            argument_node: 3,
        }];
        let values = RefCell::new(NativeValueStore::new(1024 * 1024).unwrap());
        values.borrow_mut().begin_window().unwrap();
        let row = InputRow {
            occurrence: 0,
            input_row: 0,
        };
        let token = values
            .borrow_mut()
            .insert_clone(
                &Datum::Bytes(vec![0xff; 100]),
                NativeValueRole::CharInput,
                NativeValueOrigin { node: 3, row },
            )
            .unwrap();
        let wrong = values
            .borrow_mut()
            .insert_clone(
                &Datum::Int(1),
                NativeValueRole::CharInput,
                NativeValueOrigin {
                    node: 3,
                    row: InputRow {
                        occurrence: 1,
                        input_row: 0,
                    },
                },
            )
            .unwrap();
        let context = Warnings(std::cell::Cell::new(0));
        let mut provider = NativeBatchHosts::new(catalog, &metadata, &context, 512)
            .with_native_values(Some(&values));
        let mut ctx = EvalContext::default();
        let invocation = || HostInvocation {
            slot,
            row,
            arg_types: &args,
            return_type: &output,
        };
        let HostStart::Pending { task, .. } = provider.start(&mut ctx, invocation()).unwrap()
        else {
            panic!("request")
        };
        let input = VectorValue::from_scalar(&ScalarValue::Bytes(Some(token)), 1);
        let failure = provider
            .resume(
                &mut ctx,
                &task,
                HostArgReply {
                    index: 0,
                    field_type: &argument,
                    values: &input,
                },
            )
            .unwrap_err();
        assert!(
            matches!(failure, LocalError::ResourceLimit(reason) if reason == "native warning capture policy exceeded")
        );
        assert_eq!(context.0.get(), 0);
        assert!(provider.take_native_error().is_none());
        provider.cancel(&task);
        let HostStart::Pending { task, .. } = provider.start(&mut ctx, invocation()).unwrap()
        else {
            panic!("request")
        };
        let input = VectorValue::from_scalar(&ScalarValue::Bytes(Some(wrong)), 1);
        assert!(matches!(
            provider.resume(
                &mut ctx,
                &task,
                HostArgReply {
                    index: 0,
                    field_type: &argument,
                    values: &input
                }
            ),
            Err(LocalError::BindingContract(_))
        ));
        assert_eq!(context.0.get(), 0);
        assert!(provider.take_native_error().is_none());
        provider.cancel(&task);
    }
    fn string_ty_for_test() -> FieldType {
        FieldType::new(FieldTypeCode::VarString).with_collation(tidb_datatype::Collation::Binary)
    }
}
