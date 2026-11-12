// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! WEIGHT_STRING phases inside the existing EagerValuesNative SDK frame.
use super::*;
use crate::scalar_function::weight_value;

pub(in super::super) struct NativeWeightString {
    pub arguments: Box<[usize]>,
    pub logical_arity: usize,
    pub source_type: Option<FieldType>,
    pub explicit_collation: bool,
    pub result_type: Option<FieldType>,
    pub output_node: usize,
    pub output_role: NativeValueRole,
}
#[derive(Clone, Copy)]
pub(super) enum WeightTask {
    Kind,
    Length([u8; native_values::HANDLE_BYTES]),
    Value(Option<(bool, i64)>),
    Numeric,
    Consumed,
}
impl WeightTask {
    pub(super) fn next_argument(self) -> Option<usize> {
        match self {
            Self::Kind => Some(1),
            Self::Length(_) => Some(2),
            Self::Value(_) => Some(0),
            Self::Numeric | Self::Consumed => None,
        }
    }
}
impl NativeBatchHosts<'_> {
    pub(super) fn resume_weight_string(
        &mut self,
        id: &HostTaskId,
        token: Option<&[u8]>,
    ) -> LocalResult<HostStep> {
        let task = self.task(id)?;
        let SqlDemand::WeightString(state) = task.demand else {
            return Err(binding("weight phase differs"));
        };
        let (slot, row) = (task.slot, task.row);
        let Some(NativeHostMetadata::WeightString(meta)) = self.metadata.get(slot.index()) else {
            return Err(binding("weight metadata absent"));
        };
        let store = self
            .native_values
            .ok_or_else(|| binding("weight store absent"))?;
        let origin = |index| NativeValueOrigin {
            node: meta.arguments[index],
            row,
        };
        let index = state.next_argument();
        if index.is_some() != token.is_some() || matches!(state, WeightTask::Consumed) {
            return Err(binding("weight reply phase differs"));
        }
        if let (Some(index), Some(token)) = (index, token) {
            store
                .borrow()
                .get(token, NativeValueRole::CharInput, origin(index))?;
        }
        if let WeightTask::Kind = state {
            let kind = token
                .expect("kind reply")
                .try_into()
                .map_err(|_| binding("weight kind width differs"))?;
            self.slots[id.slot]
                .task
                .as_mut()
                .expect("authenticated weight task")
                .demand = SqlDemand::WeightString(WeightTask::Length(kind));
            return Ok(HostStep::NeedArg(HostArgRequest {
                index: 2,
                mode: ArgMode::Fresh,
            }));
        }

        {
            let values = store.borrow();
            if let (Some(index), Some(token)) = (index, token) {
                values.get(token, NativeValueRole::CharInput, origin(index))?;
            }
            if let WeightTask::Length(kind) = state {
                values.get(&kind, NativeValueRole::CharInput, origin(1))?;
            }
        }

        self.slots[id.slot]
            .task
            .as_mut()
            .expect("authenticated weight task")
            .demand = SqlDemand::WeightString(WeightTask::Consumed);
        let check_tokens = || -> LocalResult<()> {
            if let (Some(index), Some(token)) = (index, token) {
                store
                    .borrow()
                    .get(token, NativeValueRole::CharInput, origin(index))?;
            }
            if let WeightTask::Length(kind) = state {
                store
                    .borrow()
                    .get(&kind, NativeValueRole::CharInput, origin(1))?;
            }
            Ok(())
        };

        let padding = if let WeightTask::Length(kind) = state {
            let (kind, length) = {
                let values = store.borrow();
                (
                    values.clone_for_kernel(&kind, NativeValueRole::CharInput, origin(1))?,
                    values.clone_for_kernel(
                        token.expect("length reply"),
                        NativeValueRole::CharInput,
                        origin(2),
                    )?,
                )
            };
            let result = (|| weight_value::padding(&kind, &length))();
            check_tokens()?;
            match result {
                Ok(padding) => Some(padding),
                Err(e) => return self.native_failure(e),
            }
        } else if let WeightTask::Value(padding) = state {
            padding
        } else {
            None
        };
        let code = weight_value::numeric_code(meta.source_type.as_ref(), padding);
        if matches!(state, WeightTask::Length(_)) && code.is_none() {
            self.slots[id.slot]
                .task
                .as_mut()
                .expect("live weight task")
                .demand = SqlDemand::WeightString(WeightTask::Value(padding));
            return Ok(HostStep::NeedArg(HostArgRequest {
                index: 0,
                mode: ArgMode::Fresh,
            }));
        }
        let result = if let Some(code) = code {
            crate::string_packet::weight_string_numeric_type(code, self.context)
        } else {
            let WeightTask::Value(_) = state else {
                return Err(binding("numeric weight lacks genuine numeric source"));
            };
            let value = store.borrow().clone_for_kernel(
                token.expect("value reply"),
                NativeValueRole::CharInput,
                origin(0),
            )?;
            (|| {
                weight_value::evaluate(
                    value,
                    &weight_value::source_type(meta.source_type.as_ref()),
                    meta.explicit_collation,
                    padding,
                    self.context,
                )
            })()
        };
        check_tokens()?;
        let value = match result {
            Ok(value) => value,
            Err(e) => return self.native_failure(e),
        };

        let value = match crate::scalar_function::coerce_function_result(
            "weight_string",
            meta.result_type.as_ref(),
            value,
        ) {
            Ok(value) => value,
            Err(e) => return self.native_failure(e),
        };
        check_tokens()?;
        self.finish_native_value(id, value, meta.output_node, meta.output_role, row)
            .map(HostStep::Ready)
    }
}
