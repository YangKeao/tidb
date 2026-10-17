// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Native checked-coefficient stage. Unsupported is a typed result, never
//! an exception or SQL NULL; scheduling stays in the canonical SDK frame.
use super::*;
use tidb_query_datatype::codec::mysql::decimal::{
    native_decimal_fast_binary, NativeDecimalBinaryOp,
};
use tidb_query_expr::{NativeDecimalFastOutcome as Outcome, NativeDecimalFastValue as FastValue};

fn operation(value: SqlBatchHostOperation) -> LocalResult<NativeDecimalBinaryOp> {
    match value {
        SqlBatchHostOperation::AddWindowNative => Ok(NativeDecimalBinaryOp::Add),
        SqlBatchHostOperation::SubtractWindowNative => Ok(NativeDecimalBinaryOp::Subtract),
        SqlBatchHostOperation::MultiplyWindowNative => Ok(NativeDecimalBinaryOp::Multiply),
        _ => Err(binding("fast decimal operation is not arithmetic")),
    }
}
impl NativeBatchHosts<'_> {
    pub(super) fn start_decimal_fast_window(
        &mut self,
        invocation: HostWindowInvocation<'_>,
    ) -> LocalResult<HostStart> {
        let signature = self.catalog.signature(invocation.slot)?;
        let actual = self.catalog.sql_batch_operation(invocation.slot)?;
        let Some(NativeHostMetadata::DecimalFast {
            operation: expected,
            argument_nodes,
            ..
        }) = self.metadata.get(invocation.slot.index())
        else {
            return Err(binding("fast decimal descriptor missing"));
        };
        let arity = if actual == SqlBatchHostOperation::CastDecimalWindowNative {
            1
        } else {
            operation(actual)?;
            2
        };
        if *expected != actual
            || signature.arg_types.as_ref() != invocation.arg_types
            || &signature.return_type != invocation.return_type
            || invocation.arg_types.len() != arity
            || argument_nodes.len() != arity
            || invocation.physical_rows.is_empty()
            || invocation
                .occurrence_start
                .checked_add(invocation.physical_rows.len() - 1)
                .is_none()
            || self.native_values.is_none()
        {
            return Err(binding("fast decimal invocation differs from descriptor"));
        }
        self.check_bytes(
            self.retained_bytes()?
                .checked_add(array_bytes::<usize>(invocation.physical_rows.len())?)
                .ok_or_else(|| resource("fast decimal row map allowance overflow"))?,
        )?;
        let mut rows = Vec::new();
        rows.try_reserve_exact(invocation.physical_rows.len())
            .map_err(|error| LocalError::ResourceLimit(error.to_string()))?;
        rows.extend_from_slice(invocation.physical_rows);
        self.install_task(
            invocation.slot,
            InputRow {
                occurrence: invocation.occurrence_start,
                input_row: rows[0],
            },
            2,
            SqlDemand::WindowDecimalFast {
                next: 0,
                rows,
                left: Vec::new(),
            },
        )
    }
    pub(super) fn resume_decimal_fast_window(
        &mut self,
        id: &HostTaskId,
        reply: HostWindowArgReply<'_>,
    ) -> LocalResult<HostStep> {
        let task = self.task(id)?;
        let SqlDemand::WindowDecimalFast { next, rows, .. } = &task.demand else {
            return Err(binding("fast decimal reply reached a different task"));
        };
        let (slot, start, index) = (task.slot, task.row.occurrence, *next);
        let arity = self.catalog.signature(slot)?.arg_types.len();
        if index >= arity
            || reply.index != index
            || reply.physical_rows != rows
            || reply.occurrence_start != start
            || reply.values.len() != rows.len()
            || reply.values.eval_type() != TikvEvalType::Bytes
            || self.catalog.signature(slot)?.arg_types.get(index) != Some(reply.field_type)
        {
            return Err(binding("fast decimal reply differs from demand"));
        }
        let Some(NativeHostMetadata::DecimalFast { argument_nodes, .. }) =
            self.metadata.get(slot.index())
        else {
            return Err(binding("fast decimal reply descriptor missing"));
        };
        let count = rows.len();
        self.check_bytes(
            self.retained_bytes()?
                .checked_add(array_bytes::<Option<FastValue>>(count)?)
                .ok_or_else(|| resource("fast decimal argument allowance overflow"))?,
        )?;
        let mut values = Vec::new();
        values
            .try_reserve_exact(count)
            .map_err(|error| LocalError::ResourceLimit(error.to_string()))?;
        self.check_bytes(
            self.retained_bytes()?
                .checked_add(array_bytes::<Option<FastValue>>(values.capacity())?)
                .ok_or_else(|| resource("fast decimal reserved argument allowance overflow"))?,
        )?;
        let store = self
            .native_values
            .ok_or_else(|| binding("fast decimal store missing"))?;
        // Every returned capability must join this argument vector, including
        // entries after an Unsupported result. A contract error must not be
        // mistaken for permission to start a different semantic phase.
        let mut unsupported = false;
        for (position, &input_row) in rows.iter().enumerate() {
            let ScalarValueRef::Bytes(Some(token)) = reply.values.get_scalar_ref(position) else {
                return Err(binding("fast decimal argument is not a capability"));
            };
            let origin = NativeValueOrigin {
                node: argument_nodes[index],
                row: InputRow {
                    occurrence: start + position,
                    input_row,
                },
            };
            unsupported |= matches!(
                store.borrow().decimal_fast(token, origin)?,
                Outcome::Unsupported
            );
        }
        if unsupported {
            drop(values);
            return self.publish_decimal_fast(id, None).map(HostStep::Ready);
        }
        for (position, &input_row) in rows.iter().enumerate() {
            let ScalarValueRef::Bytes(Some(token)) = reply.values.get_scalar_ref(position) else {
                unreachable!("validated capability shape")
            };
            let origin = NativeValueOrigin {
                node: argument_nodes[index],
                row: InputRow {
                    occurrence: start + position,
                    input_row,
                },
            };
            let observed = store.borrow().decimal_fast(token, origin)?.clone();
            let Outcome::Value(value) = observed else {
                return Err(binding("validated fast argument changed domain"));
            };
            values.push(value);
        }
        if arity == 1 {
            return self
                .publish_decimal_fast(id, Some(values))
                .map(HostStep::Ready);
        }
        if index == 0 {
            let SqlDemand::WindowDecimalFast { next, left, .. } = &mut self.slots[id.slot]
                .task
                .as_mut()
                .expect("checked task")
                .demand
            else {
                unreachable!()
            };
            *next = 1;
            *left = values;
            self.check_bytes(self.retained_bytes()?)?;
            return Ok(HostStep::NeedArg(HostArgRequest {
                index: 1,
                mode: ArgMode::Fresh,
            }));
        }
        let Some(NativeHostMetadata::DecimalFast {
            operation: actual, ..
        }) = self.metadata.get(slot.index())
        else {
            unreachable!()
        };
        let op = operation(*actual)?;
        let SqlDemand::WindowDecimalFast { left, .. } = &self.task(id)?.demand else {
            unreachable!()
        };
        if left.len() != values.len() {
            return Err(binding("fast decimal argument lengths differ"));
        }
        // Both entire operands have arrived. A checked-domain refusal stops
        // computation and propagates Unsupported to the parent argument stage.
        for position in 0..values.len() {
            values[position] = match (left[position], values[position]) {
                (Some(left), Some(right)) => match native_decimal_fast_binary(left, right, op) {
                    Some(value) => Some(value),
                    None => {
                        drop(values);
                        return self.publish_decimal_fast(id, None).map(HostStep::Ready);
                    }
                },
                _ => None,
            };
        }
        self.publish_decimal_fast(id, Some(values))
            .map(HostStep::Ready)
    }
    fn publish_decimal_fast(
        &mut self,
        id: &HostTaskId,
        values: Option<Vec<Option<FastValue>>>,
    ) -> LocalResult<VectorValue> {
        let task = self.task(id)?;
        let SqlDemand::WindowDecimalFast { rows, .. } = &task.demand else {
            return Err(binding("fast decimal output reached a different task"));
        };
        let Some(NativeHostMetadata::DecimalFast { output_node, .. }) =
            self.metadata.get(task.slot.index())
        else {
            return Err(binding("fast decimal output descriptor missing"));
        };
        let count = rows.len();
        if values.as_ref().is_some_and(|values| values.len() != count) {
            return Err(binding("fast decimal result length differs"));
        }
        let payload = count
            .checked_mul(native_values::HANDLE_BYTES)
            .ok_or_else(|| resource("fast decimal output bytes overflow"))?;
        let base = self
            .retained_bytes()?
            .checked_add(array_bytes::<Option<FastValue>>(
                values.as_ref().map_or(0, Vec::capacity),
            )?)
            .ok_or_else(|| resource("fast decimal result allowance overflow"))?;
        self.check_bytes(
            base.checked_add(array_bytes::<usize>(
                count
                    .checked_add(1)
                    .ok_or_else(|| resource("fast decimal offsets overflow"))?,
            )?)
            .and_then(|bytes| bytes.checked_add(payload))
            .and_then(|bytes| bytes.checked_add(count))
            .ok_or_else(|| resource("fast decimal result vector allowance overflow"))?,
        )?;
        let mut output = ChunkedVecBytes::try_with_capacities(count, payload)
            .map_err(|error| LocalError::ResourceLimit(error.to_string()))?;
        self.check_bytes(
            base.checked_add(
                output
                    .retained_heap_bytes()
                    .ok_or_else(|| resource("fast decimal output capacity overflow"))?,
            )
            .ok_or_else(|| resource("fast decimal output capacity total overflow"))?,
        )?;
        let store = self
            .native_values
            .ok_or_else(|| binding("fast decimal output store missing"))?;
        for (position, &input_row) in rows.iter().enumerate() {
            let value = values.as_ref().map_or(Outcome::Unsupported, |values| {
                Outcome::Value(values[position])
            });
            let token = store.borrow_mut().insert_decimal_fast(
                value,
                NativeValueOrigin {
                    node: *output_node,
                    row: InputRow {
                        occurrence: task.row.occurrence + position,
                        input_row,
                    },
                },
            )?;
            output.push_ref(Some(&token));
        }
        self.slots[id.slot].task = None;
        Ok(VectorValue::Bytes(output))
    }
}

#[cfg(test)]
#[path = "batch_decimal_fast_window_tests.rs"]
mod tests;
