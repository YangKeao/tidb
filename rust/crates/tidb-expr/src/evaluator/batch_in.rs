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

//! IN source demands in the same SDK driver, without expression callbacks.
use super::*;
use std::collections::HashSet;
use tidb_query_expr::{NativeInMembership, NativeInTypedDomain};

pub(in super::super) struct NativeInArgument {
    pub node: usize,
    pub field: Option<FieldType>,
    pub facts: crate::ops::OperandFacts,
}
pub(in super::super) struct NativeInMetadata {
    pub arguments: Box<[NativeInArgument]>,
    pub prepared: Option<HashSet<Vec<u8>>>,
    pub dynamic: Box<[usize]>,
    pub has_null: bool,
    pub collation: tidb_datatype::Collation,
    pub result_type: Option<FieldType>,
    pub output_node: usize,
    pub output_role: NativeValueRole,
}
impl NativeInMetadata {
    fn domain(&self) -> Option<NativeInTypedDomain> {
        Some(match self.arguments[0].field.as_ref()?.eval_type() {
            NativeEvalType::Datetime => NativeInTypedDomain::Datetime,
            NativeEvalType::Timestamp => NativeInTypedDomain::Timestamp,
            NativeEvalType::Duration => NativeInTypedDomain::Duration,
            NativeEvalType::Json => NativeInTypedDomain::Json,
            _ => return None,
        })
    }
    fn following(&self, current: usize) -> Option<usize> {
        if self.domain().is_some() || self.prepared.is_none() {
            (current + 1 < self.arguments.len()).then_some(current + 1)
        } else if current == 0 {
            self.dynamic.first().copied()
        } else {
            self.dynamic
                .binary_search(&current)
                .ok()
                .and_then(|position| self.dynamic.get(position + 1).copied())
        }
    }
}
impl NativeBatchHosts<'_> {
    pub(super) fn validate_in_start(&self, invocation: &HostInvocation<'_>) -> LocalResult<()> {
        let Some(NativeHostMetadata::In(metadata)) = self.metadata.get(invocation.slot.index())
        else {
            return Err(binding("native IN descriptor missing"));
        };
        if self.native_values.is_none()
            || metadata.arguments.len() != invocation.arg_types.len()
            || metadata.arguments.len() < 2
            || metadata
                .dynamic
                .iter()
                .any(|&i| i == 0 || i >= metadata.arguments.len())
            || metadata.dynamic.windows(2).any(|pair| pair[0] >= pair[1])
        {
            return Err(binding("native IN source descriptor differs"));
        }
        Ok(())
    }
    pub(super) fn resume_in(&mut self, id: &HostTaskId, token: &[u8]) -> LocalResult<HostStep> {
        let task = self.task(id)?;
        let SqlDemand::In {
            next,
            tokens,
            membership,
        } = &task.demand
        else {
            return Err(binding("IN resume has another task kind"));
        };
        let index = *next;
        let state = *membership;
        let left_token = tokens.first().copied();
        let row = task.row;
        let Some(NativeHostMetadata::In(metadata)) = self.metadata.get(task.slot.index()) else {
            return Err(binding("IN source descriptor vanished"));
        };
        let store = self
            .native_values
            .ok_or_else(|| binding("IN store absent"))?;
        let origin = NativeValueOrigin {
            node: metadata.arguments[index].node,
            row,
        };
        // Authenticate every reply, even an exhaustive comparison whose NULL or
        // match state is already known. A wire NULL never represents SQL NULL.
        store
            .borrow()
            .get(token, NativeValueRole::CharInput, origin)?;
        let typed = metadata.domain().is_some();
        let mut state = state;
        if index == 0 || typed {
            let handle = token
                .try_into()
                .map_err(|_| binding("IN token width differs"))?;
            let SqlDemand::In { tokens, .. } = &mut self.slots[id.slot]
                .task
                .as_mut()
                .expect("validated IN task")
                .demand
            else {
                unreachable!()
            };
            tokens
                .try_reserve_exact(1)
                .map_err(|error| LocalError::ResourceLimit(error.to_string()))?;
            tokens.push(handle);
        }
        if !typed && index == 0 {
            store
                .borrow()
                .get(token, NativeValueRole::CharInput, origin)?;

            let value =
                store
                    .borrow()
                    .clone_for_kernel(token, NativeValueRole::CharInput, origin)?;
            let mut actual =
                NativeInMembership::new(crate::tikv::in_eq_observation(&value), metadata.has_null);
            if let Some(keys) = &metadata.prepared {
                let bytes = match crate::coerce::coerce_str_bytes(&value) {
                    Ok(value) => value,
                    Err(error) => return self.native_failure(error),
                };
                if let Some(bytes) = bytes {
                    // Resolve after actual coercion, under the current global
                    // mode. Stored keys are never regenerated for that mode.
                    let collation = tidb_datatype::get_collator(metadata.collation.name())
                        .new_collation()
                        .map_or(
                            tidb_datatype::Collation::Binary.native_policy(),
                            tidb_datatype::Collation::native_policy,
                        );
                    actual.probe_prepared(keys, &bytes, collation);
                }
            }
            state = Some(actual);
            // Probe owners are dropped before any dynamic source request.
        } else if !typed {
            let left_token = left_token.ok_or_else(|| binding("IN left capability absent"))?;
            let left_origin = NativeValueOrigin {
                node: metadata.arguments[0].node,
                row,
            };

            {
                let values = store.borrow();
                values.get(&left_token, NativeValueRole::CharInput, left_origin)?;
                values.get(token, NativeValueRole::CharInput, origin)?;
            }

            let (left, right) = {
                let store = store.borrow();
                (
                    store.clone_for_kernel(&left_token, NativeValueRole::CharInput, left_origin)?,
                    store.clone_for_kernel(token, NativeValueRole::CharInput, origin)?,
                )
            };
            // Original generic IN reads this even for NULL and already-matched
            // values. It uses generic comparison, without a typed-first probe.
            let _ = self.context.div_precision_increment();
            let prepared = match crate::ops::prepare_in_comparison(
                left,
                right,
                metadata.collation,
                [metadata.arguments[0].facts, metadata.arguments[index].facts],
                self.context,
            ) {
                Ok(value) => value,
                Err(error) => return self.native_failure(error),
            };
            let compared = prepared.compute(usize::MAX)?;
            let mut actual = state.ok_or_else(|| binding("IN membership state absent"))?;
            actual.observe(compared.map_or(
                tidb_query_expr::NativeInControlValue::Null,
                tidb_query_expr::NativeInControlValue::Int,
            ));
            state = Some(actual);
            // Intentionally ignore match: all original dynamic/generic sources
            // are mandatory, even after a match or a NULL left value.
        }
        if let Some(next) = metadata.following(index) {
            let SqlDemand::In {
                next: cursor,
                membership,
                ..
            } = &mut self.slots[id.slot]
                .task
                .as_mut()
                .expect("validated IN task")
                .demand
            else {
                unreachable!()
            };
            *cursor = next;
            *membership = state;
            return Ok(HostStep::NeedArg(HostArgRequest {
                index: next,
                mode: ArgMode::Fresh,
            }));
        }
        let result = if let Some(domain) = metadata.domain() {
            self.finish_typed_in(id, domain)?
        } else {
            crate::tikv::in_control_datum(
                state
                    .ok_or_else(|| binding("IN final state absent"))?
                    .finish(),
            )
        };
        // Match ScalarFunction.Eval and the enclosing Expression.Eval output
        // publication; the latter runs in finish_native_value.
        let result = match crate::scalar_function::coerce_function_result(
            "in",
            metadata.result_type.as_ref(),
            result,
        ) {
            Ok(value) => value,
            Err(error) => return self.native_failure(error),
        };
        self.finish_native_value(id, result, metadata.output_node, metadata.output_role, row)
            .map(HostStep::Ready)
    }
    fn finish_typed_in(
        &mut self,
        id: &HostTaskId,
        domain: NativeInTypedDomain,
    ) -> LocalResult<Datum> {
        let task = self.task(id)?;
        let SqlDemand::In { tokens, .. } = &task.demand else {
            return Err(binding("typed IN task missing"));
        };
        let Some(NativeHostMetadata::In(metadata)) = self.metadata.get(task.slot.index()) else {
            return Err(binding("typed IN metadata absent"));
        };
        if tokens.len() != metadata.arguments.len() {
            return Err(binding("typed IN incomplete source barrier"));
        }
        let store = self
            .native_values
            .ok_or_else(|| binding("typed IN store absent"))?;

        let mut values = Vec::new();
        values
            .try_reserve_exact(tokens.len())
            .map_err(|error| LocalError::ResourceLimit(error.to_string()))?;
        for (token, argument) in tokens.iter().zip(&metadata.arguments) {
            let origin = NativeValueOrigin {
                node: argument.node,
                row: task.row,
            };
            values.push(store.borrow().clone_for_kernel(
                token,
                NativeValueRole::CharInput,
                origin,
            )?);
        }
        // Every real source is now available. Only now perform ALL casts in
        // order, even if the first is NULL or an earlier value would match.
        for (index, (value, argument)) in values.iter_mut().zip(&metadata.arguments).enumerate() {
            let converted = match domain {
                NativeInTypedDomain::Datetime | NativeInTypedDomain::Timestamp => {
                    crate::cast::cast_arg_as_datetime(value, argument.field.as_ref(), self.context)
                }
                NativeInTypedDomain::Duration => {
                    crate::cast::cast_arg_as_duration(value, argument.field.as_ref(), self.context)
                }
                NativeInTypedDomain::Json if index == 0 => {
                    crate::builtin_ext::cast_as_json_typed(value, argument.field.as_ref())
                }
                NativeInTypedDomain::Json => {
                    crate::builtin_ext::cast_as_json_value_typed(value, argument.field.as_ref())
                }
            };
            let converted = match converted {
                Ok(value) => value,
                Err(error) => return self.native_failure(error),
            };

            *value = converted;
        }
        match crate::tikv::eval_in_typed_values_in(self.context, domain, &values) {
            Ok(value) => Ok(value),
            Err(error) => self.native_failure(error),
        }
    }
}
