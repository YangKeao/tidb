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

//! Both actual sources, original argument casts, then statement SafeTS demand.
use super::*;
impl NativeBatchHosts<'_> {
    pub(super) fn resume_bounded_staleness(
        &mut self,
        id: &HostTaskId,
        token: &[u8],
    ) -> LocalResult<HostStep> {
        let task = self.task(id)?;
        let SqlDemand::BoundedStaleness { first } = task.demand else {
            return Err(binding("bounded task differs"));
        };
        let Some(NativeHostMetadata::BoundedStaleness {
            arguments,
            argument_types,
            result_type,
            output_node,
            output_role,
        }) = self.metadata.get(task.slot.index())
        else {
            return Err(binding("bounded metadata missing"));
        };
        let row = task.row;
        let store = self
            .native_values
            .ok_or_else(|| binding("bounded value store missing"))?;
        if first.is_none() {
            store.borrow().get(
                token,
                NativeValueRole::CharInput,
                NativeValueOrigin {
                    node: arguments[0],
                    row,
                },
            )?;
            let first = token
                .try_into()
                .map_err(|_| binding("bounded capability width differs"))?;
            let SqlDemand::BoundedStaleness { first: saved } = &mut self.slots[id.slot]
                .task
                .as_mut()
                .expect("validated bounded task")
                .demand
            else {
                unreachable!()
            };
            *saved = Some(first);
            return Ok(HostStep::NeedArg(HostArgRequest {
                index: 1,
                mode: ArgMode::Fresh,
            }));
        }
        let tokens = [
            first.as_ref().expect("retained bounded source").as_slice(),
            token,
        ];
        let base = self.retained_bytes()?;
        let mut live = base;
        {
            let values = store.borrow();
            for i in 0..2 {
                let value = values.get(
                    tokens[i],
                    NativeValueRole::CharInput,
                    NativeValueOrigin {
                        node: arguments[i],
                        row,
                    },
                )?;
                live = live
                    .checked_add(
                        native_payload_bytes(value)?
                            .checked_mul(2)
                            .ok_or_else(|| resource("bounded clone allowance overflow"))?,
                    )
                    .ok_or_else(|| resource("bounded source extent overflow"))?;
            }
        }
        self.check_bytes(live)?;
        let mut owned = {
            let values = store.borrow();
            let get = |i: usize| {
                values.clone_for_kernel(
                    tokens[i],
                    NativeValueRole::CharInput,
                    NativeValueOrigin {
                        node: arguments[i],
                        row,
                    },
                )
            };
            [get(0)?, get(1)?]
        };
        // All heap ownership is bounded and detached before any original cast,
        // warning or SafeTS callback can access the store or statement context.
        let remaining = self.max_retained_bytes - live;
        if let Err(error) = crate::tikv::bounded_staleness_cast_arguments(
            self.context,
            &mut owned,
            argument_types,
            remaining,
        ) {
            return self.native_failure(error);
        }
        let value = crate::tikv::bounded_staleness_bounded(self.context, &owned, remaining);
        drop(owned);
        let value = match value {
            Ok(value) => value,
            Err(error) => return self.native_failure(error),
        };
        let value = match crate::scalar_function::coerce_function_result(
            "tidb_bounded_staleness",
            Some(result_type),
            value,
        ) {
            Ok(value) => value,
            Err(error) => return self.native_failure(error),
        };
        if let Some(role) = output_role {
            return self
                .finish_native_value(id, value, *output_node, *role, row, base)
                .map(HostStep::Ready);
        }
        self.check_bytes(
            base.checked_add(128)
                .ok_or_else(|| resource("bounded time publication overflow"))?,
        )?;
        let value = match value {
            Datum::Null => None,
            Datum::Time(value) => Some(TikvDateTime::from_go_raw_like_go(value.go_raw())),
            _ => return Err(binding("bounded result differs from temporal carrier")),
        };
        let output = VectorValue::from_scalar(&ScalarValue::DateTime(value), 1);
        self.slots[id.slot].task = None;
        Ok(HostStep::Ready(output))
    }
}
