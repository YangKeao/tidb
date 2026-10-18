// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Timestamp sources over original eager, authenticated native capabilities.
use super::*;
impl NativeBatchHosts<'_> {
    pub(super) fn resume_epoch(
        &mut self,
        id: &HostTaskId,
        token: Option<&[u8]>,
    ) -> LocalResult<HostStep> {
        let task = self.task(id)?;
        let SqlDemand::Epoch { first } = task.demand else {
            return Err(binding("epoch task differs"));
        };
        let Some(NativeHostMetadata::Epoch {
            arguments,
            from,
            result_type,
            output_node,
            output_role,
        }) = self.metadata.get(task.slot.index())
        else {
            return Err(binding("epoch metadata missing"));
        };
        let row = task.row;
        let arity = arguments.len();
        let store = self
            .native_values
            .ok_or_else(|| binding("epoch store missing"))?;
        if arity == 2 && first.is_none() {
            let token = token.ok_or_else(|| binding("epoch first reply missing"))?;
            store.borrow().get(
                token,
                NativeValueRole::CharInput,
                NativeValueOrigin {
                    node: arguments[0],
                    row,
                },
            )?;
            let token = token
                .try_into()
                .map_err(|_| binding("epoch capability width differs"))?;
            let SqlDemand::Epoch { first: saved } = &mut self.slots[id.slot]
                .task
                .as_mut()
                .expect("validated epoch task")
                .demand
            else {
                unreachable!()
            };
            *saved = Some(token);
            return Ok(HostStep::NeedArg(HostArgRequest {
                index: 1,
                mode: ArgMode::Fresh,
            }));
        }
        if token.is_some() != (arity > 0) {
            return Err(binding("epoch source presence differs"));
        }
        let token_for = |i: usize| -> LocalResult<&[u8]> {
            if arity == 1 || i == 1 {
                token.ok_or_else(|| binding("epoch current source missing"))
            } else {
                first
                    .as_ref()
                    .map(|v| v.as_slice())
                    .ok_or_else(|| binding("epoch first source lost"))
            }
        };
        // All eager source expressions have run before any operation/callback,
        // including a format whose later conversion will be suppressed.
        {
            let values = store.borrow();
            for i in 0..arity {
                values.get(
                    token_for(i)?,
                    NativeValueRole::CharInput,
                    NativeValueOrigin {
                        node: arguments[i],
                        row,
                    },
                )?;
            }
        }
        let base = self.retained_bytes()?;
        let owned = if arity > 0 {
            let values = store.borrow();
            let origin = NativeValueOrigin {
                node: arguments[0],
                row,
            };
            let value = values.get(token_for(0)?, NativeValueRole::CharInput, origin)?;
            let charge = native_payload_bytes(value)?
                .checked_mul(2)
                .and_then(|n| base.checked_add(n))
                .ok_or_else(|| resource("epoch first clone extent overflow"))?;
            self.check_bytes(charge)?;
            Some((
                values.clone_for_kernel(token_for(0)?, NativeValueRole::CharInput, origin)?,
                charge,
            ))
        } else {
            None
        };
        let live = owned.as_ref().map_or(base, |(_, n)| *n);
        let remaining = self
            .max_retained_bytes
            .checked_sub(live)
            .ok_or_else(|| resource("epoch work allowance exhausted"))?;
        let result = if *from {
            crate::tikv::eval_from_unixtime_head_values_bounded(
                self.context,
                &owned
                    .as_ref()
                    .ok_or_else(|| binding("epoch conversion source lost"))?
                    .0,
                remaining,
            )
        } else {
            crate::tikv::eval_unix_timestamp_values_bounded(
                self.context,
                owned
                    .as_ref()
                    .map_or(&[] as &[Datum], |(v, _)| std::slice::from_ref(v)),
                remaining,
            )
        };
        drop(owned);
        let mut result = match result {
            Ok(v) => v,
            Err(e) => return self.native_failure(e),
        };
        if *from && arity == 2 && !result.is_null() {
            // The frame owns the source window throughout callbacks. Re-check
            // its original capability when the format is actually consumed;
            // no store borrow spans warning, zone, clock or truncate callbacks.
            let local = native_payload_bytes(&result)?
                .checked_mul(2)
                .and_then(|n| base.checked_add(n))
                .ok_or_else(|| resource("epoch local extent overflow"))?;
            let (format, live) = {
                let values = store.borrow();
                let origin = NativeValueOrigin {
                    node: arguments[1],
                    row,
                };
                let value = values.get(token_for(1)?, NativeValueRole::CharInput, origin)?;
                let live = native_payload_bytes(value)?
                    .checked_mul(2)
                    .and_then(|n| local.checked_add(n))
                    .ok_or_else(|| resource("epoch format clone extent overflow"))?;
                self.check_bytes(live)?;
                (
                    values.clone_for_kernel(token_for(1)?, NativeValueRole::CharInput, origin)?,
                    live,
                )
            };
            let value = crate::tikv::eval_date_format_values_bounded(
                self.context,
                &result,
                &format,
                self.max_retained_bytes - live,
            );
            drop(format);
            drop(result);
            result = match value {
                Ok(v) => v,
                Err(e) => return self.native_failure(e),
            };
        }
        if *from {
            let live = native_payload_bytes(&result)?
                .checked_mul(2)
                .and_then(|n| base.checked_add(n))
                .ok_or_else(|| resource("epoch result extent overflow"))?;
            self.check_bytes(live)?;
            result = match crate::tikv::finish_from_unixtime_values_bounded(
                self.context,
                result,
                result_type,
                self.max_retained_bytes - live,
            ) {
                Ok(v) => v,
                Err(e) => return self.native_failure(e),
            };
        }
        let live = native_payload_bytes(&result)?
            .checked_mul(2)
            .and_then(|n| base.checked_add(n))
            .ok_or_else(|| resource("epoch return-family extent overflow"))?;
        self.check_bytes(live)?;
        result = match crate::tikv::finish_epoch_result_bounded(
            result,
            result_type,
            *from,
            self.max_retained_bytes - live,
        ) {
            Ok(v) => v,
            Err(e) => return self.native_failure(e),
        };
        self.finish_native_value(id, result, *output_node, *output_role, row, base)
            .map(HostStep::Ready)
    }
}
