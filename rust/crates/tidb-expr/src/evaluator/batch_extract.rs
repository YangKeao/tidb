// Copyright 2026 PingCAP, Inc.
// Licensed under the Apache License, Version 2.0.
//! Original eager EXTRACT sources over detached authenticated actual values.
use super::*;
impl NativeBatchHosts<'_> {
    pub(super) fn resume_extract(
        &mut self,
        id: &HostTaskId,
        token: &[u8],
    ) -> LocalResult<HostStep> {
        let task = self.task(id)?;
        let SqlDemand::Extract { first } = task.demand else {
            return Err(binding("EXTRACT task differs"));
        };
        let Some(NativeHostMetadata::Extract {
            arguments,
            source_type,
            unit,
            result_type: _,
            output_node,
            output_role,
        }) = self.metadata.get(task.slot.index())
        else {
            return Err(binding("EXTRACT metadata missing"));
        };
        let row = task.row;
        let arity = arguments.len();
        let store = self
            .native_values
            .ok_or_else(|| binding("EXTRACT value store missing"))?;
        if arity == 2 && first.is_none() {
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
                .map_err(|_| binding("EXTRACT capability width differs"))?;
            let SqlDemand::Extract { first: saved } = &mut self.slots[id.slot]
                .task
                .as_mut()
                .expect("validated EXTRACT task")
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
        let token_for = |i: usize| {
            if arity == 1 || i == 1 {
                Ok(token)
            } else {
                first
                    .as_ref()
                    .map(|v| v.as_slice())
                    .ok_or_else(|| binding("EXTRACT first source lost"))
            }
        };
        let base = self.retained_bytes()?;
        let mut live = base;
        let null_unit = {
            let values = store.borrow();
            let null_unit = arity == 2
                && matches!(
                    values.get(
                        token_for(0)?,
                        NativeValueRole::CharInput,
                        NativeValueOrigin {
                            node: arguments[0],
                            row
                        }
                    )?,
                    Datum::Null
                );
            for i in 0..arity {
                let value = values.get(
                    token_for(i)?,
                    NativeValueRole::CharInput,
                    NativeValueOrigin {
                        node: arguments[i],
                        row,
                    },
                )?;
                // Every eager source remains authenticated. A NULL unit
                // suppresses all further value work, including detached copies.
                if null_unit {
                    continue;
                }
                live = live
                    .checked_add(
                        native_payload_bytes(value)?
                            .checked_mul(2)
                            .ok_or_else(|| resource("EXTRACT clone allowance overflow"))?,
                    )
                    .ok_or_else(|| resource("EXTRACT source extent overflow"))?;
            }
            null_unit
        };
        self.check_bytes(live)?;
        if null_unit {
            return self.finish_extract(id, Datum::Null, *output_node, *output_role, row, base);
        }
        let (owned, second) = {
            let values = store.borrow();
            let get = |i| {
                values.clone_for_kernel(
                    token_for(i)?,
                    NativeValueRole::CharInput,
                    NativeValueOrigin {
                        node: arguments[i],
                        row,
                    },
                )
            };
            (get(0)?, if arity == 2 { Some(get(1)?) } else { None })
        };
        let remaining = self.max_retained_bytes - live;
        // No value-store borrow survives original casts, independently demanded
        // modes, clock/zone/truncation reads or unconditional warning delivery.
        let value = match unit {
            Some(unit) => crate::tikv::eval_extract_composite_values_bounded(
                self.context,
                unit,
                std::slice::from_ref(&owned),
                remaining,
            ),
            None => crate::tikv::eval_extract_scalar_values_bounded(
                self.context,
                &owned,
                second
                    .as_ref()
                    .ok_or_else(|| binding("EXTRACT value source missing"))?,
                Some(source_type),
                remaining,
            ),
        };
        drop(owned);
        drop(second);
        let value = match value {
            Ok(v) => v,
            Err(e) => return self.native_failure(e),
        };
        self.finish_extract(id, value, *output_node, *output_role, row, base)
    }
    fn finish_extract(
        &mut self,
        id: &HostTaskId,
        value: Datum,
        output_node: usize,
        output_role: Option<NativeValueRole>,
        row: InputRow,
        base: usize,
    ) -> LocalResult<HostStep> {
        if let Some(role) = output_role {
            return self
                .finish_native_value(id, value, output_node, role, row, base)
                .map(HostStep::Ready);
        }
        self.check_bytes(
            base.checked_add(128)
                .ok_or_else(|| resource("EXTRACT integer publication overflow"))?,
        )?;
        let value = match value {
            Datum::Null => None,
            Datum::Int(v) => Some(v),
            _ => return Err(binding("EXTRACT result differs from integer carrier")),
        };
        let output = VectorValue::from_scalar(&ScalarValue::Int(value), 1);
        self.slots[id.slot].task = None;
        Ok(HostStep::Ready(output))
    }
}
