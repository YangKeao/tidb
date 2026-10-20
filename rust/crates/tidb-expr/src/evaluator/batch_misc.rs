// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Eager misc operands, followed by each original body conversion phase.
use super::*;
use tidb_query_expr::NativeMiscOperation as O;
impl NativeBatchHosts<'_> {
    pub(super) fn resume_misc(&mut self, id: &HostTaskId, token: &[u8]) -> LocalResult<HostStep> {
        let task = self.task(id)?;
        let SqlDemand::Misc { next, mut tokens } = task.demand else {
            return Err(binding("misc demand differs"));
        };
        let row = task.row;
        let Some(NativeHostMetadata::Misc {
            arguments,
            operation,
            result_type,
            output_node,
            output_role,
            ..
        }) = self.metadata.get(task.slot.index())
        else {
            return Err(binding("misc metadata absent"));
        };
        let arity = arguments.len();
        if next >= arity {
            return Err(binding("misc task consumed"));
        }
        let store = self
            .native_values
            .ok_or_else(|| binding("misc store absent"))?;
        let origin = |i: usize| NativeValueOrigin {
            node: arguments[i],
            row,
        };
        store
            .borrow()
            .get(token, NativeValueRole::CharInput, origin(next))?;
        tokens[next] = Some(
            token
                .try_into()
                .map_err(|_| binding("misc capability width differs"))?,
        );
        let SqlDemand::Misc {
            next: saved,
            tokens: saved_tokens,
        } = &mut self.slots[id.slot]
            .task
            .as_mut()
            .expect("validated misc task")
            .demand
        else {
            unreachable!()
        };
        // Close before ANY body coercion/callback/panic. All children remain eager.
        *saved = next + 1;
        *saved_tokens = tokens;
        if next + 1 < arity {
            return Ok(HostStep::NeedArg(HostArgRequest {
                index: next + 1,
                mode: ArgMode::Fresh,
            }));
        }
        let token_for = |i: usize| {
            tokens[i]
                .as_ref()
                .map(|v| v.as_slice())
                .ok_or_else(|| binding("misc capability lost"))
        };
        {
            let values = store.borrow();
            for i in 0..arity {
                values.get(token_for(i)?, NativeValueRole::CharInput, origin(i))?;
            }
        }
        let base = self.retained_bytes()?;
        let maximum = self
            .max_retained_bytes
            .checked_sub(base)
            .ok_or_else(|| resource("misc work allowance exhausted"))?;
        let result = match operation {
            O::TidbShard | O::VitessHash => {
                let borrowed = {
                    let values = store.borrow();
                    crate::tikv::misc_value::prepare_borrowed_hash(
                        values.get(token_for(0)?, NativeValueRole::CharInput, origin(0))?,
                        maximum,
                    )
                };
                let borrowed = match borrowed {
                    Ok(v) => v,
                    Err(e) => return self.native_failure(e),
                };
                if borrowed {
                    // Both original warning stages are empty for these actual
                    // kinds. Read the genuine zone in its original phase with
                    // NO store borrow, then reauthenticate before pure conversion.
                    let zone = self.context.time_zone();
                    let values = store.borrow();
                    crate::tikv::misc_value::hash_after_zone(
                        *operation,
                        values.get(token_for(0)?, NativeValueRole::CharInput, origin(0))?,
                        &zone,
                        maximum,
                    )
                } else {
                    let owned = {
                        let values = store.borrow();
                        crate::tikv::identity_actual::copy_bounded(
                            values.get(token_for(0)?, NativeValueRole::CharInput, origin(0))?,
                            maximum,
                        )
                    };
                    let owned = match owned {
                        Ok(v) => v,
                        Err(e) => return self.native_failure(e),
                    };
                    crate::tikv::misc_value::hash(*operation, &owned, self.context, maximum)
                }
            }
            O::BinToUuid => {
                let callback_flag = if arity == 2 {
                    let values = store.borrow();
                    matches!(
                        values.get(token_for(1)?, NativeValueRole::CharInput, origin(1))?,
                        Datum::String(_) | Datum::Bytes(_)
                    )
                } else {
                    false
                };
                let flag = if callback_flag {
                    let owned = {
                        let values = store.borrow();
                        crate::tikv::identity_actual::copy_bounded(
                            values.get(token_for(1)?, NativeValueRole::CharInput, origin(1))?,
                            maximum,
                        )
                    };
                    let owned = match owned {
                        Ok(v) => v,
                        Err(e) => return self.native_failure(e),
                    };
                    // Only string/bytes flags call Columns. Share actual string
                    // headers and detach demanded byte ownership before callbacks.
                    crate::tikv::misc_value::flag(*operation, Some(&owned), self.context, maximum)
                } else if arity == 2 {
                    let values = store.borrow();
                    crate::tikv::misc_value::flag(
                        *operation,
                        Some(values.get(token_for(1)?, NativeValueRole::CharInput, origin(1))?),
                        self.context,
                        maximum,
                    )
                } else {
                    crate::tikv::misc_value::flag(*operation, None, self.context, maximum)
                };
                let flag = match flag {
                    Ok(v) => v,
                    Err(e) => return self.native_failure(e),
                };
                // Reauthenticate the original payload after any flag callbacks.
                let values = store.borrow();
                crate::tikv::misc_value::binary(
                    values.get(token_for(0)?, NativeValueRole::CharInput, origin(0))?,
                    flag,
                    maximum,
                )
            }
            O::UuidToBin => {
                let parsed = {
                    let values = store.borrow();
                    crate::tikv::misc_value::parse(
                        values.get(token_for(0)?, NativeValueRole::CharInput, origin(0))?,
                        maximum,
                    )
                };
                match parsed {
                    Err(e) => Err(e),
                    Ok(None) => Ok(Datum::Null),
                    Ok(Some(parsed)) => {
                        let flag = if arity == 2 {
                            let values = store.borrow();
                            crate::tikv::misc_value::flag(
                                *operation,
                                Some(values.get(
                                    token_for(1)?,
                                    NativeValueRole::CharInput,
                                    origin(1),
                                )?),
                                self.context,
                                maximum,
                            )
                        } else {
                            crate::tikv::misc_value::flag(*operation, None, self.context, maximum)
                        };
                        flag.and_then(|flag| crate::tikv::misc_value::swap(&parsed, flag, maximum))
                    }
                }
            }
            _ => {
                let values = store.borrow();
                crate::tikv::misc_value::unary(
                    *operation,
                    values.get(token_for(0)?, NativeValueRole::CharInput, origin(0))?,
                    maximum,
                )
            }
        };
        let result = match result {
            Ok(v) => v,
            Err(e) => return self.native_failure(e),
        };
        let held = native_payload_bytes(&result)?
            .checked_mul(2)
            .and_then(|n| base.checked_add(n))
            .ok_or_else(|| resource("misc result extent overflow"))?;
        self.check_bytes(held)?;
        let result = match crate::tikv::coerce_result_bounded(
            operation.name(),
            result_type,
            result,
            self.max_retained_bytes - held,
        ) {
            Ok(v) => v,
            Err(e) => return self.native_failure(e),
        };
        self.finish_native_value(id, result, *output_node, *output_role, row, base)
            .map(HostStep::Ready)
    }
}
