// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Eager authenticated sources followed by original demanded value conversions.
use super::*;
use crate::tikv::duration_construction_value as values;
use tidb_query_expr::NativeDurationConstructionOperation as O;
fn copy_value(
    store: &RefCell<native_values::NativeValueStore>,
    token: &[u8],
    origin: NativeValueOrigin,
    held: usize,
    maximum: usize,
) -> LocalResult<(Datum, usize)> {
    let store = store.borrow();
    let value = store.get(token, NativeValueRole::CharInput, origin)?;
    let live = native_payload_bytes(value)?
        .checked_mul(2)
        .and_then(|n| held.checked_add(n))
        .ok_or_else(|| resource("duration source clone extent overflow"))?;
    if live > maximum {
        return Err(resource("duration source clone allowance exhausted"));
    }
    Ok((
        store.clone_for_kernel(token, NativeValueRole::CharInput, origin)?,
        live,
    ))
}
impl NativeBatchHosts<'_> {
    pub(super) fn resume_duration_construction(
        &mut self,
        id: &HostTaskId,
        token: &[u8],
    ) -> LocalResult<HostStep> {
        let (slot, row, next, mut tokens) = {
            let task = self.task(id)?;
            let SqlDemand::DurationConstruction { next, tokens } = &task.demand else {
                return Err(binding("duration task differs"));
            };
            (task.slot, task.row, *next, *tokens)
        };
        let Some(NativeHostMetadata::DurationConstruction {
            arguments,
            argument_types,
            operation,
            result_type,
            output_node,
            output_role,
        }) = self.metadata.get(slot.index())
        else {
            return Err(binding("duration metadata missing"));
        };
        let arity = operation.arity();
        if next >= arity {
            return Err(binding("duration task already consumed all arguments"));
        }
        let store = self
            .native_values
            .ok_or_else(|| binding("duration store missing"))?;
        store.borrow().get(
            token,
            NativeValueRole::CharInput,
            NativeValueOrigin {
                node: arguments[next],
                row,
            },
        )?;
        tokens[next] = Some(
            token
                .try_into()
                .map_err(|_| binding("duration capability width differs"))?,
        );
        // Close demand before any fallible conversion or panic. A caught panic
        // cannot replay a partially converted invocation into a NULL success.
        let SqlDemand::DurationConstruction {
            next: saved_next,
            tokens: saved_tokens,
        } = &mut self.slots[id.slot]
            .task
            .as_mut()
            .expect("validated duration task")
            .demand
        else {
            unreachable!()
        };
        *saved_next = next + 1;
        *saved_tokens = tokens;
        if next + 1 < arity {
            return Ok(HostStep::NeedArg(HostArgRequest {
                index: next + 1,
                mode: ArgMode::Fresh,
            }));
        }
        let token_for = |index: usize| {
            tokens[index]
                .as_ref()
                .map(|v| v.as_slice())
                .ok_or_else(|| binding("duration capability lost"))
        };
        {
            let source = store.borrow();
            for index in 0..arity {
                source.get(
                    token_for(index)?,
                    NativeValueRole::CharInput,
                    NativeValueOrigin {
                        node: arguments[index],
                        row,
                    },
                )?;
            }
        }
        let base = self.retained_bytes()?;
        let maximum = self
            .max_retained_bytes
            .checked_sub(base)
            .ok_or_else(|| resource("duration work allowance exhausted"))?;
        let outcome = if *operation == O::GetFormat {
            let source = store.borrow();
            values::get_format_source(
                [
                    source.get(
                        token_for(0)?,
                        NativeValueRole::CharInput,
                        NativeValueOrigin {
                            node: arguments[0],
                            row,
                        },
                    )?,
                    source.get(
                        token_for(1)?,
                        NativeValueRole::CharInput,
                        NativeValueOrigin {
                            node: arguments[1],
                            row,
                        },
                    )?,
                ],
                maximum,
            )
        } else if *operation == O::DateDiff {
            // Both original ETDatetime casts precede signature text conversion.
            // A failing left cast must not clone or convert the right payload.
            let mut converted = Vec::new();
            converted
                .try_reserve_exact(2)
                .map_err(|_| resource("date diff conversion allocation failed"))?;
            let mut held = base;
            let mut error = None;
            for index in 0..2 {
                let (original, live) = copy_value(
                    store,
                    token_for(index)?,
                    NativeValueOrigin {
                        node: arguments[index],
                        row,
                    },
                    held,
                    self.max_retained_bytes,
                )?;
                let cast = crate::tikv::calendar_value::cast_source(
                    tidb_query_expr::NativeCalendarOperation::Date,
                    &original,
                    &argument_types[index],
                    self.context,
                    self.max_retained_bytes - live,
                );
                drop(original);
                match cast {
                    Ok(v) => {
                        held = native_payload_bytes(&v)?
                            .checked_mul(2)
                            .and_then(|n| held.checked_add(n))
                            .ok_or_else(|| resource("date diff cast extent overflow"))?;
                        self.check_bytes(held)?;
                        converted.push(v);
                    }
                    Err(e) => {
                        error = Some(e);
                        break;
                    }
                }
            }
            match error {
                Some(e) => Err(e),
                None => values::eval_bounded(
                    *operation,
                    &converted,
                    self.context,
                    self.max_retained_bytes - held,
                ),
            }
        } else {
            let first = if values::start_calls_context(*operation) {
                let (original, live) = copy_value(
                    store,
                    token_for(0)?,
                    NativeValueOrigin {
                        node: arguments[0],
                        row,
                    },
                    base,
                    self.max_retained_bytes,
                )?;
                values::start(
                    *operation,
                    &original,
                    self.context,
                    self.max_retained_bytes - live,
                )
            } else {
                let source = store.borrow();
                let original = source.get(
                    token_for(0)?,
                    NativeValueRole::CharInput,
                    NativeValueOrigin {
                        node: arguments[0],
                        row,
                    },
                )?;
                values::start(*operation, original, self.context, maximum)
            };
            match first {
                Err(e) => Err(e),
                Ok(mut prepared) => loop {
                    let Some((index, callback)) = prepared.argument() else {
                        break prepared.into_value();
                    };
                    if index >= arity {
                        return Err(binding("duration value stage exceeds original arity"));
                    }
                    let held = base
                        .checked_add(prepared.retained_bytes())
                        .ok_or_else(|| resource("duration continuation extent overflow"))?;
                    self.check_bytes(held)?;
                    let next = if callback {
                        let (original, live) = copy_value(
                            store,
                            token_for(index)?,
                            NativeValueOrigin {
                                node: arguments[index],
                                row,
                            },
                            held,
                            self.max_retained_bytes,
                        )?;
                        prepared.advance(&original, self.context, self.max_retained_bytes - live)
                    } else {
                        let source = store.borrow();
                        let original = source.get(
                            token_for(index)?,
                            NativeValueRole::CharInput,
                            NativeValueOrigin {
                                node: arguments[index],
                                row,
                            },
                        )?;
                        prepared.advance(original, self.context, self.max_retained_bytes - held)
                    };
                    match next {
                        Ok(v) => prepared = v,
                        Err(e) => break Err(e),
                    }
                },
            }
        };
        let result = match outcome {
            Ok(v) => v,
            Err(e) => return self.native_failure(e),
        };
        let live = native_payload_bytes(&result)?
            .checked_mul(2)
            .and_then(|n| base.checked_add(n))
            .ok_or_else(|| resource("duration result extent overflow"))?;
        self.check_bytes(live)?;
        let result = match values::finish_source(
            *operation,
            result,
            result_type,
            self.context,
            self.max_retained_bytes - live,
        ) {
            Ok(v) => v,
            Err(e) => return self.native_failure(e),
        };
        self.finish_native_value(id, result, *output_node, *output_role, row, base)
            .map(HostStep::Ready)
    }
}
