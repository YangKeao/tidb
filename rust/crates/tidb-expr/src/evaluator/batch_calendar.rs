// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Calendar sources over original eager, authenticated native capabilities.
use super::*;
impl NativeBatchHosts<'_> {
    pub(super) fn resume_calendar(
        &mut self,
        id: &HostTaskId,
        token: &[u8],
    ) -> LocalResult<HostStep> {
        let task = self.task(id)?;
        let SqlDemand::Calendar { first } = task.demand else {
            return Err(binding("calendar task differs"));
        };
        let Some(NativeHostMetadata::Calendar {
            arguments,
            operation,
            source_type,
            result_type,
            output_node,
            output_role,
        }) = self.metadata.get(task.slot.index())
        else {
            return Err(binding("calendar metadata missing"));
        };
        let row = task.row;
        let arity = arguments.len();
        let store = self
            .native_values
            .ok_or_else(|| binding("calendar store missing"))?;
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
                .map_err(|_| binding("calendar capability width differs"))?;
            let SqlDemand::Calendar { first: saved } = &mut self.slots[id.slot]
                .task
                .as_mut()
                .expect("validated calendar task")
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
        let token_for = |i: usize| -> LocalResult<&[u8]> {
            if arity == 1 || i == 1 {
                Ok(token)
            } else {
                first
                    .as_ref()
                    .map(|v| v.as_slice())
                    .ok_or_else(|| binding("calendar first source lost"))
            }
        };
        // All eager source expressions have run before any operation/callback,
        // including a mode whose later conversion will be suppressed.
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

        let owned = if arity > 0 {
            let values = store.borrow();
            let origin = NativeValueOrigin {
                node: arguments[0],
                row,
            };
            values.get(token_for(0)?, NativeValueRole::CharInput, origin)?;

            Some(values.clone_for_kernel(token_for(0)?, NativeValueRole::CharInput, origin)?)
        } else {
            None
        };

        let remaining = usize::MAX;
        let converted = if operation.datetime_argument() {
            match crate::tikv::calendar_value::cast_source(
                *operation,
                owned
                    .as_ref()
                    .ok_or_else(|| binding("calendar source lost"))?,
                source_type,
                self.context,
                remaining,
            ) {
                Ok(v) => Some(v),
                Err(e) => return self.native_failure(e),
            }
        } else {
            None
        };
        // Eager session getter follows the original cast, with no store borrow.
        let default = if *operation == tidb_query_expr::NativeCalendarOperation::Week {
            self.context.default_week_format()
        } else {
            0
        };
        let value = converted.as_ref().unwrap_or(
            owned
                .as_ref()
                .ok_or_else(|| binding("calendar source lost"))?,
        );
        let head = crate::tikv::calendar_value::head(*operation, value, self.context, remaining);
        drop(converted);
        drop(owned);
        let result = match head {
            Err(e) => return self.native_failure(e),
            Ok(crate::tikv::calendar_value::Head::Value(v)) => v,
            Ok(crate::tikv::calendar_value::Head::Week(v)) => {
                let mode = if arity == 2 {
                    let values = store.borrow();
                    let value = values.get(
                        token_for(1)?,
                        NativeValueRole::CharInput,
                        NativeValueOrigin {
                            node: arguments[1],
                            row,
                        },
                    )?;
                    crate::tikv::calendar_value::mode(value, remaining)
                } else {
                    Ok(Some(default))
                };
                let mode = match mode {
                    Ok(v) => v,
                    Err(e) => return self.native_failure(e),
                };
                match crate::tikv::calendar_value::finish_week(v, mode, remaining) {
                    Ok(v) => v,
                    Err(e) => return self.native_failure(e),
                }
            }
        };

        let result = match crate::tikv::calendar_value::finish_source(
            *operation,
            result,
            result_type,
            self.context,
            usize::MAX,
        ) {
            Ok(v) => v,
            Err(e) => return self.native_failure(e),
        };
        self.finish_native_value(id, result, *output_node, *output_role, row)
            .map(HostStep::Ready)
    }
}
