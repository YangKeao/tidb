// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Original regexp value phases in the existing EagerValuesNative SDK frame.
use super::*;

pub(in super::super) struct NativeRegexpValues {
    pub arguments: Box<[usize]>,
    pub source_types: Box<[Option<FieldType>]>,
    pub operator: bool,
    pub protobuf_result: bool,
    pub integer_batch_result: bool,
    pub invocation: tidb_query_expr::NativeRegexpInvocation,
    pub collation: tidb_datatype::Collation,
    pub result_type: Option<FieldType>,
    pub output_node: usize,
    pub output_role: NativeValueRole,
}
pub(super) enum RegexpTask {
    First,
    RawPattern([u8; native_values::HANDLE_BYTES]),
    Pattern(String),
    Flags { text: String, pattern: String },
}
impl RegexpTask {
    pub(super) fn next_argument(&self) -> Option<usize> {
        match self {
            Self::First => Some(0),
            Self::RawPattern(_) | Self::Pattern(_) => Some(1),
            Self::Flags { .. } => Some(2),
        }
    }
    #[cfg(test)]
    pub(super) fn retained_bytes(&self) -> LocalResult<usize> {
        let bytes = match self {
            Self::Pattern(text) => text.capacity(),
            Self::Flags { text, pattern } => text
                .capacity()
                .checked_add(pattern.capacity())
                .ok_or_else(|| resource("regexp strings extent overflow"))?,
            _ => 0,
        };
        std::mem::size_of::<Self>()
            .checked_add(bytes)
            .ok_or_else(|| resource("regexp task extent overflow"))
    }
}
impl NativeBatchHosts<'_> {
    pub(super) fn resume_regexp_values(
        &mut self,
        id: &HostTaskId,
        token: &[u8],
    ) -> LocalResult<HostStep> {
        let task = self.task(id)?;
        let SqlDemand::RegexpValues(state) = &task.demand else {
            return Err(binding("regexp value phase differs"));
        };
        let index = state
            .next_argument()
            .ok_or_else(|| binding("regexp stage consumed"))?;
        let (slot, row) = (task.slot, task.row);
        let Some(NativeHostMetadata::RegexpValues(meta)) = self.metadata.get(slot.index()) else {
            return Err(binding("regexp value metadata absent"));
        };
        if index >= meta.arguments.len() {
            return Err(binding("regexp stage exceeds arity"));
        }
        let store = self
            .native_values
            .ok_or_else(|| binding("regexp value store absent"))?;
        let origin = |index| NativeValueOrigin {
            node: meta.arguments[index],
            row,
        };
        let raw_first = if let RegexpTask::RawPattern(first) = &**state {
            Some(*first)
        } else {
            None
        };
        let is_null = store
            .borrow()
            .get(token, NativeValueRole::CharInput, origin(index))?
            .is_null();
        if let Some(first) = raw_first {
            store
                .borrow()
                .get(&first, NativeValueRole::CharInput, origin(0))?;
        }
        if meta.operator && matches!(&**state, RegexpTask::First) && !is_null {
            let first = token
                .try_into()
                .map_err(|_| binding("regexp text capability width differs"))?;
            self.slots[id.slot]
                .task
                .as_mut()
                .expect("authenticated regexp task")
                .demand = SqlDemand::RegexpValues(Box::new(RegexpTask::RawPattern(first)));
            return Ok(HostStep::NeedArg(HostArgRequest {
                index: 1,
                mode: ArgMode::Fresh,
            }));
        }

        {
            let values = store.borrow();
            values.get(token, NativeValueRole::CharInput, origin(index))?;
            if let Some(first) = raw_first.filter(|_| !is_null) {
                values.get(&first, NativeValueRole::CharInput, origin(0))?;
            }
        }

        let demand = std::mem::replace(
            &mut self.slots[id.slot]
                .task
                .as_mut()
                .expect("authenticated regexp task")
                .demand,
            SqlDemand::Consumed,
        );
        let SqlDemand::RegexpValues(state) = demand else {
            unreachable!()
        };
        let check_tokens = || -> LocalResult<()> {
            store
                .borrow()
                .get(token, NativeValueRole::CharInput, origin(index))?;
            if let Some(first) = raw_first {
                store
                    .borrow()
                    .get(&first, NativeValueRole::CharInput, origin(0))?;
            }
            Ok(())
        };

        let value =
            store
                .borrow()
                .clone_for_kernel(token, NativeValueRole::CharInput, origin(index))?;
        let result = (|| {
            (|| -> std::result::Result<Option<(String, String, String)>, EvalError> {
                if meta.operator {
                    if value.is_null() {
                        return Ok(None);
                    }
                    let RegexpTask::RawPattern(first) = &*state else {
                        unreachable!()
                    };
                    let text = store
                        .borrow()
                        .clone_for_kernel(first, NativeValueRole::CharInput, origin(0))
                        .map_err(|error| {
                            EvalError::ExpressionRuntimeFailure(
                                crate::tikv::ExpressionRuntimeFailure::from_local_eval(error, None),
                            )
                        })?;
                    let (text, pattern) =
                        crate::scalar_function::regexp_value::operator_strings(&text, &value)?;
                    return Ok(Some((text, pattern, String::new())));
                }
                let result = crate::scalar_function::regexp_value::named_string(
                    value,
                    meta.source_types[index].as_ref(),
                    self.context,
                );
                result.map(|value| value.map(|value| (String::new(), String::new(), value)))
            })()
        })();
        check_tokens()?;
        let result = match result {
            Ok(result) => result,
            Err(error) => return self.native_failure(error),
        };
        let args = match result {
            None => crate::tikv::EvaluatedArgs::NullWitness(None),
            Some((text, pattern, value)) => {
                let (text, pattern, flags) = if meta.operator {
                    (text, pattern, value)
                } else {
                    match *state {
                        RegexpTask::First => {
                            self.slots[id.slot]
                                .task
                                .as_mut()
                                .expect("live regexp task")
                                .demand =
                                SqlDemand::RegexpValues(Box::new(RegexpTask::Pattern(value)));

                            return Ok(HostStep::NeedArg(HostArgRequest {
                                index: 1,
                                mode: ArgMode::Fresh,
                            }));
                        }
                        RegexpTask::Pattern(text) if meta.arguments.len() == 3 => {
                            self.slots[id.slot]
                                .task
                                .as_mut()
                                .expect("live regexp task")
                                .demand = SqlDemand::RegexpValues(Box::new(RegexpTask::Flags {
                                text,
                                pattern: value,
                            }));

                            return Ok(HostStep::NeedArg(HostArgRequest {
                                index: 2,
                                mode: ArgMode::Fresh,
                            }));
                        }
                        RegexpTask::Pattern(text) => (text, value, String::new()),
                        RegexpTask::Flags { text, pattern } => (text, pattern, value),
                        _ => return Err(binding("regexp string phase differs")),
                    }
                };
                let match_type =
                    crate::regexp::regexp_match_type_with_collation(&flags, meta.collation);
                let invocation = meta.invocation.with_context_id(self.context.context_id());
                // Every original context callback completed before cache lookup.
                check_tokens()?;
                crate::tikv::EvaluatedArgs::RegexpLike {
                    invocation,
                    text: text.into_bytes(),
                    pattern: pattern.into_bytes(),
                    match_type: match_type.into_bytes(),
                }
            }
        };
        let cache = self.slots[id.slot]
            .ready_preparation
            .as_ref()
            .map(|guard| guard.cache());
        let result = (|| crate::tikv::evaluate_closed_regexp_args(cache, args))();
        check_tokens()?;
        let value = match result {
            Ok(value) => value,
            Err(error) => return self.native_failure(error),
        };
        let value = if meta.protobuf_result {
            crate::scalar_function::interpret_integer_result(value, meta.result_type.as_ref())
        } else {
            value
        };

        let name = if meta.operator {
            "regexp"
        } else {
            "regexp_like"
        };
        let value = match coerce_function_result(name, meta.result_type.as_ref(), value) {
            Ok(value) => value,
            Err(error) => return self.native_failure(error),
        };
        let value = if meta.integer_batch_result {
            let result =
                present_integer_batch_value(value, meta.result_type.as_ref(), self.context);
            check_tokens()?;
            match result {
                Ok(value) => value,
                Err(error) => return self.native_failure(error),
            }
        } else {
            value
        };
        check_tokens()?;
        self.finish_native_value(id, value, meta.output_node, meta.output_role, row)
            .map(HostStep::Ready)
    }
}
