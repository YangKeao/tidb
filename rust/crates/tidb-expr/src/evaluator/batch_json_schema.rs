// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Actual schema preparation, then lazy document demand in the same HostFrame.
use super::*;
use crate::builtin_ext::{JsonSchemaCache, PreparedSchemaValue};

pub(in super::super) struct NativeJsonSchema {
    pub arguments: Box<[usize]>,
    pub cacheable: bool,
    pub cache: JsonSchemaCache,
    pub result_type: FieldType,
    pub output_node: usize,
    pub output_role: NativeValueRole,
}
pub(super) enum SchemaTask {
    Schema,
    Document {
        schema: [u8; native_values::HANDLE_BYTES],
        prepared: PreparedSchemaValue,
    },
}
impl SchemaTask {
    pub(super) fn next_argument(&self) -> usize {
        match self {
            Self::Schema => 0,
            Self::Document { .. } => 1,
        }
    }
    pub(super) fn retained_bytes(&self) -> LocalResult<usize> {
        std::mem::size_of::<Self>()
            .checked_add(match self {
                Self::Schema => 0,
                Self::Document { prepared, .. } => prepared
                    .owned_payload_bytes()
                    .ok_or_else(|| resource("schema tree retained extent overflow"))?,
            })
            .ok_or_else(|| resource("schema task retained extent overflow"))
    }
}
enum Progress {
    Ready(Datum),
    More(SchemaTask),
}
impl NativeBatchHosts<'_> {
    pub(super) fn resume_json_schema(
        &mut self,
        id: &HostTaskId,
        token: Option<&[u8]>,
    ) -> LocalResult<HostStep> {
        let task = self.task(id)?;
        let SqlDemand::JsonSchema(state) = &task.demand else {
            return Err(binding("schema phase differs"));
        };
        let (slot, row, index) = (task.slot, task.row, state.next_argument());
        let Some(NativeHostMetadata::JsonSchema(meta)) = self.metadata.get(slot.index()) else {
            return Err(binding("schema metadata absent"));
        };
        let store = self
            .native_values
            .ok_or_else(|| binding("schema value store absent"))?;
        let origin = |i: usize| -> LocalResult<NativeValueOrigin> {
            Ok(NativeValueOrigin {
                node: *meta
                    .arguments
                    .get(i)
                    .ok_or_else(|| binding("schema child ordinal absent"))?,
                row,
            })
        };
        if let Some(token) = token {
            store
                .borrow()
                .get(token, NativeValueRole::CharInput, origin(index)?)?;
        }
        if let SchemaTask::Document { schema, .. } = &**state {
            store
                .borrow()
                .get(schema, NativeValueRole::CharInput, origin(0)?)?;
        }
        let base = self.retained_bytes()?;
        self.check_bytes(base)?;
        let maximum = self.max_retained_bytes - base;
        let demand = std::mem::replace(
            &mut self.slots[id.slot]
                .task
                .as_mut()
                .expect("authenticated schema task")
                .demand,
            SqlDemand::Consumed,
        );
        let SqlDemand::JsonSchema(state) = demand else {
            unreachable!()
        };
        let previous = match &*state {
            SchemaTask::Schema => None,
            SchemaTask::Document { schema, .. } => Some(*schema),
        };
        let value = token
            .map(|token| {
                NativeValueStore::lease_for_kernel(
                    store,
                    token,
                    NativeValueRole::CharInput,
                    origin(index)?,
                )
            })
            .transpose()?;
        let result = tidb_query_expr::local::with_value_kernel_work_limit(maximum, || {
            let result = (|| -> std::result::Result<Progress, EvalError> {
                if meta.arguments.len() != 2 {
                    return Err(EvalError::WrongParameterCount("json_schema_valid"));
                }
                let value = value
                    .as_ref()
                    .expect("actual schema/document reply")
                    .datum();
                let progress = match *state {
                    SchemaTask::Schema => {
                        if value.is_null() {
                            Progress::Ready(Datum::Null)
                        } else {
                            let context = meta.cacheable.then(|| self.context.context_id());
                            // The only context callback precedes construction.
                            // Authenticate again before the original cache may
                            // publish a prepared schema or demand the document.
                            store
                                .borrow()
                                .authenticate(
                                    token.expect("real schema capability"),
                                    NativeValueRole::CharInput,
                                    origin(0).map_err(schema_error)?,
                                )
                                .map_err(schema_error)?;
                            let prepared = meta.cache.prepare_value(value, context)?;
                            if prepared.is_absent() {
                                Progress::Ready(Datum::Null)
                            } else {
                                Progress::More(SchemaTask::Document {
                                    schema: token
                                        .expect("real schema capability")
                                        .try_into()
                                        .expect("authenticated capability width"),
                                    prepared,
                                })
                            }
                        }
                    }
                    SchemaTask::Document { prepared, .. } => {
                        Progress::Ready(prepared.validate_value(value)?)
                    }
                };
                match progress {
                    Progress::Ready(value) => Ok(Progress::Ready(
                        crate::tikv::identity_actual::coerce_result(
                            "json_schema_valid",
                            &meta.result_type,
                            value,
                            tidb_query_expr::local::value_kernel_work_remaining()
                                .expect("schema finite lexical scope"),
                        )?,
                    )),
                    more => Ok(more),
                }
            })();
            crate::scalar_function::eager_values::audit_work_result(result)
        });
        if let Some(token) = token {
            store
                .borrow()
                .authenticate(token, NativeValueRole::CharInput, origin(index)?)?;
        }
        if let Some(schema) = previous {
            store
                .borrow()
                .get(&schema, NativeValueRole::CharInput, origin(0)?)?;
        }
        drop(value);
        match result {
            Err(error) => self.native_failure(error),
            Ok(Progress::Ready(value)) => self
                .finish_native_value(id, value, meta.output_node, meta.output_role, row, base)
                .map(HostStep::Ready),
            Ok(Progress::More(state)) => {
                self.check_bytes(
                    base.checked_add(state.retained_bytes()?)
                        .ok_or_else(|| resource("schema continuation extent overflow"))?,
                )?;
                self.slots[id.slot]
                    .task
                    .as_mut()
                    .expect("live consumed schema task")
                    .demand = SqlDemand::JsonSchema(Box::new(state));
                Ok(HostStep::NeedArg(HostArgRequest {
                    index: 1,
                    mode: ArgMode::Fresh,
                }))
            }
        }
    }
}
fn schema_error(error: LocalError) -> EvalError {
    EvalError::ExpressionRuntimeFailure(crate::ExpressionRuntimeFailure::from_local_eval(
        error, None,
    ))
}
