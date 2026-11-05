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

//! Actual charset sources and original cast/context phases in the same SDK frame.
use super::*;
use tidb_query_expr::{NativeCharsetOperation as Op, NativeCharsetSourceStep as Step};
pub(in super::super) struct NativeCharsetMetadata {
    pub operation: Op,
    pub arguments: Box<[usize]>,
    pub source_type: FieldType,
    pub explicit_collation: bool,
    pub result_type: FieldType,
    pub output_node: usize,
    pub output_role: Option<NativeValueRole>,
}

impl NativeBatchHosts<'_> {
    pub(super) fn start_charset(&self, invocation: &HostInvocation<'_>) -> LocalResult<SqlDemand> {
        let Some(NativeHostMetadata::Charset(metadata)) =
            self.metadata.get(invocation.slot.index())
        else {
            return Err(binding("charset descriptor absent"));
        };
        if self.native_values.is_none()
            || metadata.arguments.len() != metadata.operation.arity()
            || metadata.arguments.len() != invocation.arg_types.len()
            || self.catalog.sql_batch_operation(invocation.slot)?
                != SqlBatchHostOperation::CharsetNative(metadata.operation)
        {
            return Err(binding("charset source descriptor differs"));
        }
        Ok(SqlDemand::Charset { source: None })
    }
    pub(super) fn resume_charset(
        &mut self,
        id: &HostTaskId,
        token: &[u8],
    ) -> LocalResult<HostStep> {
        let task = self.task(id)?;
        let SqlDemand::Charset { source } = task.demand else {
            return Err(binding("charset has another task kind"));
        };
        let row = task.row;
        let Some(NativeHostMetadata::Charset(metadata)) = self.metadata.get(task.slot.index())
        else {
            return Err(binding("charset metadata vanished"));
        };
        let store = self
            .native_values
            .ok_or_else(|| binding("charset value store missing"))?;
        let index = usize::from(source.is_some());
        let node = *metadata
            .arguments
            .get(index)
            .ok_or_else(|| binding("charset demanded source outside descriptor"))?;
        let origin = NativeValueOrigin { node, row };
        let decision = {
            let values = store.borrow();
            let value = values.get(token, NativeValueRole::CharInput, origin)?;
            if source.is_none() {
                Some(tidb_query_expr::native_charset_after_source(
                    metadata.operation,
                    value.is_null(),
                ))
            } else {
                None
            }
        };

        let value = if source.is_none() {
            match decision.expect("actual first source decision") {
                Step::Null => crate::tikv::eval_charset_null_in(self.context),
                Step::Target => {
                    let handle = token
                        .try_into()
                        .map_err(|_| binding("charset source capability width differs"))?;
                    let SqlDemand::Charset { source } = &mut self.slots[id.slot]
                        .task
                        .as_mut()
                        .expect("validated charset task")
                        .demand
                    else {
                        unreachable!()
                    };
                    *source = Some(handle);
                    return Ok(HostStep::NeedArg(HostArgRequest {
                        index: 1,
                        mode: ArgMode::Fresh,
                    }));
                }
                Step::Convert => {
                    // No context callbacks exist in strict one-input codecs.
                    // Borrow original typed bytes without an input copy, then
                    // drop the borrow before result publication mutates store.
                    let result = {
                        let values = store.borrow();
                        let value = values.get(token, NativeValueRole::CharInput, origin)?;
                        match metadata.operation {
                            Op::ToBinary => crate::tikv::to_binary_bounded(
                                value,
                                metadata.source_type.charset_name(),
                                usize::MAX,
                            ),
                            Op::FromBinary => crate::tikv::from_binary_bounded(
                                value,
                                metadata.result_type.charset_name(),
                                usize::MAX,
                            ),
                            Op::ConvertUsing => {
                                return Err(binding("charset conversion lost target demand"))
                            }
                        }
                    };
                    result
                }
            }
        } else {
            if metadata.operation != Op::ConvertUsing {
                return Err(binding("unary charset task received target"));
            }
            let source = source.expect("checked source capability");
            // Target coercion precedes source casting; failures must not inspect
            // or charge a later source Decimal formatter.
            let (target, value) = {
                let values = store.borrow();
                // A retained first-source capability must still be authentic
                // before any later target coercion or resource work.
                values.get(
                    &source,
                    NativeValueRole::CharInput,
                    NativeValueOrigin {
                        node: metadata.arguments[0],
                        row,
                    },
                )?;
                let target = values.get(token, NativeValueRole::CharInput, origin)?;

                let target = match crate::coerce::coerce_str_bytes(target) {
                    Ok(value) => value.unwrap_or_default(),
                    Err(error) => return self.native_failure(error),
                };
                let target = String::from_utf8_lossy(&target).into_owned();
                let value = values.get(
                    &source,
                    NativeValueRole::CharInput,
                    NativeValueOrigin {
                        node: metadata.arguments[0],
                        row,
                    },
                )?;

                let value = match crate::cast::cast_arg_as_string(
                    value,
                    Some(&metadata.source_type),
                    self.context,
                ) {
                    Ok(value) => value,
                    Err(error) => return self.native_failure(error),
                };

                (target, value)
            };
            // This actual getter is unconditional even for an ETString source
            // whose metadata selector ignores connection names. No store borrow
            // survives the callback; GB mode may change here and is read later.
            let connection = self.context.connection_charset_info();
            let source = &metadata.source_type;

            let string_type = crate::cast::cast_arg_as_string_type(
                source,
                metadata.explicit_collation,
                connection,
            );
            crate::tikv::convert_using_bounded(&value, &string_type, &target, usize::MAX)
        };
        let value = match value {
            Ok(value) => value,
            Err(error) => return self.native_failure(error),
        };
        let value = match crate::scalar_function::coerce_function_result(
            metadata.operation.sql_name(),
            Some(&metadata.result_type),
            value,
        ) {
            Ok(value) => value,
            Err(error) => return self.native_failure(error),
        };
        if let Some(role) = metadata.output_role {
            return self
                .finish_native_value(id, value, metadata.output_node, role, row)
                .map(HostStep::Ready);
        }
        // Typed string parents keep their original carrier/demand profile.
        // This is ETString publication of the SAME computed value, not another
        // evaluator or a new cast. Native source inputs remain authenticated.
        let value =
            crate::expression::apply_enum_set_integer_view(value, Some(&metadata.result_type));
        let bytes = match crate::arg_eval_type::eval_string_ref(&value) {
            Ok(bytes) => bytes,
            Err(error) => return self.native_failure(error),
        };
        let len = bytes.map_or(0, <[u8]>::len);

        let mut output = ChunkedVecBytes::try_with_capacities(1, len)
            .map_err(|error| LocalError::ResourceLimit(error.to_string()))?;
        output.push_ref(bytes);

        self.slots[id.slot].task = None;
        Ok(HostStep::Ready(VectorValue::Bytes(output)))
    }
}
