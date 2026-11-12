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

//! Complete source-typed vector ownership shared by native window operators.
use super::*;
impl NativeBatchHosts<'_> {
    pub(super) fn collect_window_source(
        &mut self,
        reply: &HostWindowArgReply<'_>,
        facts: WindowSourceFacts<'_>,
        auth: Option<&numeric_parent::NumericAuthentication<'_>>,
    ) -> LocalResult<Vec<Datum>> {
        let WindowSourceFacts {
            field,
            owner,
            node,
            target: source,
        } = facts;
        let count = reply.physical_rows.len();
        let start = reply.occurrence_start;
        let store = self
            .native_values
            .ok_or_else(|| binding("window source store missing"))?;

        let mut values: Vec<Datum> = Vec::new();
        values
            .try_reserve_exact(count)
            .map_err(|error| LocalError::ResourceLimit(error.to_string()))?;

        for position in 0..count {
            let ScalarValueRef::Bytes(Some(token)) = reply.values.get_scalar_ref(position) else {
                return Err(binding("window arithmetic wire NULL is not a native value"));
            };
            let origin = NativeValueOrigin {
                node,
                row: InputRow {
                    occurrence: start + position,
                    input_row: reply.physical_rows[position],
                },
            };
            let value = if matches!(owner, WindowArithmeticSource::Literal) && position != 0 {
                // Validate every capability, but perform literal typed conversion
                // only once, exactly before the original broadcast.
                store
                    .borrow()
                    .get(token, NativeValueRole::CharInput, origin)?;
                values[0].clone()
            } else {
                let value =
                    store
                        .borrow()
                        .clone_for_kernel(token, NativeValueRole::CharInput, origin)?;

                // Forwarded values have already crossed their typed owner.
                // No display/formatter work occurs in these branches.

                let value = match owner {
                    WindowArithmeticSource::Literal => {
                        crate::constant::typed_constant_value(field, self.context, source, value)
                    }
                    WindowArithmeticSource::Column => {
                        crate::scalar_function::normalize_numeric_row_value(
                            value,
                            Some(field),
                            source,
                            self.context,
                        )
                    }
                    WindowArithmeticSource::Computed if source == NativeEvalType::Int => {
                        crate::scalar_function::normalize_numeric_row_value(
                            value,
                            Some(field),
                            source,
                            self.context,
                        )
                    }
                    WindowArithmeticSource::Computed
                    | WindowArithmeticSource::IntegerReady
                    | WindowArithmeticSource::TypedReady => Ok(value),
                };
                if let Some(auth) = auth {
                    auth.check()?;
                }
                match value {
                    Ok(value) => value,
                    Err(error) => return self.native_failure(error),
                }
            };
            let value = if source == NativeEvalType::Int {
                match crate::scalar_function::native_batch_integer_bits(value) {
                    Ok(bits) => crate::scalar_function::native_batch_integer_value(
                        bits,
                        field.is_unsigned(),
                    ),
                    Err(error) => return self.native_failure(error),
                }
            } else {
                value
            };

            values.push(value);
        }
        Ok(values)
    }
}
