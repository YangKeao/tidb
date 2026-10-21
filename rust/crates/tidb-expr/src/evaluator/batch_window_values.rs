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
/// Includes Decimal padding before any native conversion can format it.
/// Coefficient storage alone does not bound a value's displayed work buffers.
pub(super) fn conversion_work_bound(value: &Datum, multiplier: usize) -> LocalResult<usize> {
    let mut bytes = native_payload_bytes(value)?;
    if let Datum::Decimal(value) = value {
        bytes = bytes.max(
            value
                .coefficient_bytes()
                .len()
                .checked_add(value.scale().max(value.storage_scale()) as usize)
                .and_then(|bytes| bytes.checked_add(4))
                .ok_or_else(|| resource("window Decimal conversion shape overflow"))?,
        );
    }
    bytes
        .checked_mul(multiplier)
        .and_then(|bytes| bytes.checked_add(1024))
        .ok_or_else(|| resource("window conversion work allowance overflow"))
}
impl NativeBatchHosts<'_> {
    pub(super) fn collect_window_source(
        &mut self,
        reply: &HostWindowArgReply<'_>,
        facts: WindowSourceFacts<'_>,
    ) -> LocalResult<WindowSourceValues> {
        self.collect_window_source_charged(reply, facts, 0, None)
    }
    pub(super) fn collect_window_source_charged(
        &mut self,
        reply: &HostWindowArgReply<'_>,
        facts: WindowSourceFacts<'_>,
        held: usize,
        auth: Option<&numeric_parent::NumericAuthentication<'_>>,
    ) -> LocalResult<WindowSourceValues> {
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
        let base = self
            .retained_bytes()?
            .checked_add(held)
            .ok_or_else(|| resource("window detached owner extent overflow"))?;
        self.check_bytes(
            base.checked_add(array_bytes::<Datum>(count)?)
                .ok_or_else(|| resource("window source vector allowance overflow"))?,
        )?;
        let mut values = Vec::new();
        values
            .try_reserve_exact(count)
            .map_err(|error| LocalError::ResourceLimit(error.to_string()))?;
        // Capacity is fixed for this source phase; carry the payload charge
        // incrementally rather than rescanning every prefix before each row.
        let mut retained_values = array_bytes::<Datum>(values.capacity())?;
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
            let raw_payload = native_payload_bytes(store.borrow().get(
                token,
                NativeValueRole::CharInput,
                origin,
            )?)?;
            let payload = if matches!(owner, WindowArithmeticSource::Literal) && position != 0 {
                native_payload_bytes(&values[0])?
            } else {
                raw_payload
            };
            self.check_bytes(
                base.checked_add(retained_values)
                    .and_then(|bytes| bytes.checked_add(payload.checked_mul(2)?))
                    .ok_or_else(|| resource("window source clone allowance overflow"))?,
            )?;
            let value = if matches!(owner, WindowArithmeticSource::Literal) && position != 0 {
                // Validate every capability, but perform literal typed conversion
                // only once, exactly before the original broadcast.
                values[0].clone()
            } else {
                let value =
                    store
                        .borrow()
                        .clone_for_kernel(token, NativeValueRole::CharInput, origin)?;
                let multiplier = if matches!(owner, WindowArithmeticSource::Literal)
                    && source == NativeEvalType::String
                    && !matches!(&value, Datum::String(_) | Datum::Bytes(_))
                {
                    128
                } else {
                    2
                };
                // Forwarded values have already crossed their typed owner.
                // Copy capacity is billed separately; no display/formatter
                // work occurs in these branches, including Decimal + NULL.
                let conversion = if matches!(
                    owner,
                    WindowArithmeticSource::TypedReady | WindowArithmeticSource::IntegerReady
                ) || (matches!(owner, WindowArithmeticSource::Computed)
                    && source != NativeEvalType::Int)
                {
                    0
                } else {
                    conversion_work_bound(&value, multiplier)?
                };
                self.check_bytes(
                    base.checked_add(retained_values)
                        .and_then(|bytes| bytes.checked_add(conversion))
                        .ok_or_else(|| resource("window typed conversion allowance overflow"))?,
                )?;
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
            self.check_bytes(
                base.checked_add(retained_values)
                    .and_then(|bytes| {
                        bytes.checked_add(native_payload_bytes(&value).ok()?.checked_mul(2)?)
                    })
                    .ok_or_else(|| resource("window normalized source allowance overflow"))?,
            )?;
            retained_values = retained_values
                .checked_add(
                    native_payload_bytes(&value)?
                        .checked_mul(2)
                        .ok_or_else(|| resource("window normalized payload overflow"))?,
                )
                .ok_or_else(|| resource("window normalized ledger overflow"))?;
            values.push(value);
            self.check_bytes(
                base.checked_add(retained_values)
                    .ok_or_else(|| resource("window normalized source bytes overflow"))?,
            )?;
        }
        Ok(WindowSourceValues {
            values,
            retained_bytes: retained_values,
        })
    }
}
