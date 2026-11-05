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

//! Actual-value adapter under independent comparison with the original worker.
use super::*;
use extrema_sdk::{
    NativeExtremumComparisonOp, NativeExtremumComparisonValue, NativeExtremumDomain as Domain,
    NativeExtremumHead as Head, NativeExtremumNumericCursor, NativeExtremumPromotion as Promotion,
    NativeExtremumSequence as Sequence, NativeExtremumSequenceValue as Value,
    NativeExtremumValueError as ValueError,
};
use tidb_datatype::FieldTypeCode;
use tidb_query_datatype::codec::mysql::{decimal::NativeDecimalError, time::NativeTemporalValue};
fn resource(reason: &'static str) -> EvalError {
    EvalError::ExpressionRuntimeFailure(ExpressionRuntimeFailure::from_local_eval(
        LocalError::ResourceLimit(reason.into()),
        None,
    ))
}
fn value_error(error: ValueError) -> EvalError {
    match error {
        ValueError::Invalid => invalid_report(),
        ValueError::ResourceLimit(reason) => resource(reason),
    }
}
fn check(bytes: usize, maximum: usize) -> Result<(), EvalError> {
    if bytes > maximum {
        Err(resource("extremum actual value/work allowance exceeded"))
    } else {
        Ok(())
    }
}
fn payload(value: &Datum) -> Result<usize, EvalError> {
    match value {
        Datum::Decimal(value) => Ok(value.coefficient_bytes().len()),
        Datum::Json(value) => value
            .value()
            .len()
            .checked_add(1)
            .ok_or_else(|| resource("extremum JSON extent overflow")),
        Datum::VectorFloat32(value) => value
            .elements_capacity()
            .checked_mul(4)
            .ok_or_else(|| resource("extremum vector extent overflow")),
        _ => Ok(value.go_bytes().len()),
    }
}
fn conversion(value: &Datum, maximum: usize) -> Result<(), EvalError> {
    let bytes = match value {
        Datum::Decimal(value) => value
            .coefficient_bytes()
            .len()
            .checked_add(value.scale().max(value.storage_scale()) as usize)
            .ok_or_else(|| resource("extremum Decimal text extent overflow"))?,
        _ => payload(value)?,
    };
    check(
        bytes
            .checked_mul(16)
            .and_then(|n| n.checked_add(1024))
            .ok_or_else(|| resource("extremum conversion work extent overflow"))?,
        maximum,
    )
}
fn decimal_error<T: std::fmt::Debug>(value: Result<T, NativeDecimalError>) -> Result<T, EvalError> {
    match value {
        Ok(value) => Ok(value),
        Err(NativeDecimalError::Resource(reason)) => Err(resource(reason)),
        Err(error) => panic!("shared native decimal precision cast failed: {error:?}"),
    }
}
pub(crate) fn eval_extremum_values_bounded(
    ctx: &dyn Columns,
    vals: &[Datum],
    want: Ordering,
    signature: Option<GlSignature>,
    arg_decimals: &[i64],
    all_constant: bool,
    collation: Collation,
    maximum: usize,
) -> Result<Datum, EvalError> {
    if vals.is_empty() {
        return Err(EvalError::Unsupported("bad function arity"));
    }
    let bytes = vals
        .len()
        .checked_mul(std::mem::size_of::<extrema_sdk::NativeExtremumValueMeta>())
        .ok_or_else(|| resource("extremum metadata extent overflow"))?;
    check(bytes, maximum)?;
    let mut metadata = Vec::new();
    metadata
        .try_reserve_exact(vals.len())
        .map_err(|_| resource("extremum metadata allocation failed"))?;
    let retained = metadata
        .capacity()
        .checked_mul(std::mem::size_of::<extrema_sdk::NativeExtremumValueMeta>())
        .ok_or_else(|| resource("extremum metadata capacity overflow"))?;
    check(retained, maximum)?;
    metadata.extend(
        vals.iter()
            .map(|value| extrema_sdk::NativeExtremumValueMeta {
                kind: value.kind() as u8,
                time_kind: match value {
                    Datum::Time(value) => Some(value.kind()),
                    _ => None,
                },
                decimal_scale: match value {
                    Datum::Decimal(value) => Some(value.scale()),
                    _ => None,
                },
            }),
    );
    let domain = match extrema_sdk::native_extremum_head(
        &metadata,
        signature.map(extremum_signature_metadata),
    )
    .map_err(|_| invalid_report())?
    {
        Head::Null => return Ok(Datum::Null),
        Head::Domain(domain) => domain,
    };
    let available = maximum - retained;
    if domain == Domain::Numeric {
        let mut cursor =
            NativeExtremumNumericCursor::new(vals.len(), want).map_err(|_| invalid_report())?;
        while let Some(request) = cursor.request() {
            let candidate = &vals[request.candidate_index];
            let best = &vals[request.best_index];
            let clone_bytes = payload(candidate)?
                .checked_add(payload(best)?)
                .and_then(|n| n.checked_mul(2))
                .ok_or_else(|| resource("extremum comparison clone allowance overflow"))?;
            check(clone_bytes, available)?;
            // Decimal display padding is used only by a demanded real/string
            // comparison; exact integer/Decimal comparison uses shared words.
            if !candidate.is_range_sentinel()
                && !best.is_range_sentinel()
                && !matches!(candidate, Datum::Raw(_) | Datum::VectorFloat32(_))
                && !matches!(best, Datum::Raw(_) | Datum::VectorFloat32(_))
            {
                if matches!(
                    candidate,
                    Datum::Real(_) | Datum::Float32(_) | Datum::String(_) | Datum::Bytes(_)
                ) {
                    conversion(best, available - clone_bytes)?;
                }
                if matches!(
                    best,
                    Datum::Real(_) | Datum::Float32(_) | Datum::String(_) | Datum::Bytes(_)
                ) {
                    conversion(candidate, available - clone_bytes)?;
                }
            }
            if !candidate.is_range_sentinel() && !best.is_range_sentinel() {
                if matches!(candidate, Datum::Json(_)) || matches!(best, Datum::Json(_)) {
                    conversion(candidate, available - clone_bytes)?;
                    conversion(best, available - clone_bytes)?;
                } else if matches!(candidate, Datum::VectorFloat32(_))
                    || matches!(best, Datum::VectorFloat32(_))
                {
                    for value in [candidate, best] {
                        if matches!(value, Datum::String(_) | Datum::Bytes(_)) {
                            conversion(value, available - clone_bytes)?;
                        }
                    }
                }
            }
            // Exact Decimal comparison packs coefficient/storage words, not
            // visible padding. Bound that actual preparation before its legacy
            // unbounded representation bridge can allocate.
            let mut packed = 0usize;
            if !candidate.is_range_sentinel()
                && !best.is_range_sentinel()
                && !matches!(
                    candidate,
                    Datum::Raw(_) | Datum::VectorFloat32(_) | Datum::Json(_)
                )
                && !matches!(
                    best,
                    Datum::Raw(_) | Datum::VectorFloat32(_) | Datum::Json(_)
                )
                && !matches!(
                    candidate,
                    Datum::Real(_) | Datum::Float32(_) | Datum::String(_) | Datum::Bytes(_)
                )
                && !matches!(
                    best,
                    Datum::Real(_) | Datum::Float32(_) | Datum::String(_) | Datum::Bytes(_)
                )
            {
                for value in [candidate, best] {
                    if let Datum::Decimal(value) = value {
                        packed = packed
                            .checked_add(
                                value
                                    .coefficient_bytes()
                                    .len()
                                    .checked_add(value.storage_scale() as usize)
                                    .and_then(|n| n.checked_mul(16))
                                    .and_then(|n| n.checked_add(1024))
                                    .ok_or_else(|| {
                                        resource("extremum Decimal packing extent overflow")
                                    })?,
                            )
                            .ok_or_else(|| {
                                resource("extremum Decimal pair packing extent overflow")
                            })?;
                    }
                }
            }
            check(packed, available - clone_bytes)?;
            let op = match request.op {
                NativeExtremumComparisonOp::Lt => extrema_sdk::ComparisonOp::Lt,
                NativeExtremumComparisonOp::Gt => extrema_sdk::ComparisonOp::Gt,
            };
            let actual = crate::ops::prepare_literal_comparison(
                op,
                candidate.clone(),
                best.clone(),
                &crate::NoColumns,
            )?
            .compute(available - clone_bytes - packed)
            .map_err(|error| {
                EvalError::ExpressionRuntimeFailure(ExpressionRuntimeFailure::from_local_eval(
                    error, None,
                ))
            })?;
            cursor
                .observe(match actual {
                    Some(value) => NativeExtremumComparisonValue::Int(value),
                    None => NativeExtremumComparisonValue::OtherValue,
                })
                .map_err(|_| invalid_report())?;
        }
        let promotion = cursor
            .finish(&metadata, arg_decimals, all_constant)
            .map_err(|_| invalid_report())?;
        return match promotion {
            Promotion::KeepWinner { index } => {
                check(
                    payload(&vals[index])?
                        .checked_mul(2)
                        .ok_or_else(|| resource("extremum winner clone allowance overflow"))?,
                    available,
                )?;
                Ok(vals[index].clone())
            }
            Promotion::ToReal { index } => {
                conversion(&vals[index], available)?;
                Ok(Datum::Real(crate::ops::to_f64(vals[index].clone())))
            }
            Promotion::ToDecimal {
                index,
                precision_scale,
            } => {
                check(
                    payload(&vals[index])?
                        .checked_mul(2)
                        .ok_or_else(|| resource("extremum Decimal clone allowance overflow"))?,
                    available,
                )?;
                let value = crate::ops::to_decimal(vals[index].clone());
                match precision_scale {
                    None => Ok(Datum::Decimal(value)),
                    Some(scale) => {
                        let shared = decimal_error(value.try_to_shared_math(available))?;
                        let rounded = decimal_error(
                            shared.try_native_cast_to_precision(0, scale, available),
                        )?;
                        Ok(Datum::Decimal(decimal_error(
                            tidb_datatype::Decimal::try_from_shared_math(&rounded, available),
                        )?))
                    }
                }
            }
        };
    }
    let policy = tidb_datatype::get_collator(collation.name())
        .new_collation()
        .map_or(
            super::super::NativeCollation::Binary,
            Collation::native_policy,
        );
    let mut sequence =
        Sequence::new(domain, want, policy, vals.len(), available).map_err(value_error)?;
    while let Some(index) = sequence.next_index() {
        let remaining = available
            .checked_sub(sequence.retained_bytes().map_err(value_error)?)
            .ok_or_else(|| resource("extremum sequence allowance exhausted"))?;
        let value = &vals[index];
        conversion(value, remaining)?;
        let actual = match domain {
            Domain::Time { .. } => match crate::cast::cast_arg_as_datetime(value, None, ctx)? {
                Datum::Null => None,
                Datum::Time(value) => Some(Value::Time(NativeTemporalValue {
                    raw: value.core_time().raw(),
                    kind: value.kind(),
                    fsp: value.fsp(),
                })),
                _ => return Err(invalid_report()),
            },
            Domain::Vector => match value
                .convert_to(
                    &FieldType::new(FieldTypeCode::VectorFloat32),
                    tidb_datatype::ConversionFlags::default(),
                )
                .map_err(|error| EvalError::Vector(error.to_string()))?
                .value
            {
                Datum::VectorFloat32(value) => Some(Value::Vector(value)),
                _ => return Err(invalid_report()),
            },
            Domain::DirectString => Some(Value::Bytes(extremum_string_value(value)?)),
            Domain::StringAsTime { as_date } => match crate::coerce::coerce_str(value)? {
                None => None,
                Some(text) => {
                    let modes = ctx.date_modes();
                    let zone = ctx.time_zone();
                    Some(Value::Bytes(
                        extrema_sdk::native_extremum_time_text_value(
                            &text,
                            as_date,
                            modes.allow_invalid_dates,
                            &zone,
                            remaining,
                        )
                        .map_err(value_error)?,
                    ))
                }
            },
            Domain::Numeric => return Err(invalid_report()),
        };
        sequence.accept(actual).map_err(value_error)?;
    }
    Ok(match sequence.finish().map_err(value_error)? {
        None => Datum::Null,
        Some(Value::Bytes(value)) => Datum::new_string(value),
        Some(Value::Vector(value)) => Datum::VectorFloat32(value),
        Some(Value::Time(value)) => Datum::Time(tidb_datatype::Time::from_raw_parts(
            tidb_datatype::CoreTime::from_raw(value.raw),
            value.kind,
            value.fsp,
        )),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    struct NoEffects;
    impl Columns for NoEffects {
        fn get(&self, _: &[String]) -> Option<Datum> {
            panic!("unexpected source callback")
        }
        fn ready_value_cache(&self) -> Option<&super::super::super::ReadyValueCache> {
            panic!("retired extremum worker")
        }
        fn date_modes(&self) -> tidb_datatype::DateModes {
            panic!("unexpected modes")
        }
        fn time_zone(&self) -> tidb_datatype::SessionTimeZone {
            panic!("unexpected zone")
        }
        fn append_warning(&self, _: u16, _: &str) {
            panic!("unexpected warning")
        }
    }
    fn run(values: &[Datum], signature: Option<GlSignature>) -> Result<Datum, EvalError> {
        eval_extremum_values_bounded(
            &NoEffects,
            values,
            Ordering::Less,
            signature,
            &[],
            false,
            Collation::Binary,
            4096,
        )
    }
    fn denied(value: Result<Datum, EvalError>) {
        let Err(EvalError::ExpressionRuntimeFailure(error)) = value else {
            panic!("resource refusal lost")
        };
        assert_eq!(
            error.class(),
            crate::ExpressionRuntimeFailureClass::ResourceLimit
        );
        assert_eq!(error.phase(), None);
    }
    #[test]
    fn extremum_bounds_demanded_decimal_text_and_math_packing_before_work() {
        let visible = Datum::Decimal(tidb_datatype::Decimal::from_raw_parts(
            false,
            vec![b'1'],
            u32::MAX,
            0,
        ));
        let storage = Datum::Decimal(tidb_datatype::Decimal::from_raw_parts(
            false,
            vec![b'1'],
            0,
            u32::MAX,
        ));
        let string = Some(GlSignature {
            arg_type: EvalType::String,
            cmp_string_mode: GlCmpStringMode::Directly,
            ret_date: false,
        });
        assert_eq!(
            run(&[visible.clone(), Datum::Null], string),
            Ok(Datum::Null)
        );
        denied(run(&[visible.clone(), Datum::new_string("2")], string));
        denied(run(
            &[visible.clone(), Datum::Real(2.0)],
            Some(GlSignature {
                arg_type: EvalType::Real,
                cmp_string_mode: GlCmpStringMode::Directly,
                ret_date: false,
            }),
        ));
        denied(run(&[storage, Datum::Int(2)], None));
        let json = Datum::Json(tidb_datatype::BinaryJSON::parse("1").unwrap());
        denied(run(
            &[visible.clone(), json],
            Some(GlSignature {
                arg_type: EvalType::Int,
                cmp_string_mode: GlCmpStringMode::Directly,
                ret_date: false,
            }),
        ));
        // No Decimal text/packing work is performed behind original sentinel
        // refusal, or behind the complete actual-NULL prepass.
        assert_eq!(
            run(&[Datum::MinNotNull, visible.clone()], None),
            Err(EvalError::Unsupported("range sentinel expression operand"))
        );
        assert_eq!(
            run(&[visible, Datum::Int(2), Datum::Null], None),
            Ok(Datum::Null)
        );
        assert_eq!(
            run(&[Datum::Int(3), Datum::Int(2)], None),
            Ok(Datum::Int(2))
        );
    }
}
