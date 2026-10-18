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

use super::adapter_failure::{ExpressionAdapterFailure, ScopeFailureKind};
use super::{ExpressionRuntimeFailure, LocalError};
use crate::{Columns, Datum, EvalError};
use tidb_datatype::{EvalType, FieldType};
use tidb_query_expr::{
    NativeIntervalCast as Cast, NativeIntervalEagerInput as Input,
    NativeIntervalEagerStart as Start, NativeIntervalEvalType as Type, NativeIntervalFieldType,
    NativeIntervalNumber as Number, NativeIntervalSearch, NativeIntervalValueError as Error,
    NativeIntervalValueStep as Step,
};

fn invalid_report() -> EvalError {
    EvalError::ExpressionAdapterFailure(ExpressionAdapterFailure::from_scope(
        ScopeFailureKind::Contract,
        "invalid native INTERVAL report",
    ))
}
pub(crate) fn interval_value_error(error: Error) -> EvalError {
    match error {
        Error::Arity { .. } => {
            EvalError::ExpressionRuntimeFailure(ExpressionRuntimeFailure::from_local_eval(
                LocalError::evaluated_argument_shape_mismatch(),
                Some(crate::ExpressionRuntimeFailurePhase::Invoke),
            ))
        }
        Error::Invalid => invalid_report(),
        Error::ResourceLimit(reason) => resource(reason),
    }
}
fn resource(reason: &'static str) -> EvalError {
    EvalError::ExpressionRuntimeFailure(ExpressionRuntimeFailure::from_local_eval(
        LocalError::ResourceLimit(reason.into()),
        None,
    ))
}
fn budget(bytes: usize, maximum: usize) -> Result<(), EvalError> {
    if maximum == usize::MAX || bytes > maximum {
        return Err(resource(
            "INTERVAL actual state/conversion allowance exceeded",
        ));
    }
    Ok(())
}
pub(crate) fn interval_field_metadata(field: &FieldType) -> NativeIntervalFieldType {
    NativeIntervalFieldType {
        eval_type: match field.eval_type() {
            EvalType::Int => Type::Int,
            EvalType::Real => Type::Real,
            EvalType::Decimal => Type::Decimal,
            EvalType::String => Type::String,
            EvalType::Datetime => Type::Datetime,
            EvalType::Timestamp => Type::Timestamp,
            EvalType::Duration => Type::Duration,
            EvalType::Json => Type::Json,
            EvalType::VectorFloat32 => Type::VectorFloat32,
        },
        flags: field.flags(),
    }
}
/// Bound only conversion that is actually demanded. Decimal-to-real uses its
/// original visible formatter; retained coefficient bytes alone are insufficient.
fn conversion_work(value: &Datum, cast: Cast) -> Result<usize, EvalError> {
    let bytes = match value {
        Datum::Null
        | Datum::Int(_)
        | Datum::UInt(_)
        | Datum::MinNotNull
        | Datum::MaxValue
        | Datum::VectorFloat32(_) => return Ok(0),
        Datum::Real(_) | Datum::Float32(_) if cast == Cast::Real => return Ok(0),
        Datum::Decimal(value) => value
            .coefficient_bytes()
            .len()
            .checked_add(value.scale().max(value.storage_scale()) as usize)
            .and_then(|bytes| bytes.checked_add(4))
            .ok_or_else(|| resource("INTERVAL Decimal conversion shape overflow"))?,
        Datum::String(value) => value.bytes().len(),
        Datum::Bytes(value) | Datum::Raw(value) => value.len(),
        Datum::BinaryLiteral(value) | Datum::Bit(value) => value.as_bytes().len(),
        Datum::Json(value) => value
            .value()
            .len()
            .checked_add(1)
            .ok_or_else(|| resource("INTERVAL JSON conversion extent overflow"))?,
        Datum::Enum(_, _)
        | Datum::Set(_, _)
        | Datum::Time(_)
        | Datum::Duration(_)
        | Datum::Real(_)
        | Datum::Float32(_) => 0,
    };
    bytes
        .checked_mul(16)
        .and_then(|bytes| bytes.checked_add(1024))
        .ok_or_else(|| resource("INTERVAL conversion work extent overflow"))
}
/// Original actual-value cast, shared by the direct adapter and canonical host.
/// No identity frame, synthetic literal or source evaluation occurs here.
pub(crate) fn prepare_interval_number_in(
    ctx: &dyn Columns,
    value: &Datum,
    source: Option<&FieldType>,
    cast: Cast,
    maximum: usize,
) -> Result<Option<Number>, EvalError> {
    budget(conversion_work(value, cast)?, maximum)?;
    match cast {
        Cast::Real => {
            if value.is_null() {
                Ok(None)
            } else {
                crate::ops::to_f64_with_mysql_string(value, ctx)
                    .map(|value| Some(Number::Real(value.to_bits())))
            }
        }
        Cast::Int => match crate::cast::cast_arg_as_int(value, source, ctx)? {
            Datum::Null => Ok(None),
            Datum::Int(value) => Ok(Some(Number::Signed(value))),
            Datum::UInt(value) => Ok(Some(Number::Unsigned(value))),
            _ => Err(invalid_report()),
        },
    }
}
fn eager_view(value: &Datum) -> Input {
    match value {
        Datum::Null => Input::Null,
        Datum::Int(value) => Input::Int(*value),
        Datum::UInt(value) => Input::UInt(*value),
        Datum::MinNotNull | Datum::MaxValue => Input::Sentinel,
        _ => Input::Other,
    }
}
pub(crate) fn eval_interval_in(ctx: &dyn Columns, vals: &[Datum]) -> Result<Datum, EvalError> {
    eager_bounded(ctx, vals, super::control_value::default_budget())
}
fn eager_bounded(ctx: &dyn Columns, vals: &[Datum], maximum: usize) -> Result<Datum, EvalError> {
    if vals.len() < 2 {
        return Err(interval_value_error(Error::Arity { actual: vals.len() }));
    }
    let planned = vals
        .len()
        .checked_mul(std::mem::size_of::<Input>())
        .ok_or_else(|| resource("INTERVAL borrowed view extent overflow"))?;
    budget(planned, maximum)?;
    let mut views = Vec::new();
    views
        .try_reserve_exact(vals.len())
        .map_err(|_| resource("INTERVAL borrowed view allocation failed"))?;
    let retained = views
        .capacity()
        .checked_mul(std::mem::size_of::<Input>())
        .ok_or_else(|| resource("INTERVAL borrowed view capacity overflow"))?;
    budget(retained, maximum)?;
    views.extend(vals.iter().map(eager_view));
    match tidb_query_expr::native_interval_eager_values(&views, maximum - retained)
        .map_err(interval_value_error)?
    {
        Start::Index(index) => Ok(Datum::Int(index)),
        Start::Sentinels => Err(EvalError::Unsupported("range sentinel INTERVAL argument")),
        Start::Real(mut state) => {
            let state_bytes = state.retained_bytes();
            let available = maximum
                .checked_sub(retained)
                .and_then(|bytes| bytes.checked_sub(state_bytes))
                .ok_or_else(|| resource("INTERVAL eager state allowance exhausted"))?;
            loop {
                match state.step() {
                    Step::Index(index) => break Ok(Datum::Int(index)),
                    Step::Probe {
                        index,
                        cast: Cast::Real,
                    } => {
                        let Some(Number::Real(value)) = prepare_interval_number_in(
                            ctx,
                            &vals[index],
                            None,
                            Cast::Real,
                            available,
                        )?
                        else {
                            return Err(invalid_report());
                        };
                        state
                            .accept_real(f64::from_bits(value))
                            .map_err(interval_value_error)?;
                    }
                    _ => return Err(invalid_report()),
                }
            }
        }
    }
}
pub(crate) fn eval_interval_lazy_in(
    ctx: &dyn Columns,
    arg_types: &[Option<FieldType>],
    eval: impl FnMut(usize) -> Result<Datum, EvalError>,
) -> Result<Datum, EvalError> {
    debug_assert!(arg_types.len() >= 2);
    lazy_bounded(ctx, arg_types, eval, super::control_value::default_budget())
}
fn lazy_bounded(
    ctx: &dyn Columns,
    arg_types: &[Option<FieldType>],
    mut eval: impl FnMut(usize) -> Result<Datum, EvalError>,
    maximum: usize,
) -> Result<Datum, EvalError> {
    let mut search = NativeIntervalSearch::new(
        arg_types
            .iter()
            .map(|field| field.as_ref().map(interval_field_metadata)),
    )
    .map_err(interval_value_error)?;
    budget(0, maximum)?;
    loop {
        match search.step() {
            Step::Index(index) => break Ok(Datum::Int(index)),
            Step::Probe { index, cast } => {
                let value = eval(index)?;
                let value = prepare_interval_number_in(
                    ctx,
                    &value,
                    arg_types[index].as_ref(),
                    cast,
                    maximum,
                )?;
                search.accept(value).map_err(interval_value_error)?;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    struct NoEffects;
    impl Columns for NoEffects {
        fn get(&self, _: &[String]) -> Option<Datum> {
            panic!("unexpected source")
        }
        fn ready_value_cache(&self) -> Option<&super::super::ReadyValueCache> {
            panic!("retired worker")
        }
        fn append_warning(&self, _: u16, _: &str) {
            panic!("unperformed conversion emitted warning")
        }
    }
    #[test]
    fn interval_arity_keeps_actual_invalid_batch_cause_and_invoke_phase() {
        for count in [0, 1] {
            let values = vec![Datum::Null; count];
            let types = vec![None; count];
            for actual in [
                eager_bounded(&NoEffects, &values, 1024),
                lazy_bounded(
                    &NoEffects,
                    &types,
                    |_| panic!("invalid arity read a source"),
                    1024,
                ),
            ] {
                let Err(EvalError::ExpressionRuntimeFailure(error)) = actual else {
                    panic!("arity boundary changed")
                };
                assert_eq!(
                    error.class(),
                    crate::ExpressionRuntimeFailureClass::InvalidBatch
                );
                assert_eq!(
                    error.phase(),
                    Some(crate::ExpressionRuntimeFailurePhase::Invoke)
                );
                assert_eq!(
                    error.local_error().to_string(),
                    LocalError::evaluated_argument_shape_mismatch().to_string()
                );
            }
        }
    }
    #[test]
    fn interval_bounds_actual_state_and_demanded_decimal_format_without_touching_skips() {
        let denied = |actual: Result<Datum, EvalError>| {
            let Err(EvalError::ExpressionRuntimeFailure(error)) = actual else {
                panic!("resource refusal was folded")
            };
            assert_eq!(
                error.class(),
                crate::ExpressionRuntimeFailureClass::ResourceLimit
            );
            assert_eq!(error.phase(), None);
        };
        for value in [
            Datum::Null,
            Datum::Int(1),
            Datum::UInt(u64::MAX),
            Datum::Real(1.0),
        ] {
            assert!(prepare_interval_number_in(&NoEffects, &value, None, Cast::Real, 0).is_ok());
        }
        let huge = Datum::Decimal(tidb_datatype::Decimal::from_raw_parts(
            false,
            vec![b'1'],
            u32::MAX,
            0,
        ));
        denied(eager_bounded(
            &NoEffects,
            &[huge.clone(), Datum::Real(2.0)],
            1024,
        ));
        assert_eq!(
            eager_bounded(
                &NoEffects,
                &[Datum::Null, Datum::MaxValue, huge.clone()],
                1024
            ),
            Err(EvalError::Unsupported("range sentinel INTERVAL argument"))
        );
        let views = [Datum::Int(1), Datum::Int(2)];
        assert_eq!(
            eager_bounded(&NoEffects, &views, 2 * std::mem::size_of::<Input>()),
            Ok(Datum::Int(0))
        );
        let real = FieldType::new(tidb_datatype::FieldTypeCode::Double).with_flags(1);
        let types = vec![Some(real); 4];
        let seen = std::cell::RefCell::new(vec![]);
        let values = [
            Datum::Real(100.0),
            huge.clone(),
            Datum::Real(10.0),
            Datum::Real(20.0),
        ];
        assert_eq!(
            lazy_bounded(
                &NoEffects,
                &types,
                |i| {
                    seen.borrow_mut().push(i);
                    assert_ne!(i, 1);
                    Ok(values[i].clone())
                },
                1024
            ),
            Ok(Datum::Int(3))
        );
        assert_eq!(*seen.borrow(), vec![0, 2, 3]);
        seen.borrow_mut().clear();
        denied(lazy_bounded(
            &NoEffects,
            &types,
            |i| {
                seen.borrow_mut().push(i);
                Ok(if i == 0 {
                    Datum::Real(0.0)
                } else {
                    values[i].clone()
                })
            },
            1024,
        ));
        assert_eq!(*seen.borrow(), vec![0, 2, 1]);
        assert_eq!(
            lazy_bounded(
                &NoEffects,
                &types,
                |i| {
                    assert_eq!(i, 0);
                    Ok(Datum::Null)
                },
                0
            ),
            Ok(Datum::Int(-1))
        );
    }
}
