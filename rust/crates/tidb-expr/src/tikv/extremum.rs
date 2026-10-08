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

use std::cmp::Ordering;

use tidb_datatype::{Collation, EvalType, FieldType, FieldTypeCode};
use tidb_query_expr as extrema_sdk;

use crate::builtin_ext::{GlCmpStringMode, GlSignature};
use crate::{Columns, Datum, EvalError};

use super::adapter_failure::{ExpressionAdapterFailure, ScopeFailureKind};
use super::{
    evaluate_args_in, evaluate_prepared_args_scoped_in, identity_value, EvaluatedArgs,
    EvaluatedBytesOp as Op, EvaluatedBytesResult, ExpressionRuntimeFailure, LocalError,
};
use extrema_sdk::{NativeExtremumRequest as Request, NativeExtremumResult as Report};

fn extremum_signature_metadata(value: GlSignature) -> extrema_sdk::NativeExtremumSignature {
    use extrema_sdk::{NativeExtremumEvalType as Type, NativeExtremumStringMode as Mode};
    extrema_sdk::NativeExtremumSignature {
        arg_type: match value.arg_type {
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
        cmp_string_mode: match value.cmp_string_mode {
            GlCmpStringMode::Directly => Mode::Directly,
            GlCmpStringMode::AsDate => Mode::AsDate,
            GlCmpStringMode::AsDatetime => Mode::AsDatetime,
        },
        ret_date: value.ret_date,
    }
}

// Keep this existing coercion boundary: Real uses Rust Display, while Float32
// and the remaining kinds take Datum::to_bytes. This is not winner selection.
fn extremum_string_value(value: &Datum) -> Result<Vec<u8>, EvalError> {
    Ok(match value {
        Datum::String(value) => value.bytes().to_vec(),
        Datum::Bytes(value) => value.clone(),
        Datum::Int(value) => value.to_string().into_bytes(),
        Datum::UInt(value) => value.to_string().into_bytes(),
        Datum::Decimal(value) => value.to_string().into_bytes(),
        Datum::Real(value) => value.to_string().into_bytes(),
        Datum::Null => return Err(EvalError::Unsupported("NULL string operand")),
        Datum::MinNotNull | Datum::MaxValue => {
            return Err(EvalError::Unsupported("range sentinel string operand"));
        }
        other => other
            .to_bytes()
            .map_err(|_| EvalError::Unsupported("datum string conversion"))?,
    })
}

fn invalid_report() -> EvalError {
    EvalError::ExpressionAdapterFailure(ExpressionAdapterFailure::from_scope(
        ScopeFailureKind::Contract,
        "invalid native extremum report",
    ))
}

fn frame_error(error: extrema_sdk::NativeIdentityFrameError) -> EvalError {
    match error {
        extrema_sdk::NativeIdentityFrameError::Invalid => invalid_report(),
        extrema_sdk::NativeIdentityFrameError::Capacity => {
            EvalError::ExpressionRuntimeFailure(ExpressionRuntimeFailure::from_local_eval(
                LocalError::ResourceLimit("native extremum frame allocation or size failed".into()),
                None,
            ))
        }
    }
}

#[derive(Clone, Copy)]
enum Stage {
    Head,
    Numeric,
    Time,
    Vector,
    String,
    TimeText,
    TimeContext,
    Finish,
}

fn request_allowed(stage: Stage, request: Request) -> bool {
    match stage {
        Stage::Head => matches!(
            request,
            Request::CompareLt
                | Request::CompareGt
                | Request::CastTime
                | Request::CastVector
                | Request::StringBytes
                | Request::TimeText
                | Request::Original
                | Request::ToReal
                | Request::ToDecimal
        ),
        Stage::Numeric => matches!(
            request,
            Request::CompareLt
                | Request::CompareGt
                | Request::Original
                | Request::ToReal
                | Request::ToDecimal
        ),
        Stage::Time => matches!(request, Request::CastTime),
        Stage::Vector => matches!(request, Request::CastVector),
        Stage::String => matches!(request, Request::StringBytes),
        Stage::TimeText => matches!(request, Request::TimeContext),
        Stage::TimeContext => matches!(request, Request::TimeText),
        Stage::Finish => false,
    }
}

pub(crate) fn eval_extremum_in(
    ctx: &dyn Columns,
    vals: &[Datum],
    want: Ordering,
    signature: Option<GlSignature>,
    arg_decimals: &[i64],
    all_constant: bool,
    collation: Collation,
) -> Result<Datum, EvalError> {
    evaluate_prepared_args_scoped_in(
        ctx,
        || {
            // The head receives actual identities, not frontend classifications
            // or a preselected winner. All subsequent demands come from the SDK.
            let identities = vals
                .iter()
                .map(identity_value::encode)
                .collect::<Result<Vec<_>, _>>()?;
            let views = identities.iter().map(Option::as_deref).collect::<Vec<_>>();
            let packet = extrema_sdk::encode_native_extremum_head(
                &views,
                signature.map(extremum_signature_metadata),
                arg_decimals,
                all_constant,
            )
            .map_err(frame_error)?;
            Ok((
                Op::ExtremumHeadNative,
                EvaluatedArgs::BytesIntInt(
                    Some(packet),
                    Some(match want {
                        Ordering::Less => -1,
                        Ordering::Equal => 0,
                        Ordering::Greater => 1,
                    }),
                    Some(
                        tidb_datatype::get_collator(collation.name())
                            .new_collation()
                            .map_or(super::NativeCollation::Binary, Collation::native_policy)
                            .tag(),
                    ),
                ),
            ))
        },
        |computed, selected| {
            let mut report = computed.into_bytes()?;
            let mut stage = Stage::Head;
            loop {
                let Some(state) = report else {
                    return if matches!(stage, Stage::Head | Stage::Time | Stage::TimeText) {
                        Ok(Datum::Null)
                    } else {
                        Err(invalid_report())
                    };
                };
                let (kind, index, best_index) =
                    match extrema_sdk::decode_native_extremum_result(&state)
                        .ok_or_else(invalid_report)?
                    {
                        Report::BadArity if matches!(stage, Stage::Head) => {
                            return Err(EvalError::Unsupported("bad function arity"));
                        }
                        Report::Value(value)
                            if matches!(stage, Stage::Time | Stage::Vector | Stage::Finish) =>
                        {
                            let value = identity_value::decode(Some(value.to_vec()))?;
                            return match (stage, &value) {
                                (Stage::Time, Datum::Time(_))
                                | (Stage::Vector, Datum::VectorFloat32(_)) => Ok(value),
                                (Stage::Finish, _) => Ok(value),
                                _ => Err(invalid_report()),
                            };
                        }
                        Report::RetagString(value)
                            if matches!(stage, Stage::String | Stage::TimeContext) =>
                        {
                            return Ok(Datum::new_string(value.to_vec()));
                        }
                        Report::Request {
                            kind,
                            index,
                            best_index,
                            ..
                        } if request_allowed(stage, kind) => (kind, index, best_index),
                        _ => return Err(invalid_report()),
                    };
                let value = vals.get(index).ok_or_else(invalid_report)?;
                // No recursive iterator or stack of live workers: the selected
                // authority outlives this streaming loop, and each reply is owned.
                let (operation, next_stage) = match kind {
                    Request::CompareLt | Request::CompareGt => {
                        (Op::ExtremumNumericNative, Stage::Numeric)
                    }
                    Request::CastTime => (Op::ExtremumTimeNative, Stage::Time),
                    Request::CastVector => (Op::ExtremumVectorNative, Stage::Vector),
                    Request::StringBytes => (Op::ExtremumStringNative, Stage::String),
                    Request::TimeText => (Op::ExtremumTimeTextNative, Stage::TimeText),
                    Request::TimeContext => (Op::ExtremumTimeContextNative, Stage::TimeContext),
                    Request::Original | Request::ToReal | Request::ToDecimal => {
                        (Op::ExtremumFinishNative, Stage::Finish)
                    }
                };
                report = evaluate_args_in(
                    operation,
                    selected,
                    || {
                        if matches!(kind, Request::TimeContext) {
                            let modes = ctx.date_modes();
                            let zone = ctx.time_zone();
                            return Ok(EvaluatedArgs::TemporalText {
                                value: state,
                                modes: i64::from(modes.allow_invalid_dates),
                                zone,
                            });
                        }
                        let actual = match kind {
                            Request::CompareLt | Request::CompareGt => {
                                let best = vals.get(best_index).ok_or_else(invalid_report)?;
                                let op = match kind {
                                    Request::CompareLt => tidb_ast::BinaryOp::Lt,
                                    _ => tidb_ast::BinaryOp::Gt,
                                };
                                let scope =
                                    selected.ready_value_scope().ok_or_else(invalid_report)?;
                                // Preserve eval_binary's exact context-free policy,
                                // but charge the real selected execution and scope.
                                let comparison =
                                    scope.with_columns(&crate::NoColumns, |defaults| {
                                        crate::ops::eval_binary_with_div_precision(
                                            op,
                                            value.clone(),
                                            best.clone(),
                                            4,
                                            defaults,
                                        )
                                    })?;
                                identity_value::encode(&comparison)?
                            }
                            Request::CastTime => {
                                let cast = crate::cast::cast_arg_as_datetime(value, None, ctx)?;
                                if !matches!(cast, Datum::Time(_) | Datum::Null) {
                                    return Err(invalid_report());
                                }
                                identity_value::encode(&cast)?
                            }
                            Request::CastVector => {
                                let cast = value
                                    .convert_to(
                                        &FieldType::new(FieldTypeCode::VectorFloat32),
                                        tidb_datatype::ConversionFlags::default(),
                                    )
                                    .map_err(|error| EvalError::Vector(error.to_string()))?
                                    .value;
                                if !matches!(cast, Datum::VectorFloat32(_)) {
                                    return Err(invalid_report());
                                }
                                identity_value::encode(&cast)?
                            }
                            Request::StringBytes => Some(extremum_string_value(value)?),
                            Request::TimeText => {
                                crate::coerce::coerce_str(value)?.map(String::into_bytes)
                            }
                            Request::Original => identity_value::encode(value)?,
                            Request::ToReal => identity_value::encode(&Datum::Real(
                                crate::ops::to_f64(value.clone()),
                            ))?,
                            Request::ToDecimal => identity_value::encode(&Datum::Decimal(
                                crate::ops::to_decimal(value.clone()),
                            ))?,
                            Request::TimeContext => return Err(invalid_report()),
                        };
                        // Forward the WHOLE SDK report, retaining its real capacity.
                        Ok(EvaluatedArgs::Bytes2(Some(state), actual))
                    },
                    EvaluatedBytesResult::into_bytes,
                )?;
                stage = next_stage;
            }
        },
    )
}

#[cfg(test)]
mod tests {
    use std::cell::{Cell, RefCell};
    use std::panic::{catch_unwind, AssertUnwindSafe};

    use tidb_datatype::{
        CoreTime, DateModes, MySqlDuration, SessionTimeZone, Time, TimeType, VectorFloat32,
    };

    use super::*;
    use crate::{ExpressionAdapterFailureClass, ReadyValuePoolOwner, ReadyValuePoolPolicy};

    #[test]
    fn extremum_bridge_splits_default_comparison_policy_from_actual_scope_and_context() {
        struct Statement {
            events: RefCell<Vec<&'static str>>,
            big_zone: Cell<bool>,
            panic_zone: Cell<bool>,
        }
        impl Columns for Statement {
            fn get(&self, _: &[String]) -> Option<Datum> {
                panic!("ready extremum values")
            }
            fn div_precision_increment(&self) -> u32 {
                panic!("numeric comparison must use default 4")
            }
            fn handle_truncate(&self, _: &str) -> Result<(), EvalError> {
                panic!("numeric comparison must keep NoColumns truncation policy")
            }
            fn append_warning(&self, _: u16, _: &str) {
                panic!("no statement warning in these fixtures")
            }
            fn date_modes(&self) -> DateModes {
                self.events.borrow_mut().push("mode");
                DateModes::default()
            }
            fn now(&self) -> Option<(i64, u32, i32)> {
                self.events.borrow_mut().push("now");
                Some((0, 0, 0))
            }
            fn time_zone(&self) -> SessionTimeZone {
                self.events.borrow_mut().push("zone");
                assert!(!self.panic_zone.get(), "late original zone getter panic");
                if self.big_zone.get() {
                    let mut name = String::with_capacity(128 << 10);
                    name.push_str("UTC");
                    SessionTimeZone::Fixed {
                        name,
                        offset_secs: 0,
                    }
                } else {
                    SessionTimeZone::utc()
                }
            }
        }
        let statement = Statement {
            events: RefCell::new(Vec::new()),
            big_zone: Cell::new(false),
            panic_zone: Cell::new(false),
        };
        let owner = |slots| {
            ReadyValuePoolOwner::new(
                ReadyValuePoolPolicy::checked(
                    slots,
                    slots,
                    16 << 20,
                    1 << 20,
                    2 << 20,
                    64,
                    16,
                    1 << 16,
                )
                .unwrap(),
            )
            .unwrap()
        };
        let text = |value: &str| Datum::new_string(value);
        let signature = |arg_type, cmp_string_mode| {
            Some(GlSignature {
                arg_type,
                cmp_string_mode,
                ret_date: false,
            })
        };
        let numeric = signature(EvalType::Int, GlCmpStringMode::Directly);
        let date_string = signature(EvalType::String, GlCmpStringMode::AsDate);
        let epoch = Datum::Time(Time::from_raw_parts(
            CoreTime::from_date(1970, 1, 1, 0, 0, 0, 0),
            TimeType::DateTime,
            0,
        ));
        for slots in [0, 1] {
            let owner = owner(slots);
            let execution = owner.begin_execution().unwrap();
            let scope = execution.scope();
            scope.with_columns(&statement, |bound| {
                for want in [Ordering::Less, Ordering::Greater] {
                    let greatest = want == Ordering::Greater;
                    for (values, signature, expected, events) in [
                        (
                            vec![Datum::Null, Datum::MaxValue],
                            None,
                            Datum::Null,
                            vec![],
                        ),
                        // The original numeric path deliberately compares under
                        // NoColumns even when the statement overrides truncation.
                        (
                            vec![text("2x"), Datum::Int(1)],
                            numeric,
                            if greatest { text("2x") } else { Datum::Int(1) },
                            vec![],
                        ),
                        (
                            vec![text("a"), text("b")],
                            None,
                            text(if greatest { "b" } else { "a" }),
                            vec![],
                        ),
                        (
                            vec![Datum::Duration(MySqlDuration::from_raw_parts(0, 0))],
                            signature(EvalType::Datetime, GlCmpStringMode::Directly),
                            epoch.clone(),
                            vec!["mode", "now"],
                        ),
                        (
                            vec![text("[]")],
                            signature(EvalType::VectorFloat32, GlCmpStringMode::Directly),
                            Datum::new_vector_float32(VectorFloat32::default()),
                            vec![],
                        ),
                        (
                            vec![text("2024-1-2"), text("2023-1-2")],
                            date_string,
                            text(if greatest { "2024-01-02" } else { "2023-01-02" }),
                            vec!["mode", "zone", "mode", "zone"],
                        ),
                        (
                            vec![Datum::Int(1), Datum::Real(2.5)],
                            None,
                            Datum::Real(if greatest { 2.5 } else { 1.0 }),
                            vec![],
                        ),
                    ] {
                        statement.events.borrow_mut().clear();
                        let result = eval_extremum_in(
                            bound,
                            &values,
                            want,
                            signature,
                            &[],
                            false,
                            Collation::Utf8Mb4Bin,
                        );
                        if slots == 0 {
                            assert!(
                                matches!(result, Err(EvalError::ExpressionAdapterFailure(failure))
                                if failure.class() == ExpressionAdapterFailureClass::PoolResource)
                            );
                            assert!(statement.events.borrow().is_empty());
                        } else {
                            assert_eq!(
                                identity_value::encode(&result.unwrap()).unwrap(),
                                identity_value::encode(&expected).unwrap()
                            );
                            assert_eq!(*statement.events.borrow(), events);
                        }
                    }
                }
                if slots == 1 {
                    statement.big_zone.set(true);
                    let refused = eval_extremum_in(
                        bound,
                        &[text("2024-1-2")],
                        Ordering::Greater,
                        date_string,
                        &[],
                        false,
                        Collation::Utf8Mb4Bin,
                    );
                    assert!(
                        matches!(refused, Err(EvalError::ExpressionRuntimeFailure(failure))
                        if matches!(failure.local_error(), LocalError::ResourceLimit(_)))
                    );
                    statement.big_zone.set(false);
                    assert_eq!(
                        eval_extremum_in(
                            bound,
                            &[text("2024-1-2")],
                            Ordering::Greater,
                            date_string,
                            &[],
                            false,
                            Collation::Utf8Mb4Bin
                        ),
                        Ok(text("2024-01-02"))
                    );
                }
            });
            drop(scope);
            execution.close();
        }
        let owner = owner(1);
        let execution = owner.begin_execution().unwrap();
        let scope = execution.scope();
        statement.panic_zone.set(true);
        statement.events.borrow_mut().clear();
        assert!(catch_unwind(AssertUnwindSafe(|| scope.with_columns(
            &statement,
            |bound| {
                let _ = eval_extremum_in(
                    bound,
                    &[text("2024-1-2")],
                    Ordering::Greater,
                    date_string,
                    &[],
                    false,
                    Collation::Utf8Mb4Bin,
                );
            }
        )))
        .is_err());
        assert_eq!(*statement.events.borrow(), vec!["mode", "zone"]);
        statement.panic_zone.set(false);
        scope.with_columns(&statement, |bound| {
            assert!(
                matches!(eval_extremum_in(bound, &[Datum::Int(1)], Ordering::Greater,
                None, &[], false, Collation::Utf8Mb4Bin),
                Err(EvalError::ExpressionAdapterFailure(failure))
                    if failure.class() == ExpressionAdapterFailureClass::ScopePoisoned)
            );
        });
        drop(scope);
        execution.close();
        assert_eq!(
            eval_extremum_in(
                &crate::NoColumns,
                &[Datum::Int(1), Datum::Int(2)],
                Ordering::Greater,
                None,
                &[],
                false,
                Collation::Utf8Mb4Bin
            ),
            Ok(Datum::Int(2))
        );
        // This process-wide source switch requires the serialized test gate.
        // Restore even on assertion unwind, including the charset defaults.
        struct RestoreCollation(bool);
        impl Drop for RestoreCollation {
            fn drop(&mut self) {
                tidb_datatype::set_new_collation_enabled(self.0);
            }
        }
        let _restore = RestoreCollation(tidb_datatype::new_collation_enabled());
        for enabled in [false, true] {
            tidb_datatype::set_new_collation_enabled(enabled);
            assert_eq!(
                eval_extremum_in(
                    &crate::NoColumns,
                    &[text("a"), text("B")],
                    Ordering::Greater,
                    None,
                    &[],
                    false,
                    Collation::Utf8Mb4GeneralCi
                ),
                Ok(text(if enabled { "B" } else { "a" }))
            );
            assert_eq!(
                eval_extremum_in(
                    &crate::NoColumns,
                    &[text("a"), text("a ")],
                    Ordering::Greater,
                    None,
                    &[],
                    false,
                    Collation::Utf8Mb4Bin
                ),
                Ok(text(if enabled { "a" } else { "a " }))
            );
        }
    }
}
