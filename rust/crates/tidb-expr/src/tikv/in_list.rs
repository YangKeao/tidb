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
use super::{
    evaluate_args_in, evaluate_prepared_args_scoped_in, identity_value, EvaluatedArgs,
    EvaluatedBytesOp as Op, EvaluatedBytesResult, ExpressionRuntimeFailure, LocalError,
};
use crate::{Columns, Datum, EvalError};
use tidb_query_expr::{NativeInRequest as Request, NativeInResult as Report};

/// Only the already-evaluated, already-cast temporal/JSON IN branch enters here.
/// The caller retains its all-evaluation then all-casting barrier.
pub(crate) fn eval_in_typed_values_in(
    ctx: &dyn Columns,
    domain: tidb_query_expr::NativeInTypedDomain,
    values: &[Datum],
) -> Result<Datum, EvalError> {
    evaluate_args_in(
        Op::InTypedValuesNative,
        ctx,
        || {
            let identities = values
                .iter()
                .map(identity_value::encode)
                .collect::<Result<Vec<_>, _>>()?;
            let views = identities.iter().map(Option::as_deref).collect::<Vec<_>>();
            let packet = tidb_query_expr::encode_native_in_typed_values(domain, &views)
                .map_err(frame_error)?;
            Ok(EvaluatedArgs::Bytes(Some(packet)))
        },
        |computed| {
            let report = computed.into_bytes()?.ok_or_else(invalid_report)?;
            match tidb_query_expr::decode_native_in_result(&report).ok_or_else(invalid_report)? {
                Report::Null => Ok(Datum::Null),
                Report::Bool(value) => Ok(Datum::Int(i64::from(value))),
                Report::Request { .. } => Err(invalid_report()),
            }
        },
    )
}

#[derive(Clone, Copy)]
enum LegacyKind {
    Int,
    Bytes,
}

fn drive<E: From<EvalError>>(
    computed: EvaluatedBytesResult,
    selected: &dyn Columns,
    kind: LegacyKind,
    count: usize,
    eval: &mut impl FnMut(usize, &dyn Columns) -> Result<Option<Vec<u8>>, E>,
) -> Result<Option<i128>, E> {
    let mut state = computed
        .into_bytes()
        .map_err(E::from)?
        .ok_or_else(|| E::from(invalid_report()))?;
    let mut at_head = true;
    loop {
        let report = tidb_query_expr::decode_native_in_result(&state)
            .ok_or_else(|| E::from(invalid_report()))?;
        let request = match report {
            Report::Null if !at_head => return Ok(None),
            Report::Bool(value) if !at_head => return Ok(Some(i128::from(value))),
            Report::Request { kind: request, .. } => request,
            _ => return Err(E::from(invalid_report())),
        };
        let actual = match (kind, request) {
            (LegacyKind::Int, Request::Int128 { index })
            | (LegacyKind::Bytes, Request::Bytes { index }) => {
                // The original reader is asked for operand zero even for an
                // empty child list; it supplies its actual missing-child NULL.
                if (at_head && index != 0) || (index != 0 && index >= count) {
                    return Err(E::from(invalid_report()));
                }
                eval(index, selected)?
            }
            (LegacyKind::Bytes, Request::Collation { collation_id }) if !at_head => {
                // Resolve only when the SDK requests the original late
                // non-NULL-pair lookup. Do not snapshot this mutable mode in
                // the head and do not return a host-computed equality result.
                let tag = tidb_datatype::get_collator_by_id(collation_id)
                    .new_collation()
                    .map_or(
                        super::NativeCollation::Binary,
                        tidb_datatype::Collation::native_policy,
                    )
                    .tag();
                Some(tag.to_le_bytes().to_vec())
            }
            _ => return Err(E::from(invalid_report())),
        };
        state = evaluate_args_in(
            Op::InLegacyStepNative,
            selected,
            || Ok(EvaluatedArgs::Bytes2(Some(state), actual)),
            |computed| computed.into_bytes()?.ok_or_else(invalid_report),
        )
        .map_err(E::from)?;
        at_head = false;
    }
}

fn legacy<E: From<EvalError>>(
    ctx: &dyn Columns,
    kind: LegacyKind,
    count: usize,
    metadata: impl FnOnce() -> Result<Vec<u8>, tidb_query_expr::NativeIdentityFrameError>,
    mut eval: impl FnMut(usize, &dyn Columns) -> Result<Option<Vec<u8>>, E>,
) -> Result<Option<i128>, E> {
    evaluate_prepared_args_scoped_in(
        ctx,
        || {
            let operation = match kind {
                LegacyKind::Int => Op::InLegacyIntHeadNative,
                LegacyKind::Bytes => Op::InLegacyStringHeadNative,
            };
            Ok((
                operation,
                EvaluatedArgs::Bytes(Some(metadata().map_err(frame_error)?)),
            ))
        },
        |computed, selected| {
            // Preserve the original callback E under the live head pack guard;
            // no artificial EvalError, SQL folding, or extra worker is involved.
            Ok(drive(computed, selected, kind, count, &mut eval))
        },
    )
    .map_err(E::from)?
}

/// Legacy integer IN reads the caller's native integer channel, not a folded
/// surrogate. Its full i128 domain and original errors survive unchanged.
pub fn eval_legacy_in_int_in<E: From<EvalError>>(
    ctx: &dyn Columns,
    count: usize,
    mut eval: impl FnMut(usize, &dyn Columns) -> Result<Option<i128>, E>,
) -> Result<Option<i128>, E> {
    legacy(
        ctx,
        LegacyKind::Int,
        count,
        || tidb_query_expr::encode_native_in_legacy_int_head(count),
        |index, selected| {
            eval(index, selected).map(|value| value.map(|value| value.to_le_bytes().to_vec()))
        },
    )
}

/// Legacy byte IN retains the caller's byte-channel SQL folding and receives
/// the actual collation ID; only the SDK owns comparison and list progress.
pub fn eval_legacy_in_bytes_in<E: From<EvalError>>(
    ctx: &dyn Columns,
    count: usize,
    collation: i32,
    eval: impl FnMut(usize, &dyn Columns) -> Result<Option<Vec<u8>>, E>,
) -> Result<Option<i128>, E> {
    legacy(
        ctx,
        LegacyKind::Bytes,
        count,
        || tidb_query_expr::encode_native_in_legacy_string_head(count, collation),
        eval,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AsciiPoolOwner, AsciiPoolPolicy, ExpressionAdapterFailureClass};
    use std::panic::{catch_unwind, AssertUnwindSafe};
    use tidb_datatype::{
        BinaryJSON, CoreTime, DateModes, MySqlDuration, SessionTimeZone, Time, TimeType,
    };
    use tidb_query_expr::NativeInTypedDomain as Domain;

    #[derive(Debug, PartialEq)]
    enum ChildError {
        Native(EvalError),
        Original,
    }
    impl From<EvalError> for ChildError {
        fn from(value: EvalError) -> Self {
            Self::Native(value)
        }
    }

    #[test]
    fn partial_in_bridge_keeps_cast_values_legacy_demand_late_collation_and_scope_lifetime() {
        struct Original;
        impl Columns for Original {
            fn get(&self, _: &[String]) -> Option<Datum> {
                panic!("original callback owns child reads")
            }
            fn date_modes(&self) -> DateModes {
                panic!("typed casts already finished")
            }
            fn time_zone(&self) -> SessionTimeZone {
                panic!("no new temporal context read")
            }
            fn now(&self) -> Option<(i64, u32, i32)> {
                panic!("no new clock demand")
            }
        }
        let owner = |slots| {
            AsciiPoolOwner::new(
                AsciiPoolPolicy::checked(slots, slots, 16 << 20, 1 << 20, 2 << 20, 64, 16, 1 << 16)
                    .unwrap(),
            )
            .unwrap()
        };
        let time = Time::from_raw_parts(
            CoreTime::from_date(2024, 1, 1, 0, 0, 0, 0),
            TimeType::DateTime,
            0,
        );
        let timestamp = Time::from_raw_parts(time.core_time(), TimeType::Timestamp, 0);
        let json_null = Datum::Json(BinaryJSON::parse("null").unwrap());
        for slots in [0, 1] {
            let owner = owner(slots);
            let execution = owner.begin_execution().unwrap();
            let scope = execution.scope();
            scope.with_columns(&Original, |bound| {
                for (domain, values, expected) in [
                    (Domain::Datetime, vec![Datum::Time(time), Datum::Time(time)], Datum::Int(1)),
                    (Domain::Timestamp, vec![Datum::Time(timestamp), Datum::Time(timestamp)], Datum::Int(1)),
                    (Domain::Duration, vec![Datum::new_duration(MySqlDuration::from_raw_parts(1_000_000_000, 0)),
                        Datum::new_duration(MySqlDuration::from_raw_parts(2_000_000_000, 0))], Datum::Int(0)),
                    (Domain::Json, vec![json_null.clone(), Datum::Null, json_null.clone()], Datum::Int(1)),
                    (Domain::Json, vec![Datum::Null, json_null.clone()], Datum::Null),
                ] {
                    let actual = eval_in_typed_values_in(bound, domain, &values);
                    if slots == 0 {
                        assert!(matches!(actual, Err(EvalError::ExpressionAdapterFailure(failure))
                            if failure.class() == ExpressionAdapterFailureClass::PoolResource));
                    } else { assert_eq!(actual, Ok(expected)); }
                }
                let mut visited = Vec::new();
                let actual = eval_legacy_in_int_in(bound, 3, |index, selected| -> Result<Option<i128>, ChildError> {
                    assert!(std::ptr::eq(selected.evaluated_ascii_scope().unwrap(), &scope));
                    visited.push(index);
                    if index == 2 { return Err(ChildError::Original); }
                    Ok(Some(i128::MAX))
                });
                if slots == 0 {
                    assert!(matches!(actual, Err(ChildError::Native(EvalError::ExpressionAdapterFailure(failure)))
                        if failure.class() == ExpressionAdapterFailureClass::PoolResource));
                    assert!(visited.is_empty());
                } else {
                    assert_eq!(actual, Ok(Some(1)));
                    assert_eq!(visited, vec![0, 1]);
                }
                visited.clear();
                let actual = eval_legacy_in_bytes_in(bound, 4, 46, |index, selected| -> Result<Option<Vec<u8>>, ChildError> {
                    assert!(std::ptr::eq(selected.evaluated_ascii_scope().unwrap(), &scope));
                    visited.push(index);
                    match index {
                        0 | 2 => Ok(Some(b"a".to_vec())),
                        1 => Ok(None),
                        _ => Err(ChildError::Original),
                    }
                });
                if slots == 0 {
                    assert!(matches!(actual, Err(ChildError::Native(EvalError::ExpressionAdapterFailure(failure)))
                        if failure.class() == ExpressionAdapterFailureClass::PoolResource));
                    assert!(visited.is_empty());
                } else {
                    assert_eq!(actual, Ok(Some(1)));
                    assert_eq!(visited, vec![0, 1, 2]);
                    assert_eq!(eval_legacy_in_int_in(bound, 2, |_, _| -> Result<Option<i128>, ChildError> {
                        Err(ChildError::Original)
                    }), Err(ChildError::Original));
                    let refused = eval_legacy_in_bytes_in(bound, 2, 46, |index, _| -> Result<Option<Vec<u8>>, ChildError> {
                        assert_eq!(index, 0);
                        Ok(Some(vec![b'a'; 128 << 10]))
                    });
                    assert!(matches!(refused, Err(ChildError::Native(EvalError::ExpressionRuntimeFailure(failure)))
                        if matches!(failure.local_error(), LocalError::ResourceLimit(_))));
                    let mut visited = Vec::new();
                    assert_eq!(eval_legacy_in_int_in(bound, 0, |index, _| -> Result<Option<i128>, ChildError> {
                        visited.push(index);
                        Ok(None)
                    }), Ok(None));
                    assert_eq!(visited, vec![0]);
                }
            });
            drop(scope);
            execution.close();
        }
        let owner = owner(1);
        let execution = owner.begin_execution().unwrap();
        let scope = execution.scope();
        assert!(catch_unwind(AssertUnwindSafe(|| scope.with_columns(
            &Original,
            |bound| {
                let _ = eval_legacy_in_int_in(
                    bound,
                    2,
                    |index, _| -> Result<Option<i128>, ChildError> {
                        if index == 1 {
                            panic!("late original child panic");
                        }
                        Ok(Some(1))
                    },
                );
            }
        )))
        .is_err());
        scope.with_columns(&Original, |bound| {
            assert!(matches!(eval_legacy_in_int_in(bound, 0, |_, _| -> Result<Option<i128>, ChildError> {
                panic!("poisoned before child")
            }), Err(ChildError::Native(EvalError::ExpressionAdapterFailure(failure)))
                if failure.class() == ExpressionAdapterFailureClass::ScopePoisoned));
        });
        drop(scope);
        execution.close();
        assert_eq!(
            eval_legacy_in_int_in(
                &crate::NoColumns,
                0,
                |index, selected| -> Result<Option<i128>, ChildError> {
                    assert_eq!(index, 0);
                    assert!(selected.evaluated_ascii_scope().is_some());
                    assert_eq!(
                        super::super::eval_interval_in(selected, &[Datum::Int(1), Datum::Int(2)]),
                        Ok(Datum::Int(0))
                    );
                    Ok(None)
                }
            ),
            Ok(None)
        );
        // Process-wide source mode changes require the serialized native gate.
        struct Restore(bool);
        impl Drop for Restore {
            fn drop(&mut self) {
                tidb_datatype::set_new_collation_enabled(self.0);
            }
        }
        let _restore = Restore(tidb_datatype::new_collation_enabled());
        tidb_datatype::set_new_collation_enabled(false);
        assert_eq!(
            eval_legacy_in_bytes_in(
                &crate::NoColumns,
                3,
                45,
                |index, _| -> Result<Option<Vec<u8>>, ChildError> {
                    Ok(Some(match index {
                        0 => b"a".to_vec(),
                        1 => {
                            tidb_datatype::set_new_collation_enabled(true);
                            b"A".to_vec()
                        }
                        _ => panic!("lookup must observe mode after candidate evaluation"),
                    }))
                }
            ),
            Ok(Some(1))
        );
        let mut visited = Vec::new();
        assert_eq!(
            eval_legacy_in_bytes_in(
                &crate::NoColumns,
                4,
                45,
                |index, _| -> Result<Option<Vec<u8>>, ChildError> {
                    visited.push(index);
                    Ok(Some(match index {
                        0 | 3 => b"a".to_vec(),
                        1 => b"b".to_vec(),
                        2 => {
                            tidb_datatype::set_new_collation_enabled(false);
                            b"A".to_vec()
                        }
                        _ => panic!("unexpected child index"),
                    }))
                }
            ),
            Ok(Some(1))
        );
        assert_eq!(visited, vec![0, 1, 2, 3]);
    }
}

fn invalid_report() -> EvalError {
    EvalError::ExpressionAdapterFailure(ExpressionAdapterFailure::from_scope(
        ScopeFailureKind::Contract,
        "invalid native IN report",
    ))
}

fn frame_error(error: tidb_query_expr::NativeIdentityFrameError) -> EvalError {
    match error {
        tidb_query_expr::NativeIdentityFrameError::Invalid => invalid_report(),
        tidb_query_expr::NativeIdentityFrameError::Capacity => {
            EvalError::ExpressionRuntimeFailure(ExpressionRuntimeFailure::from_ascii_local(
                LocalError::ResourceLimit("native IN frame allocation or size failed".into()),
                None,
            ))
        }
    }
}
