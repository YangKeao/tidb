// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Independent old already-cast typed-IN output and arity-contract witnesses.
use super::*;
use crate::{Columns, Datum, EvalError};
use tidb_datatype::{BinaryJSON, MySqlDuration, Time, TimeType};
use tidb_query_expr::NativeInTypedDomain as Domain;
struct Context {
    cache: Option<ReadyValueCache>,
    reject_cache: bool,
}
impl Context {
    fn new(cached: bool) -> Self {
        Self {
            cache: cached.then(ReadyValueCache::new),
            reject_cache: false,
        }
    }
}
impl Columns for Context {
    fn get(&self, _: &[String]) -> Option<Datum> {
        panic!("already-cast IN must not fetch a source")
    }
    fn ready_value_cache(&self) -> Option<&ReadyValueCache> {
        assert!(
            !self.reject_cache,
            "typed IN attempted retired Ready worker lookup"
        );
        self.cache.as_ref()
    }
    fn time_zone(&self) -> tidb_datatype::SessionTimeZone {
        panic!("already-cast IN must not re-read time zone")
    }
    fn truncate_level(&self) -> crate::ErrorLevel {
        panic!("already-cast IN must not cast")
    }
    fn div_precision_increment(&self) -> u32 {
        panic!("already-cast IN must not read comparison config")
    }
    fn append_warning(&self, _: u16, _: &str) {
        panic!("already-cast IN must not repeat warnings")
    }
}
fn fixtures() -> Vec<Datum> {
    let mut values = super::control_value_tests::fixtures();
    let time = values
        .iter()
        .find_map(|v| {
            if let Datum::Time(v) = v {
                Some(*v)
            } else {
                None
            }
        })
        .unwrap();
    let duration = values
        .iter()
        .find_map(|v| {
            if let Datum::Duration(v) = v {
                Some(*v)
            } else {
                None
            }
        })
        .unwrap();
    values.extend([
        Datum::Time(Time::from_raw_parts(time.core_time(), TimeType::Date, 0)),
        Datum::Duration(MySqlDuration::from_raw_parts(duration.nanoseconds(), 0)),
        Datum::Json(BinaryJSON::parse("1").unwrap()),
        Datum::Json(BinaryJSON::parse("1.0").unwrap()),
        Datum::Json(BinaryJSON::parse("\"1\"").unwrap()),
    ]);
    assert_eq!(values.len(), 45);
    values
}
fn direct_value(
    domain: Domain,
    values: &[Datum],
) -> Result<Option<bool>, tidb_query_expr::NativeInTypedArityError> {
    use tidb_query_expr::NativeInTypedValue as V;
    tidb_query_expr::native_in_typed_values(
        domain,
        values.iter().map(|value| match value {
            Datum::Null => None,
            Datum::Time(value) => Some(V::Time(value.core_time().raw())),
            Datum::Duration(value) => Some(V::Duration(value.nanoseconds())),
            Datum::Json(value) => Some(V::Json {
                type_code: value.type_code(),
                bytes: value.value(),
            }),
            _ => Some(V::Other),
        }),
    )
}
fn witness(hash: &mut u64, value: &Result<Datum, EvalError>) {
    for byte in format!("{value:?}\n").bytes() {
        *hash = (*hash ^ u64::from(byte)).wrapping_mul(0x100000001b3);
    }
}
fn arity_error(value: &Result<Datum, EvalError>) {
    let Err(EvalError::ExpressionAdapterFailure(error)) = value else {
        panic!("typed IN arity must remain adapter contract")
    };
    assert_eq!(error.class(), ExpressionAdapterFailureClass::ScopeContract);
    assert_eq!(error.origin(), ExpressionAdapterFailureOrigin::Scope);
}
#[test]
fn borrowed_typed_in_original_value_witness() {
    let values = fixtures();
    let mut hash = 0xcbf29ce484222325;
    let mut cases = 0;
    for mode in 0..3 {
        let reused = Context::new(true);
        for domain in [
            Domain::Datetime,
            Domain::Timestamp,
            Domain::Duration,
            Domain::Json,
        ] {
            for first in &values {
                for second in &values {
                    for input in [
                        vec![first.clone(), second.clone()],
                        vec![first.clone(), Datum::Null, second.clone()],
                        vec![first.clone(), second.clone(), Datum::Null],
                        vec![first.clone(), first.clone(), second.clone()],
                    ] {
                        let fresh = Context::new(mode == 1);
                        let ctx = if mode == 2 { &reused } else { &fresh };
                        let actual = eval_in_typed_values_in(ctx, domain, &input);
                        assert!(
                            matches!(actual, Ok(Datum::Null | Datum::Int(0 | 1))),
                            "actual typed result: {actual:?}"
                        );
                        let expected = direct_value(domain, &input)
                            .unwrap()
                            .map_or(Datum::Null, |v| Datum::Int(i64::from(v)));
                        assert_eq!(
                            actual,
                            Ok(expected),
                            "direct borrowed typed value differs from OLD frame"
                        );
                        witness(&mut hash, &actual);
                        cases += 1;
                    }
                }
                let fresh = Context::new(mode == 1);
                let ctx = if mode == 2 { &reused } else { &fresh };
                let actual = eval_in_typed_values_in(ctx, domain, std::slice::from_ref(first));
                arity_error(&actual);
                assert_eq!(
                    direct_value(domain, std::slice::from_ref(first)),
                    Err(tidb_query_expr::NativeInTypedArityError { actual: 1 })
                );
                witness(&mut hash, &actual);
                cases += 1;
            }
            let fresh = Context::new(mode == 1);
            let ctx = if mode == 2 { &reused } else { &fresh };
            let actual = eval_in_typed_values_in(ctx, domain, &[]);
            arity_error(&actual);
            assert_eq!(
                direct_value(domain, &[]),
                Err(tidb_query_expr::NativeInTypedArityError { actual: 0 })
            );
            witness(&mut hash, &actual);
            cases += 1;
        }
    }
    assert_eq!(cases, 97752);
    // Captured before either old typed-frame membership or this frontend
    // wrapper was replaced. Reproduce at0ab81038/SDK6a36ba0 with N6v's patch;
    // never regenerate from the borrowed value implementation.
    assert_eq!(
        hash, 0xe128f6d8e515d162,
        "original typed IN value/contract witness"
    );
}
#[test]
fn typed_in_value_worker_route_must_be_retired() {
    let ctx = Context {
        cache: None,
        reject_cache: true,
    };
    let value = Datum::Duration(MySqlDuration::from_raw_parts(7, 0));
    assert_eq!(
        eval_in_typed_values_in(&ctx, Domain::Duration, &[value.clone(), value]),
        Ok(Datum::Int(1))
    );
}
