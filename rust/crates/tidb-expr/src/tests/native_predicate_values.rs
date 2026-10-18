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

//! Independent old-program witnesses for normalized predicates and demand markers.
use crate::{
    BooleanFunction as B, Columns, Datum, EvalError, LogicalArgs, LogicalFunction as L,
    ReadyValueCache,
};
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
        panic!("normalized predicate must not fetch a source")
    }
    fn ready_value_cache(&self) -> Option<&ReadyValueCache> {
        assert!(
            !self.reject_cache,
            "predicate attempted retired Ready worker lookup"
        );
        self.cache.as_ref()
    }
    fn truncate_level(&self) -> crate::ErrorLevel {
        panic!("normalized predicate must not coerce")
    }
    fn div_precision_increment(&self) -> u32 {
        panic!("normalized predicate must not read arithmetic config")
    }
    fn append_warning(&self, _: u16, _: &str) {
        panic!("normalized predicate must not repeat warnings")
    }
}
fn direct_boolean(function: B, ready: Option<bool>) -> Datum {
    use tidb_query_expr::impl_op::{
        int_is_false, int_is_true, is_null, unary_not_int, KeepNullOff, KeepNullOn,
    };
    let value = ready.map(i64::from);
    let computed = match function {
        B::UnaryNot => unary_not_int(value.as_ref()),
        B::IsNull => is_null::<i64>(value.as_ref()),
        B::IsTrue => int_is_true::<KeepNullOff>(value.as_ref()),
        B::IsFalse => int_is_false::<KeepNullOff>(value.as_ref()),
        B::IsTrueWithNull => int_is_true::<KeepNullOn>(value.as_ref()),
        B::IsNotNull => is_null::<i64>(value.as_ref()).and_then(|v| unary_not_int(v.as_ref())),
        B::IsNotTrue => {
            int_is_true::<KeepNullOff>(value.as_ref()).and_then(|v| unary_not_int(v.as_ref()))
        }
        B::IsNotFalse => {
            int_is_false::<KeepNullOff>(value.as_ref()).and_then(|v| unary_not_int(v.as_ref()))
        }
    }
    .unwrap();
    computed.map_or(Datum::Null, Datum::Int)
}
fn direct_logical(function: L, args: LogicalArgs) -> Option<Datum> {
    let (left, right) = match args {
        LogicalArgs::Both(left, right) => (left, right),
        LogicalArgs::UndemandedRight { left } => {
            if !matches!(
                (function, left),
                (L::And, Some(false)) | (L::Or, Some(true))
            ) {
                return None;
            }
            (left, Some(false))
        }
    };
    let left = left.map(i64::from);
    let right = right.map(i64::from);
    let computed = match function {
        L::And => tidb_query_expr::impl_op::logical_and(left.as_ref(), right.as_ref()),
        L::Or => tidb_query_expr::impl_op::logical_or(left.as_ref(), right.as_ref()),
        L::Xor => tidb_query_expr::impl_op::logical_xor(left.as_ref(), right.as_ref()),
    }
    .unwrap();
    Some(computed.map_or(Datum::Null, Datum::Int))
}
fn witness(hash: &mut u64, result: &Result<Datum, EvalError>) {
    for byte in format!("{result:?}\n").bytes() {
        *hash = (*hash ^ u64::from(byte)).wrapping_mul(0x100000001b3);
    }
}
#[test]
fn boolean_and_logical_values_match_original_program_witness() {
    let mut hash = 0xcbf29ce484222325;
    let mut cases = 0;
    for mode in 0..3 {
        let reused = Context::new(true);
        for _ in 0..4 {
            for function in [
                B::UnaryNot,
                B::IsNull,
                B::IsTrue,
                B::IsFalse,
                B::IsTrueWithNull,
                B::IsNotNull,
                B::IsNotTrue,
                B::IsNotFalse,
            ] {
                for ready in [None, Some(false), Some(true)] {
                    let fresh = Context::new(mode == 1);
                    let context = if mode == 2 { &reused } else { &fresh };
                    let expected = crate::eval_boolean_ready_in(function, ready, context);
                    assert_eq!(expected, Ok(direct_boolean(function, ready)));
                    witness(&mut hash, &expected);
                    cases += 1;
                }
            }
            for function in [L::And, L::Or, L::Xor] {
                for left in [None, Some(false), Some(true)] {
                    let args = [
                        LogicalArgs::UndemandedRight { left },
                        LogicalArgs::Both(left, None),
                        LogicalArgs::Both(left, Some(false)),
                        LogicalArgs::Both(left, Some(true)),
                    ];
                    for args in args {
                        let fresh = Context::new(mode == 1);
                        let context = if mode == 2 { &reused } else { &fresh };
                        let expected = crate::eval_logical_ready_in(function, args, context);
                        match direct_logical(function, args) {
                            Some(value) => assert_eq!(expected, Ok(value)),
                            None => {
                                let Err(EvalError::ExpressionAdapterFailure(ref failure)) =
                                    expected
                                else {
                                    panic!("invalid marker lost adapter failure")
                                };
                                assert_eq!(
                                    failure.class(),
                                    crate::ExpressionAdapterFailureClass::ScopeContract
                                );
                                assert_eq!(
                                    failure.origin(),
                                    crate::ExpressionAdapterFailureOrigin::Scope
                                );
                                let rejecting = Context {
                                    cache: None,
                                    reject_cache: true,
                                };
                                assert!(matches!(
                                    crate::eval_logical_ready_in(function, args, &rejecting),
                                    Err(EvalError::ExpressionAdapterFailure(_))
                                ));
                            }
                        }
                        witness(&mut hash, &expected);
                        cases += 1;
                    }
                }
            }
        }
    }
    assert_eq!(cases, 720);
    // Captured from independently active old workers before removal. Reproduce
    // with N6t's test-only patch at473b648a/SDK6a36ba0, never from new callers.
    assert_eq!(
        hash, 0x8c5608c8b554ec31,
        "original predicate result witness"
    );
}

#[test]
fn boolean_predicate_entries_do_not_acquire_ready_workers() {
    let rejecting = Context {
        cache: None,
        reject_cache: true,
    };
    for function in [
        B::UnaryNot,
        B::IsNull,
        B::IsTrue,
        B::IsFalse,
        B::IsTrueWithNull,
        B::IsNotNull,
        B::IsNotTrue,
        B::IsNotFalse,
    ] {
        for ready in [None, Some(false), Some(true)] {
            assert_eq!(
                crate::eval_boolean_ready_in(function, ready, &rejecting),
                Ok(direct_boolean(function, ready))
            );
        }
    }
}
#[test]
fn logical_predicate_entries_do_not_acquire_ready_workers() {
    let rejecting = Context {
        cache: None,
        reject_cache: true,
    };
    for function in [L::And, L::Or, L::Xor] {
        for left in [None, Some(false), Some(true)] {
            for args in [
                LogicalArgs::UndemandedRight { left },
                LogicalArgs::Both(left, None),
                LogicalArgs::Both(left, Some(false)),
                LogicalArgs::Both(left, Some(true)),
            ] {
                match direct_logical(function, args) {
                    Some(value) => assert_eq!(
                        crate::eval_logical_ready_in(function, args, &rejecting),
                        Ok(value)
                    ),
                    None => assert!(matches!(
                        crate::eval_logical_ready_in(function, args, &rejecting),
                        Err(EvalError::ExpressionAdapterFailure(_))
                    )),
                }
            }
        }
    }
}
