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

//! `JSON_SEARCH`: preserve source argument demand while the shared worker owns
//! the string-leaf walk, LIKE matching, full-path deduplication and JSON output.
//! Its array-selection rule deliberately differs from JSON_EXTRACT: an array
//! leg selects only arrays, never an otherwise eligible non-array value.

use super::path::parse_path;
use super::value::parse_json_document_argument;
use crate::coerce::coerce_str;
use crate::{Columns, Datum, EvalError, JsonError};

/// `JSON_SEARCH(json_doc, one_or_all, search_str [, escape_char [, path] ...])`.
/// The dispatcher supplies the original minimum arity. Prepare every demanded
/// path before execution, even when an earlier path could satisfy `one` mode.
pub(super) fn json_search(vals: &[Datum], ctx: &dyn Columns) -> Result<Datum, EvalError> {
    use crate::tikv::{EvaluatedArgs, EvaluatedBytesOp as Op};
    crate::tikv::evaluate_prepared_args_in(
        ctx,
        || {
            let null = || (Op::JsonOutputNullNative, EvaluatedArgs::NullWitness(None));
            let Some(document) = parse_json_document_argument(&vals[0])? else {
                return Ok(null());
            };
            let Some(mode) = coerce_str(&vals[1])? else {
                return Ok(null());
            };
            // This family reports 3150, not JSON_CONTAINS_PATH's 3154.
            let one = tidb_query_expr::parse_native_json_search_mode(&mode)
                .ok_or(EvalError::Json(JsonError::InvalidContainsPathType))?;
            let Some(pattern) = coerce_str(&vals[2])? else {
                return Ok(null());
            };
            let escape = match vals.get(3) {
                None | Some(Datum::Null) => '\\',
                Some(value) => {
                    let Some(value) = coerce_str(value)? else {
                        return Ok(null());
                    };
                    if value.is_empty() {
                        '\\'
                    } else if value.chars().count() == 1 {
                        value.chars().next().expect("one character is present")
                    } else {
                        return Err(EvalError::Unsupported("JSON_SEARCH escape length"));
                    }
                }
            };
            let mut paths = Vec::new();
            if vals.len() > 4 {
                for value in &vals[4..] {
                    let Some(path) = coerce_str(value)? else {
                        return Ok(null());
                    };
                    paths.push(parse_path(&path)?);
                }
            }
            Ok((
                Op::JsonSearchSerdeNative,
                crate::tikv::prepare_json_search_args(&document, &paths, one, &pattern, escape)?,
            ))
        },
        |computed| {
            Ok(computed
                .into_bytes()?
                .map_or(Datum::Null, Datum::new_string))
        },
    )
}

#[cfg(test)]
#[test]
fn json_search_workers_preserve_root_demand_and_escape_boundaries() {
    struct NoWarnings;
    impl Columns for NoWarnings {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }
        fn append_warning(&self, _: u16, _: &str) {
            panic!("JSON_SEARCH must not invent a warning")
        }
        fn time_zone(&self) -> crate::context::SessionTimeZone {
            panic!("JSON_SEARCH has no timezone demand")
        }
    }
    let s = |text: &str| Datum::new_string(text);
    for slots in [1, 0] {
        let owner = crate::ReadyValuePoolOwner::new(
            crate::ReadyValuePoolPolicy::checked(
                slots,
                slots,
                16 * 1024 * 1024,
                4 * 1024 * 1024,
                4 * 1024 * 1024,
                64,
                8,
                4 * 1024 * 1024,
            )
            .unwrap(),
        )
        .unwrap();
        let execution = owner.begin_execution().unwrap();
        let ctx = NoWarnings;
        let eval = |values: &[Datum]| {
            execution.scope().with_columns(&ctx, |columns| {
                super::dispatch_in("JSON_SEARCH", values, columns).expect("valid search arity")
            })
        };
        let check = |values: Vec<Datum>, expected: Datum| {
            let result = eval(&values);
            if slots == 1 {
                assert_eq!(result.unwrap(), expected);
            } else {
                let error =
                    result.expect_err("JSON_SEARCH must retain the supplied zero-slot scope");
                let EvalError::ExpressionAdapterFailure(failure) = error else {
                    panic!("{error:?}")
                };
                assert_eq!(
                    failure.class(),
                    crate::ExpressionAdapterFailureClass::PoolResource
                );
                assert_eq!(
                    failure.origin(),
                    crate::ExpressionAdapterFailureOrigin::Pool
                );
            }
        };
        check(vec![s("\"x\""), s("ONE"), s("x")], s("\"$\""));
        check(vec![s("\"x\""), s("all"), s("x"), Datum::Null], s("\"$\""));
        check(vec![s("\"x\""), s("all"), s("x"), s("")], s("\"$\""));
        check(
            vec![
                s("[\"x\",\"x\"]"),
                s("one"),
                s("x"),
                Datum::Null,
                s("$[1]"),
                s("$[0]"),
            ],
            s("\"$[1]\""),
        );
        check(
            vec![
                s("{\"a\":{\"a\":\"x\",\"b\":\"x\"}}"),
                s("all"),
                s("x"),
                Datum::Null,
                s("$**.a"),
            ],
            s("[\"$.a.a\", \"$.a.b\"]"),
        );
        check(
            vec![
                s("{\"a\":\"x\"}"),
                s("all"),
                s("x"),
                Datum::Null,
                s("$[0].a"),
            ],
            Datum::Null,
        );
        check(vec![s("null"), s("all"), s("%")], Datum::Null);
        check(vec![s("\"x\""), s("all"), s("no-hit")], Datum::Null);
        // A trailing escape tests the next literal but deliberately leaves the
        // rest of the input unconstrained in this frozen source matcher.
        check(
            vec![
                s("[\"abλtail\",\"abλ\",\"ab\"]"),
                s("all"),
                s("abλ"),
                s("λ"),
            ],
            s("[\"$[0]\", \"$[1]\"]"),
        );
        check(
            vec![s("[\"%tail\",\"%\",\"x\"]"), s("all"), s("%"), s("%")],
            s("[\"$[0]\", \"$[1]\"]"),
        );
        check(
            vec![s("[\"%tail\",\"%\",\"x\"]"), s("all"), s("λ%"), s("λ")],
            s("\"$[1]\""),
        );
        let invalid = || Datum::new_bytes(vec![0xff]);
        for values in [
            vec![Datum::Null, invalid(), invalid()],
            vec![s("\"x\""), Datum::Null, invalid()],
            vec![s("\"x\""), s("one"), Datum::Null, invalid()],
            vec![
                s("\"x\""),
                s("one"),
                s("x"),
                Datum::Null,
                s("$"),
                Datum::Null,
                invalid(),
            ],
        ] {
            check(values, Datum::Null);
        }
        assert!(matches!(
            eval(&[s("{"), s("bad-mode"), Datum::Null]),
            Err(EvalError::Json(JsonError::InvalidText))
        ));
        for mode in ["bad-mode", " one"] {
            assert!(matches!(
                eval(&[s("\"x\""), s(mode), invalid()]),
                Err(EvalError::Json(JsonError::InvalidContainsPathType))
            ));
        }
        assert!(matches!(
            eval(&[s("\"x\""), invalid(), Datum::Null]),
            Err(EvalError::Unsupported("invalid UTF-8 byte datum"))
        ));
        assert!(matches!(
            eval(&[s("\"x\""), s("one"), s("x"), s("ab"), Datum::Null]),
            Err(EvalError::Unsupported("JSON_SEARCH escape length"))
        ));
        // Neither a known first hit nor a later NULL hides an earlier invalid
        // path. A NULL escape selects the default escape rather than NULL.
        for paths in [
            vec![s("$"), s("bad-path")],
            vec![s("bad-path"), Datum::Null],
        ] {
            let mut values = vec![s("\"x\""), s("one"), s("x"), Datum::Null];
            values.extend(paths);
            assert!(matches!(
                eval(&values),
                Err(EvalError::Json(JsonError::InvalidPath(_)))
            ));
        }
        assert!(matches!(
            eval(&[s("\"x\""), s("one"), s("x"), invalid(), Datum::Null]),
            Err(EvalError::Unsupported("invalid UTF-8 byte datum"))
        ));
        assert!(super::dispatch_in("JSON_SEARCH", &[Datum::Null, Datum::Null], &ctx).is_none());
    }
}
