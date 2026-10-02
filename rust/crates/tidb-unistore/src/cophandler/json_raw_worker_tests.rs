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

use super::{convert_expr, LegacyEvalError, LegacyEvaluator, SimpleExpr, SimpleSig};
use tidb_datatype::{BinaryJSON, BinaryJSONValue, Datum, Opaque, SessionTimeZone, Time, TimeType};
use tidb_proto::tipb;

#[test]
fn legacy_json_merge_patch_worker_preserves_raw_order_absence_and_child_demand() {
    let zone = SessionTimeZone::utc();
    let row = [Datum::Null];
    let evaluator = LegacyEvaluator::new(&row, 4, &zone);
    let json = |text: &str| BinaryJSON::parse(text).unwrap();
    let leaf = |text: &str| SimpleExpr::Json(json(text));
    let call = |children| SimpleExpr::Func(SimpleSig::JsonMergePatchSig, children);
    let bad = BinaryJSON::from_encoded_parts(tidb_datatype::JSON_TYPE_CODE_ARRAY, Vec::new());
    // These are fixed documents, not answers obtained from another provider.
    for (children, expected) in [
        (
            vec![
                leaf("{\"a\":1,\"b\":2}"),
                leaf("{\"a\":null}"),
                leaf("{\"a\":3}"),
            ],
            Some(json("{\"a\":3,\"b\":2}")),
        ),
        (
            vec![
                leaf("{\"a\":1,\"b\":2}"),
                leaf("{\"a\":3}"),
                leaf("{\"a\":null}"),
            ],
            Some(json("{\"b\":2}")),
        ),
        (vec![leaf("[1,2]"), leaf("[3]")], Some(json("[3]"))),
        (
            vec![leaf("null"), leaf("{\"a\":1}")],
            Some(json("{\"a\":1}")),
        ),
        (vec![leaf("{\"a\":1}"), leaf("null")], Some(json("null"))),
        // Legacy stops at SQL NULL; it cannot adopt native PATCH's recovery
        // from SQL NULL followed by a non-object JSON document.
        (
            vec![SimpleExpr::Null, leaf("null"), leaf("{\"a\":1}")],
            None,
        ),
        (
            vec![SimpleExpr::Column(0), leaf("null"), leaf("{\"a\":1}")],
            None,
        ),
        (vec![], None),
        (vec![leaf("{\"a\":1}")], Some(json("{\"a\":1}"))),
        // The raw codec validates the first document before a later scalar
        // can replace it. Its business error becomes absence inside the worker.
        (vec![SimpleExpr::Json(bad.clone()), leaf("null")], None),
    ] {
        let expression = call(children);
        assert_eq!(
            evaluator.eval_json(Some(&expression)).unwrap(),
            expected,
            "{expression:?}"
        );
    }
    let absent = call(vec![]);
    assert_eq!(evaluator.eval_expr(&absent).unwrap(), Some(0));
    assert_eq!(evaluator.folded_int(Some(&absent)).unwrap(), Some(0));
    let present = call(vec![leaf("null")]);
    assert_eq!(evaluator.eval_expr(&present).unwrap(), Some(1));
    let time =
        Time::from_date_checked(2024, 1, 2, 3, 4, 5, 600_000, TimeType::DateTime, 6).unwrap();
    for value in [
        BinaryJSON::from_opaque(Opaque {
            type_code: 233,
            bytes: vec![1, 2, 3],
        }),
        BinaryJSON::from_time(time),
        BinaryJSON::from_typed_value(&BinaryJSONValue::Uint64(u64::MAX)).unwrap(),
    ] {
        let expression = call(vec![leaf("0"), SimpleExpr::Json(value.clone())]);
        let result = evaluator.eval_json(Some(&expression)).unwrap().unwrap();
        // Input payload identity, not a Display or mutation-helper oracle.
        assert_eq!(result.type_code(), value.type_code());
        assert_eq!(result.value(), value.value());
    }
    let owner = tidb_expr::AsciiPoolOwner::new(
        tidb_expr::AsciiPoolPolicy::checked(
            0,
            0,
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
    let assert_pool = |error| match error {
        LegacyEvalError::Infrastructure(tidb_expr::EvalError::ExpressionAdapterFailure(
            failure,
        )) => {
            assert_eq!(
                failure.class(),
                tidb_expr::ExpressionAdapterFailureClass::PoolResource
            );
            assert_eq!(
                failure.origin(),
                tidb_expr::ExpressionAdapterFailureOrigin::Pool
            );
        }
        other => panic!("merge-patch infrastructure error was softened: {other:?}"),
    };
    execution
        .scope()
        .with_columns(&tidb_expr::NoColumns, |columns| {
            let scoped = LegacyEvaluator {
                raw_columns: columns,
                ..LegacyEvaluator::new(&row, 4, &zone)
            };
            // Ten root-only cases use actual raw/plain leaves, never a different
            // worker child that could supply the expected resource error.
            for children in [
                vec![],
                vec![leaf("{}")],
                vec![leaf("[1,2]"), leaf("[3]")],
                vec![SimpleExpr::Null],
                vec![SimpleExpr::Column(0)],
                vec![SimpleExpr::Column(9)],
                vec![SimpleExpr::Int(1)],
                vec![SimpleExpr::Json(bad.clone()), leaf("null")],
                vec![leaf("{}"), SimpleExpr::Null, leaf("[1]")],
                vec![leaf("null"), leaf("{\"a\":1}")],
            ] {
                let expression = call(children);
                assert_pool(
                    scoped
                        .eval_json(Some(&expression))
                        .expect_err("raw PATCH requires its own worker"),
                );
                assert_pool(
                    scoped
                        .eval_expr(&expression)
                        .expect_err("predicate must keep infrastructure"),
                );
                assert_pool(
                    scoped
                        .folded_int(Some(&expression))
                        .expect_err("fold must keep infrastructure"),
                );
            }
            // Separate child-only scope: the root retains its normal raw_columns.
            let shared = convert_expr(&tipb::Expr {
                tp: Some(tipb::ExprType::ScalarFunc as i32),
                sig: Some(tipb::ScalarFuncSig::IntIsNull as i32),
                field_type: Some(tipb::FieldType {
                    tp: Some(8),
                    ..Default::default()
                }),
                children: vec![tipb::Expr {
                    tp: Some(tipb::ExprType::Null as i32),
                    ..Default::default()
                }],
                ..Default::default()
            })
            .unwrap();
            let child_only = LegacyEvaluator {
                shared_override: Some(columns),
                ..LegacyEvaluator::new(&row, 4, &zone)
            };
            for children in [
                vec![SimpleExpr::Null, shared.clone()],
                vec![leaf("{}"), SimpleExpr::Column(0), shared.clone()],
                vec![SimpleExpr::Column(9), shared.clone()],
                vec![
                    SimpleExpr::Json(bad.clone()),
                    SimpleExpr::Null,
                    shared.clone(),
                ],
            ] {
                assert_eq!(child_only.eval_json(Some(&call(children))).unwrap(), None);
            }
            for children in [
                vec![leaf("{}"), shared.clone(), SimpleExpr::Null],
                vec![SimpleExpr::Json(bad.clone()), shared.clone()],
                vec![shared.clone(), SimpleExpr::Null],
            ] {
                assert_pool(
                    child_only
                        .eval_json(Some(&call(children)))
                        .expect_err("demanded child precedes raw codec and later NULL"),
                );
            }
        });
}

#[test]
fn legacy_json_raw_workers_preserve_values_presence_codecs_and_child_demand() {
    let zone = SessionTimeZone::utc();
    let row = [Datum::Null];
    let evaluator = LegacyEvaluator::new(&row, 4, &zone);
    let json = |text: &str| BinaryJSON::parse(text).unwrap();
    let leaf = |text: &str| SimpleExpr::Json(json(text));
    let path = |text: &str| SimpleExpr::Bytes(text.as_bytes().to_vec());
    let call = |sig, children| SimpleExpr::Func(sig, children);
    let replace = SimpleSig::JsonReplaceSig;
    let append = SimpleSig::JsonArrayAppendSig;
    let bad = BinaryJSON::from_encoded_parts(tidb_datatype::JSON_TYPE_CODE_ARRAY, Vec::new());

    // Literal expected documents, not results from another mutation provider.
    for (sig, children, expected) in [
        (
            replace,
            vec![leaf("{\"a\":1}"), path("$.a"), leaf("9")],
            Some(json("{\"a\":9}")),
        ),
        (
            append,
            vec![leaf("{\"a\":[1]}"), path("$.a"), leaf("2")],
            Some(json("{\"a\":[1,2]}")),
        ),
        (
            append,
            vec![leaf("{\"a\":1}"), path("$.a"), leaf("2")],
            None,
        ),
        (
            replace,
            vec![leaf("{\"a\":1}"), path("$.a"), SimpleExpr::Null],
            Some(json("{\"a\":null}")),
        ),
        (
            append,
            vec![leaf("{\"a\":[1]}"), path("$.a"), SimpleExpr::Null],
            Some(json("{\"a\":[1,null]}")),
        ),
        (
            replace,
            vec![leaf("{\"a\":1}"), path("$.a"), SimpleExpr::Column(0)],
            None,
        ),
        (
            append,
            vec![leaf("{\"a\":[1]}"), path("$.a"), SimpleExpr::Column(0)],
            None,
        ),
        // REPLACE decodes a value even at a missing target; APPEND does not.
        (
            replace,
            vec![leaf("{}"), path("$.missing"), SimpleExpr::Json(bad.clone())],
            None,
        ),
        (
            append,
            vec![leaf("{}"), path("$.missing"), SimpleExpr::Json(bad.clone())],
            Some(json("{}")),
        ),
        // REPLACE(empty) decodes/reencodes; APPEND(empty) preserves raw bytes.
        (replace, vec![SimpleExpr::Json(bad.clone())], None),
        (
            append,
            vec![SimpleExpr::Json(bad.clone())],
            Some(bad.clone()),
        ),
        // An extraction codec error is the legacy APPEND missing-target no-op.
        (
            append,
            vec![SimpleExpr::Json(bad.clone()), path("$"), leaf("1")],
            Some(bad.clone()),
        ),
    ] {
        let expression = call(sig, children);
        assert_eq!(
            evaluator.eval_json(Some(&expression)).unwrap(),
            expected,
            "{expression:?}"
        );
    }
    // The bare-predicate caller uses document presence, not JSON truthiness.
    let terminal = call(append, vec![leaf("{\"a\":1}"), path("$.a"), leaf("2")]);
    assert_eq!(evaluator.eval_expr(&terminal).unwrap(), Some(0));
    assert_eq!(evaluator.folded_int(Some(&terminal)).unwrap(), Some(0));
    let malformed_identity = call(append, vec![SimpleExpr::Json(bad.clone())]);
    assert_eq!(evaluator.eval_expr(&malformed_identity).unwrap(), Some(1));

    // Compare raw typed scalar payloads with their input identities. Display
    // would collapse opaque/time/unsigned distinctions and is not an oracle.
    let time =
        Time::from_date_checked(2024, 1, 2, 3, 4, 5, 600_000, TimeType::DateTime, 6).unwrap();
    for value in [
        BinaryJSON::from_opaque(Opaque {
            type_code: 233,
            bytes: vec![1, 2, 3],
        }),
        BinaryJSON::from_time(time),
        BinaryJSON::from_typed_value(&BinaryJSONValue::Uint64(u64::MAX)).unwrap(),
    ] {
        let expression = call(
            replace,
            vec![leaf("0"), path("$"), SimpleExpr::Json(value.clone())],
        );
        let result = evaluator.eval_json(Some(&expression)).unwrap().unwrap();
        assert_eq!(result.type_code(), value.type_code());
        assert_eq!(result.value(), value.value());
        let expression = call(
            append,
            vec![leaf("[]"), path("$"), SimpleExpr::Json(value.clone())],
        );
        let result = evaluator.eval_json(Some(&expression)).unwrap().unwrap();
        assert_eq!(result.element_count().unwrap(), 1);
        let scalar = result.array_get(0).unwrap().unwrap();
        assert_eq!(scalar.type_code(), value.type_code());
        assert_eq!(scalar.value(), value.value());
    }

    let owner = tidb_expr::AsciiPoolOwner::new(
        tidb_expr::AsciiPoolPolicy::checked(
            0,
            0,
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
    let assert_pool = |error| match error {
        LegacyEvalError::Infrastructure(tidb_expr::EvalError::ExpressionAdapterFailure(
            failure,
        )) => {
            assert_eq!(
                failure.class(),
                tidb_expr::ExpressionAdapterFailureClass::PoolResource
            );
            assert_eq!(
                failure.origin(),
                tidb_expr::ExpressionAdapterFailureOrigin::Pool
            );
        }
        other => panic!("raw JSON infrastructure error was lost: {other:?}"),
    };
    execution
        .scope()
        .with_columns(&tidb_expr::NoColumns, |columns| {
            let scoped = LegacyEvaluator {
                raw_columns: columns,
                ..LegacyEvaluator::new(&row, 4, &zone)
            };
            // All children here are plain legacy leaves: no other worker can mask
            // the root's own admission. Missing child, absent column and observed
            // NULL/non-JSON document are real caller outcomes, never dummy values.
            for sig in [replace, append] {
                for children in [
                    vec![],
                    vec![SimpleExpr::Null],
                    vec![SimpleExpr::Column(9)],
                    vec![SimpleExpr::Int(1)],
                    vec![leaf("{\"a\":[]}")],
                    vec![leaf("{\"a\":[]}"), path("$.a")],
                    vec![leaf("{\"a\":[]}"), path("$.a"), leaf("9")],
                    vec![leaf("{\"a\":[]}"), path("$.missing"), leaf("9")],
                    vec![leaf("{\"a\":[]}"), SimpleExpr::Null, leaf("9")],
                    vec![leaf("{\"a\":[]}"), path("$.a"), SimpleExpr::Column(0)],
                    vec![leaf("{\"a\":[]}"), path("$.a"), SimpleExpr::Null],
                ] {
                    let expression = call(sig, children);
                    assert_pool(
                        scoped
                            .eval_json(Some(&expression))
                            .expect_err("raw JSON root requires worker"),
                    );
                    assert_pool(
                        scoped
                            .eval_expr(&expression)
                            .expect_err("bare predicate must retain infrastructure"),
                    );
                    assert_pool(
                        scoped
                            .folded_int(Some(&expression))
                            .expect_err("fold cannot turn infrastructure into NULL"),
                    );
                }
            }

            // A real already-admitted shared child is the demand probe. Its pool
            // fails while the legacy root uses its normal pool; this cannot be
            // confused with the root-only zero-slot checks above.
            let shared = convert_expr(&tipb::Expr {
                tp: Some(tipb::ExprType::ScalarFunc as i32),
                sig: Some(tipb::ScalarFuncSig::IntIsNull as i32),
                field_type: Some(tipb::FieldType {
                    tp: Some(8),
                    ..Default::default()
                }),
                children: vec![tipb::Expr {
                    tp: Some(tipb::ExprType::Null as i32),
                    ..Default::default()
                }],
                ..Default::default()
            })
            .unwrap();
            let child_only = LegacyEvaluator {
                shared_override: Some(columns),
                ..LegacyEvaluator::new(&row, 4, &zone)
            };
            for sig in [replace, append] {
                for children in [
                    vec![SimpleExpr::Null, path("$.a"), shared.clone()],
                    vec![leaf("{\"a\":[]}"), path("bad path"), shared.clone()],
                    vec![leaf("{\"a\":[]}"), SimpleExpr::Null, shared.clone()],
                ] {
                    assert_eq!(
                        child_only.eval_json(Some(&call(sig, children))).unwrap(),
                        None
                    );
                }
                let dangling = call(sig, vec![leaf("{\"a\":[]}"), shared.clone()]);
                assert_eq!(
                    child_only.eval_json(Some(&dangling)).unwrap(),
                    Some(json("{\"a\":[]}"))
                );
                for first_path in ["$.missing", "$.*"] {
                    let expression = call(
                        sig,
                        vec![leaf("{\"a\":[]}"), path(first_path), shared.clone()],
                    );
                    assert_pool(
                        child_only
                            .eval_json(Some(&expression))
                            .expect_err("current value precedes target business checks"),
                    );
                }
                let expression = call(sig, vec![shared.clone(), path("$.a"), leaf("1")]);
                assert_pool(
                    child_only
                        .eval_json(Some(&expression))
                        .expect_err("document is demanded first"),
                );
                let expression = call(
                    sig,
                    vec![
                        leaf("{\"a\":[]}"),
                        path("$.a"),
                        shared.clone(),
                        path("bad path"),
                        leaf("2"),
                    ],
                );
                assert_pool(
                    child_only
                        .eval_json(Some(&expression))
                        .expect_err("first value precedes later path parsing"),
                );
            }
            // REPLACE prepares the entire list before a business/codec failure;
            // APPEND performs one pair at a time and stops before the suffix.
            for (doc, first_path, value) in [
                (leaf("{\"a\":1}"), path("$.a"), leaf("9")),
                (leaf("{\"a\":[]}"), path("$.*"), leaf("9")),
                (
                    leaf("{\"a\":[]}"),
                    path("$.a"),
                    SimpleExpr::Json(bad.clone()),
                ),
            ] {
                let children = vec![doc, first_path, value, path("$.a"), shared.clone()];
                assert_pool(
                    child_only
                        .eval_json(Some(&call(replace, children.clone())))
                        .expect_err("REPLACE full preparation demands suffix"),
                );
                assert_eq!(
                    child_only.eval_json(Some(&call(append, children))).unwrap(),
                    None
                );
            }
            // Missing targets skip raw value decoding, not child evaluation; an
            // extraction codec error also continues to the next actual child.
            for (doc, value) in [
                (leaf("{\"a\":[]}"), SimpleExpr::Json(bad.clone())),
                (SimpleExpr::Json(bad.clone()), leaf("9")),
            ] {
                let expression = call(
                    append,
                    vec![doc, path("$.missing"), value, path("$.a"), shared.clone()],
                );
                assert_pool(
                    child_only
                        .eval_json(Some(&expression))
                        .expect_err("no-op pair continues to suffix"),
                );
            }
        });
}
