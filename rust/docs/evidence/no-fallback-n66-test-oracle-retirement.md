# Retire test-only recursive evaluators (2026-10-29)

YangKeao requested this retirement in thread message c49af5d4. It follows the production fallback removal delivered at TiDB source `29451228a9`, report `d4df288ab2`, and SDK `8f675914`. The old implementations and their activation switches are now removed from test builds as well. SDK algorithms, execution limits, and host compatibility decisions are unchanged.

## Source changes

Remove the recursive AST module, Expression/ScalarFunction and protobuf dispatch, native typed/Decimal batch walkers, Unistore's old typed/Datum methods, and retained-original control modules. Remove their old-only lazy/stateful wrappers and counters. `test_evaluation.rs` observes current shared driver calls and source preparation; it cannot select an old evaluator.

Public/prepared and cached/uncached entrypoints retain their fixed-result and lifecycle checks. Existing JSON fixtures are byte-identical. The Unistore primary `940248/B02C0B8B9BDDD00C`, scope `66048/3F895141D852AA15`, and eager value-kernel `796554/B0601ED2964A5721` assertions remain fixed. The eager kernel test calls the existing value primitive without traversing expression children.

The nonrecursive Unistore `eval_shared` test helper remains: it invokes the current public SDK and supports existing cache regression tests. Value-only algorithms and diagnostic formatters are not old tree evaluators.

## Retired reference contracts

Some original-only observers described the explicitly selected old interpreter, rather than the default public path. Their replacement requires an explicit change of test contract, not a silent golden update.

The diagnostic observer recorded `7200/A4F6DB5E8D40E458` under the old-interpreter switch. The DEFAULT public entry was captured twice from pre-deletion commit `d4df288ab2`; both captures were `7200/80E3F7155D1FD890`, identical to the post-deletion public path. The replacement is explicitly named `typed_dynamic_public_diagnostic_empty_row_effects_and_short_circuits`. The old digest remains here and in its source comment. This difference predates the cleanup; the cleanup does not fix or claim equality between these contracts.

The original-only unsealed PB capture and duplicated old-versus-current runs are retired. The real Projection and sealed-PB golden corpora remain.

## Unbounded reference data and finite execution

The old reference accepted malformed Decimal declarations beyond the SDK's existing finite limits. Capture their reference results before deletion with the original hard assertions enabled, then remove the code again. `batch_retired_boundary_original.json` holds 260 reference-only rows: 60 arithmetic and 200 membership. Its SHA256 is `59141827dc1e37ece512d1ef2160ac613f9a5f6a00472f7854b84490240f1fc6`. The original raw digests `07DA5D6EBAB0FE48` and `0BBCF87024DEBD9F`, and live terminal-resource counts 60 and 79, remain unchanged. These records never supply runtime results.

For the larger membership corpus, independently capture actual SDK outputs from the pre-deletion source after every old-versus-SDK equality assertion. The original whole-reference assertion still passes at `1144320/0926C1DF1FCFFBE1`. The admitted cohort is `1141600/6DCAFE78EBD1BFF5`, with exactly 2,720 terminal resource cases. The final test still executes all 1,144,320 inputs: it checks rejected-case identity, unchanged sentinel output and zero replay, and hashes all admitted values, columns and effects. The original-only duplicate test is removed. No budget is raised.

The PB quota reference uses the existing production numeric operand conversion followed by the existing PB value primitive, retaining the actual argument declarations. Its helper's internal visibility changes to `pub(crate)`; the function body is byte-identical.

## Validation and scope

The complete frontend sweep finished with **2,345 passed, 6 failed, 95 ignored** in 1,599.67 seconds. Three failures were inherited; three were test-reference adaptations. After that sweep, only three Rust files change: the two affected test modules and the numeric helper's visibility. A source comparison proves the entire production file is otherwise byte-identical. The final membership matrix and PB quota/recovery test both pass; the duplicate failed old-only membership observer is retired. The remaining failures are exactly:

- `pushdown_catalog::tests::ifnull_string_column_literal_uses_go_signature_and_column_collation`
- `tests::builtin_info_json_math_source::exp`
- `tests::builtin_math_misc_op_source::vectorized_builtin_op_func`

This is a full sweep followed by narrow targeted reruns, not a claimed full sweep of the exact final source. The final test inventory has 2,445 tests, 16 fewer than the original 2,461; the reductions remove obsolete oracle controls or duplicate runs. The combined verified inventory is 2,347 passing tests, the same three inherited failures, and 95 ignored tests.

Unistore passes 207 tests with the exact three inherited failures and 13 ignored tests; its count falls by two retained-original controls. Projection passes 52 tests and Expand passes six. Focused raw, typed preparation, all 13 typed dynamic tests, public driver, eager policy, value kernel, duration, cached-owner and PB cast quota checks pass. Both formatter versions check 44 changed Rust files. Final lint and source audit pass. No standalone SDK tests, real-service SQL or performance measurements are repeated for this retirement.

Commands from repository root use `RUSTFLAGS=''` and `CARGO_BUILD_JOBS=8`:

    ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib
    ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-unistore --lib
    ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-executor --lib projection::tests
    ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-executor --lib expand::tests -- --test-threads=1
    ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib optional_membership_full_values_match_frozen_admitted_results
    make lint

The second final test runs directly from the freshly built test executable with filter `pb_binary_source_direct_consumers_and_fixed_cap_recovery`; it selects exactly one test and covers the existing signature matrix.

## Remaining scope

The previous SQL/performance report still describes its original measured source and binaries. This cleanup claims no speedup or production release readiness. Old executable controls can be recovered from Git history, but are no longer part of either production or test builds. Independent human review remains outstanding.

Coverage remains **240/245**. The five explicit host compatibility algorithms remain uncredited: `JSON_SCHEMA_VALID`, `TIDB_DECODE_PLAN`, `TIDB_DECODE_BINARY_PLAN`, `TIDB_ENCODE_SQL_DIGEST`, and `VALIDATE_PASSWORD_STRENGTH`. No push, PR, release or deployment is performed.
