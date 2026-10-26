# N52: compile a genuine borrowed function root

Based on localc9acf0df3f. The source compiler now accepts the actual ScalarFunction root as well as the actual Expression root. No substitute Expression, cloned tree, Suite, physical Chunk or SQL declaration is constructed. Public eval entries have not switched; the enabling borrowed scalar consumer remains cfg(test). Overall migration remains unfinished.

## Behavior and ownership

SourceNodeRef retains the genuine root reference and pointer identity. Children remain genuine Expression references, including real deferred children and diagnostic re-demands. A direct ScalarFunction return has its existing result-family normalization and selected protobuf interpretation, but lacks the enclosing Expression enum/set integer view. FunctionRoot and authenticated FunctionOutput role4 express that distinction; existing roles1/2/3 and token-origin validation retain their meaning. Only the common enclosing-expression view is skipped for role4.

The original function arms of numeric/math diagnostic bounds and constant-fold metadata were extracted into helpers accepting a real ScalarFunction. Existing Expression facades delegate to them. Original public eval, value algorithms, eager argument policy, caches and SDK remain unchanged. Workspace n52-final-check.py reconstructs the archived metadata arms and verifies every other original scalar_function.rs and constant_fold.rs body unchanged.

The first whole Projection run exposed an actual resource regression: a16-byte sum reference widened every formerly8-byte Expression reference in source arrays and exceeded the unchanged1MiB preparation allowance in existing mixed configuration7. The added regression reproduced16 versus8 before repair. SourceReference now specializes only borrowed reference storage: ordinary Expression consumers store8-byte references, genuine direct-function consumers store16-byte sum references, and both instantiate the same compiler and demand policy. Pending queues, checked nodes and numeric/diagnostic mirrors charge their actual generic type size. No unsafe pointer tagging, budget increase, input reduction or digest rebase was used.

## Independent original evidence

Before observers or source changes,87 core/evaluator Rust files were archived atc9acf0df3f. Four new original cohorts were captured, independently rerun and pinned before changing the compiler: return views75600/9D7254FFD2481AF5; actual protobuf ingestion540/4CDD027B2A300F8D; RAND/clone owner30/E5DCB211D42975EC; regexp cache/statement transition10/05308B90353B745C. Exact original observer-prefix SHA256 is7504f3cb9116f1d548658e404acf3de0809c4137c023ffd4cb8d6870302acca0. The RAND assertions check each actual original/cloned function address before recording a stable owner result; cache tests use actual original and cloned instances rather than cloned replacements for both branches.

Together with the prior5880/2550BA35CB389B63 direct-function cohort, all82060 fixed function observations match through the real source compiler, SDK frame, real row and original context. Earlier N50/N51 ordinary and error-Datum pins and their exact observer prefixes remain unchanged. Additional294 numeric/math/optional-return/context diagnostic cases and64KiB resource/NULL/small-value recovery are post-factor controls; they are not represented as before-production pinned observations. Width rejection occurs before callbacks and the same plan then recovers.

## Validation

From the TiDB worktree:

    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib public_source_tests -- --nocapture
    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib
    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-executor --lib projection::tests
    make lint

Fresh public28/0 and post-repair lint pass. The whole frontend finishes2266pass/exact inherited3fail/95ignore in844.19seconds; all725 batch tests pass and the aggregate346warnings remain unchanged. The inherited failures are pushdown_catalog::tests::ifnull_string_column_literal_uses_go_signature_and_column_collation, tests::builtin_info_json_math_source::exp, and tests::builtin_math_misc_op_source::vectorized_builtin_op_func. No failure was filtered out.

Fresh Projection52/0 finishes in80.79seconds. Current executor Expand gives6/0 plus the unchanged64/B42FB8BE316E6C79 original witness. The current frontend inventory gives83088cases/0refused/0names; it remains a bounded inventory, not global coverage. Both pinned nightly-2026-01-30 and nightly-2026-08-22 rustfmt --check --edition 2021 --config skip_children=true pass for all ten Rust files. Workspace n52-final-check.py --gates verifies87 original archives, three exact pinned prefixes, unchanged original value bodies after metadata extraction, real source identities and output roles, generic actual-size storage, ten reviewed source hashes and all final gates. The first Projection51/1 RED and the superseded explicitly cancelled frontend log remain separately retained and are not treated as complete successful runs.

SDKad73b11 remains clean and unchanged; its documented N16 baseline is retained as prior evidence, not a fresh SDK run. Protected unrelated untracked BUILD content remains unchanged. No push, PR or release.

## Remaining work

Complete physical public-row contracts and preserve an independent original recursive test oracle before promoting the genuine public scalar consumer. Then integrate actual AST/filter/join/Unistore callers, remove generic fallback, validate the full service and measure final unified performance. Borrowing a genuine function root is one integration step, not global completion or a final scalar-plan cache design.
