# N53: preserve actual public-row metadata and cursor coordinates

Based on TiDBafbda740c7 and SDKad73b11. SDK implementation is saved locally as3ee7511. Public eval entries have not switched; the genuine borrowed scalar consumer and PhysicalRow policy remain cfg(test). The SDK scalar-cursor facade is production code and uses the existing batch frame.

## Behavior

Public Column.eval ignores virtual_expr and correlated_col_unique_id planner metadata. The borrowed PhysicalRow source now follows that same rule, including not visiting a virtual expression with an invalid parameter. Projection keeps its prior declared-input admission and rejects those markers. Original optional declarations, negative/out-of-width indices and deferred-Constant error Datum handling retain their actual SQL outcomes.

An actual Row cursor may carry any usize coordinate. Constants, parameters and unselected column branches do not assert that a physical cell exists, so idx+1 cannot be used to invent a row-count extent; at usizeMAX that arithmetic caused an unnecessary resource error. LocalBatchProgram::eval_cursor_with_bindings now admits one actual coordinate, preserving it in every InputRow and host request, with sole occurrence0. It validates the complete binding schema and then uses the same checked SQL entry, host-catalog authentication, work budget and driver frame as bounded selection. Demanded physical cells remain the real source's responsibility. The existing bounded-selection facade retains its exact row-universe check, including rejecting usizeMAX when that is the declared extent.

The SDK change factors schema validation and the existing post-preflight body without changing any value or frame algorithm. Workspace n53-final-check.py reconstructs the entire archived local/batch.rs modulo whitespace after removing only the new facade and restoring those factorizations. No fake Row/Chunk/type, coordinate remapping, recursive expression callback, resource-limit increase or new dispatcher was introduced. Unregistered ordinary function names were already handled by CommonEager; their actual eager reads and Unsupported error were verified without changing that owner.

## Independent original evidence

Before observers or source changes,89 TiDB core/evaluator files and the two affected SDK files were archived atafbda740c7/ad73b11. Original cohorts were captured and independently rerun, then pinned before source changes: planner metadata288/433BC944B34D725D, actual cursor20/8A3846E1FFF37587, arbitrary names96/636A12C8D9724869. Exact original observer-prefix SHA256 is8928c142f92c20c02fcd80fc38f18be2d4dc2ab9bde643ba3ac79fe55902273b.

The actual old shared consumer reproduced two semantic REDs before repair: metadata record157 returned admission instead of the physical Int17, and cursor record16 returned Resource instead of the undemanded constant7 at usizeMAX. The arbitrary-name cohort already passed. All404 fixed observations now match through the genuine source and SDK frame, with original error-Datum and context records unchanged. Earlier N50/N51/N52 fixed observers and original public value bodies remain unchanged.

Two SDK protocol tests verify exact MAX coordinates, ordinary-coordinate recovery, unchanged bounded-selection rejection, schema/resource checks before input reads, actual host continuations, original owned error identity, task cleanup and foreign-catalog rejection. These are protocol witnesses, not an assertion that an invalid demanded cell contains a value.

## Validation

From the TiDB worktree:

    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib public_source_tests -- --nocapture --test-threads=1
    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-executor --lib projection::tests
    make lint

From the SDK worktree:

    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tikv test --release --locked --offline -j8 -p tidb_query_expr --lib

The repository Makefile environment and its scoped package clippy script apply the current Makefile lint flags to all targets. Exact command/log are retained in workspace evidence/no-fallback/n53-sdk-clippy.log and the n15-clippy-scoped.sh runner.

Fresh public32/0, focused43 existing context tests43/0, Projection52/0 in24.79seconds, Expand6/0 plus unchanged64/B42FB8BE316E6C79, bounded inventory83088/0/0, lint and both pinned nightly rustfmt checks for all six Rust files pass. Fresh SDK1150pass/exact inherited1fail/1ignore in0.26seconds; the sole failure remains local::str_to_date_worker_tests::str_to_date_workers_pass_whole_states_keep_wide_days_and_read_typed_mode_late. SDK full warning-title Counter17 and clippy Counter171, including89 expression warnings, exactly match the prior baseline without additions or removals.

The first SDK build encountered a stale proc-macro artifact from a different worktree, producing metadata_expr_ptr/CallShape compile mismatches despite unchanged correct source. A package-scoped release clean of tidb_query_codegen rebuilt the real macro; no source repair was made for that cache issue. Both protocol tests then passed and the fresh full/clippy gates above completed. The failed compile log is retained and is not treated as a semantic baseline.

The whole frontend was not rerun for this narrow row-policy/facade change. The fresh N52 full2266/exact inherited3/95ignore/all725batch/346warnings is retained explicitly as prior evidence; all affected current public/context/Projection tests and the entire changed SDK were rerun. n53-final-check.py --gates verifies original archives and pins, complete old SDK frame reconstruction, strict Projection policy, six reviewed source hashes, warning Counters and protected unrelated BUILD content. No push, PR or release.

## Remaining work

Promote the genuine public scalar consumer while preserving an independent test-only original recursive oracle and keeping default tests on the new public entry. Actual AST/filter/join/Unistore integration, generic fallback removal, full service verification and final unified performance remain. This cursor facade closes two public caller differences and does not complete the overall migration.
