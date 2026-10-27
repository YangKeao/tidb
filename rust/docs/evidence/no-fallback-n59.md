# N59: genuine parsed AST and reusable SHOW predicates

Validated local checkpoint from main `507d954c6c` and SDK `3ee7511`. Overall task #4 remains active; this receipt does not claim final service performance or completion of the remaining native batch consumers.

Public `eval_in` now compiles a `PreparedAstExpression` and executes it through the existing official SDK `LocalBatchProgram`, checked input bindings, host frame and native-value capability store. The source borrows the genuine parser AST. Every demanded value child is an SDK graph producer, including CHAR's final genuine RawString under its original charset consumer. No replacement Expression/ScalarFunction/Suite, inferred SQL declaration, recursive AST value callback, retry or separate expression driver is introduced. The Blob field is the existing opaque handle transport ABI, not an invented SQL result type.

AST-specific phases preserve eager AND/OR, exhaustive IN, all-left/all-right ROW comparisons, intermediate Position/Trim/Between conversions, lazy IF/IFNULL/COALESCE/CASE, late malformed-header errors and actual RAND owner identity. Pure metadata preflight charges even non-value ROW wrappers, charset payloads and sequence paths before allocation. Host tasks authenticate every retained child capability around callbacks and consume continuation state before invoking effects; they hold no value-store borrow across a callback.

The original recursive AST, function and IN bodies remain only in cfg(test) `original_ast.rs`. Reconstruction verifies all three against the unchanged pre-edit archive, removing only the dedicated test entry counter. The archive contains 605 files (488 main, 117 SDK). Independent original observations remain fixed:

| Corpus | Observations | Original digest |
| --- | ---: | --- |
| Genuine parsed/API roots, Datum values and context phases | 191763 | 789B6C5CB3543C08 |
| Actual registered functions, arities and name cases | 82080 | 3E5E6C815EB16E9C |
| AES modes and values | 2160 | 6E2ECFB35F591274 |
| Cache selection, callback errors, panic poison and recovery | 70 | 53FD82D7D84ECAF7 |

The 19 final focused tests pass. Public/default and prepared routes match the independent originals without entering the original AST oracle. They also cover 143 real roots reused over six changing-context/error/panic/NULL phases (2574 observations), entropy properties rather than fixed random outputs, real raw-charset child identity, cancellation, stale generations, wrong origins, setter invalidation without replay and fixed resource limits. The cache observer does not compile the new program on its original route.

All seven SHOW predicate consumers retain one lazy borrowed plan per statement. Validation and compilation start only when an actual row needs its predicate; later rows reuse the plan. Original unknown-column checks, virtual-row-only context capabilities, empty output and LIKE-excluded behavior are preserved. The existing actual Session SQL filter test was expanded to cover the direct SHOW consumers, CASE/multiple rows, unknown names, session-variable isolation and an empty database with an unregistered predicate function. It passes.

Fresh validation:

| Gate | Actual result |
| --- | --- |
| Complete frontend | 2324 passed, exact inherited 3 failed, 95 ignored; 981.50 s |
| Complete Projection | 52 passed; 256.47 s |
| Expand | 6 passed |
| Complete Unistore | 195 passed, exact inherited 3 failed, 13 ignored |
| Actual Session SHOW group | 31 passed, 2 inherited failures, 0 ignored |
| SDK complete | 1151 passed, exact inherited 1 failed, 1 ignored; unchanged 17 warning titles |
| SDK clippy | exit 0; unchanged 171 total / 89 expression warning titles |
| Production Expr/Session check and normal/test library compilation | passed |

The SHOW failures were verified by running the same aggregate command with every dirty tracked main/SDK Rust file restored to its exact original HEAD body, then restoring all nine current files byte-for-byte. The original run has the same 31/2 result: SHOW CREATE reports AUTO_INCREMENT=30001 where the old test expects101, and warning_count returns String'1' where the old test expects Int1. Neither expectation was changed. The new expanded SHOW filter itself passes. Original and current logs are retained separately; a later current-source rerun additionally hashes the expanded Session test, which the first generic source snapshot omitted. Earlier snapshots are not retroactively represented as covering that file.

The complete frontend's inherited failures are the IFNULL signature/collation assertion, builtin EXP and vectorized arithmetic operator cases. Unistore retains the two ready-operation worker-count assertions and strict DECIMAL'abc' case. SDK retains the existing str_to_date worker case. No inherited failure is filtered, rebased or counted as passing.

Final root lint, both pinned nightly formatters over15 main plus1 SDK Rust files, diff checks, unchanged gate source hashes, original body/digest audit and the protected unrelated BUILD hash passed before local DCO. The supplemental plain Cargo clippy run also exited0 but used default lint settings (342 warning titles), so it is not the171-title repository baseline. The scoped Makefile runner applies the existing repository lint flags and is checked separately, without weakening them. Exact commands and source snapshots are retained in owner workspace `evidence/no-fallback/n59-*`; `n59-final-check.py` checks actual terminal results and original/current failure identity. All original records, partial failures and corrective runs remain available.

Development failures are explicit: an unbounded BENCHMARK observer was canceled before producing a digest; its tombstone is retained. Its bounded genuine fixture was fixed before original pins, and a separate i64::MAX test preserves exact third-lookup failure. A source-site patch initially made no change; the actual correction retained SDK validation. A niladic-host test incorrectly assumed depth1 was sufficient and was corrected to the existing SDK depth rule. Metadata budget gaps were observed RED before their pure preflight correction. An attempted cfg(test) gate on AES's reusable ready-values helper broke a normal build and was reverted; the algorithm is unchanged. The first SHOW command named nonexistent individual targets and ran no tests; only the aggregate `--test all` runs count as evidence.

Reproduce from the TiDB root with `RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib` (add `genuine_ast -- --nocapture --test-threads=1` for focused records). Projection uses `-p tidb-executor --lib projection::tests`; Unistore uses `-p tidb-unistore --lib`; Session uses `-p tidb-session --test all show_ -- --test-threads=8`. Lint is `make lint`; both nightly2026-01-30 and nightly2026-08-22 rustfmt run edition2021 with skip_children=true. SDK commands and warning comparisons are recorded with their actual pinned toolchain in the workspace receipts.

Next: retire the real eligible typed/native batch traversal, close the public Suite consumer contracts without manufacturing SQL schema, delete generic Admission-to-run routing, then complete actual services and final unified performance validation. No original branch push, PR, release or deployment is part of this checkpoint.
