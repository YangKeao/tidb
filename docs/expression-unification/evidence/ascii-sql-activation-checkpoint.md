# ASCII SQL activation — ascii-sql-activation-06

## Functional change

TiDB's ASCII dispatch now passes its real Columns context and delegates to the official TiKV RPN kernel. The old first-byte/NULL algorithm in string_fn.rs is deleted; only frontend arity checking and delegation remain. Active scope takes precedence, then an execution-scoped evaluator; contextless callers use a TiKV one-shot evaluator with RAII close, not native fallback. Existing child evaluation, charset conversion, byte-preserving coercion and return casting remain native frontend work.

The explicit one-shot experiment policy is1worker/1creating,8MiB pool allowance,1MiB worker-retained allowance,2MiB creation reservation,64steps/16frames and usize::MAX per-call allowance. This is neither a Go default nor a physical heap/peak/OOM guarantee. A real2MiB input test passes despite the smaller worker-retained allowance. Cold one-shot preparation and incomplete operation-scope reuse are performance follow-ups.

TiKV now has one closed EvaluatedBytesOp/Worker implementation for ASCII, LENGTH/OCTET_LENGTH, BIT_LENGTH, LTRIM, RTRIM and UNHEX. Owned results are Int or Bytes. The old ASCII API is a thin compatible wrapper over that implementation; there is no second worker/compiler/driver. The other five families are backend-ready only, NOT yet migrated in TiDB.

## Actual checks

Run from `tikv/` with tools/cargo-tikv (Jan2026):

- `test --locked -p tidb_query_expr --lib local:: -- --test-threads=1`:176 passed/1 ignored/466 filtered.
- `test --locked -p tidb_query_expr --lib test_evaluated_bytes_rejects_same_carrier_operation_and_kernel_drift -- --test-threads=1`:1 passed/642 filtered.

Run from `tidb/rust/` with tools/cargo-tidb (Aug2026):

- `test --locked -p tidb-expr --lib tikv::evaluated_ascii::tests::ascii_dispatch_ -- --test-threads=1`:6 passed/1462 filtered.
- `test --locked -p tidb-session --lib tests_core::lifecycle::evaluated_ascii_ -- --test-threads=1`:15 passed/2078 filtered.
- `test --locked -p tidb-expr --lib -- --test-threads=1`:1370 passed/4 unchanged baseline failures/94 ignored,1468 discovered, exit101. All four complete failure blocks match checkpoint05 after only thread-ID normalization; no expected values changed.

The two actual SQL regressions read VARBINARY table columns, not folded literals: NULL, empty,FF,C3A9,E4B8AD,A. One-slot execution returns NULL,0,255,195,228,65 twice. Zero-slot execution rejects every corresponding SQL evaluation (including NULL) with the original PoolResource/Pool adapter cause and1105/HY000/evaluation origin, instead of computing natively. Tests explicitly use serial executor/projection concurrency for the one-slot domain. The former checkpoint05 dormant-path test was intentionally changed for activation, not used to re-record upstream SQL fixtures.

Six dispatch tests also verify real official fn_ptr witnesses, active-scope priority, borrowed-execution worker reuse without closing it, contextless evaluation, frontend/arity precedence,2MiB input and one-shot cleanup. Existing ASCII/coercion tests run in the full expression suite.

## Coverage and limitations

The frozen baseline ASCII row has AST/typed callers and shared-value dispatch, no implemented typed-PB/unistore ASCII signatures. Both existing direct ascii consumers now pass context. No new PB support was added for credit. Functional delegation/deletion progress is1/245; the other five backend-ready families receive no migration credit. The stricter final-audited counter remains separate until deferred whole-entry/performance/final checks are closed.

Not claimed: protocol-network end-to-end testing, new allocator observation, factory peak bounds, release performance, all business-wrapper capability propagation, full Session/workspace suites, make lint, overall goal completion or PR readiness. The old pinned192-byte observation belongs to checkpoint04; this modified backend/new binary is not covered by that old receipt. Test output excerpts are published without thousands of repetitive compiler warnings; full raw local logs remain under expression-unification/logs/.

Architecture-index review: referenced source paths exist, commands are explicitly Rust-scoped, no AGENTS policy/Make/PR workflow or generated fixture changes. Continue with shared-pool caller bindings for the five backend-ready families rather than another foundation-only audit cycle.
