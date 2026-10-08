# Ready-value naming, direct limits, and statement lifecycle checkpoint

Checkpoint `ready-value-statement-lifecycle-218` makes three focused runtime changes without changing the 240/245 functional-family count.

## Changes

- Generic TiDB pool/session/capability names are now `ReadyValue*`; the modules are `tikv/ready_value.rs`, `tikv/ready_value_tests.rs`, and `tidb-session/ready_value_runtime.rs`. True SQL `ASCII()`, charset/collation/encoding symbols, `EvaluatedAsciiWorker`, and the fixed ASCII recipe retain their semantic names. No compatibility aliases keep the old generic names alive.
- TiKV deletes `LocalEvalState`. Public local evaluators accept the immutable `Copy` `ExecutionLimits` directly. Each invocation still creates one fresh `EvalBudget` shared across all occurrences/nested frames and one invocation-local row scratch; workers store only a copied limits policy.
- The shared pool no longer has a rotating global current epoch. Every outer statement/request gets an independently closable `ReadyValueExecution` with a monotonic ID and private closed state. Workers may be reused only by that same execution. Beginning or closing a peer does not invalidate a live/detached statement; close retires only matching idle workers, while in-flight/creating/retiring debt remains charged until actual destruction.
- `SessionReadyValueRuntime` tracks every still-live execution admitted on its root. Record sets close only their captured execution, and Session Drop closes all attached/detached executions rather than only the latest context-propagation handle.
- `Columns` remains the TiDB statement/effect facade. Warning/context restructuring and removal of four invocation-metadata binding guards remain explicitly deferred.

## Prepared-worker construction measurement

A temporary TiKV release integration probe pinned to CPU2 ran 500 warmups and 11 samples of 20,000 `prepare_evaluated_bytes + operation + retained_storage + drop` iterations per operation. Median ns/op: ASCII 543.153, integer add 687.988, STRCMP 844.112, Decimal add 851.984, JSON_TYPE 530.752, REGEXP_LIKE 851.114. All sample min/max values lie between 525.982 and 881.954 ns.

This is warm allocator/thread-cache steady state and excludes TiDB owner/slot/Arc/Mutex/public-Datum glue; it is not a cold-process or physical-allocation-peak result. It is nevertheless small relative to the measured default public one-shot totals (2.17–12.10 microseconds), so this experiment chooses statement-owned reconstruction instead of cross-statement mutable prepared-worker reuse. Raw and distilled receipts are `../logs/worker-prepare-probe-build-run.log` and `../logs/worker-prepare-probe-summary.txt`; the temporary probe source was removed.

## Validation

- `cargo-tikv check --locked -p tidb_query_expr --tests`: GREEN.
- Four focused TiKV tests covering fresh per-invocation budgets and prepared-worker reuse/prewarm: GREEN. A dedicated structural regression additionally proves that the public ready-value path creates exactly one `EvalBudget` and threads it through temporal preflight, driver/frame work, and output checks: GREEN.
- `cargo-tidb check --locked -p tidb-expr -p tidb-session -p tidb-unistore`: GREEN with the pinned nightly and GCC/CMake compatibility environment.
- The same three TiDB crates compile with `--tests`: GREEN.
- Four focused TiDB tests covering distinct worker ownership, same-execution debt, detached-statement close isolation, and Session Drop closing all captured executions: GREEN.
- Full `tikv::ready_value::tests::` module: 196 passed, 1 ignored, 0 failed; complete session lifecycle filter: 208 passed; complete executor statement-context filter: 23 passed.
- TiDB `make lint`: GREEN.
- TiKV full `make clippy` with the established GCC14/CMake compatibility environment: GREEN.

Distilled commands and outcomes are in `../logs/round224-runtime-cleanup-summary.txt`.

The existing release-only MD5 Prepare failure and broader performance regressions remain unresolved; therefore `pr_ready=false` remains unchanged.
