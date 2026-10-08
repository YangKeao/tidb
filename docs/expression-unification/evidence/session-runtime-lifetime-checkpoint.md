# Session lifetime checkpoint — session-runtime-lifetime-05

This cut installs explicit, optional session ownership for the shared evaluator. It does not yet switch SQL ASCII. The user's updated priority is a working migration with focused checks; exhaustive audits, allocator remeasurement and release performance are follow-ups rather than per-cut blockers.

## Changes

- Executor StmtContext carries Option<ReadyValueExecution>, defaults None, and preserves the execution across cloning/COW. It stores no affine Scope and never begins/closes an execution.
- Session accepts an explicit policy once and privately creates a stable pool. A lexical marker distinguishes nested calls from a detached result; only an outer entry begins an independent execution and owns its captured closer. Busy installation is refused even when not configured.
- Repeated contexts carry that execution. Nested EXECUTE/IMPORT borrow it. A returned result receives the closer but not lexical-marker ownership. Finish/materialization/Drop close the captured execution; old result cleanup cannot close a newer execution or forgive retained worker debt. The session tracks every live execution, and Session Drop closes all attached/detached executions before other cleanup. Normal Next/EOF/Err do not close, whereas a native epilogue unwind does.
- The native owner error has an explicit into_eval_error method retaining its original typed cause. A trial From implementation caused E0282 inference in an unchanged vector builtin and ran zero tests; it was removed, not worked around by altering that builtin.
- No SQL defaults, native kernel deletion, projection binding, or new threads. The term worker still means a synchronous evaluator object.

## Actual validation

Commands below ran from expression-unification/tidb/rust with `/home/agent/tidb/expression-unification/tools/cargo-tidb` as `<cargo>`:

```sh
<cargo> test --locked -p tidb-session --lib tests_core::lifecycle:: -- --test-threads=1
<cargo> test --locked -p tidb-executor --lib stmt_context::tests::ready_value_execution_ -- --test-threads=1
<cargo> test --locked -p tidb-executor --lib stmt_context::tests:: -- --test-threads=1
<cargo> test --locked -p tidb-expr --lib tikv::adapter_failure::tests::public_owner_error_conversion_retains_the_native_cause -- --exact --test-threads=1
<cargo> test --locked -p tidb-expr --lib -- --test-threads=1
<cargo> test --locked -p tidb-executor --lib driver::errors::exec:: -- --test-threads=1
```

- Before session edits:14 lifecycle tests passed. After:28 passed, including14 new runtime cases;2064 filtered, exit0.
- New executor carrier tests:4 passed; entire context module23 passed, exit0.
- Named native-error bridge:1 passed,1461 filtered, exit0.
- Full expression:1364 passed/4 unchanged baseline failures/94 ignored,1462 discovered, exit101. All complete failure blocks equal checkpoint04 after only thread-ID normalization.
- Renderer:9 passed/1 unchanged Sequence-origin baseline failure, exit101; complete block also equal checkpoint04 after only thread-ID normalization.
- Pinned Aug2026 rustfmt applied; git diff --check passed. Some existing test formatting changed without assertion changes.

The runtime cases use actual public C4 values, context/COW reuse, SQL EXECUTE cached UPDATE and IMPORT SELECT nesting, real stream Next/EOF/cancellation, one-slot old-worker debt, stale Close, Session/attached/detached Drop, cached PointGet plan invalidated by ALTER, sandbox versus syntax admission, and materialized replay. The cfg(test) one-shot epilogue hook proves its structural unwind window, not an executor panic escaping existing inner catchers. Runtime-start failure cleanup remains structural; no natural poisoned-start execution is claimed.

## Scope and remaining work

Pool layout/algorithm is unchanged; the last independent192-byte caller allocation-request observation belongs to checkpoint04, not this newly built binary. No fresh allocator or peak/OOM proof is claimed. No release performance, make lint, whole-workspace or full Session suite acceptance. Final broad checks remain open. Default policy, ordinary operation scopes and complete business-wrapper propagation remain work for activation; the explicit policy path here is dormant, and zero-slot SQL success in this checkpoint is not proof of TiKV SQL execution.

Agent-doc review: new architecture-index paths exist; test commands are explicitly scoped to rust/. No AGENTS normative policy, Make target, PR workflow, generated fixture or Go-package completion claim changed. make lint remains unrun, so this is an intermediate checkpoint only.

Next: switch the ASCII dispatch to the existing TiKV value boundary, remove its native byte algorithm, and batch the next five kernels through a shared evaluator. Parallel work on that activation and backend batch is deliberately excluded from this checkpoint's staged source set. Completed fully audited families remain0/245; do not confuse this with the functioning lower-level runtime.
