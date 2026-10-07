# Expression unification experiment

Paired YangKeao/tidb and YangKeao/tikv branch: `expression-unification-demo`.
Current checkpoint: **rand-kernel-171**, following **expr-diagnostic-argument-170**.

Functional **239/245 (97.55%)**, strict **0**, remaining **6**. RAND adds one functional family; the prior238 family objects remain byte-identical.

`tidb_query_crypto` owns MySQL RAND seed-state derivation and recurrence; `tidb_query_expr` owns Datum seed routing. Native code keeps session entropy, generator identity/lifetime, Mutex storage and concrete conversions. Local seed derivation and source selector are deleted.

## Verification

[Evidence](evidence/rand-kernel-checkpoint.md), [commands/counts/hashes](logs/rand-kernel-summary.txt), [manifest](checkpoint.json), [ledger](migration-progress.json).

Six Cargo gates GREEN: crypto seed1, query-expr route1, native RNG1, native route1, typed-row identity1 and session SQL sequence/order1. Four new tests;57 TiKV/7 TiDB old touched-file test bodies unchanged. No new Rust/SQL files or fixture/probe credit. No PB RAND signature exists and no admission was invented.

## Remaining

[Remaining acceptance](evidence/remaining-acceptance.md): CAST plus five complex families; remaining expression renderer/evaluator and typed/write SQL lowering; broader M2, request-root/default-NoColumns/liveDAG and final acceptance. Full expression/unistore/workspace/lint/dev/bazel/release/exhaustive/performance/physical memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/whole-Go-package/PR readiness remain unverified. R100 historical failures remain explicit. Goal remains active.
