# Expression unification experiment

Paired branch: `expression-unification-demo` in YangKeao/tidb and YangKeao/tikv.
Current checkpoint: **mydecimal-core-104**, after **legacy-date-arithmetic-103**.

Functional **237/245 (96.73%)**, strict **0**, remaining **8**—unchanged. This round is type-layer sharing, not new evaluator or family credit. Overall goal remains active.

## Fixed-word MyDecimal

TiKV `native_mydecimal.rs` owns the existing algorithms; native `mydecimal.rs` retains the real private-field40B storage facade and thin safe adapters. Raw fields, hidden/result fractions and mutation on errors/unwind remain intact. Required binary writer/size primitives are shared through `native_decimal_codec.rs`; wire Decimal is not substituted.

R106's generic preparation now uses these shared algorithms without changing its evaluator protocol. Broader CAST/M2, the general digit-string Decimal parser and binary reader are not declared complete.

## Evidence

[Checkpoint](evidence/mydecimal-core-checkpoint.md), [commands/counts/hashes](logs/mydecimal-core-summary.txt), [manifest](checkpoint.json), [ledger](migration-progress.json).

Ten locked serial launches pass on the first attempt, all nonzero. Full native datatype462; chunk10; codec decimal6/hash7; SDK core/codec and legacy/SQL regressions pass. Three new tests; original21 tests in changed native files and their complete support suffix remain unchanged. No new SQL probes.

## Still open

[Remaining acceptance](evidence/remaining-acceptance.md): CAST/M2, IN, six complex candidates and broader request-root/default-NoColumns/liveDAG/final acceptance.

Full expression/unistore were not rerun; historical4+1 failures remain. Workspace/lint/dev/bazel/release/exhaustive/TiFlash/FIPS/performance/physical memory/OOM/allocator/dual-tzdata/whole-Go-package/PR readiness remain unverified. The manifest pins paired TiKV and three identical Plans. No force push or PR; unrelated BUILD excluded.
