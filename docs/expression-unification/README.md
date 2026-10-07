# Expression unification experiment

Paired YangKeao/tidb and YangKeao/tikv branch: `expression-unification-demo`.
Current checkpoint: **expr-diagnostic-argument-170**, following **expr-diagnostic-render-169**.

Functional **238/245 (97.14%)**, strict **0**, remaining **7**—unchanged. No whole expression renderer/evaluator, complete write-lowering, whole CAST/M2 or Go-package credit.

Existing SDK owner `native_eval_type.rs` now owns diagnostic column naming, numeric binary-literal display-domain selection and INTDIV DECIMAL argument-wrap policy. Native expressions keep AST/Datum/FieldType projection and actual float formatting; duplicate naming/domain/predicate branches are deleted.

## Verification

[Evidence](evidence/expr-diagnostic-argument-checkpoint.md), [commands/counts/hashes](logs/expr-diagnostic-argument-summary.txt), [manifest](checkpoint.json), [ledger](migration-progress.json).

Five targeted Cargo gates GREEN: SDK1, native-new1, existing arithmetic-overflow1, existing numeric-domain1 and session SQL1. Two new tests;15 SDK/33 native old touched-file test bodies unchanged. No new Rust/SQL files or fixture/probe credit. Full `tidb-expr` was not claimed or rerun as green because R100 retains four known historical failures.

## Remaining

[Remaining acceptance](evidence/remaining-acceptance.md): remaining expression renderer/evaluator and typed/write SQL lowering; broader M2, six complex candidates, request-root/default-NoColumns/liveDAG and final acceptance. Full expression/unistore/workspace/lint/dev/bazel/release/exhaustive/performance/physical memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/whole-Go-package/PR readiness remain unverified. Goal remains active.
