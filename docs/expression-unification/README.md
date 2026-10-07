# Expression unification experiment

Paired YangKeao/tidb and YangKeao/tikv branch: `expression-unification-demo`.
Current checkpoint: **expr-diagnostic-render-169**, following **go-slice-growth-168**.

Functional **238/245 (97.14%)**, strict **0**, remaining **7**—unchanged. No whole expression renderer/evaluator, complete write-lowering, whole CAST/M2 or Go-package credit.

Existing SDK owner `native_eval_type.rs` now owns six arithmetic operator symbols plus binary, CAST, DECIMAL CAST and function-call diagnostic text composition. Native expressions keep AST recursion, constant/column evaluation, target metadata and typed errors; repeated formatting bodies are deleted.

## Verification

[Evidence](evidence/expr-diagnostic-render-checkpoint.md), [commands/counts/hashes](logs/expr-diagnostic-render-summary.txt), [manifest](checkpoint.json), [ledger](migration-progress.json).

Five targeted Cargo gates GREEN: SDK1, native-new1, existing arithmetic-overflow1, existing math-overflow1 and session SQL1. Two new tests;14 SDK/32 native old touched-file test bodies unchanged. No new Rust/SQL files or fixture/probe credit. Full `tidb-expr` was not claimed or rerun as green because R100 retains four known historical failures.

## Remaining

[Remaining acceptance](evidence/remaining-acceptance.md): remaining expression renderer/evaluator and typed/write SQL lowering; broader M2, six complex candidates, request-root/default-NoColumns/liveDAG and final acceptance. Full expression/unistore/workspace/lint/dev/bazel/release/exhaustive/performance/physical memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/whole-Go-package/PR readiness remain unverified. Goal remains active.
