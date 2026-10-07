# Shared arithmetic diagnostic renderer

**expr-diagnostic-render-169 / R175**, after [Go slice growth](go-slice-growth-checkpoint.md). Functional238/245, strict0 and remaining7 unchanged; no complete evaluator/renderer, family, whole CAST/M2/package credit.

Existing SDK owner `native_eval_type.rs` now owns six arithmetic operator symbols and exact binary, CAST, DECIMAL CAST and function-call diagnostic text composition. Native `scalar_function.rs` keeps AST recursion, constant/column evaluation, source selection, target metadata and typed error construction.

## Validation

Five targeted Cargo gates GREEN: SDK1, native-new1, existing arithmetic-overflow1, existing math-overflow1 and session SQL1. [Exact commands/counts/hashes](../logs/expr-diagnostic-render-summary.txt). Two new tests;14 SDK/32 native old touched-file test bodies unchanged. No new Rust file. Session/SQL files unchanged; no new SQL fixture/probe/cell credit. Full `tidb-expr` is not used as a green gate because historical R100 has four known unrepaired failures; this is retained, not hidden. No new SQL fixture. Remaining expression rendering/evaluator work, typed/write SQL lowering and broader M2/root/liveDAG/final acceptance remain. Full expression/unistore/workspace/lint/release/exhaustive/performance/physical memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/package/PR readiness unverified.
