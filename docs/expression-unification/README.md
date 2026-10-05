# Expression unification experiment

Paired YangKeao/tidb and YangKeao/tikv branch: `expression-unification-demo`.
Current checkpoint: **decimal-context-126**, following **arg-integer-125**.

Functional **238/245 (97.14%)**, strict **0**, remaining **7**—unchanged. No whole CAST/M2 or Go-package credit.

SDK `codec/native_decimal_context.rs` owns context-aware Decimal source selection, diagnostics and effect ordering. Native retains only actual data, typed Terror construction and generic context effects. MyDecimal-to-Decimal projection/scale padding and native literal Display also have single SDK owners; old native entries delegate. Plain/context conversion differences remain explicit.

## Verification

[Evidence](evidence/decimal-context-checkpoint.md), [commands/counts/hashes](logs/decimal-context-summary.txt), [manifest](checkpoint.json), [ledger](migration-progress.json).

Five final gates GREEN: SDK2, full native datatype477, new expression1/old constants16, SQL1. Eight matched launches include three RED gates from new-test setup/expectations, corrected from original source without production/old-test changes. All RED receipts retained. Four new tests;14 SDK/248 native old test bodies unchanged.

Final SQL:2 SELECTs/6 Decimal cells across both vector modes, temporal millisecond arguments and fractional projection/narrowing, with result codes and exact1292 warnings. JSON context policies are covered by units, not claimed fully exercised by SQL. No performance or physical-memory claim.

## Remaining

[Remaining acceptance](evidence/remaining-acceptance.md): other typed/write numeric selectors including scalar_function composition, broader M2, six complex candidates, request-root/default-NoColumns/liveDAG and final acceptance. Full expression/unistore/workspace/lint/dev/bazel/release/exhaustive/performance/physical memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/whole-Go-package/PR readiness remain unverified. Historical R100 expression4/unistore1 failures remain unrepaired; prior incident receipts retained. Goal remains active.
