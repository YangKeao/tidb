# Expression unification experiment

Paired YangKeao/tidb and YangKeao/tikv branch: `expression-unification-demo`.
Current checkpoint: **float-target-135**, following **numeric-completion-134**.

Functional **238/245 (97.14%)**, strict **0**, remaining **7**—unchanged. No whole CAST/M2 or Go-package credit.

New SDK datatype owner `native_float_convert.rs` shares float round/truncate/shift/max foundations and ProduceFloat target fitting. Native aliases the overflow type and projects actual metadata, values/events and typed diagnostics. Original NaN/Inf, unsigned, precision-before-FLOAT-range and lazy-message order are retained; the distinct wire rounding domain is not substituted.

## Verification

[Evidence](evidence/float-target-checkpoint.md), [commands/counts/hashes](logs/float-target-summary.txt), [manifest](checkpoint.json), [ledger](migration-progress.json).

Five matched gates GREEN without failure/retry: SDK2, full native datatype478, numeric consumers5 and existing SQL2. Three new tests;30 old native touched-file test bodies unchanged. One new SDK file, no new native files; expression/SQL files unchanged. No new SQL fixture/probe credit.

## Remaining

[Remaining acceptance](evidence/remaining-acceptance.md): source conversion/event merging, fixed-shortest rendering/integer parsing, other datatype controllers, expression rendering, typed/write/SQL lowering; broader M2, six complex candidates, request-root/default-NoColumns/liveDAG and final acceptance. Full expression/unistore/workspace/lint/dev/bazel/release/exhaustive/performance/physical memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/whole-Go-package/PR readiness remain unverified. Historical R100 expression4/unistore1 failures remain unrepaired; prior RED receipts retained. Goal remains active.
