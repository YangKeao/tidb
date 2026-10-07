# Expression unification experiment

Paired YangKeao/tidb and YangKeao/tikv branch: `expression-unification-demo`.
Current checkpoint: **string-target-137**, following **numeric-text-136**.

Functional **238/245 (97.14%)**, strict **0**, remaining **7**—unchanged. No whole CAST/M2 or Go-package credit.

New SDK datatype owner `native_string_convert.rs` shares ProduceStr byte/rune limiting, complete UTF-8 prefixes, whitespace-tail diagnostics and binary fixed-string padding. Named classification extends `native_string_type.rs`. Native projects actual FieldType/charset and typed errors. Canonical UTF-8 helpers and original invalid-byte, lazy-length and warning/truncate behavior are retained.

## Verification

[Evidence](evidence/string-target-checkpoint.md), [commands/counts/hashes](logs/string-target-summary.txt), [manifest](checkpoint.json), [ledger](migration-progress.json).

Five Cargo gates GREEN: SDK target2/named type1, full native datatype480 and existing SQL2. Three new tests;1 SDK/24 native old touched-file test bodies unchanged. One new SDK file, no new native files or SQL fixture/probe credit. The native subagent failed after its final edit; parent adopted, formatted and validated the complete change.

## Remaining

[Remaining acceptance](evidence/remaining-acceptance.md): source conversion/event merging, other datatype controllers, expression rendering, typed/write/SQL lowering; broader M2, six complex candidates, request-root/default-NoColumns/liveDAG and final acceptance. Full expression/unistore/workspace/lint/dev/bazel/release/exhaustive/performance/physical memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/whole-Go-package/PR readiness remain unverified. Historical R100 expression4/unistore1 failures remain unrepaired; prior RED receipts retained. Goal remains active.
