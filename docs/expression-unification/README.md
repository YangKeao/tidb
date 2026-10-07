# Expression unification experiment

Paired YangKeao/tidb and YangKeao/tikv branch: `expression-unification-demo`.
Current checkpoint: **numeric-event-140**, following **reverse-bound-139**.

Functional **238/245 (97.14%)**, strict **0**, remaining **7**—unchanged. No whole CAST/M2 or Go-package credit.

New SDK owner `native_conversion_event.rs` shares generic numeric outcome ownership, prefer-second selection and parsed-truncation versus bounded-overflow precedence. Native retains actual typed events/errors and Diagnostics call sites/effect order. Moved values and errors are not cloned.

## Verification

[Evidence](evidence/numeric-event-checkpoint.md), [commands/counts/hashes](logs/numeric-event-summary.txt), [manifest](checkpoint.json), [ledger](migration-progress.json).

Five Cargo gates GREEN: SDK2, full native datatype483, new/existing precedence2 and session SQL consumer1. Three new tests;0 SDK/27 native old touched-file test bodies unchanged. One new SDK file, no new native/SQL files or fixture/probe credit. The summary records parent adoption after the native agent completed adapters but delayed its test/final response.

## Remaining

[Remaining acceptance](evidence/remaining-acceptance.md): other datatype controllers, expression rendering, typed/write/SQL lowering; broader M2, six complex candidates, request-root/default-NoColumns/liveDAG and final acceptance. Full expression/unistore/workspace/lint/dev/bazel/release/exhaustive/performance/physical memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/whole-Go-package/PR readiness remain unverified. Historical R100 expression4/unistore1 failures remain unrepaired; prior RED receipts retained. Goal remains active.
