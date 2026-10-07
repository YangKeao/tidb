# Expression unification experiment

Paired YangKeao/tidb and YangKeao/tikv branch: `expression-unification-demo`.
Current checkpoint: **datum-string-route-160**, following **datum-target-select-159**.

Functional **238/245 (97.14%)**, strict **0**, remaining **7**—unchanged. No complete write-lowering, whole CAST/M2 or Go-package credit.

Existing SDK owner `native_eval_type.rs` now owns string-target conversion route selection: raw binary, decode, encode, checked text, binary-literal decode and generic stringify. Native conversion keeps actual bytes/charset transforms, typed errors and context effects; local kind/from-to route branching is deleted.

## Verification

[Evidence](evidence/datum-string-route-checkpoint.md), [commands/counts/hashes](logs/datum-string-route-summary.txt), [manifest](checkpoint.json), [ledger](migration-progress.json).

Five Cargo gates GREEN: SDK1, full native datatype505, native-new1, existing binary-string conversion1 and GBK write session SQL1. Two new tests;7 SDK/31 native old touched-file test bodies unchanged. No new Rust/SQL files or fixture/probe credit.

## Remaining

[Remaining acceptance](evidence/remaining-acceptance.md): remaining typed/write SQL lowering, expression text rendering; broader M2, six complex candidates, request-root/default-NoColumns/liveDAG and final acceptance. Full expression/unistore/workspace/lint/dev/bazel/release/exhaustive/performance/physical memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/whole-Go-package/PR readiness remain unverified. Historical R100 expression4/unistore1 failures and prior RED receipts retained. Goal remains active.
