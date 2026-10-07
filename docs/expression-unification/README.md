# Expression unification experiment

Paired YangKeao/tidb and YangKeao/tikv branch: `expression-unification-demo`.
Current checkpoint: **field-value-policy-153**, following **field-aggregate-controller-152**.

Functional **238/245 (97.14%)**, strict **0**, remaining **7**—unchanged. No whole CAST/M2 or Go-package credit.

New SDK owner `native_field_value.rs` now owns complete runtime/parser `DefaultTypeForValue` matches, digit/Go-float widths and metadata policy. Native value code projects value shapes and applies returned specs; both large native matches and four width helpers are deleted.

## Verification

[Evidence](evidence/field-value-policy-checkpoint.md), [commands/counts/hashes](logs/field-value-policy-summary.txt), [manifest](checkpoint.json), [ledger](migration-progress.json).

Five Cargo gates GREEN: SDK1, full native datatype496, new/source runtime+parser value tests3 and numeric/temporal session SQL1. Two new tests; no old touched-file test bodies. One new SDK Rust owner; no new SQL files or fixture/probe credit. Tests cover runtime/parser literal differences, widths, flags, charset and DECIMAL caps.

## Remaining

[Remaining acceptance](evidence/remaining-acceptance.md): expression rendering, typed/write/SQL lowering; broader M2, six complex candidates, request-root/default-NoColumns/liveDAG and final acceptance. Full expression/unistore/workspace/lint/dev/bazel/release/exhaustive/performance/physical memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/whole-Go-package/PR readiness remain unverified. Historical R100 expression4/unistore1 failures remain unrepaired; prior RED receipts retained. Goal remains active.
