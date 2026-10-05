# Expression unification experiment

Paired branch: `expression-unification-demo` in YangKeao/tidb and YangKeao/tikv.
Current checkpoint: **cast-decimal-108**, after **decimal-parse-107**.

Functional **238/245 (97.14%)**, strict **0**, remaining **7**—unchanged. This partial CAST slice adds no family credit. Overall goal remains active.

## Ordinary DECIMAL CAST

`cast.rs` delegates source/warning/precision control through `tikv/cast_decimal.rs` to SDK `native_cast_decimal.rs`, deleting the corresponding native algorithms. SDK owns warning-before-conversion order, distinct Real/Float32 policies, discarded conversion events/error folding and unspecified-scale handling.

Native code projects actual values, requests the original default datatype conversion and appends SDK-specified warnings. Raw SmallVec/five-field transport preserves storage and metadata; existing math is reused. The compatibility numeric-prefix helper also has one SDK owner. No new C4 profile, dependency, carrier or admission gate is introduced.

## Evidence

[Checkpoint](evidence/cast-decimal-checkpoint.md), [exact commands/counts/hashes](logs/cast-decimal-summary.txt), [manifest](checkpoint.json), [ledger](migration-progress.json).

Seven nonzero green receipts: SDK2, native bridge1, original CAST7, original UNION4, full native datatype463, new SQL1 and R110 SQL1. One preexisting vectorized UNION test remains ignored—not passed. Four new tests;2 SDK and221 native old test bodies unchanged.

The new SQL test runs eight SELECTs across scalar/vector modes, covering source domains, JSON conversion-event discard, ordered overflow warnings and build-time unspecified-scale preparation. Original fixtures and UNION/typed control remain unchanged.

## Still open

[Remaining acceptance](evidence/remaining-acceptance.md): other CAST and typed/vector/UNION domains, native datatype conversion actuators and broader M2, six complex candidates, request-root/default-NoColumns/liveDAG and final acceptance.

Full expression/unistore were not rerun; historical4+1 failures remain. Workspace/lint/dev/bazel/release/exhaustive/TiFlash/FIPS/performance/physical memory/OOM/allocator/dual-tzdata/whole-Go-package/PR readiness are unverified. The manifest pins paired TiKV and three identical Plans. No force push or PR; unrelated BUILD excluded.
