# Expression unification experiment

Paired branch: `expression-unification-demo` in YangKeao/tidb and YangKeao/tikv.
Current checkpoint: **cast-string-111**, after **cast-float-110**.

Functional **238/245 (97.14%)**, strict **0**, remaining **7**—unchanged. This partial CAST/M2 batch adds no family credit. Overall goal remains active.

## CHAR/BINARY and type classification

`cast.rs` delegates through `tikv/cast_string.rs` to SDK `native_cast_string.rs`; six native policy helpers are deleted. SDK owns YEAR-zero/raw-byte choice, charset demand, existing encoding DECODE, warning order, truncation and padding. The original packet handler retains its repeated limit and policy reads.

`codec/native_string_type.rs` owns string classification, preserving named/Unknown identity, effective array code and exact collation comparisons. Native FieldType methods delegate. General `Datum.sql_string` remains an actual datatype service; its selector and fixed float formatter are not claimed as migrated.

## Evidence

[Checkpoint](evidence/cast-string-checkpoint.md), [exact commands/counts/hashes](logs/cast-string-summary.txt), [manifest](checkpoint.json), [ledger](migration-progress.json).

**Nine matched gates green on first attempt**, including original casts, binary-literal wrapping, full native datatype, new CHAR/BINARY SQL and prior floating SQL. Five new tests;232 old native test bodies unchanged. New SQL covers ten SELECTs/32 cells across scalar/vector modes, including raw bytes, GBK3854→1406 order and packet-limited padding versus permitted truncation.

No new C4 profile, carrier, admission or performance claim. The previous floating test's initial failure remains in its historical evidence; no failures are erased.

## Still open

[Remaining acceptance](evidence/remaining-acceptance.md): other CAST and typed/vector/UNION domains, native conversion actuators including SQL stringification, broader M2, six complex candidates, request-root/default-NoColumns/liveDAG and final acceptance.

Full expression/unistore were not rerun; historical4+1 failures remain. Workspace/lint/dev/bazel/release/exhaustive/TiFlash/FIPS/performance/physical memory/OOM/allocator/dual-tzdata/whole-Go-package/PR readiness are unverified. The manifest pins paired TiKV and three identical Plans. No force push or PR; unrelated BUILD excluded.
