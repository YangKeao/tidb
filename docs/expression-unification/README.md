# Expression unification experiment

Paired branch: `expression-unification-demo` in YangKeao/tidb and YangKeao/tikv.
Current checkpoint: **cast-integer-109**, after **cast-decimal-108**.

Functional **238/245 (97.14%)**, strict **0**, remaining **7**—unchanged. This integer CAST batch adds no family credit. Overall goal remains active.

## Integer CAST and rounded Decimal

`cast.rs` delegates SIGNED/UNSIGNED/UNSIGNED-in-UNION and shared integer helpers through `tikv/cast_integer.rs` to SDK `native_cast_integer.rs`. SDK owns scanners, diagnostics, source selection and UNION gating. Native duplicate policy bodies are deleted.

Warning/zone ordering, distinct value-only contracts, actual datatype events/errors and the existing RealUnsigned worker are retained. Borrowed Decimal integer-rounding algorithms also have one SDK owner, preserving raw storage and signed/unsigned differences. No extra C4 profile, carrier or admission gate is introduced.

## Evidence

[Checkpoint](evidence/cast-integer-checkpoint.md), [exact commands/counts/hashes](logs/cast-integer-summary.txt), [manifest](checkpoint.json), [ledger](migration-progress.json).

Nine nonzero green receipts: SDK datatype1/controller2, native bridge1, original cast-module21/cast-function16, full native datatype463 and three SQL tests. Five new tests;2 SDK and222 native old test bodies unchanged. The preexisting vectorized Decimal UNION ignore remains unmodeled and was not selected this round.

The new SQL test runs ten SELECTs across scalar/vector modes, covering numeric rounding, complement/advisory diagnostics, malformed-prefix suppression, temporal carry, JSON signed/unsigned differences and NULLs. Existing RealUnsigned refusal and DECIMAL SQL tests also pass.

## Still open

[Remaining acceptance](evidence/remaining-acceptance.md): other CAST targets and typed/vector/UNION variants, native datatype conversion actuators and broader M2, six complex candidates, request-root/default-NoColumns/liveDAG and final acceptance.

Full expression/unistore were not rerun; historical4+1 failures remain. Workspace/lint/dev/bazel/release/exhaustive/TiFlash/FIPS/performance/physical memory/OOM/allocator/dual-tzdata/whole-Go-package/PR readiness are unverified. The manifest pins paired TiKV and three identical Plans. No force push or PR; unrelated BUILD excluded.
