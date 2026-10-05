# Expression unification experiment

Paired YangKeao/tidb and YangKeao/tikv branch: `expression-unification-demo`.
Current checkpoint: **arg-string-123**, following **datetime-control-122**.

Functional **238/245 (97.14%)**, strict **0**, remaining **7**—unchanged. No whole CAST/M2 or Go-package credit.

SDK owns generic byte coercion and string-argument value/type/width policy. `native_cast_arg_string.rs` preserves explicit identity, BIT binary bytes, collation precedence and source-backed widths; the rewriter shares the same width implementation. Native cast/coerce/rewriter paths retain only actual data and storage mapping. Generic Rust float display is not SQL float formatting.

## Verification

[Evidence](evidence/arg-string-checkpoint.md), [commands/counts/hashes](logs/arg-string-summary.txt), [manifest](checkpoint.json), [ledger](migration-progress.json).

Seven final gates GREEN: SDK2/2, native2/8/1/33, SQL1. Eight launches include an initial compile failure from deleting a constant still used by a separate old test module; restored the original literal under cfg(test), without changing old tests or production policy. RED retained. Five new tests;2 SDK/222 native old test bodies unchanged.

New SQL:2 SELECTs/12 cells over both vector modes, BIT bytes, Enum names, large Rust-display float, Decimal scale, invalid-UTF8 binary payload, NULL, metadata widths and no warnings. No performance or physical-memory claim.

## Remaining

[Remaining acceptance](evidence/remaining-acceptance.md): integer-argument and unsigned dependencies, typed/write conversions, broader M2, six complex candidates, request-root/default-NoColumns/liveDAG and final acceptance. Full suites/lint/dev/bazel/release/exhaustive/performance/physical memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/whole-Go-package/PR readiness remain unverified. Historical R100 expression4/unistore1 failures remain unrepaired; prior incident receipts retained. Goal remains active.
