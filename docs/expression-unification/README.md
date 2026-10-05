# Expression unification experiment

Paired YangKeao/tidb and YangKeao/tikv branch: `expression-unification-demo`.
Current checkpoint: **arg-integer-125**, following **decimal-datum-124**.

Functional **238/245 (97.14%)**, strict **0**, remaining **7**—unchanged. No whole CAST/M2 or Go-package credit.

SDK `native_cast_integer.rs` owns integer-argument identity/JSON/source-unsigned/guard policy and composes JSON rendering plus signed/Decimal conversions internally. Native business conversion callbacks are removed. The original sealed real-unsigned worker execution callback remains, preserving admission and infrastructure errors alongside zone/truncate/warning effects; it is not a native numerical fallback. Generic SDK APIs remain compatible.

## Verification

[Evidence](evidence/arg-integer-checkpoint.md), [commands/counts/hashes](logs/arg-integer-summary.txt), [manifest](checkpoint.json), [ledger](migration-progress.json).

Six matched gates GREEN without failure/retry: SDK4, native new1/controller1/arguments8/CAST20, SQL1. Four new tests;2 SDK/225 native old test bodies unchanged.

New SQL:2 SELECTs/8 cells across both vector modes, JSON integer-prefix warnings, Enum/Set ordinals and unsigned DOUBLE argument conversion for TRUNCATE. Unit coverage includes identity, JSON no-zone/no8031 behavior, effect veto and worker refusal. No performance or physical-memory claim.

## Remaining

[Remaining acceptance](evidence/remaining-acceptance.md): context-aware Decimal and other typed/write selectors, broader M2, six complex candidates, request-root/default-NoColumns/liveDAG and final acceptance. Full suites/lint/dev/bazel/release/exhaustive/performance/physical memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/whole-Go-package/PR readiness remain unverified. Historical R100 expression4/unistore1 failures remain unrepaired; prior incident receipts retained. Goal remains active.
