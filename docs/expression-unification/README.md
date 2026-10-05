# Expression unification experiment

Paired YangKeao/tidb and YangKeao/tikv branch: `expression-unification-demo`.
Current checkpoint: **signed-datum-120**, following **temporal-calendar-119**.

Functional **238/245 (97.14%)**, strict **0**, remaining **7**—unchanged. This is partial CAST/M2 ownership, not whole CAST or Go-package acceptance.

SDK owns actual19-kind signed-datum conversion, integer bounds/conversion/text/JSON policy, temporal numeric rendering and the pure binary-literal integer outcome. Native facades project values, typed errors and events; diagnostics execute synchronously at original sites. Enum/Set ordinals, Float32 raw precision, BIT/literal distinctions, JSON string quirks and temporal rounding order remain separate.

Existing integer-controller callback interfaces remain, but their signed-datum business implementation is shared. YEAR/DATE, implicit argument and other typed/write conversion controllers remain to be closed.

## Verification

[Evidence](evidence/signed-datum-checkpoint.md), [commands/counts/hashes](logs/signed-datum-summary.txt), [manifest](checkpoint.json), [ledger](migration-progress.json).

Eight final gates GREEN: SDK101/8, native datatype477, three expression gates and two SQL gates. Nine launches include an initial compile failure from a missing test-only import; corrected without changing old tests or production policy. Eleven new tests;12 SDK/264 native old test bodies unchanged.

New SQL:2 SELECTs/8 cells across both vector modes, actual hybrid ordinal fallback and temporal carry including DST. No performance or physical-memory claim.

## Remaining

[Remaining acceptance](evidence/remaining-acceptance.md): other CAST/M2, six complex candidates, request-root/default-NoColumns/liveDAG and final acceptance. Full suites/lint/dev/bazel/release/exhaustive/performance/physical memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/whole-Go-package/PR readiness remain unverified. Historical R100 expression4/unistore1 failures remain unrepaired. Goal remains active.
