# Expression unification experiment

Paired YangKeao/tidb and YangKeao/tikv branch: `expression-unification-demo`.
Current checkpoint: **year-control-121**, following **signed-datum-120**.

Functional **238/245 (97.14%)**, strict **0**, remaining **7**—unchanged. No whole CAST/M2 or Go-package credit.

SDK `native_cast_year.rs` owns YEAR control: Duration retains clock validation, zone and concat demand; other values use shared string/date parsing before UTC signed fallback. `native_cast_integer_signed_numeric` closes value-only signed conversion without a host conversion callback. Actual input projection is shared with ordinary integer CAST; native retains data getters and result/error wrapping.

## Verification

[Evidence](evidence/year-control-checkpoint.md), [commands/counts/hashes](logs/year-control-summary.txt), [manifest](checkpoint.json), [ledger](migration-progress.json).

Eight matched gates GREEN, no failure/retry: SDK YEAR1/integer2, four expression gates and two SQL gates. Three new tests;2 SDK/221 native old test bodies unchanged.

New SQL:4 SELECTs/20 cells, two vector modes and two zones with a fixed statement clock. Duration year boundary, date text, integer prefix, UInt wrapping, NULL and YEAR metadata are checked. No performance or physical-memory claim.

## Remaining

[Remaining acceptance](evidence/remaining-acceptance.md): DATE/DATETIME, implicit/typed/write conversions, broader M2, six complex candidates, request-root/default-NoColumns/liveDAG and final acceptance. Ordinary integer callback interfaces remain distinct from the closed value-only path. Full suites/lint/dev/bazel/release/exhaustive/performance/physical memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/whole-Go-package/PR readiness remain unverified. Historical R100 expression4/unistore1 failures remain unrepaired; prior incident receipts retained. Goal remains active.
