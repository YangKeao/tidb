# Expression unification experiment

Paired YangKeao/tidb and YangKeao/tikv branch: `expression-unification-demo`.
Current checkpoint: **datetime-control-122**, following **year-control-121**.

Functional **238/245 (97.14%)**, strict **0**, remaining **7**—unchanged. No whole CAST/M2 or Go-package credit.

SDK `native_cast_time.rs` owns explicit/computed/argument temporal control, source parser selection, warning classification and DATE finishing. Native retains actual raw views/metadata, lazy modes/clock/zone getters and warning/result/error adapters. Five native private policy helpers are deleted. YEAR/Duration and argument Time/NULL early paths remain distinct; zero-date rules, UTC numeric diagnostics and repeated DST zone reads retain their original behavior.

## Verification

[Evidence](evidence/datetime-control-checkpoint.md), [commands/counts/hashes](logs/datetime-control-summary.txt), [manifest](checkpoint.json), [ledger](migration-progress.json).

Six final gates GREEN: SDK2, native bridge1, existing CAST20/argument8, two SQL gates. Seven matched launches include an initial new-test FSP assumption failure; source-defined invalid input corrected from7 (clamps to6) to-2. No production/old-test change; RED retained. Four new tests;221 native old test bodies unchanged.

New SQL:2 SELECTs/10 cells over both vector modes, DATE clearing, numeric Decimal parsing, DST carry, numeric/text diagnostics, exact warning order and temporal metadata. No performance or physical-memory claim.

## Remaining

[Remaining acceptance](evidence/remaining-acceptance.md): other implicit/typed/write conversions, broader M2, six complex candidates, request-root/default-NoColumns/liveDAG and final acceptance. Full suites/lint/dev/bazel/release/exhaustive/performance/physical memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/whole-Go-package/PR readiness remain unverified. Historical R100 expression4/unistore1 failures remain unrepaired; prior incident receipts retained. Goal remains active.
