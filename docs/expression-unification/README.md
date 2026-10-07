# Expression unification experiment

Paired YangKeao/tidb and YangKeao/tikv branch: `expression-unification-demo`.
Current checkpoint: **legacy-cast-decimal-173**, following **union-cast-control-172**.

Functional **239/245 (97.55%)**, strict **0**, remaining **6**—unchanged. This checkpoint is partial CAST deletion, not whole-family credit.

`tidb_query_expr::native_cast_decimal` owns legacy i128 selection and numeric-input DECIMAL conversion. TiDB projects shared Decimal values; Unistore keeps child evaluation, NULL folding and Datum projection. Six local `SimpleSig::*AsDecimal` conversion bodies are deleted.

## Verification

[Evidence](evidence/legacy-cast-decimal-checkpoint.md), [commands/counts/hashes](logs/legacy-cast-decimal-summary.txt), [manifest](checkpoint.json), [ledger](migration-progress.json).

Four final Cargo gates GREEN: SDK1, native bridge1, all-six direct Unistore1 and existing comparison composition1. Two new tests;3 TiKV/116 TiDB old touched-file test bodies unchanged. One test-only compile RED from sibling-module helper names is retained; references were corrected before rerun.

## Remaining

[Remaining acceptance](evidence/remaining-acceptance.md): other direct legacy `SimpleSig::Cast*` targets still block CAST family credit; five complex exception candidates remain. Baseline unsupported signatures/admissions are unchanged. Broader M2, request-root/default-NoColumns/liveDAG and final acceptance remain. Full expression/unistore/workspace/lint/dev/bazel/release/exhaustive/performance/physical memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/whole-Go-package/PR readiness remain unverified. Goal remains active.
