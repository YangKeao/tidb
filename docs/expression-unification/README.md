# Expression unification experiment

Paired YangKeao/tidb and YangKeao/tikv branch: `expression-unification-demo`.
Current checkpoint: **union-cast-control-172**, following **rand-kernel-171**.

Functional **239/245 (97.55%)**, strict **0**, remaining **6**—unchanged. This checkpoint is partial CAST deletion, not whole-family credit.

`tidb_query_expr::native_cast` now owns seven source-specific UNION CAST clamp and zero-vs-convert policies; the existing SDK integer controller owns `UnsignedInUnion`. Native code keeps name/Datum projection, concrete Decimal construction, warning effects and final merged DECIMAL target fitting. Seven native policy branches and the typed-row unsigned special case are deleted.

## Verification

[Evidence](evidence/union-cast-control-checkpoint.md), [commands/counts/hashes](logs/union-cast-control-summary.txt), [manifest](checkpoint.json), [ledger](migration-progress.json).

Five final Cargo gates GREEN: SDK1, all-seven native names1, existing decimal2, typed-row1 and set-operation SQL1. Two new tests;1 TiKV/47 TiDB old touched-file test bodies unchanged. One new-test slice-literal compile RED and one invalid SQL-oracle RED are retained honestly; both test-only issues were corrected/removed before full targeted reruns.

## Remaining

[Remaining acceptance](evidence/remaining-acceptance.md): direct legacy `SimpleSig::Cast*` bodies block CAST family credit; five complex exception candidates remain. Baseline arrays/eight unimplemented Duration-Datetime signatures are unchanged. Broader M2, request-root/default-NoColumns/liveDAG and final acceptance remain. Full expression/unistore/workspace/lint/dev/bazel/release/exhaustive/performance/physical memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/whole-Go-package/PR readiness remain unverified. Goal remains active.
