# Expression unification experiment

Paired branch: `expression-unification-demo` in YangKeao/tidb and YangKeao/tikv.
Current checkpoint: **native-sql-string-112**, after **cast-string-111**.

Functional **238/245 (97.14%)**, strict **0**, remaining **7**—unchanged. This datatype/partial-CAST step earns no family credit. Overall goal remains active.

## Shared SQL stringification

`tidb-datatype/src/datum/stringify.rs` now projects19 actual source variants into SDK `codec/native_sql_string.rs`. SDK owns SQL byte/string selection, UTF8 stages, sentinel errors and fixed/scientific float primitives. Native selector, UTF8 helper and formatter cluster are deleted.

Borrowed raw Decimal/Time/Duration/JSON/vector views reuse existing shared formatters without normalized constructors or host formatter callbacks. Raw's early validation, arbitrary byte-kind results, Float32 narrowing and original Display-error panic domains remain distinct. General label/row/literal selectors are not claimed as migrated.

## Evidence

[Checkpoint](evidence/native-sql-string-checkpoint.md), [exact commands/counts/hashes](logs/native-sql-string-summary.txt), [manifest](checkpoint.json), [ledger](migration-progress.json).

**Nine matched gates green on first attempt**, including full native datatype464, original CAST consumers, new SQL and both prior string/floating SQL tests. Four new tests;193 old native test bodies unchanged. New SQL covers eight SELECTs/28 cells across scalar/vector modes through the unchanged CHAR/BINARY datatype API.

No new C4 profile, carrier, admission or performance claim. Historical failures remain in prior evidence.

## Still open

[Remaining acceptance](evidence/remaining-acceptance.md): other CAST and typed/vector/UNION domains, other datatype conversions and broader M2, six complex candidates, request-root/default-NoColumns/liveDAG and final acceptance.

Full expression/unistore were not rerun; historical4+1 failures remain. Workspace/lint/dev/bazel/release/exhaustive/TiFlash/FIPS/performance/physical memory/OOM/allocator/dual-tzdata/whole-Go-package/PR readiness are unverified. The manifest pins paired TiKV and three identical Plans. No force push or PR; unrelated BUILD excluded.
