# Expression unification experiment

Paired YangKeao/tidb and YangKeao/tikv branch: `expression-unification-demo`.
Current checkpoint: **from-unixtime-81**, following **decimal-presentation-80**.

Functional migration: **221/245 (90.20%)**, strict final-audited count **0**. FROM_UNIXTIME adds one family; all prior220 objects are unchanged. The functional90% threshold is reached, but **M6 and final acceptance remain unfinished**. There are24eligible families remaining.

## FROM_UNIXTIME

Five TiKV profiles own actual numeric/text parsing, epoch/report construction, zone projection, typed legacy conversion and genuine NULL. Native code retains coercion and computed warning policy before zone demand, then lazy layout coercion after valid local output. Complete SDK epoch/report bytes are forwarded unchanged under one selected scope, reusing DATE_FORMAT workers.

PB preserves observed-NULL demand, the one-argument extra Time cast and outer declared-family conversion. Legacy retains its borrowed request zone, f64 range, ordinary u32 nanos-times1000 behavior and FSP0 with hidden microseconds. Missing-first panic and delayed lossy layout remain. A scoped callback forwards raw_columns, not SimpleExpr::Shared's own context; that broader M6 gap remains explicit. Existing wire algorithms and R83 datatype/dependency policies are unchanged.

## Validation

[Evidence](evidence/from-unixtime-checkpoint.md), [exact commands/hashes](logs/from-unixtime-summary.txt), [manifest](checkpoint.json), [cumulative ledger](migration-progress.json).

Eleven locked launches: seven green, two new-test input failures corrected and retried, and two old full-suite failures. All10new tests finally pass. CPPcore2/wire2/local330, native root7/gateway196, legacy1 andSQL1 pass (local/gateway each retain one ignored). SQL includes42real-column SELECTs:20normal,20zero-slot and2filters.

Full expression **1582/4old/94ignored** and unistore **213/1old/13ignored** retain exact normalized failure sections and remain RED. New-test corrections only change invalid input construction: raw Decimal width/header corruption and legacy's binary64 inclusive range boundary. Production and expected results were not changed to match output. No compile failure, zero-match, interruption or fixture recording.

Eighteen Rust files, two new modules; CPP205/native476 original test bodies and five other production bodies are byte-identical. Pinned format/diff checks pass; dependency/lock/Go/Bazel/generated/fixture scope is unchanged.

## Remaining acceptance

Prioritize M6/request-root/default-NoColumns integration and final type/deletion acceptance; reaching221 is not whole-goal completion. General type/parser work, legacy Shared-child context, older Values reply-floor precharge, raw INTDIV, mode forwarding, CAST diagnostic and JSON/metadata gaps remain. Whole workspace/lint/dev/bazel_prepare/release, exhaustive differential/TiFlash/FIPS and performance/allocator/physical heap/peak/OOM/zero-copy/dual-timezone footprint are deferred. No whole-package transcreation or PR-readiness claim.

Three Plans agree; manifest pins their hash and paired TiKV commit. Publish TiKV then TiDB without force push or PR. The old untracked client-differential BUILD.bazel stays excluded.
