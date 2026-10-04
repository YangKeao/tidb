# Expression unification experiment

Paired YangKeao/tidb and YangKeao/tikv branch: `expression-unification-demo`.
Current checkpoint: **convert-tz-77**, following **temporal-literals-76**.

## Progress

Functional migration is **218/245**, strict final-audited count **0**. New family: **convert_tz**. All217 prior family objects are unchanged. Target221 needs3 more, with27 eligible families remaining; overall goal continues.

TiKV `native_convert_tz.rs` now owns datetime composition, SQL-zone parsing, conversion and fraction formatting. Native code retains only three original eager coercions and computed String/NULL projection through a scoped worker. Existing nullable Bytes3/Values and factory budget suffice; no new carrier, session-zone metadata, dependency or PB/legacy admission.

Named zones retain native0.10.4 identity. SYSTEM keeps its separate later-overlap/NULL-gap policy. Date parsing precedes both zone parsers; an unknown first zone does not suppress parsing the second. The original Unicode-digit offset panic is not repaired.

The old generic NaiveDateTime two-offset/gap-bisection helper is shared once with the remaining native UNIX_TIMESTAMP code. Its wide calendar/fractional domain is not replaced by packed-core behavior. This helper move earns **no Unix-time family credit**. FROM_UNIXTIME and UNIX_TIMESTAMP still require conditional coercion/getters, two potentially different zone reads, clock and independent legacy policies.

## Validation and evidence

[Evidence](evidence/convert-tz-checkpoint.md), [exact commands/hashes](logs/convert-tz-summary.txt), [manifest](checkpoint.json), [cumulative ledger](migration-progress.json).

Seven exclusive writers;14Rust files, one new module, five new tests. CPP core2/local328+1ignored, native converter4/session_tz5, and SQL1 pass. SQL has50real-column probes:24normal,24direct target-root zero-slot refusals and2filters. Typed Datetime(26,6)/binary results and prior argument-cast warnings remain intact; core-only absence of getters does not imply that unchanged SQL casts have no context reads.

Eight locked launches include one E0061 compile failure: the new facade used three Bytes3 constructor arguments instead of its existing array payload. That constructor was corrected and retried. All5newtests pass their first executed matching gate; no changed oracle. Full expression **1571/4old/94ignored** and unistore **211/1old/13ignored** retain exact normalized failure sections. No interruption or zero-match.

CPP188/native368 original test bodies and all3moved transition-helper bodies are byte-identical to prior source. Pinned format/diff checks pass; dependencies/locks, Go/Bazel/generated/fixtures unchanged.

M6/default-NoColumns, remaining evaluator closure and whole-workspace/lint/dev/bazel_prepare/release, exhaustive differential/TiFlash/FIPS, performance/allocator/heap/peak/OOM/zero-copy/dual-timezone footprint remain deferred. Existing planner date_modes forwarding, zero-date CAST warning, raw-empty INTDIV and other compatibility gaps remain explicit and unfixed. No whole-package transcreation or PR-readiness claim.

Three Plans agree; manifest pins their hash and paired TiKV commit. Publish TiKV then TiDB without force push or PR. Old untracked client-differential BUILD.bazel stays excluded.
