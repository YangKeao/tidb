# Expression unification experiment

Paired YangKeao/tidb and YangKeao/tikv branch: `expression-unification-demo`.
Current checkpoint: **bounded-staleness-90**, following **decimal-policy-89**.

Functional coverage is **227/245 (92.65%)**, strict final-audited count **0**. All226 prior family objects are unchanged; only `tidb_bounded_staleness` is added. The overall goal remains active.

## Bounded staleness now uses TiKV

The native selector in `time_fn/mod.rs` is removed. TiKV Head classifies actual endpoints: first invalid-zero warning, reversed-window NULL or NeedSafe. Only NeedSafe reads the original context's optional, already-zone-adjusted SafeTS once. Finish owns clamping and DateTime/FSP3 metadata while preserving raw microseconds and reserved bits.

Original eager arguments/datetime casts and warning handler authority remain. Actual NULL uses the existing Int NULL witness, distinct from malformed Head output. No new calendar/FSP validation, clock/storage query, transport or PB/legacy admission.

**SQL evidence boundary:** current SQL has no production SafeTS override and uses None→lower-bound fallback. Nondefault SafeTS clamping/getter order is covered by direct tests, not claimed as storage integration.

## Validation

[Evidence](evidence/bounded-staleness-checkpoint.md), [exact commands/hashes](logs/bounded-staleness-summary.txt), [manifest](checkpoint.json), [ledger](migration-progress.json).

Six new tests pass on first matching execution. SQL26SELECT:24direct and2filters, with **8new Head-root** zero-slot refusals separately labeled from **4existing NULL-witness** refusals. Raw123456 microseconds remain123456 despite display precision3.

Seven locked launches:5green,2unchanged old full RED. CPP core1/local343+1ignored; native bounded3 (including the original source test), gateway196+1ignored and SQL1 pass. Full expression **1600/4old/94ignored**, unistore **219/1old/13ignored** retain identical normalized failure sections. No compile failure, new failure, oracle correction, zero-match, interruption or fixture recording.

Pinned formatting/diff checks cover6native/8TiKV Rust files,2new modules.206CPP/373native original test bodies are byte-identical;3CPP/3native new tests. No Cargo/lock, Go/Bazel/generated or `compile.rs` changes.

## Remaining acceptance

[Remaining review](evidence/remaining-acceptance.md): **5core**, **7ordinary pending**, **6complex exception candidates**, not18approved exceptions. IN's demand/cache/legacy contracts were inventoried, but its old three-facade observation receipt remains untouched; a real reducer needs explicit mechanical-update authority rather than hidden calls or changed SQL expectations. Other CAST/M2/extrema/INTERVAL and actual request-owner/final cross-entry work remain.

Known failures and CAST/Decimal/mode/INTDIV/JSON/vector/older Values gaps remain. Workspace/lint/dev/bazel_prepare/release, exhaustive differential/TiFlash/FIPS, performance/physical memory/OOM/allocator/zero-copy/dual-timezone footprint and complete Go-package transcreation are unverified. No PR-readiness claim.

Three Plans agree; the manifest pins their hash and paired TiKV commit. Publish TiKV then TiDB without force push or PR. Unrelated untracked client-differential BUILD remains excluded.
