# Expression unification experiment

Paired YangKeao/tidb and YangKeao/tikv branch: `expression-unification-demo`.
Current checkpoint: **charset-codec-92**, following **timestamp-diff-91**.

Functional coverage stays **228/245 (93.06%)**, strict final-audited count **0**. All228 family objects are unchanged. This is a necessary M0 type/codec prerequisite, not charset evaluator closure or a new C4 profile.

## Charset byte policy now shared

TiKV `codec/collation/native_encoding.rs` owns operation/error/collection policy, ASCII/UTF8 byte grouping, seven-encoding byte operations and valid-source-prefix counting. Four native datatype files become adapters. Existing strict UTF8 decoder and GB helpers are reused; no wire Encoding alias or dependency change.

Primitive ASCII/UTF8 transforms retain their valid-input fastpath even with zero flags; registry transforms still apply flags except Latin1/Binary identity. Invalid ASCII lead groups, UTF8 one-byte errors, strictmb3 four-byte rejection, first-error/trim/replace ordering and source-byte counts remain distinct.

Native error/result carriers, exact Display/Debug, encoding metadata/name lookup and case policy remain. `CONVERT USING`/`to_binary`/`from_binary` runtime selection and context-aware workers are next, not claimed done.

## Validation

[Evidence](evidence/charset-codec-checkpoint.md), [commands/counts/hashes](logs/charset-codec-summary.txt), [manifest](checkpoint.json), [ledger](migration-progress.json).

Eight launches: one new-test compile error fixed solely by an explicit `&[u8]` annotation; seven actual test runs produce five green gates and two unchanged old full REDs. CPP collation28, native encoding22, convert3+1ignored, original charset SQL7 and UTF8-write SQL1 pass. Two new tests pass on first actual execution; no oracle correction.

Full expression remains **1602/4old/94ignored**, unistore **220/1old/13ignored**, with identical normalized failure sections. Original eight SQL tests contain21SELECTs plus write refusals—not new zero-slot, vector/pool or C4 evidence. Fourteen original native test bodies are byte-identical. Pinned formatting/diff checks pass; scope is2TiKV/4native Rust files and one new module.

## Remaining acceptance

[Review](evidence/remaining-acceptance.md):5core,6ordinary pending,6complex exception candidates, not17approved exceptions. DATE_ADD/SUB requires ordinary calendar/Duration plus48 calculating legacy signatures; eight accepted legacy Duration→Datetime paths stay unimplemented. Request-root/default-NoColumns/live-DAG ownership and final cross-entry work remain.

Known failures and CAST/Decimal/mode/INTDIV/JSON/vector/older Values gaps remain. Workspace/lint/dev/bazel_prepare/release, exhaustive differential/TiFlash/FIPS, performance/physical memory/OOM/allocator/zero-copy/dual-timezone footprint and complete Go-package transcreation are unverified. No goal-completion or PR-readiness claim.

Three Plans agree; manifest pins their hash and paired TiKV commit. Publish TiKV then TiDB without force push or PR. Unrelated untracked client-differential BUILD stays excluded.
