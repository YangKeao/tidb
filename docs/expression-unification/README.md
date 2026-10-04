# Expression unification experiment

Paired YangKeao/tidb and YangKeao/tikv branch: `expression-unification-demo`.
Current checkpoint: **timestamp-diff-91**, following **bounded-staleness-90**.

Functional coverage **228/245 (93.06%)**, strict final-audited count **0**. All227 prior family objects remain byte-identical; only `timestampdiff` is added. Overall goal stays active.

## TIMESTAMPDIFF and temporal types now shared

`tikv/timestamp_diff.rs` connects ordinary/typed/Shared PB text evaluation and the distinct residual manual legacy raw-core domain to TiKV. Native calendar arithmetic, CoreTime difference algorithms, duplicate types and unit lookup are removed or thin aliases/delegates.

Policies remain distinct: Text uses visible Time formatting, wide-year strict parsing, civil days and signed months; Core uses actual raw bits, full-core zero, exact unit bytes and unsigned months. Year0 Jan1→Mar1 is60days Text versus59 Core. Original integer widths/panic order and TimeDifference Debug name remain. Named datatype helpers still report InvalidUnit; TiKV wire's constant-unit/error policy is unchanged.

Ordinary eager datetime casts/coercions stay; Shared PB keeps first-NULL before prefix coercion/suffix/arity; manual legacy keeps unit-first and both-endpoint demand. No new SQL/PB admission, carrier, general driver or existing wire encoding change.

## Validation

[Evidence](evidence/timestamp-diff-checkpoint.md), [commands/counts/hashes](logs/timestamp-diff-summary.txt), [manifest](checkpoint.json), [ledger](migration-progress.json).

Nine new tests pass on first matching execution. SQL34SELECT=32direct+2filters, including16new Text-root zero-slot refusals (four with NULL), not borrowed PB NULL-witness proof. StoredTime/NULL casts introduce no earlier worker. SignedLongLong20/0 metadata, full-month microsecond boundary, negative fractional truncation, leap2000 and NULL are pinned.

Ten locked nonzero launches:8green,2unchanged old full RED. CPP datatype1/time79/local345+1ignored; native datatype91/root4/gateway196+1ignored/legacy2/SQL1 pass. Full expression **1602/4old/94ignored**, unistore **220/1old/13ignored** retain identical normalized failure sections. No compile failure, new failure, oracle correction, zero-match, interruption or fixture recording.

Pinned formatting/diff checks cover12native/9TiKV Rust files,2new modules.260CPP/542native original test bodies are byte-identical;4CPP/5native new tests. No Cargo/lock, Go/Bazel/generated or compiler changes.

## Remaining acceptance

[Review](evidence/remaining-acceptance.md):5core,6ordinary pending,6complex exception candidates, not17approved exceptions. Actual request-root/default-NoColumns/live-DAG ownership and final cross-entry work remain. IN's old observation receipt remains unchanged pending mechanical-update authority; no hidden worker calls.

Known failures and CAST/Decimal/mode/INTDIV/JSON/vector/older Values gaps remain. Workspace/lint/dev/bazel_prepare/release, exhaustive differential/TiFlash/FIPS, performance/physical memory/OOM/allocator/zero-copy/dual-timezone footprint and complete Go-package transcreation are unverified. No goal-completion or PR-readiness claim.

Three Plans agree; manifest pins their hash and paired TiKV commit. Publish TiKV then TiDB without force push or PR. Unrelated untracked client-differential BUILD stays excluded.
