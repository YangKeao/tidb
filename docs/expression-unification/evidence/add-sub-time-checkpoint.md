# ADDTIME and SUBTIME — add-sub-time-71

Round72 follows time-microsecond-70. Both families are functionally closed: **213/245**, strict0,32 eligible remain and8 more needed for221. Previous211 family objects remain unchanged.

## Shared value foundation

Existing shared duration/datetime DTOs gain the original combine, date addition, formatting, zero/range and day-number inverse methods. Native private DTOs become aliases and remaining free functions delegate. Day-number calculation reuses the existing shared i64 implementation rather than another formula. The duration-shape predicate and add/sub FSP policy also move once.

The zero-valued duration exception, multiply-before-zero-check, saturating addition, checked date arithmetic then signed absolute value, zero-date inverse result, wide fields, fractional truncation and original loose range predicate are preserved. This is not a replacement with wire Duration/Time or chrono validation. Other consumers benefit from SDK sharing without gaining family credit.

## Three closed profiles

AddTimeNative/SubTimeNative consume two actual nullable coerced UTF8 texts and actual signature metadata through existing BytesBytesInt. Sign belongs to the fixed profile, not an operation encoded in data. Six metadata bits represent original left/right temporal categories, constant-row path and the right BinaryLiteral/Bit fact. They do not contain a parsed answer.

Static right-Datetime signatures originally return NULL before any value coercion. A separate TimeAddRightDatetimeNative unary-Int profile carries only actual signature metadata and computes that NULL. No invented SQL NULL text operands or witness stand in for unread data. Missing metadata is an infrastructure contract error, not automatically propagated NULL. Existing outer child evaluation remains eager.

The ordinary path preserves tuple coercion: left NULL still coerces right, left error does not. Datetime/Date signatures parse right first; Duration parses left first; Other parses right first, retains binary warning suppression and the ADDTIME-only constant-row trailing-dash check. Constant-row selection is not the session vectorized flag.

Shared output is silent NULL, actual formatted text, or a strict computed warning disposition implying NULL. Native keeps original coerced text solely for diagnostics, applies the original byte cap only to truncated-time warnings and uses full text for incorrect-time/datetime warnings. All use direct append_warning1292, regardless of truncation policy; no new getter or handle_truncate call is introduced. Native never parses, performs arithmetic or supplies the answer.

## Existing entry boundaries

AST and typed roots keep their existing signatures. Typed post-result Datetime/Duration conversion stays outside the worker: non-NULL datetime reads modes then timezone, while duration conversion reads timezone even on NULL. Core getter absence does not imply entry-wide getter absence.

No native PB/catalog/legacy admission exists for ADDTIME/SUBTIME; ordinary TiKV functions do not prove such admission. No cophandler modification or fictitious NULL shim is included. Timestamp and TimestampAdd remain separate families. Timestamp needs the remaining generic numeric/string and session-timezone parser; TimestampAdd has a bounded unit/month arithmetic closure available for a later batch.

## Validation

Eleven serialized parent Cargo gates:10 nonzero green,1 unchanged old full-expression failure, no compile/new RED/zero-match/retry. [Exact command receipts and hashes](../logs/add-sub-time-summary.txt) cover SDK2/core-and-wrappers2/local323+1ignored; native root1/source52+6ignored/captured1/SDK2/calendars22/TIMESTAMP consumers1/SQL2 (overlapping filters). All five original ADDTIME/SUBTIME source tables ran and passed. SQL observes32 typed results plus4 constant-row controls, exact metadata/warnings and8 direct zero-slot executions. Malformed stored Duration and binary suppression are not falsely claimed SQL cases; native/core tests cover those boundaries.

Full expression1566/4/94 retains the complete old failure section/list byte-for-byte after numeric panic-thread IDs alone become THREAD, normalized SHA256411274feba11e202735df5a8056c72babbc8cf9df2eab3258c670e9289a1ee95. Unistore had no source/admission change and was not rerun. CPP182/native352 original test bodies and ten neighboring temporal functions are byte-identical. Seven new tests pass; all13 sources pass pinned formatting, both diff checks pass, and no dependency/generated/Go/Bazel changes occur. A bounded independent review found no required correction. No provider-derived expectation, fixture recording or old oracle change.

The inverse's full i64 admission guard remains before narrowing: only internal years1..10000 reach checked conversion to the existing calendar helper. Its original upper edge3652499 still maps to10000-03-15, with separate unchanged range validation. These oddities are preserved rather than fixed during migration.

## Limits

No new carrier, result kind, driver, binding, runtime cause or admission. Three arguments plus a call fit the existing four-node allowance. Strict completion, entire temporal type/parser migration, whole workspace/lint/dev/bazel_prepare/release, exhaustive differential/TiFlash/FIPS, allocator/fault/peak/OOM/zero-copy/performance, M6/default-NoColumns and previously documented JSON_KEYS/AST/SQL metadata/parser/GB/vector/Decimal/raw-math gaps remain unclaimed. This is not whole-package transcreation or PR readiness.

Seven exclusive write owners implement the batch; one agent checks existing admission boundaries. Parent owns integration, formatting, tests, Plan, evidence and paired publication. TiKV publishes first, then TiDB with matching Plan snapshots; the old untracked client-differential BUILD.bazel remains excluded.
