# Expression unification experiment

Paired YangKeao/tidb and YangKeao/tikv branch: `expression-unification-demo`.
Current checkpoint: **date-core-63**, following **clock-three-62**.

## Progress

Functional delegation plus native algorithm deletion: **202/245**. Target221 needs19 more;43 eligible families remain. Strict final-audited acceptance stays **0**; overall goal remains active.

DATE now uses one shared native raw-core projection, also reused by the typed clock SDK. Ordinary/PB evaluation preserves original zero-mode diagnostics and sends actual core/modes to the worker; soft-invalid input is not replaced with a fake NULL. The computed signed bit carrier preserves high year bits when boxed as native Date/fsp0.

Legacy DATE keeps its separate nullable-core predicate policy without native mode reads. Duplicate projection/decimal truth work is deleted. PB observed NULL retains unread suffixes and arity bypass. No wire policy or temporal-value admission is expanded.

## Evidence and limits

[Evidence](evidence/date-core-checkpoint.md), [exact commands and hashes](logs/date-core-summary.txt), [manifest](checkpoint.json), [cumulative ledger](migration-progress.json).

Eight exclusive writers;17 Rust files;no new Rust source;10 new tests pass. Ten Cargo runs: eight green targeted gates and two known full-suite RED runs. No compile failure, interruption, zero-match, retry or gate-driven source/test repair.

TiKV datatype1/core2/prior typed SDK2/local312+1ignored; native SDK1/typed-helper5/legacy1/SQL2 pass. SQL includes8 value/metadata pins,2 soft-warning cases and6 direct zero-slot expressions. Full expression **1540/4old/94ignored**, unistore **208/1old/13ignored** retain complete prior failure sections after only thread-ID normalization. All four new expression tests, including native policy and PB tests, are verified passing in the full log; no extra targeted invocation is claimed.

Original test bodies/oracles are unchanged. Prior extra JSON_KEYS aggregate mismatch remains unresolved and was not rerun; the earlier CRC32 quoted-SQL rejection gains no credit.

M6, broader default-NoColumns roots, workspace/lint/dev/bazel_prepare, release/performance/zero-copy/physical heap/peak/OOM, exhaustive differential/TiFlash/FIPS and prior parser/GB/vector/Decimal/deep-JSON exceptions remain deferred. No package-transcreation, PR-readiness or performance-neutrality claim.

Both tracked Plans equal the root Plan; the manifest pins their hash and paired TiKV commit. TiKV publishes first, then TiDB; no force push or automatic PR. The old untracked client-differential BUILD.bazel stays excluded. Next RO preference: WEIGHT_STRING plus FORMAT, with explicit public locale-SDK dependency design; password-strength and temporal parser closures remain separate work. No advance credit.
