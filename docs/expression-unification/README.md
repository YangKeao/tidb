# Expression unification experiment

Paired YangKeao/tidb and YangKeao/tikv branch: `expression-unification-demo`.
Current checkpoint: **temporal-parser-75**, following **temporal-foundation-74**.

## Progress

Functional migration remains **215/245**, strict final-audited count **0**. This datatype prerequisite adds **no family credit**; target221 needs6 more, with30 eligible families remaining. All215 prior family objects are unchanged.

TiKV now owns DATE/DATETIME/TIMESTAMP string/numeric parsing, construction/setter/validation policy and the original nine-cause error. Native TimeType reuses TiKV's existing enum; native Time remains a raw/kind/FSP facade. Hidden DATE clocks, FSP ordering, numeric error-side values, float casts/rounding and original Decimal Display representation are preserved.

Native SessionTimeZone/Offset are shared aliases, using a separately pinned **chrono-tz0.10.4**, not wire **0.5.3**. Both Cargo locks were generated, with no existing package upgrades; native/shared package and chrono trait identities are verified. Original raw-name/offset, conversion-only clamp, UTC-name and from_offset behavior remain. Dual timezone-database footprint/performance is unmeasured.

No C4 profile, admission, carrier or driver is introduced. YEAR/INTERVAL, broader temporal methods and evaluator closure remain open. This completes parser prerequisites for subsequent TIMESTAMP/literal workers, not those evaluator families.

**Retained compatibility limitations:** INTDIV raw-empty coefficient lhs divided by1 previously returned exact zero, but shared math rejects it; no native fallback hides it. Newly exercised zero-date CAST warning mismatch also exists on the prior checkpoint and remains unfixed (details below).

## Validation and evidence

[Evidence](evidence/temporal-parser-checkpoint.md), [exact commands/hashes](logs/temporal-parser-summary.txt), [manifest](checkpoint.json), [cumulative ledger](migration-progress.json).

Four exclusive writers plus bounded reviews;7 Rust files,3 new modules,4 new CPP tests. Ten test launches:9 current and1 isolated previous-checkpoint replay. CPP time65, native datatype453 and4 original SQL tests pass; all new tests pass first gate. Full expression **1568/4old/94ignored** and unistore **211/1old/13ignored** retain identical normalized failure sections.

One additional existing SQL test remains **RED**: CAST zero-date warning uses `0000-00-00 00:00:00.000000` instead of original `0000-00-00`. Clean detached native8987c0c2/CPPc5e3861 replay reproduces the same complete failure section. No test/fixture/production repair; cases after the failing assertion are not claimed exercised. Replay worktrees were removed without touching active implementation trees.

CPP51/native64 original test bodies are byte-identical; SQL files unchanged. Pinned formatting and diff checks pass. Two preliminary dependency-audit scripts failed on Cargo name qualification/path-registry duplication assumptions; corrected semantic-edge audit passes. No compile failure, zero-match, interruption or current-tree test retry. One manifest/two locks intentionally change; no Go/Bazel/generated/fixture changes.

M6/default-NoColumns and remaining evaluator closure, RAND state, password Unicode/lazy policy, lexer/digest, plan codec/proto and JSON_SUM_CRC32 ARRAY admission remain open. Whole workspace/lint/dev/bazel_prepare/release, performance/zero-copy/heap/peak/OOM and exhaustive differential/TiFlash/FIPS remain deferred. No whole-package transcreation or PR-readiness claim.

Three Plans agree; manifest pins their hash and paired TiKV commit. Publish TiKV then TiDB without force push or PR. Old untracked client-differential BUILD.bazel stays excluded. Overall goal continues.
