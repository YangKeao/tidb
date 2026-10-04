# Expression unification experiment

Paired YangKeao/tidb and YangKeao/tikv branch: `expression-unification-demo`.
Current checkpoint: **decimal-presentation-80**, following **unix-timestamp-79**.

Functional migration remains **220/245**, strict final-audited count **0**. All220 family objects are unchanged; no evaluator credit added. Target221 needs1 more, with25eligible families remaining.

## Shared type prerequisites

TiKV datatype now owns native Decimal visible formatting, exact Ryu Go-g float conversion through the existing MySQL nine-word parser, and128-byte diagnostic subject clipping. Native Display/from_f64 are adapters; clipping is an alias. Raw sign/empty/leading/UTF8/panic and hidden-storage rounding policies are preserved. Finite-underflow empty-word zero is adapted only at the new float constructor boundary; general parser/shift/wire policies stay unchanged.

Ryu retains its existing1.0.23 identity. Its direct dependency moves to TiKV datatype, with offline-generated lock edges and removal of the unused native workspace declaration. No package version changes. Display now creates an intermediate String; allocation/performance equivalence is unverified.

FROM_UNIXTIME's required ordinary/PB/legacy stages are source-reviewed but **not migrated**. Native general Decimal parsing, M6/default-NoColumns and remaining evaluator closure are also open.

## Validation

[Evidence](evidence/decimal-presentation-checkpoint.md), [exact commands/hashes](logs/decimal-presentation-summary.txt), [manifest](checkpoint.json), [cumulative ledger](migration-progress.json).

Seven locked test launches: CPPdecimal111/warning1, native decimal101/warning5 and existing FROM_UNIXTIME3 pass. All5newtests pass first matching gate. Full expression **1578/4old/94ignored** and unistore **212/1old/13ignored** retain exact normalized failure sections and remain RED. No compile failure, retry, expectation correction, interruption or zero-match run; no new SQL probes.

Four Rust files; CPP129/native31 original test bodies byte-identical. Eleven original parser/conversion production bodies byte-identical. Pinned format/diff checks pass. Go/Bazel/generated-code/fixtures unchanged.

Existing raw INTDIV, mode forwarding, CAST diagnostic, JSON/metadata and other compatibility gaps remain. Whole workspace/lint/dev/bazel_prepare/release, exhaustive differential/TiFlash/FIPS, performance/allocator/physical heap/peak/OOM/zero-copy/dual-timezone footprint remain deferred. No whole-package transcreation or PR-readiness claim.

Three Plans agree; manifest pins their hash and paired TiKV commit. Publish TiKV then TiDB without force push or PR. Old untracked client-differential BUILD.bazel stays excluded.
