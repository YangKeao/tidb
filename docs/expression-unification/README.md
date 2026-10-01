# Expression unification experiment

Checkpoint-ID: `compression-two-29` (previous: `exp-log-two-28`)

**93/245 frozen families delegate to TiKV with native evaluator algorithms removed; target 221.** This checkpoint adds COMPRESS and UNCOMPRESS. Strict final-audited acceptance remains 0; the experiment is incomplete and not PR-ready.

## Paired repositories

- [YangKeao/tidb, expression-unification-demo](https://github.com/YangKeao/tidb/tree/expression-unification-demo)
- [YangKeao/tikv, expression-unification-demo](https://github.com/YangKeao/tikv/tree/expression-unification-demo)

Use sibling checkouts. `checkpoint.json` pins TiKV and the published Plan hash. Root Plans mirror `/home/agent/tidb/EXPRESSION_UNIFICATION_PLAN.md` at publication. Validated steps push both branches without force-push or automatic PRs.

## This checkpoint

- **One compression owner:** the existing Go-compatible encoder moves to TiKV `impl_encryption/native_go_flate.rs`. Before formatting, all 1153 relocated lines match the original. After a limited header correction, the entire production body matches the original under the same pinned formatter. Native `go_flate.rs` shrinks from 1177 to 44 lines, retaining only the original byte fixtures and a test-only import.
- **One native inflation owner:** the bounded decoder moves to TiKV `impl_encryption.rs`. Its inner body is byte-identical: 8 KiB scratch, a one-byte excess probe, limit rejection before growth, complete-stream/checksum/progress requirements and ignored trailing data remain. Decoded-empty success remains distinct from wire's policy.
- **Minimal wire edits:** wire UNCOMPRESS's body is unchanged. Wire COMPRESS and native framing each replace only two primitive calls to share the little-endian prefix and trailing-dot rule, preserving their allocation and I/O order. Wire's encoder is not substituted for native's byte-compatible encoder.
- **Actual completed outcomes:** COMPRESS reuses ordinary owned Bytes. UNCOMPRESS has a separate sealed result: NULL, decoded bytes (including empty), corruption or output limit. Only its selected recipe interprets the canonical internal envelope. Malformed result framing is a contract error, not a SQL corruption warning.
- **Warnings remain native policy:** guarded coercion passes actual input to the existing worker. Only completed outcomes produce the original 1259/1258 warnings. NULL and empty inputs still enter the worker. Pool refusal does not become early NULL, overflow or a guessed zlib warning.

Value extraction copies only the decoded payload, with fallible allocation and the existing requested-length/actual-capacity overlap checks. The encoded result, including its tag, remains charged until release. Status outcomes retain no payload allocation. This is not zero-copy or a bound on allocations inside the decoder. No new input role, driver, generic graph, mutable worker warning context, four-column allowance or PB/legacy entry is added. Three narrow pure-function exports preserve original tests; native production does not use that bypass.

Fourteen live Rust files changed: six native and eight TiKV, including one new module. Both lockfiles, the original 24-line Go fixture block, the 758-line crypto test block and all crypto source from UNCOMPRESSED_LENGTH onward remain unchanged. Obsolete module comments were corrected without changing expected values.

## Actual validation

| Scope | Result |
|---|---|
| TiKV all local evaluator tests | 250 passed, 1 existing ignored |
| TiKV encryption tests, including original wire cases | 10 passed |
| Original Go compression byte-fixture test | 1 passed |
| Original native crypto tests | 15 passed |
| New native dispatch/diagnostic tests | 3 passed |
| SQL/lifecycle tests | 61 passed |
| Full native expression library | **1440 passed, 4 unchanged failures, 94 ignored; exit 101** |

All six targeted Rust runs passed on their first attempt. The entire full-expression failure section matches checkpoint28 after replacing only panic-heading thread IDs; the old EXP expectation conflict remains among those failures. Before source freeze, text comparisons caught and corrected transcription differences; this is distinct from the absence of Rust compilation failures or test retries.

New SQL tests cover four normal stored rows, independent static Go/MySQL compressed frames, result metadata, three corruption/limit diagnostics and eight direct zero-slot refusals. The raw `00FF20` compression case checks only prefix/suffix and round-trip consistency, not a complete independent compressed-byte golden. Kernel-generated test streams are similarly policy/ownership checks, not independent encoder oracles.

Seven exact command receipts: [summary](logs/compression-summary.txt). Ownership, source-copy hashes and compatibility: [evidence](evidence/compression-checkpoint.md).

## Remaining work

JSON_VALID, JSON_TYPE and JSON_DEPTH are next read-only candidates, not credited. Their numeric, malformed typed-BinaryJSON and document-conversion policies must be preserved; existing TiKV JSON representation limits prevent assuming a general lossless bridge. JSON_LENGTH's optional path and diagnostic differences require separate work.

Complete operation-scope coverage, allocation/high-water and physical-peak checks, paired differential reruns, full codec-domain equivalence, release performance, whole workspace, `make lint` and TiFlash integration remain unfinished. Full datatype, unistore and parser-charset suites were not rerun; historical non-green results are not passing evidence. No compressed-input OOM-safety, all-input/CPU equivalence or complete Go-package transcreation claim is made.
