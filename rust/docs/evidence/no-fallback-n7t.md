# N7t: actual LIKE/ILIKE values and lazy canonical sources

Validation completed with local SDK DCO source checkpoint
388976edbb60d5c426eeeb72118b1c3d171c08b7. The frontend commit contains this receipt
and its living plan. No push or global completion is claimed.
The preceding independently signed SDK entry is TiKV
091eeaa82aaf1f45ef653f42ac027b5a8b02fb7e (N7t0).

## Scope and original behavior

Ordinary three-argument LIKE/ILIKE, AST LIKE, public utility/NULL helpers and the
legacy LIKE helper consume actual SDK values without a new Ready argument
program. The legacy unistore caller retains its own eager child traversal; that
walker is still global work. Explicit wire workers retain their compatibility
contracts and original transport planning over the same wildcard algorithm.

ScalarFunction preserves ordered child evaluation and SQL byte conversion at
each NULL barrier, the complete source escape FieldType and original
cast_arg_as_int, strict integer reading, wrapping u8 escape, original cache
eligibility and late context_id, and ordinary return-family conversion once.
SQL byte formatting is distinct from expression coerce_bytes: keep Go float
formatting, arbitrary bytes and strict Raw validation. AST LIKE still evaluates
both children before testing either NULL and suppresses both conversions when
NULL wins. NOT retains its existing outer Boolean policy. Legacy invalid NULL
witnesses retain their original Scope/Contract error and exact reason.

The dedicated LikeNative host uses the existing frame and native capability
ABI. Preparation snapshots immutable metadata and shares real owner caches;
it does not evaluate children, fetch context id, resolve caches or compile.
Each demanded capability authenticates its namespace, generation, role, source
ordinal, physical row and occurrence. Consuming actual text/pattern stages close
before conversion or callbacks; only success installs the next stage. Output
provenance belongs to the actual producer and requested output role.

Only escape casting and the final conditional context_id call may invoke session
services. String/Enum/Set cloning preserves actual immutable GoString headers;
unused huge names do not incur deep copies. Other actual inputs have bounded
copies, and all store borrows are released before callbacks. Unsupported
vector/sentinel escapes retain strict errors before copying their payloads.
Decimal integer projection reads storage; visible precision is charged only when
an original diagnostic demands it. Original panics remain panics; caught failures
cannot replay a consumed task and later invocations recover.

## Independent original evidence

Before any production candidate edit, original capture96656 recorded:

| Original corpus | Observations | Frozen checksum |
| --- | ---: | --- |
| Utility/legacy | 63,513 | 1952BB7F6D8F1E9B |
| Scalar values/coercions/context | 34,344 | C49CB830518858A9 |
| Cache history/AST | 78 | 94085E5905346BCD |

Counts/checksums were fixed and independently rerun4741 successfully before
candidate edits. The original seven-route Ready rejection test and both source
route assertions failed: LIKE and ILIKE each reported prepared0/fallback1 with
Admission refusal. Earlier compile-only test namespace/AST variant errors are
not behavioral REDs.

While old callers still existed, candidate pairing10088 passed63,504 utility/
legacy,34,344 scalar and78 cache/AST comparisons:97,926 actual pairs. Each scalar
pair used independent cold owner clones and fresh contexts/caches; history pairs
kept independent persistent owners while changing context ids7/8 and patterns.
The remaining9 original Missing/NULL/invalid-null cases are fixed witnesses with
unchanged contract branches, not additional claimed kernel pairs. All original
checksums remained unchanged before and after retirement. Do not regenerate
witnesses from corrected code or call a fixed witness new independent proof.
The N7t0 direct SDK tests additionally cover156,674 supported-policy comparisons.

Persistent workspace evidence is under `evidence/no-fallback/` in the
TiDBEngineer agent workspace. Original sources are `n7t-before-*.rs`; fixed
original tests are `n7t-original-wildcard-value-tests.rs`; executed independent
pair sources are `n7t1-independent-{all-pair-tests,candidate-value,candidate-scalar}.rs`.
Logs are `n7t-original-capture.log`, `n7t-original-frozen.log`,
`n7t1-scalar-pair-fixed.log` and the final focus/full logs. Temporary independent
pair tests were removed only after capture/archive and caller retirement; a
later empty filter is not renewed proof.

## Resource and source verification

N7t0 adds optional finite token/backtracking steps to the single original matcher,
not a second wildcard algorithm. Raw, compiled and native-collation finite entries
keep actual false, exhaustion and comparison errors distinct. A trailing percent
can skip a huge unread suffix. Cold cache checks occur inside get_or_init_cache,
and matching uses the same retained Arc whose capacities were checked. Context ids
remain the only cache key: warm entries may ignore newly supplied patterns.
Owner Clone resets caches while invocation Clone shares them.

ILIKE retains whole-buffer ASCII folding and charges it before allocation. Legacy
UTF-8 validation examines a bounded prefix; a definite invalid prefix makes the
whole operand empty without scanning/copying an ignored suffix. Unresolved
prefixes exhaust work. Go simple Unicode lowercase and trailing-escape Reject
remain separate from native ILIKE and weighted LIKE. Work/visible ownership
allowances do not certify allocator failures or whole-statement physical peaks.

The source matrix covers16 input declarations,11 return declarations, all3
argument positions, both names, four flag runs, three truncation levels, selected
and duplicate rows, CHAR parents, four collations, original short-circuit/error
priority and physical output prefixes on errors/panics. NewDate is tested as a
computed source because existing Chunk has no physical NewDate column. Existing
contradictory-return Chunk panics are compared on both paths and not normalized.

Provider tests cover both names and both output roles, all three demanded
positions and8 invalid capability classes, exclusive callback borrows, actual
huge hybrid names and unsupported vectors, consumed failure/replay/cancellation,
zone/context panics and later recovery. Persistent cache history changes actual
context ids7/8 and supplied pattern values. A full-source1MiB backtracking case
returns Resource without fallback, then the same prepared source executes NULL
successfully. Projection adds13 mixed roots, including an earlier arithmetic
output, under both flags and workers0/2.

The first Projection extension moved the existing clock cohort away from the
fixture's index0 clock setup and failed with no session clock. Only test ordering
was corrected by appending the new cohort after the existing cohorts. This is a
test-fixture correction, not a production semantic fix or a weakened assertion.

## Commands and final validation

From the TiDB worktree:

    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib wildcard_ -- --nocapture
    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib
    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-executor --lib projection::tests
    make lint

From the TiKV worktree:

    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tikv test --release --locked --offline -j8 -p tidb_query_expr --lib
    RUSTFLAGS='' TIKV_FRAME_POINTER=0 make run COMMAND='bash /home/agent/.slock/agents/1555103c-9f3e-46ca-ac2e-b964c650c2f1/evidence/no-fallback/n3-clippy-scoped.sh --lib --tests --release --locked --offline -j8'

The datatype production changes were fully tested at the signed N7t0 step:

    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tikv test --release --locked --offline -j8 -p tidb_query_datatype --lib

They remain unchanged in the subsequent source step. N7t0 datatype results are
565 passes and exactly the two inherited failures from N7c. The direct SDK finite
suite has3 passing tests. Final focus/full/Projection results, exact failure and
warning-title comparisons, lint, dual pinned14-file formatting, immutable fixture
sizes/hashes and diffs must pass the read-only `n7t-final-check.py` before DCO.

Final results: frontend1748 passes/exact3 inherited failures/95 ignored, all272
batch checks green; SDK1130 passes/exact1 inherited failure/1 ignored; Projection26
passes including the new13-root cohort under both flags/workers0/2; focus10 passes.
Frontend346 and clippy89 expression/171 all warning-title Counters match N7s/N7t0
exactly. Lint passed. Both pinned2026-01-30/2026-08-22 formatters check14 changed
Rust files, immutable fixture sizes/hashes and both diffs pass. The read-only
checker passed before DCO and is rerun after commits. Logs are
`n7t-focus-final-returns.log`, `n7t-front-final.log`, `n7t-sdk-full.log`,
`n7t-projection-final.log`, `n7t-clippy.log`, `n7t-lint.log` and
`n7t-final-check.log`. Previous Projection failure remains preserved in
`n7t-projection.log`; previous full frontend before the additional JSON return case
remains `n7t-front-full.log`. No production behavior changed after runtime gates.

Inherited frontend failures remain IFNULL column-collation, builtin exp and the
vectorized math/misc duration-FSP case. The inherited SDK failure is the
STR_TO_DATE worker cause assertion. Datatype inherited failures are duration-text
and decimal-text conversion panic expectations. They are not filtered, renamed,
or reclassified as success.

Generic admission fallback, other Ready families, old unistore traversal,
unsupported/malformed/PB sources, semantic audits, inherited failures and full
server SQL validation remain open globally. This family checkpoint is not a
complete Go-package transcreation or a release readiness claim. No Go/Bazel
changes were made; the unrelated untracked tikv-client-rs BUILD.bazel is untouched.
