# N7w: execute ASCII and space trimming over actual bytes

SDK signed local checkpoint: a278ff7 (DCO); no push.

This local checkpoint replaces ordinary ASCII/LTRIM/RTRIM Ready calls with
actual byte values and consuming canonical sources in the existing SDK frame.
The original SDK ASCII owner remains unchanged. After independent proof, both
original trim writer workers and the new borrowed trim API delegate to the
single original U+0020 scan owner, impl_string::trim_space_bytes. The explicit
wire ABI remains available. ASCII borrows real byte-bearing inputs without
copying a suffix it never reads; trim budgets the demanded scan and owned result.
The frontend keeps general expression byte coercion separate from trim's actual
ETString body and its original pure signature cast. Binary-versus-character
results follow the actual cast value; original ordinary return coercion runs once.

## Independent original proof

Before candidate source edits, n7w-original-capture.log recorded direct4023/
C717C464D28B4353 and scalar486783/B3CF56F72DC700B1. Those counts/checksums were
frozen from actual output and rerun unchanged in n7w-original-frozen.log. All
three Ready guards and all three source prepared0/fallback1/Admission guards
failed on the original implementation. Tests use149 raw values, three context
profiles and three truncation levels; scalar calls cover11 source declarations
and11 return declarations, with original event/panic/identity observations.
An explicit test-only UTC date input prevents the known daily conversion-clock
dependency; production clock behavior and old N7u hashes are unchanged.

While original frontend functions and SDK writer loops were still intact,
n7w-independent-pairs.log compared every490806 genuine direct/scalar old/new
outcome with fresh independent contexts. All passed. Executed source snapshots
n7w-independent-{tests,front,scalar,sdk-candidate,sdk-original}.rs were archived
BEFORE caller retirement or trim owner consolidation. The original SDK archive
is byte-identical to signed c15a1f13's impl_string.rs. Candidate/pair methods
were removed only after archival. Fixed final witnesses are not new independent
proof. The final borrowed scan has one owner; no marker, wire encoding, fake
operand, expression traversal or alternate SQL algorithm is introduced.

## Consuming actual sources and boundaries

ByteUnaryNative has exactly one authenticated actual capability argument. Its
metadata records the real argument ordinal, operation, stored constant probe,
result field, producer node and output role. Preparation budgets actual probe
ownership before cloning. Start performs the original stored first-Constant
probe before demanding argument0. Resume authenticates role/node/generation/
namespace/row/occurrence, then consumes body demand BEFORE conversion, resource
failure, panic or output publication. The original ETString cast/body are pure
and read no Columns; source callbacks run without a store borrow. Output has
the producer's own capability role/origin, and return coercion occurs once.
Prior exact typed SDK recipes retain their existing same-frame route.

Expanded source checks cover17 declarations,11 returns, literal/column/computed
positions, three profiles/truncation levels, four flag runs, reversed duplicate
selections, eager child errors and original physical output prefixes. Stored
constant and computed malformed Decimal coefficients retain their original
probe/body panic phases. Provider checks exercise both output roles,54 bad
capability/index cases, invalid replay, double cancel, original raw/binary/hybrid
results and panic consumption. Real1MiB Bytes and Raw ASCII inputs succeed under
128KiB body allowance because no unused suffix is copied. Demanded huge valid
Decimal formatting refuses before unbounded work. All three source Resource
errors are terminal without fallback; the SAME prepared source then executes
NULL successfully. SDK goldens separately check NULL versus empty, invalid UTF-8,
Unicode and U+0020-only trim rules, actual borrowed slice identity, ASCII's finite
one-byte work and finite resource rejection. New7-root mixed Projection cohort
is appended last, retaining the original clock setup index, and covers both
execution flags and workers0/2.

## Preserved cache tests

The initial integration exposed seven compile-only references to the old private
ASCII wire helper in cache protocol tests. That exact helper is retained only
as PRIVATE cfg(test), with no production reexport/caller; old cache assertions
remain intact. The first full frontend run separately exposed an old suite test
that expected ASCII to create one Ready worker. The ordinary ASCII route now
correctly creates none. The cache reuse test still asserts one prepared worker
across two chunks, using the surviving explicit UNCOMPRESSED_LENGTH header worker.
A separate ASCII test preserves its original A/B->65/66 result checks and
requires zero Ready workers across both chunks. No Ready call was reintroduced
to satisfy an obsolete architecture witness, and no cache reuse assertion was
removed. The first full log is retained as
n7w-front-before-cache-witness-update.log.

## Validation and remaining work

All builds reached terminal status. Final frontend71540 passes1773 with exactly
the prior3 failures and95ignored; all285 batch tests pass. SDK92308 passes1132
with exactly the prior1 failure and1ignored. Provider focus36335 passes7 with
both frozen checksums unchanged. Projection9514 and final83684 pass26 with the
new7-root cohort, both flags and workers0/2. The final frontend additionally
passes both retained cache reuse and new ASCII zero-worker tests.

Frontend346 and SDK17 warning-title Counters exactly match N7v; scoped SDK
clippy62846 completes with89 expression warnings and171 overall, Counter-identical
to N7v. Lint59613 passes. Both pinned formatters check18 touched Rust files;
immutable STR_TO_DATE fixtures and both git diff --check gates pass. Read-only
n7w-final-check.py checks exact counts/failures/warnings, original hashes, all
independent pair evidence, signed original SDK archive and unchanged clock
harness. It passes after all runtime checks. Production datatype sources and the
frontend development clock harness are unchanged in N7w: reuse N7v wrapper515/
exact2 including scope/nesting/unwind/thread/zone guards and default-OFF production
check, and unchanged SDK datatype565/exact2. These are distinct datatype suites.

Inherited frontend failures remain
pushdown_catalog::tests::ifnull_string_column_literal_uses_go_signature_and_column_collation,
tests::builtin_info_json_math_source::exp and
tests::builtin_math_misc_op_source::vectorized_builtin_op_func. SDK failure remains
local::str_to_date_worker_tests::str_to_date_workers_pass_whole_states_keep_wide_days_and_read_typed_mode_late.
No inherited failure is removed, filtered or relabeled.

Final logs in the persistent agent evidence/no-fallback directory are
n7w-provider-focus.log, n7w-front-final.log, n7w-sdk-full.log,
n7w-projection-final.log, n7w-projection-after-cache-test.log, n7w-clippy.log,
n7w-lint.log and n7w-final-check.log. The compile-only missing private test helper
and obsolete cache architecture witness are recorded separately above; they
are not production result/demand regressions.

No Go/Bazel source changed; make bazel_prepare was not triggered. Finite logical
work/copy allowances do not certify physical allocator failures or whole-query
peak memory. Remaining functions, generic admission fallback, unistore traversal,
unsupported/PB/malformed/deferred sources, typed NAME_CONST parent coverage,
inherited failures and independent Go calendar/UNIX precision/full-server SQL
validation remain open. No complete upstream Go package transcreation is claimed.


## Reproduction

From the frontend root:

    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib byte_unary_ -- --nocapture
    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib
    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-executor --lib projection::tests
    make lint

From the SDK root:

    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tikv test --release --locked --offline -j8 -p tidb_query_expr --lib
    RUSTFLAGS='' TIKV_FRAME_POINTER=0 make run COMMAND='bash /home/agent/.slock/agents/1555103c-9f3e-46ca-ac2e-b964c650c2f1/evidence/no-fallback/n3-clippy-scoped.sh --lib --tests --release --locked --offline -j8'

From the persistent agent workspace run
`python3 evidence/no-fallback/n7w-final-check.py`. Original-candidate pair scripts
are applied historical artifacts and must not be rerun against the retired
implementation; their executed source snapshots and logs preserve that proof.
