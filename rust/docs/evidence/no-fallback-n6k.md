# N6k: typed batch output ownership and exact Duration transport

This local signed-off checkpoint continues the no-fallback migration; global
alternate-evaluator retirement remains incomplete. The companion SDK commit is
`20f6608`.

## Reproduced defects and implementation

CONCAT(CAST_CHAR(ABS(column)),"x") over rows1,MIN_INT reproduced a successful
unified output prefix before the later error, while the actual native eligible
batch published no new rows (`n6k-typed-publication-red.log`). The children had
correct row demand; the root returned a typed Bytes carrier, excluded by the
former native-value Output-role staging predicate.

Publication now derives from the shared native static candidate fact and the
compiled whole-selection contract for both result representations. Native
capability results retain their existing checked Datum owner. Typed results
materialize into one private, unaliased, single-column Chunk through the same
existing write_output implementation. No expression work or second evaluator
occurs in this owner. Every conversion completes before final publication.
Final column-range append preserves the actual output index, existing prefix,
shared column identity and COW behavior. Global row mode retains streaming;
constants/columns stay excluded from native batch candidate storage.

Typed capture validates shape before copy/refusal, precharges the pending
owner, result carrier, selection and conservative encoded/conversion allowance,
and checks actual data/bitmap/offset capacities after each capture, before
further demand. Initial empty selections create no typed output owner. Header
accounting uses a conservative Chunk/opaque-column-slot allowance, not the
public Go struct-size memory_usage constant. The scratch element width is fixed
by the private constructor. Provider/SDK budgets remain separate; allocator
bookkeeping, final caller-owned output and hard whole-statement allocator peak
are outside this retained/logical accounting claim. Failures discard pending
output and remain terminal; no value/error retry selects another evaluator.

The all-carrier native comparison then exposed a second real defect: the old
Duration import rounded -123456789012ns at FSP6 to -123456789000ns. This failed
raw native output comparison (`n6k-typed-publication-front-full.log`). Duration
already stores a complete i64 nanosecond value; SQL from_nanos deliberately
rounds. A separate Duration::from_native_raw_parts validates the same FSP
metadata but preserves the already evaluated native nanoseconds. All three
frontend transport seams (literal, column batch, individual import) now use it.
SQL constructors, parsing and arithmetic policies are unchanged. The SDK test
covers28 nanosecond/FSP combinations, three invalid FSPs and unchanged SQL
rounding, including both i64 extrema. A first test compile used ParsedTime
instead of its .time field; the fixture was corrected before final checks.

## Verification

Evidence is under workspace `evidence/no-fallback/`.

- Final frontend1496 passed / exact four inherited failures /95ignored,
  including all137 batch tests (`n6k-typed-publication-front-final.log`).
- New actual native comparisons:288 across nine declared fields/eight SDK
  carriers, each with column and literal sources, four selections and four
  flag changes; four for the initial CONCAT error,12 for preexisting shared
  output/success/error/empty behavior, and one occurrence-aware individual
  Duration import. Total305 native comparisons. Selections include2050 rows;
  Duration includes sub-FSP nanoseconds and i64 extrema.
- Budget tests refuse after genuine row execution without modifying the output
  prefix, then reuse the same prepared owner successfully. A zero-budget empty
  generic selection succeeds and retains its original getter observation.
- SDK expression1058 passed / exact six inherited failures /1ignored,
  including all14 window protocol tests (`n6k-duration-core-full.log`).
- Datatype564 passed /2 failed (`n6k-duration-datatype.log`). An isolated,
  clean parent checkout at cff06c7 with the same pinned wrapper reproduced
 563 passed / the exact same two failures
  (`n6k-duration-datatype-parent.log`):
  codec::mysql::time::tests::test_native_duration_text_and_month_names and
  codec::native_integer_convert::tests::decimal_text_to_uint_preserves_round_bounds_errors_and_panic_order.
  They assert panic behavior in unchanged code. All three full-suite failure
  lists are compared exactly to their authoritative prior/parent runs. The
  full suites are explicitly non-green.
- Production Projection25 passed, including an eight-root unary matrix mixing
  native results with typed ANY_VALUE, PI and CONCAT; both global flag states
  match native in serial/two-worker execution
  (`n6k-typed-publication-projection.log`).
- `make lint`, scoped SDK project clippy (89 inherited warnings), both pinned
  format checks and both git diff checks passed. All job handles are terminal.

## Reproduction

From TiDB root:

    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib
    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-executor --lib projection::tests
    make lint

From TiKV root:

    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tikv test --release --locked --offline -j8 -p tidb_query_datatype --lib
    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tikv test --release --locked --offline -j8 -p tidb_query_expr --lib
    RUSTFLAGS='' TIKV_FRAME_POINTER=0 make run COMMAND='bash /home/agent/.slock/agents/1555103c-9f3e-46ca-ac2e-b964c650c2f1/evidence/no-fallback/n3-clippy-scoped.sh --lib --tests --release --locked --offline -j8'

The parent comparison uses a separate detached worktree at cff06c7 and the
same datatype command. Pinned rustfmt: nightly2026-08-22 for frontend files,
nightly2026-01-30 for duration.rs, edition2021 and skip_children=true.

## Remaining scope

Output ownership does not establish native operand demand. Ordinary typed
roots and source/value bridges still require native error-order comparisons;
non-candidate roots must retain their row behavior even when the global flag
is enabled. Do not replace valid bulk arithmetic with row evaluation when its
operand closure is incomplete. Deferred/type-before-value sources, other
function families/callers, effectful multi-output ordering and global alternate
Expr dispatch retirement remain open.
