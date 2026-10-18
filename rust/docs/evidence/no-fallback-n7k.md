# N7k: transaction timestamp values and canonical sources

Local checkpoint on `expression-unification-demo`; no push, PR or deployment.
Base TiDB `faa2aed4a7bf86f156c003c6df4e7d3e0735c64d`, SDK
`baea038f2759fab59fe787a8da51c7298fc9c888`. Global task #4 remains active. Paired signed local SDK checkpoint: `86d8cf1d842f7e1ebaf57ac0e3bfa98beea90d7b`.

## Change and policy

`time_fn/session_tz.rs::tidb_parse_tso` and
`time_fn/mod.rs::tidb_parse_tso_logical_in` no longer construct Ready invocations.
Current TSO shares the actual-value entry. `tikv/tso_value.rs` delegates physical
UTC/calendar work to unchanged `native_tso_utc/native_tso_core`; the logical mask
now has a single SDK value owner also used by its original RPN worker. Existing
physical wire validation, output identity codec and error channel remain intact.

The closed `TsoNative(Current/Physical/Logical)` catalog operations execute in
the existing frame. Current has no invented child and reads `current_tso`, never
`now`. Physical kernels read the actual session zone only after a positive signed
integer; raw fixed offsets retain the entire i32 range, named/local zones resolve
the original instant. Raw Time output is DateTime/FSP6 even with FSP0 metadata.
Nonpositive physical/logical kernel inputs return NULL. Current preserves even
negative transaction timestamps unchanged.

Canonical sources first run the original ScalarFunction ETInt argument cast with
its original full source FieldType. That cast can independently warn, truncate
and read a zone for either physical or logical TSO; the later physical-zone
getter remains independent. Direct time_fn callers retain their distinct
`int_arg` conversion. The ordinary ScalarFunction return-family conversion runs
once after the value kernel; TSO has no extra temporal-result policy to invent.

The host validates actual value capabilities before cloning or callbacks. Wrong
role/node/occurrence/input row and stale generations fail before context demand.
A native NULL is authenticated as a value, not a missing wire source. No store
borrow crosses transaction, zone, truncate or warning callbacks. Source/copy/
result ownership and actual formatter work use finite checked allowances.
Failures remain failures; cancellation is repeatable and slots remain reusable.
These allowances are conservative logical work bounds, not allocator-fault or
whole-statement peak-memory certification.

## Independent evidence and actual repairs

Original `tso_original_observations` records **8586 / 05FEB3EE0578F58D** before
production changes: three cache profiles,18 contexts, all three operations,
arity errors, original arithmetic fixtures, signed/unsigned/Decimal/text/error
inputs and raw Time identity/getter records. The direct physical Ready lookup
and current-TSO source refusal fail executable prohibition assertions in
`n7k-original-red.log`. The intentionally zero initial count/hash assertion only
captures observations and is not counted as semantic defect evidence.

Additive candidates match **all8586 original observations** in handle51563
(`n7k-independent-pairs.log`) while original callers and logical worker remain
independent. Only afterward does `/tmp/n7k-integrate.py` retire those callers and
make the logical worker delegate. Archived original frontend bodies and paired
test are workspace evidence, not restart scripts. Original SDK worker authority
is the signed base above. Final public calls retain the exact frozen digest. The original
Local observations were captured on home-gentoo with Asia/Shanghai localtime.
The frozen test now runs its exact original corpus in an isolated child process
with that TZ set before startup; it never changes global TZ in concurrent parent
tests. Full suite35991 also passes under a UTC parent with the same1685/exact3
and warning-title multiset, proving the fixture no longer depends on parent TZ.
The immutable hash and actual Local inputs are unchanged.

Source matrix43303 exposes a real omission: invalid text returns an error where
the original ScalarFunction succeeds because ETInt wrapping was skipped. The
source now calls the exact original `cast_arg_as_int`, preserving unsigned
metadata, warning/zone order and return conversion. Direct time_fn semantics do
not change. Fixed20363 passes all10 initial TSO tests and its full source matrix.

Review40023 exposes a real budget defect: a one-byte Decimal with integer
storage1 and million-digit visible precision returns1 under the original cast,
but the candidate incorrectly rejects16KiB. Visible precision is now charged only
for the original signed-overflow or unsigned-negative diagnostic. Quiet signed/
unsigned and negative-zero casts avoid formatting; demanded huge diagnostics are
refused before callbacks. The earlier new test expecting every huge visible
shape to fail was incorrect and was replaced, not called a semantic success.
No old hash or historical calendar fixture was regenerated.

New test-only literal ordering, Time kind method, signed Duration FSP conversion,
logical expected literal35 (actual246563), and LongLong spelling errors were
corrected before final validation; none is semantic RED evidence.

## Final validation

All jobs below are closed. Workspace logs are under
`/home/agent/.slock/agents/1555103c-9f3e-46ca-ac2e-b964c650c2f1/evidence/no-fallback/`.

- Final frontend35991 `n7k-front-full-portable.log` (parent TZ=UTC): **1685 pass / exact3 inherited failures /
  95 ignored /0 filtered**, all244 batch tests pass. Every frontend warning title
  matches N7j. Frozen8586 digest and all new source/resource tests pass.
- SDK14527 `n7k-sdk-full.log`: **1112 pass / exact1 inherited failure /1 ignored /
  0 filtered**. Existing physical/logical worker vectors and new public logical/
  catalog arity/native-capability checks pass.
- Projection18448 `n7k-projection.log`: **26 pass**. A new seven-root mixed group
  retains the old arithmetic root and adds all3 TSO outputs plus their CHAR
  parents, both flags and workers0/2; all previous mixed groups remain.
- Source matrices cover18 dynamic contexts, both flags, empty/full/2050 duplicate
  selections, original integer/unsigned/text/Decimal/Real inputs, seven return
  declarations and CHAR parents. Supplementary nine-context cases cover DateTime,
  Timestamp, Duration, JSON, ENUM, SET, BIT and unsigned Real casts. Contexts trace
  actual warnings/truncation with Warn/Error/Ignore and dynamically changing zones.
- Provider tests cover both root and operand output roles, all3 operations,
  provenance forgeries, clone vs parser refusal, original cast errors, NULL and
  nonpositive suppression, repeated cancellation/reuse, and exclusive transaction/
  zone/truncate/warning access to the value store.
- Clippy22863 `n7k-clippy.log`: **89 expression /171 all-crate warning-title multiset
  EXACT N7j**, no new lint error. Pinned Jan/Aug formatters check all15 Rust files;
  immutable STR fixtures retain exact size/SHA/valid JSON. Both git diff checks pass.
- Root `make lint` passes; no Go/Bazel file was changed, so no bazel_prepare gate.

From the TiDB root, commands used (redirected to logs above):

    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib tso_ -- --nocapture
    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib tso_independent_old_new_pairs -- --nocapture
    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib tso_unused_decimal_display_precision -- --nocapture
    TZ=UTC RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib
    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-executor --lib projection::tests
    make lint

From the SDK root:

    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tikv test --release --locked --offline -j8 -p tidb_query_expr --lib
    RUSTFLAGS='' TIKV_FRAME_POINTER=0 make run COMMAND='bash /home/agent/.slock/agents/1555103c-9f3e-46ca-ac2e-b964c650c2f1/evidence/no-fallback/n3-clippy-scoped.sh --lib --tests --release --locked --offline -j8'

Pinned formatters use `--check --edition 2021 --config skip_children=true` under
`/home/agent/tidb/expression-reuse/rustup-home/toolchains/nightly-{2026-01-30,2026-08-22}-x86_64-unknown-linux-gnu/bin/rustfmt`;
exact covered paths and fixture guards
are recorded in `n7k-format-fixtures.log`.

The inherited frontend failures remain
`pushdown_catalog::tests::ifnull_string_column_literal_uses_go_signature_and_column_collation`,
`tests::builtin_info_json_math_source::exp`, and
`tests::builtin_math_misc_op_source::vectorized_builtin_op_func`.
The SDK failure remains
`local::str_to_date_worker_tests::str_to_date_workers_pass_whole_states_keep_wide_days_and_read_typed_mode_late`.
They were not filtered, dropped or reclassified. Prior datatype two failures are
prior evidence only: no datatype production change or rerun in N7k.

Clock/calendar callers, generic admission fallback, unistore's old walker and
unsupported sources, inherited failures, Go semantic audits (%j and UNIX declared
precision), and full server/SQL gates remain open. No complete package
transcreation, broad PB-source coverage, performance equivalence or full-server
validation is claimed. The unrelated frontend untracked client BUILD.bazel was
never touched or staged.
