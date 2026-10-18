# N7l: statement/live clock values and canonical sources

Local-only checkpoint on expression-unification-demo, task #4 remains active.
Base TiDB8d321345f344356df947191fffe036976c7843e9 and
SDK86d8cf1d842f7e1ebaf57ac0e3bfa98beea90d7b. No push/PR/deployment.
Paired signed local SDK checkpoint: `52cfb464e31bd14ab1f3e7babf7e8ecdd6849909`.

## What changed

All12 native clock names now use actual clock values without Ready construction:
NOW/CURRENT_TIMESTAMP/LOCALTIME/LOCALTIMESTAMP, UTC_TIMESTAMP,
CURDATE/CURRENT_DATE, UTC_DATE, CURTIME/CURRENT_TIME, UTC_TIME and SYSDATE.
SDK native_clock.rs retains every existing pure arithmetic/formatting kernel;
NativeClockOperation and finite native_clock_values select those policies over
actual UTC seconds/nanoseconds/offset/FSP. Compatibility RPN workers/validators,
ordinary wire behavior and SQL builder precision gates remain unchanged.

Frontend tikv/clock_value.rs owns original context/precision demand, not an
expression evaluator. SYSDATE reads its alias flag before arity/FSP validation.
True selects NOW labels/truncation. False validates precision via original
Datum.ToInt64, gets the statement's frozen offset, then captures actual SystemTime.
It never substitutes statement seconds. CURTIME() truncates; explicit precision
(including zero/NULL) first truncates to microseconds and rounds. UTC_TIMESTAMP
rounds original nanoseconds at every arity. UTC_TIME(NULL) suppresses clock demand.
Raw u32 nanoseconds, i64 arithmetic, wide dates and raw offsets stay unchanged;
there is no new panic catch, normalization or NULL substitute.

The same existing SDK frame admits ClockNative{operation,arity} for valid0/1
arities (dates0 only). It authenticates capabilities before context demand,
resolves the flag with no store borrow, then borrows only to validate precision.
No Columns callback spans that borrow, and no source payload is cloned. A second
get revalidates the capability after the flag callback. Original unsupported
precision tags fail without copying/scanning giant data; live numeric precision
charges actual conversion work and ignores unused Decimal visible shape.
Zero-argument hosts complete without a dummy child, sharing existing failed-start
cleanup with Epoch/Tso. Source output retains original full result metadata,
generic temporal conversion and the ordinary return-family boundary once.
SYSDATE preserves its distinct unconditional DateTime parse even for string/other
declarations. A genuine NULL still traverses its original result conversion,
including any Duration timezone getter, after suppressing clock demand.

Retired prepare_now_args/no_clock_err/native clock packing helper have no callers
and are removed. Live precision continues using the original MAX_FSP constant.
The value-only temporal finishing function is generically named; its existing
FROM_UNIXTIME alias and body stay unchanged. Native value roles/provenance,
error causes, cancellation and staging stay in the existing frame.

## Before/after evidence

Workspace evidence is under
/home/agent/.slock/agents/1555103c-9f3e-46ca-ac2e-b964c650c2f1/evidence/no-fallback/.

91277 n7l-original-red.log captures **132408 deterministic records /
2376 live successes /0CD20F17C50A8D2B**, before production edits. All12 Ready routes
and all12 canonical source refusals fail executable assertions. The initial zero
count/hash assertion is only a capture witness, not semantic bug evidence.
Runs include no cache and two independently created reused caches (not three
distinct policies),12 clock seeds, both alias flags, three error levels, all
original arithmetic fixtures, precision boundaries/NULL/arity/errors and getter
sequences. Clock seeds cover missing values, negative epoch, years0/10000, i64
extremes, u32::MAX fractions, i32 extreme/day offsets and carry boundaries.

A temporary test-only observer in prepare_clock_args records the actual tuple
AFTER old SYSDATE's real SystemTime sample, before worker execution. Additive
candidates pass **all134784 comparisons, including2376 replays of those same
actual live samples** in10292 n7l-independent-pairs.log, while old caller bodies
and worker interfaces remain independent. This compares actual recomputation,
not output replay or two independently sampled instants. Live successes are
paired separately and never represented as deterministic timestamps in the hash.
Only after this pass are callers/observer/pair test retired. Final fixed tests
retain the exact deterministic digest and live-success count, with no regeneration.
Archived n7l-before-{time-mod,time-add-sub}.rs, n7l-original-clock-transport.rs and
n7l-independent-paired-test.rs and n7l-candidate-replay-service.rs preserve
original bodies/observer/replay proof; they are
historical evidence, not restart scripts for current sources.

There was no production semantic mismatch in these paired comparisons. Integration/new-test
compile mistakes (missing enum argument_type arm; Datum has no Display) were
corrected before executable source checks, not called semantic defects. Removing
SYSDATE's old MAX_FSP import temporarily exposed its now-unused original constant;
reusing the same constant in actual live preparation restores the baseline warning
set. The unused native encoder is removed. Live test argument declarations now
match actual Decimal/Real/String kinds; focused23667 and final full suite verify
the corrected fixtures. No old observation hash or historical fixture was changed.

## Validation

All jobs closed. Commands are from the TiDB root unless stated otherwise.
The independent pairing filter is a historical pre-retirement command; its test
was archived afterward. Running that filter against the final tree selects zero
tests and is not a replacement for the captured comparison evidence. Current
full suites/fixed observations/source tests remain executable:


    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib clock_ -- --nocapture
    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib clock_independent_old_new_pairs -- --nocapture
    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib native_live_sysdate_sources_sample_actual_instants_and_keep_result_precision -- --nocapture
    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib
    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-executor --lib projection::tests
    make lint

From the SDK root:

    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tikv test --release --locked --offline -j8 -p tidb_query_expr --lib
    RUSTFLAGS='' TIKV_FRAME_POINTER=0 make run COMMAND='bash /home/agent/.slock/agents/1555103c-9f3e-46ca-ac2e-b964c650c2f1/evidence/no-fallback/n3-clippy-scoped.sh --lib --tests --release --locked --offline -j8'

- Final frontend73602 n7l-front-full-final.log: **1692 pass /exact3 inherited /
  95 ignored /0 filtered, all248 batch pass**. Warning-title multiset EXACT N7k.
  Earlier41305 also1692/exact3; final73602 covers constant/encoder cleanup and
  matching live argument static types. Frozen records, noReady/source tests,
  provider/matrices and actual live source timing all pass.
- SDK37631 n7l-sdk-full.log: **1114 pass /exact1 inherited /1 ignored /0 filtered**,
  including2 new actual-clock API/catalog checks and original clock workers.
- Projection43046 n7l-projection-final.log: **26 pass**. New25-root group combines
  one previous arithmetic output with all12 clock names and12 CHAR parents,
  dynamic precision column, Date/DateTime3/Duration3, both flags/workers0/2.
  Only that group supplies its fixed statement clock and sysdate-is-now. All
  previous group contexts stay unchanged. Live SYSDATE has separate source tests;
  live parallel Projection behavior is not asserted by this fixed-clock group.
- Focus82914 n7l-front-clock-closed.log:22 pass. Source matrix covers12 dynamic
  clock seeds×3 truncate levels×12 names×9 result declarations, original typed
  Int/UInt/String/Real/Decimal columns, CHAR parents, empty/full/reordered rows and
  four flag transitions. Safe dynamic statement time covers full2050 duplicate
  occurrences/name. Skipped IF, eager ABS error and deterministic live errors pass.
- Live source tests cover8 precisions with matching original argument declarations,
  before/after actual instant bounds, statement seconds0, both flags, root/CHAR
  parent and5 duplicate occurrences; exactly5 flag/clock reads and no fallback.
- Providers cover both output roles×all8 policies/valid0/1 arities; corrupt prefix,
  wireNULL, stale/wrong role/node/occurrence/input row; zeroarg missing clock then
  reuse;1MiB unsupported no-copy; exact precision labels; genuine NULL; actual
  finite numeric work and unused million-digit display shape; repeated cancel/
  reuse; exclusive flag/clock/mode/zone/truncate/warning access to the value store.
- Clippy38891 n7l-clippy.log: **89 expression /171 all-title multiset EXACT N7k**.
  Lint56689 n7l-lint.log passes. Both pinned formatters check all15 Rust files,
  edition2021/skip_children=true; paths and unchanged fixture size/SHA/JSON in
  n7l-format-fixtures.log. Both git diff checks pass.

Exact inherited frontend failures are
pushdown_catalog::tests::ifnull_string_column_literal_uses_go_signature_and_column_collation,
tests::builtin_info_json_math_source::exp, and
tests::builtin_math_misc_op_source::vectorized_builtin_op_func.
SDK failure remains
local::str_to_date_worker_tests::str_to_date_workers_pass_whole_states_keep_wide_days_and_read_typed_mode_late.
No failure was filtered/dropped/reclassified to declare this checkpoint green.
No datatype/unistore production change or rerun: prior datatype two failures are
prior evidence only. No Go/Bazel edit, hence no bazel_prepare gate triggered.

Finite allowances are conservative logical work/retention bounds, not allocator
failure or whole-statement heap certification. This is not full Go-package
transcreation, broad PB-source coverage, full server/SQL validation or performance
equivalence. Calendar/component Ready callers, generic admission fallback (including
unsupported/malformed shapes), old unistore walker/unsupported sources, inherited
failures and Go %j/UNIX declared-precision audits remain open. Global goal stays
active. The unrelated untracked client_go_differential/BUILD.bazel is untouched.
