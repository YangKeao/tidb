# N7r: vector SQL uses actual values and canonical sources

Local expression-unification-demo checkpoint; no push, PR, deployment or complete
Go-package port. Global goal/task4 remains active. Prior signed checkpoint:
TiDBdda40631/TiKVb5c60ae. SDK companion DCO commit: `6c084919dcbf2eaf7ed82db4ff79e12e6df9710e`.

## Ownership and preserved behavior

All eight builtin_ext/vec.rs Ready callers are retired: VEC_DIMS,
VEC_L1_DISTANCE, VEC_L2_DISTANCE, VEC_NEGATIVE_INNER_PRODUCT,
VEC_COSINE_DISTANCE, VEC_L2_NORM, VEC_FROM_TEXT and VEC_AS_TEXT. Their canonical
sources now use dedicated VectorNative catalog/metadata/demand through the
existing single SDK HostFrame and native capability ABI. Generic compatibility
routing for other families is unchanged.

SDK native_vector_value.rs defines original operation spelling/arity, actual
nullable dimension/text/metric output and strict text parsing. These actual APIs
and unchanged explicit wire workers in impl_vec.rs delegate the same datatype
native raw_ref algorithms, formatter and parser. No vector arithmetic, SIMD,
rounding, NaN policy or grammar is reimplemented. Native values are never encoded
for another expression program. Existing wire ABI/planners remain unchanged;
no new codec, dependency or fabricated validity/NULL operand was introduced.

Original conversion uses convert_to against an unspecified-dimension VectorFloat32
target. Existing actual vectors now borrow storage, retaining empty/uncapped
sizes, mutated NaN/Inf payloads and signed zero. String/Bytes parsing and all
other kind errors retain the original converter. Left conversion precedes right:
NULL left suppresses RHS conversion, but bad left still errors before NULL right.
Source child expressions remain eager, with every capability authenticated before
body conversion, even for a later ignored operand. The fixed one/two-argument
demand closes before fallible work; consumed/expired calls cannot replay.

Only VEC_FROM_TEXT has the original source ETString cast. Identity byte/name/
binary-literal views remain borrowed; other rendering is bounded before owned
materialization, followed by strict parsing. Vector input metadata does not
impose new dimensions or casts. These stages are pure: no Columns callback runs
under a store borrow. Source borrows end before original ordinary return-family
conversion and output insertion. That return boundary runs once; no generic
temporal-result parse is added.

NaN metric results become NULL exactly as before; other f64 bits, including
infinity and negative zero, remain intact. Dims reads length without scanning or
cloning. Original mismatched-dimension errors precede any metric scan. Parsing,
matching metrics, norm and rendering check finite work before scans/allocations,
and retained owned conversions reduce later allowance. Logical work/ownership
bounds do not certify allocator failures or whole-statement peak memory.

## Independent original proof

Original51952, before candidate edits, captured207504 result/error/panic/context
observations and actual Real bits: **D0CDB9F6BB8FB5EB**. It reproduced all eight
Ready entries and all eight source refusals/fallbacks as two failing guard tests.
The count/hash was frozen before actual APIs were added. Each case uses a fresh
old context/cache under three profiles; vector bodies have no session callbacks.
A second original read records exact Real bits; it adds no poisoned-cache history
claim. No process TZ mutation is needed for these vector functions.

Independent75258 compared every candidate observation and actual Real bit with
the still-present old callers/workers. All207504 pairs and the fixed original
checksum passed before retirement. Corpus includes106 general Datums plus vector
texts and raw empty/negative-zero/subnormal/NaN/Inf/extreme vectors, all unary
inputs and all binary pairs. Existing dimension/text/error policy remains intact.

Persistent workspace evidence/no-fallback retains n7r-before-{front,sdk}.rs,
n7r-original-witness.rs, n7r-final-independent-pairs.rs, n7r-original-red.log and
n7r-independent-pair.log. The permanent fixed witness calls the new owners and
is not renewed independent proof. Never regenerate its hash or rerun applied
/tmp/n7r-host.py or /tmp/n7r-tests.py. Subsequent private arity guards and removal
of a tiny temporary reference Vec do not change SQL policy. No finite-resource
RED is claimed here: Ready/source REDs preceded implementation, and finite-work
checks are new API contract tests.

## Final validation

| Check | Result |
| --- | --- |
| Frontend25513, n7r-front-full.log |1728 pass, exact3 inherited failures,95 ignored,0 filtered; all263 batch pass |
| SDK6768, n7r-sdk-full.log |1125 pass, exact1 inherited failure,1 ignored,0 filtered; new actual API test passes |
| Projection12467, n7r-projection.log |26 pass; new17-root group under both flags/workers0 and2 |
| Focus98709, n7r-focus-final.log |29 pass; original hash/Ready/source/matrix/provider/finite checks |
| Provider in focus and full frontend |all8 operations/both output roles, capability and cancellation checks |
| Scoped clippy67213, n7r-clippy.log |89 expression/171 all warning-title multiset exactly N7q |
| Frontend warning-title multiset |346 exactly N7q |
| Lint72965, n7r-lint.log |pass |
| Dual pinned formatting |all13 changed Rust files pass |
| Immutable fixtures and both diff checks |original lengths/SHA256 and git diff --check pass |

All actual builds are terminal; no production behavior changed after these final
runs. Early focus86811 already passed28 checks; after adding direct API/finite
checks and the private adapter cleanup, final focus98709 passed29.

The source matrix covers all eight signatures/all argument positions,14 declared
input families including real raw vectors and Timestamp/NewDate aliases,9 result
families,4 context seeds/3 levels/4 flag transitions, empty/full/reordered/duplicate
selections, CHAR parents and eager ABS errors under NULL siblings. Both panic
observations and full physical columns/row count/context are compared across
reused suites; raw NaN/Inf cases are retained. NewDate has no old fixed-time input
column, so unchanged alias cases use actual computed/literal Time values over
supported integer input selections. No alias or nonfinite case was dropped.
Representation parity under contradictory result declarations does not certify
malformed source validity or physical NewDate storage.

Provider checks cover every argument's corrupt/stale/wire NULL/wrong role/node/
row/occurrence capabilities and both output roles. All capabilities authenticate
before body conversion, and context hooks reject accidental Ready use. NULL left
still authenticates an ignored huge invalid RHS without copying it. Cancellation
is idempotent, expired replay fails and later invocations succeed. Pure vector
stages need no callback; no claim of a newly invented vector callback is made.
Direct tests cover huge borrowed dimension count at0 work, ignored RHS, huge
mismatch at0 work, unsupported Decimal before display rendering, text/metric/norm/
render limits, strict invalid UTF8, subnormal and negative-zero parser bits,
NaN/infinity metrics, and infinite-allowance refusal. Projection exercises all
8 values and CHAR parents with an older arithmetic output; earlier groups and
clock context remain intact.

Exact inherited frontend failures remain:

- pushdown_catalog::tests::ifnull_string_column_literal_uses_go_signature_and_column_collation
- tests::builtin_info_json_math_source::exp
- tests::builtin_math_misc_op_source::vectorized_builtin_op_func

SDK failure remains
local::str_to_date_worker_tests::str_to_date_workers_pass_whole_states_keep_wide_days_and_read_typed_mode_late.
No inherited failure was filtered, renamed, removed or reclassified.

Commands from TiDB root:

    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib vector_ -- --nocapture
    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib
    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-executor --lib projection::tests
    make lint

The independent proof used the first command with filter
vector_original_observations before retirement. It is archived; the current test
is the fixed checksum witness. Finite-work checks are
vector_values_bound_demanded_work_without_copying_ignored_storage and
native_vector_value::tests::actual_vector_values_keep_native_bits_null_dimension_errors_and_work.
Commands from TiKV root:

    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tikv test --release --locked --offline -j8 -p tidb_query_expr --lib
    RUSTFLAGS='' TIKV_FRAME_POINTER=0 make run COMMAND='bash /home/agent/.slock/agents/1555103c-9f3e-46ca-ac2e-b964c650c2f1/evidence/no-fallback/n3-clippy-scoped.sh --lib --tests --release --locked --offline -j8'

Both rustfmt binaries under
/home/agent/tidb/expression-reuse/rustup-home/toolchains/nightly-{2026-01-30,2026-08-22}-x86_64-unknown-linux-gnu/bin/rustfmt
ran with --check --edition 2021 --config skip_children=true on all13 files listed
in workspace evidence/no-fallback/n7r-final-check.py. That read-only script also
checks exact failure/title Counters, counts, original hashes, immutable
STR_TO_DATE fixture lengths/SHA256 and both diffs; output n7r-final-check.log.
The checker reads the printed original hash from capture, independent pair and
final focus logs. Successful witness stderr is captured by the full Rust harness,
so the full-suite check uses its passing fixed-hash test name; it does not demand
an absent printed line or regenerate a hash.

SDK maintenance guide and all four living ExecPlan sections are updated. No Go/
Bazel, datatype or unistore production file changed; no bazel_prepare gate was
triggered. Datatype/unistore suites were not rerun; prior datatype failures remain
open. The unrelated untracked client differential BUILD.bazel is untouched.
Remaining Ready families, generic admission fallback, old unistore traversal/
eight child-free DurationDatetime IDs, unsupported/malformed sources, inherited
failures, Go semantic audits and full-server SQL gates remain open. This signed
local checkpoint does not complete the global objective.
