# N7e: reject changed ready-value dispatch before kernel effects

The five existing SDK recipe-identity failures are fixed in local signed SDK
commit `940f123` (not pushed). An unchanged generated
factory result still reaches the selected kernel, while changed run pointers,
validators, metadata constructors and renamed different factories fail the
closed recipe check before invocation. This checkpoint does not finish the
no-fallback migration. It changes no calendar semantics or historical fixtures.

`RpnFnMeta::new` is the doc-hidden const constructor used by all three generated
factory forms. It keeps a private copy of the construction-time name and three
pointers. `evaluated_bytes_shape` checks the current fields against that same
instance's copy, then retains every previous canonical name, arity, input/output
type and typed-metadata check. The selected checked FunctionRef remains the
construction authority. No second dispatch map, evaluation probe, retry or
fallback is added. This is an integrity contract for generated metadata, not
an authentication mechanism against arbitrary Rust calling the constructor.

A function can have different addresses in different codegen units. Therefore
this fix deliberately does not compare the compiled kernel with a fresh official
factory pointer. Each saved pointer is copied from the exact same argument as
its public counterpart. Metadata sizes and node capacities are still charged
using their actual Rust sizes; no worker or execution limit was increased.
Ordinary unannotated raw/wire admission and the shared kernel helper are unchanged.
The macro ABI changes from struct literals to a const constructor; downstream
hand-written literals must migrate, while generated consumers remain supported.

## Behavioral evidence

Before the fix, `n7e-identity-red.log` CLOSED101 reports20pass/5fail/1ignored.
The ASCII negative case actually panics with `wrong kernel entered`. The other
four existing failures accept a changed kernel or call chain. After the fix,
`n7e-identity-green.log` CLOSED0 reports25pass/0fail/1ignored. All five existing
names are retained. The composite test additionally rejects a whole different
factory renamed to match, and independent validator/metadata-constructor pointer
substitution. The original raw ASCII positive case still returns255 and all
refusals keep invocation witnesses at zero.

`n7e-codegen.log` CLOSED0 reports20pass. Exact macro token expectations include
generic const factories; the actual SDK and frontend compilation exercise all
three factory generation forms without imposing new static lifetime bounds.

## Validation

From the SDK root, with the repository wrapper and pinned compiler:

    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tikv test --release --locked --offline -j8 -p tidb_query_expr --lib test_evaluated_
    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tikv test --release --locked --offline -j8 -p tidb_query_codegen --lib
    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tikv test --release --locked --offline -j8 -p tidb_query_expr --lib

Full SDK `n7e-sdk-full.log` CLOSED101:1096pass/1fail/1ignored/0filtered.
Machine comparison against N7d proves only the five identity failures disappeared.
The exact remaining failure is
`local::str_to_date_worker_tests::str_to_date_workers_pass_whole_states_keep_wide_days_and_read_typed_mode_late`.
It still fails the same diagnostic assertion; it was neither filtered nor
allowlisted. The original datatype baseline564pass/same2fail is unchanged and
was not rerun because no datatype source changed.

From the TiDB root:

    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib
    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-executor --lib projection::tests
    make lint

Full frontend `n7e-front-full.log` CLOSED101:1637pass/same3fail/95ignored/0filtered;
all218 `evaluator::batch` tests pass. The immutable STR_TO_DATE recorded history
and explicit Go calendar deltas test passes. Exact remaining frontend failures:

- `pushdown_catalog::tests::ifnull_string_column_literal_uses_go_signature_and_column_collation`
- `tests::builtin_info_json_math_source::exp`
- `tests::builtin_math_misc_op_source::vectorized_builtin_op_func`

Lint `n7e-lint.log` CLOSED0. SDK-scoped clippy through the Makefile environment:

    RUSTFLAGS='' TIKV_FRAME_POINTER=0 make run COMMAND='bash /home/agent/.slock/agents/1555103c-9f3e-46ca-ac2e-b964c650c2f1/evidence/no-fallback/n3-clippy-scoped.sh --lib --tests --release --locked --offline -j8'

`n7e-clippy.log` CLOSED0. The expression test target reports89 warnings. The
entire emitted diagnostic-title multiset, including dependencies, has171 titles
and is exactly equal to N7d (no added/removed titles), excluding generated-count
summaries and the future-incompatibility summary.

Projection `n7e-projection.log` CLOSED0:26pass. The isolated ignored body-origin
check `n7e-wrapper-isolated.log` CLOSED0:1pass proves the unchanged official ASCII
wrapper body is entered once; it is deliberately run alone because its counter
is global:

    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tikv test --release --locked --offline -j8 -p tidb_query_expr --lib local::batch::ready_value_tests::test_evaluated_ascii_wrapper_body_origin_isolated -- --ignored --exact

Pinned formatting of the three changed Rust files under both repository
compilers, both `git diff --check` checks and SHA256/JSON/gzip validation pass
(`n7e-final-static.log`). Original compressed history remains15188bytes with
SHA256 `fd9ed6080649e8987dd86b64bd761998c7db2bf66cd8aff52fdbd4a9a9464571`;
Go calendar fixture remains32388bytes with
SHA256 `b39e3c61207f877917bea98ff68b87f2c01901b04070f644e2a70eb40ef84a8f`.
No fixture or historical digest was updated. All validation processes are closed.

## Remaining work and limits

Read-only audit of original `pkg/types/time.go:2968` confirms `%j` is stored in
context and explicitly ignored by a TODO in `mysqlTimeFix`. Native scanning
stores wide day999; original Go fixture evidence gives2020-12-00 instead of the
native invalid-day result. Invalid-date diagnostics in both original Go and
native retain parsed fields, unlike the SDK worker test's fullzero expectation.
These are separate scanner/oracle issues and remain visible. A future parser
change needs independent expectations and explicit semantic deltas against the
immutable N7b observations, not a regenerated historical digest.

The remaining ready-value date callers, source closure, frontend/datatype gaps,
whole-server/SQL validation and global alternate dispatch removal are unfinished.
No performance benchmark or whole-server build was run for this checkpoint.
There is no push, PR or deployment authorization.
