# N7u: actual identity values and native sources

This local checkpoint removes the ordinary ANY_VALUE and NAME_CONST Ready
handoffs. No push, global completion or complete Go-package port is claimed.
The SDK DCO commit is da0bf55a60f0da96d70f52f425823cdc0178d94a; the frontend
commit contains this receipt and the living ExecPlan.

## Behavior and ownership

`tikv/identity_actual.rs` uses the original actual Datum Clone. It preserves all19
representations: full f64 bits even in Float32, signed/nonfinite IEEE values,
Decimal coefficient/visible and storage scale/declared shape, time core/kind/FSP,
raw duration metadata, opaque JSON, mutated vectors and all original collation
metadata. Bytes/Raw/bit/Decimal coefficient/JSON/vector copies are checked against
a finite allowance first. String/Enum/Set clone genuine shared immutable GoString
headers. Identity does not render, validate, normalize or encode the selected
value. The obsolete private prepare/decode forwarders are removed and the old
frontend codec is test-only; public SDK identity/wire worker ABI is unchanged.

Both scalar child expressions for NAME_CONST remain eager in original order.
Its body ignores the evaluated name without coercion/encoding; NULL name does
not suppress the value, and first-child errors stop before the value. ANY_VALUE
remains unfoldable, even with a literal input. Original return-family conversion
runs once with the whole result declaration, without generic temporal parsing.

An original hidden signature prefix is observable: generic ScalarFunction probes
its stored first Constant.value with uservar_name BEFORE testing its function
name and before evaluating children. This unrelated behavior is preserved for
these two valid-arity signatures. The bounded real probe remains separate from
the actual identity body. Invalid coefficient UTF-8 still reaches the original
formatter panic before unused huge scale. Valid huge precision is bounded before
rendering. The first candidate omitted this phase and genuinely failed old/new
comparison; original hashes were never changed to accept that omission.

`ValueIdentityNative { name_const }` is closed to arity1/2 with native capability
inputs/output on the existing SDK frame. Source metadata charges each actual
stored Constant header before cloning it, snapshots child ordinals/result type/
output provenance, and host start probes that immutable header before argument0.
Preparation itself neither evaluates nor formats. Parameter/deferred source
admission is unchanged and remains global work. Every demanded token authenticates
namespace/generation/role/source/physical row/occurrence. Advance before any copy,
conversion/panic; NAME_CONST discards its authenticated first token, then requests
the selected value. Release store borrows before return conversion/publication;
outputs get the producer's own role/origin. Caught failures cannot replay tasks,
and subsequent calls recover. No expression/callback evaluator is stored here.

The previously supported exact typed ANY_VALUE source recipes remain their
original SDK RPN kernels inside the same frame, including typed ROUND parents
and the explicit typed provider API. Native-value source interception applies
only to actual-value views; it does not require capability tokens from a typed
provider. These recipes do not call Ready or another interpreter. Additional typed
NAME_CONST parent closure, malformed/PB/deferred producers and other unsupported
sources remain part of global coverage, not a completion claim here.

## Original and regression evidence

Before candidate edits, `n7u-original-capture-fixed.log` recorded3213 direct
observations, checksum60F023D7530726AF, and231102 scalar observations,
checksumE5E2EFBE8F3E3F49. `n7u-original-frozen.log` independently reran both fixed
counts/hashes. The Ready rejection guard failed for both functions and the source
guard reported prepared0/fallback1/Admission for both. Compile-only test fixture
or namespace errors are not behavioral REDs.

Before retiring either old caller, `n7u-independent-pair-probe.log` passed234315
actual old/candidate comparisons. Its remaining exit101 is only the expected
Ready/source RED guards. Archived executed tests/candidate/scalar sources are
`n7u-independent-{tests,candidate,scalar}.rs` in the persistent agent workspace's
`evidence/no-fallback/`. Original five sources are `n7u-before-*.rs`; the frozen
witness is `n7u-original-identity-tests.rs`. Do not regenerate historical hashes
or call a fixed-witness rerun renewed independent proof.

The first expanded source run exposed another genuine boundary: a computed
malformed Decimal, converted to Int, originally panics on coefficient UTF-8 before
scale, while the general work estimator rejected huge scale first. The dedicated
identity return adapter now checks a bounded coefficient prefix and calls the
real original coerce_function_result on that malformed input. It does not force
a panic, fabricate an operand, or change unrelated function returns. The source
comparison then passes; valid huge precision still produces a terminal Resource
error before unbounded work. A test-only NaN panic assumption was corrected to
use a known genuine malformed-coefficient conversion panic; this was not a
production semantic change.

Eight final focused tests cover17 source declarations,11 return declarations,
constant/column/computed selected positions, both functions, three context profiles,
three truncation levels, four flag runs, reversed selections/repeated rows,
NULL/name error demand, original physical prefixes and constant-versus-computed
malformed Decimal behavior. NewDate uses computed/literal values because the old
physical chunk does not represent that declaration. Raw opaque values have exact
identity-frame checks. Resource copies and probe validation happen before work;
a tiny coefficient with huge scale remains a valid opaque identity body.

Provider tests cover both output roles and names, each demanded argument's invalid
capability/magic, generation, role, ordinal, occurrence and physical-row boundaries,
NULL wire tokens, double cancel and rejected replay after error/panic/publication.
A real1MiB ignored Enum name is not formatted/copied by the body. Resource failure
on computed huge-precision Decimal-to-string never falls back; the SAME prepared
source then processes NULL successfully. A new17-root mixed Projection cohort is
appended last, retaining the old clock cohort's setup; both execution flags and
workers0/2 are compared to original serial execution.

## Validation and remaining work

All checks reached terminal status. Final focus51765 passed8; full frontend9075
passed1756 with exactly the prior3 failures and95ignored; all276 batch tests
passed. Full SDK63736 passed1130 with exactly the prior1 failure and1ignored.
Final Projection69615 passed26 after the final source-selector change. The first
full frontend run46442 exposed three new typed-view admission regressions; all
three original unchanged tests pass after limiting native interception to actual
value views. No inherited failure was filtered, removed or reclassified.

Frontend346 warning titles and SDK warning titles exactly match N7t; clippy7886
completed with89 expression warnings and171 overall titles, Counter-identical to
N7t. Lint65482 passed. Both pinned formatters (2026-01-30 and2026-08-22) checked
all13 touched Rust files; immutable STR_TO_DATE fixtures and both diff gates pass.
The read-only n7u-final-check.py passed all counts, fixed hashes, independent
pair evidence and exact failure/warning lists. Only a module documentation comment
was clarified after runtime checks; no executable behavior changed. The datatype
production surface is unchanged since the signed N7t0 full565/exact2 inherited
baseline, which is explicitly reused here.

Final logs are n7u-focus-final.log, n7u-front-final.log, n7u-sdk-full.log,
n7u-projection-final.log, n7u-clippy.log, n7u-lint.log and n7u-final-check.log.
Inherited frontend failures remain
pushdown_catalog::tests::ifnull_string_column_literal_uses_go_signature_and_column_collation,
tests::builtin_info_json_math_source::exp, and
tests::builtin_math_misc_op_source::vectorized_builtin_op_func. The SDK failure
remains local::str_to_date_worker_tests::str_to_date_workers_pass_whole_states_keep_wide_days_and_read_typed_mode_late.

Logical copy/work budgets do not certify physical allocator failures or whole
statement peak memory. Remaining Ready families, generic fallback, old unistore
traversal, unsupported/PB/malformed/deferred sources, inherited front3/core1/
datatype2 failures, Go calendar/UNIX precision audits and full-server SQL gates
remain open. No Go/Bazel file was changed; make bazel_prepare was not triggered.

Reproduce from the frontend root (logs are in the persistent agent workspace):

    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib misc_identity_ -- --nocapture
    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib
    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-executor --lib projection::tests
    make lint

From the SDK root:

    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tikv test --release --locked --offline -j8 -p tidb_query_expr --lib
    RUSTFLAGS='' TIKV_FRAME_POINTER=0 make run COMMAND='bash /home/agent/.slock/agents/1555103c-9f3e-46ca-ac2e-b964c650c2f1/evidence/no-fallback/n3-clippy-scoped.sh --lib --tests --release --locked --offline -j8'

From the agent workspace, `python evidence/no-fallback/n7u-final-check.py` compares
exact inherited failure/warning lists and all counts, original hashes, independent
pair evidence, both pinned formatters, unchanged immutable STR_TO_DATE fixtures
and both repositories' whitespace gates. The final logs and signed local SDK id are recorded above.
