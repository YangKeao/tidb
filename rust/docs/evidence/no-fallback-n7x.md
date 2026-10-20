# N7x: execute deterministic crypto over actual values

This local checkpoint replaces ordinary SM3/PASSWORD/ENCODE/DECODE Ready
handoffs with actual finite SDK values and eager canonical sources. TiDBEngineer
integrated the production change and archived independent comparisons;
ExpressionReuseEngineer accepted task4, completed the source/provider/Projection
coverage and finalized validation and local DCO commits. No push is authorized.

SDK local DCO checkpoint:28a69df. No push.

## Scope and original owners

SM3, PASSWORD, ENCODE and DECODE ordinary calls no longer transport values through
Ready. `native_crypto_value.rs` exposes finite actual byte services over the same
four original pure functions in SDK `impl_encryption.rs`; those functions change
visibility only. The SDK's original algorithms and explicit wire ABI remain.
Native PASSWORD's double-raw-SHA1 policy remains distinct from wire PASSWORD.
The frontend's original `hash_input` and `sql_string_bytes` helpers each remain
the single owner of their actual conversion/error policy, including arbitrary
bytes, Raw formatting and distinct sentinel/error subjects. Borrowed String/Bytes
avoid copies; demanded formatting, retained conversion capacity and crypto work
are bounded. Character result metadata remains even for non-UTF8 ciphertext.

CryptoNative uses the existing SDK frame with exactly one/two eager argument
capabilities, original stored constant probe, result declaration and producer
role/node. Preparation budgets probe ownership before cloning; start performs
the original constant probe before child0. Every child expression is eager and
every required capability authenticates before body work. PASSWORD emits its
original warning1681 before conversion/NULL/resource handling. The demand stage
is consumed before that callback, no store borrow survives it and capabilities
are authenticated again afterward. Return conversion runs once. ENCODE/DECODE
skip password conversion if data is NULL, while still demanding and authenticating
that child's actual expression/value. Existing exact typed SDK recipes keep their
same-frame route. No expression traversal, marker operand or alternate evaluator
is introduced in the actual APIs.

## Independent original proof

TiDBEngineer captured direct34866/51014D84299C1F50 and scalar383526/96AFB40E405F1DF7
from untouched original implementations, froze and reran them before candidate
source edits. The corpus contains149 raw values, twelve password shapes for both
two-argument functions, three context profiles/truncation levels and11 scalar
return declarations. Four original Ready and four source guards failed as intended.
With original functions intact, all418392 actual original/candidate comparisons
passed, including events/errors/panics. Six executed archives were saved BEFORE
retirement: n7x-independent-{tests,front,original-front,scalar,sdk-candidate,sdk-original}.rs.
The archived SDK original matches signed N7w apart from the four visibility changes.
Temporary comparison methods were removed only afterward. Fixed current witnesses
remain unchanged and use the explicit development-only UTC test clock; they are
regression witnesses rather than new independent proof. No expected checksum or
historical fixture was rewritten by the successor.

## Expanded source, callback and recovery validation

The source matrix covers17 input declarations and11 returns, both argument
positions, literals/columns/computed inputs, three context profiles/truncation
levels, four flag runs, reversed duplicate selections, physical output prefixes,
raw invalid bytes and CHAR parents. Constant versus computed malformed Decimal
coefficients preserve original probe/body panic phases. NULL data cannot suppress
an error from the eager second child expression.

Provider checks cover both output roles and128 rejected capability/index cases:
wire NULL, magic, short/long token, namespace, generation, role, node, physical row,
occurrence and wrong index. A first argument invalidated between replies cannot be
rehabilitated by a valid second argument. Consumed calls reject replay; double
cancel is safe. PASSWORD's real warning callback can borrow the store exclusively,
panic or advance its generation; reauthentication rejects invalidated values and
a fresh call recovers. Conversion errors and malformed coefficients retain their
original events/outcomes. NULL data with a real1MiB password, large vector, huge
precision or malformed coefficient succeeds without cloning/formatting the ignored
payload in the body. Demanded huge valid Decimal precision refuses finite work,
consumes the body and allows the same provider to execute NULL afterward.

All four sources reject resource errors without fallback and then execute NULL
on the SAME prepared source. PASSWORD NULL still produces warning1681. A nine-root
mixed Projection cohort is appended LAST, retaining the original first-cohort
clock configuration; it exercises direct results and CHAR parents, both execution
flags and workers0/2, using empty crypt input for genuinely successful text output.

The first expanded focus run had6passes and one test-harness failure: its direct
dispatch oracle passed the lowercase SDK name to the uppercase frontend dispatcher.
That test-only lookup was corrected; no production behavior was changed. The log
is preserved as n7x-expanded.log. Subsequent full validation includes the corrected
provider tests, extra between-reply generation rejection and provider resource
consumption/recovery.

## Validation and limitations

All builds are terminal. Successor full frontend session20126 exits101 solely
for the exact three inherited failures:1780passed/3failed/95ignored, all289batch
checks pass. Projection session28666 exits0:26passed, including the nine-root
crypto cohort under both flags and workers0/2. The predecessor's terminal SDK
run passes1133 with precisely its prior1failure and1ignored; SDK17
and scoped clippy89 expression/171overall warning-title multisets exactly match
N7w. Only tests/docs changed in the successor's work, so those unchanged SDK gates
are reused. The successor's make lint session15949 exits0. Final frontend
warning titles remain Counter-identical to N7w346. Both pinned formatters check
all15 touched Rust files; immutable STR_TO_DATE fixtures, final git diff --check
and read-only n7x-final-check.py pass. That checker also verifies the original
algorithm archive against signed N7w after normalizing only the four visibility
changes, and verifies unchanged datatype/test-clock sources. All local commits
are preserved; explicit-file local DCO checkpoints include this receipt.

Inherited frontend failures are:

- pushdown_catalog::tests::ifnull_string_column_literal_uses_go_signature_and_column_collation
- tests::builtin_info_json_math_source::exp
- tests::builtin_math_misc_op_source::vectorized_builtin_op_func

The inherited SDK failure is
local::str_to_date_worker_tests::str_to_date_workers_pass_whole_states_keep_wide_days_and_read_typed_mode_late.
These failures are not filtered or reclassified. Production datatype and test-clock
sources are unchanged from N7w; reuse N7v frontend-wrapper515/exact2 (including
clock scope/nesting/unwind/thread/zone and production default-OFF checks) and the
distinct SDK-datatype565/exact2 baseline. No full-server SQL or new performance
measurement is claimed. Logical finite-work/copy allowances do not guarantee
physical allocator failure or whole-query peak memory.

No Go/Bazel source changed. The pre-existing untracked
rust/third_party/tikv-client-rs/tests/client_go_differential/BUILD.bazel is excluded.
Other Ready families, generic Admission fallback, old unistore traversal,
unsupported/PB/malformed/deferred sources, typed NAME_CONST parents, inherited
failures, independent Go calendar/UNIX precision and full-server validation remain
open. This four-function checkpoint does not claim complete Go-package transcreation.

## Commands and evidence

From the frontend root:

    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib crypto_actual_
    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib
    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-executor --lib projection::tests
    make lint

The unchanged SDK terminal gates were run from the SDK root:

    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tikv test --release --locked --offline -j8 -p tidb_query_expr --lib
    RUSTFLAGS='' TIKV_FRAME_POINTER=0 make run COMMAND='bash /home/agent/.slock/agents/1555103c-9f3e-46ca-ac2e-b964c650c2f1/evidence/no-fallback/n3-clippy-scoped.sh --lib --tests --release --locked --offline -j8'

Original/frozen/independent/integrated-focus/SDK/clippy evidence remains in
/home/agent/.slock/agents/1555103c-9f3e-46ca-ac2e-b964c650c2f1/evidence/no-fallback/.
Successor evidence and the read-only n7x-final-check.py live in
/home/agent/.slock/agents/02fef0a9-76ce-4afe-9bce-73eebe82ff7d/evidence/no-fallback/:
n7x-expanded.log, n7x-front-final.log, n7x-projection-final.log, n7x-lint.log and
n7x-final-check.log. Run the checker from that workspace with
`python3 evidence/no-fallback/n7x-final-check.py`. Historical applied original/pair
scripts are not resume commands and must not be rerun over the migrated tree.
