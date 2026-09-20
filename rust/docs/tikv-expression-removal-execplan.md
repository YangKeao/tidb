# Replace the native expression evaluator with TiKV's engine


This is a living ExecPlan governed by repository-root `PLANS.md`. It supersedes
`tikv-expression-coverage-execplan.md` as the plan of record: the earlier plan
added a second implementation, this one removes the first. It is an
exploration on YangKeao forks, not an upstream commitment.


## Purpose / Big Picture


Today `tidb-expr` contains two evaluators: its own (`scalar_function::eval`,
`ops`, `time_fn`, `string_fn`, `builtin_ext`, the comparison and control
families) and the TiKV RPN engine reached through the `tikv-expr` feature. The
goal is to make TiKV's engine the **only** evaluator, so that one set of
kernels defines SQL value semantics everywhere.


The purpose of the plan is to sequence that removal so each step is verifiable
and reversible. Definition of done, stated as behavior: the `tikv-expr`
feature disappears (the engine is always on), no code path evaluates an
expression through the old kernels, and the full Rust suite plus the
mysql-tester replay pass with the engine executing every expression. Until
then the native path stays and remains the default.

The feature gate is the temporary coexistence mechanism. It is deleted only
at milestone E, and deleting it is the signal that the replacement is
complete. A claim that the old implementation has been removed is only valid
once no `#[cfg(not(feature = "tikv-expr"))]` arm and no `tikv-expr` feature
remain in the workspace.


## Progress


- [x] Recovery audit: pin TiKV `d847323beba1e93513314018fbb5ee946e4b9c79`
      in `crates/tidb-expr/Cargo.toml` and regenerate `Cargo.lock`. The selected
      borrowed facade was used by the adapter while the manifest still pinned
      an older engine without that API; local Cargo patches had hidden this.
      Unpatched fork validation: 1212 lib tests passed (99 ignored) and 60
      integration tests passed. Commands and limitations are recorded below.
- [x] Fix selected borrowed dense bounds in TiKV: a `None` selection with
      `output_rows > row_count` now returns an error before any sink callback.
      The new regression failed before the fix; the engine package now passes
      481 tests. Added cross-batch independent selection, empty-selection and
      invalid-length/index/nonfinite preflight coverage.
- [ ] Route Window/Join at their original demand points. Borrowed selected
      evaluation still rejects lazy programs and requires equal physical input
      lengths; independent selections alone do not remove these limitations.

- [x] Milestone A (point 6): engine shareable and thread-safe.
      TiKV metadata is `Send + Sync`, `PreparedExpression` is asserted
      `Send + Sync`, and a compiled program is split from caller-owned
      `ExecutionState`. The TiDB adapter caches the compiled programs on the
      shared `EvaluatorProgram`, and a test proves one plan compiles ONCE for
      three suites while a real statement-policy change recompiles and the
      copying/borrowed backend does not.
- [x] Milestone B (point 2): explicit admission table and fallback gate.
      384 rows (228 admitted, 156 excluded with a reason) covering the
      309-name Go-derived registry plus the synthesized spellings; a test fails
      if a name has no row. Falls back are reported as `NotAdmitted`,
      `LazyRisk` or `UnrepresentableInput` and the SQL differential helper
      fails on a silent fallback.
- [x] Milestone C (point 1): short-circuit evaluation.
      Every lazy-sensitive family the engine dispatches is lazy: Tier 1
      (IF/IFNULL/COALESCE/CASE/AND/OR/XOR), Tier 2 (ELT/FIELD/GREATEST/LEAST/
      INTERVAL) and Tier 3 (`AddTime*Null`). `LAZY_SENSITIVE_KERNELS` holds
      only the all-lazy Tier-1 names, so `eager_lazy_risk()` can no longer
      report anything and the adapter's mixed-shape gate is inert. The wire
      format is unchanged; laziness is a signature-driven marker. TiDB's
      acceptance test is `crates/tidb-expr/tests/tikv_lazy.rs`.
- [x] Milestone D (point 4): the type support the removal needs.
      `Set` only: `FieldTypeTp::Set -> EvalType::Set`, the `Set`/`SetRef`/
      `ChunkedVecSet` carriers, chunk and raw-datum codecs, the hybrid
      Int/Bytes borrows, the standalone `Column::Set`, the cast registration
      and the adapter bridge, with round-trip fixtures. A latent
      `Column::get_enum` bug (wrong indexing for every row) was fixed on the
      way. Geometry and arrays were shown not to be needed.
- [x] Every constant case in the 33 Go source-port files dual-runs through the
      engine: `tests/mod.rs::chunk_e` evaluates the same rewritten expression
      both ways, skips only what the adapter declines, and compares errors by
      classification. Its first run found seven real divergences, all now
      fixed at the boundary or recorded.
- [ ] Milestone E: flip the default, delete the native evaluator and the
      `tikv-expr` feature, and re-point the corpora from "dual-run" to
      "engine only". The engine-only measurement exists and is the E work
      list: `TIKV_EXPR_ENGINE_ONLY=1` makes a declined expression a failure,
      and at `47ce598` plus the temporal-bridge fix the lib corpus reports
      1149 passed / 61 failed / 99 ignored, i.e. 59 distinct constant
      expressions still have no engine path (grouped in
      `tikv-expression-corpus-plan.md` section 7). The rate is measured on the
      adapter's unfolded input, though, and production folds constants first
      (`plan_builder.rs` folds right after rewriting): 40 of the 59 collapse to
      a `Constant` and never reach a lowering, 2 are planning-time refusals, so
      the *observed* production surface is 17 (corpus plan 7.13). Column-bearing
      shapes are not in the corpus at all, which is the one direction the
      correction could not bound -- until now: `tests/tikv_column_shapes.rs`
      rewrites 48 shapes through a column resolver and pins 28 engine / 20
      native, so the production shape surface is measured too (corpus plan
      7.14). The 20 are all deliberate: collation/padding (6), missing kernel
      or policy (6), a constant regex the engine compiles at build time (1),
      `max_allowed_packet` (1), the statement clock (1), computed digits (2),
      and three named exclusions. Two of the audited reasons turned out to name
      a *shape* rather than a capability or a policy: `oct` was excluded for its
      binary-literal argument, which the constant rule already refuses (7.16),
      and `if(<datetime>, a, b)` was refused by the lazy-arm leaf rule even
      though a condition is never a skipped arm (7.17). Remaining
      known divergences are listed in the TiKV
      `EXPRESSION_SEMANTIC_GAPS.md` (26 open entries; the CRC32 declaration
      bug and the `LAST_DAY` DATE-shape mismatch are fixed).


## Surprises & Discoveries


The parity target is narrower than "all of MySQL": it is whatever the native
evaluator can do today. Functions the Rust port never implemented are out of
scope for the removal, because deleting code cannot lose behavior that does
not exist. Milestone D therefore starts from an inventory of the native
surface, not from a wish list.

`tidb_query_expr` is declared `publish = false` and the TiDB dependency is a
`git` dependency on the whole TiKV workspace. Removing native makes
`tidb-expr` unusable without that fork checkout, which is acceptable for this
exploration but is the reason packaging (the previous point 7) cannot be
postponed forever.

Only about 30 sites make the compiled engine non-`Sync`: four
`Box<dyn Any + Send>` metadata signatures and 24 codegen references. The
thread-local varg buffers do not block `Sync`; they require only that one
compiled program is not executed concurrently on two threads.

The upstream issue `pingcap/tidb#70156` proposes the switch
`tidb_enable_short_circuit_expression` and says TiKV "can introduce
short-circuit expression nodes". It is an open, unimplemented proposal: the
variable does not exist in either checkout and the dispatcher has no lazy
node. Milestone C therefore implements laziness with the wire format
unchanged, by making the evaluator consult a lazy-signature set instead of
emitting new node kinds. That keeps `tipb` untouched, which matters because
the wire schema lives in a different repository.

The engine-on build stopped working because the host toolchain moved under it,
not because of a code change: CMake 4 (4.3.4) rejects the `cmake_minimum_required`
of the `c-ares` copy bundled in `grpcio-sys 0.10.3`, and GCC 16 no longer lets
that copy of abseil get `uint8_t` from a transitive include. `grpcio-sys`
reaches the TiDB Rust test link through `tikv_util`, so both must be worked
around before any engine-on suite runs:

    CMAKE_POLICY_VERSION_MINIMUM=3.5 \
    CXXFLAGS="-w -std=gnu++14 -include cstdint" CFLAGS="-w" \
    cargo test -p tidb-expr --features tikv-expr ...

`cargo`'s `rerun-if-env-changed` turns those into a one-time rebuild of the two
`grpcio-sys` variants. Do not scope the target selection with `--lib` while
doing this: a narrower target set changes feature unification, which mints a
new unit hash and re-runs the C build for nothing.

The adapter's blanket "no temporal cast over a constant" rule in
`tikv::lowering::coerce` was covering exactly one real divergence. Deleting it
and re-running the corpus brought `date('20111213')` and
`month(20240315123045)` in line, and exposed
`last_day(20240315123045)` -- TiKV's `last_day` is typed `DateTime` internally
and returns a midnight `DateTime` for a `DATE`-declared result, which the exact
bridge rejected as "unsupported TiKV temporal value shape" while native
answered `2024-03-31`. Go's DATE decoder drops the time part, so
`bridge::check_time` now rebuilds the declared `DATE` from the calendar fields
when the engine kind differs, and leaves an already-`DATE` value (including its
wall fields) untouched -- that is the shape the chunk round trip is defined on.
A blanket rule that costs one line of bridge code should be preferred to
refusing a whole family.

The same lesson then applied in reverse. The `Shape` policy that allows only
leaves in a possibly-skipped lazy arm looks removable, because the engine's
lazy boundary evaluates each child on demand (`ChildHandle::eval` calls
`eval_subtree`), so a skipped arm is never entered. Replacing the leaf rule with
`coerce` and rerunning the dual-run corpus produced three disagreements in one
run: `case when 0.1 then 1 else 2 end` and
`if(cast('0.1' as decimal(2,1)), 1, 2)` answer `2` instead of `1` because Go's
truthiness on a non-zero DECIMAL is not Go's integer cast, and
`coalesce(1, 123.456)` comes back with decimal scale 0 instead of 3. Inserting
an implicit cast changes the *value*, not just the plan, so the leaf rule is a
value rule and stays; the experiment is recorded in
`tikv-expression-corpus-plan.md` section 7.2. The narrow half that is safe did
land: a `NULL` leaf is retagged to the arm's family rather than cast, because
`NULL` is family-less and the engine's validator reads the declared `FieldType`.

The same "it is the same operation" trap sits in the explicit-cast spellings.
`CAST(x AS SIGNED)` arrives as `cast_signed` and `CAST(x AS DATETIME)` as
`cast_datetime`, minted by the rewriter; admitting them against the local
arithmetic arm (which already derives `Cast{source}As{target}` from the
function's static type) took the corpus gap from 61 to 59 and immediately broke
two shapes: a `COALESCE` over `DATETIME(0)` and `DATETIME(3)` lost the promoted
scale (`.000`), and an `INTERVAL` argument through a minted cast rounded the
other way. Reverted; the minted spellings stay native, and the experiment is in
`tikv-expression-corpus-plan.md` section 7.4. The lesson repeats: the dual-run
corpus is cheap and it decides these questions faster than reasoning about the
code does.

The removal's error contract is now measured for the whole remaining set: the
ratchet drives each of the 59 declined expressions through a resolver with
`tikv_expression_required() == true` and asserts the outcome is never a value.
57 return the structured refusal error naming the reason; 2 fail at planning
time (`(1, 2) = (1, 2, 3)` needs a column resolver, `convert(... using cp866)`
names an unsupported charset), which is a different and already-correct
contract. That is corpus plan section 7.12.

The "no session" path was only half-built. `tikv_expression_required()`
reported a *missing engine context* as a structured error, but a resolver that
requires the engine and gets a context that **declines** the expression still
fell through to the native evaluator -- a silent fallback with nothing behind
it once native is gone. Evaluation now returns the same structured
`ExternalEngine` error (code 1105) naming the refusal reason, and the reason is
still recorded first, so a gate can see it. The test uses `translate`, which is
a permanent native exception (no signature in the pinned tipb), so it exercises
the exact shape the removal has to answer for.

`NULLIF` is the first name to leave the "row excluded: no local lowering"
cluster. Go rewrites it to `IF(a <=> b, NULL, a)`, so the lowering is that
tree -- but with the two sides built separately, because MySQL returns
*expr1's* type while the comparison promotes: `NULLIF(1, 1.0)` compares as
DECIMAL and returns BIGINT, and an `If` node that declared BIGINT over a
DECIMAL value child is what the engine refused with `Expect Int, received
Decimal`. The same-type shape needs no cast at all, which is the common
`NULLIF(col, 0)`.

That cluster is now triaged rather than renamed. Its single placeholder reason
("no local engine lowering") read as "write a lowering", but 100 rows shared
it and only some are adapter work: 4 names have no signature in the pinned
`tipb` at all, so no lowering can ever reach the engine and they are permanent
native exceptions; 5 have a signature the engine does not dispatch, so they are
TiKV work; 15 are the explicit-cast spellings section 7.4 rules out; 18 need
session state the facade's `Context` does not carry. The admission table now
says which, a test pins the triaged names, and the method is in
`tikv-expression-corpus-plan.md` section 7.6. For the removal that distinction
is the difference between "a lowering to write" and "a function that can never
be pushed".

The "18 need session state" bucket has since been split further, because
"session state" was hiding two different futures: the ten clock names (`now`,
`current_timestamp`, `curdate`, `current_date`, `curtime`, `current_time`,
`utc_date`, `utc_time`, `utc_timestamp`, `sysdate`) have a wire signature the
engine's dispatch table does not implement -- except `SysDateWithoutFsp`, which
reads the host's own clock -- so they need a host clock *and* kernels, while
`localtime`/`localtimestamp` have no `tipb` variant whose name contains
`LocalTime` at all and are permanent exceptions next to `translate`. That is
`SESSION_CLOCK_NEEDS_HOST_CLOCK` versus `NO_WIRE_SIGNATURE` in the table, and
`admission::tests::clock_names_state_the_wire_and_host_clock_facts` pins both
halves rather than asserting them in prose.

`cast_signed` and `cast_unsigned` came out of that triage as the one safe
subset of the explicit-cast spellings: `CAST(x AS SIGNED|UNSIGNED)` is exactly
`Cast{source}AsInt`, so the local arm derives it with no metadata of its own.
Admitting them immediately exposed a hazard in a *different* place: the corpus
test `test_interval_func` had been passing only because the whole expression was
declined while `cast_unsigned` was excluded, and once it ran, the engine's
`IntervalInt` compared an UINT64 above `i64::MAX` as a raw `i64`.
`comparison()` now refuses unsigned ordering shapes rather than answering them
wrongly. The wider lesson: widening admission can surface a pre-existing engine
difference that the refusal was hiding, which is why every widening is measured
against the dual-run rather than assumed.

`cast_char` and `cast_binary` came out of the same triage for the same reason:
`CAST(x AS CHAR|BINARY)` is `Cast{source}AsString` with the charset in the
result type, and the local arm already derives that. Only a *fixed-width*
binary target still declines, because its padding is bounded by
`max_allowed_packet`. What is left of the minted spellings is the temporal group
(`cast_datetime`, `cast_date`, `cast_time`) plus `cast_json`/`cast_year`. The
temporal three then came in as well, once the missing piece turned out to be on
the bridge rather than in the arm: TiKV's `CastTimeAsTime` passes the source
value through, while TiDB renders a *declared* precision as part of the value,
so `bridge::check_time` now carries the declared FSP onto the result the way it
already rebuilt a declared DATE. That closed the last known divergence of the
minted-cast group; only `cast_json` and `cast_year` remain.

The lazy-arm rule has now been tested three times (7.2, 7.4, 7.10) and the
generalisation is narrower than "no coercion in an arm": a coercion is safe
exactly when it is the coercion Go applies at that position. `WrapWithCastAsString`
on a lazy value is (verified: `elt(1, 65)` agrees); a numeric-to-int cast on a
`case` condition or on an `elt` index is not (both measured divergences). Each
test costs one dual-run, so the practical rule is to try the narrowest
relaxation, run the corpus, and keep only what stays at zero divergences.

The lazy design (`components/tidb_query_expr/SHORT_CIRCUIT_DESIGN.md`) found
that materializing a lazy child at a subset boundary must produce an owned
`VectorValue`, because an `RpnStackNode` borrows one lifetime; that the
thread-local varg buffers must not be held across a nested evaluation; that a
lazy kernel must keep `borrowed_fn_ptr: None` or the borrowed facade panics;
and that `logical_rows()` has a latent panic at exactly `BATCH_MAX_SIZE`
generated rows, so the lazy path must use the indexed accessors. The first
implementation step registers no lazy kernel, so the existing suite proves the
refactor is behavior-preserving before any semantics change.

The type inventory (`tikv-expression-type-gaps.md`) finds that `Set` is the
only missing value type; `Geometry` and arrays have no native datum or builtin
and need only explicit refusal. TiKV already contains unreachable `Set`
scaffolding, and extending the existing `Int`/`Bytes` hybrid carriers makes
every string/int kernel accept `Set` without a new ordinary signature.


## Decision Log


- Decision: parity target is the current native surface, not all of Go TiDB.
  Rationale: removal cannot regress behavior that does not exist, and chasing
  the full Go surface would make the first removal unreachable.
- Decision: keep laziness out of the wire format. Rationale: `ExprType` lives
  in `tipb`; a signature-driven lazy path inside `tidb_query_expr` needs no
  schema change and no second fork.
- Decision: admission becomes an explicit allow-list keyed by signature and
  shape, and silent fallback becomes a test failure. Rationale: a deny-list
  is silently wrong the moment a new signature appears, which is exactly the
  failure mode removal must not have.
- Decision: engine host capabilities (clock, RNG, user variables, locks,
  sequences, statement state) are injected through a trait, not through
  globals. Rationale: the engine must stay usable from tests and from
  planning-time call sites with no session.
- Decision: an error is never retried natively once a kernel has run.
  Rationale: warnings, RNG draws and lock side effects are not replayable.
- Decision: engine wording does not have to match Go; error-versus-success
  classification does. Rationale: agreed scope with the requester.


## Outcomes & Retrospective


The engine can now serve every expression the adapter admits, with the native
evaluator still the default and still the fallback, and with a corpus-wide
dual-run proving the two agree wherever the engine runs.

Current suites (TiDB numbers from `expression-reuse/round57-tests.log`):
`tidb-executor` 1340 + 355 + 6 + 2 with the engine feature and
1334 + 329 + 6 + 0 without; the added engine-on tests are the `VecGroupChecker`
and hash-splitter engine receipts. `tidb-expr` is 1212 + 60 with the feature (the three additions
are the shared-chunk result/error receipt, the retained-suite cache receipt, and
the shared-input ownership guard) and 1182 + 18 without. The most recently recorded TiKV suites remain
`tidb_query_expr` 477 passed and `tidb_query_datatype` 318. `catalog_diff` 31
and `query_diff` 1 pass; `expr_diff` keeps its two pre-existing `EXPORT_SET`
divergences, which are red with the feature disabled too.

The enrolled mysql replay was renewed on the current branch state
(`expression-reuse/replay-r52.log`, TiDB `1d9fd6f`): 142 of 10,251 compared
statements diverge -- the same count and the same divergence set (md5
`7b6445a8493f445641a9d07d787f0cba`, the id of the sorted `--- [topic]` header
list) as the engine-off baseline
(`expression-reuse/tidb-coverage-integration-native.log`, `backend=Native`, 0
engine rows) -- while the engine evaluated 217,539 expression rows over 1,831
statements, up from 12,480 over 1,552 in the previous receipt. That run links
the pinned fork rev `9fd4f94`, which is code-equal to the TiKV branch head:
`git diff --name-only 9fd4f94..HEAD` in the TiKV checkout lists exactly one
documentation file (`components/tidb_query_expr/EXPRESSION_SEMANTIC_GAPS.md`).
The widened admission surface therefore added no replay divergence.

What remains is not adapter plumbing but the removal itself: 43 production-path
textual sites of the 65 native `eval` hits outside projection (2 are the
unlinked `stream_agg.rs` duplicate, so 41 are reachable routing work), the
other 22 being test-only and re-pointed with the corpora (the check is
`rust/scripts/classify-native-eval-sites.py`; 11 documented sites no longer call
the native evaluator: 7 through the once-per-statement engine helpers, 2 row
loops moved onto retained `EvaluatorSuite::eval_chunk`, 2 removed as provably
error-only), the 33
corpora's conversion from dual-run to engine-only, and the deletion of the
feature gate and kernels. Known engine divergences are tracked rather than
hidden, and the dual-run makes any new one fail the suite.


## Context and Orientation


Repositories: `/home/agent/tidb/expression-reuse/tidb` (branch
`feat/tikv-expression-coverage`, committed base `30f3a2e`) and
`/home/agent/tidb/expression-reuse/tikv` (branch
`feat/standalone-expression-coverage`, `d2d9718`; its only delta from the
TiDB-pinned `9fd4f94` is documentation at this receipt).

The native evaluator lives in `rust/crates/tidb-expr/src/`:
`scalar_function.rs` (`ScalarFunction::eval`, `eval_by_signature`),
`ops.rs`, `compare`/`control` handling inside `scalar_function.rs`,
`time_fn/`, `string_fn.rs`, `builtin_ext/` (JSON), `arg_eval_type.rs` and the
`tests/` source-port corpora. The workspace currently has 353 raw `.eval(`
hits: 273 inside `tidb-expr/src` (the evaluator's own recursion) and 80
outside. The scripted outside inventory identifies 65 actual
`Expression::eval` sites (43 production, 22 test-only); the other 15 raw hits
are different `eval` APIs. Projection work reaches the engine through
`EvaluatorSuite`.

The engine adapter lives in `rust/crates/tidb-expr/src/tikv/` (`tikv.rs`
compilation and routing, `bridge.rs` value transport, `lowering.rs` and
`lowering/families.rs` signature selection) and in
`rust/crates/tidb-executor/src/stmt_context.rs` for the session context.

On the TiKV side the embedding seam is
`components/tidb_query_expr/src/standalone.rs` plus its child modules; the
evaluator is `components/tidb_query_expr/src/types/expr_eval.rs`; the
dispatcher is `components/tidb_query_expr/src/lib.rs`; function metadata types
are in `types/function.rs` and `types/expr_builder.rs`; the code generator is
`components/tidb_query_codegen/src/rpn_function.rs`.

Terms: a *signature* is a typed function identity (`ScalarFuncSig`), not a SQL
name. *Admission* is the decision to let a given expression tree run in the
engine. *Fallback* is a native evaluation performed instead of an engine one.
*Compiled program* is the immutable RPN form; *execution state* is the
per-call scratch the evaluator needs.


## Plan of Work


### Milestone A — the engine stops being per-execution and single-threaded

Make the compiled form `Send + Sync` and separate it from execution state.
On the TiKV side change the metadata signature from `Box<dyn Any + Send>` to
`Box<dyn Any + Send + Sync>` in `types/function.rs`, follow the compile errors
through the metadata constructors in the kernel files and through the 24
codegen references, and update `STANDALONE.md`. Then give the facade a
compiled form that can be evaluated through `&self`: either move the mutable
stack and output buffers into an explicit `ExecutionState` argument to
`eval_decoded`, or add a variant that allocates scratch per call. The stock
server path may keep its current shape; the standalone path is what must
become shareable.

On the TiDB side, cache the compiled engine program on the shared
`EvaluatorProgram` instead of in the per-suite `Mutex<ProjectionCache>`, so
parallel projection workers share one compilation with no lock on the hot
path. The cache key is the canonical expression hash plus the input column
schema. Report cache hits and misses in the coverage counters.

After this milestone a compiled program can be created once and evaluated from
several threads, and the feature-on suites still pass.

### Milestone B — one table decides admission, and silence fails

Replace the name-matching `admitted()`/`local_call()` logic with a table whose
rows are (signature, required argument eval types, shape constraints,
exclusion reason). Shape constraints cover the lazy rules and the
constant/leaf rules that exist today. Every SQL function name the Rust
rewriter can produce must appear either as an admitted row or as an excluded
row with a reason; generate that name list from the same registry the
rewriter uses, so a new function cannot appear without a decision.

Turn fallback into a gate. In test builds, when an expression compiles for the
engine but executes natively, record the reason and fail unless the reason is
an explicitly listed exclusion. The existing engine-row counters already
distinguish the cases; the work is to assert rather than to observe.

The inventory generator consumes the table so the CSV/JSON report shows
admitted, excluded and untested status per signature, not source-text
evidence.

### Milestone C — laziness

In TiKV, define the set of signatures that must see unevaluated children
(`if`, `ifnull`, `coalesce`, `case`, `and`, `or`, `xor`, `elt`, `field`,
`interval`, `greatest`, `least`, the `addtime*null` forms and `nulltimediff`).
Teach the evaluator to walk those with a lazy path: evaluate the selector
first, then evaluate only the children still required for the rows that still
need a value, and merge while preserving SQL three-valued logic. In the
row-oriented path this is an interpreter change; do not change `tipb`.

The TiDB side must not have to guess which signatures are lazy, because
compiling an eager kernel and a lazy kernel succeeds identically and an eager
one would silently produce the wrong semantics for a skipped branch. TiKV
therefore exposes a capability query (`is_lazy_signature(signature)`), and the
admission table's lazy shape rule consults it: a lazy shape is admitted only
when the engine reports that signature lazy. A test asserts the table's lazy
set and the engine's reported set agree, so adding or removing a lazy kernel
cannot drift silently. Until a signature is lazy, its non-leaf shapes stay
native exactly as today.

Provide the host capability trait at the same time, because laziness without
it still cannot express `IF(cond, getvar(...), 0)`:

    trait HostEval {
        fn current_time(&self, fsp: i8) -> Result<Time>;
        fn rand(&self) -> f64;
        fn user_var(&self, name: &str) -> Option<ScalarValue>;
        fn statement_value(&self, which: StatementValue) -> Result<ScalarValue>;
        fn advisory_lock(&self, op: LockOp) -> Result<i64>;
        fn max_allowed_packet(&self) -> usize;
        fn default_week_format(&self) -> i64;
    }

The standalone `Context` gains an optional implementation of this trait;
`None` means "capability absent" and the call returns a structured
`Unsupported` error rather than 0 or a panic.

On the TiDB side, the local adapter admits non-leaf lazy shapes and the engine
itself refuses a program that mixes lazy and eager lazy-sensitive nodes, so
the adapter never has to guess. It deliberately does NOT add the issue's
eager/lazy kill switch for the local path: laziness here is a correctness
property the adapter depends on (a skipped branch must not run), not an
optimization that can be turned off, and a program whose nodes are eager is
already refused. The issue's `tidb_enable_short_circuit_expression` variable
and DAG flag remain the right shape for *remote* pushdown rollout, which is a
different code path. TiDB's own vectorized short-circuit for `AND`/`OR`
(selection based) only matters while the native evaluator still exists.

### Milestone D — types the removal needs

Start from the native surface inventory produced in milestone B. The completed
inventory (`tikv-expression-type-gaps.md`) finds that **`Set` is the only value
type that must be added**; `Geometry` and arrays are not needed because the
native evaluator has no datum kind and no builtin for them. `Set` is an
input-only value: no builtin returns one and there is no wire `SetLiteral`, but
every implemented `ETString`/`ETInt` builtin accepts one, so all `Set`
expressions fall back today. TiKV already owns most of the scaffolding
(`EvalType::Set`, `ScalarValue::Set`, `VectorValue::Set`, `SetRef`, the
`*ForSet` aggregators, `cast_set_as_int`) but it is unreachable; the additions
are the `FieldTypeTp::Set` mapping, chunk/raw codecs whose name is the
comma-joined selected `elems`, the `ChunkedVecSet` element-name storage, the
`Int`/`Bytes` hybrid borrow arms, the standalone `Column::Set`, and registering
the existing cast. No new ordinary kernel signature is needed, because
extending the two hybrid carriers makes the existing string/int kernels accept
`Set` exactly as they accept `Enum` today. A latent `get_enum` var-length
indexing bug in the TiKV chunk codec is flagged while adding `get_set`.

### Milestone E — remove

The native evaluator's surface *outside* projections is inventoried by
`rust/scripts/classify-native-eval-sites.py` and recorded in
`tikv-expression-removal-native-sites.md`: 80 raw `.eval(` hits outside
`crates/tidb-expr/src`, 15 of them another API, leaving **65**
`Expression::eval` sites. **43 are production-labelled text** (26 row
loops/comparators, 11 one-chunk-row probes, 6 `Row::empty()` constants); two
row-loop calls are the unlinked `stream_agg.rs` duplicate, so 41 are reachable.
The other 22 are test-only.
None needs a new kernel: a row loop evaluates each item over its chunk, a probe
uses a one-row chunk, and a constant uses a virtual one-row chunk.

Eleven documented sites no longer call the native evaluator: 3 DDL constant
rows, 4 pruning rows (one retains its explicit sparse-input native fallback), 2
error-only sort branches, and 2 retained-suite grouping loops
(`VecGroupChecker` and the hash-shuffle splitter). Each retains one
`EvaluatorSuite` per grouping item and calls `EvaluatorSuite::eval_chunk` per
chunk; its immutable `EvaluatorProgram` holds
the compiled engine cache across chunks. The free `evaluator::eval_chunk` is the
one-off shared-chunk seam, while a retaining operator must use the suite method.
Each conversion has a native-vs-engine result receipt and an engine-row counter
receipt; the retained suite has a separate two-chunk/one-compilation receipt.


Flip the default to the engine, delete the `tikv-expr` feature, delete the
fallback branches, then delete the native kernels and their source-port tests
whose subject is now the engine, keeping the Go-oracle corpora that still
validate results. Before deleting, run the full Rust suites, the enrolled
mysql replay, and the Go expression suites as the external oracle. Record the
remaining exclusions as explicit errors, not as silent native execution.


## Concrete Steps


One heavy command runs at a time across both repositories, through
`/home/agent/tidb/expression-reuse/tools/limited-run.py`, with
`CARGO_BUILD_JOBS=1`, `RUST_TEST_THREADS=1` and single-threaded Go. Use
`RUST_MIN_STACK=2097152 MALLOC_ARENA_MAX=2` whenever a suite spawns worker
pools, otherwise the address-space cap turns thread creation into `EAGAIN`.

TiKV:

    cd /home/agent/tidb/expression-reuse/tikv
    python3 /home/agent/tidb/expression-reuse/tools/limited-run.py -- \
      cargo test -p tidb_query_expr --lib -j1 -- --test-threads=1

TiDB Rust (engine on), with the local engine checkout patched in during
development:

    cd /home/agent/tidb/expression-reuse/tidb/rust
    python3 .../limited-run.py -- cargo test -p tidb-expr --features tikv-expr --lib --test all -j1 \
      --offline --config 'patch."https://github.com/YangKeao/tikv.git".tidb_query_expr.path="/home/agent/tidb/expression-reuse/tikv/components/tidb_query_expr"' \
      -- --test-threads=1

Expected: the feature-on counts are 1200 + 39 for `tidb-expr`, 353 for
`tidb-executor`, 1726 + 338 for `tidb-session`; feature-off is 1180 + 18,
329 and 1726 + 337. Any drop means a regression in the coexistence period.

The replay is the semantic gate:

    INTEGRATION_TIKV_BACKEND=copying python3 .../limited-run.py -- \
      cargo test -p difftest-result-tests --features tikv-expr --test integration_diff -j1 -- --test-threads=1

Its current state is 142 divergences out of 10,251 compared statements with a
divergence set identical to the native baseline (`md5
7b6445a8493f445641a9d07d787f0cba`) and therefore pre-existing; `expr_diff` (2
cases), `table_diff` (7 of 1,942) and `join_shape` (stale ratchet) are red with
the feature disabled too. Milestone E requires those to be resolved or
explicitly ratcheted with a reason.


## Validation and Acceptance


Milestone A: a token `PreparedExpression` (or its replacement) is `Send + Sync`
proven by a compile-time assertion, two threads evaluate one compiled program
concurrently producing identical output to a single-threaded run, and the
feature-on suites keep their counts.

Milestone B: every function name in the rewriter registry has an admission
row; a deliberately admitted-but-not-executing expression fails the test; the
inventory reports per-signature status.

Milestone C: `IF(0, <overflow>, 7)` executes in the engine and returns 7 with
no error and no warning; `IF(1, 1, <overflow>)` likewise; an unused branch
containing `GETVAR` does not run; TiDB and TiKV agree on `AND`/`OR` with NULL
operands under selection; the issue's switch exists in TiDB and is propagated.

Milestone D: each added type round-trips through the adapter and matches the
native evaluator on the same fixture; a type whose encoding cannot be matched
is refused with an explicit error.

Milestone E: `rg 'tikv-expr' rust/` returns no feature gate, the native
evaluator modules are deleted, and the full suites plus the replay pass. The six
names the pinned `tipb` cannot address (`translate`, `weight_string`,
`load_file`, `json_schema_valid`, `localtime`, `localtimestamp`) return the
classified "no engine" error instead of running natively; the checklist's
permanent-exception table is the list, and nothing else may join it without
evidence.


## Idempotence and Recovery


Every milestone is additive until E. The native path stays default until the
last step, so a failed experiment can be reverted by dropping a commit rather
than by repairing a broken evaluator. Cache and admission-table changes are
pure functions of the expression tree and input schema, so re-running is safe.
Deleting native code is the only irreversible step and is gated on the
milestones before it.


## Artifacts and Notes


Recovery-audit commands (TiDB repository `rust/`, no Cargo patch):

    cargo test -q -p tidb-expr --features tikv-expr --lib -j1 -- --test-threads=1
    cargo test -q -p tidb-expr --features tikv-expr --test all --locked --offline -j1 -- --test-threads=1

Both ran serially under `limited-run.py --rss-mib 8192 --as-mib 16384` using
nightly-2026-08-22, `CARGO_BUILD_JOBS=1`, `RUSTFLAGS=-Awarnings`,
`MALLOC_ARENA_MAX=2`, `RUST_MIN_STACK=4194304`,
`CMAKE_POLICY_VERSION_MINIMUM=3.5`, `CXXFLAGS='-w -std=gnu++14 -include cstdint'`
and `CFLAGS=-w`. Peak group RSS: 2473.3 MiB (lib), 1109.1 MiB (integration).
TiKV repository validation used the same environment and guard:

    cargo test -q -p tidb_query_expr -j1 --offline -- --test-threads=1

Result: 481 passed, four documentation tests ignored; peak RSS 1051.3 MiB.
This audit did not rerun executor/mysql replay or repository-wide lint and does
not claim native removal or PR readiness. An optional whole-workspace offline
`cargo metadata` inspection failed because `adler32 v1.2.0` was not cached;
this did not affect the above package tests or the generated git-source lock.

`components/tidb_query_expr/EXPRESSION_SEMANTIC_GAPS.md` (TiKV) is the running
list of kernel divergences. This plan, the coverage report and the inventory
generator under `rust/docs` and `rust/scripts` are updated as milestones land.
The earlier copying/borrowed benchmark artifacts remain untouched.


## Interfaces and Dependencies


TiKV keeps `PreparedExpression::compile(serialized_expr, serialized_schema,
Context)` and gains a `Sync` compiled form with an explicit execution-state
argument. It exposes `scalar_function_signature(name) -> Option<i32>` and the
host trait above, and its `Context` carries the optional trait object.
`tidb_query_datatype` gains only the types milestone D justifies.

TiDB's `tikv.rs` keeps `TikvExpression::compile` and `evaluate`/`evaluate_into`
signatures, gains a per-`EvaluatorProgram` cache, and consumes the admission
table instead of name matching. The `Backend` enum and counters stay until
milestone E. No new dependency on a second fork is introduced.

Revision note: the recovery audit pins the actually used selected-input engine,
records the dense-selection regression and unpatched tests, and corrects the
remaining borrowed-lazy and unequal-input-length limitations. It does not mark
Window/Join migration or native deletion complete.
