# Stateful services and repeated children in the shared expression frame

Status: local checkpoint paired with unchanged SDK `ad73b11`. Task #4 remains
active: JSON-schema, RAND, owner-state, AES, optional/PB/public/unistore/filter
entry closure, generic Admission deletion and final server/performance remain.

VALUES, BENCHMARK, SLEEP and five advisory-lock operations now use the existing
SDK HostFrame argument broker. The source retains immutable SQL metadata and
actual value capabilities. It contains no expression callback, nested compiler
or second expression driver. The eight early public scalar bodies share plain
value/context phases; their executed original branches remain test-only controls.

BENCHMARK validates its real count, preserves NULL/negative barriers and validates
the original body declaration even at zero iterations. Every iteration requests
the genuine body Fresh in the same frame. Lock-name conversion precedes timeout
child demand; timeout conversion, warning and clamping precede the actual session
service. VALUES retains its real saved offset and arity check. SLEEP retains its
original NULL/negative handling and interrupt result. All returns pass through the
original scalar family conversion before output publication.

## Ownership fixes demonstrated by failing tests

1. Ten ignored one-MiB BENCHMARK results originally succeeded but the new append-only
   value store exhausted its allowance. Completed Fresh subtrees now release their
   values after authentication. Slots carry checked versions; old capabilities
   cannot read reused slots. The private ABI remains forty bytes, with a new
   internal version, real producer ordinals and actual occurrence/physical rows.
   Ancestors and other occurrences remain live. Checked namespace, window and slot
   exhaustion never wrap.
2. SLEEP rejected a two-MiB vector in the original numeric conversion, while an
   extra host copy caused ResourceLimit first. A shared pure nontext preparation
   now borrows the authentic value only before context callbacks. Text continues
   through its original context-aware conversion. The old conversion body remains
   an independent test-only control.
3. BENCHMARK count and GET_LOCK timeout reproduced the same extra-copy problem.
   The host now transfers the actual SQL owner into a stack lease instead of
   cloning it. No RefCell borrow crosses a context callback. A private non-SQL
   marker rejects ordinary readers; metadata authentication remains possible.
   Drop restores only the same namespace, window, slot version, role and origin.
   A lazy shared counter charges the actual ledger allocation and outstanding
   payload even if a callback resets the window. It cannot resurrect a stale or
   reused slot. This is the existing logical allowance, not a statement heap
   tracker; resource ceilings are unchanged.
4. VALUES returning a malformed Decimal with huge unused shape metadata exposed
   formatter estimation preceding the original coefficient-validation panic.
   The host now uses the existing identity return-coercion owner, which preserves
   that original validation before estimating later demanded formatting work.

All four failing scenarios remain in archived logs and executable regressions.
The first loan test setup also exceeded its own one-MiB insertion allowance before
reaching the loan; only that new fixture's allowance was corrected. Two new-test
compilation errors concerned an optional result type and a copied control's
receiver name. None reclassifies an inherited failure or changes production limits.

## Retained comparisons

| Observation | Count / checksum | Reference |
| --- | --- | --- |
| Original stateful suite | 10,368 / `C0B8ADE1ECE31EEF` | Original public traversal captured and rerun before extraction; fresh Ready per independent invocation |
| Independent phases | 1,728 / `90EA3BA8B5A947B2` | Executed old early branches versus additive descriptor phases, before public retirement |
| Value storage | 3,576 / `9F323E997721426B` | Executed independent append-only owner versus generational slot pool |
| SLEEP values and returns | 59,004 / `8C582504AD4E5981` | Actual sources versus original public results, effects and publication prefixes |
| Nontext conversion | 174 / `0DF653FAA526C2B2` | Still-active original conversion versus additive pure helper, before extraction; f64 bits and context events |
| Other stateful values and returns | 472,032 / `E1A862511376506D` | Full actual-source integration across 149 values, eleven return declarations, eight variants, six profiles, both flags and three selections |

The last integration matrix was added after phase extraction; it is not represented
as a pre-extraction observation. All earlier frozen checksums remain unchanged.
Executed original source archives, capture/rerun logs, JSON receipts and the
reconstruction script are in the agent's `evidence/no-fallback/n17-*` archive.

Additional checks cover large literal/column/parameter and nested BENCHMARK bodies,
100,000 actual small reads, 1,024 pool reuse epochs, all native Datum representations,
forged/foreign/stale/wrong-role/node/row capabilities, wire NULL, exclusive context
borrowing, panic and generation mutation, consumed-task replay and real resource
or SQL failures. Loan tests cover ordinary/error unwind, active-payload accounting
through reset, namespace replacement, slot reuse and budget failure before transfer.

Long checks use 2,051 and 2,057 physical rows, selections crossing the existing
1,024-row window, reverse/duplicate/empty selections, late child errors and public/
shared-frame alternation on the same source owner. Pending BENCHMARK-body and owned
GET_LOCK-timeout cancellation each run 128 times, with stale cancellation/reply
rejection and fresh NULL count/timeout recovery on the same provider.

Projection adds two cohorts after all existing configurations. They mix all eight
operations with prior cached-list roots, plus an ordered BENCHMARK assignment and
read. Both flags and serial/two-worker execution preserve original values and
warnings. The actual lock service is the default isolated in-memory session
service, timeout zero; SLEEP zero never waits. Other service tests use test doubles.

## Validation

Frontend: **1,974 passed / exact inherited 3 failed / 95 ignored**, all **433 batch
checks**, exact **346 warning titles**. Projection: **26 passed**, including the two
new cohorts. Lint, both pinned formatters on twelve changed Rust files, diff checks
and original-evidence reconstruction pass.

The SDK stays clean at `ad73b11`; its N16 fresh gates remain applicable without a
new SDK run: **1,148 passed / exact inherited 1 failed / 1 ignored**, **17 warning
titles**, and successful repository all-target clippy with **171 total / 89
expression warning titles**. No current measurement is substituted for those logs.

Reproduction from the TiDB worktree:

```sh
RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib
RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-executor --lib projection::tests
make lint
```

The current source closure requires genuine declared scalar argument/result types
and no protocol origin. This checkpoint does not close the overall task.
