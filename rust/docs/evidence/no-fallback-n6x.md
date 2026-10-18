# N6x: genuine INTERVAL values and same-driver sources

INTERVAL eager/lazy frontend adapters now use actual SDK values and search
state, with no native caller identity/head/step Ready programs or scoped-context
rebinding. Canonical `IntervalNative` sources execute in the same SDK frame,
including composition under arithmetic and row controls. Overall expression
coverage and global fallback retirement remain incomplete. No push/PR/deploy.

## Contracts

SDK `native_interval_value.rs` owns eager borrowed classification, preallocated
real-conversion state and `NativeIntervalSearch`. Eager arity0/1 retains actual
original InvalidBatch at Invoke, including the closed-input-shape cause. The
same pure LocalError producer is used by the original worker admission guard.
Invalid internal progression retains original private Scope/Contract report
failure; only explicit resource errors map to ResourceLimit/phaseNone.

Eager inspects every range sentinel before target NULL; all required non-NULL
real conversions finish before its answer. Lazy complete optional metadata
selects all-integer vs real and nullable linear vs NOT NULL binary probes before
reading sources. Target NULL yields -1 even with NOT NULL metadata. Actual
signed/unsigned kinds are retained. Eager boundary<=target partition and lazy
target<boundary deliberately preserve distinct NaN behavior. Existing ordinary
wire compatibility profiles retain complete validation/protocol and share
classification/search primitives only after independent proof; no unrelated
wire allocation contract is changed.

Canonical sources snapshot complete actual FieldTypes and detached producer
ordinals. SDK picks each exact probe, including nonlinear 0→2→1 reads; the
frontend authenticates capability role/node/row occurrence, releases source
borrows before original context effects, and performs only requested actual
casts. No Expr callback, fabricated literal or secondary program is stored.
Results pass existing scalar-function RT coercion and outer ENUM_SET_AS_INT
publication. NativeEligibility remains separate from compiled source closure.

Finite logical allowances cover eager view/state ownership, canonical source
clone allowance, and demanded conversion work (default64MiB). Decimal visible/
storage padding is bounded before formatting; skipped arguments are never
inspected or charged for casts. Resource refusal does not retry/replay or become
a permanent Unsupported result. This is a conservative logical work/retained
bound, not whole-process or unknown source allocation recovery. Cancellation
and fresh reuse preserve capability epochs and per-occurrence origins.

## Independent original evidence

Before any production change at TiDB95399873/SDK3a7b17c:

- eager73728 actual return/cast/context cases: fixed319a9e8733ea8257;
- lazy294912 metadata/probe/cast/context cases: fixed242c1ac24fae8178;
- lazy960 actual source SQL/resource error identity/skip/retry cases:
  fixed2bb943c97c73bb21.

Never regenerate these hashes from the new implementation. Original production
and test snapshots and test-only baseline patch
`n6x-original-interval-baseline.patch` are retained in agent evidence/no-fallback;
cached apply-check against the original baseline passed using a separate index.
Three genuine source roots each refused/fell back once; three actual rejecting
Ready-cache contexts failed old eager-integer/eager-real/lazy adapters.

The first helper insertion failed before writing, so
n6x-old-vs-interval-values.log is only an OLD rerun, NOT independent proof.
Actual independent comparison initially exposed an arity mismatch, while all
lazy cases passed. After sharing the real admission error producer, ALL369600
old/new cases matched, including exact structured cause/class/phase, while the
old controllers and wire bodies were STILL ACTIVE. Only then were they replaced.
`n6x-old-vs-new-interval_value_tests.rs` preserves that independent comparison.

## Verification and artifacts

Logs are in agent workspace evidence/no-fallback/:

- n6x-original-interval-witness.log:original three fixed cohorts pass.
- n6x-interval-source-red.log and n6x-interval-{eager-int,eager-real,lazy}-worker-red.log:
  genuine pre-change route/cache failures.
- n6x-old-vs-interval-values-actual.log:independent eager arity discrepancy;
  n6x-old-vs-interval-values-arity.log:all369600 pass BEFORE replacement.
- n6x-interval-value-green.log:all fixed original witnesses and three actual
  rejecting-worker contexts pass after replacement.
- n6x-interval-source-first.log:all three original source roots turn green.
- n6x-interval-parent-diagnostic.log:NEW parent fixture was correctly rejected
  for inconsistent complete column declarations (NOT NULL vs nullable use).
  Fixed only the fixture declarations; admission remains strict. Corrected
  n6x-interval-parent-schema.log passes every original parent assertion.
- n6x-interval-provider.log:nonlinear probes reject stale/wrong-role/node/
  occurrence handles, clone-budget overflow and demanded huge Decimal work.
  No later context effects, repeated cancellation and same-provider success pass.
- n6x-interval-front-final.log:1574 pass, exact inherited4/95ignored/zero
  filtered; all175 batch tests pass, including12288 raw-source/metadata cases,
  parent error/short-circuit/skipped branches/2050 duplicates and exact reuse.
  Private eager/lazy budgets and exact arity cause/Invoke phase also pass.
- n6x-interval-core-full.log:1076 pass, exact inherited6/one ignored/zero
  filtered. New units cover actual integer/real/NULL/NaN search, malformed
  complete wire input before NULL, finite preallocation and sentinel precedence.
- n6x-interval-clippy-final.log:success,89 warnings unchanged.
- n6x-interval-lint-final.log:make lint success before Projection fixture split.
- n6x-inherited-failures.txt:machine equality against N6w exact failure lists.

The first proposed single34-root Projection hit the existing equal-share source
metadata limit before reaching any INTERVAL: slot2 was the OLD nested CAST_JSON
root. `n6x-interval-projection-diagnostic.log` records the actual ResourceLimit
cause (batch caller source metadata policy exceeded), retaining terminal Prepare
semantics. Temporary diagnostic instrumentation was removed; no production
limit, resource classification or admission was relaxed. The final test keeps
the complete prior31-root matrix and adds a separate8-root heterogeneous matrix
(arithmetic/string, all3 IN domains, all3 INTERVAL domains), each exercising
both vectorization flags and zero/two workers with native result/warning/route
checks. `n6x-interval-projection-bounded-final.log` passes all25 Projection
checks, including both complete mixed matrices under both flags/worker counts.
`n6x-interval-lint-bounded-final.log` passes final lint. Pinned rustfmt checks
for all14 changed Rust files and both repositories' diff checks pass. All jobs
closed. No code was changed to enlarge the old resource envelope.

Exact commands from TiDB root:

```sh
RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib
RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-executor --lib projection::tests
make lint
```

From TiKV root:

```sh
RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tikv test --release --locked --offline -j8 -p tidb_query_expr --lib
RUSTFLAGS='' TIKV_FRAME_POINTER=0 make run COMMAND='bash /home/agent/.slock/agents/1555103c-9f3e-46ca-ac2e-b964c650c2f1/evidence/no-fallback/n3-clippy-scoped.sh --lib --tests --release --locked --offline -j8'
```

Datatype and Unistore sources are unchanged; their suites are not rerun for this
checkpoint. No SQL-server/real-TiKV or final inherited-failure/global-closure
claim. Other native caller families, source/composition coverage and final
alternate-dispatch retirement keep goal/task4 active.
