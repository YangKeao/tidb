# N6t — direct normalized Boolean/logical values

Date:2026-10-18. Parent TiDB473b648a / TiKV9b9e060; SDK production
remains6a36ba0. Local-only incremental checkpoint, not global completion.

## Result

Public Boolean/logical normalized-value entries and canonical truth/logical
hosts now share the same plain predicate_value helpers and actual impl_op
kernels. Removed the old logical argument-program adapter and Boolean
operation-to-Ready-args construction. Public function signatures remain;
these entries no longer look up ReadyValueCache or construct an RPN argument
program. Existing canonical source demand, truth conversion, warning policy,
resource checks and result metadata publication stay at their original sites.

Eight Boolean operations retain actual true/false/NULL semantics and the
original NOT/IS composite calls. Three logical operations consume only
already-converted values. An explicit undemanded RHS still permits only false
AND or true OR, using the original irrelevant false representative; it is not
a fetched child or a fabricated SQL NULL. Invalid markers preserve the actual
opaque ExpressionAdapterFailure Scope/Contract kind and exact reason before
any cache access. Pure SDK failures retain their actual LocalError, original
outer classification and no fabricated worker phase. Helpers use stack-sized
truth markers and introduce no formatting/work-buffer allocation.

## Independent OLD and route evidence

Agent workspace evidence/no-fallback/:

- n6t-predicate-original-witness.log:720 independent comparisons of actual
  still-active OLD workers against direct kernels. All8 Boolean functions x3
  values, all3 logical functions x12 demand/value profiles, no-cache/fresh
  cache/reused cache and four passes. It includes84 invalid markers; another
  84 rejection-priority probes ensure those reject before cache lookup.
- n6t-predicate-original-private-cause.log:the original seven invalid marker
  profiles retain the exact private ScopeFailureKind::Contract and reason
  "invalid undemanded logical right argument". This tests the cause privately,
  not by parsing the opaque rendered message or comparing independent capture
  identities.
- OLD fixed FNV result trace8c5608c8b554ec31 is now asserted. Never regenerate
  it from the new callers. The test-only reproduction patch
  n6t-original-predicate-baseline.patch applies at TiDB473b648a/SDK6a36ba0;
  run original_program_witness and the private-cause test to reproduce. Source
  snapshots n6t-before-lib.rs / n6t-before-tikv-ready_value.rs are preserved.
  Historical artifacts are not restart scripts for the current tree.
- n6t-predicate-old-worker-route-red.log:both new Boolean/logical regressions
  fail at an actual old Ready worker/cache lookup for legitimate values.
  Their rejecting contexts still pass after migration, including all24
  Boolean and36 logical profiles; illegal markers retain contract failures.
- n6t-predicate-front-full.log:the initial public-wrapper compatibility pass,
  1532 pass/exact inherited4/95 ignored. A stale edit-script host anchor stopped
  the remaining source edits; this run did not claim final canonical helper
  sharing or the fixed-hash assertion. Those were completed afterward and
  require the final shared-helper run below.

## Final validation

Final gates all closed:

- n6t-predicate-front-full-shared.log:1532 pass, exactly the four inherited
  failures/95 ignored, zero filtered. All159 batch checks, fixed OLD predicate
  result trace, both actual worker-route regressions, private marker-cause
  checks and prior arithmetic/comparison witnesses pass. The exact inherited
  failure list is machine-compared to N6s in n6t-predicate-inherited-failures.txt.
- n6t-predicate-projection.log:all25 production Projection tests pass,
  including the mixed28-root matrix under both flags and serial/two workers.
- n6t-predicate-lint.log:make lint passes. Pinned nightly formatting for all
  eight changed Rust files and both repository diff checks pass. A final
  comment-only module overview correction does not alter the verified source
  behavior; no extra execution gate was run for that prose change. Existing batch source/resource/error
reuse checks and prior independent arithmetic/comparison witnesses stay
active. SDK production/datatype did not change, so N6q0's1068 pass/exact six
inherited failures/one ignored and89 clippy warnings are reused, not rerun.

Commands from TiDB root:

```sh
RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib original_program_witness -- --nocapture
RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib predicate_entries_do_not_acquire_ready_workers
RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib
RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-executor --lib projection::tests
make lint
```

Remaining adapters and builtin/source/composition/caller coverage, global
alternate dispatch retirement and final inherited/SQL/server gates still
remain. Goal/task4 stays active. No push/PR/deployment is authorized.
