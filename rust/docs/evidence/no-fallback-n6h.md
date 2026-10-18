# N6h: control demand inside row and window programs

This checkpoint continues the no-fallback migration. It does not retire the
alternate evaluator globally. All commits are local and signed off; no push,
PR or deployment is authorized by this checkpoint.

## Behavior and implementation

Before the fix, the real native comparison for IF(1, NULL-column +
(MAX_INT * 2), 0) succeeded, while the prepared typed control attempted the
right-side multiplication and failed. The regression was run before changing
production (`n6h-control-root-red.log`). Production root promotion now chooses
the closed native-value row program for controls/comparisons with arithmetic
subtrees. This uses the same canonical SDK frame driver and shared kernels.
Explicit old strict test-consumer compilation is unchanged.

A second native comparison found different output publication after a late
failure: native eligible batch evaluation publishes no prefix, whereas row
mode does. The caller now stages final owned Datums for these eligible batch
roots. Cell capacity and conservative payload allowances are checked before
cloning; capacity grows amortized. Failure drops the pending batch and remains
terminal. The staging policy is selected from immutable native eligibility and
the actual per-output global getter, before data effects. It is not an error
retry or an expression evaluator.

The SDK previously rejected a typed control anywhere below an explicit window
root with a structural occurrence limit of one. Two protocol regressions
reproduced that restriction before the fix (`n6h-control-window-red.log`).
The existing RowHostScope now schedules typed control operands one original
occurrence at a time. Each ControlFrame still evaluates one row, completes its
demanded branches before the next occurrence, and only then returns a complete
vector to the window parent. Window→control→Window preserves occurrence_start,
including repeated physical rows. The original HostFrame cache, Fresh/Reuse,
step/depth/storage/task budgets and cancellation guard remain in use.

Typed scoped copies share the existing eight-carrier batch import dispatch.
Borrowed payload size is charged before copying; successful singleton owners
and final compact output coexist under a conservative allowance and measured
retained storage. This is not a hard allocator-peak guarantee. Host copies keep
their original Int/Bytes contract. Ordinary/control roots and legacy entry
limits are unchanged; only an authenticated explicit window root admits its
whole selection with scoped control descendants.

## Validation

Evidence logs are in the agent workspace `evidence/no-fallback/`.

- Original four control roots pass after staging, including both global flag
  states and late errors (`n6h-control-root-staged.log`).
- Expanded sixteen roots × seven selections × four flag changes =448 native
  comparisons pass, including empty, reordered and2050-occurrence selections.
  A separate test proves terminal staging resource refusal and clean reuse
  (`n6h-control-front-expanded.log`).
- Six new rewritten SQL forms add72 actual native comparisons; the full
  frontend has1485 passed / exact four inherited failures /95ignored, including
  all126 batch-module cases (`n6h-control-front-full.log`). The full suite is
  not green; failure names are compared exactly with N6g.

- All14 window protocol tests pass. The three new cases cover four selection
  controls, a logical condition, nested windows,2050 occurrences, Fresh/Reuse,
  primary-error identity, task cleanup, resource refusal, clean reuse and five
  non-Bytes typed carriers through actual ordinary frames, including a wide
  Decimal (`n6h-control-core-full.log`).
- Full SDK:1058 passed / exact six inherited failures /1ignored, with the
  failure names matched exactly against N6g. This suite remains non-green.
- Production Projection25 passed, including four new mixed-control runs across
  both flag states and serial/two-worker execution
  (`n6h-control-projection.log`).
- `make lint`, scoped project clippy (89 inherited warnings), both pinned
  format checks and both `git diff --check` passed
  (`n6h-control-lint.log`, `n6h-control-core-clippy.log`).

SDK checkpoint: local signed-off commit `cff06c7`. Frontend production tests
used this same SDK implementation; subsequent SDK edits added only the final
five-carrier protocol test and documentation comments before the full SDK and
clippy runs. No source/build jobs remain at signing.

## Reproduction

From TiDB root, use the pinned wrapper:

    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib native_control_
    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-executor --lib projection::tests
    make lint

From TiKV root:

    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tikv test --release --locked --offline -j8 -p tidb_query_expr --lib

Scoped clippy command from TiKV root:

    RUSTFLAGS='' TIKV_FRAME_POINTER=0 make run COMMAND='bash /home/agent/.slock/agents/1555103c-9f3e-46ca-ac2e-b964c650c2f1/evidence/no-fallback/n3-clippy-scoped.sh --lib --tests --release --locked --offline -j8'

Format only changed Rust files with each repository's pinned rustfmt; run
`git diff --check` in both trees.

## Remaining work

General function families, dynamic/deferred/type-before-value sources,
ordered effectful multi-output evaluation, remaining callers and global
alternate-driver retirement remain open. Resource limits and SQL failures
must stay terminal throughout; valid SQL cannot be replaced with unsupported
as a permanent coverage substitute.
