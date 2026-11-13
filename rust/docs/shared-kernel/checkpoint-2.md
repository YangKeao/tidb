# Checkpoint 2: demanded columns and first review corrections

This checkpoint remains opt-in and does not complete M1. Its parent pair is TiDB7e3a636674 / TiKV52d7be276. Historical worktrees are unchanged.

The original RPN loop now requests native columns only when their ColumnRef is reached. Native input supplies data, never evaluates an expression. PB keeps its original eager predecode before eval_decoded; the decoded loop uses the same input interface. Signed integer columns now retain NULLs, repeated and shuffled selections. A false native IF can skip an absent physical column; choosing that column errors.

Review corrections: FLOAT/BIT/unsigned/array IF results are explicitly rejected until shared result coercion is implemented. They are not counted as supported types. Native original evaluation is asserted to produce Float32/Bit in the regression. Double/Decimal NULL constants now retain their declared family, and the program records an explicit root result type. The constant-last optimization is disabled for these declared control programs. Decimal parts validate leading integer residual range and trailing fractional alignment in both repositories. General control-program validation/freezing and dynamic branch joins remain next-stage work.

The inventory conservatively scans whole original files, retaining candidate anchors after inline tests. It records exact family-ledger and sorted-identity SHA256 hashes. Candidate hits may be tests or registrations; none is equivalence credit. The budget correction and original/revised/actual comparison are in inventory-notes.md. The22–34K known scope includes13–22K core with contingency and9–12K overhead once each; algorithm/extra-compatibility/retirement costs remain unbounded before M2.

## Validation

Run from tidb/rust using ../../tools/cargo-tidb, with --locked --offline:

- `test -p tidb-expr --features shared-kernel --lib shared_kernel -- --nocapture`: before fixes,7 selected/4passed/3failed, reproducing FLOAT admission, typed NULL exit and partial Decimal alignment. After fixes,7passed/0failed.
- `test -p tidb-expr --no-default-features --lib compress -- --nocapture`:8passed/0failed/2pre-existing ignored. This builds and exercises the default compression path with shared-kernel disabled. The ignored memory-tracker/vector-signature gaps remain explicit.
- Root `make lint`:passed. Both `git diff --check`:passed.
- SDK executor library check completed successfully during the preceding input-adapter edit interval; not a frozen-checkpoint test attribution. New SDK loop tests are still compiling dependencies at publication; no passing result claimed for them.

The optional route does not isolate workspace dependencies: flate2 changed1.1.9→1.0.11, protobuf registry2.28→PingCAP2.8, and cmake changed source. The default test is representative, not complete validation of all consumers. Full SDK tests/clippy, Bazel preparation (bazel unavailable), remaining M1 gates and performance are not passed by this checkpoint.

Raw logs are retained on home-gentoo in the implementer's evidence/shared-kernel-refactor directory: input-tests.log (5passes before review corrections), review-regressions-before-2.log (3expected failures), review-regressions-after.log (7passes), default-compression-tests.log (8passes/2ignored), lint-review-fixes.log. The first regression command used an incorrect relative edit path and then ran the unchanged5-test suite; review-regressions-before.log is that non-regression run, not failure evidence.

## Raw change accounting

Counts include comments/blank lines. Tests are split at the single terminal test module of the new native adapter; other changed Rust is production in this checkpoint. Build/config and documents remain separate and count toward total additions plus deletions. No historical experimental reduction is credited.

| Repository / basis | Production + / - | Tests + / - | Build + / - | Documents + / - |
| --- | ---: | ---: | ---: | ---: |
| TiDB / original | 472 / 0 | 357 / 0 | 1866 / 91 | 7813 / 0 |
| TiKV / original | 291 / 44 | 0 / 0 | 0 / 0 | 12 / 0 |
| TiDB / checkpoint1 | 127 / 43 | 148 / 0 | 0 / 0 | 1085 / 70 |
| TiKV / checkpoint1 | 86 / 28 | 0 / 0 | 0 / 0 | 0 / 0 |
