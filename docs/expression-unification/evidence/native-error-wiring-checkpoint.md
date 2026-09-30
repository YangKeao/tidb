# Native error envelope checkpoint: native-error-wiring-03

This is compiled error transport/terminal mapping, not a live public evaluator producer, opaque-error SQL end-to-end acceptance, ASCII activation or a migrated family. Completed families remain0/245; target221. TiKV product code is unchanged.

## Source changes and ownership

D changed only `tidb-expr/src/context.rs` (one EvalError variant) and `tidb-executor/src/driver/errors/exec.rs` (one fixed-message arm and two existing-native-origin regression tests). Parent added native-only exports in `tidb-expr/src/{lib,tikv/mod}.rs` and one actual EvalError-envelope test in `tikv/runtime_failure.rs`, plus documentation. Both modules stay private; only Failure/Class/Phase are public. Original LocalError, constructor/accessor and payload remain private. Clone/Eq retain capture identity and Debug excludes raw cause text/code.

The terminal arm uses only `MysqlError::unknown(failure.client_message())`: fixed1105/HY000, with the existing outer Eval-origin logic untouched. It does not classify by backend numeric status, phase, Debug, text, or a guessed SQL expression. Frontend/Pool/Scope/Bridge errors are not converted into fake LocalError values. No global caller, fold/default behavior or existing expected output was changed.

Parent also repaired seven pre-existing test-only TableResolver initializers in `tidb-executor/src/index_range/tests/prepared.rs`. The first actual executor test command failed compilation with seven E0063 errors and ran ZERO tests. Each repair only adds `clause_message: "expression"`, the documented generic resolver policy in catalog.rs:3023–3027. No assertion/data/residual/production logic changed. A independently reviewed all six Rust files and the fixture rationale; final source manifest matched6/6, no additional concrete source finding. Reviewers did not run builds/tests.

## Commands and actual results

All Cargo commands below ran from `/home/agent/tidb/expression-unification/tidb/rust` (Aug2026 pin, locked dependencies, separate TEST/DEV artifacts):

```sh
/home/agent/tidb/expression-unification/tools/cargo-tidb test --locked -p tidb-executor --lib driver::errors::exec:: -- --test-threads=1
/home/agent/tidb/expression-unification/tools/cargo-tidb test --locked -p tidb-executor --lib index_range::tests::prepared:: -- --test-threads=1
/home/agent/tidb/expression-unification/tools/cargo-tidb test --locked -p tidb-expr --lib tikv::runtime_failure::tests:: -- --test-threads=1
/home/agent/tidb/expression-unification/tools/cargo-tidb test --locked -p tidb-expr --lib -- --test-threads=1
/home/agent/tidb/expression-unification/tools/cargo-tidb check --locked -p tidb-session -p tidb-exec -p tidb-unistore --lib
```

- **Before-variant characterization:** temporarily disabled exactly the new enum variant, new renderer arm and new native-envelope unit test with three `#[cfg(any())]` attributes; native exports and two new origin tests remained. This was a renderer-before-variant cohort, not an untouched whole-worktree baseline. After the seven fixture repairs, actual renderer **7 passed/1 pre-existing failure/1468 filtered**, exit101. Prepared fixtures **7 passed/1469 filtered**, exit0.
- All three temporary gates were removed; source restoration checked against the five-file pre-characterization SHA manifest before pinned formatting. No disabled variant/arm/test survives. **After-variant renderer again7 passed/1 same failure**, prepared fixtures again7 passed. The complete renderer failure block matches after only thread-ID normalization and the known single temporary cfg source-line offset366→365. Its unchanged failure is `an_error_with_a_code_of_its_own_never_renders_an_empty_message`: sequence4135 message/state match, but old expected `from_evaluation:false` differs from actualtrue. Old expectation remains untouched.
- Actual native carrier filter **8 passed/1438 filtered**, exit0: seven existing carrier tests plus the real EvalError Clone/Eq/Debug/Send+Sync and retained-cause identity test. Its LocalError is a unit-test input, not a public production failure.
- Full expression comparison **1348 passed/4 unchanged failures/94 ignored**, exit101,1446 discovered. All four complete failure bodies match the preceding published checkpoint after normalizing ONLY thread IDs.
- Downstream session/old-exec/unistore library check **exit0**. This is a compile check, not their runtime tests or whole-workspace validation.
- Pinned Aug2026 `rustfmt --check --edition 2021 --config skip_children=true` on all six changed Rust files and `git diff --check` passed. Final hashes are in `logs/native-error-wiring-final-source.sha256`.

Raw first compile failure, before/after results and full check outputs are retained under `logs/native-error-*`.

## Current artifact allocation revalidation

The unchanged observer/runner were actually rerun against the newly built final TEST executable, not transferred from the old receipt:

```sh
# From /home/agent/tidb/expression-unification
python3 tools/pool-arc-runner.py --binary target-tidb/debug/build/tidb-expr/1a66296e036585b2/out/tidb_expr-1a66296e036585b2 --observer tools/pool-arc-observer.so --output logs/native-error-arc-final
```

Final ELF SHA: b5d49459c8f4a7486b462289b38e1336a2f44fcb0ffc3fd9869de6e676872cf4. All eight fresh samples actually passed one fixture and observed malloc192 plus matching final free, valid inherited-allocation controls, zero clone/empty/gap/foreign events,14 markers/16 events. The same-ELF missing-marker negative actually ran one normal test but was rejected with86. Current sources, reused unchanged C/SO/runner hashes and actually resolved dynamic libraries are bound in `logs/native-error-arc-final/{receipt,cohort}.json`; no observer recompilation is claimed this round. This remains only the exact pinned request basis, not ABI/usable/peak/factory-high-water/OOM proof.

## Explicit remaining limits

There is still no public producer of the new runtime error. The two renderer tests exercise existing generic Eval/Internal and Unknown-charset paths, NOT the new opaque arm end-to-end. Actual C4 failure capture with known phase, adapter-origin policy, Eval→Exec/Driver→wire, fold/default interaction and all public ASCII routes need the next integration. Do not add a public test-only constructor to fake that proof.

E's concrete scope/lifetime/catcher proposal is in `ascii-public-activation-next-cut.md`. C identified five next full-family candidates, not migrated code, in `next-five-expression-families.md`. Factory transient high-water, fixed differential150 rows, release performance, make lint/clippy and final whole-workspace/M6 acceptance remain open. No package-transcreation or PR-ready claim is made.

Agent-guide review: only architecture-index's existing runtime-failure entry was updated; no policy, precedence, test workflow, Bazel or PR rule changed. All referenced source paths and scoped commands were used above; no generated code or recorded fixtures changed.
