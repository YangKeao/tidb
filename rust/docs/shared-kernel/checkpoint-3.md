# Checkpoint 3: flat dynamic branches and demanded complex kernels

Parent pair: TiDBd52b7802e1 / TiKV4b492c320. This remains an opt-in, partial M1 candidate. It is not a default switch or a completed compatibility/performance claim.

The original RPN loop now partitions an integer condition by logical occurrence, evaluates demanded branches and joins at an explicit endpoint. A frame stores the parent selection and branch output positions; nested selections preserve duplicates. Native intermediate Real/Decimal vectors retain NaN payloads, hidden precision and declared shape. The adapter supplies input data only; it does not replay whole programs per row. Original protobuf still uses the same loop and keeps its existing eager predecode/IF and flagged lazy AND/OR contracts.

`RpnExpression::finish` checks forward branch regions, owned jumps, stack families at merges, literal carriers and exit family, then freezes structural mutation. This is structural validation; it does not certify arbitrary function metadata/signatures. The native builder still admits a narrow checked signature set. Mutating a finished control program is rejected; ordinary legacy protobuf builders retain their mutable straight-line API.

JSON_LENGTH over default UTF-8 strings lowers to the original SDK JSON cast and length kernels. REGEXP_LIKE uses the original typed SDK kernel with deferred native pattern preparation, so a guarded invalid constant pattern does not fail compilation. MD5 now rejects a numeric result declaration. No alternative native function walker or fallback was added.

## Evidence and limits

From tidb/rust, using ../../tools/cargo-tidb and --locked --offline:

- `test -p tidb-expr --features shared-kernel --lib shared_kernel -- --nocapture`:11passed. Includes nested partial selections,512-level control construction/evaluation/drop, NaN/wide Decimal joins, real invalid JSON/regexp branches, unselected bad JSON rows, malformed/frozen programs and earlier regressions. The freeze test catches its intentional mutation panic; the suite has no failing test.
- `test --manifest-path ../../tikv/Cargo.toml -p tidb_query_expr --lib types::expr_eval::tests -- --nocapture`:34passed. Original PB warning/short-circuit tests remain, plus eager IF with both flag settings.
- The new PB IF test also passed alone on original548812e source in the independent sdk-baseline-tests worktree, with only that test added. It observes unchosen unary-minus overflow with both flag settings.
- `check --manifest-path ../../tikv/Cargo.toml -p tidb_query_executors`:completed for the frozen checkpoint source.
- Scoped `clippy --manifest-path ../../tikv/Cargo.toml -p tidb_query_expr -p tidb_query_executors --lib`:completed with warnings. Full SDK `make clippy` fails in pre-clippy because rustup is absent from PATH; it is not passed or replaced by this scoped check.
- TiDB root `make lint` and both `git diff --check`:passed. Default compression8passed/2ignored remains checkpoint2 evidence; no dependency files changed here.

Raw logs remain in the implementer's home-gentoo evidence/shared-kernel-refactor directory: checkpoint3-native-final.log, checkpoint3-sdk-final.log, checkpoint3-executors-final.log, pb-if-original-baseline.log, checkpoint3-scoped-clippy-final.log, sdk-make-clippy.log and lint-dynamic.log. MD5's result-admission check was temporarily removed to reproduce the new failure (md5-admission-before.log), then restored before the final11-test run. Earlier dynamic builds failed due to a wrong command cwd, field-type conversion APIs and integer vector construction; the initial SDK test build also found its missing test-only import. All failed logs are retained; sdk-loop-tests.log built across edits and is not attributed to checkpoint2.

Remaining M1 gates include actual wide-value arithmetic, explicit Scalar versus width-one Vector demand, warning/error prefixes, multi-root ordering, RAND lifetime, user variables and BENCHMARK repetition. Dynamic batch selection alone does not establish those contracts. FLOAT/BIT coercion and broader string/result metadata remain unimplemented admissions. Native errors still use the checkpoint's string API; complete diagnostic adaptation is not established. No full native/SDK suites, SQL performance, whole-family completion or Bazel preparation success is claimed.

## Raw additions and deletions

Source counts include comments/blank lines. For this report, test hunks in the terminal native shared module and SDK expr_eval/impl_op test modules are separated. Earlier reports put small existing SDK test-match edits in the residual production bucket; total source and all-file counts are unaffected by this classification correction.

| Repository / basis | Production + / - | Tests + / - | Build + / - | Documents + / - |
| --- | ---: | ---: | ---: | ---: |
| TiDB / original | 562 / 0 | 684 / 0 | 1866 / 91 | 7854 / 0 |
| TiKV / original | 665 / 45 | 47 / 4 | 0 / 0 | 12 / 0 |
| TiDB / checkpoint2 | 117 / 27 | 327 / 0 | 0 / 0 | 41 / 0 |
| TiKV / checkpoint2 | 425 / 47 | 39 / 1 | 0 / 0 | 0 / 0 |
