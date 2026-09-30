# M0 baselines and integrated checkpoint receipts

2026-09-28. This is executed evidence, not a second plan. The authoritative living plan is `/home/agent/tidb/EXPRESSION_UNIFICATION_PLAN.md`.

## Fixed sources

- TiDB: `364aef2bab5cc633ecb76a775ae8f36f86a6687d`, `expression-unification/tidb`, branch `experiment/shared-expression-foundation`.
- TiKV: `548812e1ef57aef077a2062a9cc356640a6347f5`, `expression-unification/tikv`, same branch name in its independent repository.
- Both original algorithm baselines were captured before implementation release. TiKV was verified source-clean after its runs. TiDB had only a Bazel-generated BUILD file when its original tests ran; the four new LIKE tests were added afterward. No old expression-reuse worktree contents were changed.

## Environment

System `cargo`/`rustc` are stable 1.97.1; they are NOT the compiler used for the receipts below. The executable helpers `../tools/cargo-tidb` and `../tools/cargo-tikv` use the existing pinned toolchain binaries read-only:

- TiDB `nightly-2026-08-22`: rustc `1.100.0-nightly (c656540d6 2026-08-21)`.
- TiKV `nightly-2026-01-30`: rustc `1.95.0-nightly (842bd5be2 2026-01-29)`.

Both helpers set a new experiment-local Cargo home and separate `target-tidb` / `target-tikv`, with `CARGO_BUILD_JOBS=4`. Heavy Rust builds are serialized by the parent. Native TiKV dependencies required the following external environment compatibility settings; vendor and product source remained unchanged:

    CMAKE_POLICY_VERSION_MINIMUM=3.5
    CC=/usr/bin/gcc-14
    CXX=/usr/bin/g++-14
    CXXFLAGS=-include cstdint

The first TiKV attempt failed because installed CMake 4.3.4 rejects the old c-ares minimum policy. The documented external policy variable fixed configuration. Old Abseil then failed because its header omits `<cstdint>`; both GCC16 and installed GCC14 reproduced it. Supplying that standard header via CXXFLAGS fixed compilation. The compiler identities and each failed attempt are retained in `logs/tikv-collation-baseline*.log`. These are environment failures, not product test regressions.

## Successful preparation

`git fetch` and worktree creation completed with exact approved SHAs. Both `cargo metadata --locked --no-deps --format-version 1` and both `cargo fetch --locked` commands succeeded, with outputs under `evidence/*metadata-baseline.json` and `logs/*cargo-fetch-baseline.log`.

In the TiDB repository root, this fresh-workspace prerequisite passed:

    env PATH="/home/agent/tidb/expression-unification/tools:$PATH" \
      GOCACHE=/home/agent/tidb/expression-unification/go-cache \
      GOPATH=/home/agent/tidb/expression-unification/go-path \
      make bazel_prepare

It emitted a nonfatal Gazelle warning about the local parser replacement and produced one untracked generated artifact, `rust/third_party/tikv-client-rs/tests/client_go_differential/BUILD.bazel`; that required generated file is retained. No generated fixture was rerecorded.

## Original TiDB test results

Cwd: `/home/agent/tidb/expression-unification/tidb/rust`. Replace `cargo-tidb` below with `/home/agent/tidb/expression-unification/tools/cargo-tidb`.

| Exact arguments | Observed result | Log under `expression-unification/logs/` |
| --- | --- | --- |
| `cargo-tidb test --locked -p tidb-datatype --lib collation -- --test-threads=1` | 25 passed, 402 filtered, exit 0 | `tidb-datatype-collation-baseline.log` |
| `cargo-tidb test --locked -p tidb-datatype --test all collation_sort_keys_match_go_byte_for_byte -- --test-threads=1` | 1 passed, 66 filtered, exit 0 | `tidb-go-collation-baseline.log` |
| `cargo-tidb test --locked -p tidb-expr --lib -- --test-threads=1` | 1323 discovered: 1226 passed, 4 failed, 93 ignored; exit 101 | `tidb-expr-lib-baseline.log` |

The seven PB-typed decoder/signature/wire-type/signedness/JSON reuse/laziness/binary-vs-UTF8 tests all passed in the original expression run. This does not imply that the whole expression library passed.

All four original failures reproduced individually using:

    cargo-tidb test --locked -p tidb-expr --lib <full_test_name> -- --exact --test-threads=1

| Full test name | Failure |
| --- | --- |
| `pushdown_catalog::tests::ifnull_string_column_literal_uses_go_signature_and_column_collation` | Reverse literal/column build_call returned Some where test expects None. |
| `tests::builtin_info_json_math_source::exp` | EXP(100000.0) did not return the expected FloatOverflow variant. |
| `tests::builtin_math_misc_op_source::vectorized_builtin_op_func` | Duration-to-number panicked at negative FSP conversion. |
| `time_fn::tests::str_to_date_partial_formats_follow_no_zero_date` | STR_TO_DATE("01", "%d") returned NULL instead of "0000-00-01" under relaxed NO_ZERO_DATE. |

The authoritative isolated rerun is `tidb-expr-baseline-isolated-failures-v2.log`: four single-test failures, each exit 101. The first loop logged the same failed tests but accidentally returned shell status 0 due to pipeline/subshell aggregation; the wrapper was corrected and all four were rerun. Neither log is a green test receipt.

## Original TiKV test results

Cwd: `/home/agent/tidb/expression-unification/tikv`. Replace `cargo-tikv` with `/home/agent/tidb/expression-unification/tools/cargo-tikv`.

| Exact arguments | Observed result | Log |
| --- | --- | --- |
| `cargo-tikv test --locked -p tidb_query_datatype --lib codec::collation:: -- --test-threads=1` | 8 passed, 294 filtered, exit 0 | `tikv-collation-baseline-native-compat.log` |
| `cargo-tikv test --locked -p tidb_query_expr --lib -- --test-threads=1` | 428 passed, 0 failed/ignored, exit 0 | `tikv-expr-baseline.log` |
| `cargo-tikv test --locked -p tidb_query_datatype --lib codec::mysql::decimal::tests:: -- --test-threads=1` | 26 passed, 276 filtered, exit 0 | `tikv-decimal-baseline.log` |

All filters executed nonzero tests. `git status --short` and `git diff --check` were clean after the original TiKV tests and before A/B/C implementation release.

## Added LIKE regressions: intentional red phase

Foundation A added only four tests in TiDB `rust/crates/tidb-expr/src/like.rs`, leaving production algorithms unchanged. Parent ran:

    cargo-tidb test --locked -p tidb-expr --lib shared_like_regression -- --test-threads=1 --nocapture

Result: 0 passed, 4 failed, 1323 filtered, exit 101; `logs/tidb-like-regression-red.log`. The 22 table rows check cached and uncached paths. Failures demonstrate escaped wildcard tail, UCA literal-space equivalence, GBK binary rune identity, and GB18030 binary byte-unit inconsistencies. These are planned corrections, not preexisting red tests included in the 1323-test baseline. Their green phase is pending the shared implementation.

## Additional original TiDB Decimal evidence

To avoid rebuilding a moving implementation tree, the parent invoked the original pre-integration datatype test executable recorded by the original collation build:

    /home/agent/tidb/expression-unification/target-tidb/debug/build/tidb-datatype/070ff0bc03cebfb9/out/tidb_datatype-070ff0bc03cebfb9

Its SHA-256 was `24c6877d8c30c7916fd493858803ccc93f816d4b46bb20879344bf2cd92e3314`. Running `decimal_tests:: --test-threads=1` discovered 58 tests but hit the harness's 20-second timeout at `decimal_tests::test_from_string_my_decimal`, after 35 test completions. This is **not a complete pass or an assertion failure**. Source inspection found the existing exponent cases `1e1073741823` and `-1e1073741823`; additional characterization is needed rather than calling this a migration regression. Log: `tidb-decimal-original-binary-baseline.log`.

The same original executable with `RUST_MIN_STACK=33554432` and `decimal_tests::test_mul_my_decimal --exact --test-threads=1` passed one test (426 filtered), including the existing `0.000 * -1 => 0.000` behavior. Log: `tidb-decimal-mul-original-binary-baseline.log`.

To isolate the remaining baseline, `RUST_MIN_STACK=33554432 <same-original-executable> decimal_tests:: --skip decimal_tests::test_from_string_my_decimal --test-threads=1` passed 57 tests (370 filtered) in 4.43 seconds. Log: `tidb-decimal-original-binary-except-slow-parser.log`. The skip is explicit; it does not turn the previously timed-out 58th test into a pass.

## First implementation checkpoint, not final acceptance

After source-clean native baselines, Foundation A's shared key/charset/pattern implementation compiled and its `cargo-tikv test --locked -p tidb_query_datatype --lib codec::collation:: -- --test-threads=1` filter passed 12 tests (295 filtered), including four new pattern tests. Log: `tikv-collation-shared-first-checkpoint.log`. This was only the early TiKV datatype checkpoint; the TiDB facade, full key API matrix, expr LIKE wrapper and complete consumer tests were not yet accepted.

Before any Decimal arithmetic fix, Foundation B added `codec::mysql::decimal::tests::test_mul_zero_preserves_result_scale`. Parent ran:

    cargo-tikv test --locked -p tidb_query_datatype --lib codec::mysql::decimal::tests::test_mul_zero_preserves_result_scale -- --exact --test-threads=1 --nocapture

It failed exactly as predicted: actual `"0"`, expected `"0.000"`; 0 passed, 1 failed, 306 filtered, exit 101. Log: `tikv-decimal-zero-scale-red.log`. The narrowly scoped successful-zero sign/scale fix was released after that RED, while preserving overflow negative-zero behavior; green verification is pending.

## Cross-workspace dependency integration status

The independent `probes/tikv-dependency` initial lock resolution succeeded but chose different protocol revisions and registry protobuf, demonstrating that TiKV's patches/lock are not inherited. It is a diagnostic artifact, not an implemented evaluator or compatibility pass.

Parent then added adjacent TiKV dependencies to TiDB datatype/expr, mirrored only the seven reached root crate patches, and let Cargo minimally extend the existing TiDB lock (not hand-editing or globally regenerating it). Eight precise update steps pinned kvproto, tipb, yatp, raft/raft-proto, protobuf/protobuf-codegen, fs2, cmake and sysinfo to the reviewed TiKV baseline commits; the final `cargo metadata --locked --format-version 1` succeeded. The 15 Git source entries were inspected and match the corresponding original TiKV lock sources/revisions. Evidence: `logs/tidb-shared-dependency-pins.log`, `evidence/tidb-metadata-shared-pinned.json`, `evidence/tikv-expression-dependency-tree.txt`.

A subsequent `cargo-tidb check --locked -p tipb` passed on the TiDB pinned compiler (job `bash-32`, `logs/tidb-toolchain-protocol-check.log`). This proves the pinned protocol dependency builds under that caller, not the complete shared evaluator.

TiDB's `flate2` resolved from 1.1.9 to TiKV's exact 1.0.11 requirement; this is a real compatibility/performance risk. Metadata success alone did not establish runtime compatibility. The subsequently executed integration receipts below cover the first merged slice and crypto/compression cases; they do not complete migration, lint or performance acceptance.

## First integrated source checkpoint — executed

The first table gives reproducible module filters for the observed runs; a trailing `::` narrows to the same module and is not a claim about the spelling of an earlier broader filter. Commands use `test --locked` and append `-- --test-threads=1`, unless explicitly marked exact. Native KV cwd is `/home/agent/tidb/expression-unification/tikv`, using `../tools/cargo-tikv`. DB cwd is `/home/agent/tidb/expression-unification/tidb/rust`, using `../../tools/cargo-tidb`. Each row below completed with exit 0 and a nonzero matched count.

| Side / arguments after `test --locked` | Passed / ignored / filtered | Log under `logs/` |
| --- | --- | --- |
| KV `-p tidb_query_datatype --lib codec::collation::` | 17 / 0 / 301 | `tikv-collation-shared-key-contracts.log` |
| KV `-p tidb_query_datatype --lib codec::mysql::decimal::tests::` | 33 / 0 / 285 | `tikv-decimal-parts-green-v3.log` |
| KV `-p tidb_query_codegen --lib` | 20 / 0 / 0 | `tikv-codegen-shared-call-checkpoint.log` |
| KV `-p tidb_query_expr --lib` | 438 / 0 / 0 (original 428 + local 10) | `tikv-expr-local-first-checkpoint-v3.log` |
| DB `-p tidb-datatype --test all tikv_value_bridge_source::` | 11 / 0 / 76 | `tidb-value-bridge-first-checkpoint.log` |
| DB `-p tidb-datatype --lib collation` | 25 / 0 / 403 | `tidb-collation-shared-first-checkpoint.log` |
| DB `-p tidb-datatype --test all shared_collation_contract::` | 9 / 0 / 78 | `tidb-shared-collation-contracts.log` |
| DB `-p tidb-datatype --test all collation_sort_keys_match_go_byte_for_byte` | 1 / 0 / 86 | `tidb-shared-collation-go-fixture.log` |
| DB `-p tidb-expr --lib like::tests::` | 11 / 0 / 1316 | `tidb-like-shared-first-checkpoint.log` |
| DB `-p tidb-util --lib stringutil::` | 13 / 0 / 564 | `tidb-stringutil-shared-first-checkpoint-v2.log` |
| DB `-p tidb-expr --lib like_pattern_cache_reuses_only_within_context` | 1 / 0 / 1326 | `tidb-like-shared-cache-scope.log` |
| DB `-p tidb-datatype --lib binary_json_ops::` | 18 / 0 / 410 | `tidb-json-like-shared-policy.log` |
| DB `-p tidb-expr --lib tests::crypto_encryption_source::` | 16 / 3 / 1308 | `tidb-shared-flate2-crypto-compatibility.log` |

The four prior LIKE regressions passed without changing their expectations. The original Go key fixture and generator were not rerecorded. Three preexisting encryption tests remain explicit ignored harness/memory/vector gaps, not passing coverage. First local RPN admission remains signed Int; the bridge does not yet admit Decimal/temporal/JSON. No family is claimed complete from these slices.

### Investigated failures and intentional correction

- The first merged Decimal run was **32 passed / 1 failed**, exit 101: original `test_mul` expected stored string `"0"` for `0 * -1.1`, while the scale-preserving change produced `"0.0"`. All seven new tests passed. This was a real old-contract change, not an ignored failure.
- The parent read pinned Go `pkg/types/mydecimal.go` (zero helper 117–121, result scale 2068, negative-zero normalization 2136–2147), then executed an independent public-Go-API oracle, not a reimplementation. From the TiDB repository root:

      env GOCACHE=/home/agent/tidb/expression-unification/go-cache \
        GOPATH=/home/agent/tidb/expression-unification/go-path GOMAXPROCS=4 \
        go run -p 4 /home/agent/tidb/expression-unification/probes/decimal-zero/main.go

  Go 1.26.5-X:nodwarf5, exit 0. `0 * -1.1` and its reversal both returned String/ToString `0.0`, stored fraction 1; `0.000 * -1` and reversal returned `0.000`, stored fraction 3. Evidence: `go-decimal-zero-oracle.log`. Only that one old TiKV expected value was then changed, with provenance. This is an explicit shared-kernel correction, **not** blanket TiKV baseline equivalence. Hidden storage/result-scale and truncated-negative-zero policies remain separate unresolved domains.
- Codegen passed 20 tests immediately. First expression compilation failed at two `impl_time.rs` closures with six E0282/E0283 inference diagnostics; explicit error-return control flow corrected these, and the full 438-test retry passed. The failed log remains `tikv-expr-local-first-checkpoint.log`.
- `bash-37` was intentionally cancelled: native commands had been combined with `--manifest-path` from the caller cwd, which would inherit TiDB's Cargo configuration. No result from that attempt is accepted; both workspaces were rerun from their own cwd.
- The first utility command selected nonexistent `tidb-util --test all`, exit 101. Reading its manifest established inline unit tests; `--lib stringutil::` then ran 13 passing tests. This was command selection, not a product test failure.

## Generated-image deletion and consumer gates

The generator now emits only retained GB images, while mandatorily validating shared General/UCA sources. The authorized `--prune-obsolete` operation verified every candidate before deletion and removed exactly five unmodified images, **2,128,092 bytes**. Retained GB images stayed byte-identical to HEAD. The normal `--check` now rejects reintroduced obsolete files, including dangling symlinks. Parent inspected the guarded deletion code and independently ran, from the TiDB root:

    python3 rust/crates/tidb-datatype/scripts/generate_collation_data.py --check
    git diff --check
    git -C /home/agent/tidb/expression-unification/tikv diff --check

All exited 0. Source verification covered General 65,536 entries, UCA0400 65,536 + 22 long expansions, UCA0900 183,969 + 27 long expansions, source-pinned hashes/planes/Hangul/implicit bounds, the 2,048 non-scalar surrogate slots and both retained GB tables. The existing U+2CEA1 strict-boundary behavior was not silently fixed. A's additional 16 cleanup-safety tests are agent-reported; the parent has not independently rerun those mocked tests.

Post-prune consumer commands use the DB cwd/helper above, append `-- --test-threads=1`, and all exited 0. Rows marked exact add `--exact` to the test-harness arguments. These **61** matched tests are not full-package runs.

| Arguments after `test --locked` | Passed / filtered | Log |
| --- | --- | --- |
| `-p tidb-datatype --lib collation` | 25 / 403 | `tidb-collation-post-prune.log` |
| `-p tidb-datatype --lib enum_set_tests::` | 4 / 424 | `tidb-enum-set-shared-collation.log` |
| `-p tidb-codec --lib collation` | 4 / 42 | `tidb-codec-collation-lib.log` |
| `-p tidb-codec --test all collation_keys::` | 1 / 166 | `tidb-codec-collation-keys.log` |
| `-p tidb-codec --test all runtime_collation_mode_source::operational_keys_follow_exact_name_and_process_mode` (exact) | 1 / 166 | `tidb-codec-collation-mode.log` |
| `-p tidb-codec --test all codec_package_source::source_enum_set_hash_modes` (exact) | 1 / 166 | `tidb-codec-enum-set-hash.log` |
| `-p tidb-executor --test all index_entry_go_bytes::` | 6 / 522 | `tidb-executor-collation-index-bytes.log` |
| `-p tidb-planner --lib ranger::points::tests::like_prefix_builds_the_increment_range` (exact) | 1 / 1053 | `tidb-planner-collation-like-range.log` |
| `-p tidb-session --lib tests_collation::` | 13 / 2065 | `tidb-session-shared-collation.log` |
| `-p tidb-session --lib tests_partition_prune_collation::` | 3 / 2075 | `tidb-session-shared-collation-prune.log` |
| `-p tidb-stats --test all sample_collector_source::source_sample_builder_collator_gate_and_index_order_match` (exact) | 1 / 274 | `tidb-stats-shared-collation.log` |
| `-p tidb-unistore --lib like_follows_collation_case_sensitivity` | 1 / 181 | `tidb-unistore-shared-like.log` |

GB encoded-key/compare compatibility, charset transcoding, and two non-expression security/host wildcard loops remain explicit residuals; this is not repository-wide wildcard or collation unification. Full SQL evaluator activation, full type unification, final coverage, complete lint/clippy, allocation/performance measurements and all-workspace acceptance remain unverified. The frozen denominator stays 245, the complete-family target stays 221, and current complete-family count remains zero.

## B2.0 owned-NULL safety prerequisite — RED then full GREEN

From the native TiKV cwd, the parent executed these exact test commands using the pinned helper:

    ../tools/cargo-tikv test --locked -p tidb_query_datatype --lib codec::data_type::chunked_vec_sized::tests::test_push_null_uses_default_payload -- --exact --test-threads=1
    ../tools/cargo-tikv test --locked -p tidb_query_datatype --lib -- --test-threads=1

The first ran before the production change: **0 passed / 1 failed / 318 filtered, exit 101**, in `tikv-null-default-safety-red.log`. The test uses only a `u64` marker (all bit patterns are valid), with Default = 7. Old generic zero-initialization safely produced marker 0; the NULL bitmap/get checks passed before the 0-vs-7 assertion. No invalid owning/NonZero value was constructed to obtain RED.

E then changed NULL backing to initialized `T::default()`, added Default only to owned Evaluable, and supplied Duration Default. B supplied valid Decimal Default. Existing `set` semantics were retained. The full follow-up ran **324 passed / 0 failed / 0 ignored / 0 filtered, exit 0** (`tikv-null-default-safety-full-green.log`): it includes the marker GREEN, owned Vec/Box clone/append/truncate/drop and replacement tests, selected/repeated NULL physical-cell and datum-byte goldens, and Decimal's valid default-zero test. A non-NULL default Decimal still has integer count 1; bitmap-selected NULL remains a 40-zero-byte cell/NIL encoding. Hidden NULL payloads can own allocations and must be dropped/accounted normally.

Both pinned parent-root formatter checks passed after three small wiring formatting fixes. E's five safety files passed pinned native rustfmt after the parent only rewrapped their header documentation; the datatype-scoped `git diff --check` passed. An intermediate global whitespace check reported C's in-progress expression root EOF blank line; it was sent to C and is not a final repository-quality pass. The new architecture-map paths were checked to exist, deleted image names had no TiDB Markdown references, and the agents-review guide was reviewed; no policy was added to the navigation index.

## B2.1 dependency prerequisite — no lock drift

Parent added only `smallvec = "1.4"` to `tikv/components/tidb_query_datatype/Cargo.toml`. In each repo's own Cargo cwd, its pinned helper ran `metadata --offline --format-version 1`, then `metadata --locked --offline --format-version 1`; all exited 0. Before/after TOML was normalized **only** by removing the target `tidb_query_datatype` package's `smallvec` dependency edge, then JSON-sorted and SHA256 compared. Entire normalized locks remained identical:

- Native: `5860e233b733017666f5317f1d0ffd5ce62b435e162f32ad9600d515d402e23f`.
- Caller: `bbe7611c82e78154cb543a85dec8d93580592872ee323b5b782729deb7cd677b`.

Thus no package version/source/checksum/other dependency changed. Native Git diff showed exactly one manifest and one lock dependency line; caller gained the corresponding edge while retaining all previous exact pins. This is dependency readiness, not SmallVec-core compilation.

## Next integration preparation — source checks, not compile acceptance

E's twelve external Decimal caller files are now stable: 14 production clones, nine test/diagnostic clones, checked legacy u8 shape boundaries, and widened comparison/FSP-clamping sites. The existing SQL overflow warning/clamp and rounding policy remains in place; metadata narrowing is not reclassified as SQL overflow. Parent's first pinned rustfmt check found three wrapping-only differences (`datum_codec.rs`, `mysql/time/interval.rs`, `impl_cast.rs`); E corrected only those locations, and the parent independently reran the pinned `--edition 2021 --config skip_children=true --check` over all twelve files plus their scoped `git diff --check`: both exit 0. Exact source hashes and complete command/file list are in `decimal-null-safety.md`. This is not proof that the non-Copy core compiles.

From the TiDB worktree root, the parent reran exactly `python3 -B /home/agent/tidb/expression-unification/evidence/collect-coverage-baseline.py --check` and `python3 rust/crates/tidb-datatype/scripts/generate_collation_data.py --check`: both exit 0. The frozen denominator is still 245/target221, full migrations remain zero, and all five obsolete images remain absent. The source checker independently matched General/UCA/GB tables and retained the documented boundary/non-scalar differences.

C2a's actual public input-service API is frozen, permitting parallel D1 implementation without any public evaluator activation. Parent added only private `mod tikv;` to the expression crate and the exact shared `protobuf = "=2.8.0"` workspace/consumer dependency for generated `ProtobufEnum::from_i32`. The one already-existing root module-order formatter difference was corrected; pinned TiDB root rustfmt and wiring-scoped diff checks passed. Caller metadata, then locked offline metadata, passed from its own Cargo cwd. Entire normalized lock SHA256 before/after (excluding only the `tidb-expr` protobuf edge) was identical: `80e6128a674bc2b8bb174f99df77d7533ea06dd299a816f03bb2a28591e63d33`. No other dependency/version/source changed. Parent also exported the now-declared `DecimalWordsRef`; native pinned module formatting passed.

B2.1 core, C2a runtime, and D1 private control seed still await stable compiled gates. The parent additionally identified a physical-domain constraint for B: existing checked TiDB MyDecimal raw import accepts result scale up to 127 independently of stored scale, and lacks partial-word padding checks; TiKV's former raw-copy codec admitted arbitrary u8 result scale with a valid bool. Strict logical parts and physical byte transport must not silently share narrower admission. B subsequently preserved the empty/noncanonical shapes and all 0..255 result-header bytes in the explicit physical adapter; invalid bool, overcapacity count and out-of-base active-word domains remain explicit refusals. Six focused B2.1 tests include the physical distinctions; this is not full MyDecimal/raw-like-Go compatibility.

## B2.1 compiled datatype checkpoint and downstream repair queue

All native commands run from the TiKV root through `../tools/cargo-tikv test --locked`, with `-- --test-threads=1`:

- `-p tidb_query_datatype --lib`: first compile failed with one E0382 in existing `codec/row/v2/compat_v1.rs::tests::test_decimal`, because `Column::new` consumed the non-Copy fixture before its assertion. Parent added only `value.clone()`; pinned format/diff checks passed. Retry: **330 passed, zero failed/ignored/filtered** (`tikv-b21-datatype-full-v2.log`), including **40 Decimal tests** and all preceding ownership/NULL-byte tests.
- `-p tidb_query_codegen --lib`: **20 passed, zero failed/ignored/filtered** (`tikv-b21-codegen-full.log`).
- `-p tidb_query_expr --lib`: first compile failed with eight diagnosed ownership errors (`tikv-c2a-expr-full.log`), not a passing/runtime gate. Two arithmetic fixture clones, two unary-op fixture clones, two FROM_UNIXTIME operand clones, and two inner-Decimal clones in UUID timestamp were assigned to E/C. UUID's existing status policy was preserved; cloning Res itself would not fix the consuming dereference. The source census was incomplete: there are at least 19 external production Copy dependencies across ten files, not a proven final 17-file footprint. Test-only discovered files are recorded separately. E/C returned scoped formatting/diff passes. Retry `-p tidb_query_expr --lib`: **462 passed, zero failed/ignored/filtered**, including all preceding438 plus24 new C2a tests (`tikv-c2a-expr-full-v2.log`, 2.64 s). Following `-p tidb_query_aggr --lib`: **40 passed, zero failed/ignored/filtered** (`tikv-b21-aggr-full.log`). The datatype/codegen/expr/aggr queue is accepted for B2.1/C2a, not wide arithmetic or staged hosts.

Independently, while expression repair files were not build inputs, the caller datatype gates ran from `tidb/rust` with `../../tools/cargo-tidb test --locked` and the same thread option:

| Arguments | Pass | Log |
|---|---:|---|
| `-p tidb-datatype --test all tikv_value_bridge_source` | 11 | `tidb-b21-value-bridge.log` |
| `-p tidb-datatype --lib collation` | 25 | `tidb-b21-collation.log` |
| `-p tidb-datatype --test all shared_collation_contract` | 9 | `tidb-b21-shared-collation.log` |
| `-p tidb-datatype --test all collation_sort_keys_match_go_byte_for_byte` | 1 | `tidb-b21-go-collation.log` |

All four exited 0 with nonempty filters. These verify the existing primitive bridge/collation against the owning core under the caller compiler; they do not activate Decimal transport, wide arithmetic or D1.

Additional source finding for the old timeout: `decimal_tests.rs`'s `1e1073741823` reaches the equality edge of `decimal/mod.rs`'s exponent check. `shift_mysql_with_word_limit` appends 1,073,741,823 zeros before checking the nine-word integer limit. This structurally entails a roughly 1 GiB expansion for a bounded-overflow fixture and is a source-based explanation candidate for the observed timeout, not a profiler attribution. The future shared Fixed worker must check capacity before expansion without changing the source oracle.

## D1 explicit caller seed — accepted checkpoint, not global activation

From `tidb/rust`, exact command:

```sh
../../tools/cargo-tidb test --locked -p tidb-expr --lib tikv::tests:: -- --test-threads=1
```

First compile failed only in the new fixture source: missing `ColumnResolver::time_zone` and two calls assuming an incorrect return-buffer `encode_int` API. D checked existing definitions, added explicit UTC like the existing resolver fixture, and used `encode_int(&mut Vec<u8>, value)`. No production/default behavior or expected values changed. Retry: **15 passed, zero failed/ignored, 1327 filtered**, exit0 (`tidb-d1-control-seed-v2.log`). This verifies explicit signed-LongLong closure admission, PB identity/presence and stale-origin checks, physical selections, poison/demand/error/budget behavior, worker ownership and deep construction for this seed.

The broader comparison was necessary because D1 adds metadata fields to Constant/Column/ScalarFunction and factors shared pushdown facts:

```sh
../../tools/cargo-tidb test --locked -p tidb-expr --lib -- --test-threads=1
```

**1342 discovered: 1245 passed, 4 failed, 93 ignored, zero filtered; exit101**, log `tidb-d1-expr-full-comparison.log`. The four failure names and panic contents match the M0 baseline above: reverse IFNULL pushdown, EXP FloatOverflow, negative duration FSP and partial STR_TO_DATE. The nineteen additional passes equal four LIKE regressions plus fifteen seed tests. No new failure was observed; this is not a green full-library/lint receipt and the93 ignored tests remain unverified.

The sole plan's live ledger now releases B2.2 shared Grow/Fixed workers and C2b staged hosts. D1 product sources are frozen; D2 is refining structural preparation APIs without product edits. Complete-family counts remain **0/245**, target at least221. Maintenance mapping now describes the actual owning Decimal, physical-vs-logical transport, shared frame driver and explicit caller boundary; no Go package transcreation or full M2/M3/M4 claim is made.

Parent independently checked the compiler-fix hashes reported by E (all three matched), pinned rustfmt on the three files plus unary-op/row-v1 fixture/export, and their scoped diff including the TiKV guide: exit0. From the TiKV root, exact format command:

```sh
/home/agent/tidb/expression-reuse/rustup-home/toolchains/nightly-2026-01-30-x86_64-unknown-linux-gnu/bin/rustfmt --edition 2021 --config skip_children=true --check components/tidb_query_expr/src/impl_arithmetic.rs components/tidb_query_expr/src/impl_time.rs components/tidb_query_expr/src/impl_miscellaneous.rs components/tidb_query_expr/src/impl_op.rs components/tidb_query_datatype/src/codec/row/v2/compat_v1.rs components/tidb_query_datatype/src/codec/mysql/mod.rs
```

The TiDB `docs/agents/agents-review-guide.md` checklist was applied to `architecture-index.md`: the change is subsystem/test navigation only, adds no normative policy, does not alter Bazel/PR/RealTiKV gates and explicitly scopes Cargo's cwd to `rust/`. A Python `Path.exists()` assertion checked all eleven referenced shared-expression paths; output `Architecture shared-expression mapping: 11 referenced paths exist`. The parent also ran `git diff --check -- rust/Cargo.toml rust/Cargo.lock rust/crates/tidb-expr/Cargo.toml rust/crates/tidb-expr/src/lib.rs docs/agents/architecture-index.md` and the absolute coverage-baseline checker from the TiDB root, both exit0. The pushdown catalog's unrelated preexisting line-wrap formatter residual is still documented by D; broad formatter/lint/clippy/performance/full workspace gates are not claimed.

## Follow-up source review and first private Grow checkpoint

Independent reviewer E found a real **P2 immutable-metadata gap**, not a demonstrated wrong Int result: shared `PbOrigin.effective_type` exposes a `FieldType` whose cloned elements still share mutable Go-like backing. A consumer can mutate published origin metadata and poison re-lowering of an otherwise unchanged bound tree. The admitted signed-LongLong+elements domain is already tested. E added only `tikv::tests::pb_origin_metadata_cannot_mutate_a_published_spec_or_relowering`; existing15 tests are byte-unchanged. At the B2.2-A checkpoint this sixteenth test was **pending RED execution**, not reported failed/passed. The subsequent joint cohort below actually reproduced and fixed it before claiming GREEN. Proposed fix is private origin storage plus a comparison predicate and detached inspection getter; a publication-only copy is insufficient. D2 edits other files and does not fix/widen D1 implicitly.

B2.2-A reached an independent coherent core checkpoint while C2b/D2 were moving. From the TiKV root:

```sh
../tools/cargo-tikv test --locked -p tidb_query_datatype --lib -- --test-threads=1
../tools/cargo-tikv test --locked -p tidb_query_datatype --lib codec::mysql::decimal::tests::test_legacy_boundary_characterization -- --exact --nocapture --test-threads=1
```

**335 passed, zero failed/ignored/filtered** (`tikv-b22a-datatype-full.log`), including five new private Grow add/sub/mul/alignment/comparison/ownership/limit tests; the focused characterization separately ran **1 passed/334 filtered** (`tikv-b22a-legacy-boundary.log`). General import/exact public methods remain closed. Division, AVG, rounding, parser, general result formatter and caller facades are not claimed ready.

### Independent Decimal boundary evidence

`probes/decimal-boundaries/main.go` calls only the pinned Go implementation. From the TiDB root:

```sh
gofmt -w /home/agent/tidb/expression-unification/probes/decimal-boundaries/main.go
env GOCACHE=/home/agent/tidb/expression-unification/go-cache GOPATH=/home/agent/tidb/expression-unification/go-path GOMAXPROCS=4 go run -p 4 /home/agent/tidb/expression-unification/probes/decimal-boundaries/main.go
```

Exit0 means characterization completed, **not that recovered panics passed**. `go-decimal-boundaries-oracle.log` preserves the observations:

- `(-1e60) * 1e60`: Overflow, headers81/0/0, negative zero, String/ToString `-0`.
- `(-1e60 - 0.1) * (1e60 + 0.1)`: Overflow, headers81/2/2, nine zero words; **both String and ToString panic at index9/len9**, caught and labeled by the probe.
- Extreme DIV fixture from the retained TiKV test: Overflow with capped quotient, Go headers78/0/5.
- Extreme MOD: Ok, headers0/60/60, magnitude `0.000000000000000000000000000000000000000000010939552551501580`; negative dividend gives its negative.

`probes/decimal-boundaries/main.rs` was separately linked against the **already validated B2.1** native rlib, avoiding any rebuild of moving product files. The initial `4f51481c3f3e546e` artifact attempt failed compilation because it was older and had no `words()` API; actual timestamps identified the correct `c654d7c6d7b1484d` artifact. Its SHA256 was `03bbeb421605e826e0ff326cfc5d5e5f097d58ed65c9d4b4c999ddb3ce60973a`. From the experiment root:

```sh
/home/agent/tidb/expression-reuse/rustup-home/toolchains/nightly-2026-01-30-x86_64-unknown-linux-gnu/bin/rustc --edition 2021 probes/decimal-boundaries/main.rs --extern tidb_query_datatype=target-tikv/debug/deps/libtidb_query_datatype-c654d7c6d7b1484d.rlib -L dependency=target-tikv/debug/deps -o probes/decimal-boundaries/tikv-b21-probe
probes/decimal-boundaries/tikv-b21-probe
```

Exit0; the rlib must match the recorded SHA before reproducing this observation: Cargo can overwrite that artifact path during later checkpoints, so a newly built rlib is not automatically B2.1. The retained probe executable and before-log belong to the observed build. `tikv-b21-decimal-boundaries-oracle.log` records fractional Overflow Display `0`, integer Overflow `-0`, extreme DIV Overflow with headers81/0/5, and extreme MOD **Ok** with the old TiKV stored remainder `0.000000000000000000000000000000000003564345362392880000000000`. Its legacy Display further rounds that remainder to thirty-place zero. The new B2.2-A characterization independently retained the exact old storage string and status. Neither the capped quotient nor the larger remainder was silently corrected.

An independent Python `fractions.Fraction` calculation (no binary float) verified quotient-times-divisor-plus-remainder and strict remainder bounds for both signs. The full positive quotient is `312723662343590746587750435944686855597018456899102054479447138416084646758822877655408325148828`; its remainder matches Go, not the old TiKV fixed MOD fixture.

The sole plan explicitly approves the forthcoming general-formatter policy for overcapacity Overflow zero (81/2/2 example `0` → `-0.00`) without changing payload/status/SQL warning policy. This is a named safety/domain-extension change, **not Go/legacy string equivalence**; its implementation/tests remain pending. Fixed MOD behavior remains a documented legacy discrepancy and its correction is not approved. Grow must use correct exact quotient iteration through the same worker with an explicit policy, not a second arithmetic engine.

## C2b / D2 / immutable PB-origin joint acceptance

All product writers froze before serialized builds; B confirmed that `decimal.rs` was byte-unchanged from accepted B2.2-A. C's two helpers (`b7c8ccdf…`, `91e4e4af…`) handed back their exclusive files before C finalized nine Rust files. Parent independently reproduced the ordered source-manifest SHA `fde1f0f6695a9abb461912fd296144c1fdb92f680004c55eece3baa841455f4d` and scoped `git diff --check`, exit0.

From the TiKV root:

```sh
../tools/cargo-tikv test --locked -p tidb_query_expr --lib -- --test-threads=1
../tools/cargo-tikv test --locked -p tidb_query_aggr --lib -- --test-threads=1
```

- `tikv-c2b-expr-full.log`: **499 passed**, no failures/ignored/filtered (37 new host tests), 2.63s.
- `tikv-c2b-aggr-full.log`: **40 passed**, no failures/ignored/filtered.
- Parent inspected `local/host.rs` and the shared frame loop/task guard: typed catalog identity, callback-scoped reborrows, pending-token registration, cancellation order, output validation, Fresh/Reuse and retained-storage checks. These do not prove actual SQL host migration, total host allocation bounds, external cancellation, or generic ordinary-call profiles.

From the TiDB Rust root, the first provenance regression command failed **compilation**, not RED: `tidb-d1-pb-origin-alias-red.log` recorded two E0061 errors in BETWEEN's `let compare = binary_expression` alias. D added the missing explicit purpose to the two calls, preserving order and admission. The retry actually ran one test and failed at the unchanged tuple oracle: **(changed metadata, false re-lowering) versus (original metadata, true)** (`tidb-d1-pb-origin-alias-red-v2.log:1993–2003`).

Only after that RED, E made the effective type private, added a boolean comparison predicate plus test-only detached snapshot getter, and migrated two test getter expressions. Fixtures, mutation and all assertions stayed unchanged; no second DTO or publication-only workaround. Source/schema metadata assertions had already passed before the original final failure.

```sh
../../tools/cargo-tidb test --locked -p tidb-expr --lib tikv::tests::pb_origin_metadata_cannot_mutate_a_published_spec_or_relowering -- --exact --test-threads=1
../../tools/cargo-tidb test --locked -p tidb-expr --lib tikv::tests:: -- --test-threads=1
../../tools/cargo-tidb test --locked -p tidb-expr --lib rewriter::preparation_tests:: -- --test-threads=1
../../tools/cargo-tidb test --locked -p tidb-expr --lib -- --test-threads=1
```

| Post-fix gate | Actual result | Log |
|---|---|---|
| Same provenance regression | 1 passed / 1350 filtered | `tidb-d1-pb-origin-alias-green.log` |
| D1 complete private seed | 16 passed / 1335 filtered | `tidb-d1-post-origin-fix.log` |
| D2 preparation boundary | 8 passed / 1343 filtered | `tidb-d2-structural.log` |
| Full expression comparison | 1254 passed / 4 failed / 93 ignored / 1351 discovered | `tidb-d2-expr-full-comparison.log` |

The four full-suite failures match the previous 1245/4/93 gate **by names and complete assertion/panic content**, not just count: reverse IFNULL pushdown, EXP FloatOverflow, negative duration FSP, and partial STR_TO_DATE. Parent read both failure blocks. Full suite still exits101 and is not claimed green. The nine new passing cases are the alias regression plus eight D2 tests; ignored tests remain unverified.

Before E's source fix, D2's eight tests also passed in the already compiled RED artifact, without a concurrent Cargo/source-read race:

```sh
/home/agent/tidb/expression-unification/target-tidb/debug/build/tidb-expr/1a66296e036585b2/out/tidb_expr-1a66296e036585b2 rewriter::preparation_tests:: --test-threads=1
```

That supplemental `tidb-d2-structural-pre-origin-fix.log` is superseded for current acceptance by the post-fix Cargo gate, not mixed into a fictitious aggregate test count. D2 remains bounded preparation only: public SqlBuild behavior is retained, structural metadata is not value-refined metadata, and prepared signed CAST is still not D1-executable.

### Independent exact-value corpus

E produced `probes/decimal-exact/generate.py` and `evidence/decimal-exact-oracle.json`; parent independently ran `python3 -B probes/decimal-exact/generate.py --check` from the experiment root, exit0, and verified both SHA256s. Version `decimal-exact-v1`, seed20260928: 24 operand pairs, 96 operations and 120 numerical results; full quotient up to498 digits, result coefficient/scale up to556. Fraction/arbitrary-precision int only, with quotient/remainder identities and sign/bound checks. Normalized coefficient+scale describes value, **not** Decimal headers, result precision, SQL status, context, AVG or rounding.

- Generator SHA: `b184d7e295065e1a679eb725f053f28260526140f914ae43a49499bf898b2a99`.
- Cases SHA: `86a2722320feb68ba6e399507c0cccb12a6bc1ff0c24e496ba6a7bc72a01964b`.
- Artifact SHA: `fb6e5337259af18d12a05ce623d698a45072c8cfd2b2866504701a951ac3fc53`.

No product links to the external experiment fixture. Its 120 values are **not yet 120 passing core tests**; B will integrate selected independent storage-value cases into self-contained native tests. The earlier extreme quotient has96 digits (parent rechecked with Fraction); an informal94-digit message was wrong, while the recorded full quotient string was correct.

The parent updated TiKV's coprocessor guide for actual host ownership/cleanup and TiDB's architecture index for private PB provenance and StructuralOnly. Following `agents-review-guide.md`, this index change adds no policy, changes no Bazel/PR/RealTiKV rules, and keeps command cwd explicit. Parent verified all14 referenced experiment paths exist, compared E's three final SHA256s, and ran the following checks, all exit0:

```sh
# TiDB repository root; absolute path selects the pinned caller formatter.
/home/agent/tidb/expression-reuse/rustup-home/toolchains/nightly-2026-08-22-x86_64-unknown-linux-gnu/bin/rustfmt --edition 2021 --check --config skip_children=true rust/crates/tidb-expr/src/{distsql_builtin.rs,new_function.rs,rewriter.rs} rust/crates/tidb-expr/src/tikv/{lower.rs,tests.rs} rust/crates/tidb-expr/src/rewriter/{preparation.rs,preparation_tests.rs}
git diff --check -- docs/agents/architecture-index.md rust/crates/tidb-expr/src/{distsql_builtin.rs,new_function.rs,rewriter.rs} rust/crates/tidb-expr/src/tikv/{lower.rs,tests.rs} rust/crates/tidb-expr/src/rewriter/{preparation.rs,preparation_tests.rs}
# TiKV repository root.
git diff --check -- doc/maintenance-guides/src/coprocessor.md
```

No public evaluator route, new complete-family count, whole-workspace lint/clippy, SQL host adapter or performance acceptance is implied.

## B2.2-B private division / integer-pair / AVG gate

B froze `decimal.rs` before the parent ran, from the TiKV root:

```sh
../tools/cargo-tikv test --locked -p tidb_query_datatype --lib -- --test-threads=1
```

`tikv-b22b-datatype-full.log:350`: **341 passed, 0 failed/ignored/filtered**, exit0, including six new tests. The existing normalization/quotient-guess/subtract/add-back loop handles explicit Mysql, retained-quotient, remainder and integer-pair requests under WordLimit. Selected independent oracle values are now self-contained native fixtures, including the full498-digit quotient and remainder; this is not a claim that all120 external oracle outputs became core tests. Old Fixed quotient/MOD value and status oracles remain unchanged.

Separate tests exercise exact96-digit quotient/Go-correct remainder, zero shape/sign, hidden8/7 and9/7 AVG scale, result targets9/31/81/300 and checked count/scale limits. The earlier source-only 'integer divisor AVG18 versus general-div9' example was rejected as incorrect before becoming a fixture: the correctly applied ordinary formula rounds10 retained digits to18. Explicit AVG requests still avoid public u8/MAX30 narrowing and preserve their own source policy. No header-only result>storage AVG value equivalence is claimed.

A private witness `(1.5e44 + 1e-100) % 1e44` proves exact Grow quotient1 and remainder `5e43 + 1e-100`. The checked Fixed planner reports its legacy fractional-header **count error**, not a fabricated SQL numeric status: it computes selected fraction words minus integer words, which underflows for this newly reachable wide input. This is a held domain/invariant discrepancy, not evidence of actual allocator OOM. The legacy infallible wrapper could still panic there; **general public Grow admission remains closed**. No Fixed correction was approved by this gate. B supplied a source bound excluding this tail for canonical <=n-active-word operands, explicitly excluding malformed partial-leading headers and diagnostic/noncanonical shapes; that is not proof about the entire accepted physical transport domain.

B may continue the next private round/formatter block. C3a is concurrently being implemented in its separately released seven files; no new full-expression or caller gate is claimed against those moving sources. E's independent read-only audit of admitted D2 preparation reported no concrete finding; it ran no new tests and widens no acceptance domain.

## C3a exact ordinary-row profile acceptance

Both C helpers handed back their files and stopped before the seven-file source freeze. B confirmed the core was unchanged from accepted341 before this cohort. Parent reviewed `local/profile.rs`'s complete snapshot/ordinal/site validation and the Ordinary frame/shared prepared-kernel seam, and independently reproduced the ordered manifest SHA `e33024a7d3bff2d2826972b86a8d1d0bfc977a9f140698aec33b547f698d73a2`, with scoped diff check exit0:

```sh
# TiKV root.
set -o pipefail
sha256sum components/tidb_query_expr/src/local/{compile.rs,mod.rs,profile.rs,profile_tests.rs} components/tidb_query_expr/src/types/{expr.rs,expr_eval.rs,function.rs} | sha256sum
git diff --check -- components/tidb_query_expr/src/local/{compile.rs,mod.rs,profile.rs,profile_tests.rs} components/tidb_query_expr/src/types/{expr.rs,expr_eval.rs,function.rs}
../tools/cargo-tikv test --locked -p tidb_query_expr --lib -- --test-threads=1
../tools/cargo-tikv test --locked -p tidb_query_aggr --lib -- --test-threads=1
```

Separately, from the TiDB `rust/` root (the caller's own Cargo configuration):

```sh
../../tools/cargo-tidb test --locked -p tidb-expr --lib tikv::tests:: -- --test-threads=1
../../tools/cargo-tidb test --locked -p tidb-expr --lib rewriter::preparation_tests:: -- --test-threads=1
../../tools/cargo-tidb test --locked -p tidb-expr --lib -- --test-threads=1
```

| Gate | Actual result | Log |
|---|---|---|
| Native RPN | 520 passed, no failed/ignored/filtered, 2.83s | `tikv-c3a-expr-full.log:568` |
| Native aggregate consumer | 40 passed, no failed/ignored/filtered | `tikv-c3a-aggr-full.log:96` |
| Existing caller D1 | 16 passed /1335 filtered | `tidb-c3a-d1-seed.log:2031` |
| Existing caller D2 | 8 passed /1343 filtered | `tidb-c3a-d2-structural.log:2011` |
| Full caller comparison | 1254 passed /4 failed /93 ignored /1351 discovered | `tidb-c3a-expr-full-comparison.log:3386` |

All four caller failure names and complete assertion/panic text were reread and match the previous accepted comparison. Full suite still exits101; ignored cases are not verified. The new21 native cases comprise16 profile tests, four expression metadata/drop tests and one driver retained-byte overlap regression.

This gate admits only exact203 signed LongLong, Typed Int/typed-NULL and identity conversion under explicit TypedRow/PbRow facts. It does not remap203 to222, alter legacy admission, authenticate PB ingestion from labels, activate SQL entrypoints, provide native error-site/text adaptation, or implement AST/native-batch schedules. The snapshot is a flat immutable consistency check, not a cache or another executable graph. The coprocessor guide records these boundaries.

After acceptance, B was released for private round/formatter work and D for exactly six caller files (`tikv/ordinary.rs`, `ordinary_tests.rs`, and narrow `mod/catalog/lower/context.rs` factoring). That D3 slice requires a trusted retained original-wire pair for PB ancestry consistency, homogeneous Typed trees, demanded native-kind checks before erasing transport, and per-computed-node output metadata. It is not yet implemented/tested by this receipt. At that checkpoint C3d reported-error APIs remained design-only; no native formatter, reevaluation, root-only error-site guess or new caller dependency was authorized. Later releases are recorded below.

## B2.2-C round/formatter and D3 caller acceptance

B froze its shared round/formatter checkpoint before parent execution. C3d was staged safely: its two new files were not declared as modules while this cohort ran; existing runtime files remained frozen until the parent explicitly unlocked the remaining three after D3 acceptance.

From the TiKV root:

```sh
../tools/cargo-tikv test --locked -p tidb_query_datatype --lib -- --test-threads=1
../tools/cargo-tikv test --locked -p tidb_query_datatype --lib codec::mysql::decimal::tests::test_legacy_boundary_characterization -- --exact --nocapture --test-threads=1
../tools/cargo-tikv test --locked -p tidb_query_expr --lib -- --test-threads=1
../tools/cargo-tikv test --locked -p tidb_query_aggr --lib -- --test-threads=1
```

| Native gate | Actual result | Log |
|---|---|---|
| Owning datatype including private round/format | 347 passed, no failed/ignored/filtered | `tikv-b22c-datatype-full.log:356` |
| Legacy/domain-extension characterization | 1 passed /346 filtered | `tikv-b22c-legacy-boundary.log:11` |
| RPN consumers | 520 passed, no failed/ignored/filtered | `tikv-b22c-expr-full.log:565` |
| Aggregate consumers | 40 passed, no failed/ignored/filtered | `tikv-b22c-aggr-full.log:93` |

The focused log shows the approved change explicitly: Overflow negative-zero headers81/2/2 and ten initialized zero words are unchanged, **private legacy format0; Default Display/full storage-0.00**. Fixed MOD remains Ok with its old stored remainder. No old numeric/byte fixture was rewritten. Six new native tests cover wide rounding/carry, hidden scale, result31/81/300, raw/logical255 policy, negative zero/extreme counts, and a rejecting sink at result scale `u32::MAX`. The shared emitter stages at most128 bytes; no enormous padding allocation is claimed or performed by that sink test. General wide construction/public arithmetic remains closed; the held wide Fixed MOD discrepancy is not silently mapped to SQL Overflow.

From the TiDB `rust/` root:

```sh
../../tools/cargo-tidb test --locked -p tidb-expr --lib tikv::ordinary::tests:: -- --test-threads=1
../../tools/cargo-tidb test --locked -p tidb-expr --lib tikv::tests:: -- --test-threads=1
../../tools/cargo-tidb test --locked -p tidb-expr --lib rewriter::preparation_tests:: -- --test-threads=1
../../tools/cargo-tidb test --locked -p tidb-expr --lib -- --test-threads=1
```

| Caller gate | Actual result | Log |
|---|---|---|
| D3 explicit ordinary caller | 10 passed /1351 filtered | `tidb-d3-ordinary-seed.log:2023` |
| Frozen D1 | 16 passed /1345 filtered | `tidb-d3-d1-compat.log:2017` |
| Frozen D2 | 8 passed /1353 filtered | `tidb-d3-d2-compat.log:2009` |
| Full expression comparison | 1264 passed /4 failed /93 ignored /1361 discovered | `tidb-d3-expr-full-comparison.log:3394` |

Parent reread the complete four failure blocks; names and assertion/panic contents match the prior comparison. Full-suite exit101 remains expected evidence of those registered failures, not an all-green claim. Ten new passing D3 tests prove only the explicit runtime slice: homogeneous Typed roots, trusted-original-wire paired PB consistency, complete source ordinals/own result metadata, demanded raw Int/NULL checks before transport, selection occurrences, and unchanged old entry behavior. No native diagnostic adapter, global route or migrated-family increase follows from this gate.

E reported no concrete admitted-domain D3 source finding. The harness closed that audit turn as failed; on clarification E explicitly confirmed completed scope, no unfinished portion and no writes/builds/tests. This is a delivered source-review result, not execution evidence or a hidden extra passing gate.

### Independent pinned-Go parser and shift-policy observations

E authored only the experiment probe. Parent read the Go exponent guards, fixed-array Shift exits and Round zero branch before running the selected cases; no old Rust-DB giant-allocation path was executed. Parent added the two first values beyond the asymmetric negative guard, then separately appended the equivalent-value shift-order pair. The original18-case log is retained; the second log records20 cases. From the TiDB repository root:

```sh
gofmt -w /home/agent/tidb/expression-unification/probes/decimal-parser/main.go
env GOCACHE=/home/agent/tidb/expression-unification/go-cache GOPATH=/home/agent/tidb/expression-unification/go-path GOMAXPROCS=4 go run -p 4 /home/agent/tidb/expression-unification/probes/decimal-parser/main.go
```

Both executions exited0 and recorded no caught panic. They characterize the pinned Go implementation, **not a passing shared-parser implementation**. Observed outputs are in `go-decimal-parser-policy-oracle.log` and `go-decimal-parser-policy-oracle-v2.log`:

- The same90-digit mantissa ending98765: clean `e-9` leaves98765 with Overflow; `e-9x` becomes0.000098765 with Truncated. Exponent disposition can replace an earlier mantissa status and change whether shifting occurs.
- `-1e+9223372036854775808` becomes **positive**81-digit max with Overflow after the receiver/sign reset; negative lexical exponent overflow becomes zero/Truncated.
- VT/FF/empty/junk return1292 and zero; tested ASCII space/tab prefixes are accepted.
- Positive threshold1073741823: zero remains Ok, nonzero overflows. At1073741824 even zero overflows to max.
- Negative guard is `< MinInt32/2`, not a symmetric negation of the positive bound: zero is Ok at-1073741823 and-1073741824, but Truncated at-1073741825. Nonzero is Truncated zero at all three.
- Both `9` followed by80 zeros plus `e-162`, and short `9e-82`, denote the same mathematical value. The long form produces **wrapped Overflow with partial1e72**, raw header81/0/0 and words `[1,0,0,0,0,0,0,0,0]`; the short form produces ordinary Truncated zero. It is incorrect to saturate every Overflow to max. Shift propagates traced Round status before the all-lost reset, and FromString's direct equality with ErrOverflow does not match that traced error.

B's next private parser uses these Go-only observations and preserves separate legacy KV policy. The proposed ephemeral shift outcome records Direct versus Rounding disposition inside the shared operation; it is not a hidden origin tag on owning Decimal. Scanner/shift/publication closure is not accepted by these probes.

After this cohort, C3d's exact five-file reported-evaluation implementation and D4's exact four-file private diagnostic-view implementation were released. Their source work alone was not a new tested gate; the later native gate is below. The parent updated coprocessor maintenance boundaries and the TiDB architecture index. Parent scoped checks passed on unchanged D3 `mod/catalog/lower/context/ordinary_tests.rs` and verified the test SHA `44dbfec7d6714dd9e5eb161d8fa92c81b69d0c8a738e0199cc4ce01d97fa1341`; `ordinary.rs` was already loaned to D4, so no post-release D3 hash claim is made for it. Updated document diff checks passed, the new architecture source/test paths exist, and Go probe formatting passed. A byte comparison confirmed that the original18 observation lines were unchanged in the expanded20-case log; none contains a recovered panic. Broad lint/clippy, performance, wider traits/carriers and public activation remain unverified.

## B2.2-D parser and C3d native acceptance

The first parser run compiled and executed353 tests: **352 passed, one NEW test failed**, no ignored/filtered tests (`tikv-b22d-datatype-full.log`). It failed because the newly authored legacy-policy test assumed vertical tab was accepted by Rust byte `is_ascii_whitespace`. The implementation actually rejected it, so the chained RPN/aggregate commands were not reached.

Before correcting that test, parent compiled `probes/decimal-parser/legacy-kv.rs` against the preserved **pre-parser-refactor** native rlib. Its mtime was20:54:52, before the new test executable21:41:32; rlib SHA256 `4d6169949f80d350eb5a9edcce3bcc20e9410f92f230c753bfb89398f7aff784`. From the experiment root:

```sh
/home/agent/tidb/expression-reuse/rustup-home/toolchains/nightly-2026-01-30-x86_64-unknown-linux-gnu/bin/rustc --edition 2021 probes/decimal-parser/legacy-kv.rs --extern tidb_query_datatype=target-tikv/debug/deps/libtidb_query_datatype-c654d7c6d7b1484d.rlib -L dependency=target-tikv/debug/deps -o probes/decimal-parser/legacy-kv-probe
probes/decimal-parser/legacy-kv-probe
```

Exit0; `tikv-pre-parser-whitespace-oracle.log` independently shows space/tab→1, **VT rejected**, FF→1; the direct byte predicates are VT=false/FF=true. Only after this proof, B corrected the **new test assumption** to distinguish VT from FF. Production scanner code and all original fixtures stayed unchanged. This was not a fix that relaxed parsing or rerecorded an old oracle. As with other frozen probes, that rlib path can later be overwritten by Cargo: the recorded hash, retained probe and before-log identify the actual observation.

Parent then reran from the TiKV root:

```sh
../tools/cargo-tikv test --locked -p tidb_query_datatype --lib -- --test-threads=1
../tools/cargo-tikv test --locked -p tidb_query_expr --lib -- --test-threads=1
../tools/cargo-tikv test --locked -p tidb_query_aggr --lib -- --test-threads=1
```

| Gate | Actual result | Log |
|---|---|---|
| Private shared parser/scanner/shift policies | 353 passed, no failed/ignored/filtered | `tikv-b22d-datatype-full-v2.log:382` |
| RPN plus reported failures | 537 passed, no failed/ignored/filtered, 2.88s | `tikv-c3d-expr-full.log:610` |
| Aggregate consumers | 40 passed, no failed/ignored/filtered | `tikv-c3d-aggr-full.log:121` |

Parser tests include the20 executed Go cases, legacy whitespace/exponent differences, checked canonical parsing/shift and invalid UTF-8. It uses one scanner/packer and shared shift loops, with explicit policy and ephemeral Direct/Rounding disposition; no owning-Decimal origin tag or public general-wide admission follows. Checked words import and legacy trait/Fixed MOD closure remain unfinished.

C3d adds17 tests. Its raw owned error, actual-operation first-write capture, typed SQL-code getter, stage precedence, empty/repeated selection, budgets, panic retry, warning-prefix fixtures and old-host compatibility are native-tested. Parent read the diagnostic/report/recorder and shared batch core and independently matched the exact five-file manifest SHA `9fd38c3284ba02a9676e03a96f8f93e2d9a68677c26917f4b5f659e10bada099`, with scoped diff check exit0. Validation/resource/post-success failures have no invented site; shared input slots are not universally leaf ordinals; the new reporting API refuses Host programs before host hooks while the old API is unchanged.

At that native checkpoint D4 caller sources were still in flight, so no caller/native diagnostic acceptance was inferred. The subsequent completed caller gate is recorded below.

## D4 private overflow-view caller acceptance

Parent independently matched all four frozen source hashes reported in D-r10 and ran pinned rustfmt checks on `ordinary_diagnostics.rs`, its tests, `ordinary.rs`, and `scalar_function.rs`. The scalar-function loan was only two pure-helper visibility modifiers. Scoped diff checks passed; they are not a substitute for checking untracked new source content. From TiDB `rust/`:

```sh
../../tools/cargo-tidb test --locked -p tidb-expr --lib tikv::ordinary::diagnostics::tests:: -- --test-threads=1
../../tools/cargo-tidb test --locked -p tidb-expr --lib tikv::ordinary::tests:: -- --test-threads=1
../../tools/cargo-tidb test --locked -p tidb-expr --lib tikv::tests:: -- --test-threads=1
../../tools/cargo-tidb test --locked -p tidb-expr --lib rewriter::preparation_tests:: -- --test-threads=1
../../tools/cargo-tidb test --locked -p tidb-expr --lib -- --test-threads=1
```

- `tidb-d4-diagnostics.log`: **10 passed /1361 filtered**.
- Compatibility logs `tidb-d4-d3-compat.log`, `tidb-d4-d1-compat.log`, `tidb-d4-d2-compat.log`: **10/16/8 passed** respectively.
- `tidb-d4-expr-full-comparison.log:3432`: **1274 passed /4 failed /93 ignored /1371 discovered**. Parent read all four complete failure blocks; names and panic/assertion text match the earlier comparison. Exit101 remains registered, not all-green.

This validates only a private own-program/spec overflow view for exact203. Kernel/site/domain joins precede typed1690, the original report remains recoverable, own PLUS rendering differs from nested display-label facts, and bounded rendering does not invoke native evaluation/formatters. Warning endpoints cover all normal caller returns, not panic or general prefix authentication. No statement-context/severity sink, warning sites, broader diagnostic domain, public route or family completion is implied. A fresh independent read-only reviewer (`a39c5e1c…`) found no concrete admitted-domain defect; it did not execute tests.

## B2.2-E private words import and integer-conversion RED→GREEN

The focused test genuinely failed before the four loop-count substitutions:

```sh
# TiKV root; same filter before and after the fix.
../tools/cargo-tikv test --locked -p tidb_query_datatype --lib codec::mysql::decimal::tests::test_private_wide_integer_conversion_extents -- --exact --test-threads=1
../tools/cargo-tikv test --locked -p tidb_query_datatype --lib -- --test-threads=1
../tools/cargo-tikv test --locked -p tidb_query_datatype --lib codec::mysql::decimal::tests::test_private_fixed_policy_current_observations -- --exact --nocapture --test-threads=1
```

`tikv-b22e-wide-integer-red.log:34–44` records storage scale91: **Ok(0) versus required Truncated(0)**, one failed/356 filtered. After only the four `as_i64/as_u64` integer/fraction loop extents changed, `tikv-b22e-wide-integer-green.log` records one passed/357 filtered and `tikv-b22e-datatype-full.log:386` records **358 passed**, no failed/ignored/filtered. Strict words import preserves supplied initialized inactive cells, counts, sign and independent result scale; malformed logical shapes are rejected and metadata-only max scale is not densely expanded. Constructors/exports remain private, and the global capped macro was not rewritten.

Consumer reruns used the same native RPN/aggregate commands above, with `tikv-b22e-expr-full.log:621` **537 passed** and `tikv-b22e-aggr-full.log:132` **40 passed**. The full caller command produced `tidb-b22e-expr-full-comparison.log:3455`: **1274/4/93**, with all four complete failures reread and unchanged.

### Observations are not permission to replace Fixed with Grow

`tikv-b22e-fixed-policy-observations.log` records current moderate-size, unadmitted-wide behavior without asserting a mathematical correction:

- Positive1 ×0.1/storage90: Truncated positive zero with9/31/30 metadata; negative1 counterpart resets to positive zero1/0/0.
- Fixed round10^90 to scale0: Truncated partial1e72 with81/0/0, not a mathematical-round equivalence claim.
- A metadata-only visible300 zero MOD result expands to stored300; this bounded analogue exposed visible-driven storage growth. The unsafe max-u32 dense operation was **not run** before the correction.
- Critically, an **old canonical bounded** overlap also loses an integer cell:1000000001 × `'.1' + 80 zeros` produces Truncated100000000 with storage31/result30. A blanket fraction-only loss clamp would change old behavior.

Parent independently pinned that last case in Go and the accepted pre-extension native rlib (SHA `f533ac03b18a686ede21c3acd0325f19976ae6308f852476d7203325beb20738`) using `probes/decimal-boundaries/main.go` and `probes/decimal-parser/legacy-fixed-mul.rs`. Logs `go-decimal-bounded-mul-oracle.log:42–48` and `tikv-fixed-bounded-mul-oracle.log` agree on Truncated,9/31/30 and word0=100000000. Go storage text contains31 fractional zeros; Default text contains30. The Rust probe's first build failed because Res has no Display; formatting its borrowed Decimal payload fixed the **probe API use**, not numerical policy. The earlier before-logs remain untouched.

### B2.2-F MOD correction remains separately gated

Parent approved direction A: keep legacy traits Fixed and exact methods Grow, preserving the observed MUL total-word-prefix selections and Fixed ROUND partials. No AutoGrow/origin-tag shortcut or exact-then-clamp was approved. Only the actual MOD layout/allocation hazards received a separate change specification.

The two new desired tests actually failed in `tikv-b22f-fixed-mod-red.log`: public Fixed MOD panicked with the known fractional-header count underflow; metadata-visible300 zero produced `(0,300,300,false)` instead of `(0,0,300,false)`. Both failed/358 filtered. The approved correction is full integer+fraction extent including leading gap, selected fractional words×9 and bounded prefix copy; capped quotient/early/sign behavior remains. Visible<=255 zero behavior is retained; newly wide visible>255 uses input stored scales rather than allocating storage from visible metadata. The max-u32 regression is permitted only after the allocation path is checked, and it must not format billions of visible zeros.

At that point the MOD candidate waited for the C3b datatype helper writers. The following joint native gate was run only after both owners explicitly froze their files.

## B2.2-F MOD GREEN and C3b Stage-A helper acceptance

From the TiKV root:

```sh
../tools/cargo-tikv test --locked -p tidb_query_datatype --lib codec::mysql::decimal::tests::test_private_fixed_mod_ -- --test-threads=1
../tools/cargo-tikv test --locked -p tidb_query_datatype --lib -- --test-threads=1
../tools/cargo-tikv test --locked -p tidb_query_expr --lib -- --test-threads=1
../tools/cargo-tikv test --locked -p tidb_query_aggr --lib -- --test-threads=1
```

| Gate | Actual result | Log |
|---|---|---|
| Fixed MOD regression set, including safe max-visible metadata case | 3 passed /365 filtered | `tikv-b22f-fixed-mod-green.log:12` |
| Datatype: prior361 plus seven new helper tests | 368 passed, no failed/ignored/filtered | `tikv-b22f-c3b-helpers-datatype.log:376` |
| Existing RPN | 537 passed, no failed/ignored/filtered | `tikv-c3b-helpers-expr-compat.log:610` |
| Existing aggregates | 40 passed, no failed/ignored/filtered | `tikv-c3b-helpers-aggr-compat.log:121` |

The MOD fix follows the separately approved full-fraction/gap projection and visible-only-zero allocation rules. Before adding max-u32 tests, B verified the zero-construction path takes input storage scale, then assigns visible metadata; tests inspect fields/inline backing, never format billions of digits. The capped quotient loop, early/sign/finalization behavior and original bounded numeric/byte fixtures remain unchanged; only the explicitly pending private underflow characterization was updated under approval. Fixed MUL and ROUND retain their observed partial-selection policy, not Grow mathematics.

C3b Stage-A modified only `codec/data_type/bit_vec.rs` and `chunked_vec_bytes.rs` plus two **unwired** new expression lineage files. Seven executed helper tests cover actual allocation capacities and checked fallible reservations; old layouts, traits and capacity semantics remain. The staged23 lineage tests are not declared by a module yet and **were not compiled or run** at this gate. Parent independently reproduced the ordered four-file manifest SHA `6d05e4e9ad8682b49931dc9d89c7ee0126cb4af8f096f0443066b50b001b4484` and scoped datatype diff check, exit0.

Only after these results did parent unlock C3b's seven existing expression integration files, including `impl_op.rs` for accounting access only. Its memory contract is measured retained storage at admitted boundaries before a **subsequent** effect/publication, not a hard allocator peak or pre-bound unknown callback payload. At that checkpoint general wide publication, remaining codec/f64 closure and later integrations were still pending. The following narrow observer and boundary gates do not imply publication or family completion.

## D5 prerequisite FieldType observer acceptance

Only `tidb-datatype/src/field_type/memory.rs` was released: a checked logical snapshot-payload observer plus eight inline tests, not the five-file D5 caller. Parent read the observer and arithmetic helper, matched SHA `a6a69afe9332d0843422cfc97c8b398d00f931ac2ff35c78a53065f087c31f4a`, and ran pinned rustfmt/scoped diff checks, exit0. Existing memory_usage/storage_length bodies were retained.

The crate has `autotests=false`, but these tests are **inline library tests**, not the generated `--test all` integration target. Parent read the manifest and used, from TiDB `rust/`:

```sh
../../tools/cargo-tidb test --locked -p tidb-datatype --lib field_type::memory::tests::snapshot_payload_ -- --nocapture --test-threads=1
```

`tidb-d5-field-type-observer.log:1553`: **8 passed /428 filtered**, no failure/ignored, pinned caller1.100. The observer borrows visible elements, counts the independently sized private marker slice and checks arithmetic; it does not allocate, compare/hash or clone snapshots. Its charge is logical detached/projection content, not retained capacity or a hard heap/peak bound. The owner must serialize alias mutation across observation→equality/copy/projection; this is not an atomic snapshot. New architecture references and scoped document checks were also verified; no policy/Bazel/PR gate was changed.

## B2.2-G target, codec and storage-float safety gate

Two safe desired tests first ran and failed in `tikv-b22g-boundary-red.log`: convert target(81,1) was incorrectly accepted before a fast path, and encode(1,2) panicked on subtraction overflow. **Two failed /370 filtered**; no giant format/allocation was attempted.

Before production edits, parent also ran the moderate visible300 codec observation and current storage-float observation filters. Codec output was Overflow with bytes `[1,0,130]` and unchanged source metadata. Fifteen printed float observations pinned the existing Rust **storage-text** parse policy: +/-infinity for huge magnitudes, +/-zero for underflow/raw negative zero, subnormal/finite-neighbor bits, visible400 versus visibleMAX ignored, raw physical storage and hidden1/3. Warnings remained zero. These are not Go/TiDB result-projection equivalence claims or a new float algorithm.

After the actual RED and before observations, parent released only the shared checked MAX factory, convert target/full-cell fallible copy hardening, pre-write encoder validation/bounded metadata logs and fallible storage-text allocation. No new SQL65/30 declaration restriction, Go postcheck, infinity rejection, public wide export or external convert.rs edit was authorized. The old public MAX signatures remain, with both target preconditions documented. Full-cell no-op copies do not use strict logical import or discard supplied inactive cells.

From the TiKV root:

```sh
../tools/cargo-tikv test --locked -p tidb_query_datatype --lib codec::mysql::decimal::tests::test_private_boundary_rejects_ -- --test-threads=1
../tools/cargo-tikv test --locked -p tidb_query_datatype --lib codec::mysql::decimal::tests::test_private_codec_visible_300_observation -- --exact --nocapture --test-threads=1
../tools/cargo-tikv test --locked -p tidb_query_datatype --lib codec::mysql::decimal::tests::test_private_storage_float_current_observations -- --exact --nocapture --test-threads=1
../tools/cargo-tikv test --locked -p tidb_query_datatype --lib -- --test-threads=1
```

- `tikv-b22g-boundary-green.log`: **2 passed /372 filtered**.
- Codec/float after logs: **one test each passed**. Parent compared only the observation lines, proving the codec line and all15 float lines byte-identical before/after. An earlier informal17-case description was incorrect;15 printed observations is authoritative.
- `tikv-b22g-datatype-full.log:382`: **374 passed**, zero failed/ignored/filtered, including the previously accepted seven byte-helper cases.

The f64 helper counts through the same storage emitter, reserves fallibly and uses the same Rust parse, ignoring visible padding as before. Encoder diagnostics now intentionally log bounded scalar shape metadata instead of formatting the entire Decimal; wire projection/status/header behavior is retained. Only after both log paths were reread did B add the max-visible encoding case, checking bounded bytes/status/no mutation without huge Display. This is a safety/log-payload change, not a numerical-policy rewrite.

At that datatype374 gate C3b expression integration was still moving; no RPN/caller result was inferred from it. The following separate gate ran after all nine expression files froze.

## C3b control lineage native and caller compatibility acceptance

Parent reproduced the ordered nine-expression-file manifest SHA `276b2df89cecdb379da3099ee1cff51d432f2569a850980c649d3a7e4862590b`; scoped diff check exited0. The previously accepted two datatype helpers were unchanged. All writer handbacks and the read-only flow/heap reviews completed before execution.

From the TiKV root:

```sh
../tools/cargo-tikv test --locked -p tidb_query_expr --lib -- --test-threads=1
../tools/cargo-tikv test --locked -p tidb_query_expr --lib test_lineage_frame_layout_storage_is_actual -- --nocapture --test-threads=1
../tools/cargo-tikv test --locked -p tidb_query_aggr --lib -- --test-threads=1
```

- `tikv-c3b-expr-full.log:648`: **575 passed**, no failed/ignored/filtered. This is prior537 plus38 new tests; prior expected resource fixtures were not rewritten.
- `tikv-c3b-frame-layout.log:69–72`: **one passed /574 filtered**, measured EvalFrame400, ProgramFrame128, ControlFrame400, FrameResult176, RpnStackNode152 bytes for this build.
- `tikv-c3b-aggr-full.log:132`: **40 passed**, no failed/ignored/filtered.

The new unrun16,384-row/128KiB fixture was corrected under explicit approval before integration: IDs alone require256KiB, so static refusal before any read is required. This is not an old oracle rewrite. Exact retained measurements and conservative minimum precharges may safely over-refuse; no iff-within-budget acceptance claim is made. Both old/new modes charge actual enlarged frame layouts, so fixed numeric byte-limit refusal prefixes are not asserted identical across this change. No historical frame size was fabricated.

From TiDB `rust/`:

```sh
../../tools/cargo-tidb test --locked -p tidb-expr --lib -- --test-threads=1
```

`tidb-c3b-expr-full-comparison.log:3456`: **1274 passed /4 failed /93 ignored /1371 discovered**, exit101. Parent read all four failure blocks and programmatically compared each complete block against `tidb-b22e-expr-full-comparison.log`, normalizing only thread IDs: all identical. This verifies existing caller compatibility, not the new D5 caller, which was not yet implemented. Only after native+caller+observer gates did parent release its exact five private caller files.

C3b remains SQLTypedRow control-only Int/String lineage, not PB/AST/native-batch, ordinary composition, general SQL diagnostics or public activation. Current complete-family migration is still0/245.

## B2.2-H external typed Decimal producer RED→GREEN

The source loan was exactly `codec/{convert.rs,mysql/decimal.rs}`. Two desired tests ran against unchanged G production (`tikv-b22h-producer-red.log`): an invalid fully specified(81,1) zero/no-op was accepted, and the bounded overflow path reached MAX's nine-word precondition panic after the overflow-warning path. **Two failed /376 filtered**, exit101.

The first new characterization fixture itself failed before implementation (`tikv-b22h-policy-before.log`): public literal parsing of `0.` plus81 ones returned its existing Truncated error, so unwrap did not construct the intended exact scale81 value. No parser or producer policy was changed to make it pass. B moved only that new observation to the private Decimal test, using checked logical words(int0/storage81/result81/nine111111111 words). The two corrected before filters each passed one test/377 filtered:30 policy rows and7 private-wide rows. The initial31-row claim was never an executed success; the skipped second command in that failed run was not credited.

Parent then released the three codec-scoped associated mechanisms: checked declared target (old ordered-count error first, checked conversion and existing separate-word validation), shared checked MAX/sign, and borrowed Fixed rounding through one fallible ALL-initialized-cell copy and the same MAX30 worker. `produce_dec_with_specified_tp` keeps either-UNSPECIFIED bypass, natural-width i128 comparison, overflow disposition before saturation, original truncation/DML ordering and unsigned handling last. It does not call convert_to, add SQL65/30 restrictions or add an integer post-carry check.

From the TiKV root, parent ran:

```sh
../tools/cargo-tikv test --locked -p tidb_query_datatype --lib test_producer_boundary_rejects_ -- --test-threads=1
../tools/cargo-tikv test --locked -p tidb_query_datatype --lib test_producer_policy_current_observations -- --nocapture --test-threads=1
../tools/cargo-tikv test --locked -p tidb_query_datatype --lib test_private_producer_wide_current_observations -- --nocapture --test-threads=1
../tools/cargo-tikv test --locked -p tidb_query_datatype --lib -- --test-threads=1
../tools/cargo-tikv test --locked -p tidb_query_expr --lib -- --test-threads=1
../tools/cargo-tikv test --locked -p tidb_query_aggr --lib -- --test-threads=1
```

- `tikv-b22h-producer-green.log:11`: **2 passed /376 filtered**.
- `tikv-b22h-datatype-full.log:386`: **378 passed**, zero failed/ignored/filtered.
- `tikv-b22h-expr-full.log:648`: **575 passed**, zero failed/ignored/filtered.
- `tikv-b22h-aggr-full.log:132`: **40 passed**, zero failed/ignored/filtered.
- Parent compared the30 policy and7 private-wide printed observation rows exactly before/after: identical, including error strings/codes/warning counts, signed/unsigned behavior, carry, raw/noncanonical no-ops, visibleMAX ignored by storage observation and all14 supplied initialized cells.

Frozen candidate hashes: decimal.rs `3aa563771ffa51e5cd68ec3d4397dc16731415f2ceca34d4018a7b07ac5d057a`; convert.rs `350cfcb1e2881a74cf78eda172491858c7d9064f05751a6ef5bd7827c981efbf`. This closes the producer cut, not public wide admission. Source-derived eager/true-error Decimal formatting, temporal conversion and other fallible/contextual bridges remain separately reviewed; no huge formatter was executed.

## D5 initial compile feedback (not execution evidence)

After H/native sources froze, parent ran from TiDB `rust/`:

```sh
../../tools/cargo-tidb test --locked -p tidb-expr --lib tikv::lineage::tests:: -- --nocapture --test-threads=1
```

`tidb-d5-lineage-first.log` exited101 with five E0277 errors in the NEW test file: `VectorValue::from(Vec<Option<i64>>)` is a TiKV test-only conversion, unavailable when compiled as a dependency. **Zero D5 tests ran.** Only the new test fixture construction was released for correction using existing production vector APIs; no datatype trait/public export or old expected outcome was changed. That first compile was not test evidence. B's H sources stayed frozen and D changed only those five new test constructors to the existing production `VectorValue::from_scalar` API; all hostile IDs/counts/values and assertions remained unchanged.

## D5 corrected private caller acceptance

The same focused command then passed **18 tests /1371 filtered** (`tidb-d5-lineage-fixture-corrected.log:2061`). Parent also ran the complete `tidb-expr --lib -- --test-threads=1`: **1292 passed /4 failed /93 ignored**, zero filtered (`tidb-d5-expr-full-comparison.log:3463`, exit101). All four complete failure blocks were programmatically compared against `tidb-c3b-expr-full-comparison.log`; only thread IDs were normalized and the blocks were otherwise identical. The93 ignored tests remain unverified.

The18 tests cover native Datum comparison for all admitted controls, true source/type/metadata provenance, native binary/blob String identity, invalid UTF8 and collation, same-read admission, sparse/own IDs, selected/generated NULL and computed Boolean IDs, transitive UInt predicate refusal, skipped/duplicate selection/read ordering, whole incoming metadata preflight before snapshots/Eq, hostile materialization joins, actual source/IDs/native-output coexistence accounting and immutable-spec worker ownership. This is private SQLTypedRow control-only acceptance, not public activation, ordinary composition, PB/AST/batch entry or generic D4 SQL diagnostics.

Parent captured the exact five-file hashes in `tidb-d5-accepted-sources.sha256` and ran pinned TiDB rustfmt with `--edition 2021 --check --config skip_children=true`, exit0. The files are untracked additions, so an empty scoped git diff check is not content-diff proof. Accepted test source SHA is `dbf91cabeaca22743c360d6aa8f782a94bb20beb62844d8788792583ad2c1055`; the other four match D's original coherent checkpoint. Metadata alias stability remains an owner prerequisite, not atomic snapshot budgeting. Retained accounting is checked before subsequent effects/publication, not a hard allocation peak bound.

Parent subsequently linked/executed the standalone bounded Decimal allocation probe: see `wide-decimal-consumer-audit.md` §9 and `decimal-diagnostic-alloc-red.log`. Instrumentation controls passed; eight stable MOD/DIV pairs each had two extra malloc requests and DAY had one. This authorized only B2.2-I's exact lazy three-file implementation, not a wide diagnostic/temporal policy. C3c's separate seven-file implementation was also released after the D5 gate.

## B2.2-I datatype-only gate; joint consumer/probe gate pending

B returned exactly decimal.rs, impl_arithmetic.rs and time/interval.rs; H convert.rs remained unchanged. Parent ran `../tools/cargo-tikv test --locked -p tidb_query_datatype --lib -- --test-threads=1` from TiKV: **381 passed**, zero failed/ignored/filtered (`tikv-b22i-datatype-full.log:390`). The three new datatype tests cover the eager baseline, lazy factory/disposition compatibility and bounded simple-interval compatibility. Parent inspected the one shared contextual Res match; existing eager APIs delegate and the FnOnce factory is invoked only on Overflow.

Candidate SHA256: decimal.rs `47b93b115a75e4235fe9964d573dab99b84f0711dc459e6d3891725d626736ba`; impl_arithmetic.rs `5a5131552beedbff4c88963329dbd8b97ed6cae50af9441ae2d15040ae0046c6`; interval.rs `3f29b0057679d3017a3510931730e2ca6649ed48a52cab6d5a9d9bef5b68a9af`. Native RPN has one additional authored bounded compatibility test, not yet run. C3c expression sources are still owned by active writers, so parent intentionally did not build that partial candidate. Full RPN/aggr/caller compatibility, frame-layout remeasurement and identical-source allocation-probe relink/GREEN await the coherent combined handback. Rebuilding datatype alone does not update the already-linked prepatch RED executable.

Guide updates describe the H producer contract, narrow I laziness (without claiming true-error/temporal closure) and D5 private caller. Complete-family migration remains0/245; public wide admission, native deletion, C3c/C4, lint/clippy and performance remain unfinished at this datatype-only checkpoint.

## Round4: coherent C3c/I production and compatibility acceptance

All commands below use the pinned wrapper from the named worktree cwd. C froze production before the initial library build, then all seven files before full tests. Parent independently matched the original seven-file digest `cd6589…31c0` and the corrected digest `413f4f5ccc5154888aee9f3e02ba3c166420de268f7306d40b01439c39342894` (separate original/corrected manifests in logs).

From `expression-unification/tikv`:

- `../tools/cargo-tikv build --locked -p tidb_query_expr --lib`: exit0 (`tikv-c3c-b22i-library-first.log:340`). This is a DEV library build, not test execution.
- `../tools/cargo-tikv test --locked -p tidb_query_aggr --lib -- --test-threads=1`: **40 passed**, no failures/ignored/filtered (`tikv-c3c-b22i-aggr-full.log:134`). This also refreshed the matching TEST-profile dependency graph.
- First `../tools/cargo-tikv test --locked -p tidb_query_expr --lib -- --test-threads=1` exited101 with **zero tests run** (`tikv-c3c-b22i-expr-full-first.log:64–118`). Two new fixture constructions caused three compiler errors: `HostCatalog::new(701)` instead of the existing Vec-taking Result API, and dependency-invisible `VectorValue::from(Vec<Option<i64>>)`. Only the new test hunks in compile.rs/expr_eval.rs were corrected to the existing empty catalog and public ChunkedVecSized construction; no production API or assertion changed.
- The identical full command on the corrected checkpoint: **608 passed**, zero failed/ignored/filtered (`tikv-c3c-b22i-expr-full-corrected.log:679`), comprising old575 +32 C3c +1 I tests.
- `../tools/cargo-tikv test --locked -p tidb_query_expr --lib test_lineage_frame_layout_storage_is_actual -- --nocapture --test-threads=1`: **1 passed /607 filtered**, actual sizes `EvalFrame400 /Program128 /Control400 /FrameResult176 /RpnStackNode152` (`tikv-c3c-frame-layout.log:68–72`). The first compile-failing chain never reached this command.

From `expression-unification/tidb/rust`, `/home/agent/tidb/expression-unification/tools/cargo-tidb test --locked -p tidb-expr --lib -- --test-threads=1`: **1292 passed /4 failed /93 ignored**, exit101 (`tidb-c3c-b22i-expr-full-comparison.log:3475`). Parent extracted both complete failure sections and compared against D5 with ONLY numeric thread IDs normalized; all four full blocks were identical. No ignored case is credited.

C3c is closed signed-203 operand-major batch execution through the same driver/kernel, not public native-batch activation or a complete migrated family. Compiled entry, execution semantics and retained-storage policy remain distinct. Maintenance guide updated accordingly.

## Round4: identical-source allocation GREEN and artifact-specific Send checks

See `wide-decimal-consumer-audit.md` §§9–10 for controls, exact compiler/link flags and scope. The probe source remained `4f03e8…58cc`. Parent's first post-DEV-build relink accidentally selected still-existing OLD TEST rlibs: both hashes in `decimal-diagnostic-probe-after-artifacts.sha256` equal the prepatch hashes. The misleadingly named `decimal-diagnostic-alloc-green.log` is therefore **stale RED**, not patched behavior. Its set-e chain skipped the following Send compilation.

After the aggregate test graph refresh, parent relinked the same source with the same wrapper flags and matched TEST-profile libraries (datatype `186ef167…76ec`, expr `93dcca94…0697`, full hashes in `decimal-diagnostic-probe-after-test-artifacts.sha256`). `decimal-diagnostic-alloc-green-test-profile.log:1–36` records **exit0**: empty/four-allocation-route/diagnostic controls pass; eight stable alternating-order pairs EACH of MOD/DIV/DAY now have actual `[1,0,0,0]` equal to their same-native baseline. Result/shape and zero-warning assertions pass. The excess2/2/1 requests are removed; this is neither zero allocation nor a wide/peak-memory guarantee. True emitted oversized diagnostics, temporal input and public-wide admission remain closed.

`tools/local-runtime-send-probe.rs` was actually compiled with pinned TiKV rustc `--edition=2021 --crate-type lib --emit metadata`, both against original H/C3b artifacts and refreshed C3c/I TEST artifacts, exit0. It checks LocalProgram/LocalEvalState/EvalContext and their aggregate: Send; Mutex<Vec<aggregate>>: Send+Sync. No unsafe impl or product trait was added. It does not prove future C4 aggregate types, borrowed ScopedColumns traits, lifecycle, pooling or memory limits. A's independent source review is recorded separately in `evaluated-value-review.md`.

## Round4: D6 actual native-dispatch caller checkpoint

The explicit four-file loan followed C3c/I joint acceptance: evaluator.rs, scalar_function.rs and new evaluator/{numeric_batch,numeric_batch_tests}.rs. Public `run` keeps the Native consumer. The mandatory private route shares the actual Decimal-first/current-flag-once dispatch and consumes a genuine invocation token without executing the native value worker as a probe. Source/slot ownership, full incoming type/layout preflight, actual selection and native same-read kinds are checked independently of D3–D5.

From `expression-unification/tidb/rust`:

- `/home/agent/tidb/expression-unification/tools/cargo-tidb test --locked -p tidb-expr --lib evaluator::numeric_batch::tests:: -- --nocapture --test-threads=1`: first exit101, **zero tests**, E0599 missing existing `protobuf::ProtobufEnum` import (`tidb-d6-numeric-batch-first.log:1860–1874`). Only that import was added; no conversion/fixtures/Cargo change.
- Same focused command: **18 passed /1389 filtered**, zero failed/ignored (`tidb-d6-numeric-batch-import-corrected.log:2086–2106`).
- `/home/agent/tidb/expression-unification/tools/cargo-tidb test --locked -p tidb-expr --lib -- --test-threads=1`: **1310 passed /4 failed /93 ignored**, exit101 (`tidb-d6-expr-full-comparison.log:3526`). Parent again compared the four COMPLETE failure blocks to C3c/I after numeric thread-ID normalization only: identical.

Corrected source hashes: evaluator `e3f706fc…46b6`, scalar `b986c5a4…95b4`, numeric batch `3b3a94d1…b657`, tests `75c44334…9ac9`. D5's five files remain unchanged. A subsequently completed an independent four-file/all18-test source review, chasing native dispatch, actual Chunk access and capacity helpers, with no additional finding within the declared domain. This is source review, not another test execution; alias-stability and retained-not-peak exclusions remain. Parent captured `tidb-d6-accepted-sources.sha256`, ran pinned caller rustfmt `--edition 2021 --check --config skip_children=true` on all four files and scoped git diff/reference checks, exit0. The two new files are untracked, so empty git diff output alone is not their content evidence. Architecture-index/maintenance maps were updated without changing AGENTS policy. No general SQL diagnostic adapter, public route or whole-family credit is implied.

## Round4: actual old-native ASCII frontend baseline

E authored only `probes/ascii-baseline/source.rs`, SHA `8326eafe34d16cef415b37df227f2d0181d8ee3792b858557cacac7304c14440`. Parent compiled against E's matched existing DEV snapshot without rebuilding active source. The installed caller compiler uses split metadata: RLIB-only failed with four metadata-stub errors; RMETA-only typechecked but lacked three linkable RLIBs; paired `--extern crate=…rmeta` AND `--extern crate=…rlib` linked successfully. All three logs and exact successful flags are retained (`ascii-baseline-link-{first,metadata,paired}.log`, `ascii-baseline-link-paired-command.txt`).

Parent then ran `probes/ascii-baseline/native-baseline old-native split-paired-dev-347ed91b09f692de`, **exit0**, stdout `ascii-native-baseline.tsv`:150 observation rows (2 actual frontends ×5 inputs ×5 samples ×3 phases), plus untimed numeric2→50 and manual-GBK228/AST214 controls. Every timed evaluated Datum is individually asserted after timing (3,328,000 outputs); the51,200 warmups check Result success only. Checksums are not a separate value oracle (non-NULL repeated folds can collapse); cold construction rows reuse first-eval checksum and do not represent extra evaluations. Typed input is constructor-supplied collation-string data; actual chunk-decoded kind is not printed/proven by the benchmark.

Source/binary hashes are frozen in `ascii-native-baseline-binary.sha256`. `-Zbinary-dep-depinfo` produced a manifest of812 distinct compiler-reported paths, all hashed in `ascii-native-baseline-dependencies.sha256`; this is not a system-linker-input inventory or proof every listed artifact was linked. The four direct RLIB hashes match E's recorded cohort. Protocol, all30 median/min/max groups and caveats are recorded in `ascii-baseline-contract.md` AB-r1. Warm public evaluation is NOT a warmed explicit shared-kernel scope; DEV dependencies are NOT release code; allocation call counts and numeric performance acceptance thresholds are unmeasured/unset. This is baseline setup, not C4 performance acceptance.

## Round4: B2.2-J staged dispatch RED → datatype-only GREEN

Only convert.rs/decimal.rs were released. Stage1 added a test-only private adapter scaffold and four tests, while recovering the exact accepted old whole-file hashes after removing only those new blocks. A fake specializes the checked adapter to return `ColumnOffset(731)`; it does NOT implement fake ConvertTo. The actual old generic String/Bytes wrappers instead return their tiny infallible marker.

From `expression-unification/tikv`:

- `../tools/cargo-tikv test --locked -p tidb_query_datatype --lib test_storage_bridge_propagates_ -- --nocapture --test-threads=1`: actual desired **0 passed /2 failed /383 filtered**, exit101 (`tikv-b22j-storage-dispatch-red.log:7–26`); failures show `Ok("J infallible marker")` / its bytes, not a compile failure.
- `../tools/cargo-tikv test --locked -p tidb_query_datatype --lib test_storage_bridge_fallback_current_observations -- --nocapture --test-threads=1`: before **1 passed /384 filtered**, four rows with nine context variants each (`tikv-b22j-fallback-before.log:7–13`).
- `../tools/cargo-tikv test --locked -p tidb_query_datatype --lib test_private_storage_bridge_current_observations -- --nocapture --test-threads=1`: before **1 passed /384 filtered**, eighteen STORAGE rows with nine context variants each (`tikv-b22j-storage-before.log:7–27`). Includes signs/trailing zeroes, hidden one-third storage, modest wide values, raw noncanonical/negative-zero/all initialized cells and tiny six-byte storage with result metadata300/u32::MAX. MAX metadata is never Display-formatted.

After these actual results, the separate GREEN grant allowed only removal of the new adapter's two cfg(test) gates, two generic wrapper bodies using its UFCS result / `.map(String::into_bytes)`, and one Decimal specialization delegating to existing G `try_storage_text`. Reverse-only-those edits recovered both entire stage1 hashes; assertions/observation sequences/old tests/other production stayed byte-identical. Public APIs/bounds/defaultness and old infallible ToStringValue/Display remain unchanged.

The same three commands then passed: dispatch **2/383**, fallback **1/384**, storage **1/384** (their `*-green.log` / `*-after.log` files). Parent programmatically extracted and compared all **4+18 before/after rows byte-for-byte**, including shapes/all initialized cells and nine-context unchanged observations: identical. Full `../tools/cargo-tikv test --locked -p tidb_query_datatype --lib -- --test-threads=1`: **385 passed**, zero failed/ignored/filtered (`tikv-b22j-datatype-full.log:393`), exit0.

Accepted datatype-only source hashes: convert.rs `bc70dca2314831d20ab56860105fc4b03d27a23431e094d170897753dd8c90a3`; decimal.rs `66d6a902ce7c96bc461c68c1cf0b8297f38240dab86a8a89607aa3efe3bcd8ad`. Parent captured `tikv-b22j-accepted-sources.sha256`, ran pinned rustfmt `--edition 2021 --check --config skip_children=true` on both and scoped git diff checks including the updated maintenance guide, exit0. This RED is dispatch/error propagation after an explicit test-only prerequisite, **not physical Decimal ENOMEM reproduction**. Default non-Decimal behavior and owned-JSON specialized value formatting are retained. This closes only the generic Decimal STORAGE Result-wrapper mechanism, not Datum RESULT Display, temporal text, quotas, general fallibility, or public-wide admission.

**Post-J RPN/aggr/full caller gates remain pending the coherent C4 six-file handback.** Pre-J608/40/1310-with-four-baseline-failures are not credited as post-J results. C4 now has six explicit expression-file owners; A's isolated real Arc allocation-request probe and E's concrete native owner/lease API proposal run independently. A pinned Arc accounting proxy is conditionally approved without unsafe pointer access, subject to real both-toolchain measurements; fixed metadata cache prewarming before worker publication is mandatory and cannot execute fake NULL kernels. None of these pending C4/owner gates counts a complete family. The unique main plan retains0/245 and all M6 gates open.

## Round4 follow-up: real Arc config allocation requests on both pins

A authored only `tools/arc-eval-config-alloc-probe.rs` (595 lines, SHA `617eea064d150c5a51aca0e7253758af633456c96f20f563eeebbb6856372255`). Parent reviewed and compiled that unchanged source twice, without any product rebuild or allocator replacement. GNU wraps observe malloc/calloc/realloc/posix_memalign/free using32 fixed atomic event slots; the accounting proxy is never allocated or used to inspect Arc memory. Each actual Arc::new request is observed independently and matched to its final free by the opaque allocation identity.

Both exact compiler invocations are saved in `logs/arc-eval-config-{jan-c3c-i-test,aug-ascii-dev}-command.txt`; `*-cohort.txt` records rustc -vV and source/compiler/direct artifact hashes. Common flags: edition2021, opt-level3, codegen-units1, lto off, panic unwind, gcc-14, all five GNU wrap flags, linker trace, `-Zbinary-dep-depinfo --emit=link,dep-info`. Each resulting binary was run directly with no arguments. **Both link and run exited0.**

| Linked cohort (not future C4) | Direct datatype artifact | Actual repeated result |
| --- | --- | --- |
| Jan1.95 nightly842bd5be2, C3c/I TEST | RLIB `186ef167…76ec` | 8/8 samples, one88-byte Arc config request, exact final matching free |
| Aug1.100 nightlyc656540d6, old ASCII DEV | RLIB `f15b0301…84a6` + paired RMETA `bf90c60b…cb9a` | 8/8 samples, same actual request/retirement result |

Both117-line `*-run.log` files report config size72/alignment8; accounting proxy size88/alignment8; Arc handle size8 is NOT the allocation size. Before/after empty controls and actual Rust allocation-route controls all passed: malloc73, realloc149 with original/final identity checks, calloc91, posix_memalign257@64, and matching frees. Each sample independently observes config construction0; actual Arc config allocation1×88;64 Arc clone/drop operations0;64 zero-detail context clone/wrap/drop operations0; moving the sole Arc into context0; final drop exactly one free of the recorded allocation. Both warning count/details/capacity and no external Weak/sole strong ownership are checked. Repeated context wrapping is a probe control, not authorization for per-row C4 context recreation.

The parent hashed452 Jan and545 Aug distinct compiler-reported dependency paths in `*-dependencies.sha256`. These are not full system-linker-input inventories or claims that every listed metadata/shared artifact was linked. `*-receipt.sha256` covers source, binary, dependency-info, exact command, compiler/direct-cohort receipt, link/run logs and dependency manifest. Binary SHAs: Jan `eaa9a644b746dc28fc6a388b3a4b2fd797026db137c33e65db0fc9f088dc7893`; Aug `e17a49f5d1389985e12efcee3426b6e1381c5b55d4ffe1bdb0589ff5497c0267`.

This discharges only the proposed fixed-config allocation-request extent on these two x86_64 GNU cohorts. Jan installed ArcInner source is repr(C,align(2)); Aug rust-src is absent and its internal offsets remain unverified. Equal observed request size does not establish Aug field layout, portable ABI, allocator usable slack/bookkeeping, peak memory, Weak-outliving-strong behavior, a caller PoolCore layout or whole-worker/pool accounting. C4 implementation, prewarming, actual kernel/body dispatch, lifecycle and combined post-J regressions remain open. No product file, fixture, expected result or probe source changed to obtain this result.

## Round5: post-J caller datatype boundaries, without active C4 compilation

Parent verified both worktree HEADs still equal the fixed baselines and both native `tidb-expr/src/context.rs` and TiKV `expr/ctx.rs` have no tracked diff. The TiDB datatype manifest depends on TiKV datatype, not expression; therefore the following commands could safely validate the frozen J change on the Aug caller compiler while C4's expression writer remained active.

From `expression-unification/tidb/rust`:

- `/home/agent/tidb/expression-unification/tools/cargo-tidb test --locked -p tidb-datatype --test all tikv_value_bridge_source:: -- --test-threads=1`: **11 passed /76 filtered**, zero failed/ignored (`tidb-post-j-value-bridge.log:1545–1558`), exit0.
- `/home/agent/tidb/expression-unification/tools/cargo-tidb test --locked -p tidb-datatype --lib field_type::memory::tests::snapshot_payload_ -- --test-threads=1`: **8 passed /428 filtered**, zero failed/ignored (`tidb-post-j-metadata-memory.log:1544–1554`), exit0.

These19 tests and successful caller-side compilation were not expression/aggregate/full-caller gates and do not imply Decimal public-wide admission. The subsequent coherent six-file C4 gate is recorded below. EV-r2's caller cut remains separate from public capability/dispatcher/deletion activation; D independently maps later entry/error requirements while B prepares an isolated bounded fault probe without product writes.

## Round5: C4/J coherent native and full-caller gates

Parent verified the original six-file manifest digest `5868a59c478846cb6ef4a200e8f50f14777adcc4f844050e694e73537bbd0fa3` before compiling. The files are local/{compile,batch,mod}.rs, types/{expr,expr_eval}.rs and impl_string.rs under TiKV's expression crate; the last file adds only the ASCII cfg(test) hook in this cut. C3c profile/profile_tests/runtime files remain unchanged. All product writers stopped before these commands.

From `expression-unification/tikv`:

1. `../tools/cargo-tikv build --locked -p tidb_query_expr --lib`: **exit0**, DEV production library (`tikv-c4-b22j-library-first.log`). This is not unit-test execution.
2. `../tools/cargo-tikv test --locked -p tidb_query_aggr --lib -- --test-threads=1`: **40 passed**, no failed/ignored/filtered (`tikv-c4-b22j-aggr-full.log:134`). This refreshes the TEST-profile production dependency cohort including J.
3. First `../tools/cargo-tikv test --locked -p tidb_query_expr --lib -- --test-threads=1`: **exit101, zero tests run** (`tikv-c4-b22j-expr-full-first.log:64–76`). One new expr_eval.rs fixture called private `ChunkedVecSized::get`. A one-line, new-test-only loan changed it to existing public `ChunkRef::get_option_ref(&values, 0).copied()`. No production API/body or assertion/input changed. Reversing that line in memory recovered the exact old whole-file SHA; the other five hashes stayed identical. The first chain never reached the isolated-origin or layout commands.
4. Same full command on the corrected checkpoint: **636 passed /1 intentionally isolated test ignored**, zero failed/filtered (`tikv-c4-b22j-expr-full-corrected.log:708`). This is old608 +28 new regular C4 tests, not637 ordinary passes.
5. `../tools/cargo-tikv test --locked -p tidb_query_expr --lib local::batch::evaluated_ascii_tests::test_evaluated_ascii_wrapper_body_origin_isolated -- --ignored --exact --nocapture --test-threads=1`: **1 passed /636 filtered**, no failed/ignored (`tikv-c4-ascii-origin-isolated.log:71`). Parent read the actual test: oversized-capacity refusal is wrapper0/body0; preparation0; ready NULL increments wrapper only; empty/NUL/ff and two joined, separately owned thread workers yield the asserted total non-NULL body delta5. This is actual official wrapper/body execution, not an adapter-call counter. The synthetic finish_invocation original-error/dirty-postflight fixture is separate and is not represented as a naturally emitted ASCII kernel error.
6. `../tools/cargo-tikv test --locked -p tidb_query_expr --lib test_lineage_frame_layout_storage_is_actual -- --nocapture --test-threads=1`: **1 passed /636 filtered**, actual sizes still **400/128/400/176/152** (`tikv-c4-frame-layout.log:69–72`). The worker ownership Send/!Sync/!Clone and synchronized-owner assertions also executed in the regular suite; none was added by unsafe impl.

Corrected six-file digest: `3697a2482392124d830d3c876616707bba3da46d05438c002f6a00516090bd41` (`tikv-c4-corrected-source-manifest.sha256`); corrected expr_eval.rs SHA `c572b141191df62ded911db38f10e7b3b36ee4255a1d4b35eb1ec7720be5cf4e`. Parent reran pinned rustfmt `--edition 2021 --check --config skip_children=true` on all six, scoped git diff checks including the maintenance guide, and sha256sum --check of all six: exit0. Ordinary git diff does not inventory untracked local files; direct hashing/formatting and actual compilation cover those files.

From `expression-unification/tidb/rust`, `/home/agent/tidb/expression-unification/tools/cargo-tidb test --locked -p tidb-expr --lib -- --test-threads=1`: **1310 passed /4 failed /93 ignored**, expected exit101 (`tidb-c4-b22j-expr-full-comparison.log:3538`). Parent extracted the COMPLETE failure blocks and compared against D6 with ONLY numeric thread IDs normalized: identical. This is post-J/C4 caller evidence, unlike the earlier pre-J receipts. Ignored caller cases remain unverified.

A independently reviewed the five frozen compiler/driver/observer/hook files and supporting seams with no additional concrete finding in that bounded source cut; no test execution was attributed to A. A then completed the separate frozen batch-worker review (matching SHA bdf773f1…ca48d) with no additional concrete source blocker: actual capacity accounting, mandatory prewarm, sealed context, post-TaskGuard input/output coexistence, poison/primary-error behavior, opacity and all17 test definitions. Its panic/primary-error synthetic fixtures are explicitly distinguished from actual wrapper/body origin. These two source reviews cover the full six-file cut but are not additional test executions or caller-pool acceptance. The two-toolchain actual88-byte config-Arc request evidence remains a narrow accounting premise, not a whole-worker/caller-pool/peak proof.

After the native gate, E received exactly two NEW, initially unwired TiDB files: tikv/evaluated_ascii.rs (E) and evaluated_ascii_tests.rs (one exclusive helper). Parent alone owns module/root/Columns/dispatcher wiring. They must use the actual C4 worker and preserve the63-method native forwarding surface; caller Arc allocation observation, pool/creation/retirement accounting, panic/epoch tests and later public propagation remain distinct gates. D's source audit also established parent decisions: preserve native fold-error suppression and DEFAULT error remapping; direct sort expressions and root DEFAULT ASCII keep their existing negative admission, while projected SQL ORDER BY and allowed nested-default forms require separate positive evidence. No native ASCII algorithm has been deleted or public route activated, and complete-family credit remains0/245.

## Round5: bounded real Datum RESULT allocation-failure RED, no product fix yet

B's source-only audit identified the Decimal arm of inherent Datum::to_string as an infallible RESULT Display consumer, distinct from J's STORAGE trait adapter. Its private new fault probe was not coherently handed back: repeated harness failures interrupted a RETURNED-record hardening edit. Parent stopped/revoked B; a fresh author99ac also failed before handback and was stopped/revoked. Parent then read the existing complete probe, fixed the observer/RETURNED/FINISH and future-GREEN record grammar, validated the positive controls' sizes/alignment, constrained compilation to64-bit GNU/Linux, formatted and froze the source. No product file, old probe, global allocator or fixture was changed. A is independently reviewing this new probe read-only; no uncompleted author/audit run is credited as evidence.

Frozen source `tools/decimal-datum-display-fault-probe.rs`: SHA256 **e63c85fef7979adc73b165a2fda92451bc0e584ffa360633b38fe6d15b2ab04b**. Parent used Jan rustc842bd5be2 with the freshly refreshed TEST normal datatype artifact `libtidb_query_datatype-c654d7c6d7b1484d.rlib`, SHA **e806f5107170f28b658bd8fcc06eb3265cb926a0a16f0229f168a0e13a0f9ff3**. Compiler identity, exact escaped link command (GNU wrap malloc/calloc/realloc/posix_memalign, no competing global allocator),452 compiler-reported dependency paths and hashes are in `logs/decimal-datum-display-fault-before-{command.txt,cohort.txt,dependencies.sha256,receipt.sha256}`. Compiler-reported paths are not a complete system-linker-input inventory. The executable SHA is **dbecf196ff264dbade6d1b954fd8c69526b7347524eb6545b2b10f6ba2754c2d**; compile/link exited0.

Actual execution `tools/decimal-datum-display-fault-before` (parent additionally set ulimit -c0) exited **1, verified RED**, not a generic failure credited as a regression. The complete14-line run log proves:

- Both fresh child processes use real bounded Decimal12.34; no huge/MAX value is built. Nonarmed output is12.34. Full bounded physical cell bytes, precision/storage/visible scale/sign are observed; the observer's post-call snapshot is unchanged. Context is honestly N/A for this inherent method, not a fake clean SQL context.
- Single-thread checks, zero core limit AND checked process-local nondumpable state precede injection. The parent driver never arms refusal. Child positive controls observe empty0, malloc64, realloc128, calloc64×1 and aligned128/64 with real successful pointers. Pipe endpoints and child wait/reaping remain separate from the armed call.
- The observer's ACTUAL Datum call performs exactly one malloc8 and returns12.34. Its persistent sequence is REQUEST8 → RETURNED → FINISH; it exits0.
- A fresh injected child confirms the same first small request. The fixed raw pipe HIT record is written successfully BEFORE returning NULL and the refusal gate is disabled first. Its sequence is REQUEST8 → HIT8, with NO RETURNED/FINISH; stderr says `memory allocation of 8 bytes failed`, and parent observes SIGABRT. A later harness failure after a normally returned call cannot be classified as this RED because RETURNED is recorded immediately at the call boundary, before post-snapshot/printing.

This is a deliberate isolated **single transient small allocation refusal**, not physical or sustained OOM and not recovery from arbitrary later error-message allocation failures. The inherited allocator path and current product code remain unchanged. There is no K GREEN or datum.rs change yet. Any later product candidate must preserve the frozen source, rerun controls against a newly verified matching artifact, and yield the actual outer codec error with unchanged owner; merely a rejecting sink/helper test would not replace that target-call gate. Product scope proposed for later: one datum.rs private fallible fmt::Write sink that uses the untouched existing Decimal Display exactly once, with no duplicated/extracted formatter and no new quota/public-wide admission.

## Round6: K actual GREEN and first paired publication checkpoint

Only `codec/datum.rs` changes the product: a private String sink calls try_reserve before each append and delegates once to the unchanged Decimal Display via fmt::write. Only its Decimal to_string arm returns that helper; into_string and codec JSON-object keys naturally reuse it. Error construction can still allocate. No Decimal/J/temporal formatter or numeric disposition changed. The preceding two NEW characterization tests ran BEFORE the product change:36 bounded raw rows including independent visible scale0/1/2/3/30/31/127/128/255, signed zero and an initialized inactive word; plus a real Decimal JSON key and non-Decimal controls. Both tests pass before and after. The36 complete output/cell lines are byte-identical. In-memory removal of only the helper and reversal of the one Decimal arm recovers exact before-product SHA24c4e45e95e54a68b5623f19fd62cf81550dd405012bfc017cb5d0dbdf930262; no expectations were rewritten. Current datum.rs SHA2f70320b3bd1df4f57bd69ef23e4532d1eb9579e036f254e6b43e6f8f5c04fc5.

Parent native commands (from TiKV, using ../tools/cargo-tikv, all --locked): focused test_decimal_result_text_ --nocapture **2/385 filtered**; full datatype **387**, aggregate **40**, expression **636/1 isolated ignored**, then exact ignored ASCII origin **1/636 filtered**. Logs are tikv-k-{result-before,result-after,datatype-full,aggr-full,expr-full,ascii-origin-isolated}.log. The normal TEST datatype artifact was refreshed before relinking; its SHA is now6ff3848d19b9feca48d27dec6b10f647b99a4f045db740abdbb1a73b52145047, not the RED cohort's e806f510….

The EXACT frozen e63c85fe…ab04b probe relinks successfully and actually exits **0 GREEN**: observer still returns12.34 with one malloc8 and unchanged owner; the injected child records REQUEST/HIT/RETURNED/FINISH and exits20 after actual outer InvalidDataType1105, original shape unchanged. No SQL1690 or fake status. New runner is a Linux subreaper, uses a separate process group and20-second deadline, kills/reaps that group on timeout and classifies timeout124 as inconclusive. This actual run completed normally. Full command/cohort/run/runner/452 compiler dependencies/receipt hashes are decimal-datum-display-fault-after-*; frozen probe source is unchanged. A's independent source/receipt review found no additional required probe correction; it did not execute either run.

Caller command initially used the wrong workspace directory and exited101 BEFORE testing (tidb-k-expr-full-comparison.log); that is not the registered four-failure comparison. Corrected cwd tidb/rust, `../.../tools/cargo-tidb test --locked -p tidb-expr --lib -- --test-threads=1`, actually ran **1310 passed /4 baseline failures /93 ignored**, exit101, in tidb-k-expr-full-corrected-workdir.log. Parent compared all complete failure blocks against D6, normalizing ONLY thread IDs: identical. All broad lint/clippy/workspace/performance/family gates remain open; this is not PR readiness.

User now explicitly authorizes committing/pushing each verified step with the Plan to YangKeao/tidb AND YangKeao/tikv. First paired Checkpoint-ID: foundation-c4-k-01, branch expression-unification-demo. Only validated sources and curated evidence are eligible; the new unwired evaluated_ascii.rs/evaluated_ascii_tests.rs and unrelated generated vendor BUILD.bazel stay local. Repo-root Plan files are byte-identical publication mirrors of the single external editable master. Git refs are verified after normal, non-force pushes; prose is not evidence that a push succeeded. A subsequently completed the narrow K helper/arm/two-test and full RED/GREEN receipt source review with no additional scope blocker; no execution was attributed to that review.

## Rounds7–8: private caller compilation and two actual concurrent-state regressions

Parent registered only the private `tikv::evaluated_ascii` module and ran the 28 existing regular tests (plus one ignored, non-measuring fixture). Full comparison was1338/4 identical baseline failures/94 ignored; exact commands are in `tidb-c4-caller-first-command.txt`. This did not accept the caller gate: independent READONLY review found two uncovered paths. First, cached validation could read an old matching epoch and a newly cleared uncertain-retirement counter even though those two eligible conditions never coexisted. Second, a cached lease need not acquire the pool mutex, so std Mutex poison could remain invisible until a later lock/snapshot copied it into the separate atomic poison flag.

E authored only two new structural regressions plus cfg(test) one-shot thread-local instrumentation, with no PoolCore fields, substitute worker, production callback or allocator change. Stage1 impl SHAba7dcd6630daaf57d1867e66295e2abc4d0cfa7a4a769785b2feeb7593341b24; tests SHA4bd182aa2cbce956be430557805af1e9d8f06be2347778090e803cdc8522e4d4. Parent ACTUALLY ran `tools/cargo-tidb test --locked -p tidb-expr --lib tikv::evaluated_ascii::tests::structural_ -- --test-threads=1 --nocapture` from tidb/rust, using the absolute wrapper path. RED log `tidb-c4-caller-race-red.log`, exit101: one pre-existing structural test passed, TWO new tests failed (1435 filtered). The first accepted the deliberately impossible epoch/debt pair. The second actually returned Ok(NULL), entered eval_one once and advanced the actual worker counter1→2 after an accounting-mutex panic, without an intervening snapshot/lock.

Parent then changed ONLY check_epoch: observe both std Mutex and root sticky poison before/after the atomic check, close the root on poison, and read the non-reused/non-wrapping epoch again after the uncertain-debt observation. Matching epoch reads bracket a real eligible debt-observation instant; final sticky-poison checks certify that same instant without locking a kernel/native callback. In-memory reversal of this function alone restores EXACT Stage1 impl SHAba7dcd…1b24. Actual same-filter GREEN log `tidb-c4-caller-race-green.log`:3 passed/1435 filtered, exit0. The fixed poison case refuses before eval_one entry; disposed workers have NO observable getter afterwards, not a fabricated zero official invocation count.

Parent subsequently added only safe inherited-Rust allocator controls to the ignored observation fixture and applied pinned rustfmt. Current frozen manifest `tidb-c4-caller-fixed-source.sha256`: impl205f83e1e9efb97f9c9c8d5c4f35f10453d25fd4660526fe1e44ef0ac9445ceb, tests7ea2227e8e132dcef1b973e424b6c7ed31159b2f298b24a97b42185244225ee3. No regular test assertions were edited. A whole-file attempt to reverse only the ignored controls did NOT restore the pre-rustfmt test hash (decc012d…bc45 versus4bd182aa…2e4d4); therefore we do not claim literal full-test-file byte identity across formatting. Actual focused fixed suite:30 passed/1 ignored/1407 filtered. Actual full fixed comparison:1340 passed/4 registered failures/94 ignored, exit101; all complete failure bodies match K, normalizing ONLY numeric thread IDs. Separate safe-control fixture:1 passed/1437 filtered WITHOUT observer; its size192/align8 candidate output and successful fixture execution still do NOT measure allocations. At that pre-observer stage no family was counted, no SQL dispatcher installed, no native ASCII body removed, and factory high-water/Arc measurement/public activation gates remained open.

### Round8 final paired checkpoint

`caller-private-pool-checkpoint.md` now records the final acceptance and exact commands for `caller-private-pool-02`. Private opaque carrier7 also passes; final full caller is **1347 passed /4 identical baseline failures /94 ignored**, exit101. Final source manifest includes implf7287aa2…78087 (only two accounting comments changed after the actual repair), tests7ea2227e…25ee3, mod5351acf0…9933e, opaque carrier60d39ae5…8f1a8. Parent rebuilt this comment-final source before measuring it, not transferring old artifact results.

Actual final TEST ELFddb196e81f054e5fff33659c7d0a0e135bf8e37fe94f11ddafb5bc62c54b7a9b: **eight fresh measured processes each native1 PASS/1444filtered and observerPASS/exit0**, independently reporting one actual malloc192 with matching final free, all empty/64-clone windows0, both inherited SAFE-Rust control groups valid,14 markers/16 events/gap0/foreign0. Same-ELF normal-test missing-marker negative actually native1 PASS but observerINVALID/UNPROVEN/exit86. Frozen Ceff83df6…eee18e compiled by gcc14 with-Werror; actual imports and dynamic hashes recorded, no resolver/libatomic/TLS helper/stdio dependency. Bounded runner5b37f818…1ce8e, SOe6fd5344…5705d7; final source/artifact/library cohort and all9 receipts are under `logs/tidb-pool-arc-final/`. C independently reviewed this FINAL cohort with no consistency finding; it did not execute or modify anything.

The192-byte requested-extent basis is now accepted for this exact pinned cohort only. Revalidation obligation stays true; no malloc requested-align/portable ABI/usable/peak/whole-pool or OOM guarantee follows. Both concurrency findings are closed in the reviewed scope; the opaque carrier is compiled, not publicly integrated. Public activation/deletion/performance, factory transient high-water and broad M6 gates remain open; complete families remain0/245. Publication uses identical generated Plan snapshots and the normal TiKV-first/TiDB-paired-commit push policy.

## Round9: native error envelope and terminal mapping, not public producer activation

Checkpoint `native-error-wiring-03`; exact commands, scope and limitations are in `native-error-wiring-checkpoint.md`. D contributed context/renderer only, parent native exports/real EvalError test and7 missing generic-clause test fields. Actual carrier8, full expression1348/4 unchanged complete failure blocks/94ignored; source/destination diagnostics not re-recorded. Three downstream libraries (session, old exec, unistore) check0. Before-variant renderer initially could not compile seven old TableResolver fixture initializers (ZERO tests); after7-line fixture-only repair, before/after both7 pass/1 identical pre-existing Sequence-origin equality failure. Prepared fixtures7 pass before and after. Complete renderer failure comparison normalizes ONLY thread IDs and the single temporary cfg location366→365; all3 temporary gates removed and source restoration verified. Final six-file hashes/pinnedfmt/diffcheck passed; A independently confirmed no new source finding and no raw-backend public API. Native-envelope unit input is not a public production LocalError; the new terminal arm is not yet exercised end-to-end by one.

Unchanged observer/runner actually rerun on final TEST ELF b5d49459c8f4a7486b462289b38e1336a2f44fcb0ffc3fd9869de6e676872cf4: eight fresh1-test processes pass, actual malloc192/final free and inherited controls valid,14 markers/16 events, clone/empty/gap/foreign0; same-ELF missing-marker normal-test negative rejected86. New source/artifact/actual dynamic-library bindings and raw receipts are in `logs/native-error-arc-final/`. The SO was reused and hash-verified, not recompiled this round. No broad ABI/peak/OOM/factory-high-water or public activation acceptance follows.

E's source-only concrete activation plan and C's five-family shared-driver candidates are recorded separately; no implementation authority or family credit follows from those proposals. Public producer/known-phase capture, separate adapter errors, all-entry capability/lifetime/catcher integration, native deletion, differential/performance and broad M6 remain open. Completed families still0/245; no transcreated package or PR-ready claim.

## Round10: explicit public value/capability, not SQL/session activation

Checkpoint `native-capability-value-04`; exact commands/scope in `native-capability-value-checkpoint.md`. E's two caller files add the opaque public surface,63 ordinary forwarders plus2 effective queries, discovery/effective guards and five actual phase-capture sites; D's new adapter carrier retains native Pool/Scope/Bridge causes and renderer tests. Parent owns four registry/context/docs boundaries. Focused tikv107 passed/1ignored; includes7 new public caller and8 adapter tests. Actual public value producers—not private error constructors—exercise Prepare ResourceLimit and native PoolResource through terminal MysqlError:2 passed,1105/HY000/correct fixed message/from_evaluation. This does not execute a SQL query or network packet. Observe remains source-only structural site coverage; actual Invoke API refusal in one case correctly has zero official kernel invocations.

Full expression1363 passed/4 old complete failure blocks/94ignored/1461 discovered, exit101. Complete blocks equal round9 after ONLY numeric thread-ID normalization. Renderer9 passed/1 same Sequence-origin equality failure, exit101; normalize only thread IDs plus three new arm lines365→368. No expected fixture changed. Three downstream library checks0, pinnedfmt/diffcheck0, eight-file manifest unchanged. A independently source-reviewed both source stages/final8-file cohort with matching start/end hashes and no new in-scope finding; no pool algorithm/layout/ledger change or weakened resource/count/retirement assertions.

Unchanged observer/runner actually rerun on final TEST ELF dec586c4585ac6de56c271fad1ce17a3717b977a811272615d8a2dbffd4df4b4: eight fresh1-test processes pass, actual malloc192/final free and inherited controls valid,14 markers/16 events, clone/empty/gap/foreign0; same-ELF missing-marker normal-test negative rejected86. Current sources/artifacts/actual unchanged linked-library hashes bind all raw receipts in `logs/native-capability-arc-final/`. No portable ABI/physical heap/peak/OOM/factory-high-water guarantee; error-carrier allocation is outside that ledger.

C's next lifetime inventory identifies real nested begin/finish, fallback, detached-close, shared-subquery and SET_VAR traps; recorded in `session-runtime-lifecycle-next-cut.md`, not implemented. Policy defaults/session/operation scopes/business wrappers/SQL activation/native deletion/differential/performance and broad M6 remain open; complete families0/245. Agent-index review: architecture paths and new tests exist, Rust commands are explicitly scoped to rust/, root AGENTS policy and precedence unchanged, no new normative policy/Make target/PR workflow or generated fixture edit introduced. No overall completion or PR-readiness claim.

