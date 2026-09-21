# TiKV expression migration: verified checkpoint, not completion

## Revisions and decision

This checkpoint now includes the thirteenth native string-auxiliary deletion tranche.
Its semantic-gap companion is published on the YangKeao TiKV fork at
`0193236`; TiDB still builds pinned engine
`db9c7f08d954fef02519b86a55c2d72cf72ef697`, not that documentation HEAD.
The revision and publication text below is retained as historical context for
earlier tranches; this is not a hosted-CI claim and does not establish TiKV
server compatibility.

Historical sixth-tranche revision context: that checkpoint built from published
TiDB `e76c191` and TiKV documentation HEAD `4c372b8`; TiDB actually built pinned
engine `db9c7f08d954fef02519b86a55c2d72cf72ef697`, not arbitrary TiKV HEAD.
Only YangKeao personal forks had been used for that publication. This paragraph
is retained as history, not a current publication or hosted-CI result.

**Do not switch the default yet. Physical deletion is deliberately in
progress.** Thirteen tranches have now removed the complete native math-kernel
folder plus crypto/encryption, vector, JSON depth/storage, both regexp modules,
the packet-limited string module, miscellaneous kernels, `builtin_ext/string2.rs`,
the packet-context string tail, all four INET conversion kernels, and the native
case conversion/ASCII/BIT_LENGTH/LEFT/RIGHT/REVERSE/REPLACE/STRCMP kernels,
HEX/UNHEX/BIN/OCT/ORD/BIT_COUNT plus their private coercion helpers, and
SUBSTRING_INDEX/QUOTE plus their byte-splitting/quoting helpers, together with
their residual dispatch. `CONCAT`, `CONCAT_WS`, `INSERT_FUNC`, `MAKE_SET`,
and `FROM_BASE64` are now
explicit contractions because the shared facade does not transport their
`max_allowed_packet` and warning policy. Earlier miscellaneous/string2
contractions remain in force. `INET_ATON`, `INET_NTOA`, `INET6_ATON`, and
`INET6_NTOA` are retained only through TiKV; malformed INET_ATON/INET6_ATON
return NULL instead of the production Go kernels' error, an explicit semantic
gap. The eleventh tranche makes UPPER/UCASE, LOWER/LCASE, ASCII, BIT_LENGTH,
LEFT/RIGHT, REVERSE, REPLACE and STRCMP TiKV-only for admitted shapes; nested
CONVERT/ELT, SET-subquery REPLACE and mixed CHAR_FUNC/STRCMP shapes are explicit
contractions. LOCATE/INSTR/POSITION remain native because the embedded TiKV
path currently loses `utf8mb4_bin` case sensitivity. The twelfth tranche makes
HEX/UNHEX/BIN/OCT/ORD/BIT_COUNT TiKV-only for admitted shapes, including OCT's
string signature; binary-literal provenance and non-leaf/numeric ORD are
explicit contractions. The thirteenth tranche makes SUBSTRING_INDEX and QUOTE
TiKV-only for guarded shapes. Runtime/unsigned/i64::MIN counts contract around
the pinned kernel's `abs()` behavior, while binary/BIT/ENUM/SET QUOTE contracts
because Go substitutes U+FFFD and TiKV preserves malformed bytes. This
invalidates older retained/native claims for the deleted names without claiming
full compatibility.
The earlier refusal-intolerant corpus still exposed 59 distinct first-refused
expressions, so the migration objective remains active/incomplete.

## Acceptance status for the six requested items

The six-item acceptance framework is unchanged by the thirteenth deletion tranche;
none of the rows below should be read as completion.

| Item | Established foundation | Remaining acceptance work |
| --- | --- | --- |
| 1. Lazy/short-circuit | Signature-driven engine lazy kernels, unchanged wire format, TiDB lazy tests and full-tree risk metadata. Hash/Merge filter owners retain programs. | Complete TiDB vectorized short-circuit and default-switch validation. Scalar cache reuse is not vectorization; requested Borrowed mode does not prove borrowed-lazy execution. |
| 2. Capabilities/admission/gates | Explicit signature/shape admission, structured refusal, static inventory checks, CI-callable runtime receipt checker. | Wire and validate hosted CI; extend engine-required execution beyond current fixtures, remaining kernel/host capabilities and all native-supported shapes. Current compatibility mode still admits native execution. |
| 3. Semantic gap tracking | `components/tidb_query_expr/EXPRESSION_SEMANTIC_GAPS.md` records divergences, guards and the resolved embedder probe error boundary. | Resolve or explicitly account for remaining gaps before deletion; do not silently widen admission. |
| 4. Required datatypes | Set transport/codecs and adapter bridge, bounded literal/text casts and temporal profiles have tests. | Root/lazy literal provenance, implicit binary coercions, remaining numeric profiles, named-zone/DST and restrictive zero-date transport. Set support alone does not establish native-removal completeness. |
| 5. Error classification/no replay | Differential helper compares error versus success; engine errors are not replayed natively. Probe-child errors now stop later filter execution. | Enforce the same contract at every remaining entrypoint/corpus. Successful compatibility tests are not proof of zero native fallback. No retry-after-error guarantee was established. |
| 6. Sharing/cache/context | Sync metadata, compiled/execution split, EvaluatorProgram cache, structured required-engine errors; retained Hash/Merge condition programs with reopen tests. | Remaining live entrypoints, partition retention, statement-owned generated/default descriptors and their invalidation contracts; avoid caches on publicly mutable descriptors. |

The living implementation record is `tikv-expression-removal-execplan.md`.
Historical milestone checkboxes there describe implemented foundations, not
completion of all six requirements. This checkpoint corrects stale admission
counts and overbroad lazy/datatype milestone wording.

## Validation results

Current thirteenth-tranche gates: runtime **30 tests / 322 receipts / 2064 engine
rows / 160 borrowed rows / zero native fallbacks**; static **384 rows / 216
admitted / 168 excluded / 0 missing**. The source corpus remains **36
`*_source.rs` files / 447 tests**. The expression library and external suites
pass **1185 + 77** tests; expression/query differential gates pass, and the two
new focused session tests pass in both feature modes. Feature-off expression-library
compilation remains red on previously documented unguarded engine-only test
imports. These facts do not establish hosted CI, an end-to-end SQL demo, full
compatibility, or TiKV server compatibility.

The command/log table below is preserved from the sixth packet-string tranche;
only rows explicitly labelled current remain current. All heavy commands ran
serially with one worker and the memory guard below.
Logs are in `/home/agent/tidb/expression-reuse/`.

| Log | Result | Scope/qualification |
| --- | --- | --- |
| `checkpoint-executor-native.log` | 1346 library + 329 integration + 6 tests passed; 184 integration ignored | Feature-off executor compatibility, not engine coverage. |
| `native-string-packet-delete-full-lib-tests.log` + `native-string-packet-delete-integration-tests.log` | 1196 library + 77 integration passed; 99 library ignored | Feature enabled after six physical-deletion tranches; seven deleted-file tests are replaced one-for-one by explicit contraction receipts. |
| `native-string-packet-session-{focused,unit}.log` | 6 targeted runs passed | Source SQL shapes assert exact packet-string refusal; retained conversion/position/reverse/lock/global-variable behavior still executes. |
| `native-string-packet-executor-focused.log` | 2 targeted runs passed | Executor coverage and statement context split exact SPACE/REPEAT contraction from retained CONCAT packet-limit warnings. |
| `native-regexp-session-focused.log` | 1 passed | Opted-in end-to-end SQL covers all six regexp names and requires at least five observed TiKV rows. |
| `native-regexp-session-unit-{chunk,collation,default}.log` + `native-regexp-session-feature-off.log` | 6 targeted runs passed | Chunk/operator, derived-collation, and computed-default SQL execute through TiKV when opted in; feature-off shapes assert exact refusal. |
| `native-json-leaf-session-focused.log` + `native-json-leaf-session-unit-{storage,depth}.log` | 3 targeted tests passed | Session SQL verifies structured contraction for deleted JSON storage functions and non-typed JSON_DEPTH shapes, including NULL. |
| `native-string-packet-session-full-lib.log` | **1707 passed / 19 failed / 209 ignored**, exit 101 | No packet-string/regexp/JSON test failed; the broad suite remains red on stale math/crypto/vector contraction expectations and is not a green gate. |
| `native-string-packet-runtime-gate-green.log` | 30 tests, 323 fixture receipts, 2072 engine rows, 160 observed borrowed rows | Reviewed admitted baseline remains exact after packet-string deletion and executes with zero fallback. |
| `native-string-packet-delete-lint.log` | exit 0 | Repository lint passed under the memory guard with one make job. |
| `native-misc-full-lib-final.log` + `native-misc-integration-final.log` | 1197 library + 77 integration passed; 99 library ignored | Seventh-tranche expression suites; deleted misc tests were replaced by explicit contractions and TiKV-only ANY_VALUE evidence. |
| `native-misc-session-full-lib-final.log` | **1707 passed / 19 failed / 209 ignored**, exit 101 | Exactly the pre-tranche known math/crypto/vector failure baseline; no miscellaneous-deletion regression remains. |
| `native-misc-session-integration-final.log` | **333 passed / 5 failed**, exit 101 | Five pre-existing stale math/crypto source expectations; UUID contraction integration tests pass. |
| `native-misc-{expr,query}-diff.log` | 1 + 1 gates passed | Native removals are named contractions rather than `ERR` skips; query ANY_VALUE uses Copying TiKV with a positive row counter. |
| `misc-review-integration-native-final3.log` + `misc-review-integration-copying-final4.log` | native: 77 exact statement contractions + 1 native-only `ANY_VALUE` refusal and 160 carried divergences; Copying: 416611 engine rows and 161 carried divergences | Generic marker-only acceptance was removed. Parsed function identity and exact prepared/default SQL are required; executable engine-backed `ANY_VALUE` must succeed and increment the TiKV row counter. This is not a green full-integration claim. |
| `string2-expr-final-battery.log` + `string2-runtime-gate-first.log` | 1185 expression library + 77 integration passed; runtime 30 tests / 323 receipts / 2072 engine rows / 160 borrowed rows | `string2.rs` and the orphaned SUBSTRING/LOCATE/FORMAT/EXPORT_SET helpers are physically absent; retained binary/string search shapes use TiKV and contractions fail before warnings or replay. |
| `string2-integration-native-final2.log` + `string2-integration-copying-final2.log` | native: 91 exact contractions + 1 native-only refusal / 181 carried divergences; Copying: 416592 engine rows / 167 carried divergences | Two complex retained SUBSTRING statements remain visible engine-routing failures; the replay is intentionally red and is not a full-integration claim. |
| `packet-tail-full-lib-final2.log` + `packet-tail-test-all-final.log` | 1183 expression library + 77 external tests passed; 99 library tests ignored | Native CONCAT/CONCAT_WS/INSERT_FUNC/MAKE_SET/FROM_BASE64 kernels and duplicate scalar paths are physically absent; all former value domains are exact packet-string contractions. |
| `packet-tail-session-{concat2,insert2}.log` + `packet-tail-result-diffs3.log` | 2 focused session tests and expression/query differential gates passed | SQL paths fail closed with the exact packet-string marker; classifier acceptance requires parsed function identity and does not treat literal/comment text as a contraction. |
| `packet-tail-integration-{native2,copying}.log` | native: 181 carried divergences; Copying: 416240 engine rows / 167 carried divergences | Full replay remains intentionally red; the packet-tail deletion adds explicit contractions without hiding the two retained SUBSTRING routing failures or unrelated divergences. |
| `inet-lib-final.log` + `inet-test-all-final.log` + `inet-result-diffs-final.log` | 1186 expression library + 77 external tests and expression/query differential gates passed | Native INET converter kernels/dispatch are absent; source vectors assert exact native refusal or independent engine-only values without fallback. Existing runtime fixtures record engine rows for all four converters, while query diffs require positive row deltas for IPv4 converters. |
| `inet-integration-{native,copying}.log` | native: 181 carried divergences; Copying: 416241 engine rows / 167 carried divergences | Full replay remains intentionally red; admitted INET execution does not mask engine errors or unrelated divergences. |
| `string-core-lib-review-final.log` + `string-core-test-all-final.log` | 1185 expression library + 77 external tests passed; 99 library tests ignored | Eleven native string kernels and all residual dispatch are absent; LOCATE/INSTR/POSITION are deliberately retained and tested. |
| `string-core-session-feature-{on,off}-final.log` + `string-core-result-diffs-final.log` | eight focused session tests passed in each feature mode; expression/query differential gates passed | Feature-on rows require TiKV counters; feature-off rows preserve retained functions and assert exact refusal for deleted names. |
| `string-core-integration-{native-final,copying-final3}.log` | native: 181 carried divergences; Copying: 416245 engine rows / 167 carried divergences | Shape-specific SET/CHAR_FUNC contractions are exact and cannot classify standalone REPLACE/STRCMP/LOCATE failures; the replay remains intentionally red. |
| `radix-lib-final.log` + `radix-test-all-final.log` | 1185 library + 77 external expression tests passed; 99 library tests ignored | Six native radix/code kernels and their private helpers are physically absent; independent values and exact contractions remain. |
| `radix-session-focused1.log` + `radix-result-diffs3.log` | five focused session tests passed in each feature mode; expression/query differential gates passed | Feature-on rows require positive TiKV counters; feature-off rows assert exact radix refusal. |
| `radix-{runtime,static}-final.log` | runtime 323 fixtures / 2072 engine rows / 160 borrowed rows; static 216 admitted / 168 excluded / 0 missing | Signature inventory remains a count/hash gate, not SQL semantic coverage. |
| `radix-integration-{native1,copying2}.log` | native 182 divergences; Copying 167 divergences / 416238 engine rows | Native adds one empty partition result after a refused HEX write; Copying returns to the carried divergence count. This is an intentionally red diagnostic. |
| `string-aux-integration-{native-final,copying-final}.log` | native 182 divergences; Copying 167 divergences / 416244 engine rows | SUBSTRING_INDEX/QUOTE guarded shapes add no broad-replay divergence; the replay remains an intentionally red diagnostic. |
| `char-delete-{lib,test-all,session-focused,result-diffs,runtime,static}-final.log` | 1185 library + 77 external passed; CHAR session contraction passed in both feature modes; diffs/runtime/static passed | Native CHAR_FUNC kernel/helper/preprocessing/dispatch are absent; TiKV has no CHAR signature, so preserved source values now assert exact structured refusal and constant folding cannot erase the call. |
| `char-delete-integration-{native,copying}-final.log` | native 182 divergences; Copying 167 divergences / 416242 engine rows | Explicit CHAR contractions remove two previously replayed native rows without masking unrelated broad-replay divergences; the diagnostic remains intentionally red. |
| `field-elt-{lib4,test-all1,session-focused1,collation-final2,diffs2}.log` | 1185 library + 77 external passed; focused external/internal session tests passed in both modes; expression/query diffs passed | Native FIELD/ELT kernels, dispatch, and scalar bypass are absent; ordinary shapes require TiKV while exact coercion contractions remain visible. |
| `field-elt-integration-{native,copying}-final.log` | native 182 divergences; Copying 167 divergences / 416246 engine rows | FIELD/ELT ordinary shapes execute through TiKV; the intentionally red replay does not mask unrelated divergences. |
| `string-tail-{lib2,test-all-final,session-focused2,diffs5,runtime2}.log` | 1185 library + 77 external passed; focused session/collation passed both modes; expression/query diffs passed; runtime 317 receipts / 2026 engine rows / 160 borrowed rows | Complete `string_fn.rs` is physically absent; LOCATE/INSTR/POSITION/TRIM are bounded explicit contractions because bridge metadata is unsafe. |
| `string-tail-integration-{native,copying}-final.log` | native 182 divergences; Copying 167 divergences / 416246 engine rows | Broad replay remains intentionally red with no new divergence count; unsupported tail statements stay visible. |
| `info-delete-{lib3,test-all-final,session-focused1,diffs2,runtime-final}.log` | 1178 library + 77 external passed; SQL digest contraction passed both modes; expression/query diffs passed; runtime 317 receipts / 2026 engine rows / 160 borrowed rows | `builtin_ext/info.rs` is physically absent; five information helpers fail closed under parsed miscellaneous markers. |
| `info-delete-integration-{native,copying}-final.log` | native 182 divergences; Copying 167 divergences / 416246 engine rows | Broad replay remains intentionally red with no new divergence count after info-family contraction. |
| `native-misc-lint.log` | exit 0 | Repository `make -j1 lint` passed under the memory guard. |
| current static gate | Self-check and check pass | 384 declaration rows, 216 admitted / 168 excluded, zero missing registry/synthesized names. Static candidates are not execution coverage. |
| `checkpoint-engine-only.log` | **1163 passed / 61 failed / 99 ignored**, exit 101 | All 61 failed sections report adapter refusal; 59 distinct first-refused expressions. This is an incomplete cutover gate. |

Sampled peak for these fresh runs: 3467.6 MiB. Very short cached runs can be
below the guard's sampling interval; their near-zero samples are not actual
zero-memory claims. Feature-on executor previously passed 1402/355/6/2 with
184 integration ignored in `probe-error-executor.log` at this code revision.
Repository `make -j1 lint` passed in `probe-error-lint.log`; this is Go
revive/dashboard checking, not Rust clippy or SQL equivalence.

### Important meaning of the engine-only environment variable

`TIKV_EXPR_ENGINE_ONLY=1` affects `tidb-expr/src/tests/mod.rs::chunk_e`:
that helper still computes the native oracle first and compares it with the
engine. The variable turns adapter refusal into a panic instead of skipping the
engine comparison. It does **not** remove native code or force every unit test
through the engine. Therefore neither the 1163 passing tests nor the difference
between passing-suite counts is an engine execution count.

Each failing test stops at its first refused expression. The 59 distinct strings
are a lower bound on missing corpus shapes, not a complete inventory. Examples
include `benchmark(-3, 1)` and `cot(1)`; additional families include temporal and
implicit casts, binary literals, collation-sensitive lookup, regexp and JSON.
Keep refusal/error/value-divergence classifications separate when fixing these.

## Exact reproduction

From `expression-reuse/tidb/rust`:

```bash
export CARGO_HOME=/home/agent/tidb/expression-reuse/cargo-home
export RUSTUP_HOME=/home/agent/tidb/expression-reuse/rustup-home
export RUSTUP_TOOLCHAIN=nightly-2026-08-22
export PATH="$CARGO_HOME/bin:$PATH"
export CARGO_BUILD_JOBS=1 MALLOC_ARENA_MAX=2 RUST_MIN_STACK=4194304
export CMAKE_POLICY_VERSION_MINIMUM=3.5
export CXXFLAGS='-w -std=gnu++14 -include cstdint' CFLAGS='-w' RUSTFLAGS='-Awarnings'
# Run each command serially; preserve each exit status.
python3 ../../tools/limited-run.py --rss-mib 8192 --as-mib 16384 -- cargo test -q -p tidb-executor --locked --offline -j1 -- --test-threads=1
python3 ../../tools/limited-run.py --rss-mib 8192 --as-mib 16384 -- cargo test -q -p tidb-expr --features tikv-expr --locked --offline -j1 -- --test-threads=1
python3 ../../tools/limited-run.py --rss-mib 8192 --as-mib 16384 -- python3 scripts/tikv_expression_runtime_gate.py
python3 scripts/tikv_expression_coverage.py --self-check --check
# Expected to fail until remaining admitted-shape gaps are fixed:
TIKV_EXPR_ENGINE_ONLY=1 python3 ../../tools/limited-run.py --rss-mib 8192 --as-mib 16384 -- cargo test -q -p tidb-expr --features tikv-expr --lib --locked --offline -j1 -- --test-threads=1
```

Next priority: fix one refusal family at a time using a failing engine-required
fixture, engine-side semantics where needed, and narrow signature/shape admission;
rerun the refusal-intolerant corpus after each change. Do not lower the gate or
relabel native fallback as coverage. Hosted CI, fresh Go/mysql replay, broad
performance comparison, package-transcreation completion and native deletion
remain unverified/not achieved.
