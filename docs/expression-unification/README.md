# Expression unification experiment

Checkpoint-ID: `uuid-translate-six-41` (previous: `format-time-three-40`)
**137/245 frozen families delegate with native evaluator algorithms removed; target 221.** Added: IS_UUID, UUID_VERSION, UUID_TIMESTAMP, UUID_TO_BIN, BIN_TO_UUID and TRANSLATE. Strict final-audited acceptance remains **0**; incomplete and not PR-ready.
Paired branches: [TiDB](https://github.com/YangKeao/tidb/tree/expression-unification-demo) · [TiKV](https://github.com/YangKeao/tikv/tree/expression-unification-demo). Parent owns publication pins, Plan mirrors and paired pushes; no force-push or automatic PR.

## This checkpoint
- **Scope:** 15 Rust files (TiKV 10/native 5), zero metadata commands, manifest changes or changes to either lockfile. Native UUID parse/hex/swap/format/timestamp/decimal_micros algorithms and both TRANSLATE maps are deleted. Parent proves both complete native test modules, seven host generation/hash/coercion bodies and scalar_function.rs/arg_eval_type.rs unchanged. UUID/UUID_V4/UUID_V7 generators merely share formatter/epoch exports from the tidb_query_expr root; generation gains no credit.
- **UUID policies:** native 32/36/45-byte forms, case-insensitive URN and arbitrary 38-byte wrappers select their payload before using the existing uuid-crate decoder; hand-written hex decoding is removed. Formatting uses Uuid::hyphenated. Native signed v1/v6/v7 timestamps truncate toward zero to exact six-place decimals; other versions return NULL. The final decimal shift/round is shared with wire, without replacing wire parsing or unsigned timestamp arithmetic. Version/Timestamp retain original Unsupported/1105, not falsely upgraded Go 1411.
- **Demand:** UUID_TO_BIN parses once in its first complete call, transports actual 16-byte output, then quietly coerces the optional flag in a second complete call. NULL/error skips the flag; this is not TIME_FORMAT's double parse. Two leases/result transport and two one-shot workers without capability are explicit costs; first admission precedes flag demand. BIN_TO_UUID keeps flag warning/coercion before the payload, including NULL, and 1411 carries its actual input.
- **TRANSLATE:** UTF-8/byte kernels preserve first-occurrence wins and deletion. Any binary argument selects the signature, but only arg 0 selects output charset. Original ordered coercions and NULL stopping occur before capability discovery/scope guard; not all coercion is guarded. An actual NULL uses NullWitness(None), without fake NULL suffix operands; successful inputs use the existing Bytes3 array carrier.
- **Protocol:** nine operations (six UUID, three TRANSLATE), existing roles and results only: two Int, one Decimal, six Bytes. Swap accepts exactly Some(16 bytes)+Some(flag). Five operation-sealed typed causes use borrowed actual bin_to_uuid_input, not reparsing or string/error-code classification. No new Args/result/report/driver/module or native PB/cop admission.

## Actual validation
| Receipt (`uuid-translate-` prefix) | Result | Compile / run seconds |
|---|---|---|
| tikv-local.log | **Compilation failure: three E0061 errors; no tests; exit 101** | — |
| tikv-local-retry.log | 272 passed, 1 existing ignored, 509 filtered | 10.36 / 0.19 |
| tikv-misc.log | 22 passed, 760 filtered | 0.13 / 0.00 |
| tikv-string.log | 66 passed, 716 filtered | 0.12 / 0.02 |
| native-misc.log | 13 passed, 1559 filtered | 15.02 / 0.00 |
| native-string.log | 1 passed, 1571 filtered | 0.16 / 0.00 |
| native-tables.log | 1 passed, 1571 filtered | 0.12 / 0.01 |
| dispatch.log | 4 passed, 1568 filtered | 0.15 / 0.00 |
| sql.log | 89 passed, 2078 filtered | 28.41 / 1.42 |
| expr-full.log | **1474 passed, 4 old failures, 94 ignored; 1572 total; exit 101** | 0.17 / 10.39 |
| unistore-full.log | **195 passed, 1 old failure, 13 ignored; 209 total; exit 101** | 6.66 / 3.01 |

**11 test-Cargo attempts = 10 runs actually executing tests (8 green + 2 current old full-suite failures) + 1 compilation failure.** One failed-target retry; no new test RED, zero-match or launch failure. C's three new local-test Bytes3(a,b,c) constructors caused E0061; parent corrected only to Bytes3([a,b,c]), without expectation/business changes. Overall checks did **not** all pass first try.
**This round requires no address mapping:** both complete failure sections equal format-time after numeric thread-ID normalization only. Expression SHA `25f964887541f45602363f00dc60df4f0af21c74edccc16c38d0804094fb9bff`, duration.rs:164:55; unistore SHA `e285bfdba646f2d01f85b485ae317cc07c51b78cf0ae6d664c0fb2bc39259759`, line 194:5. The former 164→212 mapping and `80be9...` digest belong only to format-time's historical comparison with construct-time.
All 15 pinned formatter --check runs and both diff checks passed (check failures 0). Separately, one formatter invocation targeted `tidb/rustates/nonexistent.rs`, exited 1 without touching files, and succeeded after one corrected invocation; do not call formatter failures zero. One static-proof check guessed nonexistent func_prop.rs; after glob confirmed absence, the corrected proof checked only real scalar_function.rs and arg_eval_type.rs byte equality, with no source repair.
Three precompile interface corrections are separate from the actual C compilation failure: B's Bytes3 tuple→array; B's formatter/epoch import corrected from datatype to expr root; 7's EvaluateError import corrected from root to error module. Three ordinary read-only lookup errors: parent's coverage-baseline path omitted evidence/, 7 guessed local/error.rs, and A guessed codec/mysql/json/native.rs during the earlier JSON review. No old or new test expected values changed this round.
Three new SQL tests include 12 direct zero-slot calls plus NULL, invalid UTF-8 and preparation-precedence cases. Four dispatch tests exercise all nine real workers, AST/typed routes, zero/closed pools, format metadata and five causes. Ten new TiKV tests comprise four UUID, three TRANSLATE and three local tests. New literals are derived from original source policy, not a provider oracle or relabeled old fixture; the original row/chunk TRANSLATE table test also ran.
Filters: TiKV tidb_query_expr --lib local::/impl_miscellaneous::/impl_string::; native tidb-expr --lib builtin_ext::misc::tests, builtin_ext::string2::tests::translate, test_translate_tables, uuid_translate_dispatch_; SQL tidb-session --lib evaluated_ascii_. Runs use --locked and --test-threads=1 with pinned tools/quiet tee; exact commands: [summary](logs/uuid-translate-summary.txt), [evidence](evidence/uuid-translate-checkpoint.md), `checkpoint.json`.
Historical parser-all three-E0061 failure remains unresolved and unrun; auth_shared's 20-pass receipt is historical. No parser/auth gate is claimed this round, and format-time's two compile failures/new test RED/zero-match are not current counts.

## Remaining work
DATE, strict MICROSECOND, JSON_LENGTH and later temporal/JSON-path work remain uncredited. JSON paths require original serde/text and path-policy closure, not blind BinaryJson/parser substitution; JSON_PRETTY retains compact/float-format locks. Legacy capability propagation, operation scopes, allocation/peak, differential reruns, release/profile gates, old duration-panic adaptation, parser all, workspace, make lint, M6 and TiFlash remain unfinished. No whole-package/type, performance or OOM-safety completion.
