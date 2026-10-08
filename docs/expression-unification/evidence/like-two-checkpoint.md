# LIKE/ILIKE shared-worker checkpoint

Checkpoint `like-two-48`, following `binary-three-47`: **162/245 functional families; strict final acceptance0**. Two families added, not five recipes counted as five families. Target221; 83 eligible families remain. This is not final acceptance or PR readiness.

## Review map

Shared TiKV:
- `components/tidb_query_datatype/src/codec/collation/pattern.rs`: original ASCII lowercase, alphabetic-escape walk and const byte width; checked actual compiled Vec capacities. Existing matcher loops/wire policies stay unchanged.
- `components/tidb_query_expr/src/native_like.rs` (new): sole compiled LIKE/ILIKE wrappers, ASCII policy, pure SDK, legacy policy and lazy invocation cache handles.
- `impl_like.rs`: five generated closed kernels; original wire function/test bank retained.
- `native_regexp.rs`: only `share_state` becomes crate-visible; no other byte changes. `lib.rs` exposes the narrow SDK/types.
- `local/{batch,compile,registry,mod,tests}.rs`, `types/{function,expr_eval}.rs`: exact operation/role/metadata admission, binding guard, known-storage observation and rejection of ordinary private-signature compilation.

Native TiDB (under `rust/crates/`):
- `tidb-datatype/src/collation.rs`, `tidb-util/src/stringutil.rs`: shared reexports and thin utility delegates, not another lowering loop.
- `tidb-expr/src/{like.rs,lib.rs,scalar_function.rs}`: pure SDK facades/compiled aliases plus actual scoped AST and typed routes.
- `tidb-expr/src/tikv/{ready_value.rs,ready_value_tests.rs,mod.rs}`: actual owned results and narrow legacy SDK; `tests/ilike_info_cast_source.rs` changes constructor inputs only to explicit `native_policy()`.
- `tidb-unistore/src/cophandler.rs`: original legacy child/coercion demand, actual raw bytes and typed infrastructure errors; no local matching/folding.
- `tidb-executor/src/{predicate_pushdown.rs,lib.rs}`: real context/fallibility for fast LIKE/NOT LIKE, existing facade reexports rather than a new session dependency.
- `tidb-session/src/{show.rs,show_statistics.rs,tests_core/lifecycle.rs}`: real statement context and errors, including SHOW WHERE. Statistics edits are eight private helper bridges. The row resolver forwards only scope/execution and preserves its other defaults.

27 Rust files, TiKV12/native15; one new Rust module. No Cargo manifest, dependency or lockfile changes. Both ownership guides and the generated Plan snapshots accompany the sources.

## Protocol and preserved policies

| Recipe | Actual operands | Metadata | Output |
|---|---|---|---|
| LikeNative | nonNULL Bytes, Bytes, Int(u8-normalized) | live native LIKE invocation | OwnSignedInt |
| IlikeNative | same three | live native ILIKE invocation | OwnSignedInt |
| LikeLegacyNative | same three | original legacy case policy | OwnSignedInt |
| LikeNullIntNative | actual NullWitness(None) | unit | actual NULL |
| LikeMissingLegacyNative | genuine NoArgs | unit | actual NULL |

The invocation is **not a fourth SQL operand**, callback, warmed cache or host match result. Exact kind/op/role checks reject NULL value packets, nonnormalized escape and mismatched holders. A separate typed RAII guard binds only for the real generated wrapper and clears on success/error/unwind. The established driver, dispatch witness, factory bounds and regexp tuple/binding protocol remain unchanged.

Cache constructors and invocation Clone only share live state; owner Clone resets. The wrapper resolves the actual cache once and measures the same Arc, preserving old same-context hits even with changed incoming bytes and successful new-context replacement. Dynamic calls retain no compiled cache. Known storage includes the actual compiled value and Vec capacities, not cache lock/Arc allocation headers or transient folded buffers. Checked overflow/refusal is infrastructure, never SQL overflow or false. This does not establish M6, physical-heap, peak or OOM safety.

AST still eagerly demands both children; typed evaluation stops sequentially on NULL/error, preserves escape cast-to-u8 and only reads context id at the original cache-enabled frontier. Legacy still demands target/pattern/escape before NULL/missing classification. A complete tuple alone selects the original collator `compare(a,A)` case policy. The shared legacy kernel retains invalid UTF-8→empty, Go simple Unicode lowercase and Reject trailing escape; modern LIKE uses original native collation/Literal trailing behavior, ILIKE uses ASCII lowering and its alphabetic-escape policy. Native Binary is byte-oriented; the original other-collation→Utf8Mb4Bin ILIKE mapping remains, including its known GBK/GB18030 gap.

Scan filters consume actual nullable results before WHERE rejection; their existing predicate negation is preserved. SHOW keeps Unicode preprocessing, original collations and privilege/LIKE/WHERE order. Real scope errors propagate, never become false. Public bool/statistics utilities intentionally use the pure shared SDK and claim no C4 receipt or scope limit. No dedicated vector tier is invented; existing selected-row fallbacks use the migrated evaluator, and Go-only ignored gaps stay ignored. Ordinary PB LikeSig/IlikeSig refusal, wire LikeSig charset/collation/u32 escape and absent wire ILIKE remain unchanged.

## Measured validation and repairs

[Exact commands and all15 whole-log hashes](../logs/like-two-summary.txt); machine ledger in `../checkpoint.json`.

- Nine final focused green runs: shared pattern5, native utility13, TiKV LIKE10, local287 (1 ignored), native LIKE61 (9 ignored), legacy1, SQL2, executor1, NOT instrumentation1.
- Two compile failures are not counted as tests: legacy observer result type/exhaustiveness, then session's nonexistent direct `tidb_expr` dependency. Fixes used a generic test observer, exact helper refusal arms and the existing executor facade; no new dependency or SQL expected value.
- Actual legacy RED→GREEN: a NULL target/pattern with nonNULL escape was mistakenly encoded as a nonNULL witness. SDK correctly refused it. The corrected branch uses the already-observed tuple NULL after all original demands; tests and expectations unchanged.
- Full-expression initial RED: old `NULL NOT LIKE '%'` instrumentation expected one facade, while migrated LIKE then existing NOT now make two. SQL NULL was already correct. Only the count/snapshot mode changed; focused recovery and a full confirmation followed.
- Final expression **1488 passed, 4 old failures, 94 ignored; exit101**. Final unistore **199 passed, 1 old failure, 13 ignored; exit101**. Entire failure sections equal binary-three-47 after numeric panic-thread IDs only, with no address mapping: `27654f2c0ad2971242c00f50182467598cdfd9c2c65e5bf5a32e683708448637` / `b64dcced405c699c11b72ee31888c58e491b4e5b4e74fa2d445454e1d1e146e9`.

Fifteen Cargo attempts, thirteen nonzero-test runs; no zero-match, launch failure or fixture recording. Twelve new focused test functions. Existing constructor changes are ABI-only; SQL value expectations remain unchanged. Original TiKV LIKE test module is byte-identical (`491b50b924ef6589f51a778bffc9c011d717b98c553d1f1b4fbc4a4ed8ea65d9`). Pinned formatter checks cover all27 source files; both diff checks pass.

The original native Go-simple lowercase table and existing TiKV leaf have the same328 upper/lower ranges. Read-only source parsing plus independently translated source algorithms checked1,112,064 valid scalars with zero lower differences. This disproved an initial unverified old-table suspicion; no negative test was fabricated and no table/generator/dependency was copied or changed. It is static source evidence, not execution of Rust or an entire Go package. Raw static receipt hash: `0de9495283a67301c4473053a3ea27da7195e3d64d53693d13cdf81382a944aa`.

## Limitations and next work

No `make lint`, `make dev`, `make bazel_prepare`, whole workspace, release, performance/zero-copy, allocator peak/OOM, 150-row differential, M6, TiFlash handshake or whole Go-package/type-domain equivalence claim. Existing parser-all/full-suite failures and prior extreme Decimal shape exception remain. Broader default-NoColumns request-root integration is not completed by these particular SHOW bridges.

MOD/DIV are read-only next candidates; IntDIV requires warning-before-integer-conversion preservation, not a narrow native replacement or wire policy substitution. JSON renderer closure, FORMAT/DATE/MICROSECOND and the remaining eligible families are not credited. Previous160 family entries remain unchanged. The paired TiKV commit and identical Plan SHA are bound in `checkpoint.json`; the native commit is intentionally not self-referential.
