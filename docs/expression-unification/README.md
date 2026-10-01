# Expression unification experiment

Checkpoint-ID: `regexp-foundation-44` (previous: `vector-eight-43`)
**151/245 functional families, target 221; strict final-audited acceptance 0 — unchanged.** This checkpoint shares regexp leaf algorithms only. It does **not** migrate REGEXP_LIKE/SUBSTR/INSTR/REPLACE to TiKV evaluators; no family is added. Incomplete, not PR-ready.
Paired branches: [TiDB](https://github.com/YangKeao/tidb/tree/expression-unification-demo) · [TiKV](https://github.com/YangKeao/tikv/tree/expression-unification-demo). Parent owns publication pins/Plan mirrors/paired pushes; no force-push or automatic PR.

## Shared foundation, not worker activation
- **Scope:** five Rust files: TiKV regexp_policy.rs (new), lib.rs, impl_regexp.rs; native src/regexp.rs and builtin_ext/regexp.rs. No manifest, dependency or lockfile changes; metadata/lock-resolution commands 0. All five pinned formatter checks, both diff checks and three complete original test-module byte proofs passed. Formatter/static-proof failures 0; no original assertion or expected-value changes.
- **Six helpers:** shared match-flag reduction, character-position trim, substring matching, INSTR character counting, replacement tokenization and replacement matching. Group/Literal instructions share one type; typed errors retain actual flag, position/count, capture number and Utf8Error payloads. Native maps them to original static Unsupported messages; wire retains dynamic diagnostics, without string classification.
- **Policy:** raw occurrence normalization occurs only in the leaf; prefix concatenation and capture replacement traversal are shared. NativeUtf8 validates every selected replacement immediately, preserving error precedence; WireBytes retains raw output. Shared HashSet flag scanning preserves rightmost i/c, m/s and initial case-insensitivity, while native RegexBuilder and wire inline-HashSet compilation remain distinct representations.
- **Demand/cache:** frontend empty-pattern/UTF-8/NULL/coercion/compile/return_option order remains distinct. Native double/triple string-coercion tuples fully evaluate before testing NULL; all three positional paths trim before compile, while wire compiles before positional checks. REPLACE compiles before resolving cached instructions. Original context_id/constness, success/error/replacement memoization and clone invalidation remain. Public native statistics retain UTF-8/error→None behavior; PB/legacy allow-empty and fallback policies are untouched.
- **Boundary:** native frontends still call shared leaves directly. No new closed operations, Args, result carrier, driver, six-slot extension, cache protocol or PB admission. This is not four evaluator-family migrations, deletion of all native regexp behavior, whole-domain/Go-package completion, or allocator/OOM/performance proof.

## Actual validation
| Receipt (`regexp-foundation-` prefix, `.log`) | Result | Compile / run seconds |
|---|---|---|
| tikv (`tidb_query_expr`, `regexp`) | 7 passed, 790 filtered | 8.03 / .04 |
| native (`tidb-expr`, `regexp::tests::`) | 7 passed, 1571 filtered | 10.82 / .06 |
| cache (`tidb-expr`, `regexp_cache_identity_by_statement_context`) | 1 passed, 1577 filtered | .17 / .00 |
| sql (`tidb-session`, `regexp_`) | 2 passed, 2171 filtered | 23.90 / .04 |
| expr-full | **1480 passed, 4 old failures, 94 ignored; 1578 total; exit 101** | .19 / 10.60 |
| unistore-full | **195 passed, 1 old failure, 13 ignored; 209 total; exit 101** | 5.69 / 2.99 |
**Six Cargo attempts, six nonzero-test runs: four final focused green + two current old full-suite failures.** All focused gates passed first try, but overall checks did not all pass first try: full suites remain non-green and source-tool failures occurred. Compile/new RED/zero-match/launch failures, retries and supplemental replays 0.
Both complete full-suite failure sections equal vector-eight after **numeric thread-ID normalization only; no address mapping**: expression `25f964887541f45602363f00dc60df4f0af21c74edccc16c38d0804094fb9bff`, duration.rs:164:55; unistore `e285bfdba646f2d01f85b485ae317cc07c51b78cf0ae6d664c0fb2bc39259759`, 194:5. Vector's two compile failures, omitted-assertion restoration/static-proof failure and presentation incident remain historical, not current counts.
Source-tool incidents: two ordinary lookup errors (7's incorrect native root read; A's nonexistent scalar_function/vectorized_eval.rs grep, then corrected discovery). 7 and B each had one file-not-read edit observation-precondition failure; fresh read and the original edit recovered both: failures 2/retries 2, not sandbox or compile failures. Literal mismatch, presentation failure and ABI-guess correction counts are 0.
Only three hand-derived leaf tests are new. Existing coverage is wire 4, native 7, cache 1 and SQL 2; SQL checks existing LIKE/predicate rows only, not new positional SQL or new metadata assertions. Exact commands and all six raw receipts: [summary](logs/regexp-foundation-summary.txt), [evidence](evidence/regexp-foundation-checkpoint.md), `checkpoint.json`.

## Remaining work
Cache-aware workers still need a real contract: shared cache ownership with native Clone resetting it, private typed metadata and bind/unbind preventing cross-operation reuse leaks, lazy initialization/write-back of misses in original order (not snapshots alone), and unit-payload/observe_storage accounting. None is implemented here; these gaps neither imply current support nor demand unbounded measurement before the demo.
Arithmetic/JSON proposals remain read-only/deferred, as do FORMAT locale/numeric/error-1649 closure, DATE, strict MICROSECOND, legacy capability propagation, operation scopes, allocation/peak, differential and release/profile reruns, duration-panic adaptation, whole workspace, make lint, M6 and TiFlash. Historical parser-all three-E0061 remains unresolved/unrun; isolated auth_shared 20-pass is not a current gate. No final performance/OOM-safety completion.
