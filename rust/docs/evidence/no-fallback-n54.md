# N54: public scalar entries use the shared SDK driver

Based on TiDBbeffa9e48a and unchanged SDK3ee7511. Expression.eval, Expression.eval_with_error_value and ScalarFunction.eval now borrow the genuine caller root, compile its existing native source plan, and execute through LocalBatchProgram and its shared host frame. They retain the actual Row and Columns context. No substitute expression, Suite, Chunk, SQL declaration or interpreter fallback is constructed.

## Production behavior and independent reference

The already validated PhysicalRow/scalar consumer and existing opt-in error-Datum channel are promoted from test-only code. Ordinary evaluation drops an accompanying error Datum; the pair API retains the actual converted-input or cached Constant Datum according to the existing contract. Native error ownership is preserved, and framework errors retain preparation versus invocation phase. Direct functions keep their own result-family normalization without an enclosing Expression enum/set view.

Before changing default routing,136 core/evaluator source files were archived atbeffa9e48a. The exact old bodies of the three public entries were copied under cfg(test), verified against that archive, and explicitly exercised in five passing original-reference/scope tests. A routing test then reproduced the old default's failure to enter the shared driver. After promotion, the old scalar recursive dispatcher and integer fast driver are cfg(test) only. The original scope is thread-local, defaults off, and restores after nested calls and unwinding; that oracle module and its public-entry guards do not exist in production.

New comparisons outside the four unchanged historical observer prefixes execute99144 fixed records once through the explicit original recursion and once through default public routing. They assert exact historical hashes and record equality, no shared public entry while taking the old reference, and exactly one shared entry per default public root with no old entry. The ordinary, error-Datum, enum/set, actual protobuf, RAND/clone ownership, cache and genuine-row contracts remain unchanged within their documented policies.

Historical source tests also need an independent original reference after public routing changes. Only110 expected/reference callbacks in16 test files now use an explicit original-reference helper; actual/got candidates retain their SDK execution. Five additional callbacks in three untouched legacy control files explicitly select the old phase counter or recursive diagnostic renderer they were designed to observe. These three files were verified byte-for-byte againstbeffa9e48a and archived before modifying them. Forward reconstruction against all archived originals verifies that inputs, loops, assertions and golden values are unchanged; formatting accounts for added closure blocks. Existing original eager/diagnostic counters are never incremented to imitate old dispatch in the new route.

## Resource contract and performance scope

The public adapter uses existing CompileLimits and SDK ExecutionLimits defaults. Metadata and native value allowances use that existing64MiB execution allowance; Projection's1MiB source policy and explicit test/caller limits are unchanged. A complete graph exceeding the native node policy returns Resource before execution, even when uncapped old COALESCE could have returned its first argument. The control test proves that result is terminal, invokes no old evaluator, and that truncating the same real function's arguments permits a later successful public call. Resource failure is not an invitation to bypass the limit.

This is a compile-at-call-boundary adapter. It preserves current mutable caller metadata and moved/clone function ownership rather than installing an unchecked permanent cache. It is a functional migration step, not a final standalone scalar performance claim. Actual filter/Unistore callers should prepare once and reuse over their rows; subsequent service and final performance work remains required.

## Validation and limits

From the TiDB worktree:

    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib public_ -- --nocapture --test-threads=1
    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib
    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb check --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr
    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-executor --lib projection::tests
    make lint

Public48/0, including all four explicit old/default corpora and the default-routing control, passed after production promotion. The final routing/node-budget controls pass2/0. Initial source-test reference wiring failures were corrected and all ten affected tests passed individually without changing production algorithms. A deliberately guessed large-Bit budget fixture was discarded when actual execution correctly returned16.00EiB; the deterministic native-node-policy fixture above is the final control.

The last complete frontend run on the current production implementation finished2272pass/8fail/95ignore in865.50seconds, all733 batch tests passed, and aggregate346warnings remained unchanged. Three failures are the inherited ifnull catalog, EXP overflow expectation and vectorized operation source failures. The other five were legacy eager-phase/recursive-diagnostic controls still observing the newly changed public entry. After changing only those test reference call sites, the two eager-policy controls and remaining three original controls all passed separately, with their original hashes intact. The complete suite was not rerun after those test-only fixes; this is combined full-plus-focused evidence, not a claim of a later clean full run.

Production-only check passes with the oracle and old scalar dispatcher excluded. Actual Projection52/0 passed in222.73seconds; no production source changed after that run. Fresh focused context43/0, Expand6/0 plus unchanged64/B42FB8BE316E6C79, bounded inventory83088/0/0, final lint, both pinned nightly rustfmt checks for all30 Rust files, source ownership and reconstruction checks pass. SDK3ee7511 remains clean, so its N53 full1150/exact inherited1/1ignore/17warnings and clippy171/89 evidence is retained as prior unchanged-SDK validation.

Workspace n54-final-check.py --gates verifies actual public routing, three exact original entry bodies only under cfg(test), four frozen observer prefixes, original source archives, compiler/host promotion without algorithm changes, default-off oracle controls,115 reference-only callback changes,30 reviewed source hashes, exact full-run failure set and focused repairs, and protected BUILD/SDK ownership. No push, PR or release.

Evidence limitation: reusing the N53 context runner once accidentally overwrote its historical raw context log/JSON. Those paths now contain provenance tombstones; the original43/0 result remains recorded in the signed N53 receipt and its pre/post-commit execution journal. Fresh N54 context results are stored under N54 names and are not represented as N53 raw evidence.

## Remaining work

The generic Admission-to-run fallback and remaining actual caller/typed/AST paths are not globally retired by this change. Continue actual caller integration and prepared-plan reuse, remove remaining fallback/old driver routes, validate the complete service and measure final unified performance. The overall task remains in progress.
