# Regexp closed-worker checkpoint

Checkpoint `regexp-worker-45`, following `regexp-foundation-44`. Four frozen families (`regexp_like`, `regexp_substr`, `regexp_instr`, `regexp_replace`) advance functional delegation/native deletion **151 → 155 / 245**. REGEXP/RLIKE are aliases, not extra families. Strict final-audited count remains **0**, target 221; incomplete and not PR-ready.

## Implementation and policy
- TiKV `native_regexp.rs` owns the generic context cache, native compiler and typed errors. Native `builtin_ext/cache.rs` is a complete alias. Owner Clone resets; explicit invocation Clone shares live state. New/clone/bind do not look up, initialize, clear or change context. Same-context literals/errors/replacements remain memoized; false flags bypass without clearing; failed constructors do not install. Poison recovery and independent lazy context replacement remain.
- Four kernels borrow actual resolved cache Arcs, including failures, rather than detached snapshots or cloned Regex objects. Native trim → compile → replacement resolution and per-selected expansion UTF-8 validation remain distinct from wire policies. Caller coercion/collation/NULL/error order remains unchanged; typed native causes restore original static diagnostics without text classification.
- Exactly four operation/role/kind combinations allow typed metadata and 3/5/6/6 real SQL slots. Invocation handles are metadata, not fake control Bytes. An RAII guard binds after poisoning and unbinds before postflight; ordinary failures release handles, unwinding cannot heal poison, and dirty state cannot return to the pool. The existing driver and ordinary PB admission are unchanged.
- Invalid INSTR return_option alone authorizes undemanded flags, with worker validation before reading them. Early NULL uses a real witness. Legacy's two children retain their original demand and CI/Bin/invalid-UTF-8/empty/bad-syntax policy. Missing children use a ninth genuine NoArgs recipe, never a manufactured NULL. Collator comparison is demanded only with two actual non-NULL values; extra third legacy children remain undemanded.
- Native boolean helpers, AST predicates and existing typed/PB callers delegate; public statistics remain a pure shared Option SDK helper, explicitly not a pooled SQL route. No native matching/compilation/trimming/replacement fallback remains.
- Fixed holders and observed known cache structures/parts/error-buffer capacities use checked accounting at actual use sites, without re-reading potentially replaced slots. Opaque engine/TLS storage, Arc/allocator headers, transient uncached engines and allocation peaks are excluded from this partial logical accounting; M6/full heap/OOM/performance remain deferred.

## Actual validation and corrections
Exact commands, all twelve whole-log SHA256 receipts and full results: [summary](../logs/regexp-worker-summary.txt).

| Final gate | Result |
|---|---|
| TiKV regexp / local | 16 passed / 279 passed, 1 ignored |
| Native original regexp / cache / new dispatch | 7 / 1 / 2 passed |
| Session regexp / legacy regexp | 4 / 2 passed, after retries |
| Full expression | 1482 passed, **4 old failures**, 94 ignored |
| Full unistore | 197 passed, **1 old failure**, 13 ignored |

Twelve nonzero-test Cargo runs: seven final focused green, two new-fixture reds, one existing instrumentation mismatch, and two final known-baseline non-green full runs. Three failed-target retries; compilation/zero-match/launch/no-test failures 0. Not all first-pass.

The new SQL fixture wrongly expected binary metadata for integer outputs; the unchanged collation derivation stamps utf8mb4/Utf8Mb4Bin onto all eight results. The new PB fixture wrongly expected legacy conversion despite the original Shared-first decoder. Both corrections derive from unchanged source, not provider recording. PB keeps AbC/abc=1 and explicitly uses its existing test scope override; this does not close the production default-NoColumns request-root gap. One old NOT REGEXP instrumentation expectation changes from one facade to two independent REGEXP/NOT workers, retaining SQL Int(1). No production behavior was changed to satisfy these assertions.

Both complete final full-suite failure sections equal foundation after numeric panic-thread IDs alone are replaced with `(<id>)`: expression `27654f2c0ad2971242c00f50182467598cdfd9c2c65e5bf5a32e683708448637`; unistore `b64dcced405c699c11b72ee31888c58e491b4e5b4e74fa2d445454e1d1e146e9`. No address remapping. Duration remains `164:55`, unistore `194:5`. Historical digest markers/receipts remain historical.

Twenty-two Rust sources (TiKV10/native12), no manifest/dependency/lock changes. All final pinned formatter/diff checks pass; one first formatter check needed a second formatting pass. Three whole original regexp test modules are byte-identical. Fifteen new test functions cover cache identity/clone/reset/miss/error, actual worker reuse, lifecycle/overflow/unwind, policy and SQL metadata/demand/resource boundaries. C's two edit-match failures and one restored test-name typo are disclosed; no observation-precondition or presentation failures. Existing SQL expected values and recorded fixtures are unchanged, but the separate structural counter assertion was intentionally updated.

## Limits and next work
Whole workspace/Go packages, lint, release/profile, physical allocation/peak/OOM, performance/zero-copy, 150-row differential, M6 and TiFlash remain unverified. FORMAT locale/numeric closure, DATE/MICROSECOND parser policy and remaining arithmetic/JSON families are still work. The next candidate is a small closed arithmetic or JSON batch with the shared datatype/formatter layer settled first. Both repository Plan mirrors and exact paired commit are pinned in `checkpoint.json`; no force-push or automatic PR.
