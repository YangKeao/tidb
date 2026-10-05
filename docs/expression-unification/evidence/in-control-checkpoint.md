# IN control ownership over existing comparison workers

**in-control-106 / R109**, continuing [typed and legacy IN](in-typed-legacy-checkpoint.md).
Functional **238/245 (97.14%)**, strict **0**, remaining **7**. Only `in` is added; all237 prior family objects and partial/type histories remain unchanged. The remaining native membership algorithms move to a synchronous SDK control service, not a new RPN profile.

## Two explicit execution layers

SDK `native_in_control.rs` owns traversal, NULL/match reduction, row equality and prepared-string cache policy. Its callbacks execute the original native child/coercion adapters and existing SDK comparison workers. AST negation still uses the existing SDK UnaryNot worker.

There is no hidden Head, observer bypass or second comparison invocation. AST `1 NOT IN(1,2)` remains Eq, Eq, NOT—three real facade entries. Empty-list and pure-cache paths retain their original zero-comparison behavior and admission semantics. This checkpoint does not invent a new root-resource gate for them.

The native adapters classify actual comparison datums as Null, raw Int(i64), or Other; they do not precompute matching or NULL reduction. SDK Null/Bool results and structural errors are projected back without changing child error identity.

## Distinct policies retained

- **AST scalar:** evaluate the mandatory left operand once, then evaluate/compare every RHS, including after a match or NULL. Empty lists return false even for a NULL left value. Optional NOT remains the original SDK operation.
- **Ready values:** arguments are already available; comparison stops at the first match. A later invalid comparison is not demanded.
- **Generic typed:** every dynamic candidate is evaluated and compared. Original casts, derived collation, actual operand metadata and each fresh `div_precision_increment` read remain. The stored cache NULL flag seeds the result independently, even when the key set is absent.
- **AST rows:** prepare the whole left row; each actual RHS row is fully evaluated before width checking. A non-row candidate errors before its child evaluation. The SDK field loop continues after NULL but stops at the first false comparison; outer candidates remain exhaustive. Equal empty rows need no fabricated comparison witness.

SDK `native_row_equality` is the single field-reduction implementation used by both row IN and native `row_eq_in`. Other non-IN row operators and their common structural dispatch remain outside this step. Row leaf comparisons retain their original derivation-free collation, literal operand metadata and no division-precision getter.

## Real prepared cache, unchanged storage

SDK performs cache eligibility, full-RHS capacity reservation, strict-level==2 literal selection, key insertion, NULL bookkeeping and ordered dynamic-index construction. Lazy metadata callbacks preserve name/count checking before first-type inspection, then effective-collator resolution before argument facts. Ineligible preparation does not clear old fields.

Native nodes keep their actual standard HashSet, NULL flag and index vector. Existing invalidation still clears all three; original private length/capacity tests remain meaningful. No fake cache, new cache transport, reconstruction under a later collation mode, or host-computed `contains` result is introduced.

At runtime SDK requests original string coercion first, resolves the current effective collator only for non-NULL bytes, and chooses original borrowed raw-key versus allocated-key lookup. A cached match skips work only when no dynamic suffix exists. Otherwise every stored dynamic index is compared, preserving later warnings/errors and duplicate/index order.

## Existing domains and boundaries

R108's four typed temporal/JSON and legacy Int/String profiles remain unchanged. Typed evaluation/cast preparation still precedes membership; legacy full-i128/error-folding/demand/late-collator policies remain distinct. Shared PB still admits no IN signature, and the existing recursive UnaryNotInt(IN) refusal is not widened or repaired.

A focused independent source review found no additional runtime IN reducer. Planner null proofs, constant propagation, row/heterogeneous rewriting, producer construction and ranger logic are not new runtime evaluator domains. No whole-comparison, Go-package or M6 completion follows from IN ownership.

## Validation

[Exact commands/counts/hashes](../logs/in-control-summary.txt): twelve actual locked single-threaded launches, all nonzero GREEN; no failed, zero-match or interrupted run. SDK core1, native control1, unchanged gateway196 (one existing ignored test), private cache2, prepared rebind1, original IN source4, row3, expanded bridge2, original row/IN vectors1, real legacy mapper1, new SQL1 and R108 typed SQL1.

Three new explicit tests (SDK1/native2); original233 native test bodies in the six changed Rust files remain byte-identical. The two changed SDK files contained no old explicit test bodies. `evaluated_ascii.rs` and its complete test file are unchanged; the old three-facade test was executed and passed. No provider output was used as an oracle.

After the first native control receipt, the newly added test was extended to exercise the actual ready-value dispatcher and row false-first versus full-field-evaluation boundary. The expanded bridge run executes that final test plus R108's existing bridge test; it is supplemental validation, not a retry after failure. The new SDK module's import path was corrected during static preflight before the first command; no fail-before receipt is invented.

The new SQL fixture has12 probes:8 direct SELECTs across scalar/vector modes,3 EXECUTEs of one named prepared statement with changed parameters, and one EXPLAIN. SQL row IN is explicitly rewrite coverage, not proof of the AST row service; direct native tests cover that service. Prepared-statement reuse is not a physical plan-cache-hit claim. The late Real/text warning comes from the demanded implicit cast child, not a relabeled comparison warning. R108 typed SQL and real legacy mappings are also rerun.

## Deferred

Strict final audit, broader CAST/M2, six complex candidates and general request-root/default-NoColumns/liveDAG acceptance remain. Full suites/lint/dev/bazel/release/exhaustive/performance/physical memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/whole-Go-package/PR readiness are unverified. Historical R100 expression4/unistore1 failures remain unrepaired. Overall goal remains active.
