# Partial IN takeover: typed temporal/JSON and legacy

**in-typed-legacy-105 / R108**, after [MyDecimal sharing](mydecimal-core-checkpoint.md).
Functional **237/245**, strict **0**, remaining **8** are unchanged. This is not whole-IN completion.

## Actual migrated paths

- `ScalarFunction`'s Datetime/Timestamp/Duration/JSON IN branch keeps original evaluate-all then cast-all preparation, including source field types and JSON first-document versus candidate-string-value modes. SDK `native_in.rs` owns its subsequent NULL check, typed comparison loop and result. Existing core-time, raw-nanosecond and binary-JSON comparison services are reused; wire IN is not substituted.
- Legacy has exactly two actual mappings: InInt and InString. Their native loops and NULL/match reduction are deleted. SDK heads request index0 even when actual children are empty; native adapters read `children.get(index)`. A NULL target still visits every RHS, while a real match skips later children/errors. Full i128 comes from original `eval_expr`, not folded integer or i64 conversion. Bytes retain original `eval_bytes` SQL-error folding.
- String comparison requests the original collation ID only after both operands are non-NULL. The adapter resolves the current effective native collator and returns its actual policy tag; SDK performs comparison and controls continuation. No host-computed match, early `contains`, head-time snapshot or NULL-pair lookup is introduced.

## Four closed profiles

IDs558–561: `InTypedValuesNative`, `InLegacyIntHeadNative`, `InLegacyStringHeadNative`, `InLegacyStepNative`. Typed/head inputs are one Values byte argument; Step takes state and actual reply. All return one non-NULL owned byte report, including SQL NULL.

Typed input contains the four actual static domains and already-cast NativeIdentity frames, preserving time kind/FSP, duration FSP and JSON representation. Legacy integer replies are16LE bytes; string replies are actual nullable bytes; requested effective-collation policy is8LE bytes. SDK reports are terminal Null/Bool or a typed request plus complete continuation state. Head postflight accepts only its index0 request; typed postflight accepts only terminal results.

The checked report bound is input-length-sum+128. Input capacities, admission and output capacity remain checked. This is not a transient/physical heap or OOM guarantee. Existing selected-scope lifetime, child request context and generic error identity are retained. Typed worker admission follows original evaluation/casts; it is not a pre-child head.

## Deliberately untouched

AST scalar/row, generic typed/prepared-string cache and ready-value IN remain native follow-ups with distinct exhaustive/early-stop policies. The original AST `1 NOT IN(1,2)` still uses Eq, Eq and NOT: its three-facade test and observer are unchanged. No family credit is earned by this partial slice.

Shared PB admits zero IN signatures. Positive producers can reach the two legacy mappings. An actual producer's UnaryNotInt(IN) wrapper is recursively refused by Shared PB because its IN child is unsupported; this differs from a manually constructed legacy NOT wrapper. The baseline refusal is now explicitly tested, not fixed or mislabeled new admission.

## Validation and incidents

[Commands/counts/hashes](../logs/in-typed-legacy-summary.txt) record every launch. Initial SDK compilation failed with E0432 because `NativeCollation` was imported from `codec::collation` instead of `codec::collation::native`; only that import was corrected. The original log is retained, and local tests were not launched after that failure.

The first gateway filter `tikv::evaluated_ascii_tests` compiled but matched zero tests. It is not a passing gate; the original filter `tikv::evaluated_ascii` was rerun and passed196 tests with one existing ignored test, including the original three-facade NOT IN assertion. No old assertion, fixture or observer is modified to obtain green.

Nine actual launches yield seven nonzero green receipts, one compile failure and one zero-match receipt. SDK core filter passes6, local360 (one ignored), native bridge1, corrected gateway196 (one ignored), original IN source4, real legacy mapping1 and SQL1. Both corrections and original logs are retained; no test execution failed.

Five appended tests cover SDK behavior, local framing/budget/reuse, native bridge lifetime/generic errors/late collator changes, both real legacy mappers, and SQL. Original154 SDK and526 native test bodies in changed files remain byte-identical.

SQL has12 probes:11 SELECTs plus one EXPLAIN. Eight successful SELECTs run four projection groups in scalar/vector modes (26 typed IN expressions), covering four domains, SQL NULL versus JSON null/string, and a demanded late cast warning. Two zero-slot SELECTs isolate the typed IN root using same-typed stored columns; one plain-column SELECT proves session reuse, not restored worker capacity. EXPLAIN guards five actual IN expressions against equality/OR rewriting. Headers retain LongLong1/0 and IS_BOOLEAN.

## Still open

Whole IN, broader CAST/M2, prepared cache, AST/row/generic/ready-value decisions, request-root/default-NoColumns/liveDAG and six complex candidates remain. Whole suites/lint/dev/bazel/release/exhaustive/performance/physical memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/whole-Go-package/PR readiness are not verified. Historical R100 expression4/unistore1 failures remain unrepaired. Overall goal remains active.
