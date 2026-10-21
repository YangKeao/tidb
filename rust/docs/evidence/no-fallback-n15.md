# Typed dynamic sources and owned diagnostic continuation

Work after TiDB416efae1 and the compatible SDKcdd5c30 scoped-window protocol. This is local evidence for task #4, which remains in progress. No push, PR, release or overall fallback-retirement claim.

The frontend now compiles original parameter/correlated/deferred sources into the existing SDK driver. Optional original FieldTypes remain optional; Blob describes capability transport only. Typed source metadata owns immutable declarations, saved literals, original input-column facts or live context bindings. It retains no executable expression/callback. Per-source invocation caches preserve whole-source reads and reset across invocations. Distinct numeric probe/general/diagnostic producers retain the original source identity and demand ordinal.

Numeric and math diagnostic plans are bounded immutable instructions. Actual demanded constants, typed values and empty-row reads return through the same frame. SQL failure to produce diagnostic text remains distinct from SQL NULL; cancellation, resource and binding failures remain terminal. Paused Real-to-Decimal and integer-division continuations own already-produced values, request only the genuine subject/report phase and consume before callbacks. Window arithmetic completes the entire left source and conversion phases before reading the right source. Scoped-row requests never launch another driver.

Deferred root workers own cold generic and supported typed profiles over the same original source. The original once-only type probe remains after the vector flag and before the empty guard. The selection is cleared before callbacks; unsupported actual types choose original generic row publication. Typed roots stage results across driver windows and apply the original outer integer unsigned flag. Pure deferred forwarding uses a complete-child window host; row integer normalizers retain their original per-row failure boundary.

## Original evidence and independence

Every listed hash was observed from the executed original, frozen and rerun before the corresponding owner/source changes. Archives and logs are retained in the persistent agent workspace under evidence/no-fallback/n15-*. A final shared-owner hash is a regression witness, not a renewed independent comparison.

| Original observation | Cases | FNV-1a64 |
| --- | ---: | --- |
| Public typed context leaves | 186624 | 1F8A2B96768DA8AB |
| Automatic typed entry | 18432 | 015B674ABA041ABB |
| Root admission and phase order | 9792 | E4C86226EC383F1D |
| Dynamic diagnostic demand | 7200 | A4F6DB5E8D40E458 |
| Direct math rendering | 4320 | BC1F79F0E8CB417D |
| Optional declarations/storage | 50160 | A9F134CB35861E1F |
| Numeric operand conversion | 58752 | EA48CF6E3E46CF89 |
| Saved/live argument diagnostics | 246240 | D589F55424DB5FF5 |
| Diagnostic structure/refinement | 15552 | 1765DBC05E4A2D95 |
| CAST source and publication | 2839680 | 0EAB866CE231BC47 |
| Numeric root domains/phases | 13023360 | 0CF2861942A0B059 |
| Integer fast probe/general retry | 1152 | 4483FB661E03551F |
| Paired quotient contexts/effects | 282240 | 1A4A2ABFEC2B5A5B |
| Deferred root type/publication | 677376 | B634495626BD8ECB |
| CAST_CHAR context/publication | 1419840 | 10E3C88C3FFBA67D |

Independent pairs before retirement include public186624, automatic18432, diagnostic7200, conversion58752, argument246240+15552, prepared columns6480/literals14256 and numeric operand29920/6D9435492158AE78. Integer-promotion head8192/07C8B3B549864CA7 was independently compared before its old wrapper became an adapter. Integer-division operand2304/9B8E89E1D40EABEE still compares against the separately active old actual-operand converter. Executed old/candidate files were archived before retirements; no expected tuple was copied from the candidate.

## Actual sources and reproduced corrections

Actual source comparisons cover CAST2839680 and row CASTJSON1419840, unary2291328, ABS1860480, plus/minus/multiply5581440 roots and5581440 row consumers, MOD/DIV3720960 roots/188160 paired contexts, INTDIV1860480 roots/94080 paired contexts,1728 strict diagnostic refinement and7680 saved-header/live-value combinations. Contexts change precision, truncation/division levels, zones, parameters, correlated values and assignment effects. Complete values/errors/panics, callback state, original declared output storage and selections match.

The expanded mixed Projection first reproduced a CAST_CHAR Source Admission gap. Original1419840 observations were captured/frozen/rerun and eight executed files archived before the narrow source/publication change; actual1419840 now pass. Static closed typed sources remain unchanged. Original eligible CAST_CHAR(ABS(parameter)) keeps generic row operands with atomic typed publication across2051/2057-row windows; its12 late-error cases pass. Direct parameter CAST_CHAR remains an originally ineligible generic row entry and retains prefix publication, including when the vector flag is enabled. No conversion value body was changed.

The new deferred-root677376 actual matrix reuses each worker across36 mode/type/error/panic/empty transitions. An additional96 long cases cover2051 physical rows/2057 duplicate selections and root declared/unsigned/optional fields. Twelve row-child cases explicitly cross driver windows: a genuine range-sentinel SQL failure on read1026 leaves typed output unchanged and generic output with1025 successful values. Bounded type-probe error/panic/resource cases clear the selected profile before recovery, including empty canonical invocations.

Real regressions were reproduced before production fixes: large Enum row arithmetic rounded prematurely; a failed integer probe needed fresh general producers; callback store invalidation allowed later row callbacks; unused sparse Decimal display scale was charged under NULL; deferred row forwarding performed four reads instead of six and changed the overflow report. The forwarding regression now retains six reads, three left conversions and the original correlated snapshot/report. Formatting/JSON/binary work regressions reject at the actual demanded work boundary, with NULL/small-value recovery and no fallback.

Direct source/host tests cover1/3/2050 rows, duplicate/reordered physical positions, foreign/stale capabilities, role/node/occurrence/physical-row mismatches, wire NULL and non-value completions, consumed-task replay, callback errors/panics/store mutation, retained-byte limits and reuse. Value-store borrows do not cross context callbacks. The unchanged fifteen-root source guard passes in full.

## Validation

All final gates are terminal. n15-front-final2.log reports1944 passed, exactly the three inherited failures,95 ignored and all403 batch tests passed. Its warning-title Counter346 exactly matches N14. The inherited failures remain ifnull_string_column_literal_uses_go_signature_and_column_collation, builtin_info_json_math_source::exp and builtin_math_misc_op_source::vectorized_builtin_op_func. n15-projection-char-fixed.log reports26 passed, including two added typed dynamic arithmetic/deferred-root cohorts under serial/two-worker and unordered/ordered suites; prior configurations and allowances remain intact. n15-lint-final.log passed make lint. Both pinned Jan30/Aug22 formatters and git diff --check pass on31 changed Rust files. The persistent agent checker evidence/no-fallback/n15-final-check.py passes against executed archives, frozen tuples, source/driver ownership, exact failures/warnings, fixtures and excluded BUILD.

Commands from the TiDB worktree:

    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib deferred_typed_roots_ -- --nocapture
    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib
    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-executor --lib projection::tests
    make lint

SDKcdd5c30 is unchanged during frontend final validation. Its fresh full1144 passed/exact inherited1/1ignored and all-target clippy89 expression/171 total warning baselines are detailed in no-fallback-n15-sdk-row.md. Default clock and existing fixtures remain unchanged. The unrelated untracked differential BUILD is excluded.

## Continuing scope

FIND_IN_SET requires serialized cache initialization across suspended same-frame child demand and the original Row::empty semantics. Remaining stateful functions, optional/PB/public AST/unistore/filter source seams, generic Admission dispatch retirement, full-server validation and performance on the final unified implementation remain open. Continue directly after this local checkpoint.
