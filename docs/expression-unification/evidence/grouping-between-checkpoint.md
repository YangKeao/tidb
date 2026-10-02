# GROUPING and BETWEEN — grouping-between-54

Previous `compare-six-53`. Two added frozen families: `grouping`, `between`; functional **174/245**, target221, remaining47. Final core gates are recorded in `../checkpoint.json` and [exact receipts](../logs/grouping-between-summary.txt). Strict final acceptance remains0; no package-transcreation, PR readiness, performance or full compatibility claim.

## Implementation and review map

Six parallel exclusive owners, parent-only formatting/builds/Plan/docs/publication.14 existing Rust files: CPP7, native7; no new source, dependency, manifest or lock.

- CPP `components/tidb_query_expr/src/impl_miscellaneous.rs` and `lib.rs`: unique grouping core, four public API types, three fixed mode kernels, real-NULL terminal and shared envelope validator.
- CPP `local/{batch,mod,registry}.rs`, `types/{function,expr_eval}.rs`: four unit profiles, closed admission, actual checked packing and owned Int bits.
- Native `rust/crates/tidb-expr/src/grouping.rs`: aliases and guarded adapters, no duplicate bit/set algorithm. `scalar_function.rs` retains argument/metadata demand and submits real inputs.
- Native `tikv/{evaluated_ascii,evaluated_ascii_tests,mod}.rs`: existing Int materializer, narrow resource-safe packing bridge, lifecycle tests.
- Native `lib.rs`: two BETWEEN composition tests only; no production rewrite or new BETWEEN evaluator.
- Native `rust/crates/tidb-session/src/tests_core/lifecycle.rs`: two SQL tests for BETWEEN and existing GROUPING rollup admission.

## GROUPING ownership

`GroupingMode`, `GroupingMetadataError`, `GroupingMetadata` and `GroupingFunction` retain their original public API and error variants, now owned by TiKV and reexported by native code. The pure public metadata helper and the real scalar kernel use the **same** shared implementation. Failed metadata replacement still leaves the function uninitialized; singleton requirements and ordered `BTreeSet<u64>` semantics are unchanged.

Three fixed unit-metadata identities—`GroupingBitAndNative`, `GroupingNumericCmpNative`, `GroupingNumericSetNative`—use existing Bytes2, not host-computed result bits. First owner is the actual gid as LE8. Second contains u64LE group count, then each set's u64LE count and ascending u64LE marks. There is no mode/opcode in the byte stream. `local::prepare_grouping_args` checks extents and reserves both owners fallibly; the native crate-private bridge maps its real LocalError inside the existing computation guard.

The same allocation-free `grouping_native_args_valid` walker validates facade and official readiness. It checks exact gid width, bounded counts/lengths, strict mark order/uniqueness, singleton modes and no trailing data. NumericSet permits empty sets; zero groups remain valid for all modes. Worker decoding still validates and calls the shared metadata implementation. All unsigned result bits, including64 ones and more-than64-mark wrapping, are preserved through existing OwnSignedInt and `into_uint_bits_datum`, not boolean conversion.

`GroupingNullNative` requires genuine `NullWitness(None)` and refuses Some. Scalar demand stays arity1 → eval_int(child) → NULL before metadata read → original missing-metadata error → actual value worker. Child coercion/errors are unchanged. No new driver, result kind, carrier, binding, PB/legacy admission or generic public execution program is introduced.

## BETWEEN composition, not another kernel

The preceding checkpoint closed its scalar comparisons. Existing logical/NOT workers already produce the composition's final answer; this checkpoint establishes the whole remaining family through integration evidence rather than claiming another primitive implementation.

- AST evaluates the selector once, then lower/Ge and upper/Le, even when lower is false/NULL (errors still stop), shared AND and optional shared NOT.
- Rewritten forms retain their existing common-domain casts and lazy Ge/Le/AND or negated Lt/Gt/OR. The selector may be repeated when demanded.
- SETVAR/strict-cast tests pin that demand difference. NaN NOT BETWEEN remains AST1 versus rewritten0. Default AST collation versus typed explicit binary collation is also retained, not silently unified.
- Fixed native tests cover all six common domains, unsigned values, NULL and PAD/no-pad distinctions. Direct bare-column zero-slot tests cannot fail in a preceding unrelated worker.

SQL tests use six fixed domains and ten literal expectations per domain.24 direct-column zero-slot BETWEEN/NOT BETWEEN calls and one existing `SELECT GROUPING(a) ... GROUP BY a WITH ROLLUP` projection require structured PoolResource. No WHERE-id, ORDER, CAST wrapper or different aggregate can mask ownership; positive GROUPING0/1 rows use existing planner admission without planner changes.

## Final validation and actual corrections

Nine Cargo attempts: eight actual test runs and one compile failure. Five focused gates pass: CPP GROUPING4/local297+1ignored; native GROUPING6/BETWEEN3; SQL2. Full expression is **1502pass/4oldfail/94ignored, exit101**. Its complete failure section matches `compare-six-53` after thread IDs only, SHA256 `27654f2c0ad2971242c00f50182467598cdfd9c2c65e5bf5a32e683708448637`.

One new test implementation initially failed compilation (missing resolver timezone method and duration helper integer type). Two actual test-red runs exposed new fixture errors: typed arithmetic's public decorated error was mistaken for its primitive IntOverflow, and Duration fixture metadata used unspecified FSP despite actualFSP0. Source-based fixes retained production behavior and test domains; no original expectations changed. Exact source trails, failed receipts and three retries are retained. A formatter invocation used wrong cwd-relative paths before Cargo; corrected to `git diff --relative`. An independent RO discovery glob failure had no source impact. These are reported separately, not fabricated test REDs.

## Preservation and scope

Original native grouping test suffix is byte-identical: SHA256 `351f7599b5e243e994883f20b5497db9b570e91ae4d3cda38cff23b19128e586`. Ten additive tests: shared kernels2/local2/native SDK2/BETWEEN2/SQL2. No original SQL expected values or fixtures regenerated. Architecture-index and coprocessor-guide changes explain actual ownership, with no new repository policy.

IntDIV was investigated but **not implemented or credited**: its mutable precision getter counts, warning-before-conversion, native bounded versus legacy exact division and old MIN/-1 panic require a separate bounded implementation. IN/INTERVAL still have native membership/search answers, and NullEq retains native predicates; shared comparison leaves alone do not earn those families. Plan decoders, digest normalization and password-policy consumers are likewise deferred rather than represented by precomputed answers.

The prior explicit row-context activation compatibility change remains documented in `compare-six-53`. Whole workspace/make lint/dev/bazel_prepare, release/performance/zero-copy, physical heap/peak/OOM/M6, exhaustive differential/TiFlash/FIPS and prior parser/GB/ignored-vector/extreme Decimal exceptions remain unverified. Full unistore is not rerun in this checkpoint because legacy production was untouched; its preceding non-green receipt remains historical, not a current pass.
