# Extrema selection and numeric policy foundation

**extrema-policy-99 / R102**, after **extract-runtime-98**. Functional **232/245 (94.69%)**, strict0, remaining13 unchanged. No family or C4 credit.

## Ownership moved

SDK `native_extremum_policy.rs` now owns three policies used by existing native GREATEST/LEAST calls:

1. Empty-arity error, global actual NULL prepass, explicit signature precedence and value-derived domain selection.
2. Numeric winner cursor: original candidate-left/best-right requests, only an actual comparison `Int(1)` replaces the incumbent, first ties retained.
3. Promotion after every comparison succeeds: Real/Float32 first, then Decimal and exact scale policy, mixed signed/unsigned to Decimal, otherwise original identity.

`builtin_ext/compare2.rs` projects real DatumKind, Time kind, Decimal scale and optional planner signature metadata. It executes the original requested comparison/conversion, without calculating a winner or selecting a promotion domain. The duplicate native head/fallback and numeric decision bodies are deleted.

Arbitrary `arg_decimals` length is retained. Constants use maximum nonnegative metadata; otherwise winner metadata takes precedence over maximum actual Decimal scale. The original `as u32` conversion remains unnormalized. Singleton Raw/sentinel values can remain identity; NULL suppresses later value conversion but not already-eager child evaluation. NaN and signed-zero behavior continues to come from original comparisons, not total ordering.

## Explicitly partial

Four other reducers—Vector, Time, DirectString and StringAsTime—and string-as-time parsing remain native. Planner aggregation and result metadata are unchanged. No worker/profile/carrier, PB/legacy or specialized vector admission is added. Numeric comparison still uses its historical NoColumns policy.

This checkpoint is a reviewable policy step, **not a missing-primitive blocker**. Shared time parser/display/set-kind, Vector and Decimal services already suffice for complete staged runtime. Next integration must separate historical NoColumns semantic policy from selected execution authority and retain per-item mode/timezone reads. The Plan records existing scoped APIs and temporal carriers; that read-only finding is not implementation credit.

## Validation

[Exact commands/counts/hashes](../logs/extrema-policy-summary.txt), [manifest](../checkpoint.json).

Six locked single-threaded launches: five green and one retained new-test failure. Final SDK core1, native entry4, original cross-tier1, new SQL1 and original SQL6 all pass. Three new tests were added;189 original native test bodies are byte-identical. Existing ignored vector tests remain ignored.

The new test initially expected Real3 from a manually typed LongLong-return node. Original ParamMarker handling preserves the Real input; the reducer returns Real3, then existing `ScalarFunction::coerce_to_ret_type` produces Int3. Only the new expected value was corrected from those sources. AST/direct-helper Real3 expectations remain. No production fix, old fixture change or provider-output recording was used.

New SQL has **4 SELECTs /18 function calls** across two vector settings, pool1 only. It pins winner-own Decimal scale versus declared header scale, constant maximum scale, mixed signed/unsigned promotion, finite Real promotion and first/late NULL. Materialized values, signed/binary metadata and quiet warnings are checked. It claims neither SQL NaN nor zero-slot/new-root coverage.

Two CPP/two native Rust files, one new module. Formatting/diff checks and focused source review completed. No Cargo/lock/Go/Bazel/generated edits; guides remain descriptive. Full expression/unistore were not rerun; prior4+1 failures remain historical. Workspace/lint/dev/release/exhaustive/TiFlash/FIPS/performance/physical memory/OOM/allocator/dual-tzdata/whole Go-package/PR readiness remain unverified.

All232 family objects and prior partial/type records remain unchanged; one three-id policy slice is appended. [Remaining acceptance](remaining-acceptance.md) and the overall goal remain active.
