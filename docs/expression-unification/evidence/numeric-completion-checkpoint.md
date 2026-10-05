# Shared numeric target and result policies

**numeric-completion-134 / R139**, after [numeric routing](numeric-route-checkpoint.md). Functional238/245, strict0 and remaining7 unchanged; no whole CAST/M2/package credit.

Existing SDK `native_numeric_argument.rs` owns target metadata and skip-fitting decisions, reusing the existing Decimal shape helper. Ordinary targets leave FieldType defaults untouched. String numeric Real-versus-Decimal selection is shared without changing the distinct Int-target behavior. Generic and context-Decimal result controllers preserve their two original unsupported messages, typed errors and accepted values.

Native constructs actual FieldType/Datum storage and invokes the existing contextful converter. Actual conversion results are projected into a tuple; no business callback or origin/profile tag is added. Original zone→flags/context effects remain. The underlying datatype conversion engine, expression renderer and other controllers remain explicit dependencies—not whole numeric/CAST ownership.

## Validation

Six matched gates GREEN without failure/retry: SDK11, native consumers5/old Real1/division1, existing SQL2. [Exact commands/counts/hashes](../logs/numeric-completion-summary.txt). Three new tests;9 SDK/36 native old touched-file test bodies unchanged. Renderer byte-identical; datatype engine/SQL files unchanged. No new Rust files or SQL fixture/probe credit. Native tests cover String Int intermediate Decimal, ordinary target defaults despite negative scale, metadata and actual generic/typed/context errors with original effect order. The old numeric_decimal_cast_type remains a one-line redirect for other callers, avoiding duplicate storage adapters. Broader M2/root/liveDAG/final acceptance remain. Full suites/lint/release/exhaustive/performance/physical memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/package/PR readiness unverified. Historical R100 expression4/unistore1 failures and prior RED receipts retained.
