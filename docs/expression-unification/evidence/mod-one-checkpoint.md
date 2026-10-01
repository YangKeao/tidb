# MOD shared-worker checkpoint

`mod-one-49` follows `like-two-48`: **163/245 functional families, strict final acceptance0**. Eight new recipes represent one family, not eight credits. Target221 still needs58; 82 eligible families remain. Not PR-ready.

## Review map

TiKV `components/tidb_query_datatype/src/codec/mysql/decimal.rs` adds `try_native_rem(&self,rhs,limit)->NativeDecimalResult<Option<Self>>`. It validates native shapes, reuses the private Grow remainder loop and preflights additional live scratch/output words before allocation. Existing callers retain their unbudgeted wrapper and original wire policies. `None` means only zero divisor; allocation/count/shape errors stay infrastructure. The remainder retains max stored and visible scale, dividend sign and normalized zero; no quotient is unnecessarily constructed. Native `rust/crates/tidb-datatype/src/decimal/mod.rs::rem_mysql` is a thin conversion/shared-call/conversion facade.

TiKV `components/tidb_query_expr/src/impl_arithmetic.rs` owns eight generated value kernels. `local/{batch,registry,tests}.rs` and `types/{function,expr_eval}.rs` register precise closed input/result/error domains. Existing `lib.rs`, `local/{compile,mod}.rs` exports/generic shape machinery needed no changes.

Native `rust/crates/tidb-expr/src/ops.rs`, `ops/{integer_coerce,real_coerce}.rs` select profiles, preserve coercion and pack actual results; old integer/real/Decimal remainder computation is removed. `scalar_function.rs` and `scalar_function/pb_builtin.rs` retain old demand and connect three real NULL exits. `tikv/{evaluated_ascii,evaluated_ascii_tests}.rs` extend existing result materialization and generic legacy SDKs; no new export file is needed. `tidb-unistore/src/cophandler.rs` routes four integer signatures and real/Decimal MOD; `tidb-session/src/tests_core/lifecycle.rs` provides SQL scope/diagnostic probes.

17 Rust files: TiKV7/native10. No dependency, manifest, lockfile or new module. Both ownership guides and the identical generated Plan snapshots accompany this checkpoint.

## Closed protocol

| New recipe | Existing role | Owned result |
|---|---|---|
| ModIntSsNative / ModIntSuNative / ModIntUsNative / ModIntUuNative | Int2 | OwnSignedInt raw bits |
| ModInt128Legacy | Int1282 | OwnInt128 |
| ModRealNative / ModRealLegacy | Ieee754Bits2 | OwnIeee754Bits |
| ModDecimalNative (also legacy) | Decimal2, including existing infrastructure budget | OwnDecimal |

Both facade admission and official ready admission require two actual non-NULL operands **only for these eight recipes**. No narrowing of existing nullable operations. Real NULL uses `BinaryArithmeticNullNative` with `NullWitness(None)`; missing children use `BinaryArithmeticMissingLegacy` with genuine NoArgs. No fake RHS, new role/result kind/metadata binding, private context mutation or driver.

For non-NULL value packets, successful computed `None` means zero divisor. Native packing first consumes the result, then invokes the original `Columns::handle_division_by_zero`; explicit frontend input-presence metadata suppresses that handler for genuine input NULL. It does not inspect divisor values or tag a host-computed answer. A valid zero remainder remains `Some(0)`. Legacy returns computed None silently. Resource/contract/kernel failures propagate rather than becoming NULL or warnings.

Integer result unsignedness follows LEFT only. Native SS keeps `wrapping_rem`, SU retains its original negation quirks, US uses divisor magnitude, UU raw unsigned remainder. All four legacy integer signatures keep full-i128 `%`, including the preexisting extreme panic profile; no signature-based narrowing/reinterpretation is added. Native real rejects nonfinite computed results, including NaN; legacy retains IEEE bits. Only `ModRealNative` plus actual caused `Modulo/FloatOverflow` and this call's dispatch witness authenticates native overflow. Decimal remainder does not fabricate SQL overflow. The existing fast arithmetic SDK rejects the new Modulo enum rather than exposing an unimplemented fast path.

Preserved native frontier: sentinel, unsigned interpretation, JSON rejection, Raw/vector rejection, temporal conversion, left/right string coercion, Real before Decimal, then actual NULL selection. MOD does not enter the vector +/−/* branch. Typed/PB integer and real MOD still demand RHS after lhs NULL; Decimal still short-circuits. Batch execution completes the left column, then the right column, before row arithmetic. Legacy integer remains eager; legacy real/Decimal remain sequential. DIV/IntDIV and wire policies are untouched.

## Validation

[Exact nine commands and full-log hashes](../logs/mod-one-summary.txt); structured ledger in `../checkpoint.json`.

Seven focused first-pass green runs: shared Decimal87; native Decimal24; TiKV MOD20; local289 (1 ignored); native MOD35 (2 ignored); legacy1; SQL2. Eleven new test functions cover exact metadata/getters, both nonnull admission layers, real zero versus genuine NULL, full-i128 values, signed/unsigned profiles, -0/NaN/Inf, hidden/wide Decimal results, scratch/output budget refusal, reuse and actual scope failures. SQL includes SELECT warning1365, strict UPDATE error, NULL silence, original RHS demand and fifteen real-column zero-slot cases.

Final full expression: **1490 passed, 4 old failures, 94 ignored; exit101**. Final full unistore: **200 passed, 1 old failure, 13 ignored; exit101**. Entire failure sections are byte-equal to like-two-48 after only numeric panic-thread IDs: expression `27654f2c0ad2971242c00f50182467598cdfd9c2c65e5bf5a32e683708448637`, unistore `b64dcced405c699c11b72ee31888c58e491b4e5b4e74fa2d445454e1d1e146e9`. No address mapping.

No new RED, compile/launch failure, retry, zero-match or fixture recording. Three complete original test suffixes are byte-identical: TiKV Decimal `9a217340d9d495773a043cbe0378a68c5401737315752dcdc1105be52cc2e332`, arithmetic `f72a3ad990dbfe20c207e8764950c53276bbd4ea3e0953545f74c89764b511ea`, native ops `f823ff05f80ca755b998f6e92db3f3ef79ea462f5ae607d5aece0c2c9ee3bbaa`. Existing bridge test helper only gains exact rejection arms; SQL values and instrumentation expectations remain unchanged. Pinned formatter checks cover all17 sources; both diff checks pass.

## Deferred boundaries

Known-word budgeting is not whole-heap/allocator-header/peak/OOM accounting. No `make lint`, `make dev`, `make bazel_prepare`, whole workspace, release/performance/zero-copy, M6, 150-row differential, TiFlash or complete Go-package/type-domain equivalence claim. Existing extreme Decimal release-shape exceptions, ILIKE gaps, parser errors and baseline failures remain.

DIV is deliberately a separate step: full-u32 precision and actual Ok/Truncated/Overflow/ZeroDivisor must travel through an explicit narrow typed binding/report, not a fake fourth SQL input or Fixed9 arithmetic. A read-only design can reuse the current guard/materialization lifecycle without serializing wide Decimals; it is not implemented here. IntDIV additionally needs warning-before-integer-conversion preservation. Previous162 family entries remain unchanged. Paired commit and identical Plan SHA are recorded in `checkpoint.json`, without a self-referential native commit hash.
