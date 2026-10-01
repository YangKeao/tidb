# Numeric unary checkpoint

`unary-two-46`, following `regexp-worker-45`: **157/245 functional families**, target 221, strict final-audited count **0**. The prior 155 objects remain unchanged; new frozen ids are `unaryplus` and `unaryminus`. Incomplete, not PR-ready.

## Bottom-up implementation
- TiKV datatype `NativeDecimalOp::Negate` reuses the existing wire `Neg` leaf and native finish. Native public `Decimal::negate()` is a thin bridge, retaining wide coefficients, visible/storage scales, canonical native zero and original declared-shape clearing. Wire raw zero policy is unchanged.
- `impl_op.rs` owns one checked integer-negation policy shared by wire and native kernels. Column overflow carries an actual typed `NativeUnaryMinusError`; constant promotion occurs in TiKV and returns a real Decimal. The two constant recipes extend only the existing worker-computed `checked_i64_view` whitelist; native packing does not compare magnitude thresholds or negate values.
- Eleven unit recipes reuse existing Values, IEEE-bits, DecimalUnary and NULL-witness roles. Decimal's actual physical shape remains `[Decimal, Int budget]`; other recipes have one slot. No new driver, carrier, binding, factory bound, ordinary PB admission or regexp lifecycle change. The original finite-budget and typed Decimal-cause helpers are shared rather than copied.
- Runtime plus preserves primitive values via real computed output, while other values retain original decimal coercion. SQL rewriting still eliminates unary plus. Float32 carries f64 bits, not a rounded f32; UInt/String collation and plus-Decimal declared column shape are original packing metadata. The latter is restored over the computed value, never by returning the original operand.
- Original string/hybrid/temporal coercion, warnings, NULL, child demand and NOT/BitNeg behavior remain. Only exact column-negation operations, matching signedness, actual typed cause and this call's dispatch witness authorize the original BIGINT diagnostic, including signed MIN's double minus. Resource/count failures are not SQL overflow.

## Actual verification
Exact commands and all eleven whole-log SHA256 receipts: [summary](../logs/unary-two-summary.txt).

| Final gate | Result |
|---|---|
| Shared datatype / native decimal | 1 / 22 passed |
| TiKV unary / local | 17 / 281 passed, local 1 ignored |
| Native unary / SQL unary | 19 / 1 passed |
| Full expression | **1483 passed, 4 old failures, 94 ignored; exit101** |
| Full unistore | **197 passed, 1 old failure, 13 ignored; exit101** |

Eleven Cargo attempts: nine nonzero-test runs and two test-compilation failures. Six final focused green, one existing instrumentation RED, two final old full-suite non-green; three failed-target retries. No zero-match or launch failure. Not all first-pass.

Compilation fixes were limited to an existing trait import in new TiKV tests and selecting `Session::warnings()` instead of the tuple-projecting helper in the new SQL test. All expected warning fields remain. One old AST observation for `FROM_DAYS(-140)` now expects two independent unary-minus/FROM_DAYS workers; its zero-Date value is unchanged. Counters from different workers are not subtracted. No production fix to satisfy a test guess and no fixture recording.

Both complete final failure sections match `regexp-worker-45` after numeric panic-thread IDs only are replaced with `(<id>)`: expression `27654f2c0ad2971242c00f50182467598cdfd9c2c65e5bf5a32e683708448637`, unistore `b64dcced405c699c11b72ee31888c58e491b4e5b4e74fa2d445454e1d1e146e9`. No address mapping; duration remains `164:55`, unistore `194:5`.

Fifteen Rust sources (TiKV9/native6); no manifest/dependency/lock changes. Final pinned formatter and diff checks pass. Five entire original test-module tails are byte-identical (TiKV op/math/decimal, native ops/decimal bridge). Nine new tests cover value/header identity, wide Decimal, overflow/promotion, actual worker lifecycle, NULL, SQL warnings and resource refusal. Seven recovered RO lookups and one literal-edit retry are disclosed; no formatter/static-proof failures. The separate old instrumentation update is not hidden behind the module proofs.

## Remaining work
Binary arithmetic can reuse the new typed causes and existing scalar carriers but still needs signedness, subtraction mode, division precision, warning and legacy i128 closure. JSON_UNQUOTE/PRETTY has a concrete shared decoder/renderer proposal, including raw temporal and opaque cases; it is not implemented here. Remaining families, full type/Go-package acceptance, production request-root integration, workspace/lint/release, differential tests, physical allocation/OOM/performance, M6 and TiFlash remain incomplete. Paired commit and byte-identical Plan hash are pinned in `checkpoint.json`.
