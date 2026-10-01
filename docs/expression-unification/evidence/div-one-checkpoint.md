# True division shared-worker checkpoint

`div-one-50` follows `mod-one-49`: **164/245 functional families, strict final acceptance0**. This is `/`, not integer `DIV`. Four profiles earn one family; target221 needs57, with81 eligible families remaining. Not PR-ready.

## Review map

- TiKV `components/tidb_query_datatype/src/codec/mysql/decimal.rs`: `try_native_mysql_div(&self,rhs,frac_increment:u32,limit)->NativeDecimalResult<Option<Res<Self>>>` reuses the existing private Grow long-division loop. Additional live scratch/output words are checked before budgeted allocation; unbudgeted wire allocation order is unchanged.
- Native `rust/crates/tidb-datatype/src/decimal/mod.rs`: `div_mysql_with_warning` becomes a thin shared-value bridge, deleting its sole-use `div_mysql_unbounded` and `bound_decimal_codec_result`. Existing public wrappers, `div_rem`, `div_round`, and IntDIV control/conversion remain intact.
- TiKV `impl_arithmetic.rs`, `local/{batch,mod,registry,tests}.rs`, `types/{function,expr_eval}.rs`: four kernels, transient precision/status binding and owned official-result materialization. No new driver or CallMetadata schema; generic compilation needs no change.
- Native `ops.rs`, `ops/real_coerce.rs`, `scalar_function.rs`, `scalar_function/pb_builtin.rs`: preserve original preparation and demand, delete quotient/range/zero mathematics, consume actual results and dispositions. No integer/fast tier expansion.
- Native `tikv/{evaluated_ascii,evaluated_ascii_tests,mod}.rs`, `lib.rs`: typed result adaptation and explicit precision-aware legacy SDK. `tidb-unistore/src/cophandler.rs` migrates only DivideReal/DivideDecimal; `tidb-session/src/tests_core/lifecycle.rs` pins SQL behavior.

19 Rust sources: TiKV8/native11. Eleven new test functions. No new source module, dependency, manifest or lock change. Ownership guides and byte-identical Plan mirrors accompany the checkpoint.

## Closed protocol

| Profiles | Physical inputs | Actual result |
|---|---|---|
| DivRealNative / DivRealLegacy | two existing IEEE-bit operands | OwnIeee754Bits |
| DivDecimalNative / DivDecimalLegacy | Decimal, Decimal, existing remaining budget | OwnDecimalDivision |

All four value recipes require two non-NULL operands at both admission layers. Genuine NULL and missing children reuse existing arithmetic terminal recipes. `DecimalDivision {left,right,frac_increment:u32}` has a separate closed role; precision is typed invocation metadata, not a fourth SQL operand or a cache key. Real metadata stays unit; Decimal metadata has a fixed native/legacy kind and Copy state only.

The actual wrapper transitions Bound→Entered→Completed exactly once, recording Ok/Truncated/Overflow/ZeroDivisor or an infrastructure-error terminal. It moves the actual Decimal into the official return column; metadata never stores a duplicate owner. Materialization checks this invocation's wrapper count and status/presence (ZeroDivisor iff None; other statuses require Some), clones only through the existing budgeted Decimal ownership path, and consumes status before guard cleanup. Invalid transitions remain sticky and prevent reuse. Pre-dispatch failures, post-kernel output-budget errors, original-error preservation, unwind poison, metadata footprint and empty postflight are covered by focused tests.

`ComputedDecimalDivision` owns the actual optional Decimal and disposition, with checked public accessors; native adaptation does not serialize wide values to Bytes or compute status from the value. Only `DivRealNative` plus an actual caused Divide/FloatOverflow and this-call witness authenticates SQL overflow. Decimal statuses are successful reports, not fabricated SQL exceptions.

## Preserved policies

Native `/` keeps captured precision getter placement and frontend `raw0→4`. In the native shared kernel, an actual zero divisor bypasses target-scale arithmetic; otherwise the original plain-u32 add followed by saturating subtraction is preserved. Legacy passes raw precision, including0, without that extra step. The datatype API does neither frontend normalization nor policy inference. Source plain-u32 planning/order remains, including bounded profile-sensitive tests; invalid extreme wrapped result shapes remain explicit infrastructure refusals, not SQL Overflow/NULL. No unlimited release-domain equivalence claim.

Zero dividend's early path and a full-precision quotient that becomes zero have distinct original retained-scale behavior. Signed overflow saturates to the corresponding81-nines value. Truncation preserves `max(remaining_fraction_words*9, visible_scale)` even when that visible floor exceeds wire Fixed9. Integer width is bounded before Truncated can be produced, and fraction truncation cannot carry; the old post-warning >81-digit check is therefore redundant rather than moved into host arithmetic.

Native packing handles Overflow as the original DecimalOverflow, Truncated with the original1292 warning text before returning the value, and ZeroDivisor through the original zero handler after consuming the actual result. Explicit input presence distinguishes genuine NULL. Legacy retains payloads and ignores dispositions as before. Native real rejects NaN/nonfinite results; legacy preserves IEEE values. No host divisor test or quotient fallback remains.

Sentinel/JSON/Raw/vector/coercion precedence is unchanged. Real wins; non-Real `/`, including integer pairs, promotes to Decimal. Typed/PB real and Decimal left-NULL short-circuit through actual witness admission; only those two seams change. AST eagerness and whole-left/whole-right batch demand remain. Legacy real/Decimal remain sequential. Ordinary PB admission, wire policies, IntDIV warning-before-conversion and coefficient-fast tiers are not expanded.

## Validation and incidents

[Exact commands, all ten log hashes and limitations](../logs/div-one-summary.txt); current structured ledger in `../checkpoint.json`.
Seven final focused green gates: shared Decimal88; native Decimal25; TiKV division13; local291 (1 ignored); native division20; legacy1; SQL2. Coverage includes metadata/report lifecycle, owned results after worker drop, precision replacement, status/zero/input presence, full-width/hidden values, native/legacy IEEE behavior, signed saturation, finite word budgets and fifteen real-column zero-slot SQL cases.

One actual E0004 compile failure, zero tests: integer-only `require_computed_int` missed the new result variant. Parent added only its exact rejection arm; retry passed. No new test RED or zero-match. Before legacy tests ran, parent replaced two newly authored same-provider expected-value calls with independently derived fixed pins; no provider output was recorded, and no original fixture/SQL expectation changed. Read-only hypotheses about wrapped padding were corrected after inspecting source subtraction; no artificial failure was invented.

Full expression: **1492 passed,4 old failures,94 ignored; exit101**. Full unistore: **201 passed,1 old failure,13 ignored; exit101**. Complete failure sections match mod-one-49 after numeric panic-thread IDs only: `27654f2c0ad2971242c00f50182467598cdfd9c2c65e5bf5a32e683708448637` / `b64dcced405c699c11b72ee31888c58e491b4e5b4e74fa2d445454e1d1e146e9`. No source-address mapping.
Three complete original test suffixes remain byte-identical: shared Decimal `9a217340d9d495773a043cbe0378a68c5401737315752dcdc1105be52cc2e332`, shared arithmetic `f72a3ad990dbfe20c207e8764950c53276bbd4ea3e0953545f74c89764b511ea`, native ops `f823ff05f80ca755b998f6e92db3f3ef79ea462f5ae607d5aece0c2c9ee3bbaa`. Exhaustive rejection arms are adapted, not old expected values. All19 scoped formatter checks and both diff checks pass; original163 family entries remain unchanged.

## Deferred

IntDIV evaluator migration, whole workspace, make lint/dev/bazel_prepare, release/performance/zero-copy, allocator headers/physical heap/peak/OOM/M6,150-row differential, TiFlash and existing compatibility exceptions remain unverified. No complete Go-package/type-domain claim. DATE/TIME/MICROSECOND still need their actual typed-time/parser boundaries; proposed AES is only read-only next-batch planning and requires shared public crypto ownership plus dependency approval, not a preexisting TiKV leaf. Paired commit and frozen Plan hash are recorded in checkpoint.json without a self-referential native commit hash.
