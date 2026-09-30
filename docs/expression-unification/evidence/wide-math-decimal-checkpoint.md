# Wide math and decimal — wide-math-decimal-five-25

**Final functional checkpoint: ten actual Rust logs, comprising one compile-only failure and nine test runs. Eight test runs passed; the full expression suite remains non-green.**

Functional delegation/native-deletion advances **77→82/245**; final acceptance remains **0/245**. The five complete frozen IDs are `abs`, `ceil`, `floor`, `round`, `truncate` ([baseline](coverage-baseline.json), lines87/109/153/284/331). `ceiling` aliases `ceil` (line23); neither that alias, the legacy ROUND entries nor the decimal bridge earns an extra family. The denominator is unchanged. This is not a completed/transcreated Go-package claim.

## Single ownership and admission boundary

TiKV `impl_math.rs` supplies **23 private RPN operations** plus the shared native scale policy. Existing wire operations reuse common integer, real and decimal primitives where the policies agree. **The native Go-compatible ROUND/TRUNCATE real kernels are newly migrated compatibility policy, not a claim that wire `powi` already implements native Go behavior.** Existing wire signatures, bounded decimal policies, error behavior and context warnings remain distinct.

The old five-family value-layer algorithms, native `go_pow10`/round/truncate helpers and datatype `round_or_truncate_to_scale_with_storage` digit loop are removed in favor of the shared owner. The datatype's independent `round_ceiling_to_scale` remains native; that magnitude-rounding routine is not SQL CEIL. This checkpoint does **not** claim whole-Decimal or whole-datatype completion.

Decimal uses actual **VectorValue::Decimal** inputs and computed Decimal outputs, retaining wide coefficients and storage/result scales rather than transporting a rounded Display string or narrowing through f64/nine words. Raw IEEE754 uses canonical LE8; legacy full-width Int128 uses canonical **LE16 identity**, without narrowing to i64. The largest new physical recipe is Decimal + resolved scale + internal budget: **three columns**, not a widened four-column/graph/driver allowance.

No new PB/unistore signature, public/default projection admission or native fallback/replay is introduced. The two current unistore tests exercise an existing legacy ROUND consumer, not a new admission path or a full-unistore pass.

## Preserved policies and error provenance

- **ABS:** only the new private signed-ABS branch emits `EvaluateError::AbsSignedOverflow { source }`, and only on actual signed-MIN overflow. It retains the original 1690 cause; native mapping requires an actual C4 receipt, not error-text recognition. Old wire errors stay unchanged; UInt retains all bits. Native decimal ABS reuses the existing `Decimal::abs()` worker before native result finishing.
- **CEIL/FLOOR:** Int/UInt are identities; Real and Float32 retain their kinds. Decimal result-domain precedence is typed override, declared shape, then unstamped payload width; narrow-result i64 conversion failure keeps Decimal. This is not wire DecToInt's warning/overflow policy.
- **ROUND:** unary Int/UInt remains identity. Two-argument Int always round-trips through f64, even at nonnegative scale; UInt is read through signed i64 bits before rounding and restored unsigned afterward. Native real uses the original GoPow10 table/order, ties-even and special-value guards, not wire `powi` or legacy ties-away.
- **TRUNCATE:** signed/unsigned integer division stays exact; unsigned scale makes the integer signature identity while retaining the actual scale operand. Native real retains its GoPow10, NaN, infinity and underflow behavior. No conversion is made more precise than its original policy.
- **Decimal scale:** `native_decimal_target_scale(i64, Option<i64>) -> i32` owns the `i32::MIN..=30` clamp followed by a nonnegative declared-result-scale cap. Resolved scale is checked as i32, never wrapped through i8. TiKV's existing `RoundMode::HalfEven` implementation is half-away for Decimal; its name does not authorize a bankers-rounding correction.
- **Legacy ROUND:** full Int128 identity, real ties-away, and shared exact Decimal Round(0) followed by existing Rust storage-value f64 conversion remain separate policies. The real evaluation channel casts only the owned result. NULL/missing inputs and original child-error/coercion precedence remain covered, not replaced by fabricated numeric inputs.
- **NULL/errors:** actual NULL/identity cases enter the worker; the special NULL witness accepts only an observed absent value, not Some(0). Pure builder LocalError stays opaque with phase None; kernel failures retain their actual causes. No invented SQL site or native retry follows a backend failure.

C4 appends its real finite retained-byte allowance as an internal raw-u64 Int budget; the getter restores usize and rejects usize::MAX. `NativeDecimalError`, including legacy-conversion codec errors wrapped in `Core`, is preserved explicitly through `EvaluateError::Caused`, not erased through the generic boxed-error string conversion or mislabeled as SQL overflow.

The original unchecked decimal scale subtraction/padding/unary-minus points follow the **actual overflow-check profile**, not `cfg(debug_assertions)`. A wrapped expansion is not silently replaced with mathematical zero: the shared worker preflights its logical word-buffer allowance and may refuse resources. These current-profile tests are not a separate release-execution receipt. The finite C4 contract is not a blanket claim about existing infallible datatype facades, which can still pass usize::MAX, or about allocator capacity, physical peak or OOM safety.

## Ten retained receipts

All test commands and exit codes are parent-owned; the writer read/grepped the ten raw logs and hashed them without rerunning Rust. [Exact commands, counts, chronology and whole-log SHA256 values](../logs/wide-math-decimal-summary.txt) are retained. Logs live in `/home/agent/tidb/expression-unification/logs`; filenames below have prefix `wide-math-decimal-`.

| Log suffix | Actual result | Exit |
| --- | --- | --- |
| `tikv-datatype.log` | Jan: 98 passed, 300 filtered, 0.01s; first compilation 3.34s | 0 |
| `native-datatype.log` | Aug: six compile diagnostics, zero tests executed | 101 |
| `native-datatype-rerun.log` | Aug: 90 passed, 347 filtered, 21.84s; compilation 2.24s | 0 |
| `tikv-datatype-rerun.log` | Jan: 98 passed, 300 filtered, 0.01s after shared ABS factoring; compilation 1.78s | 0 |
| `tikv-local.log` | Jan: 242 discovered, 241 passed, one old ignored, 471 filtered, 0.19s; compilation 11.66s | 0 |
| `tikv-math.log` | Jan: 48 passed, including two new tests, 665 filtered, 0.10s; compilation 0.12s | 0 |
| `dispatch.log` | Aug: three passed, 1524 filtered, 0.00s; compilation 15.83s | 0 |
| `legacy.log` | Aug: two focused unistore legacy ROUND tests passed, 193 filtered, 0.00s; compilation 8.55s | 0 |
| `sql.log` | Aug: 53 passed, 2078 filtered, 0.77s; compilation 32.53s | 0 |
| `expr-full.log` | Aug: 1527 discovered, 1429 passed, **four failed**, 94 ignored, 10.49s; compilation 0.16s | 101 |

The first Aug datatype attempt failed before any tests: E0308×4 and E0277×2 at then-current `decimal/mod.rs:230/237/243/271`, where shared u32 words met native i32 `CODEC_POWERS10`. Parent added only four `as u32` casts for that correction, preserving expected values, then reran the native gate. Later parent changed native shared ABS finishing to `value.abs()` and reran the Jan datatype gate. Both original and rerun logs remain; this was **not all first-attempt success**.

The new SQL coverage stores five rows including the signed-MIN error row; **four normal rows × thirteen function columns** are checked, plus the separate overflow result and **17 direct zero-slot probes**. This is not five successful projection rows. The SQL gate includes both new lifecycle tests; the dispatcher gate verifies receipt-bound overflow, wide/storage and typed result domains, and PB child/NULL precedence. Focused legacy tests verify all three raw domains and NULL/missing/child-error behavior.

The four full-suite failures remain `pushdown_catalog::tests::ifnull_string_column_literal_uses_go_signature_and_column_collation`, `tests::builtin_info_json_math_source::exp`, `tests::builtin_math_misc_op_source::vectorized_builtin_op_func`, and `time_fn::tests::str_to_date_partial_formats_follow_no_zero_date`.

For this run, parent compared the complete failure section against round24 `field-make-export-expr-full.log`: extract after the first `\nfailures:\n` and before `\ntest result:`, replace only the parenthesized thread number before `panicked` with THREAD, then compare bytes. Both normalized sections equal SHA256 `0930217d98e0b92d727527dc3c7cb7313f1bbe35e643da60114fa6d78203839b`. This is a fresh, scoped equality receipt, **not a whole-log hash**, a new-failure signal or a green-suite claim. Parent also reports both repository diff checks and both lockfile diff checks exited0.

## Remaining limits

Old expected values/fixtures were not weakened. Current decimal-filter and focused legacy receipts do not establish whole datatype, whole unistore, parser or workspace completion; previous broader unistore/parser non-green observations remain historical and were not all rerun. Release execution, make lint, deeper scope/guard validation, performance, allocator/physical-peak guarantees and final acceptance remain unverified.

After source freeze this writer created only this document and its new summary, without build/test/fmt, Plan, README, JSON, guide, index or source edits. This pair is frozen at **82/245**, final **0/245**; it is functional evidence, not PR readiness.
