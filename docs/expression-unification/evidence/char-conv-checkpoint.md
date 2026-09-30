# CHAR and CONV — char-conv-two-26

**Final functional checkpoint: eight actual Rust test runs, six green, one retained first-SQL assertion failure and one full-expression run with four known failures. There was no compile-only failure.**

After the actual SQL rerun and full-suite receipt, functional delegation/native-deletion advances **82→84/245**; final acceptance remains **0/245**. The two complete frozen family IDs are `char_func` (SQL `CHAR`) and `conv`. `char` is a spelling, not another frozen ID; legacy handling and compatibility helpers earn no duplicate credit. This is not a completed/transcreated Go-package claim.

## Source scope and ownership

**CHAR adds a new single-owner TiKV compatibility packing core. CONV genuinely reuses existing wire prefix extraction, u64 radix parsing, signed-magnitude clamping and uppercase magnitude formatting.** These are different kinds of reuse; this checkpoint does not claim a preexisting wire CHAR implementation or identical CONV policies across all consumers.

Four private operations have physical shapes **B / BII / BII / BBB**: CHAR computed packing, native text CONV, native binary-literal CONV and legacy CONV. Mode belongs to the closed operation, not a fourth input. The CHAR blob can represent the existing variadic logical operands without widening the driver. No new facade graph, four-column ABI, generic driver allowance, PB/unistore signature or default/public admission is claimed.

Parent froze and pinned-formatted 19 source files (nine native, ten TiKV). The six duplicated helpers `append_char_integer`, `conv_text`, `to_radix_upper`, `conv_convert`, `valid_prefix` and `format_u64_base` are removed. The old native `conv_valid_prefix` exists only as a cfg(test) thin call into `conv_valid_prefix_native`; that shared prefix-only helper deliberately **does not trim**. Top-level CONV owns trimming, and no second native scan loop remains.

## Policies retained, not flattened

- **CHAR:** the existing frontend keeps guarded numeric coercion and charset lookup before shared computed-byte packing, followed by the original decoding, warning/strict-mode and collation handling. An empty/all-NULL numeric list is not replaced with a fabricated numeric input. No extra charset/packet/SQL-mode policy getter is introduced; the existing C4 capability-discovery path now applies to these migrated calls.
- **Native CONV preparation:** sentinel rejection precedes base NULL checks, signed-i64 base readings, number NULL and strict text coercion. PB routing preserves the real execution context, ordinary sentinel precedence and existing child/NULL demand; normal non-NULL PB zero-slot rejection checks that the context was not dropped.
- **Binary literal:** the complete original payload becomes the original trim-leading-zero bit representation, then **2→from→to**. First-stage NULL or overflow terminates before the second stage, even if the final target is invalid. Empty literals produce zero under valid bases; no preliminary u64 narrowing/truncation replaces this path.
- **Three CONV policies:** wire retains original-sign formatting, wrapping base normalization, lossy text and its original `conv(...)` overflow expression. Native and legacy recompute sign from wrapped u64 bits: U64MAX to -10 is `-1`, and negative zero loses its sign. Their original unchecked from-then-to negations precede radix validation and follow the actual overflow-check profile, not `cfg(debug_assertions)`.
- **Legacy demand/full width:** both base columns carry nullable canonical LE16 **full i128** values. The kernel checks input NULL, then from NULL/i64 range, then to NULL/i64 range, before mathematics. A merely invalid from radix does not skip to. An out-of-i64 non-NULL base produces NULL inside the worker rather than arriving as a fabricated NULL input; skipped operands retain validated non-NULL irrelevant representations.
- **Overflow provenance:** only the two native private kernels' actual `ParseIntError` with `IntErrorKind::PosOverflow` branch emits `ConvUnsignedOverflow { digits, source }`. The complete signless valid prefix retains case and leading zeros, including digits after the overflow point. The original parser error is explicitly held in `Caused` (source code10000), while the semantic marker itself is fixed code1690/EVAL. Native mapping requires the actual invocation receipt, not code/text guessing. Legacy parse overflow produces actual NULL; invalid-digit/UTF8/resource errors do not acquire the native overflow marker. No backend failure triggers native replay.

## Eight retained receipts

All Rust commands, exit statuses, formatting and the test-oracle correction are parent-owned. The writer read/grepped completed receipts and hashed all eight logs, without rerunning Rust. [Exact commands, chronology and whole-log SHA256 values](../logs/char-conv-summary.txt) are retained. Raw files are under `/home/agent/tidb/expression-unification/logs`; suffixes below share `char-conv-`.

| Log suffix | Actual result | Exit |
| --- | --- | --- |
| `tikv-local.log` | Jan: 246 discovered, 245 passed, one old ignored, 473 filtered, 0.19s; compilation 11.11s | 0 |
| `tikv-math.log` | Jan: 50 passed, including the two new CONV units, 669 filtered, 0.10s; compilation 0.13s | 0 |
| `tikv-string.log` | Jan: original 63 passed, 656 filtered, 0.02s; compilation 0.12s | 0 |
| `dispatch.log` | Aug: three passed, 1527 filtered, 0.00s; compilation 14.34s | 0 |
| `legacy.log` | Aug: two passed, 195 filtered, 0.00s; compilation 6.68s | 0 |
| `sql.log` | Aug: 55 discovered, **54 passed, one new assertion failed**, 2078 filtered, 0.79s; compilation 27.93s | 101 |
| `sql-rerun.log` | Aug: 55 passed, 2078 filtered, 0.81s; compilation 3.14s | 0 |
| `expr-full.log` | Aug: 1530 discovered, 1432 passed, **four failed**, 94 ignored, 10.37s; compilation 0.13s | 101 |

The first SQL run reached the new `evaluated_ascii_shared_pool_char_conv_dispatch_sql_values_metadata_and_diagnostics` test and failed its STRICT_TRANS_TABLES warning-text assertion at then-current `lifecycle.rs:4966:9`: actual `(1300, "Invalid utf8mb4 character string: 'FF'")`, expected `(1300, "Invalid utf8 character string: 'FF'")`. This was a **runtime test assertion failure, not compilation failure**. The later lax subcase had not executed in that attempt; the separate new zero-slot test did pass.

Parent and E checked the unchanged pre-migration decoder: `multibyte_encoding.rs:245` maps utf8 to `Encoding::Utf8`, line107 names it utf8mb4, lines198–206 construct the error from `self.name()`, and lines88–94 display it. Parent changed only the new assertion's charset word at then-current line4968, **utf8→utf8mb4**. Column metadata remains **utf8/Utf8Bin**; production decoder diff is zero, no production behavior or old expected value/fixture was changed for this correction. The original failed log remains. The rerun actually executed both strict and lax cases and passed: **not all first-attempt success**.

The completed SQL coverage includes a main **five rows × five function columns**, a separate **four rows × one CHAR5/UTF8 column**, one row each for strict/lax handling, an independent overflow case and **16 direct zero-slot probes (seven CHAR, nine CONV)**. These are SQL row/probe counts, not additional Rust test receipts. Dispatcher tests cover coercion/charset/decode order, real overflow receipts and PB demand/context; the two focused legacy tests cover full i128, NULL, out-of-range and child demand.

The full expression failures remain `pushdown_catalog::tests::ifnull_string_column_literal_uses_go_signature_and_column_collation`, `tests::builtin_info_json_math_source::exp`, `tests::builtin_math_misc_op_source::vectorized_builtin_op_func`, and `time_fn::tests::str_to_date_partial_formats_follow_no_zero_date`.

Parent freshly compared the complete current failure section with round25 `wide-math-decimal-expr-full.log`: extract after the first `\nfailures:\n` and before `\ntest result:`, then apply only `r"(?m)^(thread '[^\n]*' \()\d+(\) panicked at)"` → `r'\1THREAD\2'`. The normalized sections are byte-identical, both SHA256 `0930217d98e0b92d727527dc3c7cb7313f1bbe35e643da60114fa6d78203839b`. This is a fresh scoped comparison, **not a whole-log hash**, copied historical proof or a green full-suite claim. Parent reports both repository diff checks and both lockfile diff checks exited0.

## Limits and freeze

These focused receipts do not establish whole unistore, parser, datatype or workspace completion. Earlier broader non-green observations remain historical. The current-profile MIN comparison is not a separate release-execution receipt. No zero-copy, allocator-capacity, physical-peak, OOM-safety or performance claim follows from computed packing or logical limits; make lint, broader scope/guard gates and final acceptance remain unverified.

After source freeze this writer created only this document and its new summary: no source, old expected, Plan, README, JSON, guide, index, build/test or fmt changes. This pair is frozen at **84/245**, final **0/245**, with both non-green receipts retained; it is functional evidence, not PR readiness.
