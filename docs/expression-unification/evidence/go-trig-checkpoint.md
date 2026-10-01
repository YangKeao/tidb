# Go trig — go-trig-five-27

**Final functional checkpoint: seven actual Rust test runs, no retries or compile failures. Six scoped runs passed; the full expression suite retains four known failures.**

Functional delegation/native-deletion advances **84→89/245**; final acceptance remains **0/245**. The five whole frozen IDs are `sin`, `cos`, `tan`, `cot`, `atan`; `atan2` is an alias, not a sixth family. Legacy entries and pure test bridges earn no extra credit. This is not a completed/transcreated Go-package claim.

## Unique owner and exact migration scope

**NativeGo compatibility arithmetic is uniquely migrated, not replaced with supposedly equivalent wire/libm arithmetic.** Ordinary COT/ATAN inputs can differ by ULPs. TiKV `impl_math/native_go_trig.rs` owns the original constants, range reduction, SIN/COS/TAN approximations and the trailing Xatan/Satan/ATAN/ATAN2 production bodies. Wire and legacy genuinely share the small libm primitives instead; their result policies are still different.

The original native file's test block occupied lines314–447, between production lines1–313 and449–535. Both production regions moved; the ATAN tail was not lost. Original license/provenance remains. The native production module keeps no second Go implementation; its fixture imports are confined to the cfg(test) bridge (four used by the original block, six exposed by the native test-only adapter), while the empty production module can still support the existing rustdoc link.

The provider module is `pub(crate)` within TiKV, with six public pure functions: `go_sin`, `go_cos`, `go_tan`, `go_atan`, `go_atan2`, `trig_reduce`. C local exposes only those test-bridge exports across the crate boundary. Provider definitions cannot be cfg(test)-only because TiKV is a dependency of the native tests. Production native evaluation still enters the private RPN operations, not a pure-function bypass.

The three original native tests remain in their **unchanged 134-line block**. Parent's post-format byte comparison gives SHA256 `37e34eb959d142d59ccd06c6c4787f17a1ac37d06e4a4b6cdccf657ae0c0b129`; all three actually passed through the shared provider.

The source-copy receipts have deliberately different scopes: the **pre-format 400-line** migration matched pinned native production bytes after exactly six visibility changes, SHA256 `70407b5c603fd4f56f68c73e1a0c17b1ea3682002f26d24997eac41d0c43f1c7`. Jan formatting then compressed only the `shl` and `shr` if-expression layouts. The **current post-format module is 392 lines**, SHA256 `51b36fb03bbd265d12cb4e76e7e54b66f48f571ea0e75e1cb4f1896295630f88`. Parent compared it allowing exactly those six visibility changes and two known layout changes; everything else remained byte-identical. It would be false to label the final file “400 lines” or “raw bytes wholly unchanged.” Parent pinned-formatted/froze 18 Rust sources (nine native, nine TiKV including the new module).

## ABI and preserved policies

- **Eleven private operations:** six NativeGo entries for SIN/COS/TAN/COT/ATAN/ATAN2, and five LegacyLibm entries for SIN/COS/COT/ATAN/ATAN2. Unary operations use B→B, binary ATAN2 uses BB→B in **(y,x)** order; each scalar uses canonical raw IEEE754 LE8. There is no LegacyLibm TAN entry or new native/legacy PB TAN admission.
- **Computed results, not input replay:** every raw wrapper applies its selected arithmetic and encodes the computed f64. Native COT is exactly `1.0 / go_tan(x)`. Native `finite_float`, original GoError/AST rendering and existing coercion/NULL policies finish the computed result afterward, as with RadiansRaw; no new kernel cause or guessed policy replaces them.
- **Wire versus legacy:** wire retains Real NaN→NULL and COT infinity's existing overflow error/text. Legacy preserves computed raw NaN/infinity and libm values rather than adopting NativeGo or wire result filtering. Existing wire signatures, including their original math policy, are unchanged.
- **Demand/context:** ordinary native ATAN2 still coerces the right operand after a NULL left operand, unlike the existing PB short-circuit path. Admitted PB/legacy child demand and non-NULL PB context forwarding remain covered by focused representatives. NULL cases still use the admitted worker route rather than fabricated numeric inputs or native replay after a backend failure.

No new physical role, error-cause variant, context getter, driver/graph capability, four-column allowance or default/public admission is established by these B/BB operations. The six pure test exports do not add a production execution path.

## Seven retained receipts

Commands, exit statuses, pinned formatting and final source/golden comparisons are parent-owned. The writer read/grepped completed receipts and hashed all seven logs without rerunning Rust. [Exact commands, counts and whole-log SHA256 values](../logs/go-trig-summary.txt) are retained. Raw logs live under `/home/agent/tidb/expression-unification/logs`; suffixes below share `go-trig-`.

| Log suffix | Actual result | Exit |
| --- | --- | --- |
| `tikv-local.log` | Jan: 248 discovered, 247 passed, one old ignored, 475 filtered, 0.19s; compilation 9.45s | 0 |
| `tikv-math.log` | Jan: 52 passed, including two new raw-policy tests, 671 filtered, 0.10s; compilation 0.13s | 0 |
| `goldens.log` | Aug: three unchanged native Go tests passed, 1530 filtered, 0.00s; compilation 12.39s | 0 |
| `dispatch.log` | Aug: three passed, 1530 filtered, 0.00s; compilation 0.12s | 0 |
| `legacy.log` | Aug: two focused tests passed, 197 filtered, 0.00s; compilation 5.71s | 0 |
| `sql.log` | Aug: 57 passed, 2078 filtered, 0.85s; compilation 23.57s | 0 |
| `expr-full.log` | Aug: 1533 discovered, 1435 passed, **four failed**, 94 ignored, 10.58s; compilation 0.12s | 101 |

SQL coverage checks **four regular rows × seven trig forms**, an independent **fifth-row COT(0)** overflow case, and **16 direct zero-slot probes**. It does not claim five successful seven-column projection rows. The three dispatcher tests exercise selected reduction/signed-zero, NULL/coercion, overflow-pack and PB-context representatives, **not an exhaustive PB matrix**. Two focused legacy tests cover libm/raw specials and child/NULL demand. The old three-test golden block is not copied into a new fixture suite, and no old expected value was changed.

The full-suite failures remain `pushdown_catalog::tests::ifnull_string_column_literal_uses_go_signature_and_column_collation`, `tests::builtin_info_json_math_source::exp`, `tests::builtin_math_misc_op_source::vectorized_builtin_op_func`, and `time_fn::tests::str_to_date_partial_formats_follow_no_zero_date`.

Parent freshly compared the complete current failure section with round26 `char-conv-expr-full.log`: extract after the first `\nfailures:\n` and before `\ntest result:`, then apply only `r"(?m)^(thread '[^\n]*' \()\d+(\) panicked at)"` → `r'\1THREAD\2'`. The normalized sections are byte-identical, both SHA256 `0930217d98e0b92d727527dc3c7cb7313f1bbe35e643da60114fa6d78203839b`. This is a fresh scoped comparison, **not a whole-log hash**, copied historical proof or a green full-suite claim. Parent reports both repository diff checks and both lockfile diff checks exited0.

## Limits and freeze

No retries, compilation failures or expected-value corrections occurred in this batch. Earlier non-green receipts remain historical, not current passes. These focused checks do not establish whole unistore, parser, datatype or workspace completion, exhaustive PB coverage, separate release execution, make lint or broader scope/guard acceptance. No zero-copy, allocator-capacity, physical-peak, OOM-safety or performance proof follows from bit-exact fixtures or raw carriers.

After source freeze this writer created only this document and its new summary: no source, old expected, Plan, README, JSON, guide, index, build/test or fmt changes. The pair is frozen at **89/245**, final **0/245**, with the non-green full-suite receipt retained; it is functional evidence, not PR readiness.
