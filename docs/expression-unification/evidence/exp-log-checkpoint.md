# EXP and LOG10 — exp-log-two-28

**Final functional checkpoint: six actual Rust test runs, all first attempts, with no compile failure, retry, oracle correction or old expected-value change. Five scoped runs passed; the full expression suite still has four known failures.**

Functional delegation/native-deletion advances **89→91/245**; final acceptance remains **0/245**. The two whole frozen IDs are `exp` and `log10`, with no additional alias credit or new PB/legacy entry. Internal `go_log`/`go_frexp` helpers do not credit LOG/LN families. This is not a completed/transcreated Go-package claim.

## Unique owner and source preservation

The existing native compatibility body moves to TiKV `impl_math/native_go_exp_log.rs`, not to the existing wire libm implementations. Its module is private and its two providers remain `pub(crate)`; only the two closed private RPN recipes are used by native production. No local pure-function export or native test bridge is added: the deleted native file had no embedded test block.

Parent's **post-format** comparison against `a80a017a` confirms that the entire suffix from `pub(crate) fn go_exp` through EOF is **136 lines, byte-identical**, SHA256 `c4ed9c6a2fee9e42831d33e3f8c184a7da9a220a08d6abfc9db8e72016e288e4`. This includes `mul_add`, `round_ties_even`, underflow handling, coefficients, LOG10's reciprocal and the existing `go_frexp` subnormal condition. That condition was preserved, not repaired; copy equality is not full-domain mathematical validation.

The first 13 license lines remain identical. The old 170-line native file is deleted; the new complete module is **168 lines**, post-format SHA256 `559aafe7018c76934e98a00a5a0db25411dbb4258711ef6b7bd694d4ab64dc3c`, also identical to the pre-format new-file hash. Only the header was revised to describe the actual retained **amd64 FMA oracle path**, not every Go CPU/non-FMA implementation, and to use the valid sibling trig reference. Wire EXP and LOG10 function bodies remain unchanged.

The source manifest has **12 Rust paths**: native five (one deletion, four live) and TiKV seven (one new module, seven live). All **11 live** sources were pinned-formatted. The factory integration stayed within its five-source scope rather than seven, without changes to its mod/expr_eval surfaces. Parent reports the old `tests/math.rs` and `builtin_info_json_math_source.rs` diffs are zero. No PB routing or legacy admission was added.

## Computation and diagnostic boundaries

- **Two exact private operations:** `ExpGoNative`/`exp_go_native_fn_meta` and `Log10GoNative`/`log10_go_native_fn_meta`. Both use existing nullable canonical raw-IEEE LE8 **B→B**, producing `OwnIeee754Bits` from the computed f64 rather than a Real projection or replay of input bytes.
- **EXP:** the frontend consumes the computed result before its original non-finite packing/diagnostic policy. The `Cell`-retained coerced input is used only to render the original diagnostic after a computed non-finite result, not to predict overflow or synthesize a result. A missing retained input is an internal `Unsupported` invariant failure, **not** a typed `ScopeContract` guarantee.
- **LOG10:** coercion and the original **3020** domain warning precede backend admission. The worker receives the actual input bits; the computed result is consumed before the frontend applies its original domain-NULL policy. Native NaN/+Inf results retain their original raw policy instead of being forced through EXP's finite-result policy or wire Real filtering.
- **NULL/resource behavior:** the admitted nullable worker path remains real. Zero-slot failures preserve already-issued coercion/domain warnings, but do not fabricate overflow from an unevaluated EXP result. No backend error triggers native replay.

There is no new carrier, cause variant, driver, four-column expansion, pure test bridge or PB/legacy operation. The original frontend error renderer and wire math policies are not replaced with generic diagnostic guessing.

## Six retained receipts

Commands, exit statuses, formatting and final source/failure-section comparisons are parent-owned. The writer read/grepped the completed logs and hashed all six, without rerunning Rust or modifying source. [Exact commands, counts and whole-log SHA256 values](../logs/exp-log-summary.txt) are retained. Raw logs are under `/home/agent/tidb/expression-unification/logs`; suffixes below share `exp-log-`.

| Log suffix | Actual result | Exit |
| --- | --- | --- |
| `tikv-local.log` | Jan: 249 discovered, 248 passed, one old ignored, 477 filtered, 0.19s; compilation 9.04s | 0 |
| `tikv-math.log` | Jan: 54 passed, including two new raw-value tests, 672 filtered, 0.10s; compilation 0.14s | 0 |
| `native-math.log` | Aug: original 20 passed, 1515 filtered, 0.01s; compilation 12.29s | 0 |
| `dispatch.log` | Aug: two passed, 1533 filtered, 0.00s; compilation 0.13s | 0 |
| `sql.log` | Aug: 59 passed, 2078 filtered, 0.88s; compilation 22.60s | 0 |
| `expr-full.log` | Aug: 1535 discovered, 1437 passed, **four failed**, 94 ignored, 10.56s; compilation 0.15s | 101 |

The two new small kernel tests cover the existing EXP(1.5)=4.481689070338065 and LOG10(100)=2 points, NULL and raw NaN/±Inf. The original 20-test native math gate includes `exp_matches_go_source_vectors_and_arity` and `log10_source_vectors`; it is not a newly copied fixture suite. Two dispatcher tests are limited bit/policy/NULL/admission representatives, not full-domain verification.

SQL coverage comprises **four rows × two columns**, two independent domain rows with **3020**, a date-text case whose **1292** conversion warning precedes **1690** with the original `exp(2020)` diagnostic, and **six direct zero-slot probes**. The resource probes retain actual 1292/3020 warnings without inventing overflow. These are row/probe counts, not additional Rust test runs.

## Retained full-suite failure and limits

The four failures remain `pushdown_catalog::tests::ifnull_string_column_literal_uses_go_signature_and_column_collation`, `tests::builtin_info_json_math_source::exp`, `tests::builtin_math_misc_op_source::vectorized_builtin_op_func`, and `time_fn::tests::str_to_date_partial_formats_follow_no_zero_date`. In particular, the old `EXP(100_000.0)` assertion still expects `EvalError::FloatOverflow` and **still fails**. Its preexisting discrepancy was neither resolved nor rewritten green by this batch.

Parent freshly compared the complete current failure section with round27 `go-trig-expr-full.log`: extract after the first `\nfailures:\n` and before `\ntest result:`, then apply only `r"(?m)^(thread '[^\n]*' \()\d+(\) panicked at)"` → `r'\1THREAD\2'`. The sections are byte-identical after that normalization, both SHA256 `0930217d98e0b92d727527dc3c7cb7313f1bbe35e643da60114fa6d78203839b`. This is fresh section-scoped evidence, **not a whole-log hash**, historical proof substituted for a current comparison, or a green full-suite claim. Parent reports both repository diff checks and both lockfile diff checks exited0.

The focused tests and unchanged body do not establish exhaustive f64/subnormal/CPU coverage, whole datatype/unistore/parser/workspace completion, separate release execution, make lint or broader scope/guard acceptance. No zero-copy, allocator-capacity, physical-peak, OOM-safety or performance proof follows from raw carriers or bit samples. Earlier non-green observations remain historical, not current passes.

After source freeze this writer created only this document and its new summary: no source, old expected, Plan, README, JSON, guide, index, build/test or fmt changes. The pair is frozen at **91/245**, final **0/245**, with all four full-suite failures retained; it is functional evidence, not PR readiness.
