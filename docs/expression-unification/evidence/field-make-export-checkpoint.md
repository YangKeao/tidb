# FIELD, MAKE_SET and EXPORT_SET — field-make-export-three-24

**Final functional checkpoint: seven actual Rust logs, including one native compilation failure and six test runs; the full expression suite remains non-green.**

After the backend/native/SQL gates, functional delegation/native-deletion advances **74→77/245**; final acceptance remains **0/245**. The three added frozen IDs are `export_set`, `field`, `make_set` ([coverage baseline](coverage-baseline.json), lines149/151/241), not aliases or small-arity subsets. The denominator is unchanged. This is not a completed/transcreated Go-package claim.

All **15 source files** are frozen and parent-formatted with pinned toolchains: native7 (B2+A1+D3+E1) plus C8. A changed only `builtin_ext/string2.rs`; `builtin_ext/mod.rs` already forwards ctx, and D needed no scalar ctx-only diff. Parent's later two-constructor test fix is recorded below, not hidden as a first-attempt pass. [Exact commands/results and writer checks](../logs/field-make-export-summary.txt) preserve all seven receipts.

## Single ownership and unchanged admission

**EXPORT_SET gets a newly migrated TiKV single-owner compatibility core; there was no original EXPORT_SET wire core to reuse.** Its native value and extension surfaces converge only after their distinct original preparation policies. No new wire/PB/unistore signature or public/default projection admission is claimed. FIELD comparison/first-match and MAKE_SET joining use shared backend helpers with the existing wire surfaces while retaining their policy differences.

The agreed recipes use one packed Bytes input for each FIELD mode and MAKE_SET, and **three physical inputs (Bytes, Int, Int)** for EXPORT_SET. They carry actual readiness and arity, not fabricated SQL results. No generic driver, four-column allowance or graph admission is broadened. Pure preparation failures retain the actual LocalError through opaque ExpressionRuntimeFailure with **phase None**, without inventing an invocation phase or SQL site. No native replay follows a backend failure.

## FIELD: three domains and coercion cutoff

FIELD keeps the string/collation, tagged signed/unsigned integer, and IEEE754 real domains. String/integer candidate NULLs preserve their positions and skip needle conversion for that comparison; real mode still converts its needle once even if all candidates are NULL. Shared `field_bytes_equal`, `field_int_equal` and `field_real_equal` determine the original coercion cutoff, not a locally returned SQL index.

`FieldReady` carries the real SQL arity, coerced prefix, mode/collation, and `FieldTerminal::{NeedleNull, Matched, Exhausted}`. A NULL needle and no-match/zero results still enter the worker. A first match stops later value coercions and their diagnostics, **not SQL-child evaluation**: the preexisting eager child/wrap behavior is unchanged. Integer precision above 2^53, signedness, collation and real-mode warnings remain distinct obligations; source inspection alone is not their runtime receipt.

## MAKE_SET: native demand and actual overflow profile

The existing **value-helper arity of one** (mask only) remains in scope: a non-NULL mask produces the empty string through the worker; a NULL mask remains NULL through the worker. This does not authorize changing the wire minimum arity. Selected operands retain strict `coerce_str`: selected NULLs are omitted from the join, while selected empty strings remain. Unselected operands are `Undemanded`, not fake NULLs. A NULL mask preserves its original lack of candidate coercion. This is selected-coercion demand, not lazy SQL children. The native adapter no longer joins results.

Native selection is the shared original expression `mask & (1_u64 << idx) != 0`. At the 65th candidate/index64 and beyond its behavior follows the **actual overflow-check profile**, not `cfg(debug_assertions)`. No unconditional debug-panic or release-wrap result, and no release execution, is claimed here. The original wire surface retains its rolling signed bit: after bit63 it becomes zero rather than adopting native shifting. Shared comma joining preserves these separate selection policies.

The passed C regression uses `black_box(64_usize)` with masks0/1, separately catches the old expression and shared selector, and compares boolean-or-panic outcomes under the **same active profile**, without assuming either outcome. It also verifies the wire65th candidate is not selected. This is not a release observation or a >64 native SQL execution; the new SQL fixture deliberately stays below that boundary.

## EXPORT_SET: preserve two frontend policies

The B value entry performs its original whole-argument NULL precheck before conversion. It records the real NULL operand's position; a later NULL is **not** rewritten as NULL bits. Non-NULL text still uses eval_string followed by Rust `String::from_utf8_lossy`, with the original typed integer reads.

The A extension entry first checks actual NULL bits, otherwise retains `to_i64_signed` (including UInt bits and existing numeric/UTC conversion). It then evaluates **both on/off tuple elements left-to-right with strict coerce_str**: on=NULL still demands off, so off's UTF8/coercion error wins over that NULL. Only a completed nullable tuple can stop separator/count demand. Separator is next; its NULL/error skips count. Count is last. Count0 does not bypass earlier string coercion. No packet getter, warning policy or ctx timezone conversion is introduced.

`ExportSetReady(PreparedExportSetArgs)` uses ReadyIntArg/ReadyBytesArg `Value(Some(...))`, real `Value(None)`, and `Undemanded`. Outer separator/count None means an omitted SQL operand; count Some requires separator Some. Undemanded is allowed only with a genuine observed NULL witness, and cannot be replaced by fake numbers/strings/SQL NULLs. All legal NULL and empty outcomes still require the actual worker; coercion errors retain their original earlier boundary.

The backend owns three/four/five-argument defaults (comma,64), count0, the out-of-range count clamp to64, and output generation. The signed `(bits & (1_i64 << bit)) > 0` rule remains: bit63 is off even for UIntMAX. Neither native frontend retains a bit loop or join. Results use into_bytes followed by the original Datum::new_string.

## Seven actual logs and preserved failure history

The first January compilation passed in9.32s. The first August dispatcher attempt failed compilation with **two E0277 diagnostics in A's new tests, zero tests executed**: native Decimal has no FromStr. Parent changed only the two `"2.5".parse().unwrap()` constructors to `tidb_datatype::Decimal::from_literal("2.5")`, preserving values/expectations and leaving datatype unchanged. The failed log remains; the successful dispatcher retry has a separate log. Extension/SQL commands after the failed attempt's `&&` were **not started**, not zero-pass runs or extra logs.

| Actual scope | Result and exit |
|---|---|
| TiKV local | 237 discovered;236 passed,1 ignored,469 filtered;0.19s; exit0 |
| Original wire string | 63 passed,643 filtered;0.02s; exit0 |
| First native dispatcher compile | 2 E0277, no tests executed; exit101 |
| Native dispatcher retry | 3 passed,1521 filtered;0.00s; exit0 |
| Complete string2 extension module | 18 passed,1506 filtered;0.01s; exit0 |
| SQL evaluated_ascii_ | 51 passed,2078 filtered;0.70s; exit0 |
| Full expression | 1524 discovered;1426 passed,4 failed,94 ignored,0 filtered;10.41s; **exit101** |

Both new A regressions passed in the extension run. The three D tests passed, including eager-child/coercion-cutoff distinctions, mask-only MAKE_SET and real ext-NULL root refusal. Both new E tests passed: **five rows, nine function outputs plus two controls (11 columns), and17 direct zero-slot probes**; FIELD/MAKE_SET use >4 candidates and EXPORT_SET exercises3/4/5arity. Zero-slot errors are PoolResource/Pool, evaluation-origin1105/HY000, with no warning row. Original expected values/fixtures are unchanged.

Parent asserted the entire current failures section equals `variadic-oct-elt-expr-full.log` after only thread-ID normalization. This round extracts after `\nfailures:\n` and before `\ntest result:`, replacing numeric thread IDs with `THREAD`; normalized-section SHA256 is `0930217d98e0b92d727527dc3c7cb7313f1bbe35e643da60114fa6d78203839b`. This explicitly scoped hash is not a whole-log hash or a new-failure signal; historical hashes are not substituted. All four old failures remain, so the full suite is not green. Parent reports both repo diff checks and both lockfile diff checks exit0.

## Remaining limits

Packing and owner/column storage can allocate or retain memory; encodings and logical output bounds prove neither allocator/physical-peak/OOM guarantees nor performance. No current standalone datatype/collation, unistore/parser or generator validation is claimed; historical non-green observations remain visible. Whole workspace, make lint, deeper scope/guard validation, release execution, physical-resource guarantees and final acceptance remain unverified. This writer ran no build/test/fmt and changed no earlier evidence, Plan or index. Source and these two documents are frozen at **77/245**, final **0/245**, not PR-ready.
