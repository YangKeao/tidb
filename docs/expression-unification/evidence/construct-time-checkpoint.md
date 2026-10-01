# MAKEDATE / FROM_DAYS / MAKETIME / SEC_TO_TIME checkpoint

Checkpoint **construct-time-four-39**, following `week-auth-five-38`: functional **124→128/245**, final acceptance **0/245**. Four operations/four families, not five. **Seven test-Cargo attempts = seven actual test runs: five green and two current baseline non-green full runs.** Compile failures/launch failures/retries/new product RED counts0; metadata commands0. One static-proof check correction means **all_checks_firstpass=false**. Exact commands/counts and all seven whole-log SHA256 values: [`../logs/construct-time-summary.txt`](../logs/construct-time-summary.txt).

Parent source scope: **14 Rust files (TiKV8/native6), zero manifest changes; both locks byte-identical to HEAD**. Both repositories' pinned format checks and both diffs passed their first checks (fmtfail0). No new result kind/report/cause/driver/PB/legacy route. The sole new typed role is MakeTimeParts; existing IEEE result/seconds-plus-FSP roles are reused.

MakeDateNative receives nullable Int year/day, preserves the original two-digit pivot and ordinary i64 `days_from_civil(...) + day - 1` order, without overflow repair. FromDaysNative receives nullable Int: 3652425..3652499 remains NULL, 366..3652424 yields its civil date, and the other non-NULL domain yields **actual canonical date text `0000-00-00`**. Native packing recognizes that computed result and restores the original zero-Time datum; it neither classifies the input number nor uses a validity marker. Time owns the single native civil inverse; the distinct wire day-number domain is not merged with it.

MakeTimePartsNative is nullable typed **B9/I/B8**: LE-i64 hour plus unsigned-byte0/1, nullable Int minute, and IEEE754 seconds. Its worker validates the native range, sign/UInt-wrap and overflow rules, then returns **actual signed total seconds** through existing OwnIeee754Bits, not a marker. Only after this complete first call returns Some does native request the original seconds (third) argument's FSP and make a second complete SecToTimeNative call. No recursion inside coercion/packing. Successful MAKETIME explicitly incurs extra transport/two leases, or **two one-shots without a capability**; first admission precedes the still-undemanded FSP.

SecToTimeNative uses existing Ieee754Int→OwnBytes: **(None,None) means NULL seconds plus FSP not demanded**, not two SQL NULL arguments; Some/Some requires IEEE8 plus a nonnegative usize-representable actual i64 scale, **not a6/u8 cap**. Native duration_precision/int_arg/number_arg policies remain; raw Time FSP7 and the original bad-Duration number_arg panic are not repaired. The unique moved formatter retains shortest-decimal half-up, NaN/inf/-0 behavior and **second-only carry without minute/hour normalization**. Original SQL typed-result parsing uses its original context.

SQL83 includes E3: strict NO_ZERO_DATE returns genuine zero Time without warnings; date windows/leap/FROM_DAYS NULL band; decimal FSP/UInt max/actual Duration; early NULL still permits original third-argument1292. SEC_TO_TIME('123x') keeps whole-parse0+1292, while invalid UTF-8 bytes keep0 without warning. **All15 new zero-slot calls return Resource1105: two retain original1292, thirteen have empty warnings.** No all-warnings-empty claim.

D3 observes two facades separately, not a cumulative total. Its public raw FSP7 witness is **SEC_TO_TIME only**, using zero Time bits0b1111. C3 combines original source fixtures and explicitly hand-derived policy literals (3020399 seconds bits, zero/FSP7 seven zeros, MAKEDATE(2012,1)); these are not mislabeled old fixtures or outputs recorded from the new provider. A's second60/FSP7 literal is likewise hand-derived from the old formatter, not a normalization fix. Existing expected fixtures were unchanged. Full expr actually exercises the old make/from/sec source fixtures; no extra standalone gate is credited.

**Static-proof correction:** the first inverse-civil free-function→impl-method body comparison asserted/exit1 because it omitted the method's extra four-space indentation. Removing precisely that structural indentation produced an empty diff/body-byte identity; no source repair. Original FSP/int_arg/number_arg bodies are byte-identical. Parent also verified scalar_function.rs, arg_eval_type.rs, time_fn/tests.rs and the PB/coph files (five files) byte-identical to baseline0c2b. Formatter identity is after comment/whitespace removal, **not raw-body identity**; the native formatter body was deleted, and all three existing wire date/time bodies remain byte-identical.

The parent's diagnostic wording change says **jointly absent/present**, replacing jointly NULL to describe FSP-not-demanded accurately; normal arithmetic is unchanged. This round's static correction is unrelated to Round39's SM3-doc proof correction or parser-all compile failure. Round39 parser all remains historically unresolved, but neither that failure nor isolated auth20 is a Round40 run or credit.

## Current logged gates (Finished time is not a benchmark)

| `construct-time-` suffix | Passed / ignored / failed; filtered | Test / compile seconds; exit |
| --- | --- | --- |
| `tikv-time.log` | 51 / 0 / 0; 361 | 0.01 / 2.05; 0 |
| `tikv-local.log` | 266 / 1 old / 0; 499 (267 discovered) | 0.19 / 8.98; 0 |
| `tikv-kernels.log` | 65 / 0 / 0; 701 | 0.01 / 0.12; 0 |
| `sql.log` | 83 / 0 / 0; 2078 | 1.26 / 18.80; 0 |
| `dispatch.log` | 3 / 0 / 0; 1562 | 0.00 / 13.87; 0 |
| **`expr-full.log`** | 1467 / 94 / **4**; 0 (1565 discovered) | 10.39 / 0.13; **101** |
| **`unistore-full.log`** | 193 / 13 / **1**; 0 (207 discovered) | 2.99 / 5.17; **101** |

Parent compared complete current failure sections against week-auth logs, normalizing **only panic-heading thread IDs**: expr `80be9bda05e5bf630e9c246ca9523d23f82c122eaaeec6e220b1b00cec436615` (same four, including EXP FloatOverflow; duration212:55), unistore `e285bfdba646f2d01f85b485ae317cc07c51b78cf0ae6d664c0fb2bc39259759` (same DECIMAL-'abc', source194:5). No location mapping. These are failure-section hashes, not whole-log hashes, and both current full suites remain non-green.

Writer read/hashed the seven current logs, wrote only these two docs and did no build/test/fmt/index/Plan/additional source audit. Source/gate/proof ownership and exit codes are parent-reported; whole-log hashes and displayed results were read directly. No metadata command or previous-round attempt is folded into the seven.

Full workspace/Go-package/complete-temporal acceptance, lint, release/profile/panic-test adaptation, allocation/OOM peak, performance/zero-copy and M6 remain unverified. This checkpoint is **128/245 functional, final0/245**, not PR readiness; unchanged failure sections do not establish whole-suite success.
