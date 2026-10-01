# Expression unification experiment

Checkpoint-ID: `json-storage-quote-three-31` (previous: `json-introspection-three-30`)

**99/245 frozen families delegate to TiKV with native evaluator algorithms removed; target 221.** This checkpoint adds JSON_STORAGE_FREE, JSON_STORAGE_SIZE and JSON_QUOTE. Strict final-audited acceptance remains **0**; this is incomplete and not PR-ready.

Paired branches: [TiDB](https://github.com/YangKeao/tidb/tree/expression-unification-demo) · [TiKV](https://github.com/YangKeao/tikv/tree/expression-unification-demo). Use sibling checkouts.
`checkpoint.json` pins TiKV and the published Plan hash. Root Plans mirror `/home/agent/tidb/EXPRESSION_UNIFICATION_PLAN.md` at publication; paired pushes do not force-push or create PRs automatically.

## This checkpoint

- **Storage:** actual worker parsing feeds FREE's computed zero and SIZE's shared encoder-layout/public native-size primitive, including the root byte. No duplicate varint algorithm or binary-encoding bridge introduces u16 key limits. Typed binary-size and JSON path-quote helpers are not conflated with these SQL families.
- **Quote:** one traversal retains distinct native serde and wire escaping. Native HTML and U+2028/U+2029 handling stays unchanged. Checked `6n+2` reservation is not preservation of the old allocation pattern, a physical-peak bound or an OOM-safety guarantee.
- **Error phases:** storage coercion stays guarded; parsing occurs after admission, so zero-slot refusal precedes malformed/empty JSON errors. This explicitly changes former error precedence. QUOTE's original UTF-8 and 3064 input errors still precede admission. NULL/empty values reach real workers.
- **Scope:** 15 Rust files (TiKV 9, native 6); no new module, result kind, input role, NoArgs case, PB/legacy admission, driver or four-column allowance. UNQUOTE and other JSON algorithms remain outside this claim.

## Actual validation

| Final run | Result |
|---|---|
| TiKV datatype | 35 passed |
| TiKV local evaluator | 253 passed, 1 existing ignored |
| TiKV JSON kernels | 21 passed |
| Native JSON | 40 passed |
| Original JSON source tests | 30 passed |
| New native dispatch tests | 2 passed |
| SQL/lifecycle | 65 passed |
| Full native expression library | **1445 passed, 4 unchanged failures, 94 ignored; 1543 total, exit 101; 10.45 s** |

**8 actual runs: 7 green, 1 known non-green; no compilation failures, retries or expected-value edits.** Pinned formatting/checks covered all 15 files; lockfiles stayed unchanged and diff checks passed. Original JSON/source fixtures, json2/construct/jcodec test blocks and UNQUOTE-onward source remain unchanged. The full failure section matches checkpoint30 after only thread-ID normalization: SHA-256 `0930217d98e0b92d727527dc3c7cb7313f1bbe35e643da60114fa6d78203839b`.
SQL checks cover four rows across three families, two actual control-byte HEX columns, four 3140 diagnostics and eight zero-slot calls. A 65536-byte key with NULL retains the old size 65556; the inline case remains 24. Only that long-key test uses a `4 × 64 KiB` call cap, not a production-policy change or peak-allocation claim.

Exact commands: [summary](logs/json-storage-quote-summary.txt). Ownership and compatibility: [evidence](evidence/json-storage-quote-checkpoint.md).

## Remaining work

Next read-only candidates, **not credited**: YEAR, MONTH, DAYOFMONTH and QUARTER; DAY aliases DAYOFMONTH, not another family. Only MONTH has existing PB/legacy paths, which must also connect. Any future bridge must retain zero/invalid date fields without validation and preserve ETDatetime casts, getters and warnings; this is not a shared-Time migration. JSON_LENGTH remains deferred. DAYOFWEEK/DAYOFYEAR are also deferred: SQL rejection, public-helper Gregorian normalization and TiKV chrono panic/warning policies are not interchangeable through a strict constructor. No next-batch implementation is claimed.
Operation-scope coverage, allocation/physical-peak checks, paired differential reruns, release performance, whole workspace, `make lint` and TiFlash remain unfinished. No whole-JSON-codec or complete Go-package claim is made; full datatype, unistore and parser-charset suites were not rerun.
