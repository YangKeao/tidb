# JSON_STORAGE_FREE / JSON_STORAGE_SIZE / JSON_QUOTE checkpoint

Checkpoint **json-storage-quote-three-31**, following `json-introspection-three-30`: functional **96→99/245**, final acceptance **0/245**. Credit only `json_storage_free`, `json_storage_size`, `json_quote`. Exact commands and eight whole-log hashes: [`../logs/json-storage-quote-summary.txt`](../logs/json-storage-quote-summary.txt).

## Shared implementation and policy boundaries

- Storage reuses the native-document parser and TiKV layout primitives. FREE actually parses before computing0; SIZE's shared estimator includes the root byte and preserves the old `usize as i64` cast. Neither answer is precomputed in the host; long text keys are not truncated through a binary-JSON bridge.
- QUOTE has **one policy-driven quoting traversal**, with common quote/backslash/ordinary writes. Wire keeps `07→\a`, `0b→\v` and its other raw controls; Native keeps serde string semantics: common short escapes, otherwise lowercase `\u00xx` for `00..1f`. HTML, U+2028/U+2029 and UTF-8 bytes stay literal, not Go binary-JSON separator marshaling.
- All three NULL paths execute nullable workers. Storage uses the existing computed `JsonReport` integer/status envelope; QUOTE is ordinary owned Bytes. Invalid direct-C QUOTE UTF-8 is an `Other` error, not lossy conversion. Resource/transport failures are not fabricated JSON statuses; no new argument role, driver, context getter, cause, fourth internal column or PB/legacy admission.
- **Ordering is not wholly unchanged:** original source-type/UTF-8/numeric preparation precedes admission, but actual storage parsing follows it, so zero slots preempt bad-JSON domain errors. QUOTE's strict UTF-8 and original type-error (3064) preparation still precede zero-slot refusal.
- Typed-binary physical sizing and JSON-path quoting are different APIs, not this textual-storage/SQL-QUOTE contract; they are not claimed migrated by this checkpoint.

## Eight current Rust receipts

Exactly eight runs in this batch: **seven scoped green, one retained non-green full run**; no preliminary wave, duplicate run, compile failure, failed-target retry, old-expected/oracle modification or fixture recording. Writer globbed, grepped/read and SHA256-hashed all eight; execution/exits and source/coverage proofs are parent-owned. `Finished` times below are not benchmarks.

| `json-storage-quote-` suffix | Passed / ignored / failed; filtered | Test / compile seconds; exit |
| --- | --- | --- |
| `tikv-datatype.log` | 35 / 0 / 0; 366 | 0.00 / 1.92; 0 |
| `tikv-local.log` | 253 / 1 old / 0; 483 (254 discovered) | 0.19 / 9.97; 0 |
| `tikv-kernels.log` | 21 / 0 / 0; 716 | 0.00 / 0.14; 0 |
| `native-json.log` | 40 / 0 / 0; 1503 | 0.32 / 12.97; 0 |
| `native-source.log` | 30 / 0 / 0; 1513 | 0.27 / 0.16; 0 |
| `dispatch.log` | 2 / 0 / 0; 1541 | 0.00 / 0.12; 0 |
| `sql.log` | 65 / 0 / 0; 2078 | 1.07 / 24.35; 0 |
| `expr-full.log` | 1445 / 94 / **4**; 0 (1543 discovered) | 10.45 / **0.13**; 101 |

Dispatcher coverage includes the 65,536-byte key with null value and old size formula **65,556**; only that case uses `4 × TEST_CALL_BYTES`, not a production limit change or peak-allocation evidence. SQL coverage is four rows × three main projections, a separate two-column actual-control HEX check, four3140 diagnostics and eight zero-slot calls; representative, not exhaustive.

Parent pinned-formatted/checked **15 live Rust sources (TiKV9/native6)**; both repository diff checks and both lockfile checks exit0. Parent's exact byte-preservation scope is the complete native JSON/source fixture files, old json2/construct/jcodec cfg(test) blocks, and construct code from JSON_UNQUOTE onward; see summary, without broadening that proof.

Four full-suite failures remain, including the old EXP FloatOverflow assertion. Parent freshly compared the complete failure section against `json-introspection-expr-full.log`, normalizing only panic-heading thread IDs: byte-identical, SHA256 `0930217d98e0b92d727527dc3c7cb7313f1bbe35e643da60114fa6d78203839b`. This is neither a whole-log hash nor a green full suite.

Native QUOTE's checked `6n+2` reserve differs from the old allocation strategy; wire retains `2n+2`. Neither reserve nor the test-only cap proves whole-call allocation peaks, OOM safety, zero-copy or performance. No whole JSON codec/Go package/workspace, release, make lint or broader guard completion claim follows.

Only the two authorized docs were written after source freeze. Historical non-green evidence remains historical; frozen **99/245**, final **0/245**, not PR readiness.
