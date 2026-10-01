# JSON_VALID / JSON_TYPE / JSON_DEPTH checkpoint

Checkpoint **json-introspection-three-30**: functional **93→96/245**, final acceptance **0/245**. Only `json_valid`, `json_type` and `json_depth` earn credit; six private signatures and shared helpers are not extra families. Commands and twelve whole-log hashes: [`../logs/json-introspection-summary.txt`](../logs/json-introspection-summary.txt).

## Ownership and review completion

TiKV owns the shared native-document parser, type-name selector and depth kernel. Native text uses the original serde Value policy; wire/native type and depth views share primitives without replacing their representation-specific policies, encoding large keys into a u16-key binary bridge or constructing dummy arrays.

**The first four passing runs were not final coverage.** Review then found recursion still in native `binary_json_ops::element_depth`. The supplement preserves its original `to_node` validation, supplies only actual child views, and delegates max/+1 computation to the single TiKV depth kernel through narrow `native_json_depth_from_children`, not a generic evaluator. One new helper test and post-supplement gates complete this boundary; the initial edits were not all complete on the first wave.

Parent reports all **24 Rust sources (12 native/12 TiKV)** pinned-formatted/checked, both repository diff checks and both lockfile checks exit0. Complete old JSON fixture files/selected cfg(test) blocks and report code from JSON_LENGTH onward remain byte-identical (scope listed in summary). No old expected value or oracle was modified; the new empty-literal expectation was checked against pinned pre-move source rather than inferred from the new provider.

## Preserved policy and explicit ordering change

- Native text keeps `i64::MAX` as INTEGER, duplicate-key last-wins, serde's default 128-level guard and keys longer than u16. No new stringify/binary-codec round trip substitutes for these rules.
- Typed TYPE preserves numeric/date/time/opaque tags and only its original local checks: empty or malformed non-null literal payloads yield BOOLEAN; wire literal policy differs. Native opaque framing is stricter than wire TYPE, without blanket validation of other typed payloads.
- VALID's typed-JSON signature returns1 without payload validation; text bad UTF-8/parse errors return0; Others is an actual NoArgs worker returning0, not a fabricated 0/1 input. SQL NULL also reaches a real nullable wrapper.
- Source-type, applicable strict-UTF-8 and numeric-to-JSON/Display preparation remain before admission. **Actual parsing and typed-TYPE validation now run after admission: zero-slot resource refusal precedes bad-JSON domain errors.** This is an explicit priority change, not a claim that old preparation ordering is wholly unchanged.
- The five-state typed `JsonReport` consumes the computed result: Type payload is copied from transport then moved, depth is inline. Malformed envelopes are contract errors; envelope allocation `Other` and genuine helper/resource errors are not fabricated SQL JSON statuses. No new argument role, cause, driver, general context getter, fourth internal column or PB/legacy route is added.

## Twelve retained Rust receipts

Writer globbed, grepped/read and SHA256-hashed all twelve completed logs; execution/exits and source proofs are parent-owned. Four initial green runs plus seven final scoped green runs and one known non-green full run: no compile failure or failed-target retry. Final revalidation followed the review supplement, not an all-first-wave success claim. Compilation times are emitted `Finished` times, not performance measurements.

| Phase / `json-introspection-` suffix | Passed / ignored / failed; filtered | Test / compile seconds; exit |
| --- | --- | --- |
| Initial `tikv-datatype.log` | 34 / 0 / 0; 366 | 0.00 / 3.35; 0 |
| Initial `native-datatype.log` | 31 / 0 / 0; 406 | 0.00 / 3.55; 0 |
| Initial `tikv-local.log` | 252 / 1 old / 0; 481 | 0.19 / 11.07; 0 |
| Initial `tikv-kernels.log` | 19 / 0 / 0; 715 | 0.00 / 0.13; 0 |
| Final `tikv-datatype-final.log` | 34 / 0 / 0; 366 | 0.00 / 1.65; 0 |
| Final `tikv-json-final.log` | 32 / 0 / 0; 702 | 0.01 / 3.74; 0 |
| Final `native-datatype-final.log` | 32 / 0 / 0; 406 | 0.00 / 1.95; 0 |
| Final `native-json.log` | 40 / 0 / 0; 1501 | 0.31 / 15.68; 0 |
| Final `native-source.log` | 30 / 0 / 0; 1511 | 0.26 / 0.14; 0 |
| Final `dispatch.log` | 3 / 0 / 0; 1538 | 0.00 / 0.12; 0 |
| Final `sql.log` | 63 / 0 / 0; 2078 | 1.00 / 31.91; 0 |
| Final `expr-full.log` | 1443 / 94 / **4**; 0 (1541 discovered) | 10.54 / **0.14**; 101 |

Final joint JSON32 includes both C JSON-core tests, all19 kernel tests and related casts/miscellaneous coverage; it is **not a final full-local rerun**. Dispatcher3 and SQL63 cover values/tags, metadata, preparation-versus-parse error ordering and zero-slot refusal, not exhaustive JSON-domain verification. The native datatype32 run includes `test_binary_json_depth_shared_typed_helper`.

All four old full-suite failures remain, including the still-failing EXP FloatOverflow assertion. Parent freshly compared the complete failure section with round29 `compression-expr-full.log`, normalizing only panic-heading thread IDs: byte-identical, SHA256 `0930217d98e0b92d727527dc3c7cb7313f1bbe35e643da60114fa6d78203839b`. Full names and normalization are in the summary; this is not a whole-log hash or a green suite.

Only these two authorized docs were written after source freeze. No whole JSON codec/Go-package/workspace, exhaustive input, internal allocation-peak/OOM, zero-copy/performance, release, make lint or wider-guard claim follows. Earlier non-green evidence stays historical; frozen **96/245**, final **0/245**, not PR readiness.
