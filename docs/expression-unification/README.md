# Expression unification experiment

Checkpoint-ID: `compare-six-53` (previous `compare-substrate-52`).
**172/245 functional families; target221; strict final acceptance0.** Added EQ/NE/LT/LE/GT/GE. Incomplete, not PR-ready.
Paired branches: [TiDB](https://github.com/YangKeao/tidb/tree/expression-unification-demo) · [TiKV](https://github.com/YangKeao/tikv/tree/expression-unification-demo). Matching Plan snapshots accompany both commits; no force-push or automatic PR.

## Shared comparison evaluators
13 explicit profiles carrying finite `ComparisonOp` select78 fixed kernels, with unit runtime metadata and separate real-NULL/missing terminals. Native preparation submits actual signed/unsigned, IEEE, Decimal, collated-byte, vector, calendar, duration or raw JSON values. Native IEEE and legacy total float order remain distinct. Raw JSON reuses the preceding native-policy substrate, not wire ordering or canonical text.

Typed, numeric batch/filter, existing PB and30 legacy signatures now use shared bool production. Row predicates compose actual shared results and shared NOT; NaN Eq-then-Lt/NOT behavior is preserved. No new carrier, driver, binding or PB/legacy admission. Old native six-predicate calculations are removed; NullEq and nonexpression sorting utilities remain separate.

**Explicit compatibility change:** row preparation now observes statement truncation/date-mode/timezone/warning context rather than NoColumns defaults and silence. This is not full old-row-context equivalence. Original row collation, precision4, literal descriptors and empty structural identities remain; tests pin context activation. Wider request-root closure remains unfinished.

## Validation
| Gate | Result |
|---|---|
| TiKV comparison kernels / local | 29 / 295 passed; local1 ignored |
| Native profiles / existing comparisons | 24 / 67 passed; existing5 ignored |
| Legacy / SQL / NOT instrumentation | 2 / 2 / 1 passed |
| Final full expression | **1498 passed,4 old failures,94 ignored; exit101** |
| Full unistore | **203 passed,1 old failure,13 ignored; exit101** |

Twelve attempts: eleven actual runs plus one compile failure. Seven final focused gates pass. Two actual test-red runs were repaired: wide test fixtures used a wire parser instead of Grow construction, and an old instrumentation test needed source-derived counts for newly shared IN/BETWEEN comparisons. Three private collation-path compile errors were fixed through existing public exports. Original SQL expected values remain unchanged; no fixtures regenerated. Final complete failure sections equal the previous checkpoint after thread IDs only.21 Rust sources,13 new tests, no dependency/lock changes.

Exact commands and hashes: [summary](logs/compare-six-summary.txt). Review map, compatibility boundary and deferred work: [evidence](evidence/compare-six-checkpoint.md), `checkpoint.json`, and the root Plan.

## Remaining work
73 eligible families remain; target needs49. IntDIV and further comparison/control consumers remain candidates, not completed families. Whole workspace/lint, release/performance, zero-copy, physical heap/peak/OOM/M6, broader request-root and exhaustive compatibility/differential gates are unverified. Existing parser/GB/ignored-vector/extreme Decimal exceptions remain. No complete Go-package/type-domain or FIPS claim.
