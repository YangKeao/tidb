# Expression unification experiment

Paired branch: `expression-unification-demo` in YangKeao/tidb and YangKeao/tikv.
Current checkpoint: **json-paths-57**. Previous: **json-values-56**.

## Progress

Frozen denominator245; target221. Functional delegation plus native algorithm deletion: **189/245**. Strict final-audited acceptance: **0**; goal remains active. Five complete families added: EXTRACT, INSERT, SET, REMOVE, ARRAY_INSERT.56 eligible remain;32 more functional migrations needed.

Native REPLACE/ARRAY_APPEND also use workers, but their legacy evaluators remain open: **neither gets family credit**. Shared raw SDK algorithms alone do not close those evaluator routes. UNQUOTE remains deferred.

## Boundaries

- Seven fixed unit byte-result identities carry actual documents, parsed selector ASTs and ordered values through existing Values/Bytes2/Bytes3/OwnBytes. Original cached multiple-selection metadata is retained, with no reconstructed path text or action program. Existing genuine-NULL terminal is reused; no new result kind/driver/carrier/binding/admission.
- Shared serde mutation cores apply the original ordered operations and serialize only the final value. Frontend coercion orders remain all-paths-before-values versus per-pair-path-before-value; no-op targets still demand their values.
- One guarded cached facade retains document-before-context/cache demand, observed NULL, same-context hits, failed-parse retry and clone reset. Existing PB signatures, five-argument lowering, cast flags and eager children stay unchanged.
- Separate raw datatype cores preserve cross-path identity dedup, ranges/flags/duplicate policy and original SDK codec stages. ARRAY_INSERT still skips replacement decoding on early no-op and returns original bytes. Callback walk/search share selectors without earning family credit. No new raw encoder.

## Evidence

[Review map and contracts](evidence/json-paths-checkpoint.md), [commands/hashes/incidents](logs/json-paths-summary.txt), [manifest](checkpoint.json), [sole cumulative ledger](migration-progress.json). Parent manages eight exclusive contributors and serialized gates;23 Rust files,2 new shared modules,15 additive tests, unchanged manifests/locks.

13 Cargo attempts:11 actual runs and2 new-test compile failures corrected without expectation changes. Nine final focused gates pass: native SDK22/parser5, CPP JSON10/local303+1ignored, native result SDK2/cache2/PB1, legacy1, SQL2 with30 direct zero-slot cases. Full expression **1516/4old/94ignored** and unistore **205/1old/13ignored** remain non-green; entire failure sections match their prior receipts after only thread-ID normalization. All23 scoped formatter checks pass. Two parent nonexistent source/guide lookups were corrected by discovery.

StrictM6, whole workspace/lint/dev/bazel_prepare, release/performance/zero-copy/physical heap/peak/OOM, exhaustive context/domain/wire/differential/TiFlash/FIPS and prior parser/GB/vector/extreme Decimal exceptions remain deferred. Actual context resource activation and previous row-context/float-feature surfaces are explicit. No whole Go-package transcreation, PR readiness, force push or overall completion claim.

`checkpoint.json` pins the exact paired TiKV commit and common Plan SHA256. Both tracked Plans must equal the root Plan; publication remains TiKV first, then TiDB with the paired SHA and Plan. History/evidence from earlier checkpoints is retained. The pre-existing untracked client differential `BUILD.bazel` is excluded.
