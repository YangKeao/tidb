# Expression unification experiment

Paired branch: `expression-unification-demo` in YangKeao/tidb and YangKeao/tikv.
Current checkpoint: **json-raw-values-58**. Previous: **json-paths-57**.

## Progress

Frozen denominator245; target221. Functional delegation plus native algorithm deletion: **191/245**. Strict final-audited acceptance: **0**; goal remains active. REPLACE and ARRAY_APPEND now earn their two whole-family credits: this checkpoint completes the legacy evaluator paths left open after native AST/SQL/PB migration.54 eligible remain;30 more functional migrations needed.

UNQUOTE remains deferred: strict SQL text, direct BinaryJSON content, SDK conditional second-unescape and raw Display are distinct policies, with no new native PB/legacy admission.

## Boundaries

- The original raw encoder is shared in TiKV datatype `native_codec.rs`; native copies are deleted. Depth, child/error order, scalar bytes, literal inlining, sorting/duplicates and size checks remain. Distinct serde codec conversion loops retain their original order; no wire-builder substitution.
- `native_json_legacy.rs` owns full-pair REPLACE versus per-pair APPEND evaluation, preserving every raw codec stage and future-child demand. APPEND extraction errors/missing targets return original bytes, selected nonarrays return NULL, and zero-pair APPEND identity differs from zero-pair REPLACE re-encoding.
- Four fixed unit byte profiles carry actual raw documents, parsed raw ASTs/original flags and values. A separate NoArgs terminal represents genuine observed legacy no-value, not fake SQL NULL. No new driver/kind/carrier/binding/admission.
- Three narrow SDKs retain existing by-value legacy preparation and error classes. Checked transport/evaluation is guarded; this does not introduce a whole-call/root driver. Root raw_columns and child-only shared_override remain separate. Computed raw output is transferred without parse/render/validation.

## Evidence

[Review map and contracts](evidence/json-raw-values-checkpoint.md), [exact commands/hashes](logs/json-raw-values-summary.txt), [manifest](checkpoint.json), [sole cumulative ledger](migration-progress.json). Seven exclusive contributors;18 Rust files,3 new files,11 additive tests; unchanged manifests/locks.

12 actual Cargo runs:10 focused green,2 known-baseline full failures. No compile failures, new test REDs, retries or zero-match runs. Core gates: native binary JSON35; CPP codec2/JSON14/local305+1ignored; native SDK2; new legacy scope1/old legacy1; existing cache2/PB1/SQL2 with30 direct zero-slot cases. Full expression **1518/4old/94ignored** and unistore **206/1old/13ignored** remain non-green; complete failure sections match the previous checkpoint after only thread-ID normalization. All18 scoped formatter checks pass; old oracles are untouched.

StrictM6, broader default-NoColumns request-root integration, workspace/lint/dev/bazel_prepare, release/performance/zero-copy/physical heap/peak/OOM, exhaustive context/domain/wire/differential/TiFlash/FIPS and prior parser/GB/vector/extreme Decimal exceptions remain deferred. Legacy resource activation includes early no-value/empty/identity routes. Performance of scalar projection/transport allocations is unmeasured. No whole Go-package transcreation, PR readiness or overall completion claim.

`checkpoint.json` pins the paired TiKV commit and common Plan SHA256. Both tracked Plans equal the root Plan; publication remains TiKV first, then TiDB with exact paired SHA and Plan. History/evidence is retained. No force push or automatic PR. The pre-existing untracked client differential `BUILD.bazel` remains excluded.
