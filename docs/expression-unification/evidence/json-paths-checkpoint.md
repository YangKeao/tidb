# Five complete JSON path families — json-paths-57

Previous `json-values-56`. **189/245 functional families**, strict final acceptance0, target221;32 more required. Adds EXTRACT, INSERT, SET, REMOVE and ARRAY_INSERT. Native REPLACE/ARRAY_APPEND also use workers, but their legacy evaluators do not: **neither earns family credit**. [Exact commands, failures and hashes](../logs/json-paths-summary.txt).

## Review map

Eight exclusive owners, parent integration and serialized gates.23 Rust sources (CPP10/native13), two new shared modules,15 additive tests; no manifest/dependency/lock changes.

| Owner | Scope |
|---|---|
| B | New CPP datatype `json/native_path_ops.rs`/exports; native `json_path.rs`/`binary_json_ops.rs`: raw type aliases, unique traversals/mutations, staged SDK wrappers,2 tests |
| H | New CPP expression `native_json_modify.rs`/`lib.rs`: exact serde mutation closure, fixed exports,2 tests |
| G | CPP `impl_json.rs`:7 unit byte-result kernels,5 validators,2 tests |
| C | CPP local/type files: checked parsed-selector transport, closed profiles,2 tests |
| D | Native tikv adapter/test/mod:7 existing-Bytes mappings,2 narrow bridges,2 tests |
| E | Native JSON `modify.rs`/`path.rs`: original preparation and complete native algorithm deletion |
| A | JSON dispatch/test, builtin exports, scalar evaluator: guarded cache/context closure,2 tests |
| F | SQL lifecycle and `scalar_function/pb_builtin.rs` tests:2 SQL tests plus1 existing-signature PB test |

## Input data and actual result ownership

Seven fixed unit identities: `Json{Extract,Insert,Set,Replace,Remove,ArrayAppend,ArrayInsert}SerdeNative`. Extract/Remove use Bytes2 (actual document and parsed paths); five mutation identities use Bytes3 with the existing ordered-value frame. Existing `JsonOutputNullNative` receives genuine NullWitness(None). No new driver, result kind, carrier, binding, terminal, NoArgs rule or ordinary admission.

The path packet carries **actual cached selector data**, not reconstructed path text or an action program: u64LE path count; each path's original boolean multiple-selection flag and u64LE leg count; tags0 Key(length/UTF8),1 KeyWildcard,2 ArrayAll,3 ArrayIndex(i64LE),4 ArrayRange(two i64LE),5 Recursive. The original flag is retained independently, not recomputed. Fixed operation identity selects the action; no packet field supplies it. Zero-count lists are real low-level input lists, not new SQL arities.

Checked packing validates sizes and reserves space. Both admission layers and kernels share bounded decoding and fixed-role validation. Update path/value counts must agree; the frontend sends precisely the effective original zipped prefix. It never mutates the document or computes existence/index decisions. Workers apply the complete ordered sequence, then serialize **only the final result** with the original formatter. Existing `into_json_datum` retains the original BinaryJSON parse boundary. Missing EXTRACT output is a real worker NULL, not a fabricated witness.

## Preparation and cache contracts

All child expressions still evaluate eagerly before preparation. Distinct original orders remain:

- EXTRACT: strict document-kind check, document parse/NULL, then ordered path coercion/parsing.
- SET/INSERT/REPLACE: dynamic arity, document, **all paths before any values**; values still coerce for no-op targets.
- APPEND/ARRAY_INSERT: arity, document, then **each path's validation before its value**, before the next path. ARRAY_INSERT checks multiple-selection3149 before cell3165.
- REMOVE: document, ordered path coercion; root3153 precedes multiple-selection3149. Sequential index shifts remain.

One guarded cached facade serves scalar and existing PB REPLACE: document first; document NULL/error avoids context-id access/cache warming; then exactly one context-id lookup and original cache access; observed cached NULL suppresses value coercion but enters the NULL worker. Cache hits never recoerce/reparse original paths, changed path values in the same context stay ignored, errors are not cached, and expression clone clears cache. `_with_paths` retains count checks before document parsing; `_with_document` retains its original shortest-zip behavior rather than adding a new count error. No nested worker invocation.

The seven original serde mutation helpers moved intact before formatting; native copies and apply loops were removed. Missing parents, scalar autowrap, no-autowrap REMOVE, root operations, negative insertion clamp, NULL-as-value and original typed Boolean/binary/ETJson coercions remain distinct.

## Raw SDK remains a separate policy

Native raw path-leg/array-selection/mode names alias shared datatype types. Native path-expression parser, cache, flags and Display remain unchanged. The raw algorithms retain global cross-path pointer-identity dedup, negative-range clamp, literal-star/flag quirks and first-duplicate behavior, rather than substituting serde or wire algorithms.

Both extraction and concrete-path selection are unique shared implementations. Existing extract_matches/walk/search consumers reuse the latter; this is foundation reuse, not SEARCH/WALK family credit. Concrete paths are rebuilt through the original path-expression push methods as representation conversion.

SDK modify retains length/document/per-pair path-validation/replacement-decode order. ARRAY_INSERT deliberately retains stages: validate/pop path → extract and encode selected parent → decode/shape/index probe → only then decode demanded replacement → shared insertion → encode array → modify/encode document. Missing/nonarray/too-negative no-ops return the original raw bytes without decoding the replacement. Removing these codec stages would change normalization, errors and demand. No new raw encoder is introduced.

## Existing PB and uncredited legacy routes

Only REPLACE/ARRAY_APPEND already have native PB/legacy admission among these seven. Native PB dispatch now uses the guarded workers, including cached NULL paths and eager child errors. Existing five-argument lowering and cast flags remain unchanged; no other signature is admitted. `pb_builtin.rs` production prefix is byte-identical; dispatch integration is in JSON `mod.rs`.

Legacy cophandler remains byte-identical. Its REPLACE prepares raw pairs in a different order; APPEND mutates per demanded pair, returns NULL for nonarray targets and may suppress later children. Shared SDK algorithms beneath it are not a substitute for actual legacy evaluator ownership. Both families remain excluded from the cumulative ledger until that closure is migrated. UNQUOTE also remains open.

## Evidence and limits

13 Cargo attempts:11 actual runs,2 new-test compile failures, no new test RED. Nine final focused gates pass: native raw SDK22/parser5, CPP JSON10/local303+1ignored, native result SDK2/cache2/PB1, existing legacy1, SQL2. SQL has30 direct zero-slot cases without unrelated worker masking. The two compile corrections were only new PB-test imports and a bounded column-index usize→i64 conversion; old expectations and production behavior were untouched.

Full expression **1516/4old/94ignored**, full unistore **205/1old/13ignored**, both exit101. Complete failure sections match the preceding relevant receipts after only panic thread IDs are normalized. All23 pinned formatter checks and both diff checks pass; prior184 family objects remain exact. Two parent nonexistent-source/guide-path lookups were corrected by discovery, not treated as test failures.

56 eligible families remain. StrictM6, whole workspace/lint/dev/bazel_prepare, release/performance/zero-copy/physical heap/peak/OOM, exhaustive domain/context/wire/differential/TiFlash/FIPS and prior parser/GB/vector/extreme Decimal exceptions remain deferred. Context resource activation is intentional and tested; previous row-context/float-feature surfaces remain disclosed. No whole Go-package transcreation, PR readiness or overall-completion claim.
