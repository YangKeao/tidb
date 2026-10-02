# Shared MERGE/PATCH — json-merge-pair-61

Previous: `clock-four-60`. Functional **198/245**, strict final acceptance **0**; target221,23 more needed,47 eligible remain. Whole additions: JSON_MERGE (including JSON_MERGE_PRESERVE) and JSON_MERGE_PATCH. [Receipts](../logs/json-merge-pair-summary.txt).

## Ownership and implementation

Eight exclusive writers,21 Rust files (CPP9/native12),2 new sources,15 additive tests. B owns datatype `json/native_merge.rs`, datatype JSON exports and native raw SDK; H owns expression `native_json_merge.rs`/exports; G fixed workers; C four local metadata/packing files; D native mappings/SDK; E native merge adapters/tests; A four context-forwarding calls and PB test; F legacy PATCH plus legacy/SQL tests. Parent owns integration, Plan, formatting, serialized gates, guides and paired publication. No dependency, manifest, lock, Go or Bazel changes.

**One structural algorithm per policy**, not separate serde/raw copies: datatype generic `merge_native_json_nodes` owns adjacent-object runs, recursive duplicate-key combination and one-level array flattening; `merge_patch_native_json_node` owns recursive patching. Scalar null predicates/constructors are pure representation leaves, not evaluation bindings. Raw duplicate keys retain first-match removal and remove/push order; raw opaque/time/UInt payloads never pass through serde.

The expression bridge converts actual serde trees to/from the shared node representation and owns only nullable sequence/reset policy. All native PATCH inputs are prepared before selecting the last SQL NULL or nonobject reset. SQL NULL differs from JSON null. Existing sorted final formatting makes unique-key insertion order unobservable here; this is not a claim that every TiKV build disables serde `preserve_order`. Native removes three serde helpers/reset logic and four raw helpers. Original native parse/encoding result projection remains.

Raw SDK PRESERVE decodes all documents before merging and encoding once. Raw PATCH decodes first, then decodes/merges each patch, encoding once at the end. Its empty list returns None; native malformed-empty PB PATCH retains the original index panic. Native PB constructors do not impose the wire kernel's minimum arity. Singleton behavior, signature tables and ordinary wire policies remain unchanged.

## Fixed protocol and demands

Three unit Bytes1/OwnBytes profiles: JsonMergeSerdeNative uses the existing actual-document-list packet; JsonMergePatchSerdeNative uses u64 count plus each actual presence0/1 and, for Some, u64 length/serde bytes; JsonMergePatchRawLegacy uses u64 count plus each length/type-byte/raw payload. None presence is actual SQL NULL, not a mode or computed answer. Zero count is legal framing for both PATCH profiles. Raw framing validation does not decode business payloads. Shared validators serve facade and official entry; the existing official JSON guard needed no edit. No new driver, result kind, carrier, binding, cause or NoArgs profile.

Native PRESERVE stops value coercion at the first SQL NULL and executes the existing genuine-null worker, without changing outer eager child evaluation. PATCH prepares all nullable documents, including invalid prefixes that later scalar patches cannot hide. The frontend does not merge or choose a reset suffix. JSON_MERGE's original caller emits warning1681 only after a successful nonNULL computed result.

Legacy retains first-absent-child suppression via the existing genuine-absence worker. If all children are present, all are demanded before raw codec work. Only the raw SDK's codec error enum folds into a worker-computed None; infrastructure errors propagate. Its separate raw-columns root proves dispatch rather than borrowing a shared-child receipt.

## Gates and disclosed corrections

13 Cargo attempts: **11 nonzero test runs** (7 green,2 old full RED,1 new SQL RED,1 interrupted),1 compile failure and1 zero-match harness run. Final15 new tests pass; zero-match, ignored and interrupted tests are not passing evidence.

- CPP raw retry2, pure/fixed JSON22, local308+1ignored; native raw SDK1, expression filter12 (including the two original column/expression fixture tests), legacy1 and corrected SQL2 pass.
- Full expression **1531/4old/94ignored** and unistore **207/1old/13ignored**, exit101. Entire prior failure sections/lists match `clock-four-60` after only panic-thread-ID normalization. Production did not change after first gates; the last SQL edit was test-only.
- New SQL proves12 fixed stored-column results,6 diagnostic cases and13 direct zero-slot statements. Legacy adds30 plain-root zero-slot channel checks,7 separate shared-child-demand cases and raw identity/value pins. PB verifies actual values, root refusals, eager child/coercion errors and another successful scope; private factory/idle accounting is verified in SDK/local tests, not claimed observable from PB tests.

**Interrupted raw test:** the initial depth100 raw-decoding case did not finish and parent terminated it (SIGTERM); another test had passed but the run has no final result. Unchanged decoder `native_policy.rs:803-805` validates each child at depth0 before the caller decodes it again at outer depth (723/752), producing exponential work for a deep chain. Only the new test was bounded: pure generic-node merge→encoder still tests TooDeep, while raw malformed-input checks use shallow data. Deep raw error precedence is source-supported, not runtime-verified. Production decoder/performance was not repaired.

**Compile RED:** new PB test incorrectly called private `snapshot()` five times (E0624). Those five assertions were removed, preserving actual PB scope evaluations and existing visibility. SDK/local tests cover accounting. No production API was broadened.

**SQL RED1/1:** the positive test reached numeric3146, passed its code/evaluation-origin assertions, then wrongly required empty diagnostics. Existing `tidb-session/src/lib.rs:2257-2287` records3146 as a statement Error diagnostic, unlike3140. Only the two new numeric warning assertions now prohibit deprecation1681 rather than all existing diagnostics; the four3140 cases still require none. All fixed values and old tests stay unchanged. Retry2/0 passes.

**Zero match:** parent selected `tidb-session --test all json_merge_patch_integration_source`, yielding0/336filtered. These fixtures actually belong to `tidb-expr` and had already passed within the12-test native gate (and full suite); no zero-match pass is claimed. A source-proof script also initially sliced the coercion function through EOF, including the following adapter; correcting its function-end selector proved the original body exact. No production assertion mismatch or source repair resulted.

## Limits and next work

21 pinned formatter checks and both diff checks pass. Original JSON test prefix, old CPP worker/wire suffix, warning owner, admission/decoder/codec/locks remain exact. Architecture/ownership guides updated without new policy. Prior JSON_KEYS aggregate type mismatch remains unresolved and was not rerun.

StrictM6, broader default-NoColumns request-root integration, workspace/lint/dev/bazel_prepare, release, performance/zero-copy/physical heap/peak/OOM, exhaustive context/domain/wire/differential/TiFlash/FIPS and prior parser/GB/vector/extreme Decimal exceptions remain deferred. Tree conversion adds allocations and generic key search differs from the old serde BTreeMap; no performance neutrality claim. No package-transcreation, PR-readiness or overall-completion claim.

Read-only next: NOW/CURDATE/SYSDATE need both SQL clock profiles and distinct typed `get_time_value` helper closure, including timezone/getter/constructor order. JSON_SUM_CRC32 can reuse existing IEEE CRC but must retain number spelling; quoted-function SQL is only a source candidate awaiting a runtime probe, and ARRAY syntax stays unadmitted. Identity pairs still require all19 Datum representations. No candidate earns advance credit.
