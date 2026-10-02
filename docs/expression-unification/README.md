# Expression unification experiment

Paired branch: `expression-unification-demo` in YangKeao/tidb and YangKeao/tikv.
Current checkpoint: **json-merge-pair-61**; previous: **clock-four-60**.

## Progress

Functional delegation plus native algorithm deletion: **198/245**. Target221:23 more needed,47 eligible remain. Strict final-audited acceptance stays **0**; overall goal remains active. New whole families: JSON_MERGE (including PRESERVE) and JSON_MERGE_PATCH.

## What changed

One generic node merge/patch implementation now serves both serde and raw JSON. The serde wrapper retains nullable reset sequencing; raw SDK wrappers retain distinct codec stages, duplicate keys and opaque payloads. Native algorithms are deleted. Three fixed workers consume actual ordered documents, never a host merge answer or reset selection. Existing PB context and legacy child-demand paths are connected without expanding admission.

Native empty PB PATCH still panics; raw empty PATCH still returns None. SQL NULL differs from JSON null. Only raw codec errors fold into legacy None; infrastructure errors propagate. The original JSON_MERGE warning owner remains unchanged.

## Validation and limitations

[Evidence](evidence/json-merge-pair-checkpoint.md), [commands/hashes](logs/json-merge-pair-summary.txt), [manifest](checkpoint.json), [cumulative ledger](migration-progress.json).

Eight exclusive writers;21 Rust files,2 new sources,15 new tests passing in the final state.13 Cargo attempts:11 nonzero runs (7 green,2 known full-suite RED,1 initial SQL RED,1 interrupted),1 compile failure and1 zero-match harness run. None of the interrupted/zero-match/ignored cases is counted as passing.

The new deep raw test exposed impractical work in an unchanged double-pass decoder; only the new test was bounded, leaving deep runtime/performance validation deferred. A PB test's private snapshot calls were removed without widening APIs. SQL numeric3146 cases now allow the original statement diagnostics while forbidding deprecation1681, based on existing source policy. All fixed values and old fixtures remain unchanged. The wrong-package zero-match command is disclosed; the two intended original fixtures already passed within the12-test native expression gate.

Final CPP raw2/core22/local308+1ignored; native SDK1/expression12/legacy1/SQL2 pass. Full expression **1531/4old/94ignored**, unistore **207/1old/13ignored** retain complete prior failure sections after only thread-ID normalization. Prior extra JSON_KEYS aggregate mismatch remains unresolved and was not rerun.

StrictM6, broader default-NoColumns root closure, workspace/lint/dev/bazel_prepare, release/performance/zero-copy/physical heap/peak/OOM, exhaustive context/domain/wire/differential/TiFlash/FIPS and prior parser/GB/vector/extreme Decimal exceptions remain deferred. Tree conversion/key-search costs are unmeasured. No package-transcreation, PR readiness or overall completion claim.

The manifest pins the paired TiKV commit and common Plan hash. Both tracked Plans equal the root Plan; publication is TiKV first, then TiDB with exact paired SHA. No force push or automatic PR; the old untracked client differential `BUILD.bazel` remains excluded. Next read-only candidates are clock3 plus typed GetTimeValue closure, CRC32 with exact number spelling and a still-unverified quoted SQL entry, and all19-kind identity families. No advance credit.
