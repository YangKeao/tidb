# Expression unification experiment

Paired branch: `expression-unification-demo` in YangKeao/tidb and YangKeao/tikv.
Current checkpoint: **str-to-date-types-94**, after **convert-charset-93**.

Functional coverage remains **229/245 (93.47%)**, strict final count **0**. All229 family objects are unchanged; this step adds no family/C4 credit. Overall goal remains active.

## Public STR_TO_DATE datatype policy shared

Native `str_to_date.rs` now contains public aliases and the original parse→Time construction→validation adapter. Parser, format classification and Go punctuation policy live in SDK `native_str_to_date.rs`.

Raw packing, trailing-input flag, hidden fractional microseconds under FSP0, original validation order and classifier early stop are preserved. The exact Unicode dependency/version moves with the existing Go-version exclusions; lock changes are limited to dependency ownership.

The broader public datatype grammar is **not** substituted for ordinary expression or wire parsing. Ordinary STR_TO_DATE worker migration remains next, with delayed SQL-mode/typed-result/getter contracts recorded in Plan.

## Evidence

[Checkpoint](evidence/str-to-date-types-checkpoint.md), [commands/counts/hashes](logs/str-to-date-types-summary.txt), [manifest](checkpoint.json), [ledger](migration-progress.json).

Four locked test commands: SDK1, native datatype8 and SQL2 pass; ordinary expression subset gives **5passed/1known old failure**. Both new tests pass on first execution. The old partial-format mismatch is unchanged against R96's diagnostic; no repair or oracle change. Full expression/unistore were not rerun this step.

Two existing SQL tests cover5SELECTs, constant/dynamic return metadata and DDL defaults—not new SQL probes or runtime takeover proof. Scope:2CPP/1native Rust files,1new module;52CPP/7native original tests unchanged. Pinned formatting, lock ownership checks and independent current-contract review pass.

## Still open

[Remaining acceptance](evidence/remaining-acceptance.md):5core,5ordinary,6complex candidates—not blanket exceptions—plus request-root/default-NoColumns/liveDAG/final acceptance.

Known gaps and workspace/lint/dev/bazel_prepare/release/exhaustive/TiFlash/FIPS/performance/physical memory/OOM/allocator/dual-tzdata/complete Go-package/PR-readiness remain unverified. Three identical Plans and paired TiKV commit are pinned in the manifest. No force push or PR; unrelated untracked BUILD excluded.
