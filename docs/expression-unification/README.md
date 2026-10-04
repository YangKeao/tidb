# Expression unification experiment

Paired branch: `expression-unification-demo` in YangKeao/tidb and YangKeao/tikv.
Current checkpoint: **str-to-date-runtime-95**, after **str-to-date-types-94**.

Functional coverage **230/245 (93.88%)**, strict final count **0**. All229 previous family objects are unchanged; only `str_to_date` is added. Overall goal remains active.

## STR_TO_DATE runtime now shared

Three real SDK workers own ordinary parsing, validation/rendering/warning selection and typed DATETIME's late zero-date-prefix decision. Native `calendar.rs` keeps thin entry adapters; its parser/sentinel/private helpers and scalar prefix logic are deleted. The R97 public datatype grammar and original wire grammar remain distinct.

SDK continuation reports trigger the original delayed mode reads. Native retains warning delivery and generic CAST finishing, including Duration(NULL)'s timezone demand. Actual Unknown(12) metadata stays distinct from Datetime; wide day999 is not prematurely packed. No PB/legacy admission is added.

A narrow BytesIntInt carrier keeps two actual mode flags separate. Generic arity/factory limits are unchanged; checked input-length-plus64 precharges retained replies, not physical parser allocation peaks.

## Evidence

[Checkpoint](evidence/str-to-date-runtime-checkpoint.md), [commands/counts/hashes](logs/str-to-date-runtime-summary.txt), [manifest](checkpoint.json), [ledger](migration-progress.json).

Six new tests finally pass. One new test initially put the zero-date prefix in the Head report; corrected from the frozen source contract, which adds it only after the late mode getter. Failure retained; no production or original test change.

New SQL34SELECTs pass, separating14new nonNULLHead refusals from2old NULL-witness refusals. Original SQL classifier4SELECTs also pass. Full expression **1606/4old/94ignored**, unistore **220/1old/13ignored**: whole failure sections match R96 after thread-ID normalization. The known month0 mismatch is deliberately not repaired.

Scope7CPP/7native Rust files,2new modules;134CPP/411native original test bodies unchanged. Pinned formatting, receipt checks and independent review pass. No Cargo/lock/Go/Bazel edits.

## Still open

[Remaining acceptance](evidence/remaining-acceptance.md):5core,4ordinary,6complex candidates—not blanket exceptions—plus request-root/default-NoColumns/liveDAG/final acceptance.

Workspace/lint/dev/bazel_prepare/release/exhaustive/TiFlash/FIPS/performance/physical memory/OOM/allocator/dual-tzdata/complete Go-package/PR-readiness remain unverified. Three identical Plans and paired TiKV commit are pinned in the manifest. No force push or PR; unrelated untracked BUILD excluded.
