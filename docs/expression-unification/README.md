# Expression unification experiment

Paired branch: `expression-unification-demo` in YangKeao/tidb and YangKeao/tikv.
Current checkpoint: **decimal-parse-107**, after **in-control-106**.

Functional **238/245 (97.14%)**, strict **0**, remaining **7**—unchanged. This is a type-layer prerequisite, not whole CAST/M2 completion. Overall goal remains active.

## Shared Decimal parsing and shift

SDK `mysql/native_decimal_parse.rs` owns digit-string literal/integer construction, normalization, MySQL parsing/error policy and bounded shift. Native `Decimal` retains its private SmallVec24 storage, metadata and API. Owned parts move the actual coefficient; unchanged/overflow shifts preserve raw state and declared shape.

The existing rounder and mysql-internal coefficient extractor are reused. Fixed-word MyDecimal and wire parsers retain their distinct policies. No new profile, dependency, carrier or admission gate is added.

## Evidence

[Checkpoint](evidence/decimal-parse-checkpoint.md), [exact commands/counts/hashes](logs/decimal-parse-summary.txt), [manifest](checkpoint.json), [ledger](migration-progress.json).

Five nonzero green receipts: SDK2, full native datatype463, focused CAST7, new SQL1 and original SQL1. One preexisting vectorized string-to-DECIMAL UNION test remains ignored, not passed. Four new tests;98 SDK and198 native old test bodies unchanged.

The new SQL test runs10 SELECTs across scalar/vector modes, including source-derived diagnostics, exponents, storage/arithmetic scale and the existing LF/TAB double-parse distinction. Native CAST orchestration and original test fixtures are unchanged.

## Still open

[Remaining acceptance](evidence/remaining-acceptance.md): CAST source/diagnostic control, broader M2, six complex candidates, general request-root/default-NoColumns/liveDAG and final acceptance.

Full expression/unistore were not rerun; historical4+1 failures remain. Workspace/lint/dev/bazel/release/exhaustive/TiFlash/FIPS/performance/physical memory/OOM/allocator/dual-tzdata/whole-Go-package/PR readiness are unverified. The manifest pins paired TiKV and three identical Plans. No force push or PR; unrelated BUILD excluded.
