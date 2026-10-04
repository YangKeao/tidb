# Expression unification experiment

Paired YangKeao/tidb and YangKeao/tikv branch: `expression-unification-demo`.
Current checkpoint: **coalesce-85**, following **if-84**.

Functional coverage: **224/245 (91.43%)**; strict final-audited count: **0**. The223 previous family objects are byte-identical; only COALESCE is added. **The overall goal remains active.**

## COALESCE reuses shared nullable selection

- Each actual candidate uses the unchanged IFNULL head. Only computed Done/NeedSecond results drive an iterative cursor; real exhaustion invokes the new NoArgs `CoalesceEndNative`, which delegates the existing empty wire COALESCE. No second nullable kernel, fake NULL operand or native NULL answer.
- AST/typed/eager selectors and pure null-proof selection delegate. Three wire COALESCE loops reuse the same chooser. Eager inputs are borrowed, not cloned across a prefix/dead suffix; results come from computed frames.
- The first scoped pack retains selected columns across later heads and End without recursive continuation growth. Original first-preparation/NoColumns order and existing IFNULL behavior remain.
- COALESCE-specific typed return-FSP binding stays an explicit native adapter. The Time setter is already shared; raw Duration metadata and later generic conversion remain unchanged. No PB/legacy COALESCE or SQL zero-arity admission is added.

## Validation and corrections

[Evidence](evidence/coalesce-checkpoint.md), [exact commands/hashes](logs/coalesce-summary.txt), [manifest](checkpoint.json), [ledger](migration-progress.json).

All8new tests ultimately pass. SQL54SELECT probes:48three-column root queries (24zero-slot refusals),4lazy-error checks and2filters. Final CPP core1/wire1/local336+1ignored, native root7, existing IFNULL regression1, gateway196+1ignored, legacy refusal1 and SQL1 pass. The legacy test proves an unsupported boundary, not COALESCE execution.

Two new-test expectations were corrected from existing source, with no production policy changes: NULL output still requires accounted row metadata; non-Date Time FSP above6 clamps6 rather than errors. All initial RED logs remain. Thirteen locked launches total:8green,3containing those new-test failures,2only-old full RED—not first-attempt all-green.

Final expression **1591/4old/94ignored** and unistore **217/1old/13ignored** remain RED with byte-identical normalized failure sections. No compile failure, zero-match, interrupted test or provider fixture recording. Original162CPP/516native test bodies remain byte-identical.

Pinned formatting and diff checks cover10native/7TiKV Rust files; one new module, eight new tests. No Cargo/lock, Go/Bazel or generated changes.

## Remaining acceptance

[Remaining review](evidence/remaining-acceptance.md): **7core**, **8ordinary pending**, **6complex exception candidates**—not21approved exceptions. Next CASE/NULLIF, CAST/M2, IN/extrema/INTERVAL, actual request-owner lifecycle and final cross-entry evidence. Preserve CASE base-evaluation differences and NULLIF's existing eager demand; do not replace the accepted scoped design with a universal compiler.

Known baseline failures and documented INTDIV/CAST/mode/JSON/vector/older Values gaps remain. Workspace/lint/dev/bazel_prepare/release, exhaustive differential/TiFlash/FIPS, performance/physical heap/stack peak/OOM/allocator/zero-copy/dual-timezone footprint and whole Go-package transcreation are not verified. No PR-readiness claim.

Three Plans agree; the manifest pins their hash and paired TiKV commit. Publish TiKV then TiDB without force push or PR. The unrelated untracked client-differential BUILD.bazel remains excluded.
