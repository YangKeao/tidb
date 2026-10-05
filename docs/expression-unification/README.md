# Expression unification experiment

Paired YangKeao/tidb and YangKeao/tikv branch: `expression-unification-demo`.
Current checkpoint: **numeric-completion-134**, following **numeric-route-133**.

Functional **238/245 (97.14%)**, strict **0**, remaining **7**—unchanged. No whole CAST/M2 or Go-package credit.

SDK numeric target metadata, skip-fitting decisions, String Real selection and generic/context-Decimal result policies replace native decisions. Existing shape/construction primitives are reused. Native projects actual storage and invokes the existing contextful datatype engine, preserving default metadata and effect/error order. The engine and expression renderer remain explicit dependencies.

## Verification

[Evidence](evidence/numeric-completion-checkpoint.md), [commands/counts/hashes](logs/numeric-completion-summary.txt), [manifest](checkpoint.json), [ledger](migration-progress.json).

Six matched gates GREEN without failure/retry: SDK11, native consumers5/old Real1/division1 and existing SQL2. Three new tests;9 SDK/36 native old touched-file test bodies unchanged. Renderer byte-identical; datatype engine/SQL files unchanged. No new Rust files or SQL fixture/probe credit.

## Remaining

[Remaining acceptance](evidence/remaining-acceptance.md): datatype engine, expression rendering, other typed/write selectors and SQL lowering; broader M2, six complex candidates, request-root/default-NoColumns/liveDAG and final acceptance. Full suites/lint/dev/bazel/release/exhaustive/performance/physical memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/whole-Go-package/PR readiness remain unverified. Historical R100 expression4/unistore1 failures remain unrepaired; prior RED receipts retained. Goal remains active.
