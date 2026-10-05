# Expression unification experiment

Paired branch: `expression-unification-demo` in YangKeao/tidb and YangKeao/tikv.
Current checkpoint: **legacy-date-arithmetic-103**, after **date-arithmetic-102**.

Functional **237/245 (96.73%)**, strict **0**, remaining **8**. This round appends DATE_ADD/SUB; all235 prior family objects remain unchanged. Overall goal is active.

## DATE_ADD/SUB implemented-domain closure

Five SDK profiles now own legacy getter order, reformatting, arithmetic and explicit presence through `tikv/legacy_date_arithmetic.rs`. The48 implemented signatures use thin `cophandler.rs` adapters; raw CoreTime prerequisites are shared with TiKV. Together with R105's ordinary takeover, this earns two functional families.

Eight baseline-unimplemented Duration→Datetime signatures remain child-free refusals. No new PB/legacy admission. Generic MyDecimal parse/round/render remains explicit native preparation—not a whole-type sharing claim.

## Evidence

[Checkpoint](evidence/legacy-date-arithmetic-checkpoint.md), [commands/counts/hashes](logs/legacy-date-arithmetic-summary.txt), [manifest](checkpoint.json), [ledger](migration-progress.json).

Twelve locked serial launches: eleven nonzero passing runs and one retained compile failure, corrected before retry. Five new tests;205 TiKV/327 native old test bodies unchanged. The new consumer verifies all56 real mappings and the48 implemented routes, rather than decode-only coverage. Three unchanged legacy tests and three unchanged SQL tests pass; zero new SQL probes this round.

## Still open

[Remaining acceptance](evidence/remaining-acceptance.md): CAST/M2, IN, six complex candidates—not blanket exceptions—and broader request-root/default-NoColumns/liveDAG/final acceptance.

Full expression/unistore were not rerun; historical4+1 failures remain. Workspace/lint/dev/bazel/release/exhaustive/TiFlash/FIPS/performance/physical memory/OOM/allocator/dual-tzdata/whole-Go-package/PR readiness remain unverified. The manifest pins paired TiKV and three identical Plans. No force push or PR; unrelated BUILD excluded.
