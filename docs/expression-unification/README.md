# Expression unification experiment

Paired branch: `expression-unification-demo`. Current checkpoint: **legacy-cast-json-174**, after **legacy-cast-decimal-173**.

Functional **239/245 (97.55%)**, strict **0**, remaining **6**—unchanged; this is partial CAST deletion.

TiKV owns legacy JSON encoding/parsing, lossy text boundary, temporal FSP restamping and error folding. A narrow TiDB bridge projects encoded JSON; Unistore keeps child/NULL/Datum projection. Seven local `SimpleSig::*AsJson` conversion bodies are deleted.

[Evidence](evidence/legacy-cast-json-checkpoint.md) · [receipts](logs/legacy-cast-json-summary.txt) · [manifest](checkpoint.json) · [ledger](migration-progress.json).

Four final Cargo gates GREEN. Three new tests;2 TiKV/116 TiDB old touched-file test bodies unchanged. A new-test Decimal expectation RED is retained and corrected from1.3 to exact1.25 before rerun.

[Remaining](evidence/remaining-acceptance.md): other direct legacy CAST targets block family credit; five complex exceptions, broader M2/root/liveDAG/final acceptance remain. Full expression/unistore/workspace/lint/dev/bazel/release/exhaustive/performance/memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/package/PR readiness unverified. Goal active.
