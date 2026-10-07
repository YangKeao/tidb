# Expression unification experiment

Paired branch: `expression-unification-demo`. Current checkpoint: **legacy-cast-real-175**, after **legacy-cast-json-174**.

Functional **239/245 (97.55%)**, strict **0**, remaining **6**—unchanged; partial CAST deletion.

TiKV owns legacy i128/real/decimal/lossy-text REAL conversion and event/error folding. TiDB bridge projects Datum; Unistore keeps child/NULL/result projection. Four local `SimpleSig::*AsReal` bodies are deleted and the direct Real identity bug is fixed.

[Evidence](evidence/legacy-cast-real-checkpoint.md) · [receipts](logs/legacy-cast-real-summary.txt) · [manifest](checkpoint.json) · [ledger](migration-progress.json).

The new regression failed before implementation (`None` vs `Some(2.5)`) and the identical test plus four targeted gates pass afterward. Three new tests;2 TiKV/118 TiDB old touched-file tests unchanged.

[Remaining](evidence/remaining-acceptance.md): other direct legacy CAST targets block family credit; five complex exceptions, broader M2/root/liveDAG/final acceptance remain. Full expression/unistore/workspace/lint/dev/bazel/release/exhaustive/performance/memory/TiFlash/FIPS/package/PR readiness unverified. Goal active.
