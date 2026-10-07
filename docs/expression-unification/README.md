# Expression unification experiment

Current paired checkpoint **legacy-cast-string-176**, after legacy REAL CAST. Functional **239/245 (97.55%)**, strict **0**, remaining **6**; partial CAST deletion.

TiKV owns legacy i128 sign selection, numeric/temporal STRING rendering, raw bytes passthrough and error folding. TiDB bridge projects Datum; Unistore keeps child/NULL projection. Six local `SimpleSig::*AsString` bodies are deleted.

[Evidence](evidence/legacy-cast-string-checkpoint.md) · [receipts](logs/legacy-cast-string-summary.txt) · [manifest](checkpoint.json) · [remaining](evidence/remaining-acceptance.md).

Four final targeted Cargo gates GREEN. Other legacy CAST targets block family credit. R100 historical failures and full expression/unistore/workspace/lint/dev/bazel/release/performance/memory/TiFlash/FIPS/package/PR-readiness remain unverified. Goal active.
