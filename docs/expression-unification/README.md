# Expression unification experiment

Current paired checkpoint **legacy-cast-integer-177**. Functional **239/245 (97.55%)**, strict **0**, remaining **6**; partial CAST deletion.

TiKV owns legacy REAL half-away rounding/range, Decimal truncation/overflow and lossy text prefix saturation for integer CAST. TiDB projects Value/Overflow; Unistore keeps child/NULL and concrete error construction. Three local bodies are deleted.

[Evidence](evidence/legacy-cast-integer-checkpoint.md) · [receipts](logs/legacy-cast-integer-summary.txt) · [manifest](checkpoint.json) · [remaining](evidence/remaining-acceptance.md).

Four final targeted gates GREEN; one new-test compile RED retained and fixed with assertion derives. Other CAST targets, R100 failures and broad final gates remain. Goal active.
