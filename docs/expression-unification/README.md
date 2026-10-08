# Expression unification experiment

Current paired checkpoint **legacy-cast-time-183**. Functional **239/245 (97.55%)**, strict **0**, remaining **6**.

TiKV owns admitted legacy Time casts for five sources plus JSON opaque-first/string fallback. Unistore retains child/NULL/Datum projection. [Evidence](evidence/legacy-cast-time-checkpoint.md) · [receipts](logs/legacy-cast-time-summary.txt).

Three focused gates GREEN. Duration→Time stays refused; Time→Duration and i128 identity remain. Whole CAST is partial; R100 and broad gates remain. Goal active.
