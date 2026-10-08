# Expression unification experiment

Current paired checkpoint **decimal-avg-division-202**. Functional Demo: **240/245 (97.96%)**, strict **0**.

AVG finalization now delegates SUM/COUNT division to TiKV shared MySQL decimal math, and TiDB's remaining schoolbook digit-division body is deleted. Six focused consumer gates are GREEN; one broader `tidb-exec` target remains compile-blocked and is not counted. [Evidence](evidence/decimal-avg-division-checkpoint.md) · [receipts](logs/decimal-avg-division-summary.txt).

Broader M2 remains active for hash normalization, assignment fitting, natural codec shape and chunk fallback. M6 final acceptance and PR readiness are not claimed.
