# Expression unification experiment

Current paired checkpoint **cast-complete-184**. Functional **240/245 (97.96%)**, strict **0**, remaining **5**.

Frozen family `cast` is functionally TiKV-only across implemented AST/typed/PB/vector/helper/Unistore routes. Native retains only child/NULL/Datum/error/context effects and full-width i128 identity projection. Explicit unsupported exceptions are documented without fallback. [Evidence](evidence/cast-complete-checkpoint.md) · [receipts](logs/cast-complete-summary.txt).

Two focused gates GREEN. R100, M2/root/live-DAG and broad final gates remain. Goal active.
