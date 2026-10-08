# Expression unification experiment

Current paired checkpoint **strict-metadata-timing-207**. Functional Demo: **240/245 (97.96%)**, strict **0**.

Constant REGEXP calls no longer compile/cache during bottom-up folding and execute only when demanded. String-IN metadata prepares once and is rebuilt only after argument invalidation/rebinding. Nine focused gates are GREEN. [Evidence](evidence/strict-metadata-timing-checkpoint.md) · [receipts](logs/strict-metadata-timing-summary.txt).

Broader M2 is complete. M3 still defers strict-local IN argument-order support and the wire >32 nested logical eager fallback. M6 final acceptance and PR readiness remain open.
