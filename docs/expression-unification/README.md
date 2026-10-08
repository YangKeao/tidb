# Expression unification experiment

Current paired checkpoint **strict-local-in-208**. Functional Demo: **240/245 (97.96%)**, strict **0**.

Signed-LongLong SQL IN now lowers to a TiKV private source-order local identity. It performs no wire constant extraction or runtime-parameter hashing, delegates three-valued reduction to the shared IN control, and safely reuses compiled programs across rebinding. The PB/wire mapping remains unchanged. Eight focused gates are GREEN. [Evidence](evidence/strict-local-in-checkpoint.md) · [receipts](logs/strict-local-in-summary.txt).

Broader M2 is complete. M3's remaining explicit item is the official wire builder's eager fallback for nested AND/OR deeper than 32. M6 final acceptance and PR readiness remain open.
