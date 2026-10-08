# Strict production wire depth

`strict-wire-depth-209` closes the last planned M3 production-control gap.

TiKV now exposes `build_from_expr_tree_strict_controls`. Every production DAG expression constructor—aggregate parser/grouping, selection, projection, TopN and partition TopN, and rank limit—uses that path. An eligible nested AND/OR therefore remains a `ShortCircuitFnCall` beyond depth 32 instead of silently becoming eager. Evaluation still uses the existing iterative frame driver and its retained-storage accounting.

The old public builder keeps its depth-32 behavior for compatibility; its immutable stress oracle remains unchanged and GREEN, but no production request constructor calls it. A new 33/256-depth test covers AND/OR with and without cast separators, proves no regular logical call exists, and compares strict versus eager results on a 2 MiB stack. Production aggregate/executor compilation and TiDB's 1,024-pair iterative bridge are GREEN.

The attempted executor test target remains RED because existing test-only `rpn_fn` expansion accesses crate-private `CallShape` methods across crates; production `cargo check` is GREEN. The failure is retained and excluded, not reported as a pass. [Receipts](../logs/strict-wire-depth-summary.txt).
