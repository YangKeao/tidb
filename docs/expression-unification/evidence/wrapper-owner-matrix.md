# Wrapper owner matrix

`wrapper-owner-matrix-191` confirms central query and DML statement contexts carry the live execution token through `Columns`, and SDK routing selects active scope/execution before its explicit no-owner one-shot path.

Five immutable real-SQL filters are GREEN: query+DML/COW, execute/import, predicate/filter zero-slot refusal, comparison typed/filter/tuple, and grouping/rollup. [Receipts](../logs/wrapper-owner-matrix-summary.txt).

This does not claim exhaustive wrapper closure. The range audit only excluded Go production `pkg` NoColumns use, while the aggregate/window audit produced no reliable result. Rust standalone DDL/fold NoColumns paths and window-specific runtime propagation remain to classify.
