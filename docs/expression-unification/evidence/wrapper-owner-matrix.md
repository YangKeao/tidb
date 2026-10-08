# Wrapper owner matrix

`wrapper-owner-matrix-191` confirms central query and DML statement contexts carry the live execution token through `Columns`, and SDK routing selects active scope/execution before its explicit no-owner one-shot path.

Five immutable real-SQL filters are GREEN: query+DML/COW, execute/import, predicate/filter zero-slot refusal, comparison typed/filter/tuple, and grouping/rollup. [Receipts](../logs/wrapper-owner-matrix-summary.txt).

R198 classifies window runtime: `build_window` retains `StmtContext` in `WindowExec`, and partition/order/range/aggregate/value/lead-lag evaluations use it. A zero-slot `SUM(ASCII(v)) OVER` SQL test confirms the owner reaches a window argument. DDL default paths also use live context, while aggregate cast's NoColumns use is a literal metadata probe.

Two real gaps remain: null-rejection folding drops available planner `Columns`, and window integer constant extraction rejects prepared `ParamMarker` values. A suspected storage-class UTC/default-zone mismatch was not reproduced by the attempted accepted expression, so no fix or claim was made. [R198 receipt](../logs/wrapper-classification-summary.txt).
