# Expression evaluation with TiKV RPN

Rust TiDB compiles bound SQL expressions into TiKV's reverse Polish notation (RPN) and executes them through TiKV's `RpnExpression::eval_decoded`. TiKV supplies the RPN interpreter, vectorized built-in kernels, and GB18030 codec; TiDB owns SQL binding, statement and session state, short-circuit and diagnostic contracts, and conversion between TiDB `Chunk` columns and TiKV `VectorValue` buffers.

## Binding and host callbacks

The [compiler](../../crates/tidb-expr/src/rpn.rs) starts from the protobuf `ScalarFuncSig` already selected by TiDB's SQL constructor (`pb_builtin.rs`) and validates or coerces operand shapes against [TiKV's RPN engine](../../crates/tidb-expr/src/rpn/engine.rs). Declared built-ins that execute directly on TiKV's kernels use `Kernel::RpnOnly` with no separate local execution body.

Control-flow and lazy operators—such as `IF`, `IFNULL`, `COALESCE`, `CASE WHEN`, `AND`/`OR`, `CONCAT`, `INTERVAL`, `ELT`, `MAKE_SET`, seeded `RAND`, and `BENCHMARK`—lower to TiKV `ShortCircuitFnCall` nodes that evaluate only the selected row subset per branch, so skipped branches never emit warnings, conversion errors, or random draws. SQL operations with session state or SQL-specific contracts (such as user variables, advisory locks, JSON schema validation, or `max_allowed_packet`-checked outputs) bind as RPN-scheduled [SQL host callbacks](../../crates/tidb-expr/src/rpn/sql_host.rs) whose subexpressions still run inside the same RPN loop.

When an expression contains operands that require left-to-right `NULL` short-circuiting, the compiler builds both a scalar-guarded `program` (for row-ordered evaluation) and an unguarded `batch_program` (for vectorized chunk evaluation). Constant folding and single-row evaluation share the same compiled scalar entry; constructs outside RPN (such as unevaluated scalar subqueries) stay on TiDB's typed expression API.

## Execution ownership

[Projection and Selection](../../crates/tidb-expr/src/evaluator.rs) retain their compiled `EvaluatorSuite` and `FilterExecution` programs across chunks and reset execution state on operator reopen. Independent projection workers own separate program instances, while projections with mutable statement effects (such as seeded `RAND` or user-variable writes) serialize in SQL row order. `UPDATE` assignments and `HashAgg` group keys evaluate rows through `RowProgram`, which binds its expression at construction time.

The [row adapters](../../crates/tidb-expr/src/rpn/rows.rs) expose TiDB `Chunk` columns to TiKV's RPN loop through a scoped thread-local bridge (`NativeRows`). Pre-decoded `Int` and `Bytes` columns borrow zero-copy via `ColumnRef`, while other types convert lazily on demand for only the logical rows evaluated. Decimal conversion preserves stored arithmetic precision separately from visible display scale, and SQL warnings forward back into TiDB's statement context.

## In-process coprocessor and dependencies

[Unistore](../../crates/tidb-unistore-cophandler/src/lib.rs) binds scan fields and compiles wire `tipb::Expr` nodes with `WireExpressionExecution`, retaining request-local time zone, flags, division precision, and warning policy, with fallback to the typed wire decoder for signatures outside TiKV's builder coverage. Remote TiKV continues to receive the planner's protobuf requests.

The [Cargo lock file](../../Cargo.lock) pins upstream TiKV at `548812e1ef57aef077a2062a9cc356640a6347f5`. `tidb-server` links unprefixed `tikv-jemalloc-sys` for `std::alloc::System` while preserving jemalloc heap profiling, and Parquet 58.4.1 keeps GZIP support with `flate2` 1.1 from the `crates-io-parquet` sparse registry alongside TiKV's pinned `flate2` 1.0.11.
