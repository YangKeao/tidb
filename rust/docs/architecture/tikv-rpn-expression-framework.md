# Expression evaluation with TiKV RPN

Rust TiDB compiles bound SQL expressions into TiKV's reverse Polish notation (RPN). A call node receives its evaluated operands and invokes the registered function. TiDB executes the program through the original TiKV `RpnExpression::eval_decoded` and owns SQL binding, statement state, and conversion between TiDB Chunk columns and TiKV values.

## Binding

The [compiler](../../crates/tidb-expr/src/rpn.rs) starts with the protobuf signature selected by TiDB's function class. It uses [TiKV's public builder and callbacks](../../crates/tidb-expr/src/rpn/engine.rs) to bind the function and its metadata. SQL operations with a TiDB-specific contract can use a local call node with the same operand and result types. For example, a user-variable assignment receives the evaluated operand and writes it through the current statement context.

For `HOUR(IF(flag, a, b))`, the RPN callback for `IF` evaluates only the required branch for each row, and `HOUR` reads the branch result. The conditional calls the same evaluator on its subprograms, so skipping a branch skips its warnings, conversion errors, and random draws. The compiler uses the scalar or batch operand ordering required by the caller. An expression that the compiler cannot represent, such as an unevaluated scalar subquery, remains with TiDB's typed expression API.

## Execution ownership

[Projection and Selection](../../crates/tidb-expr/src/evaluator.rs) reuse their prepared programs across chunks and reset execution state when the operator opens. Independent projection workers own their own programs. Projections that change statement state run serially so a seeded call continues across chunks in SQL row order. UPDATE assignments and hash aggregation keys keep their own programs and evaluate rows in the existing operator order.

The [row adapters](../../crates/tidb-expr/src/rpn/rows.rs) use logical row indices to read physical columns. They can materialize fixed-width columns once per batch and defer conversions for inputs that may be skipped. Decimal conversion preserves the coefficient separately from display scale. SQL warnings are forwarded to TiDB's statement context.

## In-process requests and dependencies

[Unistore](../../crates/tidb-unistore-cophandler/src/lib.rs) binds scan fields and compiles wire expressions with TiKV's builder. Request-local evaluation retains its time zone, division precision, and warning policy. The existing typed wire decoder handles signatures or scan declarations outside the original TiKV builder's coverage. Remote TiKV continues to receive the planner's protobuf requests. Planner pushdown checks and remote kernel semantics continue to apply.

The [Cargo lock file](../../Cargo.lock) pins original TiKV at `548812e1ef57aef077a2062a9cc356640a6347f5`. TiKV pins flate2 1.0.11. Parquet 58.4.1 keeps GZIP support with flate2 1.1 from the crates.io project's sparse GitHub index. Cargo treats the two indexes as distinct sources, and the lock file verifies crate checksums for both.
