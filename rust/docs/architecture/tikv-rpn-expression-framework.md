# Expression evaluation with TiKV RPN

Rust TiDB compiles a typed SQL expression into a TiKV reverse Polish notation (RPN) program. For an ordinary call, the evaluator computes the operands first and then invokes the function registered on the call node. TiDB uses the upstream `RpnExpression::eval_decoded` to execute the program. TiDB owns the SQL bindings, session state and conversion between its Chunk columns and TiKV values.

## Binding an expression

[The expression compiler](../../crates/tidb-expr/src/rpn.rs) uses the SQL function name, argument types and declared result type to select a protobuf signature. For a supported signature it asks TiKV's public `RpnExpressionBuilder::build_from_expr_tree` for the registered function and its metadata. A TiDB-specific operation can bind a local `FnCall` instead. For example, a user-variable assignment receives its evaluated operand and writes it through the current statement context. Local bindings affect only the TiDB process.

A conditional needs control over which operands run. For `HOUR(IF(flag, a, b))`, TiDB binds `HOUR` to the upstream kernel. The `ShortCircuitFnCall` for `IF` evaluates the condition, calls the same upstream evaluator on the selected rows of each branch, and returns the values in logical row order. A skipped branch therefore consumes no random numbers and emits no conversion warnings. [The RPN node builder and callbacks](../../crates/tidb-expr/src/rpn/engine.rs) also distinguish scalar and batch operand demand where SQL requires different evaluation behavior.

## Execution state and columns

A projection's `EvaluatorSuite` and a selection's `FilterExecution` retain prepared expressions across input batches. Each execution owns its evaluation context and reusable buffers. Session operations use the current statement's state. The evaluator can process rows in select-list order when effects or errors make that order observable. Callers that process many rows should retain the execution object so preparation does not recur per row. [Projection and filter execution](../../crates/tidb-expr/src/evaluator.rs)

TiDB supplies a physical column batch and a logical row selection. Eligible integer and byte columns are materialized once for the batch and borrowed by repeated RPN column references. Other inputs can be read on demand, so an unselected branch can skip a fallible conversion. The Decimal adapter preserves the coefficient separately from its display scale. TiDB forwards warnings to the statement context. [Row and value adapters](../../crates/tidb-expr/src/rpn/rows.rs)

## Storage requests

For an in-process Unistore request, `WireExpressionExecution` binds scan column types and passes the complete protobuf expression to the upstream builder. TiKV validates the declaration and applies its own short-circuit and flattening rules. TiDB then adapts column reads and integer division diagnostics for the local request. A remote TiKV coprocessor receives the protobuf request and executes its own kernels. An expression using a TiDB-local callback can be pushed down only if the remote server supports an equivalent protobuf signature and semantics. [Unistore request handling](../../crates/tidb-unistore/src/cophandler.rs)
