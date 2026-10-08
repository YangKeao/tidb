# M6 bounded cost record

`m6-cost-record-214` closes the explicit Plan line-385 reporting categories without introducing a performance threshold.

## Direct dependency compilation

A locked dev `cargo check` of `tidb_query_datatype`, `tidb_query_expr`, `tidb_query_aggr`, and `tidb_query_executors` in a new isolated target directory completed in 10.17 s Cargo time / 10.19 s wall, with 1,472,560 KB maximum process-tree RSS. This is a local debug build record, not a cold-CI guarantee.

## Owned transport and width one

One compiled Plus program, `EvalContext`, and borrowed one-row input were reused for 10,000 width-one evaluations; the same immutable `ExecutionLimits` value was copied into each call, which created its own budget and row scratch. The run took 19,147,858 ns total (about 1,915 ns/evaluation) and the actual retained output accounting was 16 bytes. The `LocalBatch` borrows its input payload, so this path copies no input payload. Its scalar collection path creates the capacity-one output collector and one temporary width-one result vector, then appends once. Those are logical owned-vector/materialization counts, not physical allocator-call or peak-memory claims.

The unchanged output-shape test runs the same occurrence driver at selection widths 0, 1, 1,024 and 1,025, covering the batch boundary. This demonstrates the declared batch-safe scope; it does not claim vectorized speedup.

Full compatible workspace clippy remains GREEN after adding the record. There is no baseline, pass/fail threshold or performance-improvement claim. [Receipts](../logs/m6-cost-record-summary.txt).
