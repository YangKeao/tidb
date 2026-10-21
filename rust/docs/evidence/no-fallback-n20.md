# N20: Expand enters the shared prepared route

Expand now passes each existing level evaluator, its cached child chunk and the
actual child field types to `run_with_tikv_batch`. Production code changes only
that call. It does not clone or rebuild expressions, invent a schema, change
RNG occurrence identity or retain an expression callback in the shared frame.

The zero-width virtual-row bypass, required-row forwarding, cached input,
level advance only after success, memory checks/tracking and close/reopen
transitions are unchanged. The existing `avoid_column_evaluator=true` still
prevents consuming the cached input while later grouping levels need it.

This is caller integration within task #4. Generic Admission fallback remains
inside EvaluatorSuite for unsupported classes; no global fallback deletion,
public AST/filter/unistore closure, full-server or performance claim is made.

Before changing production, the actual old Expand executor recorded and reran
**64 output chunks / B42FB8BE316E6C79**. The matrix covers two grouping levels,
selected/reversed/duplicate and normal inputs, two batches and two opens,
seeded/column RAND, ECB/CBC AES, prepared parameters, schema caches, ordered
session variables, both flags, child read counts and memory release. The
independent child is fetched three times per open, rather than per level.
Actual variable storage is explicitly supplied; its final counter is 112.

The prepared-route guard first failed with `(prepared,fallback,refusals) =
(0,0,0)` rather than `(4,0,0)`. After integration, both levels have `(4,0,0)`
and the original hash is unchanged. A later-level missing-provider error also
preserves the original column prefix, current level and unconsumed cached input
on repeated calls, then releases memory on close.

Projection's existing mixed/ordered test now also binds actual mutable session
variable storage. The default statement context has no such provider and its
writes are inert; this strengthens the caller oracle without changing production
Projection behavior.

Validation from the TiDB root, with `RUSTFLAGS='' CARGO_BUILD_JOBS=8`:

- `../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-executor --lib expand::tests -- --nocapture`: **6 passed**, original checksum unchanged (`n20-expand-source-green.log`).
- Same base command with `--test all physical_expand_source`: **1 passed**, the actual TableDual/Projection/Expand chain (`n20-expand-physical-final.log`). Executor disables automatic test targets; the file is part of target `all`.
- Same base command with `--lib projection::tests`: **27 passed**, including actual variable storage, both flags and serial/two-worker execution (`n20-projection-real-session-final.log`).
- TiDB-root `make lint`: passed (`n20-lint-final.log`). Both pinned rustfmt versions pass both changed Rust files; `git diff --check` passes.
- `n20-final-check.py`: production changes only the caller route with genuine child types, original RED/capture/rerun/GREEN, unchanged expression/SDK bodies and validation receipts checked before/after local DCO.

Expression implementations and SDK ad73b11 are unchanged. N19 frontend baseline
1997/exact original 3 failures/95 ignored/456 batch/346 warnings and N16's fresh
SDK baseline remain applicable. No repeated broad sweep is needed for this
caller-only change. Original source snapshots, captures and logs remain in the
owning agent workspace under `evidence/no-fallback/n20-*`.

Continue optional/protobuf metadata, public/filter/unistore entries, generic
fallback removal and final caller/server/performance validation immediately.
