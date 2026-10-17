# N6f0: typed Decimal-fast outcomes in native storage

This preparatory checkpoint changes the frontend value store only. It adds no
SQL function coverage and does not retire a fallback route. TiKV is unchanged
at 7ac5b37. The unfinished N6f arithmetic root integration was saved separately
and excluded from this checkpoint and its full regression run.

Native Decimal arithmetic has a priority phase before the vectorization flag.
Checked coefficient arithmetic can report Unsupported, which is neither SQL
NULL nor a SQL/resource error. The next canonical implementation needs to carry
this distinction without encoding a flag in a SQL value.

`batch_values.rs` now stores either an actual SQL Datum or the existing typed
`NativeDecimalFastOutcome`. The latter has a separate capability role. Ordinary
SQL insertion cannot mint that role; ordinary borrowing and kernel cloning
cannot consume its payload. Namespace, generation, exact producer/occurrence,
row, role, capacity and resource checks remain in force. The shared insertion
implementation calls its ownership closure only after every pre-insertion
gate. This is a local clone constructor, not an expression callback.

Two new tests distinguish Unsupported, Value(None), and a scaled coefficient;
they reject ordinary SQL access, forged roles, stale/cross-store/wrong-occurrence
handles and SQL NULL passed as a fast payload. They also verify generation and
exact entry-plus-handle budget boundaries without publishing rejected entries.
The existing nineteen-kind and handle/resource tests remain unchanged.

Validation from the TiDB root:

```sh
RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib native_values::tests
RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib
RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-executor --lib projection::tests
make lint
```

Five store tests passed. The final complete frontend run passed 1469 tests,
with the same four failures and 95 ignored tests as N6e. Exact failure-name
comparison passed; this is not a completely green suite. The failures remain:

- pushdown_catalog::tests::ifnull_string_column_literal_uses_go_signature_and_column_collation
- tests::builtin_info_json_math_source::exp
- tests::builtin_math_misc_op_source::vectorized_builtin_op_func
- time_fn::tests::str_to_date_partial_formats_follow_no_zero_date

All 22 Projection tests passed, including serial and parallel native values,
CASTs, comparisons and arithmetic. `make lint`, pinned
frontend rustfmt checking and `git diff --check` passed. SDK code is unchanged;
its suite/clippy were not rerun for this frontend storage-only checkpoint.
Evidence is retained in the agent workspace under
`evidence/no-fallback/n6f-fast-store{,-full,-final,-projection,-lint}.log`.

The next step is an explicit fast-value phase through the same canonical SDK
frame driver, followed only on typed Unsupported by the native bulk/row phase.
SQL or resource errors must be terminal. Empty selections, literal conversion,
integer-column casts, whole-operand order and actual context reads remain part
of that work. Root integration's exact context-trace assertions were not
relaxed and are intentionally outside this checkpoint until they pass.
