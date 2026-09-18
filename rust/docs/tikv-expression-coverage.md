# Broad local reuse of TiKV expression kernels

This report accompanies `tikv-expression-coverage-execplan.md`. It describes an
experimental in-process adapter that reuses TiKV's existing RPN expression
kernels from the TiDB Rust port. It is **not** a Go `pkg/expression` package
transcreation, it does not remove the native evaluator, and it does not claim
that every dispatched TiKV signature is reachable from SQL.

The work lives on:

* TiDB Rust: branch `feat/tikv-expression-coverage`, based on `7a5c468572bbe26a3981dfee938886f4bdb6b554`.
* TiKV: branch `feat/standalone-expression-coverage`, commit `6806dc4d2537be3df63c220ba6ff977bd0ffd774`, pinned by `rust/crates/tidb-expr/Cargo.toml`.
* Only YangKeao forks are used; upstream pushes remain disabled.

The earlier copying and borrowed-copy-removal stages are preserved unchanged:
`feat/tikv-expression-borrowed-poc` (TiDB) and `feat/borrowed-expression-poc`
(TiKV), with their benchmark CSVs and ExecPlans.


## 1. What the adapter now covers


### 1.1 Signature inventory

`rust/scripts/tikv_expression_coverage.py` regenerates
`rust/docs/tikv-expression-coverage.csv` and `.json` from the pinned sources. It
separates four different questions that are easy to conflate:

| Question | Answer in this checkout |
| --- | --- |
| Variants in the upstream `ScalarFuncSig` enum | 640 (including `Unspecified`) |
| Variants the TiKV RPN dispatcher actually maps | 510 |
| Dispatched variants missing from the local Rust protobuf enum | 290 |
| Baseline local adapter (before this work) | 23 signatures / 13 names |
| Baseline engine facade whitelist | 52 signatures |
| Dispatched signatures with adapter source or generated-name evidence now | 461 |
| Admission table rows (Milestone B) | 384 = 232 admitted + 152 excluded |
| Per-signature admission status | 385 admitted, 129 excluded, 126 untested |

The last source-evidence number is deliberately an over-approximation from
static source evidence; it is no longer what decides support. Since Milestone B
the report's authority is the explicit table in
`rust/crates/tidb-expr/src/tikv/admission.rs`: one row per SQL function name
with an `admitted` or `excluded` decision, a signature family, required
argument eval types, a lazy-shape constraint and an exclusion reason. Each CSV
and JSON signature row now carries `admission_status` (`admitted`, `excluded`
or `untested`) and the `admission_names` that decided it, and the full table is
exported as `admission_table`. The row universe is the crate's 309-name
transcription of Go's `funcs` map plus 75 rewriter/internal spellings; the
generator and the Rust test both fail if a name lacks a row. The generator's
`--self-check` validates uniqueness, source hashes, table consistency and
cross-counts, not SQL behavior.

Because `tidb_proto::tipb::Expr.sig` is an optional raw `i32`, signatures absent
from the local enum can still be emitted. The engine exposes
`scalar_function_signature(name)`, and the native builder remains the only
capability authority: resolving a name is not evidence that a shape compiles or
runs.

### 1.2 Evaluation types

The standalone facade now carries exact native values for every engine type
except `Set` (which the native type/codecs do not support): `Int`, `Real`,
`Bytes`, `Decimal`, `DateTime`, `Duration`, `Json`, `Enum` and
`VectorFloat32`. Decimal is transported as the native 40-byte value rather than
text; the old text bridge silently discarded hidden fractional digits.

The TiDB side converts chunk storage to those carriers and back in
`rust/crates/tidb-expr/src/tikv/bridge.rs`. Conversions are checked and
fallible: selection order and duplicates, NULL bitmaps, unsigned bit patterns,
FLOAT (4-byte) versus DOUBLE (8-byte) storage, `BIT` width, ENUM names/indices,
temporal wall fields plus FSP, JSON tags and vector element bits are all
preserved or the value is declined.

### 1.3 Function families

`rust/crates/tidb-expr/src/tikv/lowering.rs` and its `lowering/families.rs`
child map function names onto wire signatures. The covered families include
integer/real/decimal arithmetic, comparisons and `IN`, bit operators and
`BIT_COUNT`, the math family, string functions (case, trim, substring,
`LENGTH`/`CHAR_LENGTH`, `HEX`, `INSTR`, `LOCATE`, `REPLACE`, hashes, `LIKE`,
regexp), temporal extraction/formatting/arithmetic, JSON operations, vector
operations, and leaf-only control flow.

Two structural rules keep that breadth honest:

* **No speculative coercion in possibly-skipped branches.** TiKV RPN evaluates
  every child eagerly, so `IF`/`IFNULL`/`COALESCE`/`CASE`/`AND`/`OR`/`IN`/
  `GREATEST`/`LEAST` are admitted only when the arguments that may be skipped
  are leaves, and the adapter refuses to insert an implicit cast there.
* **No session semantics the embedder cannot represent.** Clock, RNG,
  user-variable, lock, packet-limit and mode-dependent functions stay native
  because the local `Context` has no binding for them.


## 2. Problems found and how they are handled


### 2.1 Semantic divergences proved by the mysql-tester replay

The enrolled replay (`difftest-result-tests --test integration_diff`, 110
topics, 10,252 compared statements) was run native, copying and borrowed with
identical divergence sets. Three real divergences surfaced while adapting:

| Divergence | Evidence | Resolution |
| --- | --- | --- |
| `UUID_VERSION`/`UUID_TIMESTAMP` accept malformed UUID strings where Go raises error 1411 | `expression/uuid` topics appeared only in the engine-enabled replay | Both names excluded from local admission until a faithful validator exists |
| `ORD(NULL)` returns 0 where Go returns NULL | TiKV's own `test_ord` pins the 0 | Leaf-only wrapper `IF(StringIsNull(x), NULL, ORD(x))`, argument charset used for the inner node |
| `GREATEST`/`LEAST` over unsigned integers compare raw `i64` | `UInt` values above `i64::MAX` sorted as negative | Exact Decimal comparison plus the original typed cast, no kernel change |

After the first fix the replay's divergence set became byte-identical to the
native one (142 entries, same statements), so the engine adds no new divergence
on that corpus. Those 142 are pre-existing plan-text/row divergences of this
working tree; the native run asserts zero engine rows.

### 2.2 Kernel-level panic risks on valid finite input

Adversarial review plus bounded `catch_unwind` witnesses found that some
existing kernels panic on inputs the old whitelist never reached:

* `TRUNCATE(0.0, 309)` / `ROUND(1.0, -400)` build `Inf`/`NaN` internally and
  feed `Real::new(...).unwrap()`.
* `ROUND(f64::MAX, -308) * 0` returns `Inf` from a finite input and the next
  arithmetic node panics.
* `VecL2Distance([3e38], [-3e38]) * 0` is the same chain through the vector
  family.

The standalone facade now preflights fractional-digit arguments and evaluates
through a checked RPN entry that inspects each produced `Real` vector before a
following node consumes it. The stock `eval_decoded` path used by the TiKV
server is unchanged. Malformed arity/type/regexp/JSON/enum metadata and
untrusted Decimal/Time/JSON payloads are rejected before the mapped kernel.

### 2.3 Metadata representation gaps

Real SQL `JSON` and long `BLOB`/`TEXT` columns carry `flen = 4294967295`, which
the local `i32` conversion refused in both the compile schema and the shared
pushdown catalog helper. A local helper narrows exactly that sentinel to `-1`,
matching Go's `ToPBFieldType`, without changing distributed pushdown policy.

TiKV and TiDB also encode temporal JSON scalars differently. The bridge refuses
those shapes (including nested values) and synchronously-producing casts, so
the adapter never emits a JSON value the local type cannot represent.


## 3. Verification receipts


All commands run one at a time under
`tools/limited-run.py` (group RSS budget, per-process address-space cap, host
memory reserve), with `CARGO_BUILD_JOBS=1`, `RUST_TEST_THREADS=1` and Go at
`-p=1`/`GOMAXPROCS=1`.

### 3.1 Rust suites against the pinned engine

| Command | Result |
| --- | --- |
| `cargo test -p tidb-expr --features tikv-expr --lib --test all --locked` | 1200 passed, 39 passed, 99 ignored |
| `cargo test -p tidb-executor --features tikv-expr --test all --locked` | 353 passed, 184 ignored |
| `cargo test -p tidb-session --features tikv-expr --lib --test all --locked` | 1726 passed, 338 passed, 209 ignored |
| `cargo test -p tidb-query-expr` (TiKV `-p tidb_query_expr --lib`) | 441 passed |

The adapter's own fixtures are `tidb-expr/tests/tikv_coverage.rs` (four
native-versus-engine differential matrices over numeric, string, temporal,
JSON and vector families, selected rows, NULLs and counters) and
`tidb-executor/tests/tikv_expression_coverage.rs` (eight SQL suites through the
parser/rewriter/executor). A successful call must increment the engine-row
counter, so a native fallback cannot masquerade as coverage.

### 3.2 Differential and replay suites

| Target | Result |
| --- | --- |
| `--test catalog_diff` | 31 passed |
| `--test query_diff` | 1 passed |
| `--test integration_diff` with `INTEGRATION_TIKV_BACKEND=copying` | pre-existing red: 142 of 10,252 divergences, identical set to native; engine ran **12,447 expression-row evaluations over 1,555 statements** |
| same with `INTEGRATION_TIKV_BACKEND=borrowed` | identical divergence set; **10,670** of those rows went through borrowed kernels |
| `--test expr_diff`, `--test table_diff`, `--test join_shape` | pre-existing red, identical with the feature disabled (2 `EXPORT_SET` cases; 7 of 1,942; stale ratchet `(269,223,97,93,4)` vs `(246,168,90,86,5)`) |

`RUST_MIN_STACK=2097152` and `MALLOC_ARENA_MAX=2` are required for topics and
suites that spawn worker pools; otherwise the address-space cap makes
`std::thread::spawn` fail with `EAGAIN`. That is a harness setting, not a
product change.

### 3.3 Go expression surfaces (oracle, not adapter execution)

| Command | Result |
| --- | --- |
| `./tools/check/failpoint-go-test.sh pkg/expression -count=1` | PASS (16.2 s) |
| `./tools/check/failpoint-go-test.sh pkg/expression/integration_test -count=1` | PASS (49.2 s, peak RSS 10,489 MiB; an 8 GiB run had OOMed) |
| `aggregation`, `exprctx`, `expropt`, `exprstatic`, `sessionexpr`, `test/constantpropagation`, `test/multivaluedindex` | PASS |

These exercise the original Go implementation. They are an oracle/reference
result and are **not** evidence that the Rust adapter behaves identically;
that evidence is Section 3.1/3.2, where the engine actually ran.

### 3.4 Repository gates

* `cargo clippy` scoped to the changed crates, with `--no-deps`: exit 0.
* `make -j 1 lint`: exit 0.
* `rustfmt --check` on every changed Rust file: clean.


## 4. Remaining exclusions and known limits

The adapter is intentionally narrower than "everything TiKV dispatches". Not
admitted today:

* Session-dependent or effectful functions: clocks, `SYSDATE`, RNG, UUID
  generation, advisory locks, user variables, `SLEEP`, sequence functions,
  packet-limit-sensitive string builders and mode-dependent `WEEK` without an
  explicit mode.
* `UUID_VERSION`/`UUID_TIMESTAMP` (the 1411 divergence above).
* Parameters, deferred expressions, correlated columns and non-finite REAL or
  vector values, which route native before any kernel runs.
* Temporal JSON values and casts that would produce them.
* `SET`/`GEOMETRY`/array columns, which the native engine types do not support.
* Deriving a few kernel semantics that remain unproven (for example some
  FLOAT-result arithmetic and JSON document-versus-value coercion) — these stay
  native rather than being guessed.

A requested `Backend::Borrowed` falls back to the copying adapter for any type
or shape without a borrowed loader; the borrowed counter excludes that
fallback, and the differential fixtures assert the distinction.

The default remains native. Enabling `tikv-expr` alone changes nothing; a
statement context must explicitly opt in, and the new per-session opt-in used
by the replay harness is test-only.
