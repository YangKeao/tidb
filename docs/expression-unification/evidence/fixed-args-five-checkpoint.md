# Fixed-argument five-family migration — fixed-args-five-09

Functional delegation/native-deletion progress:15/245, target221. HEX, BIN, LEFT, RIGHT and REPLACE now join the same TiKV worker/driver/pool. HEX includes Int and Bytes; LEFT/RIGHT include binary and UTF-8. The frozen baseline has no implemented PB/unistore signatures for these five families; none were invented for credit. Typed HEX retains its existing declared-type/context entry.

## Shared implementation

EvaluatedArgs admits nullable Bytes, Int bit patterns, Bytes+Int and Bytes×3. The operation selects canonical ordered slot types, arity and one official FnCall. A fixed stack ScalarValue array carries1–3 ready values; no arbitrary program/schema/host API is exposed. All Bytes capacities are checked/summed, including arguments following NULL, and overlap owned result extraction. Shape mismatch refuses before actual dispatch. Old eval_one/ASCII and native Bytes helpers are thin entries into the same generic driver. Caller max_nodes is4 for the fixed three-argument recipe; steps/frame limits and one root/epoch/retirement ledger are unchanged.

Native algorithms removed: HEX/BIN formatting, LEFT/RIGHT slicing and REPLACE scanning. Native frontend retains typed HEX/BIT/UInt bit interpretation and rounding, BIN warning1292/integer conversion, count-first LEFT/RIGHT coercion (NULL skips subject conversion; zero does not), Go text normalization, and REPLACE's full tuple conversion/error order and original subject-based result packing. NULL results still come from the official wrapper, not native shortcuts. There is no extra per-shape pool or fresh-root bypass.

## Actual validation

From `tikv/`, using `../tools/cargo-tikv`:

- `test --locked -p tidb_query_expr --lib local:: -- --test-threads=1`:186 passed/1 ignored/466 filtered.
- `test --locked -p tidb_query_expr --lib test_evaluated_bytes_rejects_same_carrier_operation_and_kernel_drift -- --test-threads=1`:1 passed/652 filtered.

From `tidb/rust/`, using `../../tools/cargo-tidb`:

- `test --locked -p tidb-expr --lib args_dispatch_ -- --test-threads=1`:3 passed/1474 filtered.
- `test --locked -p tidb-session --lib tests_core::lifecycle::evaluated_ascii_ -- --test-threads=1`:21 passed/2078 filtered.
- `test --locked -p tidb-expr --lib -- --test-threads=1`:1379 passed/4 unchanged failures/94 ignored,1477 discovered, exit101. All four complete failure blocks equal08 after only thread-ID normalization.

No compile or new-test failure occurred in this checkpoint; no expected values changed. Tests cover actual official dispatch, all fixed shapes, NULL/error precedence, typed HEX/BIT and64-bit high values. Serial one-slot stored-column SQL mixes all five families;20 direct zero-slot SQL calls cover NULL/non-NULL paths without an outer HEX hiding an inner native bypass. Core backend tests also exercise wrong-shape refusal and combined retained capacity after NULL.

## Review and deferred checks

TiKV maintenance README/repository overview/coprocessor guide were read; the in-process-library guide now describes fixed ready arguments, shared driver, output ownership and capacity accounting instead of the old ASCII-only description. RPC request parsing/read pools/wire behavior are unchanged. TiDB architecture index maps the generic adapter and retained frontend responsibilities, with explicitly scoped Rust test commands. The agents-review-guide checklist found no new normative policy, PR/Bazel workflow or Make target changes; its new path references and the paired Plan/result excerpts are checked by the publication Python assertions. No Go/generated fixture/Bazel change is claimed, and make lint remains outstanding for final completion.

Not claimed: release performance, full business-wrapper operation scopes, allocator remeasurement, physical peak/OOM safety, full workspace/make lint, network end-to-end or final acceptance. Old allocation receipts are not evidence for current artifacts. Raw local logs are under expression-unification/logs/fixed-args-five-*.log; published result excerpts are in logs/fixed-args-five-summary.txt. Next integer-bitwise work remains excluded from this checkpoint's validated source snapshot.
