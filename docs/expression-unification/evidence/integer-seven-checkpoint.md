# Seven integer families — integer-seven-10

Functional delegation/native-deletion progress:22/245, target221. BIT_COUNT, bitneg(~), bitand(&), bitor(|), bitxor(^), leftshift(<<) and rightshift(>>) now delegate to the same TiKV evaluator. They are seven frozen families, not variants of BIT_LENGTH.

## Changes and domain coverage

TiKV adds nullable Int2 and seven closed operation mappings to the existing fixed stack/driver; no new evaluator pool, arbitrary schema, public callback or numeric narrowing. Official signatures are BitCount3128, BitNegSig3121, BitAndSig3118, BitOrSig3119, BitXorSig3120, LeftShift3129 and RightShift3130. Physical signed Int carries full64-bit patterns. Shift count interpreted as u64 yields0 for>=64, including negative bit patterns; right shift is logical.

TiDB removes BIT_COUNT's count_ones, all unary bit-negation branches, integer/real/decimal bitwise calculations and native shift helpers. Existing argument coercion stays in place: string1292, Decimal rounding/saturation1292, Real ties-even/1690, BIT_COUNT's distinct conversion rules, UInt bit interpretation. Original NULL points and demand/error/warning order are preserved, including both Result operands being formed before propagation. Successful and NULL results come from C4. Six bitwise families keep raw and SQL UInt; BIT_COUNT remains signed Int. No new context/root or factory-limit changes.

AST, typed scalar, existing row/vector fallback and public helpers converge on the existing ops context path. The arithmetic-only numeric fast gates already exclude bitwise operators and remain unchanged. Frozen PB/unistore admission for these families is absent; no new signatures were admitted for credit. No sibling arithmetic or boolean behavior was rewritten.

## Actual validation

From `tikv/`, using `../tools/cargo-tikv`:

- `test --locked -p tidb_query_expr --lib local:: -- --test-threads=1`:190 passed/1 ignored/467 filtered.
- `test --locked -p tidb_query_expr --lib test_evaluated_int2_rejects_kernel_identity_and_wrong_kind -- --test-threads=1`:1 passed/657 filtered.

From `tidb/rust/`, using `../../tools/cargo-tidb`:

- `test --locked -p tidb-expr --lib bit_dispatch_ -- --test-threads=1`:3 passed/1477 filtered.
- `test --locked -p tidb-session --lib tests_core::lifecycle::evaluated_ascii_ -- --test-threads=1`:23 passed/2078 filtered.
- `test --locked -p tidb-expr --lib -- --test-threads=1`:1382 passed/4 unchanged failures/94 ignored,1480 discovered, exit101. Complete four failure blocks equal09 after only thread-ID normalization. The existing vectorized_builtin_op_func failure is a duration fixture FSP panic, not a newly failing bitwise assertion.

No compile/new-test failure or expected-value modification. Backend checks include high-bit values, nullable wrappers, counts0/1/63/64/65/2^32/-1, wrong-shape preflight and reuse. Native tests cover conversion/diagnostic order, Real NaN NULL-demand behavior, Decimal callbacks, typed/vector routes and same-root refusal. One-slot stored-column SQL asserts unsigned/signed metadata and full boundary Datums. Sixteen direct zero-slot SQL calls include NULL and non-NULL operands; no outer migrated function hides bypass.

## Review and not verified

Parent source review checked ops.rs, integer_coerce.rs and real_coerce.rs deletion/delegation diff. Architecture-index/maintenance-guide updates describe the actual entrypoints and bit-pattern transport, with no new policy, RPC/read-pool or Go/Bazel change. Full workspace/make lint, complete business-wrapper operation scopes, release performance, allocator remeasurement, physical peak/OOM guarantees and final acceptance remain open. These checks are not claimed by old allocation receipts or targeted unit tests.
