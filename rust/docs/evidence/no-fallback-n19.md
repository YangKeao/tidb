# N19: RAND, DDL owner and lazy AES in the shared frame

This is a local checkpoint within task #4. Generic Admission fallback, optional
and protobuf metadata, public AST/scalar traversal, filter/Expand/unistore
entrypoints, full-server validation and final performance remain open. No push or
whole Go package transcreation is claimed. Initial source admission here requires
actual static result/argument types and non-protobuf origins.

The ordinary source now uses the existing SDK `EagerValuesNative` HostFrame for
RAND, TIDB_IS_DDL_OWNER, AES_ENCRYPT and AES_DECRYPT. No expression callback,
replacement tree, nested compiled expression or synthesized session is retained
by the new host metadata. The source owner retains the original program Arc.

RAND retains the genuine original ScalarFunction occurrence address and the old
structural Constant classification, including parameters and deferred Constants.
It demands all arguments before a wrong-arity error. Literal/parameter/deferred
seeded calls share their original generator during public/source alternation;
column arguments still initialize a generator per call. The existing value-level
RAND implementation and seed converter are unchanged.

DDL owner validates arity before demanding any child, then calls the actual
optional provider. Its original body is shared with the public scalar entry.
Niladic virtual-row tests cover both real context services and their original
unavailable/error/panic behavior without columns.

AES uses one shared AesPreparation value/context state machine in the public
wrapper and source. The discarded stored Constant header precedes the selected
actual Ready cache, mode getter, arity, input, password and optional IV. Genuine
NULL short-circuits later demands. ECB ignores its third expression and emits
warning 1618 only after non-NULL input/password. Mode selection, SQL byte
conversion, key derivation and cipher worker bodies retain their original owners.
The source carries only authenticated actual values and private continuations.

The exact pre-extraction AES wrapper remains under cfg(test), together with old
scalar controls. Actual input leases hold no RefCell borrow across callbacks.
After warning callbacks, all earlier/current capabilities are authenticated
before entering the cipher, and again before publication. The actual cache
selected before preparation is reused without another context getter. A private
borrowed unwind guard crosses broker suspensions, clears/poisons that cache on
panic and leaves ordinary errors/cancellation reusable. Its task-slot extent is
accounted by the existing size checks; no capacity limit is raised.

## Original evidence and regressions

The original production files were archived before mutation in the owning agent
workspace, `evidence/no-fallback/n19-before/SHA256SUMS`. The first two captures and independent
reruns preceded public-wrapper retirement; the third compares the preserved
original controls after sharing:

| Observation | Count / checksum |
| --- | --- |
| Original contexts, modes, argument demand, output prefixes and selections | 39312 / 139A40582F6B294D |
| 149 actual Datums, 11 return types, three AES positions and 12 modes | 727716 / 092E0C47BF46492F |
| Preserved old scalar/AES controls versus shared value phases | 121286 / 286849C89B032D6E |

Twenty actual source cases initially refused Admission and now execute with zero
fallback/refusal. Further tests cover true occurrence-pointer identity,
public/source alternation, all valid owner return types, 2051/2057-row selections,
reverse/duplicate/empty selections, failures after callback 1026, nested
BENCHMARK Fresh/NAME_CONST, malformed/foreign/stale/wire-NULL capabilities, wrong
node/role/physical/occurrence bindings, 128 pending cancellations, stale replies
and cancellation, and actual NULL recovery. Large ignored IVs and wrong-arity
RAND values retain their actual 2 MiB payloads without extra kernel-side cloning.

Two callback regressions were independently demonstrated before fixing them:
mode-callback panic left one cached worker rather than clearing the original
lane; a warning callback resetting the value store allowed a cipher worker to
prepare before stale input was rejected. Both now pass, including subsequent
cache demand and cancellation recovery.

A demanded 2 MiB vector password is different from an ignored argument: the
existing shared formatter charges 512 work units per component, exceeding the
unchanged finite source allowance. That test explicitly expects ResourceLimit,
no cipher execution or fallback, and recovery only after the caller supplies a
real NULL. It does not replace a refusal with SQL NULL or increase a quota.

The prior PI admission test expected RAND rejection. Its PI, selection and
resource assertions remain; RAND now proves explicit stateful admission and its
original missing-session error with zero fallback. This is updated capability
coverage, not an inherited failure. New Projection coverage uses the supported
getvar_real entry; an original unsupported deferred binary-string conversion is
retained as an expected caller error, alongside the missing DDL owner provider.

## Final validation

From the TiDB root, with `RUSTFLAGS='' CARGO_BUILD_JOBS=8`:

- `../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib`: **1997 passed, exact original 3 failures, 95 ignored**, all **456** batch tests pass; aggregate **346** warnings unchanged (`n19-front-final2.log`).
- Same command with `-p tidb-executor --lib projection::tests`: **27 passed** (`n19-projection-final.log`). The new mixed and ordered suites cover real statement contexts, seeded/column RAND, AES round trips, NULL/deferred/nested calls, three encryption modes, both flags, serial/two-worker execution, warnings and variables. Missing-provider and original deferred-conversion errors retain zero fallback.
- TiDB-root `make lint`: passed (`n19-lint-final.log`).
- Both pinned rustfmt versions, 2026-01-30 and 2026-08-22: all 13 changed Rust files pass; `git diff --check` passes.
- `n19-final-check.py`: original AES control, RAND/seed/conversion/cipher/closed-worker bodies, actual metadata, shared-frame boundaries and unchanged SDK checked before and after the local DCO commit.

The exact original failures remain
`ifnull_string_column_literal_uses_go_signature_and_column_collation`,
`builtin_info_json_math_source::exp`, and
`builtin_math_misc_op_source::vectorized_builtin_op_func`; none is filtered.
SDK ad73b11 is unchanged and clean. N16's fresh SDK baseline remains applicable:
1148 passed, exact original one failure, one ignored, 17 warnings; scoped clippy
171 total/89 expression warnings. Clock defaults and fixtures are unchanged.
No full-server or performance conclusion follows from these tests. Continue the
remaining global entry and fallback closure after this local checkpoint.
