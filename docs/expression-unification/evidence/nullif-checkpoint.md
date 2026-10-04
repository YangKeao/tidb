# NULLIF: actual comparison, actual left, shared selection

Checkpoint **nullif-87**, after **case-86**. Functional226/245, strict0, remaining19. Only frozen `nullif` is added; all225 earlier family objects stay byte-identical. Not M0–M6 completion or PR readiness.

## Delete the native selection without changing demand

The single `func.rs` values selector serves existing AST/direct-typed/eager paths. It retains both operand clones and exactly one `Eq(a.clone(), b, original)` call. AST and typed argument loops stay eager: NULL left does not suppress a failing right, and original extra-argument error order remains. No Go-style IF desugaring that repeats left or skips right is introduced.

`tikv/null_if.rs` runs the comparison callback first inside existing preparation authority, preserving the div-precision getter, coercion warnings and errors. The comparison success domain is already restricted by the shared comparison decoder to NULL/Int0/Int1; the bridge mechanically normalizes that domain, without truthy coercion. Only afterward does it encode the actual left, including equality-true. It submits existing `BytesInt(left, comparison)` and returns only the computed identity.

With no capability, comparison still precedes selector one-shot owner creation. No claim that a not-yet-created owner covers it. With an existing capability, the comparison's worker retires before selection, permitting one-slot reuse. First-argument return metadata, outer conversions and generic fold/proof paths are unchanged.

## One new closed profile; no carrier or driver

`NullIfNative`: Values,2input slots,1call,nullable OwnBytes. `native_if.rs::evaluate_null_if_native` validates the actual left structurally and comparison as None/0/1, then shares `native_if_choose_branch`: equality returns NULL, otherwise actual left. Equality does not allow malformed left transport to bypass validation.

The nullable RPN wrapper copies that borrowed result. Common ready-value preflight, including direct-ready, uses the same helper only to compute exact reply length. It neither caches nor substitutes the SQL result; the real dispatcher still executes. All actual input capacity is charged even for a NULL reply. Existing Bytes row metadata floor and postflight accounting remain, so zero payload is not zero retained storage.

The pre-existing local signed-integer NULLIF keeps its comparison once and mechanically shares the same choice primitive. No new report, carrier, role, codec, general compiler/driver or budget policy is added. Existing Values compile handling suffices; `compile.rs` is unchanged.

## Admission and evidence boundaries

There is no standalone NULLIF `ScalarFuncSig`, legacy SimpleSig or new PB admission. Go encodes IF/Eq, not a new NULLIF wire signature. No fake protobuf refusal test or legacy migration credit is claimed.

The existing SQL route does support `NULLIF(column,column)`: `rewriter.rs` calls `verify_args_by_count`; `builtin_registry.rs` accepts an unregistered name for that count check; then the rewriter directly constructs `ScalarFunction`. Separate registry-based `new_function.rs` FunctionBuilder still rejects NULLIF. Neither path was changed; lack of a registry entry was not treated as proof that SQL lacks admission.

All successful existing equality domains, including NULL, already dispatch Compare C4. Thus SQL zero-slot failure does **not** prove the new NULLIF selector was reached. Direct bridge tests use completed-comparison callbacks without a prior worker to isolate selector refusal. CPP tests prove actual selection dispatch, invalid-input refusal and actual-left capacity charging even when equality returns NULL.

## Core validation and retained failure

[Commands, counts, times and full hashes](../logs/nullif-summary.txt); [manifest](../checkpoint.json).

Six new tests: shared selector1, local closed-domain/budget2, bridge1, AST/typed/eager1, SQL1. All finally pass; five pass on first matching execution. Existing NULLIF comparison rows also pass in the native-root filter.

The SQL test initially failed because its new VARCHAR expectation copied derived-control-string decimal-1 instead of the stored first-column decimal0. Source proof:

- `tidb-executor/src/ddl/column_field_type.rs254–256`: VARCHAR(8) sets flen8;286–289/316 insert and store default decimal.
- `tidb-datatype/src/field_type/mod.rs311`: Varchar/VarString default decimal is0.
- `tidb-expr/src/rewriter/result_type.rs1610`: NULLIF clones arg0's static type.

Only the new expectation `(8,-1)` became `(8,0)`, with a source comment. This was not derived from provider/actual output or fixture recording. No production or old-test change. Initial log `c6a306…` and passing retry `4604da…` remain. Other first-argument Decimal8/1 and Datetime19/0 expectations were checked against source, not broadened into an exhaustive audit.

SQL38SELECT=8stored pairs × vector0/1 × slots1/0=32direct probes (16positive,16comparison-stage refusals), plus4positive eager RHS invalid-regexp checks including NULLlhs, and2positive filters. First-type metadata and raw selected values are retained; no CASE-style merged branch CAST is introduced. Selector-root SQL zero-slot evidence count is explicitly0.

Nine locked, nonzero, single-threaded launches:6green,1new-test-oracle RED,2known old full RED. CPP core1/local339+1ignored; native root2/bridge1/gateway196+1ignored; SQL-final1 pass. Full expression1596/4old/94ignored and unistore218/1old/13ignored remain RED. No compile failure, zero-match or interrupted test.

Entire full-suite failure sections are byte-identical to R89 after normalizing only numeric panic-heading thread IDs: expression `411274feba11e202735df5a8056c72babbc8cf9df2eab3258c670e9289a1ee95`, unistore `2f19c9ad5338a86c48b895e3923c3e39454117e3410d47df5aa3fb4254ed7ce1`.

Source audit:8CPP/6native Rust files, one new native module;130CPP/378native original test bodies byte-identical,3CPP/3native new tests. Pinned formatting/diff checks pass. E independently reviewed actual input validation/capacity, real dispatch, comparison-first order and admission/evidence boundaries without a blocker. Agent-doc review remains descriptive without new policy. No Cargo/lock, Go/Bazel or generated changes; unrelated untracked BUILD excluded.

## Still open

[Remaining acceptance](remaining-acceptance.md):5core,8ordinary pending,6complex candidates—not19approved exceptions. Continue CAST/M2, IN/extrema/INTERVAL, actual request-root/default-NoColumns/live-DAG ownership and final cross-entry/deletion receipts.

Comparison plus selection are separate calls with explicit packing/copy costs. Known catalog/EXP/duration/str_to_date/unistore failures and INTDIV/CAST/mode/JSON/vector/older Values gaps remain. Workspace/lint/dev/bazel_prepare/release, exhaustive differential, TiFlash/FIPS, performance/physical heap/stack/OOM/allocator/zero-copy/dual-tzdata and complete Go-package transcreation are unverified; no PR-readiness claim.
