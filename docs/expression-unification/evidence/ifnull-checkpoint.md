# IFNULL: owned demand and lossless selection

Checkpoint **ifnull-83**, after **request-scope-82**. Functional222/245, strict0, remaining23. Only frozen family `ifnull` is newly credited; the previous221 objects remain byte-identical. This is not M0–M6 completion or PR readiness.

## One selection implementation

TiKV `components/tidb_query_expr/src/native_if_null.rs` owns `native_if_null_choose_first<T>(Option<T>)`. It returns Done(actual first) or NeedSecond without requiring a clone or a supplied right-hand operand. Three existing `impl_control.rs` wire IFNULL workers mechanically delegate, retaining their original typed clone/JSON ownership/byte-copy semantics. Their bodies changed; their policies and old tests did not.

| Fixed worker | Actual inputs | Computed output |
| --- | --- | --- |
| IfNullHeadNative | Values, one Bytes slot, one call; actual first native identity or SQL absence | OwnBytes **always present**: `[0]` NeedSecond, or `[1]` plus the entire non-NULL identity frame Done |
| IfNullFinishNative | Values, two Bytes slots, one call; original whole `Some([0])` report plus actual nullable second identity | OwnBytes actual second identity or SQL NULL |

Validators close argument shape, role, call count, exact report and structural identity at facade and direct-ready boundaries. A Done report, missing report or trailing report bytes cannot masquerade as NeedSecond. Head's SQL NULL is a real invocation with a present demand report, not host NULL propagation.

No extra math/UTF8/date/JSON screen: the existing codec carries raw Decimal coefficient/sign/scale/storage/shape, Float32's entire f64 payload, NaN/-0 bits, raw Time/FSP, invalid UTF8/JSON, Raw, sentinels and vector payloads. No new carrier or metadata policy is introduced.

## Native deletion and preserved entrypoints

`rust/crates/tidb-expr/src/tikv/if_null.rs` prepares actual first input inside the existing router, decodes only the SDK's computed report, and projects Done from its returned frame. NeedSecond retains the entire report, evaluates the second child once under selected columns, and passes both to Finish. It never chooses the answer on the host and merely invokes IDENTITY. Only `eval_if_null_in` is re-exported crate-internally.

Four native selection branches are removed:

- `func.rs`: AST preserves original registry/arity gate; eager-value helper preserves both original clones before admission and does not serialize the unselected right value.
- `scalar_function.rs`: typed lazy row evaluation keeps its two-argument gate and original outer return conversion.
- `scalar_function/pb_builtin.rs`: only demanded slots are read. Missing first always fails; missing second succeeds when not demanded, and fails after a NULL head when demanded. Extra suffixes stay unread. Existing unsigned reinterpretation, return-family conversion, String and temporal FSP behavior remain outside selection.

Two non-runtime selectors also share the same pure SDK chooser: `expr_util/fold.rs::if_null_fold_handler` and `expression.rs::try_fold_nullified_function`. Actual Datum NULL maps only to nullable representation; SDK Done/NeedSecond chooses the continuation. Deferred values, second-argument collation inheritance and the proof helper's original variadic/malformed-call domain remain unchanged. These APIs have no fallible C4 boundary, so no infrastructure error is swallowed to manufacture a folded answer. COALESCE and IF are not newly credited.

Legacy `SimpleSig` has no IFNULL variants. The existing seven PB signatures—Int/Real/Decimal/String/Time/Duration/Json—already enter Shared. R85's borrowing seam keeps original request settings/warnings and parent capability. `cophandler.rs` changes only by an appended test; there is no new admission, legacy selector or owner API. Vector/filter evaluation already reaches the typed row path; no new fastpath is invented.

## Scope and resource boundary

First uses the **original context** within existing preparation. With an explicit capability the existing guard applies; without one, preparation still precedes creating the one-shot owner. This round deliberately does not claim that the first child already has a scope that does not yet exist.

The head invocation finishes before its callback evaluates a demanded second child. Head → second → Finish use the exact selected scope, including when the child replaces the parked worker. No worker borrow crosses child evaluation. Existing SQL/error precedence and scope poisoning remain intact.

Common ready preflight, including direct-ready calls, checks reply bounds: head `1 + first.len()` or1 for NULL; finish `second.len()` or0 for NULL. It accounts for actual input capacities, including the retained report, and retains output postflight. These are retained/request allowances, not physical heap, cross-stage native packing or allocation-peak bounds. Existing eager clones still precede head admission. Older generic Values reply-floor gaps are not repaired.

## Evidence

[Exact commands, counts, times and raw-log SHA256](../logs/ifnull-summary.txt); [machine manifest](../checkpoint.json).

All8new tests pass on first matching execution: one SDK codec/choice test, two local role/domain/budget tests, one bridge identity/demand/scope test, two native frontend/PB tests, one legacy Shared test and one SQL test. No fixture recording, expectation correction or provider-derived oracle.

SQL has54SELECT probes. Two real tables each contain two distinct stored rows; one has non-NULL first values, the other NULL first values. Six cases (integer, mixed-scale Decimal, text, mixed-FSP datetime, JSON, both-NULL typed integer) run under vector0/1 and slots1/0:48direct probes, including24zero-slot root refusals without WHERE/CAST/function-child substitution. Four positive-pool lazy invalid-regexp checks preserve the current Unsupported1105 policy; two filters cover row selection. Metadata and selected raw scale/FSP are pinned from source.

Eleven locked nonzero launches:8green,1known focused RED,2known full RED. The `ifnull` filter passes both new frontend tests but still selects the old reverse literal/column catalog assertion (6pass/1fail); it is not reported green. Full expression1585/4old/94ignored and unistore215/1old/13ignored have failure sections byte-identical to R85 after only numeric panic thread IDs are normalized. Digests remain `411274feba11e202735df5a8056c72babbc8cf9df2eab3258c670e9289a1ee95` and `2f19c9ad5338a86c48b895e3923c3e39454117e3410d47df5aa3fb4254ed7ce1`.

The11native/8CPP Rust files contain two new modules and eight appended tests. A source audit compares121CPP/515native original test bodies byte-for-byte. H's independent production review found no blocking demand/domain/scope/deletion issue. Pinned rustfmt and diff checks pass. One new dispatch return required a second formatting pass; a slow source-audit helper timed out and was replaced with a completed linear-time audit. Neither was a test interruption. Final bridge revalidation covers narrowing unused internal re-exports; no broad root API was added.

Agent-doc review: `docs/agents/architecture-index.md` only adds verified source-path/boundary descriptions; root policy, validation requirements and existing links remain unchanged. No Go/Bazel/Cargo/lock/generated changes. The pre-existing untracked client-differential BUILD file stays excluded.

## Still open

[Remaining acceptance](remaining-acceptance.md):9core,8ordinary pending,6complex exception candidates. Next IF must retain ordinary `truthy_of` versus PB warning-aware string/bytes numeric coercion; do not substitute TiKV wire rounding policy. Continue CASE/COALESCE/NULLIF, CAST/M2, IN/extrema/INTERVAL, real request-root/live-DAG ownership and final deletion/exception receipts.

Known catalog and other full-suite failures, raw INTDIV, zero-date CAST text, PlanScope date_modes, JSON_KEYS arity/aggregate and older Values budget gaps remain. Whole workspace/lint/dev/bazel_prepare/release, exhaustive differential, TiFlash/FIPS, performance/physical OOM/allocator faults/zero-copy/dual-tzdata and complete Go-package transcreation are not claimed.
