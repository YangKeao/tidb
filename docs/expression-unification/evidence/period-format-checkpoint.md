# PERIOD_ADD / PERIOD_DIFF / GET_FORMAT checkpoint

Checkpoint **period-format-three-35**, following `month-seconds-two-34`: functional **108→111/245**, final acceptance **0/245**. Four private operations cover three families; GetFormatNullNative earns no separate credit. Exact commands and eight whole-log hashes: [`../logs/period-format-summary.txt`](../logs/period-format-summary.txt).

## Shared implementation and demand boundaries

Native periods take two real nullable Ints; validation occurs only after both are Some. ADD retains forward-as-i64 wrapping addition then inverse-as-u64/cast-to-i64; DIFF retains u64 wrapping subtraction then i64 conversion. Shared Time helpers keep separate native wrapping and ordinary wire arithmetic policies over private zero/split/pivot logic. No chrono/range narrowing or arithmetic repair; old wire period bodies, i32 narrowing and zero handling stay unchanged. Frontend arity/coercion ordering remains original; admission now precedes worker-side period validation and its1210 outcome.

EvaluateError adds only PeriodAddIncorrectArguments/PeriodDiffIncorrectArguments, exact messages `Incorrect arguments to period_add` and `Incorrect arguments to period_diff`, code1210/EVAL. Private wrappers produce the matching cause only on actual invalid input. Receipt certification requires **exact operation + exact typed variant + witness1**, not code/message matching or host prediction; only that receipt restores native IncorrectArguments. Shape/Resource/witness-shape errors do not become SQL1210. Existing Custom/Caused and conversions retain their behavior.

GET_FORMAT uses two real nullable raw Bytes, exact type case and ASCII-insensitive locale lookup; when both are Some, unknown/non-UTF8 bytes yield Some(empty), not a UTF-8 error; either None stays NULL. Results are ordinary owned Bytes. The single-Bytes NULL witness accepts only an actually observed None and rejects Some, without inventing a second NULL or coercing the unneeded right argument. Outer expression evaluation/casts remain unchanged; the AST location child is still evaluated once. Scalar eval_string versus AST strict coerce_str remains frontend-owned. No new role/result kind/metadata layout/context getter or PB admission.

## Eight actual tests and eight Cargo attempts

**7 green test runs + 1 current baseline non-green full run**. No Cargo launch failure/retry, Rust compile failure or new product RED; this does **not** mean every command/check passed first time. Writer globbed, grepped/read and hashed all eight logs; execution/exits/source proofs are parent-owned. All tee output is retained with terminal output suppressed; `Finished` times are not benchmarks.

| `period-format-` suffix | Passed / ignored / failed; filtered | Test / compile seconds; exit |
| --- | --- | --- |
| `tikv-time.log` | 45 / 0 / 0; 361 | 0.01 / 2.19; 0 |
| `tikv-local.log` | 257 / 1 old / 0; 489 (258 discovered) | 0.19 / 9.42; 0 |
| `tikv-kernels.log` | 56 / 0 / 0; 691 | 0.01 / 0.12; 0 |
| `sql.log` | 73 / 0 / 0; 2078 | 1.14 / 22.20; 0 |
| `dispatch.log` | 3 / 0 / 0; 1551 | 0.00 / 12.96; 0 |
| `native-period.log` | 2 / 0 / 0; 1552 | 0.00 / 0.12; 0 |
| `native-table.log` | 1 / 0 / 0; 1553 | 0.00 / 0.12; 0 |
| **`expr-full.log`** | 1456 / 94 / **4**; 0 (1554 discovered) | 10.65 / 0.12; **101** |

The old GET_FORMAT table gate is **cfg(test) direct datatype compatibility**, not a C4 execution test. Actual four-operation paths have CPP local/kernel, dispatcher3 and SQL evidence. Dispatcher3 does not cover PB/legacy. Unistore was not rerun; PB/cop sources were unchanged, so older unistore runs are historical rather than current receipts.

E appends two tests: five normal rows × three projections, then two separately tested invalid-period calls on row6 with adequate resources, both1210 with exact messages. All nine zero-slot calls instead return Resource1105/HY000/eval-origin with empty warnings. GetFormatNullNative(Some) produces a real rejection without a SQL receipt; it is not a fourth SQL family.

**One final formatting check really failed:** pinned rustfmt --check exited1 on a long PeriodDiffNative ClosedPrivate return in batch.rs, interrupting the13-source script. This occurred **after the full expr run**. Parent changed only that return's whitespace and reran all13 checks: TiKV8/native5 formatting, both locks and both diffs passed. The eight Cargo/test receipts were not rerun or changed. Earlier targeted formatter invocations succeeded; neither zero formatter failures nor static/all-command first-pass is claimed.

Preservation proofs are scoped: A checked old wire bodies and common error branches/conversions against HEAD after removing authorized additions; parent freshly rechecked both complete wire period bodies byte-for-byte after final fmt and byte-compared whole native func, arg_eval_type, pb_builtin, cophandler and time_fn/tests. The complete old get_format_bytes brace body equals new Time::get_format_native after whitespace normalization only, not byte-identically. No whole-calendar/whole-codec identity claim. This round adds no production panic/profile change; retained prior panic tests pass in the current filtered/full test profiles only. Release/profile gates and panic-test adaptation remain pending.

The complete full-expr failure section matches `month-seconds-expr-full.log` after **only panic-heading thread-ID normalization**, SHA256 `80be9bda05e5bf630e9c246ca9523d23f82c122eaaeec6e220b1b00cec436615`. All four old failures remain, including the EXP FloatOverflow assertion. Duration remains at212:55; no position mapping or older093 adjustment. This is a failure-section hash, not a whole-log hash or green full suite.

Only these two docs were written under this authorization. No complete temporal codec/Go-package, M6/production-root integration, release, deep/wide guard, allocation-peak/OOM, zero-copy/performance, make lint or full-workspace completion claim. **111/245 functional, final0/245; not PR readiness.**
