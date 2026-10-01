# HOUR / MINUTE / SECOND checkpoint

Checkpoint **hms-three-33**, following `temporal-fields-four-32`: functional **103→106/245**, final acceptance **0/245**. Six private kernels cover three families, not six credits. Exact commands, redirects and ten whole-log hashes: [`../logs/hms-summary.txt`](../logs/hms-summary.txt).

## Shared implementations, distinct policies

Text3 use existing nullable Bytes→Int and the original `coerce_str`, including Duration Display/FSP, not an ETDuration cast. `Time::parse_native_hms` owns real text parsing; NULL/parse failure yields NULL, invalid direct-C UTF-8 is `Other` transport. Nanos3 use existing nullable Int→Int with actual signed nanoseconds and const `unsigned_abs` projections shared by native and wire Duration getters: no constructor, rounding, FSP normalization or text clamp. No new argument/result kind, metadata layout, module, driver, context getter, NoArgs case or PB admission.

SQL text `900:30:15` becomes838/59/59; invalid minute60 is NULL; bare `2024-01-15` becomes0/20/24. Legacy nanos over838h remain unclamped, including negative values, subsecond truncation and i64::MIN. Shared date-prefix/split/pivot/leap/month-length helpers retain the native u32-year domain and parser allocation structure; invalid month length stays31 on wire versus0 in native policy. MICROSECOND/to_secs/FSP/Display/constructors remain separate and unchanged, not a whole-Type migration.

**Priority boundary:** malformed text's quiet NULL is now computed inside the worker, so zero slots refuse before that parse outcome. Original arity/strict-UTF-8/coercion preparation remains first. PB actually observed NULL preserves no prefix coercion/no suffix read; legacy Option NULL still comes through original `eval_duration` and actual nanos with the real context. Default one-shot compatibility remains; production request-root injection is not established here.

## Ten current Rust receipts

**8 green + 2 currently non-green full suites**. No Rust compile failure, test rerun, new RED run or old-expected change. Prior index RED→GREEN is historical; its regression passes in this batch's full unistore run. Writer globbed, grepped/read and hashed all ten; execution/exits/source proofs are parent-owned. TiKV duration includes six bench smoke cases run as ordinary tests (0 measured), not performance measurements.

| `hms-` suffix | Passed / ignored / failed; filtered | Test / compile seconds; exit |
| --- | --- | --- |
| `tikv-time.log` | 43 / 0 / 0; 361 | 0.01 / 2.11; 0 |
| `tikv-duration.log` | 21 / 0 / 0; 383 | 0.00 / 0.12; 0 |
| `native-duration.log` | 24 / 0 / 0; 414 | 0.00 / 2.52; 0 |
| `tikv-local.log` | 255 / 1 old / 0; 486 (256 discovered) | 0.19 / 8.93; 0 |
| `tikv-kernels.log` | 53 / 0 / 0; 689 | 0.01 / 0.14; 0 |
| **`unistore-full.log`** | 189 / 13 / **1**; 0 (203 discovered) | 2.97 / 11.29; **101** |
| `sql.log` | 69 / 0 / 0; 2078 | 0.96 / 28.35; 0 |
| `dispatch.log` | 3 / 0 / 0; 1546 | 0.00 / 10.18; 0 |
| `native-source.log` | 21 / 0 / 0; 1528 | 0.02 / 0.14; 0 |
| **`expr-full.log`** | 1451 / 94 / **4**; 0 (1549 discovered) | 10.76 / 4.71; **101** |

Native datatype used broad filter `duration`, not `duration::`, including converter coverage. Only the first three receipts use unsuppressed tee; the other seven append `>/dev/null` after tee while retaining complete logs. SQL covers six rows × three projections plus one numeric/TIME six-column query; all nine zero-slot calls, including bad text, refuse with PoolResource1105 and empty warnings. Dispatcher3 covers source text/Display, AST/typed/PB demand and preparation-versus-admission; representative, not exhaustive.

Parent's seven moved calendar bodies compare equal **after specified normalization**, not byte-identically. Two static scripts first failed on an unhandled formatter tuple comma and the wrong assumed cfg(test) module name (`tests` versus `clock_source_tests`); correcting scripts changed no code. First final fmt check also found two batch.rs returns needing wrapping; parent reformatted that file and rechecked all **17 sources (TiKV8/native9)**, both locks and both diffs successfully. Final expr-full compiled afterward; not all checks passed first attempt.

Exact byte-preservation scope: five whole native files time_fn/tests.rs, tests/datetime.rs, arg_eval_type.rs, coerce.rs, time_fn/mod.rs; calendar's old clock_source_tests block onward; native Duration from pub const microsecond onward. Normalized seven-body proof permits Self qualification, whitespace/formatter tuple commas and the lossless const u32→i64 cast; it does not establish whole-calendar byte identity or complete Time/Duration equivalence.

Both full runs remain non-green. Unistore's complete failure section matches the temporal checkpoint after thread-ID normalization (e285… hash in summary). Expr's thread-only hash is **80be9bda…**, not prior **0930217d…**: a new import shifts the same duration panic211:55→212:55. Parent confirmed the identical FSP panic line and complete section equality only after additionally replacing that one exact address; no bulk line-number erasure. Full hashes and all five retained failure names are in the summary.

Parser allocation structure, `Finished` times and bench smoke cases are not allocation-peak/OOM, zero-copy or performance evidence. No whole Time/Duration/Date codec/Go-package, deep/wide validation, release, make lint, full-workspace or wider-guard completion claim. Only these two docs were written after source freeze: **106/245**, final **0/245**, not PR readiness.
