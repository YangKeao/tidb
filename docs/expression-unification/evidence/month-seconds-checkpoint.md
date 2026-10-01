# MONTHNAME / TIME_TO_SEC checkpoint

Checkpoint **month-seconds-two-34**, following `hms-three-33`: functional **106→108/245**, final acceptance **0/245**. Credit only these two families. Exact commands, launch-error diagnostic and ten whole-log hashes: [`../logs/month-seconds-summary.txt`](../logs/month-seconds-summary.txt).

## Shared implementation and preserved boundaries

Two private nullable Bytes kernels return ordinary owned Bytes or signed Int, without a new role/result kind/metadata layout/driver/context getter. MONTHNAME actually validates the full calendar via `Time::parse_native_date_ymd`, then uses `month_name_from_month` and the old to_string/into_bytes allocation path. Wire month_name retains zero-date context/warnings and bad-index panic; only its lookup is shared. Wire time_to_sec and old HMS values/UTF-8 diagnostics remain unchanged.

TIME_TO_SEC runs `Time::parse_native_duration_text` on real text; native duration becomes a thin delegate also serving TIME_FORMAT. Preserve junk/no-digits→0, signed colon hours (`--1:00`→3600, `--900:00`→3240000), positive h>838→NULL rather than clamp, up to six unvalidated fraction characters, ASCII-space rsplit date suffix, original allocation and unchecked multiply/add. No strict Duration constructor or nanosecond narrowing substitutes for this domain. Existing ETDatetime MONTHNAME preparation, arity/UTF-8/coercion and TIME_TO_SEC Duration Display remain frontend-owned; no ETDuration cast is added. Parsed NULL/0/name/seconds now follow admission, so zero-slot Resource can preempt them.

**Overflow is not repaired:** current test profiles retain the original unwind, not Infrastructure or SQL conversion; production adds no catch. The datatype catch test and literal-payload dispatcher test pass. Unwind discards the poisoned scope's worker; a **new scope in the same execution** rebuilds (factory count2), producing-7205. This does not make the poisoned old scope reusable. TiDB dependency builds use TiDB's root profile, not nested TiKV dev overflow=false; TiKV test explicitly enables overflow checks. No checked arithmetic/profile/flag/cfg-debug-assertions workaround was added. Release/profile gates and panic-test adaptation remain pending.

## Nine actual test runs, ten Cargo attempts

**8 green test runs + 1 current known non-green full run**, plus **1 failed Cargo launch before any compilation/test**. The wrong-CWD SQL launch was retried from the correct directory; neither zero launch retries nor all launches first-pass is claimed. There is no Rust compile failure or new product RED. Writer globbed, grepped/read and hashed all ten artifacts; execution/exits/source proofs are parent-owned. All tee commands suppress terminal output only; `Finished` times are not benchmarks.

| `month-seconds-` test-log suffix | Passed / ignored / failed; filtered | Test / compile seconds; exit |
| --- | --- | --- |
| `tikv-time.log` | 44 / 0 / 0; 361 | 0.01 / 2.01; 0 |
| `native-mysql-time.log` | 33 / 0 / 0; 405 | 0.00 / 2.18; 0 |
| `native-str-date.log` | 7 / 0 / 0; 431 | 0.00 / 0.09; 0 |
| `tikv-local.log` | 256 / 1 old / 0; 487 (257 discovered) | 0.19 / 8.73; 0 |
| `tikv-kernels.log` | 54 / 0 / 0; 690 | 0.01 / 0.13; 0 |
| `sql.log` | 71 / 0 / 0; 2078 | 1.09 / 19.76; 0 |
| `dispatch.log` | 2 / 0 / 0; 1549 | 0.00 / 12.56; 0 |
| `native-source.log` | 21 / 0 / 0; 1530 | 0.02 / 0.13; 0 |
| **`expr-full.log`** | 1453 / 94 / **4**; 0 (1551 discovered) | 10.46 / 0.12; **101** |

The additional `sql-cwd-error.log` retains the exact missing-Cargo.toml diagnostic from CWD `/home/agent/tidb/expression-unification/tidb`, exit101: no compilation or tests started. It was preserved before the successful retry from `tidb/rust` replaced `sql.log`. Formatting the two native datatype files at the repository root succeeded; that is separate from this Cargo launch error.

The existing full MONTH_NAMES table is the shared owner after removing four duplicate full tables (TiKV date_format_parser, native mysql_time, str_to_date, SQL). Abbreviations and lowercase matchers intentionally remain separate; not all month words or DATE_FORMAT/STR_TO_DATE are migrated. Parent's normalized duration-body and two-datatype proofs are not whole-source byte identity; the preserved single_date/other time_fn algorithms, TIME_FORMAT and old tests are separately byte-checked.

SQL covers six rows × two projections plus a numeric query; all six zero-slot calls return Resource1105. MONTHNAME(badDATE) additionally retains its prior1292 warning; the others have none. Dispatcher2 covers these native adapters and real overflow/unwind, **not PB/legacy**. Unistore was not rerun because PB/cop/legacy sources were unchanged; last round's unistore result is historical, not a current receipt.

The full expr failure section is byte-identical to `hms-expr-full.log` after **only panic-heading thread-ID normalization**, SHA256 `80be9bda05e5bf630e9c246ca9523d23f82c122eaaeec6e220b1b00cec436615`. All four failures remain, including the old EXP FloatOverflow assertion. Duration remains at212:55: **no location mapping this round**. This is not a whole-log hash or a green full suite.

Parent's final **13 sources (TiKV7/native6)** passed pinned formatting, both lock checks and both diff checks; no formatter/static-script failure this round, only the retained wrong-CWD Cargo launch. No old expected was changed. No whole Time/Date/Duration/Go-package, release, deep/wide guard, allocation-peak/OOM, zero-copy/performance, make lint or full-workspace completion claim. Only these two docs were written: **108/245**, final **0/245**, not PR readiness.
