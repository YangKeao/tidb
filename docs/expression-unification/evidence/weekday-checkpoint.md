# DAYOFWEEK / WEEKDAY / DAYOFYEAR / DAYNAME checkpoint

Checkpoint **weekday-four-36**, following `period-format-three-35`: functional **111→115/245**, final acceptance **0/245**. Credit only these four families, not DATE_FORMAT or date construction. Exact commands and ten whole-log hashes: [`../logs/weekday-summary.txt`](../logs/weekday-summary.txt).

## Shared civil-day and weekday policy

Four private nullable Bytes kernels return three signed Ints and ordinary owned DAYNAME Bytes. They retain strict decode_native_time_text (bad UTF-8 is Other transport, not a SQL-date cause), then full parse_native_date_ymd and compute only on Some. NULL/invalid-date NULL still executes in the worker; no host answer prediction. Original frontend ETDatetime cast, arity/UTF-8/coercion comes first; admission precedes backend parsing/field output. No new role/result kind/metadata layout/cause/driver/context getter or PB admission.

Time owns native_days_from_civil with fixed1970-01-01 epoch, native_weekday_sunday_index=(civil+4).rem_euclid(7), native_day_of_year=civil(date)-civil(Jan1)+1 and full-name lookup. DAYOFWEEK=index+1, WEEKDAY=(index+6)%7; the latter equals old(civil+3).rem_euclid(7) **within the valid parsed u32-year domain**, not arbitrary i64 inputs. DAYNAME retains to_string/into_bytes. Legal year0 stays valid: 0000-01-01 gives7/5/1/Saturday, not NULL; full-u32-year policy remains, without chrono/get_daynr/CoreTime narrowing or wire zero-date warnings. Fixed expected values come from pinned fixtures/old formulas, never the new provider.

Full weekday names now share one getter at four consumer sites; abbreviations and distinct zero/normalization/wire policies remain unchanged. **Round35's four-site month-table dedup omitted calendar DATE_FORMAT's residual MONTHS**; this round removes that residual table. It is wrong to infer repository-wide month-table uniqueness from Round35. Neither cleanup nor DATE_FORMAT's shared primitive earns extra credit.

## Ten actual test runs and ten Cargo attempts

**9 green test runs + 1 current baseline non-green full run**. No formatting/static/launch/compile failure, retry or new product RED this round; do not import Round36's formatter correction. Writer globbed, grepped/read and hashed all ten logs; execution/exits/source proofs are parent-owned. All tee output is retained with terminal output suppressed; `Finished` times are not benchmarks.

| `weekday-` suffix | Passed / ignored / failed; filtered | Test / compile seconds; exit |
| --- | --- | --- |
| `tikv-time.log` | 46 / 0 / 0; 361 | 0.01 / 2.04; 0 |
| `native-core-time.log` | 15 / 0 / 0; 423 | 0.00 / 2.66; 0 |
| `tikv-local.log` | 258 / 1 old / 0; 491 (259 discovered) | 0.19 / 8.86; 0 |
| `tikv-kernels.log` | 58 / 0 / 0; 692 | 0.01 / 0.13; 0 |
| `sql.log` | 75 / 0 / 0; 2078 | 1.16 / 18.23; 0 |
| `dispatch.log` | 2 / 0 / 0; 1554 | 0.00 / 12.12; 0 |
| `native-calendar.log` | 1 / 0 / 0; 1555 | 0.00 / 0.13; 0 |
| `native-dayname.log` | 1 / 0 / 0; 1555 | 0.00 / 0.12; 0 |
| `native-format.log` | 2 / 0 / 0; 1554 | 0.00 / 0.14; 0 |
| **`expr-full.log`** | 1458 / 94 / **4**; 0 (1556 discovered) | 10.39 / 0.12; **101** |

E covers six rows × four projections plus a cast-year0 witness that actually observes Time.year0. All12 zero-slot calls return Resource1105: the four bad-text calls additionally retain their original1292 warnings; the other eight have empty warnings. Thus neither all warnings empty nor zero-year→NULL is claimed. D2 has no PB/legacy scope. Unistore was not rerun because PB/cop sources were unchanged; older unistore runs are historical only.

Parent's complete old native civil brace body equals the new Time helper after **whitespace normalization only**. Nine original bodies remain byte-identical: CoreTime weekday/Sunday-index/abbreviation, Time weekday/ordinal/get_daynr, CPP extension name_abbr/calc_day_number and native inverse-civil. Six whole native func/lib/arg_eval_type/pb_builtin/cophandler/time_fn/tests match HEAD; A's old impl_time bodies/tests/decoder also survive its scoped stripping proof. Calendar MONTHS/WEEKDAYS literal sequences match public MONTH_NAMES/private WEEKDAY_NAMES. The stale only-DATEDIFF/arbitrary-epoch comment was corrected to the existing fixed1970 epoch, not an algorithm change. No whole-codec/tree identity claim.

The complete full-expr failure section matches `period-format-expr-full.log` after **only panic-heading thread-ID normalization**, SHA256 `80be9bda05e5bf630e9c246ca9523d23f82c122eaaeec6e220b1b00cec436615`. All four old failures remain, including the EXP FloatOverflow assertion. Duration remains at212:55; no position mapping or older093 adjustment. This is a failure-section hash, not a whole-log hash or green full suite.

Parent's final **14 sources (TiKV8/native6)** passed pinned formatting, both lock checks and both diff checks on the first check this round. No old expected was changed. Current test-profile receipts do not establish release/profile/panic-test adaptation; those gaps remain. No complete temporal codec/Go-package, M6/production-root integration, deep/wide guard, allocation-peak/OOM, zero-copy/performance, make lint or full-workspace completion claim.

Only these two docs were written under this authorization: **115/245 functional, final0/245; not PR readiness.**
