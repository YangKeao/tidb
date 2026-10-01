# DATE_FORMAT / TIME_FORMAT / LAST_DAY checkpoint

**format-time-three-40**, following `construct-time-four-39`: functional **128→131/245**, final acceptance **0/245**; DATE remains deferred. **16 test-Cargo attempts =13 nonzero-test runs (10 green +1 new test RED +2 current baseline non-green full runs) +2 compile-failure attempts +1 zero-match attempt.** Three failed-target retries (dispatch2/SQL1), launch failures0, metadata commands0; **all_checks_firstpass=false**. Exact commands/results and all16 whole-log SHA256 values: [`../logs/format-time-summary.txt`](../logs/format-time-summary.txt).

Parent scope: **22 Rust sources (TiKV9/native13), zero manifest edits, both locks byte-identical to HEAD**; pinned-format and both repository diff checks had0 failures. Source/gates/proofs are parent-owned; writer directly read/hashed all16 logs and wrote only these two docs. Failed/zero-match logs remain separate and unoverwritten.

**Seven private kernels: six OwnBytes results +one Int NoArgs result.** Text DATE_FORMAT, raw-core DATE_FORMAT, NULL witness, missing-first-child, duration-text probe, text TIME_FORMAT and LAST_DAY. Sole new typed core role requires a present full LE8 raw word plus nullable layout; no Time constructor or raw-bit truncation. PB/legacy time-NULL uses a genuine independent NULL witness without demanding layout. Legacy boolean DATE_FORMAT is instead the first Datum's actual presence, using existing IsNotNull(Some(false)/None) canonical presence representation; missing first child runs true NoArgs→Some0. Neither boolean branch formats nor reads any suffix child.

Shared Time now owns **five views, one scanner/specifier match**, replacing three native formatter loops and sharing week/ordinal/microsecond primitives through thin delegates. Necessary wire sharing retains distinct policies. Text/public raw validation, dangling-percent handling, date-only specifiers, signed/elapsed hours and fractional spelling are not flattened. Some former write! sites now create temporary format! Strings: allocation/performance equivalence is **not established**.

TIME_FORMAT preserves two complete sequential calls: worker probe returns the actual original text/None; only Some then coerces the layout and runs the formatter. Explicit double parse/transport/two leases, or two one-shots without a context capability; no nested driver callback or host parsing. DATE_FORMAT's native text lane still performs both original coercions and falls back to midnight for bad clock text; LAST_DAY keeps strict clock validation plus Unicode-whitespace, not T, separation. SQL typed argument casts still precede these workers: bad clock8034 and nondate1292 are not replaced by untyped-helper oracles.

## Failed-first attempts and corrections (all retained)
- **Dispatch compile exit101, E0271+E0308:** time_format_in passed Option<Bytes>-returning into_bytes to Datum-only evaluate_bytes_in. Parent switched this new call to generic evaluate_args_in+Args::Bytes, preserving demand/business logic; no test ran.
- **Dispatch retry new test RED,2 passed/1 failed:** new test wrongly classified not-a-time as invalid; the unchanged old no-colon/no-leading-digit parser returns Some0, so invalid-UTF8 layout is demanded and errors. Parent used900:00:00 for the invalid-duration row and retained not-a-time as the layout-error case. Parser body is byte-identical to2a8e; this corrects a NEW test from original source, not an old fixture or a provider-generated oracle. The second retry passed3.
- **SQL compile exit101, E0432:** session_tz.rs:41 imported test-only date_format. Parent changed only that newly affected source's import/call (two lines) to date_format_in(..., cols), preserving FROM_UNIXTIME's formatter helper with its real context, not ungating a test provider or crediting the whole family. Retry passed86. Native duration:: separately matched0 tests; duration_tests:: then passed16, but zero-match exit0 is **not green coverage**.

SQL3 additions include16 zero-slot Resource refusals with source-specific warnings, not a blanket empty-warning claim. Legacy2 covers Resource and PoolClosed across bytes/int-presence/real/time consumers and original child demand. Raw year10000/hour24/seven-digit microseconds, raw-Duration24h versus SQL-time behavior, new raw-core-zero %M versus literal/trailing% witnesses, and missing0 are explicitly old-policy-derived new literals, not falsely labeled original fixture rows. Old expected fixtures remain unchanged; the new D test correction above is disclosed.

Other corrections: one parent guide offset-overrun read, one writer guessed time.rs path miss, and one precompile B-unit NULL-witness correction to None identified by D; these are not Cargo launch failures. **Static-proof check failures0.** Round39 parser-all E0061 remains historically unrepaired/unrun here; its isolated auth20 and Round40's indentation-proof failure are not this round's gates/corrections.

## All current logs (Finished/test timing is not a benchmark)
| `format-time-` suffix | Passed / failed / ignored; filtered | Finished / test seconds; exit |
| --- | --- | --- |
| tikv-time.log | 54 / 0 / 0;361 | 3.32 / 0.01;0 |
| tikv-duration.log | 21 / 0 / 0;394 | 0.12 / 0.00;0 |
| native-core.log | 15 / 0 / 0;423 | 3.32 / 0.00;0 |
| native-time.log | 33 / 0 / 0;405 | 0.09 / 0.00;0 |
| **native-duration.log** | **0 matched** / 0 / 0;438 | 0.09 / 0.00;0 (**not coverage**) |
| native-duration-tests.log | 16 / 0 / 0;422 | 0.09 / 0.00;0 |
| tikv-local.log | 269 / 0 / 1 old;502 (270 discovered) | 12.00 / 0.18;0 |
| tikv-kernels.log | 68 / 0 / 0;704 | 0.12 / 0.01;0 |
| **dispatch.log** | compile failure;no tests | unavailable / not run;**101** |
| **dispatch-retry.log** | 2 / **1 new** / 0;1565 | 7.50 / 0.00;**101** |
| dispatch-corrected.log | 3 / 0 / 0;1565 | 3.24 / 0.00;0 |
| **sql.log** | compile failure;no tests | unavailable / not run;**101** |
| sql-retry.log | 86 / 0 / 0;2078 | 20.32 / 1.32;0 |
| legacy.log | 2 / 0 / 0;207 | 8.56 / 0.00;0 |
| **expr-full.log** | 1470 / **4 old** / 94;0 (1568 discovered) | 2.95 / 10.42;**101** |
| **unistore-full.log** | 195 / **1 old** / 13;0 (209 discovered) | 0.13 / 3.00;**101** |

Current expr's complete failure section with **only thread IDs normalized** hashes `25f964887541f45602363f00dc60df4f0af21c74edccc16c38d0804094fb9bff`, **not** the previous hash. Removing the earlier native Duration formatter moved the unchanged to_number body's panic from212:55 to164:55; parent proved that body byte-identical. Normalizing thread IDs and mapping **only that panic-header location164→212** then yields the previous complete-section hash `80be9bda05e5bf630e9c246ca9523d23f82c122eaaeec6e220b1b00cec436615`; all other three failure locations remain unchanged.

Current unistore's complete failure section needs **no location mapping**, only thread-ID normalization: `e285bfdba646f2d01f85b485ae317cc07c51b78cf0ae6d664c0fb2bc39259759`, same as construct-time, at source194:5 (DECIMAL-'abc'). These are failure-section hashes, not whole-log hashes; both current full suites remain non-green.

Full workspace/Go-package/complete-temporal acceptance, lint, release/profile/panic-test adaptation, allocator/OOM peak, zero-copy/performance, M6 and PR readiness remain unestablished. No new writer source audit/build/test/fmt/index/Plan run. Functional **131/245**, final **0/245**, not a first-pass or all-suite-green claim.
