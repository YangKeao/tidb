# TSO and TIMEDIFF checkpoint — tso-timediff-66

Round67 follows identity-65 (TiDB effdfd84f212e4471bed0f7d85af8512779b7c0b; TiKV 3e1d24027ca3876c3b40991e65f9ea81674d4618). Two whole functional families reach **208/245**; strict final-audited count remains **0**. There are37 eligible families left,13 more needed for221. Overall goal remains active. This is not Go-package transcreation or PR readiness.

## Single ownership and implementation

Eight exclusive owners changed15 Rust files:9 TiKV and6 native, including one new TiKV source. Parent alone formatted, ran serialized Cargo, maintained Plan/guides/evidence and published. No manifests, locks, generated tables, dependencies, Go or Bazel changes. Twelve additive tests; original test bodies remain byte-exact.

- B: `Time::native_core_from_fields` owns the original const14/4/5/5/6/6/20-bit masks and shifts50/46/41/36/30/24/4. Native `CoreTime::from_date` is now a thin const facade; only its three dead Y/M/D offset constants were removed. No calendar validation or representation narrowing was added.
- H: `native_tso_utc` owns the positive gate, TSO shift and UTC conversion. `native_tso_core` independently recomputes from the actual TSO plus raw i32 offset, then uses the shared packer. `native_time_diff` owns the moved parser/type matching/subtraction/clamp/FSP/formatter; the separate demand classifier uses that same parser. `native_format_time_diff` is also the sole formatter behind the old GoDuration facade. No other temporal parser earns credit.
- G/C/D: two fixed nullable profiles, `TidbParseTsoNative` Int2 and `TimeDiffTextNative` Bytes2, both OwnBytes. Shared validators are enforced at both closed boundaries. No new carrier, result kind, role, binding, cause, driver, NoArgs or ordinary wire admission.
- A/E: native leaves retain only original coercion, metadata capture, conditional demand and actual-output reconstruction. The old TSO calculation and TIMEDIFF parser/arithmetic/formatter bodies are deleted. Existing private source tests retain a cfg(test) NoColumns facade.
- F: two SQL lifecycle tests, with13 fixed value results and10 direct zero-slot probes.

## Preserved boundaries

TSO NULL/nonpositive values retain their actual nullable integer and absent, undemanded offset. Positive values read the timezone exactly once, before the shared UTC helper is used for Named/Local offset lookup; no clock getter is read. Fixed zones send their original full i32 offset, never the generic SessionTimeZone clamp. The worker receives no ready clock/Time answer and returns an actual identity Time frame, DateTime/FSP6. All positive signed TSOs plus any i32 offset fit approximately years1901..3153, so the old checked constructor's field-width error was unreachable. Original integer casts/coercion, source timezone implementation, and SQL metadata Datetime/flen10/FSP0 remain unchanged despite the value's FSP6.

TIMEDIFF children remain eagerly evaluated by existing callers. Only subsequent right string coercion is conditional: NULL or invalid left retains its real representation and makes right absence explicitly undemanded; after a valid left, absent right means actual SQL NULL. Bad left is never replaced by a NULL witness. The worker reparses actual inputs independently. Duration arithmetic remains individually checked i64, not widened; datetime long fractions validate the full tail then truncate to6, while duration fractions longer than6 reject. Rust numeric sign quirks, zero month/day, written year width, mixed-kind NULL, maximum FSP, saturating subtraction and silent +/-838:59:59 clamp are preserved. The raw shared formatter retains its unclamped i64/usize domain and original panic behavior for other consumers.

Native TIMEDIFF returns actual String/NULL; the existing typed Duration post-cast remains untouched and still reads timezone even after a successful NULL. Dynamic VARCHAR FSP behavior is not silently fixed. No AST/PB/legacy/parser/type inference admission is added. Old AST uppercase arity lookup and SQL collation gaps remain documented, not repaired.

TSO identity-frame encoding Capacity/Invalid errors use existing `other_err!` and the RPN `LocalError::Evaluation` channel. They are not SQL NULL/overflow, fabricated Pool failures, or the host codec's separate ResourceLimit mapping. No allocator-fault injection or performance-neutrality claim.

## Validation and retained RED

Eleven actual Cargo launches:8 green,1 corrected new-test RED,2 unchanged old full-suite REDs. No compile failure, interrupted run or zero-match result; one retry. Exact commands, timings and raw hashes are in `../logs/tso-timediff-summary.txt`.

TiKV datatype packer1, TSO5, TIMEDIFF7, local317+1ignored pass. Native datatype packer1, TSO8, TIMEDIFF2 and SQL2 pass; filters overlap and are not additive unique totals. SDK/profile tests cover actual metadata, raw i32 extremes, conditional presence, invalid left text, NULL/nonpositive roots, preparation-error precedence, malformed transport, fixed output, reuse and zero-slot refusal. SQL pins6 TSO values,6 TIMEDIFF values and one zone change, plus5+5 unmasked zero-slot roots; metadata and warnings are asserted without CAST/FMT/WHERE masks.

The new native TIMEDIFF test initially called `func::eval_func_values_in`, a partial table which correctly returns None for temporal functions, and unwrapped it (first run1pass/1fail). Unchanged `func.rs:304–318/460–490` shows the actual temporal route is `time_fn::dispatch`. Only the new test's entry point was corrected; every case/assertion and all production code stayed unchanged. The first RED log remains. No new helper admission or runtime fix was introduced.

Full expression **1554 passed/4 old failures/94 ignored**, unistore **208/1 old/13 ignored**. All four new expression tests are explicitly green in the full receipt. Entire failure sections and final failure lists equal identity-65 after only numeric panic-thread-ID normalization; prior canonical digests remain `27654f2c0ad2971242c00f50182467598cdfd9c2c65e5bf5a32e683708448637` and `b64dcced405c699c11b72ee31888c58e491b4e5b4e74fa2d445454e1d1e146e9`. All15 files pass pinned formatter checks, both repository diffs pass whitespace checks, and all12 new tests have explicit final green receipts.

## Deferred and publication

Repeated parsing/UTC lookup, extra framing copies and physical heap/peak/OOM/zero-copy/performance remain unverified. Prior JSON_KEYS aggregate mismatch, caller compatibility gaps, broader default-NoColumns/M6 roots, whole workspace/lint/dev/bazel_prepare/release, exhaustive differential/TiFlash/FIPS and prior parser/GB/vector/Decimal exceptions remain deferred. No fixtures were regenerated or expected values sampled from worker/provider output.

Three byte-identical Plans accompany the paired checkpoint: TiKV publishes first, its SHA is pinned in the native manifest, then TiDB publishes without force push or PR. The pre-existing untracked client-differential BUILD.bazel stays excluded. Next RO suggests INTDIV reuse of existing exact division rather than another native long-division copy; literal families require substantial parser/timezone SDK and build-time guard prerequisites, not ready host parsed values. No future-family credit.
