# CONVERT_TZ evaluator checkpoint

Checkpoint **convert-tz-77** follows **temporal-literals-76**. Functional family `convert_tz` (S0070/S0071) is now closed:218/245, strict0,27eligible remaining and3needed for221. All217 previous family objects are unchanged. FROM_UNIXTIME and UNIX_TIMESTAMP remain unclaimed.

## Single implementation

TiKV `native_convert_tz.rs` owns the complete original canonical datetime composition, SQL-zone regex/parser, source-instant/destination conversion and output fraction text. It reuses Time::parse_native_date_ymd and Time::parse_native_clock_with_fraction, rather than copying those primitives or replacing the broader calendar domain with packed-core/full-parser behavior.

Native convert_tz_in keeps only arity, three left-to-right coerce_str operations, actual nullable Bytes3 preparation and computed String/NULL projection. An earlier NULL does not skip later coercion; an error still stops via ?. The original helper signature remains a NoColumns wrapper. The dispatcher passes existing Columns scope authority, without new timezone/clock/date-mode getters in this leaf. Original typed argument casts and post-result temporal wrapping remain outside this migrated leaf and may still read session context.

Within the worker, any actual NULL returns NULL. Otherwise datetime parsing precedes both zone parsers. Both zone parsers execute once reached, even if the first yields unknown/None. Named zones use the shared native0.10.4 type identity, not wire0.5.3. SYSTEM remains case-insensitive but untrimmed and uses its separate LocalResult Single/later-Ambiguous/NULL-gap rule. Fixed-offset regex, single-digit fields, +14:0 and the original Unicode-digit parse panic remain; there is no normalization or silent repair.

The original generic NaiveDateTime local_to_instant/offset_at/dst_gap_bound closure is moved once and exposed as native_legacy_local_to_instant. Native session_tz uses a thin alias; its other Unix-time algorithms are untouched. This helper preserves the two-offset roundtrip search, ±24h offset bisection to≤1second, four-hour gap guard, fractional and broad calendar domain, and original arithmetic behavior. It intentionally differs from the packed-core second-stepping gap algorithm. No duplicate native helper remains and no Unix-family credit is earned by this sharing alone.

## Closed worker and caller scope

One ConvertTzNative profile uses existing Values/nullable Bytes3, three columns plus one call, existing nullable OwnBytes output and existing factory budget. Every present operand must be UTF8, including one behind another NULL; no preflight date/zone parsing occurs. No new carrier, temporal metadata, binding, EvalConfig, resource cause, dependency or wire admission. L78 literal predicates and zone binding are untouched.

Frozen coverage is AST/shared value dispatch and typed row. Existing typed/vector/filter consumers still use those routes. There are no existing PB/legacy CONVERT_TZ signatures to connect; none are added. Core tests and new explicit-scope tests distinguish actual NULL and invalid business inputs from pre-kernel resource refusal. SQL must use real stored datetime/from-zone/to-zone columns rather than folded answers or a different expression as a zero-slot witness.

## Validation

Seven exclusive writers, parent integration;14Rust files(8CPP/6native), one new module and five new tests. CPP188/native368 original test bodies are byte-identical, as are all three moved transition-helper bodies. No dependency/lock/Go/Bazel/generated/fixture changes; pinned format and diff checks pass.

[Eight exact receipts](../logs/convert-tz-summary.txt): CPP core2/local328+1ignored, native converter4/session_tz5 and SQL1 pass. One native compile attempt failed E0061 because the new facade passed three constructor arguments instead of the existing Bytes3 array payload; only that constructor was corrected and retried. No runtime assertion failed, no oracle changed; all5newtests pass their first executed matching gate. No interruption or zero-match.

Full expression1571/4old/94ignored and unistore211/1old/13ignored retain complete normalized failure sections and final lists against R78. They remain RED, not silently green. SQL covers12real-column cases under two vector flags and1/0slots:48direct probes plus2positive filters. Normal24cells include10DateTime/fsp6 and14NULL; all24zero-slot queries directly refuse the target root. Declared Datetime(26,6)/binary metadata stays intact. Four bad-datetime probes retain the original pre-cast1292 warning, including before pool refusal. Original SQL pre/postcasts remain contextual; core-only absence of getters is not a whole-SQL claim.

## Remaining scope

FROM_UNIXTIME parses/truncates/checks/rounds before reading the actual zone and delays format coercion until a valid local result. UNIX_TIMESTAMP(arg) reads zone#1 for parsing, then zone#2 only after a valid complete core; those results may differ. Its zero-argument branch uses the actual clock and original i64 arithmetic. Legacy UnixTimestampDec/Int and FromUnixTime1Arg/2Arg have separate policies, including the existing nanoseconds-times1000 behavior; none is repaired or replaced here. Future migration needs actual staged computed results and demand preservation, not prefetching or widening L78's literal predicate.

Strict audit count0 and M6/default-NoColumns remain open, as do planner date_modes forwarding, old CAST warning/raw-empty INTDIV and other compatibility exceptions. Whole workspace/lint/dev/bazel_prepare/release, exhaustive differential/TiFlash/FIPS, performance/physical heap/peak/OOM/zero-copy and dual-timezone footprint remain deferred. No whole-package transcreation or PR-readiness claim.
