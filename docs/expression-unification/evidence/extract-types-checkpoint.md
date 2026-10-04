# EXTRACT prerequisite: shared duration and extraction types

**extract-types-97 / R100**, after **json-sum-crc32-96**. M0 foundation only: functional231/245 unchanged, strict0, remaining14. All231 existing family objects are unchanged. No new worker, carrier, whitelist, PB/legacy admission or family credit.

## Why this prerequisite

The real EXTRACT selector uses a native nanosecond duration parser for mixed DAY_* values. Neither wire Duration parsing nor query_expr NativeGoDuration has that policy. Moving only calendar compatibility or sending a host-computed duration into a worker would leave the real implementation behind.

Two new SDK modules now own the needed services:

- `codec/mysql/duration/native_parser.rs`: original byte grammar, fallback classifier, full generic-timezone datetime fallback, status/error/event types, endpoint clamp and raw Time-to-duration conversion. Existing MAX_NANOS ends at838:59:59.0. The parser reuses shared FSP/fraction, byte-as-rune Go punctuation and datetime services rather than adding copies.
- `codec/mysql/time/native_extract.rs`: three unit sets and raw datetime/duration extraction formulas. ASCII uppercase is not trimming; datetime supports date/DAY_*/YEAR_MONTH only, WEEK uses mode0, and raw microseconds survive regardless of FSP. Duration uses existing absolute nanosecond components and applies sign to the whole result, without a SQL range or FSP gate.

Native `duration.rs`, `time_parse.rs` and `mysql_time.rs` are thin adapters for these services. Eight private lexer helpers and the corresponding native parsing/routing/clamp/conversion/extraction bodies are removed. Original ParsedDuration, RangeResult and rounding carriers retain private fields and Debug names; error/event aliases preserve variant identity, Display and event priority. Independent interval parsing and duration rounding remain separate work.

## Preserved boundaries

FSP validation precedes byte grammar. Outer ASCII trim, sign padding, long-leftover rejection, byte-Latin1 punctuation, fraction carry and overflow/truncation outcomes retain their source order. Only a DateTimeFallback signal enters the shared datetime parser, with the actual timezone and zero/invalid flags, and ignore-zero-date-error enabled. Successful fallback intentionally drops parsed-time truncation and returns no overflow/truncation event, exactly as before.

Raw zero Time yields nanos0/fsp0 even when stored FSP differs. Any nonzero raw value normalizes FSP then computes raw clock fields, without rounding or range clamping. Low raw tag bits therefore still distinguish zero from nonzero. Invalid extraction units retain original spelling and native private Outcome still exposes value0 plus error.

## Validation

[Exact commands, counts and hashes](../logs/extract-types-summary.txt); [manifest](../checkpoint.json).

Nine locked single-threaded launches: **seven green, two known RED**, all compiled and executed nonzero tests.

| Gate | Passed / failed / ignored |
|---|---|
| CPP datatype | 464 / 0 / 0 |
| CPP local | 350 / 0 / 1 |
| Native datatype | 461 / 0 / 0 |
| Native expression full | 1608 / 4 / 94 |
| Native unistore full | 220 / 1 / 13 |
| New SQL, old TIME, old CAST, old EXTRACT | each1 / 0 / 0 |

Full expression retains R98's ifnull-catalog, EXP-overflow, negative-duration-FSP panic and STR_TO_DATE month0 failures. Unistore retains its decimal `abc` selection failure. Failure names, source paths and diagnostic bodies match R98 after excluding process ids and shifted line numbers. Neither full suite is claimed green; no original expectation was changed.

Four new tests pass on their first matching execution. They cover original parser vectors/status/errors, all256 byte punctuation classifications against the frozen literal set, real timezone demand/forwarding, raw metadata and extraction domains. The SQL test has four SELECTs at pool1 across both vector settings: stored TIME rounding, negative typed duration, mixed day-prefix and datetime inputs, typed datetime, bad numeric conversion, materialized Datums/field metadata and warnings. It proves type consumers, not new EXTRACT worker admission. Three original SQL gates additionally exercise16/19/32 SELECTs.

Four CPP/four native Rust files; two new modules. All68CPP/235native original test bodies in changed files remain byte-identical; no fixture or provider-oracle recording. Formatting and diff checks pass. E's focused dependency/control-flow review found no blocker, but was not a HEAD line-by-line diff or execution endorsement. One parent audit regex initially captured warning separators; anchoring failure headers corrected that audit only, with no code/test change or extra Cargo launch.

## Next runtime step

The Plan freezes six EXTRACT profiles over existing carriers. Remaining native policy includes the main source-kind selector, original casts using the complete FieldType, two distinct lazy mixed-path mode reads and its datetime-versus-duration choice. Calendar's broad u32-hour/wide-year compatibility parser is separate and still needs its own worker. Unknown units must still cast before raising1105; NULL and getter demand cannot be globally rearranged. No PB/legacy or specialized vector kernel is inferred from catalog names.

[Remaining acceptance](remaining-acceptance.md) stays open. Request-root/default-NoColumns/liveDAG/final acceptance, workspace/lint/dev/bazel/release/exhaustive differential/TiFlash/FIPS/performance/physical memory/OOM/allocator/dual-tzdata/whole-Go-package and PR-readiness are not certified. No Cargo/lock/Go/Bazel change; unrelated untracked BUILD remains excluded.
