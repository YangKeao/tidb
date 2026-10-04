# Temporal foundation — temporal-foundation-74

Round76 follows json-search-73. This is a shared datatype prerequisite, not an evaluator migration. Functional credit stays215/245 and strict0;30 eligible families remain and6 more are needed for221. All215 prior family objects are unchanged.

## Ownership and interfaces

TiKV datatype codec/mysql/time/native_datetime.rs owns raw-calendar validation, generic civil-to-instant conversion and the original local/repeated/gap resolvers. Native CoreTime methods pass actual raw bits and the caller's actual chrono timezone; TimeConversionError aliases the exact shared three-variant error with its original traits and messages. No SQL validator, wire Tz conversion, timezone-name reparse or current-offset snapshot narrows this domain.

Calendar checks precede timezone access and retain chrono's wide-year and leap-second microsecond domain. Repeated times retain the Go-compatible wall-as-UTC offset lookup, transition-boundary search and later-candidate fallback, not an unconditional earliest/latest choice. Gap adjustment clears nanoseconds and searches1..14400 seconds for the valid upper boundary; ordinary conversion still returns NonexistentLocalTime. Shared datetime packing adds500ns before original field casts and the existing native_core_from_fields packer. Cached offset behavior, raw masks and chrono overflow panics are unchanged.

TiKV native_parse.rs owns timezone-suffix recognition, fraction-index/FSP and permissive date splitting/classification. Native TimezoneSuffix aliases the shared DTO while retaining fields and its original Debug label. Suffix parsing recognizes only the original uppercaseZ and signed6/5/3-byte forms; it does not trim or validate offset ranges. Fraction indexing examines suffix-excluded bytes, but FSP deliberately counts bytes in the original source suffix. Date splitting keeps Rust trim/from_utf8_lossy, consecutive-separator rules and the unexamined last-byte behavior; date-shape classification does not validate calendar values. One ASCII-punctuation predicate serves both domains and the still-native full parser.

Full parse_time/numeric/flag/kind policy, TimeError and SessionTimeZone remain native. Wire timezone/type rules and both chrono-tz versions are unchanged. This slice adds no C4 profile, carrier, driver, binding, result kind, factory budget or caller admission. Public thin facades preserve existing consumers, including temporal expression helpers, literal folding, legacy conversion, scheduling and metadata/reporting code.

## Validation

[Seven serialized Cargo receipts](../logs/temporal-foundation-summary.txt):5 nonzero green and2 unchanged old full-suite failures. CPP time61 and native full datatype453 pass; both new CPP matrix tests pass on the first gate. Three existing session-lib tests pass for LA/London repeated-time choices, timezone-suffix/fractional carry and strict/non-strict DST-gap insertion. No new RED, compile failure, zero-match, interruption or retry; no provider recording or new SQL oracle.

Full expression1568/4old/94ignored and unistore211/1old/13ignored retain byte-identical failure sections/final lists against json-search-73 and time-microsecond-70 respectively, normalizing only numeric panic-thread IDs. Digests are411274feba11e202735df5a8056c72babbc8cf9df2eab3258c670e9289a1ee95 and2f19c9ad5338a86c48b895e3923c3e39454117e3410d47df5aa3fb4254ed7ce1. Ignored tests are not passing.

CPP51/native74 original test bodies are byte-identical; existing SQL test files are unmodified. All six sources pass pinned formatting and both repository diff checks. No dependency/lock/tzdata/generated/Go/Bazel/fixture changes.

## Publication and limits

Five exclusive write owners implement six Rust files, including two new shared modules; two bounded read-only reviews identify consumers/gates. Parent owns integration, formatting, evidence, guides and paired publication. TiKV publishes first; TiDB pins its exact commit and the byte-identical three-Plan hash. No force push or PR; pre-existing client-differential BUILD.bazel stays excluded.

No new family credit, complete temporal/parser/type migration, whole package transcreation or PR readiness. M6/default-NoColumns, whole workspace/lint/dev/bazel_prepare/release, exhaustive differential/TiFlash/FIPS, allocator/fault/physical heap/peak/OOM/zero-copy/performance and earlier compatibility exceptions remain deferred.
