# Ordinary DATE_ADD/SUB and typed-duration arithmetic

**date-arithmetic-102 / R105**, after [interval-runtime-101](interval-runtime-checkpoint.md).
This is a **partial** migration: functional235/245, strict0, remaining10 are unchanged. All235 prior family objects remain identical. Ordinary SQL/AST/helper arithmetic and its required interval datatype parser move together;48 legacy calculations still prevent whole-family credit.

## Ownership

- `native_date_arithmetic.rs` owns original coercion demand, numeric-source policy and warning/overflow outcomes. `native_date_arithmetic_helpers.rs` owns calendar and typed-duration algorithms, using existing shared civil/calendar primitives and the existing EXTRACT clock parser.
- Datatype `time/native_duration_interval.rs` owns ParsedInterval and the original strict single/composite parser/extractor. Native `time_parse.rs` retains aliases and thin constructors. The private native scanner and arithmetic/parser bodies are deleted, not copied into a second active native implementation.
- `time_fn/{calendar,add_sub}.rs` and `func.rs` now delegate through `tikv/date_arithmetic.rs`. Result-FSP metadata delegates to a pure SDK service. The original formatter test retains its unchanged body over a thin SDK helper. Rustdoc references follow the moved algorithms.

## Four closed profiles

| Profile | Existing Values arguments | Reports |
|---|---|---|
| DateArithmeticHeadNative | Bytes4: actual date/amount identities, unit UTF8, actual metadata | Null/text/unsupported/request/overflow; optional1292 |
| DateArithmeticDurationHeadNative | Bytes4 with its distinct metadata | Null/duration/unsupported/request; no warning |
| DateArithmeticStepNative | Whole SDK report plus actual generic preparation result | Union of the above, optional1292; no error |
| DateArithmeticOverflowNative | Whole overflow report plus actual truncate level | Null,1441 warning+Null, or1441 error without warning |

All successful outer replies are present, including SQL NULL. Only the two specified heads gain four-argument admission and five compile nodes; generic limits remain unchanged. No new carrier, PB/wire/legacy signature or specialized vector kernel is added.

Head admission precedes SDK-directed generic coercion, so a resource refusal can now precede its original diagnostics. Existing eager child evaluation and typed pre-casts remain before Head.

Generic preparation is original `coerce_str`, `Datum::to_i64().value`, or `Decimal::parse_mysql(text).0`; the latter two retain their original discarded conversion events. The bridge forwards whole SDK continuations. It replays1292 before the next preparation or late overflow-policy getter. It neither searches for an answer nor reconstructs cursor state.

The checked retained bound is `2 * sum(actual input byte lengths) + 1024 + needed fixed precision`. Precision is charged only when a real Duration date, composite unit, finite Real/Float32 amount and nonnegative actual decimal metadata reach fixed formatting. NULL/wrong-type dates and nonfinite formatting do not invent precision work. Actual owned capacities are checked independently. This is not a transient/physical heap or OOM guarantee.

## Compatibility boundaries

AST evaluates both children in the caller context, then retains historical NoColumns semantics for the entire body while binding actual execution authority. The context-free public helper remains context-free. Typed paths keep their original statement context, preceding Duration→Datetime cast and following temporal result cast. The body adds no mode/zone/clock getters.

Single units prepare date before amount; composite units prepare amount before date. Date NULL therefore does not globally suppress invalid composite amounts. HOUR/MINUTE/SECOND check delta overflow before parsing the clock suffix, while MICROSECOND parses the suffix first. Day/month units retain original suffix text. Raw calendar sign remains a multiplier; duration uses only sign<0 to choose subtraction.

Whole string amounts emit unconditional1292; SECOND uses its distinct quiet Decimal parser and microsecond truncation. Real date truncation, Decimal date rounding and Float32 generic coercion remain different. UInt date overflow reports the fixed subject '-1'. Only calendar arithmetic overflow reads truncate level and maps1441.

Ordinary composite parsing treats excess groups as zero and does not right-pad microseconds. Typed Duration first rejects calendar components through that parser, then uses the stricter datatype parser, which rejects excess groups and right-pads microseconds. Single duration deltas use3,020,399,000,000,000ns; strict extraction uses3,020,399,999,999,999ns. Final duration addition checks i64 overflow only and retains raw result FSP, without TIME-range clamping or FSP normalization.

## Validation and incidents

Twelve actual serial locked launches: **ten nonzero passing runs**, one retained zero-match run and one retained compile failure. Initial native datatype `interval` filter matched zero; corrected `duration_value` runs both unchanged parser tests. Initial native entry compile failed E0308/E0061 because new bridge Bytes4 used four parameters instead of its existing array parameter. Only the new constructor was corrected to `Bytes4([...])`; retry passed. No original test/oracle was modified, and neither failed attempt counts as a pass.

Six new tests;205 TiKV and435 native original test bodies are byte-identical. Datatype/core/direct lifecycle, native entry/gateway/calendar and three exact SQL tests pass. [Commands, counts and hashes](../logs/date-arithmetic-summary.txt) include every attempt.

The new SQL test has52 SELECTs:13 stored-operand templates × two vector settings × pool1/0. There are26 successful rows (including warning+NULL results) and26 refusals: conservatively24 direct Head refusals plus two existing TIME→Datetime pre-cast-route refusals. No claim that getters cannot precede Head on that route. The two original SQL gates cover temporal arithmetic/aliases and statement overflow/strict INSERT/IGNORE behavior. This is not exhaustive legacy/unit cross-product validation.

## Next closure

[Remaining acceptance](remaining-acceptance.md): legacy32text/8datetime/8duration policies retain different getter orders, reformatting and CoreTime arithmetic. Eight Duration→Datetime labels remain original child-free unsupported paths. Shared PB still has no kernel. Request-root/default-NoColumns/liveDAG/final acceptance remain open. Full suites, lint/dev/bazel/release, exhaustive differential, physical memory, performance, allocator/OOM, TiFlash/FIPS, dual-tzdata and whole-Go-package/PR readiness were not verified. Historical R100 expression4/unistore1 failures remain unrepaired.
