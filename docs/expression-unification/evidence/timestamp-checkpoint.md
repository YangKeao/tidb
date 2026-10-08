# Ordinary TIMESTAMP evaluator checkpoint

**timestamp-78** follows **convert-tz-77**. Functional family `timestamp` (S0410/S0411) is closed: **219/245**, strict0,26eligible remaining,2needed for221. All218prior family objects are unchanged. No Unix-time family receives credit.

## Actual staged evaluation, one implementation

TiKV `native_timestamp.rs` owns the original duration-fsp calculation, datetime parse, diagnostic text, formatting, duration grammar/parser, arithmetic, range and result FSP. Duration FSP uses the first dot's byte tail capped6, not the different general temporal-parser FSP helper. Existing parser flags and actual source-kind boolean are preserved. Only nonnull left text reads the actual zone, once in this leaf.

Four closed profiles:

| Profile | Actual operands | Computed output |
| --- | --- | --- |
| TimestampNullNative | Existing Bytes(None), only an observed SQL NULL | NULL |
| Timestamp1Native | TemporalParseText(text,is_float,zone), physical BytesInt | Value text or Warning report |
| Timestamp2BaseNative | Same nonnull carrier and actual zone | SDK Time identity base or Warning report |
| Timestamp2AddNative | Existing Bytes2(actual SDK base frame,nullable RHS text) | Plain UTF8 text or NULL |

Head Value is tag16+UTF8, Base is the existing tag15 Time identity frame, Warning is tag17+LE1292+computed full UTF8 message. Base decoding is representation-only: DateTime kind and width are checked, raw bits/FSP retained without calendar normalization or an arbitrary-raw-math parity claim. Native does not construct a base or reencode it: it moves the original SDK frame directly into the second stage.

Native arity and left coercion still precede zone acquisition. An actual NULL left goes through its NULL worker without zone or RHS demand. A parse warning is replayed only from the computed head report and then yields NULL. A successful two-argument base always causes RHS coercion, **including yearzero**. Only afterward does the second worker decide RHS NULL, yearzero, grammar, parse/add/range/FSP and formatting. No native parser, arithmetic or formatter remains. One-argument leaf output stays String; original typed post-wrapping remains separate. SQL child evaluation is still eager: delayed leaf coercion is not lazy child evaluation.

Datatype `Time::native_core_fields` exposes seven fields through existing getters. The DATE-only view reuses it after its original clock clear. This avoids duplicating clock bit masks or introducing another calendar implementation.

## Scope and owned-zone contracts

`evaluate_prepared_args_scoped_in` and the old helper share one three-way router. After Invocation::finish and owned materialization, the callback receives a stack-bound ScopedReadyValueColumns under the original NativeGuard. Direct binding avoids a second capability discovery or scope switch. NoColumns still prepares before allocating its old one-shot owner; execution/scope live through the callback and both stages. The old helper ignores the scope parameter and gains no getter.

The first lease is parked/retired before the second checkout; one slot suffices. No worker/cell/mutex borrow crosses the callback. Ordinary error, panic and close/epoch changes retain the existing cleanup/refusal semantics, with no replay, catch, replacement pool or host-answer injection.

A distinct TemporalParseText role carries actual text/source-kind/zone; it does not disguise the boolean as literal DateModes. A shared uses_native_temporal_zone predicate generalizes only metadata/access/RAII/storage/zone capacity accounting. Literal argument/output/budget policies remain separate. Owned zone move-binding clears on success/error/panic; Fixed.name.capacity is charged before execution and through output overlap. Head report bound max(11,textlen+64) is explicit. Logical accounting is not a physical peak/OOM guarantee. No new EvalConfig, resource cause or factory limit.

## Caller closure and validation

Frozen roots are AST/shared dispatch and typed row; vector/filter reuse those routes. No original PB or legacy unistore TIMESTAMP signatures exist, and none are added. Existing callers and typed post-wrapping are unchanged.

[Exact nine receipts](../logs/timestamp-summary.txt): datatype1, CPPcore2/local330+1ignored, native timestamp30/gateway196+1ignored and SQL1 pass. An initial compile attempt found two E0308 errors in the new gateway test: Unsupported expects static str, not String. Removing only the two to_owned calls fixed construction; no production or oracle change. All9newtests pass their first executed matching gate. No runtime assertion failure/interruption/zero-match.

Full expression1574/4old/94ignored and unistore211/1old/13ignored retain complete normalized R79 failure sections and final lists. Both full suites remain RED. CPP252/native361 original test bodies are byte-identical.15Rust files, one new module and nine new tests; dependencies/locks, Go/Bazel/generated/fixtures unchanged.

SQL uses10real-column cases under two vector flags and1/0slots:40direct probes+2positive filters.20normal cells include10DateTime/10NULL; all20zero-slot queries refuse the actual target root without core warnings. Two normal bad-left+NULL-right cases warn1292. Numeric-vs-string parsing, day carry, typed FSP, duration grammar and yearzero behavior are pinned. Existing packed-VARCHAR header19/0 with valuefsp1 is explicitly retained, not overwritten by declared metadata or claimed as new Go-wide parity.

The new gateway test covers three authority routes and success/frontend error/panic/close actions with actual computed bytes, same selected scope/owner/epoch and final retirement. Native root tests additionally execute differing TIMESTAMP profiles with one slot, including NoColumns and yearzero RHS-error precedence.

## Deferred scope

FROM_UNIXTIME and UNIX_TIMESTAMP remain unclaimed; the new scope callback enables future true demand stages but does not close their distinct getter/clock/legacy policies. M6/default-NoColumns propagation, remaining evaluators, old planner mode forwarding/CAST warning/INTDIV raw-empty gaps and existing compatibility exceptions remain open. Whole workspace/lint/dev/bazel_prepare/release, exhaustive differential/TiFlash/FIPS, performance/allocator/physical heap/peak/OOM/zero-copy/dual-timezone footprint are unverified. No whole-package transcreation or PR-readiness claim.
