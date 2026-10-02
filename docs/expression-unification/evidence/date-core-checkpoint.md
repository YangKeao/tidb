# DATE core checkpoint

`date-core-63`, following `clock-three-62`. DATE adds one whole frozen family: **202/245** functional delegation/deletion, strict final-audited acceptance **0**. There are43 eligible families left;19 more reach221. Overall work remains active, not PR/package completion.

## Shared implementation and boundaries

Eight exclusive owners changed17 Rust files (TiKV8/native9), with no new Rust source files and10 additive tests. Parent owns contracts, Plan, formatting, serialized gates, guides and publication. No dependencies, locks, Go, Bazel or physical transport changes.

TiKV Time owns one native date projection: clear the low41 raw-core bits, preserving all23 YMD bits. This clears hidden clock/fraction/reserved bits even for an input already tagged Date, while retaining the native raw year/month/day domain. `native_date_fields` reads the computed core; the prior typed-clock SDK delegates that same view rather than keeping a second zeroing algorithm. Wire DATE and wire Time kind-changing policy are untouched.

A separate shared unit predicate checks the **original** raw core: full64-bit zero versus nonzero month/day zero. It does not classify a preprojected answer. Native preparation preserves arity/type/true-NULL order, reads date_modes once only for Time, and calls the original truncation handler with the original kind/FSP diagnostic before worker admission. Strict errors remain errors; soft-invalid non-NULL input still sends its actual raw core and actual modes to the worker, which independently computes NULL. No fake NULL or host date answer is transported.

DateCoreNative uses existing BytesInt: original8-byte LE core plus actual three mode bits (NO_ZERO_DATE, NO_ZERO_IN_DATE, ALLOW_INVALID_DATES). The last remains inert for DATE. Both closed boundaries share the exact8/Some0..7 validator. OwnSignedInt carries actual computed date bits, bitcast back to u64 and boxed by the unchanged native Time::new(Date,0) representation codec. A high native year bit is not converted through checked signed arithmetic. True NULL reuses DateDiffNullNative's actual witness.

PB non-NULL dispatch already forwarded ctx. The only production PB change sends the first observed NULL alone to the guarded DATE adapter, preserving malformed-arity bypass, uncoerced prefixes, unread suffixes and earlier child errors. No signature or cast admission changes.

Legacy DATE remains predicate-only in eval_expr/folded_int. It demands eval_time(first) once and sends the actual nullable raw core to DateCorePredicateLegacy; no native mode getter, warning or calendar policy is added. The worker computes projected-core nonzero, equivalent to the old nonnegative date-number truth. Original Time::new(Date,0) could not fail precision validation. Native/legacy inline projection and legacy decimal truth reconstruction are deleted. Legacy eval_time(DATE(...)) remains unadmitted.

No new driver, carrier, result kind, runtime binding, cause, NoArgs profile or ordinary wire/parser/PB/legacy admission.

## Validation

[Exact commands and complete raw-log hashes](../logs/date-core-summary.txt); [manifest](../checkpoint.json).

Ten completed nonzero Cargo runs: eight green targeted gates and two known full-suite RED runs. No compile failures, interruptions, zero matches, retries or gate-driven source/test repairs. All10 new tests pass.

- TiKV datatype1; DATE kernels/local-profile2; prior typed-clock SDK2; local312 passed/1ignored.
- Native DATE SDK1; unchanged typed-helper bank5; legacy DATE1; SQL2.
- Full expression1540/4old/94ignored includes all four new expression tests: SDK, native value/policy tests and PB. Their exact lines were checked as `ok`; no separate targeted pass is claimed for those not separately run.
- Full unistore208/1old/13ignored. Entire expression/unistore failure sections and lists equal the prior checkpoint after only numeric panic-thread-ID normalization. The verified expression hash is `27654f2c0ad2971242c00f50182467598cdfd9c2c65e5bf5a32e683708448637`; unistore `b64dcced405c699c11b72ee31888c58e491b4e5b4e74fa2d445454e1d1e146e9`.

SQL pins eight values/metadata, two soft1292 warning cases and six direct zero-slot temporal/NULL-column expressions. Existing ETDatetime casts pass Time/NULL through, so another cast worker does not mask these root proofs. Legacy tests cover12 present/absent/raw cases in1/0-slot scopes through eval_expr and folded_int, three independent shared-child demand cases, and continued absence of a DATE temporal-value channel. Native/PB tests cover original-raw zero distinctions, all mode bits, high year bits, hidden Date clock, strict/soft diagnostic order and NULL demand.

All17 files pass pinned rustfmt and both repository diff checks. All preexisting test bodies in changed files are exact; original SQL source is exact after removing only new function blocks, and original time tests remain an exact prefix. Native constructors, core representation, casts, catalog/scalar dispatcher, legacy eval_time/eval_duration, R63 helper/getter bodies, wire DATE/kind conversion, compile module and locks remain unchanged. No oracle/fixture regeneration. The official expression guard receives only the narrow new native-frame validation.

## Incidents, limits and next work

One owner hit a file-observation prerequisite and reread before editing; an initial RO path guess was corrected. Neither was a runtime failure or production repair. All actual gates and failures are retained.

Prior extra JSON_KEYS aggregate type mismatch is unresolved/not rerun. Deep raw JSON decode precedence/performance, tree conversion/key-search costs, M6, broader default-NoColumns roots, workspace/lint/dev/bazel_prepare, release/performance/zero-copy/physical heap/peak/OOM, exhaustive differential/TiFlash/FIPS and prior parser/GB/vector/Decimal exceptions remain deferred. Extra classification/packet costs are not claimed performance-neutral.

Next read-only recommendation: WEIGHT_STRING plus FORMAT. Weight-string must retain AST/typed numeric-NULL and padding/value demand differences, argument collation and global collation mode while reusing existing CPP keys. FORMAT requires exact rounding/coercion/warning order and the public locale SDK; tidb-mysql currently lacks the needed shared dependency, which must be explicitly designed. Password-strength has lazy error-sensitive global/user/dictionary stages and is not a quick pure leaf. Temporal parser prerequisites, CRC32's rejected quoted-SQL route and all19-kind identity families remain unimplemented/uncredited.

Both architecture guides describe ownership without introducing repository policy. Publication is TiKV first, then TiDB pinning its exact SHA and common Plan hash. No force push or PR; old untracked client differential BUILD.bazel stays excluded.
