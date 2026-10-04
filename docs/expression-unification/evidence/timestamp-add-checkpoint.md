# TIMESTAMPADD — timestamp-add-72

Started in round73 and closed in round74, following add-sub-time-71. TIMESTAMPADD adds one functional family: **214/245**, strict0;31 eligible remain and7 more are needed for221. Previous213 family objects remain unchanged.

## Ownership and value policy

`native_timestamp_add.rs` reuses the shared duration/datetime DTOs, parser, arithmetic, formatting and day-number helpers. It owns the original unit selection, numeric rounding, month normalization, range decisions and warnings. Native deletes the corresponding arithmetic bodies and three now-dead free-function facades, retaining the original numeric coercion helper.

SECOND truncates the scaled microseconds; other units round the whole amount. Fixed units retain their finite/absolute9e18 check; month units retain1e6. MONTH clamps the target day while QUARTER/YEAR overflow it through the original day-number inverse. Result precision is0 or6 according to computed microseconds. Invalid input date precedes unit validation; unknown unit still wins over nonfinite arithmetic for an otherwise valid date. Arithmetic failure is silent NULL, while an invalid computed date carries the original seven-field diagnostic. No chronology normalization or bug repair is smuggled into sharing.

## Demand and closed profiles

`TimestampAddNative` uses existing BytesBytesInt: present original unit text, observed nullable date text and present original IEEE-f64 bits. Every bit pattern is admitted, including NaN, infinity and signed zero. Original Float32 Datum storage remains f64; it is not narrowed a second time. Native `number_of` remains only the original input coercion, including its string parse fallback.

`TimestampAddPrefixNullNative` uses existing BytesInt and requires an actual NULL among the two prefix inputs. It has no date slot. The tuple still coerces amount after a NULL unit, but only two present prefix values demand leaf date coercion. No fabricated third NULL, NullWitness or ready host answer. Shared presence/UTF8 validators guard both boundaries; the nullable worker computes prefix NULL from those actual inputs. Existing four-node capacity suffices.

This leaf demand boundary does not change its callers: AST and typed entry points still eagerly evaluate children and wrap the datetime argument first. A prefix NULL can therefore still follow upstream third-argument coercion, warnings or getters. No native PB/catalog/legacy admission exists or is added.

## Results and diagnostics

Silent NULL is an absent result. Strict packets contain actual nonempty UTF8 text, UnknownUnit, IncorrectDateTimeInput or the complete computed IncorrectTimeResult warning text. Native only reconstructs String, raises the original Unsupported("TIMESTAMPADD unit"), or appends1292 and returns NULL. Input-date diagnostics use original full text; output diagnostics are computed by TiKV. No new truncation/date-mode/timezone/clock getter in the leaf; malformed packets use the existing scope contract failure.

## Validation

[Ten parent serialized Cargo receipts](../logs/timestamp-add-summary.txt):8 nonzero green,1 retained/resolved new SQL metadata expectation failure,1 unchanged full-expression baseline failure. CPP core/wrappers2 and local324+1ignored; native root1/calendars22/captured1/ADD-SUB1/SDK2 and SQL retry2 pass (overlapping filters). All6 added tests eventually pass. SQL covers16 fixed value/NULL results across both existing modes, metadata/warnings,1 valid-date unsupported-unit error and8 direct zero-slot roots.

The initial SQL test incorrectly expected(5,-1). Source inspection shows FieldType::new(VarString) starts at(-1,-1); the separate default-length lookup does not initialize noninteger fields, and result_type::text() leaves the length unspecified. Only that new assertion/comment was corrected; production and old fixtures were not changed. This is a retained RED, not a first-attempt pass or re-recorded oracle.

Full expression1567/4old/94ignored has byte-identical failure section/list against add-sub-time-71 after only numeric panic-thread IDs become THREAD (SHA256411274feba11e202735df5a8056c72babbc8cf9df2eab3258c670e9289a1ee95). Unistore is unchanged/not rerun. CPP183/native355 original test bodies and seven neighboring functions including numeric coercion remain byte-identical. Twelve sources pass pinned formatting and both diff checks; one new module, no dependency/lock/generated/Go/Bazel/fixture changes. The independent bounded review found no required product correction. Dead native cleanup removes three facades plus unused MIN_FSP.

Source tables, not provider output, determine expectations. SQL bare-unit syntax is preserved: units are not dynamic columns; composite DAY_SECOND can be syntactically accepted but is unsupported by this original evaluator. Typed Time/NULL date columns pass the upstream wrapper directly, so direct zero-slot probes avoid a different worker root. Bad VARCHAR date would test the wrapper instead and is not used as leaf evidence. Current String metadata is retained rather than repaired to a desired Go schema.

## Limits and publication

No new carrier, result kind, driver, binding, error-cause type, factory allowance or admission. Full TIMESTAMP/generic timezone parsing, whole temporal/type migration, strict completion, M6/default-NoColumns, full lint/dev/bazel/workspace/release, exhaustive differential/TiFlash/FIPS, allocator/fault/physical heap/peak/OOM/zero-copy/performance and prior compatibility exceptions remain unclaimed. This is not package transcreation or PR readiness.

Seven exclusive writers implement the bounded batch; a separate agent checks existing admission. Parent integrates, formats, tests and publishes evidence plus byte-identical Plans. TiKV publishes first; TiDB pins that commit. No force push or PR; the old local client-differential BUILD.bazel remains excluded.
