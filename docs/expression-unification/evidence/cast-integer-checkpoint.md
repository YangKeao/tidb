# Integer CAST control and rounded Decimal ownership

**cast-integer-109 / R112**, following [ordinary DECIMAL CAST](cast-decimal-checkpoint.md). Functional **238/245**, strict **0**, remaining **7** stay unchanged. This batch is not whole CAST/M2 or Go-package completion.

## Ownership

SDK `native_cast_integer.rs` owns SIGNED, UNSIGNED and UNSIGNED-in-UNION control, integer-prefix scanning, truncation/overflow/complement/advisory diagnostics, source selection and the static-eval-type negative gate. Native `cast.rs` keeps thin dispatch and existing public helper signatures through `tikv/cast_integer.rs`; old policy bodies are deleted.

Four SDK entry contracts share these algorithms: full target control, signed value only, unsigned value only and input-warning only. The old unsigned test surface still lacks pre-truncation/8031 but retains Decimal1292 and Real-worker warnings. It is not emulated by suppressing messages from a different entry.

The three borrowed rounded-Decimal integer primitives move to `mysql/native_decimal_parse.rs`. Native methods delegate directly. They use stored scale without normalization, preserving raw sign/bytes, overflow and panic rules. Signed empty integral storage returns0 while unsigned empty storage saturates toMAX; raw+17, malformed bytes and invalid scale are not newly screened. These are not truncation or wire aliases.

## Ordering and boundaries

- SIGNED: fallible input truncation, then overflow/complement append, then unconditional caller-zone read, then value conversion. A truncation error stops all later work.
- UNSIGNED: input truncation, negative-string advisory, then source-specific conversion. Only Int/String/Bytes/Time/Duration request the zone.
- UNION checks actual static EvalType independently of datum kind. A negative clamp precedes warnings, zone and the old RealUnsigned worker. Decimal uses rounded signed negativity, not raw sign; a negative malformed string can be silently clamped under the original eligible metadata.
- Signed Float32 still requests the original datatype conversion, unlike Real's direct branch. Both actual2.5 conversions are ties-even2; an obsolete comment does not define the oracle.
- Only JSON object/array requests diagnostic rendering. Actual datatype `(value,event)` or error is returned to SDK for the original discard/fold policy. Unsigned Other rounds before discarding its event, retaining the original lifecycle order.
- Real/Float32 unsigned conversion still invokes the existing worker and propagates its infrastructure errors. No extra or skipped facade, new C4 profile, carrier or admission gate is introduced.
- Value-only signed entry retains caller-supplied zone or the existing UTC wrapper, without reading Columns. Null/MinNotNull/MaxValue retain their original guarded-value panic; warning-only ignores them. Outer ordinary guards remain unchanged.

## Validation

[Exact commands/counts/hashes](../logs/cast-integer-summary.txt): nine nonzero green receipts—SDK datatype1, SDK controller2, native bridge1, original cast-module21, original cast-function16, full native datatype463, new SQL1, original RealUnsigned SQL1 and R111 DECIMAL SQL1. All have zero failed/ignored tests; no zero-match, interrupted or retry run. Five new tests;2 SDK and222 native old test bodies in changed files remain byte-identical. Original RealUnsigned worker, external consumers and fixtures are unchanged. The preexisting vectorized string-to-DECIMAL UNION gap remains ignored/unmodeled and was not selected by this round's filters. New SQL uses ten SELECTs across scalar/vector modes: integer/Decimal/Real/Float32 rounding, full-width/complement/advisory strings and malformed-prefix suppression, Decimal versus Real unsigned negatives, temporal carry, differing JSON signed/unsigned conversion, structured JSON diagnostics and NULLs. It does not pretend to exercise AST UNSIGNED-in-UNION through an unrelated SQL UNION route.

## Incidents and deferred work

Before any native test ran, a proposed Float32=3 expectation based on stale prose was corrected against `datum/convert.rs` and `numeric_helper.rs`: actual ties-even conversion is2. Channel choice is checked separately using the actual callback. No provider-output oracle or fail-before test receipt was manufactured.

Other targets, typed/vector routes, native datatype conversion actuators, broader M2, six complex candidates, request-root/default-NoColumns/liveDAG and final acceptance remain. Full expression/unistore/workspace/lint/dev/bazel/release/exhaustive/performance/physical-transient memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/whole-Go-package/PR readiness remain unverified. Historical R100 expression4/unistore1 failures remain unrepaired. Overall goal stays active.
