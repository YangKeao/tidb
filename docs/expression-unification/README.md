# Expression unification experiment

Paired YangKeao/tidb and YangKeao/tikv branch: `expression-unification-demo`.
Current checkpoint: **scalar-datum-127**, following **decimal-context-126**.

Functional **238/245 (97.14%)**, strict **0**, remaining **7**—unchanged. No whole CAST/M2 or Go-package credit.

SDK `codec/native_scalar_convert.rs` owns plain boolean/float Datum selectors and JSON-to-float, reusing existing parser/comparator/temporal/literal/vector implementations. Native facades map values/events/errors. Decimal coefficient-zero is shared as well. Float32 narrowing, JSON truth, UTF-8 and vector-empty semantics remain distinct.

## Verification

[Evidence](evidence/scalar-datum-checkpoint.md), [commands/counts/hashes](logs/scalar-datum-summary.txt), [manifest](checkpoint.json), [ledger](migration-progress.json).

Six final gates GREEN: SDK2, full datatype477, new expression1/old coerce1/float1, SQL1. Eight launches include two retained new SQL probe failures: ordinary CAST and arithmetic's inserted DOUBLE CAST parse JSON Display, not the assumed JSON-string getter. Source tracing confirms that route. Final SQL uses numeric JSON; production/old tests unchanged. Four new tests;5 SDK/251 native old test bodies unchanged.

Final SQL:2 SELECTs/8 Real cells across both vector modes, numeric JSON, Enum/Set ordinals and BIT, Double metadata/no warnings. Units separately cover JSON strings and boolean/float distinctions. No full JSON SQL, performance or physical-memory claim.

## Remaining

[Remaining acceptance](evidence/remaining-acceptance.md): other typed/write numeric selectors and SQL lowering paths, broader M2, six complex candidates, request-root/default-NoColumns/liveDAG and final acceptance. Full expression/unistore/workspace/lint/dev/bazel/release/exhaustive/performance/physical memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/whole-Go-package/PR readiness remain unverified. Historical R100 expression4/unistore1 failures remain unrepaired; prior incident receipts retained. Goal remains active.
