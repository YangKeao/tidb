# Expression unification experiment

Paired YangKeao/tidb and YangKeao/tikv branch: `expression-unification-demo`.
Current checkpoint: **decimal-datum-124**, following **arg-string-123**.

Functional **238/245 (97.14%)**, strict **0**, remaining **7**—unchanged. No whole CAST/M2 or Go-package credit.

SDK `codec/native_decimal_convert.rs` owns plain Datum-to-decimal, decimal text and JSON conversion. Native facades project existing Decimal transport and typed events/errors. Existing parsers/math/literal outcomes remain sole owners; JSON floats reuse the existing canonical projector via crate-local visibility. Lossy datum UTF-8, empty invalid JSON text, plain/JSON float differences, hybrid ordinals and Decimal shape remain distinct.

## Verification

[Evidence](evidence/decimal-datum-checkpoint.md), [commands/counts/hashes](logs/decimal-datum-summary.txt), [manifest](checkpoint.json), [ledger](migration-progress.json).

Five final gates GREEN: SDK2, full native datatype477, new expression1/old integer1 and SQL1. Six matched launches include a corrected new-test assumption: wide Literal/Bit Decimal conversion preserves u64MAX+Truncated, not signed-literal zero. Production/old tests unchanged; RED retained. Four new tests;5 SDK/234 native old test bodies unchanged.

New SQL:2 SELECTs/8 UInt cells across both vector modes, actual Enum/Set ordinals, BIT/JSON full unsigned range and LongLong20/0 metadata/no warnings. No performance or physical-memory claim.

## Remaining

[Remaining acceptance](evidence/remaining-acceptance.md): integer-argument/controller composition, context-aware Decimal and other typed/write selectors, broader M2, six complex candidates, request-root/default-NoColumns/liveDAG and final acceptance. Full expression/unistore/workspace/lint/dev/bazel/release/exhaustive/performance/physical memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/whole-Go-package/PR readiness remain unverified. Historical R100 expression4/unistore1 failures remain unrepaired; prior incident receipts retained. Goal remains active.
