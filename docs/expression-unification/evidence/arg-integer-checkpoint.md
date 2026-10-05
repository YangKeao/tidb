# Shared integer-argument control and dependency composition

**arg-integer-125 / R130**, after [decimal dependencies](decimal-datum-checkpoint.md). Functional238/245, strict0 and remaining7 remain unchanged; no whole CAST/M2 or Go-package credit.

SDK `native_cast_integer.rs` now composes actual numeric inputs with JSON rendering, signed-datum conversion and plain Decimal conversion internally. Native ordinary integer, input-warning and private value-only unsigned bridges no longer supply business conversion callbacks. Existing generic SDK entries remain for compatibility and unchanged tests, without duplicating algorithms.

The existing real/Float32-to-unsigned callback remains an **execution boundary** into the original sealed SDK worker. It preserves admission, authority and infrastructure errors; it is not replaced by a direct leaf call or a new native numerical implementation. Zone reads and truncate/warning effects remain in their original demand order.

The integer-argument controller is SDK-owned: Int/UInt/NULL preserve original storage before source metadata policy; JSON renders its actual document, checks integer-prefix truncation, then uses value-only signed conversion in UTC, ignoring source unsignedness and avoiding ordinary signed 8031/session-zone behavior. Other values retain sentinel/vector guard errors before context effects, then inherit unsignedness from actual source metadata. Native only maps explicit identity/results/errors and supplies data/effects/execution.

## Validation

Six matched gates GREEN without failure/retry: SDK4, native new1/controller1/arguments8/CAST20, SQL1. Four new tests;2 SDK/225 native old touched-file test bodies unchanged. [Exact commands/counts/hashes](../logs/arg-integer-summary.txt).

New SQL runs two SELECTs/8 cells over both vector modes: JSON3.5 becomes integer-prefix3 for MAKE_SET with one exact1292 warning per query, Enum/Set use actual ordinals, and an unsigned DOUBLE source feeds TRUNCATE through the preserved worker path. Decimal result code is asserted. Unit tests additionally verify truncation veto, guard ordering, JSON no-zone/no8031 behavior and zero-slot worker refusal versus one-slot success. Old tests/oracles remain immutable. Context-aware Decimal, other typed/write conversion selectors, broader M2/root/liveDAG/final acceptance remain. Full suites/lint/release/exhaustive/performance/physical memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/whole-Go-package/PR readiness remain unverified. Historical R100 expression4/unistore1 failures remain unrepaired.
