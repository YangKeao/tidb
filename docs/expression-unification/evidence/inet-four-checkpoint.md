# Four INET families — inet-four-14

Functional delegation/native-deletion progress: 36/245; final acceptance: 0/245. INET_ATON, INET_NTOA, INET6_ATON and INET6_NTOA are four complete functional families on their existing admitted surfaces. The cut covers four TiDB-side files and five TiKV files, not completion of the overall unification target.

## Changes and domain coverage

The four native algorithms, their exclusive `inet6_aton_text` helper and the `Ipv4Addr` import are removed from the caller. Shared helpers, `Ipv6Addr`, `FromStr` and the four IS* algorithms remain; this is not deletion of every IP algorithm in the repository. These INET families had no PB/unistore admission, and none is added for credit.

INET_ATON preserves `coerce_str` error propagation and frontend UTF-8 errors before admission, with UInt results. INET_NTOA preserves direct Int/UInt bit interpretation; other inputs still use `report_int_truncation?` followed by `to_i64_signed`, including the original 1292 Warning/Error policy. The official kernel owns the u32-range decision and address formatting.

The INET6 functions pass raw Bytes/String contents unchanged. Invalid UTF-8 for INET6_ATON reaches the kernel and produces NULL there, rather than being converted to a fabricated None in the caller. INET6_NTOA keeps binary address input. INET6_ATON retains Binary results and the NTOA functions retain text results. All four families' NULL results also come from real C4 execution.

IS_IPV4, IS_IPV6, IS_IPV4_COMPAT and IS_IPV4_MAPPED remain unmigrated and earn no credit: native NULL versus TiKV 0 behavior differs, and IPv4 leading-zero behavior also differs.

## Actual validation

Parent ran the pinned January TiKV and August TiDB wrappers. [Commands and exact result lines](../logs/inet-four-summary.txt) are retained separately.

- TiKV `local::`: 197 passed/1 ignored/468 filtered.
- TiKV same-carrier operation/kernel-drift guard: 1 passed/665 filtered.
- TiDB `inet_dispatch_`: 2 passed/1488 filtered.
- Session lifecycle/SQL filter: 31 passed/2078 filtered.
- Full expression suite: 1392 passed/4 existing failures/94 ignored; 1490 discovered, exit101. Parent compared all four complete failure blocks against logical-three-13: identical after only thread-ID normalization. The full suite remains non-green, with no new failures.

SQL tests use seven rows of fixed network constants, not values recorded from the current engine. Coverage includes one-slot mixed execution, original metadata/binary chunks and eight direct zero-slot refusals. SQL/Go results and existing expected values are unchanged in this cut. The prior NOT BETWEEN instrumentation adjustment belongs to logical-three-13, not the inet-four-14 diff.

## Review and not verified

Parent reviewed the `compare2.rs` deletion/delegation diff. Release performance, complete business-wrapper operation scopes/guards, allocator remeasurement, physical peak/OOM guarantees, full-workspace validation, make lint and final acceptance remain open. Targeted tests and prior allocation receipts do not establish those gates; this is not PR-ready.
