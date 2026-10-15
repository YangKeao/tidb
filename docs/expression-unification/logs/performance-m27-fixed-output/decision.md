## Decision: retain the shared owned-buffer loop

Three actual-code cohorts improve dense Duration15.082→12.879ns/row (paired
1.17x), Datetime10.933→9.551 and Timestamp10.918→9.542 (~1.14–1.15x),
Int13.424→12.220 (1.10x), Real13.856→12.411 (1.12x), signed ROUND8.553→7.481,
unsigned ROUND8.487→7.483 and UInt ANY_VALUE10.771→9.798 (1.10–1.14x).
These common fixed-output gains justify the safe-Rust storage change.

DecimalPLUS131.923→131.975 is flat; comparisons76.1→75.0 improve only about1%.
JSON130.837→132.312 has a slightly slower aggregate median (within-pair ratios
near flat), LOWER34.695→34.429 and Vector72.206→71.647 are near flat. All raw
processes/shapes remain visible; this is not a universal speedup. Independent
M15 Int12.899→12.235, Datetime10.119→9.597 and Duration14.409→12.909 improve,
while LOWER32.997→34.384 remains about4% slower. Older JSON/varlen losses are
not declared resolved. Decimal PLUS still trails original native. No QPS claim.

Retained source: this TiDB report commit / unchanged TiKV
 d5817d0bfa15ccfad1fe93bea82e779dadf5e4c9. The old ordinary per-row owned append
loop is replaced. The remaining tail handles zero-width or inaccurate iterator
counts; it is not a migration execution route. Shared/frozen buffers retain
their existing ownership semantics. No unchecked constructor, value-policy
change, new driver or function-specific path is introduced.
