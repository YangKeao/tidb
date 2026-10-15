## Decision: retain the seven-signature nullable family

Actual batch/native dense ns/row: Int7.932/109.261, Real8.009/108.750,
Datetime7.989/108.466, Timestamp7.964/108.553, Duration10.537/108.517,
String11.168/116.607, Decimal36.192/151.667 and Vector39.314/125.384.
That is3.19–13.77x faster than real original native. Explicit same-current
fallback ablations are953–1019ns and remain separate from historical claims.
Retain the family, with JSON cast exclusion and unsigned operand rules intact.

Existing controls are flat/improved in this build except a small Vector change
70.692→70.805ns. Int13.526→12.903 and DecimalPLUS134.218→128.231 improve, as do
comparisons/Real/JSON/time/Duration/LOWER. The source only extends admission;
these unrelated code-generation/layout changes are not attributed to a new
arithmetic algorithm. Independent M15 Int12.845→12.839 and LOWER32.971→32.692
are approximately flat; Duration14.395→13.685 improves, while Datetime10.149→
10.435 remains about3% slower. All shapes and process ranges remain visible,
including earlier checkpoints' slower controls. This cohort does not establish
that all historical regressions are permanently resolved. Decimal PLUS still
trails native. No universal or application QPS claim is made.

Retained TiKVf983e547a148b8bccdf99141b8e32c6de1f503c6 / this TiDB report commit.
No new driver, nullness-only implementation or temporary migration route is
introduced. The existing ordinary kernel receives full typed arguments. JSON
continues through the unchanged native fallback because its actual Go lowering
contains an unadmitted cast. Direct wire JsonIsNull is not substituted to inflate
SQL coverage. The shared cast family remains future work.
