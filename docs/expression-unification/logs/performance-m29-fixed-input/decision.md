## Final decision: reject both input candidates

The direct-only candidate A improves temporal inputs but regresses Decimal
comparisons about12%. The common fixed-input candidate B improves Int/Real
about8–9%, Duration about5% and JSON about6%, yet Decimal PLUS132.165→137.420ns
and LT74.938→82.870 / NULL-safe equality75.054→82.473ns regress about4%/10%.
These losses recur across the three rotated cohorts. Time/Timestamp are nearly
flat against M27. The extra abstraction does not resolve the targeted tradeoff.
No established cause is attributed to code layout, generics or host noise.

Both candidates are fully reverse-applied, including the added candidate test;
production source remains exactly the adopted M27. Their exact patches and
validation/measurement evidence are preserved. No import helper, new route,
constructor hint or weakened check is left in production. Coverage remains67/131
static signature metadata. Candidate binaries must never serve as an adopted
baseline for a later experiment.
