## Decision: rejected and fully reverted

The target Duration workload regresses12.939→13.472ns/row (~4% slower) over
three actual-code cohorts despite elimination of the579byte standalone symbol.
No meaningful constructor benefit justifies the hint. Int12.217→12.281,
Real12.405→12.663 and JSON131.274→132.226 are also slightly slower; Decimal is
near flat. Time improves about2%, LOWER34.373→32.855 and Vector74.343→70.709
about5%; all samples/ranges remain visible. These unrelated gains are not
attributed to a successful constructor optimization. The declared target fails.

Independent M15 controls compare experimental code only: Int12.837→12.270,
Time10.167→9.498, Duration14.383→13.513, LOWER32.999→32.804. They do not reset
the adopted baseline or erase prior receipts. All2940/210 and840/60 groups match.
No universal/QPS claim, no assertion that source inlining guarantees throughput.

The exact one-line hint is reverse-applied. TiKV is clean at
d5817d0bfa15ccfad1fe93bea82e779dadf5e4c9; adopted runtime remains M27
TiDB7f0daa9135330e5d64aef22a2f2819e41fd90c68, with this checkpoint adding
only documents/evidence. Candidatea3b554... remains experimental and MUST NOT
become the next before baseline. All FSP/range/normalization checks remain.
