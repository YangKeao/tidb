## Decision: rejected and fully reverted

The constructor hint provides no clear Decimal gain: PLUS129.435→130.475ns,
LT74.933→75.356 and NULL-safe equality75.081→75.437. Int13.147→13.766,
Real14.020→14.317 and Duration14.962→15.261 are slower; time controls improve
about1%. These are recorded tradeoffs, not a universal-gain claim.

One baseline Int cohort spikes23.124ns (others about13ns); all raw samples and
process ranges remain visible. We do not attribute its paired improvement to
the candidate. Even without that outlier, the target Decimal operations do not
justify this performance-only hint. No further cherry-picked sample is used.

Candidate reverse-applied completely. Adopted runtime remains TiKV
8d143e47eba26e2a33562961a98b4693f1e2f22a / TiDB02c3676b94f849db74e3019a5e27233629a96e2e
production; this report commit changes only documentation/evidence. Candidate
binary is explicitly experimental, not the adopted baseline for later work.
The constructor standalone symbol disappears; text grows5184bytes. Symbol
removal alone is insufficient to justify retention. Exact patch and all logs
remain reproducible. No constructor policy or alternate implementation remains.
