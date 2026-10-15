## Final decision: retain pure niladic PI

PI dense4.100ns/row versus actual original native67.078ns, median paired ratio
16.437x. The separately labeled current fallback ablation is630.365ns; its
153.75x ratio is not a native comparison. Every shape is bit/checksum-exact.
The change is one shared registry row and removal of a blanket arity guard,
with no new execution implementation or weakened semantic constructor.

Old14actual-code dense controls stay close or improve, except small Real
12.414→12.479ns and NULL-safe Decimal comparison74.899→74.979ns increases.
All shapes and ranges stay in JSON/raw logs; no universal speedup claim. JSON
and LOWER improvements are observations, not a proven causal benefit of PI.
M15 historical controls are separately preserved in the table above.

The requested pass ends at the documented next-work cost boundary. Remaining
63static signatures are explicitly unadmitted, and owning input/intermediate
vectors still limit Decimal throughput. The next clear common storage change
requires lifetime/ownership/budget interface work, while remaining function
families need their distinct semantic contracts. See remaining-work-boundary.md;
this is not a declaration that all functions or all SQL domains are optimized.
