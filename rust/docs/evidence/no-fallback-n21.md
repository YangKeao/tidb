# Actual optional scalar declarations

The shared source previously rejected common scalar expressions that the original
executor could evaluate with absent result or argument FieldTypes. It now retains
those genuine `Option<FieldType>` values for the 52 preferred common value owners.
Native Bytes remains a transport capability, never a substitute SQL declaration.
The original value bodies, argument policies, context leaves and SDK are unchanged.

For an actual result declaration, conversion and enum/set interpretation retain
the previous behavior. Without a declaration, the genuine Datum is published
without adding a conversion. Specialized, typed and protobuf profiles retain their
separate admission checks. Preflight still bounds every genuine node and metadata
allocation; absent FieldType payloads are not invented or charged as present.

Before production changes, the original evaluator recorded and independently
reran **686592 / 5EC04B9A028B7011** observations. This covers four common owners,
eight absence masks, six actual producer kinds, all 149 Datum fixtures, parameter
errors/panics, deferred effects, correlated bindings, missing column declarations,
both flags, normal/duplicate/reversed/empty selections and existing output prefixes.
The 60 direct source guards initially failed with Admission fallback. After the
change, the full observations match original outputs, outcomes and context effects,
and every case has zero fallback/refusals. The comparison is unconditional in CI.

The broader registry covers all **52 owners / 17808 cases /
96FBFCB7F4B38305**, including valid/invalid arities, actual NULL/parameters and
independent missing root/argument declarations. Both summaries are fixed assertions.
The existing capability suite now runs for both present and absent declarations:
forged/foreign/stale/wire-NULL, role/node/physical-row/occurrence mismatches,
consumed replies, warning panic/store invalidation, 128 repeated and stale
cancellations, authenticated SQL NULL, resource rejection and provider recovery.

Actual Projection uses eight optional roots over real prepared parameters and
columns with caller-provided output schema. Both flags and serial/two-worker
execution match the original caller without serial fallback. Original Expand
regressions also pass unchanged.

Validation:

- Frontend: **2000 passed, exact original 3 failures, 95 ignored**; all **459 batch
  tests** pass. Aggregate build warnings remain **346**, including 131 frontend
  test warnings. The three original failures remain the original IFNULL collation,
  EXP, and duration-FSP vectorized-operation cases; none are filtered/rebaselined.
- Projection: **28 passed**; Expand: **6 passed**, original
  `64/B42FB8BE316E6C79` unchanged.
- Root `make lint`, both pinned rustfmt versions and `git diff --check` pass.
- SDK ad73b11 is clean and unchanged; N16 fresh SDK baseline remains applicable.
- Owning workspace `evidence/no-fallback/n21-*` retains old-source snapshots,
  original capture/rerun, source RED/GREEN, complete frontend and caller logs, and
  the final ownership/baseline verification script.

This closes one actual metadata gap. Other optional/protobuf families, public
scalar/AST/filter/unistore entries, generic Admission removal, complete service
correctness and final performance remain open. Continue task #4 without a
checkpoint pause. No push or release is authorized.
