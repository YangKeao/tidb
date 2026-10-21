# Optional metadata for the remaining common-value profiles

The remaining complete common-value owners can now retain genuine absent result
or argument declarations. Admission uses the existing common demand selector,
including its explicit session, user-variable and early-string owners; protobuf
origins and specialized lazy families retain their separate contracts. SQL value
bodies, context producers and the SDK are unchanged. No guessed FieldType or
replacement operand is introduced.

Two real differences were found and fixed during source comparison:

- `CONVERT_TZ(NULL, NULL, NULL)` with a declared result and absent argument types
  selected the common source, but preflight still inspected required declarations
  belonging to the duration specialization and panicked. Optional common calls now
  budget their actual common metadata. Fully declared specialization budgets keep
  the previous behavior.
- `COT(param)` with an absent result type and parameter `abc` returned the same
  FloatOverflow and output prefix, but omitted the original second parameter read
  used to render the error. The existing declared-result diagnostic survey and
  budget are now shared with the absent-result profile. The same SDK frame demands
  genuine diagnostic producers in the original order; no expression callback or
  second driver is added. Malformed common arithmetic calls retain their original
  common error path instead of requesting a numeric specialization.

Before production widening, the independent original executor captured/reran
**1713 name/arity profiles / 657792 cases / A94C463994D2D98D**. Four genuine values,
literal/parameter/deferred-effect/correlated producers, four absence masks, both
flags, normal/reversed/duplicate/empty selections and output prefixes are covered.
Source Admission was independently reproduced before widening. The full source
comparison is now unconditional and the original hash remains a fixed assertion.
The previous 686592 and preferred-owner 17808 cases keep their original hashes.

UUID, UUID_V4, UUID_V7 and valid RANDOM_BYTES signatures use actual OS entropy.
Their **1152** separate original contract cases were run twice before widening;
**936** cases reproduced Admission fallback. The source now has zero gaps while
preserving actual generation: output length, UUID format/version/variant, fresh
values per selected occurrence, NULL, invalid lengths, exact errors, demand
records and output prefixes. Independent random draws are not compared as fixed
bytes, and no entropy source is substituted. Invalid arities remain in the exact
deterministic matrix.

The existing direct user-variable checks now cover present and absent result and
argument declarations: 44 credential/reply rejections, callback panic and store
invalidation, consumed replies, actual SQL NULL, and resource rejection followed
by recovery on the same worker. Actual Projection covers 32 mixed optional roots,
including natural declared integer LENGTH with an undeclared parameter, MD5,
SHA1 and trimming, both flags and serial/two-worker execution.

Final validation:

- Frontend: **2002 passed, exact original 3 failures, 95 ignored; all 461 batch
  tests passed**. The original failures remain IFNULL collation, EXP, and the
  duration-FSP vectorized-operation case. None were filtered or rebaselined.
- Build warnings: **346 total, 131 frontend test warnings**, unchanged.
- User-variable directed tests: **11 passed**; Projection: **28 passed**;
  Expand: **6 passed**, original `64/B42FB8BE316E6C79` unchanged.
- Root `make lint`, dual pinned rustfmt checks and `git diff --check` passed.
- SDK ad73b11 is clean and unchanged; the N16 fresh SDK baseline remains valid.
- Owning workspace `evidence/no-fallback/n22-*` retains old source snapshots,
  original/rerun, both semantic differences, Source RED/GREEN, full test logs and
  `n22-final-check.py` ownership/baseline verification.

Other optional specialized/lazy/protobuf policies, public scalar and parsed-AST
entries, filters, unistore, generic Admission removal, complete service correctness
and final performance remain open. Continue task #4 without a checkpoint pause.
No push, release or final performance claim is authorized by this checkpoint.
