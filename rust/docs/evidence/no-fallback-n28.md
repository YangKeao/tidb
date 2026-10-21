# Static metadata getters as zero-value-input sources

CHARSET, COLLATION and COERCIBILITY now retain their actual metadata result and
execute in the shared frame without fetching their argument's row value. Capture
uses the original `info_metadata_value` helper after the complete source byte gate.
The original helper and its missing-field behavior are unchanged. Field charset /
collation attributes remain distinct from the node's stored coercibility metadata;
actual absent declarations remain absent in the owning expression.

The source records original logical arity separately from value operands. Header
outcomes have zero value operands, so unused expressions do not become row-value
bindings. The original expression, its logical arity and true row schema stay
owned and authenticated. This also preserves prior header outcomes when their
unused arguments are unbound. Their original fixed hashes remain unchanged.
Root result coercion uses the existing present/absent result boundary. Captured
property representation, logical arity and the empty value signature are validated
before invocation. Copies and result work remain bounded, and task consumption
precedes the original helper/result phase.

Before production changes, independent capture and rerun each recorded:

- **77760 / E2A74FA0BA7D91F5** declaration/source observations: three getters,
  actual parameter/deferred setter/failing scalar/column/nested IFNULL shapes,
  eight absence masks, three return fields, six unused value pairs, callback
  faults, both flags, selections, prefixes and same-owner/context reuse.
- **21168 / 0FF4CB511A024679** stored-attribute observations: seven real field
  declarations including ASCII, GB18030 and unregistered UTF-8 names; seven
  argument shapes including parameter 99, deferred effects, absent correlation,
  column and damaged Decimal; three stored coercibilities; four absence masks,
  flags, selections and reuse. Stored collation information intentionally differs
  from field attributes. Original parameter reads are explicitly zero.

Source Admission was reproduced independently for both matrices. All **98928**
source observations now match with the fixed original hashes and no fallback or
refusal. The comparisons and hash assertions run unconditionally.

A further **3456 / 3C005CEFBC8C583D** observations cover ABS, COT, POW, subtraction,
ANY_VALUE, lazy branches and duration comparisons, including numeric metadata
strings and signed minimum. The data program omits unused value inputs while the
existing owned diagnostic survey retains the real logical expression and original
constant-folding facts. This preserves error expressions and comparison domains.
The fixture separately retains **216** existing numeric missing-type observations;
those are not claimed as closed by metadata-property support.

Direct checks cover present/absent result declarations, wrong logical arity,
unexpected value inputs, missing/wrong snapshot representations, 128 immediate
reuses and same-provider recovery after storage refusal. Actual Projection reads
real static attributes while skipping unavailable parameter/column 99 and deferred
writes. Both worker configurations preserve real before/after variable state;
the unread variable remains absent.

Validation:

- Frontend **2026 passed, exact original 3 failures, 95 ignored; all 485 batch
  tests passed**. Original IFNULL collation, EXP and duration-FSP failures were
  not filtered or rebaselined. Warnings remain **346 total / 131 frontend tests**.
- Projection **31 passed**; Expand **6 passed**, original
  `64/B42FB8BE316E6C79` retained; root `make lint` passed.
- Both pinned rustfmt versions over seven files, diff and ownership checks: passed.
  The original getter/default logic, other host bodies, common eager membership,
  argument policy and contextual bulk guard are unchanged.
- SDK ad73b11 remains clean and unchanged; N16 fresh SDK gates remain valid.
- Bounded registry inventory: **2010 refusals / 28 names / 83088 cases**, down
  from **2118 / 31** at N27. Counts describe that fixture, not global completion. Owning workspace `evidence/no-fallback/n28-*` retains original
  snapshots, independent RED/GREEN, nested/direct/caller and final gate logs.

GROUPING and remaining optional/typed/protobuf functions, public scalar/AST,
filter/unistore, whole-column phases, generic Admission removal, full service
correctness and final performance remain open. Continue task #4 through the overall
goal. No push or release is authorized by this checkpoint.
