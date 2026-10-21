# GROUPING preserves absent declarations and original demand

GROUPING now keeps its actual optional return declaration in the existing raw
value continuation. An absent argument declaration remains absent. The original
strict integer reader runs before planner metadata is required; NULL continues
to skip metadata. The planner's actual immutable Arc is shared, without copying
mark sets. Declared returns use the same bounded coercion as before; undeclared
returns preserve the actual UInt/NULL result. No default SQL type is introduced.

Invalid logical arity uses the existing zero-value-demand header protocol. The
original argument count is retained and authenticated; none of its argument
values or bindings is demanded. Only the original error literal was extracted
from ScalarFunction. The original grouping algorithm, integer reader, common
value classifiers, argument policy, and SDK code are unchanged.

Independent original captures and reruns preceded production changes:

| Scope | Observations | Original digest |
| --- | ---: | --- |
| Real metadata and optional declarations | 161280 | 3563DD9E5E3C3E43 |
| Invalid arity without argument reads | 20736 | 7020D56952346925 |
| All 149 actual value kinds after migration | 112644 | 398CB5D82320FEA4 |

Both original matrices reproduced source admission refusal before migration,
then passed without fallback. They cover all three grouping modes, absent
metadata, wide mark sets, actual/deferred/column/failing arguments, declaration
masks, integer/string/JSON output fields, callback faults, both batch flags,
selection duplicates and emptiness, appended output prefixes and owner reuse.
The header matrix asserts zero parameter reads and includes damaged unused
Decimal data and deferred variable writes. Digests are unconditional.

Direct protocol coverage now runs with actual Some/None result declarations:
invalid, corrupt, stale, wrong-role/node/row/occurrence tokens; terminal replay;
cancellation; context callbacks; oversized metadata and values. Both declared
and undeclared source results preserve huge shared metadata for NULL and recover
on the same suite after a real resource error. A test initially removed a
column declaration while expecting a grouping resource error; the original
column reader correctly fails earlier. The resource fixture retains that column
fact; missing-column behavior remains covered by the independent matrix.

Actual Projection covers real prepared parameters, conditional deferred writes,
all grouping modes and absent metadata, 65 marks, optional declarations, both
flags and worker settings, late errors, serial output prefixes and real session
variable state. The existing zero-demand Projection test also covers GROUPING
with an unread invalid parameter index.

Validation at this checkpoint:

- Frontend: 2031 passed, exactly the same inherited 3 failures, 95 ignored;
  all 490 batch tests passed and warnings stayed at 346 (131 frontend).
- Targeted grouping: 16 passed; existing zero-demand header regression: 4 passed.
- Projection: 32 passed. Expand: 6 passed, original 64/B42FB8BE316E6C79 unchanged.
- Root lint, both supported rustfmt versions, diff checks and ownership checker
  passed. SDK ad73b11 is clean and unchanged, so its N16 baseline remains valid.
- The bounded admission inventory now reports 1764 refusals across 27 names
  among the same 83088 scenarios; this is not a global completion percentage.

Detailed captures, source snapshots, SHA256 sums and the ownership checker are
in the owner's `evidence/no-fallback/n29-*` workspace files. The generic fallback,
other public execution entries, remaining metadata/PB gaps and final server and
performance validation remain separate unfinished work. No push or release.
