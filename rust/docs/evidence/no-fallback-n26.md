# Optional operand declarations for raw and comparison owners

Nine raw operators (bitwise, shifts, unary signs and XOR) and eight comparison
operators (EQ/NE/LT/LE/GT/GE, NULL-safe equality and NULLIF) now retain actual absent
result/operand declarations. Present declarations keep their previous conversion;
absence preserves the actual result. The existing demand operations, value helpers,
authentication, cancellation and resource boundaries remain in the same driver.
Common eager owners and protobuf signatures are outside this widening.

Comparison operand facts now preserve optional eval types, unsigned flags, constant
folding and duration-column identity directly from the real child. A missing type
is distinct from a declared signed type: signed binary-literal normalization now
requires a present eval type and a clear unsigned flag, matching the original
argument metadata query. NULLIF retains its separate literal comparison policy.
The host accepts genuine absent operand facts while rejecting contradictory
unsigned/duration flags on an absent declaration. No default SQL type is invented.

Before production changes, independent original capture and rerun each recorded
**734400 / 13E31BEF014E40C9** observations. Seventeen valid-arity names, five real
parameter/deferred setter/failing child/column/nested IFNULL shapes, eight absence
masks, three result fields, ten value pairs including high-bit and oversized binary
literals and maximum unsigned integers, callback error/panic epochs, both flags,
selections, prefixes and same-owner/context reuse are covered. Admission fallback
was reproduced before widening. All source observations now match with no fallback
or refusal. The original hash is pinned and comparisons run unconditionally.

The previous selection matrix was factored into a shared test harness; its original
**388800 / ED53E168732558ED** hash and explicitly retained whole-column profiles
remain unchanged. No contextual-root bulk guard, typed closure policy or original
SQL/SDK value implementation changed.

An additional **121584 / 762EE08F41914785** source comparison covers all 149 actual
value representations, four genuine absence masks, both flags, selections and two
calls on the same expression/context. It exposed a resource estimate preceding the
original malformed Decimal coefficient check. Raw conversion now budgets bounded
coefficient storage for invalid UTF-8 before invoking that same original value
helper; normal scale-expansion bounds remain unchanged. The original validation
panic, effects, prefixes and reuse now match. This does not duplicate the SQL
conversion or bypass the valid large-scale resource gate.

The existing raw protocol/resource/reuse suite and unary warning callback suite now
run with present and absent result metadata. Comparison capability checks cross
present/absent result and operand facts. Stale/foreign roles and rows, oversized
copies, SQL NULL recovery, callback panic/invalidation and task consumption remain
terminal. Actual Projection preserves its original 28-root unordered state/cache
case and extends the ordered condition/operator mixes with four declaration variants,
genuine VALUES and real variable write/read state, under the original resource
allowances.

The bounded registry inventory now reports **3270 refusals across 37 names / 83088
cases**, compared with **4410 / 57** before the selection and operator checkpoints.
These are local fixture counts, not a global completion percentage. Wrong arities,
metadata properties, concatenation, regexps, numeric/typed and whole-column phases
remain in the inventory.

Final validation:

- Frontend: **2016 passed, exact original 3 failures, 95 ignored; all 475 batch
  tests passed**. Original IFNULL collation, EXP and duration-FSP failures were
  not filtered or rebaselined. Warnings remain **346 total / 131 frontend tests**.
- Projection: **29 passed**; Expand: **6 passed**, original
  `64/B42FB8BE316E6C79` retained; root `make lint` passed.
- Both pinned rustfmt versions over six files, diff and ownership checks: passed.
  The reconstruction check verifies only the explicit optional/facts adapters and
  bounded early-invalid coefficient allowance changed the host bodies.
- SDK ad73b11 is clean and unchanged; N16 fresh SDK gates remain valid.
- Owning workspace `evidence/no-fallback/n26-*` retains ten original source snapshots,
  capture/rerun, Admission RED/GREEN, full value comparisons, direct protocol,
  inventory and complete final validation logs.

Public scalar/AST, filter/unistore, remaining optional/typed/protobuf owners,
whole-column phases, generic Admission removal, full service correctness and final
performance remain open. Continue task #4 through the overall goal. No push or
release is authorized by this checkpoint.
