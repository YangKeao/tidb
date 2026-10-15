# Function performance coverage plan

Updated 2026-10-16. This is a backlog and evidence index, not a declaration that
all functions have been optimized. Carrier coverage, kernel admission, actual SQL
lowering and measured performance are separate gates.

## Inventory and scope

The reproducible source audit inventories 308 static function-name registrations
(including aliases/internal names) from this checkout's `builtin_registry.rs`.
It also inventories 107 unique signature names found in the static pushdown
catalog or the paired TiKV batch registry. There are 32 registry-admitted eager
signatures. These counts describe different sets and must not be divided to
produce an SQL/function coverage percentage.

The generated function and signature CSV files are planning checklists. Their
`pending` and `not_audited` cells deliberately do not infer correctness, lowering,
or speed from a name or metadata match. The script does not enumerate dynamic
extensions, every upstream Go overload, computed date-arithmetic signature tables,
or special AST node builders; those remain explicit inventory expansion work.
The static catalog is a pushdown-oriented subset, not a complete builtin universe.

Run from this TiDB checkout with its sibling TiKV checkout:

    python3 docs/expression-unification/evidence/audit-batch-function-coverage.py --output docs/expression-unification/evidence/function-inventory.csv

Review changes to both CSV files when registration/catalog/capability sources
change. The checked-in files pin the current planning snapshot; regenerate rather
than editing generated metadata cells by hand. Add semantic/testing/measurement
receipts to a reviewed ledger alongside them, not by assuming admission implies
all argument/charset/NULL/SQL-mode combinations pass.

## Current verified slices

The eager registry contains Int/Real/Decimal PLUS, MINUS, MUL and ABS; Int EQ/GT;
CHAR_LENGTH; LOWER/UPPER; two-/three-argument SUBSTRING; and ANY_VALUE carriers.
Exact signed integer arithmetic, static types, charset/coercion and metadata
restrictions still apply. A signature's registry presence alone does not prove
that every SQL expression spelling reaches this path.

Focused compatibility tests cover heterogeneous arguments, NULL, selection
order/duplicates, cross-window execution, carrier transport, resource limits and
shared/frozen output storage. Release performance evidence measures nine
representative workloads, not all 32 admitted signatures. The retained M8/M9/M10
reports identify each measured workload and historical/current-code comparator.
Time/JSON/Vector ANY_VALUE measurements prove transport behavior, not performance
of their date/JSON/vector operations.

## Work order and acceptance gates

The shared vector-kernel slice is complete at TiKV `992e28c` / TiDB `fed88c82`.
Bulk output transport is complete at TiDB `7c4e26dc`, paired with that TiKV commit.
Owned input import, intermediate results and Decimal representation conversion
remain measurable costs; further changes need profiles and preserved resource
accounting. Direct physical Decimal codec substitution needs a domain audit:
physical headers/padding and logical coefficient admission currently differ.

For eager coverage, review whole semantic families: first the remaining primitive
comparisons/bit operations, then numeric and string families, temporal/JSON/vector
operations, casts and mixed carrier/coercion signatures. Each family must specify
exact argument/result metadata, NULL behavior, collation/SQL mode and errors;
add to the one shared registry/driver only after those facts are checked. No
function-named executor or equivalent fallback implementation should be added.

For IF/CASE/COALESCE and short-circuit boolean expressions, design demand masks
(the selected rows that need each child), not eager evaluation of skipped values.
Prove skipped errors/effects remain skipped across NULL, duplicate occurrences
and nested controls. Existing scalar/control paths are correctness references,
not evidence that a batch mask implementation already exists.

For state/session/host operations, explicitly decide whether the operation is
statement-stable, row-ordered or effectful. Preserve required order; do not
promise eager batch acceleration for every function. Registration, preparation,
context ownership and resource/error behavior all need their own evidence.

A family is complete only with frontend lowering and actual kernel coverage,
selected differential cases, a release comparison against real frozen native
where available, and removal of temporary equivalent routes. End-to-end route
hit rate and SQL workload results are required before claiming application-level
performance gains. Full-function completion remains open while any inventory
row lacks an explicit reviewed execution mode and acceptance receipt.
