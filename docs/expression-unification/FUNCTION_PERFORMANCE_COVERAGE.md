# Function performance coverage plan

Updated 2026-10-16. This is a backlog and evidence index, not a declaration that
all functions have been optimized. Carrier coverage, kernel admission, actual SQL
lowering and measured performance are separate gates.

## Inventory and scope

The reproducible source audit inventories 308 static function-name registrations
(including aliases/internal names) from this checkout's `builtin_registry.rs`.
It also inventories 131 unique signature names found in the static pushdown
catalog or the paired TiKV batch registry. There are 68 registry-admitted eager
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

The eager registry contains Int/Real/Decimal PLUS, MINUS, MUL and ABS; the seven LongLong integer comparisons and same-domain Double/Decimal comparisons;
CHAR_LENGTH; LOWER/UPPER; two-/three-argument SUBSTRING; and ANY_VALUE carriers.
Exact signed integer arithmetic, static types, charset/coercion and metadata
restrictions still apply. A signature's registry presence alone does not prove
that every SQL expression spelling reaches this path.

Focused compatibility tests cover heterogeneous arguments, NULL, selection
order/duplicates, cross-window execution, carrier transport, resource limits and
shared/frozen output storage. Release performance evidence measures nine carrier workloads plus seven LongLong and fourteen Double/Decimal
comparisons, not every admitted signature or metadata combination. The retained M8/M9/M10
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


## Reviewed LongLong comparison slice

LT/LE/GT/GE/EQ/NE/NULL-safe equality share the existing eager driver. Physical
LongLong Int transport preserves unsigned bits; the registry permits unsigned
operands for this family while retaining signed-only results/arithmetic. The
older row contract remains signed-only. Raw Tiny/Long/etc and mixed evaluation
type coercions are not covered by this receipt.

The M13 report retains 56 independent signedness/operator/column-or-literal
correctness combinations, NULL truth/demand checks, reverse/duplicate windows,
protobuf encoding/decoding/evaluation and seven signed performance probes against
both a named current-code ablation and true frozen native. The generated CSV
cells remain source-inventory defaults; this reviewed receipt supplies evidence
without turning every metadata row into a coverage claim.

## Reviewed Double and Decimal comparison slice

The same seven comparisons now admit same-domain Double and signed Decimal,
with signed LongLong boolean results. The shared generated comparer kernels and
one batch driver are unchanged; the static catalog and typed PB decoder carry
all fourteen signatures. No raw FLOAT, NaN, unsigned Decimal or implicit mixed
carrier conversion is admitted by this receipt. String comparison still needs
derived collation review; temporal comparison needs type/timezone review.

Independent hand-ordered tests cover scale-equivalent decimals and differences
beyond f64 precision, signed zero, finite extrema/infinities, all NULL combinations
and 2050 selected occurrences in reverse/duplicate order. PB round trips cover all
fourteen signatures and NULL states. The M14 report retains current fallback and
batch ablations plus a true frozen native comparison over all fourteen SQL probes,
with all five selection shapes retained. This is no claim about casts or all SQL
workloads. Generated inventory cells remain unreviewed planning metadata; this
prose receipt states the exact reviewed scope.

## Reviewed primitive integer storage extension

Tiny/Short/Int24/Long now join LongLong on the existing eight-byte Int carrier.
The seven comparison signatures preserve unsigned operand bits/metadata; signed
arithmetic and signed results retain their previous contracts. Raw SQL integer
width is input metadata, not arithmetic result clipping: e.g. signed TINYINT127
plus1 returns a LongLong128. Year remains excluded because its native Datum read
has a separate signedness rule; BIT/ENUM/SET and arrays remain excluded. The older
row profile still accepts signed LongLong only.

The M15 receipt covers 112 type/operator/signedness combinations, four signed PLUS
extrema cases and cross-window selected occurrences, plus actual typed SQL PLUS
and LT workloads for all four widths against the current fallback and true frozen
native. No signature, driver, new arithmetic or temporary migration route is added.
Inventory signature counts are unchanged. This extends the earlier LongLong
receipt's raw type boundary, not its permission for unsigned arithmetic.

## M23 integer bit operations

Five complete signatures (BitAndSig/BitOrSig/BitXorSig/LeftShift/RightShift)
share the eager driver. Their inputs preserve signed/unsigned raw64 bits and
the result must be UNSIGNED for every operand flag combination. The catalog
now states Signed/FirstArgument/Always result policy rather than inferring it.
PB decode and encoding preserve the required flag; signed bitwise results and
unsigned comparisons are refused. Golden cases cover sign bits, shifts63/64/65
and negative counts, NULL,2050 reverse/duplicate selections. Measurement and
full validation are recorded in [the M23 receipt](logs/performance-m23-bitwise/summary.md); this metadata
count is not an SQL coverage percentage. The older row profile stays signed-only.

## M24 IS NULL admission

Seven IS NULL signatures share the eager mode, preserving non-NULL signed Int
results for NULL inputs. Datetime/Timestamp share Time; Int permits unsigned
input. JSON remains outside this SQL batch family because the Go catalog inserts
CastJsonAsString before StringIsNull. Do not count direct wire JsonIsNull as
native SQL coverage or bypass the cast. Existing schema/hybrid/array exclusions
remain; [the M24 receipt](logs/performance-m24-is-null/summary.md) retains actual native measurements, all controls and validation.

## M25 matching unary workers

UnaryNotInt preserves UInt raw bits with signed0/1/NULL results. ASIN/ACOS
share asin_f64/acos_f64 with native raw workers and filter NaN output to NULL;
tests compare exact native bits for signed zero, endpoints and domain edges.
NaN input remains excluded by the existing Real carrier. SIN/COS/ATAN and ROUND
are not admitted: their native/wire policies differ. This is66admitted metadata
rows out of131static names, not a whole-SQL coverage claim.

[The M25 receipt](logs/performance-m25-unary-math/summary.md) retains exact native bit checks, declared input domains, all performance controls and source hashes.

## M26 integer identities

One-argument RoundInt is admitted as an exact identity, and IntAnyValue now
preserves UInt inputs/results. The private result-sign contract states Signed,
Unsigned or FirstArgument independently of operand permission. Nested/literal
inputs, NULL and64bit extrema remain exact beyond f64's53bit range. Two-argument
ROUND stays outside this signature and retains native nonidentity semantics;
noninteger/scaled rounding policy differences remain unoptimized.67admitted
static signatures out of131 is metadata, not an SQL coverage percentage.

[The M26 receipt](logs/performance-m26-integer-identities/summary.md) retains the flag/identity proof, unsigned schema selection, broad old-workload regressions and longer confirmation.

## M27 shared fixed-output storage

[The M27 receipt](logs/performance-m27-fixed-output/summary.md) measures the common fixed-output buffer across14workloads, preserves all source/byte/ownership contracts and reports every slower control. No signature is added;67/131 remains metadata only.

## M29 rejected input experiments

[The three-code receipt](logs/performance-m29-fixed-input/summary.md) compares
M27, direct temporal input and common fixed-input import. Both candidates are
fully reverted: local gains coincide with4–10% Decimal losses (direct-only
comparisons about12%). Runtime and67/131metadata remain unchanged.

## M30 pure niladic PI and next-work boundary

Pi()->Real now uses the same explicit-row kernel entry, with no input columns
required. Only this pure deterministic zero-argument signature is admitted;
RAND and contextual/stateful calls remain excluded. Virtual2050rows, empty and
reverse/duplicate selections, nestedReal and resource refusals are tested.
Three pairs measure4.100ns/row versus true original native67.078, about16.44x.
Old14actualM27controls and all slow shapes remain in the receipt.68/131metadata
still does not express a SQL coverage percentage.

[The M30 receipt](logs/performance-m30-pi/summary.md) pins implementation,
validation, all3150main/225groups and840historical/60groups samples.
[The remaining-work audit](logs/performance-m30-pi/remaining-work-boundary.md)
lists all63unadmitted static signature names and the ownership/semantic work
needed next. This measured pass ends at that cost boundary; broad full-function
optimization remains unfinished.

## 最终同条件复测

[完整性能演进报告](PERFORMANCE_EVOLUTION_REPORT.md)覆盖64个benchmark workload，
包含58个batch和6个canonical fallback；逐一保留更慢项。它与68个准入
签名、308个静态函数名是不同集合，不能互相代替或算作SQL覆盖率。
