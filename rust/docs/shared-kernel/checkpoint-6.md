# Shared kernel checkpoint 6 — M1 numeric and invocation probes

This completes the implemented **minimum M1 probe set**, submitted for review,
not full expression compatibility or default integration. Previous fixed commits:
TiDB5a2f40623e6151a51cb46bd8e5372394f959f021 and
TiKV9293af18046b8447cae7bd9e1747bcd480403b3a. Both candidates remain based on the
original364aef2/548812e trees, with historical implementations and evidence intact.

## Behavior and implementation

Native Real/Decimal arithmetic now executes as typed instructions in the existing
SDK RPN loop. Real retains NaN/Inf/negative-zero bits in transport; only demanded
arithmetic checks finite results. Decimal carries its full coefficient, storage
scale, visible scale and declared shape. Fresh arithmetic clears declared shape.
The expression-only Decimal arithmetic helpers were extracted from original
TiDB364aef2, including nine-word overflow/truncation policies, hidden precision
and the leading-word overflow heuristic. No MyDecimal public storage/mutation
API was moved. PB's NotNan and fixed Decimal kernels are unchanged.

This adds two SDK modules (native_decimal and native_arithmetic), counted as
production code, including reused/moved algorithms. The original native copies
still exist for the old default route and independent test oracle; retirement
is future work and receives no deletion credit here.

The current numeric admission is signed LongLong plus/multiply, matching
Double/NewDecimal plus/minus/multiply/divide, and default VarString-to-Double
coercion. Real input columns now retain their complete domain through temporary
branch selections. Decimal columns, other mixed-domain casts, unsigned arithmetic,
FLOAT/BIT result conversion and prepared parameters are not newly admitted.

Scalar NULL demand is signature-specific: Real **plus** still evaluates the
right operand; other admitted numeric scalar operations skip it. Vector numeric
execution retains complete operand passes. A real original-entry comparison
caught and corrected an initially wrong test expectation about Real plus.
String-to-Real coercion executes as its own shared instruction before the next
operand, retaining the original warning policy and the warning prefix on failure.
Division precision and truncate/divide-by-zero policy use concrete native Columns
capabilities. Native source-shaped1690 diagnostics are prepared as metadata; no
runtime AST evaluation or function callback is used by the shared kernel.

Projection mode now reads the session vector switch independently of output
traversal: disabling vectorization preserves column-major RAND. Session variable
admission now explicitly rejects Bytes and non-default-collation String carriers
instead of erasing their metadata on SETVAR; default String storage is compared
as an actual Datum. This is bounded support, not full variable coercion.

Native REGEXP shares the existing SDK regex builder with an explicit diagnostic
policy. Sequential argument instructions validate UTF-8 and stop at NULL before
later argument effects; they do not move preparation into a second evaluator.
Empty/invalid pattern, invalid flags, invalid UTF-8, and missing RAND session now
retain native1105 and text. Existing PB regexp behavior remains unchanged.

## Verification and observed corrections

Final commands run from `tidb/rust` using `../../tools/cargo-tidb`, locked/offline:

```text
test -p tidb-expr --features shared-kernel --lib shared_kernel_ -- --nocapture
test --manifest-path ../../tikv/Cargo.toml -p tidb_query_expr --lib -- --nocapture
clippy --manifest-path ../../tikv/Cargo.toml -p tidb_query_expr -p tidb_query_executors --lib
```

The wrapper uses nightly2026-08-22 and one build job. Native:29 passed,1323
filtered. SDK expression library:429 passed,0 filtered, including existing PB
arithmetic/regexp tests and the previously added eager-PB-IF regression. Scoped
expression/executor Clippy completes with warnings; it is not full workspace
Clippy. Root `make lint` exits0; source formatting and both diff checks pass.
Official `make clippy` is **incomplete**. The first attempt installed its deny
script's Rust1.92 toolchain, then failed because PATH selected direct Cargo,
which cannot dispatch `+1.92.0`. A corrected rustup-shim invocation passed the
security/log/dashboard/Docker/license checks, then terminated during deny setup
when the running session was interrupted. Its final line is `Terminated`, not a
successful check result. The precise terminating actor/signal is not established
by that log. On resumption there were no remaining check processes. This does
not block publishing the review candidate and is not claimed as a passed gate.

The native suite includes original-expression/Columns/EvaluatorSuite comparisons,
not just comparisons between two new adapters: 40-digit exact addition; scale
and declared shape; hidden8/7+9/7 precision; 82-digit cancellation; six specified
range/policy probes; NaN and finite overflow behind IF;29 total tests including
192 additional Decimal operation pairs across word/scale/sign boundaries;
13 string prefix cases in warning/strict modes; duplicated selections; and the
actual Scalar-one-warning versus Vector-two-warning failure prefix.

Raw records are in Engineer workspace `evidence/shared-kernel-refactor/`:

| Record | Result / significance |
| --- | --- |
| session-boundary-before-2 / after |19 pass2 fail /21 pass; vector switch and stored carrier admission|
| wide-arithmetic-tests-2 / -4 |23 /24 pass; arithmetic then warning-prefix probes|
| regexp-rand-before |1 failure, original1105 versus candidate1139|
| regexp-demand-rand-before |2 failures, premature SETVAR and RAND10000 versus1105|
| regexp-demand-after-2 |26 pass after diagnostics and sequential argument guards|
| numeric-boundaries |28 pass1 fail; temporary selection rejected full Real vectors|
| numeric-boundaries-2 |28 pass1 fail; original Real-plus behavior disproved the new test assumption|
| real-plus-null-before / after |1 candidate failure returning NULL /29 pass after signature fix|
| checkpoint6-native-frozen |29 pass0 fail, final source|
| checkpoint6-sdk-frozen |429 pass0 fail, final source|
| checkpoint6-clippy |scoped expression/executor library check, completed with warnings|

Earlier compilation attempts are retained, including test API/type corrections,
a missing Md5 match arm during the regexp edit, and the obsolete diagnostic-text
assertion. The freeze test's intentionally caught mutation panic is not a failed
test. No failed/intermediate log is presented as the final result.

A source snapshot brackets the final native and SDK reruns:5525 tracked/new
Rust/C/C++/header/proto/TOML/lock files across both candidates, canonical hash
`6a8b5f6ac419862238d5d408b7c87af140c903c45644bde51c9fc040753dd54f`.
`snapshot-checkpoint6.py` verifies the exact post-run set against
`checkpoint6-source-snapshot.json`. This binds these reruns to candidate source;
it is not a fresh dependency rebuild, independent compiler, or SQL product run.
Native oracle source remains original except exposing the existing diagnostic
renderer as pub(crate); its dependency closure is the candidate's closure.

## Remaining boundaries

M0's245-family inventory remains candidate classification, not passed semantic
coverage. Before M2, bound additional algorithm migration, compatibility and
retirement costs. The prior22–34K known planning subtotal is provisional; the
913-line expression arithmetic extraction is now measured within the new source
total, rather than hidden or claimed as a free reuse. Full-scope10K is unproved.

Default integration, complete signatures/coercions/collations, all SDK packages,
full native tests, SQL/performance measurement, and deletion of old default
execution are outside this minimum proof. RAND state uses frozen-node identity
in the existing session capability; tests cover concurrent live instances and
cross-batch state, not allocator-address reuse after program destruction. No new
opaque value handles, host-resume protocol or extra expression interpreter was
introduced. No universal speedup or full project completion is claimed.

## Reproduction and retained evidence

All native probe inputs, the deterministic192-pair generator, and expected-value
comparisons are committed in `rust/crates/tidb-expr/src/shared_kernel.rs`. SDK PB
regressions remain in its expression crate. `checkpoint-6-validation.json` records
source-input and preserved-log hashes, test counts, observed test-binary hashes,
and the interrupted gate. The full5525-file local snapshot is reproducible:

```sh
python3 rust/docs/shared-kernel/verify-checkpoint-6.py --tidb . --tikv ../tikv
```

Add `--evidence /path/to/evidence/shared-kernel-refactor` to check raw logs on the
original machine. Source verification does not rerun tests or reproduce binary
hashes on another compiler. All test sources and this verifier are ordinary Git
files; no archive or hidden expected-value input is required.

## Complete change counts

Counts use Git additions/deletions against both original bases and checkpoint5,
including build files, inventory, reports and verification scripts. Category
"Docs" includes these audit scripts; none is omitted from the all-file total.
Use `count-checkpoint-6.py /path/containing/tidb-and-tikv` to reproduce.

| Repo / reference | Production + / - | Tests + / - | Build + / - | Docs + / - |
| --- | --- | --- | --- | --- |
| TiDB / original |1078 /1|2080 /0|1866 /91|8370 /0|
| TiKV / original |2431 /57|47 /4|0 /0|12 /0|
| TiDB / checkpoint5 |213 /63|712 /1|0 /0|337 /0|
| TiKV / checkpoint5 |1174 /28|0 /0|0 /0|0 /0|

Combined original source+tests: +5636/−62, net5574, additions+deletions5698.
Combined all files: +15884/−153, net15731, additions+deletions16037.
Increment from checkpoint5: +2436/−92, net2344, additions+deletions2528.
