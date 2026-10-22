# Arithmetic preserves genuinely absent argument declarations

The six non-PB arithmetic operations now carry each actual optional source
FieldType through the existing native descriptor. A missing field is neither
signed nor unsigned. Raw operands retain their actual Datum; typed arithmetic
and windows still require their genuine declarations. No SQL type is invented.
The existing operator bodies, scalar classifiers, argument policy, SDK driver
and scalar value computation remain unchanged.

Missing-source expressions use the existing owned numeric diagnostic recipes,
including literal-only calls. These recipes preserve sequential ordinary
reporting and the eager paired DIV report. Unused fixed strings are neither
formatted nor charged; actual recipe extents retain existing finite budgets.
Numeric argument reporting checks for a real field before any formatting or
parameter reads. Expression-owned real DIV shares its original required-source
check at the original demand point, including its panic. The literal-value API's
separate original real-type inference is unchanged and is not used to bypass it.

Independent capture, rerun, admission failures and fixed digests preceded all
production changes:

| Witness | Observations | Original digest |
| --- | ---: | --- |
| Modes, declaration masks, side effects and failure/reuse phases | 552960 | 20C336358DC26867 |
| All149 actual values in both parameter positions | 643680 | 1D094031497F3DBD |
| Raw Datum frames, both literal positions | 53640 | 7623E8AA8B9D6A50 |

All1250280 comparisons pass without fallback or exemptions. The direct witness
includes the huge malformed Decimal that previously exposed unused diagnostic
formatting. Every original sample and expected digest is retained. The matrices
independently erase left/right/result declarations, retain real caller schemas
and valid physical column cells, vary both flags, selection duplicates/empty
inputs, late errors/panics, zero policies, changed parameter values and reuse.
Missing Column declarations retain the original error instead of inheriting a
caller schema. Datum kind is compared separately from output column bytes.

A32KiB real parameter is refused under the existing16KiB retained limit before
precision work, then NULL succeeds using the same owner, context and limit.
Another48 cases cross1024 rows, encounter a missing-type real operand after1026
completed rows, compare the original panic/output/effect prefix and recover on
later invocations with alternating flags. Actual Projection covers all six
operations and declaration masks, both unsigned-subtraction modes, real deferred
variable writes, query/strict modes,0/3/2051 rows and serial/parallel workers.

Final validation: all eight new checks pass; frontend2062 passed with exactly
the inherited three failures,95 ignored, all521 batch checks passing and346
warnings. Projection37, Expand6 with64/B42FB8BE316E6C79, root lint, both supported
rustfmt versions on all13 changed Rust files and the ownership reconstruction
checker pass. The checker reconstructs original scalar_function.rs and
ops/real_coerce.rs, and verifies unchanged old observers/classifiers/SDK. SDK
ad73b11 remains clean, preserving its N16 baseline. The fixed83088-case
inventory reports1172 refusals across17 names; this is a local fixture count.

From the TiDB root, with RUSTFLAGS='' and CARGO_BUILD_JOBS=8:

    ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib arithmetic_missing_argument_ -- --nocapture
    ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib
    ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-executor --lib projection::tests
    make lint

Expand ran from the newly built executor binary with expand::tests --nocapture
--test-threads=1. The owner's evidence/no-fallback/n34-* files retain executed
source snapshots, independent captures/reruns, admission failures, final logs
and the reconstruction checker. Original proofs were not rebaselined.

Remaining optional/PB profiles, public scalar/AST/filter/unistore entries,
generic fallback removal, full-service correctness and final performance remain
unfinished. No push or release.
