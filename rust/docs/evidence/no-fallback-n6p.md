# N6p — retire native frontend quotient argument programs

Status: verified local checkpoint. This checkpoint replaces native frontend MOD, /,
integer DIV and Decimal DIV adapters. Quotient source-root admission, full
whole/row argument integration and global alternate-driver retirement remain
open. No push or PR is authorized.

The frontend's plain quotient preparation owns the original MOD/division
coercion order, source unsigned reinterpretation and finite dense Decimal
conversion. It calls the N6o SDK value entries directly. Integer MOD/DIV keep
both signedness facts and the original result kind. Borrowed Decimal DIV asks
for each actual precision read using the shared classifier, then processes the
computed warning before its integer outcome. Its native frame adapter,
report decoder/finisher and re-export have been deleted. The common native
arithmetic NULL leaf also calls the actual shared value kernel directly.

The MOD/division ready-argument preparation ladder and obsolete result-packing
branches have been removed. Additive preparation remains a separately known
adapter. Explicit legacy recipe consumers remain unchanged. No Expression
callback, fake ready child or new RPN invocation was introduced, and native
batch eligibility and original source demand were not broadened.

Independent evidence precedes the caller switch. Three differential matrices
compared the new layer against still-active old adapters in the TiDB3ea93a67 /
TiKVe62891d source context, checking values, f64 bits, Decimal coefficient,
scale/storage/declaration, SQL errors and actual context observations:

- 49152 MOD/division cases:32 original Datum fixtures, two operators, four
signedness combinations, precision0/4/31 and strict/non-strict warnings.
- 784 integer MOD/DIV boundary, signedness and zero/error cases.
- 1452 Decimal DIV cases with independent precision sequences0/4,31/0,4/100,
unsigned result policy, strict warnings, overflow and zero divisor.

All51388 comparisons passed before deletion. Logs:
`n6p-quotient-plain-baseline-fixed.log` and
`n6p-quotient-original-witness-fixed.log`. The latter also captured FNV-1a over
complete old Debug results and context traces:3171bf8f5862af8f (MOD/division),
481ede75774e35f9 (integer),4af311db65938339 (Decimal DIV).
After retirement, tests compare against those independent constants rather
than invoking the same new helper as both expected and actual. Do not update
these expectations from the new implementation. Workspace evidence retains
`n6p-quotient-before-retirement.rs` and a reconstruction patch,
`n6p-original-adapter-baseline.patch`; apply the patch in an isolated checkout
of the paired baseline above to reproduce the original-adapter comparisons.
Two test-only import/scope compile mistakes were corrected before the passing
baseline; neither was a SQL regression or an admission RED.

The added budget test checks terminal resource failures, genuine NULL without a
zero warning, clean reuse, borrowed raw-zero priority over lhs validation,
and a million-scale fast Decimal DIV with no unperformed formatting or report
allocation. These are value-layer allowances, not hard whole-statement peak
allocation bounds. Provider import/copy/work admission still belongs to the
later canonical source integration. A private boundary test walks all four
actual resource causes (dense MOD/division preparation, finite-budget refusal
and Decimal DIV scratch refusal). Classification continues to describe the
original outer SDK Evaluation discriminant; the NativeDecimalError::Resource
cause remains owned and opaque, with no inferred phase or SQL overflow rewrite.
An initial new assertion incorrectly expected the outer ResourceLimit class;
it was corrected to the already-established boundary contract and strengthened
with actual cause-chain checks. Production classification was not changed.

Validation (agent workspace evidence/no-fallback/):

- `n6p-quotient-front-full.log`:1513 passes with exactly the four inherited
  failures and95 ignored; includes the three captured witnesses.
- `n6p-quotient-front-full-final.log`: preserved initial resource-test class
  assertion failure described above.
- `n6p-quotient-front-full-resource-fixed.log`:1515 pass, exactly the four
  inherited failures/95 ignored. Names mechanically match signed N6o. All151
  batch tests, three original-adapter witnesses, the resource behavior test
  and private four-cause preservation test pass.
- `n6p-quotient-projection.log`: all25 production Projection tests pass. Final
  follow-up changes are test-only and do not require another Projection build.
- `n6p-quotient-lint.log` and `n6p-quotient-lint-final.log`: make lint passes,
  including the final private boundary test addition.
- Pinned frontend nightly format checks for all seven changed Rust files and
  both git diff checks pass. All build/test handles are closed.

Commands from the TiDB root:

```sh
RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib ops::quotient::tests -- --nocapture
RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib
RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-executor --lib projection::tests
make lint
```

SDK production code and datatype code are unchanged; their N6o/N6k validation
was not rerun. Their inherited failures remain part of the final migration
audit. This checkpoint does not prove quotient root demand coverage, dynamic
sources, effectful multi-output, other callers or completion of the global goal.
