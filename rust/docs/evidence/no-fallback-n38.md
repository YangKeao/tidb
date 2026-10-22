# Declared string parameter batches preserve row demands and publication

ANY_VALUE, LOWER, UPPER, IFNULL and COALESCE now admit their closed, genuinely
declared string parameter templates into the shared batch source. The original
batch dispatcher evaluates these calls per row, retains lazy child selection,
and collects the complete invocation before publishing results. The change
extends the existing row-value publication policy and its metadata eligibility;
it introduces no typed-once argument preparation or new value computation.

The helper borrows the actual root, authenticates its original registered string
batch target, requires at least one real parameter marker and flat, present
string declarations, and excludes PB origin, VALUES/grouping attributes,
deferred expressions, virtual and correlated sources. Existing literal and
ordinary-column operands can accompany the parameter. Other contextual batch
contracts retain their guards. No Expr, SQL field, source row or caller suite is
manufactured or cloned to satisfy admission.

Independent original capture/rerun and source refusal checks preceded production.
All287304 observations retain their fixed counts and digests, without fallback,
resource exemptions, output normalization or changes to expected data:

| Witness | Observations | Original digest |
| --- | ---: | --- |
| Original row demands, three collations and changed parameter/NULL reads | 123984 | 4E87BDA7A984C4DD |
| All149 actual values at each logical parameter position | 160920 | DCC6C0EF888189A7 |
| Cross-window errors/panic, duplicate selections and same-owner flag changes | 2400 | 523058AAC5E4A519 |

The matrices compare exact success/error/panic, complete column bytes and
published prefixes, warnings, conversion context, parameter reads, prepared
worker counts and reuse. They cover unary calls, IFNULL arity2 and COALESCE
arity1/2/3/4/6/9, ordinary/duplicate/empty selections, both flags, repeated
markers, columns/literals and live NULL changes. The long witness includes
0/1/3/1026/2051 selected occurrences and four alternating flag rounds.

The first source run passed all ordinary observations but exposed a cache
lifecycle difference after a late LOWER parameter panic: the original suite
cleared its worker through its invocation unwind guard, while the lazy fallback
binding retained one. The existing PreparedBatchSuite binding boolean now also
selects its original once-only cache binding for this genuine closed template.
No original digest was changed. The cache implementation, hosts, driver,
original row scheduling/publication body and value kernels remain unchanged.

A separate test rejects32KiB under16KiB for each of the five owners and recovers
with NULL on the same owner/context/cap. Its metadata checks keep missing-field,
deferred and literal-only templates outside the new exception. Real Projection
covers nine name/arity profiles, prepared parameters, four actual value triples,
both flags,0/3/2051 rows and serial/parallel workers with complete outputs and
warnings compared to the original. Production Projection is unchanged.

Final validation: frontend2100 passed, exactly the inherited3 failures,
95 ignored; all559 batch checks passed and aggregate warnings remained346.
Projection41, Expand6 with original64/B42FB8BE316E6C79, make lint, both supported
rustfmt versions and source reconstruction passed. The reconstruction reverses
only the new helper, its eligibility/publication exceptions and original cache
binding fact; the remainder of batch.rs is unchanged. Scalar computation,
argument/common classification, hosts, caches, old witnesses and SDKad73b11 are
unchanged. The unchanged SDK's N16 full/clippy baseline is reused.

The fixed83088-case inventory now refuses108 cases across two names, REGEXP and
REGEXP_LIKE. This is not overall completion: other public scalar/AST/filter/
unistore/PB entries, generic fallback retirement, full service correctness and
final performance validation remain open. No push or release. Original source
snapshots, capture/rerun/RED, unsuccessful first source log, final logs and the
ownership checker are in the owner's evidence/no-fallback/n38-* artifacts.

Run from the TiDB root:

    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib string_row_batch_ -- --nocapture
    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib
    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-executor --lib projection::tests
    make lint

The full frontend exits101 for the same inherited three failures:
ifnull_string_column_literal_uses_go_signature_and_column_collation,
builtin_info_json_math_source::exp and builtin_math_misc_op_source::vectorized_builtin_op_func.
