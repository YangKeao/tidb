# Arithmetic preserves an absent result declaration

The existing native arithmetic descriptor now carries its genuine optional
result FieldType. Valid non-PB PLUS, MINUS, MUL, MOD, DIV and INTDIV calls with
no result declaration can use the shared source when both argument declarations
are present. Missing argument declarations remain a separate explicit gap.
No default SQL type, replacement expression or new value computation is used.

The original input-mode classifier is unchanged: PLUS/MINUS/MUL may attempt
integer operands when both source declarations are integer; otherwise the absent
result selects the original raw domain. Failed fast probes retain their fresh
second demands. Source fields, literal signedness, quotient promotion and all
operator bodies remain unchanged. Result coercion and the enclosing enum/set
view consume the actual option. An absent result uses the original BIGINT class
for integer overflow diagnostics.

The previously declared numeric diagnostic survey is shared with the absent
result preflight path. It preserves the six detached recipes, alternate operand
producers and exact re-demand schedule, with the same finite checks before
copies and callbacks. Arithmetic windows still require a genuine result field
at entry and completion; missing declarations do not create a window profile.
SDK, original scalar code, argument policy, driver, caches and original witnesses
are unchanged. A reconstruction checker reverses only option accessors, the
closed capability additions and the extracted metadata survey.

Independent original capture, rerun and source admission failures preceded the
production change; the original digests remain fixed:

| Witness | Observations | Original digest |
| --- | ---: | --- |
| Modes, side effects, callback/error profiles and reuse | 80640 | 80D3DF5FB7E2AFFF |
| All 149 actual values in both parameter positions | 107280 | 65E3A87908E51839 |
| Actual Datum frames, independent of output column bits | 8940 | 07DA5D6EBAB0FE48 |

The first two witnesses compare values, exact outcomes, output prefixes,
parameter read counts, warnings, variable writes, precision reads, division-by-
zero policy, both flags, ordinary/duplicate/empty selections and same-context
reuse. Their physical columns contain valid cells for the actual caller schema;
arbitrary actual values enter through real parameters or literals. Output
storage remains independent of the missing expression result declaration.

All 187920 parameter/phase observations run without fallback. Direct literal
sources compare 8880 actual Datum frames, including signedness and decimal
shape. The other 60 literal configurations contain the exact malformed negative
Decimal with coefficient [0,255,48], scale U32_MAX, storage scale 3 and declared
shape (-1,I64_MAX). They retain terminal source-metadata resource refusal under
the existing budget, rather than retrying an interpreter. All 8940 independent
original observations remain in the fixed digest; these 60 are not claimed as
successful native literal execution. All 60 refusals precede static numeric formatting. A declared-result control
selects a different carrier contract and can fail earlier at admission; it is
not used as an equivalence oracle for these absent-result literals.

A separate test refuses a real 32KiB parameter at a 16KiB retained-value limit
before precision/kernel work, then succeeds with NULL on the same owner and
context, at the same limit, after switching the batch flag. Actual Projection
covers all six operations, ordinary prepared parameters and genuine deferred
variable writes, query/strict modes, both flags, 0/3/2051 rows, serial/parallel
workers, full output columns, warnings, actual variable storage and exact errors.

Final validation: all seven new checks pass. Frontend2054 passed, exactly the
inherited three failures,95 ignored; all513 batch checks pass and warnings remain
346. Projection36, Expand6 with64/B42FB8BE316E6C79, root lint, both supported
rustfmt versions and the source ownership checker pass. SDKad73b11 is clean and
unchanged, preserving its N16 baseline. The fixed83088-case inventory reports
1316 refusals across23 names; this is a local entry fixture, not a completion
percentage. No original expected digest, fixture or old comparison changed.

Validation commands, from the TiDB root (cargo commands use RUSTFLAGS='' and
CARGO_BUILD_JOBS=8):

    ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib arithmetic_absent_result_ -- --nocapture
    ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib
    ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-executor --lib projection::tests
    make lint

Expand was run from the newly built executor test binary with expand::tests
--nocapture. Both supported toolchains checked all nine changed Rust files.
The owner's evidence/no-fallback/n33-* files retain snapshots, independent old
capture/rerun, admission failures, final logs and the reconstruction checker.

Remaining optional
argument/PB contracts, public scalar/AST/filter/unistore entries, generic fallback
removal, full-service correctness and final performance remain unfinished.
No push or release.
