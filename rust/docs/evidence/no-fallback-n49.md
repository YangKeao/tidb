# N49: selected protobuf CAST values in the shared driver

Baseline TiDB0ffc03dfcc / SDKad73b11. This checkpoint covers the43 selected Kernel::Cast signatures actually mapped by cast_types(), not every TiPB signature or all public callers. Overall task4 remains in progress.

The real selected owner now exposes PbCastValues, sharing the original NULL, JSON, actual result-field target construction and cast::eval_cast value body with its old PB facade. The existing eager host/frame consumes actual capabilities and runs the typed argument boundary; no new interpreter or nested SDK driver was introduced. Literal, computed and typed-context arguments retain their original phases. Parameter type inference is distinct from the actual static source Option<FieldType> passed to the final cast. Missing strict literal declarations produce their original error before copying even a2MiB unused value; physical missing declarations keep their original column error. There are no synthetic SQL fields or substitute function owners.

Actual scalar arity headers precede all children. Genuine legacy unary and arithmetic batch entries keep their separate original profiles. The new two-argument CAST/display-plus phase test exposed a preflight header versus real arithmetic bulk mismatch; both preflight decisions now recognize the genuine arithmetic profile. The old scalar arity contract remains unchanged.

## Independent evidence before production

Thirty original files were copied and verified at0ffc03df before adding observers. The original eager-host file, subsequently modified, is separately retrieved from the same already-executed immutable Git baseline; it is not described as one of those30 early copies. Every original capture, independent rerun and actual source admission/fallback RED yields the following identical count/hash pairs. All ten assertions were formatted and archived before production extraction; pure extraction and final actual source execution preserve them. There are zero strict-corpus exemptions.

| Cohort | Observations | Frozen checksum |
|---|---:|---|
| phases |2465792|1F3CA1870CD1D5FB|
| full149 |8611008|0B214D6A23760DFD|
| long inputs |144480|F5CE92D5D77D368F|
| physical/nested |495360|BFF076D62C9F3A1D|
| real binary literals/unsigned flags |297216|0D4706B892EE41F5|
| actual source families |185760|92E63C2709F42841|
| optional declarations/producer phases |3096000|6538DC12A562DC4D|
| real source/result attributes |6966000|9E0479763639F749|
| correlated/full149/optional fields |691956|3AACD14D71C42B0E|
| physical missing fields/arity precedence |9288|C02AAAACD0B2CECB|
| total |22962860|ten independent assertions|

All43 owners appear in the relevant cohorts. Samples preserve arity0/1/2/9, actual aliases and native batch profiles, unsigned and binary behavior, warnings/effects/read order, optional return/source metadata, parameter/deferred/correlated/computed producers, selections including empty, invocation reuse and long real input. The first phase fixture initially called a restricted wire-literal helper with unsupported UInt/Real/Decimal values. Only the new fixture gained real wire codec constructors; no sample was dropped. A further compile-only correction used the public encode_decimal_fixed with the value's actual storage shape. The first nine observed checksums stayed unchanged.

## Boundary and executor validation

Four new checks pass: actual native consumer/candidate behavior plus fixed64KiB selection-growth refusal and small/NULL recovery for all43 owners;258 stale metadata/origin checks; missing strict type before copying NULL/2MiB values with fixed64KiB metadata; and an independent original raw-return guard plus fixed-cap repeated refusal/NULL recovery.

The new recovery fixture initially reused an Int return declaration for every cast, intentionally creating a Duration-to-Int return conversion. Its already-existing guard requires135168 work bytes (2048*64+4096) and therefore rejects64/128KiB even in a fresh worker. This is not retained state after refusal. Recovery now supplies the real caller return field matching the selected target, retaining the same64KiB cap and8193-occurrence refusal. A separate check obtains the original raw selected cast result and proves that the old return guard independently rejects that actual Duration at the same64KiB allowance; actual execution then reproduces Resource/NULL/Resource/NULL recovery under the unchanged cap. This finite-policy check is not an assertion of success equivalence with the uncapped original and does not exempt any strict record.

The missing-type fixture's first16KiB metadata allowance rejected even its NULL control. Independent NULL and2MiB controls both fit64KiB; the final check uses that fixed allowance, still32times smaller than the unused value. Production limits and old tests are unchanged. Temporary diagnostic helpers were removed; diagnostic logs remain archived honestly.

Actual mixed Projection covers all43 owners, seven value classes, three return families, both expression flags, serial/two-worker execution and0/3/2051rows. The pre-existing observer of malformed fixed-column consumer panic and exact partial output is preserved; no malformed case is filtered or normalized.

## Final gates

Complete frontend:2238pass, exactly the prior3 failures,95ignored, all697batch tests pass, aggregate346warnings. Inherited failures are pushdown_catalog::tests::ifnull_string_column_literal_uses_go_signature_and_column_collation; tests::builtin_info_json_math_source::exp; tests::builtin_math_misc_op_source::vectorized_builtin_op_func. None was filtered or rebaselined.

Projection52pass. Expand6pass and unchanged64/B42FB8BE316E6C79. Fixed inventory83088cases/0refused/0names remains a local witness, not global completion. Fresh root make lint, both pinned rustfmt versions on nine files, reviewed source hashes, old observer prefixes, original cast body/table/non-cast-tail reconstruction and original-source ownership checks pass. SDK source remains clean atad73b11, so its documented N16 baseline is reused, not claimed as rerun:1148pass/exact inherited1/1ignored/17warnings; clippy171total/89expression.

Commands from the TiDB tree:

    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib pb_cast_source_original_ -- --nocapture
    # independently rerun cached binary, then actual RED before extraction
    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib pb_cast_source_
    # final complete frontend; deliberately no --nocapture
    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib
    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-executor --lib projection::tests
    make lint

Cached current executor Expand and current frontend inventory were run separately with their exact filters. Workspace evidence/no-fallback/n49-* preserves every log, original archive, pre-production pin, source checksum and checker. Existing N48 control observers are byte-preserved. No Go/Bazel source, protected unrelated BUILD file, or SDK source was changed. No push/PR/release is part of this checkpoint. Public scalar/AST/filter/join/Unistore entry closure, generic fallback removal and complete service/final performance remain explicit required work.
