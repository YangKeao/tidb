# N48: selected protobuf CASE/IF/IFNULL in the shared value frame

Verified local checkpoint on baseline TiDB08ea69f5 and unchanged clean SDKad73b11. All listed gates are terminal. No push, release or overall completion claim; public callers, remaining PB/metadata contracts and final service/performance work continue.

## Original contracts and evidence

All19 actual selected signatures are covered: six CASE, six IF and seven IFNULL signatures. Display names do not select their kernels. CASE consumes condition/value pairs and an optional ELSE; CASE1 is only an ELSE, CASE0 exhausts the original empty coalesce kernel. IF and IFNULL have demand-time missing-argument errors, not a general arity header; suffixes beyond their actual3/2 inputs are unread. Their callback contexts are the same actual context as the caller. No synthetic scope or field declaration is introduced.

Twenty-one original files and their SHA256 values were archived at08ea69f5 before observers. Original capture, independent rerun and the actual pre-fix source route all observed these same31,552,896 records:

| Cohort | Count | Original fingerprint |
| --- | ---: | --- |
| Branch/arity/declaration phases |26101440|7D9A02EA2D6640A9|
| Full149 actual values at each real argument |2145600|A68A28030D1A0365|
| Long/reused selections |63840|0B890AAA8004AB55|
| Physical columns and nested producers |218880|7C73225868D55DB7|
| Actual binary-literal constructors |262656|DF2B19DF7EF0A885|
| Ten actual source field families |82080|207E93E5500D2ECD|
| Actual absent declarations in a genuine JSON consumer |2678400|424A4B2BB05CA1A9|

The pre-fix source had seven admission/fallback failures and no value, error, output-prefix or context-event differences. Seven immutable assertions were inserted, formatted and archived before any production extraction. No resource exemptions, eligibility skips, quota changes or fingerprint rebaselines were used. Optional-declaration cases include actual untyped constants/parameters/deferred values and return fields; they do not prove every malformed typed physical-column declaration is supported.

Pure extraction passed all seven fixed original assertions. The existing eight control-value tests also passed, including the original demand/identity witness and original source error behavior. These are documented as extraction regressions after sharing, not newly independent implementations.

## Shared implementation

The original CASE callback implementation and the value continuation share one pair cursor, using the existing native branch-choice primitive. Its initializer remains before all demands, even for empty CASE. Selected values go through the original owned-identity payload boundary with its original default allowance; exhausted CASE goes through the original empty coalesce kernel and its original diagnostic. Existing IF/IFNULL helpers and the underlying owned/exhausted algorithms are unchanged.

Actual PbLazyValues metadata carries the selected owner. NativePbLazy captures genuine argument ordinals, logical arity, display name, optional return field, actual integer-batch consumer and output origin. The boxed pending state records each consumed argument index with its capability: CASE may demand0,2,3 and cannot authenticate those as consecutive0,1,2 inputs. All held inputs are authenticated before copies and again after callbacks. The task is consumed before truth conversion, warnings, errors or panics. No store borrow crosses a context callback. Only the actual next condition or selected value is requested Fresh; missing inputs fail at their original demand point.

Original PB integer interpretation, result conversion, expression view and real legacy batch consumers remain separate stages. Real old unary/arithmetic consumers keep their1/2-input views even where the PB scalar owner has different demands. Genuine IF/IFNULL suffixes are not compiled. Original ordinary conditional/nullable/Boolean controllers, all other PB facades/table entries and original value/diagnostic algorithms remain unchanged. The same existing SDK driver and EagerValuesNative host protocol execute the new continuation; no expression callback, fake SQL declaration, nested evaluator or new SDK driver is added.

## Validation

- Original capture and independent rerun: terminal0, all seven fixed fingerprints. Pre-fix source: terminal101, exactly seven admission/fallback failures with those same fingerprints and no semantic differences.
- Pure boundary extraction: terminal0, all seven original assertions. Existing control regressions: terminal0, eight tests.
- Actual source integration: terminal0, all seven original assertions, no fallback or exemptions. The complete branch/arity cohort took602.67s and the long/reuse cohort148.07s; these are verification runtimes, not benchmark results.
- Four additional checks pass: actual direct consumers plus64KiB selection refusal/recovery;114 stale declaration/origin configurations; real CASE0→2→3 input authentication, exclusive callback store access, callback resets/two intentional caught panics,128 selected-value cancellations; ignored2MiB values, genuinely unsupported unselected children and ignored IF/IFNULL suffixes. A32KiB selected payload is refused at the existing16KiB direct host limit, followed by actual NULL/Int/NULL success on that same host. This is a separate finite-policy refusal/recovery check, not a claim of old uncapped native success equivalence.
- Fresh full frontend:2214 passed, exactly the inherited3 failures,95 ignored, all673 batch tests pass; aggregate warning count346 unchanged. The inherited failures remain pushdown_catalog::tests::ifnull_string_column_literal_uses_go_signature_and_column_collation, tests::builtin_info_json_math_source::exp and tests::builtin_math_misc_op_source::vectorized_builtin_op_func.
- Actual Projection:51 pass, including all19 signatures/five branch samples/three return fields/both execution flags/serial and two workers/0,3,2051 rows. Serial warning order is exact; parallel warnings retain every level/code/message with multiplicity, because original workers can interleave their shared warning mutex.
- Fresh executor Expand:6 pass and the unchanged64/B42FB8BE316E6C79 record. Fixed admission inventory:83088 cases/0 refusals/0 names, a bounded inventory rather than global completion.
- Root make lint passes. Dual2026-01-30/2026-08-22 rustfmt checks11 Rust files. Original21 snapshots, all previous facades/value bodies/witnesses and reviewed11 SHA256 values pass the ownership audit. SDK remains cleanad73b11; its applicable N16 baseline1148/exact inherited1/1 ignored/17 warnings and clippy171 total/89 expression warnings is reused honestly, not reported as a new SDK run.

Initial attempts are retained. A new fixture first imported ScalarValueRef from the wrong module (compile only); a new wrong-reply-index assertion then incorrectly expected BindingContract instead of the existing generic HostContract. Only those new fixtures changed. The first mixed Projection fixture itself panicked while copying the original malformed fixed column: an invalid selected byte value can leave the original fixed Int column with one byte per cell. The fixture now captures both original and actual consumer outcomes, distinguishes panic from SQL error, compares the exact panic payload and exact partial columns at the observed copy stage. All malformed cases, flags, workers and earlier50 tests remain. No production change, filtering, normalization or oracle rebaseline was made for this fixture correction. The complete frontend is unaffected by this executor-test-only change; all its source hashes remain exact.

Commands from TiDB root:

    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib pb_lazy_source_original_ -- --nocapture
    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib pb_lazy_source_actual_ -- --nocapture
    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib pb_lazy_source_ -- --skip pb_lazy_source_original_ --skip pb_lazy_source_actual_ --nocapture
    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib
    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-executor --lib projection::tests
    make lint

Source/execution logs and original snapshots are in the responsible agent workspace evidence/no-fallback/n48-*; the seven assertions are in evaluator/batch_pb_lazy_tests.rs. Overall public expression/AST/filter/join/Unistore entry closure, generic fallback removal, full service validation and final performance remain outstanding.
