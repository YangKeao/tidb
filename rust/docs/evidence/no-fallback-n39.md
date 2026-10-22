# N39: original regexp phases in the existing value driver

This checkpoint is validated locally. It does not complete the overall expression-unification task. No push, PR, release or Go/cgo implementation is included.

The original implementation at TiDB de1ed54f was recorded and independently rerun before production edits. All 2,251,200 observations were pinned before implementation:

| Corpus | Records | Original digest |
| --- | ---: | --- |
| Phase, declaration and effect matrix | 1,105,920 | 443B33688A2FD934 |
| All actual values and independent optional fields | 1,144,320 | 19332738CD338935 |
| Long selections, flag changes and reuse | 960 | EFE51D028E234646 |

The source failure preceding implementation reproduced both missing admission and an already admitted typed path omitting the original cache getter. Later strict comparisons reproduced the original scalar integer publication scratch buffer and a declared-string literal containing an actual integer still being refused. The implementation fixes these source policies. No observation, digest, count, source selection or resource cap was changed.

REGEXP operator evaluation demands both raw operands and their NULL gates before formatting either value. REGEXP_LIKE converts and checks each demanded argument before requesting the next, with an optional third flags argument. `batch_regexp_values.rs` represents these phases inside the existing EagerValuesNative SDK frame, carrying real optional source/result fields, original collation, and cloned handles to the original regexp invocation caches. The operator retains an authenticated raw capability until its second NULL gate. Shared pure conversion helpers in `scalar_function/regexp_value.rs` are also used by the original scalar facade; they contain no expression evaluation.

The original Ready regexp operation selector is extracted unchanged and used by the existing closed prepared-value API. This invokes the original direct value kernel and result router without a nested expression frame or new SDK operation. Cache capture precedes preparation, but poison/reentry admission follows preparation and the original late context ID callback. The already supported typed Regexp metadata remains intact, preserving the old seeded-cache identity test. The existing Suite cache binding and registered integer batch publication now preserve the original getter, atomic output and reusable Column state.

All 2,251,200 actual observations pass strictly without fallback or resource exemptions. Eight new checks cover phase/full/long original and actual comparison, every direct phase's stale/wrong-role/node/row/wire-NULL/wrong-index replies, retained raw capability authentication, cancellation at every suspension, context callback epoch changes and panic, original poison behavior under broker cancellation, and three fixed 16KiB resource-failure-to-NULL recoveries. A separate direct host check retains a 32KiB first raw operand under an unchanged 8KiB host budget when the second operand is NULL, without formatting or cloning that first payload. The direct callback test uses the broker's real cancellation-during-unwind ownership contract.

Source ownership reconstruction verifies the complete original scalar facade and Ready recipe router around the two pure extractions. All old observers, original common support/classification and argument policy, value algorithms, SDK sources and original typed regexp cache metadata remain unchanged. Both January 30 and August 22 rustfmt checks cover the nine changed Rust files.

Final validation: all eight new source checks and four existing matching checks pass on the final binary. The complete frontend has 2108 passes, exactly the inherited three failures, 95 ignored tests, all 567 batch checks passing and the unchanged aggregate 346 warnings. All 42 Projection checks pass, including the new serial/parallel regexp prepared-parameter fixture with both flags and 0/3/2051 rows. All six current Expand checks pass with the original 64/B42FB8BE316E6C79 digest. The fixed inventory now reports 83,088 cases / 0 refused / 0 names. This inventory contains a bounded ordinary-call fixture and does not cover every public/protocol entry. `make lint` exits zero.

The added Projection fixture collects both reference and actual chunks through the same range API, which owns a separate scratch buffer; the strict original source corpus continues to compare the complete Column directly. The first test failure was only inconsistent reference collection. The unused inner Consumed variant was removed; the original outer SqlDemand::Consumed still makes continuations terminal before callbacks, and the warning baseline is unchanged.

Commands run from the TiDB root:

    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib regexp_source_ -- --nocapture
    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib
    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-executor --lib projection::tests
    make lint

The final cached frontend binary additionally ran `regexp_source_ --nocapture` and `--exact evaluator::batch::tests::stateful_tests::remaining_optional_admission_inventory_keeps_original_behavior --nocapture`; the final cached executor binary ran `expand::tests --nocapture --test-threads=1`. `python3 evidence/no-fallback/n39-final-check.py` from the agent workspace reconstructs ownership, checks exact source gates and runs both rustfmt toolchains.

Workspace evidence is under `evidence/no-fallback/n39-*`, including original snapshots, pinned tests, capture/rerun/source failure logs and an ownership checker. SDK ad73b11 remains clean and unchanged; its previously established N16 baseline applies while this condition holds.

Public scalar/AST entry routing, filter/join, protocol-origin source contracts, Unistore, generic Admission fallback removal, full service and final performance validation remain open. The optional-metadata inventory is a bounded fixture, not an overall completion measure.
