# N40: preserve selected protobuf regexp implementations

This checkpoint is validated locally. It does not complete the overall expression-unification task. No push, PR, release or cgo work is included.

The genuine wire decoder chooses a `PbBuiltin` before ordinary SQL name dispatch. Its display name can change without selecting a different scalar implementation. The original compiled-source path rejected these protobuf regexp owners and fell back to the scalar evaluator. The new source profile recognizes the selected `RegexpLikeSig`, carries the actual wire/effective declarations and original cache handles, and uses the already established N39 regexp phases inside EagerValuesNative. No host value algorithm, SDK operation, expression frame or decoder was added.

The original implementation at eb150863 was independently recorded and rerun before production changes. All 4,159,360 observations were pinned before implementation:

| Corpus | Records | Original digest |
| --- | ---: | --- |
| Phase, wire metadata, shape, alias and arity matrix | 3,870,720 | 0259457ADE53B559 |
| Full actual values in every real argument position | 286,080 | 001C38829809F571 |
| Long selections, context/cache and reuse histories | 2,560 | 40176B6F9BC54430 |

All actual records pass strictly, without fallback or resource exemptions. The preceding actual-source failure preserved the original full/phase digests while exposing Admission fallback; the long test stopped at its first refusal. Production changes did not alter any observation, count, digest or resource cap.

The fixtures use `distsql_builtin::pb_to_expr` and retain real `PbOrigin` on every decoded node. They cover wire field omission independently for the root and every argument, explicit integer/string/real/JSON and additional full-corpus field types, positive/negative collation IDs, four display names, actual wire literals and physical ColumnRef, installed real parameter/deferred SETVAR producers, nested selected PB calls, arities 0/1/2/3/4/9, both flags, empty/duplicate selections, late context panic, poisoned scopes, warm cache and 2051-row reuse. Omitted wire fields are decoded into the real default SQL declarations; they are never replaced with fabricated absent SQL fields.

For valid arities, the existing native regexp descriptor stores original optional field snapshots, real collation and original invocation cache handles. A PB owner always selects the named regexp value phases, regardless of a display name resembling the REGEXP operator. It omits the ordinary stored-name probe because the original PB dispatcher bypasses it. Native batch eligibility remains unchanged. When the original registered batch collects scalar row results, a borrowed metadata policy preserves its atomic publication instead of forcing contextual row publication.

Wrong arity is handled before any argument or regexp cache preparation. The existing header outcome frame uses a small new kind, sharing the original pure arity predicate/error constructor with `ScalarFunction::eval_regexp_like`. The original Suite cache getter and unwind guard still occur once, including wrong-arity and empty-row invocations. Genuine PB provenance is inherited only within the selected subtree. Signature, node kind, child count and effective declarations are checked against their captured origin, including the early header path. The previous general PB declaration guard is retained.

There is an explicit remaining root gap: the old enabled batch evaluator can interpret display aliases such as ABS, arithmetic, CAST_JSON and CAST_DECIMAL through specialized batch bodies that bypass scalar PB dispatch. Such a root remains Admission when the actual original static batch candidate exists; nested scalar PB calls retain signature selection. Four reference controls verify that these protected roots preserve the old result through the existing migration route. They are not counted as native equivalence or included in the 4,159,360 strict observations. Other PB signatures also remain outside this new profile.

Nine source checks pass, including 12,288 additional real wire flag-presence/unsigned/NOT_NULL/effective-type comparisons, six stale-origin refusals, a different-signature refusal, the four explicit legacy-root controls, seeded actual cache identity across context IDs 17/18/17 and two fixed 16KiB resource-failure-to-NULL recoveries. The reused N39 host's all-stage authentication, cancellation, callback epoch/panic and retained-capability guards are unchanged. Ownership reconstruction verifies the original scalar facade except shared arity extraction, the exact old native eligibility/registry, all old observers, all value kernels/hosts/Ready APIs and the unchanged SDK. Both rustfmt toolchains cover seven changed Rust files.

Final gates pass: complete frontend 2117 passed, exactly the inherited three failures, 95 ignored, all 576 batch checks passed and the unchanged aggregate 346 warnings. All 43 Projection checks pass, including the new actual wire-selected serial/parallel fixture. All six current Expand checks pass with original 64/B42FB8BE316E6C79, the fixed optional inventory remains 83088/0 refusals/0 names, and `make lint` exits zero. The new Projection fixture uses three display names, two source wire types, prepared parameters, both flags and 0/3/2051 rows, collecting both reference and actual chunks identically.

Commands from the TiDB root:

    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib pb_regexp_source_ -- --nocapture
    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib
    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-executor --lib projection::tests
    make lint

The final cached frontend binary ran the exact optional inventory with `--nocapture`; the final cached executor binary ran `expand::tests --nocapture --test-threads=1`. The workspace `python3 evidence/no-fallback/n40-final-check.py` reconstructs allowed source ownership, checks exact gate counts and runs both rustfmt toolchains.

The workspace evidence is under `evidence/no-fallback/n40-*`, including snapshots, pinned observers, original capture/rerun/source failure logs and an ownership checker. SDK ad73b11 remains clean and unchanged; the N16 SDK baseline remains applicable.

The bounded ordinary optional-call inventory is not a measure of PB/public-entry completion. Remaining PB signatures, legacy-specialized aliases, public scalar/AST routing, filter/join, Unistore, generic Admission removal, full service and final performance work continue under the overall task.
