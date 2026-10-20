# Early string signatures and bounded demand in the shared frame

Local checkpoint after TiDB `c0dab105`; TiKV remains unchanged at `7df75c9`. The overall no-fallback task remains in progress. No push or release.

`scalar_function/early_string.rs` captures immutable length signatures, trim direction, derived collation and the full LOCATE position declaration. Actual value processing reuses the original owners for LENGTH/OCTET_LENGTH/CHAR_LENGTH/CHARACTER_LENGTH, TRIM(2), LTRIM_WITH/RTRIM_WITH, LOCATE(2/3), INSTR, STRCMP and FIELD. Ordinary scalar calls and native source calls now share these owners. Metadata holds no expression or evaluator callback.

The existing `EagerValuesNative` frame uses a checked demanded-argument count. LTRIM_WITH/RTRIM_WITH demand at most the first two arguments, preserving ignored extras; missing arguments retain the original genuine bounds panic after preceding demanded values. Other families keep their existing complete eager demand. Every accepted capability still passes the original origin/stage/generation authentication and post-callback checks. No new SDK driver or SDK source changes were needed.

Early metadata is boxed only for actual early hosts and charged by preflight. Its allocation-free eligibility check runs before the metadata byte gate. LOCATE(3)'s extra full FieldType copy is also charged. Already closed typed trees containing these functions retain their previous phases and compiled call identities; new native-value parents gain the missing producers.

## Original evidence and retirement

The old scalar was active while recording, freezing, rerunning and independently comparing **129,855 / E1A91F4419B7C00E** cases. The corpus covers nineteen recipes, five argument declarations, eleven return declarations, nineteen raw carriers plus additional bounded text, three contexts, and malformed/extra trim arguments. All **nineteen actual native-value parent source gaps** still reproduced Admission/fallback during that comparison. Executed old scalar, candidate, tests, eager/host/controller and coerce sources were archived before retirement.

An additional validation gap was caught before completion: argument collation alone does not prove the result declaration's derived collation. The actual old branches were restored, then **3,540 / F069242CF30A6838** supplementary cases were captured, frozen, rerun and independently compared before restoring the shared dispatch. Four explicit result collations and real child assignments cover nullable arguments, LOCATE position conversion after all child effects, skipped extras, missing-argument effects and complete FIELD demand. Executed supplementary old scalar/candidate/tests/coerce sources were archived before that second retirement. Neither original tuple was changed.

Final shared-owner regression tests retain the frozen witnesses; they do not claim a fresh independent comparison after retirement. Evidence is under the agent workspace `evidence/no-fallback/`: `n13-original-and-source-red.log`, `n13-original-rerun.log`, `n13-independent-pairs.log`, `n13-supplement-{original,rerun,independent}.log`, and executed `n13-independent-*.rs` / `n13-supplement-independent-*.rs`.

## Reproduced defects and fixes

A finite raw Decimal with a large visible precision declaration, demanded through IF under LENGTH, reached the original strict formatter's bounds panic before the finite work policy could reject it. `n13-source-integration-and-work-red.log` records the real formatter panic. This is a visible-precision metadata fixture, not a claim that a valid one-megabyte Decimal payload was allocated. `coerce_str` now charges the existing shared string-work owner before formatting, just as the byte conversion already does. Outside a value-work scope this guard is inert; original coefficient-prefix validation and real malformed-prefix panic ordering remain in the shared owner.

That correction exposed a second defect: the actual runtime Resource error was preserved, but the blanket native-error category reported Native. `n13-expanded-fixed.log` records the failed Resource-category assertion. `BatchFailure::refusal` now recognizes the existing native-wrapped ResourceLimit class without rewriting its actual error. The same worker then accepts a NULL branch, and no variable assignment occurs on either rejection or NULL.

Broad gates found two integration regressions and prevented checkpointing them. Newly available STRCMP value producers promoted an already proved typed tree, changing its compiled call ordinals. The original typed closure is now retained and its existing identity assertion passes unchanged. An inline optional EarlyString also increased metadata for the unrelated calendar Projection until its unchanged finite budget rejected preparation. `n13-projection-resource-diagnosis.log` identifies original configuration 7; boxing only actual early metadata fixes that regression without raising the ceiling or altering prior cohorts.

The actual source matrix uses a fresh original reference per independent invocation after deliberately malformed output declarations. The legacy explicit adapter scope can become poisoned by an output-layout panic; the canonical source lane is still reused and preserves the real scalar result/panic and effects. This test envelope does not change valid SQL behavior or either frozen scalar witness.

## Final gates

`n13-front-final.log`: **1,839 passed, exactly the same three inherited failures, 95 ignored; all 325 batch tests passed**. The warning-title multiset remains exactly **346**. Inherited failures remain:

- `pushdown_catalog::tests::ifnull_string_column_literal_uses_go_signature_and_column_collation`
- `tests::builtin_info_json_math_source::exp`
- `tests::builtin_math_misc_op_source::vectorized_builtin_op_func`

Actual source coverage includes **129,855 cases with both flags** and **3,540 cases with four flag changes**, duplicate selection `[2,0,2]`, output prefixes, complete output storage and exact context state. Dead branches, empty selections and ignored huge-precision extras never format or execute their payloads. Formatter refusal is terminal, its Resource category is correct, and the same worker recovers with NULL.

`n13-projection-final.log`: **26 passed**. The appended heterogeneous suite includes eight early function shapes under actual-value assignment and CHAR parents, a direct column, a skipped unknown extra, both vector flags and workers 0/2. Final assignment state also matches. All prior cohorts and their context configuration are preserved.

`n13-lint-final.log`: `make lint` passed. `n13-final-check.py` passes exact prior failure/warning sets, both independent pre-retirement archives, byte-for-byte reconstruction of the retired scalar branches, unchanged original value algorithms, the single strict-work guard, final Resource/dead/demand checks, unchanged fixtures/datatype clock/SDK and both pinned formatters for ten Rust files. The unrelated untracked differential-test BUILD remains excluded.

From `/home/agent/tidb/expression-unification/tidb`:

    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib early_string_ -- --nocapture
    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib
    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-executor --lib projection::tests
    make lint

Original supplementary capture/rerun used the exact `early_string_supplement_original_collation_and_child_demand` filter while old branches were restored. The independent invocation then set `TIDB_EARLY_STRING_SUPPLEMENT_COMPARE=1` on the same frozen compiled test. Final shared regression was renamed after retirement. The read-only checker runs from the agent workspace as `python3 evidence/no-fallback/n13-final-check.py`; pinned rustfmt dates are 2026-01-30 and 2026-08-22, with `--check --edition 2021 --config skip_children=true`.

SDK full/clippy gates were not repeated because the SDK is unchanged. N9's applicable baseline remains 1,141 passed/one inherited failure/one ignored, and clippy 89 expression/171 total warnings. No Go/Bazel/module changes require bazel_prepare.

## Continue the overall task

FIND_IN_SET cache hits must skip list demand, and misses must preserve Row::empty and serialized once-only initialization across concurrent callers. Other cached/stateful functions, deferred/parameter/correlated leaves, PB/type seams, public AST/unistore/filter consumers and generic Admission fallback still require closure. Continue immediately; this checkpoint does not establish overall completion.
