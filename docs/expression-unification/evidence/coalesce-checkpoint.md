# COALESCE: reuse nullable choice and a real empty suffix

Checkpoint **coalesce-85**, after **if-84**. Functional224/245, strict0, remaining21. Only frozen `coalesce` is added; all223 previous objects are byte-identical. This is not whole M0–M6 acceptance or PR readiness.

## No duplicate selection kernel

Every actual candidate invokes the unchanged `IfNullHeadNative`. Its computed Done identity returns the selected value; NeedSecond is consumed as a request to evaluate the remaining suffix. The frontend advances only a static cursor, never inspects candidate nullness to select a native answer.

The only new profile is **CoalesceEndNative**: NoArgs, zero SQL input slots, one call, OwnBytes **must be SQL NULL**. `impl_compare::coalesce_end_native` delegates the existing `coalesce_bytes(&[])`. This represents an actual empty remaining candidate list, including the existing internal zero-arity domain. It is not an invented final NULL operand and does not misuse IFNULL Finish.

Three existing wire loops (`coalesce<T>`, `coalesce_bytes`, `coalesce_json`) mechanically delegate each actual Option to `native_if_null_choose_first`. Done retains original cloning/byte copying/JSON ownership; NeedSecond continues; empty remains None. Their bodies changed, but original policies/tests did not. No new module, codec, carrier, ControlKind or universal compiler was needed in TiKV.

## Native ownership and preserved adapters

`rust/crates/tidb-expr/src/tikv/coalesce.rs` owns only orchestration. First actual argument preparation retains original columns. Its outer scoped head pack holds selected columns for the entire iterative sequence. Each later nested head callback immediately returns an outcome; no continuation/context chain grows with arity. Real exhaustion invokes End under those same columns. With no capability, first preparation still precedes one-shot owner creation; zero arity invokes End without a fabricated first callback.

The private IFNULL head receives a minimal `Borrow<Datum>` input generalization. AST/typed producers return owned values; eager values are borrowed directly. No NULL-prefix or dead-suffix clone/encoding is introduced. A selected original clone is replaced by ownership of the computed frame, not a cached native answer. Existing IFNULL timing/protocol is unchanged and its original bridge regression passes.

Three runtime native selectors in `func.rs` (AST/eager) and `scalar_function.rs` (typed) are removed. `expression.rs` pure COALESCE proof uses the same nullable chooser while retaining unknown-before-later evaluation and the original ret-typed NULL on exhaustion. No special COALESCE folding rule is invented.

**The original COALESCE-specific typed return-FSP binding remains outside selection.** Its static retdecimal -1→0 and type/kind gate are retained. Actual Time delegates the already shared setter: Date returns unchanged, including raw FSP9; other negative targets except-1 fail and retain the old FSP because the caller ignores the error; targets above6 clamp6. Duration binds raw FSP without validation/rounding. The temporal core/nanos do not round merely from this binding. Selected String reaches later generic conversion instead and can round/carry there. Head/End do not claim to own this presentation layer.

Actual identity remains structurally exact: noncanonical Decimal/storage/shape, full-f64 Float32/NaN, invalid UTF8/JSON, raw Time/FSP, sentinels, Raw and vectors. No extra math validation is added.

## Admission and resources

COALESCE has seven catalog signature facts but **no existing PB/legacy admission**. Production admission remains unchanged; the new legacy test proves rejection of those seven signatures plus CaseWhenString at decoding. It is not an executable legacy COALESCE receipt. SQL's existing minimum-arity constructor boundary is also unchanged, even though internal AST/typed/eager entrypoints already admit empty lists.

End's common-ready logical reply preflight is zero, including direct-ready; the result is checked to be actual NULL. This does not bypass the existing Bytes FnCall result-row accounting: NULL still needs offsets/bitmap before dispatch and actual storage postflight afterward. No changes to older NoArgs/Values resource policies are claimed. One call per evaluated candidate, plus End when exhausted, and native frame packing/copying are explicit costs—not a performance or physical-memory proof.

## Evidence, including failed attempts

[Exact commands/times/hashes](../logs/coalesce-summary.txt), [manifest](../checkpoint.json).

Eight new tests ultimately pass: one wire/End test, two local tests, one bridge test, two native selection/projection tests, one admission-refusal test and one SQL test. Bridge coverage includes a borrowed non-Clone carrier,130NULL candidates, a late nested C4 child under the selected scope, raw selected frames, errors and zero slots. This supports iterative continuation behavior, not a measurement of maximum physical Rust stack.

SQL54SELECT probes use three actual stored candidate columns per pure root: five result families with early/late selection, Int0 as a non-NULL winner, two-NULL prefix then third winner, all-NULL exhaustion, and Date versus Datetime return metadata. Twelve cases × vector0/1 × slots1/0 give48direct probes,24of them true zero-slot root refusals without a function/CAST/filter substituting for the root. Four dynamic invalid-regexp lazy/error checks and two filters complete the matrix. Datetime0 selected into ret FSP3 becomes `.000`; Date retains Date kind rather than silently converting to Datetime.

Two new expectations were wrong and corrected from source, **without modifying production or old tests**:

1. Initial local run335pass/1newfail/1ignored expected End to run with retained budget0. `expr_eval`'s existing `result_precharge` calls `bytes_min_storage_bytes(1,0)`, whose offsets/bitmap remain for NULL. The new test now checks zero-budget pre-dispatch refusal/zero invocations and separate default-budget successful NULL calls. No special budget bypass or allocator-derived magic threshold was introduced. Final local336/0/1.
2. Initial native root6/1 and initial full1590/5/94 expected Time target9 to fail and keep4. Existing `native_normalize_fsp` explicitly clamps >6; setters/constructors were not altered. Only the new tuple `(9,4,9)` became `(9,6,9)` with a source comment. Raw Date9 and Duration9 remain9; negative-2 still leaves old Time4. Final root7/0, full1591/4old/94.

All initial logs remain. Thirteen locked nonzero single-thread launches:8green,3containing new-test expectation failures,2only-old full RED. No compile failures, zero-match, interrupted test or provider fixture recording. Final CPP core1/wire1/local336+1ignored; native root7, IFNULL regression1, gateway196+1ignored, legacy-refusal1, SQL1 pass. Full unistore217/1old/13ignored remains RED.

Final full expression and unistore failure sections are byte-identical to R87 after replacing only numeric panic-heading thread IDs. Digests: `411274feba11e202735df5a8056c72babbc8cf9df2eab3258c670e9289a1ee95` and `2f19c9ad5338a86c48b895e3923c3e39454117e3410d47df5aa3fb4254ed7ce1`.

Source audit:7CPP/10native Rust files, one new native module,162CPP/516native old test bodies byte-identical;3CPP/5native new tests. Pinned formatting/diff checks pass. H independent production review found no blocker. The two compile.rs whitelist changes were necessary; lib.rs required no touch. No Cargo/lock, Go/Bazel or generated changes. Architecture-index/coprocessor guide edits describe verified ownership paths, not new policy; unrelated untracked BUILD remains excluded.

## Remaining acceptance

[Remaining review](remaining-acceptance.md):7core,8ordinary pending,6complex candidates—not21approved exceptions. Continue CASE/NULLIF, CAST/M2, IN/extrema/INTERVAL and true request-root/default-NoColumns/live-DAG ownership plus final cross-entry receipts. CASE preserves AST simple-base-once versus rewritten typed per-WHEN; NULLIF preserves eager operands rather than a desugaring that repeats/skips evaluation.

Known catalog/EXP/duration/str_to_date/unistore failures and INTDIV/CAST/mode/JSON/vector/older Values gaps remain. Workspace/lint/dev/bazel_prepare/release, exhaustive differential, TiFlash/FIPS, performance/physical heap/stack peak/OOM/allocator/zero-copy/dual-tzdata and complete Go-package transcreation are not claimed. No PR is opened.
