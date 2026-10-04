# CASE: shared decisions without new profiles

Checkpoint **case-86**, after **coalesce-85**. Functional225/245, strict0, remaining20. Only frozen `case` is added; all224 previous objects remain byte-identical. Not M0–M6 completion or PR readiness.

## Reuse the actual conditional protocol

| Actual shape | Existing workers and demand |
| --- | --- |
| One or more WHEN pairs | Actual normalized condition → IfHeadNative. Computed Then evaluates one corresponding result; computed Else advances to the next actual condition. |
| Selected result, including NULL | Entire original report + actual selected identity → IfFinishNative. NULL stops immediately. |
| Final computed Else with real ELSE | Evaluate that ELSE under selected columns, then Finish with the original Else report. |
| No matching WHEN and no ELSE | Invoke CoalesceEndNative for a genuine empty remaining suffix. |
| Zero pairs with sole ELSE | Initialize real AST base if present, then evaluate the actual sole ELSE in original preparation; AnyValueNative returns its computed identity. |
| Zero pairs without ELSE | Initialize real AST base if present, then invoke actual empty CoalesceEndNative. |

No new profile, carrier, role, codec, general driver, budget rule or PB admission is added. There is no synthetic false/NULL condition, fabricated Else report, dummy NULL operand or host-selected answer substituted for a computed result. Sole ELSE is determined by static zero-condition shape, not a native SQL predicate; its original child error precedes parent pool refusal.

Three wire functions in `impl_control.rs`—`case_when<T>`, `case_when_bytes`, `case_when_json`—use the existing `native_if_choose_branch` after full Int nonzero normalization. Odd ELSE, empty result, selected-NULL stopping and original clone/to_vec/to_owned policies stay unchanged. Their bodies mechanically changed; no claim that they are byte-identical. Existing private ControlKind drivers are untouched.

## Native deletion and sequencing

`rust/crates/tidb-expr/src/tikv/case_control.rs` replaces three runtime selectors: AST `Expr::Case` in `lib.rs`, typed `ScalarFunction`, and PB CASE. A private `OnceCell<P>` stores only actual one-time initialization state, not an answer or origin marker. Initialization occurs inside the existing first preparation guard before first condition, sole ELSE or empty End.

Simple AST CASE still evaluates its base exactly once—even zero WHENs—and evaluates each demanded WHEN before exact Eq/Int1 matching. A NULL base does not skip WHEN evaluation. The SQL rewriter's existing cloned-base/per-WHEN behavior remains different and unchanged. Searched AST/typed use ordinary truth conversion; PB uses original warning-aware `logic_truthy`. A sole ELSE is a value, not a condition to coerce.

The outer scoped pack retains selected columns across the iterative chain. Later heads return their decoded outcome immediately; no continuation/context chain grows with the number of WHENs. First preparation still uses original columns and precedes one-shot creation where no capability exists. No live DAG owner or general default-NoColumns closure is claimed.

`if_control.rs` only factors existing private `decode_head`/`finish_in` helpers. Actual report bytes and selected nullable identity retain the previous IF boundary; its original bridge regression passes.

## Fold, return types and admission

`expr_util/fold.rs::case_when_handler` shares the IF chooser while preserving all surrounding rules: fold then eval_once, stop on nonconstant/evaluation error, continue on truth-conversion error, accumulate deferred flags, copy CASE Decimal metadata for a selected constant, and return the original expression for all-false/no-ELSE rather than inventing a new NULL fold. The existing generic ALL-argument null-proof path is unchanged.

Original SQL `wrap_case_branch` full-target casts remain. Mixed Decimal8,1/10,3 can yield1.500, and mixed Datetime0/3 can yield `.000`/FSP3. These arise from existing branch CASTs, not a new CASE or COALESCE-style stamp. Direct typed/PB CASE still preserves its original outer return conversion, unsigned interpretation and raw temporal/Decimal projection policies.

Six existing PB CASE signatures reach Shared: Int/Real/Decimal/Time/Duration/Json. CaseWhenString remains unadmitted; no SimpleSig is added. Legacy request timezone/warning authority, typed projection and SQL-only folding remain; infrastructure failures are not folded into NULL.

## Core evidence

[Exact commands/times/hashes](../logs/case-summary.txt), [manifest](../checkpoint.json).

Seven new tests all pass on first matching execution: one shared wire test, one closed-profile composition test, one bridge test, two frontend/PB tests, one compact six-signature legacy test and one SQL test. They cover selected NULL stopping, real reports, zero/sole-ELSE preparation, simple-base count including NULL base, late conditions/children under selected scope, original warning/error order and raw selected identity.

SQL54SELECT probes comprise12searched actual-column cases × vector0/1 × slots1/0 =48direct probes, including24true root refusals. First conditions are stored columns, not a prior function/Eq/CAST/filter that substitutes for the CASE root. Existing result-branch casts remain explicit. Five result families, early/later selection, actual ELSE, selected NULL and no-ELSE exhaustion are covered. Four positive-only dynamic invalid-regexp lazy/error checks and two positive-only simple-CASE filters complete the matrix. The simple filters can evaluate Eq C4 before CASE, so they are not zero-slot CASE-root or AST base-once evidence. No SQL zero-WHEN syntax is invented.

Ten locked, nonzero, single-threaded launches:8green,2known old full RED. CPP core1/wire2/local337+1ignored; native case_18, original IF regression1, gateway196+1ignored; legacy1 and SQL1 pass. Full expression1594/4old/94ignored and unistore218/1old/13ignored remain RED. No compilation failure, new execution failure, zero-match, interrupted test or fixture recording.

Entire full-suite failure sections are byte-identical to R88 final receipts after normalizing only numeric panic-heading thread IDs. Digests: `411274feba11e202735df5a8056c72babbc8cf9df2eab3258c670e9289a1ee95`, `2f19c9ad5338a86c48b895e3923c3e39454117e3410d47df5aa3fb4254ed7ce1`.

Source audit covers2CPP/9native Rust files: one new native module,19CPP/318native old test bodies byte-identical,2CPP/5native new tests. Pinned formatting and diff checks pass. H independently reviewed initialization, reports, scopes, fold policies and private IF refactor without a blocker. No Cargo/lock, Go/Bazel or generated changes. Agent-doc review confirms descriptive source-map/boundary updates only, with no new policy or stale source references. The unrelated untracked BUILD remains excluded; no PR is opened.

## Remaining acceptance

[Remaining review](remaining-acceptance.md):6core,8ordinary pending,6complex candidates—not20approved exceptions. Continue NULLIF while retaining eager operands, CAST/M2, IN/extrema/INTERVAL, actual request-root/default-NoColumns/live-DAG ownership and final cross-entry/deletion receipts.

Known catalog/EXP/duration/str_to_date/unistore failures and INTDIV/CAST/mode/JSON/vector/older Values gaps remain. Per-condition calls, Finish, packing/copies and metadata adapters are explicit costs. Workspace/lint/dev/bazel_prepare/release, exhaustive differential, TiFlash/FIPS, performance/physical heap/stack/OOM/allocator/zero-copy/dual-tzdata and complete Go-package transcreation are not claimed.
