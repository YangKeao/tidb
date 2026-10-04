# IF: shared branch demand with original truth policies

Checkpoint **if-84**, after **ifnull-83**. Functional223/245, strict0, remaining22. Only frozen family `if` is newly credited; all222 previous objects remain byte-identical. Not M0–M6 completion or PR readiness.

## Shared decision and staged result

TiKV `components/tidb_query_expr/src/native_if.rs` owns `native_if_choose_branch(Option<bool>) -> NativeIfBranch`. Only Some(true) selects Then; false/NULL select Else. Its head, three existing wire IF functions and two native pure optimizer/proof sites reuse this rule. Existing private ControlKind drivers remain untouched; this is not a new universal compiler.

| Fixed profile | Actual input | Computed output |
| --- | --- | --- |
| IfHeadNative | Values, Int1, call1: actual normalized None/0/1 | OwnBytes always present: `[1]` Else for None/0, `[0]` Then for1 |
| IfFinishNative | Values, Bytes2, call1: original whole exact `[0]`/`[1]` report and actual chosen nullable identity | OwnBytes actual selected identity/NULL |

The native head restriction does not narrow wire IF: `if_condition`, `if_condition_json` and `if_condition_bytes` normalize their original full Int domain with nonzero before invoking the common chooser. Their original typed clone, JSON ownership and byte-copy behavior remain. Their bodies mechanically changed; old policies and tests did not.

Facade and direct-ready validation close role, shape, call count, canonical native condition, exact report and structural identity. Head NULL still invokes and returns a report. Selected NULL still reaches Finish. No unselected payload is encoded or supplied as a fake NULL.

## Native deletion and preserved boundaries

`rust/crates/tidb-expr/src/tikv/if_control.rs` exposes only crate-internal `eval_if_in`. It evaluates and converts the condition once inside existing preparation, using original columns. Only the decoded computed report chooses one callback under selected columns. The original report and actual callback result then go to Finish under those columns. The returned Datum is decoded only from the computed frame; no native cached answer replaces it.

Three native runtime selectors are deleted in `func.rs`, `scalar_function.rs` and `scalar_function/pb_builtin.rs`:

- AST retains its registry/arity gate; typed exact-three dispatch retains its old malformed-arity eager unsupported tail. No eager-value IF API is invented.
- Ordinary AST/typed calls still use `truthy_of`. PB still uses `logic_truthy`: String/Bytes use lossy numeric conversion and original truncate handling; other kinds use existing truth conversion. No wire rounding/int-conversion policy is substituted.
- Condition warnings/errors precede head admission. A warning-producing `0.4tail` condition remains true with one DOUBLE warning; zero slots refuse the head only after that warning. Strict condition errors still precede resource refusal.
- PB reads only condition and demanded branch. Missing condition fails before head; missing chosen branch fails after head; dead missing arguments and suffixes are untouched. Existing six signatures exclude IfString. Outer unsigned reinterpretation, return casts and temporal FSP are unchanged.

The two pure selectors retain different error policies: `expr_util/fold.rs::if_fold_handler` preserves fold-condition then eval_once and treats truth errors as Else; `expression.rs::try_fold_nullified_function` returns unknown on truth errors. These APIs do not gain a fallible C4 boundary or swallow infrastructure errors. Original metadata/deferred behavior stays outside choice.

Legacy has no IF SimpleSig. Existing Shared/PB dispatch already covers Int/Real/Decimal/Time/Duration/Json; `cophandler.rs` only gains a test. Its original request timezone, warning sink, typed projection and SQL-only error folding remain; infrastructure is not folded into NULL. Existing row/vector/filter execution reaches the migrated typed path.

## Domain, scope and budget

The selected identity preserves raw/noncanonical Decimal sign/coefficient/scale/storage/shape, full-f64 Float32, NaN/-0 bits, invalid UTF8/JSON, raw Time/FSP, Raw, sentinels and vectors. No extra math or semantic parsing filter is added.

Condition conversion itself remains entry-specific. Before running tests, source review established that current `VectorFloat32::is_zero_value` means `is_empty`, despite an older zero-vector comment. Empty is false; nonempty `[0,-0]` is true. The test pins that existing implementation; production is not silently corrected.

With a capability, original preparation runs within the existing guard. Without one, condition preparation still precedes the parent's one-shot owner. The head invocation ends before evaluating a chosen child; head → child → Finish receive selected columns, including when the child replaces the parked worker. No worker borrow crosses the callback. This does not create a live DAG owner or close all default-NoColumns roots.

Common ready preflight, including direct-ready, budgets the one-byte head and selected frame length (NULL0), actual report/value capacities and existing output postflight. Two complete calls and native packing are explicit costs, not a physical heap, cross-stage peak, OOM, zero-copy or performance claim. Older generic Values budget gaps remain untouched.

## Validation receipts

[Exact commands, times and hashes](../logs/if-summary.txt); [machine manifest](../checkpoint.json).

All8new tests pass on first matching execution: one SDK test, two local tests, one bridge test, two frontend/PB tests, one legacy test and one SQL test. The native `if_` filter explicitly executes all three new native expression tests; its one ignored old test is not counted as passed. No provider-derived oracle or fixture recording.

SQL54SELECT probes use a single stored row with true/false/NULL condition columns and actual branch columns. Five result families each take both branches; NULL condition also selects an integer or typed NULL. These12cases × vector0/1 × slots1/0 give48direct probes, including24true root refusals with no condition function/CAST/WHERE/ORDERBY substitution. Four positive-pool lazy invalid-regexp checks and two filters complete the matrix. Return widths and actual Decimal scale/Time FSP are source-derived.

Nine locked, nonzero, single-threaded launches:7green,2known full RED. CPP core1/wire4/local334+1ignored; native root9+1ignored/gateway196+1ignored; legacy1 and SQL1 pass. Full expression1588/4old/94ignored and unistore216/1old/13ignored retain entire failure sections byte-identical to R86 after replacing only numeric panic-heading thread IDs. Digests: `411274feba11e202735df5a8056c72babbc8cf9df2eab3258c670e9289a1ee95`, `2f19c9ad5338a86c48b895e3923c3e39454117e3410d47df5aa3fb4254ed7ce1`.

Source audit:11native/8CPP Rust files, two new modules,123CPP/519native old test bodies byte-identical, only3CPP/5native new tests. Pinned rustfmt and both diff checks pass. H independently reviewed production policies, report flow, scope and budgets without a blocking finding. No Cargo/lock, Go/Bazel or generated changes; unrelated untracked BUILD stays excluded.

Agent-doc review covers the updated architecture index and coprocessor guide: verified source paths and boundary descriptions only, no new policy or conflicting validation rule. No PR is opened.

## Remaining work

[Remaining acceptance](remaining-acceptance.md):8core,8ordinary pending,6complex exception candidates—not22approved exceptions. Continue CASE/COALESCE/NULLIF, ordinary CAST/M2, IN/extrema/INTERVAL, actual request-owner integration and final cross-entry/deletion receipts.

Known catalog/EXP/duration/str_to_date/unistore failures, raw INTDIV, zero-date CAST text, PlanScope date_modes, JSON_KEYS, vector truth and older Values gaps remain. Workspace/lint/dev/bazel_prepare/release, exhaustive differential, TiFlash/FIPS, performance/physical OOM/allocator faults/zero-copy/dual-tzdata and complete Go-package transcreation are not claimed.
