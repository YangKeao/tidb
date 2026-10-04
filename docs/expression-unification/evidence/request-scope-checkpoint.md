# request-scope-82: borrow existing request authority

Functional221/245 and strict0 are unchanged. All221 prior family objects remain byte-identical; latest functional-family checkpoint is still from-unixtime-81. This is a bounded integration/bug-fix checkpoint, not completion of M6 or approval of24exceptions. See [remaining acceptance](remaining-acceptance.md).

## Ownership changes

1. `rewriter.rs` adds `ColumnResolver::literal_execution_context`, defaultNone and forwarded through `&T`. PlanScopeResolver returns its existing warning_context. DATE/TIMESTAMP/ODBC rewriting still computes literal text, captures timezone and captures modes in that order, then supplies the optional context to the existing `_in` roots. Only the old convenience wrappers become cfg(test); no parser/kernel/policy moves. Existing generic fold warning bookkeeping remains untouched.
2. `cophandler.rs::eval_shared` still evaluates the original Shared expression over the same MutRow and original request settings/warning sink. Available parent scope, or else parent execution's lexical scope, binds that child context through existing `with_columns`. Active child scope remains authoritative; absent parent capability retains the old path. No new owner, epoch, wrapper framework, SDK API, PB admission or error folding.

Compatibility scope: resource refusals now occur at the actual owner instead of escaping to a fresh one-shot, so nested Shared warning timing changes only when that owner refuses execution. Successful source semantics, child timezone/precision/flags and warning ownership are preserved. PlanScopeResolver still omits date_modes forwarding; SQL mode-empty zero DATE is still1292. Standalone before-owner preparation does not acquire a stale owner. Real DAG owner lifetime and other default roots remain open.

## Genuine before/after proof

All commands, counts, times and raw hashes: [request-scope-summary.txt](../logs/request-scope-summary.txt).

- SQL-before: zero-slot `SELECT DATE '2024-01-01' FROM ...` incorrectly returned typed Rows.
- Planner-before-retry1: zero-slot rewrite incorrectly returned a typed DATE Constant; successful owner/zone/FSP/diagnostic controls had already passed.
- Legacy-before-retry1 collects allthree leaks before asserting: raw parent scope and execution-only both escaped with `Ok(Int(1))`; a nested Shared child emitted1292 before the parent worker eventually refused. This distinguishes child escape from a later parent refusal.
- After production changes: planner1, legacy1 and SQL temporal-literal group5 pass. The new SQL test covers18SELECTs:12literal probes (six normal/six zero-slot), four plain-column dormant-owner controls and two unchanged mode-default refusals. Typed width/FSP/offset results are fixed from source, not output re-recording.

Two initial new-test setup failures were not defect reproductions. Planner's warning_count trap incorrectly denied existing `purpose.fold` bookkeeping (literal_text recursively rewrites its String, and successful Time folds again). It now returns0; temporal/clock/policy getter traps stay. Legacy's new CastIntAsString PB return type omitted flen: protobuf0 propagates into Char len0, truncating to empty and warning1406. Only this new fixture sets flen20; expected `1` remains from Shanghai1970-01-01 08:00:01 → epoch1 → literal layout1. Neither correction changes existing tests, expected SQL outputs or production policy.

Ten locked nonzero launches: two setup failures, three genuine defect reproductions, three green post-fix runs and two old full-suite RED runs. Full expression1582/4old/94ignored and unistore214/1old/13ignored match entire previous failure sections after numeric thread-ID normalization, with unchanged digests411274fe… and2f19c9ad…. No compile failure, interrupted run, zero-match or fixture recording.

## Review and delivery limits

Five native Rust files changed, zero new Rust files and three new test functions. Existing expression test modules, planner prior source outside the new hook, lifecycle prior source outside the inserted new test, and legacy original test module were byte-compared. Audit helper boundary assumptions were corrected (lifecycle insertion is not EOF; legacy adds a blank separator); these were helper assertion failures, not test/source regressions. One early source lookup used the wrong plan_scope.rs path; actual owner is plan_builder.rs. No original test body or oracle changed.

Pinned formatting and both diff checks pass. TiKV Rust/kernel/SDK, Cargo/locks, generated files, Go and Bazel are unchanged; no current TiKV Cargo pass is claimed for this docs/Plan-only side. Maintenance/architecture guidance describes the actual two seams, not new policy or a general M6 closure. Publish synchronized Plans to TiKV first, then pin that commit in TiDB.

No full workspace, make lint/dev/bazel_prepare, release, exhaustive differential, TiFlash/FIPS or physical/performance acceptance was run. Existing raw INTDIV, zero-date CAST, date-mode, JSON and metadata gaps stay explicit. Core conditional/CAST/comparison ownership and live request lifecycle remain next work; the goal stays active.
