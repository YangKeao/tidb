# Session ready-value runtime lifetime contract — round11 narrow cut

This is the implementation contract for the sole ExecPlan, not a new plan, SQL dispatcher activation or completed family. Current baseline is native-capability-value-04 (TiDB258541a3/TiKVada4d28). Source inventory and earlier proposed hooks are in session-runtime-lifecycle-next-cut.md.

## Approved ownership

- Installation accepts an explicit ReadyValuePoolPolicy, not a shareable externally cloned owner. Session internally constructs its own private stable root once. `try_install_ready_value_policy` returns Result<bool,ReadyValueOwnerError>: true means installed; false means already installed or currently lexically busy. Both refusals precede owner construction. No replacement, defaults, sysvars or SET_VAR-derived guesses.
- Lexical busy state is tracked even before installation. It is distinct from a detached recordset's live execution. An outer guard sets/resets the marker and owns a non-Clone captured closer; nested EXECUTE/IMPORT operations borrow without rotating or closing. Prefer one AtomicBool rather than an incrementing nesting counter. Establish reset RAII before any failing begin/error conversion. Transfer only the closer, never marker-reset authority, into a returned recordset.
- Session retains the latest execution for context propagation within the lexical operation and separately tracks every independently live execution admitted on its root. Ordinary finish closes only its captured execution. Session Drop closes all still-live attached or detached executions before unrelated potentially panicking cleanup; closed handles are pruned on a later outer admission.
- StmtContext stores optional ReadyValueExecution, never ReadyValueScope or Arc<ReadyValueScope>. All constructors defaultNone. Repeated session context creation, clone and configuration COW retain the same admitted execution; only native borrowed getters are exposed. Context construction/clone/drop does not begin or close anything. Send+Sync remains required.
- A detached result's captured closer closes only its execution; an old Close/Drop cannot close a newer statement or reset its lexical marker. Real late-worker charges stay on the same pool root until actual destruction; no lifecycle transition clears debt or substitutes a new root.

## Admission and existing behavior

Existing run/open begin succeeds BEFORE runtime admission. Sandbox refusal is therefore pre-admission: it creates no new runtime execution and must not close an older detached result. Existing parameter binding errors, cached-DML with_plan(None), and metadata-only plan_bound_prepared_columns also are not newly admitted executions. Do not claim all public-method preflight is under an execution guard. Ordinary SQL parsing inside the run/raw-execute body is after runtime entry: its syntax error is post-admission, so the new captured execution is closed on return while any older detached execution remains independently live. It is not equivalent to sandbox/binding preflight.

The public unwrapped execute_statement gets runtime-only lexical ownership without adding native begin/finish side effects. Existing SQL warning/transaction/process-state behavior is preserved, including currently nested native begin/finish. PointGet Ok(None) drops only its captured runtime closer; do not fabricate a native finish. No blanket operator/QueryRecordSet closer: scalar subqueries can share the same context and outer execution.

Native startup failures use explicit `ReadyValueOwnerError::into_eval_error()` then the existing ExecError→DriverError path, retaining the original typed owner cause. An attempted From<ReadyValueOwnerError> for EvalError caused E0282 inference in unchanged builtin_ext/compare2.rs and ran ZERO tests; it was removed rather than annotating/refactoring the old builtin. Named conversion passed one actual test (1461 filtered). No public backend/Bridge conversion is added and no failure silently becomes None or restarts another root.

## Result windows

Normal Next, EOF and ordinary Next Err do not close the execution. An unwind-only guard is derived ONLY from that recordset's owning closer, not an arbitrary context/latest execution. Borrowed nested results have no close authority. Finish/retain take the closer before potentially failing native work so normal completion, Err and unwind dispose the captured execution. Finished/closed fast paths cannot strand a closer. Materialization closes live execution before retained-row replay; native replay state is otherwise unchanged. Detached Drop adds runtime cleanup only, not invented warning/transaction cleanup.

For deterministic tests, a cfg(test)-only TLS one-shot hook may panic in a real native recordset epilogue while its guard remains live. It substitutes no C4 worker, backend, recordset or public constructor and adds no production root field. QueryRecordSet's normal executor catcher can turn internal panic into Err; an epilogue test proves that epilogue's unwind window, NOT natural inner panic propagation through an existing catcher. Label evidence accordingly.

## Writer and validation boundaries

D owns only executor stmt_context.rs/lib.rs; parent owns adapter_failure.rs, docs and builds. E owns session lib.rs/stmt_ctx.rs/dispatch.rs/record_set.rs/tests_core/lifecycle.rs plus new private ready_value_runtime.rs after baseline. A reviews read-only. C found no eligible additional pure-forwarding wrapper in tidb-expr; FoldWarningContext is zone-only and must not gain speculative context fields just to create a patch.

Actual pre-session-change lifecycle baseline:14 passed/2064 filtered, exit0, with the new executor carrier compiled but no session implementation. Subsequent implementation acceptance requires explicit tests for installation/busy refusal, defaultNone, repeated contexts/COW, nested execution, real PointGet fallback, sandbox pre-admission, materialization, normal Next versus epilogue unwind, stale detached Close, and Session Drop. The real IMPORT FROM SELECT path is preferred to invented CSV fixtures. The former baseline expectations must not be re-recorded.

This cut supplies no affine operation scopes around business executors, no projection binding, no general Columns propagation, no default policy, no SQL ASCII switch/native deletion and no whole-package/migration/performance acceptance. The public raw execution close method still relies on caller discipline outside the private lifecycle owner; this is not a new type-level security boundary.
