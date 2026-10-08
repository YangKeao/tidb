# Next ASCII activation cut — source-derived plan, not activation

Round9: E independently inspected the current caller, real Columns wrappers, operation lifetimes and panic catchers. No implementation or test execution was delegated to that review. The validated public error envelope does not activate any evaluator; completed families remain0/245.

## Ordered implementation boundary

1. **Native capability surface, no default policies yet.** Publish the existing opaque policy/owner/execution/scope/owner-error shapes, keeping fields, workers, leases, ready values and TiKV types private. Add default-None `Columns::ready_value_scope` and `ready_value_execution`. Refine EV-r2's closure argument from `&dyn Columns` to the existing opaque borrowed `ScopedReadyValueColumns`: actual EvaluatorSuite/join/generated-column consumers require Sized contexts, so this avoids unrelated generic API rewrites. Both wrapper and unwind guard use the effective already-active scope when nesting. Its execution capability comes from that same scope, never an unrelated base context. Scope remains affine/Send/not Sync; shared statement context holds execution only. These changes require explicit file loans; no code in this proposal is installed.
2. **Persistent lifetimes.** Session holds one accounting root and starts epochs at `begin_statement_execution`, not on every `statement_context()` call. COW context clones retain the same epoch. Only its lifecycle owner closes it, including streamed-result close/cancellation. Projection's fresh parallel EvaluatorSuite values borrow that execution rather than create roots per chunk. A standalone executor closes its own epoch on reset/close, not a borrowed statement's epoch.
3. **Bind around real operation scopes.** Hoist the scope outside native batch/row/filter loops. At scalar calls it covers children, dispatch and return coercion, not only `func`. Public standalone operations can bind one C4-only execution if genuinely unbound; closed/poisoned/busy existing capability is an error, never permission to create a replacement. Explicit production policies still require source-grounded parent review; private test_policy constants are not defaults.
4. **Propagate through actual consumers.** FoldWarningContext keeps its warnings/zone and fold `.ok()?`; DEFAULT's Unsupported remap stays unchanged. CachedPlanRebuildContext also needs its captured planner_bridge/dml callbacks bound. SHOW row resolver and index-condition contexts have no base context to forward: bind their enclosing operation rather than fabricate statement defaults. Selection/join/aggregate/window/generated-column helpers must retain the same effective capability through the real row wrappers.
5. **Guards inside existing catchers.** Use `recover_worker_panic(|| scope.with_columns(native, |bound| native_operation(bound)))`, not the opposite nesting. Actual reviewed locations include projection:298, aggregate partial-worker:1542, probe-worker:157, TopN:722 and sort:757 (pre-change source anchors). Unwinding must quarantine before those catchers report recovery, including native return/output panics.
6. **Atomic activation and deletion.** Pass existing context from `func.rs`'s ASCII arm to the checked value boundary, and remove string_fn.rs's first-byte/NULL/empty native computation in the same coherent cut. Preserve existing arity/coercion/transcode order. Every admitted value, including NULL, uses the official C4 wrapper. No capability-based native/C4 dual path, replay or resource-error fallback.

## Error origins and phase

The current `ReadyValueBoundaryError::Kernel(LocalError)` loses phase. Capture the original error into the accepted opaque runtime handle at the known producer: Prepare for `prepare_evaluated_ascii`, Observe for `retained_storage`, Invoke for `eval_one`. Invoke does not certify entry into the kernel body. Preserve the primary error if cleanup also fails.

Frontend EvalError passes through unchanged. Pool, Scope and Bridge errors are not LocalError and must not be fabricated into one of its six classes or into a SQL numeric status. E recommends a separate native opaque adapter-failure envelope with typed origin/kind, private original cause and fixed1105/HY000 messages. Exact API/message policy and file loan remain pending; boolean unhealthy observations are adapter contract failures, not invented backend causes.

## Runtime gates still required

- Real AST, typed/manual scalar, deferred constants and EvaluatorSuite routes; nested ASCII, NULL, bytes, numeric and return coercions; official-wrapper witnesses.
- Multiple chunks and fresh parallel suites reuse workers under the same execution; prepare counts follow worker creation rather than rows/chunks.
- Dynamic identity through real recursive wrappers, effective-scope precedence and coherent execution identity.
- Actual catcher tests for native panic before/after ASCII and during return/output handling; verify quarantine before recovery returns.
- Session COW, close/reopen, cancellation and late old-worker completion preserve root debt.
- Actual public error paths for known phases and separate adapter origins, without a test-only public constructor; frontend errors win before admission.
- Frozen ASCII differential150 rows, source deletion, explicit-scope release performance and factory transient high-water acceptance. The old pinned Arc request receipt is not a substitute for these gates.

The next implementation should follow these concrete cuts, not build another generic resource-measurement framework. No default, API, family coverage or package-transcreation completion is implied by this document.
