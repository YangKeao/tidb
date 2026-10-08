# Next session execution lifetime cut — source-only inventory

Round10 C independently inspected then-current session/executor source. No edits, commands, builds or tests were performed by C. This was a proposed dormant lifetime cut after the explicit value/capability API, not SQL activation or a second ExecPlan. **Round224 supersedes its rotating/current-epoch assumptions:** executions are now independent statement-owned states, peer admission does not invalidate them, and Session Drop closes every still-live execution. Paths are relative to `tidb/rust/crates/`; S denotes tidb-session/src, X denotes tidb-executor/src. Anchors are historical navigation hints, not post-edit guarantees.

## Ownership and actual hooks

- S/lib.rs:454–464,839,1013: session_memory is an existing stable-root precedent; unbootstrapped initializes it, Session::drop closes session lifetime. An explicitly configured runtime root should likewise stay stable, and Session drop must invalidate executions still held by contexts/workers.
- S/stmt_ctx.rs:1018,1134–1167,1197,1333: statement_context_ignoring repeatedly constructs authorities and query/DML contexts. It must carry the same execution, never begin or close one.
- X/stmt_context.rs:302–316,1827–1845: Arc/COW configuration creates context IDs. A context ID or variable generation is not an execution epoch. Clone/COW retains the same execution capability.
- S/lib.rs:2037–2059,2134,2200: non-stream begin→execute→finish exists, but each begin is NOT necessarily a new outermost execution. Closure needs a captured identity and an explicit outer owner.
- S/record_set.rs:141,210,248,261–274,331: EOF, Finish and Close differ. Detached StatementRecordSet has no Session borrow or its own Drop and can be closed after a newer statement starts. Close its captured epoch, never Session.current_epoch blindly. Attached Drop must converge on the same idempotent closer.
- X/projection.rs:295–306,381–390: close drops the receiver and does not join detached tasks. Old worker scopes must continue charging the same root until actual destruction and must never republish into a successor epoch.
- X/driver/physical_builder.rs:5447–5467: scalar subqueries create and close an inner root using the same context. Closing every operator/QueryRecordSet/root must NOT close the entire borrowed execution. An independent executor needs an explicit genuine outer close-owner.

ReadyValueScope is Send/not Sync; never store it or Arc<ReadyValueScope> in shared StmtContext/program state. Carry the cloneable execution, then create an affine scope per serial operation/task and bind it inside the actual panic catcher. The currently published execution API exposes close(); unique lifecycle-close authority is a caller discipline at this stage, not a Rust type-system guarantee. A stronger borrowed-capability split, if required, needs a separate explicit API decision.

## Entry traps to test before activation

- S/dispatch.rs:1271→1322: prepared PointGet begins, then Ok(None) fallback has no finish.
- S/lib.rs:2071: sandbox rejection returns early with `?`; do not leak a newly opened epoch.
- Nested begin/finish: SQL EXECUTE cached DML (prepared_statements.rs:356→dispatch.rs:1452 run_with_columns_using), and IMPORT (dispatch.rs:2551,2634 self.run). Inner work joins/borrows rather than rotating or closing the outer execution.
- S/dispatch.rs:1147 execute_statement is a public unwrapped body entry.
- Cancellation only signals; Next errors rely on outer Close/Drop. A materialized cursor closes its live execution when materialization completes, not on subsequent FETCH or cursor destruction.

## Real configuration, and the missing policy decision

Existing operator concurrency defaults are not a session pool-worker maximum: executor concurrency defaults5, projection defaults−1/non-positive means fallback (S/sysvar/catalog/concurrency.rs:112,196). Query memory defaults1GiB, permits−1 and CANCEL/LOG actions (memory_limits.rs:473); LOG is not a hard bound and this is not a dedicated pool quota. Chunk32/1024 counts rows, not bytes/workers. CPU pool available_parallelism().unwrap_or(4).max(2) (X/worker_pool.rs:218–238) is not SQL runtime policy.

No production-equivalent values were found for max_creating, worker-retained cap, creation allowance, pool/call retained bytes or steps/frame depth. The next dormant cut should take an explicit experimental policy; no test_policy promotion or guessed defaults. SET_VAR applies after begin and is restored at the next statement boundary (variables.rs:1427, warnings.rs:397); binding can override hints. A begin-time snapshot is not automatically the final statement policy. Dynamic reconfiguration needs a separate rule and cannot replace the root to erase outstanding debt.

## Proposed minimal single-owner loan

S/lib.rs, S/stmt_ctx.rs, S/dispatch.rs, S/record_set.rs, S/tests_core/lifecycle.rs, X/stmt_context.rs and X/lib.rs. The executor carrier/reexport can avoid adding a direct session→expr Cargo dependency. Projection lexical binding is a separately approved add-on, not permission to alter all row loops now. No SQL dispatcher, default, sysvar/server or current E/D source edit is implied.

Existing test anchors: statement_contexts_keep_one_session_memory_root; stats_load_wait_is_capped_and_failure_state_is_shared_by_clones; record_set_after_finish; query_cancellation_reaches_non_accounting_executor_batches; set_var_hint_overlays_one_statement; binding_set_var_overrides_the_query_hint_and_restores_the_persistent_value; prepared_program_is_shared_but_parameter_contexts_are_isolated; projection_workers_answer_the_serial_rows_in_order. Pair these with the current independent-execution debt/stale-close pool regressions.

Runtime evidence must cover repeated contexts/COW without beginning a replacement execution, nested/fallback ownership, stale detached Close not closing a peer execution, Session Drop closing every live execution, and late-worker debt release only on destruction. This document grants no migration credit.
