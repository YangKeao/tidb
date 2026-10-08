# Explicit native capability/value checkpoint: native-capability-value-04

This publishes a real evaluated-value API backed only by the closed TiKV ASCII worker. It does not activate the SQL ASCII dispatcher, propagate capabilities through business wrappers/session lifetimes, supply a default policy, delete native ASCII, or complete a function family. Completed families remain0/245, target221.

## Implementation boundary

E changed only ready_value.rs and its tests. Existing opaque policy/owner/execution/scope/owner-error types are public, as is the borrowing Sized ScopedReadyValueColumns; all fields and worker machinery stay private. Lifecycle/config methods retain ReadyValueOwnerError. `ReadyValueScope::evaluate_ascii_value(&Datum)` delegates the original checked value boundary and returns native EvalError. Inputs must already have undergone argument evaluation, arity checks and context-specific casts/transcoding; native return coercion remains the caller's responsibility. No runtime/execution/default is synthesized and no error falls back to native computation.

Columns has two pure optional borrowed capability queries, defaultNone. ScopedReadyValueColumns forwards63 ordinary methods and explicitly overrides those TWO queries. Already-active scope wins even if stale/poisoned; scope, execution and body guard agree. Before successful capability discovery the requested scope is guarded; getter panic conservatively quarantines that requested scope only, not an unknowable hidden scope. Effective guard is armed before discovery guard is disarmed with no fallible gap. No pool/layout/ledger algorithm changed. Inside the callback, evaluate through the effective scope returned by the bound Columns capability, not through a separately captured fallback handle: a different explicitly called scope is a different operation, not implicitly covered by this binding's guard.

Five actual error-producing call sites capture the original LocalError once: factoryPrepare1, retained_storage Observe3, eval_one Invoke1. The native mapper moves the existing handle, preserving primary cause/phase when cleanup also fails. Invoke means the API was invoked, not that its kernel body ran. Observe is source-site/structural coverage only: no natural Observe LocalError execution is claimed.

D changed only new adapter_failure.rs and executor terminal mapping/tests. Pool/Scope/Bridge original causes are retained in a separate private Arc with typed native class/origin, same-capture equality, Clone sharing and cause-redacted Debug. Nine fixed messages use1105/HY000 and the existing SQL evaluation-origin rule. No public raw constructor, backend type, generic From, source/downcast or invented LocalError is exposed. Frontend EvalError passes through unchanged. Parent owns context/lib/mod exports and runtime_failure documentation.

A independently reviewed the final eight-file source cohort, checking its manifest8/8 at both start and end. No additional concrete source finding: guard handoff, native API visibility, effective identity, five capture sites and unchanged pool/ledger logic were verified. Old resource/count/retirement assertions were not relaxed; raw-cause and Scope assertions were mechanically adapted and strengthened. This was read-only source review, not a second execution of the tests or observer.

## Actual commands and results

From `/home/agent/tidb/expression-unification/tidb/rust`:

```sh
/home/agent/tidb/expression-unification/tools/cargo-tidb test --locked -p tidb-expr --lib tikv:: -- --test-threads=1
/home/agent/tidb/expression-unification/tools/cargo-tidb test --locked -p tidb-executor --lib driver::errors::exec::tests::public_ascii_value_ -- --test-threads=1
/home/agent/tidb/expression-unification/tools/cargo-tidb test --locked -p tidb-executor --lib driver::errors::exec:: -- --test-threads=1
/home/agent/tidb/expression-unification/tools/cargo-tidb test --locked -p tidb-expr --lib -- --test-threads=1
/home/agent/tidb/expression-unification/tools/cargo-tidb check --locked -p tidb-session -p tidb-exec -p tidb-unistore --lib
```

- Shared-runtime expression filter: **107 passed/1 ignored/1353 filtered**, exit0. Includes37 regular caller tests (seven new public-path cases),8 new adapter tests and8 runtime-carrier tests. The seven public cases actually cover real C4 NULL/raw-byte/signed-result computation and reuse, Sized/dynamic scope identity, conflicting foreign execution, effective-only body-panic quarantine, requested getter-panic quarantine, invalid active scope without fallback, actual Prepare/Invoke capture and original frontend errors before zero-slot/closed/poisoned admission. Existing primary-failure test also checks the same opaque capture and phase through cleanup and mapping.
- Two **actual public value producer→terminal MysqlError** tests:2 passed/1476 filtered, exit0. Both use public checked policy→owner→execution→scope→evaluate_ascii_value(Int65). A valid one-byte worker cap yields actual C4 Prepare ResourceLimit; a legal zero-slot pool yields native PoolResource instead. Returned original EvalError moves through ExecError/Driver rendering; exact class/phase or adapter origin,1105/HY000/message and from_evaluation=true are asserted. No private constructor or test factory participates. This is not SQL parsing/dispatch or network-packet transmission evidence.
- Complete renderer filter: **9 passed/1 unchanged old Sequence-origin equality failure/1468 filtered**, exit101. Complete failure block matches the prior published checkpoint after only thread-ID normalization and the known3-line added-arm offset365→368. Old expected data is unchanged.
- Full expression comparison: **1363 passed/4 unchanged complete failure blocks/94 ignored**, exit101,1461 discovered. Only thread IDs were normalized; all four remaining bodies equal the prior published checkpoint.
- Session/old-exec/unistore library checks: exit0, compile only. Whole-workspace/runtime validation is not claimed.
- Parent ran the pinned Aug2026 rustfmt on all8 Rust files, then --check and git diff --check successfully. `logs/native-capability-first-source.sha256` remained8/8 identical through all validation. E's earlier formatter command-selection failures and formatting-only check were not builds or tests; no failed test/fixture expectation was re-recorded.

## Current pinned allocation observation

From `/home/agent/tidb/expression-unification`:

```sh
python3 tools/pool-arc-runner.py --binary target-tidb/debug/build/tidb-expr/1a66296e036585b2/out/tidb_expr-1a66296e036585b2 --observer tools/pool-arc-observer.so --output logs/native-capability-arc-final
```

The unchanged observer/runner actually ran on final TEST ELF dec586c4585ac6de56c271fad1ce17a3717b977a811272615d8a2dbffd4df4b4. Eight fresh processes each passed one fixture and observed malloc192/matching final free, valid inherited controls,14 markers/16 events, clone/empty/gap/foreign0. Same-ELF missing-marker negative ran one normal test but was rejected86. Fresh receipt/cohort files bind current sources, artifacts and actually resolved dynamic libraries. The SO was reused and hash-verified, not rebuilt this round. This is only the exact pinned request basis; handles/coercion/native error carriers remain outside the pool ledger. No portable ABI/usable/peak/factory-high-water/physical-OOM guarantee follows.

## Remaining work

Actual session/root ownership, close tokens, nested/fallback lifetimes, real recursive wrapper propagation, guards inside business panic catchers, statement/standalone policy decision, SQL entry activation, native deletion, immutable150-row differential and release performance remain open. Source-only lifecycle traps and the proposed seven-file dormant cut are in `session-runtime-lifecycle-next-cut.md`; its inventory is not implementation acceptance. make lint/clippy and final M6/whole-workspace gates have not passed. No package-transcreation or PR-ready claim is made.
