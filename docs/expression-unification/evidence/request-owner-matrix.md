# Request-owner lifecycle matrix

`request-owner-matrix-189` verifies the existing local lifecycle: Session owns begin/close, StmtContext/Columns borrow the live token, planner literals preserve it, and Legacy Shared inherits a selected parent while retaining child request semantics and active-child priority.

GREEN filters: Session zero-slot 1, Session one-slot 1, planner 1, Unistore Shared 1, StmtContext token lifecycle 4. Hashes are in `../logs/request-owner-matrix-summary.txt`.

R193's shared manifest change initially left TiDB `rust/Cargo.lock` stale; all four locked launches refused before compilation. The lock was synchronized and identical commands then passed.

Remote Unistore DAG remains an explicit boundary: `DirectUnaryRequest` crosses process/API boundaries and carries no process-local execution token or server pool policy. `RequestEvalContext` and production `LegacyEvaluator::new` therefore remain ownerless. Adding a dead optional token or borrowing a Session epoch across remote transport would be unsafe. Closure requires a request-local server policy/owner with begin/close and remote/local separation across multiple crates.
