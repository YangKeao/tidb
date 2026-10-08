# Request-owner lifecycle matrix

`request-owner-matrix-189` verifies the existing local lifecycle: Session owns begin/close, StmtContext/Columns borrow the live token, planner literals preserve it, and Legacy Shared inherits a selected parent while retaining child request semantics and active-child priority.

GREEN filters: Session zero-slot 1, Session one-slot 1, planner 1, Unistore Shared 1, StmtContext token lifecycle 4. Hashes are in `../logs/request-owner-matrix-summary.txt`.

R193's shared manifest change initially left TiDB `rust/Cargo.lock` stale; all four locked launches refused before compilation. The lock was synchronized and identical commands then passed.

R196 closes the remote Unistore DAG boundary without transporting a Session token: production `build_dag` creates a server-local one-slot lazy epoch, `RequestEvalContext` exposes it through `Columns`, and final context drop closes it. Standalone construction remains ownerless. See [DAG receipts](../logs/dag-request-owner-summary.txt).
