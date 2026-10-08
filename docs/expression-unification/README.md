# Expression unification experiment

Current paired checkpoint **request-owner-matrix-189**. Functional Demo: **240/245 (97.96%)**, strict **0**.

The local request-owner lifecycle matrix is GREEN: real Session SQL zero/one-slot behavior, StmtContext token lifecycle, planner literals, and Legacy Shared inheritance. The TiDB Rust lock is synchronized with the shared crypto manifest. [Evidence](evidence/request-owner-matrix.md) · [receipts](logs/request-owner-matrix-summary.txt).

Remote Unistore DAG remains ownerless by design pending a server-local pool policy and begin/close lifecycle; process-local Session tokens must not cross `DirectUnaryRequest`. Broader M2/M4 and M6 final acceptance remain active. PR readiness is not claimed.
