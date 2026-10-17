# N3a: composed native STRCMP

Paired local source: TiKV b815c75; TiDB parent a555e04a plus this commit.
Reuses the existing StrcmpNative kernel and native collation tag; no SQL pushdown
mapping is invented. Validated constant policy, exact typed arity and signed result
are snapshotted in BatchFacts. Original SQL call identities survive inserted policy
nodes. Suite limits count lowered nodes, including the policy literal.

Validation on repository-pinned toolchains:

- TiDB batch41 pass; full1393 pass, same4 inherited failures,95 ignored.
- TiKV focused native policy test pass; full1033 pass, same6 inherited failures,1 ignored.
- Actual SQL rewrite benchmark: STRCMP, sizes1/8/64/256/1024, dense/10%NULL/sparse/reverse/duplicate:25 checksum comparisons pass; every measured entry has zero fallback and refusal.
- TiDB make lint, scoped rustfmt and both git diff --check pass.
- TiKV scoped copy of scripts/clippy invoked through make run, preserving its lint flags and using the repository compiler: lib/tests pass with existing warnings. Initial test lint exposed an inherited approximate PI literal; replaced it with the identical std constant. Scope changed only workspace selection; retained script is in raw evidence.

Raw logs: workspace evidence/no-fallback/n3-{front-tests,front-full,core-focused-fixed,core-full,sql-strcmp,lint,core-clippy-fixed}.log.
The first clippy failure is retained in n3-core-clippy.log. Test-only PI constant
cleanup occurred after runtime tests and was compiled by the passing lib/test
clippy check. No runtime algorithm changed in that cleanup.

This closes only STRCMP among the six former fallback benchmarks. CONCAT demand,
JSON report decoding, regex and lazy controls remain open. Resource/contract errors
still propagate; no new failure invokes the old evaluator. The benchmark is a
route/checksum smoke test (one short sample), not a performance conclusion.
