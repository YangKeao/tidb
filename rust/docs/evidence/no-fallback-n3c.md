# N3c: CONCAT context operations in the shared driver

Paired local source: TiKV2de821d; TiDB parent9beb188e plus this commit.

CONCAT/CONCAT_WS now lower to a checked SQL-only host catalog. HostFrame owns
argument scheduling, cache validation, resource charges and task cleanup. The
production adapter holds no Expression, ScalarFunction or evaluation callback;
it receives values/types and native session policy only. It calls the shared
pure join directly, without another expression program or old evaluator.

ConcatDemand contains the original native prefix/null/packet rules, shared by
the existing native reference producer and the new host provider. The generated
RPN writer and the direct owned result entry both use the same concat_join.
Packet limits are read at the original demanded points, including CONCAT_WS's
original data-index accounting through NULL slots. Warnings use the actual native
handler. An actual native error is retained and forwarded unchanged; the outer
host error marker is never parsed. Resource errors never enter another evaluator.

The host catalog has explicit operation/domain identity and full signature
validation. Legacy row catalogs stay signed-Int-only. BatchFacts retain every
host/source site and catalog identity, even for dead branches. Empty facade
calls validate a required provider before publishing output. Generation checks
and idempotent cancellation protect reused task slots. Opaque provider heaps
are bounded separately from exact frame caches/results; these are retained/demo
limits, not a statement-memory tracker or allocator peak guarantee.

Validation:

- Frontend52 focused tests pass (47 prior plus four CONCAT route/policy tests
  and one provider heap/generation test). Full1404 pass, exactly the same4 known
  failures,95 ignored. New coverage includes NULL/packet-skipped invalid JSON,
  strict native error equality, dynamic/fresh packet policy, nested calls,
  invalid UTF-8 bytes, empty/reverse/duplicate2050 selections and resource denial.
- Core full1039 pass, exactly the same6 known failures,1 ignored; three added
  tests cover the closed host factory/source snapshots and prefix/result caps.
- Production Projection10 tests pass. The added test exercises native query
  warning1301 and strict error in serial AND parallel StmtContext projections;
  the serial route also explicitly asserts prepared/no fallback/refusal.
- Actual rewritten concat(s,'-x'):25 scenarios match native checksums, every
  measured call prepared with zero fallback/refusal. Singleton windows are
  deliberate; the short samples establish routes/results, not speedups.
- make lint, scoped project clippy through make run, scoped rustfmt and both
  git diff --check pass. Initial compile candidates (generic join type inference,
  import and internal test pattern updates) are retained in raw evidence.

Raw workspace evidence/no-fallback: n3c-front-tests-fixed.log, n3c-front-full.log,
n3c-core-full.log, n3c-core-clippy.log, n3c-projection.log, n3c-sql-concat.log,
n3c-lint.log. Source audit confirms batch_hosts.rs contains no expression-eval
entry call. Supplement to N3b: standalone common-error crate11 tests also passed
in n3b-common-tests.log after that checkpoint was committed.

The historical vector_allocations scratch field is a static carrier-node
estimate, not an allocator counter. It is documented accordingly; current probe
labels use estimated_carrier_nodes. Old measurement artifacts were not changed.

Still unfinished: numeric/other implicit CONCAT argument coercions, effectful
ordered projections, REGEXP_LIKE, remaining function/context families and other
expression/AST/filter callers. Five of the original six fallback workloads have
now been connected; this does not establish complete SQL coverage or finish the
no-fallback goal.
