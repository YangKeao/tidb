# N4a: demanded controls in the shared frame driver

Paired local source: TiKV4b108b9; TiDB parent5790b5d5 plus this commit.

IF, IFNULL, variable-arity COALESCE and CASE select native typed payloads through
the existing iterative ControlFrame. AND/OR use its existing logical scheduler.
BatchFacts retain all source/call sites, including skipped branches. Every control
is checked for exact argument/result carriers and signed declarations. No native
expression callback or second expression driver was introduced. Signed LongLong
NULLIF uses the existing exact-metadata primitive.

The compiled facade reports max_batch_size1 for any control-containing graph;
the native caller tiles selection occurrences accordingly. Wider direct facade
invocations fail before input reads. This intentionally sacrifices vector width
until demanded-row masks are implemented; it is not a performance claim.

Validation on repository-pinned toolchains:

- TiDB batch43 pass, including selected/empty/repeated/reverse2050-row inputs,
  skipped and demanded overflow, arbitrary COALESCE/CASE arity and native carrier
  payloads. Full1395 pass, exactly the same4 inherited failures,95 ignored.
- TiKV local profile59 pass; full1035 pass, exactly the same6 inherited failures,
  1 ignored. Earlier candidate tests caught a test tuple-index mistake and the
  old Int/Bytes-only null helper; the final helper handles every native carrier.
- Production Projection caller9 pass.
- Actual rewritten SQL lazy_if and coalesce_nullif: each25 scenarios (five sizes
  times dense/NULL/sparse/reverse/duplicate), matching native checksums and every
  measured call prepared with zero fallback/refusal. One short sample per case;
  these are correctness/route smoke tests, not performance measurements.
- make lint, scoped project scripts/clippy through make run, both scoped rustfmt
  checks and git diff --check pass. Existing compiler/lint warnings retained.

Raw evidence in workspace evidence/no-fallback:
n4-front-carriers-fixed.log, n4-front-full.log, n4-core-carriers-fixed.log,
n4-core-full.log, n4-projection.log, n4-sql-{if,coalesce}.log, n4-lint.log,
n4-core-clippy.log. All failed candidates remain alongside final receipts.

Boundaries still open: predicate coercion, unsigned branches, temporal COALESCE
FSP transforms (only already matching kind/FSP is admitted), effectful ordered
outputs and other scalar/AST/filter callers. Source-shaped SQL error adaptation
and eager row/vector argument policy need further work. CONCAT, JSON_TYPE and
REGEXP_LIKE remain among the original fallback workloads. This is a runnable
migration checkpoint; the no-fallback goal is not complete.
