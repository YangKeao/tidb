# N6b: row-host operands inside window phases

TiDB parent8e456bed plus this checkpoint; paired TiKVec9cc67. Signed local
commits, no push. The no-fallback goal remains active.

An explicit window root can now request row-host operands without changing the
parent into a row evaluator. RowHostScope is another scheduling frame inside
the existing canonical frame loop. It completes the entire row-host subtree
for one selected occurrence, then the next occurrence, and returns the complete
operand vector before the parent requests its next argument. It never calls
an Expression evaluator or recursively invokes another driver.

The scope retains only successful checked singleton values until it can publish
the compact Int/Bytes result. Both singleton owners and output-copy overlap are
included in the existing retained-scratch budget. Each row scheduling action
costs a step and a frame; all host tokens use the original task guard. Failure
or a resource refusal cannot return an earlier successful row or cache entry.
Whole-operand Fresh restarts every row in the operand subtree.

FrameRows::Scoped carries the original occurrence identity alongside the
physical row. It survives row-host argument programs and selected subsets.
Window invocation and argument replies now expose occurrence_start, so a
Window -> RowHost -> Window tree retains the outer row identity in the innermost
callback. Duplicate physical rows remain distinct occurrences. Scoped reads
use the occurrence-aware read_input seam; the optional column-only batch hook
cannot carry occurrence identity and is bypassed there.

Admission remains explicit: only an actual window root gains row-host operand
wrapping. A row root remains limited to one occurrence, and any typed-control
node still constrains admission to one. The original row-root preflight test
still proves an oversized call fails before callbacks. The earlier blanket
restriction on a row child beneath a window root is replaced with a stronger
execution test for the newly implemented contract, rather than removing checks.

Two new protocol tests prove full parent-operand phase order and original row
identity through nested window/row/window calls; exact original boxed error
identity; failure prefix and cancellation of three pending ancestor tasks;
successful reuse after failure; repeated/reordered selections; complete Fresh
reexecution; active-task/byte refusal and cleanup. The existing five protocol
tests also pass. These fixtures relay opaque bytes and do not implement SQL
arithmetic or claim new frontend root coverage.

Validation:

- Core1051 pass, six exact inherited failures, one ignored; failure-name list
  compared with N6a. All seven window protocol tests pass.
- Frontend focused106 pass; full1464 pass, four exact inherited failures and
  95 ignored; failure-name list compared with N6a.
- All22 Projection tests pass on the final SDK source.
- Required make lint, scoped SDK clippy, pinned scoped rustfmt and
  git diff --check pass. Full suites are not green.
- Logs: workspace evidence/no-fallback/n6b-row-scope-{core-full,front,
  front-full,projection,lint,clippy}.log. The initial scope build passed four
  prior protocol tests and failed the old blanket mixed-graph admission
  assertion before that assertion was replaced with the new execution contract.

Commands use the pinned cargo-tidb/cargo-tikv release/locked/offline wrappers,
eight jobs and RUSTFLAGS='', as recorded in N6a/N5g3. Full frontend validation
also runs the freshly built test binary without a filter. Scoped clippy uses
the repository make run wrapper and n3-clippy-scoped.sh.

Next implement the frontend window arithmetic provider and source lowering,
with each edge retaining its actual row/bulk numeric conversion and demand.
Whole selections beyond1024 require phase-preserving orchestration rather
than root tiling. Existing frontend root selection is unchanged by this SDK
step. Other functions/effects, dynamic sources, remaining callers, multi-output
support and global migration-dispatch retirement remain unfinished.
