# N6e: whole-selection window execution

TiDB parent396e9a1f plus this signed local checkpoint; paired TiKV7ac5b37.
No push. The global no-fallback goal remains active.

Explicit window roots now carry an immutable whole-selection contract in
LocalBatchProgram. Callers can inspect requires_whole_selection separately
from the structural max_batch_size. Without typed controls, a window root has
no fixed1024 cap; layout and existing step/frame/task/retained-storage checks
still apply. Ordinary roots retain1024, row roots one, and typed-control
structural admission remains one. That unfinished control restriction never
authorizes tiling a whole-selection root.

A separate private SqlWindowBatch execution profile enters the SAME canonical
frame loop. Its actual prepared root must be a checked SQL window host before
demand starts. Child ordinary/host/import phases inherit its checked width;
row hosts still run through RowHostScope and keep singleton callback semantics.
Row, decoded, PB and closed ready-value entry limits remain unchanged. Neither
caller labels nor a corrupted private facade can widen an ordinary root.

The width audit found a genuine fixed array: generated vectors previously
returned a slice of IDENTICAL_LOGICAL_ROWS. Above1024 they now use the existing
LogicalRows::Identical representation, while legacy generated values preserve
their previous slice representation, including exactly1024. The new path uses
logical_rows_struct/get_idx. The legacy logical_rows() slice accessor remains
bounded and is not used by the opaque wide facade. Generated kernel wrappers
iterate usize counts; VectorArg uses the logical-row abstraction. All width/
identity constants in the expression crate and relevant generated accessors
were inspected rather than merely raising the entry guard.

Provider source/cast payload totals now update incrementally. Suspended native
window tasks cache their conservative complete-left charge; RowHostScope caches
completed child-vector bytes. Neither a source prefix nor a suspended ancestor
vector is rescanned for every later row callback. These charges retain the
existing logical allowance policy; they do not claim a hard allocator peak.
The native arithmetic provider uses checked map/vector layouts and its normal
resource cap rather than a second hard1024 limit.

Validation:

- Ten SDK window protocol tests pass. A2050-occurrence nested graph has a left
  input error at2049 and an independent right error at0: the original left
  error object wins, no right input runs, both tasks cancel, and reuse succeeds.
  Other cases prove full left/right/nested phases, repeated physical-row
  identities through a2050-row Window -> Row -> Window graph, generated Upper
  vectors and scalar broadcasts beyond the old identity array, step/byte
  refusal, and preserved ordinary-root bounds. Corrupting a private ordinary
  facade's whole flag still fails before effects.
- Core1054 pass, six exact inherited failures and one ignored. Failure names
  match N6b/N6d's paired SDK code evidence exactly. Initial two new fixture
  failures came from missing UTF8 charset metadata; fixtures now provide full
  valid declarations and the original charset validator is unchanged.
- Two native arithmetic tests cover102 primary invocations:100 actual native
  batch reference comparisons and two injected binding failures, plus156
  provider resource refusals. Four cases use the complete2050-row selection,
  checking values, native errors, diagnostics, configuration reads, argument
  phases and cleanup. Both incremental and cached ledgers retain prior results.
- Full frontend1467 pass, four exact inherited failures and95 ignored. Names
  match N6d; all108 batch checks pass inside the same full run.
- All22 Projection tests, required make lint, scoped SDK clippy, pinned scoped
  rustfmt and git diff --check pass. Clippy retains its existing89 warnings.
  Full suites are not green.
- Logs: workspace evidence/no-fallback/n6e-wide-{core-final,native-final,
  front-full,projection,lint,clippy}.log. The earlier core-full log includes the
  two invalid fixture declarations; final evidence is core-final.

Commands use the pinned cargo-tidb/cargo-tikv wrappers, RUSTFLAGS='', eight
jobs and release/locked/offline as recorded in N6d. SDK full --lib tests and
scoped make run clippy were run; native focused tests use arithmetic_window::tests.
The freshly emitted frontend test binary was then run without a filter.

Source lowering still emits existing row/typed descriptors, so this checkpoint
alone promotes no frontend root fallback. Caller integration must select the
actual row or bulk domain BEFORE effects: forcing window_size=1 on a bulk graph
changes NULL demand, while ignoring disabled vectorization is also incorrect.
Both domain programs must use the canonical driver without retrying a legacy
interpreter. Source lowering must preserve genuine native eligibility and each
edge's typed owner phases, including bulk ABS/CAST special cases. General
control wrapping must remove its temporary structural restriction rather than
turning unsupported valid SQL into a permanent resource refusal. Other families,
effects, dynamic sources, remaining callers, multi-output and final global
migration-dispatch removal remain unfinished.
