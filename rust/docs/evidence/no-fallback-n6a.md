# N6a: explicit window host protocol

TiDB parent18cd9380 plus this checkpoint; paired TiKVae8e9f0. Signed local
commits, no push. The global no-fallback goal remains active.

The canonical HostFrame now supports explicit whole-window start/resume
callbacks. AddWindowNative, SubtractWindowNative and MultiplyWindowNative are
catalog descriptors with checked Bytes arguments and result. They identify the
protocol; this checkpoint does not implement their frontend SQL kernels or
promote arithmetic roots. Existing frontend row callbacks reject these
operations explicitly. Missing window callbacks fail with HostContract and
never retry a row callback.

Each invocation borrows the complete physical-row map; argument replies borrow
the successful compact vector, complete field type and row map. Fresh requests
reevaluate the complete argument, while Reuse can reuse only successful values
inside that invocation. The existing frame loop, task guard, cancellation,
step/depth limits, catalog identity and resource observer own both protocols.
There is no additional evaluator, expression callback or root iteration loop.
The frame checks exact occurrence count and registered carrier before retaining
argument or Ready vectors. Compact Int/Bytes copies are precharged, with the
original singleton copy/precharge behavior retained. Provider transient capacity
remains outside this retained-scratch observer, as documented previously.

Nested window-only graphs admit up to1024 occurrences. Existing row-host and
control descendants still constrain the entire program to one occurrence.
A scalar NULL argument broadcasts across a window; repeated physical rows retain
separate occurrence identities. The maintenance guide documents ownership,
callback lifetimes, cache scope and these admission boundaries.

Five protocol tests exercise nested full-left/full-right phase order; repeated
and reordered selections; original boxed error identity and diagnostic prefix;
parent/child cancellation and retry; Fresh versus Reuse including failure after
cached success; empty selection; invalid task/request/Ready shapes; step, byte
and active-task limits; absent callbacks; scalar broadcasting; and unchanged
mixed row-host admission. The provider fixtures relay opaque byte vectors; they
do not pretend to implement SQL arithmetic. Initial fixture assertions were
corrected to account for the original diagnostic display prefixes, without
changing production error handling.

Validation on final source:

- Core1049 pass, six exact inherited failures, one ignored. Failure names match
  N5g3 exactly. All five new protocol tests pass.
- Frontend focused batch106 pass; full1464 pass, four exact inherited failures,
  95 ignored. Failure names match N5g3 exactly.
- All22 Projection tests pass.
- Required make lint, scoped core clippy, pinned scoped rustfmt and both
  git diff --check checks pass. Clippy retains existing warnings.
- Logs in workspace evidence/no-fallback/: n6-window-core-final.log,
  n6-window-front-final.log, n6-window-front-full.log, n6-window-projection.log,
  n6-window-lint.log and n6-window-clippy.log. Full suites are not green.

Primary commands remain those recorded in the N5g3 receipt, using the pinned
cargo-tidb/cargo-tikv wrappers with RUSTFLAGS='', eight jobs and
release/locked/offline. Full frontend validation also runs the freshly emitted
test binary without a filter. Scoped SDK clippy uses the repository make run
wrapper and evidence/no-fallback/n3-clippy-scoped.sh.

Next implement frontend window arithmetic values/conversion and canonical
row-host wrapping, preserving each source edge's original row/bulk demand.
Whole selections beyond1024 require phase-preserving orchestration rather than
root tiling, which could change error/effect order. Root/caller source promotion,
other functions/effects, dynamic sources, multi-output support and final global
migration-dispatch removal remain unfinished. This foundation alone closes no
new frontend root fallback.
