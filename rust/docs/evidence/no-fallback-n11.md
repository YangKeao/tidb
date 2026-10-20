# N11: Live session reads in the shared value frame

This is a local checkpoint after TiDB 189bbcbd in the continuing overall task.
TiKV remains unchanged at 7df75c9. The session-write/ordered-output and broader
source/traversal work remains active after this checkpoint.

## Original proof and shared owner

Before production changes, the real scalar entrance recorded 1,760 observations
for 14 niladic session functions, five context profiles and 11 return declarations.
VERSION additionally receives 19 actual raw carriers. Observations include exact
identity frames, errors/panics and context-getter/warning events. Missing/empty
state, login versus grant identity, roles/resource group, Unicode/embedded zero,
unsigned high bits and ordinary result-family conversion are covered. The frozen
witness is 1760/33F3607B995E80CC. A fixed test-only UTC conversion clock leaves that
observed hash unchanged; the frozen unchanged rerun passed before the candidate.
No datatype or production clock setting changed.

The original and candidate helpers then executed independently for all 1,760
cases, matching exact results and events before retirement. The same run still
reproduced all 14 actual preparation Admission/fallback guards. Executed original
scalar, candidate and tests are archived as n11-independent-*.rs. The old scalar
match was removed only after this proof. The retained shared helper regression
uses the frozen original hash; final runs do not claim two independent algorithms.

SessionRead contains only an immutable operation selector. Its value method
reads the actual invocation context and retains no expression, callback or session
value. Both the ordinary scalar entrance and EagerValueKernel call that same
owner. Metadata recognizes exact zero-argument/non-PB calls. Existing
EagerValuesNative zero-argument host frames carry actual values through the
original bounded return-family conversion and authenticated output roles. The
common eager value-body code after the new session guard is unchanged. Aliases
retain their original getter, notably CURRENT_USER versus USER/SESSION_USER/
SYSTEM_USER, and TIDB_VERSION still supplies process information without a session.

## Demand, ownership and recovery

The actual source matrix repeats all 1,760 cases with duplicate/reversed selection,
a pre-existing NULL output prefix, four vector-flag/reuse runs and exact original
outputs/events. All 14 sources also track changed properties across seven
profiles using the same compiled suite. Dead IF branches and empty selections
perform no session reads. A 64 KiB canonical worker rejects a 1 MiB system-variable
result terminally, preserves its empty output and then returns NULL successfully
with the same worker when the property becomes missing.

The zero-argument callback test exposed an existing lifecycle gap: start installed
a pending task, but a context panic could unwind before its handle reached the
caller. Repeated panics retained unreachable tasks until the byte cap was hit.
This genuine failure was recorded before the fix. An immediate-task scope guard
now cancels on unwind, error and successful completion (idempotently); the same
branch serves existing clock/TSO/epoch immediate calls. The original context panic
is preserved. Both output roles pass 64 consecutive callback panics followed by
fresh success, with exclusive mutable value-store borrowing and original output
provenance checks. No alternate evaluator or retry is introduced.

The first callback success fixture used an integer under an unrelated 8 KiB
string-conversion allowance and failed before reaching the intended panic loop.
It was corrected to an actual string, retaining the callback/cleanup assertions;
the corrected old implementation then reproduced the unreachable-task resource
failure. An earlier resource fixture used the historical mandatory typed consumer
and was corrected to the canonical profile used by production. Compile-only
fixture issues (private test codec access, a result alias and error wrapper) and
all failed runs remain in evidence. Original outputs/hashes/inherited failures
were not changed or filtered.

## Validation

Front 12951 exits 101 solely for the exact prior three failures: 1,822 pass,
3 fail, 95 ignore, all 314 batch tests pass. All 346 warning-title counts exactly
match N10. Projection 95826 passes 26, adding an 11-root session/CHAR cohort with
real database/grant-user/version properties and UInt64 connection/found-row
boundaries, both flags and workers 0/2. Previous configurations and clock index
zero remain intact. Focus 66891 passes eight tests, including 128 callback panics
and recovery. Lint 22833 exits zero. Both pinned formatters check all eight touched
Rust files; diff, immutable fixtures, original scalar reconstruction/shared-body
comparison and SDK-clean checks pass in n11-final-check.py.

Commands from the TiDB root use RUSTFLAGS='' CARGO_BUILD_JOBS=8 and
`../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked
--offline -j8 -p tidb-expr --lib`, its `session_read_ -- --nocapture` focus, the
same command with `-p tidb-executor --lib projection::tests`, and `make lint`.
Workspace evidence/no-fallback/n11-*.log, archived sources and the read-only final
checker retain the exact proof, attempts and final receipts.

No SDK code changed; N9 SDK 1,141/exact-one/one-ignore and clippy 89-expression/
171-total baselines remain reused evidence, not newly executed N11 runs. No
Go/Bazel sources changed; unrelated untracked client BUILD.bazel stays excluded.
The memory allowance covers frame-owned data and demanded conversions, not the
external context provider's own allocations. No push, release, whole-server or
performance claim is made. Continue session effects/ordered output, remaining
early/value adapters, deferred/PB/correlated/optional-type/public/unistore/filter
sources and generic Admission removal without waiting for another prompt.
