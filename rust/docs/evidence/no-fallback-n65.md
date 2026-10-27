# N65: genuine legacy and Shared sources in one SDK execution graph

The public Unistore `SimpleExpr` path now uses the same SDK compiler and frame
as genuine native `Expression` children. The seven original typed walkers and
the Datum walker are test-only. No preparation or invocation refusal retries
through them. The previously open default-entry regression now passes with zero
old-tree entries; the inherited worker-count assertions remain failures.

## Source and ownership

The Unistore compiler borrows actual `SimpleExpr` owners and consumer views.
`NativeSourceFragment` contributes the original Arc-owned native graph, host
signatures, source identities and input schema to that same program. It has no
executable program or evaluation method. Relocation changes derived slots and
call ordinals only. Private binary carriers describe the outcome capability
protocol; they do not replace source SQL declarations or create expressions.

Owned continuations retain full-width i128, typed nullable results and original
SQL/infrastructure causes. Capabilities authenticate source, consumer view,
namespace, generation, occurrence and physical row; they are consumed once.
Actual selected cache guards preserve original cache queries and panic poisoning.
Bindings and completed value buffers end before their scopes and later siblings.
The original SDK frame, normal driver variants and public resource allowances
are unchanged. Retained storage accounting includes task/capability capacities,
owned bytes and parsed JSON path storage; it is not a total-process heap claim.

Existing SDK value kernels perform arithmetic, conversion, comparison, JSON and
formatting. Date arithmetic suspends its existing owned value state. Legacy IN
now shares one owned control state between its original synchronous producer and
the new continuation API; equality and quota algorithms remain the originals.
CONV, substring, epoch formatting and JSON operations preserve their actual
intermediate demand phases. No child evaluator callback or prefix replay is used.

Both scan filters retain borrowed root plans across rows. TopN keys and aggregate
arguments do likewise; genuine aggregation source ownership is separate from
mutable group state. An idle plan is taken before callbacks and returned by a
guard, so parameter reentry holds no RefCell borrow and aliases no live program.
No raw-address cache outlives a source owner.

## Independent results

The original observations were captured and pinned before retirement:

| Contract | Count / digest | Complete record SHA-256 |
| --- | --- | --- |
| Primary legacy values, types, demands and panics | 940248 / B02C0B8B9BDDD00C | 253465873621b2cf473a0314c40a0dd32f55935afb0279561c1ca347a00e5eae |
| Absent/retired caches and complex genuine Shared sources | 66048 / 3F895141D852AA15 | de45da57779964623dda16cb2164a1245f5cfbe53b188f75d6aeb038bb64c190 |

Both unchanged observers reproduce their fixed digests through the new default.
Separate retained originals independently reproduce both digests too. Every
recursive original call explicitly targets an original method. The source audit
checks the executed observer bodies and immutable record files; no digest was
rebased. The actual wire-ingestion priority inventory remains unchanged.

Before promotion, 831060 comparisons across all actual metadata configurations
and nine consumer views passed with zero unimplemented cells. The full 66048-case
scope cohort also matched values, demand traces and reuse. Capability, opaque
cause, cancellation, poisoning, binding exclusivity and real parameter reentry
controls pass without entering an old typed tree.

## Validation

Commands are the existing `cargo-tidb` / `cargo-tikv` wrappers with release,
locked, offline and eight build jobs unless noted. Complete commands, source
snapshots, exits and logs are in the agent workspace `evidence/no-fallback`.

| Gate | Result |
| --- | --- |
| Complete frontend | 2363 passed, exact inherited 3 failed, 95 ignored |
| Complete Unistore | 209 passed, exact inherited 3 failed, 13 ignored |
| Projection / Expand | 52 / 6 passed |
| Session SHOW | 31 passed, exact inherited 2 failed |
| Complete SDK expression tests | 1164 passed, exact inherited 1 failed, 1 ignored |
| All native IN controls | 6 passed |
| Native fragment integration | 5 passed |
| SDK scoped clippy | exact prior 171 warning Counter; expression lib-test 89 |
| Production Expr / Session / Unistore check | passed; Expr library retains 149 warnings |
| Root `make lint`, two pinned Rust formatters | passed; 29 Rust files |
| Original/source/protected-file audits and reconstruction | passed |

The full frontend run preceded a final field-documentation fix and making three
already-unused private entry/reset functions test-only. An exact four-file proof
preserves their complete bodies. Fresh fragment tests, full Unistore, production
checks, lint, formatting and server builds validate that limited cleanup. The
frontend warning Counter is again exactly the prior 346. No broad post-cleanup
frontend run is claimed.

An initial candidate filter matched zero tests; that artifact is retained and is
not evidence. Other setup failures, including private visibility and caller-test
setup updates, are retained. The unrelated untracked client-go differential
BUILD.bazel remains untouched and unstaged.

The paired SDK continuation change is committed locally as 8f67591496dbcddbb422f5b6049eb07498bd006e.

## Remaining overall work

This closes the actual legacy-tree routing gap. Current Rust SQL and TiKV server
builds pass. Initial real-service checks compare the frozen native frontend and
current frontend against independent formulas on the same genuine TiKV fixture;
performance measurement and any necessary optimization continue separately.
This receipt does not claim overall task completion, a performance improvement,
production readiness, or publication of either original branch.
