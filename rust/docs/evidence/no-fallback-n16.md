# Serialized cached-list demand in the shared expression frame

Status: validated local checkpoint, paired with SDK `ad73b11` (signed local DCO,
not pushed). Task #4 remains active; optional declarations,
stateful/PB/public/unistore/filter entry closure and final performance are subsequent
work. No push, PR or release is included.

FIND_IN_SET now consumes the real needle first, then reserves cache initialization
before demanding a cacheable list on the semantic empty row. A hit skips the list;
a NULL needle still initializes. Constructor success commits before needle lookup,
while errors, unwind and cancellation preserve the previous context item. The
source holds only immutable metadata, capabilities and an owned cache reservation;
it calls no expression evaluator or nested expression driver.

The SDK uses one shared NativeContextCache owner with a mutex, condition variable
and a non-clone initializer. Every reader waits while initialization is pending,
including readers of a previously cached different context. Normal owner Clone
starts empty; source compilation explicitly shares the actual SQL-function owner.
The synchronous public facade uses the same reservation protocol. Existing regexp
cache observers count the actual core layout; resource ceilings are unchanged.

The full actual matrix exposed a column COLLATE overlay rejected by complete typed
binding equality. Native Datum columns now retain the original expression metadata
while authenticating equal physical code and width against the independently
validated row schema. Typed imports keep complete declaration equality. The failing
matrix, original collations and SQL results remain retained.

A second regression demonstrated that 256 comma bytes could construct and publish
more than 2 KiB of ordered-key headers under a 1,024-byte work allowance. The SDK's
original encoding reservation now charges actual additions before allocation.
Unscoped callers keep their original explicit byte cap and SQL algorithm. Failure
cancels initialization before publication and permits NULL recovery.

## Independent evidence

| Observation | Count / checksum | Authority |
| --- | --- | --- |
| Historical cached-list suite | 48,600 / `1BE95C69A586382F` | Original public traversal, warm SQL cache and historical Ready authority |
| Clean SQL cache suite | 48,600 / `98391F5AC2C266FB` | Same warm SQL cache, fresh independent original Ready authority each invocation |
| Cache protocol | 7,776 / `B7F093DF39181199` | Executed original lock versus independent reservation candidate, before promotion |
| Value preparation | 135,888 / `4913F925E3CA7FDA` | Executed original tuple/lookup closures versus additive pure preparation, before wrapper extraction |

Exactly 2,932 historical-versus-clean differences require the retired Ready
ScopePoisoned/Scope error, initial output only and an event-prefix relation. The
historical checksum remains executable. This follows the existing N7n decision to
preserve obsolete backend history separately from SQL behavior; it does not copy
that old poisoning into the new source.

The original value closures remain cfg(test) controls after extraction. Production
public wrappers and source consumers share pure value preparation. Public wrappers
retain their original capability-query timing. Executed pre-extraction files and
JSON receipts are in the agent's `evidence/no-fallback/n16-*` archive.

Actual source checks include the 48,600-case matrix and 90-invocation admission
guard, plus 266,412 complete-value comparisons across cacheable/nonconstant lists,
three collations and both flags. They retain values, exact errors/panics, child and
context effects, selection and output prefixes with zero refusal/fallback.

Further checks cover:

- 2,051 physical rows and 2,057 repeated/out-of-order selections, context changes,
  failed constructors and same-owner public/canonical alternation.
- Context callback failure at row 1,026 in literal roots, preserving original
  publication prefixes with both vector switches.
- Forged, foreign, stale, malformed, wire-NULL, wrong-node/role/occurrence/physical
  capabilities and wrong argument indices before context services.
- Context and prepared-value callback unwind/generation mutation with exclusive
  store borrowing, consumed-task replay rejection and fresh-call recovery.
- Initializer cancellation, wrong list capability, stale cancellation, retained
  old context, resource refusal and same-provider NULL recovery.
- Eleven original return declarations across all ten list shapes, both flags
  and context epochs. The new matrix reproduced missing outer family conversion;
  both hit and miss now use the existing bounded original return owner before
  publication.
- Warm NULL cache ignoring a one-MiB needle and billion-scale sparse Decimal
  formatting only when the original tuple/lookup actually demands it.

## Validation

Final gates: frontend **1,956 passed / exact inherited 3 failed / 95 ignored**,
all **415 batch checks** (12 cached-list checks), exact **346 warning titles**.
SDK **1,148 passed / exact inherited 1 failed / 1 ignored**, exact **17 warning
titles**. Repository all-target clippy succeeds with the exact **171 total / 89
expression warning titles**. Projection **26 passed**, including two new cached
and ordered cohorts across serial/two-worker and both vector switches. Lint, both
pinned formatters on eleven frontend/four SDK Rust files, diff checks and the
original-evidence reconstruction checker pass.

The initial Projection extension failed in its original reference with an unbound
parameter because new configurations were inserted ahead of existing parameter
setup. Only the new cohorts were moved after the existing configurations, keeping
prior indices, fixtures, budgets and assertions. Both subsequent full Projection
runs pass. An initial return-fix compilation used the enclosing module's wrong
Result alias; the explicit SQL Result type fixes compilation without changing the
error ownership. All such intermediate logs remain archived.

Run from the TiDB worktree:

```sh
RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib
RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-executor --lib projection::tests
make lint
```

Run from the SDK worktree:

```sh
RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tikv test --release --locked --offline -j8 -p tidb_query_expr --lib
```

The final logs are `n16-front-return-final2.log`, `n16-sdk-full-final.log`,
`n16-sdk-clippy-final.log`, `n16-projection-return-final2.log`, and
`n16-lint-return-final.log`; `n16-final-return-check.log` records reconstruction.

The repository Makefile clippy flags are applied to `--all-targets` through
`n15-clippy-scoped.sh`; the script uses the current repository flags and applies no
historical source patch. Both pinned rustfmt versions check all changed Rust files.
The final reconstruction checker verifies original archives, independent controls,
exact inherited failures/warnings, immutable fixtures, unchanged default clock,
source ownership and the unrelated untracked BUILD.bazel file.

This milestone retains explicit declared-type/non-PB FIND source admission.
Optional-type, protocol and other global closure remain part of the overall task.
No full server or final performance completion is claimed here.
