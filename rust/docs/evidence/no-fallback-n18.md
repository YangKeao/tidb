# JSON-schema demand and cache ownership in the shared frame

Status: validated local checkpoint with unchanged SDK `ad73b11`. Task #4 remains
active: RAND, owner-state, AES, optional/PB/public/AST/unistore/filter closure,
generic Admission removal and final caller/server/performance remain.

JSON_SCHEMA_VALID now uses the existing SDK HostFrame and EagerValuesNative
capability transport. A metadata-only source holds genuine argument ordinals,
cacheability, the actual SQL cache owner, the declared return type and output
origin. It contains no expression callback, nested compiler or second driver.

The schema is always evaluated, including on a cache hit. A real SQL NULL schema
returns before context lookup or document demand. Otherwise a cacheable schema
reads the actual context identifier, authenticates the input again and calls the
original serialized cache facade. A hit uses the original prepared item even if
the newly read non-NULL schema is invalid. A miss parses and meta-validates before
publication. Dynamic schemas prepare independently without a context lookup.

Schema preparation completes and commits before the same frame requests the
actual document Fresh. The original document NULL/parse boundary runs before
accessing its prepared validator's OnceLock. That lock preserves both successful
builds and builder errors. External references are therefore loaded only for a
non-NULL, parsed document. NULL or invalid documents cannot create or expose a
cached reference-loading error earlier than the original boundary.

Public scalar callers and the frame now share value-only preparation and
validation. The executed old cache/demand wrapper remains cfg(test), with a
separate original scalar entry for independent comparisons. The pure schema
preparation, validator builder, validation and non-reusable Datum adapter bodies
are unchanged. Ordinary cache Clone still starts empty; explicit invocation
handles alone share the actual SQL owner.

## Ownership and resource scope

The source uses the existing actual-Datum stack lease, so context callbacks and
validation retain no RefCell borrow. Context-identifier panic or generation
mutation cannot publish a prepared schema. Both schema and document capabilities
are checked again before output publication. Actual return conversion uses the
existing identity owner before the normal enum/set view and materialization.

A pending document owns its real prepared schema. Cached items belong to the
original external SQL cache; dynamic parsed JSON is charged by its retained
string/array capacities and logical map-entry allowance before attaching the
continuation. Validator internals, reference retrieval and allocator peaks remain
the existing host-library boundary. This observer is not a statement heap tracker.
No resource ceiling or SDK API changes.

The two-MiB input checks demonstrated that no additional value-store protocol was
needed: the retained row domain already clears the native arena between rows.
Cold unsupported vectors preserve the original error, and three consecutive
cache-hit rows with now-ignored non-NULL vector schemas complete successfully.
No proposed consumed-value marker or extra ownership API was introduced.

## Retained evidence

| Observation | Count / checksum | Reference |
| --- | --- | --- |
| Original schema suite | 19,440 / `5A44B7F912410B39` | Executed original public cache/demand wrapper, captured and rerun before extraction |
| Independent value phases | 3,240 / `8EEBA8F200EECE0C` | Still-active original wrapper versus additive phases and separate real cache owners, before public retirement |
| Complete actual-value integration | 39,336 / `DC8D2D5F9AEF7D85` | 149 schema/document values, eleven return declarations, parameter/deferred producers, both flags and three selections |

All tuples were rerun and frozen; all remain unchanged after public-wrapper
sharing. The complete-value matrix is post-integration evidence, not presented
as a pre-extraction observation. The independent test continues to call the exact
old cfg(test) wrapper after retirement. Executed source archives, JSON receipts,
logs and the reconstruction checker are in `evidence/no-fallback/n18-*` in the
agent workspace.

The original suite covers parameter/literal/column/deferred producers, a literal
NULL document, wrong arities, true cache reuse across changing context identifiers,
late child errors and panics, context callbacks, duplicate/empty selections and
publication prefixes. All nine source-admission witnesses initially reproduced
Admission fallback and now execute with zero fallback/refusal.

Further checks include:

- Temporary-file references: NULL/invalid documents defer loading, later file
  creation succeeds, a compiled validator ignores subsequent file changes,
  context changes rebuild, and cached builder errors survive file recreation.
  NULL skips a cached error and a parse error still precedes it.
- Loopback HTTP: zero requests before a valid document; exactly one load per
  independent original/source validator, reused across three rows. A changed
  server response is ignored until a new context and parsed document demand it.
- Forged, foreign, stale, malformed, wrong-node/role/occurrence/physical and
  wire-NULL capabilities; exclusive context borrowing, panic, generation
  mutation before cache publication and consumed-task replay.
- 128 canceled pending documents, stale cancellation/reply rejection and actual
  NULL-schema recovery on the same provider. Valid preparation stays committed
  before the canceled document, as in the original public wrapper.
- A stale schema with a fresh document, dynamic-tree refusal under the existing
  16-KiB provider policy before document demand, and same-provider NULL recovery.
- 2,051/2,057 selected rows, reverse/duplicate/empty selections, failures at row
  1,026 and public/frame alternation sharing the same SQL cache. These context
  programs retain genuine per-row driver windows. The original complete dynamic
  cross-product also passed; retained gates avoid redundant validator rebuilds
  while keeping every cached/public/failure case and each dynamic selection.
- BENCHMARK Fresh bodies containing cached, column, deferred and NULL-document
  schema calls, retaining real child reads, cache history and callback prefixes.

## Validation

Frontend: **1,985 passed / exact inherited 3 failed / 95 ignored**, all **444 batch
checks**, exact **346 warning titles**. Projection: **26 passed**, with two appended
schema cohorts mixing cached/dynamic preparation, nested JSON and string output,
NULL-before-reference resolution, existing stateful/cache roots and ordered
assignment/read across both flags and serial/two-worker execution.

Lint, both pinned formatters on ten changed Rust files, diff checks and original
reconstruction pass. Initial test-only compilation errors concerned a raw-string
reference delimiter, an output-view import and reuse of an already moved old
fixture type. Only new fixtures changed; prior assertions and configurations stay
intact. The final Projection run uses its own genuine JSON declaration.

SDK `ad73b11` remains clean and unchanged. Its N16 fresh gates remain applicable:
**1,148 passed / exact inherited 1 failed / 1 ignored**, **17 warning titles** and
successful repository all-target clippy with **171 total / 89 expression warning
titles**. These are retained baseline gates, not newly repeated SDK measurements.

This source closure still requires declared scalar arguments/results and no
protocol origin. It does not complete the overall task.
