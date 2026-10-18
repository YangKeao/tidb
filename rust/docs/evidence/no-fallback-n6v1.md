# N6v1: genuine same-driver IN source integration

SQL-function IN now lowers to a vararg `SqlBatchHostOperation::InNative` row
host in the canonical driver. The original generic ABS, duration-column and
computed-JSON source regressions changed from refusal1/fallback1 to zero/zero.
This is source/composition coverage, beyond N6v0's borrowed value API. It does
not complete global fallback retirement. No push/PR/deployment is authorized.

## Implementation and semantic boundaries

`evaluator/batch_in.rs` owns immutable detached producer ordinals, source fields,
unsigned/constant/duration-column facts, original stored string keys/dynamic
indices and return metadata. No Expr/ScalarFunction evaluation callback or fake
literal/Ready program enters the host. The SDK main frame requests actual
children with Fresh and owns row scopes, effects, errors and cancellation.

Generic IN evaluates its real base and every candidate, including after NULL or
an earlier match. It reads div_precision_increment at the original comparison
point and uses the same generic PreparedComparison kernel as original
`ops::eval_binary_full`, without the scalar binary node's typed-integer probe or
signed-literal rewrite. SDK NativeInMembership owns match/NULL/final policy.

Prepared strings coerce the actual base, resolve effective collation late only
when coercion returns bytes, and probe ORIGINAL stored keys. No global collation
snapshot rebuilds keys. The original dynamic indices remain ordered and are all
demanded even after a cache hit. Probe/coercion owners drop before dynamic reads.
Empty dynamic lists finish after the real probe. Cache NULL remains independent.

All four temporal/JSON domains fetch and authenticate ALL actual sources first,
then clone the complete value vector, then cast ALL values in order before
borrowed membership. This preserves source errors ahead of earlier cast warnings,
even with NULL/match. JSON first input keeps document parsing; string list
members become JSON string values. Pure original argument cast helpers and
N6v0's fixed OLD97,752 witness remain in use. Native return coercion and the
outer ENUM_SET_AS_INT view precede checked output publication.

Compile preflight bounds detached source/cache allocations. Host tasks account
capabilities, clones, retained values and conservative conversion work before
allocation/effects; the existing store independently authenticates owner role,
producer, occurrence and physical row. These finite logical limits are not a
whole-process allocator or recovery guarantee. Every source/SQL/resource error
is terminal, with no replay or alternate execution. Cancellation drops task
state; no invocation state survives in reusable workers. Old native character/
typed eligibility remains unchanged: only CompiledSource closure grows.

## Evidence and final validation

Workspace logs under `evidence/no-fallback/`:

- n6v-in-root-red.log and n6v0-source-still-red.log: three original source REDs,
  actual values matched before route assertion; no test was ignored.
- n6v1-in-source-second.log: all3 original REDs and3 demand/cast/cache tests pass.
- n6v1-in-source-full.log:1552 pass, exact inherited4/95ignored/zero filtered.
- n6v1-in-source-full-matrix.log:final1554 pass, exact inherited4/95ignored/
  zero filtered. All169 batch module tests and earlier independent fixed OLD
  value/contract witnesses pass. No pending IN source test is filtered.
- n6v1-in-source-core-full.log:1071 pass, exact inherited6/one ignored/zero
  filtered. The shared host catalog/driver operation change required this run.
- n6v1-in-source-clippy.log:successful scoped clippy,89 warnings unchanged.
- n6v1-in-source-projection.log:all25 production Projection tests pass. Existing
  mixed28-root matrix now includes generic, JSON and duration IN (31 roots),
  under both vectorization flags with serial/zero and two workers.
- n6v1-in-source-lint.log:make lint passes. Pinned rustfmt for all8 changed
  Rust files and both repository diff checks pass. All jobs closed.
- n6v1-inherited-failures.txt:machine comparison of exact frontend/SDK inherited
  failure names to N6u/N6v0. No inherited failure ignored or reclassified.

Generic source comparison covers8192 raw literal/unsigned/NULL-placement cases,
each under four flag/reuse phases, against the unchanged original scalar SQL
owner. Typed proof still uses the independently captured OLD hash; source demand
proof compares original ordering, not another call through the new host.
Duration-column versus constant-text facts, typed JSON document/member differences,
NULL and early-match errors, prepared-hit dynamic overflow, all temporal domains,
2050 duplicate/late-error selections and nested IN beneath whole arithmetic and
row controls are covered. Values, warning contents/order, configuration traces,
error identity, output rows and raw bytes are compared.

Direct provider tests cover stale, wrong-role, wrong-producer and wrong-occurrence
capabilities AFTER an earlier successful match, in generic/duration/JSON domains.
They also exercise actual large-payload ResourceLimit, no later comparison/cast
observation, repeated cancellation and successful reuse of the exact provider.
The first compile exposed an ordinary Rust borrow conflict and temporary error
message lifetime; both were corrected before any passing gate. No semantic test
expectation was weakened in this source step.

Commands from TiDB root:

```sh
RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib evaluator::batch::tests::native_in_ -- --nocapture
RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib
RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-executor --lib projection::tests
make lint
```

Commands from TiKV root:

```sh
RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tikv test --release --locked --offline -j8 -p tidb_query_expr --lib
RUSTFLAGS='' TIKV_FRAME_POINTER=0 make run COMMAND='bash /home/agent/.slock/agents/1555103c-9f3e-46ca-ac2e-b964c650c2f1/evidence/no-fallback/n3-clippy-scoped.sh --lib --tests --release --locked --offline -j8'
```

SDK datatype unchanged; datatype suite not rerun. No SQL server/real-TiKV or final
global closure gate claimed. Remaining functions, other source kinds (including
parameter/deferred/correlated/PB boundaries), AST/legacy IN namespaces and global
alternate dispatch/inherited-failure gates remain. This checkpoint does not
change the separate legacy int/bytes Ready request loops. Goal/task4 stays active.
