# N3d: native REGEXP_LIKE in the shared host frame

Paired local source: TiKV24b02fe; TiDB parent1be9bfad plus this commit.

REGEXP_LIKE now uses the existing HostFrame argument scheduler. The closed SQL
catalog admits exactly two/three ready Bytes arguments and a signed LongLong
result. Its immutable operation carries the collation-derived initial flag.
The source captures the original function's native cache handles and original
constant-pattern/match-type recipe, without reading or resolving the cache.
The adapter owns values, demand state, context and cache handles only; it has no
expression tree or evaluation callback.

Each argument is demanded left to right and passes the original UTF-8 gate
before the next request. NULL skips later arguments, context lookup and pattern
compilation. After all non-NULL arguments the actual native context ID binds
the invocation. The initial collation flag precedes explicit match_type, so
rightmost flag precedence remains unchanged. PreparedNativeRegexpLike is the
same plain value primitive used by the native RPN kernel: it resolves and holds
the actual cached Arc, observes known retained cache bytes and matches without
another program/interpreter. Typed NativeRegexpError uses the frontend's
existing mapping; native errors remain owned and are forwarded unchanged.

The provider checks prefix, flags-capacity and visible cache/result retention.
Regex engine/allocator/TLS allocations remain opaque, as in the existing native
observer. These limits do not establish complete statement or peak allocation
accounting. Task cancellation/generations and the compiled singleton window
remain the existing shared mechanisms.

Validation:

- Frontend57 focused tests pass: prior52 plus exact error/NULL/UTF-8 demand,
  dynamic patterns/flags with nested CONCAT and empty/repeated2050 selections,
  collation/flag precedence, observable original cache handles/context rebinding
  and resource denial before context reads. Reference expressions clone owners
  so the native comparison cannot warm the very cache being tested.
- Full frontend1409 pass, exactly the same4 inherited failures,95 ignored.
  Full core1041 pass, exactly the same6 inherited failures,1 ignored. Failure
  names were compared against N3c, not merely counted. Two new core tests cover
  the precise catalog contract and direct live cached failure/context behavior.
- Actual rewritten regexp_like(s,'^a'):25 scenarios match native checksums,
  each with prepared execution and zero fallback/refusal. The first probe
  correctly detected its old expected-fallback table was stale; only the
  REGEXP_LIKE expected route changed. Route and checksum assertions remain.
  Samples=1/target1ms establish behavior/routes, not a performance claim.
- make lint, scoped project clippy via make run, scoped rustfmt and both
  git diff --check pass. Initial check failures are retained in raw evidence.

Reproduce from TiDB root with the existing pinned wrapper:

    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib evaluator::batch

From TiKV root:

    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tikv test --release --locked --offline -j8 -p tidb_query_expr --lib

Raw workspace evidence/no-fallback: n3d-front-tests-route.log,
n3d-front-full-final.log, n3d-core-full.log, n3d-core-clippy-final.log,
n3d-sql-regexp-final.log, n3d-lint.log. Front full and SQL probe used the fresh
release test binary printed by the focused cargo command. Clippy uses the
existing scoped copy of project lint flags with the pinned TiKV wrapper;
no repository lint script was changed.

All six original fallback workload families are now connected for their
specific benchmark shapes. Numeric/other argument coercions, complete control
transforms, other function families, context/effect ordering, native datum
transport and scalar/AST/filter callers remain unfinished. Global alternate
execution still exists for uncovered shapes; the no-fallback goal is active.
