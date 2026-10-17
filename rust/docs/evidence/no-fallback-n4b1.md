# N4b1: native integer CHAR casts through the existing host frame

Paired local source: TiKV7032b3c; TiDB parentea7ae17d plus this commit.

Native cast_char over an Int argument now uses the existing HostFrame. The
closed catalog admits one exact Int argument (preserving unsigned flags/bits)
and a Bytes result. Its immutable operation carries the original CHAR length.
Legacy row catalog contracts remain unchanged. Original target charset is
captured in a bounded value-only metadata snapshot; native binary-string CHAR
targets preserve the original uppercase BINARY branch.

The source's host metadata is now a closed enum: plain operations, original
regexp invocation handles, or CHAR target data. There are no Expression or
ScalarFunction objects/evaluation callbacks in the provider. The existing
native_cast_string primitive receives the demanded integer, shared native SQL
stringifier and original context warning/effect handlers. It does not construct
another expression program. Source YEAR is outside the current Int closure;
for admitted integer source codes the primitive's source-sensitive YEAR and
binary-string branches do not apply.

Integer display is at most20 bytes. A conservative256-byte provider reservation
covers temporary strings and output transport before emitting any warning;
actual retained output is checked before publication. The immutable charset
snapshot is charged with source metadata, not copied per invocation. NULL
skips the native cast and its context operations. Host cancellation and task
identity/generations stay in the existing mechanism. These are demonstration
retention policies, not a complete statement/allocator peak-memory guarantee.

Validation:

- All63 frontend focused tests pass (59 prior plus four tests). Coverage includes
  signed/unsigned extrema, NULL,5 widths and3 collations, exact warning codes
  AND message contents, and assertions against unwanted connection-charset or
  packet-limit reads. Nested CONCAT and IF skip an invalid JSON branch.
- The real parser/rewriter runs4 SQL forms for signed and unsigned columns,
  each with empty, duplicate and2050-row selections (24 executions): CAST AS
  CHAR, CHAR(1), nested CONCAT, nested REGEXP_LIKE. Results match native and
  every route has zero fallback/refusal. The historical64-workload probe and
  its original measurements were not changed.
- A provider resource-denial test confirms no native warning occurs before a
  rejected reservation; explicit cancellation leaves no live task/native error.
- Full frontend1415 pass, exactly the inherited4 failures,95 ignored. Full core
  1042 pass, exactly the inherited6 failures,1 ignored. Names were compared
  against N1b/N3d. One new core test validates exact arity/carriers and retained
  unsigned input metadata in the closed catalog.
- make lint, scoped project clippy through make run, scoped rustfmt and both
  git diff --check pass.

Commands from TiDB root:

    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j8 -p tidb-expr --lib evaluator::batch
    make lint

From TiKV root:

    RUSTFLAGS='' CARGO_BUILD_JOBS=8 ../tools/cargo-tikv test --release --locked --offline -j8 -p tidb_query_expr --lib

Raw workspace evidence/no-fallback: n4b-front-tests-final.log,
n4b-front-full-final.log, n4b-core-full.log, n4b-core-clippy.log, n4b-lint.log.
Full frontend used the fresh release binary printed by the focused command.
Initial candidate check logs retain the corrected test timezone method and
Option-to-resource-error conversion diagnostics.

Still unfinished: other CHAR source carriers, BINARY and other casts, implicit
argument coercions without explicit cast nodes, full native19-kind transport,
context/effect ordering, complete control transforms and scalar/AST/filter
callers. Global migration fallback remains and the overarching goal is active.
