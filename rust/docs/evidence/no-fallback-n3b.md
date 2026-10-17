# N3b: native JSON_TYPE in SQL graphs

Paired local source: TiKV9a3a6ca; TiDB parenta0449afb plus this commit.

Two SQL graph primitives reuse the existing native JSON document/type-name
helpers. They return SQL strings rather than the old ready-worker report
envelope. Explicit native failure variants distinguish empty text, invalid
text/binary framing and invalid UTF-8. The frontend maps only those designated
failure kinds to its existing JsonError/Unsupported values; it never infers a
SQL disposition from a numeric code or a matching error message. An error-code
lookalike is covered by a negative test.

Direct JSON columns/literals lower to an explicit raw byte view. NodeRecord and
incoming row-schema validation retain the original native JSON declaration;
only the lowered checked input carrier becomes Blob. This keeps unknown tags,
opaque framing and temporal tags intact, instead of converting them to wire
JsonType or rendered text. Raw literal bytes are bounded before cloning.

Validation:

- TiDB batch47 pass. Four new tests cover literal and selected-column JSON,
  native malformed Literal/temporal/opaque behavior, SQL error equality to the
  existing native scalar producer (including unknown tag and invalid UTF-8),
  skipped invalid JSON and preparation resource failure without fallback.
  Full1399 pass, exactly the same4 inherited failures,95 ignored.
- TiKV local profile60 pass; full1036 pass, exactly the same6 inherited failures,
  1 ignored. Legacy local entries still reject the new SQL-specific IDs.
- Actual rewritten json_type(js) SQL: all25 batch size/selection/NULL scenarios
  match native checksums; every measured call prepared, zero fallback/refusal.
- make lint, scoped project clippy through make run, scoped rustfmt and both
  git diff --check pass. The initial clippy failure was an import-path mistake;
  the fixed run passed before runtime compilation. Formatting of the export
  list after runtime checks changed no code behavior.

Raw logs: workspace evidence/no-fallback/n3b-{front-tests,front-full,core-tests,
core-full,core-clippy-fixed,sql-json-type,lint}.log. A supplementary standalone
common-error crate test was started; its different default feature set triggered
a cold native gRPC build and was still running at this checkpoint. The passing
query-expression suite already compiles and executes the changed common error
variants. No standalone-common result is claimed here.

Remaining scope: computed JSON subtrees need an explicit representation bridge;
non-String/Bytes text datum kinds and implicit conversions are not admitted by
this checkpoint. CONCAT and REGEXP_LIKE remain among the original fallback
workloads. No performance conclusion is drawn from this short route probe.
The no-fallback goal remains active.
