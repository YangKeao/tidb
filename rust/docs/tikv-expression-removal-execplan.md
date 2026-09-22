# Replace the native expression evaluator with TiKV's engine


This is a living ExecPlan governed by repository-root `PLANS.md`. It supersedes
`tikv-expression-coverage-execplan.md` as the plan of record: the earlier plan
added a second implementation, this one removes the first. It is an
exploration on YangKeao forks, not an upstream commitment.

Latest verified handoff: [migration checkpoint](tikv-expression-migration-checkpoint.md).
At TiDB `982bad9`, the refusal-intolerant corpus still reports **1163 passed /
61 failed / 99 ignored** (59 distinct first-refused expressions). All failures
are adapter refusals, not newly classified value mismatches. The environment
variable makes chunk_e reject declines but still runs its native oracle; this is
NOT evidence that the native implementation has been removed. Cross-configuration
regressions and both count gates passed. The six-point objective remains
incomplete; do not interpret historical foundation milestones as cutover approval.


## Purpose / Big Picture


Today `tidb-expr` contains two evaluators: its own (`scalar_function::eval`,
`ops`, `time_fn`, `string_fn`, `builtin_ext`, the comparison and control
families) and the TiKV RPN engine reached through the `tikv-expr` feature. The
goal is to make TiKV's engine the **only** evaluator, so that one set of
kernels defines SQL value semantics everywhere.

The current shortest-path policy physically deletes native kernel families
before complete compatibility. A retained function must execute in TiKV;
otherwise admission and the residual structural evaluator must both return a
structured `Unsupported` error. There is no native replay after an engine
error. Tests keep their source vectors but assert the contraction explicitly
when parity is not established.

The purpose of the plan is to sequence that removal so each step is verifiable.
Definition of done, stated as behavior: the `tikv-expr` feature disappears (the
engine is always on), no code path evaluates an expression through old kernels,
and the declared Rust and SQL demonstration gates pass. Full historical MySQL
function compatibility is not a prerequisite for the deletion demo; every
intentional contraction must be listed and must never silently fall back.

The feature gate is the temporary coexistence mechanism. It is deleted only
at milestone E, and deleting it is the signal that the replacement is
complete. A claim that the old implementation has been removed is only valid
once no `#[cfg(not(feature = "tikv-expr"))]` arm and no `tikv-expr` feature
remain in the workspace.


## Progress

- [x] First physical-deletion tranche: removed `src/math_fn/` (1,674 lines of
      kernels plus 170 lines of support/tests), removed every production
      `math_fn` dispatch, and added fail-closed guards to both residual native
      entry points. Retained ABS/SIGN/SQRT/EXP/LN/LOG/LOG2/LOG10/PI/CRC32
      vectors execute through TiKV. CONV, POW/POWER, ROUND/TRUNCATE,
      CEIL/CEILING/FLOOR, RAND and all trig functions are explicitly excluded
      because parity is not established; their preserved source vectors assert
      both engine decline and structured native refusal. The guarded full suite
      passes: 1216 library tests and 77 integration tests (99 ignored). The
      runtime gate's reviewed contraction baseline is 30 tests / 334 receipts /
      2150 engine rows / 160 borrowed rows; static admission is 223 admitted /
      161 excluded. This is one family only and is not the final native-removal
      claim.
- [x] Second physical-deletion tranche: removed the 1,466-line
      `src/builtin_ext/crypto.rs`, its dispatch/re-export and crypto-only direct
      dependencies; both residual evaluator entry points now fail closed for
      MD5, SHA/SHA1/SHA2, SM3, RANDOM_BYTES, PASSWORD,
      VALIDATE_PASSWORD_STRENGTH, ENCODE/DECODE, COMPRESS/UNCOMPRESS/
      UNCOMPRESSED_LENGTH, and AES_ENCRYPT/AES_DECRYPT. The AST and chunk
      rewriter boundaries refuse these names before arity validation or child
      evaluation/folding, so malformed or nested calls cannot expose another
      native path. All fifteen are explicit contractions: even the pinned
      engine's hash/compression kernels stay excluded because construction-time
      charset errors, result collation, warnings, and session semantics are not jointly verified. The preserved
      Go source tables now assert engine decline plus the exact structured
      native refusal, including vectors formerly embedded beside the deleted
      kernels. Static admission is 216 admitted / 168 excluded; the reviewed
      runtime baseline is 30 tests / 323 receipts / 2072 engine rows / 160
      borrowed rows. This is still not the final native-removal claim.
- [x] Third physical-deletion tranche: removed the 200-line
      `src/builtin_ext/vec.rs` SQL kernel module and its dispatch. VEC_DIMS,
      VEC_L1_DISTANCE, VEC_L2_DISTANCE, VEC_NEGATIVE_INNER_PRODUCT,
      VEC_COSINE_DISTANCE, VEC_L2_NORM, and VEC_AS_TEXT now execute only in
      TiKV; independent source vectors pin their results while both residual
      native boundaries return the exact structured refusal. VEC_FROM_TEXT is
      explicitly contracted before arity/child rewriting because the pinned
      engine dispatches neither `VecFromTextSig` nor
      `CastStringAsVectorFloat32`. Planner/result metadata and the
      `VectorFloat32` datatype remain as bridge structure, not SQL
      kernels. The guarded suite passes 1201 library tests plus 77 integration
      tests (99 ignored); static admission and the reviewed runtime baseline
      remain 216/168 and 30 tests / 323 receipts / 2072 engine rows / 160
      borrowed rows. This is still not the final native-removal claim.
- [x] Fourth physical-deletion tranche: removed the 251-line
      `src/builtin_ext/json2.rs` JSON depth/storage SQL-kernel module and its
      dispatch. Typed JSON-column `JSON_DEPTH` executes in TiKV against every
      preserved depth vector; the original text/other constant shapes require
      explicit engine decline and exact native refusal. JSON_STORAGE_FREE and
      JSON_STORAGE_SIZE are explicit contractions because no pinned-engine
      binary-storage-accounting lowering is admitted; all former Go result,
      NULL, malformed-document and arity vectors remain and assert decline plus
      refusal. The guarded suite passes 1198 library tests plus 77 integration
      tests (99 ignored), and the migrated session SQL contraction test passes.
      Static admission and the reviewed runtime baseline remain 216/168 and 30
      tests / 323 receipts / 2072 engine rows / 160 borrowed rows. This is still
      not the final native-removal claim.
- [x] Fifth physical-deletion tranche: removed the 817-line positional-regexp
      module, the 215-line shared regexp/operator module, their dispatch and
      re-export, plus the residual AST, ScalarFunction, and value-evaluator
      kernels. REGEXP/RLIKE and REGEXP_LIKE/SUBSTR/INSTR/REPLACE now execute
      only in TiKV. Every former scalar source vector remains with independent
      outputs and TiKV runtime error 1139; the all-success 96/1200/2400/4320
      generator sweeps compare Copying and Borrowed result strings byte-for-byte.
      Both native boundaries assert the exact structured refusal for all
      six names. Invalid literal patterns/match types that TiKV rejects
      while compiling are explicit shape contractions, never native fallback.
      The guarded suite passes 1196 library tests plus 77 integration tests (99
      ignored), and the opted-in session SQL test proves at least five regexp
      rows executed through TiKV. This is still not the final native-removal
      claim.
- [x] Sixth physical-deletion tranche: removed the 738-line
      `src/string_packet.rs` module plus every REPEAT/SPACE/LPAD/RPAD/TO_BASE64
      dispatch arm and both WEIGHT_STRING AST/scalar kernels. All six names
      were already excluded because the embedded engine has no equivalent
      statement `max_allowed_packet` setting or verified WEIGHT_STRING wire
      path; they now return one structured unsupported error before arity,
      child evaluation, allocation, or warning emission. Seven deleted-file
      test functions were replaced one-for-one by contraction receipts carrying
      their source inputs, including the 42 collation/padding shapes. The
      guarded library suite remains 1196 passed / 99 ignored, and six targeted
      session source tests pass with exact refusal while unrelated retained
      behavior remains asserted. This is still not the final native-removal
      claim.
- [x] Seventh physical-deletion tranche: removed
      `tidb-expr/src/builtin_ext/misc.rs`, `tidb-util/src/vitess.rs`, and
      `tidb-executor/src/tidb_decode_key.rs`, removed the DES dependency, and
      deleted the `TIDB_DECODE_KEY` snapshot/cache plumbing. `UUID`, `UUID_V4`,
      `UUID_V7`, `NAME_CONST`, `IS_UUID`, `UUID_VERSION`, `UUID_TIMESTAMP`,
      `UUID_TO_BIN`, `BIN_TO_UUID`, `TIDB_SHARD`, `TIDB_DECODE_KEY`, and
      `VITESS_HASH` are explicit contractions at both residual native
      boundaries. `ANY_VALUE` remains admitted and is proven TiKV-only. Planner
      `TIDB_SHARD` generated-column synthesis now declines instead of creating
      an expression whose native owner was deleted. The reviewed runtime gate
      remains 30 tests / 323 receipts / 2072 engine rows / 160 borrowed rows
      with zero native fallbacks; the static gate remains 384 rows / 216
      admitted / 168 excluded / 0 missing. The source corpus is now 35
      `*_source.rs` files / 437 tests, including 14 tests in
      `misc_contraction_source.rs`. This is still not a full-compatibility,
      feature-off, end-to-end SQL, hosted-CI, TiKV-server-compatibility, or final
      native-removal claim.
- [x] Seventh-tranche review hardening: preserve every deleted miscellaneous
      source input, including non-UTF8 UUID wrappers, all UUID binary spellings,
      swap flags, shard strings/arities, overflow-warning rows, and the generic
      index/malformed key decoder fixtures. Visible-call acceptance now requires
      a parsed AST builtin node plus its exact marker; literal/comment text does
      not qualify. Prepared/default shapes require exact topic+complete-statement
      equality plus the marker. `ANY_VALUE` has a distinct refusal kind only on
      the native backend; Copying/Borrowed errors stay errors, and executable
      statements must succeed and increment the TiKV row counter. Native
      integration replay records 77 explicit contractions plus one native-only
      refusal and still exposes the same 160 unrelated carried divergences.
- [x] Eighth physical-deletion tranche: physically deleted
      `builtin_ext/string2.rs` plus the orphaned SUBSTRING, three-argument
      LOCATE, FORMAT, and EXPORT_SET helpers from `string_fn.rs`; guarded
      SUBSTRING/SUBSTR/MID, LOCATE, FORMAT, FIND_IN_SET, EXPORT_SET, LTRIM,
      RTRIM, and TRANSLATE at every residual native boundary. FORMAT,
      EXPORT_SET, TRANSLATE, and non-binary FIND_IN_SET are explicit
      contractions. Retained SUBSTRING/LOCATE/TRIM and binary FIND_IN_SET source
      rows execute engine-only; LOCATE/INSTR now select TiKV's byte signature
      from the aggregated binary collation, honoring explicit/implicit collation
      precedence over a raw binary operand. Validation is 1185
      expression library tests + 77 integration tests green, five focused
      session SQL tests green with positive TiKV row deltas, runtime 30 tests /
      323 receipts / 2072 engine rows / 160 borrowed rows, and static 216
      admitted / 168 excluded / 0 missing. Native replay records 91 explicit
      contractions + one native-only refusal with 181 carried divergences;
      Copying executes 416592 engine rows and exposes 167 divergences, including
      two retained SUBSTRING statements whose whole complex program is not yet
      engine-routable. This is not a green full-integration claim.
- [x] Ninth physical-deletion tranche: removed the remaining native `CONCAT`,
      `CONCAT_WS`, `INSERT_FUNC`, `MAKE_SET`, and `FROM_BASE64` kernels from
      `string_fn.rs`, their value-dispatch arms, the duplicate lazy scalar
      CONCAT evaluators, the context-sensitive FROM_BASE64 path, and the public
      `concat_values` escape hatch. All five names were already excluded by the
      admission table as `PACKET_CONTEXT_UNAVAILABLE`: the embedded facade has
      no verified `max_allowed_packet` or warning-policy transport. Every
      residual native boundary now returns the exact packet-string unsupported
      error before argument evaluation, allocation, decoding, or warning
      emission. Former scalar, binary, NULL, Unicode, malformed-base64, packet
      overflow, and folding inputs remain as explicit contraction receipts;
      constant folding must leave these calls visible rather than recreating a
      native kernel. The source corpus is 36 `*_source.rs` files / 444 tests.
      Validation is 1183 expression library tests plus 77 external expression
      tests green, two focused session SQL contraction tests green, expression
      and query differential tests green, runtime 30 tests / 323 receipts / 2072
      engine rows / 160 borrowed rows, and static 216 admitted / 168 excluded /
      0 missing. Full replay remains intentionally red at the pre-existing 181
      native and 167 Copying divergences; Copying executes 416240 engine rows.
      This is not a final native-removal or full-compatibility claim.
- [x] Tenth physical-deletion tranche: removed the native `INET_ATON`,
      `INET_NTOA`, `INET6_ATON`, and `INET6_NTOA` kernels and their compare2
      dispatch arms. The four admitted miscellaneous signatures now execute
      only through TiKV; AST, contextual-value, values-only, and scalar native
      boundaries return the exact INET engine-required error before arity or
      child evaluation. All former IPv4 shorthand/full/range/NULL and IPv6
      binary/render/malformed vectors remain: native receipts assert refusal and
      engine-only source rows assert independent values without fallback. The
      existing runtime coverage fixtures record positive engine rows for all
      four signatures. TiKV returns NULL for malformed INET_ATON and INET6_ATON
      text where the production Go kernels report an error; these accepted gaps
      are pinned in tests and `EXPRESSION_SEMANTIC_GAPS.md`, not hidden as parity.
      The source corpus is 36 `*_source.rs` files / 447 tests. Validation is
      1186 expression library tests plus 77 external expression tests green;
      expression/query differential tests green; runtime remains 30 tests / 323
      receipts / 2072 engine rows / 160 borrowed rows and static admission
      remains 216 admitted / 168 excluded / 0 missing. Full replay remains the
      intentionally red 181 native / 167 Copying divergence diagnostic;
      Copying executes 416241 engine rows. This is not a final native-removal or
      full-compatibility claim.
- [x] Eleventh physical-deletion tranche: removed the native `UPPER`/`UCASE`,
      `LOWER`/`LCASE`, `ASCII`, `BIT_LENGTH`, `LEFT`, `RIGHT`, `REVERSE`,
      `REPLACE`, and `STRCMP` kernels and every residual value/scalar dispatch.
      Four native entry boundaries reject those names before arity or child
      evaluation; admitted standalone shapes execute only in TiKV. Independent
      source values include both case aliases, Unicode/binary boundaries,
      count edges, replacement and comparison NULL/coercion rows. Nested
      `CONVERT USING`/`ELT`, SET-subquery REPLACE, and mixed CHAR_FUNC/STRCMP
      shapes are explicit structured contractions rather than fallback. Strict
      non-NULL constant provenance through these functions, plus Go-compatible
      CAST propagation, preserves view metadata without falsely marking a
      function over a NOT NULL column as non-nullable or recreating value kernels. `LOCATE`,
      `INSTR`, and `POSITION` remain native: an engine probe returns 2 for
      `INSTR('ABC' COLLATE utf8mb4_bin,'b')` where Go/native return 0, so the
      unresolved RPN collator-selection gap is documented rather than silently
      accepted. Validation is 1185 expression library tests, 77 external tests,
      eight focused session tests in both feature modes, expression/query diffs,
      runtime 30 tests / 323 receipts / 2072 engine rows / 160 borrowed rows,
      and static 216 admitted / 168 excluded / 0 missing. The broad replay is
      still an intentionally red 181-native / 167-Copying diagnostic; Copying
      executes 416245 engine rows. This is not a compatibility claim.
- [x] Twelfth physical-deletion tranche: removed the native `HEX`, `UNHEX`,
      `BIN`, `OCT`, `ORD`, and `BIT_COUNT` kernels, their type-aware helpers,
      private coercion/encoding helpers, direct tests, and residual value/scalar
      dispatch. All four native entry boundaries now refuse the names before
      evaluation. Ordinary admitted numeric/string/typed-column shapes execute
      only in TiKV, including OCT's string signature. OCT binary-literal
      provenance, non-leaf/numeric ORD, binary-literal HEX, and HEX over
      non-lowerable TRANSLATE/NULLIF shapes are
      explicit contractions. The independent Go source values remain in
      engine-only tables or alongside exact contraction assertions. A deletion
      regression exposed and fixed adapter signature selection for temporal HEX:
      ETDatetime/ETTimestamp/ETDuration select `HexStrArg`, while decimal/real/int
      select `HexIntArg`. A typed TIMESTAMP column executes with its Go-captured
      value. JSON is explicitly declined before signature selection: its
      inferred HEX result width is `8 * MaxBlobWidth = 34359738360`, which the
      checked protobuf-i32 metadata bridge refuses rather than truncates.
      `CastJsonAsString` itself is supported; constant and typed JSON Go values
      are retained as exact contractions around this composition-level gap.
      Validation is 1185 expression library
      tests, 77 external expression tests, ten focused session runs in both
      feature modes, expression/query diffs, runtime 30 tests / 323 receipts /
      2072 engine rows / 160 borrowed rows, and static 216 admitted / 168
      excluded / 0 missing. The full session external suite still has the same
      four carried non-radix failures (math/crypto contractions) as the earlier
      packet-tail baseline; this tranche adds none. Broad replay remains an
      intentionally red diagnostic: Native has 182 divergences (one new empty
      partition result after a refused native HEX write), while Copying returns
      to 167 divergences and executes 416238 engine rows in the final run. This
      is not a compatibility claim.
- [x] Thirteenth physical-deletion tranche: removed the native
      `SUBSTRING_INDEX` and `QUOTE` kernels plus `split_bytes`, binary-result,
      and UTF-8-lossy quoting helpers. All four native entry boundaries now
      refuse both names before argument evaluation. Ordinary signed-literal
      SUBSTRING_INDEX and non-binary QUOTE shapes execute only in TiKV. The
      lowering bridge deliberately rejects runtime/unsigned/i64::MIN counts:
      the pinned kernel applies `count.abs()`, so those values can wrap or
      overflow. QUOTE rejects binary, BIT, ENUM, and SET sources because Go
      substitutes U+FFFD for malformed bytes while the pinned TiKV kernel
      preserves them. Direct binary literals, string/decimal counts, and the
      captured unsigned count are exact contractions; independent Go values
      remain alongside refusal assertions. Difftest acceptance is an exact
      whole-statement allow-list so an engine failure in an ordinary shape
      cannot be relabelled as an expected contraction. Validation is 1185
      expression library tests, 77 external tests, two focused session tests in
      both feature modes, expression/query diffs, runtime 30 tests / 322 receipts
      / 2064 engine rows / 160 borrowed rows, and static 216 admitted / 168
      excluded / 0 missing. The full session suite adds no failures to its four
      carried math/crypto contractions. Broad replay remains intentionally red:
      Native has 182 divergences; Copying has 167 and executes 416244 engine
      rows. This is not a compatibility claim.
- [x] Fourteenth physical-deletion tranche: removed the native `CHAR_FUNC`
      byte-assembly/charset-decoding kernel, its integer-byte helper, raw charset
      sentinel preprocessing, and residual value dispatch. All four native entry
      boundaries now refuse `CHAR_FUNC` before argument evaluation. TiKV has no
      CHAR signature, so every CHAR shape is an explicit structured contraction;
      former Go values remain beside refusal assertions rather than being silently
      dropped or replayed. Constant folding can no longer erase constant CHAR
      calls through the deleted kernel, and the ratchet now requires the call to
      survive as a visible adapter decline. Validation is 1185 library and 77
      external tests, the focused session contraction in both feature modes,
      expression/query diffs, runtime 30 tests / 322 receipts / 2064 engine rows
      / 160 borrowed rows, static 216 admitted / 168 excluded / 0 missing, and
      repository lint. The full session suite adds no failures to the four
      carried math/crypto contractions. Broad replay remains intentionally red:
      Native has 182 divergences; Copying has 167 and executes 416242 engine
      rows. FIELD and ELT remain native and are the next string-tail deletion
      candidates; this is not a compatibility claim.
- [x] Fifteenth physical-deletion tranche: removed native `FIELD` and `ELT`,
      including FIELD's comparison-mode/collation kernel, ELT's selected-value
      body, value dispatch, and the scalar FIELD bypass. All four native entry
      boundaries now refuse both names before child evaluation. Ordinary
      homogeneous integer/string/real FIELD and integer-selector ELT shapes run
      only in TiKV with positive session row-counter evidence. NULL/mixed FIELD,
      warning-bearing coercions, fractional/string ELT selectors, and the
      already-pinned binary-result compositions remain exact structured
      contractions. Constant folding must preserve those contractions rather
      than evaluating them through the deleted kernels. Planner argument typing
      and TiKV signature selection remain as bridge metadata, not native kernels.
      Validation is 1185 library and 77 external tests, focused session SQL in
      both feature modes, expression/query diffs, runtime 322 receipts / 2064
      engine rows / 160 borrowed rows, static 216 admitted / 168 excluded / 0
      missing, and repository lint. The full session suite adds no failures to
      the four carried math/crypto contractions. Broad replay remains red by
      design: Native has 182 divergences; Copying has 167 and executes 416246
      engine rows. This is not a compatibility claim.
- [x] Sixteenth physical-deletion tranche: physically removed the complete
      `string_fn.rs` module and its LOCATE/INSTR/POSITION and TRIM kernels,
      collation-aware search helpers, offset conversion, byte-level trim helper,
      module declaration, value dispatch, scalar bypasses, and direct helper
      tests. AST POSITION/TRIM and all scalar/value boundaries now refuse before
      evaluating children. The pinned bridge loses required collation/direction
      metadata for these signatures (including a confirmed utf8mb4_bin result
      mismatch), so all shapes are explicit structured contractions rather than
      potentially wrong TiKV values. Whole-call differential classification is
      bounded to one complete outer call, plus one exact two-column corpus row;
      nested or additional SELECT expressions cannot be masked. Former Go values
      remain as independent oracle data in migrated tests. Runtime receipts fall
      intentionally from 322/2064 to 317/2026; borrowed rows remain 160.
      Validation is 1185 library and 77 external tests, focused session/collation
      SQL in both feature modes, expression/query diffs, static 212 admitted / 172
      excluded / 0 missing, and repository lint. Full session has only the four
      carried math/crypto failures. Broad replay remains red by design: Native
      has 182 divergences; Copying has 167 and 416246 engine rows. This is not a
      compatibility claim.
- [x] Seventeenth physical-deletion tranche: removed `builtin_ext/info.rs`, its
      family registration and dispatch hook, eliminating native FORMAT_BYTES,
      FORMAT_NANO_TIME, TIDB_DECODE_PLAN, TIDB_DECODE_BINARY_PLAN and
      TIDB_ENCODE_SQL_DIGEST implementations, formatter/coercion helpers and plan
      codec wrappers. All names now hit the existing miscellaneous fail-closed
      guard before argument coercion or warnings. Admission rows remain excluded
      but now carry the concrete removed-kernel reason instead of NOT_TRIAGED.
      Direct helper, rewriter, simple-expression, session and differential tests
      retain former Go values as independent oracle data and assert structured
      refusal. Constant folding ratchet entries keep all five names visible.
      Validation is 1178 library and 77 external tests, SQL-digest contraction
      in both feature modes, expression/query diffs, runtime 317 receipts / 2026
      engine rows / 160 borrowed rows, static 212 admitted / 172 excluded / 0
      missing, and repository lint. Full session has only the four carried
      math/crypto failures. Broad replay remains red by design: Native has 182
      divergences; Copying has 167 and 416246 engine rows. This is not a
      compatibility claim.
- [x] Eighteenth physical-deletion tranche: removed native ISNULL and
      IS_IPV4/IS_IPV4_COMPAT/IS_IPV4_MAPPED/IS_IPV6 value dispatch, IP parsing
      and raw-byte predicate helpers, the ISNULL chunk-bitmap shortcut, and the
      raw AST IS NULL/UNKNOWN evaluator. Direct native entry points fail closed
      before child evaluation. Admission remains enabled: ISNULL columns and
      leaf IP columns execute through the TiKV bridge (including its NULL mask),
      while constant-only Session corpus statements are explicit bounded
      contractions because that path evaluates before retained program
      execution. Planner null-rejection keeps its symbolic IF(ISNULL(inner),...)
      proof without creating a runtime evaluator or folding ISNULL calls away.
      The prepared point-get IS NULL residual evaluator and its dead
      contradiction-plan machinery are also deleted; the fast path declines and
      a paired prepared-session test requires positive TiKV row accounting. The
      dedicated constant-folder ISNULL handler is deleted too: constants,
      NOT-NULL columns and deferred parameters retain the scalar call until the
      TiKV/structured-unsupported boundary.
      Affected join, subquery, CHECK, ordering and truth-predicate tests preserve
      former expected values while asserting the precise contraction; neighboring
      successful shapes keep value assertions. Validation is 1181 library and 77
      external tests, expression/query diffs, runtime 319 receipts / 2062 engine
      rows / 160 borrowed rows, and repository lint. Session returned to only the
      four carried math/crypto failures: Copying 334 passed and feature-off 332.
      Broad replay remains intentionally red: Native has 176 divergences;
      Copying has 167 divergences and 416271 engine rows. This is not a
      compatibility claim.
- [x] Nineteenth physical-deletion tranche: `builtin_ext/compare2.rs`, its dead
      local signature metadata, and native LEAST/GREATEST/INTERVAL value,
      typed, temporal, numeric, string, vector and lazy kernels plus scalar
      dispatch arms are physically deleted. TiKV lowering derives its engine
      signature directly.
      Direct AST/value/chunk boundaries fail closed before child evaluation.
      Former Go and TiDB value tables remain as independent oracle data;
      simple admitted integer/decimal shapes additionally require TiKV engine
      values while lowerer-declined shapes explicitly verify contraction.
      Library is green at 1171 passed / 99 ignored; external is 77 passed;
      expression/query differential are 3/4 passed; static inventory remains
      212 admitted / 172 excluded with no missing names; runtime is now 321
      fixtures / 2074 engine rows / 160 borrowed rows after adding independent
      LEAST/GREATEST value receipts. Session Copying is back
      at the carried 334 passed / 4 unrelated failures and feature-off at 332
      passed / the same 4. Broad replay remains intentionally red: Native 176
      divergences / 9877 compared; Copying 167 / 10253 with 416267 engine rows
      and zero borrowed rows. `make -j1 lint` passes. These replay numbers are
      contraction evidence, not a compatibility claim.
- [x] Twentieth physical-deletion tranche: delete the single-purpose `build.rs`
      string-length evaluator, its exported typed-build API, the AST/scalar
      LENGTH/OCTET_LENGTH/CHAR_LENGTH/CHARACTER_LENGTH branches, and all native
      byte/rune-count helpers. All four local boundaries now fail closed before
      child evaluation; admitted text/binary column shapes execute in TiKV and
      lowerer-declined literals/casts/nested removed children are explicit
      contractions. Library is 1167 passed / 99 ignored; external is 77 passed;
      expression/query differential are 3/4 passed; session Copying is 334
      passed / 4 carried failures and feature-off is 332 / the same 4. Static
      and runtime gates pass. Broad replay remains intentionally red: Native
      176 divergences / 9830 compared; Copying 167 / 10253 with 416268 engine
      rows and zero borrowed rows. `make -j1 lint` passes. Review found that
      admitted constant length shapes were initially masked as contractions;
      the harness now routes them through Copying with positive row accounting,
      leaving only five exact lowerer-declined statements as contractions.
- [x] Engine-only demo cutover: `EvaluatorSuite` and predicate filtering now
      have one production execution path. A TiKV context is mandatory, every
      admitted expression executes in TiKV, and every compile/runtime decline
      returns `ExternalEngine` immediately; the old constant, scalar loop and
      native vector fallback branches are absent from the central evaluator.
      New sessions default to the copying TiKV backend. Setting the backend to
      `None` is retained only as a proof hook and returns `requires a TiKV
      expression context` without changing the engine-row counter. The runnable
      `tikv-expr` is now a default feature of `tidb-expr`, `tidb-executor` and
      `tidb-session`, so an ordinary build remains usable. The runnable
      `tidb-session` example executes projection plus predicate SQL, returns
      rows `(1,42),(2,NULL)`, records five TiKV engine rows, then proves native
      replay is refused. Exact command: `cd rust && cargo run -p tidb-session
      --example engine_only_expression_demo --locked --offline -j1`. The focused two-test session gate passes. The broad
      external session suite is deliberately not re-blessed: 271 pass and 67
      fail because declined/row-major/session-owned shapes now surface the new
      structured engine-only errors. Native kernel source is still present and
      is the next deletion phase; this is a runnable architectural cutover demo,
      not final physical removal or full compatibility.
- [x] Twenty-seventh physical-deletion tranche: remove the complete
      `Expression::eval` dispatcher and `ScalarFunction::eval` implementation,
      including its native signature ladder, child-recursive evaluation,
      integer fast path, direct cast/time/JSON/function dispatch and family
      guards. The deleted scalar block was 1,201 lines; the tranche is 21
      insertions / 1,316 deletions overall. The nine remaining production
      callers now use `EvaluatorSuite` through `eval_expression_once`, or were
      deleted when they existed only to serve the native dispatcher (deferred
      row evaluation and JSON schema cache evaluation). Constant-folding with a
      resolver that lacks a TiKV context now leaves the tree unfolded instead
      of invoking native code. `cargo check -p tidb-session` passes; the focused
      two-test engine-only session gate passes; the runnable SQL demo still
      reports five TiKV engine rows and refuses the no-context replay probe.
      Native helper/kernel modules still remain and must be deleted next; this
      tranche removes the two central native dispatchers, not the whole source
      inventory.
- [x] Twenty-eighth physical-deletion tranche: remove the complete raw-AST
      `eval` / `eval_in` evaluator (397 lines), including literal arithmetic,
      row comparison, user-variable side effects, function/cast/time dispatch,
      LIKE/IN/BETWEEN/CASE recursion and its native short-circuit paths. The
      only production AST consumer found by compilation, `SHOW ... WHERE`, now
      crosses an explicit `rewrite_expr` + `EvaluatorSuite` bridge exported by
      `tidb-executor`; it returns a structured missing-context/decline instead
      of replaying native code when its resolver cannot supply TiKV state.
      Temporal-default unary parsing uses the same bridge. The tranche is 20
      insertions / 399 deletions; `cargo check -p tidb-session` and the runnable
      engine-only SQL demo pass unchanged with five TiKV engine rows and a
      refused no-context probe. `SHOW ... WHERE` is currently an explicit demo
      contraction until its virtual-row resolver carries statement TiKV state.
- [x] Twenty-ninth physical-deletion tranche: delete `func.rs` in full (1,623
      lines), removing the unreachable value-list/AST builtin router, its native
      control/string/date/row kernels and every family-specific runtime guard.
      Rewriter-only crypto and temporal-residual refusal predicates, plus the
      sequence path separator, are retained locally as construction metadata;
      no evaluator implementation moved with them. Production compiles without
      the module, proving the router had no remaining runtime caller after the
      three central dispatcher cuts. The tranche is 34 insertions / 1,636
      deletions; `cargo check -p tidb-session` and the SQL demo pass with the
      same five TiKV engine rows and refused no-context probe.
- [x] Thirtieth physical-deletion tranche: remove 987 lines from
      `scalar_function.rs`: native return coercion, integer/decimal comparison,
      NOT filtering, decimal arithmetic, cast decoding, overflow rendering and
      IN-string hash execution caches. The rewriter no longer prepares the
      native IN cache, and the now-unreferenced JSON-schema evaluator cache is
      removed as well. AST node metadata, grouping/hash identity, collation and
      TiKV lowering inputs remain. The tranche is one insertion / 1,017
      deletions; `cargo check -p tidb-session` and the runnable SQL demo pass
      with five TiKV engine rows and an explicitly refused no-context probe.
- [x] Thirty-first physical-deletion tranche: delete the complete native JSON
      dispatcher and all construct/merge/modify/path/predicate/report/search/text
      kernels plus their native test module. `builtin_ext/json` contracts from
      ten production files and 2,878 lines to a 128-line `CAST(... AS JSON)`
      datum/type bridge in two files; no JSON SQL function dispatch remains.
      The unused `jsonschema`/`fluent-uri` dependency chain is removed from the
      workspace and lockfile, while `reqwest`'s independently required JSON
      feature is made explicit. The code/dependency tranche is 40 insertions /
      5,026 deletions before this ExecPlan entry. Locked offline
      `cargo check -p tidb-session`, the two focused session tests and the SQL
      demo pass with five TiKV engine rows and a refused no-context probe.
- [x] Thirty-second physical-deletion tranche: delete `time_fn` completely:
      calendar/date arithmetic, duration parsing, add/subtract, extract and
      dispatch kernels (3,832 production lines) plus 1,902 lines of native
      temporal tests. The only production dependencies exposed by compilation
      were type-shaping helpers: DATE cast parsing is retained locally in
      `cast.rs`, and DATE_ADD result-FSP inference locally in
      `rewriter/result_type.rs`; neither evaluates a SQL function. The stale
      crate-level native-evaluator documentation and unused public interval
      helper are removed. The tranche is 112 insertions / 6,023 deletions;
      locked offline `cargo check -p tidb-session`, the focused session tests
      and the SQL demo pass with five TiKV engine rows and refused native replay.
- [ ] Post-tranche-32 deletion frontier (2026-03-24): the remaining source
      inventory is intentionally measured before another cut, not counted as
      native-kernel LOC. `builtin_ext/json` is only a 128-line cast/type bridge
      and `time_fn` is absent. The four core dispatch/bridge files total 6,770
      lines (`scalar_function.rs` 1,647, `evaluator.rs` 2,227, `cast.rs` 2,367
      and `lib.rs` 529). The next physical cut should separate and delete the
      remaining generic cast/operator/coercion kernels while retaining parser,
      result-type, transport and TiKV lowering pieces. This snapshot prevents the outside-call-site
      audit from being mistaken for completion: substantial native expression
      implementation remains.
- [x] Twenty-sixth physical-deletion tranche: delete the native
      FROM_UNIXTIME and UNIX_TIMESTAMP session-zone kernels plus the complete
      `time_fn/session_tz.rs` module (489 lines), including fixed/named-zone,
      DST-gap/ambiguity and epoch conversion helpers. All four native runtime
      boundaries fail closed with `native session temporal evaluation was
      removed; TiKV engine required`. The former source tests retain their
      names and independent values as explicit contractions. Constant-only
      corpus rows use a 23-statement exact-SQL allowlist; integration replay
      never masks this marker. Review found that pinned-engine UNIX_TIMESTAMP
      cannot reproduce named-zone DST gap/ambiguity semantics, so that function
      is admission-excluded rather than returning wrong values. FROM_UNIXTIME
      typed-column shapes still execute in TiKV: the temporal extended receipt
      records three rows and six engine rows (signatures 4/6088), with zero
      fallback. Raw-TSO AS OF remains operational; DATETIME AS OF and
      UNIX_TIMESTAMP partition bounds are explicit contractions. Library is
      1169 / 99 ignored; external is 77; expression/query differential are 3/4;
      session Copying is 334 / 4 carried failures and feature-off is 332 / the
      same 4. Static/runtime gates pass at 211 admitted / 173 excluded and 320
      fixtures / 2068 engine rows / 160 borrowed rows. Broad replay remains
      intentionally red: Native 197/9781; Copying 178/10232 with 416220 engine
      rows and zero borrowed rows. These numbers are contraction evidence, not
      a compatibility claim.
- [x] Twenty-fifth physical-deletion tranche: remove the excluded native
      CONVERT_TZ, FROM_DAYS, TIDB_PARSE_TSO and TIMESTAMPADD kernels, dispatch
      arms and exclusive helpers. The complete `convert_tz.rs` implementation is
      physically absent; TIMESTAMPADD's dedicated AST path now refuses before
      either child is evaluated. All four ordinary native boundaries fail closed
      with `native temporal residual evaluation was removed; function
      unsupported`. Parser/result-type metadata remains, and FROM_DAYS stays
      admission-excluded because the pinned TiKV engine's out-of-range zero-date
      behavior differs from TiDB's NULL sub-band. Deleted CONVERT_TZ source tests
      retain their names and independent fixed-offset/DST/invalid-zone former
      oracles as contraction tests. Construction also refuses all four names
      before recursively rewriting/folding children. The replay harness
      recognizes TIMESTAMPADD's dedicated AST and uses only a whole-statement
      exact-SQL allowlist; lazy children and DDL defaults are regression-tested
      as unmasked. Library is 1169 passed / 99 ignored; external is 77;
      expression/query differential
      are 3/4; session Copying is 334 / 4 carried failures and feature-off is
      332 / the same 4. Static/runtime gates and `make -j1 lint` pass at 212
      admitted / 172 excluded and 321 fixtures / 2074 engine rows / 160
      borrowed rows. Broad replay remains intentionally
      red: Native 192/9804; Copying 178/10233 with 416224 engine rows and zero
      borrowed rows. These replay numbers are contraction evidence, not a
      compatibility claim.
- [x] Twenty-fourth physical-deletion tranche: remove the native statement-clock
      and TiDB clock-metadata kernels for NOW/CURRENT_TIMESTAMP, LOCALTIME,
      LOCALTIMESTAMP, UTC_TIMESTAMP, CURDATE/CURRENT_DATE, UTC_DATE,
      CURTIME/CURRENT_TIME, UTC_TIME, SYSDATE, TIDB_BOUNDED_STALENESS and
      TIDB_CURRENT_TSO. Dispatch, scalar conversion branches, dedicated clock
      helpers, host-clock reads, FSP error plumbing and obsolete Columns seams
      are physically absent. All four native boundaries fail closed before
      child evaluation; result-type and parser/registry metadata remain. The
      statement-owned DEFAULT CURRENT_TIMESTAMP/CURRENT_DATE bridge remains
      separate from ordinary SQL-function execution and keeps fresh-store
      bootstrap working without rebuilding a clock kernel. All 14 direct SQL
      spellings have one session contraction table with former independent
      oracles. The two TiDB metadata functions and LOCAL aliases have no wire
      signature; the other clock signatures are excluded because TiKV needs a
      host clock. Library is 1167 / 99 ignored before the added metadata-only
      test; external is 77; expression/query differential are 3/4; session
      Copying is 334 / 4 carried failures and feature-off is 332 / the same 4.
      Static/runtime gates pass at 212 admitted / 172 excluded and 321 fixtures
      / 2074 engine rows / 160 borrowed rows. Broad replay remains intentionally
      red: Native 192/9804 and Copying 178/10233 with 416225 engine rows and
      zero borrowed rows. Clock markers are never replay-masked, including
      lazy children and DDL defaults. These numbers are contraction evidence, not a
      compatibility claim. Final library count after adding the metadata-only
      result-type test is 1168 passed / 99 ignored.
- [x] Twenty-third physical-deletion tranche: remove the excluded native
      TIDB_PARSE_TSO_LOGICAL, GET_FORMAT, SEC_TO_TIME and TIME_FORMAT kernels,
      dispatch arms and exclusive helpers (323 production lines). GET_FORMAT's
      dedicated AST escape now refuses before child evaluation; stale scalar
      typed and argument-cast seams are absent. These four have no admitted
      TiKV signature and contract with the structured temporal-tail marker.
      Library is 1167 / 99 ignored; external is 77; expression/query
      differential are 3/4; session Copying is 334 / 4 carried failures and
      feature-off is 332 / the same 4. Static/runtime gates pass. Broad replay
      is Native 192/9825 and Copying 167/10253 with 416272 engine rows and zero
      borrowed rows.
- [x] Twenty-second physical-deletion tranche: remove the native DATE,
      MICROSECOND, TIME, YEARWEEK, TIME_TO_SEC, TIMEDIFF, MAKEDATE, MAKETIME,
      PERIOD_ADD and PERIOD_DIFF dispatch arms/kernels plus private TIMEDIFF
      and period helpers (401 production lines removed). A single independent
      Copying receipt proves all ten values and positive TiKV rows. The exact
      `period_add(9223372036854775807,1)` shape contracts because TiDB's former
      result is `i64::MIN` while TiKV returns 27201459512. Library is 1167 / 99
      ignored; external is 77; expression/query differential are 3/4; session
      Copying is 334 / 4 carried failures and feature-off is 332 / the same 4.
      Static/runtime and lint pass. Broad replay is Native 192/9824 and Copying
      167/10253 with 416271 engine rows and zero borrowed rows.
- [x] Twenty-first physical-deletion tranche: remove the native MONTH,
      DAY/DAYOFMONTH, DAYOFWEEK, DAYOFYEAR, WEEKDAY, QUARTER, WEEKOFYEAR,
      MONTHNAME, DAYNAME and LAST_DAY dispatch arms, kernels and private
      single-date/datetime parsers. Valid retained shapes are TiKV-required and
      an independent integration receipt proves values plus positive engine
      rows. The one mixed zero-month QUARTER statement is an exact contraction:
      TiDB answers 0 for `2008-00-01`, while TiKV answers NULL. Library is 1167
      passed / 99 ignored; external is 77 passed; expression/query differential
      are 3/4 passed; session Copying is 334 / 4 carried failures and feature-off
      is 332 / the same 4. Static/runtime and lint pass. Broad replay remains
      intentionally red: Native 192 divergences / 9824 compared; Copying 167 /
      10253 with 416267 engine rows and zero borrowed rows.
- [x] Recovery audit: pin TiKV `d847323beba1e93513314018fbb5ee946e4b9c79`
      in `crates/tidb-expr/Cargo.toml` and regenerate `Cargo.lock`. The selected
      borrowed facade was used by the adapter while the manifest still pinned
      an older engine without that API; local Cargo patches had hidden this.
      Unpatched fork validation: 1212 lib tests passed (99 ignored) and 60
      integration tests passed. Commands and limitations are recorded below.
- [x] Fix selected borrowed dense bounds in TiKV: a `None` selection with
      `output_rows > row_count` now returns an error before any sink callback.
      The new regression failed before the fix; the engine package now passes
      481 tests. Added cross-batch independent selection, empty-selection and
      invalid-length/index/nonfinite preflight coverage.
- [x] Route Window partition/order key comparisons through retained suites and
      single-row selections at their original demand points. Three focused
      tests cover engine receipts, one compilation across repeated calls,
      skipped overflowing rows/keys and selection restoration after errors.
      Full executor regressions passed with engine on/off (see artifacts).
- [x] Route Window FIRST/LAST/NTH_VALUE and LEAD/LAG argument/default reads
      through retained suites, only on the demanded target/current row. New
      emission-path tests seed a drained buffer and exercise `WindowExec::next`
      with native and engine contexts, including skipped overflowing arguments
      and defaults, missing NTH targets, and selected default error restoration.
- [x] Route Window RANGE calculation/comparison through retained per-bound
      suites. New tests compare native and engine paths for ascending/descending
      scans, comparison-key short-circuit, selected errors and CURRENT ROW /
      UNBOUNDED paths. There are no direct native eval calls left in `window.rs`;
      suite-level native admission/fallback remains, so Window is not engine-only.
- [x] TiKV `eval_borrowed_selected_columns_shared` accepts independent physical
      lengths per input column. The existing shared-count API remains compatible
      and delegates without allocating a row-count vector. Tests cover unequal
      lengths across batches, null/repeated/reordered rows, mixed dense and
      selected inputs, empty output, constants and preflight failures.
- [x] TiDB adapter exposes `evaluate_selected` and `evaluate_into_selected`
      for explicit physical rows without mutating the source chunk's selection.
      Both copying and borrowed paths, plus representability preflight, use the
      supplied selection. Existing APIs delegate using `input.sel()`. Tests
      cover reordered/repeated rows, unselected overflow/nonfinite input,
      input-selection preservation, dense/empty selection and selected errors.
- [x] `EvaluatorSuite::eval_selected` evaluates explicit physical rows through
      the existing shared program/cache and uses `Chunk::physical_row` for the
      native coexistence path. Tests prove reordered/repeated selection across
      two suites compiles once, preserves the input selection, and matches both
      engine backends and native results; a decimal test guards against applying
      the original chunk selection a second time.
- [x] Fix the mandatory-engine row-major dispatch hole. A `getvar` regression
      failed before the fix; row-major programs now emit a `NotAdmitted` receipt
      and mandatory-engine contexts receive an `ExternalEngine` error before
      any native side effect. Optional-engine behavior stays native but visible.
- [x] Route `joiner::eval_bool` through `ConditionEvaluator` and selected suites,
      preserving condition-by-condition short-circuit and NULL-from-IN
      continuation. Semi-family joiners retain the programs; clones share them
      through `Arc`. Tests assert engine execution, physical row semantics,
      skipped later compilations/errors and shared compilation counts.
- [x] Retain full/residual programs on `JoinExec` for datum-row and index-pair
      matching, and share them with scalar index-hash task/worker descriptors.
      Keep ordinary NULL-immediate-rejection separate from the anti-semi CNF
      NULL-from-IN continuation policy. Refresh residual programs whenever
      merge-key selection rewrites the residual expression list.
- [x] Audit native-site classification scopes: the old first-`#[cfg(test)]`
      heuristic hides ten production calls and misses three file-wide test-only
      calls. That audit's 39 evaluator sites split into 26 production-scope / 13
      test-only after manual correction, or 24 excluding the known unlinked
      duplicate. Earlier production/test splits are historical heuristic output,
      not an accurate deletion-work count; source evidence is in the inventory.
- [x] Repair the classifier with bounded attribute/item scopes and conservative
      cfg evaluation. The initial 16-test suite failed before the fix; all 18
      final tests pass. The script reproduced that manual 26 production /
      13 test-only split. Unknown scopes remain production-labelled; this
      textual inventory still is not a reachability or deletion-readiness proof.
- [x] Route `matches_chunk_rows` through retained ordinary-match programs.
      Serial chunk-backed probes and exact/general parallel workers now share
      the same residual compilation cache rather than owning another expression
      vector. Next-loop tests assert results, exact engine rows, multiple task
      windows and one compilation; NULL still skips an overflowing later term.
      That step's inventory: 38 sites, 25 production-scope / 13 test-only,
      or 23 production-scope after excluding the known unlinked duplicate.
- [x] Route local `IndexJoinLookupExec` filters through ordinary-match programs,
      shared by the source, fork template and rebuilt tasks. `set_filters`
      refreshes the source program set without mutating old templates. Three
      tests verify local Next/engine rows/cache reuse and NULL/FALSE/error
      ordering. Template construction, not remote cursor opening, is tested.
      That step's inventory: 37 sites, 24 production-scope / 13 test-only, or 22
      production-scope after excluding the known unlinked duplicate.
- [x] Route index-probe bounds through shared per-bound programs and selected
      physical rows, without a new scratch copy. Regression coverage pins key
      rejection, NULL short-circuit, pre-dedup demand, unselected overflowing
      rows, cross-batch compilation reuse and recovery after a demanded error.
      The first run caught an incorrect column-swap setting for direct bounds;
      calculated-value mode fixed it. No direct native call remains in `join.rs`.
      That step's inventory: 36 sites, 23 production-scope / 13 test-only, or 21
      production-scope excluding the unlinked duplicate. Fallback still exists.
- [x] Route all seven direct aggregate argument/order-key sites through
      per-expression programs retained in `AggInputMode`. Keep typed dispatch
      as `AggInputKind`; cloned plans/bindings share caches across groups rather
      than putting metadata in group state or mutable `AggFunc` descriptors.
      Tests pin NULL/extra-argument/sort-key/FIRST_ROW demand, physical selection,
      engine row counts and one compilation per used argument across clones.
      That step's inventory: 29 sites, 16 production-scope / 13 test-only, or 14
      production-scope excluding the unlinked duplicate.
- [x] Retain window aggregate input programs across frame recomputation in
      `WindowAggregateEvaluator`. Every frame still starts a fresh accumulator.
      Emission tests pin overlapping-frame results, empty/FIRST_ROW frames,
      delayed overflow demand and one compilation across frames. Reproducing
      per-frame reconstruction made both new cache assertions fail first.
      Typed aggregate kernels remain outside expression-engine row receipts;
      this does not replace recomputation with a sliding-window algorithm.
- [x] Retain Selection predicates in `FilterProgram` and remove its duplicate
      NULL/string-IN kernels. Batch and VecEvalBool engine requests now enter
      admission rather than silently using native kernels. The required-engine
      getvar regression failed before the fix and now rejects before effects.
      Pin physical masks, NULL policy, cross-chunk cache reuse and row-mode
      side-effect demand. That step's inventory: 28 sites, 15 production / 13 test,
      or 13 production excluding the unlinked duplicate. Convenience filter
      APIs outside Selection still need caller-retained programs.
- [x] Retain UnionScan generating programs and CNF conditions. Borrow the
      existing mutable-row chunk per expression; finish cast/zero substitution/
      writeback before the next dependency. Tests pin real Next receipts/cache
      reuse and stopping/recovery after overflow. Replace Sort's column-only
      AST dispatch with validated materialized-cell transfer (not engine work),
      preserving deferred-constant skipping and unsupported-shape rejection.
      That step's inventory: 26 sites, 13 production / 13 test, or 11 production
      excluding the dead duplicate.
- [x] Route general generated-row and computed-default evaluation through the
      facade, preserving physical row selection, name rebinding, virtual empty
      rows, and the existing session rewrite/cast boundary. Reproduced and fixed
      BinaryLiteral tag loss (16 became 0 or an error) with a separate
      `eval_selected_for_cast` path; ordinary typed projection APIs stay intact.
      Reproduced and fixed missing ENUM/SET numeric metadata adaptation in Sort
      and late-cast scalar results. That step's inventory: 24 sites, 11 production /
      13 test, or 9 production excluding the unlinked duplicate.
- [x] Retain pushed-scan conditions, sharing caches across clone/conjoin and
      rebuilding after column remapping. Remove local IN/LIKE expression
      shortcuts; preserve FALSE/NULL demand. Route both access-cost evaluator
      sites; share one program across each estimate's TopN/bounds/NULL samples.
      Scalar facade accepts erased contexts without dropping statement methods.
      Reproduce/fix binary-literal truth loss in ConditionEvaluator/FilterProgram
      using the existing scalar-preserving entry. That step's textual inventory:
      21 evaluator sites, 8 production / 13 test, or 6 production excluding the
      unlinked duplicate. This does not inventory every forwarding helper.
- [x] Retain UPDATE scalar/physical and post-Apply DML programs; preserve fresh
      row binding, physical indexes, Apply order and scalar assignment values.
      Fix zero-column virtual input. Remove sparse partition native bypass via
      the scalar-preserving facade (temporary program, not cross-bound caching).
      That step's classifier: 39 raw / 23 candidates / 5 production / 18 test;
      manual inspection subtracts two dead duplicate calls and two routed DML
      wrappers, leaving one direct live candidate (INSERT VALUES). Earlier
      counts included wrappers and were not direct-native-call counts.
- [x] Route explicit INSERT VALUES through prepared suites at original demand
      points. Repair scalar helpers: preserve original indexes and scalar kinds,
      provide virtual empty rows, and return Some/error instead of external
      native-fallback requests. Reordered operands reproduced -7 versus 7.
      Current narrow inventory: 38 raw / 22 candidates / 4 production / 18 test;
      four production candidates are dead duplicates or routed DML wrappers.
- [x] Admit verified nonzero DATE/DATETIME constants and typed NULLs through
      existing MysqlTime wire encoding. Require matching kind/FSP and physical
      bounds; reject TIMESTAMP (including hidden timestamp values), packed zero
      and metadata mismatches. Engine-required tests cover offsets, SQL modes,
      copying/borrowed transports, partial/invalid dates and nested YEAR.
- [x] Permit packed-zero DATE/DATETIME only when engine compilation is warning-
      free. Test permissive execution, restrictive-mode compile decline, required
      rejection, optional native fallback and mode changes on the same program.
      Decoder warnings never reach the host, including with warning capacity 0.
- [x] Admit matching TIMESTAMP constants when the effective compile timezone is
      proven UTC (offset zero with no name, or exact named UTC). Recursively pass
      context into admission and test retained programs across timezone switches,
      both transports, typed NULL, nested YEAR, zero and mismatched values.
- [x] Reuse TiKV's codec for fixed-offset TIMESTAMP literal packing, with exact
      warning-free round-trip validation. Local lowering supplies Context; shared
      distributed catalog/output bridge remain unchanged. Test +/- offsets, day/
      year boundaries, hidden fractional micros, NULL, cache/profile switches.
- [ ] Unify packed-zero decoding under restrictive modes and named-zone/DST
      TIMESTAMP transport. London fold has a reproduced native/engine instant
      mismatch; named non-UTC zones remain declined. No default switch yet.
- [x] Admit direct, bounded signed/unsigned BinaryLiteral integer CAST via the
      existing TiKV kernel, retaining numeric output kinds and compiled programs.
      Refuse ordinary binary-collated non-null constants in integer CAST after
      reproducing native 1 versus engine 49; keep signed high-bit casts excluded.
- [x] Admit canonical BIT roots only, preserving exact width/padding and Datum
      kind through existing MysqlBit/bridge paths. Keep nested consumers closed.
- [x] Reproduce unsafe implicit binary-string casts and enforce a common
      signature+operand-shape guard for local nodes and catalog substitution,
      preserving only source-authorized literal CAST subtrees.
- [x] Add TiKV opt-in compile_with_text_constants using existing numeric text
      kernels, keeping legacy default dispatch. Fix MysqlBit scalar CAST detection;
      test interleaved policies, truncation diagnostics and a skipped failing arm.
- [x] Pin/adopt engine db9c7f0 in TiDB with one fixed text-constant compile policy.
      Encode only source-authorized numeric literals as MysqlBit + CastIntAsInt;
      preserve raw payload bytes and let TiKV decode. Existing guards remain.
- [x] Admit direct cast_signed/cast_unsigned of canonical binary VarString
      String/Bytes constants containing 1..6 ASCII digits. Keep their textual
      wire kind distinct from BinaryLiteral numeric transport; verify kinds,
      zero warnings, both transports, retained compilation and mode profiles.
- [x] Repair static inventory checks: scan production functions after test items,
      resolve symbolic exclusion reasons, reject unparsed rows, and ignore only
      informational snapshot HEADs when comparing committed reports. Generated
      table has 384 rows (223 declared admitted / 161 excluded), no missing names.
- [x] Add a CI-callable runtime receipt gate for the fixed tikv_coverage module:
      copying/borrowed engine parity fixtures (explicitly not a semantic oracle),
      actual row/borrowed accounting, exact baseline comparison, and failure/
      empty/ignored/fallback/identity/count rejection. Independent retained-math
      source vectors remain in the unit/source corpus. Reviewed post-math-deletion baseline:
      334 receipts, 2150 engine rows, 160 observed borrowed rows, 30 tests
      passed (47 filtered out).
- [ ] Wire/validate the runtime gate in actual hosted CI and extend engine-only
      coverage beyond this fixture set. Static names and runtime receipt counts
      are not proof of all SQL shapes or native deletion readiness.
- [ ] Prove native value/diagnostic parity for remaining ordinary binary-string
      numeric profiles before relaxing implicit/real/range/truncation guards.
- [ ] Add a literal-kind carrier/provenance contract for root/lazy forwarding and
      remaining binary/BIT coercions. Direct numeric CAST is only a narrow subset.
- [ ] Audit expression-internal and other forwarding entrypoints before native
      removal; retain partition programs at a safe owner. Zero direct live hits
      in the narrow outside-core `.eval` inventory is not deletion readiness.
- [ ] Retain generated/default compatibility programs in statement-owned plans
      with schema/zone/LIKE rewrite invalidation. The public mutable descriptors
      are not safe cache owners; temporary compilation and gather copies remain.
      Binary literal and clock admission gaps still need engine support.
- [x] Retain hash JoinExec outer-filter programs per open, reusing across build
      and probe rows/chunks. Reopen replaces the expression snapshot; a false
      first CNF term never compiles an unreachable unsupported tail. Direct
      executor tests cover 8204 engine filter rows with one compile per open.
- [x] Reuse the same owner for Merge outer-filter programs. Add 4102 engine
      filter rows across reopen/chunks and six NULL-filter rows in left/right
      outer joins; preserve outer rows and skip unsupported CNF tails.
- [x] Stop hash probe filtering immediately on a child error, even with partial
      rows. Preserve the original failure, restore an empty typed chunk, and
      prove zero filter execution/compilation in native and engine contexts.
- [ ] Retain condition programs in remaining `eval_bool` hot callers (the public
      convenience wrapper currently builds temporary programs). Existing joined scratch-row copies remain;
      eliminating them requires the independent-column facade, not more row
      copies. TiDB is pinned to engine `db9c7f0`; borrowed lazy remains unsupported.

- [x] Milestone A foundation (partial point 6): shareable engine metadata.
      TiKV metadata is `Send + Sync`, `PreparedExpression` is asserted
      `Send + Sync`, and a compiled program is split from caller-owned
      `ExecutionState`. The TiDB adapter caches the compiled programs on the
      shared `EvaluatorProgram`, and a test proves one plan compiles ONCE for
      three suites while a real statement-policy change recompiles and the
      copying/borrowed backend does not.
- [x] Milestone B foundation (partial point 2): explicit admission table and
      observable fallback gate. Current static inventory has 384 rows
      (223 declared admitted, 161 excluded with a reason) covering the
      309-name Go-derived registry plus the synthesized spellings; a test fails
      if a name has no row. Falls back are reported as `NotAdmitted`,
      `LazyRisk` or `UnrepresentableInput` and the SQL differential helper
      fails on a silent fallback.
- [x] Milestone C foundation (partial point 1): signature-driven lazy engine
      kernels and TiDB acceptance tests in `crates/tidb-expr/tests/tikv_lazy.rs`.
      The wire format is unchanged. `eager_lazy_risk()` checks lazy-sensitive
      nodes lacking lazy_fn_ptr; keep that full-tree gate, even when today's
      registered signatures yield no risk. It is not a general SQL admission
      proof, and requested Borrowed mode is not borrowed-lazy support. TiDB's
      vectorized short-circuit/default-switch acceptance remains separate.
- [x] Milestone D foundation (partial point 4): Set transport support.
      This is not proof that all native representations can be removed:
      literal provenance and temporal transport gaps remain above. Implemented: `FieldTypeTp::Set -> EvalType::Set`, the `Set`/`SetRef`/
      `ChunkedVecSet` carriers, chunk and raw-datum codecs, the hybrid
      Int/Bytes borrows, the standalone `Column::Set`, the cast registration
      and the adapter bridge, with round-trip fixtures. A latent
      `Column::get_enum` bug (wrong indexing for every row) was fixed on the
      way. Geometry and arrays were shown not to be needed.
- [x] Every constant case in the 33 Go source-port files dual-runs through the
      engine: `tests/mod.rs::chunk_e` evaluates the same rewritten expression
      both ways, skips only what the adapter declines, and compares errors by
      classification. Its first run found seven real divergences, all now
      fixed at the boundary or recorded.
- [ ] Milestone E: flip the default, delete the native evaluator and the
      `tikv-expr` feature, and re-point the corpora from "dual-run" to
      "engine only". The engine-only measurement exists and is the E work
      list: `TIKV_EXPR_ENGINE_ONLY=1` makes a declined expression a failure,
      and at `47ce598` plus the temporal-bridge fix the lib corpus reports
      1149 passed / 61 failed / 99 ignored, i.e. 59 distinct constant
      expressions still have no engine path (grouped in
      `tikv-expression-corpus-plan.md` section 7). The rate is measured on the
      adapter's unfolded input, though, and production folds constants first
      (`plan_builder.rs` folds right after rewriting): 40 of the 59 collapse to
      a `Constant` and never reach a lowering, 2 are planning-time refusals, so
      the *observed* production surface is 17 (corpus plan 7.13). Column-bearing
      shapes are not in the corpus at all, which is the one direction the
      correction could not bound -- until now: `tests/tikv_column_shapes.rs`
      rewrites 48 shapes through a column resolver and pins 28 engine / 20
      native, so the production shape surface is measured too (corpus plan
      7.14). The 20 are all deliberate: collation/padding (6), missing kernel
      or policy (6), a constant regex the engine compiles at build time (1),
      `max_allowed_packet` (1), the statement clock (1), computed digits (2),
      and three named exclusions. Two of the audited reasons turned out to name
      a *shape* rather than a capability or a policy: `oct` was excluded for its
      binary-literal argument, which the constant rule already refuses (7.16),
      and `if(<datetime>, a, b)` was refused by the lazy-arm leaf rule even
      though a condition is never a skipped arm (7.17). Remaining
      known divergences are listed in the TiKV
      `EXPRESSION_SEMANTIC_GAPS.md` (26 open entries; the CRC32 declaration
      bug and the `LAST_DAY` DATE-shape mismatch are fixed).


## Surprises & Discoveries


The eleventh tranche exposed a non-obvious LOCATE/INSTR boundary: TiKV's
UTF-8 kernel lowercases only for a case-insensitive collator, but the embedded
path still returns 2 for `INSTR('ABC' COLLATE utf8mb4_bin,'b')` instead of 0.
The exact metadata-loss point is not yet isolated, so LOCATE, INSTR and POSITION
remain native and an engine regression probe keeps the gap visible. This avoids
changing TiKV server behavior speculatively.

The parity target is narrower than "all of MySQL": it is whatever the native
evaluator can do today. Functions the Rust port never implemented are out of
scope for the removal, because deleting code cannot lose behavior that does
not exist. Milestone D therefore starts from an inventory of the native
surface, not from a wish list.

`tidb_query_expr` is declared `publish = false` and the TiDB dependency is a
`git` dependency on the whole TiKV workspace. Removing native makes
`tidb-expr` unusable without that fork checkout, which is acceptable for this
exploration but is the reason packaging (the previous point 7) cannot be
postponed forever.

Only about 30 sites make the compiled engine non-`Sync`: four
`Box<dyn Any + Send>` metadata signatures and 24 codegen references. The
thread-local varg buffers do not block `Sync`; they require only that one
compiled program is not executed concurrently on two threads.

The upstream issue `pingcap/tidb#70156` proposes the switch
`tidb_enable_short_circuit_expression` and says TiKV "can introduce
short-circuit expression nodes". It is an open, unimplemented proposal: the
variable does not exist in either checkout and the dispatcher has no lazy
node. Milestone C therefore implements laziness with the wire format
unchanged, by making the evaluator consult a lazy-signature set instead of
emitting new node kinds. That keeps `tipb` untouched, which matters because
the wire schema lives in a different repository.

The engine-on build stopped working because the host toolchain moved under it,
not because of a code change: CMake 4 (4.3.4) rejects the `cmake_minimum_required`
of the `c-ares` copy bundled in `grpcio-sys 0.10.3`, and GCC 16 no longer lets
that copy of abseil get `uint8_t` from a transitive include. `grpcio-sys`
reaches the TiDB Rust test link through `tikv_util`, so both must be worked
around before any engine-on suite runs:

    CMAKE_POLICY_VERSION_MINIMUM=3.5 \
    CXXFLAGS="-w -std=gnu++14 -include cstdint" CFLAGS="-w" \
    cargo test -p tidb-expr --features tikv-expr ...

`cargo`'s `rerun-if-env-changed` turns those into a one-time rebuild of the two
`grpcio-sys` variants. Do not scope the target selection with `--lib` while
doing this: a narrower target set changes feature unification, which mints a
new unit hash and re-runs the C build for nothing.

The adapter's blanket "no temporal cast over a constant" rule in
`tikv::lowering::coerce` was covering exactly one real divergence. Deleting it
and re-running the corpus brought `date('20111213')` and
`month(20240315123045)` in line, and exposed
`last_day(20240315123045)` -- TiKV's `last_day` is typed `DateTime` internally
and returns a midnight `DateTime` for a `DATE`-declared result, which the exact
bridge rejected as "unsupported TiKV temporal value shape" while native
answered `2024-03-31`. Go's DATE decoder drops the time part, so
`bridge::check_time` now rebuilds the declared `DATE` from the calendar fields
when the engine kind differs, and leaves an already-`DATE` value (including its
wall fields) untouched -- that is the shape the chunk round trip is defined on.
A blanket rule that costs one line of bridge code should be preferred to
refusing a whole family.

The same lesson then applied in reverse. The `Shape` policy that allows only
leaves in a possibly-skipped lazy arm looks removable, because the engine's
lazy boundary evaluates each child on demand (`ChildHandle::eval` calls
`eval_subtree`), so a skipped arm is never entered. Replacing the leaf rule with
`coerce` and rerunning the dual-run corpus produced three disagreements in one
run: `case when 0.1 then 1 else 2 end` and
`if(cast('0.1' as decimal(2,1)), 1, 2)` answer `2` instead of `1` because Go's
truthiness on a non-zero DECIMAL is not Go's integer cast, and
`coalesce(1, 123.456)` comes back with decimal scale 0 instead of 3. Inserting
an implicit cast changes the *value*, not just the plan, so the leaf rule is a
value rule and stays; the experiment is recorded in
`tikv-expression-corpus-plan.md` section 7.2. The narrow half that is safe did
land: a `NULL` leaf is retagged to the arm's family rather than cast, because
`NULL` is family-less and the engine's validator reads the declared `FieldType`.

The same "it is the same operation" trap sits in the explicit-cast spellings.
`CAST(x AS SIGNED)` arrives as `cast_signed` and `CAST(x AS DATETIME)` as
`cast_datetime`, minted by the rewriter; admitting them against the local
arithmetic arm (which already derives `Cast{source}As{target}` from the
function's static type) took the corpus gap from 61 to 59 and immediately broke
two shapes: a `COALESCE` over `DATETIME(0)` and `DATETIME(3)` lost the promoted
scale (`.000`), and an `INTERVAL` argument through a minted cast rounded the
other way. Reverted; the minted spellings stay native, and the experiment is in
`tikv-expression-corpus-plan.md` section 7.4. The lesson repeats: the dual-run
corpus is cheap and it decides these questions faster than reasoning about the
code does.

The removal's error contract is now measured for the whole remaining set: the
ratchet drives each of the 59 declined expressions through a resolver with
`tikv_expression_required() == true` and asserts the outcome is never a value.
57 return the structured refusal error naming the reason; 2 fail at planning
time (`(1, 2) = (1, 2, 3)` needs a column resolver, `convert(... using cp866)`
names an unsupported charset), which is a different and already-correct
contract. That is corpus plan section 7.12.

The "no session" path was only half-built. `tikv_expression_required()`
reported a *missing engine context* as a structured error, but a resolver that
requires the engine and gets a context that **declines** the expression still
fell through to the native evaluator -- a silent fallback with nothing behind
it once native is gone. Evaluation now returns the same structured
`ExternalEngine` error (code 1105) naming the refusal reason, and the reason is
still recorded first, so a gate can see it. The test uses `translate`, which is
a permanent native exception (no signature in the pinned tipb), so it exercises
the exact shape the removal has to answer for.

`NULLIF` is the first name to leave the "row excluded: no local lowering"
cluster. Go rewrites it to `IF(a <=> b, NULL, a)`, so the lowering is that
tree -- but with the two sides built separately, because MySQL returns
*expr1's* type while the comparison promotes: `NULLIF(1, 1.0)` compares as
DECIMAL and returns BIGINT, and an `If` node that declared BIGINT over a
DECIMAL value child is what the engine refused with `Expect Int, received
Decimal`. The same-type shape needs no cast at all, which is the common
`NULLIF(col, 0)`.

That cluster is now triaged rather than renamed. Its single placeholder reason
("no local engine lowering") read as "write a lowering", but 100 rows shared
it and only some are adapter work: 4 names have no signature in the pinned
`tipb` at all, so no lowering can ever reach the engine and they are permanent
native exceptions; 5 have a signature the engine does not dispatch, so they are
TiKV work; 15 are the explicit-cast spellings section 7.4 rules out; 18 need
session state the facade's `Context` does not carry. The admission table now
says which, a test pins the triaged names, and the method is in
`tikv-expression-corpus-plan.md` section 7.6. For the removal that distinction
is the difference between "a lowering to write" and "a function that can never
be pushed".

The "18 need session state" bucket has since been split further, because
"session state" was hiding two different futures: the ten clock names (`now`,
`current_timestamp`, `curdate`, `current_date`, `curtime`, `current_time`,
`utc_date`, `utc_time`, `utc_timestamp`, `sysdate`) have a wire signature the
engine's dispatch table does not implement -- except `SysDateWithoutFsp`, which
reads the host's own clock -- so they need a host clock *and* kernels, while
`localtime`/`localtimestamp` have no `tipb` variant whose name contains
`LocalTime` at all and are permanent exceptions next to `translate`. That is
`SESSION_CLOCK_NEEDS_HOST_CLOCK` versus `NO_WIRE_SIGNATURE` in the table, and
`admission::tests::clock_names_state_the_wire_and_host_clock_facts` pins both
halves rather than asserting them in prose.

`cast_signed` and `cast_unsigned` came out of that triage as the one safe
subset of the explicit-cast spellings: `CAST(x AS SIGNED|UNSIGNED)` is exactly
`Cast{source}AsInt`, so the local arm derives it with no metadata of its own.
Admitting them immediately exposed a hazard in a *different* place: the corpus
test `test_interval_func` had been passing only because the whole expression was
declined while `cast_unsigned` was excluded, and once it ran, the engine's
`IntervalInt` compared an UINT64 above `i64::MAX` as a raw `i64`.
`comparison()` now refuses unsigned ordering shapes rather than answering them
wrongly. The wider lesson: widening admission can surface a pre-existing engine
difference that the refusal was hiding, which is why every widening is measured
against the dual-run rather than assumed.

`cast_char` and `cast_binary` came out of the same triage for the same reason:
`CAST(x AS CHAR|BINARY)` is `Cast{source}AsString` with the charset in the
result type, and the local arm already derives that. Only a *fixed-width*
binary target still declines, because its padding is bounded by
`max_allowed_packet`. What is left of the minted spellings is the temporal group
(`cast_datetime`, `cast_date`, `cast_time`) plus `cast_json`/`cast_year`. The
temporal three then came in as well, once the missing piece turned out to be on
the bridge rather than in the arm: TiKV's `CastTimeAsTime` passes the source
value through, while TiDB renders a *declared* precision as part of the value,
so `bridge::check_time` now carries the declared FSP onto the result the way it
already rebuilt a declared DATE. That closed the last known divergence of the
minted-cast group; only `cast_json` and `cast_year` remain.

The lazy-arm rule has now been tested three times (7.2, 7.4, 7.10) and the
generalisation is narrower than "no coercion in an arm": a coercion is safe
exactly when it is the coercion Go applies at that position. `WrapWithCastAsString`
on a lazy value is (verified: `elt(1, 65)` agrees); a numeric-to-int cast on a
`case` condition or on an `elt` index is not (both measured divergences). Each
test costs one dual-run, so the practical rule is to try the narrowest
relaxation, run the corpus, and keep only what stays at zero divergences.

The lazy design (`components/tidb_query_expr/SHORT_CIRCUIT_DESIGN.md`) found
that materializing a lazy child at a subset boundary must produce an owned
`VectorValue`, because an `RpnStackNode` borrows one lifetime; that the
thread-local varg buffers must not be held across a nested evaluation; that a
lazy kernel must keep `borrowed_fn_ptr: None` or the borrowed facade panics;
and that `logical_rows()` has a latent panic at exactly `BATCH_MAX_SIZE`
generated rows, so the lazy path must use the indexed accessors. The first
implementation step registers no lazy kernel, so the existing suite proves the
refactor is behavior-preserving before any semantics change.

The type inventory (`tikv-expression-type-gaps.md`) finds that `Set` is the
only missing value type; `Geometry` and arrays have no native datum or builtin
and need only explicit refusal. TiKV already contains unreachable `Set`
scaffolding, and extending the existing `Int`/`Bytes` hybrid carriers makes
every string/int kernel accept `Set` without a new ordinary signature.


## Decision Log


- Decision: parity target is the current native surface, not all of Go TiDB.
  Rationale: removal cannot regress behavior that does not exist, and chasing
  the full Go surface would make the first removal unreachable.
- Decision: keep laziness out of the wire format. Rationale: `ExprType` lives
  in `tipb`; a signature-driven lazy path inside `tidb_query_expr` needs no
  schema change and no second fork.
- Decision: admission becomes an explicit allow-list keyed by signature and
  shape, and silent fallback becomes a test failure. Rationale: a deny-list
  is silently wrong the moment a new signature appears, which is exactly the
  failure mode removal must not have.
- Decision: engine host capabilities (clock, RNG, user variables, locks,
  sequences, statement state) are injected through a trait, not through
  globals. Rationale: the engine must stay usable from tests and from
  planning-time call sites with no session.
- Decision: an error is never retried natively once a kernel has run.
  Rationale: warnings, RNG draws and lock side effects are not replayable.
- Decision: engine wording does not have to match Go; error-versus-success
  classification does. Rationale: agreed scope with the requester.
- Decision: retain LOCATE/INSTR/POSITION until the embedded `utf8mb4_bin`
  collator-selection gap is isolated. Rationale: the TiKV kernel is conditional
  on its selected collator, so changing server-side search semantics based only
  on the embedded symptom would be speculative.
- Decision: retain strict-constant result-type/nullability provenance and Go's
  CAST propagation while deleting value kernels, but do not infer NOT NULL from
  string-function columns. Rationale: planner/view metadata is bridge structure;
  native constant folding must not be required to preserve a constant view,
  while Go intentionally leaves `UPPER(not_null_column)` nullable.


## Outcomes & Retrospective


The engine can now serve every expression the adapter admits, with the native
evaluator still the default and still the fallback, and with a corpus-wide
dual-run proving the two agree wherever the engine runs.

Current suites (TiDB numbers from `expression-reuse/round57-tests.log`):
`tidb-executor` 1340 + 355 + 6 + 2 with the engine feature and
1334 + 329 + 6 + 0 without; the added engine-on tests are the `VecGroupChecker`
and hash-splitter engine receipts. `tidb-expr` is 1212 + 60 with the feature (the three additions
are the shared-chunk result/error receipt, the retained-suite cache receipt, and
the shared-input ownership guard) and 1182 + 18 without. The most recently recorded TiKV suites remain
`tidb_query_expr` 477 passed and `tidb_query_datatype` 318. `catalog_diff` 31
and `query_diff` 1 pass; `expr_diff` keeps its two pre-existing `EXPORT_SET`
divergences, which are red with the feature disabled too.

The enrolled mysql replay was renewed on the current branch state
(`expression-reuse/replay-r52.log`, TiDB `1d9fd6f`): 142 of 10,251 compared
statements diverge -- the same count and the same divergence set (md5
`7b6445a8493f445641a9d07d787f0cba`, the id of the sorted `--- [topic]` header
list) as the engine-off baseline
(`expression-reuse/tidb-coverage-integration-native.log`, `backend=Native`, 0
engine rows) -- while the engine evaluated 217,539 expression rows over 1,831
statements, up from 12,480 over 1,552 in the previous receipt. That run links
the pinned fork rev `9fd4f94`, which is code-equal to the TiKV branch head:
`git diff --name-only 9fd4f94..HEAD` in the TiKV checkout lists exactly one
documentation file (`components/tidb_query_expr/EXPRESSION_SEMANTIC_GAPS.md`).
The widened admission surface therefore added no replay divergence.

What remains is not adapter plumbing but the removal itself: 43 production-path
textual sites of the 65 native `eval` hits outside projection (2 are the
unlinked `stream_agg.rs` duplicate, so 41 are reachable routing work), the
other 22 being test-only and re-pointed with the corpora (the check is
`rust/scripts/classify-native-eval-sites.py`; 11 documented sites no longer call
the native evaluator: 7 through the once-per-statement engine helpers, 2 row
loops moved onto retained `EvaluatorSuite::eval_chunk`, 2 removed as provably
error-only), the 33
corpora's conversion from dual-run to engine-only, and the deletion of the
feature gate and kernels. Known engine divergences are tracked rather than
hidden, and the dual-run makes any new one fail the suite.


## Context and Orientation


Repositories: `/home/agent/tidb/expression-reuse/tidb` (branch
`feat/tikv-expression-coverage`, committed base `30f3a2e`) and
`/home/agent/tidb/expression-reuse/tikv` (branch
`feat/standalone-expression-coverage`, `d2d9718`; its only delta from the
TiDB-pinned `9fd4f94` is documentation at this receipt).

The native evaluator lives in `rust/crates/tidb-expr/src/`:
`scalar_function.rs` (`ScalarFunction::eval`, `eval_by_signature`),
`ops.rs`, `compare`/`control` handling inside `scalar_function.rs`,
`time_fn/`, `string_fn.rs`, `builtin_ext/` (JSON), `arg_eval_type.rs` and the
`tests/` source-port corpora. The workspace currently has 353 raw `.eval(`
hits: 273 inside `tidb-expr/src` (the evaluator's own recursion) and 80
outside. The scripted outside inventory identifies 65 actual
`Expression::eval` sites (43 production, 22 test-only); the other 15 raw hits
are different `eval` APIs. Projection work reaches the engine through
`EvaluatorSuite`.

The engine adapter lives in `rust/crates/tidb-expr/src/tikv/` (`tikv.rs`
compilation and routing, `bridge.rs` value transport, `lowering.rs` and
`lowering/families.rs` signature selection) and in
`rust/crates/tidb-executor/src/stmt_context.rs` for the session context.

On the TiKV side the embedding seam is
`components/tidb_query_expr/src/standalone.rs` plus its child modules; the
evaluator is `components/tidb_query_expr/src/types/expr_eval.rs`; the
dispatcher is `components/tidb_query_expr/src/lib.rs`; function metadata types
are in `types/function.rs` and `types/expr_builder.rs`; the code generator is
`components/tidb_query_codegen/src/rpn_function.rs`.

Terms: a *signature* is a typed function identity (`ScalarFuncSig`), not a SQL
name. *Admission* is the decision to let a given expression tree run in the
engine. *Fallback* is a native evaluation performed instead of an engine one.
*Compiled program* is the immutable RPN form; *execution state* is the
per-call scratch the evaluator needs.


## Plan of Work


### Milestone A — the engine stops being per-execution and single-threaded

Make the compiled form `Send + Sync` and separate it from execution state.
On the TiKV side change the metadata signature from `Box<dyn Any + Send>` to
`Box<dyn Any + Send + Sync>` in `types/function.rs`, follow the compile errors
through the metadata constructors in the kernel files and through the 24
codegen references, and update `STANDALONE.md`. Then give the facade a
compiled form that can be evaluated through `&self`: either move the mutable
stack and output buffers into an explicit `ExecutionState` argument to
`eval_decoded`, or add a variant that allocates scratch per call. The stock
server path may keep its current shape; the standalone path is what must
become shareable.

On the TiDB side, cache the compiled engine program on the shared
`EvaluatorProgram` instead of in the per-suite `Mutex<ProjectionCache>`, so
parallel projection workers share one compilation with no lock on the hot
path. The cache key is the canonical expression hash plus the input column
schema. Report cache hits and misses in the coverage counters.

After this milestone a compiled program can be created once and evaluated from
several threads, and the feature-on suites still pass.

### Milestone B — one table decides admission, and silence fails

Replace the name-matching `admitted()`/`local_call()` logic with a table whose
rows are (signature, required argument eval types, shape constraints,
exclusion reason). Shape constraints cover the lazy rules and the
constant/leaf rules that exist today. Every SQL function name the Rust
rewriter can produce must appear either as an admitted row or as an excluded
row with a reason; generate that name list from the same registry the
rewriter uses, so a new function cannot appear without a decision.

Turn fallback into a gate. In test builds, when an expression compiles for the
engine but executes natively, record the reason and fail unless the reason is
an explicitly listed exclusion. The existing engine-row counters already
distinguish the cases; the work is to assert rather than to observe.

The inventory generator consumes the table so the CSV/JSON report shows
admitted, excluded and untested status per signature, not source-text
evidence.

### Milestone C — laziness

In TiKV, define the set of signatures that must see unevaluated children
(`if`, `ifnull`, `coalesce`, `case`, `and`, `or`, `xor`, `elt`, `field`,
`interval`, `greatest`, `least`, the `addtime*null` forms and `nulltimediff`).
Teach the evaluator to walk those with a lazy path: evaluate the selector
first, then evaluate only the children still required for the rows that still
need a value, and merge while preserving SQL three-valued logic. In the
row-oriented path this is an interpreter change; do not change `tipb`.

The TiDB side must not have to guess which signatures are lazy, because
compiling an eager kernel and a lazy kernel succeeds identically and an eager
one would silently produce the wrong semantics for a skipped branch. TiKV
therefore exposes a capability query (`is_lazy_signature(signature)`), and the
admission table's lazy shape rule consults it: a lazy shape is admitted only
when the engine reports that signature lazy. A test asserts the table's lazy
set and the engine's reported set agree, so adding or removing a lazy kernel
cannot drift silently. Until a signature is lazy, its non-leaf shapes stay
native exactly as today.

Provide the host capability trait at the same time, because laziness without
it still cannot express `IF(cond, getvar(...), 0)`:

    trait HostEval {
        fn current_time(&self, fsp: i8) -> Result<Time>;
        fn rand(&self) -> f64;
        fn user_var(&self, name: &str) -> Option<ScalarValue>;
        fn statement_value(&self, which: StatementValue) -> Result<ScalarValue>;
        fn advisory_lock(&self, op: LockOp) -> Result<i64>;
        fn max_allowed_packet(&self) -> usize;
        fn default_week_format(&self) -> i64;
    }

The standalone `Context` gains an optional implementation of this trait;
`None` means "capability absent" and the call returns a structured
`Unsupported` error rather than 0 or a panic.

On the TiDB side, the local adapter admits non-leaf lazy shapes and the engine
itself refuses a program that mixes lazy and eager lazy-sensitive nodes, so
the adapter never has to guess. It deliberately does NOT add the issue's
eager/lazy kill switch for the local path: laziness here is a correctness
property the adapter depends on (a skipped branch must not run), not an
optimization that can be turned off, and a program whose nodes are eager is
already refused. The issue's `tidb_enable_short_circuit_expression` variable
and DAG flag remain the right shape for *remote* pushdown rollout, which is a
different code path. TiDB's own vectorized short-circuit for `AND`/`OR`
(selection based) only matters while the native evaluator still exists.

### Milestone D — types the removal needs

Start from the native surface inventory produced in milestone B. The completed
inventory (`tikv-expression-type-gaps.md`) finds that **`Set` is the only value
type that must be added**; `Geometry` and arrays are not needed because the
native evaluator has no datum kind and no builtin for them. `Set` is an
input-only value: no builtin returns one and there is no wire `SetLiteral`, but
every implemented `ETString`/`ETInt` builtin accepts one, so all `Set`
expressions fall back today. TiKV already owns most of the scaffolding
(`EvalType::Set`, `ScalarValue::Set`, `VectorValue::Set`, `SetRef`, the
`*ForSet` aggregators, `cast_set_as_int`) but it is unreachable; the additions
are the `FieldTypeTp::Set` mapping, chunk/raw codecs whose name is the
comma-joined selected `elems`, the `ChunkedVecSet` element-name storage, the
`Int`/`Bytes` hybrid borrow arms, the standalone `Column::Set`, and registering
the existing cast. No new ordinary kernel signature is needed, because
extending the two hybrid carriers makes the existing string/int kernels accept
`Set` exactly as they accept `Enum` today. A latent `get_enum` var-length
indexing bug in the TiKV chunk codec is flagged while adding `get_set`.

### Milestone E — remove

The native evaluator's surface *outside* projections is inventoried by
`rust/scripts/classify-native-eval-sites.py` and recorded in
`tikv-expression-removal-native-sites.md`: 80 raw `.eval(` hits outside
`crates/tidb-expr/src`, 15 of them another API, leaving **65**
`Expression::eval` sites. **43 are production-labelled text** (26 row
loops/comparators, 11 one-chunk-row probes, 6 `Row::empty()` constants); two
row-loop calls are the unlinked `stream_agg.rs` duplicate, so 41 are reachable.
The other 22 are test-only.
None needs a new kernel: a row loop evaluates each item over its chunk, a probe
uses a one-row chunk, and a constant uses a virtual one-row chunk.

Eleven documented sites no longer call the native evaluator: 3 DDL constant
rows, 4 pruning rows (one retains its explicit sparse-input native fallback), 2
error-only sort branches, and 2 retained-suite grouping loops
(`VecGroupChecker` and the hash-shuffle splitter). Each retains one
`EvaluatorSuite` per grouping item and calls `EvaluatorSuite::eval_chunk` per
chunk; its immutable `EvaluatorProgram` holds
the compiled engine cache across chunks. The free `evaluator::eval_chunk` is the
one-off shared-chunk seam, while a retaining operator must use the suite method.
Each conversion has a native-vs-engine result receipt and an engine-row counter
receipt; the retained suite has a separate two-chunk/one-compilation receipt.


Flip the default to the engine, delete the `tikv-expr` feature, delete the
fallback branches, then delete the native kernels and their source-port tests
whose subject is now the engine, keeping the Go-oracle corpora that still
validate results. Before deleting, run the full Rust suites, the enrolled
mysql replay, and the Go expression suites as the external oracle. Record the
remaining exclusions as explicit errors, not as silent native execution.


## Concrete Steps


One heavy command runs at a time across both repositories, through
`/home/agent/tidb/expression-reuse/tools/limited-run.py`, with
`CARGO_BUILD_JOBS=1`, `RUST_TEST_THREADS=1` and single-threaded Go. Use
`RUST_MIN_STACK=2097152 MALLOC_ARENA_MAX=2` whenever a suite spawns worker
pools, otherwise the address-space cap turns thread creation into `EAGAIN`.

TiKV:

    cd /home/agent/tidb/expression-reuse/tikv
    python3 /home/agent/tidb/expression-reuse/tools/limited-run.py -- \
      cargo test -p tidb_query_expr --lib -j1 -- --test-threads=1

TiDB Rust (engine on), with the local engine checkout patched in during
development:

    cd /home/agent/tidb/expression-reuse/tidb/rust
    python3 .../limited-run.py -- cargo test -p tidb-expr --features tikv-expr --lib --test all -j1 \
      --offline --config 'patch."https://github.com/YangKeao/tikv.git".tidb_query_expr.path="/home/agent/tidb/expression-reuse/tikv/components/tidb_query_expr"' \
      -- --test-threads=1

Expected: the feature-on counts are 1200 + 39 for `tidb-expr`, 353 for
`tidb-executor`, 1726 + 338 for `tidb-session`; feature-off is 1180 + 18,
329 and 1726 + 337. Any drop means a regression in the coexistence period.

The replay is the semantic gate:

    INTEGRATION_TIKV_BACKEND=copying python3 .../limited-run.py -- \
      cargo test -p difftest-result-tests --features tikv-expr --test integration_diff -j1 -- --test-threads=1

Its current state is 142 divergences out of 10,251 compared statements with a
divergence set identical to the native baseline (`md5
7b6445a8493f445641a9d07d787f0cba`) and therefore pre-existing; `expr_diff` (2
cases), `table_diff` (7 of 1,942) and `join_shape` (stale ratchet) are red with
the feature disabled too. Milestone E requires those to be resolved or
explicitly ratcheted with a reason.


## Validation and Acceptance


Milestone A: a token `PreparedExpression` (or its replacement) is `Send + Sync`
proven by a compile-time assertion, two threads evaluate one compiled program
concurrently producing identical output to a single-threaded run, and the
feature-on suites keep their counts.

Milestone B: every function name in the rewriter registry has an admission
row; a deliberately admitted-but-not-executing expression fails the test; the
inventory reports per-signature status.

Milestone C: `IF(0, <overflow>, 7)` executes in the engine and returns 7 with
no error and no warning; `IF(1, 1, <overflow>)` likewise; an unused branch
containing `GETVAR` does not run; TiDB and TiKV agree on `AND`/`OR` with NULL
operands under selection; the issue's switch exists in TiDB and is propagated.

Milestone D: each added type round-trips through the adapter and matches the
native evaluator on the same fixture; a type whose encoding cannot be matched
is refused with an explicit error.

Milestone E: `rg 'tikv-expr' rust/` returns no feature gate, the native
evaluator modules are deleted, and the full suites plus the replay pass. The six
names the pinned `tipb` cannot address (`translate`, `weight_string`,
`load_file`, `json_schema_valid`, `localtime`, `localtimestamp`) return the
classified "no engine" error instead of running natively; the checklist's
permanent-exception table is the list, and nothing else may join it without
evidence.


## Idempotence and Recovery


Every milestone is additive until E. The native path stays default until the
last step, so a failed experiment can be reverted by dropping a commit rather
than by repairing a broken evaluator. Cache and admission-table changes are
pure functions of the expression tree and input schema, so re-running is safe.
Deleting native code is the only irreversible step and is gated on the
milestones before it.


## Artifacts and Notes


Probe-child failure boundary (TiDB `rust/`, same guarded env):

    cargo test -q -p tidb-executor --features tikv-expr --lib probe_error_does_not_evaluate_partial_chunk_filters --locked --offline -j1 -- --test-threads=1
    cargo test -q -p tidb-executor --features tikv-expr --locked --offline -j1 -- --test-threads=1

The fixture's child writes a row then returns a sentinel Internal error.
`probe-error-red.log` records a test-code compilation mistake (ExecError has
Debug, not Display). After fixing that assertion, `probe-error-runtime-red.log`
proves the actual bug: the successful filter reports one engine row AFTER the
child failed, instead of zero. fill_probe_chunk used to defer propagation until
after filters; a failing filter could also replace the original failure.

The error is now returned before filter compilation/execution. Partial rows are
reset and the typed allocated chunk is restored to its owner, with selection
state cleared. No automatic retry or native replay is introduced; the test
closes the executor after failure and does not establish retry-after-error
semantics. `probe-error-green.log` covers native/engine contexts and both a
successful constant filter and an unsupported poison filter: original sentinel
error, zero output, zero engine filter rows/compilations, empty one-column chunk.
Exact sentinel matching identifies the source failure in this local fixture,
not a requirement to match Go's diagnostic wording.

`probe-error-executor.log`: 1402/355/6/2 passed, 184 integration ignored. Serial,
single-worker 8192 RSS / 16384 AS MiB guard, sampled peak 2847.7 MiB. From repository
root, lint passed in `probe-error-lint.log` (104.7 MiB sampled peak):

    GOMAXPROCS=1 GOFLAGS='-p=1' GOPATH=/home/agent/tidb/expression-reuse/go GOCACHE=/home/agent/tidb/expression-reuse/go-cache python3 ../tools/limited-run.py --rss-mib 8192 --as-mib 16384 -- make -j1 lint

This is an embedder execution-boundary fix, not a TiKV kernel divergence or a
new SQL admission. No Go oracle, mysql replay, new feature-off suite, performance,
retry-after-error, hosted CI, Rust clippy or whole-migration readiness claim.

Merge JoinExec outer-filter program retention (TiDB `rust/`, same guarded env):

    cargo test -q -p tidb-executor --features tikv-expr --lib join_outer_filter_programs --locked --offline -j1 -- --test-threads=1
    cargo test -q -p tidb-executor --features tikv-expr --lib join::tests:: --locked --offline -j1 -- --test-threads=1
    cargo test -q -p tidb-executor --features tikv-expr --locked --offline -j1 -- --test-threads=1

`merge-filter-cache-red.log`: extending the existing lifecycle fixture to Merge
fails with retained compilation count 0 instead of 1 before changing the row
loop. fetch_outer_group now takes the saved ConditionEvaluator rather than
reconstructing programs via eval_bool. Row order, CNF NULL policy, truthiness,
selection and execution state remain unchanged. The owner is refreshed at open,
just like the hash path. This is cached scalar filtering, NOT a new vectorized
short-circuit implementation or borrowed-lazy support.

`merge-filter-cache-green.log` records an intermediate test compilation failure:
the new fixture initially named nonexistent LeftOuter/RightOuter variants. After
using the actual Left/Right variants, `merge-filter-cache-join.log` passes all
45 join tests. The shared lifecycle fixture covers 2051 Merge rows across three
chunks, then reopen with a false head and unsupported tail: 4102 engine filter
rows, one compilation per open, no stale true snapshot or eager tail compilation.
A separate NULL-head fixture checks both left/right outer joins in native/engine
modes: all outer rows survive with NULL padding, only the first term compiles,
and the engine counts three rows per side. These remain direct-executor seed
fixtures; planner population, package-transcreation completeness and performance
are not established.

`merge-filter-cache-executor.log`: 1401/355/6/2 passed, 184 integration ignored.
Rust runs are serial/single-worker under the 8192 RSS / 16384 AS MiB guard;
sampled peak 2852.9 MiB. No Go oracle, mysql replay, new feature-off suite, hosted
CI or native deletion claim. Repository lint passed (exit 0, sampled peak
267.8 MiB) in `merge-filter-cache-lint.log`, using from repository root:

    GOMAXPROCS=1 GOFLAGS='-p=1' GOPATH=/home/agent/tidb/expression-reuse/go GOCACHE=/home/agent/tidb/expression-reuse/go-cache python3 ../tools/limited-run.py --rss-mib 8192 --as-mib 16384 -- make -j1 lint

This runs the repository's Go revive/dashboard checks, not Rust clippy or SQL
semantic parity. It does not establish whole-migration/PR readiness.

Hash JoinExec outer-filter program retention (TiDB `rust/`, same guarded env):

    cargo test -q -p tidb-executor --features tikv-expr --lib chunk_probe_paths_share_one_residual_compilation --locked --offline -j1 -- --test-threads=1
    cargo test -q -p tidb-executor --features tikv-expr --lib hash_outer_filter_programs --locked --offline -j1 -- --test-threads=1
    cargo test -q -p tidb-executor --features tikv-expr --locked --offline -j1 -- --test-threads=1
    cargo test -q -p tidb-executor --lib join::tests:: --locked --offline -j1 -- --test-threads=1

`outer-filter-cache-red.log`: adding the owner plus a compile-count assertion
without routing execution through it yields zero retained compilations rather
than one; the old row loop still uses the temporary eval_bool wrapper. Hash
build/probe filters now use an owned ConditionEvaluator refreshed by open.
No truthiness kernel, filter order or runtime-error fallback is changed. This
retains immutable programs, not execution scratch or rows. The private filter
snapshot must remain stable during execution; close/reopen picks up changes.

`outer-filter-cache-green.log` passes the new lifecycle test: native/engine,
left build versus left probe, 2051 rows over three chunks, then reopen with a
false filter plus an unsupported tail. The output changes from all rows to none,
compilation resets to zero on open then reaches exactly one, and the unreachable
tail stays uncompiled. Total 8204 engine filter evaluations across the two sides
and two opens. The existing serial/parallel residual fixture also now checks its
separate outer-filter compilation count. These direct-executor seed tests set the
private outer_filter; production planner population and performance are not
claimed. Merge's temporary outer-filter wrapper remains a separate follow-up.

`outer-filter-cache-executor.log`: 1400/355/6/2 passed (184 integration ignored).
`outer-filter-cache-native.log`: 39 feature-off join tests passed, 1307 filtered.
All heavy runs serial, single worker; sampled peak 2759.1 MiB with 8192 RSS /
16384 AS MiB limits. No full feature-off suite, Go oracle, mysql replay, performance,
lint or PR-readiness claim. The native evaluator/default has not been removed.

Runtime execution receipt gate (TiDB repository root, existing guarded Rust env):

    python3 rust/scripts/tikv_expression_runtime_gate.py --self-test
    python3 rust/scripts/tikv_expression_runtime_gate.py --update
    python3 rust/scripts/tikv_expression_runtime_gate.py
    python3 rust/scripts/tikv_expression_coverage.py --self-check --check

The runtime driver uses the fixed command (cwd `rust/`):

    cargo test -q -p tidb-expr --features tikv-expr --test all tikv_coverage:: --locked --offline -j1 -- --nocapture --test-threads=1

Run the driver under ../tools/limited-run.py --rss-mib 8192 --as-mib 16384; all
runs serial. Update is explicit, never automatic during normal comparison and
never allowed after Cargo failure. The driver also rejects no/empty/ignored
suite, malformed receipts, native fallback, impossible row accounting, missing
receipts and any count/identity change (including duplicate multiplicity). It
compares the exact sorted records, not just totals; equal totals cannot conceal
a changed label/signature/row shape. Rust's check helper now requires the engine
for both enabled contexts and emits V1 receipts only after row/fallback checks
and native value/warning parity. Legacy PASS lines remain for existing readers.

`runtime-gate-bootstrap.log`: 30 tests passed, then deliberate missing-baseline
refusal (47 other integration tests filtered out). `runtime-gate-update.log`
creates the reviewed baseline; `runtime-gate-check.log` independently repeats
it. `runtime-gate-parser-red.log` reproduces acceptance of malformed `1,,2` IDs;
strict syntax plus self-tests now reject it. `runtime-gate-final.log` passes
30 tests and the exact baseline: 368 receipts, 2394 engine rows, zero native
fallbacks in those fixtures, 160 observed borrowed rows. A real-baseline mutation
2394 -> 2393 is rejected in `runtime-gate-drift-red.log`.

Important qualification: Borrowed is a requested mode, not proof of borrowed
execution. These receipts observe only 160 borrowed rows; the other 2234 engine
rows used copying, including copying within requested Borrowed mode. Do not
report the whole suite as zero-copy or SIMD evidence. The native reference is
intentionally run separately, and tests/rows outside check() produce no receipt;
this is not coverage of all functions, corpus inputs, or unsupported shapes.
Sampled compile-run peak 1310.3 MiB; very short cached reruns are below the guard's
sampling resolution. No production kernel/SQL admission changes, executor/full
library rerun, Go oracle, mysql replay, performance, hosted CI, lint or PR-readiness
claim. Baseline JSON is generated only from successful real runs.

Static capability inventory repair (TiDB repository root):

    python3 rust/scripts/tikv_expression_coverage.py --self-check
    python3 rust/scripts/tikv_expression_coverage.py --self-check --check

Both commands run serially under ../tools/limited-run.py with the existing 8192
RSS / 16384 AS MiB limits. No Rust build or SQL admission change in this step.
`inventory-scan-red.log` exposed the first problem before the new regression even
ran: symbolic exclusion-reason constants were omitted by the row regex, falsely
reporting many registered builtins missing. Constants (including Rust continued
strings) now resolve from source; unknown reasons and partially unparsed tables
fail closed. Parsing is bounded to ADMISSION_ROWS, not later test fixtures.

`inventory-truncation-red.log` separately reproduces zero generated candidates
for a production function after a cfg(test) module. Test-only items are now
masked with balanced delimiters instead of truncating the file. Both direct
signature references and generated-name candidates exclude test items. Fixtures
cover interleaved test modules/functions, later production code, quoted braces,
array-parameter semicolons and external test module declarations. This remains a
lexical scanner for the adapter's exact test attributes, not a general Rust cfg
or overload interpreter.

`inventory-head-red.log` reproduces a second independent gate defect: exact JSON
comparison invalidates a checked-in report whenever its commit advances HEAD.
Only informational current_revisions are now ignored in JSON checks; source
hashes, counts, baseline revisions and all result fields remain compared, and
CSV remains byte-checked. Regression fixtures reject changed source hashes and
counts. Lazy risk wording now requests runtime strategy verification rather than
incorrectly declaring every conditional eager.

`inventory-parser-check.log` shows parser self-checks passing followed by expected
stale-artifact refusal. `inventory-regenerate.log` / `inventory-check-green.log`
record regeneration and successful comparison. Source inventory: 384 admission
rows, 240 declared admitted and 144 excluded, covering 309 registry + 75
synthesized names with zero missing names. 510 engine-dispatched signatures and
lexical/generated candidates are NOT SQL shape or tested execution counts. JSON
and CSV were regenerated by the script, not hand-edited. Post-commit check log:
`inventory-postcommit.log`. No actual hosted CI run, runtime count-gate wiring,
Rust/Go oracle, mysql replay, performance, make lint or PR-readiness claim.

Bounded ordinary binary-text integer CAST (TiDB `rust/`, same serial guard):

    cargo test -q -p tidb-expr --features tikv-expr --test all bounded_binary_text --locked --offline -j1 -- --test-threads=1
    cargo test -q -p tidb-expr --features tikv-expr --locked --offline -j1 -- --test-threads=1
    cargo test -q -p tidb-executor --features tikv-expr --locked --offline -j1 -- --test-threads=1

`bounded-text-red.log` reproduces required-engine NotAdmitted. The source-level
integer-cast classifier now distinguishes Literal/Text/Declined after shared
metadata, arity, target signedness and deferred/parameter checks. Only explicit
cast_signed/cast_unsigned over ordinary String/Bytes, canonical binary VarString,
and 1..6 ASCII digits may enter the text kernel directly. No parsing or numeric
constant folding occurs in TiDB. Literal operands retain MysqlBit numeric encoding.
The generic node/catalog coercion guard is unchanged: wire equality still cannot
authorize a synthesized sibling conversion. Generic cast, float/real conversion,
signs, spaces, fractions, exponent, suffix/NUL/non-UTF8, longer digit sequences,
metadata/name mismatch, deferred and parameter cases remain declined.

Five value shapes (0, 1, 000001, 123456, 999999) x both source datum kinds x both
integer signednesses: 120 selected engine rows across Copying/Borrowed, exact
native Int/UInt parity, empty/repeated selection, no fallback/warnings, one cached
compilation per retained program. Direct engine profile checks add flags 0/482 x
sql_mode 0/u64::MAX (160 rows), with identical values and no warnings. A nested
explicit text CAST succeeds, while previous implicit-provenance tests remain
closed. The old binary-collation test now asserts text 1, never literal 49.
These are bounded Rust-native comparisons, not a Go oracle or diagnostic-profile
compatibility claim beyond the admitted warning-free subset.

`bounded-text-expr.log`: 1224/77 passed (99 unit ignored).
`bounded-text-executor.log`: 1399/355/6/2 (184 integration ignored). All heavy
commands serial and single-worker; sampled peak 2323.1 MiB, limits 8192 RSS /
16384 AS MiB. No feature-off/TiKV code rerun, mysql replay, performance, lint or
PR-readiness claim. Native default/fallback remains outside verified shapes.

TiDB adoption of text-constant compilation (TiDB `rust/`, same serial guard):

    cargo test -q -p tidb-expr --features tikv-expr --test all binary_literal_integer_casts_use_engine --locked -j1 -- --test-threads=1
    cargo test -q -p tidb-expr --features tikv-expr --locked --offline -j1 -- --test-threads=1
    cargo test -q -p tidb-executor --features tikv-expr --locked --offline -j1 -- --test-threads=1

The first command fetches the pinned personal-fork revision db9c7f0. Manifest and
lock source identities migrate from c93c2bb; a diff assertion confirms all 46
changed lock lines are only those revisions. Prior dependency bindings/versions
are retained, and --locked accepts the graph. The adapter now always selects
compile_with_text_constants; its cache never mixes compilation policies.

`text-adoption-red.log` proves simply changing the compile call is unsound:
existing BinaryLiteral integer CAST now leaks a truncation warning (empty literal
misread as text). Source-authenticated direct literal CAST now carries unchanged
raw bytes in existing MysqlBit wire nodes with unsigned metadata; CastIntAsInt
consumes the TiKV-decoded ordinal. No numeric parsing/arithmetic or constant
folding is added to TiDB. A unit test pins raw payload identity, wire kind, width,
unsigned metadata and signature for empty, ASCII, padded and full-u64 literals.
The signed-boundary, deferred/parameter, root/lazy carrier and ordinary-text
coercion guards remain. No unsafe shape is newly admitted in this adoption.

`text-adoption-expr.log`: 1224/75 passed (99 unit ignored), including the existing
84-row signed/unsigned literal matrix, exact Datum kinds, no warnings, both
transports, repeated/empty selections and one retained compilation.
`text-adoption-executor.log`: 1399/355/6/2 (184 integration ignored). Sampled peak
3626.3 MiB, one worker, RSS/AS limits 8192/16384 MiB. No feature-off/Go oracle/full
mysql replay/performance/lint rerun or PR-readiness claim. Next work is native
parity for ordinary binary string numeric conversion, not blanket guard removal.

Engine text-constant compile policy (TiKV root, same serial guarded environment):

    cargo test -q -p tidb_query_expr --lib constant_ --locked --offline -j1 -- --test-threads=1
    cargo test -q -p tidb_query_expr --lib --locked --offline -j1 -- --test-threads=1
    cargo fmt -p tidb_query_expr -- --check

`engine-text-policy-red.log` reproduces two failures: an API stub delegating to
legacy compile still returns 49 instead of text 1, and MysqlBit used as CAST's
argument errors as unsupported scalar despite already having a literal decoder.
TiKV `db9c7f0` adds an opt-in compile_with_text_constants entrypoint. An internal
context-aware builder mapper selects existing CastStringAsInt/Real textual
kernels for ordinary String/Bytes constant nodes; no kernel body, global setting,
Context field, or wire layout changes. Default compile/coprocessor string dispatch
stays legacy. MysqlBit joins scalar classification and can retain full-u64 bits
through integer CAST in both modes. Compiled programs remain Send+Sync; caches
mixing the entrypoints must include policy in their keys.

First `engine-text-policy-green.log` failed on the test's unintended DOUBLE(0,0)
metadata (49 out of range), not policy dispatch. The fixture now uses unspecified
flen/decimal (-1). Final `engine-text-policy-lib.log`: ALL 488 expression-library
unit tests pass, including both String/Bytes wire kinds, integer/real 1 versus
legacy 49, nested sqrt 1 versus legacy 7, interleaved programs, repeated selection,
strict truncation error, warning reset/count with storage disabled, and skipping
an error-producing text CAST in an unselected lazy IF arm. Sampled peak across
runs 1266.6 MiB; final full run 1078.8 MiB, under 8192 RSS / 16384 AS MiB limits.
Formatter later required only reflow of the new cache-policy doc comment; rerun
fmt/check succeeded. STANDALONE.md and the coprocessor maintenance embedding
section now document the policy and correct stale eager-only/unsupported-Set text.

TiDB code/pin are deliberately unchanged this round: no claim of native parity,
borrowed-policy validation, full binary-literal carrier semantics, or removed
fallback. Next adoption must preserve source literal provenance (e.g. numeric
MysqlBit encoding for the verified numeric CAST subset), test diagnostics/profile
boundaries and then relax the adapter guards. No full TiKV workspace suite,
TiDB rerun, Go oracle, mysql replay, performance, make lint/dev or PR readiness.

Canonical BIT roots and implicit-coercion provenance (TiDB `rust/`, same guard):

    cargo test -q -p tidb-expr --features tikv-expr --test all canonical_bit_roots --locked --offline -j1 -- --test-threads=1
    cargo test -q -p tidb-expr --features tikv-expr --test all bit_ --locked --offline -j1 -- --test-threads=1
    cargo test -q -p tidb-expr --features tikv-expr --test all implicit_binary_string_numeric --locked --offline -j1 -- --test-threads=1
    cargo test -q -p tidb-expr --features tikv-expr --locked --offline -j1 -- --test-threads=1
    cargo test -q -p tidb-executor --features tikv-expr --locked --offline -j1 -- --test-threads=1

`bit-root-red.log` reproduces required-engine NotAdmitted. `bit-root-green.log`
passes 2 tests after a root-position-aware shape gate: BIT type, width 1..64,
exactly ceil(width/8) bytes, no unused high bits, no deferred/parameter marker.
Existing MysqlBit decoding plus the existing bridge preserves the native Bit
kind and padding, so no codec/kernel/protocol changes are needed. Eighteen
value/flag shapes cover widths 1/8/9/16/25/64, zero padding, top bit and full u64;
108 selected engine rows across both transports, exact native-kind parity,
empty/repeated selections, one compilation per retained program, projection and
NULL controls. Noncanonical/mismatched roots and nested CAST/math/HEX/COALESCE
literal consumers remain declined; root support is not general BIT coercion.

An independent audit found the previous direct-CAST provenance guard insufficient.
`binary-implicit-red.log` reproduces ordinary binary bytes b"1": sqrt native 1 /
engine 7, plus 1 / 49, leftshift 2 / 562949953421312, LEFT one character / four.
Generic Real `cast` also showed native Unsupported versus engine success in this
hand-built shape (no Go-runtime correctness claim). ROUND already declined in
this test. `binary-implicit-green.log` passes after centralized
CastStringAsInt/Real + direct String/Bytes-constant + binary-collation guarding.
The shared node builder covers synthesized local/family casts; catalog
substitution checks newly created nodes after children are substituted, preventing
fallback from bypassing the rule. Validated child subtrees are not scanned again
or authorized by protobuf equality: ordinary String and BinaryLiteral can have
identical wire leaves. Only the checked source-level direct integer-literal cast
uses the raw node constructor. Tests keep that nested subset working, reject
ordinary String/Bytes siblings, and retain ordinary text catalog cast_double.
A unit test covers both wire kinds and both signs of the binary collation ID;
the guard deliberately does not depend on mutable global collation mode.

This is safe compile refusal, not repaired engine provenance or runtime error
replay. An engine-owned literal-provenance contract is still needed to regain
these numeric string shapes; no consumer-name blacklist or wire change was added.

Final logs: `bit-provenance-expr-final.log`: 1223/75 passed (99 unit ignored);
`bit-provenance-executor-final.log`: 1399/355/6/2 (184 integration ignored).
Earlier full logs: `bit-provenance-expr.log` / `bit-provenance-executor.log`.
All heavy commands serial, one worker; sampled peak 2498.1 MiB with 8192 RSS /
16384 AS MiB limits. No feature-off/TiKV-code rerun, performance, Go oracle, full
mysql replay, make lint or PR-readiness claim. Native fallback/default remains.

Bounded BinaryLiteral integer CAST (TiDB `rust/`, same guarded environment):

    cargo test -q -p tidb-expr --features tikv-expr --test all binary_ --locked --offline -j1 -- --test-threads=1
    cargo test -q -p tidb-expr --features tikv-expr --locked --offline -j1 -- --test-threads=1
    cargo test -q -p tidb-executor --features tikv-expr --locked --offline -j1 -- --test-threads=1

`binary-cast-red.log` reproduces two distinct failures: safe BinaryLiteral CAST
is NotAdmitted in required mode; ordinary binary-collated bytes b"1" cast to
signed yields native Int(1) versus engine Int(49). TiKV's existing CastStringAsInt
selector treats any binary-collated scalar constant as a binary number; collation
alone does not prove literal provenance. A local shape rule, after the normal
admission-table gate, admits only direct cast_signed/cast_unsigned with matching
LongLong signedness, canonical binary VarString source, no deferred/parameter
input, payload <=8 bytes, and signed value <=i64::MAX. Non-null ordinary binary
string constants in integer CAST are now declined rather than misinterpreted;
NULL/column/text-collation handling is otherwise unchanged. No lowering, kernel,
wire or engine-pin change is required.

`binary-cast-green.log` and final `binary-cast-final.log`: 3 focused tests pass.
14 signed/unsigned value shapes (empty, ASCII-as-literal 49, leading zeros,
max signed, high-bit/full-u64 unsigned) produce 84 engine rows across copying/
borrowed modes and repeated selections; native and required-engine numeric Datum
kinds agree, empty selections stay empty, warnings remain empty, and each retained
program compiles once. Negative cases include root/lazy literal forwarding,
generic/real/decimal casts, metadata mismatch, BINARY flag without binary
collation, >8-byte payload, deferred and parameter constants. Final targeted test
also bypasses only adapter admission to reproduce full-u64 signed native
saturation to i64::MAX versus raw TiKV -1; this is a guarded discrepancy, not a
Go-correctness or native-bug-fix claim. Other implicit binary-string coercions
are not covered, and root/lazy literal-kind transport remains missing.

Full `binary-cast-expr.log`: 1222/72 passed (99 unit ignored);
`binary-cast-executor.log`: 1399/355/6/2 (184 integration ignored). The final
raw-engine assertion was added afterward and passed in the 3-test targeted run.
Serial one-worker guard: sampled peak 2432.5 MiB, RSS/AS limits 8192/16384 MiB.
No feature-off/TiKV rerun, performance, new Go oracle, full mysql replay, make lint
or PR-readiness claim; native fallback remains.

Fixed-offset TIMESTAMP literal packing (same serial guarded environment):

TiKV root:

    cargo test -q -p tidb_query_expr --lib temporal_literal_packing --locked --offline -j1 -- --test-threads=1
    cargo test -q -p tidb_query_expr --lib standalone:: --locked --offline -j1 -- --test-threads=1

TiDB `rust/`:

    cargo test -q -p tidb-expr --features tikv-expr --test all timestamp_literals_ --locked --offline -j1 -- --test-threads=1
    cargo test -q -p tidb-expr --features tikv-expr --locked --offline -j1 -- --test-threads=1
    cargo test -q -p tidb-executor --features tikv-expr --locked --offline -j1 -- --test-threads=1

`timestamp-fixed-red.log`: positive offset still records 0 instead of 1 engine
row. New TiKV Context::pack_time_literal reuses Time::to_packed_u64 and decodes
back under the same context, checking total warning count and exact chunk bytes.
It preserves kind/FSP and raw micros; no parsing, manual offset arithmetic,
normalization in the output bridge, or wire change. TiDB local lowering supplies
Context and uses this only for TIMESTAMP. Named zones other than exact UTC remain
excluded; offset validation/name priority remain in TiKV's existing config.

`timestamp-fixed-green.log` passes the expanded matrix. Final full runs include
168 engine rows across TIMESTAMP(0/3/6), NULL, YEAR, a year-boundary fixture,
UTC/+8/-12/+14, empty name, name priority, invalid/oversized offsets, retained
program profile switches and copying/borrowed modes. Optional native results and
required structured refusal are tested for declined profiles. The existing zero
matrix checks decoder-warning isolation for TIMESTAMP too.

New paired reproduction: London 2021-10-31 01:30 resolves to 01:30 UTC in native
Rust's Go-style resolver but 00:30 UTC in TiKV's earliest-fold resolver. A round
trip alone preserves wall fields despite that one-hour instant difference.
`timestamp_london_fold_remains_declined_due_to_instant_mismatch` verifies both
packed instants and the named-zone admission refusal. No new Go-runtime oracle
was run; neither named-zone nor DST semantics are declared unified.

TiKV `c93c2bb` adds only the facade API/test relative to the old pinned engine
(code diff audited); 44 standalone tests pass (`timestamp-engine-standalone.log`),
with exact packed-byte assertions in `timestamp-engine-pack.log`. TiDB Cargo.toml
and lock pin that personal-fork commit. Initial `cargo update -p tidb_query_expr
--precise c93c2bbf6c04f4b86710b7fc8df921da166a0540` introduced unrelated compatible
socket/platform dependency-edge churn. Exact-version/recursive resolver attempts
did not fully preserve the graph; the original lock graph was retained with only
engine source identities migrated. A diff assertion verifies the final 46 changed
lock lines are only old/new engine revisions; final --locked --offline tests
accept it. No dependency package versions or unrelated bindings changed.

Final `timestamp-fixed-expr-final.log`: 1222/69 passed (99 unit ignored);
`timestamp-fixed-executor-final.log`: 1399/355/6/2 (184 integration ignored).
Initial full logs are `timestamp-fixed-expr.log` / `timestamp-fixed-executor.log`.
Guarded peaks: 3344.1 MiB initial executor, 3306.9 MiB final executor; guard
8192 RSS / 16384 AS MiB, one worker. Cached standalone 0.6 MiB is an undersample,
not a runtime bound. No feature-off rerun, full TiKV suite, performance, mysql
replay, make lint or PR-readiness claim. Native fallback remains.

UTC TIMESTAMP literal admission (TiDB `rust/`, same guarded environment):

    cargo test -q -p tidb-expr --features tikv-expr --test all timestamp_literals_require_utc --locked --offline -j1 -- --test-threads=1
    cargo test -q -p tidb-expr --features tikv-expr --locked --offline -j1 -- --test-threads=1
    cargo test -q -p tidb-executor --features tikv-expr --locked --offline -j1 -- --test-threads=1

`timestamp-utc-red.log` reproduces the blanket refusal (0 versus 1 engine row).
Admission now receives the compile context recursively: TIMESTAMP payload fields
can be preserved by the existing context-free encoder only when UTC decode is an
identity. Exact name `UTC` has priority over offset, matching Context::config;
otherwise only absent/empty name with offset zero is permitted. Non-UTC names,
offsets and unverified aliases remain declined, not normalized or guessed.

`timestamp-utc-green.log` passes the initial matrix; the full suite includes the
expanded copying/borrowed matrix. The final targeted `timestamp-utc-final.log`
also pins TIMESTAMP(3) retaining sub-precision raw micros. Final cases:
TIMESTAMP(0/3/6), typed NULL and nested YEAR,
UTC / named UTC with conflicting offset / non-UTC / invalid name / UTC recovery,
optional native fallback versus required 1105 refusal. Same retained programs
are exercised across context changes; 60 engine rows prove admission. The zero
SQL-mode matrix now also covers TIMESTAMP(6), and the wire test pins Timestamp
metadata/bytes. Mismatched kind and invalid-calendar TIMESTAMP still decline.

Full logs `timestamp-expr-on.log` and `timestamp-executor-on.log`: expression
1222/68 passed (99 unit ignored), executor 1399/355/6/2 (184 integration ignored).
Serial single-worker guarded peak sample 2389.6 MiB; RSS/AS limits 8192/16384 MiB.
No TiKV code/wire changes. Feature-off/TiKV suites, performance, new Go oracle,
full mysql replay and make lint were not rerun. Non-UTC/DST transport, restricted
zero modes and native deletion remain incomplete.

Context-dependent zero temporal admission (TiDB `rust/`, same guarded environment):

    cargo test -q -p tidb-expr --features tikv-expr --test all zero_temporal_constants --locked --offline -j1 -- --test-threads=1
    cargo test -q -p tidb-expr --features tikv-expr --locked --offline -j1 -- --test-threads=1
    cargo test -q -p tidb-executor --features tikv-expr --locked --offline -j1 -- --test-threads=1

`zero-temporal-red.log` records zero engine rows instead of one in permissive
mode. Removing the unconditional zero gate allows the existing standalone compile
policy to decide: warning-free decoding executes; warnings/errors produce a
compile decline before host-visible effects. This is not execution-error replay.
The DATE/DATETIME(0/6) matrix uses one retained program per type across mode
changes and recovery to permissive mode. NO_ZERO_DATE, strict mode and
IGNORE_TRUNCATE variants test warning/error rejection, optional native output,
required 1105 errors, and no warning leakage; warning capacity zero still rejects
based on total warning count. Existing admission instrumentation records declines
even in required mode, not only actual native fallbacks; the initial
`zero-temporal-green.log` assertion assumed otherwise and was corrected.

`zero-temporal-fixed.log` passes the focused matrix. Full expression:
1222/67 passed (99 unit ignored); executor: 1399/355/6/2 (184 integration
ignored), in `zero-temporal-expr.log` and `zero-temporal-executor.log`.
Serial single-worker guarded peak sample 2529.9 MiB (8192 RSS / 16384 AS MiB
limits). Only feature-on adapter/test code changed; feature-off and TiKV suites
were not rerun. No performance, new Go oracle, full mysql replay or make lint.
Restricted zero-date profiles still decline and TIMESTAMP remains excluded;
there is no engine-only or native-removal claim.

DATE/DATETIME literal admission validation (TiDB `rust/`, same guarded environment):

    cargo test -q -p tidb-expr --features tikv-expr --test all temporal_constant_ --locked --offline -j1 -- --test-threads=1
    cargo test -q -p tidb-expr --features tikv-expr --locked --offline -j1 temporal_ -- --test-threads=1
    cargo test -q -p tidb-expr --features tikv-expr --locked --offline -j1 -- --test-threads=1
    cargo test -q -p tidb-executor --features tikv-expr --locked --offline -j1 -- --test-threads=1

`temporal-admission-red.log` fails engine-required evaluation with 1105/NotAdmitted
before the shape gate changes. The final matrix requires engine execution for
DATE/DATETIME(0/3/6), default precision, typed NULL, partial-zero and invalid-calendar
(nonzero packed) dates. It covers offsets -12h/UTC/+8h, sql_mode 0/all-known bits,
copying/borrowed modes, literal value/kind/FSP equality and nested YEAR: 132 engine
rows with no warnings or native fallback. A unit test pins MysqlTime's exact
8-byte big-endian packed payload and existing type/FSP metadata; no wire changes.
Mismatch, malformed physical fields, hidden/declared TIMESTAMP and packed-zero
shapes remain explicitly declined. This is existing TiKV kernel capability now
admitted by TiDB, not new general temporal parsing or clock support.

Audit found a material boundary in TiKV Time::from_packed_u64: nonzero
DATE/DATETIME uses unchecked wall-field construction; zero uses SQL-mode
validation and can warn/error; TIMESTAMP applies timezone conversion. Native
Constant evaluation just returns its value. Admission therefore checks kind/FSP,
reuses bridge physical-shape checks, and leaves zero/TIMESTAMP excluded rather
than silently changing success/error or emitting decode warnings. The context-free
catalog encoder and TiKV code are unchanged.

`temporal-admission-green.log`: 15 unit / 4 integration targeted tests pass.
Full expression: 1222/66 passed (99 unit ignored); executor: 1399/355/6/2
passed (184 integration ignored). Full logs: `temporal-expr-on.log` and
`temporal-executor-on.log`. Serial single-worker 8192-RSS/16384-AS MiB guard;
sampled peak 3071.0 MiB (scoped build 2538.3 MiB). Feature-off code is unchanged
and was not rerun this round. No TiKV code changes/test rerun, new benchmark,
Go oracle, full mysql replay or make lint; native fallback remains.

INSERT/scalar-helper validation (TiDB `rust/`, same serial guarded environment):

    cargo test -q -p tidb-executor --features tikv-expr --lib insert_engine_tests --locked --offline -j1 -- --test-threads=1
    cargo test -q -p tidb-expr --features tikv-expr --locked --offline -j1 scalar_row_helpers -- --test-threads=1
    cargo test -q -p tidb-expr --features tikv-expr --test all --locked --offline -j1 eval_row_values_preserves_indexes -- --test-threads=1
    cargo test -q -p tidb-expr --features tikv-expr --locked --offline -j1 -- --test-threads=1
    cargo test -q -p tidb-executor --features tikv-expr --locked --offline -j1 -- --test-threads=1
    cargo test -q -p tidb-expr --locked --offline -j1 -- --test-threads=1
    cargo test -q -p tidb-executor --locked --offline -j1 -- --test-threads=1
    cargo test -q -p tidb-planner --lib --locked --offline -j1 ranger::points -- --test-threads=1
    cargo test -q -p tidb-planner --lib --locked --offline -j1 physical::tests -- --test-threads=1

`insert-engine-red.log` has zero engine rows instead of two. Final INSERT tests
pass both receipts and scalar binary assignment, and pin overflow stopping before
later rows/storage writes. `scalar-helper-before.log` demonstrates wrong indexed
arithmetic (-7 instead of +7); `scalar-index-before.log` catches the old external
None fallback. Initial `scalar-helper-red.log` was a BinaryLiteral fixture compile
error, not behavioral evidence; `scalar-index-red.log` used an invalid test target
(`all` is the actual integration target). Final tests preserve original indexes,
empty constants, scalar kinds, required-context errors and parameter single-read
behavior, including feature-off and both engine transport modes.

`insert-helper-expr-on/off.log`: 1221/64 and 1187/18 passed, 99 ignored unit
tests each. Executor on/off: 1399/355/6/2 and 1346/329/6/0, with 184 ignored
integration tests each. Planner ranger points: 15 passed. The attempted
physical::scan_ranges filter selected zero tests, so is not evidence; the actual
physical::tests module was run instead (46 passed). Serial single-worker guarded commands
sampled at most 2700.3 MiB RSS this round; full expr/executor on/off samples were
2022.3/2560.4/1542.9/2175.8 MiB (limits 8192 RSS / 16384 AS MiB).

Scalar helper Option success semantics intentionally changed (always Some,
otherwise error); there are no production eval_row_values callers. Constant-row
consumers include planner ranges and DDL, covered by executor and scoped planner
regression, not a full Go oracle. Native fallback/conversions/kernels remain;
INSERT/helper programs are local, not cross-statement retained. No new benchmark,
full mysql replay, TiKV suite or make lint run; no PR-readiness claim.

DML/pruning validation (TiDB `rust/`, same serial guarded environment):

    cargo test -q -p tidb-executor --features tikv-expr --lib driver::dml::correlated::engine_tests --locked --offline -j1 -- --test-threads=1
    cargo test -q -p tidb-executor --features tikv-expr --lib partition_pruning::tests::partition_expression_ --locked --offline -j1 -- --test-threads=1
    cargo test -q -p tidb-executor --features tikv-expr --lib partition_pruning::tests --locked --offline -j1 -- --test-threads=1
    cargo test -q -p tidb-executor --features tikv-expr --locked --offline -j1 -- --test-threads=1
    cargo test -q -p tidb-executor --locked --offline -j1 -- --test-threads=1

`dml-program-red.log`: zero engine rows instead of four, and zero-column DML
returned an empty-result error. `pruning-sparse-red.log`: five failures including
empty-input panics, zero engine receipt and mandatory-engine native bypass.
Initial `*-before.log` attempts had an AST fixture compile error (Int requires
String); `dml-program-green.log` caught an incorrect Chunk import path. Neither
is behavioral RED evidence. `dml-program-fixed.log` additionally caught virtual
input being shadowed by the original empty chunk; preferring the virtual input
fixed it. Final `dml-program-final.log` passes 3 tests and
`pruning-sparse-final.log` passes 28. Cache assertions cover scalar/physical and
post-Apply execution, current-row rebinding, binary assignment values and
recovery after overflow. All selected-row evaluation remains at original demand
points; subquery/assignment ordering was not rewritten.

Full `dml-pruning-on.log`: 1397/355/6/2 passed; off: 1346/329/6/0, with 184
integration tests ignored each. Commands were serial with single Cargo/test
workers and the 8192-MiB RSS / 16384-MiB AS guard. Behavioral-red sampled peak
3126.7 MiB; final scoped compile sample 2398.6 MiB, full on/off samples
2080.6/2062.1 MiB. The 0.8-MiB cached pruning sample is not a runtime bound.
No expression-core or TiKV code changed; their full suites were not rerun this
round. Full mysql replay, new Go oracle, performance and make lint were not run.
Sparse pruning still allocates a temporary suite/program and may compile on each
probe. The textual audit now counts 39 raw / 23 candidates / 5 production / 18
test: two production entries are dead duplicate code and two are routed DML
wrappers, leaving INSERT VALUES as the sole direct live call in this inventory.
This is not proof of native removal; forwarding helpers/internal kernels remain.

Pushed-scan/statistics validation (TiDB `rust/`, same serial guarded environment,
no local Cargo patch):

    cargo test -q -p tidb-executor --features tikv-expr --lib predicate_pushdown::tests::retained_engine --locked --offline -j1 -- --test-threads=1
    cargo test -q -p tidb-executor --features tikv-expr --lib statistics_samples_use_engine --locked --offline -j1 -- --test-threads=1
    cargo test -q -p tidb-executor --features tikv-expr --lib access_cost::tests --locked --offline -j1 -- --test-threads=1
    cargo test -q -p tidb-expr --features tikv-expr --lib filter_scalar_truth_keeps_binary_literal_kind --locked --offline -j1 -- --test-threads=1
    cargo test -q -p tidb-expr --features tikv-expr --lib evaluator::tests --locked --offline -j1 -- --test-threads=1
    cargo test -q -p tidb-executor --features tikv-expr --lib predicate_pushdown::tests --locked --offline -j1 -- --test-threads=1
    cargo test -q -p tidb-expr --features tikv-expr --locked --offline -j1 -- --test-threads=1
    cargo test -q -p tidb-executor --features tikv-expr --locked --offline -j1 -- --test-threads=1
    cargo test -q -p tidb-expr --locked --offline -j1 -- --test-threads=1
    cargo test -q -p tidb-executor --locked --offline -j1 -- --test-threads=1

`pushed-filter-before.log` has four engine-receipt failures (zero rows);
`statistics-engine-before.log` has zero instead of seven sample rows. Routing
then exposed `pushed-filter-typed.log`'s binary literal truth failure, independently
reproduced in the shared filter by `filter-truth-before.log`. Switching Boolean
coercion to the scalar-preserving entry fixes both without widening binary
literal admission. Cache tests pin clone/conjoin reuse, fresh remap programs,
uncompiled skipped conditions, and statistics recovery after overflow. Unknown
selectivity/error handling (including the special NULL sample) is preserved;
errors do not replay natively. The erased-context path forwards the complete
trait object rather than approximating a statement context.

Final scoped results: 29 access-cost / 23 evaluator / 10 pushed-filter tests.
Final `scan-stats-expr-on/off.log`: 1220/63 and 1186/18 (99 ignored unit tests
each). Final `scan-stats-executor-on/off.log`: 1388/355/6/2 and 1340/329/6/0
(184 ignored integration tests each). Serial single Cargo/test workers used the
8192-MiB RSS / 16384-MiB AS guard. Sampled round peak 3243.7 MiB; final full-suite
samples 1678.3 / 2385.6 / 1406.8 / 3065.6 MiB. Very short cached tests can miss
runtime peaks. Classifier: 36 raw / 21 evaluator / 8 production / 13 test, or
6 production excluding the dead duplicate (DML, correlated DML, pruning).

Removing prehashed local IN/direct LIKE shortcuts can regress scan performance,
particularly optional native fallback; caching is not proof of a speedup. No
full mysql replay, new Go oracle, performance comparison or make lint/PR-readiness
validation was run. Native casts/fallback/kernels, forwarding-helper audit and
generated/default statement-owned caching remain incomplete.

Schema-expression/late-cast validation (TiDB `rust/`, same serial guarded
environment, no local Cargo patch):

    cargo test -q -p tidb-executor --features tikv-expr --lib generated_column::tests::generated_engine_keeps --locked --offline -j1 -- --test-threads=1
    cargo test -q -p tidb-executor --features tikv-expr --lib generated_binary_literal_keeps_numeric_conversion_kind --locked --offline -j1 -- --test-threads=1
    cargo test -q -p tidb-executor --features tikv-expr --lib generated_column::tests --locked --offline -j1 -- --test-threads=1
    cargo test -q -p tidb-expr --features tikv-expr --lib evaluator::tests --locked --offline -j1 -- --test-threads=1
    cargo test -q -p tidb-executor --features tikv-expr --lib column_default::tests --locked --offline -j1 -- --test-threads=1
    cargo test -q -p tidb-executor --features tikv-expr --lib union_scan::tests::generated_engine::binary_literal --locked --offline -j1 -- --test-threads=1
    cargo test -q -p tidb-executor --features tikv-expr --lib union_scan::tests --locked --offline -j1 -- --test-threads=1
    cargo test -q -p tidb-expr -p tidb-executor --features tidb-executor/tikv-expr --lib hybrid_numeric_flag --locked --offline -j1 -- --test-threads=1
    cargo test -q -p tidb-expr --features tikv-expr --lib hybrid_numeric_flag --locked --offline -j1 -- --test-threads=1
    cargo test -q -p tidb-expr --features tikv-expr --locked --offline -j1 -- --test-threads=1
    cargo test -q -p tidb-executor --features tikv-expr --locked --offline -j1 -- --test-threads=1
    cargo test -q -p tidb-expr --locked --offline -j1 -- --test-threads=1
    cargo test -q -p tidb-executor --locked --offline -j1 -- --test-threads=1

Evidence in `expression-reuse/`: `generated-binding-before.log` fails with
zero engine rows vs two; `generated-literal-native-fixed.log` pins the original
0x10 -> 16 behavior, while `generated-binding-typed.log` fails with 0 instead
of 16 after typed-facade routing. `union-literal-before.log` independently
reproduces an IncorrectValue error from the same tag loss. The separate
`eval_selected_for_cast` path preserves native fallback Datum kinds without
changing typed projection APIs, required-engine refusal, or no-error-replay.
Binary literals remain engine-declined, not engine-executed. Initial compile
failures (Chunk import/clock fixture tuple and missing into_eval_error) were
corrected before behavioral validation. Scoped passes: 21 evaluator, 9 generated,
12 default and 11 UnionScan tests; the additional hybrid test follows below.

Self-review found `ENUM_SET_AS_INT` was lost by Sort's plain cell transfer and
late-cast result carriers. `hybrid-flag-before.log` and
`hybrid-cast-before.log` both fail Enum vs UInt; `hybrid-flag-fixed.log` passes
both ENUM/SET regressions after metadata adaptation. This is ordinal/bitmask
value transport, not copied scalar kernels. Final `schema-expr-on/off.log`
pass 1219/63 and 1185/18 (99 ignored unit tests each); final
`schema-executor-on/off.log` pass 1381/355/6/2 and 1340/329/6/0 (184 ignored
integration tests each). These final runs include all new regressions.

Single Cargo/test workers and serial heavy commands used the 8192-MiB RSS /
16384-MiB AS guard. Largest sampled RSS across the round: 3228.0 MiB; final
hybrid/expr-on/executor-on/expr-off/executor-off samples: 2531.8 / 1994.9 /
2462.4 / 1161.1 / 2018.7 MiB. Short cached scoped runs can under-sample peaks.
Classifier: 39 raw / 24 evaluator / 11 production / 13 test, or 9 production
excluding the dead duplicate. Generated/default helpers intentionally retain no
cache on mutable model descriptors; statement-owned retention and gather-copy
removal remain pending. Native fallback/casts, clock/binary admission gaps and
native kernel deletion remain. Full mysql replay, expanded Go oracle, performance
and make lint/PR readiness were not verified.

UnionScan/Sort validation (TiDB `rust/`, same serial guarded environment,
no local Cargo patch):

    cargo test -q -p tidb-executor --features tikv-expr --lib union_scan::tests::generated_engine --locked --offline -j1 -- --test-threads=1
    cargo test -q -p tidb-executor --features tikv-expr --lib union_scan::tests --locked --offline -j1 -- --test-threads=1
    cargo test -q -p tidb-executor --features tikv-expr --lib sort::tests --locked --offline -j1 -- --test-threads=1
    cargo test -q -p tidb-executor --features tikv-expr --locked --offline -j1 -- --test-threads=1
    cargo test -q -p tidb-executor --locked --offline -j1 -- --test-threads=1

The red control failed with one engine row versus three: only the old condition
facade ran in the engine, not the two generating expressions. After routing,
all 10 UnionScan / 30 Sort tests passed. Executor feature-on passed
1373 / 355 / 6 / 2; off passed 1338 / 329 / 6 / 0 (184 ignored integration
tests each). Single Cargo/test workers ran serially under the 8192-MiB RSS /
16384-MiB AS guard. Sampled peak RSS: 2601.2 MiB red control; 2754.8 / 0.8 /
2565.8 / 2096.2 MiB successful commands (the fast Sort run's 0.8-MiB sample is
not a measured runtime-memory bound). Generated-column tests verify NOT NULL
zero substitution is seen by the next column, reuse across fresh Next scratch
rows, and error-stopping/rebinding without recompilation. Sort tests cover
exact keys, NULLs, missing/negative/out-of-range column metadata, ignored deferred
constants and unmaterialized-shape rejection. Sort's transfer is not expression
engine execution. Current classifier: 41 raw / 26 evaluator / 13 production /
13 test, or 11 production excluding the unlinked duplicate. No new scratch copy
was added. Native casts/fallback remain; full mysql replay, expanded Go oracle,
performance and make lint/PR readiness remain unverified.

Retained filter/admission validation (TiDB `rust/`, same serial guarded environment,
no local Cargo patch):

    cargo test -q -p tidb-expr --features tikv-expr --lib filtering_must_not_bypass_required_engine --locked --offline -j1 -- --test-threads=1
    cargo test -q -p tidb-expr --features tikv-expr --lib evaluator::tests --locked --offline -j1 -- --test-threads=1
    cargo test -q -p tidb-expr --features tikv-expr --locked --offline -j1 -- --test-threads=1
    cargo test -q -p tidb-expr --locked --offline -j1 -- --test-threads=1
    cargo test -q -p tidb-executor --features tikv-expr --lib selection::tests --locked --offline -j1 -- --test-threads=1
    cargo test -q -p tidb-executor --features tikv-expr --locked --offline -j1 -- --test-threads=1
    cargo test -q -p tidb-executor --locked --offline -j1 -- --test-threads=1

The first command failed before implementation because the filtering facade
ignored required-engine admission and executed getvar natively. The first
post-fix compile caught an owned String passed to Unsupported(&'static str);
after that mechanical correction, 20 evaluator and 10 Selection tests passed.
Expression feature-on passed 1217 / 63 / 0; off 1183 / 18 / 0 (99 ignored lib
tests each). Executor on passed 1368 / 355 / 6 / 2; off 1335 / 329 / 6 / 0
(184 ignored integration tests each). All heavy runs were serial with one
Cargo/test worker, guarded at 8192-MiB RSS / 16384-MiB AS. Sampled peak RSS:
2593.6 MiB red control; 1101.7 MiB compile correction; successful commands
2231.5 / 2164.4 / 1493.4 / 3115.0 / 2626.5 / 2747.7 MiB.
The Selection tests read multiple real child chunks, assert admitted engine row
receipts and one retained compilation, and explicitly keep native facade
fallback for user-variable assignment. Runtime engine-off contexts retain the
native typed batch path. No errors are caught for native replay. Masks retain
the existing all-physical-rows-before-Sel intersection contract; rejected rows
are removed before later predicates. Classifier: 43 raw / 28 evaluator sites,
15 production / 13 test (13 production excluding the dead copy). Full mysql
replay, Go-oracle expansion, performance, make lint/PR readiness and engine-only
execution remain unverified.

Window aggregate-cache validation (TiDB `rust/`, same serial guarded environment,
no local Cargo patch):

    cargo test -q -p tidb-executor --features tikv-expr --lib window::selected_key_tests::aggregate_ --locked --offline -j1 -- --test-threads=1
    cargo test -q -p tidb-executor --features tikv-expr --lib window:: --locked --offline -j1 -- --test-threads=1
    cargo test -q -p tidb-executor --features tikv-expr --locked --offline -j1 -- --test-threads=1
    cargo test -q -p tidb-executor --locked --offline -j1 -- --test-threads=1

The red control retained the diagnostic fields but temporarily constructed a
fresh `WindowAggregateEvaluator` at each emission (the previous ownership
behavior). Both new tests failed on retained compilation count 0 versus 1.
Restoring the retained evaluator made all 10 window tests pass. Full feature-on
groups passed 1365 / 355 / 6 / 2; feature-off passed 1335 / 329 / 6 / 0, both
with 184 ignored integration tests. Sampled peak RSS: 3294.3 / 2727.5 / 2200.0 /
2887.3 MiB in command order, under the 8192-MiB RSS / 16384-MiB AS guard with one
Cargo/test worker. Tests seed the already-fetched window buffer and exercise
real Next emission; they do not validate remote fetching or a Go window oracle.
Frame accumulator state stays fresh, and the recomputation algorithm is
unchanged. No direct native dispatch count changes in this step. Full mysql
replay, performance, repository-wide lint and engine-only execution remain
unverified.

Aggregate argument-program validation (TiDB `rust/`, same serial guarded
environment, no local Cargo patch):

    cargo test -q -p tidb-executor --features tikv-expr --lib hash_agg:: --locked --offline -j1 -- --test-threads=1
    cargo test -q -p tidb-executor --features tikv-expr --locked --offline -j1 -- --test-threads=1
    cargo test -q -p tidb-executor --locked --offline -j1 -- --test-threads=1

The first build caught one remaining test matching `AggInputMode::FinalCount`
after the type became a plan wrapper (E0223); the assertion now inspects its
unchanged typed `kind`. All 68 aggregate tests then passed, including three new
engine/native demand/cache tests. Full feature-on groups passed 1363 / 355 / 6 /
2, feature-off passed 1335 / 329 / 6 / 0, both with 184 ignored integration
tests. Peak RSS: 2228.1 MiB for the initial build failure; 3392.9 / 2394.1 /
2878.1 MiB for the successful commands, under the 8192-MiB RSS / 16384-MiB AS
guard with one Cargo/test-harness worker. Classifier: 44 raw / 29 evaluator
sites, 16 production / 13 test (14 production after excluding the dead copy).
Typed aggregate kernels and encoding/rendering remain unchanged. GROUP_CONCAT
sort-after-NULL is pinned as current native behavior, not a Go-oracle result.
At that step, window-frame plans still needed cross-frame retention (resolved
by the window-cache validation above). No new input-row
scratch copy was added; additional per-plan metadata and runtime allocation
costs were not benchmarked. Full mysql replay, performance, repository-wide
lint and engine-only execution remain unverified.

Index-probe bound validation (TiDB `rust/`, same serial guarded environment,
no local Cargo patch):

    cargo test -q -p tidb-executor --features tikv-expr --lib join::tests:: --locked --offline -j1 -- --test-threads=1
    cargo test -q -p tidb-executor --features tikv-expr --locked --offline -j1 -- --test-threads=1
    cargo test -q -p tidb-executor --locked --offline -j1 -- --test-threads=1

The first targeted run passed 42 and failed the new bound regression with
`eval_chunk needs exactly one calculated expression`: column-swap mode had
removed a direct-column bound from the calculation list. Setting
`avoid_column_evaluator=true` fixed the integration mistake. The corrected run
passed all 43. Full feature-on groups passed 1360 / 355 / 6 / 2, feature-off
passed 1335 / 329 / 6 / 0 (both with 184 ignored integration tests). Sampled RSS
peaks: 2824.4 MiB for the initial failure, then 2503.4 / 1874.3 / 2195.8 MiB;
all ran under the 8192-MiB RSS / 16384-MiB AS guard with one Cargo/test worker.
The classifier reports 51 raw / 36 evaluator sites (23 production / 13 test).
The now-unused local `truthy` wrapper was removed. Tests exercise the real
probe-construction function across batches, not a remote database. Full mysql
replay, performance, repository-wide lint and engine-only execution remain
unverified.

Local lookup-filter validation (TiDB `rust/`, same serial guarded environment,
no local Cargo patch):

    cargo test -q -p tidb-executor --features tikv-expr --lib lookup_filter_engine --locked --offline -j1 -- --test-threads=1
    cargo test -q -p tidb-executor --features tikv-expr --locked --offline -j1 -- --test-threads=1
    cargo test -q -p tidb-executor --locked --offline -j1 -- --test-threads=1

All three targeted tests passed. Full feature-on groups passed 1359 / 355 / 6 /
2, feature-off passed 1335 / 329 / 6 / 0 (both with 184 ignored integration
tests). Sampled peak group RSS: 2673.2 / 2670.6 / 2088.3 MiB respectively under
the 8192-MiB RSS / 16384-MiB AS guard. Cargo and test-harness workers stayed at
one. The interrupted prior round had saved implementation edits but not its
tests; source inspection confirmed this before adding/running the tests.
The classifier reports 52 raw / 37 evaluator sites (24 production / 13 test).
New tests exercise local storage Next and the production template constructor;
they do not open a remote cursor. Existing scratch copies and remote-predicate
handling are unchanged. Full mysql replay, performance, repository-wide lint
and engine-only behavior remain unverified for this change.

Chunk-backed/parallel residual validation (TiDB `rust/`, same serial guarded
environment, no local Cargo patch):

    cargo test -q -p tidb-executor --features tikv-expr --lib join::tests:: --locked --offline -j1 -- --test-threads=1
    cargo test -q -p tidb-executor --features tikv-expr --locked --offline -j1 -- --test-threads=1
    cargo test -q -p tidb-executor --locked --offline -j1 -- --test-threads=1

The first targeted run passed 41 and failed one new assertion: the test had
mistakenly used the table's `parallel_exact_int_enabled` flag as the worker
selector. Source inspection confirmed dispatch uses `shared.unique_exact_int`
(which also checks key count/class). The corrected assertion checks that actual
selector and the shared cache. Targeted tests then passed 42; full feature-on
groups passed 1356 / 355 / 6 / 2, feature-off passed 1335 / 329 / 6 / 0, both
with 184 ignored integration tests. Peak RSS: 3181.4 MiB for the initial run;
2519.9 / 2562.2 / 2039.6 MiB for the three successful commands. Tests use two
small probe workers where needed; Cargo and test-harness workers stay at one.
The classifier reports 53 raw / 38 evaluator sites (25 production / 13 test).
Scratch input copies, outer-filter temporary programs and the index-bound
native site remain. Performance, full mysql replay and repository-wide lint
were not rerun; this does not establish engine-only Join execution.

Native-site scope classifier validation (TiDB repository root; tooling-only,
no heavy build jobs):

    PYTHONDONTWRITEBYTECODE=1 python3 rust/scripts/test_classify_native_eval_sites.py
    PYTHONDONTWRITEBYTECODE=1 python3 rust/scripts/classify-native-eval-sites.py --list

Before the fix, 16 fixture tests produced 16 failing subtest assertions. After
the fix and two additional controls, 18 tests pass. The audited source output
is 54 raw / 39 evaluator hits, split 26 production-scope / 13 test-only, exactly
matching the prior manual audit (24 production-scope after the known unlinked
duplicate). Unsupported generic syntax may conservatively retain test calls;
the scanner is not a Rust AST/macro/reachability analysis. No Rust execution
behavior changed; cargo, full mysql replay, performance and repository-wide
lint were not rerun for this tooling change.

JoinExec/index-hash cache validation (TiDB `rust/`, same serial guarded
environment, no local Cargo patch):

    cargo test -q -p tidb-executor --features tikv-expr --locked --offline -j1 -- --test-threads=1
    cargo test -q -p tidb-executor --locked --offline -j1 -- --test-threads=1

Feature-on groups passed 1355 / 355 / 6 / 2; feature-off passed 1335 / 329 /
6 / 0; both integration groups ignored 184 tests. Peaks: 2690.2 / 2130.4 MiB.
New tests exercise both NULL demand policies through the real datum/index-pair
methods, merge-key changes refreshing residual predicates, and scalar
index-hash task/worker descriptors seeing the same compilation before their
own first evaluation. Existing scratch copies and other native chunk/parallel
paths remain. No performance, full mysql replay or repository-wide lint claim.

Join CNF routing validation (TiDB `rust/`, same serial guarded environment,
no local Cargo patch):

    cargo test -q -p tidb-executor --features tikv-expr --lib joiner:: --locked --offline -j1 -- --test-threads=1
    cargo test -q -p tidb-executor --features tikv-expr --locked --offline -j1 -- --test-threads=1
    cargo test -q -p tidb-executor --locked --offline -j1 -- --test-threads=1

Targeted tests: 22 passed. Feature-on executor groups: 1352 / 355 / 6 / 2
passed; feature-off: 1335 / 329 / 6 / 0 passed; both integration groups have
184 ignored tests. Peaks: 3975.0 / 2246.7 / 3005.7 MiB. The classifier now
reports 56 raw hits and 41 evaluator sites (19 production-labelled, 22
test-only); excluding two known unlinked calls leaves 17 to review/route.
No zero-copy Join or engine-only claim is made. The convenience CNF wrapper
still creates temporary programs, and existing scratch-row copying remains.
Performance, full mysql replay and repository-wide lint were not rerun.

Selected-suite/cache and mandatory-engine validation (TiDB `rust/`, same
serial guarded environment, no local Cargo patch):

    cargo test -q -p tidb-expr --features tikv-expr --lib required_engine_rejects_row_major_native_dispatch --locked --offline -j1 -- --test-threads=1
    cargo test -q -p tidb-expr --features tikv-expr --locked --offline -j1 -- --test-threads=1
    cargo test -q -p tidb-expr --locked --offline -j1 -- --test-threads=1
    cargo test -q -p tidb-executor --features tikv-expr --locked --offline -j1 -- --test-threads=1

The first command failed before the row-major guard (expected `ExternalEngine`
but native evaluation ran). After the fix, expression feature-on passed
1215 lib / 63 integration tests; feature-off passed 1183 / 18, both with 99
ignored lib tests. Executor feature-on passed 1349 / 355 / 6 / 2, with 184
ignored integration tests. Peak RSS: 1798.6 / 2123.6 / 2564.1 MiB for those
three full runs. Join callers are still unconverted. Full mysql replay,
performance and repository-wide lint remain unverified for this change.

Explicit physical selection adapter validation (TiDB `rust/`, same serial
memory-guarded environment, no local Cargo patch):

    cargo test -q -p tidb-expr --features tikv-expr --locked --offline -j1 -- --test-threads=1
    cargo test -q -p tidb-executor --features tikv-expr --locked --offline -j1 -- --test-threads=1

Expression results: 1213 lib tests passed (99 ignored) and 62 integration tests
passed; peak RSS 2230.1 MiB. Executor groups passed 1349 / 355 / 6 / 2 tests
(184 integration tests ignored), peak RSS 3199.5 MiB. This adapter API does not
yet route Join callers;
no copied input chunk or concatenated row was introduced, while the pre-existing
copying backend still materializes selected cells when chosen. Performance, mysql
replay and repository-wide lint were not verified for this incremental change.

Independent physical input lengths follow-up: TiKV package tests passed
484 tests (4 doc tests ignored), peak RSS 1071.0 MiB, using the TiKV command
recorded below. TiDB was pinned to the tested fork revision `5c1fb99` and ran
without any local Cargo patch, using the two recovery-audit commands below:
1212 lib tests passed (99 ignored), 60 integration tests passed. Peaks were
2372.5 MiB and 2153.3 MiB. `Cargo.lock` was regenerated and retained. No TiDB
Join routing is claimed by these facade-only tests; executor/mysql replay and
performance/lint were not rerun for this dependency update.

Window RANGE follow-up reran the same three commands below unchanged.
Targeted tests: 8 passed. Feature-on suite groups: 1349 / 355 / 6 / 2 passed;
feature-off: 1335 / 329 / 6 / 0 passed; both integration suites had 184 ignored
tests. Peak RSS: 2548.1 MiB (targeted), 2578.5 MiB (feature-on), 2038.8 MiB
(feature-off). The classifier reports 57 raw hits and 42 evaluator sites
(20 production-labelled, 22 test-only), including two known unlinked calls.
Window now has zero direct native calls. Full mysql replay, performance and
repository-wide lint remain unverified for this incremental change.

Window value/default follow-up reran the three commands below unchanged.
Targeted tests: 5 passed. Feature-on suite groups: 1346 / 355 / 6 / 2 passed;
feature-off: 1335 / 329 / 6 / 0 passed; both integration suites still had 184
ignored tests. Peak RSS was 3696.2 MiB for the targeted build, 2198.1 MiB for the
full feature-on run and 2898.0 MiB for feature-off. The classifier now reports
59 raw hits, 44 evaluator sites (22 production-labelled, 22 test-only), with
only two direct native sites remaining in Window (RANGE). The same full-mysql,
performance and repository-wide-lint limitations apply.

Window key migration validation (TiDB repository `rust/`, same memory-guarded
serial environment as below, no Cargo patch):

    cargo test -q -p tidb-executor --features tikv-expr --lib window::selected_key_tests --locked --offline -j1 -- --test-threads=1
    cargo test -q -p tidb-executor --features tikv-expr --locked --offline -j1 -- --test-threads=1
    cargo test -q -p tidb-executor --locked --offline -j1 -- --test-threads=1

The targeted run passed all 3 tests. Feature-on suite groups passed
1344 / 355 / 6 / 2 tests, feature-off groups passed 1335 / 329 / 6 / 0;
both integration suites had 184 ignored tests. Peak RSS across these commands
was 3376.3 MiB. The native-site classifier now reports 62 raw hits, 47 evaluator
sites (25 production-labelled and 22 test-only); two production-labelled sites
remain the known unlinked `stream_agg.rs` duplicate. Window has five native sites
left, down from seven. Full mysql replay, performance and repository-wide lint
were not rerun for this incremental migration.

Recovery-audit commands (TiDB repository `rust/`, no Cargo patch):

    cargo test -q -p tidb-expr --features tikv-expr --lib -j1 -- --test-threads=1
    cargo test -q -p tidb-expr --features tikv-expr --test all --locked --offline -j1 -- --test-threads=1

Both ran serially under `limited-run.py --rss-mib 8192 --as-mib 16384` using
nightly-2026-08-22, `CARGO_BUILD_JOBS=1`, `RUSTFLAGS=-Awarnings`,
`MALLOC_ARENA_MAX=2`, `RUST_MIN_STACK=4194304`,
`CMAKE_POLICY_VERSION_MINIMUM=3.5`, `CXXFLAGS='-w -std=gnu++14 -include cstdint'`
and `CFLAGS=-w`. Peak group RSS: 2473.3 MiB (lib), 1109.1 MiB (integration).
TiKV repository validation used the same environment and guard:

    cargo test -q -p tidb_query_expr -j1 --offline -- --test-threads=1

Result: 481 passed, four documentation tests ignored; peak RSS 1051.3 MiB.
This audit did not rerun executor/mysql replay or repository-wide lint and does
not claim native removal or PR readiness. An optional whole-workspace offline
`cargo metadata` inspection failed because `adler32 v1.2.0` was not cached;
this did not affect the above package tests or the generated git-source lock.

`components/tidb_query_expr/EXPRESSION_SEMANTIC_GAPS.md` (TiKV) is the running
list of kernel divergences. This plan, the coverage report and the inventory
generator under `rust/docs` and `rust/scripts` are updated as milestones land.
The earlier copying/borrowed benchmark artifacts remain untouched.


## Interfaces and Dependencies


TiKV keeps `PreparedExpression::compile(serialized_expr, serialized_schema,
Context)` and gains a `Sync` compiled form with an explicit execution-state
argument. It exposes `scalar_function_signature(name) -> Option<i32>` and the
host trait above, and its `Context` carries the optional trait object.
`tidb_query_datatype` gains only the types milestone D justifies.

TiDB's `tikv.rs` keeps `TikvExpression::compile` and `evaluate`/`evaluate_into`
signatures, gains a per-`EvaluatorProgram` cache, and consumes the admission
table instead of name matching. The `Backend` enum and counters stay until
milestone E. No new dependency on a second fork is introduced.

Revision note: the recovery audit pins the actually used selected-input engine,
records the dense-selection regression and unpatched tests, and corrects the
remaining borrowed-lazy and unequal-input-length limitations. It does not mark
Window/Join migration or native deletion complete. Subsequent Window key and
value/default and RANGE entries record incremental routing and test evidence;
Join and native removal remain outstanding. The independent-length facade
follow-up removes the equal-physical-length restriction via a new compatible
entry point; the borrowed-lazy restriction remains.
