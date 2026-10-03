# ANY_VALUE and NAME_CONST checkpoint

`identity-65` follows `weight-format-64`: **206/245** functional delegation/deletion, strict final-audited acceptance **0**.39 eligible families remain;15 more reach221. Overall goal remains active.

## Shared evaluator and complete representation

Eight exclusive owners;16 Rust files (CPP7/native9), two new sources,11 new tests. The only native answer-producing clone arms in builtin_ext/misc are deleted. Two independently identified fixed one-Bytes profiles both use the **unchanged existing** TiKV `any_value_bytes_fn_meta`/leaf. No new identity-copy algorithm or wrapper. Ordinary wire vararg behavior (empty→NULL, multiple→first) remains unchanged; both private boundaries enforce exactly one input and the shared physical validator.

True SQL NULL uses actual Bytes(None) and actual returned None. The other18 Datum kinds use a finite operand-data frame, never a program/opcode/operation selector. The sole shared codec lives in `native_identity.rs`; native `tikv/identity_value.rs` maps actual fields and reconstructs only the returned frame. No original Datum capture, sidecar answer, fake NULL or receipt-only worker. Input kind, collation, Decimal declared shape and all payload fields are themselves returned data. Both operations still use existing Bytes and OwnBytes, with no new driver, carrier, computed-result kind, runtime binding, cause, NoArgs profile or admission.

Preserved representations include distinct range sentinels; signed/unsigned64; Real and Float32's actual **f64** bits; arbitrary String/Bytes/BinaryLiteral/Bit/Raw bytes; Enum/Set names, full u64 and collation; Duration i64 nanos/raw i64 FSP; Time independent raw core/kind/u8 FSP; raw JSON typecode/payload without decoding; and vectors with arbitrary f32 bits and usize dimensions derived from the tail, not a u32 header/SQL dimension cap.

Decimal carries raw coefficient bytes, sign, visible/storage u32 scales and optional signed declared shape. Its ordinary math bridge normalizes representations and is unsuitable for identity. Narrow `coefficient_bytes`/`from_raw_parts` APIs restore it without UTF8/ASCII/scale normalization. Time's packed format merges/clears reserved bits and aliases some raw FSP states; its narrow `from_raw_parts` restores independent fields. Existing constructors/parsers remain unchanged. Collation decoding explicitly inverts the existing16 policies without global-mode lookup or invoking Pinyin keys. Physical validation does not impose numeric, JSON, calendar, FSP, finite-float or string semantics.

NAME_CONST's name remains evaluated where the original frontend evaluated it, before value; the unused name is not additionally serialized, validated or allocated. Original arity handling, LTR errors, NULL demand, fold eligibility, labels, return-type handling and build-only1210 literal/unary gate remain. Native PB/legacy have no newly admitted signatures; native vector/helper paths already converge on this leaf.

## Validation

[Commands and raw hashes](../logs/identity-summary.txt), [manifest](../checkpoint.json), [ledger](../migration-progress.json).

Eleven Cargo launches, all completed nonzero test runs: six green gates, three corrected **new-test** REDs and two unchanged old full-suite REDs. No compile failure, interruption or zero-match run. All11 new tests finally pass; old test bodies/oracles remain byte-exact. No runtime production repair was made after gates began.

- CPP identity12 and local316/1ignored pass (overlapping filters, not unique totals).
- Native raw Time1, raw Decimal1, identity19/1ignored, SQL2 pass.
- Full expression1550/4old/94ignored, unistore208/1old/13ignored. Whole failure sections and lists match the preceding checkpoint after only numeric panic-thread-ID normalization. All four new expression tests are explicitly `ok` in the full log.
- SDK covers19 kinds plus four extra representations through **both** profiles, cache reuse, actual NULL/min/max distinctions and all23×2 zero-slot roots. It includes vector dimension16384 with NaN/-0/Inf under an explicitly sufficient call budget, malformed frames and wrong result kinds without NULL fallback. A separate decode test begins with returned frames, not an original Datum.
- SQL pins14 ANY_VALUE domains, six NAME_CONST values/labels, explicit alias/case behavior, metadata, three1210 rejection cases and16 unmasked zero-slot probes including real NULL columns. NAME_CONST('label',+column) is already admitted and unary plus is erased by the rewriter; no other worker masks these roots.
- Both pinned formatters and diff checks pass. Both locks/manifests, existing wire leaf, caller/fold/type/PB/legacy logic, FSP normalization and chunk materialization are unchanged. No fixture rerecording, generator, dependency, Go or Bazel changes.

## Corrected test assumptions and preserved compatibility gaps

These are source-backed corrections, not output-derived oracle regeneration; all three RED logs remain.

1. New raw-Time test guessed that Time::new(DateTime,7) rejects. Original fsp.rs64–77 explicitly clamps above six. Only the new same-input assertion/comment was corrected to FSP6. Raw FSP7 still requires exact reconstruction instead of normalization.
2. New AST test assumed the arity call enforced the comment's intended gate. Original func.rs42 uppercases, then57 passes that name to the case-sensitive lowercase registry (builtin_registry.rs389–396/403/424–426). It therefore misses the registry and malformed AST calls evaluate children before Unsupported. Only the new test pins this existing behavior; no production repair or new admission. SQL/new_function arity handling stays unchanged.
3. New SQL fixture expected no Decimal shape and Go-like string metadata. Original chunk/row.rs102–112 attaches declared shape(8,3). More importantly, rewriter.rs1139–1163 applies generic collation derivation after the initial identity type clone; collation_derive.rs676/682–698/718–726 uses connection collation for these string results. record_set.rs90–104 materializes using those output types, then chunk/row.rs64–76 stamps that collation on String values. With this test's explicit utf8mb4/utf8mb4_bin connection, both VARCHAR and VARBINARY results therefore use that metadata. Only new fixed expected fields and explicit metadata pins/comments changed; all bytes, types, cases, other assertions and zero-slot probes remain. Go copies the argument type after base construction, so this is an **existing SQL metadata compatibility gap**, not a Go-oracle correction or a production fix.

The SDK proves actual Datum metadata preservation. It does **not** prove Go-compatible SQL whole-FieldType identity. Both newly documented caller gaps are explicitly deferred. Initial RO conclusions about AST pre-child arity and FSP rejection are withdrawn.

## Remaining work

Framing copies, vector temporary bytes and existing vector-init allocation do not establish performance neutrality, zero-copy, physical-heap/peak/OOM guarantees. Public raw constructors are representation APIs, not new SQL parsers. Prior JSON_KEYS aggregate mismatch, broader default-NoColumns roots/M6, workspace/lint/dev/bazel_prepare, release, exhaustive differential/TiFlash/FIPS and earlier parser/GB/vector/Decimal/deep-JSON exceptions remain deferred. No whole Go-package transcreation or PR-readiness claim.

RO next candidates: TIDB_PARSE_TSO is the smallest temporal utility, preserving lazy timezone demand and raw fixed-offset behavior. TIMEDIFF is an independent smaller parser/subtract/format policy; its typed output cast/getter remains a caller boundary. INTDIV requires four policies and public exact-division SDK closure. TIME+MICROSECOND still need a shared compact-UTC datetime parser prerequisite; existing shared duration parsers differ. Plan decoders are real SDK algorithms/protobuf renderers, not base64-only stubs; SQL digest requires the complete lexer normalizer, not SHA256 alone. No next-candidate credit.

TiKV publishes first; TiDB pins its SHA and the common Plan hash. Both guides are updated. No force push/PR; preexisting untracked client-differential BUILD.bazel stays excluded.
