# Six families — json-nullsafe-55

Previous `grouping-between-54`. **180/245 functional families**, strict final acceptance0, target221;41 more required. Adds NullEq and JSON_CONTAINS, JSON_OVERLAPS, JSON_MEMBER_OF, JSON_CONTAINS_PATH, JSON_LENGTH. No partial JSON_EXTRACT credit. [Commands, failed attempts and hashes](../logs/json-nullsafe-summary.txt).

## Review map

Seven concurrent exclusive owners; parent owns integration, formatting, serialized gates, Plan/docs and publication.24 Rust files (CPP11/native13), one new source and one existing-dependency feature change.

| Owner | Files and responsibility |
|---|---|
| A | Native `tidb-expr/src/{ops.rs,ops/real_coerce.rs,lib.rs}`: delete NullEq predicates, existing-worker composition, two tests and narrow legacy SDK root export |
| B | CPP expression `native_json.rs` (new), `lib.rs`; CPP datatype `json/{native_policy,mod}.rs`; native datatype `binary_json_ops.rs`: unique serde/path core, separate raw SDK predicates, native aliases/deletion, two tests |
| C | CPP expression `local/{batch,compile,registry,mod}.rs`, `types/{function,expr_eval}.rs`: ten unit profiles, checked packing, shared admission, two tests |
| D | Native `tikv/{evaluated_ascii,evaluated_ascii_tests,mod}.rs`: existing Int materializer, checked bridges, legacy MEMBER SDK, two tests |
| E | Native `builtin_ext/json/{predicate,path,report,mod}.rs`: five guarded families, original coercion/demand/error policies, path aliases and one test |
| F | `tidb-unistore/src/cophandler.rs`, `tidb-session/src/tests_core/lifecycle.rs`: existing legacy MEMBER admission, two legacy and two SQL tests |
| G | CPP expression `impl_json.rs`: fixed kernels and shared validators, two tests |

No ordinary PB/legacy admission expansion. NullEq has no legacy opcode; JSON MEMBER OF alone among these five has the existing native PB and legacy routes. Native PB still eagerly evaluates children; legacy still stops at missing/NULL first child.

## Three intentionally different JSON policies

**Prepared serde expression values.** Existing native predicates already convert into serde Value. Their equality uses exact signed/unsigned comparison or f64 partial comparison—not raw JSON epsilon equality. Value/document string treatment, strict document3146 versus permissive length/contains-path coercion, Decimal pathways and original absent boolean-field metadata remain unchanged. Worker inputs serialize these actual already-coerced values; they are neither host-computed answers nor replacement canonicalizations of a formerly raw domain.

**Raw public SDK containment/overlap.** Both algorithms are removed from native `binary_json_ops.rs` and delegated to CPP datatype helpers. Shared raw decoding precedes structural operations. Equality leaves preserve the old from_node normalization: duplicate vectors/count retained, same byte-key sorting, exact scalar payloads and checked depth/encoded-size/u32 representability. This narrow validation/normalization does not add a byte encoder. Nested containment errors remain false; outer errors/overlap propagate. First-duplicate lookup is not replaced by serde last-key behavior.

**Legacy MEMBER OF.** Actual candidate/document binary packets retain raw comparator semantics and fallback. Source inspection corrected an early RO misunderstanding: `element_count` fully decodes the array, so any malformed child fails before even an earlier matching element. The caller retains this representation check and exact `LegacySql("invalid json array")`; the worker owns all iteration/membership/equality. Per-element from_node representability failures are skipped as before. Candidate conversion, SQL folding and missing versus observed-NULL demand remain unchanged.

## Closed execution protocol

Ten fixed unit identities; existing bytes and Int owner only:

- `JsonContainsSerdeNative`: doc/candidate Bytes2.
- `JsonContainsPathSerdeNative`: actual doc/candidate/original path Bytes3.
- `JsonOverlapsSerdeNative`, `JsonMemberOfSerdeNative`: two actual serde values.
- `JsonLengthSerdeNative`: one value; `JsonLengthPathSerdeNative`: value/path.
- `JsonPathExistsSerdeNative`: value/path, including valid multiple-selection paths.
- `JsonMemberOfBinaryLegacy`: actual `[type_code || payload]` candidate/document.
- `JsonPredicateNullNative`: genuine NullWitness(None); `JsonPredicateMissingLegacy`: NoArgs.

Both admission layers use the same validators. Optional-path containment/length reject multiple-selection paths; existence accepts them. Native preparation preserves original typed SQL errors before packet construction; invalid direct protocol packets are infrastructure failures, not fabricated SQL NULL. Real JSON null is a value (length1), unlike SQL NULL. Two narrow checked packing helpers and one legacy MEMBER SDK reuse the existing runtime and Int materializer. No new carrier, result kind, binding, driver, opcode slot or diagnostic cause.

**Contains-path demand:** all original child expressions still evaluate first. The value helper then coerces/parses only demanded paths, invoking one existence worker per path. ONE returns a decisive actual true; ALL a decisive actual false; otherwise the final actual result is returned unchanged. At least one path is guaranteed by arity. No fabricated accumulator, host answer, eager suffix parsing or generic path program.

## Shared path foundation and NullEq

The original path parser/walker moved to CPP `native_json.rs`; native path consumers use aliases and a narrow position-error wrapper. ASCII unquoted identifiers, quoted Unicode, recursive/range/scalar-wrap behavior and per-path pointer dedup remain. Existing modify/search/extract callers retain their own surrounding policy; search's different scalar-array traversal is not unified. Shared foundation alone earns no other JSON family credit.

NullEq uses existing workers rather than another domain ladder. Sentinels retain rejection precedence. If an actual operand is NULL, IsNull receives the other operand's **real documented presence** (None/Some0), not a fake comparison witness. Non-NULL values use existing Eq preparation. Only the original duration-column/constant-text special case adds IsTrue(Eq(actual inputs)), retaining warning+false on failed duration parsing. Invalid Time conversion still returns NULL; existing row projection/empty identity/old malformed-Time panic remain. Integer/real/Decimal/vector/JSON/collation runtime predicates are no longer duplicated in native code.

## Compatibility surface and verification

CPP expression explicitly enables serde_json `float_roundtrip`, already enabled in the native runtime through tidb-model. A prepared Value must survive textual transport bit-exactly. A literal from serde_json's own `tests/test.rs:957–979` verifies the known hard-double case; its final run is distinct from the earlier gate. Feature unification may affect other standalone CPP serde readers' rounding, so whole-wire equivalence is **not** claimed.21 existing ordinary JSON tests pass; exhaustive wire/performance checking is deferred.

15 Cargo attempts:14 actual runs plus1 new-fixture compile failure. Nine final focused gates pass: CPP raw1/newJSON3/local299+1ignored/ordinaryJSON21; native profiles3/NullEq2/rawSDK19; legacy2/SQL2. Full expression **1507/4old/94ignored**, full unistore **205/1old/13ignored**, both exit101. Complete failure sections match their preceding receipts after thread IDs only. Broad native `json` also matched the known unrelated EXP failure; it is reported, not treated as new or green.

One SQL fixture compile error consumed DriverError before diagnostic formatting. One actual new-test RED expected planning3143 for a column-sourced runtime path error. Unchanged executor source explicitly maps typed InvalidPath to1105 at execution; only the NEW expected code was corrected from source. Other error codes, production and old expectations were untouched. All24 source formatter checks and both diff checks pass; locks unchanged; first174 family records identical.19 JSON plus50 NullEq direct-column zero-slot SQL cases isolate actual worker ownership without unrelated WHERE/ORDER/wrapper computations.

## Remaining limitations

Schema2 compacts the current manifest: cumulative family records stay in `migration-progress.json`; exact prior manifests/evidence remain in Git and existing files rather than being copied as stale current claims. Strict acceptance remains0. IntDIV's precision/warning/exact legacy phases, IN/INTERVAL native answers and65 eligible families remain. Whole workspace/lint/dev/bazel_prepare, M6/release/performance/zero-copy/physical heap/peak/OOM, exhaustive domain/context/wire/150-row differential/TiFlash/FIPS and existing parser/GB/ignored-vector/extreme Decimal exceptions remain deferred. The prior row-context activation and broader default-NoColumns request-root gap remain explicit. No whole Go-package transcreation claim.
