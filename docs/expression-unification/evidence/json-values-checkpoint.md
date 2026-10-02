# Four JSON output families — json-values-56

Previous `json-nullsafe-55`. **184/245 functional families**, strict final acceptance0, target221;37 more required. Adds JSON_ARRAY, JSON_OBJECT, JSON_KEYS and JSON_PRETTY. No EXTRACT/UNQUOTE partial credit. [Exact commands and hashes](../logs/json-values-summary.txt).

## Review map

Seven parallel exclusive owners; parent owns formatting, serialized builds, Plan/evidence and paired publication.22 Rust sources (CPP10/native12), no new source/dependency/manifest/lock changes;12 additive tests.

| Owner | Scope |
|---|---|
| B | CPP expression `native_json.rs`/`lib.rs`, datatype `json/{native_policy,mod}.rs`, native `binary_json_ops.rs`: unique constructor/key/formatter algorithms, raw SDK key delegation, root exports and2 tests |
| G | CPP `impl_json.rs`: six fixed byte-result kernels, two packet validators and2 tests |
| C | CPP `local/{batch,registry,mod}.rs`/`types/{function,expr_eval}.rs`: closed profiles, checked packing and2 tests; compile.rs unchanged |
| D | Native `tikv/{evaluated_ascii,evaluated_ascii_tests,mod}.rs`: existing Bytes materializer, checked bridges, computed-JSON projection and2 tests |
| E | Native JSON `construct.rs`/`report.rs`/`text.rs`: original preparation, guards and duplicate-core deletion; original constructor test suffix byte-identical |
| A | Native JSON `mod.rs`/`tests.rs`, `builtin_ext/mod.rs`, `scalar_function.rs`: actual typed context forwarding and2 tests; cached modifier route unchanged |
| F | Session `tests_core/lifecycle.rs`:2 SQL tests, including22 direct zero-slot cases |

None of these four has existing native PB/legacy admission. Ordinary TiKV wire implementations remain unchanged; no new admission is invented. Existing row/vector fallback reaches the contextful scalar evaluator.

## Actual operands, not precomputed answers

Six fixed unit-metadata profiles reuse Values, Bytes/Bytes2 and OwnBytes:

- `JsonArraySerdeNative`: one actual ordered-value packet, u64LE count followed by length/value-text records.
- `JsonObjectSerdeNative`: one ordered-pair packet, pair count followed by key length/UTF8 and value length/JSON text. Duplicate pairs remain intact.
- `JsonKeysSerdeNative`: actual prepared serde document; `JsonKeysPathSerdeNative`: document and original single-selection path.
- `JsonPrettySerdeNative`: actual prepared serde document.
- `JsonOutputNullNative`: genuine NullWitness(None), returning Bytes(None).

A zero-count packet is the real empty input list, not a fabricated argument or a precomputed []/{} result. A bounded shared walker validates count conversion, minimum framing, lengths, JSON/UTF8 and exact packet end; both admission layers use the same validators. No untrusted-count reservation, dynamic opcode, carrier, binding, result kind or driver.

Frontend constructor preparation retains original argument order and FieldType policies: Boolean flags, genuine binary charset/opaque text, ETJson text parsing and ordinary string values stay distinct. SQL NULL values become actual JSON-null operands. OBJECT retains odd-arity rejection and each key's coercion/NULL error before that pair's value conversion. The frontend uses ordered vectors, never a final array/object or duplicate-key map. Workers construct values and resolve last-key-wins duplicates.

## Exact result and SDK boundaries

The original native `format_json`/pretty/number/float closure moved to CPP `native_json.rs`. Empty containers, two-space pretty indentation, byte-sorted keys, serde escaping, signed zero, fixed/scientific cutoffs and integral-double `.0` remain exact. Wire MySqlFormatter and generic serde pretty-printing are intentionally not substituted.

ARRAY/OBJECT/KEYS workers return their actual computed value serialized by that shared original formatter. Native `into_json_datum` performs only the old `BinaryJSON::parse` representation boundary. None is SQL NULL; present `null` is JSON null. Invalid worker UTF8 is a ScopeContract failure; valid text rejected by the old parser retains JsonError::InvalidText. PRETTY returns actual text bytes, never a JSON-value parse.

Generic BinaryJSON parse/from_value/from_node/from_typed_value remain representation codecs, not claimed migrated encoders. Public `BinaryJSON::keys`, however, no longer owns its key-selection/sort algorithm: it invokes shared `native_json_sorted_object_keys` on its original decoded tree, then uses old encoders for computed strings. This preserves duplicate keys and nonobject `[]`, deliberately unlike SQL KEYS' nonobject NULL. Full malformed-child decoding still precedes key extraction.

Native formatter aliases also serve unclaimed JSON consumers. Foundation reuse alone does not grant those families credit.

## Context and observable compatibility

ARRAY/OBJECT/PRETTY previously reached a contextless typed-dispatch shortcut. Actual scalar evaluation now calls `dispatch_typed_in` with its real context. Test-only old-signature wrappers still execute shared workers; they are not production fallback. The cached modifier/document route is unchanged.

This intentionally activates evaluator resource enforcement for the typed routes. SQL value/coercion/error semantics are preserved, but resource refusal is now observable as intended. Both actual empty constructors and NULL paths are tested under zero-slot scopes; no dummy operands or unrelated WHERE/ORDER/CAST worker supplies the failure. The previous row-context and float_roundtrip compatibility surfaces remain disclosed.

## Validation and limits

Seven attempts, seven actual runs, no compile failures/retries/new REDs/zero-match runs. Six focused gates pass: CPP JSON6/local301+1ignored, native raw SDK20/result SDK2/context2, SQL2. Full expression remains **1511 passed,4 old failures,94 ignored; exit101**. Its complete failure section matches `json-nullsafe-55` after only panic thread IDs are normalized. Ignored tests are not passes. Unistore full was not rerun: no legacy/coprocessor route changed; its prior failure remains unresolved, not relabelled green.

EXTRACT still needs its different raw SDK cross-path dedup/range/wildcard closure. UNQUOTE needs its separate strict-text/direct-JSON/SDK-secondary-unescape and raw-display policies. Both remain uncredited.61 eligible families remain. Whole workspace/lint/dev/bazel_prepare, strictM6/release/performance/zero-copy/physical heap/peak/OOM, exhaustive domain/context/wire/150-row differential/TiFlash/FIPS and previous parser/GB/vector/extreme Decimal exceptions remain deferred. No whole Go-package transcreation, final acceptance or PR-readiness claim.
