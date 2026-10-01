# Comparison datatype substrate — compare-substrate-52

Previous checkpoint: `aes-two-51`. Functional evaluator count **166/245**, strict final acceptance **0**, required **221**: all unchanged. This is a bottom-up shared-type step, **not six comparison-family completion**, PR readiness, or complete Go-package transcreation. Paired TiKV SHA and Plan hash are in `../checkpoint.json`.

## Scope and review map

Three exclusive parallel owners implemented the locked substrate after read-only comparison entry-point work. The parent alone formatted, ran serialized gates, maintained guides/Plan and published. No expression runtime code changed.

| Owner | TiKV implementation | Native forwarding |
|---|---|---|
| JSON | `components/tidb_query_datatype/src/codec/mysql/json/native_policy.rs`; exports in `json/mod.rs` | `rust/crates/tidb-datatype/src/binary_json.rs` |
| Calendar | `components/tidb_query_datatype/src/codec/mysql/time/mod.rs` | `rust/crates/tidb-datatype/src/core_time.rs` |
| Decimal | `components/tidb_query_datatype/src/codec/mysql/decimal.rs`; exports in `mysql/mod.rs` | `rust/crates/tidb-datatype/src/decimal/mod.rs` |

Eight existing Rust files, no new source file/dependency/manifest/lock. Four additive shared test functions; no native test changes. Architecture index and coprocessor maintenance guide describe the new ownership, not new repository policy.

## Native raw JSON closure

Shared APIs:

```text
compare_native_binary_json(u8, &[u8], u8, &[u8]) -> Ordering
 decode_native_binary_json_node(u8, &[u8])
    -> Result<NativeJsonNode<(u8, Vec<u8>)>, NativeBinaryJsonError>
 decode_native_binary_json_value(u8, &[u8])
    -> Result<serde_json::Value, NativeBinaryJsonError>
```

`NativeBinaryJsonError` has only `InvalidBinary` and `TooDeep`. Existing `NativeJsonError` and its consumers are unchanged. `NativeJsonNode<T>` is a data-only scalar/array/object tree, not a callback, executable description, or runtime metadata schema. Native `JSONNode` aliases it with `BinaryJSON` scalars. Native `to_node` structurally maps owned scalar tuples; `to_value` maps the two errors; `compare_binary_json` directly delegates. The original native comparison and decoder algorithms are deleted rather than retained as fallback.

Preserved policies:

- Rank before payload decoding. Invalid equal-rank opaque/temporal/container values retain Equal fallback; ordinary scalar decode failure retains raw-byte ordering.
- Object entries remain a vector, including duplicate keys and encoded order. Comparison checks member count first, then byte-sorts key references and recursively compares values. This is not wire object raw-byte ordering.
- Double/double comparison is exact; mixed double/integer uses the original `1e-8` epsilon. Signed/unsigned number handling remains unchanged. Non-finite serde rejection retains original raw fallback.
- Opaque framing/type precedence remains native; payload comparison does not invent a field-type tie breaker.
- Temporal payloads require exactly eight bytes and use the same shared calendar-core leaf. Duration requires twelve bytes and compares signed nanoseconds, ignoring FSP as before.
- Lossless node decoding still validates each entry at depth zero before outer-depth traversal, and preserves checked slicing. The separate serde entry path retains its original unchecked payload slice behavior. The migration does not silently fix or merge these malformed-input policies; an additive test catches the preserved serde panic.
- Mixed successfully decoded node kinds can compare ranks without re-encoding: original decoding already establishes key-length/depth admission, and different kinds have different ranks. This removes redundant encoding solely for classification, not encoding behavior itself.

Native encoding, rendering, parsing, public temporal accessors and JSON operations remain where they were. Their decoders now delegate to shared code. Wire `JsonRef` comparison/decoding is not replaced. Heap peak, OOM, zero-copy and arbitrary adversarial-decoder performance are not claimed.

## Calendar ordering

`Time::native_core_compare(left: u64, right: u64) -> Ordering` constructs private shared Time values solely to reuse the unchanged `Time::Ord`. That implementation clears the low four FSP/type bits and compares unsigned raw values. Native year/month/day/hour/minute/second fields preserve decimal calendar lexicographic order because each lower calendar field is below100, including invalid bit-width-admitted components; microseconds follow them in both representations.

No packed-wire conversion, parser, calendar validation, timezone or FSP admission is added. Native `CoreTime::compare` is a single call. `datetime_to_u64` remains for its independent numeric-conversion consumer. JSON temporal comparison uses this same leaf. The additive test fixes expected order for invalid components, maximum microseconds, high unsigned bits and all low-nibble combinations; it does not ask a second provider for expected results.

## Borrowed Decimal ordering

`NativeDecimalCmpParts` borrows `negative`, `digits`, and `storage_scale`. `native_decimal_cmp` owns the exact former native sign-first and coefficient-magnitude implementation. Native Ord constructs two views and delegates; its former helper is removed.

It keeps original `min(storage_scale, digits.len())` splitting, leading/trailing zero treatment, hidden fractional digits and raw sign-first negative-zero behavior. It adds no validation, result/error, budget, allocation or shape cap. In particular, an allocating/fallible `try_to_shared_math(...).expect(...)` is not substituted into infallible Ord. Word-backed shared Decimal Ord remains unchanged: this helper is a native coefficient-storage policy, not a claim that wire words and character coefficients are identical representations. Fixed tests cover wide coefficients, hidden scale and source clamp boundaries.

## Validation and preserved evidence

Exact ten Cargo commands, counts, elapsed times and whole-log SHA256 values are in [the receipt summary](../logs/compare-substrate-summary.txt). All use pinned wrappers, `--locked`, `--lib` and one test thread.

- Shared JSON37, Decimal89, Time55 pass.
- Native binary JSON32, Decimal25, core time15 pass.
- Existing expression comparisons67 pass/5 ignored; SQL comparisons19 pass.
- Full expression1494 pass/4 old failures/94 ignored; full unistore201 pass/1 old failure/13 ignored. Both exit101 and are **not green**.
- Complete failure sections match `aes-two-51` after numeric panic-heading thread IDs only; no source-address substitution. Hashes remain `27654f2c…` and `b64dcced…`.
- Six original test suffixes compare byte-for-byte against starting HEAD. Shared Time removes only its new test block for this comparison. The exact suffix boundaries/hashes are in the summary; all original expectations remain unchanged. Wire Time Ord was separately byte-compared.
- Scoped pinned formatter checks and both repository diff checks pass. No fixture recording, new failing test, compile failure, Cargo retry or zero-match gate.

Three non-Cargo mistakes were corrected without code changes: wrong maintenance-guide path, a proof script assuming all original test modules were named `tests`, and `git status` run at the non-repository workspace root. Their precise scope is recorded separately from Cargo validation. Review of `docs/agents/architecture-index.md` against `agents-review-guide.md` found no new normative policy or changed build/test/PR rules; touched source paths exist and no path is deleted/renamed.

## Next integration boundary — not implemented here

Six comparison predicates still require actual shared evaluator bool production across all supported native domains. Numeric-only dispatch does not earn six-family credit. The locked map includes signedness profiles, full-width legacy i128, native IEEE unordered versus legacy `total_cmp`, Decimal, collated bytes, vector, raw calendar cores, duration and raw JSON. JSON transport can carry two actual `[type_code || payload]` byte envelopes, never canonical text or a host-computed comparison answer.

A finite six-predicate enum in each explicit domain/profile identity is viable: existing native pool/affine/factory equality compares complete operation values, not discriminants alone. It still needs six real fixed metadata functions and cache-switch tests, and is not implemented by this checkpoint.

Required seams: native temporal preparation without precomputed Ordering; typed integer and generic paths; numeric comparison batch with real context; existing PB EqInt/GtInt left-NULL demand; all30 existing legacy signatures with their original error/NULL demand; context-aware row facades. Legacy JSON/vector admission must not be invented. NullEq and planner refinement are distinct scopes. IntDIV remains a later independent family with warning-before-integer-conversion constraints.

Broader request-root integration, release/performance/zero-copy, physical heap/peak/OOM/M6, 150-row differential, TiFlash, FIPS, complete type/package equivalence, whole workspace, `make lint`, `make dev`, and `make bazel_prepare` were not validated here. Existing parser, GB/ILIKE, ignored-vector and extreme Decimal shape exceptions remain recorded. There are79 eligible families left and55 more needed to reach221.
