# Legacy raw JSON value closure — json-raw-values-58

Previous `json-paths-57`. **191/245 functional families**, strict final acceptance0; target221,30 more required. REPLACE and ARRAY_APPEND now earn whole-family credit: their native AST/SQL/PB paths were completed previously, and this checkpoint closes their actual legacy evaluators. UNQUOTE remains uncredited. [Exact commands and hashes](../logs/json-raw-values-summary.txt).

## Review map

Seven exclusive writers, parent integration/serialized gates.18 Rust sources (CPP11/native7),3 new files,11 additive tests; no dependency, manifest or lock changes.

| Owner | Scope |
|---|---|
| B | New CPP datatype `native_codec.rs`, JSON exports, native `binary_json.rs`: unique raw encoder,2 tests |
| H | New CPP expression `native_json_legacy.rs` and exports: exact staged legacy algorithms,2 tests |
| G | CPP `impl_json.rs`:4 unit byte-result kernels and3 framing validators,2 tests |
| C | CPP local/type files: checked raw AST/value packing, fixed profiles,2 tests; `compile.rs` only extends2 existing NoArgs allowlists |
| D | Native evaluated adapter/tests/tikv exports/root exports:3 narrow public SDKs, raw output projection,2 tests |
| A | `cophandler.rs`: replace only the two legacy business arms and declare F's test module |
| F | New `cophandler/json_raw_worker_tests.rs`:1 bounded raw-value, presence, scope and child-demand test |

## Shared raw codec, not wire or serde substitution

`encode_native_binary_json_node` owns the old raw node/array/object encoder, with a noncapturing scalar representation projection called only at a scalar after the original depth check. Native BinaryJSON uses its original fields; the CPP worker uses a CPP tuple projection. This function pointer is not transported, stored in RPN metadata, or used as an evaluator binding.

Native raw encoder bodies are deleted. The shared encoder retains depth100, child-first encoding, unvalidated scalar tag/payload cloning, literal inlining, bytewise unstable object sorting/duplicates, key-u16 and offset/header-u32 checks and their original error order. Shared checked header writing also serves the original serde codec through a thin bridge. The serde array/object conversion loops retain their different conversion/error order; they are not routed through from_node.

Wire builders differ in sorting, narrowing, empty-literal behavior and raw-tag admission and are not substitutes. Existing shared predicate normalization is a representability helper, not a second byte encoder, and remains unchanged.

## Fixed raw-value protocol

Four unit OwnBytes profiles:

- `JsonReplaceRawLegacy`: Bytes3 actual document, ordered parsed paths and ordered raw values; zero pairs still decode/re-encode.
- `JsonArrayAppendRawLegacy`: Bytes3 exactly one actual path/value pair; one worker completes before the next pair is demanded.
- `JsonArrayAppendEmptyLegacy`: Bytes1 original raw document identity, including malformed payloads.
- `JsonValueAbsentLegacy`: existing NoArgs transport for **genuine observed legacy no-value**, not a fabricated SQL NULL witness.

The fourth recipe is necessary because legacy eval_json(None) also represents missing/non-JSON/preparation-failed input. It is not honest to call all of these SQL NULL. No new driver, result kind, carrier, binding, cause or ordinary/wire admission is introduced.

Raw scalar data is type byte plus the entire payload. Paths retain actual shared raw legs and their original multiple-selection flag: u64LE count; each flag byte and u64LE leg count; tags0 Key(length/UTF8),1 ArrayAsterisk,2 ArrayIndex(i64LE),3 ArrayRange(two i64LE),4 DoubleAsterisk. Values are a counted list of length-prefixed raw scalar packets. These are selector operands, not action instructions. In particular quoted Key("*") may have a false flag; it must not be stringified/reparsed or converted to serde paths.

Checked packing validates counts, extents, reservations and actual iterator consumption. Shared validators check framing, UTF8 keys, complete consumption and fixed pair counts only. They deliberately do not predecode raw data or reject wildcard/root business cases: those produce the original worker None/identity results, not infrastructure errors.

Actual computed output is type byte plus raw payload. The private native projection only checks output kind/nonempty framing and transfers bytes into BinaryJSON::from_encoded_parts. No serde, formatter, JSON parse or semantic validation is inserted; malformed identity and opaque/time payloads survive.

## Legacy preparation and codec stages

Original legacy input preparation remains in its established by-value SDK convention, preserving Sql/Infrastructure/InvalidResult errors and demand without introducing a generic error callback or a new root driver. Checked transport and evaluator invocation are guarded. Every real early no-value observation goes through the absent worker. Preparation errors escape unchanged; they are not disguised as NULL or placeholder EvalError values. SDKs use the existing raw_columns context, while test-only shared_override remains child-only.

REPLACE evaluates its document, then prepares every complete path/value pair in source order. Invalid path spelling stops before that value; an already parsed wildcard still demands its value and all later pairs before business rejection. Dangling final children are ignored. The worker then performs length/document decode → each path's original flag check → replacement decode even for no-op targets → shared replacement → raw encoding. Zero pairs are not an identity shortcut.

APPEND prepares the current path and value before business checks, then enters one worker. Multiple-selection rejection or selected nonarray returns None and suppresses later pairs. Any extraction decode/selected-output-encode failure, or missing target, instead returns **the original raw document** and continues. These cases must not be conflated.

For a selected array, the worker preserves element_count full decode, each array_get full decode/child encode, typed-array child/value decoding and array encoding, followed by the original document's SET decode/flag/value-decode/mutation/encode stages. It does not flatten these into one tree update. Missing/nonarray branches retain their original skipped raw replacement decoding. Literal NULL value still constructs JSON null; computed/column SQL NULL is observed no-value. Native scalar-wrapping APPEND remains distinct from legacy nonarray-NULL behavior.

## Validation and remaining work

12 actual Cargo runs:10 focused green,2 known-baseline full failures. No compile failure, new test RED, retry, zero-match, metadata/lock-resolution attempt or formatter/static-proof failure.

Core gates: native binary JSON35; CPP codec2/JSON14/local305+1ignored; native raw SDK2; new legacy scope/demand1 and old legacy1; existing native cache2/PB1/SQL2. R58 SQL tests retain30 direct zero-slot cases. The new legacy test separates plain-leaf root-zero-slot eval_json/eval_expr/folded_int checks from genuine shared-child demand probes, so another worker cannot masquerade as root admission proof.

Full expression **1518/4old/94ignored**, full unistore **206/1old/13ignored**, both exit101. Complete failure sections match `json-paths-57` after only thread-ID normalization. All18 formatter checks and both diff checks pass. Prior189 family objects are exact. Native seven-path evaluator/PB sources and raw path parser/SDK policies are unchanged; cophandler outside the two arms/test declaration is unchanged, as are the original serde conversion/error-order bodies. No old oracle or fixture was changed.

54 eligible families remain. UNQUOTE requires distinct strict SQL-text, direct BinaryJSON-content and SDK conditional second-unescape policies plus the exact raw Display closure; no native PB/legacy admission is added. StrictM6, broader default-NoColumns request-root integration, workspace/lint/dev/bazel_prepare, release/performance/zero-copy/physical heap/peak/OOM, exhaustive context/domain/wire/differential/TiFlash/FIPS and prior parser/GB/vector/extreme Decimal exceptions remain deferred. Function-pointer/transport allocation performance is unmeasured. No whole Go-package transcreation, PR readiness or overall completion claim.
