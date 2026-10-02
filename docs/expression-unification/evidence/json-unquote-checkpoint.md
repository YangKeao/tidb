# UNQUOTE and shared raw text — json-unquote-59

Previous `json-raw-values-58`. **192/245 functional families**, strict final acceptance0; target221,29 more needed,53 eligible remain. This adds only **json_unquote**, not temporal-family credit. [Exact commands and hashes](../logs/json-unquote-summary.txt).

## Ownership and deletion

Eight exclusive writers, parent formatting/serialized integration.20 Rust files (CPP10/native10),1 new source,15 additive tests; manifests, dependencies and locks unchanged.

- B: new datatype `json/native_text.rs` and JSON exports, native `binary_json.rs` thin facades. Unique raw Display, string projection, SDK quote/unquote/unicode helpers;2 tests.
- A: CPP Time/Duration helpers and two thin native Display implementations;2 tests. Existing CPP wire Display implementations remain byte-identical.
- H: CPP expression `native_json.rs`/exports, strict SQL-text and direct-binary operations plus unit validators;2 tests.
- G: `impl_json.rs`, two fixed kernels and two shared validators;2 tests.
- C: local batch/registry and operation enum only;1 focused lifecycle test. Existing official-boundary selector needs no changes; no new packer, compile whitelist or driver.
- D: native evaluated adapter/tests/tikv bridge, two profiles and checked raw input;2 tests.
- E: native JSON construct/dispatch/tests, guarded context-last UNQUOTE and native algorithm deletion;2 tests.
- F: lifecycle tests for fixed stored-column semantics and nine direct zero-slot probes;2 tests.

Seven native private formatting/escape helpers are removed. Existing public SDK APIs remain thin delegates. Native Time/Duration Display bodies are moved once rather than copied into JSON formatting. Old binary JSON tests and the original expression JSON test prefix are exact; PB, legacy evaluator, path parser, raw mutation SDK and admission sources remain unchanged.

## Three distinct policies

**SQL text:** String/Bytes require the original UTF8 validation. Empty text, whitespace, incomplete quotes and anything not bounded at both ends by double quotes pass through exactly. Fully quote-bounded text must be a complete strict JSON string; unknown escapes, invalid surrogates, controls and multiple roots retain InvalidText3140. A private generic string parser shares the edge gate between the actual worker and a unit-returning `deserialize_string` visitor. Validation does not return an owned decoded answer and does not use permissive IgnoredAny or SDK unescaping.

**Direct typed JSON:** first use the original partial string projection and validate its projected UTF8. Return that content verbatim, with no second unescape. Trailing raw bytes outside the projection are accepted. A malformed string header giving no projection falls through to the original raw Display policy, not an invented validation error. Nonstring data is formatted by the actual worker; the frontend neither formats it nor sends a precomputed answer.

**Public SDK:** BinaryJSON::unquote retains its separate conditional second-unescape for already-decoded strings. Unknown escapes drop the slash; surrogate adjacency and replacement rules are preserved separately from the public Unicode helper. The SDK and SQL policies are not merged with ordinary TiKV unquote, whose existing implementation is unchanged.

## Raw formatting and temporal foundation

`write_native_binary_json_text` shares original branch order, opaque/base64, exact raw lengths, encoded object order/duplicates, float spelling, Go U+2028/U+2029 escaping and malformed fallback. Containers fully decode before building output; malformed/nested-nonfinite data emits empty text. A root exact-width nonfinite double instead returns fmt::Error and to_string-equivalent callers panic. These cases are deliberately different.

The partial string projector keeps the original varint/index behavior, not a whole-document validator. Raw scalar codecs and path parsing are unchanged.

`Time::write_native_core_display` uses raw fields without validation/normalization. It preserves date early return and the native first-FSP-byte slice of `{:06}` microseconds, including seven-digit raw microseconds. `Duration::write_native_display` preserves sign, raw field projection, fractional prefix slicing, negative FSP and invalid-positive-FSP panic behavior. JSON uses forced FSP6 and ignores stored duration FSP as before. CPP wire formatters retain their distinct policies.

## Runtime and lifecycle

`JsonUnquoteTextNative` and `JsonUnquoteBinaryNative` are unit Bytes1/OwnBytes profiles; NULL reuses the existing genuine JsonOutputNullNative witness. Inputs are actual text or actual raw type-plus-payload. Checked raw packing reuses the representation helper without selecting its unrelated identity operation. Outputs are plain string bytes, never into_json_datum/JSON parsing.

Native preparation remains under the existing guard: exact source errors precede worker admission; only valid actual operands reach the two profiles. Binary validation checks the partial string UTF8 policy only, not raw formatting/full decoding. Both closed boundaries use the same validators. No new result kind, carrier, driver, binding, cause, NoArgs profile, PB/legacy or ordinary admission.

**Panic correction:** source review and the real SDK test confirm that there is no production catcher in this seam. The actual formatter panic propagates; existing guards poison the scope, drop the lease and retire the worker. Subsequent use fails ScopePoisoned without another worker. It is not converted to NULL or a new error cause. Zero-slot refusal may preempt reaching the kernel panic; rejected-admission panic precedence is not claimed unchanged.

## Validation, including failures

12 Cargo attempts:1 invalid test-target selection,11 actual test runs. **8 green,3 non-green.** No compile failure or source repair; all15 added tests pass.20 scoped formatter checks and both diff checks pass.

Green: CPP datatype40 (all four new text/temporal tests), JSON18, local306+1ignored; full native datatype445 (general Display move warrants broad datatype coverage); native unquote6 (four new and two original tests); legacy raw scope1; two new SQL tests and one original SQL value-functions test. SQL has9 direct stored-column zero-slot probes, without CAST/QUOTE/EXTRACT/other workers masking the UNQUOTE admission. Fixed hex is inserted into VARCHAR then assigned to JSON before policy installation; direct BinaryLiteral→JSON is rejected by existing charset policy.

Full expression **1522/4old/94ignored** and unistore **206/1old/13ignored**, exit101. Their complete failure sections and lists match `json-raw-values-58`, normalizing only panic thread IDs.

The extra integration attempt first selected a nonexistent standalone target. `autotests=false` and `tests/all.rs` require `--test all json_introspection_and_unquote`; the corrected invocation ran **0pass/1fail/335filtered**. It fails at the first JSON_KEYS assertion, **before reaching UNQUOTE**: actual `Json(BinaryJSON { type_code: 3, ... })`, expected `s:["a", "b"]`. The test, aggregate, manifest and native JSON_KEYS path are unchanged. This is a newly exercised failing gate, not a runtime-baselined old failure; it is neither hidden nor counted as a UNQUOTE pass. No fixture or expected result was changed. Independent direct UNQUOTE SQL/unit proofs above remain valid; the aggregate test remains unresolved.

One parent guide lookup incorrectly included `src/` for repo-overview.md; glob corrected it. Source review also corrected the initial panic-catching assumption before implementation. No code change was needed for either correction.

## Limits and next work

The integration JSON_KEYS type mismatch remains visible and uncorrected. StrictM6, broader default-NoColumns request-root integration, workspace/lint/dev/bazel_prepare, release/performance/zero-copy/physical heap/peak/OOM, exhaustive context/domain/wire/differential/TiFlash/FIPS and prior parser/GB/vector/extreme Decimal exceptions remain deferred. New writer/transport allocation performance is unmeasured. Native/CPP wire formatting and SQL/SDK compatibility distinctions are intentional, not silently normalized. No whole Go-package transcreation, PR readiness or overall completion claim. Next read-only candidates are remaining simple temporal functions and ANY_VALUE; no advance credit.
