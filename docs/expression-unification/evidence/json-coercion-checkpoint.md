# Shared JSON expression value-boundary coercion

**json-coercion-116 / R119**, following [datatype parsing](native-json-parse-checkpoint.md). Functional **238/245**, strict **0**, remaining **7** stay unchanged. This closes one expression coercion module, not whole JSON/CAST/M2, new-family or Go-package acceptance.

## Ownership

SDK `native_json_coercion.rs` owns all eleven former `builtin_ext/json/value.rs` entry policies: ordinary/typed/value CAST; JSON argument conversion; SQL/document string views; document value/text/strict helpers; binary JSON result construction; and strict expression parsing. Its five private selectors own boolean, binary-opaque, typed-scalar and fallback selection. Native keeps original entry visibility/signatures with only field, storage and error adapters.

`Datum::as_shared_json_input` is a public borrowed-view wrapper over the existing private-to-parent19-kind SQL-string projection. That projection and its old consumers are unchanged; it does not parse, validate or choose a JSON policy. Source metadata carries the actual effective code in two existing views, collation, flen and raw u64 flags. Known245 identifies JSON evaluation, while Unknown245 does not; array metadata already exposes effective JSON code. There are no synthetic carriers, origin tags or host policy/formatter/parser callbacks.

## Preserved distinctions

- CAST NULL returns SQL NULL; argument NULL becomes JSON null; document helpers return None. Typed CAST checks binary source, then boolean Int/UInt, then typed scalar policy after its NULL guard.
- Only String/Bytes plus the real binary-collation predicate enter the expression opaque arm. This remains narrower than datatype conversion's unconditional Bytes rule. BinaryLiteral becomes opaque253; direct Bit retains fallback semantics.
- Source boolean flags affect Int/UInt only; untyped callers do not guess them. UInt uses the original wrapping i64 projection before its nonzero test.
- Document versus value strings remain distinct, with effective JSON source metadata forcing document parsing. Expression parsing stays strict; datatype surrogate repair is not substituted here.
- Real nonfinite argument conversion reports FloatOverflow; Float32 fallback and typed numeric CAST report the original Unsupported conversion error. Raw Float32 storage is not newly narrowed. Decimal arguments parse visible decimal text, while typed CAST uses the datatype DOUBLE conversion.
- Time uses the existing `set_fsp(6)`: DATE is a no-op even for raw FSP metadata, and calendar bits do not change. Duration's original constructor only validates/normalizes FSP; nanoseconds are not rounded, range-checked or clamped.
- JSON CAST can raw-clone; document/argument routes retain Display then strict parse. The binary-result helper still formats then calls the lenient datatype parser and folds all its errors to InvalidText; it is not replaced with direct from_value.
- Numeric document admission is exactly Int/UInt/Decimal/Real, not Float32. Strict admission retains3146 argument/function payloads and the separate unsupported messages.

Typed row/batch preparation and static Vector refusal in `scalar_function.rs`, generic CAST dispatch, field finishing, C4 gateway admission and the frozen AST NOT IN lock are unchanged. In particular, stored BIT and FLOAT preparation occurs before these controllers; direct Bit/Float32 unit domains are not interchangeable with stored SQL values.

## Validation

[Nine focused gates](../logs/json-coercion-summary.txt) passed on first attempt: SDK policy3, full native datatype468, JSON45, original CAST16 and actual-column1, plus new SQL1 and original R117/R118/typed-CAST SQL1 each. Six new tests (SDK3/native3);242 old native test bodies remain byte-identical, with no old tests in changed SDK files. The native regression directly covers11/11 entry wrappers. New SQL has10 successful SELECTs across two vector modes and38 cells:32 JSON,2 SQL NULL and4 typed-IN integers.

A source-only preflight clarified new expectations before execution: floating JSON formatting retains `.0`, and rounded Decimal document conversion remains a floating JSON number. No target-output recording or failed/retry run was involved. Existing malformed-array display yields empty text, while nonfinite root display retains its original panic; these are preserved, not repaired. New tests use fixed source-derived bytes, values and errors, never output from the target parser/constructor as expectations. Original tests/fixtures remain immutable. No new C4 profile, carrier, performance, zero-copy or physical-allocation claim follows.

## Remaining

Other CAST and typed caller-preparation domains, broader M2, six complex candidates, request-root/default-NoColumns/liveDAG and final acceptance remain. Full expression/unistore/workspace/lint/dev/bazel/release/exhaustive/performance/physical memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/whole-Go-package/PR readiness are unverified. Historical R100 expression4/unistore1 failures remain unrepaired. Overall goal stays active.
