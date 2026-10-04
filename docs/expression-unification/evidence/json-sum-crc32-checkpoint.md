# JSON_SUM_CRC32: existing implemented-domain closure

**json-sum-crc32-96**, after **str-to-date-runtime-95**. Functional231/245 over frozen implemented domains, strict0, remaining14. Only `json_sum_crc32` is added; all230 prior family objects and old partial/type records remain byte-identical, with one formatter dedup record appended.

## Scope, not invented SQL compatibility

Plan's denominator is the Rust implementation at the frozen baseline. This family implements homogeneous string/number scalar arrays through helper, manually constructed AST FuncCall and manually constructed ScalarFunction entries. The same existing domains now execute the SDK; no native checksum fallback remains.

Normal SQL accepts only JSON_SUM_CRC32's `expr AS type ARRAY` syntax, then AST/typed rewriting rejects before the child. Target-type width/signedness/charset/path conversions are absent; input JSON or result BIGINT metadata cannot substitute for that target. No registry, PB, legacy or ARRAY admission is added. **New SQL checksum-success probes:0.** This is not full Go SQL compatibility or package-transcreation completion.

## Shared ownership

Native `builtin_ext/json/report.rs` is a thin bridge; its105-line array classification/number formatting/CRC/sum implementation is removed. The existing single-argument dispatch forwards actual context; other arities still miss the dispatcher. `tikv/json_sum_crc32.rs` keeps original guarded `parse_json_document_argument` preparation and projects three original Unsupported messages.

Actual SQL NULL becomes nullable Bytes(None), while JSON null remains a present document and computes RequiresArray. DatumJson still uses its original Display→shared parse boundary. Unsupported Datum kinds and UTF8/JSON errors retain their original order. Children remain eager, including unsupported arity; no mode/timezone/truncate/warning getters are added.

One `JsonSumCrc32SerdeNative` Values1/nullableBytes/OwnBytes/one-call profile consumes the whole real prepared serde document. Structural validation does not reject semantic root/member/homogeneity cases before the worker. SDK preserves ordered first errors, i64/u64/f64 number priority, IEEE CRC and wrapping i64 accumulation.

Datatype `Decimal::native_format_json_sum_float` keeps finite Rust Display digits, both zeros→0 and original abs[1e-4,1e6) branch. Outside that interval, original significant digits/exponent use the existing Go-g layout; Ryu/LowerExp policies are not substituted. CRC reuses `file_system::calc_crc32_bytes`, removing the second bit loop.

Reports are tag0+LEi64 exactly9B or error tags1/2/3 exactly1B; output presence matches input presence. Fixed9/actualNULL0 is precharged before producers with all actual capacities and fresh report capacity checked. No new carrier, compile/factory limit or driver. Serde/format temporaries, physical heap/peak/OOM and allocator behavior are not certified.

## Validation and retained failures

[Commands/hashes](../logs/json-sum-crc32-summary.txt), [manifest](../checkpoint.json).

Eight locked single-threaded launches: six final green gates, one compile failure with no tests, one new-test runtime failure. CPP datatype1/core1/local350+1ignored; native checksum4 (two original vectors plus two new tests)/gateway196+1ignored; SQL refusal1 pass. Six new tests finally pass, not all first-run green.

- E0308: new test used usize for ParamMarker.order:i64. Added checked conversion only in that test.
- New test's assert_eq left temporary retained an AsciiScope lease while the right operand requested a second scope from a one-slot pool. Original `AsciiScope::Drop` owns release. Storing the left result in a separate statement fixes the test lifetime, without increasing slots or changing values/production.
- A new CPP test enum typo was corrected statically before its first build, not counted as a failed launch.

All failure logs remain. Original tests/oracles/fixtures were not modified; no provider outputs recorded as expected values. Full expression/unistore not rerun: R98's4+1 failures remain historical evidence only.

Nine isolated new-root zero-slot probes cover actualNULL/[]/JSONnull across helper/manualAST/manualtyped. Source vectors, integer1e6 versus float1e6, ±zero, Unicode/BinaryJSON, rejected kinds, error ordering and eager-child demand are pinned. CPP max_steps0 (not frontendpool0), malformed transport/role,128Ki actual capacity versus64Ki cap and healthy reuse are separate proofs.

Four SQL SELECTs cover legal ARRAY syntax ×2vector ×pool1/0, retaining exact typed Unsupported before the invalid-DATETIME child. No warnings or pool admission are substituted for checksum success. Driver `from_evaluation` is a mechanical diagnostic marker here, not proof the worker ran.

Eight CPP/seven native Rust files, two new modules, three new tests per side.233CPP/374native original test bodies unchanged. Pinned formatting/diff/receipt checks pass; E's independent source review found no blocker. Guides descriptive; no Cargo/lock/Go/Bazel changes, unrelated BUILD excluded.

## Remaining work

[Acceptance review](remaining-acceptance.md):5core,3ordinary and6complex candidates, plus request-root/default-NoColumns/liveDAG/final acceptance. The baseline-unimplemented SQL ARRAY-target gap remains explicit rather than being counted as migrated behavior.

Workspace/lint/dev/bazel/release/exhaustive/TiFlash/FIPS/performance/physicalheap/OOM/allocator/dual-tzdata/complete Go-package/PR-readiness remain unverified. Overall goal active.
