# Charset runtime conversion through three shared workers

**convert-charset-93**, following **charset-codec-92**. Functional229/245, strict0, remaining16. All228 prior family objects are byte-identical; only `convert_charset` is added. Previous partial CAST/type records remain, with encoding-name lookup appended as a prerequisite—not extra family credit.

## Deleted native business logic

`convert_charset.rs` no longer implements encode/decode/is_valid/replace or result-domain selection. `tikv/convert_charset.rs` prepares actual operands and projects SDK reports. Ordinary AST, typed, direct public helpers, LENGTH's special path and general binary-aware wrappers pass their real context. Legacy NoColumns public facades remain available; metadata passthrough still clones without invoking a fake worker. Encoding name/lookup bodies now delegate to SDK metadata helpers.

ToBinaryNative/FromBinaryNative use Bytes2; ConvertUsingNative uses a necessary closed Bytes4. Exact source spelling and effective field charset are different facts: raw `GBK` with effective `gbk` encodes to binary by unknown-name identity fallback, while unknown raw spelling with effective binary must decode character targets. Packing those into a hidden flag or canonicalizing the raw name would change behavior.

Only this profile gets the four-argument compile whitelist. Generic1..3 remains, existing four-ScalarValue storage is reused, and the native factory's exact5nodes were corrected before gates. No native PB/legacy or explicit to/from-binary SQL admission is added. Existing TiKV wire Encoding rules remain separate.

## Result and demand contract

- Shared producers reuse the R95 generic encoding policy, preserving ASCII/UTF8/GB grouping and Latin1 arbitrary-byte identity.
- Report0 is Bytes,1 retag payload,2 invalid-character and3 unknown-charset; errors have exact one-byte width. Outer None is legal only for Convert's computed decode failure. To/From reject None/retag/unknown reports as contract violations.
- Direct helper NULL stays an actual nullable input and computes empty bytes or empty retag String. Caller-observed NULL uses the existing genuine NULL witness before target/type/getter demand. By-collation passthrough NULL does not become a conversion call.
- Target support validation invokes the shared metadata service before ETString extraction. AST retains lowercase/literal-source rules; typed evaluation retains lossy target spelling without folding, casts and connection-getter timing.
- SDK selects retag and supplies the resulting bytes; only then does the frontend project original target default collation. GB defaults read the existing global collation mode, so they are neither eagerly captured nor hardcoded.
- Targetbinary previously extracted the same ETString bytes twice. That operation only clones supported datum bytes, without getters, warnings or context; one actual extraction is reused. This is not a performance claim.

Checked `8*n+1` conservatively precharges retained reply space before the shared producer. Fresh exact-reserved reports are checked against actual capacity, and all actual data/name capacities count even when their values are not used by the selected branch. Tests refuse128KiB spare capacities against64KiB caps before invocation and then reuse the worker. Codec temporary allocation/physical peak/OOM remain unverified; no replacement collector or general driver was introduced.

## Core evidence

[Exact commands and hashes](../logs/convert-charset-summary.txt); [manifest](../checkpoint.json).

Six new tests pass on first actual execution. Thirteen locked single-threaded Cargo launches include four compile RED attempts: two commands first lacked EvaluableRef/EvaluableRet imports, then the expanded macro required ChunkedVec. All raw logs/hashes remain; only imports changed, not expected values or algorithms. Nine nonzero test runs give seven green and two only-old full REDs. The initial pinned formatter needed a second pass before Cargo; no zero-match, interruption, fixture recording, oracle correction or new execution failure.

CPP core1/local347+1ignored; native encoding22/root2/gateway196+1ignored/newSQL1/originalSQL7 pass. Full expression1604/4old/94ignored and unistore220/1old/13ignored remain RED. Whole failure sections match R95 after only numeric panic-heading thread IDs are normalized: `411274feba11e202735df5a8056c72babbc8cf9df2eab3258c670e9289a1ee95` / `2f19c9ad5338a86c48b895e3923c3e39454117e3410d47df5aa3fb4254ed7ce1`.

New SQL34SELECT=8storedcases×2vector×2pool=32direct+2positive filters. Fourteen zero-slot probes isolate new ConvertUsing, including nonNULL bad bytes that compute NULL. Two actual caller-NULL refusals are old-witness evidence, not new selector proof. Stored String/Bytes casts clone without an earlier worker. VarString(-1,-1), actual String materialization and target collations are pinned. Positive stored-GBK HEX/filter queries exercise implicit ToBinary; their composite path is not substituted for isolated refusal proof. Seven original charset tests/20SELECTs also pass. FromBinary's internal helper domain is directly tested, not invented as explicit SQL/PB coverage.

Scope:9CPP/10native Rust files,2new modules,3new tests per repository.145CPP/425native original test bodies are byte-identical. Pinned formatting/diff checks and independent E source review pass. Agent-doc additions are descriptive/path-checked; no Go/Bazel/Cargo/lock/generated changes. Unrelated BUILD excluded.

## Remaining work

[Remaining review](remaining-acceptance.md) keeps5core,5ordinary and6complex candidates open, plus real request-root/default-NoColumns/live-DAG and final cross-entry acceptance. Charset/FieldType metadata/case adapters and late retag projection are explicit frontend boundaries, not a whole-M0 completion claim.

Known CAST/Decimal/mode/INTDIV/JSON/vector/older Values gaps and full-suite failures remain. Workspace/lint/dev/bazel_prepare/release, exhaustive differential/TiFlash/FIPS, performance/physical memory/OOM/allocator/zero-copy/dual-tzdata, complete Go-package transcreation and PR-readiness are not claimed.
