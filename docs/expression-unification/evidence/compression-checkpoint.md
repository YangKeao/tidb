# COMPRESS / UNCOMPRESS functional checkpoint

Checkpoint **compression-two-29**; exact commands, whole-log SHA256 values and source receipts are in [`../logs/compression-summary.txt`](../logs/compression-summary.txt).

Functional delegation/native-production deletion advances **91→93/245**; final acceptance remains **0/245**. Credit is only the two frozen IDs `compress` and `uncompress`, not aliases, helpers, UNCOMPRESSED_LENGTH or a completed/transcreated Go package. Read-only JSON follow-up planning earns no credit.

## Ownership and preservation

TiKV's `impl_encryption/native_go_flate.rs` uniquely owns the existing Go-compatible level-6 encoder; ordinary zlib encoding is not substituted. Its pre-format **1153-line** production copy was byte-identical to `16001161`, SHA256 `17b251625e0eac68b73637858681b9ec995bbfb0105259871750f17b592354b4`. The final module is **1152 lines**, SHA256 `15a4c146f42e4c92b6130eaa32cf44b42961a9da01eb75b22ff5751f658c767a`.

Parent corrected inherited universal claims in the header while preserving BSD attribution, the first 13 license lines and the concrete COMPRESS-helper explanation. The original const-start section has **1114 lines**, the new one **1116**: their raw hashes differ; applying the same pinned January formatter to both yields byte-identical bodies. This is not whole-file/raw-body byte identity or all-input/CPU/Go-package validation.

Parent verified native inflate's inner body and wire UNCOMPRESS's inner body byte-identical to their pinned originals; full hashes are in the summary. Native framing and wire COMPRESS differ only by two shared LE-u32-prefix/trailing-dot primitive calls, preserving their respective allocation and I/O order. The wire encoder, decoded-zero and error-priority policies are not replaced by the native policy.

Native `go_flate.rs` shrank **1177→44 lines**; its original **24-line** test block (one test, two golden vectors) and the original **758-line** crypto test block remain byte-identical after formatting. Three narrow function exports plus `InflateError` support native cfg(test) bridges only, not a production bypass. UNCOMPRESSED_LENGTH and all subsequent source are unchanged. Parent pinned-formatted all **14 live Rust files** (native six/TiKV eight); both repository diff checks and both lockfile diff checks exited0.

## Closed protocol and distinct policies

Exact private operations/getters are `CompressGoNative`/`compress_go_native_fn_meta` and `UncompressNative`/`uncompress_native_fn_meta`, using existing nullable Bytes arguments. COMPRESS retains the owned-byte result path; UNCOMPRESS has `ComputedUncompress::into_outcome`, four-state `UncompressOutcome` and `OwnUncompress` metadata. No new argument role, cause, driver, general context getter, fourth internal column or PB/legacy admission is added.
- Canonical nullable-byte transport: `None` is SQL NULL; `Some([0] + actual decoded bytes)` is Value, including empty; `Some([1])` is Corrupt; `Some([2])` is OutputLimit. Both status forms require exactly one byte; other encodings are rejected.
- Only backend checks/inflation produce the disposition. Native inflation retains its 8-KiB scratch buffer, remaining+1 bound, StreamEnd/checksum/progress rules and final declared-length comparison. Successfully decoded empty output is Value; data after the first complete stream is ignored. NULL/empty inputs also execute the real wrappers.
- Native code coerces through the real context, invokes C4, then packs the computed result/warnings **1259/1258**; it neither decodes again nor guesses from input. No warning is emitted into the sealed worker context. Resource/transport failures are not zlib warnings; envelope `try_reserve` preserves `other_err!`, not a claim that every allocation failure is `PoolResource`.
- C copies only `source[1..]` for Value. Its status decoder allocates no payload and retains zero bytes; that is not a zero-allocation claim for the whole invocation. Encoded-tag physical budgeting and separate payload-length/actual-capacity checks do not bound the encoder/inflater's internal allocation peak or establish zero-copy/OOM safety.

## Seven retained Rust receipts

This writer globbed, grepped and read the seven completed raw logs and hashed them with `sha256sum` (exit0), without rerunning tests. Commands, exits, source proofs and coverage details are parent-owned. All seven Rust runs were first attempts: six scoped green, one retained non-green full run; no compile failure, test retry, old-expected change or new-oracle correction. Compilation times below are emitted `Finished` times, not performance measurements.

| `compression-` log suffix | Actual result; elapsed; compilation | Exit |
| --- | --- | --- |
| `tikv-local.log` | Jan: 251 discovered, 250 passed, one old ignored, 479 filtered; 0.19s; 9.36s | 0 |
| `tikv-encryption.log` | Jan: 10 passed, 720 filtered (existing wire tests + two new units); 0.00s; 0.12s | 0 |
| `go-flate.log` | Aug: original one test passed, 1537 filtered; 0.00s; 12.47s | 0 |
| `native-crypto.log` | Aug: original 15 passed, 1523 filtered; 0.01s; 0.12s | 0 |
| `dispatch.log` | Aug: three passed, 1535 filtered; 0.00s; 0.12s | 0 |
| `sql.log` | Aug: 61 passed, 2078 filtered; 0.85s; 24.12s | 0 |
| `expr-full.log` | Aug: 1538 discovered, 1440 passed, **four failed**, 94 ignored; 10.58s; 0.12s | 101 |

The two new kernel units use partly correlated generated streams. Two factory units cover canonical transport, eight actual owned-result calls and wrong-role/capacity rejection. Three dispatcher tests exercise real C4, two-context isolation, NULL/empty workers and sentinel coercion rejection before admission. These are representative checks, not full-domain or independent encoder-oracle coverage for every input.

SQL has **seven stored fixture rows**. The normal matrix is four rows × three HEX projections; a separate unwrapped one-row × two-column projection checks metadata (`VarString` flen29 / `LongBlob` flen16777216, both binary). It checks the complete existing Go `hello` golden and independently supplied Go/MySQL frames. Raw `00FF20` checks only prefix/suffix and round-trip: **correlated, not a full golden**. Three separate diagnostic queries cover junk→1259, declared length2→1258 and a one-byte-short checksum→1259; five fixture values feed eight direct zero-slot probes, all1105/no warnings. Row/probe counts are not additional Rust runs.

## Retained failures, history and limits

Full-expression failures remain `pushdown_catalog::tests::ifnull_string_column_literal_uses_go_signature_and_column_collation`, `tests::builtin_info_json_math_source::exp`, `tests::builtin_math_misc_op_source::vectorized_builtin_op_func` and `time_fn::tests::str_to_date_partial_formats_follow_no_zero_date`. The old `EXP(100_000.0)` FloatOverflow assertion still fails; no old expected value was changed or failure rewritten green.

Parent freshly compared the complete failure section against round28 `exp-log-expr-full.log`, normalizing only numeric thread IDs in panic headings. Both normalized sections have SHA256 `0930217d98e0b92d727527dc3c7cb7313f1bbe35e643da60114fa6d78203839b`; the exact extraction/regex is in the summary. This is section-scoped evidence, not a whole-log hash or a green full-suite claim.

Before source freeze, textual checks caught the encoder table96 transcription error and nonverbatim inflate reconstruction; both were restored to the original. The framing verifier's predicate normalization was also corrected. These failed pre-freeze text checks are disclosed separately from the seven first-attempt Rust runs; they were not compilation failures, test retries or oracle changes.

Only this document and its new summary were written under this authorization; source/old expected/other docs/Plan/JSON/index/build/test/fmt remained untouched. Exhaustive inputs/DEFLATE variants, compressed-input OOM safety, internal allocation peaks, zero-copy/performance, whole-package/workspace completion, release execution, make lint and wider guards remain unverified. Earlier non-green observations remain historical. The pair is frozen at **93/245**, final **0/245**, not PR readiness.
