# Expression unification experiment

Paired branch: `expression-unification-demo` in YangKeao/tidb and YangKeao/tikv.
Current checkpoint: **json-source-117**, after **json-coercion-116**.

Functional **238/245 (97.14%)**, strict **0**, remaining **7**—unchanged. This is partial CAST/M2 ownership, not whole JSON/CAST/M2, Go-package or new-family acceptance. Goal remains active.

## Shared ownership

- `native_eval_type.rs` owns the nine native EvalType variants, methods/constants/traits and effective-code/raw-flag classifier. Native uses aliases, not a duplicate table or wire EvalType.
- `native_json_coercion.rs` owns JSON source admission and post-evaluation preparation: unsigned/Year, hybrid names and document/value mode.
- `native_mysql_json.rs` owns datatype JSON-target selection, distinct from ordinary conversion and expression coercion.

Row refusal stays before child evaluation. The complete typed source batch stays before conversion. Generic child evaluation, target/arity routing and PB's separate NULL/arity behavior remain native.

## Verification and incidents

[Evidence](evidence/json-source-checkpoint.md), [exact commands/counts/hashes](logs/json-source-summary.txt), [manifest](checkpoint.json), [ledger](migration-progress.json).

**14 launches:12 matched GREEN,1 matched RED,1 zero-match.** Final required filters are green: SDK classifier1/target2/source5; native datatype470/source1/JSON45/PB2; four SQL gates. Eight new tests;4 old SDK and286 old native test bodies unchanged.

A supplemental assertion caught changed Debug text after relocating the invalid-EvalType error. The real RED is retained; preserving its original struct name with an SDK alias restored the text and passed. An incorrect PB module filter selected zero tests; it was not counted as coverage, and the corrected filter passed2 tests.

New SQL covers8 SELECTs/24 successful cells, including YEAR/unsigned identities, ENUM/SET names, document versus value mode and two strict CAST errors. No output-recorded oracle or new performance/physical-memory claim.

## Remaining

[Remaining acceptance](evidence/remaining-acceptance.md): other CAST, broader metadata/M2, six complex candidates, request-root/default-NoColumns/liveDAG and final acceptance.

Full expression/unistore were not rerun; historical4+1 failures remain. Workspace/lint/dev/bazel/release/exhaustive/TiFlash/FIPS/performance/physical memory/OOM/allocator/dual-tzdata/whole-Go-package/PR readiness remain unverified. Paired TiKV and three identical Plans are pinned; unrelated BUILD excluded. No force push or PR.
