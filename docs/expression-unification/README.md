# Expression unification experiment

Paired branch: `expression-unification-demo` in YangKeao/tidb and YangKeao/tikv.
Current checkpoint: **json-unquote-59**. Previous: **json-raw-values-58**.

## Progress

Frozen denominator245; target221. Functional delegation plus native algorithm deletion: **192/245**. Strict final-audited acceptance: **0**; goal remains active. UNQUOTE adds one whole-family credit.53 eligible remain;29 more functional migrations needed. Shared Time/Duration formatting earns no temporal-expression credit.

## What changed

- TiKV datatype `native_text.rs` owns the original raw Display, partial string projection and SDK escape policies; native duplicates are removed. Native Time/Duration Display bodies also move to shared helpers, without changing wire formatters.
- Separate fixed workers preserve strict SQL-text parsing, verbatim typed-JSON string contents and SDK conditional second-unescape. Validation uses a unit string visitor, not an owned frontend answer or permissive IgnoredAny. Raw values are not normalized through serde/wire formatting.
- Guarded preparation keeps original errors; actual input bytes reach unit Bytes1/OwnBytes recipes and only computed bytes become String. Genuine SQL NULL uses the existing witness. No new kind/carrier/driver/binding/cause/NoArgs or PB/legacy/ordinary admission.
- Malformed raw fallback remains empty text; root nonfinite formatting still panics. Unwind propagates through the existing driver, poisoning the scope and retiring the worker. Zero-slot refusal may prevent reaching that panic; no hidden NULL/error substitution.

## Validation — not all green

[Review evidence](evidence/json-unquote-checkpoint.md), [exact commands/hashes](logs/json-unquote-summary.txt), [manifest](checkpoint.json), [sole cumulative ledger](migration-progress.json). Eight exclusive contributors;20 Rust files,1 new source,15 new tests, all added tests passed.20 formatter checks pass; no dependency/manifest/lock changes.

12 Cargo attempts:1 nonexistent-target selection,11 actual runs; **8 green,3 non-green**. Green: CPP datatype40/JSON18/local306+1ignored; native datatype445; native unquote6; legacy scope1; new SQL2 and old SQL1. Nine zero-slot SQL probes use direct stored columns with no other worker masking admission.

Full expression **1522/4old/94ignored** and unistore **206/1old/13ignored** match complete prior failure sections after only thread-ID normalization. Additionally, corrected `--test all json_introspection_and_unquote` runs **0pass/1fail** at its first JSON_KEYS assertion (actual JSON, expected String), before UNQUOTE. The fixture/JSON_KEYS path is unchanged, but no prior runtime baseline was rerun: this newly exercised failing gate is not claimed as old-baseline proof or a UNQUOTE pass. No expectation was changed to make it green. Independent UNQUOTE unit/direct SQL evidence is recorded.

That integration mismatch, strictM6, broader default-NoColumns request-root integration, workspace/lint/dev/bazel_prepare, release/performance/zero-copy/physical heap/peak/OOM, exhaustive context/domain/wire/differential/TiFlash/FIPS and prior parser/GB/vector/extreme Decimal exceptions remain deferred. Writer/transport allocation performance is unmeasured. No whole Go-package transcreation, PR readiness or overall completion claim.

`checkpoint.json` pins the paired TiKV commit and common Plan SHA256. Both tracked Plans equal the root Plan. Publication remains TiKV first, then TiDB with exact paired SHA/Plan; no force push or automatic PR. The pre-existing untracked client differential `BUILD.bazel` remains excluded. Next candidates are simple temporal functions and ANY_VALUE, without advance credit.
