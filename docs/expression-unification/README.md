# Expression unification experiment

Paired branch: `expression-unification-demo` in YangKeao/tidb and YangKeao/tikv.
Current checkpoint: **temporal-calendar-119**, after **duration-control-118**.

Functional **238/245 (97.14%)**, strict **0**, remaining **7**—unchanged. This is partial CAST/M2 evidence, not whole YEAR/DATE/CAST, temporal-family or Go-package acceptance. Goal remains active.

## Shared ownership

- `native_temporal_convert.rs` owns kind conversion, temporal rounding, duration calendar/year conversion, year adjustment/parsing and the strict year-event fold.
- `native_coerce_string.rs` owns the generic19-kind expression string selector, preserving Rust float display and original UTF-8 error classes rather than substituting SQL float formatting.
- Native methods retain raw value/event projection and a thin `coerce_str` delegate. No host business callback is added to these entries.

DATE/zero/same-kind/FSP identity, DST gap metadata reset, rounding reprojection versus duration-midnight behavior, raw YEAR storage and error subjects remain distinct. YEAR/DATE controllers and signed-datum fallback still remain native.

## Verification

[Evidence](evidence/temporal-calendar-checkpoint.md), [exact commands/counts/hashes](logs/temporal-calendar-summary.txt), [manifest](checkpoint.json), [ledger](migration-progress.json).

**11 matched GREEN launches**, no failure, zero-match, retry or interruption. SDK2/2; native datatype474; expression1/2/1/1/1; three SQL gates. Eight new tests;256 old native test bodies unchanged, with no existing SDK test bodies in the changed files.

New SQL covers8 SELECTs/22 cells plus2 strict typed INSERT error probes: fixed statement clock, calendar/YEAR fields, negative durations and DST boundaries. Concat=true remains unit/old-expression evidence. Non-strict typed-gap storage was not guessed from string-gap results; its existing kind propagation is unchanged and unverified here. No output-recorded oracle or performance/physical-memory claim.

## Remaining

[Remaining acceptance](evidence/remaining-acceptance.md): signed-datum conversion needs genuine hybrid ordinals plus temporal numeric, integer-text, JSON-integer and binary-literal policies before YEAR/DATE closure. Other CAST/M2, six complex candidates, request-root/default-NoColumns/liveDAG and final acceptance remain.

Full expression/unistore were not rerun; historical4+1 failures remain. Workspace/lint/dev/bazel/release/exhaustive/TiFlash/FIPS/performance/physical memory/OOM/allocator/dual-tzdata/whole-Go-package/PR readiness remain unverified. Prior incident receipts are retained. Paired TiKV and three identical Plans are pinned; unrelated BUILD excluded. No force push or PR.
