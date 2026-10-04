# Expression unification experiment

Paired YangKeao/tidb and YangKeao/tikv branch: `expression-unification-demo`.
Current checkpoint: **timestamp-add-72**, following **add-sub-time-71**.

## Progress

Functional migration: **214/245**; strict final-audited count **0**. TIMESTAMPADD adds one family; target221 needs7 more, with31 eligible families remaining.

TiKV now owns TIMESTAMPADD unit dispatch, rounding, month clamp-versus-roll, range checks, formatting and computed diagnostics over existing shared temporal DTOs. Native retains only input coercion and report interpretation, deleting old arithmetic and dead facades. Two existing carriers separate demanded date input from actual prefix NULL without fabricating an unread third operand. Raw coerced IEEE bits, eager upstream child evaluation/datetime wrapping, source String metadata and warning order remain unchanged. No new PB/catalog/legacy admission, carrier, binding, driver, result kind or factory allowance. TIMESTAMP/generic timezone parsing remains unclosed.

**Retained compatibility limitation:** prior INTDIV raw-empty coefficient lhs divided by1 used to yield exact zero; shared math rejects it (infallible SDK panic, evaluated infrastructure failure). No native fallback conceals it; arbitrary invalid-raw mathematical parity is unclaimed.

## Validation and evidence

[Evidence](evidence/timestamp-add-checkpoint.md), [exact commands/hashes](logs/timestamp-add-summary.txt), [manifest](checkpoint.json), [cumulative ledger](migration-progress.json).

Seven exclusive write owners plus bounded independent review;12 Rust files, one new module,6 added tests. Ten Cargo launches:8 nonzero green,1 retained/resolved new SQL expectation RED and1 unchanged old full-expression failure. No compile failure, interrupted or zero-match run.

CPP core/wrappers2/local324+1ignored; native root1/calendars22/captured1/ADD-SUB1/SDK2 and SQL retry2 pass (overlapping filters). SQL covers16 values/NULLs in both modes, metadata/warnings,1 unsupported composite-unit error and8 direct zero-slot roots. The first SQL test confused the separate default-length lookup with FieldType::new: source proves VarString(-1,-1), not(5,-1). Only that new assertion/comment was corrected; its failed receipt is retained. All6 new tests eventually pass.

Full expression **1567/4old/94ignored** retains the identical failure section/list after thread-ID normalization. Unistore unchanged/not rerun. CPP183/native355 original test bodies and seven neighboring functions are byte-identical; pinned formatting and both diff checks pass. No dependency/generated/Go/Bazel edits or fixture recording.

Full temporal parsing/type migration, whole workspace/lint/dev/bazel_prepare, release/performance/zero-copy/physical heap/peak/OOM, exhaustive differential/TiFlash/FIPS, M6/default-NoColumns and prior JSON_KEYS/AST/SQL metadata/parser/GB/vector/Decimal gaps remain deferred. This is neither whole-package transcreation nor PR readiness.

Three Plans agree; manifest pins their hash and paired TiKV commit. TiKV publishes first, then TiDB, without force push or PR. Old untracked client-differential BUILD.bazel stays excluded. Overall goal continues.
