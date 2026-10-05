# Expression unification experiment

Paired branch: `expression-unification-demo` in YangKeao/tidb and YangKeao/tikv.
Current checkpoint: **cast-float-110**, after **cast-integer-109**.

Functional **238/245 (97.14%)**, strict **0**, remaining **7**—unchanged. This partial CAST/M2 batch adds no family credit. Overall goal remains active.

## Floating CAST and parsing

`cast.rs` delegates DOUBLE/FLOAT and the distinct value-only helper through `tikv/cast_float.rs` to SDK `native_cast_float.rs`. Native duplicate source, warning, narrowing and range policies are deleted.

SDK `codec/native_float_parse.rs` owns native byte parsing and reported diagnostic order without replacing the wire character scanner. Ordinary CAST observes one final event with a capped subject; datatype reports retain full subjects and their original context effects. Borrowed Decimal float conversion keeps visible formatting and the original Rust parse.

Ordinary lossy text/JSON Display and value-only strict UTF8/Decimal-prefix conversion stay separate. FLOAT's source-specific core behavior and the existing per-function typed finish remain distinct. No new C4 profile, carrier or admission gate is introduced.

## Evidence and retained failure

[Checkpoint](evidence/cast-float-checkpoint.md), [exact commands/counts/hashes](logs/cast-float-summary.txt), [manifest](checkpoint.json), [ledger](migration-progress.json).

Ten matched launches: **nine green, one retained failed new-SQL attempt**. The new test incorrectly assumed outer DOUBLE avoided inner FLOAT typed finishing. Unchanged source proved otherwise; only that new test was corrected and given a direct DOUBLE contrast. Its rerun passes. No production fix, old-test alteration or provider-output oracle.

Six new tests;3 SDK and244 native old test bodies unchanged. The corrected SQL test covers ten SELECTs:36 successful cells and two expected typed-error probes across scalar/vector modes. Core non-narrowing is checked at SDK/bridge level, not falsely claimed from SQL.

## Still open

[Remaining acceptance](evidence/remaining-acceptance.md): other CAST targets and typed/vector/UNION variants, native datatype conversion actuators and broader M2, six complex candidates, request-root/default-NoColumns/liveDAG and final acceptance.

Full expression/unistore were not rerun; historical4+1 failures remain. Workspace/lint/dev/bazel/release/exhaustive/TiFlash/FIPS/performance/physical memory/OOM/allocator/dual-tzdata/whole-Go-package/PR readiness are unverified. The manifest pins paired TiKV and three identical Plans. No force push or PR; unrelated BUILD excluded.
