# Shared-pool five-family migration — shared-bytes-five-07

## Change

LENGTH/OCTET_LENGTH (one family), BIT_LENGTH, LTRIM, RTRIM and UNHEX now delegate to the same TiKV evaluator/pool as ASCII. Their native algorithms are removed. Functional delegation/deletion progress is6/245 families; strict final audit/performance gates remain open.

Pool slots/cache entries match the operation as well as epoch. Under slot/byte pressure an incompatible idle worker is retired through the existing actual-destruction path before its reservation is released. Scope operation switching likewise retires its incompatible cached worker. No new root, per-operation pool or native fallback is introduced. Existing ASCII public APIs remain thin compatibility paths. A one-slot mixed-operation query necessarily recompiles between operations; this is a performance follow-up, not hidden reuse.

LENGTH includes public BuiltStringLength::eval_in and existing eval (NoColumns), with its original stored_bytes→coerce_str behavior rather than substituting more permissive coercion for invalid ENUM/SET. CHAR_LENGTH remains unchanged in this checkpoint. Trim retains original binary/text packing and removes only its native scan. UNHEX retains binary output/invalid→NULL semantics but removes decoding. Int output uses owned signed metadata; Bytes output is owned, with SQL packing left to the existing frontend.

## Actual validation

From `tidb/rust/`, using `/home/agent/tidb/expression-unification/tools/cargo-tidb`:

- `test --locked -p tidb-expr --lib tikv::evaluated_ascii::tests::bytes_dispatch_ -- --test-threads=1`:4 passed/1468 filtered, exit0. Covers cached/idle op switching, same-root refusal and raw/2MiB result packing.
- `test --locked -p tidb-session --lib tests_core::lifecycle::evaluated_ascii_ -- --test-threads=1`:17 passed/2078 filtered, exit0 after correcting the newly added test's result representation.
- `test --locked -p tidb-expr --lib -- --test-threads=1`:1374 passed/4 unchanged baseline failures/94 ignored,1472 discovered, exit101. All four complete failure blocks equal06 after only thread-ID normalization.

The initial SQL run was16 passed/1 failed: all payloads were correct, but the new mixed-query fixture expected Datum::Bytes instead of the canonical String+Binary collation. The unchanged `tidb-chunk/src/row.rs:64–76` materializes all string/blob fields via SetString with field collation. Only this newly authored expected representation was corrected; all payloads, upstream expectations and production materialization stayed unchanged. This is not a claimed production bug RED/GREEN.

Real stored-column SQL alternates LENGTH/LTRIM/BIT_LENGTH/RTRIM/OCTET_LENGTH/UNHEX in a single one-slot query. Rows cover NULL, empty,FF, space-padded UTF-8, all spaces, odd valid hex and invalid hex. A zero-slot table-driven query checks all six spellings with NULL/non-NULL inputs (12 SQL calls) for typed PoolResource/Pool and1105/HY000/evaluation origin, rather than native or fresh-root bypass. Executor/projection concurrency is explicitly1 for these one-slot tests.

## Deferred, not claimed

No fresh backend change beyond the paired Plan;06's six-operation backend is reused. No new allocator receipt, factory peak/physical-OOM proof, release performance, whole-workspace/make lint acceptance or overall completion. Broad operation-scope propagation and cache efficiency remain follow-ups. Frozen coverage gives these five families no implemented PB/unistore signatures; unsupported signatures were not added for credit. Old192-byte observations do not validate current artifacts. Exact result excerpts are in logs/shared-bytes-five-summary.txt; raw local logs remain under expression-unification/logs/bytes-five-*.log.

Architecture index references existing paths and explicitly Rust-scoped commands; no normative AGENTS/Make/PR policy or generated fixtures changed. Next batch: CRC32, REVERSE, CHAR_LENGTH/CHARACTER_LENGTH and QUOTE, including binary/text branches and preserved frontend normalization, followed by fixed multi-input kernels.
