# Expression unification experiment

Checkpoint-ID: `crypt-hash-format-six-42` (previous: `uuid-translate-six-41`)
**143/245 frozen families delegate with native evaluator algorithms removed; target 221.** Added: ENCODE, DECODE, TIDB_SHARD, VITESS_HASH, FORMAT_BYTES and FORMAT_NANO_TIME. Strict final-audited acceptance **0**; incomplete, not PR-ready.
Paired branches: [TiDB](https://github.com/YangKeao/tidb/tree/expression-unification-demo) · [TiKV](https://github.com/YangKeao/tikv/tree/expression-unification-demo). Parent owns publication pins/Plan mirrors/paired pushes; no force-push or automatic PR.
**Scope:** 22 Rust files (TiKV 13, including three new crypto modules; native 9), two manifests and two Cargo-generated locks. All 22 pinned formatter checks and both diff checks passed; formatter invocation/check, static-proof and lookup errors 0. One prebuild ABI-document correction: Bytes2 is a tuple, not an array; no compile failure.
**Protocol:** seven operations, old roles/results: SqlEncode/SqlDecode Bytes2(Some(data),Some(password))→Bytes; SqlCryptNull actual NullWitness(None)→Bytes NULL; TidbShard/VitessHash Int→OwnSignedInt→existing into_uint_bits_datum; FormatBytes/FormatNanoTime Ieee754Bits→Bytes. No new role/report/cause/driver/PB/cop admission; high hash bits are not overflow.
**Demand:** crypto raw coercion runs before capability discovery/scope guard to select the operation; data NULL skips password and real NULL is not a fake suffix. Original new_string raw bytes/connection metadata remain, not new_bytes. Hash warning casts and format real_arg coercion remain inside the guard, with no new finite-value filter. Worker owns hash/modulo and both formatter unit tables/algorithms; no native business fallback.
**Shared foundation:** pure TiKV crypto modules mysql_rng/sql_crypt/vitess own the dual-seed stream, password space/tab skipping, float×255 and original inverse naming, plus Des 0.9 LazyLock zero-key/BE8 hashing. The third recurrence in native mathutil/rand.rs required public wrapping mysql_rand_step(&mut u32,&mut u32)->f64: normalized MySqlRand::next_f64 and raw native Mutex<State>::gen share it. Raw setters/getters, Mutex/time/seed and TiKV seed/clock/Default remain unchanged; native RAND gets no family credit. Utility public APIs are thin, not duplicate algorithms; no whole-Go-package, FIPS or security claim.
**Formatting:** parent proved all three moved formatter helper bodies byte-identical. Original abs/negative-zero/scientific formatting and expect remain: direct positive infinity panics; NULL/NaN succeed. This is preserved policy, not a quiet production fix or a whole-panic-safety proof.

## Actual validation
| Receipt (`crypt-hash-format-` prefix, `.log`) | Result | Compile / run seconds |
|---|---|---|
| Early tikv-crypto / native-crypt-util / native-vitess-util | 5/0, 2/575, 1/576 passed/filtered; before raw-RNG extension, not final coverage | .85/.00; 10.25/.00; .11/.00 |
| tikv-crypto-final | 5 passed, 0 filtered | .23 / .00 |
| tikv-local | **273 passed, 1 new RED, 1 ignored, 514 filtered; exit 101** | 11.96 / .19 |
| tikv-local-retry | 274 passed, 1 ignored, 514 filtered | 4.08 / .19 |
| tikv-encryption | 13 passed, 776 filtered | .12 / .00 |
| tikv-misc | 25 passed, 764 filtered | .12 / .00 |
| tikv-rand | 4 passed, 785 filtered | .12 / .10 |
| native-crypt-util-final | 2 passed, 575 filtered | 3.42 / .00 |
| native-vitess-util-final | 1 passed, 576 filtered | .11 / .00 |
| native-rng-util | 3 passed, 574 filtered | .11 / .00 |
| native-crypto | 15 passed, 1560 filtered | 15.28 / .01 |
| native-crypto-source | 16 passed, 3 old ignored, 1556 filtered | .12 / .01 |
| native-misc | 13 passed, 1562 filtered | .12 / .00 |
| native-info | 3 passed, 1572 filtered | .14 / .00 |
| native-info-source | 2 passed, 1573 filtered | .12 / .00 |
| dispatch | 3 passed, 1572 filtered | .12 / .00 |
| sql | 92 passed, 2078 filtered | 32.06 / 1.39 |
| sql-rand | 2 passed, 2168 filtered | .14 / .03 |
| expr-full | **1477 passed, 4 old failures, 94 ignored; 1575 total; exit 101** | .12 / 10.49 |
| unistore-full | **195 passed, 1 old failure, 13 ignored; 209 total; exit 101** | 7.98 / 3.01 |
**22 test-Cargo attempts = 22 nonzero-test runs = 19 green (3 early + 16 final current) + 1 new RED + 2 current old full failures.** One failed-target retry; compile/launch failures and zero-match runs 0; not all first-pass. C's new physical probe incorrectly asserted is_ok() for +Inf; direct invocation hit the original scientific expect at impl_miscellaneous.rs:272. Parent changed only that new test to catch_unwind/assert panic, retaining NULL/NaN successes and invocation counts; failed log retained. One new-test expectation correction, no production repair or old expected-value change.
Both complete full-suite failure sections match uuid-translate using **only numeric thread-ID normalization; no address mapping**: expression `25f964887541f45602363f00dc60df4f0af21c74edccc16c38d0804094fb9bff`, duration.rs:164:55; unistore `e285bfdba646f2d01f85b485ae317cc07c51b78cf0ae6d664c0fb2bc39259759`, 194:5. These suites are not green; older 164→212/80be9 mapping remains history only.
Two separate non-test `cargo update --offline` commands (-p tidb_query_crypto on TiKV January tools; -p tidb-util on native August tools) succeeded: tikv-lock/native-lock logs, lock-resolution 2, metadata commands 0, **24 total raw receipts**. TiKV adds cipher 0.5.2, crypto-common 0.2.2, des 0.9.0, hybrid-array 0.4.13, inout 0.2.2; typenum 1.16→1.20.1 is the sole upgrade. digest 0.10.7 only disambiguates old crypto-common 0.1.6, without upgrade; other old packages unchanged. Native adds/upgrades no packages, only tidb-util/tidb_query_crypto dependency edges. January crypto compatibility was actually tested, not pending.
Seven complete original test modules remain byte-identical: TiKV math; native util crypt/vitess/rand; native expr crypto/misc/info. Native rand outside gen is unchanged. Three early leaf passes preceded the discovered raw-RNG extension; three final leaf reruns plus native RNG and old SQL RAND regressions cover the completed closure. New raw-MAX-seed literals are hand-derived, not provider oracles. New coverage: five kernel tests, two local, three leaf, three dispatch (seven materialize arms only), three SQL with 12 zero-slot calls and original values/metadata/warnings.
Exact commands/receipts: [summary](logs/crypt-hash-format-summary.txt), [evidence](evidence/crypt-hash-format-checkpoint.md), `checkpoint.json`. Historical parser-all three-E0061 remains unresolved/unrun; isolated auth_shared 20-pass is historical, not a current parser/auth gate. Prior checkpoint records remain separately historical.
## Remaining work
FORMAT requires locale/numeric/error-1649 closure; DATE, strict MICROSECOND and JSON path/pretty policies remain deferred. Legacy capability propagation, operation scopes, allocation/peak, differential reruns, release/profile, duration-panic adaptation, parser all, whole workspace, make lint, M6 and TiFlash remain unfinished; no performance/OOM-safety completion.
