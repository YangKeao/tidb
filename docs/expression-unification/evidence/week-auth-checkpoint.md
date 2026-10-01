# WEEK / WEEKOFYEAR / YEARWEEK / PASSWORD / SM3 checkpoint

Checkpoint **week-auth-five-38**, following `daynumber-four-37`: functional **119→124/245**, final acceptance **0/245**. Eight operations, five family credits; no new role/driver/DTO/result kind/report. **16 test-Cargo attempts, 15 actual test runs = 13 green + 2 current baseline non-green full runs**, plus two successful offline metadata commands. One real compile failure, one static-proof correction; no Cargo launch failure, same-target retry or new product RED. Exact commands and all16 whole-log hashes: [`../logs/week-auth-summary.txt`](../logs/week-auth-summary.txt).

WeekDateTextNative returns original owned text or None; the two-stage text route uses complete sequential worker calls, not recursion inside coercion/packing. Original date failure skips mode demand/coercion; explicit NULL mode means0. SQL WEEK's default getter stays first; PB observed NULL does not read it. Zero-slot refusal can occur at the probe before a not-yet-demanded mode error. **Repeated parse/two leases, or two one-shots without a scope, are explicit costs**, not optimized away. Shared week macros retain i32/i64 widths, day-comparison cast, %7 and year0-nonleap week policy; WEEK and YEARWEEK raw-zero guards remain distinct. Legacy raw mode0 does not acquire SQL date validation.

Pure leaf tidb_query_crypto has only sha1=0.10, with CPP0.10.6/native0.10.7 retained. It uniquely owns original sha1_hash/encode_password_bytes and the complete Sm3 state/helpers; parser-auth delegates/reexports preserve public API and other streaming SHA1 callers. Sum(x) first writes x into live state, returns only the digest, and leaves caller bytes intact; reset/chunked/write-after-sum remain. Native PASSWORD emits1681 before hash-input preparation/NULL/admission and uses double RAW SHA1 plus uppercase *hex; CPP wire PASSWORD's double-hex/lowercase/Other-warning policy remains unchanged. Existing FIPS shim is untouched; no provider replacement or FIPS claim.

**Compile failure:** parser `--test all parser_auth_package_source::` stopped at three E0061 errors: old parser_hint_source.rs112/127/351 supply3 arguments while select/hint.rs748 requires4. That target ran no tests and remains unproven. Parent verified both sources, original auth fixture and aggregate script byte-identical to HEAD16404402; no hint fix. Same parser Cargo manifest adds auth_shared pointing directly to unchanged tests/parser_auth_package_source.rs: **20 actual tests pass on that isolated target**, not an all-target recovery or same-target retry; auth unit6 separately pass.

**Static proof correction:** whitespace-only comparison of the whole moved SM3 section failed because CPP formatting reflowed one Sum doc comment, adding a second /// marker. Diff isolated only that documentation change; canonicalizing that specific comment made the whole section byte-identical. This is not a blanket whitespace/algorithm identity claim or source/algorithm repair. SHA1/encode/wire-PASSWORD bodies are byte-identical. Final **23 Rust sources (TiKV12/native11)** passed pinned formatting on the first format check in both repositories; both diffs pass. **fmt failures0** this round; do not import Round38's format failure or claim all commands first-pass.

## Logged test attempts (Finished time is not a benchmark)

| `week-auth-` suffix | Passed / ignored / failed; filtered | Test / compile seconds; exit |
| --- | --- | --- |
| `tikv-time.log` | 50 / 0 / 0; 361 | 0.01 / 2.06; 0 |
| `native-core-time.log` | 15 / 0 / 0; 423 | 0.00 / 1.95; 0 |
| `tikv-crypto.log` | 2 / 0 / 0; 0 | 0.00 / 0.67; 0 |
| `parser-unit.log` | 6 / 0 / 0; 729 | 0.06 / 3.98; 0 |
| **`parser-source.log`** | **compile failure; no test run** | n/a / not Finished; **101** |
| `parser-isolated.log` | 20 / 0 / 0; 0 | 0.17 / 0.49; 0 |
| `tikv-local.log` | 263 / 1 old / 0; 496 (264 discovered) | 0.19 / 12.55; 0 |
| `tikv-kernels.log` | 62 / 0 / 0; 698 | 0.01 / 0.12; 0 |
| `tikv-encryption.log` | 11 / 0 / 0; 749 | 0.00 / 0.12; 0 |
| `sql.log` | 80 / 0 / 0; 2078 | 1.21 / 33.38; 0 |
| `dispatch.log` | 3 / 0 / 0; 1559 | 0.00 / 10.76; 0 |
| `native-week.log` | 12 / 0 / 0; 1550 | 0.00 / 0.12; 0 |
| `native-crypto.log` | 15 / 0 / 0; 1547 | 0.00 / 0.12; 0 |
| `legacy.log` | 2 / 0 / 0; 205 | 0.00 / 13.66; 0 |
| **`unistore-full.log`** | 193 / 13 / **1**; 0 (207 discovered) | 2.98 / 0.13; **101** |
| **`expr-full.log`** | 1464 / 94 / **4**; 0 (1562 discovered) | 10.79 / 0.12; **101** |

SQL80 includes E3: changed default-week-format1→0, explicit NULL mode, year0/mode3 sentinel; auth NULL/empty/abc with PASSWORD1681 on every row. SM3 empty is tested only for 64 lowercase-hex shape, **not a new golden digest**; original SM3 metadata40/PASSWORD41 is retained. All15 zero-slot calls return Resource; three bad-DATE cases retain1292, three PASSWORD cases retain1681, the other nine have empty warnings. No all-warnings-empty or metadata-width correction claim.

D3 observes the two facades separately, not a cumulative observer total; legacy2 covers actual raw Time/columns/NULL/extras and consumer error propagation. C's expr_eval change only extends the existing NullWitness arm to week; TimeCoreBits validators remain unchanged. The week substring filter overlaps dispatch/weekday/date-format tests and is not twelve disjoint new week fixtures. Old fixtures/expected values were not changed, and new-provider outputs are not used as expected oracles.

Both current full failure sections match the corresponding daynumber logs after only panic-heading thread-ID normalization: expr `80be9bda05e5bf630e9c246ca9523d23f82c122eaaeec6e220b1b00cec436615` (four old failures, including EXP FloatOverflow; duration212:55), unistore `e285bfdba646f2d01f85b485ae317cc07c51b78cf0ae6d664c0fb2bc39259759` (DECIMAL-'abc', source194:5). No source-location mapping. These are failure-section hashes, not whole-log hashes; both current full suites remain non-green.

Five manifests/two locks changed under parent authority. Two offline metadata commands succeeded separately from test attempts; parent compared every old package object and allowed only the new leaf and approved dependency edges, preserving SHA1 versions. Writer globbed, grepped/read and hashed16 logs without build/test/fmt or additional source audit. Quiet tee keeps complete logs; metadata uses its separate JSON redirects.

Release/profile/panic-test adaptation, lint, M6/production-root, deep/wide guards, internal allocation peak/OOM, zero-copy/performance, complete temporal/crypto/Go-package and full-workspace gates remain unestablished. Parser all-target compilation is still blocked; isolated auth success does not repair it.

Only these two docs were written under this authorization: **124/245 functional, final0/245; not PR readiness.**
