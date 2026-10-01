# YEAR / MONTH / DAYOFMONTH / QUARTER checkpoint

Checkpoint **temporal-fields-four-32**, following `json-storage-quote-three-31`: functional **99→103/245**, final acceptance **0/245**. Four calendar projections earn credit; DAY is an alias, not a fifth family. Exact commands and twelve whole-log hashes: [`../logs/temporal-fields-summary.txt`](../logs/temporal-fields-summary.txt).

## Projection and boundary contracts

Distinct logical nullable **TimeCoreBits** carries raw u64 through physical nullable Bytes of exact8LE width; the signed Int result kind is unchanged. Private kernels check width only (`Other` on malformed transport), then use the sole TiKV const field projections. Native CoreTime getters delegate; wire quarter calls `t.quarter()`. NULL reaches real workers, core0 projects0, month15 gives15/quarter5, all-ones gives16383/15/31/5; clock/lower bits do not participate. No strict DateTime construction or packed/whole-Time conversion substitutes for field projection.

Wire YEAR/MONTH/DAY zero-date, NO_ZERO_DATE, context-warning and nullable ordering remain distinct and unchanged. PB review corrected a too-narrow length-one NULL-prefix check: every actually observed NULL, including the old extra-arity NULL-success case, reaches a nullable worker without coercing the observed prefix or reading its suffix. This preserves old admission rather than adding a new domain; the first source edits were not already complete.

**Intentional RED→GREEN:** the actual `eval_pi_in` zero-slot infrastructure failure was swallowed by legacy index flags into Ok()+1265; retained RED shows `cophandler.rs:7707` Ok(()) versus PoolResource Err. The cophandler class fix is three lines: infrastructure returns Err before ordinary SQL/InvalidResult flag handling. The exact regression then passed and also passed inside the current full unistore run. PB NULL-prefix review changes occurred concurrently: do **not** claim the whole binary changed by only three lines; the regression did not obtain its error through PB.

## Twelve retained current-batch receipts

**9 green + 1 intentional red subsequently green + 2 known full-suite non-green runs**: not twelve green, all-first-pass or zero-rerun evidence. No compile failure or old-expected modification. Writer globbed, grepped/read and SHA256-hashed all twelve; commands/exits, source and coverage proofs are parent-owned. `Finished` times are not benchmarks.

| `temporal-fields-` suffix | Passed / ignored / failed; filtered | Test / compile seconds; exit |
| --- | --- | --- |
| `tikv-datatype.log` | 42 / 0 / 0; 360 | 0.01 / 1.96; 0 |
| `native-datatype.log` | 15 / 0 / 0; 423 | 0.00 / 2.12; 0 |
| `tikv-local.log` | 254 / 1 old / 0; 484 (255 discovered) | 0.19 / 8.77; 0 |
| `tikv-guard.log` | 1 / 0 / 0; 738 | 0.00 / 0.12; 0 |
| `tikv-kernels.log` | 51 / 0 / 0; 688 | 0.01 / 0.12; 0 |
| **`legacy-red.log`** | **0 / 0 / 1**; 200 | 0.00 / 11.52; **101** |
| `legacy-green.log` | 1 / 0 / 0; 200 | 0.00 / 2.29; 0 |
| **`unistore-full.log`** | 187 / 13 / **1**; 0 (201 discovered) | 2.97 / 0.12; **101** |
| `dispatch.log` | 3 / 0 / 0; 1543 | 0.00 / 7.53; 0 |
| `native-source.log` | 21 / 0 / 0; 1525 | 0.02 / 0.12; 0 |
| `sql.log` | 67 / 0 / 0; 2078 | 1.09 / 19.11; 0 |
| **`expr-full.log`** | 1448 / 94 / **4**; 0 (1546 discovered) | 10.47 / 0.15; **101** |

Dispatcher3 covers raw fields/metadata, preparation precedence, MONTH routes and actually observed PB NULL. SQL coverage: four rows × five projections including DAY, one1292 typed-zero setup diagnostic, four numeric projections, two string-cast queries with1292 diagnostics, and nine zero-slot calls, all refusing with PoolResource/Pool (1105); YEAR(bad) additionally retains its pre-admission1292 warning. These are representative checks, not exhaustive date-domain proof.

Parent pinned-formatted/checked **18 live Rust sources (TiKV8/native10)**; both repository diff checks and both lockfile checks exit0. Exact preserved scope: complete old time_fn/tests.rs, tests/datetime.rs and arg_eval_type.rs; native core_time old cfg(test) block; calendar from parse_date_ymd onward; all old TiKV impl_time tests from test_add_duration_and_duration onward. No broader source audit is implied.

Both full suites are **currently non-green**, not merely historical: native expr retains four failures (including EXP FloatOverflow), unistore retains its point-range selection/DECIMAL failure. Parent freshly compared complete failure sections with `json-storage-quote-expr-full.log` and `substring-gb-unistore-full.log`, normalizing only panic-heading thread IDs: respectively SHA256 `0930217d98e0b92d727527dc3c7cb7313f1bbe35e643da60114fa6d78203839b` and `e285bfdba646f2d01f85b485ae317cc07c51b78cf0ae6d664c0fb2bc39259759`; neither is a whole-log hash.

Public eval/table/default-NoColumns one-shot behavior is retained, but this evidence **does not establish production request-root injection**. No complete Time/Date codec/Go-package, deep/wide input, internal allocation-peak/OOM, zero-copy/performance, release, make lint, full-workspace or broad-guard completion claim follows.

Only these two authorized docs were written after source freeze. Frozen **103/245**, final **0/245**, not PR readiness; the intentional red and both current full-suite failures remain disclosed.
