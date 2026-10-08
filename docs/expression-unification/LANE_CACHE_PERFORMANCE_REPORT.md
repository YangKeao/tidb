# Lane cache 与真实 batch 表达式性能报告

日期：2026-10-08

TiDB 当前提交：`f5109ab113f75ebab762fef5e292efaa54542a73`（运行时代码与 `c1072a80` 相同，本轮只增加 probe/test-only route 观测）

冻结 TiDB 基线：`364aef2bab5cc633ecb76a775ae8f36f86a6687d`

## 为什么上一轮没有测 batch

round227 有意只测 warm、width-one、未折叠常量 kernel，用来回答“lane-owned cache 相对当前 one-shot 是否降低 evaluator 固定开销”。它不能回答真实 `Chunk`、selection 或 batch scaling，因此当时没有把结果写成 1024-row batch、server QPS 或端到端 SQL 性能。

round228 补上了真实 `Chunk` 的 1/8/64/256/1024 行测量、selection/NULL 形状和 15 个常见 workload，同时在更稳定的方法下重跑 round227。它仍不是 server QPS 或端到端 SQL 延迟；直接 evaluator probe 的优点是可以断言究竟进入 TiKV numeric route 还是 production fallback，不会把其他 server 开销误算成 expression 改动。

## 结论摘要

1. **round227 的 lane-cache-only 结论在 5 个独立进程对中复现。** AST tier 的 `lane_cache / one_shot` 几何平均为 **0.621×**（下降 37.9%），chunk tier 为 **0.584×**（下降 41.6%）。
2. **三个已准入 TiKV numeric PLUS workload 随 batch 增大出现明显的 per-row 下降。** 从 batch 1 到 1024 的当前 ns/row 观察比值为 **7.82×、7.02×、6.59×**，1024 行达到约 **266/271/505 ns/row**。整数 PLUS 的单次运算成本对取值不敏感，因此该 pattern 支持固定成本被摊薄的解释；但不同 size 使用不同长度的 deterministic row prefix，不是严格控制输入分布的因果实验。
3. **12 个 `production_fallback` workload 在这组 prefix dataset 上基本没有 per-row 下降。** batch 1 / 1024 观察比值只有 **1.02×–1.18×**。它与当前 fallback 在 production entry 内逐行执行 expression 的代码结构一致，但字符串长度、分支和 JSON/decimal 值分布也随 size 改变；本轮没有 matched-input 对照或 profile，不能把这一比值单独归因于 batch size、adapter 或分配步骤。
4. **当前实现与冻结 native 基线仍有显著差距。** dense 1024 行时，三个 TiKV numeric workload 的配对中位倍率为 **12.31×–13.29×**；12 个 fallback workload 为 **4.89×–42.90×**。按 route 几何平均，numeric 从 batch 1 的 **22.47×** 降到 batch 1024 的 **12.85×**；fallback 则从 **9.43×** 变为 **10.89×**。
5. **selection 和 NULL 没有改变主结论。** 在 1024 physical rows 上，dense、reverse、duplicate、每四行取一行、每十行一个 NULL 的当前 ns/row 量级一致；numeric sparse selection 比 dense 高约 2%–7%，`strcmp` 各形状约为 1,245–1,257 ns/row。
6. **机器状态足以支持本轮本地配对结论。** 40 次 process calibration 为 1.081–1.105 ns/iter，单进程前后漂移最大 1.45%；进程前后端点采样的 CPU PSI avg10 最大 0.04，`perf stat` 记录的 CPU migration/major fault 均为 0，CPU14（CPU2 的 SMT sibling）按端点 `/proc/stat` delta 计算的单进程 non-idle 最大 4.68%。这些数据不证明其他机器或 server workload 上也会保持同样倍率。

## 方法

### 对比对象与 route

- width-one：`frozen_native`、当前 `one_shot`、当前 lane-owned `ReadyValueCache`。
- real batch 当前入口：`EvaluatorSuite::run_with_tikv_numeric`。
  - `integer_add`、`integer_add_literal`、`integer_add_nested` 均硬断言 `route=tikv_numeric`；它们分别覆盖 column+column、column+literal 和 nested PLUS。
  - 其余 12 个常见 workload 均硬断言 `route=production_fallback`，即从同一个 production entry 落回 `EvaluatorSuite::run`。
- 冻结提交没有当前 numeric admission，使用当时 production `EvaluatorSuite::run`，标为 `frozen_native`。这是“当前 production entry 对冻结 production evaluator”的版本对比，不是同一实现内部的 A/B。

### workload 与输入

- dense batch sizes：`1, 8, 64, 256, 1024`。
- 常见 workload：三种整数 PLUS、整数乘法、`ABS`、`IF`、`COALESCE(NULLIF())`、`STRCMP`、`LOWER`、`UPPER`、`CONCAT`、`SUBSTRING`、decimal 加法、`JSON_TYPE`、`REGEXP_LIKE`。
- 每个 size 使用相同生成器的 `0..physical_rows` prefix，而不是把同一行复制到所有 size；因此表给出的是这些明确 dataset 的实际吞吐，1/1024 比值不是严格隔离 batch size 的 matched-input effect。
- `JSON_TYPE` 使用真实 JSON 列，不是常量表达式。
- 三个 PLUS workload 和 `STRCMP` 额外在每个 batch size 上测 `dense_null10`、`sparse_quarter`、`reverse`、`duplicate`，总计 **155 个 batch cells**。
- 每个 scenario 使用 warm `EvaluatorSuite`、复用 input/output `Chunk`；计时区间包含 `output.reset()`、production evaluator 调用和结果 materialization，不包含构造 suite/input 与 correctness 检查。
- 所有 workload 均在计时前后与 scalar reference checksum 对比；三个 numeric workload 还逐行用独立整数公式断言结果及 NULL。

`length(s)` 原本也在候选列表中，但当前 release evaluator 在计时前返回 `ExpressionRuntimeFailure / InvalidSpecification / Prepare`，因此没有把失败记录成 latency；本轮用同类常见字符串函数 `UPPER` 替代。该失败是待修 correctness/coverage 问题。

### 稳定性与统计

- release profile，`taskset -c 2`，单测试线程；主机为 Ryzen 9 9900X（12C/24T）、46 GiB RAM、Linux 7.1.8，performance governor/EPP，boost 开启。
- 不能以当前 uid 关闭 SMT sibling、移动 IRQ、关闭 boost 或设置实时优先级，因此明确记录 CPU2/CPU14、PSI、温度、频率、loadavg 和 `perf stat` sidecar。
- **5 个独立 process pairs**；奇数 pair 为 current→frozen，偶数 pair 为 frozen→current。
- batch 每 cell 7 samples，逐 cell 校准到约 75 ms/sample；统计单位是每进程 7-sample median。
- width-one 每 cell 9 samples；冻结 probe 将 iteration count 乘 16，避免上一轮个别约 6 ms 的短样本。当前 one-shot/lane 仍按 ABBA/BABA 交错。
- 表中的版本倍率先在同一 process pair 上相除，再取 5 个 pair ratio 的 median；完整 summary 提供固定 seed、10,000 次 paired bootstrap 的 95% interval。n=5 的区间用于显示本机重复性，不是总体推断。
- collection 后按以下 QC 检查：无失败、无 migration/major fault，calibration 前后漂移不超过 3%，进程端点 CPU PSI avg10 不超过 0.10；所有 20 个最终进程通过，无 reject/rerun。runner 本身不自动 reject，因此这不是预注册的自动门禁。

## 结果一：稳定重跑 width-one

单位：ns/eval；越低越好。

### AST/value tier

| workload | frozen native | current one-shot | current lane | lane / one-shot | lane 降幅 | lane / frozen |
|---|---:|---:|---:|---:|---:|---:|
| `integer_add` | 32.359 | 1,870.231 | 970.956 | 0.523× | 47.7% | 30.054× |
| `lazy_if` | 93.020 | 3,622.969 | 1,963.995 | 0.541× | 45.9% | 21.146× |
| `strcmp` | 164.361 | 2,356.930 | 1,310.165 | 0.556× | 44.4% | 7.971× |
| `decimal_add` | 333.969 | 3,328.956 | 2,256.770 | 0.678× | 32.2% | 6.786× |
| `json_type` | 227.411 | 1,841.940 | 1,123.392 | 0.608× | 39.2% | 4.934× |
| `regexp_like` | 7,647.341 | 10,553.247 | 9,356.868 | 0.887× | 11.3% | 1.223× |
| **几何平均** | — | — | — | **0.621×** | **37.9%** | — |

### Standalone chunk tier

| workload | frozen native | current one-shot | current lane | lane / one-shot | lane 降幅 | lane / frozen |
|---|---:|---:|---:|---:|---:|---:|
| `integer_add` | 92.327 | 1,940.269 | 1,086.294 | 0.558× | 44.2% | 11.765× |
| `lazy_if` | 43.529 | 3,575.567 | 1,924.502 | 0.539× | 46.1% | 44.151× |
| `strcmp` | 76.494 | 2,325.685 | 1,231.843 | 0.530× | 47.0% | 16.104× |
| `decimal_add` | 492.839 | 3,516.800 | 2,445.212 | 0.695× | 30.5% | 4.957× |
| `json_type` | 211.504 | 1,828.254 | 1,131.320 | 0.617× | 38.3% | 5.324× |
| `regexp_like` | 240.251 | 2,554.176 | 1,482.793 | 0.579× | 42.1% | 6.114× |
| **几何平均** | — | — | — | **0.584×** | **41.6%** | — |

结果与 round227 的 0.620×/0.581× 一致。新的 5-pair paired bootstrap 中，AST 各 workload 的 lane/one-shot 95% interval 均完全低于 0.90；chunk 各 workload 均低于 0.70。完整 interval 在 summary JSON 中。

## 结果二：真实 dense batch scaling

单位：当前分支 ns/selected-row；最后一列是两个 size-specific prefix dataset 的 batch 1 / batch 1024 观察比值，不是 matched-input 因果效应。

| workload | current route | 1 | 8 | 64 | 256 | 1024 | 1→1024 |
|---|---|---:|---:|---:|---:|---:|---:|
| `integer_add` | `tikv_numeric` | 2,078.7 | 492.8 | 308.2 | 271.4 | 265.9 | 7.82× |
| `integer_add_literal` | `tikv_numeric` | 1,900.5 | 480.2 | 302.1 | 262.9 | 270.7 | 7.02× |
| `integer_add_nested` | `tikv_numeric` | 3,329.2 | 864.3 | 541.8 | 500.8 | 504.9 | 6.59× |
| `integer_multiply` | `production_fallback` | 1,080.0 | 949.7 | 922.4 | 928.3 | 917.5 | 1.18× |
| `abs` | `production_fallback` | 959.2 | 930.5 | 923.8 | 929.1 | 929.7 | 1.03× |
| `lazy_if` | `production_fallback` | 3,050.6 | 3,017.3 | 3,007.5 | 2,998.6 | 3,004.0 | 1.02× |
| `coalesce_nullif` | `production_fallback` | 3,264.4 | 3,227.9 | 3,236.4 | 3,230.8 | 3,215.7 | 1.02× |
| `strcmp` | `production_fallback` | 1,315.5 | 1,268.8 | 1,253.1 | 1,254.1 | 1,253.8 | 1.05× |
| `lower` | `production_fallback` | 1,153.3 | 1,119.1 | 1,122.3 | 1,125.8 | 1,113.8 | 1.04× |
| `upper` | `production_fallback` | 1,169.7 | 1,117.7 | 1,112.2 | 1,123.5 | 1,121.9 | 1.04× |
| `concat` | `production_fallback` | 1,211.7 | 1,191.8 | 1,185.0 | 1,189.5 | 1,178.8 | 1.03× |
| `substring` | `production_fallback` | 1,622.2 | 1,583.1 | 1,580.9 | 1,575.3 | 1,572.8 | 1.03× |
| `decimal_add` | `production_fallback` | 2,290.1 | 2,215.9 | 2,203.2 | 2,212.8 | 2,201.4 | 1.04× |
| `json_type` | `production_fallback` | 1,098.0 | 1,072.4 | 1,061.6 | 1,072.4 | 1,080.8 | 1.02× |
| `regexp_like` | `production_fallback` | 1,528.5 | 1,490.1 | 1,487.9 | 1,493.3 | 1,489.3 | 1.03× |

这里最重要的观察是 route 分层：对取值不敏感的 PLUS，进入 numeric batch program 后的 per-row 曲线随 size 明显下降；fallback 在这些真实 `Chunk` prefix dataset 上没有同量级下降。严格量化纯 batch-size effect 仍需新增 uniform-row 或 matched-distribution 场景。

## 结果三：当前 / 冻结 native

配对中位倍率；越接近 1 越好。

| workload | route | 1 | 8 | 64 | 256 | 1024 |
|---|---|---:|---:|---:|---:|---:|
| `integer_add` | `tikv_numeric` | 28.53× | 17.40× | 13.59× | 12.52× | 12.31× |
| `integer_add_literal` | `tikv_numeric` | 20.02× | 16.05× | 13.85× | 12.78× | 13.29× |
| `integer_add_nested` | `tikv_numeric` | 19.86× | 15.68× | 12.93× | 12.76× | 12.98× |
| `integer_multiply` | `production_fallback` | 14.35× | 33.77× | 40.58× | 42.62× | 42.90× |
| `abs` | `production_fallback` | 8.83× | 9.35× | 9.39× | 9.46× | 9.48× |
| `lazy_if` | `production_fallback` | 25.07× | 27.09× | 27.34× | 27.18× | 27.29× |
| `coalesce_nullif` | `production_fallback` | 17.28× | 18.64× | 18.84× | 18.91× | 18.84× |
| `strcmp` | `production_fallback` | 13.94× | 13.84× | 14.15× | 14.32× | 14.33× |
| `lower` | `production_fallback` | 6.89× | 7.23× | 7.33× | 7.38× | 7.30× |
| `upper` | `production_fallback` | 7.01× | 7.23× | 7.37× | 7.44× | 7.31× |
| `concat` | `production_fallback` | 12.71× | 13.78× | 13.73× | 13.78× | 13.60× |
| `substring` | `production_fallback` | 5.79× | 5.63× | 5.78× | 5.77× | 5.81× |
| `decimal_add` | `production_fallback` | 4.05× | 4.72× | 4.85× | 4.92× | 4.89× |
| `json_type` | `production_fallback` | 7.51× | 7.41× | 7.49× | 7.60× | 7.60× |
| `regexp_like` | `production_fallback` | 5.95× | 5.87× | 6.03× | 6.02× | 5.94× |

`integer_multiply` 的冻结 evaluator 本身随 batch size 摊薄得更多，而当前 fallback per-row 几乎不变，所以版本倍率随 batch 增大到 42.90×。这是测得的现象；没有 profile 时不把它归因到某一个内部步骤。

## 结果四：selection 与 NULL

当前分支，1024 physical rows，单位 ns/selected-row。

| workload | dense | null10 | sparse quarter | reverse | duplicate |
|---|---:|---:|---:|---:|---:|
| `integer_add` | 265.9 | 271.2 | 283.8 | 265.6 | 266.6 |
| `integer_add_literal` | 270.7 | 263.6 | 276.3 | 259.1 | 260.1 |
| `integer_add_nested` | 504.9 | 501.1 | 525.0 | 504.1 | 501.4 |
| `strcmp` | 1,253.8 | 1,249.2 | 1,244.9 | 1,253.5 | 1,257.4 |

`sparse_quarter` 的 selected rows 为 256，其余为 1024；表按 selected row 归一化。reverse/duplicate 真实保留 selection 顺序与重复 occurrence。三个 numeric workload 在计时外逐输出用独立公式验证；`strcmp` 验证 row count 与包含全部输出 occurrence 的 aggregate checksum（reference 来自同一 scalar expression implementation）。

## 稳定性与局限

- 最不稳定 batch cell 的 5 个配对倍率为 12.91×–14.42×，ratio MAD/median 为 5.21%；其余 cell 更低。完整 process medians、paired ratios、MAD 和 bootstrap interval 均在 summary JSON。
- 进程前后端点 snapshot 中 Tctl 最高 86°C；calibration、cycles/ref-cycles 和 pair 结果没有出现对应的持续降频信号。CPU2 endpoint snapshot 为 3.43–5.55 GHz，process-level cycles/ref-cycles 为 1.240–1.253；这不声称捕获进程内部的瞬时温度/频率峰值。
- 这是单核、热 suite、内存中 `Chunk` 的 expression evaluator microbenchmark。它不包含 SQL parse/plan、storage、RPC、scheduler、多并发、server protocol，也不等价于 QPS。
- 当前结果已经回答 batch scaling 和 route 差异；下一步性能优化应先 profile numeric output/materialization 边界，并扩大 TiKV batch admission，而不是恢复已删除的共享 pool。

## 原始证据、collection 脚本与重跑入口

- 当前 width-one probe：`rust/crates/tidb-expr/src/tests/lane_cache_performance.rs`
- 当前 real-batch probe：`rust/crates/tidb-expr/src/tests/lane_cache_batch_performance.rs`
- 冻结 width-one probe：`docs/expression-unification/evidence/lane-cache-performance-frozen-probe-round228.rs`
- 冻结 real-batch probe：`docs/expression-unification/evidence/lane-cache-batch-frozen-probe-round228.rs`
- collection runner（要求调用者提供已经构建的 `CURRENT_BIN`/`FROZEN_BIN`）：`docs/expression-unification/evidence/run-round228-performance.sh`
- paired parser：`docs/expression-unification/evidence/summarize-round228-performance.py`
- 环境/perf parser：`docs/expression-unification/evidence/check-round228-environment.py`
- 已提交原始输入：`docs/expression-unification/logs/lane-cache-performance-round228-raw/`（width-one、batch、environment 和 20 个 perf sidecar）
- 机器可读结果：`docs/expression-unification/logs/lane-cache-performance-round228-summary.json`
- 环境摘要：`docs/expression-unification/logs/lane-cache-performance-round228-environment.json`
- hash/QC receipt：`docs/expression-unification/logs/lane-cache-performance-round228.txt`

最终 libtest SHA-256：current `d0e51fc842086c8d5c3747d4ca9da723d9ff9543b82d61f63a013c1ad59e3ce8`；frozen `c92a04c228f4992b725c4e85201c87905c41f3372929a890664de12ba552f543`。

二进制由两个独立 `CARGO_TARGET_DIR` 在 pinned nightly-2026-08-22、GCC/G++ 14、`CMAKE_POLICY_VERSION_MINIMUM=3.5` 下执行 `cargo test --release --locked -p tidb-expr --lib --no-run` 构建。frozen worktree 固定在 `364aef2b`，只把两个 retained probe 复制到 `src/tests/` 并注册 test modules；这些 test-only 文件不改变 frozen production runtime。collection 脚本不是 turnkey checkout/build orchestrator，因此重跑者必须自行完成该构建步骤并记录新 binary hash；本轮审计可以直接从已提交 raw inputs 重新生成两个 summary JSON。
