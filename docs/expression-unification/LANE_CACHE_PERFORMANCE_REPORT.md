# Lane-cache-only 表达式运行时性能报告

日期：2026-10-08

TiDB 当前提交：`c1072a8004c5650189420a939dc27f3d569126e0`

冻结 TiDB 基线：`364aef2bab5cc633ecb76a775ae8f36f86a6687d`

## 结论摘要

1. **lane cache 相对当前分支的 one-shot fallback 有明确收益。** 在 AST/value tier 的 6 个 workload 上，中位数下降 **11.2%–48.2%**，几何平均下降 **38.0%**；在 standalone chunk tier 上下降 **30.5%–46.9%**，几何平均下降 **41.9%**。
2. **删除共享 worker pool 后，executor lane 持有 cache 的方向是有效的。** 轻量 workload（整数加法、`IF`、`STRCMP`）约节省 44%–48%；AST `REGEXP_LIKE` 只节省 11.2%。后者与昂贵 kernel 摊薄固定边界成本的解释一致，但本轮没有 profile，不能据此做因果归因。
3. **与冻结原生实现相比仍有明显性能债务。** 当前 lane-cache AST 中位数仍为基线的 **1.225×–29.121×**，chunk tier 为 **5.200×–43.835×**。lane cache 去除了反复 prepare/drop 的一部分成本，但没有消除 adapter、参数物化、类型转换和跨边界调用成本。
4. 本报告证明的是**热 cache、单行、未折叠常量 kernel 的 evaluator 开销**，不是 SQL server QPS、真实 scan/selection 批量吞吐或端到端延迟。

## 方法

### 对比对象

- `frozen_native`：冻结提交 `364aef2b`，直接执行当时的原生 evaluator。
- `one_shot`：当前提交不绑定 `ReadyValueCache`；每次 routed operation 创建并销毁临时 cache/worker。
- `lane_cache`：当前提交为每个 workload 创建一个持久 `ReadyValueCache`，在整个热测量期间绑定到 evaluation lane。

AST 与 chunk tier 只在各自 tier 内比较；它们的 dispatch/coercion 路径不同，不能用二者绝对值归因 cache 成本。

### 采样纪律

- release profile，固定 CPU 2（`taskset -c 2`）。
- 解析和 chunk rewrite 均在计时区间外。
- 每个 workload 先做 2,000 次 warmup。
- 每进程每 workload 9 个 correlated samples；每 revision/mode 3 个独立进程，共 27 个 observations。
- revision 进程顺序：current / baseline / baseline / current / current / baseline。
- 当前分支在每个 workload 内使用平衡 ABBA/BABA 顺序交错 `one_shot` 与 `lane_cache`，避免 mode 与温度/频率漂移固定相关。
- 每个 workload 使用固定 SQL 结果断言；lane 测量后断言 `prepared_worker_count() > 0`，避免把 bypass workload 错记成 cache 命中。
- 表中是 27 个 observation 的中位数；完整 min/max 是观察范围，不是置信区间。

主机为 AMD Ryzen 9 9900X（12C/24T）、46 GiB RAM、Linux 7.1.8；toolchain 为项目固定的 nightly-2026-08-22。

## 结果：AST/value tier

单位：ns/eval；越低越好。

| workload | frozen native | current one-shot | current lane cache | lane / one-shot | lane 降幅 | lane / frozen |
|---|---:|---:|---:|---:|---:|---:|
| `integer_add` | 32.373 | 1,819.074 | 942.720 | 0.518× | 48.2% | 29.121× |
| `lazy_if` | 93.143 | 3,563.882 | 1,931.111 | 0.542× | 45.8% | 20.733× |
| `strcmp` | 163.677 | 2,339.523 | 1,301.113 | 0.556× | 44.4% | 7.949× |
| `decimal_add` | 321.666 | 3,323.598 | 2,240.418 | 0.674× | 32.6% | 6.965× |
| `json_type` | 226.483 | 1,813.678 | 1,100.992 | 0.607× | 39.3% | 4.861× |
| `regexp_like` | 7,604.944 | 10,496.124 | 9,315.557 | 0.888× | 11.2% | 1.225× |
| **几何平均倍率** | — | — | — | **0.620×** | **38.0%** | **7.641×** |

三个进程各自的 sample median 在各 mode/workload 上相差 0.2%–2.4%，结果稳定。`decimal_add` 的 lane 全范围有一个 3,092.502 ns/eval 离群点，但三个进程 median 分别为 2,238.6、2,231.1、2,243.6 ns/eval，不影响结论。

## 结果：standalone chunk tier

单位：ns/eval；越低越好。

| workload | frozen native | current one-shot | current lane cache | lane / one-shot | lane 降幅 | lane / frozen |
|---|---:|---:|---:|---:|---:|---:|
| `integer_add` | 92.416 | 1,904.730 | 1,032.651 | 0.542× | 45.8% | 11.174× |
| `lazy_if` | 43.603 | 3,534.536 | 1,911.341 | 0.541× | 45.9% | 43.835× |
| `strcmp` | 75.748 | 2,291.848 | 1,216.726 | 0.531× | 46.9% | 16.063× |
| `decimal_add` | 469.011 | 3,506.964 | 2,438.929 | 0.695× | 30.5% | 5.200× |
| `json_type` | 199.407 | 1,830.467 | 1,122.176 | 0.613× | 38.7% | 5.628× |
| `regexp_like` | 239.975 | 2,499.003 | 1,441.767 | 0.577× | 42.3% | 6.008× |
| **几何平均倍率** | — | — | — | **0.581×** | **41.9%** | **10.556×** |

三个进程各自的 sample median 在各 mode/workload 上相差 0.1%–3.2%。chunk `REGEXP_LIKE` 与 AST `REGEXP_LIKE` 的绝对值不可直接比较：两个 tier 使用不同的表达式结构和 regexp 状态复用路径。

## 与上一轮 pooled 结果的关系

历史报告记录的 pooled/current-default 数据来自旧提交 `a3cf882…` 和旧的共享 execution/pool 结构。新的 AST lane-cache 中位数在 6 个可比 workload 上都低于历史 pooled 中位数，方向上支持删除 pool 状态机；但两轮没有共同的、交错运行的 revision 二进制，因此这些跨轮数字只作为定性背景，不作为本报告的配对结论。

本轮重新编译并交错运行了冻结提交和当前提交，因此本报告的 `frozen_native` / `one_shot` / `lane_cache` 表才是主要对比。

## MD5

`md5('abc')` 未进入性能表。当前 release 路径已有已知的 `ExpressionRuntimeFailure / InvalidSpecification / Prepare`，无法取得有效 latency。benchmark 配置行明确记录 `md5=excluded-known-release-prepare-failure`；这仍是兼容性问题，不应被解读为性能通过。

## 解读与下一步

- lane ownership 避免了 one-shot 路径的逐调用 worker preparation/drop，测得的 evaluator 总成本显著下降，同时没有再引入共享 pool 的生命周期和锁状态机；本轮未单独测量各子成本。
- 一个待 profile 验证的假设是：轻量表达式的剩余差距来自 ready-value 参数构造、adapter dispatch、结果转换或分配。下一轮应先隔离测量这些边界，而不是恢复共享 pool。
- 生产路径还需要增加 column-backed、1024-row selection-aware batch benchmark，以及端到端 executor benchmark；本报告没有声称这些场景已测量。
- AST `REGEXP_LIKE` 的 1.225× 与“昂贵 kernel 能摊薄固定边界成本”这一假设相符，但不是该假设的证明。优化优先级可先放在测得差距更大的整数、控制流和短字符串 workload。

## 可复现入口与证据

当前分支永久 probe：`rust/crates/tidb-expr/src/tests/lane_cache_performance.rs`

本轮冻结基线 probe 原文：`docs/expression-unification/evidence/lane-cache-performance-frozen-probe-round227.rs`（SHA-256 与编译时临时源码一致）

从 TiDB 仓库 `rust/` 目录构建当前 probe：

```bash
../../tools/cargo-tidb test --release --locked -p tidb-expr --lib --no-run
```

固定 CPU 运行（将 `<test-binary>` 替换为 cargo 输出的 libtest binary）：

```bash
taskset -c 2 <test-binary> \
  'tests::lane_cache_performance::lane_cache_release_performance_probe' \
  --ignored --exact --nocapture --test-threads=1
```

详细摘要与 evidence hash：`docs/expression-unification/logs/lane-cache-performance-round227.txt`。历史性能报告保留为历史记录，不被改写成本轮结果。
