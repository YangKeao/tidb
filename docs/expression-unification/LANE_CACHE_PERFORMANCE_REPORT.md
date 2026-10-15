# Lane cache 与真实 batch 表达式性能报告

历史报告日期：2026-10-08

TiDB 当前提交：`f5109ab113f75ebab762fef5e292efaa54542a73`（运行时代码与 `c1072a80` 相同，本轮只增加 probe/test-only route 观测）

冻结 TiDB 基线：`364aef2bab5cc633ecb76a775ae8f36f86a6687d`

## 最新采用结果：Decimal cell 只做一次 checked decode（2026-10-16）

输入直接使用每列一次的 raw-cell view，经已有 checked decoder 进入原逻辑归一化，删除了旧 typed-cell/facade 输入适配器与重复校验。三组 dense Decimal PLUS **149.513→132.998 ns/row**（1.12 倍），LT **94.040→78.179**、NULL-safe equality **94.095→77.907**（约 1.20–1.21 倍）。原始 native PLUS **70.492**，当前仍慢约 1.89 倍。

其他 M19 对照基本持平或改善；M15 独立控制中的旧慢项仍可见，未宣称已解决。全部 2310 条主测量 / 165 组（含 33 个 native/current 组）和 840 条历史控制 / 60 组一致。datatype 516 / 565、batch 28 与完整 TiDB 1379 通过/原有 4 失败，旧输入适配器生产源码搜索为零。

[完整结果、删除审计、严格字节/资源对照与全部慢项](logs/performance-m20-decimal-cell-decode/summary.md)。以下是历史阶段。

---

## 历史结果：通用 vector append 内联（2026-10-16）

保留一行 inline hint 改动，移除已观察到的 Decimal vector append 调用边界。三组 dense Decimal PLUS **158.749→149.122 ns/row**（1.07 倍），LT **102.791→94.233**、NULL-safe equality **103.245→94.419**（1.09 倍）。Duration **15.878→14.919**、JSON **131.806→127.864** 也改善。

慢项仍保留：Real **13.319→13.989**，Datetime **10.406→10.761**、Timestamp **10.427→10.731**；不声称普遍提升。M15 独立四项控制仍有剩余回归。全部 2310 条主测量 / 165 组及 840 条历史控制 / 60 组一致；独立 Decimal append 符号由四个变为零，ELF text 增加约 45KB。

[完整结果、历史控制、代码生成与验证](logs/performance-m19-vector-append/summary.md)。下一步审查输入的重复物理/逻辑校验，继续沿用同一个 driver 和值域契约。

---

## 历史实验：新对象构造精简未采用（2026-10-16）

profile 引出的构造精简候选未带来清晰收益：三组 dense Decimal PLUS **156.558→158.373 ns/row**，LT **100.104→102.923**、NULL-safe equality **99.938→102.966**，其余控制基本持平或略慢。所有正确性/checksum 通过，但不足以保留仅为性能增加的代码。

候选已完整撤回，生产源码保持下面的 M17 实现；本次仅发布[实验结果、候选 patch、全部样本与决定](logs/performance-m18-decimal-construction/summary.md)。2310 条主测量 / 165 组及 840 条 M15 历史控制 / 60 组均匹配。此前的慢项仍保留，未宣称已解决。

---

## 历史结果：Decimal 输出直接构造 cell（2026-10-16）

移除输出边界的系数 Vec 分配与数字往返，保留原有严格逻辑校验、跨度预算和先校验后发布。130,236 组严格/宽值各测六种限制，并对 197,056 个 cell 复核输出字节。三组 dense Decimal PLUS **183.142→158.790 ns/row**（1.15 倍）；真实 native **70.171**，仍慢约 2.26 倍。

Int/Datetime 相比 M16 略有恢复；独立 M15 对照中仍慢约 2%–3%。本轮 Duration **14.735→15.918**、LOWER **33.147→34.550** 也变慢，全部保留，不声称普遍提升。主测量 2310 条 timing / 165 个 checksum 组（含 33 个 true-native/current 组）一致；另有 M15 控制 420 条 / 30 组一致。

[完整结果、慢项、独立基线、验证和 CPU profile](logs/performance-m17-decimal-output/summary.md)。profile 显示输入投影、vector push 和构造仍占主要成本；下一步针对构造中的重复存储检查，不改算术语义。以下是历史阶段。

---

## 历史结果：Decimal 输入的逻辑 words 转换（2026-10-16）

移除 batch 输入的中间系数对象，直接按原值语义生成共享 words。197,056 个 cell 的逐字段等价检查通过，包括非规范 padding、负零、可见 scale 扩展和资源边界。三组 dense 对照中，Decimal PLUS **228.438→182.680 ns/row**（1.25 倍），LT **148.866→102.485**、NULL-safe equality **148.356→102.584**（均约 1.45 倍）。

这不是所有 workload 都变快：Int PLUS **12.878→13.569**，单独复核也确认约 5% 的变慢；Time 控制项约 4% 变慢，其余控制项基本持平。全部结果保留，尚未建立这些控制项差异的具体原因。Decimal PLUS 仍比真实 native **70.089 ns/row** 慢约 2.61 倍；输出侧的中间系数转换尚待优化。

主测量 2310 条 timing、165 个 checksum 组及 33 个 native/current dense 组一致；另有 Int 复核 210 条 timing / 15 组通过。[完整结果、慢项、重复范围与验证](logs/performance-m16-decimal-cell/summary.md)。以下是历史阶段。

---

## 历史结果：窄整数类型（2026-10-16）

Tiny/Short/Int24/Long 已复用相同的 8 字节 Int carrier，signed 算术与 signed/unsigned 比较的权限不变。四种实际 SQL 类型的 dense PLUS 为 **12.840–12.879 ns/row**，真实冻结 native **21.772–21.862**；LT 为 **11.293–11.368**，真实 native **72.018–72.284**。这些类型原先不能进入 batch；未增加任何 kernel 或执行模式。

三组共 1680 条 timing、120 个 checksum 组（含 24 个 native/current dense 组）全部一致，batch 样本 prepared-only。112 组位宽/比较/unsigned 组合与四种 PLUS 边界测试通过；Year、BIT、ENUM/SET、数组和 unsigned 算术仍拒绝。完整库保持原有 4 / 6 个失败。

[完整报告、fallback 消融与真实 native 对照、原始样本和验证](logs/performance-m15-narrow-integers/summary.md)。以下是历史阶段。

---

## 历史结果：Double / Decimal 比较族（2026-10-16）

十四种比较沿用共享 registry、kernel、driver 和 protobuf 解码路径。dense 1,024 行：Double batch **11.618–11.846 ns/row**，真实冻结 native **80.363–80.967**（约 6.78–6.96 倍）；Decimal batch **147.140–147.545**，真实 native **166.678–178.813**（约 1.13–1.21 倍）。

同一当前二进制的 fallback 消融分别约 1209–1226、1604–1655 ns/row；这是另一种对照，不能把约 103–105 倍 / 11 倍当成历史提升。2940 条 timing、210 个 checksum 组及 42 个 native/current dense 组一致；全部 batch 样本 prepared-only。浮点 NaN、raw FLOAT、unsigned Decimal 与隐式混合类型转换仍不在范围内；Decimal PLUS 的已知 native 差距仍未消除。

[完整比较表、全部形状、重复测量、版本/hash 与验证](logs/performance-m14-numeric-comparisons/summary.md)。以下为历史阶段。

---

## 历史结果：LongLong 比较族（2026-10-16）

新增 LT/LE/GE/NE/NULL-safe equality，GT/EQ 原先已有 batch。七种均补齐 signed/unsigned LongLong 参数支持；算术仍保持 signed-only。dense 1,024 行、ns/selected-row：

| comparison | current fallback ablation | batch | ablation speedup | frozen native | native / batch |
|---|---:|---:|---:|---:|---:|
| `eq` | 1090.482 | 11.164 | 97.71x | 71.327 | 6.39x |
| `ge` | 1088.026 | 11.160 | 97.49x | 72.624 | 6.51x |
| `gt` | 1095.315 | 11.229 | 97.57x | 71.732 | 6.39x |
| `le` | 1085.500 | 11.197 | 96.88x | 72.077 | 6.44x |
| `lt` | 1088.006 | 11.192 | 97.20x | 71.763 | 6.41x |
| `ne` | 1096.537 | 11.183 | 98.19x | 71.978 | 6.44x |
| `nulleq` | 1137.923 | 11.283 | 100.85x | 78.756 | 6.98x |

“current fallback ablation” 是同一当前二进制内的消融，不是历史实现；“frozen native” 来自真实冻结版本，当前 batch 比这组原始 native 快约 6.39–6.98 倍。1470 条当前 timing / 105 个 checksum 组及 21 个 native/current dense 组均一致。性能输入为 signed；unsigned 正确性另由极值、NULL、列/常量和跨窗口测试验证。

完整方法、版本/hash、全部样本与验证见[比较族报告](logs/performance-m13-integer-comparisons/summary.md)。仍不覆盖所有 raw integer 类型、隐式转换或所有函数；[覆盖计划](FUNCTION_PERFORMANCE_COVERAGE.md) 记录边界。以下是历史阶段。

---

## 历史结果：变长序列化与输入视图（2026-10-16）

JSON/Vector 普通输出缓冲直接序列化，Int/Real/Bytes/JSON 输入每批持有一次 raw view，JSON 少一次中间 payload 拷贝。三组配对，dense 1,024 行、ns/selected-row：

| workload | M11 | M12 | paired speedup | frozen native (fresh) |
|---|---:|---:|---:|---:|
| `any_value_datetime` | 10.826 | 10.091 | 1.07x | 117.316 |
| `any_value_duration` | 14.965 | 14.448 | 1.04x | 119.242 |
| `any_value_json` | 136.204 | 122.171 | 1.11x | 137.705 |
| `any_value_timestamp` | 10.767 | 10.086 | 1.07x | 116.551 |
| `any_value_vector` | 83.312 | 72.932 | 1.14x | 158.377 |
| `decimal_add` | 230.038 | 228.664 | 1.01x | 73.061 |
| `integer_add` | 13.170 | 12.836 | 1.02x | 21.111 |
| `lower` | 36.084 | 32.570 | 1.11x | 148.327 |
| `real_add` | 14.317 | 13.863 | 1.03x | 109.633 |

JSON 136.204→122.171、Vector 83.312→72.932、LOWER 36.084→32.570；Decimal 变化约 1%，基本持平。另行复测真实原始 native 后，九个代表 workload 中八个更快，Decimal 仍慢约 3.13 倍。不能据此声称所有函数或 SQL workload 已更快。

1,890 条旧/新 timing / 135 个 checksum 组一致，另有 9 个 dense native/current checksum 组一致。仍有 owned typed 输入和中间结果，并未实现跨库零拷贝。完整版本、条件、样本及验证见[本轮报告](logs/performance-m12-varlen-transport/summary.md)。以下是历史阶段。

---

## 历史结果：Decimal 直接构造 word owner（2026-10-16）

共享 Decimal 构造器去掉临时 word Vec、重复校验及拷贝，保留原有输入与资源检查。三组旧/新配对，dense 1,024 行、ns/selected-row：

| workload | M10 | direct word owner | paired speedup |
|---|---:|---:|---:|
| `any_value_datetime` | 10.567 | 10.777 | 0.98x |
| `any_value_duration` | 14.863 | 14.923 | 1.00x |
| `any_value_json` | 135.419 | 135.900 | 1.00x |
| `any_value_timestamp` | 10.572 | 10.799 | 0.98x |
| `any_value_vector` | 81.217 | 83.039 | 0.98x |
| `decimal_add` | 250.597 | 229.120 | 1.09x |
| `integer_add` | 13.072 | 13.139 | 0.99x |
| `lower` | 35.957 | 36.050 | 1.00x |
| `real_add` | 14.374 | 14.373 | 1.00x |

本轮改善集中在 Decimal（约 9%）；其他 workload 持平或略慢，Time/Vector 约慢 2%，未隔离二进制布局与机器状态因素，不能把它们算作收益。Decimal 与 M9 单独测得的原始 native 73.172 ns/row 相比仍慢约 3.13 倍。所有 1,890 条 timing / 135 个 checksum 组一致，prepared-only。

[完整报告与原始数据](logs/performance-m11-decimal-owner/summary.md)。[函数覆盖计划与盘点](FUNCTION_PERFORMANCE_COVERAGE.md) 已补齐静态清单和验收门槛；待审项明确保留，尚未覆盖所有函数。以下为历史阶段。

---

## 历史结果：批量输出（2026-10-16）

目标列每批只获取一次，定长输出统一批量写入，Decimal 每个值只转换一次。三组旧/新交错对照，dense 1,024 行、ns/selected-row：

| workload | M9 vector kernel | M10 bulk output | paired speedup |
|---|---:|---:|---:|
| `any_value_datetime` | 15.768 | 10.484 | 1.50x |
| `any_value_duration` | 19.681 | 14.731 | 1.33x |
| `any_value_json` | 142.699 | 135.732 | 1.05x |
| `any_value_timestamp` | 15.788 | 10.563 | 1.49x |
| `any_value_vector` | 83.575 | 82.299 | 1.02x |
| `decimal_add` | 289.160 | 251.091 | 1.15x |
| `integer_add` | 17.322 | 13.039 | 1.33x |
| `lower` | 37.692 | 35.952 | 1.05x |
| `real_add` | 18.472 | 14.236 | 1.30x |

Int 降至 13.039 ns/row；Decimal 降至 251.091，仍明显慢于原始 native。Vector 的小幅变化不足以认定稳定收益。1,890 条 timing / 135 个 checksum 组均一致，NULL/sparse/reverse/duplicate 均走 prepared。仍有 owned input import 和 TiKV 中间结果分配，尚未实现跨库零拷贝或 kernel 直接写目标 Chunk。

完整条件、版本、全部样本和验证见[批量输出报告](logs/performance-m10-transport/summary.md)。以下为各阶段历史记录。

---

## 历史结果：整批 kernel（2026-10-16）

删除逐 lane 的 singleton kernel 调用，改为每个函数节点调用一次完整 vector kernel。三组旧/新二进制交错对照，dense 1,024 行（ns/selected-row）：

| workload | before | vector kernel | speedup (paired) | frozen native (fresh) |
|---|---:|---:|---:|---:|
| `any_value_datetime` | 42.812 | 15.755 | 2.72x | 118.505 |
| `any_value_duration` | 49.070 | 19.711 | 2.49x | 120.386 |
| `any_value_json` | 214.989 | 143.059 | 1.50x | 140.665 |
| `any_value_timestamp` | 42.820 | 15.687 | 2.73x | 118.747 |
| `any_value_vector` | 176.592 | 83.071 | 2.13x | 155.379 |
| `decimal_add` | 536.500 | 288.082 | 1.86x | 73.172 |
| `integer_add` | 52.792 | 17.370 | 3.05x | 21.196 |
| `lower` | 104.976 | 37.813 | 2.78x | 148.969 |
| `real_add` | 55.483 | 18.447 | 3.00x | 109.856 |

旧/新对照共 1,890 条 timing、135 个 checksum 组全部一致，TiKV 均 prepared，fallback/refusal 为 0。Int 的逐 wrapper 开销已消除，当前为 17.370 ns/row；原始 native 另行复测为 21.196。Decimal 仍约慢 3.94 倍、JSON 约慢 1.02 倍。输入 import 和输出 append 仍存在，下一步继续减少这部分搬运。

表中 speedup 是三组旧/新配对倍率的中位数；native 是随后独立三个进程复测，不属于该配对倍率。方法、全部原始记录、版本/hash 和验证范围见[本轮完整报告](logs/performance-m9-vector-kernel/summary.md)。以下 M8 及 round228 均保留为历史记录。

---

## 历史结果：全 EvalType batch（2026-10-12 测量，2026-10-15 发布）

这轮实现使用共享 eager batch registry/driver，覆盖 Int、Real、Decimal、String、Datetime、Timestamp、Duration、JSON 和 VectorFloat32 的 owned typed carrier；下面每类只测一个代表 workload，不能外推为该类所有函数或 SQL server QPS。

真实历史版本的 dense、1,024 行中位数如下，单位 ns/selected-row（越低越好）：

| TiDB EvalType / workload | original native | historical TiKV batch | optimized TiKV batch | optimized vs original native |
|---|---:|---:|---:|---:|
| Int / `integer_add` | 21.324 | 253.812 | 55.491 | 2.60x slower |
| Real / `real_add` | 111.344 | N/A | 56.781 | 1.96x faster |
| Decimal / `decimal_add` | 73.998 | N/A | 548.599 | 7.41x slower |
| String / `lower` | 150.011 | N/A | 113.659 | 1.32x faster |
| Datetime / `any_value_datetime` | 119.524 | N/A | 43.268 | 2.76x faster |
| Timestamp / `any_value_timestamp` | 119.799 | N/A | 42.469 | 2.82x faster |
| Duration / `any_value_duration` | 121.732 | N/A | 49.928 | 2.44x faster |
| JSON / `any_value_json` | 140.874 | N/A | 220.669 | 1.57x slower |
| VectorFloat32 / `any_value_vector` | 155.959 | N/A | 181.934 | 1.17x slower |

Int PLUS 比优化前 TiKV batch 快 **4.58 倍**，仍比原始 native 慢 **2.60 倍**。其他八类在历史 TiKV 的对应 batch route 尚不存在，因此标为 N/A。原始 native 使用 TiDB `364aef2bab5cc633ecb76a775ae8f36f86a6687d` / TiKV `548812e1ef57aef077a2062a9cc356640a6347f5`；优化前使用 TiDB `f388b337dfad156233904d5407230f8439e0150e` / TiKV `608bbfc9c63e8b50fa2be757533eba572ef9b874`；优化后是上述后一对提交加本次发布的 performance patch。测量时的 binary hash、原始样本及同机条件见[完整结果和证据](logs/performance-m8-all-carriers/summary.md)。这次发布复用已完成的测量，没有把文档发布日期当作一次新 benchmark。

**口径纠正：**早先 675 条同一二进制的消融结果中，`native_reference` 实际是当前迁移后的 fallback，`original_tikv` 实际是当前代码强制单行 window。源码已改为 `current_fallback_ablation` / `current_tikv_row_ablation`；它们只能解释当前代码内的 batch 收益，不能代表历史实现。“Int 比原始 native 快 16.89 倍”的说法已撤回。原始 raw log 保留旧标签并附纠正说明。

Int 剩余开销主要是 generated ArgConstructor/Evaluator wrapper（32.43% self cycles）、`finish_batch`（9.99%）和输入 import（6.19%），实际加法只有 0.88%。当前 `finish_batch` 仍逐 lane 调用 singleton kernel，边界仍复制输入并逐行 append 输出；完整 vector kernel 调用与借用/复用输入、直接输出仍是后续优化，不能宣称已消除这些成本。

最新已有验证：TiDB batch 25/25、TiKV registry/profile 48/48；完整库仍各有 4/6 个已记录的基线失败。server/FIPS/DistSQL/73-of-75 真实集群结果来自更早 checkpoint，未因这轮 carrier 扩展重跑。以下 round227/228 内容保留为历史测量，其中“当前”均指当时提交。

---

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

### 真实 server SQL smoke 的补充边界

round230 后续启动了真实PD/TiKV/TiDB进程，并通过MySQL protocol在4,096行表上验证了少量整数、控制流、字符串、Decimal、JSON、regexp、MD5、prepared parameter和warning SQL；前述value/prepared结果与独立oracle逐字节一致，warning 1292另行观察且不在该TSV diff内，`EXPLAIN`确认两组代表查询进入`cop[tikv]`。单client warm sequential smoke中，2,000条integer pushed aggregate/filter三次耗时1.58/1.55/1.50s，1,000条mixed查询为0.88/0.88/0.84s。

这不会把本报告的microbenchmark升级为production QPS结论：TiKV是unoptimized dev build、没有frozen comparator，也没有并发或持久化压力。更关键的是，Go `tidb-server`不链接实验性`rust/crates/tidb-expr`，所以该真实SQL运行验证的是Go TiDB host→当前TiKV coprocessor链路，而不是TiDB Rust lane cache。详见`logs/real-cluster-sql-smoke-round230.txt`。

round231 已纠正 target：所有验证 SQL 都发往 `rust/crates/tidb-server` 的真实 MySQL endpoint，`EXPLAIN` 显示 PLUS 位于 Rust root `Projection`、仅 scan 在 `cop[tikv]`；4,096-row 单层/嵌套 PLUS 与独立 oracle 逐字节一致。它证明 production `ProjectionExec`/`run_with_tikv_numeric` entry 在真实 PD/TiKV 链路上运行，但 binary 没有 lane-selected telemetry，不能把 `NumericVectorLane::Prepared` 当作 live hard observation。本轮还发现 default allocator build conflict、3 个 stats background-thread panic，以及 Rust `region_cache` unique-ownership non-clean shutdown；强制 process/port/data 清理仍完成。计时包含 MySQL serialization 且波动明显，仍不属于可用于替代本报告 evaluator microbenchmark 的性能结论。详见 `logs/rust-real-cluster-sql-smoke-round231.txt`。

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
