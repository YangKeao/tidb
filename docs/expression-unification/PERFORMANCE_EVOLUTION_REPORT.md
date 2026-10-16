# Expression unification：调优历程与最终性能对照

测量日期：2026-10-16。生产代码停在 M30：TiDB `c06f259300553625416981dafbb86ce5fa2dd991` / TiKV `d32cebb3ebc353748cb225fe254d6893e62c8bdc`。本轮只扩展测试探针和报告，没有继续修改生产算法。

**结论：大 batch 的已准入运算获得明显收益，但单行和若干 fallback workload 仍然很慢，Decimal PLUS 也仍落后于原始 native。** 所有结果、包括大于1的慢项，都列在表中；不能用 PI 或少数热点代表全部函数。

## 三种版本与读表方法

- **frozen native**：统一前的原始 evaluator，TiDB `364aef2bab5cc633ecb76a775ae8f36f86a6687d` / TiKV `548812e1ef57aef077a2062a9cc356640a6347f5`。只加测试探针，生产源码不改。
- **未做 M1–M30 调优的统一版**：真实历史 TiDB `f388b337dfad156233904d5407230f8439e0150e` / TiKV `608bbfc9c63e8b50fa2be757533eba572ef9b874`，已有前期 lane cache，尚无后续 batch 优化。本轮保留原二进制作PLUS控制，另按同一SQL重建仅改测试探针的版本作13项直接对照；其输出 route 名 `tikv_numeric` 是当时的名称。
- **最终 unified**：M30 生产代码，统一 batch 入口；不准入的 workload 进入当前唯一 canonical fallback。它不是 frozen native，也不能冒充历史统一版。

所有最终倍率都按截图方向 **unified / frozen native**；**小于1更快，大于1更慢**。单位为 ns/eval 或 ns/selected-row，延迟越低越好。历史逐步表中另外明确写出的“加速x”则是 before/after，不能混用。`batch=1` 与直接 AST/value 单次 eval 是不同入口。

本轮64个dense1024测例中57个更快、7个更慢（只是该测例集，不能当作SQL覆盖率）。Int PLUS为12.901ns/row，对native的配对耗时倍率0.597x；另组真实历史统一版对照中，Int PLUS为288.146→12.841ns/row，配对加速22.38x。

## 从最初统一版到最终版：逐条改动与当时测得的收益

单位除另注外均为 **ns/selected-row，batch=1024**。下面是各阶段自己的实测，**不是同一轮采样，不应把倍率连乘**。新增准入的函数原先常常没有 TiKV batch 实现；其收益使用真实 frozen native 对照，并不伪造一个“旧版 batch”。没有独立对照的数据明确写“未单独量化”。

| 阶段 | 实际改动 | 当时的代表性结果 | 结论 / 证据 |
|---|---|---|---|
| 前期 lane cache | 删除共享 worker pool，改为执行 lane 持有 ReadyValueCache，复用已准备的 worker | 同一版本 one-shot→持有 cache 的消融：AST 几何平均耗时降低37.9%，standalone chunk 降低41.6%；Int AST1870.231→970.956ns/eval | 保留。这里量化的是 cache 生命周期复用，不能把全部差异归因于删除 pool；[原记录](LANE_CACHE_PERFORMANCE_REPORT.md#结果一稳定重跑-width-one) |
| M1 | 增加生产 prepared/fallback/refusal 计数、拒绝原因和真实 route 观测 | 未单独量化吞吐收益；作用是确保后续数据确实测到 batch 路径 | 保留；[执行计划](../../rust/docs/expression-unification-performance-execplan.md) |
| M2 | 准备时 seal 不变的表达式/FieldType 元数据，避免每次深拷贝/重验；复用有界 selection scratch | Int285.179→279.861，约1.019x，耗时降低1.9% | 保留；没有做未经实测的 HashMap→dense-slot 改写 |
| M3 | 使用 TiKV 现有 whole-selection driver，删除并行 decoded/raw evaluator 和临时 route flag | 与 M4 联合验收；未把共享收益拆成两项独立提升 | 保留；同一生产入口处理整个 selection |
| M4 | 初期 Int 路径用 checked Chunk binding，直接写目的 Chunk，删除旧 decoded 输入/Vec<Datum>/逐行 append_datum 中间适配层 | M3+M4 后 Int131.879；相对最早285.179耗时下降53.8%，约2.16x | 保留；这是早期 Int 实现的作用域，**不表示最终所有 typed VectorValue 都已零拷贝** |
| M5 | PLUS 的同一基座扩展到 MINUS、MUL、ABS，不新增函数专用 driver | 当时 MINUS131.278、MUL127.335、ABS81.483；没有对应独立旧版值，不报逐项版本收益 | 保留；准入/复用扩展 |
| M6 | selection 超过1024时继续分窗执行；统一 allocator；清理迁移开关/重复入口；完成 server、FIPS、集群验证并修复 allocator/stats 阻塞 | 大 selection 不再因1024边界整体 fallback；allocator、清理和修复没有单独隔离计时 | 保留；避免性能断崖和构建/运行故障，不能虚报独立加速倍率 |
| M7 | 共享 capability registry、异构参数和通用 arity；扩展 Bytes、Real、Decimal 运算与跨载体输出 | 早期 Real PLUS60.679、Decimal PLUS563.610；原报告1178.884/1506.392是**同一版本 fallback 消融**，不是历史 native | 保留；不把消融的19.43x/2.67x当作版本/native收益 |
| M8 | typed carrier 覆盖全部 native EvalType；Datetime/Timestamp、Duration、JSON、Vector 走同一个 batch driver | 真实历史统一版 Int253.812→55.491（4.58x）；Real56.781、Decimal548.599；Decimal 仍是 native73.998的7.41倍耗时 | 保留，但覆盖完成不等于全变快；[三种基线纠正记录](logs/performance-m8-all-carriers/summary.md) |
| M9 | 每批只调用一次完整 vector kernel，替代每个 lane 的 singleton wrapper 调用，删除 singleton view/SmallVec 适配 | Int52.792→17.370（3.05x）；Real55.483→18.447（3.00x）；Decimal536.500→288.082（1.86x）；LOWER104.976→37.813（2.78x） | 保留；[记录](logs/performance-m9-vector-kernel/summary.md) |
| M10 | 一次借用目的列，通用 nullable fixed-cell 批量输出；Bytes 批量 append；Decimal 只转换一次并计入预算 | Int17.322→13.039（1.33x）；Real18.472→14.236（1.30x）；Time15.768→10.484（1.50x）；Decimal289.160→251.091（1.15x） | 保留；[记录](logs/performance-m10-transport/summary.md) |
| M11 | Decimal 直接填最终 word owner，省掉临时 word Vec、第二次扫描和复制 | Decimal250.597→229.120（1.09x）；Time/Vector约慢2%，同样保留在报告中 | 保留该取舍；[记录](logs/performance-m11-decimal-owner/summary.md) |
| M12 | JSON/Vector 直接序列化，变长输入使用 raw-cell view | JSON136.204→122.171；Vector83.312→72.932；LOWER36.084→32.570；Decimal基本持平 | 保留；[记录](logs/performance-m12-varlen-transport/summary.md) |
| M13 | 补齐7种 LongLong 整数比较，支持 signed/unsigned 输入并保持原 selector | batch约11.16–11.28，真实 native71.33–78.76，约6.4–7.0x | 保留；同一 driver；[记录](logs/performance-m13-integer-comparisons/summary.md) |
| M14 | 14个同域 Double/Decimal 比较签名复用 registry/PB decoder | Double11.618–11.846 vs native80.363–80.967；Decimal147.140–147.545 vs166.678–178.813 | 保留；当时 Decimal 比较收益还很小；[记录](logs/performance-m14-numeric-comparisons/summary.md) |
| M15 | Tiny/Short/Int24/Long 复用八字节 Int carrier，明确排除 Year/hybrid | 窄整数 PLUS12.840–12.879 vs native21.772–21.862，约1.7x；LT11.293–11.368 vs72.018–72.284，约6.4x | 保留；[记录](logs/performance-m15-narrow-integers/summary.md) |
| M16 | Decimal 输入直接导入 logical cell words，保留不规范物理 cell 的逻辑归一化 | PLUS228.438→182.680（1.25x）；比较约148→102.5；Int/Time约慢5%/4% | 保留，退化不隐藏；[记录](logs/performance-m16-decimal-cell/summary.md) |
| M17 | Decimal 输出直接构造 cell，不经通用逻辑对象/文本路径；宽值、精度/预算保持 | PLUS183.142→158.790（1.15x）；Duration/LOWER约慢8%/4% | 保留；当时 Decimal仍比 native慢2.26x；[记录](logs/performance-m17-decimal-output/summary.md) |
| M18 | 尝试精简 Decimal 构造 | PLUS156.558→158.373，比较约慢3% | **撤回**；未留下候选代码；[记录](logs/performance-m18-decimal-construction/summary.md) |
| M19 | 通用 vector append 内联，减少调用/拷贝边界 | PLUS158.749→149.122（1.07x）；比较约103→94（1.09x）；Real/Time约慢5%/3% | 保留；[记录](logs/performance-m19-vector-append/summary.md) |
| M20 | Decimal cell 只走一次现有 checked byte decoder，删除旧 typed-cell 适配 API 和重复校验 | PLUS149.513→132.998（1.12x）；比较约94→78（约1.21x） | 保留；[记录](logs/performance-m20-decimal-cell-decode/summary.md) |
| M21 | canonical Decimal head 已满足位数边界时跳过余数运算 | PLUS132.335→129.283；比较约77.8→74.9；完整倒数算法还快1.8–2ns，但复杂度更高 | **仅保留一个范围判断**，完整倒数方案撤回；[三方记录](logs/performance-m21-head-bound/summary.md) |
| M22 | 尝试仅内联 Decimal 构造 | PLUS129.435→130.475，比较持平或更慢 | **撤回**；符号消失不等于性能改善；[记录](logs/performance-m22-constructor-inline/summary.md) |
| M23 | 位 AND/OR/XOR/左右移；共享“固定 UNSIGNED 结果”类型事实 | 五项11.936–12.027 vs true native79.651–80.606，约6.6–6.7x；旧 Int/Real/Decimal有1–3%退化 | 保留功能收益与取舍；[记录](logs/performance-m23-bitwise/summary.md) |
| M24 | 7种 IS NULL 签名，共享非 NULL Boolean 结果规则；JSON 的既有 cast 边界不绕过 | 已测8种 native 类型约为 true native的3.19–13.77x加速 | 保留；不是所有 JSON IS NULL 都直接准入；[记录](logs/performance-m24-is-null/summary.md) |
| M25 | 整数 NOT、ASIN/ACOS；只接入已证明 native/wire 共用相同数学原语的函数 | NOT8.085 vs47.038（5.82x）；ASIN10.686 vs112.748、ACOS10.756 vs112.619（约10.5x）；Decimal比较约慢1–2% | 保留；SIN/COS/ATAN/非恒等 ROUND 不能直接照搬；[记录](logs/performance-m25-unary-math/summary.md) |
| M26 | 单参数整数 ROUND 与 unsigned ANY_VALUE；结果 signedness 跟随参数，避免经过 f64 丢位 | ROUND约8.4、ANY_VALUE UInt10.726，true native约100–122，11.38–12.10x；旧项确认慢2–12% | 保留并明确取舍，随后优化共同输出成本；[记录](logs/performance-m26-integer-identities/summary.md) |
| M27 | owned fixed 输出缓冲一次定长再填充，保留 NULL scratch、别名和实际迭代终止规则 | Duration15.082→12.879（1.17x）；Time10.933→9.551；Int13.424→12.220；Real13.856→12.411；Decimal持平，JSON略慢 | 保留，修复了此前部分固定输出退化；[记录](logs/performance-m27-fixed-output/summary.md) |
| M28 | 尝试 Duration 构造内联，所有检查保留 | Duration12.939→13.472，约慢4% | **撤回**；[记录](logs/performance-m28-duration-inline/summary.md) |
| M29 | 两个输入实验：每批借用一次时间 raw view；统一 outlined fixed-input helper | 直接方案时间快3–7%，Decimal比较慢约12%；helper的 Int/Real快8–9%、Duration快5%，但 Decimal PLUS慢4%、比较慢10% | **两个都撤回**；没有保留新 helper/路径；[三方记录](logs/performance-m29-fixed-input/summary.md) |
| M30 | 只把纯零参数 Pi()->Real 接入现有 explicit-row kernel，RAND保持拒绝 | PI4.100 vs true native67.078（16.44x）；旧14项大体持平，Real/Decimal NULL-safe equality略慢 | 保留；[记录](logs/performance-m30-pi/summary.md) |

以上所有“撤回”项目都只保留实验 patch/日志，不在最终生产实现里。各阶段更早的数值不替代下面新复测的最终表，也不代表其余 workload 没有退化。

## For 1 row：直接 AST/value 单次 eval

与截图的 lane-cache 口径对应，`unified` 使用执行 lane 持有的 ReadyValueCache；parse/rewrite 在计时外。**这不是无 cache owner 的默认 one-shot 入口**，后面单列其真实结果。

| workload | frozen native (ns/eval) | unified：复用 lane (ns/eval) | unified / frozen |
|---|---:|---:|---:|
| `integer_add` | 30.997 | 1,020.009 | **32.907x** |
| `lazy_if` | 92.582 | 1,997.194 | **21.413x** |
| `strcmp` | 168.482 | 1,446.590 | **8.586x** |
| `decimal_add` | 339.710 | 2,235.248 | **6.571x** |
| `json_type` | 227.548 | 1,145.166 | **5.033x** |
| `regexp_like` | 7,700.182 | 9,123.701 | **1.188x** |

默认 one-shot 入口（每次调用没有外部持有的 cache owner），不能用上面的 lane 数字代替：

| workload | frozen native (ns/eval) | unified：one-shot (ns/eval) | unified / frozen |
|---|---:|---:|---:|
| `integer_add` | 30.997 | 1,945.561 | 62.766x |
| `lazy_if` | 92.582 | 3,693.407 | 39.818x |
| `strcmp` | 168.482 | 2,570.844 | 15.279x |
| `decimal_add` | 339.710 | 3,367.265 | 9.907x |
| `json_type` | 227.548 | 1,917.719 | 8.408x |
| `regexp_like` | 7,700.182 | 10,359.461 | 1.349x |

## For batched multiple rows：unified / frozen native

真实 Chunk、相同 SQL/输入生成器、dense selection，计时包含 output reset、production evaluator 和结果 materialization。每个格子是三个独立进程对的比值中位数；**不是每 batch 的总耗时倍率与每行倍率混用**。

| workload | 1 | 8 | 64 | 256 | 1024 |
|---|---:|---:|---:|---:|---:|
| `integer_add` | 26.976x | 9.387x | 1.924x | 0.886x | 0.597x |
| `integer_add_literal` | 21.481x | 8.839x | 1.746x | 0.743x | 0.455x |
| `abs` | 17.208x | 2.347x | 0.348x | 0.136x | 0.078x |
| `lazy_if` | 29.012x | 30.752x | 31.069x | 30.950x | 31.168x |
| `coalesce_nullif` | 17.895x | 19.172x | 19.154x | 19.729x | 19.262x |
| `strcmp` | 15.036x | 14.538x | 14.917x | 14.800x | 15.023x |
| `lower` | 11.893x | 1.836x | 0.422x | 0.260x | 0.216x |
| `upper` | 11.679x | 1.826x | 0.411x | 0.251x | 0.205x |
| `concat` | 13.272x | 13.608x | 13.517x | 13.608x | 13.639x |
| `substring` | 7.414x | 1.114x | 0.254x | 0.162x | 0.134x |
| `decimal_add` | 16.648x | 4.617x | 2.149x | 1.845x | 1.800x |
| `json_type` | 7.622x | 7.529x | 7.436x | 7.501x | 7.547x |
| `regexp_like` | 6.172x | 6.081x | 6.082x | 5.948x | 5.775x |

1024行的绝对耗时（上述主表同一组采样），避免只看倍率：

| workload | frozen native (ns/row) | final unified (ns/row) | unified / frozen | route |
|---|---:|---:|---:|---|
| `integer_add` | 21.635 | 12.901 | 0.597x | tikv_batch |
| `integer_add_literal` | 20.619 | 9.399 | 0.455x | tikv_batch |
| `abs` | 99.653 | 7.786 | 0.078x | tikv_batch |
| `lazy_if` | 99.313 | 3,095.085 | 31.168x | production_fallback |
| `coalesce_nullif` | 170.099 | 3,305.633 | 19.262x | production_fallback |
| `strcmp` | 88.031 | 1,322.696 | 15.023x | production_fallback |
| `lower` | 154.044 | 33.274 | 0.216x | tikv_batch |
| `upper` | 154.507 | 31.740 | 0.205x | tikv_batch |
| `concat` | 86.257 | 1,176.495 | 13.639x | production_fallback |
| `substring` | 294.591 | 39.400 | 0.134x | tikv_batch |
| `decimal_add` | 70.432 | 126.764 | 1.800x | tikv_batch |
| `json_type` | 144.393 | 1,089.790 | 7.547x | production_fallback |
| `regexp_like` | 260.260 | 1,503.210 | 5.775x | production_fallback |

## 真正未调优统一版 → 最终统一版

以下是额外三轮真实历史二进制对照，batch1024；不是同一当前版本强制 scalar/单行的消融。SQL、行数和被选列的值相同；历史探针 schema 有6列，最终有12列（新增的6列在这些 SQL 中未引用）。这是两个真实系统版本的比较，不是只改变一行源码、完全固定所有 metadata 的因果实验。

| workload | 未做M1–M30调优的统一版 (ns/row) | final unified (ns/row) | final / initial unified | 耗时变化 |
|---|---:|---:|---:|---:|
| `abs` | 960.187 | 7.802 | 0.008x | -99.2% |
| `coalesce_nullif` | 3,326.835 | 3,293.969 | 0.989x | -1.1% |
| `concat` | 1,248.058 | 1,165.394 | 0.934x | -6.6% |
| `decimal_add` | 1,569.453 | 126.770 | 0.081x | -91.9% |
| `integer_add` | 288.146 | 12.841 | 0.045x | -95.5% |
| `integer_add_literal` | 280.918 | 9.388 | 0.033x | -96.7% |
| `json_type` | 1,142.745 | 1,088.499 | 0.961x | -3.9% |
| `lazy_if` | 3,134.403 | 3,077.433 | 0.978x | -2.2% |
| `lower` | 1,172.749 | 33.107 | 0.028x | -97.2% |
| `regexp_like` | 1,559.815 | 1,498.785 | 0.959x | -4.1% |
| `strcmp` | 1,343.364 | 1,334.385 | 0.994x | -0.6% |
| `substring` | 1,643.162 | 39.308 | 0.024x | -97.6% |
| `upper` | 1,171.483 | 31.867 | 0.027x | -97.3% |

注意：最早 M2 留存的调优前 Int 是285.179ns/row，M8另一轮真实历史版本复测是253.812；两者的探针/构建和采样时间不同。不能把这些旧数值串成一次严格配对实验；上表使用本轮重新采样的真实历史二进制。

更早、尚未引入 lane cache 的 AST/value 统一版 `a3cf8821`，历史 one-shot 记录为 Int2259.850、IF4334.148、STRCMP2872.959、Decimal4006.884、JSON_TYPE2172.428、REGEXP12099.094ns/eval；这是历史档案，不是本轮同条件复测，也不能拿 AST ns/eval 与 batch ns/row 相除。[原始口径与记录](logs/performance-before-after-summary.txt)。

## 附表：全部64个当前 benchmark workload，dense1024

覆盖58个走batch和6个走canonical fallback的 workload；**64个 workload 不等于68个准入签名，更不等于全部 SQL 函数**。本表一并报告已优化和未优化部分。

| workload | frozen native (ns/row) | final unified (ns/row) | unified / frozen | route |
|---|---:|---:|---:|---|
| `integer_add` | 21.635 | 12.901 | 0.597x | tikv_batch |
| `integer_add_literal` | 20.619 | 9.399 | 0.455x | tikv_batch |
| `abs` | 99.653 | 7.786 | 0.078x | tikv_batch |
| `lazy_if` | 99.313 | 3,095.085 | 31.168x | production_fallback |
| `coalesce_nullif` | 170.099 | 3,305.633 | 19.262x | production_fallback |
| `strcmp` | 88.031 | 1,322.696 | 15.023x | production_fallback |
| `lower` | 154.044 | 33.274 | 0.216x | tikv_batch |
| `upper` | 154.507 | 31.740 | 0.205x | tikv_batch |
| `concat` | 86.257 | 1,176.495 | 13.639x | production_fallback |
| `substring` | 294.591 | 39.400 | 0.134x | tikv_batch |
| `decimal_add` | 70.432 | 126.764 | 1.800x | tikv_batch |
| `json_type` | 144.393 | 1,089.790 | 7.547x | production_fallback |
| `regexp_like` | 260.260 | 1,503.210 | 5.775x | production_fallback |
| `pure_pi` | 67.375 | 4.080 | 0.060x | tikv_batch |
| `round_int_signed` | 104.339 | 7.607 | 0.073x | tikv_batch |
| `round_int_unsigned` | 103.980 | 7.550 | 0.073x | tikv_batch |
| `any_value_int_unsigned` | 125.577 | 9.945 | 0.079x | tikv_batch |
| `integer_not` | 47.211 | 7.645 | 0.163x | tikv_batch |
| `real_asin` | 114.146 | 10.145 | 0.090x | tikv_batch |
| `real_acos` | 111.944 | 10.272 | 0.092x | tikv_batch |
| `isnull_int` | 101.783 | 7.572 | 0.074x | tikv_batch |
| `isnull_real` | 101.545 | 7.549 | 0.074x | tikv_batch |
| `isnull_decimal` | 143.820 | 36.021 | 0.251x | tikv_batch |
| `isnull_string` | 111.112 | 10.857 | 0.097x | tikv_batch |
| `isnull_datetime` | 101.761 | 7.670 | 0.075x | tikv_batch |
| `isnull_timestamp` | 101.684 | 7.591 | 0.075x | tikv_batch |
| `isnull_duration` | 102.005 | 10.178 | 0.100x | tikv_batch |
| `isnull_vector` | 118.728 | 39.168 | 0.330x | tikv_batch |
| `integer_bit_bitand` | 80.456 | 11.202 | 0.139x | tikv_batch |
| `integer_bit_bitor` | 80.144 | 11.244 | 0.140x | tikv_batch |
| `integer_bit_bitxor` | 80.724 | 11.305 | 0.140x | tikv_batch |
| `integer_bit_leftshift` | 80.161 | 11.344 | 0.142x | tikv_batch |
| `integer_bit_rightshift` | 80.043 | 11.217 | 0.140x | tikv_batch |
| `real_compare_lt` | 81.105 | 11.295 | 0.139x | tikv_batch |
| `real_compare_le` | 81.123 | 11.286 | 0.139x | tikv_batch |
| `real_compare_gt` | 81.062 | 11.430 | 0.140x | tikv_batch |
| `real_compare_ge` | 81.726 | 11.438 | 0.140x | tikv_batch |
| `real_compare_eq` | 80.143 | 11.325 | 0.141x | tikv_batch |
| `real_compare_ne` | 80.243 | 11.400 | 0.142x | tikv_batch |
| `real_compare_nulleq` | 79.803 | 11.499 | 0.144x | tikv_batch |
| `decimal_compare_lt` | 168.268 | 73.716 | 0.439x | tikv_batch |
| `decimal_compare_le` | 167.726 | 73.781 | 0.440x | tikv_batch |
| `decimal_compare_gt` | 167.464 | 73.727 | 0.440x | tikv_batch |
| `decimal_compare_ge` | 167.616 | 73.684 | 0.439x | tikv_batch |
| `decimal_compare_eq` | 167.690 | 73.521 | 0.439x | tikv_batch |
| `decimal_compare_ne` | 167.314 | 73.616 | 0.440x | tikv_batch |
| `decimal_compare_nulleq` | 167.043 | 73.532 | 0.442x | tikv_batch |
| `integer_compare_lt` | 71.874 | 11.180 | 0.156x | tikv_batch |
| `integer_compare_le` | 71.898 | 11.204 | 0.156x | tikv_batch |
| `integer_compare_gt` | 72.404 | 11.190 | 0.155x | tikv_batch |
| `integer_compare_ge` | 72.372 | 11.192 | 0.155x | tikv_batch |
| `integer_compare_eq` | 71.943 | 11.139 | 0.156x | tikv_batch |
| `integer_compare_ne` | 71.806 | 11.188 | 0.155x | tikv_batch |
| `integer_compare_nulleq` | 78.975 | 11.255 | 0.143x | tikv_batch |
| `integer_add_nested` | 39.042 | 23.160 | 0.593x | tikv_batch |
| `integer_subtract` | 22.529 | 13.273 | 0.588x | tikv_batch |
| `integer_multiply` | 21.636 | 11.866 | 0.549x | tikv_batch |
| `char_length` | 65.518 | 14.614 | 0.223x | tikv_batch |
| `real_add` | 107.600 | 12.875 | 0.119x | tikv_batch |
| `any_value_datetime` | 129.345 | 9.803 | 0.076x | tikv_batch |
| `any_value_timestamp` | 129.923 | 9.828 | 0.076x | tikv_batch |
| `any_value_duration` | 127.474 | 13.137 | 0.103x | tikv_batch |
| `any_value_json` | 150.026 | 122.019 | 0.812x | tikv_batch |
| `any_value_vector` | 162.260 | 70.939 | 0.437x | tikv_batch |

## 附表：standalone rewritten Chunk 单次 eval

这也不是整批执行。两个版本都用已 rewrite 表达式在同一虚拟行上求值。

| workload | frozen native | unified one-shot | unified lane | lane / native |
|---|---:|---:|---:|---:|
| `integer_add` | 90.877 | 2,042.071 | 1,124.380 | 12.364x |
| `lazy_if` | 31.289 | 3,674.167 | 1,962.580 | 62.791x |
| `strcmp` | 75.671 | 2,421.159 | 1,322.887 | 17.482x |
| `decimal_add` | 487.365 | 3,588.429 | 2,434.691 | 4.996x |
| `json_type` | 214.202 | 1,862.454 | 1,159.739 | 5.414x |
| `regexp_like` | 246.508 | 2,623.998 | 1,496.114 | 6.069x |

## 复现与边界

- AMD Ryzen9 9900X，release libtest，固定CPU2、单测试线程；boost/SMT和宿主机活动未隔离，不将三个进程对当作总体置信区间。
- 主 batch 表116个 workload/size cells，三个交错进程对，每格7个约75ms样本：4872条计时、348组跨版本 checksum 全一致。width-one 两个 tier、6个 workload，每进程9个样本，共972条计时、36组结果标签一致。另保存105条真实初始PLUS复测，以及798条13workload的初始/最终额外对照（其中546条dense、39组dense checksum一致）。
- 时延是三个进程中位数的中位数；倍率是对应进程对倍率的中位数，因此可能与展示的两个总体中位数直接相除略有差别。每个进程的中位数、所有慢项和原始样本均保留，不挑最快组。
- ASIN/ACOS 使用相同正常/越界混合数据；两项 UInt identity 在两个版本均设置相同 unsigned schema 标志，其余仍为原 signed LongLong/原 Real 数据。不同 batch size 使用同生成器的不同长度 prefix，并非固定完全相同的值分布。
- 初始一次未完成的主表采样因为 CONFIG 标题仍打印旧size列表而中止；实际physical_rows正确，但仍整轮剔除、修正标题重建后从头测。额外历史对照的首次尝试还发现旧decimal_add是d+0.55、最终是d+d，因此整轮剔除，只修正旧版测试SQL、重建并重跑全部13项。两次排除记录单独保存，均未混入正式表。
- M30的生产测试结果沿用明确范围：batch34、完整TiDB1386通过/原有4失败，TiKV1032通过/原有6失败。本轮新增的是测试探针和测量断言，make lint/格式/diff及数据校验；没有声称重新执行server/PD-TiKV/FIPS或修复所有基线失败。
- 这是 warm 内存中 expression evaluator microbenchmark，不含parse/plan/storage/RPC/网络并发，不代表SQL server QPS、冷启动、P99或所有类型/字符集/SQL-mode域。

全部脚本、两个 native frozen 探针及两个历史统一版探针、当前 probe-only diff、源/二进制hash、每个进程值和原始输出见 [最终复测证据目录](logs/performance-final-evolution/)。机器可读主表为 [summary.json](logs/performance-final-evolution/summary.json)，初始/最终额外对照为 [initial-comparison-summary.json](logs/performance-final-evolution/initial-comparison-summary.json)。运行顺序与精确命令见 [validation.md](logs/performance-final-evolution/validation.md)。
