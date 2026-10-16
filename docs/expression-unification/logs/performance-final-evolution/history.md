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
