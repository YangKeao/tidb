# 小值Decimal退化：定位与最小修正

保留原datatype内的5行修正。原30位乘积的64行共享Vector耗时由15.49降至9.61微秒，四轮比修正前低37.3%–38.5%，也低于旧副本；25位边界同样改善。24位内联场景仍比旧副本慢19.9%–21.0%，原生向量快路径差距也仍存在，不能宣布全部小值路径或普遍性能达标。

范围依据Designer消息01f727b4及固定设计162070055f856acbbd1515bf0dc1bc264dd44e9c。沿用原datatype所有者，不复制算法、不新增产品执行循环，也不改变接纳、参数需求、诊断政策或最终Chunk写出。

## 版本、构建与入口

旧副本为TiDB72fedb911c／SDK3ad4dc75；原直接复用为TiDB598a313e26（产品f5daddcbdb）／SDKd822fbf9；修正a9326004a7388f00af36bcf20d5904b4bfdb1dd6／SDK仍d822fbf9。源码仅在原DecimalDigits::from_unsigned增加5行：超过24位内联容量，调用标准整数格式化并接入已有DecimalDigits构造；较短系数保留原分支。没有修改原生i128向量快路径。

原计时含关闭计数器后的分配包装分支，本次重新编译真正不链接包装器的二进制。分配模式单独构建运行；其计时不用于性能结论。两种模式均来自同一测量源码，记录完整命令、库产物、二进制与源码哈希。普通计时二进制没有__wrap_*定义；计数构建有四个可达包装符号，未使用的aligned_alloc包装由链接器去除，五个链接包装选项均在收据中。

仍使用相邻双仓及nightly-2026-08-22实验包装器，opt3、LTO关闭、16个codegen unit；不是默认生产thin-LTO。SDK无条件相邻TiDB依赖及Jan30/rustc1.95对MSRV1.97的失败没有解决，本轮未冒充重新验证默认SDK。

复用原[测量器](decimal-shared-bench.rs)、[构建脚本](build-decimal-bench.py)和[运行脚本](run-decimal-bench.py)。原四个用例保留，新增24位inline_mul和25位spill_mul乘积，另有原small_mul的30位乘积，均仍在i128范围；wide_mul、80位相消和隐藏精度链继续覆盖回退及完整尺度。width1／64是行数，调用模式另列：

- native_vector是原EvaluatorSuite正常入口，能接纳时使用已有向量快路径，其他用例允许原生回退；不能把所有行都称为执行了i128快路径。
- native_scalar在测量器中显式逐行调用原Expression::eval并立即写入同一Chunk。原EvaluatorSuite先尝试Decimal快路径，再读会话向量开关，因此仅关闭开关不足以取得Scalar。
- shared使用共享Vector调用；shared_scalar以现有Columns向量开关选择共享Scalar调用。两者都保留结果Vec的生成、释放和全部Chunk写出。

每个版本、每次运行都核对完整系数、符号、存储／显示尺度、声明形状及原生Scalar／Vector和共享结果；隐藏链仍为0.01142857。输入为常量和虚拟行，不是SQL吞吐、真实列或多输出负载。准备成本独立测量，包含当前接口的构造／销毁，不等于所有生产调用者的必付成本。

## 定位证据

第一组四用例无包装对照已有512个单元：small_mul64共享完整入口由11286.0ns变为16019.3ns，表明此前退化并非仅由分配包装引起。独立Chunk写出约2101.3／2111.3ns；共享生成值约9124.6／13881.9ns。原生Vector约3805.3／3798.7ns，原生显式Scalar约27104.6／27668.6ns，不能把共享与原生Vector差距全称为分派成本。

独立perf cycles:u采样（199Hz）在复用前取得3293样本、复用后4735样本，均无丢样。复用后的自身样本43.24%落在DecimalDigits::from_unsigned，17.58%在mul_mysql，8.63%在MyDecimal::from_decimal_parts，7.50%在UTF-8验证。原副本使用标准u128格式化；原datatype则逐位除十、扩容并反转系数。30位乘积超过24位内联容量，仍支付这段逐位物化成本。采样自身符号占比是定位线索，不是精确独占纳秒；优化构建的部分DWARF父链不完整，因此不据那些父链归因。

据此只试原datatype的溢出内联容量分支，继续使用同一个Decimal公共所有者及原数值算法。分段诊断分别测原datatype算术并生成值、完整原生Scalar值、共享值生成及预先生成值的Chunk写出。各段的借用、分配、释放和缓存边界不同，不能相减后宣称是精确RPN独占成本；验收以完整入口为准。

## 最终完整入口与分配

每个无包装单元20000次计时、32次预热，四轮交替三版本顺序，固定CPU23。每轮相同输入；机器未隔离，范围不是置信区间。分配另用计数构建，每单元100次调用，一轮三版本；它是请求调用／字节总数，不是峰值内存。计数启动自检保持3次／288字节。最终1440个计时单元及360个分配单元全部完成，15次二进制运行退出0，所有完整值和计数自检通过。

| 场景 / 行数 / 共享模式 | 旧副本 ns | 原复用 ns | 修正后 ns | 修正/原复用范围 | 修正/旧副本范围 |
| --- | ---: | ---: | ---: | ---: | ---: |
| inline_mul/1/shared | 361.4 | 370.6 | 373.6 | 0.954–1.050 | 1.010–1.068 |
| inline_mul/1/shared_scalar | 428.2 | 464.1 | 460.5 | 0.974–1.011 | 0.844–1.125 |
| inline_mul/64/shared | 10546.2 | 12906.9 | 12722.4 | 0.981–0.989 | 1.199–1.210 |
| inline_mul/64/shared_scalar | 20728.9 | 22537.6 | 22147.3 | 0.968–0.983 | 1.004–1.080 |
| spill_mul/1/shared | 365.0 | 387.1 | 308.0 | 0.744–0.853 | 0.839–0.884 |
| spill_mul/1/shared_scalar | 430.5 | 492.6 | 399.0 | 0.792–0.840 | 0.888–0.968 |
| spill_mul/64/shared | 10645.2 | 13674.1 | 8823.5 | 0.642–0.647 | 0.822–0.833 |
| spill_mul/64/shared_scalar | 20400.3 | 24709.4 | 19674.5 | 0.789–0.801 | 0.915–0.980 |
| small_mul/1/shared | 362.3 | 421.6 | 324.6 | 0.706–0.824 | 0.872–0.949 |
| small_mul/1/shared_scalar | 441.7 | 517.6 | 418.7 | 0.801–0.836 | 0.915–0.992 |
| small_mul/64/shared | 11388.5 | 15493.0 | 9610.2 | 0.615–0.627 | 0.842–0.846 |
| small_mul/64/shared_scalar | 21051.8 | 26322.1 | 20754.1 | 0.784–0.793 | 0.937–0.993 |
| wide_mul/1/shared | 794.4 | 475.8 | 468.4 | 0.941–1.021 | 0.583–0.612 |
| wide_mul/1/shared_scalar | 884.3 | 601.8 | 557.3 | 0.919–0.945 | 0.624–0.656 |
| wide_mul/64/shared | 34524.1 | 18591.6 | 17862.3 | 0.958–0.966 | 0.516–0.521 |
| wide_mul/64/shared_scalar | 43587.1 | 29449.3 | 28637.6 | 0.964–0.977 | 0.638–0.660 |
| wide_cancel/1/shared | 396.0 | 409.6 | 391.1 | 0.904–1.007 | 0.979–1.019 |
| wide_cancel/1/shared_scalar | 481.2 | 497.6 | 485.4 | 0.958–1.006 | 0.976–1.043 |
| wide_cancel/64/shared | 13001.3 | 13663.8 | 13450.8 | 0.957–1.014 | 1.012–1.044 |
| wide_cancel/64/shared_scalar | 22990.9 | 23772.4 | 23574.3 | 0.971–1.022 | 0.965–1.045 |
| hidden_chain/1/shared | 13914.2 | 692.9 | 710.1 | 0.938–1.070 | 0.051–0.052 |
| hidden_chain/1/shared_scalar | 14048.3 | 818.0 | 810.0 | 0.974–1.006 | 0.057–0.058 |
| hidden_chain/64/shared | 738877.4 | 22269.8 | 22528.6 | 1.007–1.016 | 0.030–0.031 |
| hidden_chain/64/shared_scalar | 731717.3 | 41511.6 | 41473.0 | 0.984–1.012 | 0.056–0.057 |

| 场景 / 64行原生模式 | 旧副本版本 ns | 原复用版本 ns | 修正后 ns |
| --- | ---: | ---: | ---: |
| inline_mul/native_vector | 6725.5 | 6749.9 | 6745.5 |
| inline_mul/native_scalar | 24782.0 | 23787.3 | 23968.4 |
| spill_mul/native_vector | 6917.5 | 6959.2 | 6953.6 |
| spill_mul/native_scalar | 24912.2 | 24207.6 | 20208.4 |
| small_mul/native_vector | 3768.0 | 3790.4 | 3794.9 |
| small_mul/native_scalar | 27108.0 | 26941.0 | 21565.0 |
| wide_mul/native_vector | 20705.9 | 22727.6 | 22019.2 |
| wide_mul/native_scalar | 32483.5 | 33796.5 | 32607.6 |
| wide_cancel/native_vector | 16388.0 | 18600.5 | 17542.4 |
| wide_cancel/native_scalar | 27575.1 | 30315.7 | 28298.4 |
| hidden_chain/native_vector | 29684.3 | 28647.2 | 29251.1 |
| hidden_chain/native_scalar | 54202.0 | 51030.7 | 53082.3 |

| 场景 / 行数 / 共享模式 | 旧副本 次数/字节 | 原复用 次数/字节 | 修正后 次数/字节 |
| --- | ---: | ---: | ---: |
| inline_mul/1/shared | 9/2520 | 6/2216 | 6/2216 |
| inline_mul/1/shared_scalar | 13/3224 | 9/2912 | 9/2912 |
| inline_mul/64/shared | 143/22824 | 10/11816 | 10/11816 |
| inline_mul/64/shared_scalar | 336/94568 | 139/85160 | 139/85160 |
| spill_mul/1/shared | 9/2522 | 7/2248 | 7/2241 |
| spill_mul/1/shared_scalar | 13/3227 | 11/2976 | 11/2969 |
| spill_mul/64/shared | 143/22952 | 74/13864 | 74/13416 |
| spill_mul/64/shared_scalar | 336/94760 | 267/89256 | 267/88808 |
| small_mul/1/shared | 9/2532 | 7/2248 | 7/2246 |
| small_mul/1/shared_scalar | 13/3242 | 11/2976 | 11/2974 |
| small_mul/64/shared | 143/23592 | 74/13864 | 74/13736 |
| small_mul/64/shared_scalar | 336/95720 | 267/89256 | 267/89128 |
| wide_mul/1/shared | 24/2946 | 8/2322 | 8/2322 |
| wide_mul/1/shared_scalar | 28/3668 | 12/3082 | 12/3082 |
| wide_mul/64/shared | 1103/50088 | 138/18600 | 138/18600 |
| wide_mul/64/shared_scalar | 1296/122984 | 331/96040 | 331/96040 |
| wide_cancel/1/shared | 13/2801 | 11/2537 | 11/2537 |
| wide_cancel/1/shared_scalar | 17/3482 | 14/3233 | 14/3233 |
| wide_cancel/64/shared | 399/40808 | 330/32360 | 330/32360 |
| wide_cancel/64/shared_scalar | 592/111080 | 459/105704 | 459/105704 |
| hidden_chain/1/shared | 645/9865 | 10/3795 | 10/3795 |
| hidden_chain/1/shared_scalar | 649/10563 | 13/4491 | 13/4491 |
| hidden_chain/64/shared | 40729/418552 | 148/34296 | 148/34296 |
| hidden_chain/64/shared_scalar | 41040/564264 | 395/186216 | 395/186216 |

| small_mul / 64行诊断段 | 旧副本版本 ns | 原复用版本 ns | 修正后 ns |
| --- | ---: | ---: | ---: |
| arithmetic/native | 11159.3 | 10992.0 | 5171.8 |
| evaluate/native_scalar | 24933.7 | 24265.3 | 18483.4 |
| evaluate/shared | 9192.6 | 13364.9 | 7471.9 |
| write/shared | 2104.8 | 2075.3 | 2076.5 |

原small_mul的Scalar和Vector、1行和64行四轮均低于旧副本；25位边界也如此。24位共享Vector的64行仍比旧副本高19.9%–21.0%，原small_mul共享Vector仍为原生快路径约2.53倍。隐藏链64行共享Vector较立即前版四轮高0.7%–1.6%，相消64行Vector较旧副本高1.2%–4.4%；这些剩余差距与小幅退化均保留，不用其他用例平均改善掩盖。

各ns列为四轮每次批量调用中位数；比值为同轮成对范围。原始全部prepare、evaluate、arithmetic、write及reuse单元保留，不把诊断段代替完整入口结果。

## 检查、失败尝试与限制

89项datatype Decimal相关测试通过（338过滤），31项共享原生测试通过（1323过滤），SDK表达式库429项全部通过。限定datatype／表达式库Clippy完成且有警告，root make lint退出0；既有完整正式SDK make clippy仍未完成。没有增加一套复制测试；新24／25位场景与共享Scalar在测量器中核对完整值，原192组边界、隐藏精度、符号／零值及诊断用例继续由现有测试覆盖。

首版扩展测量器引用私有context::Columns编译失败E0603，改用已有公开tidb_expr::Columns；原失败源码和日志保留。最初符号审计错误要求五个包装函数全部保留，实际未使用的aligned_alloc被链接器删除；改为核对五个链接选项、四个实际可达符号及无包装版本零符号。没有据此更改产品或分配器。性能修正前失败证据是上述无包装退化及采样；没有虚构功能断言先失败。

修正后独立采样得到2909样本、无丢样，from_unsigned自身占比为10.87%。该次使用最终六用例测量器，初次定位使用四用例版本；两次均只计时small_mul/64/reuse/shared，其他入口未纳入采样循环。符号占比受二进制布局、内联和采样波动影响，不能把43.24%与10.87%直接换算成精确节省时间；同源码三版本的完整入口与诊断计时提供独立A/B证据。完整64行小值算术段由10992.0降至5171.8ns，共享值生成由13364.9降至7471.9ns，Chunk写出2075.3／2076.5ns相近，符合系数物化是本次主要可消除开销的判断。

未改变SDK默认工具链、独立发布布局、完整M1生命周期或245族覆盖。本轮局部性能不证明完整共享内核、默认集成、10K规模或普遍提速。

## 完整增删与复核

相对本批起点598a313e26／d822fbf9和原始364aef2／548812e分别结算。生产实现只增加5行；测量器全部归测试，CSV、Python、计划和报告等全部计入文档审计，没有从规模目标扣除。

| 对照 | 生产 + / − | 测试 + / − | 构建 + / − | 文档 + / − |
| --- | ---: | ---: | ---: | ---: |
| TiDB原始 | 1039 / 1 | 2652 / 0 | 1867 / 91 | 14316 / 0 |
| SDK原始 | 1693 / 57 | 47 / 4 | 100 / 9 | 15 / 0 |
| TiDB本批 | 5 / 0 | 259 / 102 | 0 / 0 | 2918 / 10 |
| SDK本批 | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 |

本批全部文件+3182/−112，增删3294行、净增3070行；其中生产仅+5/−0。相对原始基线，源码与测试+5431/−62，增删5493行；全部文件+21729/−162，增删21891行。完整审计成本明显增加，不能把5行产品修正当作本批全部改动。

[验证收据](decimal-regression-validation.json)固定5行修正的源码提交、六份二进制、15次完整运行终态、检查命令／退出码、采样条件及130份本地记录哈希。5525份源码／构建输入摘要为`acbde542cff992e132180af1d305d62a0a7567580528e66d7b226345101d9d1d`，与源码提交及留存记录核对。SDK源码未改；旧收据只适用于其各自固定版本。

[1440行计时原始数据](decimal-regression-timing.csv)和[360行分配原始数据](decimal-regression-allocations.csv)随Git发布；大型构建日志、perf数据、二进制及首轮诊断保存在Engineer工作区`evidence/decimal-regression/`。有Git读权限即可重算表格；复核本地原始记录需该证据目录。可从TiDB根目录执行：

    python3 rust/docs/shared-kernel/summarize-decimal-regression.py rust/docs/shared-kernel/decimal-regression-timing.csv rust/docs/shared-kernel/decimal-regression-allocations.csv
    python3 rust/docs/shared-kernel/verify-checkpoint-6.py --tidb . --tikv ../tikv --receipt rust/docs/shared-kernel/decimal-regression-validation.json --evidence /path/to/evidence/decimal-regression
    python3 rust/docs/shared-kernel/count-datatype-reuse.py /path/to/parent-of-tidb-and-tikv 598a313e267ea06d0ebe24a4a194fe6f351d197b d822fbf9fbee8e0b635974a8d38e42f9e73f067a

构建脚本默认无包装器，`--allocations`另建计数程序；运行脚本`--before`、`--after`、`--candidate`依次对应旧副本、原复用、修正版本，默认四轮20000次计时。分配命令另加`--allocations --rounds 1 --iterations 1`，只取另跑100次的分配列。原始命令及实际工具链配置在收据所指构建日志中。未启动SQL服务，也没有把采样或诊断结果计为产品单元测试。
