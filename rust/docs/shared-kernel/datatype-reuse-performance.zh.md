# 底层库复用的构建与优化测量补证

本页是598a313e267ea06d0ebe24a4a194fe6f351d197b的历史补证，命令和收据应在该固定版本使用。后续无包装计时、显式Scalar及系数物化修正见[新报告](decimal-regression.zh.md)。

补证完成。直接复用原Decimal降低了本地宽值乘法和隐藏精度链的复用成本及分配，但64行小值乘法四轮耗时增加35%–38%，准备成本也没有普遍下降。保留生产代码净减812行的结构简化；这些局部结果不证明普遍提速。现有release非法值拒绝测试通过，SDK原默认工具链则实际构建失败。

功能固定版本为TiDB源码f5daddcbdb、报告4e60d898f8及SDK d822fbf9。对照为TiDB72fedb911c及SDK3ad4dc75。此次只增加测量与报告，产品实现没有继续改动；功能版本的31／429项测试仍为原记录。

## 构建边界

TiDB `tidb-expr --no-default-features`检查通过；启用shared-kernel的功能构建与SDK表达式库测试使用nightly-2026-08-22（rustc1.100.0-nightly c656540d6）包装器。SDK新增的是无条件`tidb-datatype`路径依赖，PB构建同样需要相邻TiDB源码，不能称为SDK独立检出构建通过。

实际用SDK所指定nightly-2026-01-30（rustc1.95.0-nightly）运行`cargo check -p tidb_query_expr --locked --offline`，退出101；新增datatype/error/hack/kvcache/mysql包要求rustc1.97。这个失败在开始编译前发生，是默认工具链的已知不兼容，不是尚未尝试。没有修改SDK工具链或降低库声明来绕过它。

两工作区依赖解析及实际编译均无SDK→tidb-expr回环。完整包身份、feature和依赖输入保存在Cargo锁文件与构建JSON日志；native chrono-tz0.10.4和SDK chrono-tz0.5.3并存。当前支持并实际验证的方式是相邻固定双仓加上述包装器：GCC14/G++14、CXXFLAGS包含`-include cstdint`，保留原缓存与离线锁文件解析。普通默认SDK工具链、独立发布布局和完整产品发布构建不在通过范围。

## 方法与可复现输入

[测量源码](decimal-shared-bench.rs)通过真实`EvaluatorSuite`与`SharedProjection`，覆盖小值乘法、40位值乘法、80位相消和`((8/7)*1)/100`。小值操作数沿用现有`tidb-datatype/benches/decimal.rs`输入。准备阶段分别建立原生执行器与共享程序；复用阶段排除准备，使用相同行数、输入和预分配输出Chunk，保留两侧实际输出转换。width=1和64均使用正常投影入口；不把它解释为纯函数核耗时。

每次二进制运行在复用计时前核对完整Decimal系数、符号、存储／显示尺度和声明形状，再核对两侧输出Chunk值。隐藏精度链必须为`0.01142857`。输入准备不计入复用成本；常量广播／分支内部的实际克隆仍计入。准备阶段的原生输入克隆是现有`EvaluatorSuite::new`的真实接口成本。

构建采用优化等级3、关闭LTO、16个codegen unit，两侧一致。这沿用仓库bench优化方式，**不是默认生产release的thin-LTO配置**。独立测量程序直接链接真实Cargo构建的rlib；SDK已有全局System分配器保持不变（构建脚本断言tikv_alloc没有替换分配器feature）。独立程序在链接时包装malloc/calloc/realloc/aligned_alloc/posix_memalign，转发原ABI并计数，不放宽产品crate的forbid-unsafe lint。每次启动用64字节分配、重分配96字节及128字节对齐清零分配，自检恰为3次／288字节。动态库内部未经过这些链接包装的调用不在计数范围。

计时关闭计数器；分配另跑100次。计数为上述分配／重分配调用尝试次数及请求字节总数，不是峰值内存或存活内存。计时仍包含分配器的关闭分支。每个单元计时20000次；四轮前后顺序交替，各次固定在同一可用CPU；不停止其他应用，因此表中范围用于揭示噪声，不构成统计置信区间。原生结果在两份二进制内均测量，作为变化未涉及算法的对照。

[构建脚本](build-decimal-bench.py)接受`--repo`、`--cargo`、`--rustc`和`--output`；对照树需相邻检出上述TiDB／SDK版本。它保存Cargo日志、链接命令和二进制／测量源码哈希。[运行脚本](run-decimal-bench.py)接受`--before`、`--after`二进制及`--output`，校验每次24个测量单元并输出原始CSV；全部单元必须先通过结果断言。

## 结果

192个单元全部完成：4场景×3阶段／行数×2入口×2版本×4轮。每次二进制运行均通过完整值预检和计数自检。下表耗时为每次准备／批次调用的四轮中位数；范围为同轮共享后／前之比，非统计置信区间。0表示准备阶段；1和64表示每批行数。原生前后分配次数和字节逐项相同；其耗时波动仍保留。

| 场景 / 行数 / 阶段 | 原生前 ns | 原生后 ns | 共享前 ns | 共享后 ns | 共享后/前四轮范围 |
| --- | ---: | ---: | ---: | ---: | ---: |
| hidden_chain/0/prepare | 341.8 | 348.3 | 1521.5 | 1598.6 | 1.046–1.054 |
| hidden_chain/1/reuse | 940.8 | 1048.5 | 11250.2 | 686.8 | 0.060–0.065 |
| hidden_chain/64/reuse | 29688.0 | 28166.6 | 752593.2 | 21880.4 | 0.029–0.029 |
| small_mul/0/prepare | 189.9 | 191.7 | 642.6 | 653.1 | 0.835–1.042 |
| small_mul/1/reuse | 161.3 | 155.6 | 366.4 | 424.1 | 0.941–1.201 |
| small_mul/64/reuse | 3819.6 | 3784.1 | 11350.0 | 15453.2 | 1.353–1.381 |
| wide_cancel/0/prepare | 377.1 | 350.6 | 722.7 | 767.8 | 1.048–1.078 |
| wide_cancel/1/reuse | 604.9 | 621.0 | 401.7 | 403.2 | 0.967–1.043 |
| wide_cancel/64/reuse | 16432.0 | 17889.4 | 13665.1 | 13411.1 | 0.970–1.012 |
| wide_mul/0/prepare | 212.2 | 207.0 | 631.8 | 676.2 | 1.062–1.079 |
| wide_mul/1/reuse | 663.5 | 661.3 | 785.3 | 476.1 | 0.600–0.636 |
| wide_mul/64/reuse | 21906.4 | 21736.3 | 34794.6 | 17967.2 | 0.511–0.523 |

| 场景 / 行数 / 阶段 | 原生 次数 / 字节 | 共享前 次数 / 字节 | 共享后 次数 / 字节 |
| --- | ---: | ---: | ---: |
| hidden_chain/0/prepare | 13 / 5586 | 69 / 7535 | 65 / 7753 |
| hidden_chain/1/reuse | 15 / 403 | 645 / 9865 | 10 / 3795 |
| hidden_chain/64/reuse | 141 / 22768 | 40729 / 418552 | 148 / 34296 |
| small_mul/0/prepare | 7 / 2854 | 31 / 2427 | 29 / 2428 |
| small_mul/1/reuse | 4 / 144 | 9 / 2532 | 7 / 2248 |
| small_mul/64/reuse | 4 / 9216 | 143 / 23592 | 74 / 13864 |
| wide_cancel/0/prepare | 9 / 3112 | 33 / 3889 | 33 / 4017 |
| wide_cancel/1/reuse | 16 / 1025 | 13 / 2801 | 11 / 2537 |
| wide_cancel/64/reuse | 457 / 47960 | 399 / 40808 | 330 / 32360 |
| wide_mul/0/prepare | 8 / 2918 | 30 / 2612 | 29 / 2666 |
| wide_mul/1/reuse | 11 / 426 | 24 / 2946 | 8 / 2322 |
| wide_mul/64/reuse | 200 / 21720 | 1103 / 50088 | 138 / 18600 |

分配表为独立100次调用的每次值，四轮完全一致。共享复用后的次数／字节均少于复用前，但小值64行仍比原生多；分配减少不能直接推导耗时下降。准备阶段的请求字节全部略增，且宽乘／相消／隐藏链耗时四轮均增。隐藏链与宽乘的复用耗时四轮均降；相消跨越1、方向混合。所有表达式使用常量和虚拟行，未覆盖真实列输入、String→Real、复杂混合投影或SQL服务负载，不据此归因具体机器指令或推算整体吞吐。

[原始192行数据](datatype-reuse-rounds.csv)可用`python3 rust/docs/shared-kernel/summarize-decimal-bench.py rust/docs/shared-kernel/datatype-reuse-rounds.csv`独立重算。

现有`tidb-datatype/benches/decimal.rs`源码保持原样，分别链接两份优化构建产物，在CPU23各执行一次控制测量（不与正式四轮并发）。其五个任务均完成；这不是RPN复用对照，也不足以判断性能变化：

| 原有任务 | 前 ms | 后 ms |
| --- | ---: | ---: |
| Round | 43.578228 | 34.437235 |
| ToFloat64New | 598.826057 | 492.703359 |
| ToFloat64Old | 591.714144 | 499.461884 |
| MyDecimalToBin | 4.083767 | 4.181378 |
| MyDecimalToHashKey | 6.097836 | 6.622825 |

## 失败尝试与检查

首次尝试把测量器放入Cargo bench，因构造API、Datum显示方法及安全lint约束而检查失败；日志和当时源码保留。随后将计数器隔离为独立测量程序，修正调用，未改变产品lint。首次单任务优化构建在约15分钟后由我向自己的构建进程组发SIGTERM，退出143；确认可用内存充足后改为4任务并复用缓存。随后独立链接依次暴露新版Rust分离元数据、SDK已有全局分配器及缺少rlib链接项三个问题；最终同时传入rmeta与rlib并使用链接包装器解决。原始日志均保留，构建辅助失败／中断不计为产品测试通过。两份最终二进制的测量源码SHA256相同；对照预检为1次；候选预检误用了`ITERATIONS=1`，测量器实际按默认2000次运行，CSV真实值已保留，正式四轮明确设置DECIMAL_BENCH_ITERATIONS=20000。

现有`shared_kernel_finish_rejects_invalid_control_and_freezes_valid_program`在release实际运行：1通过、0失败、1353过滤，退出0。此时构造器未因debug断言提前退出，非法值确实由程序finish拒绝；日志中的immutable panic属于现有冻结后写入的捕获断言。命令（在tidb/rust）：

    env CARGO_BUILD_JOBS=4 CARGO_PROFILE_RELEASE_LTO=false CARGO_PROFILE_RELEASE_CODEGEN_UNITS=16 ../../tools/cargo-tidb test --release -p tidb-expr --features shared-kernel --lib shared_kernel_finish_rejects_invalid_control_and_freezes_valid_program --locked --offline -- --nocapture

功能31／429项测试沿用固定功能版本记录；新增的是这项release检查及测量断言，没有将测量单元计为新增产品测试。

正式SDK `make clippy`仍未完成；此补证不代表默认SQL接入、完整M1生命周期或完整表达式范围通过。没有运行SQL服务性能测试，不能据局部数据宣布普遍提速或10K数量级目标达成。

## 完整增删与证据

分类沿用功能阶段统计，独立测量器273行全部计入测试代码，Python／CSV／收据等计入文档审计；没有以放在docs目录为由排除。原始对照为TiDB364aef2／SDK548812e，本轮对照为TiDB72fedb／SDK3ad4，包含功能修复和本次补证。以下全部文件从最终Git diff结算，不累加checkpoint净值。

| 对照 | 生产 + / − | 测试 + / − | 构建 + / − | 文档 + / − |
| --- | ---: | ---: | ---: | ---: |
| TiDB原始 | 1034 / 1 | 2495 / 0 | 1867 / 91 | 11408 / 0 |
| SDK原始 | 1693 / 57 | 47 / 4 | 100 / 9 | 15 / 0 |
| TiDB本轮 | 7 / 59 | 288 / 15 | 1 / 0 | 953 / 0 |
| SDK本轮 | 17 / 777 | 0 / 0 | 100 / 9 | 5 / 2 |

相对原始基线，源码与测试+5269/−62，增删5331行；全部文件+18659/−162，增删18821行。相对本轮起点，全部+1371/−862，净增509行；其中生产+24/−836，仍净减812行。补证引入的测量与审计成本没有从总数扣除。

[收据](datatype-reuse-release-validation.json)记录前后固定版本、优化配置、两份二进制哈希、8次运行终态和50份本地日志／构建记录／包装器哈希。当前5525份输入摘要为`e389bb4f12a93a6fc28a7590d2b611313ddb48f96b8951f45ac79605a4a6d92a`；其中只比功能阶段增加测量器，排除它后的5524项摘要仍与固定功能源码一致。对照5525项摘要也与checkpoint7原记录一致。旧功能收据仍只验证其固定历史版本；新收据适用于本次交付。大型编译日志和二进制保存在Engineer工作区`evidence/shared-kernel-datatype/`，CSV已随Git发布；仅有Git读权限可重算表格，核验本地日志需取得该证据目录。

    python3 rust/docs/shared-kernel/verify-checkpoint-6.py --tidb . --tikv ../tikv --receipt rust/docs/shared-kernel/datatype-reuse-release-validation.json --evidence /path/to/evidence/shared-kernel-datatype
    python3 rust/docs/shared-kernel/count-datatype-reuse.py /path/to/parent-of-tidb-and-tikv

最终root `make lint`退出0，两仓`git diff --check`通过，新增Python脚本语法检查及原始CSV重算通过。测量器保留构建时的确切源码，没有在测量后格式化而改变输入哈希。本地没有启动SQL服务；两份优化二进制、预检、失败构建记录及各轮stderr均保留。
