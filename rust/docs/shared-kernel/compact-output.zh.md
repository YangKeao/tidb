# 紧凑 Decimal 载体及直接 Chunk 输出原型

本轮贯通了原 datatype 紧凑计算、共享 RPN 载体及根输出。在同一真实列场景中，保留的 owned 向量接口四轮耗时均降低46.40%–47.24%；新增 direct_chunk 入口中位8.210微秒，仍比当轮 native 向量高22.18%–30.95%。这是有限原型结果，不是默认 SQL、完整共享内核或普遍提速结论。分配字节数增加，完整 SDK Clippy 也没有通过。

范围依据 [固定设计](https://github.com/YangKeao/tidb/blob/0fd36186d253a107680bf6c466ca9cce2416b02d/rust/docs/expression-shared-kernel-cost-review.zh.md)：task #43 在原接纳范围内完成一次端到端原型，到固定结果复审结束，不扩函数矩阵、生命周期或默认 SQL。

## 固定版本与实现

基线 TiDB `e18f0b5c282c728192b0c502d2e151c405807b0d`、SDK `780ab3e776f47d634ac8963437baf8477a7b2df7`。
原所有者提交 `de8a34cd3c231abe53180fe61ee0da30120817cd`；最终产品源码及测量程序见本报告配套收据的 `source_commits`，后续报告提交不改生产源码。两侧均在 `expression-shared-kernel-m1` 分支。

- `tidb-datatype/src/decimal/compact.rs` 收拢原 native `DecimalValue` 的加、减、乘、整数、尺度和 MyDecimal 写出。native 的整数/cast 调用全部迁移；旧结构方法体删除。跨 crate 的原紧凑方法保留内联机会。原 Decimal 的加法、乘法快路径共用 primitive，并保留其无符号幅值限制；负 `i128::MIN` 不能因此绕过原回退。
- 私有 `DecimalArithmeticValue` 保存紧凑值或原 Decimal。共享紧凑乘法限制 storage_scale≤38、结果显示尺度≤30及原幅值/乘积范围；加减除和 miss 委托已求值操作数上的原 Decimal。宽值、带符号零、声明形状和隐藏精度保留。列的未指定 fraction 规则由 datatype 的共同 `from_column` 转换承担，Row 和共享输入均调用它。
- SDK `ExpressionDecimal` 引用原所有者；常量、参数及 IF 仍通过原 RPN 运转。有效性检查不为紧凑值重新生成 ASCII；宽值继续验证原完整系数。
- `SharedProjection::evaluate` 与新增 `evaluate_into_chunk` 共用 `with_results` 中的一次 `eval_group`。直写在求值前检查列数、物理布局和可检测别名；全部根成功且结果验证通过后才 reset/write。失败保留旧目标，已经发生的副作用不回滚。空选择成功清空目标，保留重复选择和顺序。宽值仍走原 lossy 写出，非 Decimal 沿用原适配。

输出必须由调用者独立持有；可检测输入/输出和输出内部的列别名，不宣称能发现所有第三方持有者。原 owned 接口保持，未增加第二解释器、跨调用 scratch 或自动回调旧表达式。

## 最终完整调用数据

场景仍为 `column_mul/64`：两列实际 MyDecimal、六个左 NULL、58个非空带符号乘积。四个旧调用路线在 before/after 都保留，直写只有 after。包含输入读取、求值、输出写入及临时值释放；准备/输入构造不计时。直写仅在入口内 reset 一次。

下表为四轮每调用纳秒的 min / median / max。分配独立测量，以每调用次数/字节列出；分配版计时不参与性能比较。

| 版本与入口 | min / median / max ns | 分配次数 | 分配字节 |
| --- | --- | ---: | ---: |
| before native Vector | 6560.598 / 6590.180 / 7110.885 | 12 | 14976 |
| after native Vector | 6290.394 / 6293.847 / 6738.387 | 12 | 14976 |
| before native Scalar | 22299.087 / 22350.928 / 22677.560 | 58 | 1740 |
| after native Scalar | 22413.948 / 22576.937 / 22846.197 | 58 | 1740 |
| before shared owned Vector | 18873.497 / 18940.942 / 19261.038 | 70 | 23796 |
| after shared owned Vector | 10074.334 / 10101.256 / 10161.655 | 71 | 36540 |
| before shared owned Scalar | 30669.337 / 30954.323 / 31484.460 | 371 | 96596 |
| after shared owned Scalar | 20905.535 / 20938.207 / 21500.546 | 314 | 115804 |
| after direct_chunk Vector | 8180.134 / 8209.513 / 8236.986 | 12 | 29616 |

成对四轮变化：owned Vector −47.24%至−46.40%，owned Scalar −33.47%至−29.90%；native Vector −11.46%至+2.71%，native Scalar −0.73%至+2.37%。原生并非每轮都改善。机器未隔离、路线内部顺序固定，不能把小幅 native 变化全部归因于所有者迁移。

direct_chunk 是不同输出接口，不能冒充原 owned 接口的同口径 before/after。它取消系数物化后分配次数为12，但分配字节29616高于旧 owned 的23796，约为 native 的两倍。当前紧凑/宽值封装及形状存储使中间列更宽；本轮保留该成本，不宣称内存改善。只证明当前单场景的调用耗时改善，不能外推 SQL 服务表现。

## 测量与构建证据

使用声明的 nightly-2026-08-22、普通 Cargo/rustc、GCC14、`CXXFLAGS=-include cstdint`、`CMAKE_POLICY_VERSION_MINIMUM=3.5`。环境沿用 task41 的标准 Cargo/Rustup 缓存，无临时编译器包装器。release opt-level=3、LTO off、codegen-units=16；不是产品 thin-LTO 配置。CPU23，四轮交替版本，每单元20000次，32次预热；36个计时单元。独立分配轮9个单元，每路线100次计数，System allocator 自检保留。

before 沿用 task42 已验证的原产品库；task42后仅测量/报告发生变化，其产品源码与本轮基线相同。使用本轮同一测量源码重新链接 before，关闭 `direct_chunk` cfg；after 开启该 cfg。源码中相同 fixture、值断言及旧路线保持一致。结果检查包含完整符号、系数、存储尺度、显示尺度、声明形状和 Chunk 读回。最终同一产品版本的计时/分配二进制分别链接相同12份显式 rlib/rmeta；两类二进制不同，只有分配版含 malloc wrapper。12份显式库及选定后缀源码清单都不是整个构建输入闭包。

封存时发现第一组 after 计时/分配构建的 tidb_chunk 与 tidb_expr 共四份显式库摘要不同。初轮二进制、构建收据和36/9个单元保留为 `*-initial`；被覆盖的四份初始库不能再逐字核对，差异原因未确定，初轮不作为最终对照。随后不改产品源码，直接使用分配版固定库重新链接计时版，核对两版12份库完全相同，再重跑全部四轮计时和分配轮；本表仅采用后者。预格式化 before 链接尝试也单独保留。

对应可复核文件：

- [计时 CSV](compact-output-timing.csv)、[分配 CSV](compact-output-allocations.csv)、[重算程序](summarize-compact-output.py)。在本目录运行 `python3 summarize-compact-output.py`。
- [验证收据](compact-output-validation.json)，配合原 `verify-checkpoint-6.py` 核对选定源码与本地记录。
- Engineer 本地 `evidence/compact-output/` 保存源码快照、四个最终二进制/库副本、原始测量、全部失败记录、构建图及运行脚本。未创建服务或临时数据库。

复现构建仍用 `build-decimal-bench.py`，after 增加 `--direct-chunk`，分配另加 `--allocations`；before 不加新 API 开关。运行 `run-decimal-bench.py --before <before> --after <after> --columns --direct-chunk --rounds 4 --iterations 20000 --output <directory>`。分配改 `--allocations --rounds 1 --iterations 100`；最终收据另外记录了同库重链接的具体 rustc 命令。

## 验证范围与失败记录

最终固定源码：43项 shared（原41加2个直写边界测试）、8项 prepared、90项 datatype Decimal（原89加1个载体测试）、10项 Chunk Decimal及1项原 native 向量加减乘/整数/cast 对照通过。现有实际列测试增加直写比较，覆盖标记形状、未指定尺度、宽值、NULL、隐藏精度、分支、重复乱序选择、空批及复用。新增测试覆盖带符号零、i128边界、宽值 lossy 输出、后根溢出保留旧输出、RAND不回滚、前置布局/别名拒绝及模式次序。原 datatype 预期值测试提供独立结果校验；同时改后两条路径相等并非唯一依据。

SDK 独立检出429项库测试通过；没有相邻 TiDB、没有 Git alternates。SDK Git pin/锁文件指向 de8a34cd3c，独立图五个 TiDB包与本地图分别仅有一个身份；SDK与native在本地图连接同一个 datatype。172个所有者包文件/资源及1421个 SDK选定输入摘要匹配。主图首次 metadata 未开启 shared feature，校验未找到 SDK 节点；失败记录保留，最终按 `--features tidb-expr/shared-kernel` 重取并验证实际图。

限定 native 库 Clippy 和根 `make lint` 通过。完整 SDK `make clippy` 实际运行9.43秒、退出2：cargo-deny 的 advisories 为19个错误，bans/sources通过、licenses有3个警告，未到完整 workspace Clippy。本次不是超时中断；同一公告库下旧 SDK780ab3复查也是同19个 RUSTSEC ID。未在本原型中更新这些无关第三方依赖，也不把完整检查描述为通过。独立检出缺少标签产生 git describe 提示，实际编译/测试终态保留。

开发过程另有三项测试构造错误：新测试把 `&str` 传给要求 owned digits 的构造器而编译失败；用公开非法字面量构造测试值先触发 debug assert，改为模块内直接构造非法验证样本；直写测试的预期 Chunk 未设置虚拟行数，字节相同但整体比较失败，补齐预期元数据后通过。均是测试修正，未当作产品缺陷前后证明。所有原始失败与最终终态分开保存。

完整 SDK/workspace Clippy、macOS、默认 SQL、跨执行生命周期、245族覆盖及普遍性能目标仍未完成。

## 全部增删与交付边界

首个贯通版本已报告1045行增删：生产663、测试287、构建12、文档83；810只是初估，已经超出。最终报告、CSV及收据也计入总量，最终数字见本节下方。未移除测量材料或搬目录来压低统计。原始两仓基线仍为 TiDB364aef2b、SDK548812e1；总量包括新增与删除两端，不以净值替代。

最终本批：生产+463/−200，测试+285/−2，构建+6/−6，文档/数据/核验+848/−2；全部+1602/−210，净增1392，增删合计**1812**。相对原始两仓基线，源码与测试增删**8001**；全部+27634/−398，增删合计**28032**。

本轮固定原型交 Designer 复审，task #43 转入审阅；人类批准仍待确认。没有默认接入、PR、部署或生命周期扩展。
