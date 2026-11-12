# 表达式复用：直接调用数值恢复函数

被测源码为 [TiDB 84df525b25](https://github.com/YangKeao/tidb/commit/84df525b257f0cab76e4883a65be183095ac1a4f)，分支expression-unification-demo；SDK仍为f7672bf980143f2938fd0f900650a4c29091e70f。本轮仅修改2个生产文件，净减6行，测试及样本未改。[前一版报告](expression-resume-dispatch-results.zh.md)固定记录6765b4efa2；本轮数据独立保存。

## 改动及验证

数值单行与窗口恢复各去除一个仅包住单次函数调用的立即调用闭包，直接接收驱动函数的结果。两处调用后的认证、之后的错误传播、状态分支、提前返回和借用范围均保留。没有扩大接口或修改测试、样本。其他闭包中的局部错误处理和回调后校验保留；详见[剩余逻辑审阅记录](expression-numeric-direct-call-results/REMAINING-LOGIC-REVIEW.zh.md)。

本轮2行增加、8行删除，净减6行生产Rust；与上一轮合计，相对43ba91c201净减551行生产代码、86行测试辅助代码。空行计入，文档除外；原先2582行缩减及63行测试增加属于更早一轮，未重算混用。[本轮差异](https://github.com/YangKeao/tidb/compare/6765b4efa267025ab916d05f9040fab2ffe09b1c...84df525b257f0cab76e4883a65be183095ac1a4f)可直接审阅。

当前固定二进制执行同一1013项选择，1012通过、1项相同已知基线失败；2441项测试清单保留。已知失败为optional_common_profiles_record_original_and_actual_admission，签名1713/657792/D19F5082E4AF34C5，断言仍为output.status.success()。没有将失败计为通过，也没有重新执行全部2441项。

| 检查 | 实际结果 |
| --- | --- |
| front-build1 | 退出0，279.977秒 |
| consumers1 | 退出0，1043.144秒 |
| server-build1 | 退出0，604.418秒 |
| lint1 | 退出0，2.574秒 |

2个Rust文件格式检查通过，源文件、SDK、原始参考仓库和受保护BUILD文件的前后记录一致。当前服务SHA256为bdfa816bb44cccaa84ac9808cc8f2664c015e490dc37eda8b563be1b1351d17b，单测二进制为7105e50a7b9f0c775a08ea6d8123fa44cc9500afddd78e76028371e948561d52。SDK沿用完全相同1574个输入对应的1159通过/1已知失败/1忽略，未重跑；没有新Go/datatype全量检查或分配失败注入。

从Main根目录运行，RUSTFLAGS为空且CARGO_BUILD_JOBS=1：

    ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j1 -p tidb-expr --lib --no-run --message-format=json
    ../tools/cargo-tidb build --manifest-path rust/Cargo.toml --release --locked --offline -j1 -p tidb-server --bin tidb-server --no-default-features
    make lint

逐项测试执行固定二进制的--exact <名称> --test-threads=1 --format=json -Z unstable-options。格式检查使用rustfmt --edition 2021 --config skip_children=true --check；文件列表、实际配置和逐项终态均在数据目录。

## SQL测量

首次SQL预检仅估算行数不同，逐字段证明和全部原始记录已保留。measurement2在连接SQL前因首次工作文件未移走而被保护断言拦下，这是归档操作错误；该失败没有发起SQL。移走已确认与归档相同的工作文件后恢复严格预检。因此共重新启动两次本轮服务，实际SQL预检只重试一次；比较条件未放宽。 有效记录为measurement3.receipt.json，北京时间2026-11-13 04:44:45至05:01:23，实际退出0/997.947秒，224个单元、139471次完整查询。56次严格预检、448条前后记录及672次完整观测核对通过，未观察到已知编译、测试或profiler干扰。全部自建服务和两处临时数据库目录已清理。

A/B均为前一已测6765b4efa2的同一个二进制（6e89d6b18b079c3af5ddfa5a9c1adabfe63a50598714fc6f92a050d08148c43e），C为本次版本，F为原始Rust参考（99a4c3e566774ba9733bc9d54366cf1e9b85446f63fa9a7418bfef5d3daeddd2）。14条SQL与上一轮完全相同；每单元至少4秒且30次查询，四轮平衡顺序，设置UTC会话，并实际读取确认init_chunk_size=32、max_chunk_size=1024、vectorized=1。计时覆盖SQL执行及完整取回，包含规划、RPC和编码，不能解释为表达式内核耗时。CPU列仅计目标TiDB服务进程，不含客户端与PD/TiKV进程的CPU。

| 场景 | CPU C/A | CPU C/B | CPU C/F | 同版CPU B/A |
| --- | --- | --- | --- | --- |
| 日期／格式／Unix时间 | -1.214% | -1.452% | +116.025% | +0.242% |
| 压缩／解压 | -2.949% | -2.700% | +3.935% | -1.173% |
| MD5／SHA2／PASSWORD | -1.136% | -1.372% | +186.186% | -0.107% |
| DATE_ADD／DATE_SUB | +0.053% | -0.736% | +99.987% | +1.269% |
| EXTRACT／STR_TO_DATE | -0.671% | -0.682% | +145.597% | +0.046% |
| JSON构造／提取 | -0.967% | -1.070% | +314.200% | +0.192% |
| 时间差／MAKETIME | -1.512% | -1.301% | +157.905% | -0.228% |
| UUID转换 | -1.672% | -2.597% | +197.860% | +0.817% |
| Decimal4096行 | -1.525% | -1.643% | +277.690% | +0.154% |
| 整数4096行 | +1.763% | +1.297% | +58.623% | +1.247% |
| 扫描4096行 | -3.802% | -2.368% | -1.499% | +0.315% |
| 文本4096行 | -1.098% | -1.460% | +370.786% | +0.243% |
| 深16层／1行 | -0.035% | +0.445% | +53.985% | -0.387% |
| 深16层／4096行 | -0.414% | -0.786% | +456.951% | +0.228% |

四轮相对A/B均上升：无；均下降：压缩／解压、MD5／SHA2／PASSWORD、EXTRACT／STR_TO_DATE、时间差／MAKETIME、UUID转换、Decimal4096行、扫描4096行、文本4096行。其余方向混合或持平。

同版B/A的CPU成对中位变化在场景间为-1.173%至+1.269%。变化值是同轮比值的中位数，四轮最小/最大仅为观测范围，不是置信区间。这些是整条SQL的本次配对观测，不能将全部下降归因于两处包装删除。[全部CPU、p50、p95、QPS绝对值和变化范围](expression-numeric-direct-call-results/all-result-tables.md)均已提供。普通负载如实记录，quiet_observed=False；不把波动当作自动忽略回退的理由。

JSON仍测量支持的构造/提取，JSON_SUM_CRC32的ARRAY重写入口不支持，SQL性能及存储JSON读取未覆盖。按要求保留简化供审阅，不因单轮性能结果自动撤回；全部路径统一和普遍性能目标仍未由这些有限场景证明完成。

## 全项目审阅入口

整条分支与原始base的差异可通过[TiDB全部差异](https://github.com/YangKeao/tidb/compare/364aef2bab5cc633ecb76a775ae8f36f86a6687d...84df525b257f0cab76e4883a65be183095ac1a4f)及[SDK全部差异](https://github.com/YangKeao/tikv/compare/548812e1ef57aef077a2062a9cc356640a6347f5...f7672bf980143f2938fd0f900650a4c29091e70f)审阅。这些比较包含之前的实现、文档与验证历史，不能把全部差异行数归于本轮。两行直接调用的本轮差异已在上文单列。

## 独立复核

[完整数据](expression-numeric-direct-call-results/)包含逐查询时延、完整单测终态与输出、源输入哈希、实际回执、预检、语义观测、配置和清理记录。语义观测使用可逐字节还原的JSON字典，二进制及完整编译/服务日志留在本地。上一轮固定证据仍在提交2897e0289d，无源码压缩包。

    python3 rust/docs/expression-numeric-direct-call-results/verify-results.py

脚本仅依赖Python3.9标准库，核验所有文件哈希和1013项终态，重建448条观测并重算224个计时单元及全部表格。home-gentoo本地完整证据位于代理工作区evidence/path-unification/numeric-direct-call-cleanup。
