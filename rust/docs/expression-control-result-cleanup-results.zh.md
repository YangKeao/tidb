# 表达式复用：后续私有入口与控制流清理

被测源码为 [TiDB ffb508eeae](https://github.com/YangKeao/tidb/commit/ffb508eeae5804c5b81f39da1f14b60350fe2d73)，分支 expression-unification-demo。TiKV SDK 仍为 f7672bf980143f2938fd0f900650a4c29091e70f，本轮未改。[前一已测43ba报告](expression-shared-host-arguments-results.zh.md)及原始记录保持独立。

## 改动及范围

相对前一已测源码，本次累计净减501行生产 Rust、86行测试辅助代码。前半段85da8c2a85净减339/55行；随后这一段净减162/31行。实际测试函数名和样本保留；删除的是未使用测试辅助函数，另有两处测试语法随接口简化调整。统计包含空行，不含文档；内嵌cfg(test)辅助函数按测试分类。前一报告2582行生产缩减和63行测试增加属于另一轮，未改写其口径。

- 40fa46dfe4、ae4b0b5965：删除28个未使用私有函数和7个未使用测试转发函数。
- 85da8c2a85：数值窗口赋值、名称探测和字符串权重填充直接返回值，保留格式化位置和转换后句柄校验。
- 391ad01658、4075ccac1d：删除空分支、不会失败的内部返回包装和重复闭包层；AST COALESCE直接移交已拥有的选定值。需要承接提前返回和错误传播的闭包、回调后的认证检查仍在。
- f39e38fbd3、ac54ce8992：恢复适配目录未使用代码告警，核对后删除4个生产辅助函数、3个测试辅助函数及2个从未构造的私有正则选项。公开接口及现用正则调用不变。
- ffb508eeae：删除编译器确认不可达的重复Decimal转换分支、两个已穷尽类型后的兜底分支，以及冗余返回包装和一个未使用私有参数。

[本次增量比较](https://github.com/YangKeao/tidb/compare/43ba91c201769b0d7d9930e2da3488870e95ca6f...ffb508eeae5804c5b81f39da1f14b60350fe2d73)。[Main与原始base比较](https://github.com/YangKeao/tidb/compare/364aef2bab5cc633ecb76a775ae8f36f86a6687d...ffb508eeae5804c5b81f39da1f14b60350fe2d73)包含历史变更，不能全部归入本次清理。

## 功能验证

最终1013项检查全部取得终态：1012通过、1项相同已知基线失败。覆盖既有995项，加16项AST/控制流/诊断格式化和2项算术重写测试。新增范围先在固定85da二进制运行，18项均通过；最终2441项测试清单没有缺失，没有运行全部2441项。

已知失败为 optional_common_profiles_record_original_and_actual_admission，原始观察签名仍为1713/657792/D19F5082E4AF34C5，断言仍为 output.status.success()。未把它算作通过。

| 检查 | 实际结果 |
| --- | --- |
| front-build1 | 退出0，278.866秒 |
| front-build2 | 退出0，280.273秒 |
| server-build1 | 退出0，605.013秒 |
| lint1 | 退出0，2.521秒 |

第一次构建对应中间提交f39e38fbd3，第二次及实际服务对应ffb508eeae，均成功；54个变更Rust文件格式检查通过。85da中间版本另有995项994通过/1项同基线失败、补充16项通过及服务构建记录；该版本没有SQL计时，不能将其结果当作后来源码的验证。

最终服务SHA256：7e2b08d36cbf55adcc6701a08315135dad8fd0f813f5eb694ab31eb9df76d4b2；单测二进制SHA256：2d769decf3324d36d35f1afe0f14428035d9874487c7d40f5346aa59ae59d8aa。源码、SDK及受保护文件的前后快照一致。SDK的1159通过/1项已知失败/1项忽略继承自完全相同的1574个输入，并非本次重跑。没有新Go/datatype全量检查或分配失败注入。

以下命令在Main根目录运行，RUSTFLAGS为空，CARGO_BUILD_JOBS=1：

    ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j1 -p tidb-expr --lib --no-run --message-format=json
    ../tools/cargo-tidb build --manifest-path rust/Cargo.toml --release --locked --offline -j1 -p tidb-server --bin tidb-server --no-default-features
    make lint

逐项测试用固定二进制执行 --exact <名称> --test-threads=1 --format=json -Z unstable-options。完整名称、终态、资源限制和输出见数据目录。格式检查用rustfmt --edition 2021 --config skip_children=true --check。

## SQL测量

首次尝试在计时前退出1（26.468秒）：39处比较仅计划估算行数estRows不同，共114个单元字段，包含相同二进制A/B。已保存完整失败记录及逐字段证明；只重启一次同一临时数据库，随后仍按原严格条件检查，没有过滤计划字段或放宽判定。

北京时间2026-11-13 03:06:46至03:23:24，测量退出0，用时998.168秒，共224个单元、140160次完整查询。56次严格预检和448条前后记录（672次完整观测）均核对通过；未观察到已知编译、测试或profiler干扰，全部自建服务及临时数据库目录已清理。

A/B使用同一个已测43ba91c201二进制（SHA256783723c09121c6e64512586a2fbe43b8fa9aa30a1d7983e7ce15973fb1048629），C为本次累计版本，F为原始Rust参考（99a4c3e566774ba9733bc9d54366cf1e9b85446f63fa9a7418bfef5d3daeddd2）。每个单元至少4秒且30次查询，四轮平衡顺序；实际读取UTC会话、init_chunk_size=32、max_chunk_size=1024、vectorized=1。普通负载如实保留，quiet_observed=False；同版A/B用于观察环境波动，不是自动消除回退的理由。

测量整条SQL执行及完整取回结果，包含规划、RPC与编码，不能当作表达式内核独立耗时。与前一报告保持相同14条查询。JSON场景仍为实际支持的构造/提取；JSON_SUM_CRC32的ARRAY重写入口尚不支持，SQL性能未测，也未覆盖存储JSON读取。

| 场景 | CPU C/A | CPU C/B | CPU C/F | 同版CPU B/A |
| --- | --- | --- | --- | --- |
| 日期／格式／Unix时间 | +0.442% | +0.467% | +116.882% | -0.026% |
| 压缩／解压 | -1.598% | -1.811% | +5.207% | -0.607% |
| MD5／SHA2／PASSWORD | +0.020% | -0.683% | +185.455% | +0.413% |
| DATE_ADD／DATE_SUB | +2.185% | +1.860% | +100.429% | +1.002% |
| EXTRACT／STR_TO_DATE | +1.460% | +1.654% | +149.227% | -0.474% |
| JSON构造／提取 | +1.642% | +1.275% | +321.298% | +0.375% |
| 时间差／MAKETIME | -0.994% | -0.785% | +158.867% | -0.310% |
| UUID转换 | -0.602% | -1.898% | +199.642% | +0.901% |
| Decimal4096行 | +2.154% | +2.219% | +277.868% | +0.129% |
| 整数4096行 | +1.394% | -0.234% | +58.988% | +0.800% |
| 扫描4096行 | -1.544% | -4.788% | +0.652% | +1.213% |
| 文本4096行 | +3.114% | +2.563% | +393.774% | +0.726% |
| 深16层／1行 | -0.103% | +0.263% | +53.512% | -0.440% |
| 深16层／4096行 | +1.512% | +1.302% | +459.526% | +0.630% |

四轮相对A和B均上升：EXTRACT／STR_TO_DATE、JSON构造／提取、Decimal4096行、文本4096行、深16层／4096行；均下降：时间差／MAKETIME。其余方向混合或持平。

同版B/A的CPU成对中位变化在不同场景间为-0.607%至+1.213%。变化列为同轮比值中位数，不能用绝对值中位数相除替代；四轮最小/最大值只是观测范围，不是置信区间。[全部CPU、p50、p95、QPS绝对值及变化范围](expression-control-result-cleanup-results/all-result-tables.md)提供完整数据。

按要求保留简化供审阅，不因一次性能比较自动撤回。原项目的全部路径统一与普遍性能目标仍未由这些有限场景证明完成。

## 独立复核

[数据目录](expression-control-result-cleanup-results/)含源码输入哈希、实际回执、所有时延、完整测试终态与输出、预检及观测、配置、顺序和清理记录。重复语义观测以JSON字典去重，可逐字节还原；二进制、完整编译和服务日志留本地，不提供源码压缩包。

    python3 rust/docs/expression-control-result-cleanup-results/verify-results.py

核验脚本兼容Python3.9，只依赖标准库，不启动服务。它检查文件哈希、最终1013项和中间995项终态、18项补充基线、构建回执、被测提交，并重算224个计时单元及全部表格。本地完整证据位于home-gentoo代理工作区 evidence/path-unification/control-result-cleanup；中间85证据位于unused-private-helpers，前一已测证据位于shared-host-arguments/overnight。
