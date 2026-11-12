# 表达式复用：简化任务分派与直接调用

被测源码为 [TiDB 6765b4efa2](https://github.com/YangKeao/tidb/commit/6765b4efa267025ab916d05f9040fab2ffe09b1c)，分支expression-unification-demo；SDK仍为f7672bf980143f2938fd0f900650a4c29091e70f。本轮仅修改6个生产文件，净减44行，测试及样本未改。[前一版报告](expression-control-result-cleanup-results.zh.md)固定记录ffb508eeae；本轮数据独立保存。

## 改动及验证

无参数启动把最多14次重复任务读取改为一次读取后match分派，保留原有处理函数和取消守卫。数值父节点直接使用validate_row_reply返回的任务，其普通恢复和诊断恢复入口仍验证回复。另去除参数类型、CONCAT、eager求值、用户变量读写和WEIGHT_STRING的7处立即调用闭包；提前返回、错误传播以及回调后的句柄校验均保留。没有为21个单入口适配函数引入携带状态的新结构或扩大参数接口。

本轮48行增加、92行删除，净减44行生产Rust；与上一轮合计，相对43ba91c201净减545行生产代码、86行测试辅助代码。空行计入，文档除外；原先2582行缩减及63行测试增加属于更早一轮，未重算混用。[本轮差异](https://github.com/YangKeao/tidb/compare/ffb508eeae5804c5b81f39da1f14b60350fe2d73...6765b4efa267025ab916d05f9040fab2ffe09b1c)可直接审阅。

当前固定二进制执行同一1013项选择，1012通过、1项相同已知基线失败；2441项测试清单保留。已知失败为optional_common_profiles_record_original_and_actual_admission，签名1713/657792/D19F5082E4AF34C5，断言仍为output.status.success()。没有将失败计为通过，也没有重新执行全部2441项。

| 检查 | 实际结果 |
| --- | --- |
| front-build1 | 退出0，280.175秒 |
| consumers1 | 退出0，1038.210秒 |
| server-build1 | 退出0，603.842秒 |
| lint1 | 退出0，2.268秒 |

6个Rust文件格式检查通过，源文件、SDK、原始参考仓库和受保护BUILD文件的前后记录一致。当前服务SHA256为6e89d6b18b079c3af5ddfa5a9c1adabfe63a50598714fc6f92a050d08148c43e，单测二进制为a586f244b7430c7a8bef1a5d29307a13357278abc5e40ce4fe2467d9fab84b7b。SDK沿用完全相同1574个输入对应的1159通过/1已知失败/1忽略，未重跑；没有新Go/datatype全量检查或分配失败注入。

从Main根目录运行，RUSTFLAGS为空且CARGO_BUILD_JOBS=1：

    ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j1 -p tidb-expr --lib --no-run --message-format=json
    ../tools/cargo-tidb build --manifest-path rust/Cargo.toml --release --locked --offline -j1 -p tidb-server --bin tidb-server --no-default-features
    make lint

逐项测试执行固定二进制的--exact <名称> --test-threads=1 --format=json -Z unstable-options。格式检查使用rustfmt --edition 2021 --config skip_children=true --check；文件列表、实际配置和逐项终态均在数据目录。

## SQL测量

首次尝试在计时前失败；完整回执、原始观测及估算行数逐字段证明均保留。仅重启一次同一临时数据库，严格比较条件没有放宽。 有效记录为measurement2.receipt.json，北京时间2026-11-13 03:54:27至04:11:05，实际退出0/997.965秒，224个单元、139985次完整查询。56次严格预检、448条前后记录及672次完整观测核对通过，未观察到已知编译、测试或profiler干扰。全部自建服务和两处临时数据库目录已清理。

A/B均为前一已测ffb508eeae的同一个二进制（7e2b08d36cbf55adcc6701a08315135dad8fd0f813f5eb694ab31eb9df76d4b2），C为本次版本，F为原始Rust参考（99a4c3e566774ba9733bc9d54366cf1e9b85446f63fa9a7418bfef5d3daeddd2）。14条SQL与上一轮完全相同；每单元至少4秒且30次查询，四轮平衡顺序，设置UTC会话，并实际读取确认init_chunk_size=32、max_chunk_size=1024、vectorized=1。计时覆盖SQL执行及完整取回，包含规划、RPC和编码，不能解释为表达式内核耗时。CPU列仅计目标TiDB服务进程，不含客户端与PD/TiKV进程的CPU。

| 场景 | CPU C/A | CPU C/B | CPU C/F | 同版CPU B/A |
| --- | --- | --- | --- | --- |
| 日期／格式／Unix时间 | +1.484% | +1.796% | +117.809% | +0.036% |
| 压缩／解压 | +2.366% | +2.696% | +7.035% | -1.029% |
| MD5／SHA2／PASSWORD | +1.598% | +1.568% | +191.392% | +0.463% |
| DATE_ADD／DATE_SUB | +0.434% | +0.105% | +97.517% | +0.100% |
| EXTRACT／STR_TO_DATE | +0.956% | +0.054% | +146.730% | +0.575% |
| JSON构造／提取 | -0.138% | -0.341% | +321.470% | +0.170% |
| 时间差／MAKETIME | +1.644% | +0.880% | +161.451% | +0.757% |
| UUID转换 | +5.088% | +3.418% | +203.796% | +1.600% |
| Decimal4096行 | -0.518% | -0.548% | +279.445% | +0.059% |
| 整数4096行 | +2.108% | -2.586% | +51.044% | +1.721% |
| 扫描4096行 | +4.277% | +1.715% | +4.701% | +3.063% |
| 文本4096行 | +0.080% | -0.467% | +383.252% | +0.552% |
| 深16层／1行 | -0.325% | -0.472% | +52.863% | -0.039% |
| 深16层／4096行 | +0.668% | +0.958% | +465.272% | -0.112% |

四轮相对A/B均上升：日期／格式／Unix时间、MD5／SHA2／PASSWORD、时间差／MAKETIME、UUID转换、扫描4096行；均下降：无。其余方向混合或持平。

同版B/A的CPU成对中位变化在场景间为-1.029%至+3.063%。变化值是同轮比值的中位数，四轮最小/最大仅为观测范围，不是置信区间。[全部CPU、p50、p95、QPS绝对值和变化范围](expression-resume-dispatch-results/all-result-tables.md)均已提供。普通负载如实记录，quiet_observed=False；不把波动当作自动忽略回退的理由。

JSON仍测量支持的构造/提取，JSON_SUM_CRC32的ARRAY重写入口不支持，SQL性能及存储JSON读取未覆盖。按要求保留简化供审阅，不因单轮性能结果自动撤回；全部路径统一和普遍性能目标仍未由这些有限场景证明完成。

## 独立复核

[完整数据](expression-resume-dispatch-results/)包含逐查询时延、完整单测终态与输出、源输入哈希、实际回执、预检、语义观测、配置和清理记录。语义观测使用可逐字节还原的JSON字典，二进制及完整编译/服务日志留在本地。上一轮固定证据仍在提交28b19112e3，无源码压缩包。

    python3 rust/docs/expression-resume-dispatch-results/verify-results.py

脚本仅依赖Python3.9标准库，核验所有文件哈希和1013项终态，重建448条观测并重算224个计时单元及全部表格。home-gentoo本地完整证据位于代理工作区evidence/path-unification/resume-dispatch-cleanup。
