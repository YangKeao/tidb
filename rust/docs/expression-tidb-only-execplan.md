# 在 Rust TiDB 中复用原版 TiKV RPN

本执行计划遵循仓库根目录的 PLANS.md。

## 目标

Rust TiDB 的表达式与进程内 Unistore 使用原版 TiKV 的 RPN 求值器。TiDB 从 Git 锁定 TiKV 548812e1ef57aef077a2062a9cc356640a6347f5。TiKV 源码改动为零，TiDB 的修改在 rust/ 下。原先两仓候选保留供对照。

RPN 是把子表达式排在使用它的指令前面的程序。普通指令调用原版 PB 签名绑定的回调。SQL 分支按需要调用同一个 eval_decoded 接口执行选中的子程序。语句时钟、用户变量、随机数和错误策略由 TiDB 传入。

## 进度

- [x] 用类型化 HOUR、IF、RAND 和溢出对比验证真实 SQL 投影。
- [x] 迁移上一版候选的公共入口，删除已退出的递归求值路径。
- [x] 用官方 Git 提交构建服务，核对 Cargo 解析的 TiKV 包和实际编译环境。
- [x] 在原版 TiKV 上复跑表达式单测，修复已发现的声明、分支、转换和诊断差异。
- [ ] 比较固定的执行器、Unistore 与 Session 套件，收口新增差异。
- [ ] 重放真实 SQL 与性能场景，固定提交、证据和适用范围。

## 意外与发现

原版 TiKV 的 PB 映射函数是私有接口，RpnExpressionBuilder 和公开节点已足以取得绑定后的回调。保留了前者对于 PB 签名、参数和元数据的校验。

共用 Chunk 和 SDK Decimal 之间的转换会报告错误。物理整数和字节列按选择集读入。Decimal 在第一次实际读取时转换，同一次 Chunk 求值中的重复列引用复用转换成功的结果。

Linux 上全局 Rust 分配器由原版 tikv_alloc 注册为 System。默认服务链接 TiDB 已有的 jemalloc 进程分配符号，关闭默认功能后使用 glibc。测试记录出自真实服务的分配调用。

## 决策记录

- 直接调用原版 eval_decoded，统一保留求值栈和错误传播。TiDB 的短路回调只负责选择行并调用子程序。
- PB 回调优先使用原版注册函数。保留无法从 PB 表获得的宿主能力，以及固定对照已证明存在差异的本地 SQL 回调。
- 时间类型通过原版 SDK 公开的 Chunk 字节读写接口连接。SQL 字段声明、客户端格式和会话时区由 TiDB 保留。
- 进程内 Unistore 使用同一份原版 TiKV 依赖。远端 TiKV 自行处理已下推的表达式。

## 所在目录

实现入口为 rust/crates/tidb-expr/src/rpn.rs。子模块 rpn/engine.rs 使用原版节点及回调。rpn/rows.rs 把 TiDB Chunk 的值传入求值器，并把结果还原为 TiDB 的 Datum。rust/crates/tidb-executor 接入 SQL 算子。rust/crates/tidb-unistore 解码本地 DAG 请求。

## 验收

在 rust/ 下执行带 Cargo.lock 的验证。启用正式依赖的构建必须成功。首次运行要能访问 Git 和 Cargo 源，填充缓存后可加 --offline。

    cargo test --locked -p tidb-expr --lib --tests
    cargo test --locked -p tidb-executor --test unified_expression
    cargo test --locked -p tidb-unistore --lib
    cargo test --locked -p tidb-session --lib
    cargo test --locked -p tidb-session --test all
    cargo metadata --locked --format-version 1 --filter-platform x86_64-unknown-linux-gnu

Session 基线已有失败，逐个比对测试名。Rust TiDB 服务应在相同数据和语句设置下复现固定 SQL 的结果、告警及错误。性能比较记录二进制、CPU、分配器、测试参数和原始样本。最终交付附上源码差异统计及证据入口。

## 恢复和回滚

分支从原始 TiDB 364aef2bab5cc633ecb76a775ae8f36f86a6687d 起步，先保存试验提交 c688574726。用独立工作树比较两仓候选 TiDB 25cc03dde38b3f6fcc60c0abed15a0fff428e61a 与 TiKV 57127f26bdb0f969c89d8cdd390ffd1846e376b6。回滚候选时切回原始 TiDB 工作树即可。

## 结果与复盘

最终固定提交和结果待完成。
