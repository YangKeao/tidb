# PB AND／OR 在同一 RPN 指令循环短路

Task #49 按 caaf90db 执行。TiDB 基线为 6ad11f61，SDK 基线为 af3dcf96，实现固定在 SDK b4ee1f2。 [设计与验收条件](https://github.com/YangKeao/tidb/blob/0a26d277c137a2a187d5f691f929920095154c14/rust/docs/expression-shared-kernel-cost-review.zh.md#task-49-协议-andor-短路执行设计) · [SDK 实现](https://github.com/YangKeao/tikv/commit/b4ee1f2c26f1e7ccf68732d97aaf51ebb78b702f)

## 构建和执行

PB 构建器为每个操作数照常验证签名、初始化 metadata 和检查列引用，然后把获准短路的操作数放进一条指令序列。LogicalStart／LogicalNext／LogicalEnd 标记范围和每个操作数的边界。运行时在原来的指令循环内合并三值结果，按未决输出位置压缩物理行映射。全部位置都未决时沿用原行映射，结束恢复父级栈、行映射及输出宽度。压缩后的行沿用已有临时行选择接口。

协议开关、简单参数使用普通函数、相邻同类合并、混合深度 32 及 PB IF 的求值策略均由原控制条件和既有测试验证。根的完整列引用仍用于该根的预解码。标记保留旧逻辑工作量、不增加逻辑节点数或列引用数。末标记保留完整 FieldType，聚合追加 cast 后统计和返回类型按原方式更新。TiDB SQL 的配置、默认选路和请求标志沿用基线。

删除旧 AND／OR 回调包装 39 行、映射 14 行、函数指针类型 14 行及求值器分派分支 23 行；旧递归循环 117 行内包含迁移的压缩逻辑。新逻辑状态与压缩 102 行，主循环帧与分支 54 行，临时构建结构和线性指令发射 62 行。三值合并主体仍为原函数。以上是选定代码段的物理行数，完整 Git 增删见下表。

## 实际求值与投影

对 `(col0 OR CAST(col1 AS SIGNED)) AND col2` 使用输入物理行号 `[3,1,2,0,3,1]`。修复前进入求值器 5 次，修复后进入 1 次。测试在同一个求值函数入口计数。

| 操作数 | 两侧均实际请求的行 |
|---|---|
| OR 左 | `[3,1,2,0,3,1]` |
| OR 右 | `[3,1,2,3,1]` |
| AND 右 | `[3,2,0,3]` |

两侧结果均为 `[NULL,0,0,1,NULL,0]`。原测试继续覆盖多轮压缩、非零归一化、Int／Enum、空物理映射下的多个常量结果和 PB IF。其它对照覆盖：

- 生产 BatchProjectionExecutor 由实际 PB 构建，使用模拟子扫描给出的物理行 `[2,0,1,0]`，开关两侧均输出 `[NULL,1,1,1]` 和相同字段声明。关闭时两条转换告警，开启后零条。原版与修正版在这组数据中一致。
- 另一个生产投影中，第一根的转换告警发生后，第二根在短路前预解码全部引用列，仍报告坏 Bytes 数据的标志 255。这个错误与被跳过的运行期转换分开观察。被跳过的 regexp 子节点仍在构建时报告坏字面模式。
- 实际 SUM／AVG 表达式重写为逻辑结果追加 Decimal cast，检查了两侧的字段声明、末节点、列引用、工作量与稀疏重复行上的输出。

Endpoint 的 `tests/integrations` 目标在基线和固定 SDK 均因 `src/server/tablet_snap.rs:102` 的 `BorrowedBuf<'_>` 缺少泛型参数而构建失败（E0107）。有表数据的 Endpoint 成功回归留待该目标恢复。上面的生产算子对照验证了 PB 构建、投影执行及输出告警；Endpoint 层保留了实际编译失败记录。

## 验证和固定证据

独立 SDK 表达式 439 项、生产投影 8 项及聚合 41 项通过。TiDB 共享表达式 60 项、SQL 集成 7 项及默认 PREPARE 1 项通过。SDK 限定三包 Clippy、TiDB 根 `make lint` 和 SDK 修改处格式检查通过。完整 SDK 公告检查则保留此前结果，本批未测性能。

```sh
# 独立 SDK 根
cargo test -p tidb_query_expr --lib --locked --offline -- --nocapture --test-threads=1
cargo test -p tidb_query_executors --lib projection_executor::tests --locked --offline -- --nocapture --test-threads=1
cargo test -p tidb_query_aggr --lib --locked --offline
cargo clippy -p tidb_query_expr -p tidb_query_aggr -p tidb_query_executors --lib --locked --offline
# TiDB rust/
cargo test -p tidb-expr --features shared-kernel --lib shared_kernel --locked --offline
cargo test -p tidb-executor --features shared-kernel --test shared_execution --locked --offline
cargo test -p tidb-session --lib prepared_rand_restarts_seeded_and_preserves_session_stream --locked --offline
```

原始日志、完整命令和双仓 Git HEAD／二进制补丁在 `evidence/pb-logical/`。保留基线求值次数断言、Endpoint 两次编译失败及初次新测试对 regexp 诊断文本的断言差异。可重建 18 对跟踪源码树，九次最终成功检查对应固定实现。清单共 81 项，SHA256 为 19227243e43b8e364990fb2f78bf5da6ce5c858174b0f6bd2752d15479123e2a。工具链按既有固定环境，依赖构建缓存另计。

| 对照 | 生产 Rust 新增／删除 | 测试 Rust 新增／删除 | 构建 | 材料 | 总增删 |
|---|---:|---:|---:|---:|---:|
| 相对 task 48 修正版 | 323／339 | 346／80 | 0／0 | 54／0 | 1,142 |
| 相对两仓原始端点 | 4,795／651 | 6,837／84 | 1,990／103 | 20,564／0 | 35,024 |

原始两仓源码与测试累计增删 12,367 行。统计按固定端点直接执行 Git diff，本报告计入材料。

## 复审修正版

按 cd6c6d36、5bfd425e 和 827b3b59 修正两处边界。测试时 TiDB 源码固定在 8e5d1664b8，本节为随后提交的报告。 [SDK 修正版 df5dc56c9f](https://github.com/YangKeao/tikv/commit/df5dc56c9fa0222a3e065620d16715c734d62450)

- `RpnExpression::finish` 给每条指令记录所属操作数。控制边与其目标必须由同一操作数拥有，栈指令只可读取或消耗该操作数自己的值。首版接受的四个反例现均拒绝，包括跳入、跳出、跨操作数以及取走父栈值再补回同类型值。合法的操作数内 IF 仍可分支，外层加法仍读到原有父值 7，并得到两个 8。原有 PB 聚合追加 cast 回归继续通过。
- 默认 `InputColumns::read_selected` 将借用的普通 PB 标量转为自有标量，保留声明类型和 NULL，由调用方按实际逻辑行数广播。公开 `eval_input` 的 PB OR 左右实际请求分别为 `[2,0,1,2]` 和 `[2,1,2]`。首版已正确选行，但三种右值均报标量误作向量；修正后右值 1、0、NULL 分别返回 `[1,1,1,1]`、`[NULL,1,0,NULL]`、`[NULL,1,NULL,NULL]`。直接检查选行接口仍为标量，声明长度为适配器给出的 19。实际生产投影另用存储向量运行，首节结果与告警对照继续通过。
- 筛选和聚合的七处匹配使用原有标量逻辑处理两种持有方式。慢哈希聚合的排序键和原始 Bytes 缓存跨批沿用。生产调用从 `process_batch_input` 经 `eval_exprs_decoded_no_lifetime` 和 `eval_decoded` 固定传入 `LazyBatchColumnVec`，普通列读取和选行只产出向量，PB 函数及逻辑合并也产出向量，因此两个缓存接到的普通标量来自程序常量。自定义适配器验证只经公开 `eval_input`。SDK 公开枚举新增 `OwnedScalar`，下游穷举匹配需涵盖该变体。

修正版独立 SDK 表达式 442 项、执行器 122 项及聚合库 41 项通过。执行器覆盖常量筛选、简单／流式聚合、快慢哈希聚合和生产投影。TiDB 共享表达式 60 项、SQL 集成 7 项及默认 PREPARE 1 项通过。限定三包 Clippy、TiDB 根 lint 和修改处格式检查通过。Endpoint 留在已安排的独立任务，公告检查沿用之前记录。

`evidence/pb-logical-review/` 保存 34 次双仓跟踪源码快照、命令和输出。最终九项检查均对应上面的固定源码。两组基线失败、修订中的夹具及编译失败、格式检查拒绝的两处既有格式改动均保留。证据清单共 143 项，SHA256 为 b1dfbad3d64cf164c19d7b177fb8756f250a4f1a99b18093a34288d49a213d55。源码快照覆盖 Git 跟踪文件，工具链和依赖缓存沿用固定环境。

| 对照 | 生产 Rust 新增／删除 | 测试 Rust 新增／删除 | 构建 | 材料 | 总增删 |
|---|---:|---:|---:|---:|---:|
| 相对 task 49 首版 | 105／59 | 229／0 | 0／0 | 20／0 | 413 |
| 相对 task 48 修正版 | 417／391 | 582／83 | 0／0 | 74／0 | 1,547 |
| 相对两仓原始端点 | 4,809／657 | 7,111／91 | 1,990／103 | 20,584／0 | 35,345 |

修正版相对原始两仓的源码与测试累计增删 12,668 行。累计表把首版一行 `#[cfg(test)]` 内的方法改动计入测试，首版生产 Rust 净变化相应为 −15 行。
