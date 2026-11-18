# PB 存储 Endpoint 与部分选行的向量声明

Task #50 按 3b73602d 验证。SDK 实现 872241c7a98342db18b60ea603de838f749d6c8d，TiDB 调用方 34dde9c1a01db8d679491a20c48b88d5a251c946。TiDB 默认 SQL 请求标志沿用基线。 [SDK 固定版本](https://github.com/YangKeao/tikv/commit/872241c7a98342db18b60ea603de838f749d6c8d)

## 带表数据的存储 Endpoint

在旧递归版 af3dcf96 和单循环版 df5dc56 上使用同一补丁。固定工具链为 nightly-2026-08-22，rustc 提交 c656540d6467。两版均将 `tablet_snap.rs` 的类型注解补为 `BorrowedBuf<'_, u8>`，匹配 `Vec<u8>` 的备用容量，读取调用、容量处理和长度处理沿用原实现。对照补丁逐字节相同，SHA256 为 `ca3269598fe81cf351ecc64f815eecf2f98770ab48e629684af5da21ebcaa9f0`。

用 `ProductTable` 写入三条记录，经进程内存储 Endpoint 执行 `(count = 0) OR CAST(name AS SIGNED)`。从请求中解码并断言 TableScan、Projection、表达式树和显式输出偏移 `[0]`。两版的 region、锁、外层请求错误和 `SelectResponse.error` 均为空。

| SDK | 请求标志 | 逐行输出 | 告警条数 | 告警码与文本 |
|---|---:|---|---:|---|
| af3dcf96 | 2 | `[1,1,NULL]` | 1 | `1292 evaluation failed: Truncated incorrect INTEGER value: 'invalid-int'` |
| af3dcf96 | 4098 | `[1,1,NULL]` | 0 | 无 |
| df5dc56 | 2 | `[1,1,NULL]` | 1 | `1292 evaluation failed: Truncated incorrect INTEGER value: 'invalid-int'` |
| df5dc56 | 4098 | `[1,1,NULL]` | 0 | 无 |

标志 2 将截断转为告警，4098 同时开启短路。两版均通过原有 `test_short_circuit_selection` 和 `test_short_circuit_selection_null_semantics`，以及新增投影用例。固定交付的独立 SDK 再次通过同三项。告警响应包含 `evaluation failed:` 前缀。最初夹具的 protobuf 解析入口及告警文本断言失败均已保留。

## 公开 PB 向量适配器

使用 `col0 OR (MAKETIME(col1, 0, 0) < TIME '00:00:00')`。传入相同的小时 −1 和无符号 schema，由适配器实际声明小时向量为有符号或无符号。每种声明在三个版本上实际请求左侧 `[2,0,1,2]`、右侧 `[2,1,2]`，包含重复行与 NULL。

| SDK | 适配器有符号 | 适配器无符号 | 普通存储列按无符号 schema |
|---|---|---|---|
| af3dcf96 | `[1,1,1,1]` | `[NULL,1,0,NULL]` | `[NULL,1,0,NULL]` |
| df5dc56 | `[NULL,1,0,NULL]` | `[NULL,1,0,NULL]` | `[NULL,1,0,NULL]` |
| 872241c7 | `[1,1,1,1]` | `[NULL,1,0,NULL]` | `[NULL,1,0,NULL]` |

单循环版选行时用 schema 替换了适配器声明，导致 MakeTime 按无符号解释负小时。普通向量节点现在借用或拥有完整 `FieldType`。默认选行继续生成按请求行排序的向量。暂借的自定义声明会复制到节点，已有的自有声明移交，普通存储列仍借用同一 schema 字段。直接断言选行结果为 Int 向量、逻辑行 `[0,1,2]`、值 `[-1,-1,-1]`，且保留整个声明，包括有符号声明的长度 19。既有标量 1、0、NULL 广播回归继续通过。SDK 公开构造器 `RpnStackNode::Vector` 的字段类型改用 `RpnVectorFieldType`，借用声明可用 `field_type.into()`，读取可用节点的 `field_type()`。TiDB 构造点同步迁移。

## 固定验证与增删

独立 SDK 表达式 443 项、执行器 122 项、聚合 41 项、存储 Endpoint 3 项通过。TiDB 共享表达式 60 项、共享 SQL 集成 7 项、默认 PREPARE 1 项通过。SDK 三包限定 Clippy、TiDB 根 lint 及 SDK 修改文件格式对照通过。完整公告检查沿用此前记录。

`evidence/pb-endpoint/` 保存 7 次双仓 Git HEAD／二进制补丁、命令和日志。清单 31 项，SHA256 为 `71fb5a29609eeaf490a6aecc0f47404c2c3aa728068baa6f9b34cc33ac031217`。`evidence/pb-vector/` 保存 13 次双仓快照及命令和日志，含两版相同向量夹具与修正验证。清单 60 项，SHA256 为 `66839d8df096e44c103d26a3748318795293e8e170e52a9643163d12fb042a77`。全部 20 对源码树可重建，十次最终检查对应固定 SDK 和 TiDB 调用方。快照覆盖 Git 跟踪文件，构建依赖缓存另计。

| 对照 | 生产 Rust 新增／删除 | 测试 Rust 新增／删除 | 构建 | 材料 | 总增删 |
|---|---:|---:|---:|---:|---:|
| 相对 task 49 修正版 | 61／21 | 186／3 | 0／0 | 45／0 | 316 |
| 相对两仓原始端点 | 4,856／664 | 7,296／93 | 1,990／103 | 20,629／0 | 35,631 |

相对两仓原始端点，源码与测试累计增删 12,909 行。本批 SDK 的 Endpoint 注解与集成测试独立提交，新增的兼容载体和回归计入向量修正。

本批 SDK 增删 269 行，其中 Endpoint 类型注解 1／1、集成测试 78／0，向量修正生产代码 59／19、测试 108／3。TiDB 调用点增删 1／1，其余为固定报告。
