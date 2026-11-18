# DOUBLE 和 DECIMAL 的共享条件结果

Task #58 的验证固定在 TiDB `21352acd1a16ce89f0331b973c945fa70c9c89c3` 和 SDK `1290817a57f7c8e88d52f64a1754b0e67712cec1`。 [SDK 固定源码](https://github.com/YangKeao/tikv/commit/1290817a57f7c8e88d52f64a1754b0e67712cec1)

CASE、IFNULL 和 COALESCE 接纳类型和实际载体吻合的非数组 DOUBLE、DECIMAL 结果，沿用有符号 BIGINT 的 CASE 条件。CASE 与 IF 共用分支，无 ELSE 时产生有声明的 NULL。IFNULL 和 COALESCE 延用同一非 NULL 跳转和现有值载体，覆盖完整 f64 和 Decimal。字段声明在原生输出边界保留，仍需转换的参数组在执行前回退。PB、Unistore 和默认 SQL 选路沿用。

乱序重复输入 `[2,0,2,1]` 对照原生。选中的 NaN 保存 payload `0x7ff8000000000042`，负零符号位保留，两个 Decimal 输入分别保留 7 位结果尺度与 9 位存储尺度、以及 82 位声明形状。断言完整系数、符号、结果／存储尺度及声明形状。无 ELSE 和所有参数为 NULL 时返回 NULL，且未选中的整型溢出分支跳过。

真实表上的计算投影含 DOUBLE CASE、DOUBLE IFNULL 和 DECIMAL COALESCE。三行值、完整字段声明与告警与原生一致。显式共享从旧版计数 `(0,0)` 变为 `(1,1)`。旧版原生接口也实际复现拒绝新的结果类型。

TiDB 共享表达式 68、SQL 集成 11、默认 PREPARE 1、Unistore 请求 1 项通过。独立 SDK 表达式 444、执行器 122、聚合 41、存储 Endpoint 3 项通过。限定 SDK Clippy、根 lint 和两仓格式通过。本批未作性能测量，历史完整公告和 Unistore 的基线失败沿用。

`evidence/numeric-conditionals/` 保留 16 对双仓跟踪源码树，12 项最终检查对上固定产品。清单 72 项，SHA256 为 `28a207b7bc1701fb8aa919eab15a4472d98fbc93f5eb3eb2ff534125762aeb62`。

| 对照 | 生产 Rust 新增／删除 | 测试 Rust 新增／删除 | 构建 | 材料 | 总增删 |
|---|---:|---:|---:|---:|---:|
| 相对 task 57 | 77／22 | 131／0 | 0／0 | 21／0 | 251 |
| 相对两仓原始端点 | 5,517／681 | 7,764／99 | 1,993／103 | 206／0 | 16,363 |

相对原始两仓的源码与测试累计增删 14,061 行。
