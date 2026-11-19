# BIGINT ABS

task #82 的源码和测试固定在 TiDB `ddeceb721ecff6f1a6c6140714e767648d237ed7`，SDK 固定在 `6be56aea737939da0025129160ec799bfb03a82b`。

已定型的 BIGINT 列和字面量通过 SDK 的 ABS 内核执行。无符号结果保留完整位模式。对于有符号最小值，TiDB 复用现有表达式诊断格式，实际 SQL 校验了相同的 1690、SQLSTATE 和消息。PB 仍使用原有 ABS 入口。

80 项共享表达式、31 项 SQL、444 项独立 SDK 表达式、122 项执行器、3 项 Endpoint 通过。首次 SQL 全集发现原回退测试使用 ABS 作为未接入函数。将回退用例改用 SQRT 后重新运行完整 SQL 集。表达式和 SDK 验证使用相同的产品代码。未测性能。
