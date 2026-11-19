# BIGINT CEIL、CEILING 和 FLOOR

TiDB 源码和测试固定在 ，SDK 继续使用 。

已定型的有符号和无符号 BIGINT 使用 SDK 的整数签名。实际串行 SQL 校验了最小有符号数、最大无符号数、NULL 和输出字段。DECIMAL 查询保持原有路径。首次测试的 SQL 未选用串行共享投影，补上 LIMIT 后确认共享投影实际执行。

80 项共享表达式、32 项 SQL 和限定 Clippy 通过。未测性能。
