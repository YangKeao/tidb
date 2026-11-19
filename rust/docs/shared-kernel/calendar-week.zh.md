# 类型化日期的周号与模式

TiDB 源码固定在 `46daa7a18c33f9d409790eccb1d8b8ac500dfe2e`，SDK 固定在 `3dacb96ffe4e534303024d3ab073c0a9ee2a8f3f`。

WEEK、WEEKOFYEAR 和 YEARWEEK 共享 CoreTime 的周号计算。单参数 WEEK 从当前语句读取会话默认模式，单参数 YEARWEEK 使用零，WEEKOFYEAR 使用三。显式模式读取 BIGINT 的低三位。原生显式 NULL 取零，PB 保留原有 NULL 和零日期诊断。

真实 SQL 对照覆盖跨年周号、会话模式 0、3、11、负模式、NULL 模式列及 NULL 字面量、无效日期与 NULL 日期。改动前原生结果一致，共享程序计数为零。87 项共享表达式、80 项执行器集成、独立 SDK 的 448 项表达式、122 项执行器和 3 项 Endpoint 测试通过，限定 Clippy 和根 lint 通过。性能未测。
