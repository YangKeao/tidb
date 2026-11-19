# BIGINT 的 PERIOD_ADD 和 PERIOD_DIFF

TiDB 源码固定在 `209d40a2a3a26338a44d3759ece06cc80e2f31b5`，SDK 固定在 `bb32cf0fa5bbf86b095a9b9b0ebf336ff488c201`。

类型化 BIGINT 的周期运算使用现有 RPN 与 SDK 月数转换。原生入口先处理 NULL，再报无效月份或负周期的 1210。原生月份加法及周期换算使用明确的环绕运算，PB 保留其 32 位月份和原有零周期行为。

真实 SQL 对照覆盖两位年份分界、零月份结果、最小 BIGINT 加数、NULL 和两函数的完整 1210 文案。改动前结果一致，共享程序计数为零。86 项共享表达式、78 项执行器集成、独立 SDK 的 448 项表达式、122 项执行器、303 项 datatype 和 3 项 Endpoint 测试通过。限定 Clippy 和根 lint 通过。性能未测。
