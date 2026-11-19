# 类型化日期的 TO_DAYS 和 TO_SECONDS

TiDB 源码固定在 `e56d54e4b71e10513c67cdbc26cd83b553619b3e`，SDK 固定在 `9c63f84e4040841abe8f3f43055334156c0b630e`。

TO_DAYS 和 TO_SECONDS 从共享程序的原生 Time 载体读值，两侧复用日数与整秒计算。原生入口拒绝无效月日和 DATETIME 的非法钟面，DATE 的隐藏钟面按原语义忽略。PB 签名保留自己的零日期诊断和钟面读取。

SQL 对照覆盖零年起点、现代日期、秒以下截断、无效零日期和 NULL。改动前原始结果一致，共享程序计数为零。直接 Chunk 的两种输出均验证了带隐藏钟面的 DATE 及同样字段的无效 DATETIME。初次独立 datatype 共享尝试因该包没有 TiDB datatype 依赖而编译失败，最终复用位于已有依赖的表达式包内。

86 项共享表达式、76 项执行器集成、独立 SDK 的 448 项表达式、122 项执行器和 3 项 Endpoint 测试通过，限定 Clippy 和根 lint 通过。性能未测。
