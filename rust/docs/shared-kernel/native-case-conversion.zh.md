# 二进制与 UTF-8 的 UPPER 和 LOWER

TiDB 固定在 c10ffdc92cf56bc357944c4aeda9dccb6939cd64，SDK 固定在 8f4728f7f3ebff63951dd215ee5010b1245b1bea。

二进制 UPPER、LOWER 及别名复用 SDK 恒等字节内核。原生 UTF-8 列调用同一套 Go 简单大小写表与逐个无效字节的替代规则。SQL 覆盖 ß、İ、ﬁ、非 UTF-8、NUL 和 NULL。独立结果接口同时核对二进制 Datum 和损坏 UTF-8 的替代字符。

主仓与独立 SDK 的 Cargo 图均只有一个 tidb-hack 身份。新增直接依赖沿用现有固定源码，主仓使用对应本地代码。81 项共享表达式、54 项执行器集成测试、569 项独立 SDK 测试、限定 Clippy 和根 lint 通过。性能未测。
