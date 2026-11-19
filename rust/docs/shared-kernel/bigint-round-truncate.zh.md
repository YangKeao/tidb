# BIGINT ROUND 和 TRUNCATE

TiDB 源码和测试固定在 f73745055dfad45e3195d72de1ccf68a093444d5，SDK 固定在 6be56aea737939da0025129160ec799bfb03a82b。

单参数 ROUND 和双参数 TRUNCATE 使用 SDK 已有整数签名。TRUNCATE 根据值和精度各自的有符号声明选择签名，校验了无符号最大值、负精度、最小精度、NULL 和字段。双参数 ROUND 的原生浮点舍入路径仍保留，SQL 对照固定了接近 BIGINT 最大值时的区别。

80 项共享表达式、33 项 SQL 和限定 Clippy 通过。未测性能。
