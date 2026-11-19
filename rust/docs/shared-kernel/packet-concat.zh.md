# CONCAT 和 CONCAT_WS 的包大小与 NULL

TiDB 固定在 `7b861bb6a9e2d28542ca3eeb928298fd6574fb6d`，SDK 固定在 `3055387b555cacf100cee2dd5ecde291f50c964b`。

已类型化的字符串参数复用 SDK 的 CONCAT 和 CONCAT_WS 内核。共享程序先按原生规则核对结果大小，再写入结果。CONCAT 在先遇到超限前缀时仍发出 1301，即使后续参数为 NULL。CONCAT_WS 跳过 NULL 值，包大小仍按原参数位置计入分隔符。

实际 SQL 以 4 字节限制核对刚好达到上限、二进制字节、NULL 分隔符、NULL 参数和告警顺序。83 项共享表达式、67 项执行器集成、独立 SDK 的 446 项表达式、122 项执行器和 3 项 Endpoint 测试通过。限定 Clippy 和根 lint 通过。性能未测。
