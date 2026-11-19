# 类型化 ORD 与 QUOTE

TiDB 固定在 `dd22b9fb855eba1d847054ec69d81840f99b0474`，SDK 固定在 `69f25eeff6f0e70b48ec75b994a9ccf1e9cdee3d`。

二进制 ORD 复用首字节内核，utf8mb4 ORD 复用 SDK 首字符算法。原生输入为 NULL 时仍返回 NULL。原生 QUOTE 先按既有规则替换非法 UTF-8，再调用 SDK 原转义内核。对二进制列，独立求值保留非 NULL 字节结果与 SQL NULL 转成文本 `NULL` 的区别。PB 的 ORD NULL 返回 0、QUOTE 对非法 UTF-8 原字节转义的行为仍经测试覆盖。

真实 SQL 核对了多字节首字符、空值、二进制原字节和 NUL、反斜线、引号、Ctrl-Z 转义。83 项共享表达式、65 项执行器集成、独立 SDK 的 446 项表达式、122 项执行器和 3 项 Endpoint 测试通过，限定 Clippy 和根 lint 通过。性能未测。
