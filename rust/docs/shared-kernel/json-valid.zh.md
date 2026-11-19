# 类型化 JSON_VALID

TiDB 固定在 1aadd109e3d949fb1b300f6d495787e9c22a5844，SDK 固定在 907391a80a7bc7aff67bf0d0396c01c2721d477f。

二进制 JSON 与有符号和无符号 BIGINT 复用 SDK 原有校验。字符串读取原始字节，用原生相同的 JSON 文本解析规则。实际 SQL 投影和 Selection 对齐了 SQL NULL、JSON null、非法 UTF-8、尾随内容、孤立代理转义和布尔结果字段。独立 SDK 继续验证 PB 字符串的非法 UTF-8 错误。

81 项共享表达式、52 项 SQL、569 项独立 SDK 测试、限定 Clippy 与根 lint 通过。性能未测。
