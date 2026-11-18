# 字节长度、ASCII 与二进制 CHAR_LENGTH

task #61 的产品和测试固定在 `8c039cd628a11b97d07fcaa4bb5aad1360a91d6f`。独立 SDK 固定在 `1290817a57f7c8e88d52f64a1754b0e67712cec1`，本批无 SDK 变更。

LENGTH、OCTET_LENGTH、BIT_LENGTH、ASCII 和二进制 CHAR_LENGTH／CHARACTER_LENGTH 调用 TiKV 已有内核。接纳范围是实际存储为字节的字符串列以及对应类型的字面量，返回有符号 BIGINT。列适配只在这些调用中读取原始字节。文本 CHAR_LENGTH 保留原路径和 Go 的非法 UTF-8 单字节计数行为。

实际 SQL 用 VARBINARY 中的 `FF00`、空串、NULL 核对结果、告警及字段声明，投影共享计数从 (0,0) 变为 (1,1)。第一行字节长为 2，位长为 16，ASCII 为 255。文本 `界` 的 LENGTH 与 ASCII 走共享路径。与文本 CHAR_LENGTH 同组的查询保留原路径，长度分别为 3 字节和 1 字符。原生 Chunk 测试另覆盖带重复物理行的批执行及二进制字面量的行执行。

固定版本通过 70 项共享表达式、14 项 SQL 和 444 项独立 SDK 测试。限定 Clippy、根 lint 和格式验证通过。本批未测性能。验证目录 `evidence/byte-counts/` 可重建 12 对跟踪源码树，其中 6 对固定到产品版本，完整构建依赖未封存。清单包含 55 条记录，`SHA256SUMS` 的 SHA-256 为 `344461d28462ac2f2d34bcb607e48e1f22b8d0ff758b6f5fdfc047bfd3676df2`。

| 范围 | 生产 | 测试 | 构建 | 文档 | 原始新增加删除 |
| --- | ---: | ---: | ---: | ---: | ---: |
| 本批 | 91 | 132 | 0 | 17 | 240 |
| 原始双仓累计 | 6,424 | 8,219 | 2,096 | 259 | 16,998 |

源码和测试累计增删 14,643 行。双仓分类和基线见 `counts.json` 及 `count.py`。完整入口、覆盖、规模和性能仍须验收。
