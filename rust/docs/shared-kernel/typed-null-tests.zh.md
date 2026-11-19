# 类型化向量、JSON 和字符串的空值判断

TiDB 固定在 3d0781dc5713fe8635653e32922c09608f0b5e41，SDK 固定在 b92c3fb62e27bc213187661eff2b1ab78398bd63。

实际 SQL 投影和 Selection 通过 SDK 的向量、二进制 JSON 和原始字节空值内核运行。空向量、JSON null、空字符串及非 UTF-8 字节均为非 NULL，SQL NULL 保持为 NULL。IS NOT NULL 使用现有布尔反转。结果字段、数据和选路均核对。

81 项共享表达式、46 项 SQL、限定 Clippy 和根 lint 通过。SDK 本批无改动，性能未测。
