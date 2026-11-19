# 二进制 REPLACE 和 SUBSTRING

TiDB 固定在 abf4a15526f2b5ca4d6eca2e8a5348ef0e0362e5，SDK 固定在 b8e3879fbbd29ae1dff88554a8da25f9eba46ce5。

REPLACE 与双、三参数 SUBSTRING、SUBSTR、MID 复用原有字节内核。实际 SQL 对齐了非 UTF-8、NUL、非重叠替换、空搜索串、正负位置、类型化 NULL 和结果字段。三参数原生调用保留位置加长度溢出时的空串；独立 SDK 的 PB 用例固定了同一输入的饱和切片结果。

81 项共享表达式、50 项 SQL、569 项独立 SDK 测试、限定 Clippy 与根 lint 通过。数字参数限定为有符号 BIGINT，性能未测。
