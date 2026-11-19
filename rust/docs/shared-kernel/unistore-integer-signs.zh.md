# Unistore 的 BIGINT 比较符号组合

TiDB 固定在 6ec618a7a5324f4d42127ad9c5d6a88f52c08d73，SDK 固定在 6d1962b7d21c4ddcf952da9d5ceb76163e42535b。

原有 Unistore 请求转换现在为 EQInt 和 GTInt 的两侧传递各自的 BIGINT 符号声明，复用 SDK 的四组比较内核。实际 MVCC、编解码和 Selection 请求覆盖八组比较，包含负数、相等、无符号最大值及 NULL。开启共享功能时每组证实四行确实经过同一执行循环，关闭功能时独立得到相同结果和告警。

功能开关各通过 60 项 cophandler 测试。81 项共享表达式、47 项 SQL、限定 Clippy 和根 lint 通过。Unistore 全量在开关两侧各通过 170 项，13 项忽略，同一项旧 DECIMAL 文本截断失败已在父版本独立复现。本批未测性能。
