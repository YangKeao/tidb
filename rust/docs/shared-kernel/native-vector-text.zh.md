# 原生向量列 VEC_AS_TEXT

TiDB 固定在 8f0cb37c000e99353ad01c87a0da686775ddc330，SDK 固定在 1e3cea745b4a173b8ef7116e822078e7032f964b。

已定型的向量列通过 SDK VEC_AS_TEXT 格式化。实际 SQL 对照覆盖负零、空向量、NULL、float32 极小及极大值的定点文本，以及字段声明。81 项共享表达式、39 项 SQL 与限定 Clippy 通过。未测性能。
