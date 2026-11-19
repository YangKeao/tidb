# 原生向量列 VEC_DIMS

TiDB 固定在 cca0a3fec768fd1381704c4e6a1fc6b3d9259791，SDK 固定在 fe000f8d71ad2d2fd00a9944eaad0be2d554054e。

原生 VECTOR 列使用 SDK 的既有解码和 VEC_DIMS 内核。实际 SQL 对照确认了三元素、空向量、NULL、单元素及字段声明，单独投影向量列仍走原入口。共享读取目前会复制向量数据，且只接入 VEC_DIMS 的列签名。

81 项共享表达式、36 项 SQL 和限定 Clippy 通过。未测性能。
