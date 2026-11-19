# 原生向量列 VEC_L2_NORM

TiDB 固定在 c2cc65147bbfdb71d23dcd688cf9f8a36b2da797，SDK 固定在 12c563f1e6f0496b47f46c7dd67bbfc653134891。

VEC_L2_NORM 使用 SDK 的双精度累积。SQL 对照覆盖普通向量、空向量、NULL、超出 float32 范围的有限范数，以及随后参与原生 DOUBLE 加法的结果。共享求值读取 SDK 的 REAL 结果并保留原生 DOUBLE 输出声明。

81 项共享表达式、37 项 SQL 和 569 项独立 SDK 检查通过。共享输入仍复制向量数据，未测性能。
