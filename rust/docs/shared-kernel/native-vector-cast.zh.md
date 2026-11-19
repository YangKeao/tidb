# 类型化字符串 CAST AS VECTOR

TiDB 固定在 ccaef895630c300f3d01de90d51814f62d22444b，SDK 固定在 18de0110052f46151e94331c6023d56a91dccad9。

共享程序调用原有 datatype 转换，将固定维度传给原有维度检查。实际 SQL 包含 VARBINARY 和 VARCHAR、固定及可变维度、直接向量结果和嵌套的 VEC_DIMS 与 VEC_L2_NORM。结果字段、float32 字节和 NULL 与原入口一致。维度不符、非法向量文本和非法 UTF-8 的 1105 编号与文本逐一对齐。

81 项共享表达式、43 项 SQL、569 项独立 SDK 测试、限定 Clippy 和根 lint 通过。首次编译缺少 protobuf FieldType 的限定名，已修复并保留原始失败记录。共享转换会复制输入字节和向量结果，性能未测。
