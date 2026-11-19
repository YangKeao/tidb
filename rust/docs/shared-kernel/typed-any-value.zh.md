# 类型化 ANY_VALUE

TiDB 固定在 `5a675da2077c7bcfaeb3e40fc463cf0d112231f0`，SDK 固定在 `2e123f71eb9b20e6e46b32110d60f895f7d8830b`。

共享投影将受支持的直接列 `ANY_VALUE` 编译为读取列值。实际 SQL 核对有符号和无符号 BIGINT、DOUBLE、DECIMAL、DATETIME、文本、VARBINARY、JSON、向量及 NULL。JSON 保留类型码和载荷，向量保持序列化位。VARBINARY 用例也验证原计划给函数结果单独指派的排序规则，原始字节仍逐字节保留。独立求值接口保留输入字符串值的排序规则。

`ONLY_FULL_GROUP_BY` 下的真实分组查询结果与原路径一致。移除共享接入后，同一投影记录零个共享程序。63 项执行器集成、82 项共享表达式、4 项原生 ANY_VALUE、限定 Clippy 和根 lint 通过。性能未测。
