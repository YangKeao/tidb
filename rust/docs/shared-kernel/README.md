# 表达式复用验证索引

交付分支在这里保留索引。历史的 82 份报告、运行收据、复现实验脚本和数据使用不可变的 Git 提交访问，包含检查点 1–7、独立 SDK、Decimal、执行生命周期、DATE、REGEXP_LIKE 及两个 SQL 入口。固定目录中的脚本可按对应报告在该提交运行。 [完整历史目录](https://github.com/YangKeao/tidb/tree/63a39255c4c143d7714ca46a2e65f603dca39de1/rust/docs/shared-kernel)

| 验证 | 固定证据 |
|---|---|
| 范围和剩余成本 | [245 族清单和成本报告](https://github.com/YangKeao/tidb/blob/63a39255c4c143d7714ca46a2e65f603dca39de1/rust/docs/shared-kernel/cost-report.zh.md) |
| Decimal 直接输出和载体 | [直接输出](https://github.com/YangKeao/tidb/blob/63a39255c4c143d7714ca46a2e65f603dca39de1/rust/docs/shared-kernel/compact-output.zh.md) · [载体测量](https://github.com/YangKeao/tidb/blob/63a39255c4c143d7714ca46a2e65f603dca39de1/rust/docs/shared-kernel/carrier-layout.zh.md) |
| 原生执行和状态边界 | [生命周期](https://github.com/YangKeao/tidb/blob/63a39255c4c143d7714ca46a2e65f603dca39de1/rust/docs/shared-kernel/execution-lifecycle.zh.md) · [DATE](https://github.com/YangKeao/tidb/blob/63a39255c4c143d7714ca46a2e65f603dca39de1/rust/docs/shared-kernel/typed-date.zh.md) · [REGEXP_LIKE](https://github.com/YangKeao/tidb/blob/63a39255c4c143d7714ca46a2e65f603dca39de1/rust/docs/shared-kernel/regexp-state.zh.md) |
| PB AND／OR 和存储 Endpoint | [指令与复审](https://github.com/YangKeao/tidb/blob/63a39255c4c143d7714ca46a2e65f603dca39de1/rust/docs/shared-kernel/pb-logical.zh.md) · [存储与向量声明](https://github.com/YangKeao/tidb/blob/63a39255c4c143d7714ca46a2e65f603dca39de1/rust/docs/shared-kernel/pb-endpoint-vector.zh.md) |
| 本地串行投影和筛选 | [投影](https://github.com/YangKeao/tidb/blob/63a39255c4c143d7714ca46a2e65f603dca39de1/rust/docs/shared-kernel/sql-projection.zh.md) · [逻辑](https://github.com/YangKeao/tidb/blob/63a39255c4c143d7714ca46a2e65f603dca39de1/rust/docs/shared-kernel/native-logical.zh.md) · [比较和 Selection](https://github.com/YangKeao/tidb/blob/63a39255c4c143d7714ca46a2e65f603dca39de1/rust/docs/shared-kernel/native-selection.zh.md) |
| Unistore 真实请求 | [比较入口和兼容性](https://github.com/YangKeao/tidb/blob/63a39255c4c143d7714ca46a2e65f603dca39de1/rust/docs/shared-kernel/unistore-comparison.zh.md) |

通过 Git 可查看历史产物，例如 `git show 63a39255c4c143d7714ca46a2e65f603dca39de1:rust/docs/shared-kernel/costs/family-costs.tsv`。复现脚本所需的命令和源码端点在各固定报告中。 [当前实施计划](../expression-shared-kernel-execplan.md)

当前验证补充 [BIGINT 比较的四种符号组合](mixed-bigint.zh.md)。
另见 [条件表达式的分支与 NULL 需求](native-conditionals.zh.md)。
性能对照见 [直接 BIGINT 比较的批执行](bigint-batch.zh.md)。
另见 [DOUBLE 和 DECIMAL 条件结果](numeric-conditionals.zh.md)。
- [混合列投影与 TopN](mixed-projection.zh.md)
- [BIGINT 的 NOT、NULL 与真假判断](bigint-truth.zh.md)
- [字节长度、ASCII 与二进制 CHAR_LENGTH](byte-counts.zh.md)
- [聚合参数和分组键的共享求值](aggregate-projection.zh.md)
- [有符号 BIGINT 减法](bigint-subtraction.zh.md)
- [字符串条件值的字节与排序规则](string-conditionals.zh.md)
- [原生二进制 JSON 的 JSON_TYPE](typed-json-type.zh.md)
- [IF 的 MD5 和 JSON_TYPE 共享分支](mixed-string-if.zh.md)
- [混合 IF 的 release 求值成本](mixed-if-cost.zh.md)
- [混合 IF 的批量分支](mixed-if-batch.zh.md)
- [固定函数族与调用入口的迁移差距](migration-gap.zh.md)
- [BIGINT 位运算与无符号输出](bigint-bits.zh.md)
- [DOUBLE 比较与原生 NaN 语义](double-comparison.zh.md)
- [DECIMAL 比较与精确排序](decimal-comparison.zh.md)
- [二进制字符串比较](binary-comparison.zh.md)
- [SHA、SHA1 和固定长度的 SHA2](sha-kernels.zh.md)
- [HEX 和 UNHEX 的类型化执行](hex-kernels.zh.md)
- [二进制字符串剪裁与反转](binary-string-operations.zh.md)
- [剩余函数族的 SDK PB 注册入口](sdk-registry-inventory.zh.md)
- [类型化 IP 地址转换](ip-conversions.zh.md)
- [IPv4 二进制前缀判断](ip-prefix.zh.md)
- [文本 IP 函数的共享求值](ip-text.zh.md)
- [BIN 和 OCT 的 BIGINT 格式化](integer-radix.zh.md)
- [BIGINT ABS](bigint-abs.zh.md)
- [BIGINT CEIL、CEILING 和 FLOOR](bigint-integral-rounding.zh.md)
- [BIGINT ROUND 和 TRUNCATE](bigint-round-truncate.zh.md)
- [BIGINT MOD](bigint-mod.zh.md)
- [BIGINT MOD 动态除数](bigint-mod-dynamic.zh.md)
- [BIGINT DIV](bigint-div.zh.md)
- [原生向量列 VEC_DIMS](native-vector-dims.zh.md)
- [原生向量列 VEC_L2_NORM](native-vector-norm.zh.md)
- [原生向量距离](native-vector-distances.zh.md)
- [原生向量列 VEC_AS_TEXT](native-vector-text.zh.md)
- [VEC_FROM_TEXT 与共享向量输出](native-vector-parse.zh.md)
- [向量解析和计算的组合](native-vector-compose.zh.md)
- [类型化向量比较](native-vector-compare.zh.md)
- [类型化字符串 CAST AS VECTOR](native-vector-cast.zh.md)
- [类型化向量的按需分支](native-vector-conditional.zh.md)
- [预处理 SELECT 的共享投影](prepared-decimal-projection.zh.md)
- [类型化向量、JSON 和字符串的空值判断](typed-null-tests.zh.md)
- [类型化向量加、减和乘](native-vector-arithmetic.zh.md)
- [Unistore 的 BIGINT 比较符号组合](unistore-integer-signs.zh.md)
- [CRC32 与 UNCOMPRESSED_LENGTH](byte-checksum-length.zh.md)
