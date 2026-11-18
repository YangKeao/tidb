# 固定函数族与调用入口的迁移差距

task #69 核对 TiDB `0563957688c2c3a199d93b555abe7eeebc4ec26a` 与 SDK `47c439a980528603ef437ec8a682b67d5b5bfba3`。本次只增加审计说明。函数族身份直接从固定设计所引用的原始清单读取，原分母为 245。历史实验的覆盖记录仍标在原分支。

| 当前原生候选的映射状态 | 函数族 |
| --- | ---: |
| 有至少一种限定类型或形状的接纳 | 32 |
| 尚无已核对的原生接纳映射 | 208 |
| 固定清单列明的历史例外 | 5 |

32 族包括带类型约束的整数运算和判断、条件结果、字节函数、DATE、JSON、正则和 RAND。接纳仍依赖类型、声明或调用位置。BENCHMARK、SETVAR 和内部 GETVAR_STRING 在候选中有受限路径，三种名称均未计入固定分母。5 个例外为 JSON_SCHEMA_VALID、TIDB_DECODE_PLAN、TIDB_DECODE_BINARY_PLAN、TIDB_ENCODE_SQL_7ea28985436937098200ced790dfbfca68e27e92136a57285f2e9f81bee79fca、VALIDATE_PASSWORD_STRENGTH。审计还未建立逐族完整重载覆盖。

| 入口 | 固定版本观察 |
| --- | --- |
| 本地串行投影和 Selection | 可显式选用限定共享程序。默认配置仍由原求值器处理 |
| 本地排序和聚合输入 | 已证实部分实际投影中的排序、分组键和参数可共享 |
| Unistore | 受特性控制的有符号 EQInt 和 GTInt 已验证。其他 SimpleExpr 路径保留 |
| TiKV Endpoint | 原有 SDK RPN 程序，实际请求证明 LogicalAnd／Or 及请求标志 |
| TiDB AST、公共 typed row、PB 转换和折叠 | 原入口留存 |
| DML、生成列、约束、分区、JOIN、扫描过滤、窗口与会话 AST | 原有调用仍在，完整迁移待证 |

原始 TiDB `364aef2bab5cc633ecb76a775ae8f36f86a6687d` 的独立工作树与候选均运行完整表达式库测试。原始版通过 1226 项，候选通过 1299 项，两者各忽略 93 项。相同四项失败分别是 IFNULL 下推文字参数的断言、EXP 溢出、Duration FSP 和 STR_TO_DATE 零日期。候选的 73 项共享测试全部通过。19 项实际 SQL、Unistore 筛选、默认 PREPARE 和独立 SDK 的 444 项表达式、122 项执行器、41 项聚合及 3 项 Endpoint 检查也通过。

验证目录 `evidence/migration-gap/` 保存完整 245 族映射、19 个历史入口和额外 SDK Endpoint 的源文件锚点、原始与候选日志及计数。9 对跟踪源码树重建一致，其中 8 对属于固定候选。全量清单含 46 条记录，`SHA256SUMS` 的 SHA-256 为 `7ea28985436937098200ced790dfbfca68e27e92136a57285f2e9f81bee79fca`。

产品源码和测试累计增删 15,535 行，构建 2,096 行，说明材料 415 行，总增删 18,046 行。尚需核对剩余重载和原生路径、完整释放旧执行器的代价、默认行为以及全量性能。现有局部正确性和性能测量不足以完成整体验收。
