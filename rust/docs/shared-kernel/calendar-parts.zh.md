# 类型化日期分量

TiDB 固定在 `89a95017ebcc6db3dacd9d18e2c7c392b74d010e`，SDK 固定在 `ee8fe0dc29c89c29b543635affbaf87cb5602bd8`。

YEAR、MONTH、DAY／DAYOFMONTH、QUARTER 从既有的 Time 载体读取年、月、日和季度。TiDB 与 SDK 共用日历字段提取；原生 SQL 的已类型化输入保留零分量，字符串转换沿用原路径。TiKV PB 在 NO_ZERO_DATE 下仍对全零日期的 YEAR 和 DAYOFMONTH 返回 NULL 并告警 1292，MONTH 和 QUARTER 保留 0。

真实 SQL 的 DATE、DATETIME(6) 用例覆盖有效日期、部分零、全零与 NULL，并比较两种模式的结果、字段和告警。82 项共享表达式、64 项执行器集成、独立 SDK 的 445 项表达式、122 项求值执行器与 3 项 Endpoint 测试通过。限定 Clippy 和根 lint 通过。性能未测。
