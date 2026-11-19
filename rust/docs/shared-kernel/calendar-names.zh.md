# 类型化 DAYNAME 和 MONTHNAME

TiDB 源码固定在 `8c5ab0fd844d181f2680498a5a8f9c05e2634e65`，SDK 固定在 `1de5d6aec5479e5eaa5c6e36f3943e2bdbcc3998`。

DAYNAME 和 MONTHNAME 从原生 Time 载体读取日期。共享程序使用原月份和星期名称，保留外部 SQL 字段声明。原生入口对零月、零日及无效月日返回 NULL，PB 继续由已有签名处理其零日期诊断。

真实 DATE 和 DATETIME 查询逐项核对结果与输出字段，覆盖闰日、周日、时钟字段、无效日及 NULL。改动前原始结果一致，共享程序计数为零。86 项共享表达式、77 项执行器集成、独立 SDK 的 448 项表达式、122 项执行器和 3 项 Endpoint 测试通过，限定 Clippy 和根 lint 通过。性能未测。
