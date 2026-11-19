# 类型化向量的按需分支

TiDB 固定在 2cbe8a44495f28beafd05e9359c7ea68cacef14f，SDK 固定在 b92c3fb62e27bc213187661eff2b1ab78398bd63。

IF、CASE、IFNULL 和 COALESCE 的向量结果使用同一组按需指令。显式 NULL 和 CASE 未命中都得到向量类型的 NULL。实际 SQL 核对了结果字段、float32 字节、后续 VEC_DIMS、未选坏文本，以及四种函数选中坏文本时的完整 1105。

首个反例确认投影原来走回退。接入 IF 和 CASE 后，IFNULL 仍因 SDK 程序验证拒绝向量非空跳转而回退。修复让同一条非空跳转验证和识别向量 NULL，保留错类型拒绝测试和两次反例。81 项共享表达式、44 项 SQL、569 项独立 SDK 测试、限定 Clippy 与根 lint 通过。条件执行保留按行顺序，性能未测。
