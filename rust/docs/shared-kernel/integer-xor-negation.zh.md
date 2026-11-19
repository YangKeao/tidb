# BIGINT XOR 与一元负号

TiDB 固定在 `e4d45f5a1a067eac1d530f7c5d8c07a140c8c64e`，SDK 固定在 `2e123f71eb9b20e6e46b32110d60f895f7d8830b`。

本地共享投影和筛选复用 SDK 的 BIGINT XOR。两个操作数可分别为有符号或无符号 BIGINT。左侧为 NULL 时仍先求右侧，右侧求值成功后再返回 NULL。原生布尔结果声明保留。有符号和无符号 BIGINT 列的一元负号各使用 SDK 已有取负内核，保留可表示的 −2⁶³、NULL 及原 1690 文本。超界常量仍由原生规则提升为 DECIMAL。

62 项执行器集成、81 项共享表达式、限定 Clippy 和根 lint 通过。原生运算筛选通过 27 项、忽略 6 项，有 1 项 duration FSP 崩溃在固定父提交同样复现。性能未测。
