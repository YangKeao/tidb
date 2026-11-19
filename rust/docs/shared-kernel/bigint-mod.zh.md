# 非零字面量 BIGINT MOD

TiDB 源码和测试固定在 18cb62a184cfb4e0ba8dae155aee9e837d59e208，SDK 固定在 eb7ce0dac74448ef602d5c50e53291eb5d423792。

共享已有的四种整数符号组合。SDK 的有符号余数内核对最小 BIGINT 和 -1 返回 0，避免 Rust 的溢出检查中断 SQL。实际原生 SQL 和 PB 构建器的 SDK 算术测试覆盖该边界。动态除数和零除数保留原生入口，SQL 对照验证了告警一致。

80 项共享表达式、34 项 SQL 和 569 项独立 SDK 检查通过。未测性能。
