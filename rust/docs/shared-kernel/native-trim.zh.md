# TRIM 的三个方向

TiDB 固定在 18d039555ab8fe4a3e1ff703f7e51a6095abb790，SDK 固定在 2e123f71eb9b20e6e46b32110d60f895f7d8830b。

TRIM、BOTH、LEADING 和 TRAILING 复用 SDK 已有前后缀内核。原生 BOTH 顺序剪除左右两端，因此 TRIM('aaa' FROM 'aaaaa') 返回 'aa'。独立 SDK 用例继续验证同一 PB 输入返回空串。实际 SQL 同时核对了默认空格、空模式、保留制表符、非 UTF-8 字节和 NULL。

前测结果正确但未进入共享投影，后测确认实际选路。另保留初次测试的编译失败。81 项共享表达式、55 项执行器测试、569 项独立 SDK 测试、限定 Clippy 与根 lint 通过。性能未测。
