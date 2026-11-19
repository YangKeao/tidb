# 二进制 SUBSTRING_INDEX

TiDB 固定在 7defb810b96a21d79e4557bebb54ba3726ae6c2b，SDK 固定在 1528c7b3127e42009815c20ef3493f0a4e7bfe41。

类型化的有符号与无符号 BIGINT 计数进入同一共享求值程序。正计数复用 SDK 原有扫描。原生负计数从左确定不重叠的分隔符，再从右选切点。SQL 与独立 SDK 的 PB 回归测试分别固定了重叠分隔符的原有结果。

实际 SQL 核对了非 UTF-8、NUL、最小有符号数、最大无符号数、空分隔符、零和 NULL。81 项共享表达式、51 项 SQL、569 项独立 SDK 测试、限定 Clippy 和根 lint 通过。性能未测。
