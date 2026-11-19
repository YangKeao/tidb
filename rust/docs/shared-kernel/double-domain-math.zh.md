# DOUBLE 的 ASIN、ACOS、SQRT 和 SIGN

TiDB 固定在 `40fc900409ecb6adde0813858a84d7afa7deb776`，SDK 固定在 `c57c0711aef2a15c94d12b69bac3554b1d59a04b`。

类型化 DOUBLE 的 ASIN、ACOS、SQRT、SIGN 共用 SDK 值计算。原生入口保留完整的 f64 值域，PB 返回继续经其原浮点类型转换。SQL 用例核对合法范围、域外 NULL、有符号结果及 NULL 输入。直接 Chunk 用例对比了有符号零、正负无穷和两种 NaN 位模式，也检查了重排与重复行号。

85 项共享表达式、72 项执行器集成、独立 SDK 的 448 项表达式、122 项执行器和 3 项 Endpoint 测试通过。限定 Clippy 和根 lint 通过。性能未测。
