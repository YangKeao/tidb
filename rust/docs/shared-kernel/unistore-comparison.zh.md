# Unistore 存储请求复用整数比较

Task #53 的固定 TiDB 产品为 `b3ed961f52752b6ee74c7cc91c6a53a2543456d0`，SDK 仍为 `929eb3a4facaffb03afdfbfd5b37f889ea3e7a52`。 [固定 SDK](https://github.com/YangKeao/tikv/commit/929eb3a4facaffb03afdfbfd5b37f889ea3e7a52)

Unistore 编译特性 `shared-kernel` 显式使用候选。原有请求解码把真实 EQInt／GTInt 签名和扫描列声明交给共享编译器，每个比较在读行前冻结一次。接纳匹配的有符号 BIGINT 列和整数字面量，结果仍按原生声明。扫描行的两个实际 Int／NULL 值可直接放入栈上标量槽；缺少字段、签名超范围或行载体不符时，选择原路径。共享执行产生错误后直接返回。默认 Unistore 构建和 wire 请求标志沿用原设置。

Unistore 原有 PB 表达式左 NULL 时跳过右侧，而本地普通比较会求右侧。Unistore lowering 复用共享 `NullGuard` 表达来源合同。直接输入适配器观察到 EQInt 和 GTInt 左 NULL 时均请求 `[0]` 并返回 NULL，左值 3、右值 2 时均请求 `[0,1]` 并分别返回 0 和 1。去掉守卫时实测请求 `[0,1]`，触发需求断言失败。两类入口共用同一比较算法和 SDK 指令循环。

实际存储测试将 `b=-77`、`b=88`、`b=NULL` 提交到 MVCC，发送 TableScan 加 Selection 的 PB 请求。开关两版均返回唯一一行 `[2,88]`，无错误或告警，启用时实际进入共享求值三次。公开 `SimpleExpr` 的 Send／Sync 保持。SDK 程序的类型擦除 metadata 只满足 Send，因此借用请求内可克隆的互斥持有者；初版未满足的线程类型断言已保存，修正后通过。本批未测锁或执行耗时。

## 验证与增删

Unistore 开关两版完整库测试各 169 项通过、13 项原有忽略、1 项失败。失败是基线已有的 `closure_executor_selection_over_point_range_answers_zero_rows`，将字符串列 `'abc'` 放入 EQInt 后报告 Decimal 截断。已在未修改的 #52 固定版本单独重现。仅滤除该已知失败时，两版各 169 项通过、13 项忽略。该混合类型请求未进入本批共享接纳。独立 SDK 表达式 443 项、进程内 Endpoint 3 项，TiDB 共享表达式 65 项、SQL 集成 9 项及默认 PREPARE 1 项通过。TiDB 表达式及 Unistore 限定 Clippy、根 lint 和修改处格式对照通过。

`evidence/unistore-comparison/` 保留 28 对跟踪源码快照。最终 12 项对应固定源码，含两次上述完整套件失败。清单 119 项，SHA256 为 `81a2618040ad8e4135e649701e3027bb3b2ec534b4cb56680aaa7d455865a72b`。依赖缓存另计。

| 对照 | 生产 Rust 新增／删除 | 测试 Rust 新增／删除 | 构建 | 材料 | 总增删 |
|---|---:|---:|---:|---:|---:|
| 相对 task 52 | 168／29 | 114／1 | 3／0 | 22／0 | 337 |
| 相对两仓原始端点 | 5,276／673 | 7,922／99 | 1,993／103 | 20,711／0 | 36,777 |

相对原始两仓的源码与测试累计增删 13,970 行。
