# 串行 SQL 共享投影接线与 REGEXP_LIKE 接纳修正

Task #48 的固定实现为 [TiDB 80ab4b70b0](https://github.com/YangKeao/tidb/commit/80ab4b70b03dd08111fa8157ee612aaf6c9afded) 与 [SDK af3dcf96](https://github.com/YangKeao/tikv/commit/af3dcf96ad94a389e4d00fb44fdc799bb2714c3d)。范围来自 #tidb-expression-reuse:1dc6ca2b 的 516aeb64 和 2754085e。 [实施设计与 task 47 复审](https://github.com/YangKeao/tidb/blob/fd0de8914a6fbf9384875e473c53a97b5406c9e3/rust/docs/expression-shared-kernel-cost-review.zh.md#L657)

## 接纳与执行边界

- 在类型化表达式中，默认字符串的 `A` 与 `^a$` 遇到 `utf8mb4_general_ci` 有效结果规则，修复前原生、共享分别返回 1、0。字面和列模式均复现。共享编译器现在按原生的有效规则排除大小写不敏感的 REGEXP_LIKE。整根及含该节点的投影被拒绝。现有接纳范围内，字面和动态模式均保留 `ci` 返回 1、`ic` 返回 0。
- 编译 `tidb-executor/shared-kernel` 后，调用者可通过当前 `StmtContext::with_shared_projection()` 显式开启。真实 `PhysicalPlan::Projection` 先完成 reader 下推判断、解析列和决定 worker。只选择 SELECT 的零 worker、本地、非空、全为计算根且两层批上限在 1–1024 的投影。整组编译并核对原生结果类型后选择共享执行器；其余继续原路径。这个开关随语句上下文传递。
- 共享路径拥有冻结程序和同一真实语句上下文。两类入口都短借用已绑定的每根状态，调用同一个 SDK 组求值及原帧循环。Open 重建有种子随机流和正则准备状态，非空 Next 沿用，Close 释放。输入保留 Chunk 的物理选择位置、次序及重复值，无选择时使用逻辑行的恒等映射。输出复用既有直接写出实现。
- 共享求值后遇到失败直接返回。表达式错误保留 MySQL 码、SQLSTATE、文本和来源标记，适配不变量用原内部错误类别。SQL 对照发现原 SDK 整数错误显示操作数值而原生显示源列。共享原有整数函数仍完成计算，附带的源诊断只替换其 1690 文本。整数源表达式仅作静态检查，若隐藏列展开含参数或延迟常量，编译拒绝整组，留给原路径用本次语句诊断。普通列、静态嵌套算术均可进入共享。Decimal 参数接纳和诊断定向集继续通过。

## 实际 SQL 与证据

| 验收 | 结果 |
|---|---|
| 本地表、真实 SELECT | `SELECT RAND(7), RAND(), n + 1, v / 0e0 FROM samples ORDER BY n LIMIT 5`。实际计划为物理投影，每次执行观察到 1 个共享程序，分别处理 2、3 行两个非空批。结果类型、普通值与默认入口一致。 |
| 重开与语句效果 | 两次执行各从相同的种子序列开始，会话无种子随机流连续。每次均向真实语句上下文写入 5 条 1365 告警，与原生一致。另以同一已编译程序、原 Chunk 选择 `[2, 0, 2]` 验证输出 `[31, 11, 31]`，重建状态后按相同种子开始。 |
| 出错后返回 | `SELECT RAND(), n + 1 FROM limits_case LIMIT 1` 和将加法换为 `n * 2`、`(n + 0) + 1`。每条语句观察到 1 个共享程序、1 个非空批次。与原生一致地返回 1690／22003、源列表达式和求值来源。会话 RNG 恰好前进一次，语句告警列表为空。 |
| 原生选路 | 同个 feature 构建中，默认未开启走原生。显式开启但有 worker、混合直接列、含不支持的 ABS 或批上限为 2048 时，共享程序及批次计数均为零。 |
| 隐藏列诊断 | 带参数的普通入口显示当前绑定 17，共享修复前退化为通用文本；延迟常量修复前被提前渲染。两条现均整组拒绝。隐藏列的静态嵌套表达式仍生成原生诊断文本。 |
| SDK 生命周期 | 独立检出的已拥有程序经过两轮状态重建，每轮执行两次，每轮只准备一次字面正则。 |

修复前后见证和最终命令的原始日志、HEAD、二进制 Git 补丁和命令收据在 home-gentoo 此 agent 的 `evidence/local-sql/`。其中 `regexp-before.log` 为两个模式的 1／0 见证，`sql-second.log` 为值形式溢出诊断反例，`hidden-before.log` 为隐藏参数及延迟常量的反例，`deliver-sql.log` 含实际物理计划和每批路由。保留了首次不同编译环境中断、借用期声明、测试调用私有解析方法、默认聚合测试缺少 feature 门控、复用默认测试语句上下文造成的原生 RAND 状态遗留及首次格式检查目录错误。修复均纳入相应后续记录。十次成功检查的跟踪源码树均与上述固定提交一致，三条修复前见证也已重建。原始证据清单 SHA256 为 43e6be563382163d04c30d0159a2cf8c09b03d6dcac152b84815664e1de5aaac。构建使用已配置的固定工具链和既有依赖缓存，证据针对所见运行和跟踪源码。

## 验证命令

在 TiDB `rust/` 运行：

```sh
cargo test -p tidb-executor --features shared-kernel --test shared_execution --locked --offline -- --nocapture
cargo test -p tidb-expr --features shared-kernel --lib shared_kernel --locked --offline -- --nocapture
cargo test -p tidb-expr --features shared-kernel --lib regexp --locked --offline
cargo test -p tidb-session --lib prepared_rand_restarts_seeded_and_preserves_session_stream --locked --offline
cargo test -p tidb-executor --test all projection --locked --offline
cargo clippy -p tidb-expr -p tidb-executor --features shared-kernel --lib --locked --offline
```

在 SDK 独立检出运行：

```sh
cargo test -p tidb_query_expr --lib --locked --offline -- --nocapture
cargo clippy -p tidb_query_expr --lib --locked --offline
```

共享集成 4 项、共享表达式 59 项、正则筛选 26 项通过，另有原有 1 项忽略。默认 PREPARE 随机流回归 1 项，默认 executor 集成 5 项通过。独立 SDK 表达式 437 项全部通过。限定 Clippy、TiDB 根 `make lint` 和修改处格式检查通过，保留诊断告警。本批实测限于此 opt-in；完整 SDK 公告检查沿用 task 43 的失败记录，性能与完整迁移的规模目标待验证。

## 两仓原始增删

| 对照 | 生产 Rust | 测试与测量 Rust | 构建 | 材料 | 总增删 |
|---|---:|---:|---:|---:|---:|
| 相对 task 47 | 591／167 | 419／11 | 0／0 | 54／0 | 1,242 |
| 相对原始两仓 | 4,463／330 | 6,282／4 | 1,984／100 | 20,493／3 | 33,659 |

统计使用两仓固定端点 Git diff 的新增和删除之和。测试模块内的修改计入测试，本报告计入材料。相对原始端点的源码及测试增删 11,079 行。当前仍为局部实现，后续迁移和退役成本另计。
