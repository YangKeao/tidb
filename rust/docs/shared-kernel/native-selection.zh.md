# 本地 Selection 复用有符号 BIGINT 比较

Task #52 的固定产品为 TiDB `c3166d9d0cf607a91e8448d7bd29f8ed68ecb967` 和 SDK `929eb3a4facaffb03afdfbfd5b37f889ea3e7a52`。延续同一套共享 RPN 指令和执行上下文。 [SDK 固定源码](https://github.com/YangKeao/tikv/commit/929eb3a4facaffb03afdfbfd5b37f889ea3e7a52)

## 编译和入口

有符号、非数组 BIGINT 的 EQ、NE、LT、LE、GT、GE、NullEQ 直接选 TiKV PB 使用的 `BasicComparer<i64, …>`。按原有次序求左、右操作数，使用 SDK 已有的 NULL 规则。原有原生结果声明在输出边界保留。字面量只接纳匹配的整数与有类型的 NULL。

本地 `PhysicalSelection` 的现有批路径可通过语句级 `with_shared_selection` 显式选用候选。整组谓词在读取输入前编译，验证结果类型、参与列声明和子算子 schema。默认、读端内、关闭向量表达式、非向量化组和不支持的签名仍走原实现。合格组在 Open／Close 重置执行状态，跨 Next 复用冻结程序。声明校验失败在执行前整组退回。两种本地路径共用原有的物理存活行缩减、IN 改写等式的 NULL 处理及输入行掩码交集。SDK 公开既有借用执行视图的一根求值入口，未新增比较或需求算法。PB 的请求标志沿用原设置。

## 实际运行

- 七种比较用同一组 i64 极值、负数、NULL 和乱序重复位置对照原生。普通比较含 NULL 时返回 NULL，NullEQ 在两侧均为 NULL 时为 1。旧共享编译器拒绝该组。
- 实际 SQL 查询 `SELECT id FROM (SELECT id,a,b,u FROM filter_samples ORDER BY id LIMIT 6) AS retained WHERE (a >= b OR a <=> b) AND b > BENCHMARK(2,RAND()) ORDER BY id`。保留在子查询 LIMIT 上方的本地 Selection。显式共享选中一次、处理三批，输出 ID `[4,6]`，字段、告警和下一次会话 RAND 与默认一致。移除共享入口后，开关下计数为 `(0,0)`，选中断言失败。将后一个谓词改为 unsigned 列仍输出原生的 `[3,4,6]` 且整组 `(0,0)`。
- 物理输入五行，逻辑位置 `[3,3,0]`。等式改写的 IN 标志关闭／开启时，原生与共享都给出 `[false,false,false,true,false]`；后续谓词的 RAND 调用分别为六次和八次，证明普通 NULL 与 IN 改写 NULL 的保留条件及物理选行时机一致。

## 验证和增删

独立 SDK 表达式 443 项、执行器 122 项、聚合 41 项、进程内存储 Endpoint 3 项通过。TiDB 共享表达式 64 项、SQL 集成 9 项、默认 PREPARE 1 项和定向旧掩码 1 项通过，四项历史忽略保留。SDK 三包限定 Clippy、TiDB 根 lint 与两仓修改文件格式对照通过。本批未测性能，完整 SDK 公告检查沿用已有记录。

`evidence/native-selection/` 有 19 次 Git HEAD、双仓二进制补丁、完整命令及运行记录。12 次最终检查对应固定产品。保留原型构建失败、初次 SQL 夹具批宽断言与移除共享入口的对照失败。清单 84 项，SHA256 为 `ccbb8ef84a0acfc8405afce8372f95cf92639160f16c200b4b2bb4a23b9ae58b`。快照覆盖跟踪源码，依赖构建缓存另计。

| 对照 | 生产 Rust 新增／删除 | 测试 Rust 新增／删除 | 构建 | 材料 | 总增删 |
|---|---:|---:|---:|---:|---:|
| 相对 task 51 | 262／53 | 189／0 | 0／0 | 28／0 | 532 |
| 相对两仓原始端点 | 5,137／673 | 7,808／98 | 1,990／103 | 20,689／0 | 36,498 |

相对原始两仓的源码与测试累计增删 13,716 行。两种入口仍为受限试验，本批度量的是值、需求和路径，并未计入全函数族覆盖。
