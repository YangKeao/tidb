# 本地 BIGINT 条件表达式复用按需求值

Task #56 固定在 TiDB `4752f52f33079beaf03fb9b0647127019817ee84` 和 SDK `dcec7b0394e1b2b6d6e5d4a8f34bd84d64fa4b69`。 [SDK 固定实现](https://github.com/YangKeao/tikv/commit/dcec7b0394e1b2b6d6e5d4a8f34bd84d64fa4b69)

本地签名匹配的有符号、非数组 BIGINT CASE、IFNULL 和 COALESCE 进入同一 RPN。CASE 和原有 IF 共用分支降级，短路后的 CASE 保留所选值，包括 NULL。无 ELSE 时产生有类型的 NULL。IFNULL 和 COALESCE 共用 `JumpIfNotNull`，保留首个非 NULL 值并跳至结束。NULL 值从当前操作数栈移除后继续。两类请求保留逐行标量合同和原生结果声明。目标重定位、冻结栈与跨操作数约束均参与验证。未接纳类型在执行前整组回退。PB、Unistore 及默认 SQL 的选路沿用原方式。

原生 Chunk 乱序重复位置 `[2,0,1,2]` 上，COALESCE 与 CASE 分别得到 `[0,-1,0,0]` 和 `[0,7,0,0]`，两批执行的活跃 BENCHMARK 分别推进随机流四次和六次，均与原生一致。纯 NULL COALESCE 返回 NULL，选中 NULL 的 CASE 跳过后续溢出条件。IFNULL 的非 NULL 值跳过右侧溢出，左 NULL 时返回与原生相同的 1690。SDK 冻结测试覆盖无效目标、两条分支的类型合流和非法父栈读取。

真实 SQL 保留 `CASE WHEN x THEN y WHEN y THEN id END`、`IFNULL(x,id)`、`COALESCE(x,y,id)` 和 `RAND(7)` 四个计算。原生与共享的五行输出、字段及随机数序列一致。显式开启后由基线 `(0,0)` 变为选中一次、两批。初次用一次性结果接口实际只产生一批，随后使用实际游标请求验证跨批。带 DOUBLE 转换的组保持 `(0,0)`。

## 验证和增删

TiDB 共享表达式 67、SQL 集成 10、默认 PREPARE 1 和启用的 Unistore 实际请求 1 项通过。独立 SDK 表达式 444、执行器 122、聚合 41 及存储 Endpoint 3 项通过。限定 SDK Clippy、TiDB 根 lint 与双仓格式对照通过。未作性能测量，完整公告和 Unistore 已知基线结果沿用前批。

`evidence/native-conditionals/` 保存 16 对双仓跟踪源码树、命令及日志，最终 12 项固定源码检查通过。保留旧路由及初次批宽断言的失败。清单 72 项，SHA256 为 `adb82aba3dcec2819f116ca4ab7f7ca95a2ae537108bff1ca65aff8bce67c75d`。

| 对照 | 生产 Rust 新增／删除 | 测试 Rust 新增／删除 | 构建 | 材料 | 总增删 |
|---|---:|---:|---:|---:|---:|
| 相对 task 55 | 115／12 | 250／0 | 0／0 | 23／0 | 400 |
| 相对两仓原始端点 | 5,453／681 | 7,621／99 | 1,993／103 | 155／0 | 16,105 |

相对原始两仓的源码与测试累计增删 13,854 行。
