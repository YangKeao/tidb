# 直接复用原有Decimal与字符串解析

此页保留功能阶段的固定结果与计数。后续release测量、非法值检查、默认工具链实际失败及最新全量计数见[补证报告](datatype-reuse-performance.zh.md)。下文“未实跑release”及旧计数仅描述该功能阶段。

限定试验完成：共享RPN直接持有并调用原`tidb-datatype::Decimal`，删除676行复制的Decimal模块及另一个字符串前缀扫描实现。生产代码相对此前净减812行。已有31项原生对照和429项SDK表达式测试通过；没有扩大接纳范围，也没有证明完整项目约10K目标或性能目标成立。

固定源码为TiDB `f5daddcbdb39d91d20b99b012f29c1cdf4df1ae5`、SDK `d822fbf9fbee8e0b635974a8d38e42f9e73f067a`。本轮起点为TiDB72fedb911c／SDK3ad4dc75；原始对照仍为364aef2／548812e。此文及验证收据的增删也计入下表。

## 依赖和值的流向

SDK `tidb_query_expr`新增对相邻TiDB `rust/crates/tidb-datatype`的路径依赖；两个工作区均能解析和编译。该底层crate依赖datatype自身所需的error、hack、kvcache、mysql等库，没有回指expression、executor或session，未出现依赖环。SDK构建现在需要相邻TiDB检出；独立发布／拆包不在本试验内。锁文件新增的包和版本消歧全部计入构建改动，没有把新依赖成本当成零。

`ExpressionDecimal`成为原Decimal类型的直接导出。编译常量时克隆原值，算术借用同一类型，向量结果直接移动进`Datum::Decimal`；常量广播和分支选取仍可能克隆原值，但不再逐节点重建系数、调用固定存储codec或转成f64。失去调用者的`from_expression_parts`桥接方法删除，原有Decimal公共算术／存储接口保留。原PB Decimal类型与函数分派不变。

字符串→Real调用原`str_to_float(..., true)`和`float_warning_input`。共享RPN仍控制参数需求、转换调度、批量和诊断策略；转换事件仍按原表达式入口每次处理一次，未接入宿主子树求值回调。共享循环的IF、NULL、RAND、变量及重复执行规则未改动。

## 验证与实际失败

- 原生31通过、0失败、1323过滤；包括隐藏精度后续除法、192组Decimal边界、NaN、变量存储限制、字符串前缀／告警及Scalar/Vector需求。
- SDK表达式库429通过、0失败，无过滤；保留原PB算术、regexp和RPN行为回归。
- 限定表达式／执行器库Clippy完成但有警告；TiDB根`make lint`、修改文件格式及两仓`git diff --check`通过。

首轮原生结果为30通过、1失败：旧测试可直接构造非法系数；换成原Decimal后，`from_literal("x")`在debug构造阶段就断言失败。测试现明确核对这个边界；release模式若返回非法值，程序接纳仍必须拒绝。SDK的非法系数、存储／显示scale检查保留。这里运行的是debug测试；release分支未实跑，不把它报告为通过。失败日志`native.log`保留，最终日志为`native-final.log`及`sdk-final.log`。

运行时源码与构建输入共5524份，SHA256为`3aae7e1a62867bd83ba9bfb1d44aadc9a58c6c884373517b00bdd9f30db09e4e`；逐文件哈希与上述固定源码提交一致。[验证收据](datatype-reuse-validation.json)记录源码摘要及8份原始日志哈希。本地原始日志位于Engineer工作区`evidence/shared-kernel-datatype/`；历史checkpoint收据仍描述其原固定版本，不能拿旧摘要验证本轮。

从`tidb/rust`运行的检查命令：

    ../../tools/cargo-tidb test -p tidb-expr --features shared-kernel --lib shared_kernel_ --locked --offline -- --nocapture
    ../../tools/cargo-tidb test --manifest-path ../../tikv/Cargo.toml -p tidb_query_expr --lib --locked --offline -- --nocapture
    ../../tools/cargo-tidb clippy --manifest-path ../../tikv/Cargo.toml -p tidb_query_expr -p tidb_query_executors --lib --locked --offline
    ../../tools/cargo-tidb tree --manifest-path ../../tikv/Cargo.toml -p tidb-datatype --locked --offline --prefix none

两工作区还分别用`cargo tree -p tidb-expr --features shared-kernel --offline --prefix none`及`cargo tree -p tidb_query_expr --offline --prefix none`完成锁文件解析。固定后用以下命令核验输入和留存日志（在TiDB根目录；省略`--evidence`可只核验源码）：

    python3 rust/docs/shared-kernel/verify-checkpoint-6.py --tidb . --tikv ../tikv --receipt rust/docs/shared-kernel/datatype-reuse-validation.json --evidence /path/to/evidence/shared-kernel-datatype

## 两仓完整增删

分类沿用既有统计：Rust生产／测试、Cargo构建文件、文档与审计；测试根据实际`cfg(test)`分段。列值为新增／删除，全部文件以原始Git diff结算，删除此前新增代码会减少原始新增量，不把各checkpoint增删相加。此次未把算法搬到另一个新文件；旧datatype算法原地复用。

| 对照 | 生产 + / − | 测试 + / − | 构建 + / − | 文档 + / − |
| --- | ---: | ---: | ---: | ---: |
| TiDB原始 | 1034 / 1 | 2222 / 0 | 1867 / 91 | 10586 / 0 |
| SDK原始 | 1693 / 57 | 47 / 4 | 100 / 9 | 15 / 0 |
| TiDB本轮 | 7 / 59 | 15 / 15 | 1 / 0 | 131 / 0 |
| SDK本轮 | 17 / 777 | 0 / 0 | 100 / 9 | 5 / 2 |

本轮全部文件+276/−862，增删1138行，净减少586行；其中生产+24/−836，净减少812行。原始对照源码与测试+4996/−62，增删5058行；全部文件+17564/−162，增删17726行。

## 范围与下一步

正式`make clippy`仍是checkpoint6未完成状态，本轮没有重跑或以限定检查替代宣称通过；全原生测试、release/SQL性能、默认接入、完整M1生命周期仍未完成。当前只验证原数值与字符串路径可直接复用底层所有者；时间／JSON依赖和完整245族规模需要另行设计审查，未启动扩展。
