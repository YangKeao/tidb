# 单后端方案的现状核对

核对版本为 TiDB `663920e4d001231060e99218216e0ac34ec21565`、SDK `0521b09369455887732a1fa54f6292f0515f1318`。原始基线分别是 `364aef2bab5cc633ecb76a775ae8f36f86a6687d` 和 `548812e1ef57aef077a2062a9cc356640a6347f5`。双仓共有 135 个有限原生函数族、105 个未接入函数族、5 个历史例外，分母为 245。

| 原始基线到核对版本 | 新增 | 删除 | 增删合计 |
| --- | ---: | ---: | ---: |
| TiDB | 19,607 | 427 | 20,034 |
| SDK | 6,128 | 630 | 6,758 |
| 两仓 | 25,735 | 1,057 | 26,792 |

按来源分别为生产 10,622、测试 12,999、构建 2,098、文档 1,073 行增删。原型的 `shared_kernel.rs` 共 10,803 行，测试模块之前有 4,089 行。删除原型新增文件可以降低原始基线增删；若删除原始基线已有的代码，实际删除行计入总量。原始 `scalar_function.rs`、`ops.rs`、`cast.rs`、`string_fn.rs`、`builtin_compare.rs`、`evaluator.rs` 共 17,177 行，含类型、执行组织和仍有调用者的值辅助逻辑，实际可删除量有待逐项核对。

## 已有签名数据的复用边界

`pushdown_catalog.rs` 的 `CATALOG` 有 111 项、52 个拼写，按冻结清单名称匹配 47 个函数族。加入已知的 `date_add_*` 和 `date_sub_*` 动态解析后匹配 49 个。105 个尚未接入的原生函数族中，16 个在此处有对应名称，89 个没有。名称匹配只说明有现成的签名选择数据，具体重载、参数转换、返回声明与 SQL/PB 副作用仍需核对。HOUR、MINUTE、SECOND、MICROSECOND 有现成条目，TIME_TO_SEC 需要扩展解析来源。

当前 `resolve` 同时处理签名选择和协议准入，参数为 `PbScalar`。SDK 的 `map_expr_node_to_rpn_func` 为 crate 内部函数，读取 `Expr` 的签名、返回 FieldType 和子参数声明。抽取可复用的类型和签名选择需要保留协议检查。解析成 TiPB 树再回填原生声明会增加值域和元数据转换。

## 执行入口与最小对照

- `Expression::eval` 是已绑定行表达式的入口，`lib.rs::eval_in` 仍可直接求值未绑定 AST。常量折叠已有调用 `Expression::eval` 的路径。统一入口要核对两条现有标量调用链。
- `evaluator::{vec_eval_bool, vectorized_filter_consider_null}` 及 `EvaluatorProgram/EvaluatorSuite` 是现有批量边界。投影已经把不可变程序和执行期列所有权分开。换后端时应保留带错误的计算先结束、原始列再转移的顺序。Selection 的写语句、Hash Join V2、并行投影和 DML 仍有额外准入。
- SDK 的 `eval_decoded` 及原生的 `eval_group` 最终进入同一个 `eval_input_mode_with_state`。后者的结果编排、原生输出、调用策略和控制节点仍有减量空间。恢复按操作数重进 `eval_decoded` 会改变当前已验证的短路路径。
- 固定版本的实际 SQL 已验证相同 `DATEDIFF(DATE(t),d)>0` 通过公共编译进入投影和本地筛选，提交只新增测试。普通函数下一组对照可用 HOUR 的 TIME 列和返回同型同精度的子表达式；状态对照可用 `IF(cond,RAND(7),0)` 跨两批并用同一个程序重新开始执行。

统计使用每仓 `git diff --numstat <原始基线> <核对版本>`；目录类型与 Rust 测试模块分列。签名表按 `CATALOG` 构造项的第一参数取名称，再与固定设计的 245 族拼写相交。上述统计是原型的实测值，备选实现的用时、改动量和完整覆盖尚待实际候选验证。
