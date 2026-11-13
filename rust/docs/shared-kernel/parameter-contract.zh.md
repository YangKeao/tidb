# 参数重绑定：有限合同验证

本批保持产品参数拒绝，只增加两个原生对照测试和测试内的按需输入槽原型。
同一冻结Decimal程序可以换值、NULL和上下文；这不等于真实prepared执行已经接入共享程序。
当前接口缺少参数取值后的原生转换语义和动态操作数诊断，直接把参数改写成列不成立。
另修正上批报告：同列元信息冲突是在**每个编译根内**拒绝，不是整个投影统一检查。

## 合同表

| 场景 | 原生行为／实际owner | 本批证明及边界 |
| --- | --- | --- |
| 裸ParamMarker | `Constant::eval_lazy_in`读取本次参数，通常直接返回Datum；`get_type`按当前值推导 | Int 7、String 2.5、NULL与保存的99区分；产品编译仍拒绝 |
| 类型化Decimal参数 | 父算术通过`eval_typed_as_on_row`要求Decimal，读取当前类型和值，再用原datatype转换 | 原生Int 7可参与Decimal除法；原型槽只接Decimal/NULL，明确失败，不能作为完整参数接入 |
| ParamMarker同时带DeferredExpr | marker优先，DeferredExpr树不执行；通用Eval仍按ret_type转换／补小数；typed入口按请求域转换 | 带未绑定另一参数的DeferredExpr仍不触发；有／无Deferred的裸结果不同，typed结果比较完整Decimal字段 |
| 同一签名换Decimal值 | prepared SELECT缓存按参数类型匹配，宽Decimal计划可接精度和尺度均不增大的新值 | 原型同一RPN跨Decimal 1、1.25、NULL和除法精度4／8；NULL仅证明内核载体能力，不宣称真实缓存跨NULL同计划 |
| 类型变化 | Decimal变NULL、Int或更宽Decimal不匹配该缓存项；可选另一个兼容旧项，否则重新规划 | 阅读真实执行owner，定向运行已有session Decimal宽窄／字符串collation测试；没有把缓存键假设成RPN所有权合同 |
| 不需要的参数 | 编译不求值，IF死分支及零行入口不读未绑定参数 | Scalar、Vector死分支；原生EvaluatorSuite空Chunk和共享公开入口空选择均零次参数读取 |
| 当前值溢出文本 | 原生算术诊断带本次参数，而不是规划值99 | 81位9乘10：原生含当前值；原型1690正确但冻结列名文本不含参数，形成明确反例 |
| 错误后调用和旧结果 | 每次execution拥有参数，已返回值独立于后续参数 | 原型未绑定／错类型／溢出后用新上下文成功；先前输出在原上下文销毁后逐字段重验 |

## 实际执行所有者和重编译边界

这不是从通用LRU推测的合同。实际SELECT路径为：

1. [`driver/access.rs`](../../crates/tidb-executor/src/driver/access.rs)的
   `PreparedSelectPlan::bind_inner`先计算`PreparedParameterType::of`，并匹配schema、统计、
   环境、参数类型和LIMIT值。`compatible_with`对Decimal采用旧precision≥新precision且
   旧scale≥新scale，其余类型要求相等。NULL是独立类别；Int/UInt也不同。
2. 命中时调用[`CachedSelectPlan::bind`](../../crates/tidb-executor/src/driver/planner_bridge.rs)，
   重绑AST和物理树，递增generation；失败删除该缓存项，不让半重绑树进入下一次执行。
   不命中时以本次值绑定AST，通过`cached_query_plan`重新产生物理计划。
   类型变化也可能命中此前为该类型创建的另一项，不能统一称为每次必重编译。
3. [`physical_plan_cache.rs`](../../crates/tidb-planner/src/physical_plan_cache.rs)的
   `bind_expression`更新marker保存值，但保留marker；递归后清算术参数缓存。
   该函数没有重新推导每个父算术签名。保存值更新不能代替执行时参数读取。
4. `PreparedSelectExecution`持有本次`Arc<[Datum]>`；`with_plan`检查generation；
   `StmtContext::param_value`读取该execution的参数。这个真实边界尚未拥有或缓存本批RPN。
   本次不修改SELECT／DML缓存，也不声称所有prepared语句都走此路径（参数化TableDual等可不缓存）。

因此：可沿用既有owner的计划选择边界，但仍需证明哪个完整字段／转换要求决定共享签名，
何时创建及丢弃对应RPN。不能在已开始求值后因类型不合才回退旧求值器。

## 最小原型与两个反例

测试位于[`shared_kernel.rs`](../../crates/tidb-expr/src/shared_kernel.rs)的
`shared_kernel_parameter_contract_*`。`ParameterProbe`仅存在于`cfg(test)`：
原生表达式持有真实ParamMarker；共享侧用明确的Decimal列占位符构造现有程序，
把测试上下文接到`InputColumns::read`，只按需求搬运原Decimal或NULL。
这不是把产品参数替换成保存常量，也不是产品编译器的参数支持。
算术、IF、除法精度处理和组合仍执行现有RPN；没有复制Decimal算法。

冻结程序`IF(1, slot / 3, 9)`跨六组值／上下文，分别执行Scalar、单行Vector和乱序重复行
Vector，共18个调用，完整比较符号、系数、存储尺度、结果尺度、声明形状和文本。
死分支程序分别编译一次；它不是证明一个程序的IF条件可重绑定。
旧结果保留到所有上下文释放后再比较。错误恢复覆盖同一除法及同一乘法程序。

必要接口差异仅有以下具体边界，未实现通用框架：

- **按需参数值与转换要求**：当前`InputColumns::read`返回固定schema下的typed值，
  不携带裸参数／类型化目标／Deferred语义；原型对Int明确拒绝而原生成功。
  后续接入须让内核取得本次参数载体并复用原转换规则、警告策略和错误载体，
  同时由执行owner在运行前保证该程序适用。
- **动态诊断操作数**：`NativeArithmetic`当前保存编译时的overflow字符串。
  81位9乘10的错误码相同，文本不同。后续须以本次已需求的值生成原生诊断，
  不能为诊断提前读取死分支参数，也不能继续使用保存的规划值99。
- **空批次入口**：底层`eval_input_mode`断言`output_rows > 0`；原型首次直调零行失败已保留。
  原型现沿用现有`SharedExpression::evaluate_with_context`的零行早返边界，
  并另跑真实公开入口及原生EvaluatorSuite空批次；未修改底层RPN语义。

## 验证与限制

命令在TiDB `rust/`目录使用固定`../../tools/cargo-tidb`包装器（nightly-2026-08-22）；
必须在该目录执行以应用既有Cargo配置。固定源码、原始记录摘要与测试终态见
[`parameter-contract-validation.json`](parameter-contract-validation.json)。

```sh
CARGO_BUILD_JOBS=4 ../../tools/cargo-tidb test -p tidb-expr --features shared-kernel --lib shared_kernel_ --locked --offline -- --nocapture
CARGO_BUILD_JOBS=4 ../../tools/cargo-tidb test -p tidb-expr --features shared-kernel --lib constant::tests::prepared_ --locked --offline -- --nocapture
CARGO_BUILD_JOBS=4 ../../tools/cargo-tidb test -p tidb-session --lib parameter_type_compatibility_matches_go --locked --offline -- --nocapture
```

35项共享原生测试、8项既有prepared测试和1项session缓存测试通过。
根目录另执行`make lint`通过；Rustfmt和`git diff --check`检查本次文件。
测试原型选择未绑定参数时另明确核对原生Unsupported与共享1105的相同错误文本。
初次路径错误未插入测试，筛选运行0项；随后两处测试API拼写错误导致编译失败；
再下一次1通过1失败，暴露上述非空RPN前置条件。这些尝试不计为最终通过。

本批无生产代码改动、无SDK改动，不重跑已固定的429项SDK测试，不新增性能测量。
没有验证产品参数接入、每批转换告警次数的完整兼容、所有prepared SQL、完整生命周期、
默认SDK独立构建或正式完整Clippy。
既有向量性能差距和完整M1目标保持未完成。

## 固定提交与完整计数

最终测试源码为`3034ea26f3780299b6931fb241bed6cd7cbc9874`，SDK保持
`d822fbf9fbee8e0b635974a8d38e42f9e73f067a`。早期checkpoint为f30d2377，
最后只补未绑定参数的错误身份断言；已按最终源码重新执行定向检查。
5525项源码输入摘要与原始日志哈希可用既有`verify-checkpoint-6.py --receipt`复查。
本批统计以Main c08c8164／SDK d822为基线，原始累计仍以364aef2／548812e为基线；
使用`count-datatype-reuse.py`，以下全部是最终树相对基线的增删，非提交逐笔相加。

本批：生产0，测试+279/−0，文档／证据+222/−2；全部+501/−2，净增499，原始增删503行。
原始累计：源码及测试6226行增删；全部+23299/−162，共23461行增删。SDK本批为0。

本批采用最小反例交付，建议下一步先明确owner持有的参数签名及内核转换／动态诊断边界，
再另行决定最小产品实现。现有证明不足以扩大参数接纳。
