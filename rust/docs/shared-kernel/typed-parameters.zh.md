# 显式共享投影中的Decimal类型化参数

本批在现有`SharedProjection`／`SharedExpression`入口接入Decimal算术的参数叶子。
原Constant和共享循环调用同一份datatype转换与补尺度函数；共享循环按需求取当前原生Datum，
不回调Constant或Expression求值。实际溢出时按静态片段和参数位置生成当前值诊断。
本批依据Designer cda358b9及固定设计e862529a，继续保持默认执行器和跨EXECUTE缓存范围不变。

## 接纳与所有权

| 输入位置或边界 | 行为 |
| --- | --- |
| 已支持Decimal `plus/minus/mul/div`的无DeferredExpr参数操作数 | 接纳；父算术必须通过原生`numeric_operand_domain`的Decimal签名检查 |
| IF包住上述算术 | 接纳，沿用现有共享整根逐行调用方式 |
| 裸参数结果、IF直接返回参数、其他请求域、DeferredExpr | 编译拒绝 |
| 参数缺少静态类型或父签名尚未解析 | 编译拒绝，不把固定RPN输出当成原生签名证明 |
| 当前值变为Int／UInt／Real／Float32／String／NULL等 | 在共享循环执行原类型化Decimal转换；原来不能转换的值返回原错误，不回退 |
| 原生把MinNotNull／MaxValue等推导为NULL类型 | 保持原类型判断，返回NULL；不只检查Datum是否为Null |
| 程序与绑定 | 调用方从本次执行已绑定、已解析的表达式快照编译；同一执行内跨Chunk复用，下一EXECUTE重新构造；一次求值期间绑定必须不变 |

这里的执行所有权是显式入口的调用合同和文档，不是已安装到SQL执行器中的生命周期管理。
没有修改`ProjectionExec`、并行worker或prepared计划缓存，也没有建立跨EXECUTE的共享程序缓存。
测试换值／换上下文用于核对程序不保留旧状态，不据此宣布跨EXECUTE引用或并发安全已证明。

## 实现

- `tidb-datatype/src/datum/convert.rs`新增`to_typed_decimal_with_context`，直接迁入原Constant的
  转换后补尺度逻辑；保留先转换诊断、后固定MyDecimal缓冲区补尺度错误的顺序。
  同处的`numeric_diagnostic_text`由原生诊断和共享诊断共用，保留原始值文本的规则。
- `constant.rs`委托上述primitive；原算术签名判定开放给同crate的编译器复用，
  没有复制类型匹配规则。现有裸／typed／Deferred参数入口合同保持原样。
- SDK的`NativeParameterDecimal`指令读取owned Datum，按当前值推导类型，执行原NULL判断，
  在内核构造原ConversionContext并转换，输出完整`ExpressionScalar::Decimal`。
  `InputColumns`仅提供原始参数和转换上下文／告警能力，不接收表达式树。
- 纯算术Vector一次叶子激活转换一次并广播；Scalar以及IF整根逐行路径各次激活分别转换。
  宿主会话的向量开关继续决定入口调用方式，没有参数值缓存或全局预取。
- 仅包含参数的可格式化Decimal算术保存静态文本与参数位置。
  发生1690时读取本次不变绑定的原始值，不再次转换；列名和嵌套括号保留。
  无法按原生详细格式表达的子树（例如内部IF）继续返回通用Decimal溢出文本。
  没有诊断值句柄、第二个求值循环或失败后旧入口重试。

## 原生对照与失败记录

新增四项共享测试，原35项保留（先前合同比较中的真实参数算术由拒绝改为接纳；
合成列槽自身的负例仍保留，区别于新增参数指令）。重点证据如下：

1. `shared_kernel_typed_decimal_parameter_vector_and_if`直接调用原`EvaluatorSuite::run`，
   对照纯算术和IF。Int、UInt最大值、Real、Float32、Decimal、NULL及`2.5junk`，
   覆盖warning／strict、单行Vector、三行Vector及关闭向量开关的三行Scalar。
   共84个组合；既比较完整返回Decimal，又写回Chunk比较完整字段。
   同一String参数在纯算术三行Vector只产生一条转换告警，IF和Scalar产生三条。
   严格模式对照原错误码与完整文本，读取计数证明成功路径没有额外诊断取值。
2. `...demand_and_current_diagnostics`覆盖编译拒绝位置、未绑定参数的1105、空输入、
   IF死分支、当前80位原始Decimal／带junk的String在嵌套算术乘列时溢出，
   比较原生错误身份、完整表达式、列名和告警；IF子树验证通用文本。
   未绑定／溢出后同一程序恢复成功，换NULL及上下文释放后旧输出完整字段仍有效。
3. `...runtime_carriers`比较Scalar及单行Vector的特殊边界Datum、Bytes、NaN、Infinity、
   非UTF-8 String和无数字String，核对原结果／错误及告警。
4. `...shared_typed_decimal_primitive_padding`经过原Constant验证7补成7.000，
   81位原值补尺度产生原Conversion错误，避免迁移后把补尺度错误降为告警。

修复前测试实际失败于参数编译拒绝。新增载体对照又记录了MinNotNull被错误尝试转换的失败，
随后补齐原生推导NULL类型判断；缺少静态类型的表达式曾被错误接纳，也有失败后修复记录。
其余中间失败为编译器编辑命中错误位置，以及测试把原1265写成1292、使用了错误的溢出边界；
它们已保留，未记为通过。SDK Rustfmt曾误用2024 edition，因原有`gen`标识符失败，随后按
Cargo声明的2021 edition检查。没有修改这些原有标识符。

## 验证与固定材料

精确输入摘要、固定提交和原始日志哈希见[`typed-parameters-validation.json`](typed-parameters-validation.json)。
所有Cargo命令从TiDB `rust/`目录执行，使用同一nightly-2026-08-22包装器：

```sh
CARGO_BUILD_JOBS=4 ../../tools/cargo-tidb test -p tidb-expr --features shared-kernel --lib shared_kernel_ --locked --offline -- --nocapture
CARGO_BUILD_JOBS=4 ../../tools/cargo-tidb test -p tidb-expr --features shared-kernel --lib constant::tests::prepared_ --locked --offline -- --nocapture
CARGO_BUILD_JOBS=4 ../../tools/cargo-tidb test -p tidb-datatype --lib decimal --locked --offline -- --nocapture
CARGO_BUILD_JOBS=4 ../../tools/cargo-tidb test -p tidb-datatype --lib datum::convert::tests --locked --offline -- --nocapture
CARGO_BUILD_JOBS=4 ../../tools/cargo-tidb test --manifest-path ../../tikv/Cargo.toml -p tidb_query_expr --lib --locked --offline -- --nocapture
CARGO_BUILD_JOBS=4 ../../tools/cargo-tidb clippy -p tidb-expr --features shared-kernel --lib --locked --offline
```

39项共享原生、8项prepared、89项Decimal、7项Datum转换及429项SDK表达式测试通过。
89项与7项筛选有重叠，不相加冒充独立测试总数。限定Clippy完成但有警告；根`make lint`、
两侧相关Rustfmt 2021和`git diff --check`通过。源文件摘要在最终检查前后核验。
既有验证器支持`--receipt typed-parameters-validation.json --evidence <本地记录目录>`。

本批不做新性能测量。SDK原默认工具链／独立检出问题、正式完整`make clippy`、全部SQL入口、
完整参数／Deferred兼容、跨EXECUTE生命周期与性能目标仍未完成；本次成功使用相邻源码和固定包装器。

## 全部增删

统计以Main bfde2f35／SDK d822为本批基线，原始累计以364aef2／548812e为基线；
包括生产、测试、报告及验证材料，按最终树计数，不累计相互覆盖的提交行数。

源码固定为Main `be800b9126653ff96620cb78e51ef350b1c352a0`、SDK
`3c9b12cfb80e2928896a28def1b2a3b8b7dc3517`；5525项输入与26份记录已核验。

本批生产：+299/−73，净增226行。
本批测试：+346/−2；文档／证据+275/−0；构建文件0。
本批全部：+920/−75，净增845，原始增删995行。
原始累计：源码及测试6910行增删；全部+24201/−219，共24420行增删。
