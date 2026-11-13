# 共享表达式内核 checkpoint 1 设计审查

作者 ExpressionReuseDesigner，2026-11-13，task #34。结论：可以沿此路线继续M1；本次局部实现尚不能通过M1验收。确认已复用原RPN循环，发现一个当前接纳范围内的FLOAT结果转换问题，以及程序类型元信息、默认依赖验证、清单生成和预算需要修正之处。没有发现必须重建宿主任务协议的依据。

本次只读审查固定 TiDB `7e3a6366746b5f61df3726e6a8f5cd5c6c52a582` 与 TiKV `52d7be276763f6b527ea52e11005c9e21231bdc7`，比较原始 `364aef2bab5cc633ecb76a775ae8f36f86a6687d` / `548812e1ef57aef077a2062a9cc356640a6347f5`。不将之后的工作树改动算入本结论。

实现与验收范围见 [固定checkpoint报告](https://github.com/YangKeao/tidb/blob/7e3a6366746b5f61df3726e6a8f5cd5c6c52a582/rust/docs/shared-kernel/checkpoint-1.md)。实现者报告4项定向测试及lint通过；本审查未重跑产品测试、构建、SDK检查或性能测量，以下行为反例来自源码推导，修复时须用原始入口固定预期并验证回归测试。

## 已确认的架构进展

`rust/crates/tidb-expr/src/shared_kernel.rs` 将真实Expression编译为RpnExpression；native Chunk和PB都进入SDK的`eval_decoded`。IF的前向跳转直接扩展这个循环；没有新的树求值器，也没有运行失败后回退旧求值器。编译遍历是迭代式。

新增ExpressionScalar保留原生f64位模式和宽Decimal系数、两种scale及declared shape；PB构建器不产生这些值，没有全局放宽原有NotNan。当前只是标量透传，普通函数对扩展载体仍拒绝；不能据此认为完整值域运算已解决。

常量IF测试验证真/假/NULL分支与特殊浮点值，尚未包含出错的未选分支。动态IF、按需读取、标量/向量调用区别、RAND/变量/BENCHMARK、多输出顺序均明确待做；这符合局部候选的说明。

## 应修正的问题

### 1. IF接纳范围比实际结果转换能力宽

`shared_kernel.rs:140`只比较分支和结果的EvalType；FLOAT和DOUBLE都是Real。构造返回FieldType::Float的`IF(1, Real(1.0000000000000002)[Double], Real(0)[Double])`，当前编译会接受，shared输出保持原始Real位模式。原生`ScalarFunction::eval`经过`coerce_to_ret_type`，应返回Float32(1.0)。这不是NaN运输问题，而是函数声明结果类型的实际合同。

依据：候选`shared_kernel.rs:140–148,277–288,295–300`；原生`scalar_function.rs:167–170,1019–1099`的`same_eval_family/coerce_to_ret_type`；`datum_convert.rs:136–160`的FLOAT转换。修复应把结果转换放到共享程序中，或在当前阶段明确拒绝尚不能保持的结果形状。先加入这个反例，再验证失败到通过；不要扩大“同EvalType即兼容”的假设。

BIT结果也属于Int求值族，但原生要求Datum::Bit载体，应作为同类接纳测试，不能靠整数值相等判定成功。

### 2. 平坦控制流需要明确的程序出口类型

SDK `types/expr.rs:237`的`ret_field_type()`仍看最后一条指令。当前`IF(1, Real(1.0)[Double], NULL[Double])`被接纳，但NULL在native lowering中成为Bytes(None)/VarString，程序结果元信息因而是VarString。当前native输出不使用该访问器，所以此例不是已证明的SQL输出错值；它是共享程序API中的类型矛盾，后续嵌套内核、projection及aggregate会使用该元信息。

按声明类型构造NULL，保留明确的根/出口FieldType。`is_last_constant()`的物理末尾含义也不能代替“整个程序为常量”。不要把else分支当作IF声明结果。

### 3. 可选求值器没有隔离默认依赖变化

TiDB根`rust/Cargo.toml`新增workspace级patch；锁文件中原有依赖实际发生以下变化：

| 依赖 | 原始 | checkpoint | 原有消费者示例 |
| --- | --- | --- | --- |
| flate2 | registry 1.1.9 | registry 1.0.11 | tidb-expr、executor、protocol、log、util |
| protobuf | registry 2.28.0 | PingCAP git 2.8.0 / de31a47 | prometheus及原有客户端 |
| cmake | registry 0.1.58 | git同版本 / 6319069 | aws-lc-sys构建 |

关闭shared-kernel不会取消workspace patch。默认求值路径可以保持原路由，但依赖栈并非原样。需记录这一点，并补有代表性的关闭shared-kernel构建及受影响路径测试，或隔离依赖图。这里指出验证缺口，不把版本变化直接判为运行回归。

### 4. 清单生成器漏掉测试属性之后的生产代码

`inventory.py:28,33`用`split('#[cfg(test)]')[0]`截断整个文件，不等于排除测试项。原始`tidb-expr/src/lib.rs`首个属性在329行，生产eval在460行、Between在1066行，因而BETWEEN没有native anchor；它的专门搜索也无法找回已经截掉的内容。

改用可靠的item边界，或保守地扫描完整文件并明确测试路径/候选的性质；不必为此建设新框架。同时记录`--families`输入的固定Git引用或哈希和生成命令。245行及唯一性断言本身不能固定245个具体身份。

当前245身份、5例外与191语义候选+48迁移检查候选+1 MD5候选吻合，所有equivalence_verified为false，没有将名字命中冒充兼容通过。但M0仍需逐项确认真正实现位置，才能给迁移和语义改动定预算；注册表命中不是算法拥有者。

### 5. Decimal parts公开构造器还缺部分字的对齐校验

两侧新增`from_parts`均接纳`(0,1,1,false,[1,0,0,0,0,0,0,0,0])`。一个小数位应左对齐在base-1e9字中；这个值却在声明存储精度以外含非零位。SDK会将其显示为`0.0`，同时`is_zero()`为false。存储和展示精度都为1，这不是合法的隐藏精度。

构造器可校验首个整数残字的范围和最后小数残字的低位对齐，或仅接收不公开构造的有效parts类型。现有实际桥接使用可信`to_parts()`输出，本审查没有发现有效往返损坏；这项优先级低于已接纳FLOAT反例，不要求扩大到整套存储API重构。依据为native `mydecimal.rs:1866–1878`及SDK `codec/mysql/decimal.rs:967–980,1771–1774,1978–1988`。

## 体积核算与预算修订

独立按两个固定提交的`git diff --numstat BASE HEAD`重算：

| 范围 | 新增 | 删除 | 总增删 |
| --- | ---: | ---: | ---: |
| Rust生产与测试 | 802 | 16 | 818 |
| Cargo清单和锁文件 | 1866 | 91 | 1957 |
| 生成的覆盖JSON | 6569 | 0 | 6569 |
| 清单生成脚本 | 89 | 0 | 89 |
| 说明文档 | 152 | 0 | 152 |
| 全部文件 | 9478 | 107 | 9585 |

其中TiDB为+9261/-91，SDK为+217/-16；总净增9371。818行源码/测试统计正确，但只表示首个局部候选，不证明全项目10K数量级达成。

`inventory-notes.md`现有build/config/docs预算仅+100–250/-0–100，而对应实际已+8676/-91（包含清单及脚本）。应拆分锁文件、生成清单与文档的实测项并重估；不需要为了凑行数压成难审阅的文本。预算表已列项目总和为9450–17600行，与设计13–22K基础预算之间的储备应明确列出。

算法移动、额外语义兼容、独立退休删除仍未定界。维持`T=B+2M+S+D`不重叠核算，当前生成证据和依赖文件也加入B的单独栏目。先对高风险/迁移候选落实来源、语义差异及增删估算，再扩大M2；已出现的分类超支就应触发预算修订，不必等接近100K才讨论。

## 下一阶段的具体设计边界

动态分支留在同一共享循环中：记录分支汇合位置、当前逻辑出现位置和待执行另一支，按逻辑输出位置写回。`[7,2,7]`是三次出现，不能按物理行去重。单一pc的整批跳转只能证明常量条件；不要用适配器逐行重跑全程序来替代批量控制，也不要在运行时扫描嵌套Jump猜汇合位置。

当前适配器预先复制全部引用字节列，对这个简单输入子集尚无SQL诊断差异证据。后续JSON解析、cast等可能失败的转换必须在内核需求驱动下执行；不能把它们直接加到现有eager gather。保留不可变列借用或拥有解码结果即可，不引入宿主请求/恢复协议。

一般控制组成开放前，进行一次finish校验并冻结程序：检查目标、分支汇合栈形状和类型。现有Vec可变API适用于旧直线RPN，但任意插入会破坏绝对跳转目标。只验证执行中遇到的边界和最终assert栈深不适合作为一般构建合同。

输入绑定目前依赖调用者提供匹配的Chunk布局和每列长度。原生入口也有这个前提；错配Chunk不是已证明的合法输入回归。新API应明确此合同，或在绑定边界检查必要布局/长度，不建立值来源认证协议。

## 本次验证与交付限制

执行了固定对象`git show`、`git diff --numstat`、源文件搜索及JSON/TOML计数，交叉检查了上述源码链；审查文档单独提交，未修改实现者工作树。发布前检查文档链接对象和`git diff --cached --check`。没有新运行产品测试、构建、SQL或性能测量；4项定向测试和lint仍引用实现者固定checkpoint的报告。

设计方向继续，但FLOAT反例、出口类型、默认依赖验证、清单与预算记录须跟进。task #33继续由ExpressionReuseEngineer实施，task #34跟进设计审查；本报告不标记完整M1或项目完成。
