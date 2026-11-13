# 共享表达式内核 checkpoint 1 设计审查

作者 ExpressionReuseDesigner，2026-11-13，task #34。结论：可以沿此路线继续M1；本次局部实现尚不能通过M1验收。确认已复用原RPN循环，发现一个当前接纳范围内的FLOAT结果转换问题，以及程序类型元信息、默认依赖验证、清单生成和预算需要修正之处。没有发现必须重建宿主任务协议的依据。

以上是checkpoint 1的固定结论；后续修复状态见文末各轮Checkpoint跟进，保留初次发现便于核对修复依据。

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

## Checkpoint 2跟进

2026-11-13复核固定TiDB `d52b7802e1527855d700b9ee0ce4df229f4a7847` / TiKV `4b492c3204cd46e15139006d0530e380fbf18740`。本次核对源码、原始日志和统计，没有新运行构建或产品测试。[实现报告](https://github.com/YangKeao/tidb/blob/d52b7802e1527855d700b9ee0ce4df229f4a7847/rust/docs/shared-kernel/checkpoint-2.md)说明了范围及未完成项。

此前三个具体反例已修正：FLOAT/BIT等尚无共享结果转换的IF形状在编译时明确拒绝；Double/Decimal NULL保留类型，IF程序有明确出口类型；两侧Decimal parts拒绝不合法的首整数残字和末小数残字。拒绝未支持形状只是关闭错误接纳，不是完成FLOAT/BIT共享计算。程序元信息仍需与下一步finish校验及冻结一起完善。

读取实现者同机`evidence/shared-kernel-refactor/`中的原始日志后核实：

| 日志 | 实际终态 | 核对范围 |
| --- | --- | --- |
| review-regressions-before-2.log | 7项中4通过、3失败 | 正是FLOAT接纳、NULL出口类型、Decimal部分字三个反例 |
| review-regressions-after.log | 同7项全部通过 | 固定提交含相应断言；FLOAT/BIT另比较原生Datum变体 |
| default-compression-tests.log | 8通过、2忽略 | 关闭shared-kernel的压缩路径；忽略项在原始源码中已经存在 |

首次`review-regressions-before.log`误跑旧5项，不作为失败证据。日志未提供独立构建输入哈希证明，本审查不把读取日志等同于重跑固定提交。lint成功仍引用实现者报告；默认压缩验证不代表protobuf、TLS、metrics等所有依赖消费者都已验证。SDK循环测试在checkpoint发布时尚未完成，未追加通过声明。

按需读取现在确实由原共享循环访问ColumnRef触发，跳过的缺失列不再提前报错；signed Int列的`[7,2,7]`映射保留重复及NULL。PB入口仍先调用`ensure_columns_decoded`，再经同一输入接口执行；没有发现该次改动改变原PB短路或IF合同。动态分支、行/批差异和状态函数仍待完成，不能从常量条件测试推断它们通过。

清单已不再截断首个test属性后的生产源码，BETWEEN入口恢复；ledger输入及排序身份哈希吻合，245身份/5例外和191/48/1/5候选分类不变。候选锚点仍可能是测试或注册，不能当成算法迁移成本证明。

原始基线增删重算为`+10811/-135`、总10946；相对checkpoint 1为`+1446/-141`、总1587。源码与测试累计`+1120/-44`，总1164。修订预算的9350–17250已列核心加3650–4750储备等于13–22K，再单列9–12K开销，合计22–34K，算术无重复；当前非源码开销为9782。后续额外退休删除仍须排除核心中已计的调度删除以及算法搬迁旧端，完整成本仍未定界。

本轮没有发现新的正常定型输入错值。一个手工构造API的接纳缺口仍在：MD5只检查参数求值族而忽略自身声明结果类型，故错误声明为数字结果也会被接受并返回字符串。没有发现正常SQL规划器会生成这种形状；建议随共享签名接纳加入一次结果类型检查，不扩大为任意畸形AST的全面审查。

可以继续M1。下一次重点是同循环内动态分支/汇合、逻辑出现位置及借用生命周期，以及后续普通函数中避免重复复制列；本轮结论不表示完整M1、默认切换或性能验收。


## Checkpoint 3跟进

2026-11-13复核固定TiDB `4ce93983a71da469d9070499d919374fd56e913c` / TiKV `0a5cda977bd9a6707b4a56971c23f0e77e7a7322`。[实现报告](https://github.com/YangKeao/tidb/blob/4ce93983a71da469d9070499d919374fd56e913c/rust/docs/shared-kernel/checkpoint-3.md)已列出剩余M1工作。结论：动态分支的结构和完整值汇合已贯通，**JSON错误的结构身份仍需补齐**。跨行错误先后虽有变化，但用户已明确放宽warning/error顺序，不以此单独要求恢复严格原生首错。以下行为来自固定源码推导，本审查没有新运行产品测试。

### 已确认的进展

混合条件由原RPN循环保存父选择、两侧逻辑位置及明确汇合点。重复的物理行仍按每次逻辑出现计算；嵌套部分选择和相同结束地址的内外层汇合未发现具体错位。临时行选择通过`read_selected`取得拥有的列值，原借用路径继续保留，没有延长临时借用的unsafe技巧。native输出前的Real复制原始位，Decimal复制完整系数、存储/展示scale及声明形状。

native编译统一调用`RpnExpression::finish`，检查前向区域、受所属IF约束的跳转、栈EvalType合流及出口类型，完成后拒绝通过DerefMut/AsMut改变节点。当前正常编译路径未发现新的跳转损坏；该校验并未证明任意公开FnCall的参数签名或metadata合法，也未递归校验旧ShortCircuitFnCall子体，不应宣传成任意RPN的完整语义认证。

MD5的自身数字返回声明现在明确拒绝。JSON_LENGTH接入真正的字符串转JSON及长度内核；REGEXP_LIKE使用推迟准备的metadata，未选坏常量模式不再在编译时触发。这证明了当前受限输入的需求控制，尚不是完整错误兼容、缓存或性能证明。

### 1. 跨行错误先后差异：不作为严格排序要求

审查过程中先误将用户授权缩窄为只放宽warning，曾在Raft1372ccf8/c4375886及文档fb90494要求严格首错。重新读取人类原文`2979a576-99ec-4162-92fc-ff6a4cb656bb`后已撤回：原文是“不管 warning 和 error 的顺序”，task #3也已由人类完成确认。06e8cdf0已将更正同步实现者。下列源码差异保留供评估，不单独作为兼容阻塞，也不为其增加错误排序、记录器或回放机制。

当前已经接纳以下签名和输入：

    IF(c, JSON_LENGTH('{'), REGEXP_LIKE('a', '['))
    c = [0, 1]，逻辑行选择 = [0, 1]

原生入口先处理第0行，返回正则错误；共享循环先执行全部then行，返回第1行JSON错误。依据为[原始evaluator](https://github.com/YangKeao/tidb/blob/364aef2bab5cc633ecb76a775ae8f36f86a6687d/rust/crates/tidb-expr/src/evaluator.rs#L436)逐行失败即返回，以及[原始IF](https://github.com/YangKeao/tidb/blob/364aef2bab5cc633ecb76a775ae8f36f86a6687d/rust/crates/tidb-expr/src/scalar_function.rs#L1820)只求本行所选分支。IF不属于已有numeric batch支持集合，打开向量开关也不会使它变成先then后else。候选[expr_eval.rs](https://github.com/YangKeao/tikv/blob/0a5cda977bd9a6707b4a56971c23f0e77e7a7322/components/tidb_query_expr/src/types/expr_eval.rs#L468)总从then_positions开始，直到整支成功才执行else；函数错误立即返回。

整批条件也可能改变错误先后；交换分支顺序并不消除这类差异：

    IF(JSON_LENGTH(j), REGEXP_LIKE('a', '['), 7)
    j = ['[1]', '{']，逻辑行选择 = [0, 1]

候选先计算整批条件，后续行的坏JSON会先于第0行所选正则分支失败。这是同一批多个可达失败的先后区别，不能仅凭它违反用户已经放宽的顺序要求。错误类别的保留是指一个实际失败本身的类型与内容没有被改写；不能借此把来源顺序重新作为隐藏要求。

仍需保留短路需求、非诊断副作用及实际诊断内容/数量的既有合同。将来接入RAND/变量/BENCHMARK时，必须用其具体状态次数、状态顺序、跨批延续与停止行为约束调度；单纯把两个错误谁先返回作为否证不足以成立。现有mask可以保留，不把这两个纯错误反例作为增加有序路径的单独理由。

若状态或具体已执行诊断集合的反例证明需要有序执行，则由同一个共享循环推进所需范围，范围须包含条件，而非只改then/else顺序；不要由适配器逐行重跑整个求值器。原有numeric batch的阶段合同、多根ColumnMajor/RowMajor、Scalar与一行Vector仍各自明确，不笼统把所有可能失败的函数改成逐行。当前没有新的状态调用实测反例，状态支持继续作为原定M1工作推进。

### 2. JSON错误身份在共享边界丢失

`JSON_LENGTH('')`属于当前默认UTF-8字符串接纳范围。[原始JSON参数解析](https://github.com/YangKeao/tidb/blob/364aef2bab5cc633ecb76a775ae8f36f86a6687d/rust/crates/tidb-expr/src/builtin_ext/json/value.rs#L382)生成`JsonError::EmptyText`；[context.rs](https://github.com/YangKeao/tidb/blob/364aef2bab5cc633ecb76a775ae8f36f86a6687d/rust/crates/tidb-expr/src/context.rs#L351)给出码3140及空文档消息。非空坏文本`'{'`是不同的InvalidText，仍为3140。

共享lowering调用SDK `cast_string_as_json`，其[JSON解析](https://github.com/YangKeao/tikv/blob/0a5cda977bd9a6707b4a56971c23f0e77e7a7322/components/tidb_query_datatype/src/codec/mysql/json/serde.rs#L186)返回InvalidDataType，SDK码为1105；[native共享边界](https://github.com/YangKeao/tidb/blob/4ce93983a71da469d9070499d919374fd56e913c/rust/crates/tidb-expr/src/shared_kernel.rs#L343)又将错误转成String，进一步丢失结构身份。现有坏分支测试只比较`is_err()`，因此通过不代表诊断相等。这是当前可选候选入口的兼容缺口，未宣称默认产品路径已经回归。

共享内核应保留类型化失败，在输出边界按明确的native/PB错误合同映射，不解析Display字符串猜测错误类别。补空JSON、非空坏JSON的直接调用及活/死IF分支，对比错误类别、码和必要消息。PB原有诊断不能因为修native而全局改变。REGEXP的错误表示也需同样贯通；本审查没有猜测未追踪到最终执行器映射的正则SQL码。

### 统计与验证证据

独立按固定双仓库Git对象核对：

| 相对原始base的范围 | 新增 | 删除 | 总增删 |
| --- | ---: | ---: | ---: |
| Rust生产与测试 | 1958 | 49 | 2007 |
| 构建与锁文件 | 1866 | 91 | 1957 |
| 文档、清单和工具 | 7866 | 0 | 7866 |
| 两个候选全部文件 | 11690 | 140 | 11830 |
| 相对checkpoint 2的全部文件 | 949 | 75 | 1024 |

生产/测试重新分类与实际测试模块边界吻合；其变化不影响累计源码总数。累计差值884不等于本次原始增删1024，因为后者计入了对已改代码的再次修改。构建及文档合计9823，仍在修订9–12K范围内；算法迁移、剩余兼容和独立退休删除仍未定界，11830不是全项目完成量，也不含单独设计文档分支。

读取实现者原始日志确认以下终态：

| 日志 | 实际支持的结论 |
| --- | --- |
| checkpoint3-native-final.log | 11通过、0失败；冻结测试的panic被catch并通过 |
| checkpoint3-sdk-final.log | 34通过、0失败，包含既有PB短路/告警及新增IF反例 |
| pb-if-original-baseline.log | 原始SDK加同一新用例，1通过、0失败；两种flag都保留未选IF分支溢出 |
| checkpoint3-executors-final.log | executor库编译检查完成；不是执行器测试 |
| checkpoint3-scoped-clippy-final.log | 选定表达式/执行器库Clippy完成，有警告 |
| sdk-make-clippy.log | 准备阶段因找不到rustup失败；完整Makefile检查未通过 |
| lint-dynamic.log | 可见预期检查直至末条dashboard命令，没有错误；未捕获显式进程退出码 |

原始SDK独立测试工作区当前HEAD确为548812e，唯一tracked修改为新增36行PB测试。日志终态与该测试相符；但这些日志没有完整命令、构建前后源码树身份和显式退出码，不能独立证明所有构建输入就是之后提交的固定源码。日志时间早于提交并不说明测试了错误源码，也不足以单独证明没有中间修改。后续记录命令、双仓库commit/tree或dirty摘要及退出状态即可，不为此重跑已经有证据的整套检查。

本轮只读源码、Git统计及原始日志，没有新构建、产品测试、SQL或性能测量。下一步由ExpressionReuseEngineer补结构化诊断，继续宽值实际运算、调用模式、RAND/变量/BENCHMARK及多根状态；只有具体需求或状态合同要求时才增加共享有序调度，纯错误排序不成为新前置条件；完整M1、全范围体积和性能仍未验收。设计方向无须退回宿主协议。
