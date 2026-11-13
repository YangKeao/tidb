# 用一个表达式内核替换任务协议：Expression Reuse 设计与实施计划


本文件按仓库根目录 `PLANS.md` 维护。完整方案尚未实现；2026-11-13 已开始独立候选，各轮局部实现的审查见 [checkpoint reviews](expression-shared-kernel-checkpoint1-review.zh.md)。`Progress`、`Surprises & Discoveries`、`Decision Log`、`Outcomes & Retrospective` 随后续验证更新。作者 ExpressionReuseDesigner，2026-11-13，讨论为 Raft `#tidb-expression-reuse:1dc6ca2b`，设计 task #32、实现 task #33、设计审查 task #34。

## Purpose / Big Picture


建议让 Rust TiDB 和 TiKV **直接使用现有 TiKV 表达式库，扩展它的一套程序表示和求值循环**。共享范围包括表达式组合、参数求值与转换、短路、批量调度及函数计算；两端只保留把源表达式编译进去、读写数据和提供会话能力的适配。不要把当前数十万行方案搬进新目录，也不要再为每个函数构造小程序、启动 worker、请求宿主恢复。

达到目标的关键不是把一个复杂接口写得更短，而是缩小新增机制的数量：**一个程序、一个执行循环、一种类型化值边界、每个语义一种函数实现**。原有存储格式、优化器和与表达式无关的 datatype 公共 API 留在原处；共享表达式内核不要求统一它们的全部实现。

目标保持为人类提出的“总修改在 10K 数量级”。初步为核心接入和基础测试分配 13–22K 原始增删量，其余算法迁移、语义修订和旧代码删除另算，不能把这个局部预算当作全项目总量。**设计有明确的缩减路径，但完整体积尚未证明；第 6 节给出可核算的预算公式及停止条件。**

## Progress


- [x] (2026-11-13) 读取人类的体积、共享表达式内核和本次设计指令；阅读现有复审及源码。
- [x] (2026-11-13) 分别审查原始 RPN 内核、数据/上下文边界、改动量和兼容风险。
- [x] (2026-11-13) 给出模块边界、拟新增接口、执行规则、预算及分阶段验收；经三个独立方向复核，修正 PB eager、RAND/root 顺序、状态实例及完整体积核算。
- [x] (2026-11-13) 按人类6a0aeef4授权指导独立候选；只读审查 TiDB7e3a636 / TiKV52d7be2 的 native/PB MD5、完整标量透传和常量IF初步实现。
- [x] (2026-11-13) 复核d52b780 / 4b492c3：三个反例的修复前后日志、按需列读取和PB保留语义、清单修复及修订预算；仍未完成动态控制和状态等M1范围。
- [x] (2026-11-13) 复核4ce9398 / 0a5cda9：动态IF、真实坏JSON/regexp需求控制、完整值汇合及finish冻结已出现；发现JSON诊断身份丢失；跨行错误先后差异按人类2979a576的放宽处理，已撤回审查中误加的严格首错要求。
- [x] (2026-11-13) 复核b21d5a9 / 038b1aa：JSON3140和Scalar/Vector及IF内部NULL需求修复有实际失败到通过证据；发现只包IF区域未覆盖原生整根numeric batch回退，已交实现者修正。
- [ ] 完成M1：补结构化诊断，贯通复杂值运算、状态/重复执行与行批量合同；不为纯错误排序单独增加机制；局部实现不代表M1完成。
- [ ] 证明全部函数与入口覆盖、总改动量和性能目标；未证明前不推广到默认路径。

## Context and Orientation


本轮是 **Rust TiDB → Rust TiKV crate** 的同进程复用。`rust/Cargo.toml:142–148` 指向相邻 TiKV 的 `tidb_query_expr` 和 `tidb_query_datatype`。不是给 Go TiDB 增加 cgo；后者需要独立 ABI 和生命周期设计，不能算作本轮已经覆盖。

源码审查固定 TiDB `ca40487b44cc2970817baa0accb0b2a4d572a10a`、TiKV `f7672bf980143f2938fd0f900650a4c29091e70f`。项目原始基线分别为 TiDB `364aef2bab5cc633ecb76a775ae8f36f86a6687d`、TiKV `548812e1ef57aef077a2062a9cc356640a6347f5`。推荐的产品候选从这两个基线开始，只挑选有必要的兼容修复；不把当前复杂协议作为新内核的依赖。设计文档的分支基于当前提交，方便审阅，不能将这个文档分支误作产品候选。

原始 TiKV 已有 `RpnExpression`、`FnCall`、`ShortCircuitFnCall`、常量和列节点，见基线 `components/tidb_query_expr/src/types/expr.rs:12`。RPN 指后缀指令表示，例如 `a b +`；现有实现还允许短路节点持有独立子表达式。基线 `types/expr_eval.rs:259` 的 `eval_decoded` 已有向量计算以及借用输入列、拥有中间结果的区分。`types/function.rs` 的 `RpnFnMeta` 对接现有函数实现。这里有可以直接复用的主体，无须再设计一套虚拟机或新的可恢复任务系统。

当前增长主要在 `tidb-expr/src/evaluator/`、`tidb-expr/src/tikv/`、SDK `src/local/`，以及 native datatype/函数适配。此前统计两仓库 Rust 总计 `+360,939/-47,904`，其中明确识别测试净增 209,121 行、其他 Rust 净增 97,419 行、docs 中探针 6,495 行；分类定义见 [现有复审](expression-10k-design-review.zh.md)。这不是“共享求值循环必然需要这么多代码”的证据。

当前能力清单仍以 `rust/docs/evidence/expression-unification-final-coverage.json` 及入口清单为基准；不能从预算倒推出删除功能。此前冻结分母是 245 个函数族，240 个已有共享库算法迁移记录，另五个明确保留兼容函数。不把它表述为全部 TiDB SQL 已完整支持。

## 1. 决定保留什么，替换什么


| 部分 | 决定 | 原因 |
| --- | --- | --- |
| TiKV 现有函数、类型化向量、RPN 表示 | 原地复用，最小扩展 | 它们就是已有表达式内核资产 |
| TiDB AST / Expression、TiKV PB | 各自保留解析和类型信息，编译到同一程序 | 输入格式不同不需要执行器不同 |
| TiDB `HostTask`、SDK host start/resume、能力 token | 产品候选不引入 | 同进程的普通函数求值不需要消息往返 |
| LocalExpr → facts/snapshot → 多 profile → RPN | 收敛为一次构建及一次结构验证 | 同一不可变程序不反复证明来源和形状 |
| Bytes 中的 40 字节 native value handle | 去掉，改类型化参数及返回值 | 生命周期由 Rust 借用/拥有关系约束 |
| ready-value 固定程序与 worker | 不引入 | 函数已在正在执行的程序里，直接调函数即可 |
| row / bulk / Decimal fast 的多份程序 | 一个程序，显式调用语义与内部调度模式 | 保留行为差异和向量优化，消除成套复制 |
| TiDB 全部 Decimal/JSON/时间存储 API 迁移 | 不作为表达式复用的先决条件 | 避免将低层 API 重构捆绑进本次目标 |
| 不再存在的协议测试、历史实验开关 | 不带入新候选 | 语义反例保留，消失的协议无需继续测试 |

每个函数最终应归共享库所有并由统一调度器调用。确实依赖 TiDB 产品服务的少量叶子函数允许调用能力接口，但接口只收已经求出的值，不能持有表达式树、请求宿主求子树或在失败后转去旧求值器。此前登记的五个例外需保持独立清单，不能把普通函数改名为“宿主能力”逃避共享。

## 2. 程序与构建接口


保留 `RpnExpression` 作为唯一可执行程序类型；其内部采用平坦指令区、段范围表和子段索引表。普通 `EagerCall` 消费 RPN 栈上的参数；`DemandCall` 引用参数子段，由同一循环决定何时执行。一个“段”是同一程序内的一组指令，不是另一次 compile 的子程序。只有 Region/Call 两种 frame。平坦所有权也避免深嵌套对象的递归析构；构建和析构都必须覆盖深表达式测试，不能只修执行栈。

建议在 `types/expr.rs` 和 `types/expr_builder.rs` 扩展构建器；下列名字是**拟新增接口**，不是当前已有 API：

    ProgramBuilder::constant(value, field_type) -> NodeId
    ProgramBuilder::column(column_id, field_type) -> NodeId
    ProgramBuilder::call(signature_id, children, full_type) -> NodeId
    ProgramBuilder::finish(roots, group_contract) -> Result<RpnExpression, BuildError>

    struct CallSpec {
        kernel: ResolvedKernel,
        demand: DemandRule,
        effects: EffectClass,
        // Private: resolved by shared signature registry, not source adapters.
        // Prepared constants, type/collation parameters; no source owner token.
    }

    enum DemandRule {
        AllArgs,
        NullStops,
        And,
        Or,
        If,
        Case,
        Coalesce,
        Repeat, // BENCHMARK: evaluate count, then freshly execute child N times.
        // Only add a distinct rule when an observed contract needs it.
    }

公开 builder 接收受控的 SignatureId，`CallSpec` 的 kernel/demand/effects 由共享签名表解析，source adapter 不能任意拼配函数指针和需求规则。合法语义变体以明确签名/参数登记，避免每个来源再次实现一份求值规则。

这不是要求所有现有函数都改用新 ABI。`ResolvedKernel` 可以直接保存现有 `RpnFnMeta`；少量确需完整值域的兼容函数使用同一库内的类型化入口。两者收到的都是已求出的参数，不携带宿主任务状态。

TiDB 在 `tidb-expr` 新增薄的 `shared_compile.rs`，读取 AST、已定型 Expression；SimpleExpr 定义在 `tidb-unistore/src/cophandler.rs`，它的 lowering 留在 Unistore，避免反向 crate 依赖。TiKV 保留 PB builder，所有这些入口均使用同一构建原语。不同 source adapter 只解释来源结构、声明类型和签名，不生成第二份求值状态机。不强行把 AST 包成假的 PB 或伪造 `FieldType`，也不把所有来源字段复制到程序里。

一个程序可含多个根段。`finish(roots, group_contract)` 冻结输出顺序和执行组语义；共享 `evaluate(program, input, invocation, context, state, scratch)` 负责 root×row 调度。projection 返回类型化结果列；filter 根列表按原有合取/NULL合同更新 selection，仍复用同一循环。宿主只负责 operator 调度、列交换与输出提交，不能另写根表达式批量循环。只求一个 root 的 API 是此接口的单根形式。

程序的函数签名表达真正 SQL 语义，而不是“来源是 TiDB/PB”。例如 signed/unsigned、collation、返回 scale、特定 NULL 行为是合法差异；来源 namespace、owner generation、执行到第几次不是函数语义。对名称别名复用现有 registry，不为 245 个函数族逐个生成一套 struct/worker。

这里的 FieldType 指完整 SQL 类型描述，不能直接假定 protobuf FieldType 的位宽覆盖 native flags/flen/decimal、ARRAY 及 enum 元素。共享程序保留这些可观察信息；进入原 RPN 签名时做一次有检查的窄化，无法表示的签名使用明确共享扩展类型。不能在构建时截断元数据或伪造兼容类型。

`finish` 检查段引用、参数数目和结构、列号、类型以及静态栈深；冻结后不提供任意修改节点的 API。表达式 owner 真正被重写时重新编译，普通参数重绑定不重新编译。已有接口若需要可变 AST/Expression，适配层记录明确的重建点，不能用裸地址是否相同判断程序是否仍有效。

**结构验证与 SQL 求值错误分开。** 若原接口允许未选择分支中的无效 cast、regexp 或运行期参数错误不触发，构建过程只能记录该操作或一个按需错误节点，不能预先执行它。只有不依赖求值路径的结构错误才立即返回。常量折叠也必须遵守这条规则，不能因为是常量就提前调用有错误或副作用的函数。

## 3. 同一个执行循环怎样同时处理普通、短路和批量


核心采用一个显式 frame stack，避免深表达式依赖 Rust 调用栈。frame 只记录程序位置、当前参数进度、活动行集合及结果槽；不含 Host/Ordinary/RowHostScope 等按来源区分的任务类。普通 RPN 段使用现有类型化向量函数；按需调用则逐步压入所需子段。

“活动行集合”是本次子表达式应执行的逻辑行位置。它保持输入顺序和重复项，不能按物理行号排序去重；输入选择 `[7, 2, 7]` 中两个 7 是两次逻辑出现，状态函数可能需要执行两次。输出总是对应原逻辑位置。

以 `IF(flag, MD5(s), JSON_TYPE(bad_json))` 为例：先只求 `flag`；true 的逻辑行进入 MD5 子段，其余进入 JSON 子段；子段按实际需要读取对应列；最后按原位置写回结果。在约定为 lazy IF 的入口，一个始终为 true 的 batch 既不解析 `bad_json`，也不为它产生告警。CASE 逐个条件缩小尚未匹配的行集合；COALESCE 只对仍为 NULL 的行求下一个参数。AND/OR 保留三值逻辑，NULL 不能等同 false 后直接丢弃。

TiKV PB 原 builder 仅将受 `ENABLE_SHORT_CIRCUIT_EXPRESSION` 控制的 AND/OR 构造成 lazy 节点，不能自动把所有 PB IF 变成 lazy。两端使用同一循环，但 native lazy IF 与 PB eager IF 由共享签名表解析出不同 DemandRule；按各自原合同测试，不强求不同语义生成相同程序。

普通函数也可能有按需契约，例如参数转换失败或首参数为 NULL 时不求后续参数。把必要转换表示为参数子段中的共享 cast 节点，`NullStops` 逐参数更新活动行；不能统一成“所有参数先算完”。另一方面，若某个原 batch API 的合同就是完整算完左参数列，再算右参数列，则必须保留；不能借优化改变有效输入读取或副作用次数。

### 行入口不是 batch size = 1 的同义词


两者共用 `RpnExpression` 和执行循环，但调用时传入小的显式 `InvocationContract`，例如 `Scalar` / `Vector`。它由调用 API 的既有语义确定，不由 `rows.len()` 推断。该合同影响参数阶段和错误/输出提交规则，不生成另一套程序或 source profile。

调度必须保留调用者原有的 root×row 次序。增加一个执行组合同 `ColumnMajor` / `RowMajor`，由现有 executor 的 vectorizable 判定提供；内核据此运行根列表，子树求值仍全部在共享循环。当前 TiDB 对变量和某些 sequence 组合采用逐行输出列顺序，但 `SELECT RAND(), RAND()` 仍可能按列执行；把“有随机数”一律改成逐行会改变每个输出位置取得的随机流。不得自行统一这两种合同。

调用模式还必须结合入口对整棵树的能力判定：原始numeric_batch_supported会递归检查子树，IF或JSON_LENGTH会让外层算术根一并退出numeric batch。此时仅将IF子树设为行区域再恢复Vector，会错误执行原应被NULL左式跳过的外层右式。按实际回退根选择共享循环范围，不根据父函数是算术便假定仍有批量合同。

一个根内部若有状态操作或具体诊断集合变化的反例，保留必要的需求顺序，必要时使用同一循环的单行选择。多个可达错误的先后本身已被放宽，不能单独作为禁止批量掩码的理由；不能仅把叶子 RAND 改成逐行，而让父子或同组输出列被重新分组。EffectClass 至少区分无诊断/无副作用计算、语句稳定读取、可能产生诊断/失败、可变或非确定性服务；“没有写变量”不等于可任意按列调度。

例如原先按行执行时，row0 左参数产生 warning、右参数报错，row1 尚未执行；改成先算两行左参数会多产生一条 warning。这里关注的是实际多出的诊断内容/次数，不能把两个可达错误谁先返回与之混为一谈。保留经具体反例确认的Scalar/Vector需求合同，不为严格原生首错重建排序/回放框架。真正无诊断纯表达式继续用现有向量 kernel。

BENCHMARK 是共享 Repeat 控制节点：先按既有规则求次数，再在相同逻辑行和正确状态下重新执行子段 N 次，不能先把子表达式求成一个值再重复读取。零次、负次数、NULL、错误及取消均按原合同；真实有副作用的重复不能被优化掉。Repeat 不开放任意宿主 continuation。

同一次评估中不得对有副作用节点做公共子表达式合并；同值、同源码不代表同一次出现。错误后返回并清理本次 scratch，绝不重跑另一条路径。取消检查沿用原 executor 的边界，不给每个节点添加新配额系统。

### 批量必须保留的性能形态


纯算术、字符串和 JSON 等普通函数按一批参数进入现有向量 kernel；不要在外层循环每行调用完整 prepare/eval。常量可以广播，不为每行复制一份；已计算的 owned vector 直接交给下一节点。结果槽按栈生命周期复用，selection buffer 复用容量，不在每个函数重新复制整份 schema 或验证同一批行号。

IF/CASE/COALESCE 的一般化向量掩码是本设计需要新增并测试的能力，不能把当前代码的部分逻辑运算掩码当作它已经完成。第一里程碑可用同一内核的保序模式确认语义；推广纯函数路径时必须实现上述批量调度并测量，不能以“所有批量逐行跑也正确”作为性能验收。

## 4. 类型、输入、结果与生命周期


同进程采用类型化引用和 owned vector，不能将值编码成 Bytes token 再查表恢复。但这里的“类型化”不等于两端布局天然相同。

TiDB Chunk 的 Decimal 是 Go 兼容 40 字节布局，时间、duration 和字节列也各有存储编码；TiKV 使用 `ScalarValue` / `VectorValue`。因此边界允许必要的精确解码、批量 gather 和结果写回。已有 TiKV 向量可借用，TiDB 原始 Chunk 不能声称全部零拷贝，更不能用 unsafe 直接 reinterpret。

拟定 `InputColumns` 只提供“读取指定列在指定逻辑位置的值”；不提供“求值表达式”。读取已解码列时借用稳定数据；需要解码的原始列返回 owned 临时向量。只解码当前真正需求的行，解码产生的错误/告警进入本次共享上下文。若现有 TiKV 入口的存储解码时机是必须保留的合同，显式保留在该入口绑定阶段并记录差异，不能把它误称为所有入口都可以 eager 解码。

第一版优先选择能明确证明借用安全的接口：输入在执行期间不可变，已经解码的数据生命周期覆盖本次执行；可失败的 lazy raw decode 产生 owned 结果。不要引入一个会在返回借用后被下次读取重分配的缓存。先测得重复解码的实际成本，再考虑有边界的缓存。输入读取可以重用 raw bytes 的引用，但不能缓存会随会话状态变化的 SQL cast 结果。

拟定接口及所有权如下；扩展值列与标准向量采用同一拥有规则：

    enum ColumnValue<'a> {
        Borrowed { value: &'a VectorValue, rows: &'a [usize] },
        Owned(VectorValue), // compact, in requested logical order
        // Typed full-domain variants follow the same owned/borrowed contract.
    }
    trait InputColumns {
        fn read<'a>(
            &'a self, column: usize, rows: &'a [usize], ctx: &mut EvalContext
        ) -> Result<ColumnValue<'a>>;
    }

`ctx` 的借用不与返回值绑定，返回值只借不可变 input/selection，或拥有数据。TiKV 的 `LazyBatchColumn::{Raw, Decoded}` 及 `raw()/decoded()` 已公开，`RawDatumDecoder` 支持从原始 bytes 构造新向量；可抽出 stateless selected decode helper，无需可变缓存或 unsafe。现有 wire `ensure_decoded` 会按物理行去重解码；保留该入口既有阶段，避免重复 selection 产生重复解码告警。native 按需输入保留自己的时机。`RawDatumDecoder<Set>` 当前未实现，必须按实际已支持域处理，不因枚举里有 Set 就宣称可无损解码。

每个 executor 拥有 `Evaluator { program, state, scratch }`：program 冻结代码与类型；state 持节点准备缓存、跨批函数状态；scratch 仅持本次中间值和选择表。错误清理 scratch，不自动清空语句内 RNG 流、已发生副作用或应保留诊断。这个 state 不是通用 worker、任务表或 lease 池。

`EvalContext` 管 SQL mode、时区、语句时间、诊断及语句内状态；`Capabilities` 只管真实外部能力。两者每次执行借用，不复制整份 session，也不保存过期的 callback。参数按槽号从本次绑定读取；prepare 后参数变更无需重建程序。

输出先在内部构成结果；原 API 要求 atomic publication 时，成功后一次写回 caller 的 output。不要让未完成的错误 batch 留下半个新向量替换旧结果。既有允许部分输出的入口需要独立绑定测试，不凭“统一”改写其合同。

### 共同值域必须完整，不能丢失信息


| 值/语义 | 必须保留 | 具体设计 |
| --- | --- | --- |
| 整数 | 符号、unsigned 标志、溢出语义 | 值与完整 FieldType 一起传递，不经 f64 |
| 浮点 | TiDB `f64` 的非有限输入和位模式合同 | 标准 TiKV `Real = NotNan<f64>` 不够；共享值层增加有明确意义的原始 f64 承载及对应函数适配，不丢 NaN |
| Decimal | 精确系数、scale、负号、精度及舍入/溢出 | 边界使用 parts/精确表示；能无损进入原 Decimal 的走原 kernel，超域操作进入共享库内的精确兼容实现，不经字符串或 f64 中转 |
| JSON | 二进制数值/字符串信息、非法编码和解析发生时机 | 保留原始 bytes 直到被需求；共用算法但不强迫先转 serde_json 文本 |
| 文本 | 原始非 UTF-8 字节、charset/collation、PAD 规则 | bytes + FieldType；字符转换是按需表达式操作 |
| 时间 | 时区、零日期、负 duration、FSP | 值、类型和上下文完整传递，不先格式化再解析 |
| Raw / hybrid | 原类型和特定消费者的转换要求 | 明确 tagged value；不能一律当 Bytes 已完成转换 |

推荐在现有共享值层增加少量必要的精确承载类型，并通过统一 `ColumnValue` 表示借用/拥有的标准向量或扩展值列；它们是值域差异，不是一个 opaque `Any`/宿主句柄。普通兼容输入继续用现有 `VectorValue` 快路径。函数编译时确定适用签名；扩展值不在运行时“试一下标准函数、失败后回退”。这种做法的可行性必须由 NaN 和宽 Decimal 贯通测试证实，预算超出时必须回到设计，不能继续添加逐函数协议。

保留 TiDB 的存储编码及无关公共 MyDecimal API 不等于保留另一套表达式求值。迁移的是表达式真正执行的算法与调度；没有生产调用的旧表达式算法在接入完成后移除。若必要算法的抽取造成 dependency cycle，放入无 TiDB executor/session 依赖的现有基础 crate 或一个最小共享模块；所有移动仍计入总改动，不用新仓库隐藏它们。

## 5. 函数兼容与外部能力


每个既有签名必须落在三类之一：直接复用当前 TiKV 函数；同一共享函数增加明确语义参数；确有不同的算法/值域，在共享库内保留有名称、有测试的兼容实现。第三类必须列理由和行数，不能用“Native”后缀掩盖复制。没有差异的函数不得为了 AST、PB、row、batch 各产生一份实现。

参数转换、NULL 行为、短路和函数计算全部在共享库。能力接口只包含如变量读写、取 statement timestamp、随机流与函数实例状态、sequence、TiDB 特有服务调用。它不能接受可用于访问子树的 `Expr`/`NodeId` 或 `resume token`；可以接收没有遍历能力的 `FunctionInstanceId`，仅定位具体函数出现的状态。能力返回的值按正常类型约束检查；调用期间独占借用 evaluator state，重入执行使用独立 evaluator，不能清空正在运行的 scratch 后继续使用旧引用。

RAND(const seed) 需要语句内、每个表达式实例独立的流；同一节点跨批继续推进，两个同种子节点不能错误合并，动态种子按原语义逐次初始化。FunctionInstanceId 由 prepared owner 分配，clone/新语句重新绑定状态；不能以物理行号、seed 或全局递增 program token 代替这种合同。它只关联函数状态，不能请求求值或认证传输。

必须保留五个原有例外的真实计数，并验证它们只吃值：JSON_SCHEMA_VALID、TIDB_DECODE_PLAN、TIDB_DECODE_BINARY_PLAN、TIDB_ENCODE_SQL_DIGEST、VALIDATE_PASSWORD_STRENGTH。若本次目标要求连这五个也移到共享库，需要另记实际产品依赖与改动预算，不能自动把原 240/245 宣称成 245/245。

准备好的 regex/JSON path 只按常量、collation 和必要配置建立缓存；含参数的 pattern 不能错误复用。**prepare 失败不等于 SQL 应立即失败**：SDK `impl_regexp.rs:97` 有常量 regex 编译，对于原合同允许抑制死分支错误的入口，builder 必须将可失败初始化延迟到该节点首次被需求，或保存待触发的错误；对于原合同规定 prepare 失败的入口，仍在 prepare 返回错误，不能一律推迟。不可变元数据保留在 program，lazy/cache 状态放 executor-owned state。第一版不承诺 `Arc<Program>: Sync`；现有 `Box<dyn Any + Send>` 也不能直接当成线程安全共享。不要为省一次初始化引入全局缓存、lease 和代次认证。

warning/error 的相对顺序已被人类 `2979a576` 放宽，保留实际执行产生的诊断内容、数量和 error class 即可，不恢复 failure-site provenance、排序或事后重放。但这不授权多执行分支、改变变量/sequence/RAND 次数，或为了收集所有错误继续执行原本应停止的计算。

## 6. 改动量预算与停止条件


以下分配用**两仓库原始新增+删除**计量，单位千行，包含注释、空行与基础测试。各行是暂定工程预算，不能冒充已有实现测量，也不是人类批准的新上限。

| 基础工作 B（不含下文迁移/额外语义/独立删除） | 原始增删预算（K 行） |
| --- | ---: |
| RPN 构建、按需/重复控制、调用和执行组合同 | 3–5 |
| TiDB 各来源 lowering、TiKV 接入及调用者接线 | 3–5 |
| 类型化输入输出、上下文、值域承载接线 | 3–5 |
| 基础边界/共享性/需求测试、构建与设计文档 | 4–7 |
| 基础 B 合计 | **13–22** |

完整总量必须用互斥分组计算：**T = B + 2M + S + D**。M 是从原 owner 搬到共享库、逐字保留的旧算法行数；源删除和目的新增合计 2M。S 是未包含在 B/M 的兼容算法修订及对应额外测试；D 是未包含在 B/M/S 的旧执行器和重复代码单独移除。经过实质改写的迁移部分归 S，不再重复算 2M。

例如，假设必须搬 10–20K 旧算法，仅 B+2M 就是 33–62K，**还要加目前未估定的 S+D**。这个情景用于暴露风险，不是“最终会落在33–62K”的承诺。当前 SDK 的 native 表达式及 datatype 新增中包含大量真实兼容逻辑，不能把 S 默认设零。也不能只说删旧实现 20K，却漏掉把同样算法搬到 SDK 的新增。

要实现人类希望的规模，M0 必须逐文件证明：大量函数可直接复用原 TiKV 算法；必须保留的差异能以小的语义参数/局部修订实现；低层存储 API 不扩大范围；新增测试集中在真实合同。若这一清单已显示总量将重新达到 100K，就明确报告预算失败并回到架构/范围讨论，不等完成全部移植后才发现。

当前 47,904 行删除不是新的候选不可改变的下界，其中包含 datatype API 迁移等可能退出本次范围的内容；也不能只报告相对当前大分支删掉多少。每个里程碑相对原始两个 base 报告 `新增、删除、净增、新增+删除`，并分别列生产实现、测试/fixture、生成代码、文档/数据。跨仓库移动原样计入，另列 move-adjusted 统计只用于理解，不能替代总数。历史证据继续保留在历史分支，不向新候选复制数百万行报告，也不能删除它们来伪称产品代码减少。

## 7. Plan of Work / Milestones


### M0：冻结合同，建立干净产品候选


保留当前分支和全部实验记录。实现者创建独立候选，从两个原始 base 出发，登记全部原有入口和 245 个函数族；把当前已修复的 SQL 反例作为输入，而不是 wholesale cherry-pick 当前 SDK。逐类记录：可复用 TiKV kernel、需要共享语义参数、真正兼容实现、外部能力例外。第一份文件清单必须同时指出删除/保留的旧表达式实现，防止预算靠留下死代码达成。

只对真实可观察合同做兼容；历史任务协议的 token、取消恢复过程和已撤销 demo 配额不是新架构必须复制的合同。结构非法输入仍按真实公共入口的合同处理；若某项旧测试保护的是协议而不是产品语义，说明删除理由。

### M1：最小但有鉴别力的贯通


在 SDK `types/expr.rs`、`types/expr_builder.rs`、`types/expr_eval.rs` 增加最小 builder 和需求调度；扩展 `impl_control.rs` 的共享控制逻辑。在 TiDB `tidb-expr` 新增薄的 `shared_compile.rs` / `shared_input.rs`，只接真实 Expression 与真实 Chunk；TiKV 通过真实 PB builder 接相同程序。名字是提议，可按原有模块组织调整，不能另建 `local/profile/worker` 子系统。

贯通必须包含 MD5、带 NULL/溢出的 Decimal 算术、IF 保护非法 JSON/regexp/cast、NaN 输入与结果传递、两行有状态表达式和 BENCHMARK 重复求值。每条覆盖单行和 batch；`[7,2,7]`、空输入、参数重新绑定、深嵌套和输入错误也要有代表用例。示意 SQL 是 `IF(flag, MD5(s), JSON_TYPE(doc))`；SQL 层制造错误的 doc/pattern 使用列值或参数，避免优化器提前折叠让测试没有到达目标层。此外必须直接测试 typed builder 的 `IF(false, REGEXP_LIKE('x', '['), false)`：在 native lazy 合同下 prepare 成功、执行不触发错误；在要求准备期失败的签名下保留原错误时机。这个绕过优化器的常量反例专门验证 metadata 初始化问题。

验收是同一个 `RpnExpression`/执行符号处理两个入口；TiDB 没有子树 eval callback；每批不出现逐节点 token 或小程序 prepare；native lazy 死分支不读入有错误数据，PB eager/flag 仍满足自身原合同；width-one 的 Scalar/Vector 合同分别与基线一致。测试失败、代码体积明显失控或数据转换已主导成本，就在此调整设计，不能把三条简单路径通过写成全范围完成。

### M2：并行补齐适配与兼容


在 M1 接口稳定后，可并行处理两组独立工作：一组接 AST、SimpleExpr/Unistore、projection/filter/排序键/聚合参数等剩余真实入口；另一组逐函数族完成算法复用、精确值域与类型/上下文兼容。共享控制调度只有一个 owner，避免各组各写一套 demand engine。共同使用一份签名表和语义用例数据。

每次接入后，调用者保存一个 prepared program，跨行/批次复用；不能在物理行循环里 compile。所有函数都必须明确落到共享库函数或登记的外部值能力，不能通过运行时回退掩盖未覆盖。未完成阶段只作为显式测试候选，不向产品默认推广部分覆盖。

### M3：移除旧入口并完整验证


当全部真实入口覆盖后，替换产品默认路径，删除旧表达式遍历、旧算法无调用者的残余和临时双路切换；原始二进制/分支继续作对照，不重新嵌进产品源码。TiDB datatype 的其他调用者仍用原 API；不因此再扩大成整套 datatype 重构。

按下节运行完整相关测试、实际服务 SQL 和性能对照；同时重算两仓库总 diff。达标后才报告共享内核与体积目标完成。若函数算法本身更慢，需要单独优化该共享函数；不为某个 SQL 临时恢复 native 快速执行器。

## 8. Validation and Acceptance


测试由三层构成：原有共享函数测试保留不复制；少量内核测试覆盖需求、行映射和生命周期；同一份 SQL/值域反例通过两侧真实入口与原基线独立预期比较。对真正不同的原接口语义分别维护预期，不能强行让两个不同合同相等。测试数量下降不自动证明质量提升，必须把现有反例映射到新测试或列出它所保护的协议为何已不存在。

| 验收维度 | 必须观察到的结果 |
| --- | --- |
| 共享性 | 两端进入同一程序类型/执行符号；宿主没有表达式递归或 resume 回调 |
| 覆盖 | 保留固定入口和函数清单；五个例外如实列出，未缩分母 |
| 短路/转换 | native lazy 分支的坏 JSON/regexp/cast 不触发；PB eager/flag、NULL-stop、prepare 错误时机分别保留 |
| 状态 | 两行、多输出列、重复 selection 下变量/sequence/RAND 顺序及次数不变；同种子不同实例、跨批和新语句 reset 正确；BENCHMARK 重新求值 |
| 类型 | Decimal 精确 scale、NaN/无穷和负零、raw JSON、非 UTF-8、unsigned、零日期等有反例 |
| 生命周期 | 参数重绑定、上下文变化、prepare 后错误、下一批恢复不串状态；program 冻结，state/scratch 生命周期明确 |
| 性能 | 单行、32/1024 行批量、深16/宽8、NULL密度、文本/JSON/Decimal/控制/状态均与原始和当前版比较 |
| 体积 | 双仓库原始 A/D/净增/总增删，生产、测试、生成、文档分列；无未计成本 |

以 release 构建比较三份二进制：原始 native、当前保留版、新候选。保留 compile/prepare 与 steady-state eval 的独立测量，再做完整 SQL CPU、p50/p95、吞吐；不能只看被 RPC 掩盖的 SQL 延迟。另测 allocation/复制量，避免 profiler 与计时同时运行。同版重复、随机/交错顺序和多个轮次用于评估波动，不将几轮 min/max 称为置信区间。性能目标仍是达到或超过原始水平，无法由减少抽象推断已经达成；发现稳定回退必须披露并定位。

建议 M1 增加名含 `shared_kernel` 的定向测试；以下命令是里程碑完成后需存在的测试入口。执行时先列测试清单确认匹配数非零，零测试不能算通过。环境依赖沿用现有工具链记录，不在这次设计审查中新跑产品：

    # TiKV candidate root
    cargo test -p tidb_query_expr --lib shared_kernel -- --list
    cargo test -p tidb_query_expr --lib shared_kernel
    cargo test -p tidb_query_expr --lib
    cargo clippy -p tidb_query_expr --all-targets

    # TiDB candidate root
    cargo test --manifest-path rust/Cargo.toml -p tidb-expr --lib shared_kernel -- --list
    cargo test --manifest-path rust/Cargo.toml -p tidb-expr --lib shared_kernel
    make lint
    git diff --check

执行完整相关 tests/integration 前读取实际 Cargo package/target 清单，包含原入口所在的 executor 与 Unistore 测试；不要把上面两个过滤测试冒充完整验证。当前已知失败必须在原始基线独立重现并单列，不能以“既有失败”掩盖新错误。构建服务使用已有 Rust server 入口及 `--no-default-features` 的实验构建配置，避免误测 Go tidb-server。实际 SQL 驱动沿用现有报告记录的 workload 和会话参数，核对运行中二进制身份。

体积核对不依赖过滤目录后的单一数字：

    git -C <tidb-candidate> diff --numstat 364aef2bab5cc633ecb76a775ae8f36f86a6687d HEAD
    git -C <tikv-candidate> diff --numstat 548812e1ef57aef077a2062a9cc356640a6347f5 HEAD

并保存 `--name-status`、完整分组表和二进制文件条目。文档/历史数据另外报，不将它们与产品源码混为一谈，也不默默排除生成测试。

## Source Evidence


以下链接固定到本次读取的提交；它们证明已有结构和限制，不能证明本文拟新增接口已经实现。

- [原始 RPN 表示及短路节点](https://github.com/YangKeao/tikv/blob/548812e1ef57aef077a2062a9cc356640a6347f5/components/tidb_query_expr/src/types/expr.rs)、[原始向量执行与借用结果](https://github.com/YangKeao/tikv/blob/548812e1ef57aef077a2062a9cc356640a6347f5/components/tidb_query_expr/src/types/expr_eval.rs)。
- [当前 frame 与 row/batch 参数需求](https://github.com/YangKeao/tikv/blob/f7672bf980143f2938fd0f900650a4c29091e70f/components/tidb_query_expr/src/types/expr_eval.rs)、[PB builder 的短路选择](https://github.com/YangKeao/tikv/blob/f7672bf980143f2938fd0f900650a4c29091e70f/components/tidb_query_expr/src/types/expr_builder.rs)。
- [LazyBatchColumn 的 Raw/Decoded 和解码阶段](https://github.com/YangKeao/tikv/blob/f7672bf980143f2938fd0f900650a4c29091e70f/components/tidb_query_datatype/src/codec/batch/lazy_column.rs)、[共享值域](https://github.com/YangKeao/tikv/blob/f7672bf980143f2938fd0f900650a4c29091e70f/components/tidb_query_datatype/src/codec/data_type/mod.rs)。
- [TiDB 多根调用顺序](https://github.com/YangKeao/tidb/blob/ca40487b44cc2970817baa0accb0b2a4d572a10a/rust/crates/tidb-expr/src/evaluator.rs)、[随机流/sequence 能力](https://github.com/YangKeao/tidb/blob/ca40487b44cc2970817baa0accb0b2a4d572a10a/rust/crates/tidb-expr/src/context.rs)、[SimpleExpr 实际 owner](https://github.com/YangKeao/tidb/blob/ca40487b44cc2970817baa0accb0b2a4d572a10a/rust/crates/tidb-unistore/src/cophandler.rs)。
- [当前体积分解与此前方案](expression-10k-design-review.zh.md)、[已测版本报告](expression-numeric-direct-call-results.zh.md)。本次未重跑其中的产品验证，历史结果不代表新设计已通过。

## 9. Idempotence and Recovery


本次只交设计，产品候选另行实施；不修改实现负责人当前工作树。每个可运行里程碑独立提交，包含双仓库固定 revision；任何被放弃的候选保留反例与结论，撤回自己的改动，不覆盖其他人的文件。开发期可以保留显式测试双路入口，默认切换时必须移除；产品执行中不设置“失败再跑旧实现”的分支。

输入、函数错误和取消都通过普通 Result 清理本次临时值；已编译程序可在下一次合法调用复用。真实外部能力的副作用不能靠 drop 回滚，因此不得为恢复错误重放计算。无服务部署、生产迁移或线上数据修改是此设计任务的一部分。

## Surprises & Discoveries


原始 SDK 已有 ShortCircuitFnCall；共享完整内核并不要求从零造一个求值器。但当前版本一般 IF/CASE 控制仍有单行限制，不能宣称现成支持所有向量掩码。当前 `types/expr_eval.rs` 中 ControlFrame 与 OrdinaryFrame 还证明了 Scalar 与 width-one Vector 的契约差异。

两端同为 Rust，却仍有真实值域差异：`tidb-datatype/src/datum/mod.rs` 的 Real 是 f64，SDK `codec/data_type/mod.rs` 的 Real 是 NotNan。字面上“直接用 ScalarValue”会漏掉输入，必须有贯通证据。

大量 datatype 迁移服务的是低层 API 兼容，不全是共享表达式调度所必需。这个范围区分是减少改动的重要来源，但留下无人调用的旧表达式算法不属于合理保留。

regex 常量准备本身可以失败；将所有验证移到 prepare 会破坏死分支行为。因此“一次验证”只指不可变结构和绑定完整性，不指提前执行所有潜在 SQL 计算。

首个候选的 optional feature 没有隔离 workspace Cargo patch：已有 flate2、protobuf 和 cmake 依赖发生变化。默认构建需独立验证。只按 EvalType 判断 IF 分支兼容也不足以保持 FLOAT/DOUBLE 的实际结果形状；程序出口类型不能从平坦指令的最后一个 else 推断。

第三个候选的跨行JSON/正则例子显示分支分组和整批条件会改变哪个错误先返回。审查起初误加严格首错要求；重读人类2979a576及已done的task #3后已撤回并通知实现者。纯错误先后不阻止mask批量执行；共享有序调度须由具体需求、诊断内容/数量或非诊断状态合同支撑，不能以修兼容之名恢复已取消的来源顺序。

## Decision Log


Decision: 共享完整表达式执行，不接受只共享函数体、各引擎各自遍历树。
Rationale: 人类 1214f3b7 与本次 40c0e750 的目标；函数体共享不能移除重复 demand 调度。
Date/Author: 2026-11-13 / ExpressionReuseDesigner。

Decision: 按人类2979a576保留warning/error顺序放宽，不因纯错误先后增加排序或有序调度机制。
Rationale: 人类原文同时包含warning和error，且task #3已确认完成；错误身份、需求和非诊断状态合同分别验证，不能把诊断类别保留解释成恢复source ordinal。
Date/Author: 2026-11-13 / ExpressionReuseDesigner。

Decision: 产品候选以原始 SDK RPN 为底座，当前协议只提供反例与兼容知识，不整体复用。
Rationale: 保留 50K 级接入协议再清理几十行无法达到 10K 数量级。
Date/Author: 2026-11-13 / ExpressionReuseDesigner。

Decision: 函数执行使用类型化值，宿主能力只能处理已求值参数；不建立通用恢复协议。
Rationale: 同进程 Rust 的生命周期无需伪装成消息身份认证；真正边界验证继续保留。
Date/Author: 2026-11-13 / ExpressionReuseDesigner。

Decision: 明确区分表达式算法与整个 storage datatype API 迁移，不以共享内核之名扩大重构。
Rationale: 避免不必要的移动、依赖环和兼容测试复制；所有必要共享算法仍须如实计数。
Date/Author: 2026-11-13 / ExpressionReuseDesigner。

Decision: 将 10K 数量级作为可验证的目标，全部 raw A+D 公开；基础13–22K与算法迁移、语义修订、独立删除分开核算。
Rationale: 人类关心全项目维护/审阅体积；删除、移动和测试都不能隐藏。
Date/Author: 2026-11-13 / ExpressionReuseDesigner。

## Outcomes & Retrospective


本次产物是具体可实施的设计及否证条件；没有替换产品内核，没有运行新产品测试或性能测量，也没有证明全范围可压到某个固定 LOC。最有价值的下一项证据，是 M1 同时打通真正的类型差异、死分支和状态执行，而不是再清理一轮现有 host wrapper。

本方案相较上一版“一个共享内核”的方向，补齐了如何复用原始 RPN、按需子段、行/批量合同、输入借用/解码、完整值域、错误准备时机、作用域和具体行数预算。若 M1/M2 的实际兼容成本超过预算，必须明确报告冲突并修改设计，不能继续用未经验证的“薄适配”承诺完成。

本次文档核验：用 `git cat-file -e REV:PATH` 检查九个固定源码链接对应的对象存在；检查两个相对报告链接、ExecPlan 必需章节及 `git diff --cached --check`。没有产品代码改动，未运行 make lint、构建、产品测试或性能测试；这些留给实际候选的相应里程碑。

2026-11-13 checkpoint 1 更新：两个原始基线候选已形成第一份可运行提交，核心沿用原RPN循环。审查核算源码及测试818行、所有文件9585行增删；数值均只代表局部候选。下一阶段先修复当前接纳范围内的结果类型问题、清单漏项及预算记录，补默认依赖验证，再继续同循环内的按需输入/分支汇合。审查不替代完整兼容、测试或性能验收；详见相邻review文档。

2026-11-13 checkpoint 3 更新：累计候选源码及测试2007行、全部文件11830行增删，仍是局部量。动态mask/完整值汇合与finish结构冻结已有实现，类型化诊断仍缺；严格首错不属本轮要求，先前误加的限制已撤回。11项native、34项SDK及1项原始PB反例的日志终态已核对，完整make clippy未通过，固定源码归属仍依赖实现者记录；本审查未重跑产品测试。具体反例和调度修正见相邻累计review。

2026-11-13 checkpoint 4 更新：两个候选累计源码/测试2566行、全部12476行增删；15native/34SDK测试终态及JSON/IF需求修复前失败已核对。剩余具体问题为整根回退范围及非法UTF-8错误码，已交实现者跟进；本审查未运行产品测试，完整M1和全范围体积仍未证明。
