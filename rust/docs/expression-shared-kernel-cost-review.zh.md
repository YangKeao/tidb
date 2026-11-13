# 成本复审与底层算法复用：下一步设计

当前结论是：**保留一个共享表达式执行循环和原底层算法所有者；已接纳表达式的显式执行状态改造通过限定复审，下一批验证DATE的类型化完整调用链。** 下文按固定版本保留历史审查和当时范围，已结束的阶段不构成新的等待条件；当前结论、证据边界与下一批接口在末两节。完整范围、默认SQL、性能和10K目标尚未证明。

这是设计与固定源码审查，不是替代实现的验收。完整245族、默认入口、旧执行器退役和性能目标均未完成。人类要求的10K数量级仍是目标；本轮没有证明它可达，也没有证明它不可能。

## 固定输入与已核实结论

审查输入为TiDB [9af57421的成本报告](https://github.com/YangKeao/tidb/blob/9af57421b12ba2d56f041d8b3919ea2e1f2ac241/rust/docs/shared-kernel/cost-report.zh.md)，SDK为`3ad4dc75eec6af91b77f2c36f3d10ee61073b942`。产品代码仍是checkpoint7的TiDB `4958bb6959104db9b17258212c887befaa84d1d7`与该SDK提交；原始比较基线为TiDB `364aef2bab5cc633ecb76a775ae8f36f86a6687d`、SDK `548812e1ef57aef077a2062a9cc356640a6347f5`。

245个函数族包含五个例外，18组预算的求和正确。当前候选源码和测试增删5870行；包含文档、表格和构建的两仓候选全部文件增删18284行。成本报告自身只修改文档与审计材料，产品源码未变。固定报告预计剩余源码／测试36470–79760行，全文件条件合计55254–99544行；这些数值是工程估计，不能当作必需代码的下界、保证上限或获批的新预算。

预算数字在脚本中按工作组填写，文件长度与调用者数没有用于推导区间。245行中的224行仍使用通用差异检查合同；18组只各求和一次，并不等于未来实际改动片段已经逐行互斥。表格适合作为职责和风险索引，尚不是逐签名兼容证明，也不是最小迁移量证明。JSON转换、数值coercion、生命周期与执行器等交叉职责仍须在实际提交中结算。

对57个文件的哈希、54764行原文、35723个显式测试外非空行及1401个源码位置完成独立核对。三份生成表可按固定脚本逐字再现。这些结果证明输入可核查，不能把源码暴露规模视作可移动或可删除量。

## 清单中的具体缺陷

原[扫描脚本](https://github.com/YangKeao/tidb/blob/9af57421b12ba2d56f041d8b3919ea2e1f2ac241/rust/docs/shared-kernel/cost-inventory.py#L150)的普通字符串规则没有正确识别Rust反斜杠续行。`rewriter.rs:2117–2118`的字符串使后续引号配对错位，测试属性被当成字符串内容。337条所谓生产调用／引用见证中有10条实际位于显式测试模块：

| 文件（相对`rust/crates`） | 被误列的行 |
| --- | --- |
| `tidb-expr/src/rewriter.rs` | 2646、3717、3728 |
| `tidb-planner/src/logical/rewrite.rs` | 4414、4499、4559 |
| `tidb-planner/src/task.rs` | 1282、1329 |
| `tidb-session/src/lib.rs` | 2349、2367 |

例如`rewriter.rs:3717`的`plancodec::compress`属于测试`go_captured_edge_case_values`。应修正扫描器，增加“续行字符串＋测试模块＋模块后的生产代码”回归，再完整生成；不能只手删十行。这个缺陷不改变245／18的求和，也不推翻确有其他生产调用者的事实。

后续[72fedb911c修复](https://github.com/YangKeao/tidb/commit/72fedb911c2079ca821b66dae276095f3caaf84d)已独立复核：相同探针在旧扫描器失败，新版剔除测试并保留前后生产项；真实rewriter误分类也消失。三份产物完整重生成与提交逐字一致，327条正好移除已识别的10条测试见证，无新增。修复仅改五份文档／审计文件，+38/−28；两仓候选全部文件累计变为+18141/−153，增删18294，产品源码未变。此缺陷关闭，预算仍为条件估计。

## 重新选择算法所有权

现有[tidb-datatype依赖](https://github.com/YangKeao/tidb/blob/9af57421b12ba2d56f041d8b3919ea2e1f2ac241/rust/crates/tidb-datatype/Cargo.toml)已经位于表达式层之下，不依赖`tidb-expr`或SDK表达式库。它拥有完整Decimal、时间、二进制JSON与转换primitive。这里的“底层库”指不负责遍历表达式、只接受值及明确参数的库；不是把原求值器换一个名字。

依赖方向应为：TiDB入口适配器依赖共享SDK RPN，SDK RPN依赖必要的现有底层算法；TiDB存储、聚合、DDL等原调用者继续依赖原底层公共API。SDK不能反向依赖整个`tidb-expr`，因为后者已经依赖SDK；也不能通过宿主回调求值子树。

完整内核仍负责组合、参数需求、NULL规则、短路、转换执行时机、批量和函数调用。具体算法可以由它依赖的底层库拥有；共享库不必把每个算法的源码都搬进自身目录。原有SDK PB类型和算法先保留，真实语义差异由签名选择和少量策略表达，不能用native/PB名字复制没有差异的函数。

对于既有底层算法，直接依赖不会产生算法搬迁的新增与删除，但依赖清单、锁文件、类型接线、诊断适配与验证都必须计数。对于尚在`tidb-expr`内的值函数，未来可以调整真实模块归属、由旧公开路径重新导出；是否保留原源码位置由构建和维护边界决定。不能在两个crate各包含同一文件、制造两套类型或保留第二个解释器，也不能用路径技巧掩盖实际移动量。

## 下一项限定试验：Decimal与String→Real

由ExpressionReuseEngineer在独立候选继续实现，保留已审查checkpoint7作为对照。本试验先保持当前接纳范围，验证已有库复用和实际体积；不同时展开57个时间族、27个JSON族或整批生命周期能力。完整工具链检查和审计修复可独立继续。试验固定提交、完整增删和验证结果提交后复审，再决定扩展范围；这是下一批工作的顺序，不是永久冻结其他目标。

**Decimal值与运算。** 当前SDK的[ExpressionDecimal](https://github.com/YangKeao/tikv/blob/3ad4dc75eec6af91b77f2c36f3d10ee61073b942/components/tidb_query_expr/src/types/expr.rs#L28)重新保存系数、符号、存储／结果尺度和声明形状，[native_decimal.rs](https://github.com/YangKeao/tikv/blob/3ad4dc75eec6af91b77f2c36f3d10ee61073b942/components/tidb_query_expr/src/types/native_decimal.rs)又实现一套算法。原[Decimal::mul_mysql](https://github.com/YangKeao/tidb/blob/9af57421b12ba2d56f041d8b3919ea2e1f2ac241/rust/crates/tidb-datatype/src/decimal/mod.rs#L912)及加、减、除已可直接调用。下一版让共享表达式槽直接拥有或借用原Decimal类型，以别名或极薄封装保留必要的表达式API；不得每个算术节点都拆parts、格式化、再重建对象。接口需要校验原始输入时，在明确的构造／输入边界校验；不丢弃原来实际需求时才发生的输入错误。

算术分派与诊断政策留在共享循环内：原生除零／截断通过明确的上下文能力处理，PB自己的合同保持。SDK删除本候选新增的重复算法，原`tidb-datatype`算法及非表达式使用者无需移动。已识别的`native_decimal.rs`为676行，这只是候选可替换体积，实际净减须扣除新增依赖与适配。ExpressionDecimal当前公开字段是本候选新API；改用别名会改变字段初始化方式，应检查其实际使用者并调整构造／访问测试，不能称为原样保留该临时API。删除候选新增行会降低最终新增量，不能伪称已删除原始基线的旧执行代码。

**String→Real。** [native_arithmetic.rs](https://github.com/YangKeao/tikv/blob/3ad4dc75eec6af91b77f2c36f3d10ee61073b942/components/tidb_query_expr/src/types/native_arithmetic.rs)已复制前缀扫描与警告输入截取，原[datatype转换](https://github.com/YangKeao/tidb/blob/9af57421b12ba2d56f041d8b3919ea2e1f2ac241/rust/crates/tidb-datatype/src/convert.rs)提供对应公共primitive。保留共享RPN的按需转换指令、原UTF-8／trim处理与一次截断政策，只替换重复扫描和转换实现。不能直接调用包含Expression参数求值的原native函数。

这个依赖试验无需先创造新的大公共crate。先使用候选双仓目录下可解析、可固定的现有包依赖，记录完整包身份、版本、feature与传递依赖；实际构建确认SDK能独立编译。源码中已有具体构建差异：native声明Rust最低版本1.97，SDK记录nightly-2026-01-30；两边chrono-tz为0.10与0.5.1。现有验证使用专门工具链包装器，不能由逻辑无环推断任意默认工具链可用。若存在Rust版本、workspace元数据、包身份重复或发布布局问题，给出具体最小失败，再决定提取包。不得为了继续而暗中改SDK PB数值类型或引入宿主恢复协议。

验收围绕已经存在的语义与新边界，复用现有测试，不复制整套测试文件。至少检查：

- `((8/7)*1)/100`仍为`0.01142857`，中间完整系数／存储尺度／结果尺度／声明形状与原生一致；覆盖正、负、零、宽值和原有字边界反例。
- 原生Scalar、单行Vector以及已有投影路径继续由同一RPN求值，保持已有NULL需求与会话向量开关；PB429项表达式库回归继续成立。
- 字符串空值、有效前缀、尾部垃圾、指数溢出、NUL及诊断截断边界保持值、错误身份与数量。warning/error顺序按人类要求继续放宽。
- 静态依赖与实际构建都不存在`SDK → tidb-expr → SDK`环；没有逐节点Decimal序列化／重建，没有新子树求值回调。默认与启用共享feature的构建影响分别交代。
- 报告两仓相对原始基线的生产、测试、构建与全部文件新增／删除，另列相对checkpoint7的变化和新增依赖。复用已有Decimal性能基线做有针对性的release对照，检查分配和调用成本；不能仅凭删源码宣布提速。

成功标准是原行为保留、重复算法实质减少、依赖可以维护且不引入新的热路径转换。若为接通底层库又需要一套对象翻译或执行协议，即回到边界设计；不要以此为由扩展整个datatype存储重构。试验通过只证明这项复用，不把其缩减比例外推到全部245族。

## 时间、JSON与转换的后续边界

原时间／JSON表达式文件并非都能原样移入底层库。`Columns`同时引用Constant、AST系统变量和其他表达式模块；cast、string与JSON也有交叉依赖。应先复用已在datatype里的值运算，再针对确实仍在表达式层的函数剥离最小值／策略依赖。直接包含整个`context.rs`、`cast.rs`、`time_fn`或JSON目录，会把准备、调度和其他服务一并拖入，不能按“几行re-export”计价。

下一个可观察的时间切片可取DATE的零日期／FSP／模式分支，再取有DST／时区转换的CONVERT_TZ；时间库版本和时间编码也不是可直接互换的类型，不能通过存储packed往返来冒充无损表达式转换；JSON切片可取JSON_SET，覆盖typed JSON与字符串、路径解释、数字保真及错误需求。两侧JSON均有类型码与二进制payload接口，适合验证一次边界桥接，但完整字节兼容仍须证明；不得以Display／serde文本往返丢掉typed temporal、opaque或数值信息。这些切片必须通过两侧实际入口，并展示原底层算法是否真正复用。JSON_SCHEMA_VALID属于五个例外之一；其准备缓存、延迟外部引用和表达式需求混在现模块中，需单独拆开，不能把原Expression递归作为“服务能力”带回来。

公共`eval_expression_once`和datatype方法可保留，由内部转发共享实现，减少机械改写调用者。但旧表达式遍历、重复求值分派在所有调用入口切换后仍应退役。调用者文本见证不能证明删除闭包，保留公开API也不能成为保留第二个表达式内核的理由。

## 规模结论与本轮验证

原成本表刻画的是既定迁移路线，不能用它排除“复用现有底层库”的替代边界，也不能把条件低端当成不可减少的代码。新方案优先减少重复实现和实际搬迁；剩余原生语义、控制调度、输入生命周期、入口替换、退役、测试与构建仍要付出真实改动。最终是否满足10K数量级，需要新的实际diff和逐项兼容证据；不缩减245族、不省略删除和测试来达成数字。

本轮由三个独立方向完成固定源码、依赖与成本核对，重跑审计生成器并核对表格／哈希；没有构建或运行产品测试，没有新性能测量。未运行产品make lint：本次仅改设计文档；审计复现运行固定版本的`python3 rust/docs/shared-kernel/cost-inventory.py --tidb <candidate-tidb> --tikv <candidate-tikv> --output <scratch-output>`并逐字比较三份产物。文档发布前检查固定Git链接对象、相对链接、ExecPlan必需章节及`git diff --check`。本报告补充[实施计划](expression-shared-kernel-design.zh.md)，不替代[checkpoint1–7审查](expression-shared-kernel-checkpoint1-review.zh.md)。

## 限定复用试验的功能审查（2026-11-13）

固定功能版本为TiDB [f5daddcbdb](https://github.com/YangKeao/tidb/commit/f5daddcbdb39d91d20b99b012f29c1cdf4df1ae5)、SDK [d822fbf9](https://github.com/YangKeao/tikv/commit/d822fbf9fbee8e0b635974a8d38e42f9e73f067a)，报告和收据固定在TiDB [4e60d898](https://github.com/YangKeao/tidb/blob/4e60d898f89213c526f41ab4cc65a6cb80fee067/rust/docs/shared-kernel/datatype-reuse.zh.md)。本节只评价该功能版本；实现者正在补release测量、默认构建及工具链说明，不能把后续未固定结果混入本节。

**功能边界符合设计，限定源码审查未发现新的数值或参数需求问题。** ExpressionDecimal已直接导出原Decimal；编译常量克隆一次，RPN借用常量与算术参数，结果向量直接移动进原Datum。共享调度文件没有变化，原SDK PB类型和算法不变。Decimal文件删除候选桥接方法后，整个文件的Git blob与原始TiDB基线一致，原算法及其他调用者保留。String→Real复用原str_to_float和float_warning_input，保留现有表达式的一次诊断政策；复用的是值算法，没有加入旧表达式求值回调。

输出不再通过parts重建并重新校验。当前接纳的Decimal来源是finish校验的常量、验证参数的算术和这些值的IF克隆；原生Decimal列仍明确拒绝。本审查未找到当前接纳范围内绕过验证的路径，不能由此宣称所有外部输入已经支持。ExpressionDecimal的候选字段初始化API确已改变，对应测试使用原构造器／访问器，未伪称保留原样的公共字段。

非法系数用例的首次失败已核实：30项通过、1项在原Decimal::from_literal("x")的debug断言失败。最终测试捕获该构造器panic；release配置下则要求finish拒绝实际构造出的非法值。已通过的debug运行没有执行后一分支，已要求在本轮release补证中执行这个现有定向用例。不是修改算法后隐藏失败，也不能把未跑的分支记成已验证。

两边Cargo依赖和构建日志指向同一现有tidb-datatype路径包；新依赖没有回指表达式层。**SDK依赖是无条件的**，所以默认／仅PB的SDK构建现在也需要相邻TiDB检出及其构建输入，不受TiDB的shared-kernel feature保护。两个锁文件对部分传递依赖使用不同版本；同一类型所有者不等于两个工作区的完整依赖集合相同。此版本的成功检查使用现有工具链包装器；普通独立SDK检出、默认SDK工具链和发布打包尚未因此获证。应在补充报告列出真实构建合同和默认feature检查，不能把这一实验路径包装成已经完成独立分发。

独立重算本轮生产+24/−836，净减812；含构建、测试和报告的本轮+276/−862，净减586。对原始两仓基线，源码／测试+4996/−62，增删5058；全部+17564/−162，增删17726。相对上一固定版本18294的最终原始增删降低568，与本轮净减586不是同一指标。676行复制模块和数值前缀扫描确实移除，依赖／锁文件新增也已计入；不能只报676或812当作全文件缩减。

从固定Git对象独立重建5524项输入，摘要为3aae7e1a62867bd83ba9bfb1d44aadc9a58c6c884373517b00bdd9f30db09e4e，与收据匹配；八份留存日志哈希吻合。最终31项原生与429项SDK测试通过，名称集合与checkpoint7相同；限定Clippy日志完成但有警告。lint日志有末尾检查命令而无退出码尾标，成功退出来自实现者记录。共享后的原生对照主要检查接入、类型、需求与结果保真，不能描述为两份独立算术算法互证。源码摘要不覆盖全部外部依赖、工具链和构建环境，本审查没有独立重新构建。

当前结果支持继续沿用已有底层所有者这一方向，尚未完成该试验的性能／默认构建补证，也不批准扩大为整批时间／JSON迁移。补证固定后按其实际结论决定下一切片；完整M1生命周期、正式Clippy、默认表达式入口和245族覆盖仍未完成。当前17726也只是局部候选规模，不能外推为完整项目已经达到10K数量级。

## 优化测量补证复审与下一切片（2026-11-13）

固定补证为TiDB [598a313e](https://github.com/YangKeao/tidb/blob/598a313e267ea06d0ebe24a4a194fe6f351d197b/rust/docs/shared-kernel/datatype-reuse-performance.zh.md)、SDK仍为d822fbf9；相对4e60d898只新增测量、报告和审计材料，产品实现不变。**限定复用试验的承诺补证完成，保留原datatype所有者；性能目标尚未达到。下一批转为当前Decimal退化的定位与最小修正。** 上一节“等待补证”的顺序条件至此解除；这不是完整M1、默认集成或全范围验收。

独立重算192行CSV，12个耗时行和12个分配行的中位数、同轮后／前范围及每次分配值均吻合；八份原始CSV合并与Git文件逐字一致，八次终态均为0。50份本地记录哈希、两份二进制哈希和固定Git输入匹配。当前5525项摘要为e389bb4f12a93a6fc28a7590d2b611313ddb48f96b8951f45ac79605a4a6d92a，排除新增测量器后的5524项仍等于上一节的功能摘要；对照摘要等于checkpoint7原记录。现有非法Decimal用例在release实际1通过、1353过滤，关闭了debug构造panic不能证明finish拒绝的证据缺口。原功能31／429项沿用历史记录，没有冒充此次重跑。

原始基线源码／测试+5269/−62，增删5331；全部+18659/−162，增删18821。本轮对72fedb／3ad4为+1371/−862，净增509；其中生产仍+24/−836，净减812。新增273行Rust测量器已计入测试，Python、CSV、收据和报告也计入全部文件；比功能交付新增1095行补证。因此“生产简化”与“全文件变小”不能混为一谈，现有规模也不能外推为245族最终规模。

### 这些测量能说明什么

[测量器](https://github.com/YangKeao/tidb/blob/598a313e267ea06d0ebe24a4a194fe6f351d197b/rust/docs/shared-kernel/decimal-shared-bench.rs)确实调用EvaluatorSuite与SharedProjection，保留共享结果Vec的生成、释放及两侧写出同一种Chunk的成本；计时前分别比较完整Decimal字段和落地单元。没有发现使这项限定前后对照失效的测量实现问题。

64行共享小值乘法从11350.0ns变为15453.2ns，四轮后／前为1.353–1.381；同项分配从143次／23592字节降至74次／13864字节。隐藏链和宽乘的复用耗时显著下降，相消方向混合，准备请求字节全部增加。减少分配不等于降低耗时，不能用其他项目的加速掩盖这一退化。

这里有四个具体边界。第一，NoColumns启用向量表达式，width1和64都是Vector调用；没有新增Scalar性能证据。第二，prepare测的是原生AST克隆、构造、销毁与共享compile、销毁；不是所有生产调用者必付的准备成本，已有拥有AST或保存prepared program的调用者可有不同成本。第三，计时期间计数关闭，但每次分配仍经过链接包装器和原子开关分支，次数不同使该开销不必抵消；数值仅代表这个带包装器的测量程序。第四，四轮、单CPU、非隔离机器、常量和虚拟行、单输出、无LTO配置，不能推出真实列、多输出、SQL、默认生产thin-LTO或全局性能结论。

原生小值投影通过[vec_eval_decimal_arithmetic](https://github.com/YangKeao/tidb/blob/598a313e267ea06d0ebe24a4a194fe6f351d197b/rust/crates/tidb-expr/src/scalar_function.rs#L4437)进入已有DecimalValue的i128向量路径，直接生成MyDecimal单元；共享路径则经[native_arithmetic::evaluate](https://github.com/YangKeao/tikv/blob/d822fbf9fbee8e0b635974a8d38e42f9e73f067a/components/tidb_query_expr/src/types/native_arithmetic.rs#L77)逐行调用完整Decimal算法，再转为Datum和Chunk。因此原生约3784ns与共享约15453ns的差距，与共享自身前后35%–38%的变化，是两个不同问题；不能把前者全归因于调用分派。

“small_mul”也不表示结果仍在小缓冲内：18位与13位系数相乘得到30位系数，超过原Decimal的24字节内联容量。原快路径以逐位除十构造系数，已删除副本使用to_string；原除法另有副本没有的有界u128快路径。这些是可核实的源码差异和定位线索，尚不是耗时的因果证据。不能先指定“减少某个Vec”就宣称修好了退化。

### 构建限制已从未知变为实测失败

无共享feature的TiDB表达式检查通过；固定双仓与nightly-2026-08-22包装器下的验证范围明确。SDK原nightly-2026-01-30/rustc1.95实际在编译前因新增底层包要求1.97而退出101。SDK无条件依赖相邻TiDB的源码布局仍在，普通SDK默认构建和独立分发没有通过。

此失败不推翻限定的算法复用实验，却必须作为默认集成的具体待办。后续只能以可维护的共享包布局、原默认路径的feature隔离，或明确实施并验证的共同工具链合同解决；不能仅删除rust-version声明、把实验包装器当成默认支持，或复制一份算法回SDK。当前下一批数值定位可以沿用已验证双仓工具链，无须把完整打包改造混入这一切片。

### 下一批执行范围与验收

ExpressionReuseEngineer继续承担实现。沿用现有三份版本、测量源码与原始用例，不另起一套基准框架，也不先展开整批时间／JSON或完整生命周期实现。本顺序由Designer在Raft消息01f727b4授权；范围是下一批产品工作，在下述固定定位／修正结果复审后结束。原245族目标与M1其余待办不变，完整检查或独立构建修复可并行。

先在现有构建／运行脚本增加**真正不链接分配包装器的计时方式**，分配模式继续独立运行；保存两种构建的配置和身份。沿用四个用例与width1/64完整Chunk写出，补显式Scalar对照，用相同输入分别对照原生Scalar、原生Vector和共享相应调用。原EvaluatorSuite先尝试Decimal向量快路径，关闭会话开关不足以取得Scalar对照；测量器应明确逐行调用原Expression::eval并保留Chunk写出。增加一对有鉴别力的系数长度：乘积不超过24位与超过24位但仍在i128范围；已有隐藏精度、宽值和相消保留。这样回答退化是否在无包装器环境仍出现，以及是否与系数物化／批量路径有关；不把新用例计成SQL吞吐验证。

再选独立CPU采样，或在同一测量器中分开记录现有算术、RPN求值和最终Chunk写出的诊断测量。不同分段有自己的借用、分配和释放边界，不能把差值直接叫作精确独占耗时；最终验收仍以完整入口为准。若采样工具不可用，记录限制，用最小可执行分段代替，不能为取样改产品公共接口。源码热点仅作为假设；只有在对应对照可复现后，才选择一处主要开销修改。

修正优先落在现有所有者：若系数物化是主因，就优化原datatype的共用实现；若适配、重复验证或临时缓冲是主因，就在现有RPN或SharedProjection中消除已证明的重复工作。已有i128向量路径是需保留／共享的优化资产，但不能把递归vec_eval_decimal当作共享循环的回调，也不能复制DecimalValue再形成第二套算法。若要把纯值内核下移，必须让两入口实际调用同一所有者，并如实计入移动的新增和删除；本批不预先批准新增通用值协议或大规模表示迁移。

不能为优化把运行期失败移到prepare、减少warning/error身份或数量、折叠状态函数、丢弃隐藏精度／声明形状，或去掉真实输出转换。若移动Decimal校验，必须同时证明finish常量和所有实际需求的输入入口得到验证；不能只删除逐行检查。若改动保留跨调用缓冲，还须验证输入／上下文引用不残留、已有输出在后续调用后仍有效、错误后下一批可正常执行；这些检查由实际修改触发，不要求为了数值定位先实现完整状态框架。Scalar与width1 Vector继续独立验证，诊断顺序仍按人类2979a576放宽。

交付须包含可复现原因或明确未定位的剩余范围、单处修正前后完整入口的无包装器计时与分配对照，以及受影响的已有语义测试。性能缺陷保留修正前的失败证据；若选择不能稳定改善的改动，应撤回该改动并交付负结果，不增加另一层机制。当前小值64行退化不得仅以平均其他用例改善“通过”；无法消除时继续明确列为未达，不强行扩大默认接入。具体命令在两仓按已有工具链运行相应shared_kernel测试、SDK表达式库测试、make lint及diff检查；无需为仅增加测量模式重跑与其无关的全部产品测试。固定新源码、完整增删与未通过范围后再定下一切片。

本复审只读固定源码、独立重算表格／输入／日志和增删，没有重新构建、运行产品测试或重测性能。本次使用独立Python解析固定Git CSV、逐项重算统计，并读取固定对象重建输入摘要、核对本地日志hash；增删来自git diff --numstat <original-or-before> <fixed-revision>。补证另提供summarize-decimal-bench.py、count-datatype-reuse.py和verify-checkpoint-6.py供复现，不能将其列作本审查的产品测试。文档检查固定Git对象链接、相对链接、ExecPlan章节及git diff --check。

## Decimal物化修正复审与真实列输入（2026-11-13）

固定报告为TiDB [9227639f](https://github.com/YangKeao/tidb/blob/9227639ffb7d8f254cf35850b41c0c05c53000b8/rust/docs/shared-kernel/decimal-regression.zh.md)，产品为[a9326004](https://github.com/YangKeao/tidb/commit/a9326004a7388f00af36bcf20d5904b4bfdb1dd6)，SDK仍为d822fbf9。**保留原datatype的5行修正，关闭本轮25／30位乘积在所测入口下的退化；下一批推进M1的真实Decimal列输入及跨批次复用。** 这不关闭24位退化、原生向量差距、默认SDK构建或完整性能目标。上一节限定数值定位的顺序条件至此结束，未要求先消除全部微基准差距才允许继续功能贯通。

### 修正、证据和剩余差距

[原DecimalDigits::from_unsigned](https://github.com/YangKeao/tidb/blob/a9326004a7388f00af36bcf20d5904b4bfdb1dd6/rust/crates/tidb-datatype/src/decimal/mod.rs#L63)对`value >= 10^24`使用标准u128格式化及既有构造器；其余仍用原内联路径。阈值恰好是至少25位非负系数，零值及24位以内不改分支，标准格式化保留ASCII数字和规范系数。符号、存储尺度、显示尺度、声明形状都在此函数之外，算术、共享调度和PB实现没有改动。未发现这五行引入的语义问题；这次是优化同一个原所有者，没有复制Decimal或恢复旧表达式求值。

计时和分配已经分开构建。独立检查六份二进制与构建命令：三个计时程序没有`__wrap_*`符号或包装链接参数；计数程序保留相应可达包装符号和启动自检。原生Scalar明确逐行调用`Expression::eval`；共享Scalar由现有会话开关选择；Vector仍走各自实际批量入口，行数1不再冒充Scalar。所有完整入口仍包含结果容器和Chunk写出，执行前比较完整Decimal字段及最终单元。原生Vector某些用例可能走原回退，不能把所有结果都叫作i128快路径。

无包装完整入口的small_mul/64/shared四轮中位数为旧副本11388.5ns、原复用15493.0ns、修正9610.2ns；修正／原复用0.615–0.627，修正／旧副本0.842–0.846。25位边界和这两个场景的Scalar也改善。24位64行共享Vector仍比旧副本慢19.9%–21.0%；修正后的30位共享Vector仍约为原生Vector的2.53倍。隐藏链64行Vector较立即前版高0.7%–1.6%，相消64行Vector较旧副本高1.2%–4.4%。这些保留为实际观察，不用总体平均抹去，也不将非隔离机器四轮中的小变化直接认定为因果回退或统计显著。

独立采样、阈值用例和单处源码干预支持“溢出内联容量后的系数物化是本轮主要可消除开销”。算术段10992.0→5171.8ns、共享值生成13364.9→7471.9ns，Chunk写出约2075.3／2076.5ns；这些不同边界的段不能相减得到精确RPN独占时间。`arithmetic/native`始终测原datatype，包括旧副本版本，不能把它当作旧SDK副本算法。43.24%与10.87%的采样占比来自不同布局的程序，只是定位佐证，不能换算精确节省纳秒。

本轮仍是常量、虚拟行和单输出、opt3无LTO的限定测量。SDK默认工具链的实际MSRV失败、相邻仓库依赖、正式完整Clippy和生产SQL性能均未解决。仅把path依赖标为optional也不能自动解决没有相邻manifest时的Cargo解析；后续构建合同必须用真实默认／启用共享两种入口验证，不能靠新标签或降低版本声明宣布兼容。

### 独立审计和体积

独立重算1440个计时单元、360个分配单元，均唯一且覆盖完整预期组合，与15次留存原始CSV及退出0的终态一致；报告64行表格全部吻合。130份本地记录、六份二进制及统一测量器的哈希匹配。从固定Git对象重建5525项源码／构建输入摘要为`acbde542cff992e132180af1d305d62a0a7567580528e66d7b226345101d9d1d`。源码、测量器和二进制各自可核验，但构建记录只列库产物路径，没有逐库哈希或内嵌Git版本；固定源码到库／二进制的关联仍部分依赖实现者的版本记录，不能写成审查者独立重建证明。

留存终态支持89项datatype、31项共享原生、429项SDK测试通过，限定Clippy完成有警告；lint成功退出来自收据／check-exits记录，原始日志没有shell退出尾标；正式完整SDK Clippy仍未完成。本次没有重新构建或执行产品测试／性能测量。审查运行的是独立Python统计和Git输入／增删重算，以及本地文件SHA256、`nm`符号与日志检查。

本批相对598a313e／d822fbf为全部+3182/−112，增删3294、净增3070；其中生产仅+5/−0。两仓相对原始基线源码／测试+5431/−62，增删5493；全部+21729/−162，增删21891。测量器计入测试，CSV、脚本、收据和报告计入全部文件。5行产品修正不能代表全部成本；当前主要增量是测量／审计，不能从10K目标中扣除或外推全范围可达。设计审查分支的文档增删另列，不与产品候选统计混淆。

### 下一批：实际Decimal列与复用边界

ExpressionReuseEngineer继续在当前候选推进一个限定切片：**同一份已编译投影读取真实Decimal列，经过现有算术与IF，在多次输入Chunk和会话上下文调用中保真执行。** 原完整M1、参数标记重新绑定、生命周期及245族目标保持；本切片的跨Chunk复用不等于已实现SQL参数绑定。默认构建的独立修复和完整检查可并行，暂不展开整批时间／JSON或新的常量性能矩阵。

[SharedExpression的列映射](https://github.com/YangKeao/tidb/blob/a9326004a7388f00af36bcf20d5904b4bfdb1dd6/rust/crates/tidb-expr/src/shared_kernel.rs#L67)目前只存`(index, EvalType)`，编译器拒绝Decimal列，schema把非Int／Real都转成Bytes。下一批要同时修改列接纳、元信息保存、schema和NativeInput读取；只添加一个Decimal读取分支会漏掉类型合同。每个编译列绑定保留所需的完整原生FieldType，尤其声明长度、小数位、unsigned和array标记。相同物理列但元信息不同的引用不能只按EvalType合并：保留各自绑定或明确拒绝不支持组合，不能默默采用第一次遇到的类型。原有Int／Real／Bytes接纳也须保持。

读取应复用原[Row::get_datum的Decimal物化规则](https://github.com/YangKeao/tidb/blob/a9326004a7388f00af36bcf20d5904b4bfdb1dd6/rust/crates/tidb-chunk/src/row.rs#L102)：由MyDecimal恢复完整Decimal，再依原FieldType附上声明形状；小数位未指定时沿用存储值的digits_frac。裸调`from_my_decimal`会漏掉声明形状。可直接调用这个数据读取primitive，不能调用原`Expression::eval`或递归子树作为共享内核的运行时后门。Chunk输入只有自身存储域，不保证承载任意宽的表达式中间值；保持边界区分，中间算术继续用原完整Decimal，不通过存储格式往返。

只对共享循环实际需求的物理行和列物化，返回现有ExpressionVector::Decimal。不得在prepare预取输入或提前触发死分支错误；NULL、重复／乱序选择及空批次沿用现有合同。编译结果不保存输入Chunk或会话借用，调用结束后原输出必须独立有效；换输入、换上下文或一次错误后重用同一程序，不得残留上次选择、值或诊断。现有按需InputColumns接口足以作为起点，不新增HostTask、恢复协议、值句柄或第二个执行循环，也不预先增加通用缓存框架。

验收优先扩展现有shared_kernel测试，使用实际混合列：NULL、正负、零、24／25位附近和存储允许的宽值；分别验证裸列、列算术、IF选择，以及隐藏精度由列参与运算后继续消费的链。原生Scalar、width1 Vector和多行Vector分开对照完整系数、存储／显示尺度、声明形状及最终Chunk；有声明小数位与未指定小数位均覆盖。用重复乱序行、两份不同Chunk、上下文除法精度变化、一次求值失败后下一次成功验证复用；参数标记与RAND销毁重建仍单列未完成，不能由这些用例代替。

保留旧测试中“尚不支持Decimal列”的修正前证据，并补行为先失败后通过的定向用例。依据实际修改范围运行原生shared_kernel、SDK表达式库回归和make lint；若没有改原datatype算法，不为此机械重跑整套datatype性能矩阵。以现有测量器补一组真实列的完整调用对照即可，保留原六场景的值断言、既有数据和完整写出，不另造新测量框架或把测量准备移出实际必付边界后宣传提速。原24位与原生向量差距仍为性能待办，本切片不以解决它们作为功能验收条件，也不允许隐藏新的稳定退化。

固定交付应包含实际接纳边界、真实列源／结果和未通过项，两仓原始基线与本批的完整新增／删除。必要的元信息应直接服务于输入读取，不扩展成新的类型描述系统。若现有接口无法保真，交付最小反例与必要的接口差异再调整设计，不复制算法来绕开。此执行顺序依据Designer消息9355929a-73e7-47f2-b9ba-d2b5e8cd277c，只覆盖下一批，由Designer承担，固定列输入／复用结果复审后结束；完整目标和人类对诊断顺序的放宽不变。

本轮文档检查固定Git链接对象、相对链接、ExecPlan必需章节及`git diff --check`。仅修改设计文档，未另运行产品make lint；已有产品检查只按上述实现者留存记录报告。


## 真实列与跨调用复审：限定接纳成立，向量性能仍未达标

固定TiDB产品为`b4a6785049529f398b79f22b813647ab85d7e2c5`、[报告为c08c8164](https://github.com/YangKeao/tidb/blob/c08c8164f20de97fffa6debd4971f1bdf28e1687/rust/docs/shared-kernel/decimal-columns.zh.md)，SDK仍为`d822fbf9fbee8e0b635974a8d38e42f9e73f067a`；本批比较起点为TiDB `9227639ffb7d8f254cf35850b41c0c05c53000b8`及同一SDK。以下是独立固定源码、日志、哈希与统计审查，Designer未重新构建、运行产品测试或测量性能。

未发现新接纳路径的具体正确性缺陷。[列绑定](https://github.com/YangKeao/tidb/blob/b4a6785049529f398b79f22b813647ab85d7e2c5/rust/crates/tidb-expr/src/shared_kernel.rs#L175)保留原生完整FieldType；其相等比较包含完整u64 flags、长度／小数位、字符集／排序规则、array及元素元信息。同一物理列的声明冲突在**每个编译根内部**拒绝；不同投影根有独立绑定，可以用各自声明读取同列，不能将报告理解为投影级统一拒绝。SDK schema只负责分派，原声明用于实际读取。裸unsigned Decimal列被接纳，但unsigned Decimal算术仍在原接纳范围之外。

[Decimal按需读取](https://github.com/YangKeao/tidb/blob/b4a6785049529f398b79f22b813647ab85d7e2c5/rust/crates/tidb-expr/src/shared_kernel.rs#L887)直接复用原Row::get_datum，恢复声明形状及未指定尺度，不回调Expression求值、不复制Decimal算法。物化只覆盖共享循环需求的物理行，保留重复／乱序选择并独立于Chunk.sel；结果拥有完整Decimal，可在原Chunk销毁后继续持有。每次调用重新借用输入和上下文，错误后的调用重新创建临时状态。跨Chunk、会话精度及错误恢复的证据支持这条数值输入路径，不代替参数重绑定、并发执行或RAND销毁重建合同。缺列／越界测试证明按需失败和恢复，不证明无效调用输入的全部SQL错误文本一致。

固定最终源码中的两项新测试将native shared测试从31增至33；原生逐行值对照与完整字段／最终Chunk比较覆盖NULL、宽值、元信息、IF、选择和跨调用输出。SDK429项、限定库clippy及两份lint日志均有明确CHECK_EXIT=0，clippy仍有警告。修正前不接纳列的失败、错误构建目录终止exit143、E0599以及错误的unsigned算术测试预期均保留，未冒充通过。未新增datatype89项或正式完整SDK clippy的通过结论，原默认SDK工具链／相邻仓库依赖问题仍未关闭。

独立核对5525项固定Git源码输入，摘要为`8e42abb9f52923197d4fa01221dae96e6f22070636ffb3396a47101765f903ea`；72份留存记录、两个二进制、共同测量器及12份显式链接库产物的哈希吻合。本轮补齐了链接库的实际字节留存，但仍不是独立、封闭或可复现构建证明；生成与外部输入并未由所选Git扩展名完整列出。留存candidate.rs是中途测试快照，少最终输入销毁后断言的八行；最终source-snapshot与固定Git完整源码一致，不能误把中途快照当成最终构建输入。

测量器使用一份64行真实Chunk、两列Decimal、NULL及正负值，比较实际EvaluatorSuite Vector、显式逐行native Scalar及共享两种调用方式。prepare／填充在循环外，结果容器、写出和销毁在循环内；对照完整字段及Chunk后计时。16个无包装计时单元、4个独立分配单元及表格可独立重算：

| 完整调用入口 | 四轮中位ns／64行 | 分配次数／字节 |
| --- | ---: | ---: |
| 原生Vector | 6591.8 | 12／14976 |
| 原生Scalar | 21594.2 | 58／1740 |
| 共享Vector | 18846.6 | 70／23796 |
| 共享Scalar | 31561.4 | 371／96596 |

共享Vector对原生Vector的逐轮耗时比为2.430–2.874，对原生Scalar为0.858–0.887；不能以后者掩盖向量差距。只有一个准备后反复使用的列乘法场景、固定入口顺序、四轮同二进制、一次分配测量，构建为opt3／关闭LTO而非默认生产配置。旧共享版本不接纳Decimal列，因此没有其同场景前后性能比较，也没有拿旧常量数据代替。原六常量场景只保留值预检和既有数据；这不是新的完整常量性能矩阵、SQL性能或统计显著性证据。

本批TiDB生产`+34/−17`、测试及测量`+441/−4`、文档／核验`+621/−4`，构建与SDK无变化；全部`+1096/−25`，增删1121、净增1071，生产净增17。相对原始两仓基线，源码测试`+5885/−62`，增删**5947**；全部文件`+22800/−162`，增删**22962**。这些统计包含核验和测量文件，仍只代表部分实现，不证明完整10K目标已达到。相对产品提交的最终报告只改变文档／数据。

据此保留本次输入实现，task #38的限定执行顺序到本次固定复审结束。完整兼容、性能、默认集成及旧执行器退役尚未完成；不改变人类对warning和error顺序的放宽。

## 下一批：先证明参数重绑定的真实合同

下一批依据Designer消息edc94a9c-4cb3-448f-b3df-9af3146143e8，由Designer负责限定范围，在固定合同／原型复审后结束；独立构建修复可并行。任务是用现有Decimal算术与IF中的一个参数，证明下一步接入应该采用的最小边界，不先展开通用参数框架。首选方向是共享循环按需访问参数数据叶子，适配层从当前context取值，转换与表达式组合仍属于共享内核。此方向尚不是已经证实的完整接口。

必须先解释当前原生语义与固定RPN的差异。[Constant::get_type](https://github.com/YangKeao/tidb/blob/b4a6785049529f398b79f22b813647ab85d7e2c5/rust/crates/tidb-expr/src/constant.rs#L122)从当前param_value推导类型，saved ret_type不构成稳定签名证明；literal_value拒绝参数，保存的规划值也不能作运行值。裸参数返回当前Datum，类型化调用按请求类型转换，同时含ParamMarker和DeferredExpr时还有独立的优先级／结果转换合同。原生bare marker现有测试允许跨NULL、Int、UInt、Real、Bytes和Decimal改变值域，不能凭固定RPN输出类型宣称全部支持。[原生溢出文本](https://github.com/YangKeao/tidb/blob/b4a6785049529f398b79f22b813647ab85d7e2c5/rust/crates/tidb-expr/src/scalar_function.rs#L331)还会读取当前参数，当前共享编译时保存的文本不足以覆盖这种行为。

交付一张合同表和最小原生对照／测试原型，逐项区分：裸读与类型化用法；Scalar与Vector的读取／转换次数；声明签名和当前推导类型；NULL、未绑定及DeferredExpr；当前上下文与诊断内容／数量。原生Vector常量可能每批读取一次再广播，Scalar可能逐行读取，不能自动按同一行循环产生告警。

可观察验收包括：编译无上下文且零参数读取，规划占位值刻意不同于运行值；同一冻结程序在声明支持的签名内跨值／NULL／上下文重用，完整Decimal与旧输出不失真；IF未选择分支中的缺失参数、空批次均不读取，选中才产生原生未绑定错误；一次错误后有效调用恢复；溢出文本反映当前值。另提供一次推导类型改变和marker同时带DeferredExpr的原生反例，不能用只测Decimal常量宣布完成动态参数。

在接纳产品参数前，必须确定实际执行owner怎样保持签名或在表达式产生副作用前完成兼容重编译；普通同签名重绑定不重新编译。不能全局提前读取缺失参数、冻结规划快照、在已执行SETVAR／RAND后才报“不支持类型”或退回旧求值器。若实际owner尚无所需合同，有限交付应是最小反例、必要接口差异和明确的后续实现决定，参数接纳保持关闭；不为解决不明合同引入新registry、类型描述系统、HostTask或值句柄。该验证完成后重新确定接入，不将未证明的任意类型同程序复用写成承诺。

优先复用constant.rs和shared_kernel.rs的既有测试与数据。按第8节运行实际改动所需的定向回归；若有产品改动，运行受影响库及make lint，保留失败到通过证据、固定提交和全部增删。无需再展开常量性能研究来替代这个合同决定；既有向量成本、默认构建及RAND生命周期缺口继续明确保留。本轮设计文档本身只做链接、ExecPlan格式和diff检查，未运行产品验证。


## 参数合同复审：有限反例有效，产品接纳仍未改变

本轮固定TiDB测试源码`3034ea26f3780299b6931fb241bed6cd7cbc9874`、[合同报告bfde2f35](https://github.com/YangKeao/tidb/blob/bfde2f35cf3fb98cdb16ce9461dbb0702bf5c3b5/rust/docs/shared-kernel/parameter-contract.zh.md)，SDK仍为`d822fbf9fbee8e0b635974a8d38e42f9e73f067a`；本批起点为c08c8164及同一SDK。仅新增测试和文档；共享编译器继续拒绝参数，原型没有进入产品路径。独立复核未发现合同、证据或计数矛盾，接受这次有限交付，task #39的顺序条件结束。Designer未另运行产品构建、测试或性能。

[测试原型](https://github.com/YangKeao/tidb/blob/3034ea26f3780299b6931fb241bed6cd7cbc9874/rust/crates/tidb-expr/src/shared_kernel.rs#L1973)将上下文接到测试专用Decimal槽，复用同一个IF／除法RPN，18次调用对照完整Decimal及NULL；不同上下文销毁后旧输出仍有效。死分支、空批次、未绑定错误1105及错误后复用有明确断言。原生Int参数可转换为Decimal，而槽拒绝；溢出1690虽一致，原生文本含当前81位值、槽文本只有编译时列名。这两个反例有效说明：类型化取值／转换和诊断操作数缺接口，不能把参数直接当已定型列。原型IF使整个根使用逐行回退，原生期望值也是Scalar；三种共享调用方式通过，仍不证明纯算术Vector每批转换告警次数已兼容。

实际SELECT缓存依据当前参数类型选择计划，Decimal精度和尺度只允许既有计划覆盖更窄值；NULL、整数和其他类别分别匹配，也可能命中另一个旧项，不能一律描述为每次重编译。命中后原物理树重绑值并清算术缓存；本次参数由PreparedSelectExecution的Arc切片持有，StmtContext读取。该路径尚未持有共享RPN，不覆盖所有prepared语句。报告关于绑定失败的结论应理解为“淘汰失败缓存项，保护后续新EXECUTE”：既有[CachedSelectPlan::bind](https://github.com/YangKeao/tidb/blob/3034ea26f3780299b6931fb241bed6cd7cbc9874/rust/crates/tidb-executor/src/driver/planner_bridge.rs#L3348)只在成功时递增generation，不能据此证明此前已发出的全部执行引用都被撤销。这是既有生命周期限制，本批没有修改它；下一步不以generation作为跨执行共享RPN缓存的唯一凭据。

5525项最终Git输入摘要独立重建为`75da1f299d6aac7d248e0de52d25d6fb2a1bbd179c6a5c68accd94bf5e19d485`，13份原始记录哈希吻合。早期f30d2377摘要亦核对；最终仅补精确未绑定错误断言，35项共享原生和8项prepared有不同的最终日志及明确CHECK_EXIT=0。真实session缓存定向测试有1通过的终态；lint日志有命令记录，退出0、rustfmt和diff检查由receipt报告，未提供单独原始退出尾行。零项筛选、两处测试API编译错误、底层RPN非空断言失败均保留，未算作成功。没有新增SDK429、完整clippy或性能通过结论，也不是独立可复现构建证明。

本批生产／构建／SDK均零改动；测试`+279/−0`，文档与核验`+222/−2`，全部`+501/−2`，增删503、净增499。相对原始基线，源码测试`+6164/−62`、增删**6226**；全部`+23299/−162`、增删**23461**。统计包含反例与审计材料，仍非完整功能或10K可达性证明。此前列元信息冲突的表述已改为每个编译根内部。

## 下一批：Decimal类型化参数的最小产品边界

依据Designer消息cda358b9-f0e8-4a2b-8788-a3534e5e60ac，下一批从上述固定版本实施，Designer负责范围，固定实现复审后结束；独立构建修复可并行。现在已有足够证据实施一个限定入口，不再要求先设计完整参数框架。共享范围及原生函数所有者保持，下一步不改默认执行入口、不新增跨EXECUTE共享程序缓存。

**编译边界。** 在native shared_kernel.rs的迭代编译器中，给已经解析为有符号Decimal算术的操作数传递“请求Decimal入口”的信息；只在该位置接纳无DeferredExpr的ParamMarker，并保留原静态签名接纳条件。新增一个明确的共享参数叶子，例如`ParameterDecimal { order, field_type }`，这里是拟新增名字。该节点的输出始终是Decimal／NULL，`finish`验证它的声明输出；原参数当前值的类型不必等于该输出类型。原IF可以包住这样的算术，但裸参数结果、IF直接返回参数、其他请求域和DeferredExpr继续整根编译拒绝。不把这些拒绝误称为全参数支持，也不读取保存的规划值来决定运行结果。

**取值与转换。** 在已有InputColumns能力上增加按需取原生owned Datum的方法，例如`parameter_value(order)`；NativeInput只委托本次上下文的数据读取。一次求值中的参数绑定不可变，由真实执行拥有的参数切片提供；不保存跨调用借用。共享RPN执行参数叶子后，以当前Datum推导原生字段并调用值转换primitive。把Constant::eval_typed_as_on_row中仅处理Decimal转换／补尺度的值级逻辑放到已有tidb-datatype所有者，由原Constant和共享叶子共同调用；复用既有to_decimal_with_context、错误类型和ConversionContext，适配层只提供flags、位置及typed warning sink。不能依赖整个tidb-expr、回调Constant::eval、复制算法、统一改成ConvertTo(声明列形状)，或将真实原生转换错误改成新“不支持类型”错误。

这允许Int、String等当前值按原类型化Decimal合同转换，无需凭当前值重新定RPN出口；NULL、转换错误及补尺度行为也遵守同一原生合同。读取／转换由原有调度发生：纯算术Vector的常量参数每次节点激活转换一次后广播，Scalar及原生整根回退按原调用粒度转换；IF、NULL guard和空批次边界照旧。参数绑定跨调用可以改变，但调用期间不允许变成另一份参数。共享程序只承担已解析签名，不借此声明任意raw输出域复用。

**动态溢出文本。** 将NativeArithmetic的固定overflow字符串扩为小型文本片段／参数位置模板，沿用原静态列名、字面量、嵌套算术及括号形式。不支持原生详细格式的子树（例如IF）仍整体采用原生通用错误文本，不递归执行该子树。只在该算术实际溢出时读取当前参数原始Datum并按原值格式化；native numeric_expression_text本来也会在错误路径读取marker。利用一次调用绑定不可变的合同，无需为诊断另建跨行值缓存或生命周期协议。这里不重新转换参数，也不提前读取死分支；String原文本不能替换成转换后的Decimal。诊断取值失败或格式化不能表示该值时沿用原生通用文本，仍返回本次算术1690，不让诊断读取错误替代它。输入方法是只读数据访问，测试上下文也须遵守调用期间绑定不变；带副作用或每次返回不同值的读取器不满足这个合同。若实现选择保留已经需求的raw值，必须限定到本次调用并证明行／重复求值间无旧值；它不是必需的新框架。

**所有权与接入。** 本批只扩展现有显式SharedProjection／SharedExpression入口。由一次执行从已绑定、已解析签名的表达式快照编译程序，随后跨该执行的Chunk使用；下一次EXECUTE先重新构造，暂不把RPN塞进既有计划缓存。未来实际挂接点可以是物理ProjectionExec创建EvaluatorProgram的位置，但当前默认workers共享Arc程序，而RPN内有非Sync的`Box<dyn Any + Send>`；本批不为参数叶子扩大并发容器或默认后端改造，也不能通过unsafe补Sync。已有计划选择／generation路径只作为来源合同，不作为本轮完整缓存生命周期已完成的证据。

验收沿用现有shared_kernel／prepared测试，先保留Int转换和当前值文本修正前失败，再验证类型化参数的真实成功。至少覆盖Decimal／Int／NULL、会产生截断告警的String、strict／warning两种上下文、补尺度及完整Decimal字段；纯算术用真正native EvaluatorSuite Vector对照，共享Scalar、单行Vector和多行Vector分开验证，IF逐行回退另列。检查转换告警内容／数量、raw与converted不同的溢出文本、嵌套算术、死分支／空批次、未绑定错误、错误后复用及旧输出独立。原型之外的声明接纳拒绝也须保留。warning与error顺序仍按人类要求放宽，不新增排序机制。

产品变更后运行现有35项共享原生、8项prepared、受影响datatype定向回归及SDK表达式库回归，并执行make lint、格式和diff检查；用报告中已有cargo包装器与rust/工作目录，精确命令及退出结果随固定提交交付。若转换primitive移动改变原Constant路径，以原测试／独立预期确认未改变其语义，不能用两边同时改后的相同结果作为唯一依据。只按变化补必要测量，不新开广泛常量性能研究，也不从参数接入成功推断向量成本已解决。完整原始增删、实际接纳边界、默认SDK／完整clippy／RAND生命周期／245族余项继续报告。

本轮仅更新设计文档，固定Git链接对象、相对链接、ExecPlan格式及`git diff --check`、`git diff --cached --check`验证后提交；未对产品另跑lint或测试。


## 类型化参数首版复审：记录吻合，两处语义缺陷待修正

本轮固定TiDB源码`be800b9126653ff96620cb78e51ef350b1c352a0`、[报告bb94be0e](https://github.com/YangKeao/tidb/blob/bb94be0e55bd8c590829a2ff12b8ea25b9b9a7aa/rust/docs/shared-kernel/typed-parameters.zh.md)，SDK为`3c9b12cfb80e2928896a28def1b2a3b8b7dc3517`；本批起点bfde2f35／d822fbf。原Constant和共享循环确实共用既有datatype中的类型化Decimal转换／补尺度及原值格式化，未回调旧表达式求值。完整Decimal、当前推导NULL、按需读取及真实Vector测试已有实质进展，但**首版尚不通过限定正确性复审**：独立源码审查发现以下两处可达差异。Designer未另运行产品构建、测试或性能；下面反例需由实现者保留实际失败与修正证据。

**静态源类型接纳过宽。** [编译器](https://github.com/YangKeao/tidb/blob/be800b9126653ff96620cb78e51ef350b1c352a0/rust/crates/tidb-expr/src/shared_kernel.rs#L348)对无Deferred参数跳过原`admitted(t)`检查。父函数返回Decimal且`numeric_operand_domain()`成功，并不证明参数原本请求Decimal：原生先按静态源类型请求入口，再做目标转换。静态VarString参数、当前String值`2.5junk`参与Decimal加法，就是被接纳而未测试的情况。在warning上下文的Scalar入口，原生String入口后的转换产生1292；共享直接ToDecimal产生1265。真正三行Vector中两侧的截断码都是1265，但原生先广播String，再逐行转换，产生三条告警；新参数叶子只转换一次。新测试84个组合全部声明参数为Decimal，不能覆盖这个差异。

最小修正是恢复原有静态有符号、非array Decimal操作数边界，而非增加另一套cast实现。当前raw绑定仍可以是Int／String／NULL等，由已证明的Decimal类型化入口处理；不能把静态源类型限制误做成运行时值家族限制。静态类型缺失、其他源域、unsigned／array等继续编译拒绝。新增负例先证明首版误接纳和真实原生源入口差异，再验证修正后拒绝。

**诊断读取失败替换了实际算术错误。** [SDK溢出文本](https://github.com/YangKeao/tikv/blob/3c9b12cfb80e2928896a28def1b2a3b8b7dc3517/components/tidb_query_expr/src/types/native_arithmetic.rs#L131)中的`input.parameter(order)?`会传播读取错误。可达反例是隐藏Decimal物理列100，列的virtual_expr为缺失参数0，与80位全9的Decimal相乘。实际[Column求值](https://github.com/YangKeao/tidb/blob/be800b9126653ff96620cb78e51ef350b1c352a0/rust/crates/tidb-expr/src/column.rs#L229)只读物理值，运算正常触发溢出；仅诊断格式化递归访问virtual_expr。原生通过`.ok()?`放弃详细文本，返回通用1690；共享却改成未绑定1105。这不要求参数值变化或有副作用的读取器。诊断中参数不一定已被实际求值需求过。

应在诊断模板渲染中将读取失败与不可格式化统一退回原通用1690；实际参数节点的读取失败仍保留1105。不能用编译拒绝原已接纳隐藏列、提前读取全部参数或吞掉实际参数错误来修复。该反例与诊断顺序无关，人类对warning和error顺序的放宽保持不变。

固定版本的39项共享原生、8项prepared、89项Decimal、7项转换及429项SDK测试均有最终原始成功终态和CHECK_EXIT=0；89与7重叠两项，不相加伪装独立总数。限定native表达式Clippy完成且有警告，不能替代正式完整SDK Clippy。根lint／rustfmt／diff的退出0来自receipt，留存lint没有独立退出尾行；报告中的错误edition格式检查失败未在26份记录内留存，不能视为本次独立核实的失败证据。

独立重建5525项固定Git输入与最终snapshot逐项相同，摘要`ca2ccab18525009f7d8b84eb511e282622560be652446b54a93ba14f005e86af`；26份原始记录哈希吻合。早期snapshot仅scalar_function.rs和shared_kernel.rs与最终不同，六份pre-guard记录单列；最终六份检查各有不同日志，未用旧版本通过结果替代最终签名检查。原转换primitive的迁移源码及独立预期值支持已测路径，但两侧共同调用该primitive也不构成全部转换兼容的独立oracle。本轮没有新性能或封闭可复现构建证明。

本批生产`+299/−73`、净增226；测试`+346/−2`、文档核验`+275/−0`、构建0；全部`+920/−75`，增删995、净增845。相对原始两仓基线，源码测试`+6791/−119`、增删**6910**；全部`+24201/−219`、增删**24420**。全部数字独立重算一致，最终报告提交仅改变文档与receipt。这仍是局部候选，不证明完整245族、旧入口退役或10K目标。

下一步按Designer消息2fa948e4-1b07-4328-a9d0-fc273af26b26在task #40内完成上述两处修正；范围由Designer负责，到修正固定版本复审结束。先保留两个真实反例，再用现有包装器及rust/工作目录运行受影响shared_kernel／prepared／SDK回归、make lint、格式和diff，更新固定输入、报告及两套基线的全部增删。若datatype源码未改，既有89／7项记录按原版本保留，不机械重跑无关测试或性能矩阵。显式程序按每执行持有仍是调用合同，尚未安装SQL执行器生命周期管理；默认SDK工具链／相邻布局、完整Clippy、RAND生命周期和原生向量性能差距继续保留。独立默认构建修复可以并行，不把本次有限修正当作完整M1。

本轮只改设计和复审文档，检查固定Git对象链接、相对链接、ExecPlan结构及diff；未另执行产品lint或测试。


## 参数修正版复审：两处缺陷关闭，限定实现成立

固定TiDB源码`184f525870dd5e519928217e119bc3d4a7c6dd2b`、[修正报告dece8367](https://github.com/YangKeao/tidb/blob/dece836798231e5a59e484340652569191866f24/rust/docs/shared-kernel/typed-parameters.zh.md)，SDK为`05130ce8dde0299a93ad86cf93132087d7171ff9`。本次独立源码与证据复审未发现新的限定范围缺陷，接受两处修正，task #40的2fa948e4顺序条件结束；不把该结论扩大为全参数、默认SQL、并发或跨执行生命周期完成。Designer没有另运行产品构建、测试或性能。

编译器移除了参数免检例外，恢复原静态有符号、非array NewDecimal条件；当前raw值为Int／String／NULL的原类型化Decimal转换不受此限制。[SDK诊断](https://github.com/YangKeao/tikv/blob/05130ce8dde0299a93ad86cf93132087d7171ff9/components/tidb_query_expr/src/types/native_arithmetic.rs#L131)改为读取失败或不可格式化均放弃详细文本，保持本次算术通用1690；真正参数节点仍传播读取失败，既有用例保留1105。没有拒绝原已接纳的隐藏物理列、提前读参数或新增cast框架。

两项新测试分别调用原`Expression::eval`和真正`EvaluatorSuite::run`，避免以单行Vector代替Scalar。静态String原生三行Scalar为三条1292，Vector为三条1265；修正前共享分别为三条1265和一条1265，最终断言其编译拒绝，另外拒绝unsigned／array／LongLong声明并接受原Decimal声明。隐藏列用例在最终成功运行中覆盖Scalar及Vector的通用1690全文，原实际参数缺失1105回归仍通过。

证据比单独保留失败日志更强：独立将`before-fixed-tests.patch`精确应用到首版生产源码，重建115增0删的文件摘要`fc546246581892276f162d81f089c6f942a65a104aa8a1125fd76345a7214f0c`，与记录一致；生产前缀逐字是旧实现，整个测试模块逐字是最终版本。恢复的旧SDK算术文件摘要也吻合；这两文件正是所选源码输入中的全部修正差异。`before-fixed.log`明确0通过／2失败、exit101。静态String失败日志执行了两种入口；隐藏诊断失败在首个Vector断言停止，不能说该失败日志也执行了后面的Scalar。最终定向及整组通过日志才覆盖隐藏诊断的两种入口。错误重建导致的编译失败和早期Vector错误码预期均单列，未当作语义失败证据。

最终5525项固定Git输入与snapshot逐项相同，摘要`477ecf74ebb3927fb69fe834399ae7dead009cfad8a39103037ee51638753283`；15份记录哈希吻合。41项共享、8项prepared、429项SDK及限定Clippy都有最终原始终态和CHECK_EXIT=0，Clippy有警告。datatype未变，89／7项只保留首版历史结果，未宣称重跑。lint退出0来自receipt，原日志无退出尾行；format-diff记录是完成摘要而非逐命令输出。核验支持留存输入与结果一致，不是独立或封闭可复现构建证明。

| 统计范围 | 生产新增／删除 | 测试新增／删除 | 文档核验新增／删除 | 全部新增／删除 |
| --- | ---: | ---: | ---: | ---: |
| 相对首版的修正 | 9／4 | 115／0 | 169／5 | 293／9 |
| task40完整最终变更 | 303／72 | 461／2 | 439／0 | 1203／74 |

构建文件本次未改。修正增删302、净增284；task40全部增删1277、净增1129。相对原始两仓基线，源码测试`+6911/−119`、增删**7030**，全部`+24485/−219`、增删**24704**，独立重算一致。最终树计数不等于把相互重叠的每批差分机械相加。原生向量成本、完整245族与10K目标仍未证明。

## 下一批：使独立SDK表达式库按声明构建

依据Designer消息dd2cd5b5-7996-4421-a1e3-8bc10531c7b2，下一批转入构建接线，由Designer负责范围，到固定构建结果复审结束。当前声明为Jan30的SDK工具链与原datatype MSRV1.97不兼容；SDK还无条件依赖相邻TiDB路径。外部Aug22包装器证明特定研发环境可运行表达式库，却没有关闭这两个问题。先处理这条已有基础边界，再增加新函数或默认执行器层。

首选实现是在SDK `components/tidb_query_expr/Cargo.toml`固定引用原TiDB仓库的datatype Git revision，原算法和存储API保持原所有者；TiDB `rust/Cargo.toml`以相同Git来源的path patch使用本地同一包。先验证Cargo发现嵌套workspace和继承metadata／构建脚本／资源，再分别核对实际依赖图：独立SDK选中固定Git的datatype；TiDB共享构建内native与SDK两条路径汇合到同一本地datatype及其error、hack、kvcache、mysql叶子闭包。检查两次构建对应源码版本，不要求独立Git图和本地path图的package ID相同。若TiDB共享构建内仍同时出现两份不同datatype包，Datum、ConversionContext和warning trait就不能直接跨边界；不能通过额外转换层掩盖，也不能复制整个库或仅用optional feature掩盖路径解析。

SDK `rust-toolchain.toml`优先与已测Aug22对齐，不降低原库MSRV。该声明改变具有全SDK影响，本批验收只覆盖表达式包及TiDB消费端；其余workspace／CI未验证项要明确保留。构建需要的C/C++、CMake条件应有可复建的文档或最小仓库配置，移除对未提交个人绝对路径包装器的依赖。不得用隐藏RUSTC、全局source patch或相邻目录软链接把指定工具链／固定Git检验伪装成已通过。

在没有相邻TiDB的独立SDK检出里运行普通`cargo metadata --format-version 1 --locked`、`cargo tree -p tidb_query_expr -e normal`及`cargo test -p tidb_query_expr --lib --locked -- --nocapture`，记录实际cargo／rustc版本，预期429项通过。TiDB rust/内以其声明工具链运行metadata／tree及现有41项shared_kernel、8项prepared回归。下载缓存可以复用，依赖来源和锁文件需可审查；它不是空缓存或全系统可复现性承诺。相关lint／格式／diff及原始全部增删随固定提交交付，正式完整SDK Clippy仍按实际运行结果报告。

如果嵌套包发现、单一类型身份或工具链升级暴露具体障碍，保留最小失败和确切依赖链，调整声明接线，不回退到算法副本或扩大为整套执行器重写。该批不接默认SQL后端、worker、跨EXECUTE缓存，不增加新性能矩阵；仓库声明工具链的表达式测试通过也不能冒称整个TiKV或全部CI通过。完整共享内核目标及诊断顺序放宽不变。

本轮仅更新两份设计文档，验证链接对象、相对链接、ExecPlan结构和diff；没有运行新的产品检查。


## 独立SDK构建首版复审：包级构建成立，两处接线遗漏待修正

本轮固定TiDB源码`542d31a7978f7bbee4a6a13dc89cf790b7ef5f7f`、[报告710740e1](https://github.com/YangKeao/tidb/blob/710740e193a31c4434ce6519e551ffec97ebcf3c/rust/docs/shared-kernel/independent-sdk-build.zh.md)，SDK为`41c56427b68ce7518415d22f6d14e9f410b8c2e0`；本批起点dece8367／05130ce8。独立SDK表达式库按声明工具链构建的证据成立，但本轮接线尚未通过完整限定复审：改变子模块默认行为及新增Git来源时，遗漏了两个原有入口。两处都是构建配置修正，不需要改变共享求值内核。

SDK固定引用原TiDB的`8abb688c80bd48de688bfc792bc735682404e4dd`，没有算法副本。独立SDK图中datatype及error／hack／kvcache／mysql各只有一个Git包；TiDB图中native与SDK指向同一本地datatype package ID，其余四个原叶子也一致。两个独立图的Git与path身份不要求字面相同。172个原所有者文件与资源逐项同于固定Git、本地源码及Cargo检出；该清单覆盖五包及aggregate-tests.rs，不代表全部workspace继承项、外部测试输入和第三方依赖的封闭清单。清单之外的collation_key_vectors.tsv和json_vectors.hex两份集成测试fixture也已单独比较一致；它们不构成本轮已运行所有datatype集成测试的证据。实际编译补足了嵌套workspace、构建脚本及内含排序规则资源可用的证据，没有新增expr反向依赖。 首次Git失败的具体错误是`invalid submodule URL: relative URL without a base`，不能据此断言enterprise访问凭据被拒绝；后续日志确实记录跳过该子模块。

**子模块初始化回归。** 新`.gitmodules`把enterprise的默认更新策略设为`none`，让Cargo下载datatype时跳过无关的SSH仓库，目的合理。但原[Makefile初始化](https://github.com/YangKeao/tidb/blob/710740e193a31c4434ce6519e551ffec97ebcf3c/Makefile#L274)仍为`git submodule init && git submodule update --force`，`--force`并不选择checkout策略。因此干净企业版检出即便拥有访问权也会跳过该目录；enterprise-server／enterprise-docker随后运行enterprise-prepare时缺少generate目录。报告的手动`--checkout`指令不能代替修正既有入口。最小修正是在原初始化目标显式指定`--checkout`，保留普通Cargo的默认跳过行为；Designer已用Git2.55.0的本地临时子模块独立复现：原命令exit0、显示Skipping且无payload；加`--checkout`后同一gitlink文件出现，日志SHA256为`eb9cb57ff4276bebc005a3d4a726bf8b5a204d062b50e533f2efc38ea1ea6d50`。这也符合[Git对子模块更新策略的说明](https://git-scm.com/docs/git-submodule)。没有访问enterprise代码或运行企业版构建。

**新增来源未进入现有检查配置。** SDK的[deny.toml](https://github.com/YangKeao/tikv/blob/41c56427b68ce7518415d22f6d14e9f410b8c2e0/deny.toml)设置`unknown-git = "deny"`，允许的GitHub所有者只有tikv／pingcap／rust-lang，缺少此次明确选择的YangKeao/tidb。正式make clippy尚停在工具安装，未运行到这一来源检查，不能把未完成结果视为仅剩等待。应在同一sources节为该仓库加入精确URL的allow-git，保留unknown-git及其余来源策略，不扩大到整个账号。保留sources检查前后结果；其他workspace、许可证或安全公告问题按实际输出分别记录，不能用放宽全局检查来关闭本次遗漏。这一判断来自固定配置、[cargo-deny来源规则](https://embarkstudios.github.io/cargo-deny/checks/sources/cfg.html)与脚本所用0.18.9版本源码；Designer没有安装或运行cargo-deny，不能将静态判定写成已执行的失败结果。

独立普通Cargo构建及SDK429项、TiDB41项共享和8项prepared通过的实际范围是表达式库与列明配置。声明的Aug22工具链替代Jan30，原1.95与MSRV1.97不兼容有实跑记录；旧路径与MSRV失败日志没有独立命令／退出头，不提升为完整失败复现包；未降低MSRV或隐式替换RUSTC。全SDK工具链、Docker与macOS工作流声明受到影响，不能从这些包级结果推断全部TiKV／CI兼容。标准缓存目录和下载缓存复用也不是全冷、封闭或独立机器复现。正式make clippy由运行者在deny安装达到120秒时主动SIGTERM终止，退出−15；与限定Clippy完成有警告及静态检查通过分别报告。

独立核对33份原始记录哈希及5525项固定Git源码记录，摘要为`5842cd9ad21356f089cc144a6fbdd520ec155dc345257b46a9afc33d4b0e2697`；独立SDK的1421项记录也与固定提交及复审当次干净检出一致；随后实现者开始修改deny.toml，后续工作树状态不替代这次固定版本证据。429项两次、native41／8、限定Clippy及lint均有成功终态和退出0证据。5525／1421是指定文件后缀的记录，不是整个构建输入闭包；初始target为空来自实现者报告，没有单独留存目录快照；固定SDK重复日志缺少命令及工作目录头，结合前次命令记录、固定输入比较与当次检出状态认定，不把这些重复日志单独称作全封闭重建。正式make clippy在主SDK检出执行而非独立目录，工具安装中断不影响上述包级测试事实，也未证明完整检查成功。

本批生产Rust和测试逻辑增删均为0；构建`+14/−3`，文档证据`+364/−1`，全部`+378/−4`，增删382、净增374。相对原始基线，源码测试`+6911/−119`、增删**7030**；全部`+24861/−221`、增删**25082**。以上包含构建、工具链、锁文件、报告和审计脚本，独立按固定Git对象重算；没有把不改算法理解成零项目成本。

下一步依据Designer消息5caea732-21bd-4178-bada-bfde751ba924和227514d5-18e3-48d0-9ae5-4e5dfd2f34c2，在task #41内修正两处接线；范围由Designer负责，到修正固定版本复审结束。保留最小失败到成功证据、lint／格式／diff和完整计数。若编译输入及依赖声明未变，已有429／41／8结果按对应版本复用，明确说明未重跑；不把这一配置补丁扩成新函数、默认SQL入口或性能工作。后续完整M1、生命周期、真实列向量成本和245族体积仍未完成。

Designer本轮只独立检查固定源码、记录与统计，另用本地临时Git子模块复现初始化行为；未重跑产品构建、测试或性能。设计文档检查固定链接、相对链接、ExecPlan格式与diff后发布。


## 独立SDK构建修正版复审：两处入口缺口关闭

固定TiDB修正源码`98a1535fce668c1920685663988bb1ba568dd64b`、[报告8f62b406](https://github.com/YangKeao/tidb/blob/8f62b406e32f1edfdd1f40c6bb40e5094602bf54/rust/docs/shared-kernel/independent-sdk-build.zh.md)，SDK为`780ab3e776f47d634ac8963437baf8477a7b2df7`。两处配置修正通过限定复审；5caea732／227514d5的修正顺序条件结束。这里接受的是独立表达式库构建及原入口配置，未扩大到全部SDK、企业版产品构建或默认SQL后端。

TiDB现有init-submodule只增加显式`--checkout`，普通Cargo仍遵守`.gitmodules`的默认跳过。新增55行回归脚本从实际Makefile提取配方，用真实子模块名／路径、默认none和本地临时仓库执行make。旧配方退出0但缺少文件，要求checkout的断言退出1；修正配方取出预期文件与gitlink，重复调用成功。SDK仅增加指定仓库URL的allow-git，unknown-git、unknown-registry与原allow-org不变。cargo-deny0.18.9实际sources检查从五个原TiDB包的source-not-allowed、退出8，变为零错误／警告、退出0；首次来源拒绝日志、结果及驱动脚本误写诊断名字的更正说明单列保留；原错误脚本／traceback未单独留存，不将这部分说明当作已重建的失败执行。

复审收据的22份记录哈希吻合，5525项固定记录摘要重建为`8a5a8877cfcf510c2bd98413bd9108ee9a6cc5171505d39935b7ae580f73c6aa`。相比首版仅SDK deny.toml这一策略文件改变，其余5524项、产品Rust、依赖声明及锁文件相同；Makefile不在该后缀清单内，另以固定补丁和回归日志核对。因此沿用原SDK429、native41／8产品结果有明确依据，本次没有重跑这些测试。根lint退出0由提交收据报告，原始日志没有独立退出尾行；sources通过不代表advisories／licenses／bans、完整Clippy或其他SDK包通过。初版缓存／日志来源的限定继续保留，Designer也未另运行产品构建、测试或性能。

本次修正构建`+2/−1`、文档与核验`+227/−3`，全部`+229/−4`、增删233；task #41完整批次构建`+16/−4`、文档证据`+588/−1`，全部`+604/−5`、增删609。生产Rust与产品测试逻辑零变动；本地Git回归脚本计入文档核验而未从总量排除。相对原始基线，源码测试仍**7030**；全部`+25087/−222`、增删**25309**。统计独立从固定Git对象重算。


## 下一批：限定真实列完整调用的成本归因与一次优化

依据Designer消息963621fa-39e5-4af5-a731-b43af945267f，从上述修正版启动独立任务。范围由Designer负责，到固定结果复审结束；先解释已有`column_mul/64`的完整调用差距，再决定一处有测量依据的优化。历史2.43–2.87倍差距来自旧固定版本，不直接当作新版本测量；不承诺本批消除整个差距，也不把默认SQL、全函数迁移或生命周期改造并入。

复用`rust/docs/shared-kernel/decimal-shared-bench.rs`的`DECIMAL_BENCH_COLUMNS=1`入口及已有构建／运行脚本。它比较同一真实Decimal Chunk、64个位置、NULL和四条完整调用路径：native Scalar、native Vector、shared Scalar、shared Vector；编译程序不计入重复求值计时，输出仍写回Chunk。保留所有完整数值及输出对照，不删除输入解码或输出写回以美化结果。先在当前固定源码生成无分配包装的基线计时程序与独立分配计数程序；运行器仍用同一场景、四轮交替前后版本。分配程序的计时不参与速度结论。

在固定TiDB的rust/目录中，把bench_out设为本任务证据目录的绝对路径，运行以下现有命令；候选版本改用after-time／after-alloc输出目录。rustup which返回仓库声明工具链的实际程序路径，避免构建器对符号链接执行resolve后错误指向rustup代理。记录所选版本及task #41的系统构建前提，不恢复个人包装器。这里是独立优化测量程序：opt-level 3、LTO关闭、16个codegen units，不冒充产品thin-LTO构建。

    bench_cargo=$(rustup which cargo)
    bench_rustc=$(rustup which rustc)
    python3 docs/shared-kernel/build-decimal-bench.py --repo .. --cargo "$bench_cargo" --rustc "$bench_rustc" --output "$bench_out/before-time"
    python3 docs/shared-kernel/build-decimal-bench.py --repo .. --cargo "$bench_cargo" --rustc "$bench_rustc" --output "$bench_out/before-alloc" --allocations

随后从固定TiDB根目录运行，两个版本使用同一测量程序源码；运行器记录CPU绑定并交替前后版本，二进制内部四条路径顺序仍固定。清除继承的DECIMAL_BENCH_ONLY筛选，完整结果必须包含四条路径。分配程序另计100次调用，其计数不由计时iterations决定。尚无候选时可把after也指向before程序取得基线重复结果，明确没有优化后版本。

    unset DECIMAL_BENCH_ONLY
    python3 rust/docs/shared-kernel/run-decimal-bench.py --columns --before "$bench_out/before-time/decimal-bench" --after "$bench_out/after-time/decimal-bench" --output "$bench_out/timing" --iterations 20000 --rounds 4
    python3 rust/docs/shared-kernel/run-decimal-bench.py --columns --before "$bench_out/before-alloc/decimal-bench" --after "$bench_out/after-alloc/decimal-bench" --output "$bench_out/allocations" --allocations --iterations 1 --rounds 1

原生Vector使用紧凑i128表示处理支持的算术树，并直接写出MyDecimal；共享路径会读取原Decimal、运行RPN并物化结果。源码没有证明此原生路径使用SIMD。下一步先用一个精确过滤的调用做采样，或在采样不可用时做最少的阶段测量，分别定位输入读取／物化、原datatype算术、RPN调度和输出写回。配置、固定库产物及符号可见性随证据记录；阶段成本仅作归因，不能把分别测出的微操作相加替代完整调用速度。

定向采样只用无包装基线程序，在同一CPU上分别设置DECIMAL_BENCH_COLUMNS=1、DECIMAL_BENCH_ONLY=column_mul/64/reuse/shared或column_mul/64/reuse/native_vector、DECIMAL_BENCH_ITERATIONS=200000。它仍执行一次准备及值断言；采样所得时间不用于验收。若权限或优化符号妨碍归因，允许对同一场景作一次测量程序内的求值／写出分段；若仍未定位安全改动，本批以归因未决及有预算的下一步决策结束，明确未接受性能优化，不继续扩大采样范围。

可先检验一个已有线索：该场景58个非NULL乘积需要30位系数，超出Decimal的24字节内联存储；历史shared／native分配调用相差58，可能与结果字符串物化及堆存储有关。这是待验证假设，不是耗时原因已证实。不要仅凭Vec、循环或动态接口出现在源码里便新增scratch池、公共缓存或整套新值表示。

若测量定位到可用小改动解决的成本，在既有原datatype所有者或共享RPN／边界内只实施一项优化，再保留修正前后完整调用结果及分配差异。不能复制`DecimalValue`／算术、回调旧递归表达式求值、扩大编译接纳，或丢弃隐藏精度／storage_scale／result_scale／声明类型。若主要差距来自原生紧凑整树表示且没有合理小改动，有限交付是具体归因及有源码位置、接口和增删预算的原所有者primitive复用方案；停止本批编码，不将未验证方案算作优化完成。

如修改原tidb-datatype源码，需先固定该原所有者版本，将SDK Git revision和锁文件同步到同一版本，再核对独立SDK图与本地patch图各自唯一datatype及源码对应；不能让独立SDK仍测8abb旧实现而本地已测新算法。仅改共享RPN或native输入／输出适配时，不无故改datatype依赖。保留本轮刚闭合的普通Cargo声明，不恢复未提交的cargo-tidb包装器。

验收保持原场景及完整调用：无包装计时比较实际的before／after源码与库产物，展示各轮和剩余native差距；分配程序另列调用次数与字节数。Scalar／Vector不得合并，选择顺序／重复位置、NULL、Decimal隐藏精度和类型形状、诊断内容／数量及旧输出独立性仍由现有测试保护。若更改产品路径，运行受影响41项shared、8项prepared、429项SDK与涉及的datatype定向测试，以及make lint、格式和diff；只按确切语义变更补回归，不新造大测试矩阵。若只有测量／设计交付，核对测量断言、固定输入／二进制及相关脚本即可，既有产品测试按版本沿用。全部源码、测试、构建、数据及报告均纳入本批和原始基线计数。

只有无插桩完整调用中shared Vector中位数下降、配对轮次方向一致且超过所观察的波动，其他三条路径也无可重复退化，才称为本场景的性能改善。分配下降本身不够；噪声内结果如实记为未决，剩余原生差距单列，不宣称整体性能目标完成。


## 真实列成本复审：接受有限归因，性能差距未关闭

固定[报告e18f0b5c](https://github.com/YangKeao/tidb/blob/e18f0b5c282c728192b0c502d2e151c405807b0d/rust/docs/shared-kernel/decimal-column-cost.zh.md)、测量源码1100e956dfb0d17e52949681fd7125b4c5dff48f，产品仍为TiDB8f62b406／SDK780ab3e。该批没有产品优化；最终before／after使用同一个无分配包装程序，只是重复标签。四轮各两个标签得到32个完整调用单元；标签顺序交替，进程内native Vector、native Scalar、shared Vector、shared Scalar四路顺序固定，不能称作路径随机化。

共享Vector中位19.173微秒、原生Vector6.590微秒，同次比例2.861–2.995、中位2.905；共享Scalar30.974微秒、原生Scalar22.455微秒。独立计数程序中共享Vector70次分配、原生12次。保留真实列、NULL、完整字段和Chunk输出断言、读取及临时值释放；没有拿算术切片替代完整调用或把同版本波动称为改善。机器未隔离、无LTO及固定路径顺序限制仍在；这不是默认SQL吞吐证明。

归因支持表示转换是值得改动的边界，但不能精确分摊全部差距。共享入口的采样直接落在MyDecimal解码、Datum物化、ASCII系数转换及算术等函数；选定libc分配／释放符号的Self约3.29%，未包含全部分配相关符号、间接内存及缓存成本，不能把它说成总分配开销的上界。输入9.383、原算术4.653、求值16.935、写回1.934微秒是所有权和工作量不同的诊断切片，不可精确相加。输入切片持有两列owned Datum集合，共享输入使用类型化载体，容器和生命周期不同；切片比原生完整调用慢也不能直接推导某个共享阶段占比。58个非NULL乘积与58次30字节系数分配的对应支持来源解释，不能由此承诺去掉分配就能消除差距。

两条入口各200万次采样的实际cycles:u／199Hz／DWARF8192、CPU23及记录保留；未把后来示例20万次写成实际运行。实现者说明其运行早于固定设计发布；profile记录缺少绝对开始时间，因此本次不能独立证明该先后关系或所有编译／采样均未与计时重叠。优化内联和栈回溯不完整，报告只用Self比例；可识别RPN符号0.27%不是总调度成本的上界。没有为了补齐调用树继续扩大研究。

独立核对123份记录哈希、5525项固定Git输入及三份程序与12份库文件对应，输入摘要为f22eeef2f758e6b0978c9b42f4815f24e1185aa11eda4a613280f2be41456408；仅测量程序增加73行，其余5524项及产品库不变。初始采样程序使用旧测量源码，最终计时／分配程序使用同一新源码，分别保留正确哈希；不能把三份二进制说成同一文件。对留存perf.data重新生成Self报告也与记录一致；这只是重新读取已有采样，没有重跑产品。lint、格式和diff有退出0记录，原429／41／8明确沿用，未重跑。所选后缀清单和12个库文件不是全部构建输入闭包。

本批生产／构建0、测量测试+73、文档数据核验+906，全部+979／−0。相对原始两仓基线，源码测试7103行增删；全部+26066／−222，共26288行增删。独立重算一致，未从项目总量中排除测量程序或证据材料。

消息bcf4e55a-d075-43e5-ad0f-79efa6b437fe接受本次有限交付并结束963621fa的本轮范围；原始产品测试未重跑，完整共享表达式内核、默认SQL和全SDK检查仍未完成。

拟议810行增删是尚未实现的工程估算。五段源码窗口可定位起点，不能证明调用点和兼容成本已闭合：86行DecimalValue还承载加减、整数构造、尺度对齐和写出；SDK逐操作数valid_decimal仍依赖ASCII系数；参数、常量、原生输出和测试构造都会受ExpressionDecimal类型替换影响。直接根输出还必须处理原Chunk的有损宽值存储合同及错误时的输出边界。接受归因不等于接受810行足够覆盖这些改动，也不等于完整245族与10K目标成立。


## 下一批：在原所有者内验证紧凑载体贯通共享循环与根输出

依据Designer消息bcf4e55a-d075-43e5-ad0f-79efa6b437fe，下一项是一个端到端有限原型，不把提取primitive、接入载体和根输出拆成必须逐次等待审阅的任务。沿用当前接纳范围、原RPN循环及同一column_mul/64场景；先完成可审查的所有者提取提交，再在同一任务中贯通按需输入、紧凑乘法和根写出。目标是验证这些边界能否降低完整调用成本，不预先承诺追平原生或达成810行预算。

原tidb-datatype拥有紧凑primitive及一个不暴露字段的算术值封装，后者只容纳紧凑精确值或原Decimal宽值。原生DecimalValue的加、减、乘、整数构造、尺度对齐和写出调用一并迁移到原所有者，删除旧方法体；native调用者把自身Integer枚举转换成i128，不让datatype反向依赖tidb-expr；不能只移动乘法后留下第二份同一算法。原Decimal快路径与原生向量路径委托到相同primitive；各自原有适用条件及回退仍保留，不把原生无告警helper直接当作完整SQL算术。

共享封装的紧凑运算先只接已证明等价的乘法域；加、减、除、不能精确容纳、尺度不适用及任何紧凑miss均调用原Decimal方法。fallback只处理已经求出的操作数，不重新读取输入或回调原表达式求值；输出告警、溢出文本与除零策略仍由现有SDK边界负责。首个紧凑域可保守限制存储尺度不超过38、乘法结果显示尺度不超过30，并保留原Decimal快路径的无符号幅值／乘积i128限制；signed i128::MIN不能因为原生helper能算就绕过原路径回退。保持紧凑值不是引入新的SQL接纳范围。原Decimal公开存储API及宽值实现保持，避免为了紧凑路径改写所有数字字符串访问接口。

封装构造与转换必须按来源分别定义。原Decimal进入封装时完整保留符号、系数、storage_scale、result_scale及declared_shape；有符号零或任何不能无损表示的情况保留原宽值。MyDecimal输入复用原Row读值的规范化和FieldType规则，含未指定小数位，不直接把原始存储符号和fraction当成Datum语义。算术新值按原规则清除声明形状、规范化零；宽值退化仍保留原带符号零结果。拟议接口为from_decimal(Decimal)、from_column_decimal(&MyDecimal, &FieldType)、is_valid、is_zero、原算术同签名方法、into_decimal及to_chunk_my_decimal_lossy；字段私有，不公开未检查的raw-parts构造。原Decimal也增加共享形状规则的from_column_decimal供Row调用，从而不把Row判断复制进适配器。SDK的valid_decimal改为调用原所有者的有效性方法：紧凑字段由私有构造器保证，宽值按原检查验证；不为验证紧凑值重新格式化ASCII，也不无依据删掉接纳检查。常量、参数转换、IF选值和原owned输出都要接到这一封装，不另造值句柄协议。

SharedProjection保留现有返回Vec<Vec<Datum>>的evaluate入口，新增明确的直接Chunk输出入口；SharedExpression原owned入口同样保持。两者共用已有程序组准备及一次RpnExpression::eval_group调用，输出转换在共同求值返回之后选择，不增加一个执行循环或每节点host回调。直接入口先验证目标列布局，再完成所有根的求值；求值失败时目标Chunk不变，已发生的告警和其他副作用不回滚。成功后才替换目标结果，保持选择的重复位置与顺序、Scalar／Vector模式及空选择行为。输出由调用者独立持有，可跨调用复用，但不得与输入或仍需保留的旧结果共享列；不同Chunk对象也可能共享列。现有Chunk::columns_share_identity可检查可见的输入／输出及输出内别名，不能据此宣称检测了所有外部持有者；接口文档保留独立输出前提，并在求值前拒绝可检测别名。空选择成功时清空目标；失败不清空。测量直接入口不得在外层再重复reset。

紧凑结果由原所有者直接转换为MyDecimal；不能精确转换时，走原Decimal::to_chunk_my_decimal_lossy，与当前Chunk::append_datum一致。不得因改成返回Result的严格转换而新增输出溢出错误。根转换不应先构造全部Vec<Datum>再写出，否则仍保留本次要验证的主要物化边界；需要的结果暂存由既有RPN返回值承担，不增加跨调用缓存或scratch池。非Decimal根沿用原结果适配，不能静默缩小原已支持投影。

最少验证覆盖紧凑与宽值混合、i128边界及乘积越界、尺度超过紧凑域、隐藏精度后续消费、原有符号零、声明／未指定尺度、加减除回退、参数与常量以及IF死分支。原native整数构造和cast调用不得漏迁。直接输出与owned后append对照完整字段和Chunk读回；补多根后根报错时旧目标不变、选择重复乱序、空输入和下一批重用的用例。诊断顺序仍按人类要求放宽，只保留实际需求、诊断内容与数量、非诊断状态合同。复用原有测试和反例，只为新边界增加必要断言。

若prototype产品改动完成，按两侧仓库声明工具链运行现有shared、prepared和SDK表达式库测试，补受影响原datatype／chunk及native向量定向检查，并运行make lint、格式及diff。原datatype先固定提交，SDK Git revision和锁文件同步，再核对独立SDK与TiDB本地patch图的唯一类型身份和源码对应。不得只测本地path图而让独立SDK仍引用旧原所有者。

验证在TiDB rust/执行以下已有测试入口，再按改动的原生向量用例加必要筛选；预期既有41项shared、8项prepared及对应原类型测试保持通过，新增边界用例计数单列。SDK根目录执行最后一条。两侧均使用声明工具链和task #41记录的系统前提；根make lint在TiDB根目录执行。

    cargo test -p tidb-expr --features shared-kernel --lib shared_kernel_ --locked -- --nocapture
    cargo test -p tidb-expr --features shared-kernel --lib constant::tests::prepared_ --locked -- --nocapture
    cargo test -p tidb-datatype --lib decimal --locked -- --nocapture
    cargo test -p tidb-chunk --lib decimal --locked -- --nocapture
    cargo test -p tidb_query_expr --lib --locked -- --nocapture

沿用前章的完整调用构建／运行命令与计时、分配分离；仅为新的根输出边界补一个明确命名的调用模式，不扩函数／宽度矩阵。原owned调用在固定before／after都要保留，新直写调用单列；原生向量primitive迁移后的路线也要对照旧原生，不能通过拖慢比较对象缩小比值。共用同一fixture、值断言和计时循环，若需编译开关才能让同一测量源码链接旧版本，记录开关和各二进制对应接口；在CSV及收据中分别标明owned与direct_chunk，不能把两种API重命名成同一条性能结果。直接入口包括其唯一一次reset、所有读取求值、写出及临时值释放。完整调用的稳定改善与分配变化分别报告；旧API即使仍需物化也不能静默删除或宣称同幅加速。

810行保留为原提案估算，首个可编译版本就按全部受影响调用点更新预测与实际新增／删除，计入移动两端、测试、构建、测量及报告。它不是已验证上限，也不是省略兼容项的理由；若必须扩大为通用数值重写、第二求值器或跨执行状态框架，本批在该边界停止扩展，交付固定原型与明确缺口供复审。原型若不能维持语义或无稳定完整调用收益，保留负结果，不把代码存在写成优化完成。完整共享内核与10K数量级目标继续保留，本批不接默认SQL或扩大函数覆盖。


该范围由Designer负责，到原型固定结果复审结束。此次设计发布仅改两份文档，核对固定链接、ExecPlan及diff；没有另跑产品构建、测试、性能或新采样。


## 紧凑载体与直接输出复审：限定原型成立，空间代价仍在

固定TiDB产品／测量源码`005b431ce6709682a3a31c8acf98b79ebcb4e6a5`、[最终报告ea0f4448](https://github.com/YangKeao/tidb/blob/ea0f44480dc9d5fc593b4bf477a30b1e7d660e34/rust/docs/shared-kernel/compact-output.zh.md)，SDK为`5582f0695404a40d3ae714aabef81438ef6c4172`，原datatype Git来源为`de8a34cd3c231abe53180fe61ee0da30120817cd`。本轮固定源码与留存证据复审未发现已接纳路径上的新缺陷，接受task #43有限原型；bcf4e55a的本批范围结束。没有默认SQL接入、跨执行状态或完整245族／10K目标完成的结论。Designer未另运行产品构建、测试或性能。

[原datatype紧凑primitive及完整载体](https://github.com/YangKeao/tidb/blob/ea0f44480dc9d5fc593b4bf477a30b1e7d660e34/rust/crates/tidb-datatype/src/decimal/compact.rs)承接native加减乘、整数和cast原调用点，旧方法体删除。原Decimal的加法和乘法也调用它，保留先解析无符号幅值的原范围；原生紧凑路径可表达的负i128::MIN没有扩大原Decimal快路径。SDK只引用该所有者。共享直接乘法限于已验证的尺度和系数范围；宽值、负零及miss先用已求值操作数上的原Decimal计算。实现将原算法返回值通过`From<Decimal>`重新尝试紧凑表示，而非前版设计所写的一律保留Wide；这是结果表示转换，没有再次做算术或求值子树，保留警告、完整系数／两种尺度及声明形状，但会产生转换工作。以实际实现更新这一描述，不把回退免费化。

两种输出接口在[with_results](https://github.com/YangKeao/tidb/blob/ea0f44480dc9d5fc593b4bf477a30b1e7d660e34/rust/crates/tidb-expr/src/shared_kernel.rs#L912)共用一次原eval_group。直写在任何效果前检查输出列数、物理布局及可见列别名；全部根求值及可失败的结果适配／有效性检查成功后才reset一次并写出。普通返回错误保留旧目标，先前副作用不回滚；这不承诺panic或内存耗尽后的回滚。空选择清空，重复／乱序选择保留，Decimal宽值继续原lossy存储转换，非Decimal沿用原适配。调用者仍须独立持有目标列；可见身份检查不能发现所有外部持有者，物理尺寸检查也不是全逻辑FieldType一致性认证。已有成功Decimal、成功RAND及混合根失败测试结合共同执行代码，支持本次有限边界，不要求另造输出协议或诊断排序。

四轮旧owned向量完整调用中位由18.940942降至10.101256微秒，配对降低46.40%–47.24%；owned Scalar由30.954323降至20.938207微秒。新增direct_chunk中位8.209513微秒，比当轮native向量仍高22.18%–30.95%，不得冒充旧owned接口的同口径替代。native向量本轮中位6.590180→6.293847微秒，但配对变化−11.46%至+2.71%，不能把全部小幅差异归因于提取。独立分配记录显示owned向量70→71次、23796→36540请求字节；direct为12次、29616字节，native为12次、14976字节。分配次数下降与内存流量改善是不同结论，本轮没有证明后者，也没有测峰值驻留内存。

最终同一测量源码哈希`137a60a35530a9cd3003f45e0cd052a0c642b8f511fa115a107c03fc72a00720`，before关闭新增API cfg、after开启；旧四条完整调用及值断言不变。四个最终二进制与副本哈希一致，同一版本的计时和分配程序各自连接相同12份显式rlib／rmeta。最终after计时为`7f9d41f4…`，记录选中它而非初轮`b1bfbed0…`。初轮四份chunk／expr库摘要不一致、被覆盖的原库不能逐字复核，原因未确定；它们的失败封存与36／9个中间单元保留，最终表只采用同库重链接后的完整重测。12份直接库和选定文件后缀都不是整个构建闭包。CPU23未隔离、内部路线顺序固定、opt3无LTO／16 codegen units仍限制外推；这不是SQL吞吐或普遍性能证明。

独立重建5526项固定后缀输入与快照逐项一致，摘要`f275a3f3bdfe838031520dec69bd6ba59532c89d9cc1948505dbe96c13bd3a8f`；142份收据所列记录哈希、36个最终计时／9个分配单元及整张表重算吻合。报告提交仅变更六份文档／数据／核验文件，选定输入与产品源码提交一致。两张Cargo图各自只有一个datatype及error／hack／kvcache／mysql原包，native和SDK在本地图指向同一datatype；独立图是固定Git身份，不要求两个图的ID字面相同。172份五包文件／资源及1421项独立SDK输入与固定版本相同；本次172清单未另包含aggregate-tests.rs，不能把它等同于先前同数量清单或整个继承构建闭包。工具链版本证据沿用既有Aug22记录，本批命令路径相符，没有新版本输出或独立重构建证明。

最终43项共享、8项prepared、90项Decimal、10项Chunk、1项native向量及429项独立SDK均有成功终态和退出0。限定Clippy完成有警告、根lint和最终格式／diff均有成功尾行。正式SDK make clippy在独立检出实跑9.43秒退出2，cargo-deny报19个advisory错误，bans／sources成功、许可证三警告，未进入workspace Clippy；这次是实际失败而非超时。旧SDK780ab3的advisory复查观测到完全相同的19个RUSTSEC ID，但没有固定公告库revision／hash，不能提升为两次数据库逐字相同或依赖安全已通过。三项开发期失败是构造器类型、非法字面值debug断言和预期Chunk虚拟行数的测试构造修正，不是旧产品缺陷的失败到成功证明。新数值对照共享原算法，独立预期来自既有datatype测试和原实现源码核对，不宣称两套实现相等就证明全面兼容。

本批生产`+463/−200`、测试及测量Rust`+285/−2`、构建`+6/−6`、文档／数据／核验`+848/−2`，全部`+1602/−210`，增删**1812**、净增1392；首个未加最终材料的版本835新增／210删除，确为1045增删。相对原始两仓基线，源码测试`+7706/−295`、增删**8001**；全部`+27634/−398`、增删**28032**。均为固定端点重新核算，包含移动两端和材料，不是历史批次简单相加。810初估已被超出；源码测试8001也只是部分函数候选，不能外推全部245族和旧路径退役仍能满足10K目标。


## 下一批：只消除载体重复形状的布局成本，再结束本轮Decimal定位

当前`DecimalArithmeticValue`在外层保存`declared_shape`，而`Wide(Decimal)`内部已有形状字段：普通Decimal转来的Wide可能两处都有形状，列转来的Wide则目前由外层补充形状。两种情况都预留了两处存储；所有列元素还按最大枚举变体预留空间，小值也承担这一代价，但不能把全部字节增长归因于这一字段。依据Designer消息180bc037-4be1-4876-9067-99db3372e6db，下一项只在原datatype私有载体内试验消除重复存储，由Designer负责范围，到一次固定布局结果复审结束。保持现有CompactDecimal不变，避免把形状加入native每个紧凑元素；首选将形状放进`ArithmeticValue::Compact { value, declared_shape }`，让Wide直接携带原Decimal及其已有形状。不要用Box给每个宽值增加分配，也不用packed布局、unsafe、列外形状表、第二套数值算法或新的scratch框架。Rust枚举实际大小必须在目标工具链实测，不能凭字段相加声称已经减小。

调整`From<Decimal>`、`from_column`、`into_decimal`和紧凑乘法结果构造：紧凑输入保留原声明形状，Wide列直接用原`Decimal::from_column`保留元信息；Wide返回原值，紧凑返回原数值并恢复该变体形状；算术结果依原规则清除形状。加减除及乘法miss仍先用原算法，再按当前转换规则选择结果表示，字段继续私有。该批不扩大紧凑算术范围，不改诊断／错误需求、两种输出合同或原native算术行为。

先记录`size_of::<Decimal>()`、`size_of::<CompactDecimal>()`、`size_of::<DecimalArithmeticValue>()`及其Option包装的前后大小／对齐，并记录实际嵌入它的ExpressionScalar及RpnStackNode大小，结合现有分配记录确认实际列空间变化。已有完整值／列形状／宽值回退／负零／i128边界和直写错误／别名检查保留，仅在缺少Wide列带声明形状的独立预期时补一项定向用例。原owner先固定提交、SDK Git pin与锁文件同步；本地patch图与独立SDK图分别核对单一类型身份及相同所有者内容。

复用本轮`column_mul/64`和原测量脚本，before与after都已支持direct_chunk，因而两侧均开启相同cfg，保留五条路线及原完整值／Chunk断言。运行器当前只在after允许direct行，before多出该行会被精确集合／数量断言拒绝；需作最小显式扩展以验证两侧各五行，同时保留旧产品只有四路的运行模式。建议新增明确的`--before-direct-chunk`开关（当前尚无此参数），保留`--direct-chunk`控制after；新开关要求同时有`--columns`和`--before`，每版本验证精确五条路线。完成该最小扩展后，用`run-decimal-bench.py --before <before> --after <after> --columns --direct-chunk --before-direct-chunk --rounds 4 --iterations 20000 --output <directory>`；分配另加`--allocations`，改为`--rounds 1 --iterations 100`。预期40个计时／10个分配单元；配套重算程序也须明确校验两侧direct并计算其同接口变化，不能直接用硬编码36／9的历史汇总程序处理新结果，历史材料仍须可原样重算。两个版本构建均给`build-decimal-bench.py`传`--direct-chunk`，其余cargo／rustc／repo／output参数沿用固定构建流程。每版本计时和分配二进制都核对对应12份固定库，先封存库副本再运行；四轮完整计时与独立分配计数即可，不增加场景矩阵或重复采样。报告owned和direct各自的前后耗时、请求字节及次数，保留native对照，不能用变慢的参照路线美化比例。

在TiDB rust/运行现有`cargo test -p tidb-expr --features shared-kernel --lib shared_kernel_ --locked -- --nocapture`、`cargo test -p tidb-expr --features shared-kernel --lib constant::tests::prepared_ --locked -- --nocapture`、`cargo test -p tidb-datatype --lib decimal --locked -- --nocapture`及`cargo test -p tidb-chunk --lib decimal --locked -- --nocapture`；独立SDK根运行`cargo test -p tidb_query_expr --lib --locked -- --nocapture`。按受影响范围补原native向量对照、限定Clippy、根make lint及格式／diff。完整SDK Clippy的已核对公告失败按现状保留，无需为无关依赖更新扩任务；该失败不构成新的算法副本理由。全部移动、测试、构建、测量与文档计入实际增删，不再沿用810估算。

正向验收要求请求字节减少、分配次数不增加，并且同一路线没有可重复的耗时退化。若有小幅波动，先报告原始配对范围，不用四轮结果声称统计显著。若布局未减少实际请求字节，或必须引入新的存储／所有权框架才能继续，保留固定结果与具体原因，结束这一小值Decimal优化窗口。即使有改善也不无限追加微调；后续回到共享内核M1的状态生命周期与完整覆盖／迁移预算，另以真实入口证据推进，默认SQL与扩大函数矩阵不属于此批。


## 载体布局复审：空间请求下降，结束Decimal单场景窗口

固定TiDB产品／测量源码`beda64e0282826a09f6ab34027fb8d1043fc2e56`、[报告854d47d6](https://github.com/YangKeao/tidb/blob/854d47d675a82d242467cc92182476c33b6f94ce/rust/docs/shared-kernel/carrier-layout.zh.md)、SDK`2781f6775939ea2700bd26563ae7f16cddec0344`及原datatype来源`88e183218a66197bd4a790db96fa1e373b1b17ce`通过限定复审。Designer只读固定源码及已有证据，没有另运行产品构建、测试或性能。180bc037的单次布局范围到此结束，不再追加Decimal单场景优化。

[私有载体](https://github.com/YangKeao/tidb/blob/854d47d675a82d242467cc92182476c33b6f94ce/rust/crates/tidb-datatype/src/decimal/compact.rs#L148)把声明形状移入Compact变体，Wide直接保存原Decimal；Wide列使用原`Decimal::from_column`，结果还原及算术清除形状规则保持。原CompactDecimal、全部数值算法、幅值／尺度保护及输出接纳不变，没有Box逐值分配、packed／unsafe或外部元数据表。现有45位宽值列用例覆盖未指定与固定尺度，完整值／负零／隐藏精度等反例仍保留。此次是表示调整，不宣称产品错误从失败到成功。

实际Aug22工具链测得封装及Option由112降至80字节，ExpressionScalar同样112→80，RpnExpressionNode为240→208；原Decimal80、CompactDecimal32、ExpressionVector32和RpnStackNode160字节不变，对齐不变。同一column_mul/64完整调用中，owned Vector的请求字节36540→28476，direct为29616→21552，各减少8064；分配次数仍为71／12。owned Scalar请求字节115804→102428，次数314不变；native两路不变。请求字节包含realloc请求，不等同于峰值内存或RSS，也不把整份差值只归到64个列元素。

四轮owned Vector中位10.162670→10.056558微秒，配对变化−2.77%至−0.56%；direct中位8.244824→8.306088微秒，配对−2.33%至+1.88%，仍比同轮native慢29.60%–32.31%。只接受空间请求下降，不能声称direct稳定提速或排除小幅真实退化。机器未证明隔离、进程内路线顺序固定、opt3无LTO等测量限制保留；不为获得更有利数字延长本窗口。

独立重算40个计时、10个独立分配单元及全部表格吻合；新runner对两版本各五路检查精确数量和集合，旧四／四、四／五模式有独立退出0记录，原36／9汇总脚本未改。105份记录哈希、5526项固定Git后缀输入逐项一致，输入摘要为`49b5e78606ec88163b76af71015a9482d13de633795b441522eab037ced88d0e`。四份最终程序使用相同测量源码，同版本两模式使用相同十二份封存直接库；before明确复用task43最终after库后重新链接，after由一次成功构建封存后链接。预格式化程序不进入最终表。这里证明记录中的来源与产物关联，不是独立重构建或完整传递构建闭包。

两张依赖图各唯一原datatype等五包；native／SDK本地图同一datatype，独立图使用固定Git来源。172份所有者包文件资源及1421项SDK输入吻合，包外构建输入不在此闭包内。43项shared、8项prepared、90项Decimal（另有1项同命令的集成筛选测试）、10项Chunk、1项native向量及429项独立SDK都有退出0终态；限定Clippy有警告、根lint和格式／diff通过。完整SDK Clippy本批没有重跑，沿用task43停于19项公告的exit2，尚无完整workspace检查通过结论。

本批生产`+42/−27`、测试／测量Rust`+23/−0`、构建`+6/−6`、文档／数据／核验`+667/−1`，合计`+738/−34`，全部增删**772**。相对原始两仓，源码测试`+7744/−295`，增删**8039**；全部`+28338/−398`，增删**28736**。均是固定端点重算，仍不能将局部源码数量等同于全范围10K目标；下一步转向执行状态所有权及旧完整预算的前提更新。


## 下一批设计：冻结程序与一次执行的状态分开

[当前共享RAND](https://github.com/YangKeao/tikv/blob/2781f6775939ea2700bd26563ae7f16cddec0344/components/tidb_query_expr/src/types/expr_eval.rs#L912)把节点地址传给输入适配，再由[StmtContext的有种子map](https://github.com/YangKeao/tidb/blob/854d47d675a82d242467cc92182476c33b6f94ce/rust/crates/tidb-executor/src/stmt_context.rs#L4268)持有流。当前SharedProjection虽然说明每次EXECUTE重新编译，API仍能复用同一程序并替换effects；程序销毁也不会移除map中的旧地址。新的显式执行边界必须自行拥有状态，不能把程序地址、上下文地址或缓存generation当成生命周期证明。

设计选择一个轻量的`SharedExecution`（拟新增名称），借用冻结SharedProjection及固定的本次`Columns`，拥有SDK中的每根`RpnExecutionState`。入口形态为`projection.start_execution(effects)`，随后在这个对象上调用`evaluate(input, rows, ctx)`及`evaluate_into_chunk(input, rows, ctx, output)`；effects不在每批方法中替换。单根SharedExpression使用相同的状态／循环机制，不再保留一条依赖裸地址map的带上下文入口。当前真实消费者是shared_kernel内测试及decimal-shared-bench；Unistore的同名SharedExpression属于另一类型，不能误改。审计并迁移全部受影响实验调用，保持调用者可直接拥有完整输出。

SDK状态用程序局部节点序号索引有种子RAND槽，按根隔离，首次被需求才初始化；同节点跨Chunk继续、同种子不同节点独立、新执行重新初始化。这里只保存现有MySqlRng，不复制随机算法，不把状态放回冻结节点或新建通用任务表。可用现有借用与小型内部可变容器适应`InputColumns`的只读接口，不能强制增加Sync或全局锁；先记录实际状态尺寸／创建点，不承诺所有纯表达式零额外分配。eval_group、行顺序和原帧循环只有一份，两个输出入口共享同一次求值；scratch仍按调用清理。

保留现有NoColumns不提供有种子流时每次取首样本的行为，以及PB已有函数路径。增加一个声明式`Columns`能力，默认false，真实StmtContext及已有共享测试会话为true，在开启执行时读取；不得调用`rand_seeded_next`探测支持，否则探测本身推进状态。支持时由SDK执行状态推进常量种子的流，动态种子继续每次初始化，NULL规则不变。共享桥不再向宿主发送裸节点地址；原生旧求值器及其map保持原用途。无种子RAND继续借用会话流，用户变量、参数、SQL模式／时区和告警仍是本次真实上下文的能力，不因建立或销毁执行对象而清空。

确定性见证不需要等待分配器重用地址：旧版在同一冻结程序、同一保留effects上连续运行时会接续同一map流；新API在其间结束一个执行并创建另一个，必须重新从种子开始。再让两个执行对象借用同一冻结程序交错运行，各自跨批推进而互不串流。旧版本运行时见证说明缺少这个新边界，不能以缺少新API的编译失败充数，也不能宣称默认SQL已经复现同一问题。[实际prepared SELECT](https://github.com/YangKeao/tidb/blob/854d47d675a82d242467cc92182476c33b6f94ce/rust/crates/tidb-session/src/dispatch.rs#L1392)另建语句边界，原StmtContext构造新有种子map而借用持久会话无种子流；应核对并用原测试验证这一区别。没有共享RPN默认SQL接入或跨EXECUTE缓存，现阶段生产集成政策仍是每次EXECUTE编译已绑定／解析的快照，不能因局部测试复用程序而更改这一政策。

同一任务内完成运行时见证、实现及定向回归，不增加内部步骤的审批链。覆盖多Chunk、两个同种子节点、常量／动态／NULL种子、无能力调用、空／死分支、重复乱序选择、BENCHMARK重复、Scalar／Vector及两种输出。错误或取消清理临时值而保留已经发生的随机推进和会话副作用；下一合法批次仍可用。已有direct失败保留旧目标的合同不变；不恢复warning/error排序约束。现有参数按需／诊断反例也应保留，不能让对象创建提前读取参数或执行表达式。

真实所有者用例放在已经依赖tidb-expr的executor测试层，必要时只加测试feature转发，不从expr反向依赖executor。用实际StmtContext及会话RNG／变量／参数存储贯通两批和下一次执行；若取得真实prepared快照需要无关的SQL／worker改造，就交付该真实上下文测试与明确标注的prepared源码追踪，不扩大本批接入。

按[主计划](expression-shared-kernel-design.zh.md)第8节的环境运行shared_kernel定向集、相应StmtContext／prepared测试、独立SDK表达式库回归、限定Clippy及make lint；记录实际通过数量、失败到成功、固定输入与全部增删。测量器若因API迁移修改，只验证调用与完整值，不新增Decimal计时矩阵或把旧性能值冒充新测量；需要的最小编译／调用验证可正常执行。未改依赖不必重复已知19公告检查，完整SDK Clippy仍明确未通过。由Designer消息7db0fd17-d6c6-459b-9add-39b01d4921e0负责这一批边界，固定实现复审后结束；不接默认SQL、跨执行缓存或通用生命周期框架。


## 完整预算前提更新：保留原所有者，未完成部分仍需定价


以下18组判断最初基于TiDB854d47d6／SDK2781f677；task45复审后用babdd5c9／c2ef52b8更新input_lifecycle行和当前端点，其余行无新增覆盖结论。沿用task35的245族身份、五个内含例外和18个原组ID，不生成新清单。当前实际源码测试8620／全部29914行增删是已付端点，不与历史批次相加。旧36470–79760仍是cp7条件估计，不能减掉676行旧副本便当作当前余量，也没有足够证据重算一个新总区间。

| 原组ID | 当前状态／改变的前提 | 已付事实与未闭合部分 | 本行唯一成本归属 |
| --- | --- | --- | --- |
| operator_numeric | 原所有者复用已证 | 当前接纳范围的Decimal加减乘除及紧凑primitive已共用；其余位运算、unsigned／混合签名未闭合 | 数值运算主体与本组诊断；通用转换引用cast |
| math | 原假设保留，未重估 | Go专用exp/log/trig仍在原expr；不可因datatype可依赖便视为已共享 | 数学主体；RAND状态归input_lifecycle |
| string | 下层primitive可保留，表达式策略未共享 | 编码／GoString已有所有者；StrUnits、完整载体、packet和coercion仍待接入 | 字符串签名策略；字符集转换引用convert_charset |
| pattern | 原假设保留，局部已付 | 已有REGEXP需求／诊断探针保留；全LIKE／替换语义与缓存有效期未闭合 | 匹配／替换主体；缓存寿命归input_lifecycle |
| temporal | 搬迁前提需改，非零语义成本 | 原Time／解析primitive无需复制；DATE策略、两端时间载体、FSP／DST／clock未统一 | 时间值算法／策略；共用输入转换引用cast |
| json | 搬迁前提需改，完整所有者未定 | 原BinaryJSON／path可保留；native serde修改与SDK Json仍非共同执行体 | JSON值／路径／呈现；schema服务归exceptions |
| crypto | 原假设保留，未重估 | 已有MD5路径沿用；AES需求、mode／IV、压缩错误仍需适配 | 加密压缩主体；密码服务归exceptions |
| network | 原假设保留，未重估 | 本次无新覆盖信用；地址格式、错误NULL和字节宽度仍须证明 | 网络地址转换／比较；与comparison不重计 |
| vector | 原假设保留，未重估 | 既有vector库不必搬；完整载体、维度／NaN合同未验证 | 向量运算主体；载体转换归cast |
| misc | 原假设保留，未重估 | UUID／grouping／identity完整值语义未因Decimal复用而完成 | 杂项值函数／metadata；外部服务归exceptions |
| comparison | 原假设保留，未重估 | datatype比较／collation可保留；IN准备、混合类型及需求仍未闭合 | 比较／集合算法；通用转换引用cast |
| control | 共享循环局部已付 | 现有IF／需求／模式已付；CASE／COALESCE和完整分支域未完成 | 组合／需求；旧dispatcher退役归integration |
| cast | 精确primitive复用已证，全部类型未闭合 | String→Real、类型化Decimal参数转换／完整数值载体已付；time／JSON／enum等仍有源签名策略 | 共用值转换／载体，一次计入供各组引用 |
| convert_charset | 原假设保留，未重估 | 旧表本来就保留字符集／collation表；不能再把“不复制表”算新节省 | CONVERT／binary政策及编码适配 |
| exceptions | 原五项保留，未重估 | codec／parser／password库仍原所有者；schema尚含Expression求值和懒外部引用 | 五项具体服务能力／错误桥接，不搬服务本体 |
| input_lifecycle | 列／typed参数／已接纳有种子执行状态已付 | 显式owner跨批延续／新执行重建已证；完整缓存寿命、correlation和默认集成未闭合 | 跨族绑定／状态寿命；task45实际端点已计，不重复估价 |
| integration | 显式输入输出已付，默认与退役未付 | owned/direct根输出已付；eval_expression_once仍进入原eval，默认AST/PB／Unistore等未切换 | 入口接线与真正旧运行时删除；保留值API不算重复求值器 |
| engine | 单RPN及分发依赖已付，剩余范围未定价 | 共用循环、独立表达式包固定Git构建已证；完整签名映射、嵌套PB退役与全workspace检查未完成 | 共享程序／验证／构建余项，不重复计各组算法 |

状态“未重估”不表示零成本或接受旧区间为当前上限；一处未来帮助函数只有表中一个成本归属，其余行引用。没有逐行删除闭包前，不给原始代码退役额度。已有datatype源码原地被调用可以是零算法搬迁量；尚在expr的函数不能据此声称已是公共leaf。

六条有限调用链已经核到具体入口，可用来约束下一次实际diff，无需Engineer重做清单：

1. **已证复用**：[SDK Decimal调用](https://github.com/YangKeao/tikv/blob/2781f6775939ea2700bd26563ae7f16cddec0344/components/tidb_query_expr/src/types/native_arithmetic.rs#L190)→原[算术载体](https://github.com/YangKeao/tidb/blob/854d47d675a82d242467cc92182476c33b6f94ce/rust/crates/tidb-datatype/src/decimal/compact.rs#L256)；[SDK String→Real](https://github.com/YangKeao/tikv/blob/2781f6775939ea2700bd26563ae7f16cddec0344/components/tidb_query_expr/src/types/native_arithmetic.rs#L236)→原str_to_float。结论是保留并调用原owner，已付适配／输出成本仍计入当前端点。
2. **DATE**：[native date](https://github.com/YangKeao/tidb/blob/854d47d675a82d242467cc92182476c33b6f94ce/rust/crates/tidb-expr/src/time_fn/mod.rs#L174)→date_modes／handle_truncate→原Time；[SDK date](https://github.com/YangKeao/tikv/blob/2781f6775939ea2700bd26563ae7f16cddec0344/components/tidb_query_expr/src/impl_time.rs#L61)→其invalid_zero政策／packed Time。两者不是零改动别名；需一个值算法owner和明确的入口政策适配。
3. **JSON_SET**：[native json_modify](https://github.com/YangKeao/tidb/blob/854d47d675a82d242467cc92182476c33b6f94ce/rust/crates/tidb-expr/src/builtin_ext/json/modify.rs#L265)→document/path/typed-value策略；[SDK json_modify](https://github.com/YangKeao/tikv/blob/2781f6775939ea2700bd26563ae7f16cddec0344/components/tidb_query_expr/src/impl_json.rs#L44)→SDK Json。原[BinaryJSON::modify](https://github.com/YangKeao/tidb/blob/854d47d675a82d242467cc92182476c33b6f94ce/rust/crates/tidb-datatype/src/binary_json_ops.rs#L139)虽已是leaf，尚未证明能原样替代两端入口；不得把typed／text与路径需求成本抹去。
4. **转换**：[SDK typed参数](https://github.com/YangKeao/tikv/blob/2781f6775939ea2700bd26563ae7f16cddec0344/components/tidb_query_expr/src/types/native_arithmetic.rs#L28)已调用原[Datum::to_typed_decimal_with_context](https://github.com/YangKeao/tidb/blob/854d47d675a82d242467cc92182476c33b6f94ce/rust/crates/tidb-datatype/src/datum/convert.rs#L251)；[cast_to_time](https://github.com/YangKeao/tidb/blob/854d47d675a82d242467cc92182476c33b6f94ce/rust/crates/tidb-expr/src/cast.rs#L1173)仍在表达式层，说明一个成功primitive不能代表全cast已完成。
5. **CONCAT**：[concat_with_context](https://github.com/YangKeao/tidb/blob/854d47d675a82d242467cc92182476c33b6f94ce/rust/crates/tidb-expr/src/string_fn.rs#L35)→coerce_str_bytes／max_allowed_packet→结果Datum。编码库可保留，但类型化字符串与packet策略仍需实际共享边界；不整目录搬string/context。
6. **真实入口**：[eval_expression_once](https://github.com/YangKeao/tidb/blob/854d47d675a82d242467cc92182476c33b6f94ce/rust/crates/tidb-expr/src/lib.rs#L670)→Expression::eval仍是默认事实。公开签名可保留，程序／状态owner由当前生命周期工作落实；未接入共享循环前不能领取旧runtime删除信用。

**生命周期固定结果复审后的单一建议：验证DATE完整调用链。** 只取原Time载体、DATE的类型化参数转换／零日期政策、同一共享循环和原生／PB入口差异，给出一个共同值算法owner及真实增删；不同时启动JSON_SET、全57时间族或新通用context框架。这能检验最大旧成本项的“原算法保留＋入口政策”边界是否成立，当前仍是有源码依据的下一切片建议，不是本轮产品实施授权。

Engineer后续固定交付只需在这18行中更新实际触及的owner、兼容证据和原始基线diff；Designer负责维护前提与跨组归属。无需另开一轮清单任务，也不等待这份表才继续已授权的生命周期实现。

## 执行状态复审：新执行隔离成立，默认SQL仍是独立边界

固定TiDB产品`853e0cceaa4ca8910623f642ea8676990a97ee5a`、最终测试`ae8a20aa4945a4da6ca9f15c5baf2caa3734c67c`、[交付报告babdd5c9](https://github.com/YangKeao/tidb/blob/babdd5c90a98bd164b499b420d3301e23e6af7a8/rust/docs/shared-kernel/execution-lifecycle.zh.md)，SDK`c2ef52b8eb2edba0447eaf605d4d29e218b0671d`；原datatype来源仍为88e183218a。固定源码与证据审查未发现已接纳路径的具体回归，接受task #45限定实现；Designer消息7db0fd17的本批范围结束。此次只读源码、留存记录和Git端点，不重新运行产品构建、测试或性能测量。

[SharedExpression／Projection执行对象](https://github.com/YangKeao/tidb/blob/ae8a20aa4945a4da6ca9f15c5baf2caa3734c67c/rust/crates/tidb-expr/src/shared_kernel.rs#L743)固定借用程序和effects，批次方法不能替换所借用的effects；SDK EvalContext仍由每次调用传入；[SDK RpnExecution](https://github.com/YangKeao/tikv/blob/c2ef52b8eb2edba0447eaf605d4d29e218b0671d/components/tidb_query_expr/src/types/expr_eval.rs#L317)私有持有借用程序和懒初始化HashMap，以本根指令序号为键。这里采用HashMap而非此前建议的dense槽：保持节点／根／执行隔离且无全局身份机制，限定范围可接受；不能宣称没有容器开销。旧eval_group移入该对象，原帧循环只接收状态参数。共享裸地址回调已移除，原生旧求值器仍使用原map；NoColumns和PB原有行为保持。原函数元数据仍不承诺Sync，手工拼造的native／PB混合嵌套程序也不由这次评审覆盖。

跨批、两根及同根两个同种子节点、交错执行、动态／NULL种子、空／死分支、重复选择和BENCHMARK的源码与通过记录相符。错误保留已发生效果，下一批可继续；direct仍在所有根及可失败转换完成后才reset目标。现有实验调用和Decimal测量器均在重复调用外保存执行对象；纯投影也会创建每根64字节的Vec元素，SDK／单表达式／投影对象实测64／88／48字节不含动态分配。三次iterations=1的测量器调用只证明API及完整值校验，不把以前的计时／分配结果归给新接口。

旧运行时见证确实是基线加测试并运行到断言失败：同程序／effects换EvalContext仍继续旧流；改后两个显式新执行均回到首组。它证明新API的执行隔离合同，不能称为默认SQL的既有错误。尤其要区分两项实际证据：[executor集成测试](https://github.com/YangKeao/tidb/blob/ae8a20aa4945a4da6ca9f15c5baf2caa3734c67c/rust/crates/tidb-executor/tests/shared_execution.rs#L29)使用真实StmtContext、参数42／变量／会话RNG，跨两批后在**同一个上下文**上新建执行对象；[实际PREPARE／EXECUTE测试](https://github.com/YangKeao/tidb/blob/ae8a20aa4945a4da6ca9f15c5baf2caa3734c67c/rust/crates/tidb-session/src/tests_core/numeric_domain.rs#L1051)仍走native SQL，单独证明新语句的有种子重建及无种子会话流延续。两者不能拼成共享默认SQL或跨EXECUTE缓存已经贯通的结论。

独立复算102份留存记录、5527项选定输入（SHA256 `3f18a36b79cb03db493c4122cfcc19ff70d62dd3ad0317b7366e4d2e0ee6b7b9`）、最终测量程序及12份直接库一致；双依赖图各只有原五包的一份身份，172份owner资源和1421项SDK输入吻合。源码清单／直接库并非完整构建闭包。47项shared、8项prepared、430项独立SDK、真实上下文及实际SQL各1项的终态和来源吻合；三个库限定Clippy及根lint退出0，有警告。最终ae8a20aa仅强化同一RPN两个同种子节点的测试，随后完整47项通过，不能说所有旧命令都在该提交上重跑。

失败保留且不以新目标掩盖：连续流测试误建新执行、SDK测试导入及集成用例的私有导入／超范围参数都已有修正记录；executor原lib目标仍有7个未改构造器缺clause_message，声明和夹具与批前字节一致，独立集成目标成功不关闭这项失败。首次metadata成功但漏feature而没有SDK包，最终图修正；没有独立验证器异常栈留存，不能称Cargo失败。完整SDK Clippy未重跑，仍保留task43的exit2／19项公告及未完成workspace结论，公告库未钉revision。外部取消注入、完整函数缓存寿命、完整executor／session、默认共享SQL及性能均未由这批证明。

| 固定端点范围 | 生产新增／删除 | 测试测量新增／删除 | 构建新增／删除 | 文档核验新增／删除 | 全部新增／删除 |
| --- | ---: | ---: | ---: | ---: | ---: |
| 本批task45 | 262／117 | 608／172 | 8／0 | 589／0 | **1467／289** |
| 原始两仓至固定版 | 3636／291 | 4689／4 | 1990／103 | 19201／0 | **29516／398** |

本批全部增删**1756**；原始源码测试**8620**、全部**29914**。按固定端点重算而非历史相加。18组表的input_lifecycle行现改为“已接纳有种子状态所有权已付，其他缓存／correlation及默认集成未闭合”；engine、integration等未覆盖部分无新增完成信用。全245族、真正旧运行时退役和全范围10K数量级仍无证明。

## 下一批：原Time所有者上的类型化DATE完整调用链

本批依据Designer消息b9f2f967-5ee1-4009-9b02-85747ecd133d，由ExpressionReuseEngineer实施，Designer承担范围，到固定实现复审结束。连续完成值算法提取、两端接线及验证，不为内部步骤增加审批链。目标是让已定型Time／NULL的DATE、真实时间列及其IF组合，经现有共享程序／执行对象得到完整结果，并使native旧DATE及SDK原PB DATE调用同一份值算法。它是单函数的机制原型，不是57个时间族或某个完整Go包已经迁移；完整时间转换、JSON、默认SQL、跨EXECUTE缓存和新性能矩阵不在本批。

### 已核实的差异决定接口

[native DATE](https://github.com/YangKeao/tidb/blob/ae8a20aa4945a4da6ca9f15c5baf2caa3734c67c/rust/crates/tidb-expr/src/time_fn/mod.rs#L174)在原值上检查`is_zero && no_zero_date`或`!is_zero && invalid_zero && no_zero_in_date`，拒绝时走原handle_truncate；成功总清除时钟并设Date/FSP0。原[cast_arg_as_datetime](https://github.com/YangKeao/tidb/blob/ae8a20aa4945a4da6ca9f15c5baf2caa3734c67c/rust/crates/tidb-expr/src/cast.rs#L1415)对Time／NULL确为恒等转换，所以这一明确子域不需要迁移复杂时间parser。

[SDK PB DATE](https://github.com/YangKeao/tikv/blob/c2ef52b8eb2edba0447eaf605d4d29e218b0671d/components/tidb_query_expr/src/impl_time.rs#L61)拒绝全部invalid_zero，诊断经过SDK EvalContext；其[set_time_type](https://github.com/YangKeao/tikv/blob/c2ef52b8eb2edba0447eaf605d4d29e218b0671d/components/tidb_query_datatype/src/codec/mysql/time/mod.rs#L1980)仅在原类型不是Date时清时钟，已有Date带隐藏时钟时保持。不能用一个来源名称开关掩盖差异，也不能简单让PB调native包装器；用具体的零值／时钟政策描述它们。

原Time的[unchecked Go位解码](https://github.com/YangKeao/tidb/blob/ae8a20aa4945a4da6ca9f15c5baf2caa3734c67c/rust/crates/tidb-datatype/src/mysql_time.rs#L293)将低四位15解释为Timestamp/FSP7；SDK内存同位被get_time_type解释为Date。两边日历位排列相同，不等于整个raw值语义相同。SDK`from_packed_u64/to_packed_u64`是另一种存储编码，Timestamp还会按时区转换；不用于这次桥接。日期字段、FSP、种类和隐藏时钟都必须按各自原合同保留，不能经Display或UTC存储往返。

### 一份纯值算法，两个既有诊断入口

在原`tidb-datatype/src/mysql_time.rs`增加小型纯值操作，拟定形态为`Time::date_value(self, DateValuePolicy) -> Option<Time>`。`DateValuePolicy`明确包含`reject_zero_date`、`reject_zero_in_date`和`clock: ClearAlways | PreserveAlreadyDate`，不含来源owner、表达式、会话或回调。None仅表示上述零日期政策拒绝；该函数不写诊断、不作时区转换、不验证额外日历范围。先按**未清时钟的原值**判断全零和部分零，再按政策决定清时钟，最后设Date/FSP0。现有Time/CoreTime完成位操作，不新建时间载体或另一份parser。

native旧time_fn::date和SDK共享native DATE从原DateModes取两个拒绝位，选ClearAlways；拒绝时用原输入Time的Display构造原消息；旧native包装器继续调用`Columns::handle_truncate`，共享native节点经已有`InputColumns::handle_truncate`转发，返回NULL或错误。只需给InputColumns补`date_modes()`，由NativeInput转发固定effects；无上下文默认保持原TIDB_DEFAULT_SQL_MODE，不从SDK flags猜native规则。原handle_truncate已保留Ignore/Warn/Error以及1292身份，不新增host DATE回调。`allow_invalid_dates`不新增DATE校验；原算法此处没有拒绝所有非法日历。

SDK原PB DATE选两个拒绝位均true及PreserveAlreadyDate，拒绝继续走原`handle_invalid_time_error(Error::incorrect_datetime_value(original_sdk_value))`。SDK datatype保持独立：可增加有清晰合同的`Time::to_raw_bits/from_raw_bits`安全内存位读写方法，明确它们不是packed存储API，也不是普遍等价的Go编码。只在已依赖原datatype的SDK表达式层桥接：从SDK语义kind/FSP及`CoreTime::from_raw(bits & !0xf)`构造原Time（SDK非Date的FSP已在0..6，Date用0），成功DATE结果以`core.raw() | 14`回SDK内存值。这样raw15先按SDK语义映射成Date，输出按原set_tt规范为14；原Date隐藏时钟保留。不能直接把SDK raw15送进native unchecked解码器。窄接口必须有原始位／策略见证，不能隐式扩大为所有时间函数的通用互换承诺。

### 贯通现有程序和完整载体

SDK`types/expr.rs`的ExpressionScalar／ExpressionVector新增直接承载原`tidb_datatype::Time`的Time变体；native常量和输入读取用它，所有中间节点保留完整core/kind/FSP。类型枚举、finish验证、is_null、IF／OrderedRows合并及根输出一起补齐。共享native DATE在同一帧循环使用这一变体和纯值操作；generic FnCall仍拒绝不支持的完整native载体，不把Time伪装成Bytes或SDK值句柄，也不回调旧Expression。

native`shared_kernel.rs`只接纳非array的Date／Datetime／Timestamp常量、实际列及已证Time／NULL组合；常量的实际Datum与声明必须一致，不能仅凭声明把字符串当Time。DATE只接一参、Date结果；字面NULL可在此归一为Time-NULL，参数／deferred来源及字符串、数字、YEAR、Duration到Time的转换继续整根明确拒绝。未知类型元信息不能截断。列继续保存完整FieldType并保留相同物理序号的完整声明一致性检查，schema映射真实时间类别；NativeInput只解码被需求的逻辑行，复用原Column::get_time／Row::get_datum，不根据声明FSP或时区重写存储值。

原生IF允许三个时间声明归一到同一求值族，并原样返回选中值；因此编译器的IF接纳、control_type、schema和SDK merge_selected_values要同时更新。保留Int条件，Time分支可有不同kind/FSP，不根据IF结果声明重新stamp选中值。保留原非算术row_fallback：原native DATE没有专门时间向量路径，每个被需求的行都执行政策及诊断。Time常量可广播，**DATE(constant)的诊断不能只算一次再广播**。Scalar与width1 Vector仍是独立调用合同。

owned结果恢复原Datum::Time；direct的目标布局检查为三种时间声明添加原8字节存储尺寸，继续已有Root::Datum暂存和append_datum。所有根求值及可失败转换成功前不reset目标，失败保留旧目标／已发生效果，无新事务或scratch协议。原Chunk读写合同保留：append_time写go_raw，get_time经from_go_raw／Time::new调用原check_fsp，将FSP7钳到6，保留core及Timestamp种类；它不会因FSP7拒绝或panic。unchecked常量／中间载体仍可保留FSP7，物理列按原getter得到FSP6；分别验证原位写入和规范化读回，不能把两种入口误要求为逐字段无损往返。直接输出保持原写入合同，不提前把owned结果的FSP7钳掉，也不绕过原getter的钳制。

### 能判别设计是否成立的验收

先在未修改的native／SDK原入口记录各自预期，保留固定源和结果，避免提取后两个入口共用同一错误实现还互相证明。类型化DATE原先未被实验编译器接纳可明确记录，但不能只以缺新API的编译错误代替语义反例。以下使用独立完整值／诊断断言并扩展现有套件，不新建全时间测试框架：

- 全零Time且仅no_zero_in_date=true：native成功返回零Date且无诊断；PB仍按自身政策拒绝。`0000-00-00 01:00:00`不是原is_zero：仅no_zero_date=true时native成功，反过来仅no_zero_in_date=true时拒绝；这直接捕捉先清时钟再判零的错误。
- 普通Date／Datetime／Timestamp、非零时钟／micro、FSP0/3/6、NULL、部分零、非法非零日历、已有Date隐藏时钟及原unchecked FSP7常量。比较完整core/kind/FSP，PB raw15和hidden-clock必须保留独立预期；任何新桥不触发时区、日历normalize或额外诊断。
- 真实Chunk含NULL和混合时间值，声明与存储kind/FSP不同仍按原getter；两份Chunk、重复乱序`[2,0,2]`及空选择、裸时间列、`DATE(IF(...))`和`IF(...,DATE(...),time_null)`，两种会话模式及owned/direct分别对照。声明冲突／array／非Time实际常量等在执行前拒绝。
- Ignore/Warn/Error分别比对诊断内容、1292身份／SDK既有身份和实际次数。多行DATE常量拒绝必须按原生逐行需求计数；死分支不读缺列、不触发DATE诊断，PB原eager／短路规则不改。warning/error相对顺序继续按人类放宽。
- 先发生已接纳RAND或变量效果，再发生DATE错误：direct原目标不变、效果保留；同一task45执行对象的下一合法批次继续。把调用点及原算法删改列清，保留原存储公共API，不以搬文件代替共享证明。

验证在既有声明工具链／依赖环境下执行：TiDB的`rust`目录运行`cargo test -p tidb-expr --features shared-kernel --lib shared_kernel --locked -- --nocapture`及新增纯Time操作的datatype定向测试；原native DATE受影响用例也要跑。独立SDK根运行`cargo test -p tidb_query_expr --lib --locked`，并对SDK Time新增内存接口做datatype定向测试。保持既有根`make lint`、fmt、diff及受影响库Clippy；不以已知全SDK公告失败的重复运行替代本批检查，也不顺便修未改executor夹具。若修改原owner，提交后同步SDK固定Git／锁，并验证本地patch及独立SDK双图、唯一类型身份和实际资源来源。具体测试名、命令／失败终态均入固定收据；设计者审查不冒充这些测试已经运行。

本批不预先承诺行数或提速。首个可编译版本给出实际生产／测试／构建和材料预算差异，最终用本批及原始两仓固定端点新增+删除收束；只更新18组中的temporal、cast、control和实际触及的integration／engine成本，不给未退役运行时删除信用。若保真必须扩大为通用时间context／parser迁移或第二执行机制，停在可审查原型并给最小反例和具体接口差异；这不要求反复等待内部步骤批准。完成这一调用链只能检验“原值所有者＋共享调度＋入口政策”的实际代价，不能由此推出全部时间、245族或10K目标可达。

### FSP7存储边界更正（2026-11-14）

前版1aef4f1e的“Chunk拒绝FSP7”描述有误，已撤回。Engineer的“应panic”测试实际失败后，Designer重新展开固定源码：[check_fsp](https://github.com/YangKeao/tidb/blob/ae8a20aa4945a4da6ca9f15c5baf2caa3734c67c/rust/crates/tidb-datatype/src/fsp.rs#L68)对大于6的值返回6，Time::new使用该结果；[Column::get_time](https://github.com/YangKeao/tidb/blob/ae8a20aa4945a4da6ca9f15c5baf2caa3734c67c/rust/crates/tidb-chunk/src/column.rs#L626)经from_go_raw走这条链。低四位15经native unchecked解码是Timestamp/FSP7，经Chunk原getter是同core的Timestamp/FSP6，经SDK语义解码则是Date/FSP0。原始基线的这三份文件相同，不能因构造函数名带checked就推断为拒绝。

本批范围b9f2f967不变，工程侧继续实现；验收改为独立断言常量／owned载体保留7、原Chunk写入位保留且读回钳到6、DATE结果为Date/FSP0。共享真实列继续调用原getter，不复制钳制算法；用裸时间列或恒等IF的结果直接断言FSP6，只有DATE结果的FSP0无法辨认输入是否错误绕过钳制。“应panic”失败作为被否定的设计前提保留，不标成产品回归。完整native表达式若在该unchecked值的其他格式化路径已有失败，需保留失败并明确typed DATE直接对照的证明边界，不能把它计为完整native表达式通过。此次更正只验证固定源码调用链和文档，没有重跑产品测试；51项共享通过仍是工程侧的中间报告，待固定交付复审。
