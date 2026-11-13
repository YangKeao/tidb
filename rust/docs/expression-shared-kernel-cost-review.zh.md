# 成本复审与底层算法复用：下一步设计

当前结论是：**保留一个共享表达式执行循环及现有底层算法所有者；参数合同的有限验证已通过复审，下一批在现有显式入口接入Decimal类型化参数。** 下文保留成本复审和限定试验的过程证据；末节记录bfde2f35参数合同复核、最小转换／诊断接口和执行所有权决定。

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
