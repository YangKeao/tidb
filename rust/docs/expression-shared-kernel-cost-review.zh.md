# 成本复审与底层算法复用：下一步设计

本轮建议是：**保留一个共享表达式执行循环，先让它直接复用已有的底层值类型和算法，再扩大函数覆盖。** 当前已复制的Decimal算法与数值前缀解析可以作为这个边界的实际验证对象。先新增整批参数／生命周期实现，不能回答目前最重要的规模问题。

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
