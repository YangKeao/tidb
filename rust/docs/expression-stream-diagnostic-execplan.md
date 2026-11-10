# 记录数值诊断文本直接写入试验（已撤回）

This ExecPlan follows root PLANS.md. Progress, discoveries, decisions and outcomes record the completed experiment; the candidate source is withdrawn.

## Purpose / Big Picture


用户希望继续简化共享表达式实现、提升性能，同时保留原有行为。试验尝试减少同一次数值诊断遍历中生成和复制的子树字符串。实际服务确认机制成立，单行深表达式有收益，但扫描和Decimal出现不利CPU变化，未证明不回退，因此恢复保留版。下一位维护者应看到完整正反结果，避免把未保留候选当作新基线。

## Progress


- [x] 核对现有准备缓存、调用范围，保存试验前9521项输入和两个文件原文。
- [x] 实现候选，保留读取顺序和失败短路，完成实际服务／单元产物和机制检查。
- [x] 完成前端993项、SDK表达式1161项、数据类型6项及范围内质量检查；既有失败单列。
- [x] 首次严格计划对照失败原样保留，仅重启一次自建存储；新24份完整对照通过。
- [x] 完成96组、113714次计时查询和192份前后记录核对，清理自建服务／数据。
- [x] 撤回全部候选源码／新增测试，逐项证明恢复为原9521项输入、HEAD／状态及受保护文件。
- [x] 汇总本结果记录、详细报告、原始资料及代码量；交付和本地文档DCO由独立收据记录。

## Surprises & Discoveries


EvaluatorSuite已有BatchLane准备结果缓存，Projection也回收复用它，不能把本次试验描述为消除每批重复编译。实际分配调用链中出现的是准备阶段诊断文本，不足以推算可兑现收益。

cast_json在目标类型缺失时仍先读取子表达式。候选使用延迟目标类型格式回调保留此行为；左侧失败不读右侧，非法运算符／参数数量不读子节点。

首次服务构建实际退出101：移除了dynamic_diagnostic.rs仍经父模块使用的旧转换接口导入。确认终止后恢复导入并固定候选2；第二次实际退出0／576.249秒。首次控制器也退出1，因为新增一个测试让既有失败的筛选计数从2439变为2440；完整断言正文不变，独立清单核验只接受这个精确变化，原失败收据保留。

SDK clippy扩大到数据类型测试目标后为252条告警，旧171条保留，新增81条属于扩大范围；其中9个此前未检查的位置来自原测试代码，不能统称为重复标题。相关原始诊断和过强比较断言失败均保留。

## Decision Log


Decision: use a borrowed formatter and shared writer templates, without a new cache, executor, host protocol or quota. Rationale: isolate the preparation allocation mechanism. Date/Author: 2026-11-10 / ExpressionReuseEngineer.

Decision: retain direct standalone constant/column returns and defer cast target lookup. Rationale: avoid a new leaf buffer and preserve observable reader order. Date/Author: 2026-11-10 / ExpressionReuseEngineer.

Decision: allow only one exact owned-storage restart after estimate-only preflight differences, before timing. Rationale: same-binary B differed too; keep every failed observation and require full plan equality on retry. No accepted-plan normalization. Date/Author: 2026-11-10 / ExpressionReuseEngineer.

Decision: withdraw candidate2 after the complete paired window. Rationale: single-row deep-expression benefit does not establish no regression when scan CPU increases against both controls in all four rounds, and Decimal has an adverse direction too. Production net increase80 lines is not justified by this evidence. Changes cannot all be attributed either to source or noise. Date/Author: 2026-11-10 / ExpressionReuseEngineer.

## Outcomes & Retrospective


单行深16层CPU相对两个同版基线下降2.809%／2.631%，p50下降2.805%／2.551%，四轮均同向。扫描CPU上升3.811%／3.419%，四轮均上升；Decimal相对B的CPU上升1.188%，四轮上升。深4096行CPU相对A的中位变化仅−0.005%，没有四轮一致的收益。不能以单项收益验收整轮。

候选生产115增／35删，净增80行；测试75增／0删，全部撤回。本轮新增保留性能收益、生产及测试源码删减均为0。累计继承已核对的生产2134增／7293删、净减5159行，测试5492增／5684删、净减192行。文档及证据脚本不计入这些源码数。

源码恢复后沿用已验证保留版，没有重复构建测试。原统一路径且不损失性能的目标仍有效。本次本地DCO的范围仅为本结果记录，不含候选源码；报告、统计及原始资料先行交付，不推送。

## Context and Orientation


Main起始HEAD为091faadaac52d93c56e6165bd20367ae838f9f9f，SDK为f7672bf980143f2938fd0f900650a4c29091e70f。试验只改Main的rust/crates/tidb-expr/src/scalar_function.rs及兄弟SDK的components/tidb_query_datatype/src/codec/native_eval_type.rs。前者遍历Expression并读取常量，后者统一运算符／转换文本规则。

候选用标准Display借用接口把内部子节点写入原输出，fmt::Error在根部转成原有None并丢弃私有部分文本。叶／类型文本、上层分别请求的参数文本和最终缓冲扩容仍有分配。现有String接口保留签名并调用共享writer。

完整证据目录为/home/agent/.slock/agents/02fef0a9-76ce-4afe-9bce-73eebe82ff7d/evidence/path-unification/stream-diagnostic-text。详细中文报告、statistics-and-decisions.json、候选两份完整原文／差异、初始及候选输入、真实退出和独立复核均保留在该目录及交付档案中。

## Milestones


第一里程碑是机制验证：实际服务18显示旧静态分支的两个子树自有String返回和释放，改成静态／动态借用回调写入同一Formatter。实际GOT重定位指向core::fmt::write。整个代码段减少2432字节；旧二元String接口自身139→277字节，不能据此声称所有路径都更小、更快。

第二里程碑是行为验证：原2440个前端名称保留并新增1项；实际相关973项为972通过／1项既有失败，额外20项全部通过，两组不重叠。SDK表达式1161项为1159通过／1项既有STR_TO_DATE失败／1项忽略；另6项数据类型诊断通过。范围内lint、clippy和双版本格式检查通过。

第三里程碑是实测和决策：完整窗口支持局部收益，同时保留了CPU不利数据，按约定撤回。恢复证明是全部9521项输入及三个仓库HEAD／状态和受保护BUILD摘要与试验前完全相同，而不是仅目测两份diff。

## Plan of Work


历史候选先加入共享native_write_binary_expression和native_write_cast_expression，再用NumericExpressionText／NumericTypeText借用回调替换内部子树String。special DIV包装、叶文本、宿主来源认证和求值内核未扩展。具体候选差异作为证据保存，工作树当前已恢复，无需重新应用或重跑本候选。

## Concrete Steps


以下均从Main或SDK仓库根目录执行，使用父目录tools中的固定工具链包装器、空RUSTFLAGS和CARGO_BUILD_JOBS=1。完整实际参数、时限和产物路径由对应JSON收据记录。

    ../tools/cargo-tidb build --manifest-path rust/Cargo.toml --release --locked --offline -j1 -p tidb-server --bin tidb-server --no-default-features
    ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j1 -p tidb-expr --lib --no-run --message-format=json
    ../tools/cargo-tikv test --release --locked --offline -j1 -p tidb_query_datatype --lib diagnostic_ -- --nocapture
    ../tools/cargo-tikv test --release --locked --offline -j1 -p tidb_query_expr --lib --no-run --message-format=json
    make lint

前端973项显式清单在front-consumer-names.json，另20项在extra-front-names.json；使用保存的单元18逐项运行。SDK1161项从保存的实际单元运行。SDK clippy通过make run调用固定包装器与项目原有lint配置，限定datatype／expr测试目标并设TIKV_FRAME_POINTER=0，完整命令在sdk-clippy1.json；没有声称执行工作区make dev／clippy。

## Validation and Acceptance


实际服务18摘要为fcbb086dfd98b1fbdf931faa1a8fc25a08deeb70e7ebaf115b10f48d3b458c4c；前端单元18为42424fd71b8acf9e4928f71e7c8e46ba8ee3b49a1d7b9ade735a022cd8bfdf83。两个源文件各经nightly2026-01-30和2026-08-22的rustfmt --check --edition 2021 --config skip_children=true检查。

性能A／B始终是同一保存14（2d435bce2ba0ea2c153df91b5e6dbd82a5200d78c20bb09ee3dac8c993d75fea），C为18，F为原始原生99a4c3e566774ba9733bc9d54366cf1e9b85446f63fa9a7418bfef5d3daeddd2。存储始终为旧实际ELF355be0acec593a24bb43ac0f5e6b8e30aae354b7e63fdd3530f3f2895c41e029，没有随SDK源码重建。

六场景为深16层的1行／4096行、整数4096行、Decimal4096行、文本4096行和扫描4096行。每组至少4秒且30次完整取数，四轮平衡顺序，配置逐组读取为32／1024／向量化1。源代码、实际ELF及PID／出生标识固定；先在无自建服务时取得原16秒空闲观察，复核读取结束后再取得新观察。计时期间没有构建、测试或新采样。

首次24份观察只有五场景／15个比较的42个估计行数单元不同；其他完整计划字段、结果、字段、告警、配置相同，同版B也不同。measurement1实际退出1且未计时。唯一一次同数据存储重启后，新的24份严格对照通过。measurement2实际退出0／468.939秒，96组／113714次计时查询，192份前后记录全部核对，已知构建／测试／采样进程观察事件0。

统计为成对比率中位数和观察范围，没有控制差异相减、置信区间、内核ns/eval或普遍无回退结论。计时逐查询完整取数并检查行数，完整结果摘要／字段／告警在测前两次及测后一次检查。没有声称逐次计时查询都计算完整摘要，没有运行完整前端2441项、数据类型568项或新的分配失败注入。

## Idempotence and Recovery


恢复前逐字核对工作树文件匹配候选2，再仅恢复两份原文、移除本轮自建计划并比较全部输入及HEAD／状态／受保护文件。restoration.json记录实际通过。之后只重建本结果文档；SDK干净，暂存区无其他文件。

原始输出和已撤回候选必须保留。可变Cargo输出仍是撤回版18；下一次保留基线应继续使用保存14。自建数据目录已按所有权路径及结束PID清理，禁止以复现为由删除其他工作树、进程或原档案。

## Artifacts and Notes


REPORT.zh.md和statistics-and-decisions.json列出全部CPU、p50、p95、QPS、中位成对比率及范围；primary.jsonl和cell-observations.jsonl保留原始数据。首次101、尾行比较失败、告警比较失败、首次计划差异及唯一重启均保留。与本次相关的旧回放档案没有改写，也没有修复或掩盖原任务22的后置观察失败。

本结果记录不等于原性能目标完成。源码、真实产物、前端、SDK／质量及性能／恢复已分别通过独立只读复核。性能复核465e875e重算全部指标、192份记录内的288次完整观察及恢复清理，支持保守撤回。最终封存／本地提交的收据另记范围，不把只读审查当成新增测试。

## Interfaces and Dependencies


候选只使用标准fmt::Write／Display，未新增依赖或ABI；两项旧String返回接口保持签名。两项新增writer及前端借用类型已随撤回移除，当前源码是已验证保留版。

2026-11-10更新：完成全部技术步骤，记录真实局部收益及不利变化，恢复候选并汇总最终结果；报告、原始资料和本地文档DCO交付单独留据。
