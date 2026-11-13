# 真实Decimal列完整调用：表示成本与有限复用方案

当前证据指向输入和结果的数字表示物化，未找到可以在现有接口内小改消除主因的路径。
本批交付成本归因和下一步预算，**没有产品优化、性能改善或默认SQL接入结论**。
依据Designer963621fa及[固定设计39ba998c](https://github.com/YangKeao/tidb/blob/39ba998c22c4a54618bb2fa3f1f95e618cff8238/rust/docs/expression-shared-kernel-cost-review.zh.md)。
产品固定Main8f62b406e32f1edfdd1f40c6bb40e5094602bf54／SDK780ab3e776f47d634ac8963437baf8477a7b2df7。
测量源码固定1100e956dfb0d17e52949681fd7125b4c5dff48f，仅新增四个诊断切片；没有改变datatype、共享RPN或依赖。

## 完整调用复测

保持已有column_mul：两个真实Decimal列、64个物理位置、6个左侧NULL、正负交替，
58个非NULL乘积均需30位系数。仍比较实际native Scalar／Vector、shared Scalar／Vector，
含读取、求值、完整输出写回Chunk及临时值释放；prepare和填充输入不计时。
符号、系数、storage_scale、result_scale、声明形状及Chunk读回断言均保留。

先用未改动测量源码测得共享Vector中位数18.874微秒、原生Vector6.623微秒。
增加可选诊断切片后，最终重复对照统一使用同一份新测量源码和同一个无包装二进制：
运行器的before／after只是两个重复标签，**没有优化后版本**。四轮正反顺序各一次，
每次四条路线各20000次，32次预热；以下汇总八次重复，所有原始轮次保存在CSV。
先前不同测量源码的试跑也保留在本地记录，不当作前后产品改进。

| 完整调用路线 | 最小／中位／最大微秒 | 分配调用／次 | 请求字节／次 |
| --- | --- | --- | --- |
| native Vector | 6.570／6.590／6.614 | 12 | 14976 |
| native Scalar | 22.219／22.455／23.012 | 58 | 1740 |
| shared Vector | 18.871／19.173／19.700 | 70 | 23796 |
| shared Scalar | 30.622／30.974／33.804 | 371 | 96596 |

同一次运行的shared Vector／native Vector为2.861–2.995倍，中位2.905倍。
这些是当前有限场景的剩余差距，不把历史2.43–2.87倍照搬为新数据。
分配由另一个有包装的程序每路独立计100次；其计时不参与速度结论。
实际仍使用SDK的System分配器，启动时3次／288字节自检通过；字节是累计请求量，不是峰值。

## 成本证据及边界

仅对无包装初始基线的shared和native_vector完整调用各采样一次，perf cycles:u、199Hz、
DWARF8192，同一CPU绑定，分别7498和2616个采样记录，丢失样本均为0。
在固定设计发布前运行器已按200万次调用完成，故实际不是后来示例的20万次；
保留真实命令、次数及perf.data，不重复扩大采样。采样运行的时间不作为性能数据。
优化构建的内联和不完整栈回溯使部分Children与Self重合；仅使用直接Self比例，不虚构完整调用树。

共享完整入口中，MyDecimal::to_decimal_parts直接占15.91%，DatumCell物化9.21%，
MyDecimal::from_raw_bytes8.60%，Decimal::mul_mysql10.59%，输出from_decimal_parts6.01%。
数字格式化from_unsigned5.19%、u128格式化3.57%，from_utf8验证11.05%；后者横跨阶段，
不全部归给输入或算术。直接分配／释放函数约3.29%，不足以把全部差距归因于多58次分配。
可单独识别的RPN eval_input_mode／eval_one_node／eval_group直接占0.27%；内联及栈限制
意味着这不是调度总成本的精确上界，也不能把剩余时间全叫作调度。
原生Vector的from_scaled_i128占33.53%、from_raw_bytes22.05%；它保留紧凑系数并写回，
没有共享路径的系数ASCII物化链。源码及采样均未证明使用SIMD。

同一输入上的四个可选诊断切片另测四轮；它们的临时所有权与完整入口不同，不能精确相加或相减：

| 诊断切片 | 中位微秒 | 含义 |
| --- | --- | --- |
| 输入物化 | 9.383 | 两列经原Row::get_datum生成owned Datum集合并释放 |
| 原算术 | 4.653 | 预先物化操作数，直接调用原Decimal::mul_mysql、收集及释放结果 |
| 共享求值 | 16.935 | 原SharedProjection::evaluate，含输入、共享循环及owned结果，不写Chunk |
| 写回 | 1.934 | 预先算好的完整Decimal结果写回Chunk，含reset |

诊断算术返回值先与原Scalar完整字段比较，再计时。算术单独有59次分配：一个结果Vec，
加58个30字节系数的1740字节；输入切片3次／10288字节，写回0次。
这支持“多58次分配来自结果系数”的来源解释，但输入切片本身比原生完整调用更慢，
且采样显示主要工作还包括解码、ASCII构造、解析与校验。仅去掉分配不能消除这些转换。
不删除合法性检查，不仅调大inline容量（会扩大所有值且仍需数字转换），不新增scratch缓存。

## 原所有者复用方案：拟议接口与预算，尚未实现

[精确源码窗口](decimal-column-cost-inventory.json)记录当前86行native DecimalValue主体、
17行共享Decimal输入分支、23行原try_mul_mysql_fast、51行to_decimal_parts、34行SDK Decimal算术分支。
它们是受影响代码定位，窗口行数不是实际补丁增删，也不是新增算法许可。

建议在原tidb-datatype引入一个内部算术载体（拟名DecimalArithmeticValue），包含紧凑系数
和原Decimal宽值两种情况。紧凑部分必须保存storage_scale、result_scale、声明形状；
负零或不能精确表示的值保留原宽值，不以i128容量代替原Decimal接纳合同。
从原native DecimalValue提取既有紧凑primitive，删除旧副本；原Decimal快路径和原生Vector
共同调用同一所有者，溢出或尺度不适用则回到原Decimal算法，不回调Expression求值。
不得在SDK再抄一份乘法或把每个节点经Chunk往返。

拟议的最小值级接口是from_decimal／from_my_decimal、mul_mysql、into_decimal及
原所有者to_chunk_my_decimal。读取原FieldType及未指定fraction的规则需从原Row转换中
提取复用，保留完整形状；不是共享适配器另写一份转换。SDK ExpressionDecimal改为该原所有者
载体，原RPN调度仍执行现有指令；节点间保持紧凑或宽值，不改变NULL需求及诊断路径。

只提取算术仍不够：当前SharedProjection::evaluate出口要求Vec<Datum>，会强制重新物化
ASCII Decimal。因此需要在同一求值实现末端增加一个类型化根结果写回适配入口（拟名
SharedProjection::evaluate_into_chunk），直接调用原载体的Chunk转换；现有owned Datum入口
继续通过into_decimal保持合同。这个入口只选择根输出适配器，不能另建求值循环。
该方案跨越值载体、SDK类型身份与根输出合同，已超过本批一处小改动的范围。

下面是下一批的审查预算，不是已实现统计或性能承诺；以精确窗口及既有接口作为估算依据，
完整补丁超出预算时先复审范围，不拆开遗漏测试／文档来满足数字：

| 拟修改范围 | 预计新增 | 预计删除 | 说明 |
| --- | ---: | ---: | --- |
| datatype原所有者载体及共同primitive | 170 | 30 | 收拢紧凑计算，宽值保持原算法 |
| native旧紧凑实现及输入／根输出适配 | 65 | 100 | 删除86行旧主体，复用原转换合同 |
| SDK载体及算术委托 | 65 | 30 | 同一个共享循环，无第二求值器 |
| 定向兼容及差分测试 | 220 | 0 | 完整精度／形状、负零、溢出、NULL及旧输出 |
| Git依赖、锁文件、构建声明 | 5 | 5 | 原datatype固定后同步SDK来源 |
| 报告、基准和验证材料 | 120 | 0 | 继续同一真实列场景 |
| 预算合计 | 645 | 165 | 810行增删、净480；实现前需Designer确认 |

新增测试应复用现有41项共享、8项prepared、429项SDK及datatype覆盖；不能因紧凑载体
省略宽值、隐藏精度、错误与告警。若以后实施，datatype先固定提交、SDK锁定同一版本，
再验证两个构建图及完整调用；仅算术切片更快不算验收。本轮不实施该方案。

## 复现、验证与交付统计

[完整计时](decimal-column-cost-timing.csv)、[独立分配](decimal-column-cost-allocations.csv)、
[诊断切片](decimal-column-cost-phases.csv)随提交保存。summarize-column-cost.py以本地
证据目录为参数，重算全部结果并检查固定库及二进制对应；[验证收据](decimal-column-cost-validation.json)列出原始记录哈希。
用既有verify-checkpoint-6.py指定本批收据和证据目录可核对当前源码与记录。

本地evidence/decimal-column-cost保存所有初试、三份构建记录、12份库文件副本、两份perf.data、
完整CSV和终态。三份程序使用完全相同的12个实际库文件哈希；最终plain／allocation程序
使用同一个诊断测量源码，初始采样程序使用原测量源码，差异仅四个可选切片。
5525项输入摘要f22eeef2f758e6b0978c9b42f4815f24e1185aa11eda4a613280f2be41456408；
相对本轮基线只改变测量.rs，其余5524项相同。产品429／41／8沿用task41记录，没有重跑。
没有新增性能产品补丁，不改SDK Git来源或datatype。根make lint、格式、diff及脚本核验结果见收据。

实际普通Cargo／rustc为declared Aug22，sys前提沿用task41：GCC14、CXXFLAGS=-include cstdint、
CMAKE_POLICY_VERSION_MINIMUM=3.5、jobs4。构建从rust/目录执行，opt3、LTO off、codegen16，
不是产品thin-LTO。CPU由运行器绑定为23，机器未隔离；计时期间没有本任务并行编译或采样。
脚本的全部实际命令保存在build结果、run-receipt和profiles.json；DECIMAL_BENCH_ONLY仅用于采样。

实际交付相对本轮基线：生产0、构建0、测量测试+73/−0、文档／数据／核验+906/−0，合计+979/−0（净979、增删979）。相对原始基线，源码及测试7103行增删；全部+26066/−222，共26288行增删。以上计数包含全部已跟踪材料，与尚未实施的810行方案预算分开。
