# 私有Decimal载体布局：请求字节下降，直接输出耗时方向混合

本批按Designer180bc037及固定设计c6ca4e91完成一次私有布局调整。载体及Option实测112→80字节；owned向量、直接输出每次完整调用各少请求8064字节，分配次数不变。直接输出四轮配对耗时变化−2.33%至+1.88%，因此确认这一次布局减少请求空间，但不宣称稳定提速或统计显著。此固定结果结束Decimal单场景优化窗口，后续回到M1状态生命周期及完整迁移预算；默认SQL和扩大函数矩阵不在本批。

基线TiDB ea0f44480dc9d5fc593b4bf477a30b1e7d660e34 / SDK5582f0695404a40d3ae714aabef81438ef6c4172。候选原datatype固定于88e183218a66197bd4a790db96fa1e373b1b17ce，SDK固定于2781f6775939ea2700bd26563ae7f16cddec0344；测量程序与计划固定于beda64e0282826a09f6ab34027fb8d1043fc2e56。后续提交只添加报告、数据和核验材料。

## 改动与合同

唯一生产改动在原tidb-datatype的decimal/compact.rs：将declared_shape移入私有ArithmeticValue::Compact变体，Wide直接保留原Decimal及其字段。Wide列调用原Decimal::from_column，into_decimal直接返回Wide值，紧凑值返回时恢复该变体的形状；紧凑乘法结果仍清除形状。native CompactDecimal不变，未加入Box逐值分配、packed、unsafe或列外元数据。

没有修改数值算法、紧凑范围、回退顺序、输出、诊断或编译接纳。原有90项Decimal包括宽值、完整系数、隐藏精度、声明形状、负零和i128边界；列形状对照包含45位宽值及未声明／固定小数位，使用未改动的原Decimal::from_column作为预期。43项共享测试继续覆盖真实宽列、重复／乱序选择、空集、重绑、direct错误／别名与有损写出。没有以新矩阵替代原回归，也不把共享原算法的差分当成全面兼容证明。

## 实际大小与对齐

DECIMAL_BENCH_LAYOUT=1从两侧最终无计数程序打印真实size_of/align_of，不把字段和估算当作布局事实。x86_64，仓库Aug22工具链；本批实际rustc 1.100.0-nightly c656540d6467dee1381f0cbd882412d6bd1cd5ae，Cargo 1.100.0-nightly 514c56dd7。

| 类型 | before字节 | after字节 | 对齐before/after |
|---|---:|---:|---:|
| Decimal | 80 | 80 | 8/8 |
| CompactDecimal | 32 | 32 | 16/16 |
| DecimalArithmeticValue | 112 | 80 | 16/16 |
| Option<DecimalArithmeticValue> | 112 | 80 | 16/16 |
| ExpressionScalar | 112 | 80 | 16/16 |
| ExpressionVector | 32 | 32 | 8/8 |
| RpnExpressionNode | 240 | 208 | 16/16 |
| RpnStackNode | 160 | 160 | 16/16 |

RpnStackNode仍由其他最大变体决定空间，未随标量缩小；不把全部节点或峰值内存一概称为下降。请求字节是分配器累计申请量，含realloc请求，并非存活字节或RSS。

## 同一路线完整调用

沿用column_mul/64，58个非NULL的30位乘积及6个NULL、完整输入和Chunk输出断言不变。两侧同一最终测量源码且均启用direct_chunk；每轮五路顺序固定，版本顺序四轮交替。每单元20000次计时，32次预热；另一个分配程序每单元计数100次，计数程序的100次计时不用于速度结论。运行器绑定CPU23，机器未隔离；opt3、无LTO、16 codegen units、原System分配器。没有新采样或额外场景。

| 路线 | before ns min/median/max | after ns min/median/max | 配对变化最小～最大 |
|---|---:|---:|---:|
| native_vector | 6287.049/6303.058/7158.114 | 6290.889/6338.252/6349.934 | -11.290%～+0.671% |
| native_scalar | 22162.222/22340.292/22450.097 | 22329.234/22439.110/23240.511 | -0.526%～+3.521% |
| shared | 10120.114/10162.670/10286.774 | 10002.310/10056.558/10094.591 | -2.765%～-0.560% |
| shared_scalar | 20563.122/21232.676/21473.904 | 20047.270/20380.613/21330.152 | -6.644%～+0.470% |
| direct_chunk | 8188.415/8244.824/8509.745 | 8229.445/8306.088/8386.042 | -2.331%～+1.877% |

shared是保留owned返回值的Vector入口；shared_scalar是相同owned API的Scalar调用；direct_chunk是另一输出API，只与其before同接口比较。owned向量四轮均略低；其余路线方向混合，不把一次波动归因于布局。direct候选对同轮native向量仍高29.60%～32.31%，不得用参照路线的波动美化这一差距。

| 路线 | before分配次数/请求字节 | after分配次数/请求字节 |
|---|---:|---:|
| native_vector | 12/14976 | 12/14976 |
| native_scalar | 58/1740 | 58/1740 |
| shared | 71/36540 | 71/28476 |
| shared_scalar | 314/115804 | 314/102428 |
| direct_chunk | 12/29616 | 12/21552 |

owned向量和direct各减少8064请求字节；owned Scalar减少13376，native两路保持原次数和字节。所有五路次数均不增加。四轮未出现同一路线全部退化，但不能由四轮证明不存在微小真实退化；正向结论限于布局和本场景确定的申请空间节省，速度收益仍按原表限定。

## 固定来源与检查

before直接库沿用task43最终after的十二份留存rlib/rmeta，来自其固定产品输入；这不是本次全量重构建。使用当前最终测量源码重链接plain和alloc。after仅执行一次Cargo构建，先复制并哈希核验十二份直接库，再从副本链接两种程序，之后才测量；不会第二次Cargo替换同路径库后继续使用旧收据。同版本两种程序的十二份库逐一相同，四个最终二进制均核验，计数包装只存在于alloc程序。库副本及所选后缀输入都不是完整构建闭包。

before-plain-preformat及before-alloc-preformat保留了测量源码格式化前的链接尝试，没有纳入本次数据；随后重链接两种基线，最终四程序使用相同测量源码。runner-old-mode验证四路旧基线／五路新接口混合，runner-four-routes验证原两侧四路模式，缺少before时新开关明确拒绝。正式数据精确校验40个计时、10个独立分配单元及每个版本／轮次／路线；历史36／9汇总脚本原样保留。

独立SDK不依赖相邻TiDB或Git objects alternates。双Cargo图分别只有一个datatype及其error、hack、kvcache、mysql所有者身份；本地图native/SDK共用相同本地datatype，独立图固定Git88e183。172份五包文件／资源及1421项SDK所选源码与当前版本一致。五包目录文件核对不含所有包外build输入；5526项后缀清单也不等同完整构建依赖。

本批实际通过：43项shared、8项prepared、90项datatype Decimal、1项额外field_type_source Decimal集成筛选、10项Chunk、1项原生Decimal向量（含加减乘／整数／cast）及429项独立SDK。限定Clippy有既有警告但退出0；根make lint、rustfmt、Python语法及diff检查退出0。没有新产品失败到成功的声称：这是布局重排，原回归保留。

完整SDK make clippy没有在本批重跑；沿用task43实际exit2／19项advisory错误及未进入完整workspace Clippy的限定。旧基线与task43观测到相同公告ID，但未固定公告库revision或逐字快照；不宣称两次公告库完全相同或安全检查通过。本批不更新无关依赖，不验证macOS、完整SDK workspace、默认SQL吞吐、生命周期或全部函数覆盖。

## 复核命令与材料

两仓固定检出后，TiDB rust/运行：

    cargo test -p tidb-expr --features shared-kernel --lib shared_kernel --locked --offline -- --nocapture
    cargo test -p tidb-expr --features shared-kernel --lib constant::tests::prepared_ --locked --offline -- --nocapture
    cargo test -p tidb-expr --features shared-kernel --lib evaluator::tests::projection_decimal_arithmetic --locked --offline -- --nocapture
    cargo test -p tidb-datatype decimal --locked --offline -- --nocapture
    cargo test -p tidb-chunk --lib decimal --locked --offline -- --nocapture
    cargo clippy -p tidb-expr --features shared-kernel --lib --locked --offline

独立SDK根运行cargo test -p tidb_query_expr --lib --locked -- --nocapture；TiDB根运行make lint。实际环境及命令、终态在本地evidence/carrier-layout的run.py、validate.py、sdk_checks.py及对应JSON／日志；构建前提沿用独立SDK说明中的GCC14、CXXFLAGS、CMake和普通声明工具链，未恢复个人Cargo包装器。

在TiDB根，已提交CSV可直接重算：

    python3 rust/docs/shared-kernel/summarize-carrier-layout.py

本机完整证据目录为/home/agent/.slock/agents/02fef0a9-76ce-4afe-9bce-73eebe82ff7d/evidence/carrier-layout。measure.py记录两侧direct命令、开始／结束时间及退出码；link.py、build.py和verify_graphs.py核对固定库／二进制。复测时使用新输出目录，不覆盖本次原始记录：

    python3 rust/docs/shared-kernel/run-decimal-bench.py --before <before-plain> --after <after-plain> --columns --direct-chunk --before-direct-chunk --rounds 4 --iterations 20000 --output <new-timing>
    python3 rust/docs/shared-kernel/run-decimal-bench.py --before <before-alloc> --after <after-alloc> --columns --direct-chunk --before-direct-chunk --rounds 1 --iterations 100 --allocations --output <new-allocations>

carrier-layout-validation.json固定5526项所选输入（摘要49b5e78606ec88163b76af71015a9482d13de633795b441522eab037ced88d0e）及105份日志／记录摘要；用已有verify-checkpoint-6.py显式传--receipt、--tidb、--tikv和--evidence核对。报告数据不包含旧task43比较或runner单次检查数据。

## 完整增删

本批相对ea0f4448／5582f06：生产+42/−27，测试与测量Rust+23/−0，构建+6/−6，文档／数据／核验+667/−1；全部+738/−34，增删772、净增704。相对原始两仓364aef2b／548812e1，源码与测试增删8039；含全部材料+28338/−398，增删28736。以两仓固定端点实际Git diff重算（count.py），不是逐批增删相加；新运行器、类型测量、汇总脚本、数据和收据均计入。

这些仍是部分函数候选和本批材料，不能证明完整245族、旧路径退役及10K数量级目标；一次布局结果固定后不再扩大Decimal单场景微调。
