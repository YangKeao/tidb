# 真实Decimal列与跨Chunk复用

本轮完成限定的真实Decimal列接入：完整原生FieldType保存在编译列绑定中，
实际被需求的单元通过原Row::get_datum读取，再进入原共享RPN算术和IF。
同一投影换Chunk、换上下文、错误后再次调用的定向对照通过。
这不是参数标记绑定、完整M1生命周期或默认表达式入口切换。

源码固定为TiDB `b4a6785049529f398b79f22b813647ab85d7e2c5`；
SDK仍为`d822fbf9fbee8e0b635974a8d38e42f9e73f067a`，本轮没有SDK改动。
上一交付为TiDB `9227639ffb7d8f254cf35850b41c0c05c53000b8`。
执行依据为Designer消息9355929a及
[固定设计5dd037](https://github.com/YangKeao/tidb/blob/5dd0373540474325f5b4ba66911fe6093a893f21/rust/docs/expression-shared-kernel-cost-review.zh.md)。

## 实现与接纳边界

只修改`tidb-expr/src/shared_kernel.rs`的既有绑定和输入读取。
绑定由`(index, EvalType)`改为`(index, 原FieldType)`；相同物理列若完整字段不同，
明确拒绝编译，不按求值类型合并。长度、小数位、64位标记、字符集／排序规则等
保留在原类型中；RPN schema仍仅提供这里已接纳的类型分派。
Decimal列接纳补齐schema和ExpressionVector::Decimal输入路径，旧Int／Real／Bytes
接纳规则及数组拒绝保留。原unsigned算术接纳没有扩大；unsigned裸Decimal列可以读取。

Row::get_datum负责MyDecimal存储转完整Decimal以及声明形状恢复；未指定尺度时
使用存储单元的digits_frac。没有复制转换算法，没有调用旧Expression求值作为产品
后门，没有改原datatype算法、共享求值循环或PB表达式逻辑。
Decimal的节点中间值仍是同一个原Decimal对象，不经Chunk格式往返。
输入仍须遵守原生Chunk的存储合同；这里不声称任意宽中间值都能放入存储列，
也未新增不可信任意字节或不同物理类型Chunk的校验协议。

没有prepare预读、跨调用值缓存、宿主子树回调或第二个求值循环。
每次调用提供新Chunk与上下文；只物化共享循环需求的物理行。
同一物理列声明不同现在可能被拒绝，这是明确的候选接纳边界，不静默采用首次声明。
默认生产入口仍未切换到这份候选实现。

## 功能验证

已有31项共享原生测试继续运行，在同一测试模块增加2项组合回归，最终33项通过，
1323项被过滤；SDK表达式库429项全部通过。新增检查分别覆盖：

- 裸Decimal列、列加法、IF内列乘法；NULL、正负、零、24／25位系数及40位宽系数。
- 声明长度65、小数位4／未指定尺度，以及高于32位的标记；另检查unsigned裸列。
- 完整符号、系数、存储尺度、显示尺度、声明形状、显示文本；原生Scalar和共享
  Scalar／单行Vector／多行Vector结果，以及写回Chunk后再读取的完整字段。
- 物理行乱序重复、Chunk自带selection与调用选择不同、空选择、越界失败后重用。
  同一投影循环使用不同Chunk，返回值在输入销毁后仍有效。
- 同一物理列声明长度、小数位、flags、charset冲突时拒绝；数组拒绝；
  未选中的IF分支引用不存在的Decimal列不会读取，选中后才失败。
- 列参与`((a/b)*1)/100`的隐藏精度链；8／7分支仍为0.01142857。
  同一投影和RPN上下文依次接受严格除零错误、宽松除零警告、成功调用及
  除法精度4／8的不同原生会话上下文；每次与原入口完整值、错误1365和警告对照。

修复前新增实际列用例失败，原因是“column type is outside shared input admission”；
原始失败源码和日志保留。其后有一次使用不存在的tipb eval_type方法导致编译失败，
改用已有EvalType::try_from接口。新增unsigned算术期待也失败，因为原有算术接纳明确
拒绝该类型；未扩大产品范围，而改为检验unsigned裸列字段和值。两次失败均未记为通过。
另一次从仓库根目录运行Cargo遗漏rust/.cargo配置，触发无谓依赖重建；主动终止该
进程及其后代，退出143，改从rust目录执行。原始日志与终态均保留。

## 一组真实列完整入口测量

只在既有decimal-shared-bench测量器增加`DECIMAL_BENCH_COLUMNS=1`模式。
64行、两列Decimal、声明长度65／未指定尺度，计算列乘法；第一列含6个NULL及
交替正负的`(123456789+行号).987654321`，第二列为`1.000000000000`。
先比较完整原生Scalar值与两种共享调用结果，再比较原生Vector、原生Scalar及
共享结果写入Chunk后的字段。原六个常量场景代码和断言保留，另以1次计时完成
120个单元预检；这不是重做常量性能矩阵。

原生Vector为EvaluatorSuite实际入口；它保留原Decimal批量快路径和回退规则。
显式原生Scalar逐行调用Expression::eval。共享Vector／Scalar通过同一SharedProjection，
由原会话开关选择。所有测量包含结果容器使用与Chunk写出、输出reset及临时结果释放。
准备／输入填充在循环外，输入和输出容量复用，故测量是已准备投影的重复调用，
不能作为包含prepare、输入构造、网络或SQL的总耗时。

同一优化二进制跑4轮，每个完整入口每轮20000次，32次预热；固定CPU23，
每轮路由顺序固定为原生Vector、原生Scalar、共享Vector、共享Scalar，未随机化。
计时二进制无分配包装器。另用带包装器二进制各统计100次分配；
该运行的单次计时不用于性能表。机器未隔离，不作统计显著或因果归因承诺。

| 64行完整入口 | 四轮最小／中位／最大ns | 每次分配调用 | 每次请求字节 |
| --- | ---: | ---: | ---: |
| 原生Vector | 6562.9／6591.8／7729.3 | 12 | 14976 |
| 显式原生Scalar | 21284.4／21594.2／22088.9 | 58 | 1740 |
| 共享Vector | 18781.9／18846.6／18944.1 | 70 | 23796 |
| 共享Scalar | 31339.0／31561.4／32067.2 | 371 | 96596 |

共享Vector为同轮原生Vector的2.430–2.874倍、显式原生Scalar的0.858–0.887倍。
共享Scalar也明显更慢，分配开销更多。这里证明真实输入贯通，没有证明普遍提速。
上一版本拒绝真实Decimal列，不能编造该路径修复前共享计时，也不能把常量基线
冒充真实列基线。此前24位常量约20%的退化及原生向量差距继续列为未完成。
分配字节是包装器观察到的累计请求量，不是峰值或存活内存。

原始数据为[16个计时单元](decimal-columns-timing.csv)和
[4个独立分配单元](decimal-columns-allocations.csv)。在本目录可重算计时表：

    python3 - <<'PYDATA'
    import csv, statistics
    rows = list(csv.DictReader(open('decimal-columns-timing.csv')))
    for route in dict.fromkeys(r['route'] for r in rows):
        xs = [int(r['nanos'])/int(r['iterations']) for r in rows if r['route']==route]
        print(route, min(xs), statistics.median(xs), max(xs))
    PYDATA

## 构建、检查与复核

沿用相邻固定检出和tools/cargo-tidb包装器：nightly-2026-08-22、rustc1.100.0-nightly，
GCC14，locked/offline；测量opt3、LTO关闭、codegen-units16，并非生产thinLTO配置。
SDK默认Jan30工具链的MSRV失败和相邻TiDB依赖仍未解决，本轮没有重跑或宣称修复。
正式SDK make clippy的历史中断仍未完成；本轮只有限定库Clippy完成，带既有警告。
没有修改datatype算法，因此未机械重跑89项datatype检查或它的性能矩阵。

从TiDB的rust目录执行（完整原始日志带CHECK_EXIT=0尾标）：

    CARGO_BUILD_JOBS=4 ../../tools/cargo-tidb test -p tidb-expr --features shared-kernel --lib shared_kernel_ --locked --offline -- --nocapture
    CARGO_BUILD_JOBS=4 ../../tools/cargo-tidb test --manifest-path ../../tikv/Cargo.toml -p tidb_query_expr --lib --locked --offline -- --nocapture
    CARGO_BUILD_JOBS=4 ../../tools/cargo-tidb clippy -p tidb-expr --features shared-kernel --lib --locked --offline

根目录make lint通过；修改Rust文件的rustfmt检查、Python编译检查和git diff --check通过。
完整M1、参数重新绑定、RAND销毁重建、245族完整覆盖、正式完整Clippy、默认集成和
SQL性能均不是本轮通过项。签名／接口范围仍以现有编译器实际接纳为准。

[验证收据](decimal-columns-validation.json)记录固定源码、检查终态、数据和本地留存哈希。
5525项源码／构建输入摘要为
`8e42abb9f52923197d4fa01221dae96e6f22070636ffb3396a47101765f903ea`，
测试前后工作树及固定Git对象逐项核对一致。输入清单不覆盖所有外部依赖和环境，
本轮也不是独立可复现构建证明。两份二进制共用同一测量源码及12个库产物，
构建收据新增逐库哈希，实际链接库和二进制已留存；nm确认计时程序无包装符号，
分配程序存在可达包装符号并通过3次／288字节启动自检。

本地证据位于代理工作区`evidence/decimal-columns`，包含原始失败／成功记录、
构建输出、测试输入快照、库产物和测量二进制。已有校验器可复核：

    python3 rust/docs/shared-kernel/verify-checkpoint-6.py --tidb . --tikv ../tikv --receipt rust/docs/shared-kernel/decimal-columns-validation.json --evidence /path/to/agent-workspace/evidence/decimal-columns

重新构建用既有build-decimal-bench.py，默认无包装计时，`--allocations`产生独立计数程序；
从仓库根目录用run-decimal-bench.py的`--columns --after <binary> --output <dir>`运行，
计数运行另加`--allocations --iterations 1 --rounds 1`。历史报告必须用其固定提交的脚本。

## 完整增删统计

<!-- counts begin -->
| 范围 | 生产新增／删除 | 测试新增／删除 | 构建新增／删除 | 文档与审计新增／删除 |
| --- | ---: | ---: | ---: | ---: |
| 本批TiDB | +34/−17 | +441/−4 | +0/−0 | +621/−4 |
| 本批SDK | +0/−0 | +0/−0 | +0/−0 | +0/−0 |
| 原始累计TiDB | +1056/−1 | +3089/−0 | +1867/−91 | +14933/−0 |
| 原始累计SDK | +1693/−57 | +47/−4 | +100/−9 | +15/−0 |
本批全部+1096/−25，增删1121行，净增1071行；其中生产净增17行。
原始累计源码与测试+5885/−62，增删5947行；全部文件+22800/−162，增删22962行。
<!-- counts end -->

所有测量Rust代码计为测试；CSV、脚本、报告及收据计入全部文件。
原始两仓基线仍为TiDB364aef2和SDK548812e；本批相对TiDB9227639／SDKd822。
复核命令为：

    python3 rust/docs/shared-kernel/count-datatype-reuse.py /path/containing/tidb-and-tikv 9227639ffb7d8f254cf35850b41c0c05c53000b8 d822fbf9fbee8e0b635974a8d38e42f9e73f067a

本轮结果支持交审真实列／跨Chunk切片，完整共享内核和总修改约10K的目标仍未完成。
