# 共享表达式：REGEXP_LIKE执行内按需准备

已接纳的native REGEXP_LIKE现在对严格字面pattern及省略／严格字面match_type，在第一次实际需求时准备一次，并在同一执行中跨行、跨批及BENCHMARK复用成功或原1105失败。所有参数求值和顺序NULL／UTF-8守卫仍执行；动态模式沿用逐次准备，PB保留原构建期准备。本批依据Designer871fddab和[39923820设计](https://github.com/YangKeao/tidb/blob/3992382008813742423a9d3b0ee634e78a0d100e/rust/docs/expression-shared-kernel-cost-review.zh.md)，提交限定复审，未获得人类验收。

固定源码及测试：TiDB [c3f6a19f1c](https://github.com/YangKeao/tidb/commit/c3f6a19f1c718d18a862ff23f43cd844b579ee35)，SDK [aafeec411c](https://github.com/YangKeao/tikv/commit/aafeec411c63f8d2da9b2d4fde33cd99b10c629a)。批前为dad1610309／00106bee；原datatype所有者仍为b6630d7bde，两侧Cargo声明及锁均未改变。

## 实现与证据边界

冻结程序只新增NativeRegexpLike节点，finish检查2／3个Bytes参数及Int结果。native编译器用Constant::literal_value确认Bytes／String字面来源，不运行旧求值器探测常量。字面NULL仍沿用原FnCall和守卫，不初始化准备；动态pattern或match_type也保留FnCall。相比拟定cacheable布尔字段，实际以两种既有／新增节点区分，避免给动态路径引入状态查找。手工拼造节点仍需遵守builder的字面来源合同，本批没有新增通用来源证明系统。

RpnExecution私有持有按本根指令位置索引的HashMap，首次执行到最终节点才建立条目，随执行对象drop释放。两根、同根两节点、两个执行相互独立；NoColumns及不支持seeded RAND的输入也可使用，不改变原RAND开关或状态。原RPN帧循环仍负责需求、分支、重复体和行回退；没有第二参数需求循环、宿主子树回调或全局缓存。

SDK impl_regexp::NativeRegexp复用原build_regexp与is_match。每次先检查参数NULL和UTF-8，再取准备结果；命中成功／失败都不越过这些检查。缓存错误只保存内层1105的code／msg，重建相同Custom；其他错误原样返回且不缓存，没有保存外层Display造成重复前缀。动态FnCall通过临时NativeRegexp调用相同主体，因此仍每次准备。原PB init_regexp_data、metadata mapper及旧native表达式缓存未改变。

显式执行对象跨Chunk保持准备结果；便利evaluate入口每次建临时执行，只在单次调用内复用。错误后已产生的变量效果保留，下一批NULL可以成功返回，再次正常subject重放同一编译错误。direct依旧在所有根成功后才改目标；错误时原输出不变。静态RpnExecution由前批实测64增至本批112字节（x86_64），增加48字节HashMap；条目、Regex和错误文本的动态分配另计，没有测量吞吐或分配收益。

## 检查与保留失败

| 检查 | 终态与覆盖 |
|---|---|
| 修复前运行时见证 | 相同执行两批共六行实际准备6次，期望1的断言失败；不是缺新API的编译失败 |
| 接线后的同一单执行见证 | 准备1次，通过；之后最终测试扩大为两个交错执行，合计2次 |
| 共享表达式 | 56项通过，含新增动态pattern／flags换批、乱序重复选择、两根、空／IF死分支、变量效果、缓存错误／NULL／坏UTF-8顺序及owned／direct恢复 |
| SDK准备状态 | 4项通过；私有状态及线程局部测试计数证明跨批、独立执行、同根两节点、BENCHMARK重复体、死IF无准备、错误重放和守卫优先级 |
| 原生正则选择器 | 24通过、1项原有benchmark忽略；含共享正则子集及旧native缓存／向量用例 |
| 独立SDK表达式库 | 436项全部通过，包含原PB非法字面模式构建期失败及动态参数原运行期测试 |
| 限定Clippy | Main tidb-expr/shared-kernel和独立SDK tidb_query_expr库退出0，有既有警告 |
| 格式及根lint | rustfmt、git diff、Python语法和make lint退出0 |

保留全部失败尝试：SDK新增夹具第一次使用不存在的add_int_fn_meta，改用现有arithmetic_fn_meta；随后自定义InputColumns错误返回Scalar，无法用于默认read_selected的向量提取，改回Generated vector后通过。Main最初测试feature写错，随后夹具列序号usize与原i64字段不匹配，分别修正。这些是测试命令／夹具失败；产品准备次数的基线反例单独保留。每次命令前保存两仓HEAD、完整binary diff及所选输入哈希，失败文件未被覆盖。

完整SDK make clippy本批按范围未重跑，仍保留task43的exit2／19项公告失败，不能称完整workspace通过。本次没有扩展正则函数、字符集、SQL默认共享路径、其他缓存、完整executor/session或新性能矩阵。有限准备次数证明不代表整体性能改善或全部原native缓存替代；完整迁移和全范围10K目标仍未完成。

## 固定来源及复核

双Cargo图的五个原owner身份各唯一，172份owner文件／资源和1421项SDK输入逐项吻合；独立SDK从已推送fork快进，无相邻TiDB、无Git alternates且工作区干净。独立测试重新编译变更的表达式库，复用同一声明工具链及既有依赖构建缓存，并非从空target全量构建。

收据保留124份记录、22次逐命令源码快照；最终所选输入5527项，SHA256为2aff43d40f33b6cc178f60cc65507a381a9f529c9643d8cf42256a047ac65202。

[收据](regexp-state-validation.json)索引home-gentoo本agent工作区evidence/regexp-state的原始记录，含各命令、退出码、时间和源码快照。验证器把每个HEAD加binary patch重建到临时Git index，用blob内容重新校验每个所选文件；最终源码也逐项检查。清单只含声明的源码／Cargo等后缀，不覆盖全部编译器、系统库、环境或构建输出，不把它称为完整构建闭包。无本轮性能二进制封存。

```sh
# 工作区根：仅复核留存记录，不运行产品
python3 rust/docs/shared-kernel/verify-regexp-state.py --root /home/agent/tidb/expression-shared-kernel --evidence /home/agent/.slock/agents/02fef0a9-76ce-4afe-9bce-73eebe82ff7d/evidence/regexp-state
python3 rust/docs/shared-kernel/count-regexp-state.py /home/agent/tidb/expression-shared-kernel
# TiDB rust/：共享定向集
cargo test -p tidb-expr --features shared-kernel --lib shared_kernel --locked --offline -- --nocapture
# 独立SDK根：完整表达式库
cargo test -p tidb_query_expr --lib --locked --offline -- --nocapture
```

<!-- REGEXP_COUNTS_START -->
| 本批分类 | 新增 | 删除 | 增删合计 |
|---|---:|---:|---:|
| 生产 | 124 | 20 | 144 |
| 测试Rust | 507 | 0 | 507 |
| Cargo构建 | 0 | 0 | 0 |
| 全部文档／核验／收据 | 723 | 0 | 723 |
本批全部+1354/−20，增删1374。相对原始两仓，源码测试+9903/−324，增删10227；全部+32326/−427，增删32753。
<!-- REGEXP_COUNTS_END -->

按两仓固定端点默认Git diff统计新增＋删除，生产、测试、Cargo和全部材料分别列出；测试模块外的cfg(test)准备计数也归测试。相对原始端点重算，不能把历史增量相加充当总量。全18组迁移预算和尚未退役的运行时由Designer继续核对，本批不记未发生的删除信用。
