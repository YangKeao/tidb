# 共享表达式：类型化DATE完整调用链

本批把已接纳Time／NULL的DATE贯通原值所有者和原共享帧循环。旧native、共享native和原PB DATE共用一个纯值算法，保留零日期、隐藏时钟及诊断差异；Time常量、真实Chunk列、IF汇合和两种根输出均接通。默认SQL、跨EXECUTE缓存、其他时间转换／函数和性能改善均未在本批证明。范围依据Designer b9f2f967及更正设计6e2c852f，task46提交限定复审，等待人类批准。

固定源码与测试：TiDB [6d88afb6a3](https://github.com/YangKeao/tidb/commit/6d88afb6a313ecf2f16fc676741cbeba9e2e7af9)，SDK [00106bee](https://github.com/YangKeao/tikv/commit/00106bee21d25fbf9ce18475a0400bebeaf4cdff)。原datatype算法先独立固定[b6630d7bde](https://github.com/YangKeao/tidb/commit/b6630d7bdeeccffa5262f5362bcb5cf88e6cec6b)，SDK Git及锁同步到该来源。批前TiDB为babdd5c90a、SDK为c2ef52b8；未改写旧checkpoint。

## 值、类型和执行边界

原tidb-datatype的Time::date_value接收两个零值拒绝位和清时钟策略。先判原值的全零／部分零，再清时钟，最后设Date/FSP0。native从原DateModes取拒绝位并总清时钟；仅no_zero_in_date时全零仍返回零Date，而零日期带非零时钟按部分零处理。PB拒绝零月／日，但原Date输入保留隐藏时分秒。没有新增日历范围验证、时区转换或时间解析器。

旧native的time_fn::date和SDK impl_time::date删除各自值处理片段，共用原所有者；诊断仍分别走Columns::handle_truncate与PB handle_invalid_time_error。SDK Time仍是原SDK类型，新命名的expression_memory_bits／from_expression_memory_bits只暴露unchecked内存位。桥在表达式层先按SDK语义kind、FSP及完整日历高60位构造原Time，结果按Date低位14回写；不调用带时区的packed存储接口，不宣称任意raw表示相同。

共享ExpressionScalar／ExpressionVector直接携带原Time，NativeDate节点进入同一eval_group／帧循环。常量只传值，DATE政策仍逐需求行执行；原生无DATE专用向量路线，非算术整根保持OrderedRows回退。因此常量拒绝的三行选择产生三条警告，Scalar／Vector都保留原诊断次数。InputColumns只新增date_modes声明式桥，截断策略继续使用已有接口；没有宿主子树求值或第二求值器。

接纳限于非array Date／Datetime／Timestamp来源及字面NULL。实际常量必须是Time或NULL；字符串／数值／Duration转换、参数与deferred来源明确拒绝。DATE只接一参及Date结果。IF允许三个时间声明归为同一求值族，选中值的kind／FSP／隐藏字段不按结果声明重写。原生列完整FieldType仍保存并逐列比较冲突，原生根声明由result_type完整保留，包括高位flags／flen／decimal；内部PB类型只用于执行家族选择。新增测试覆盖高位元信息、数组NULL及冲突声明。

NativeInput仅读被需求行，沿用原Column::get_time。owned恢复Datum::Time；direct沿用8字节时间列布局和Root::Datum暂存，全部根及转换成功之前不reset目标。RAND先成功、DATE随后严格报错时，旧输出不变且RAND推进保留；同一执行下一合法批继续。未改Chunk公共存储API。

## 被反例纠正的设计前提

| 同一低四位15 | 原合同与本批验证 |
|---|---|
| native unchecked常量 | Timestamp/FSP7；共享裸常量完整保留，typed DATE可清到Date/FSP0 |
| 原Chunk存储／读取 | 写入保留位，get_time→from_go_raw→check_fsp把7钳到6；裸时间列owned／direct都显式断言FSP6及原core／kind |
| SDK内存Time | Date/FSP0；桥先读语义类型，保留已有Date隐藏时钟，输出规范化低位14 |

原先“Chunk应拒绝／panic”的断言实际失败（51通过、1失败）；这是否定设计前提的证据，保留原日志并经Designer确认更正，没有改产品去满足错误断言。另一个独立边界是原native完整Expression评估FSP7时，既有Display按7位切片会panic；before-native记录该失败。旧行为固定测试随后仅对FSP7直接调用原typed DATE，其余走旧Expression。不能把这条有限常量／typed签名证据说成任意FSP7 SQL或存储保真。

## 验证及失败尝试

| 检查 | 最终结果 |
|---|---|
| 共享表达式 | 53通过；四种零日期模式、Ignore／Warn／Error、两种会话调用模式、重复乱序／空选择、两份Chunk、NULL、混合时间kind/FSP、IF死分支缺列及DATE诊断、元信息／拒绝边界、错误后继续、owned／direct |
| 原datatype Time | 34通过，含新增纯DATE政策及既有时间测试 |
| 原生时间／DATE来源行 | 21＋1通过；保留原参数cast与默认路径行为 |
| 独立SDK表达式库 | 432通过；原430＋隐藏时钟／零值固定预期＋PB诊断政策两项 |
| SDK Time位接口 | 1通过，16个低位值逐一核对语义元信息 |
| 限定Clippy | Main datatype／expr及独立SDK datatype／expr库退出0，有既有警告 |
| 格式／根lint | rustfmt、diff、Python语法及make lint退出0 |

旧native与旧PB在修改值算法前分别记录1项固定预期通过，避免只让两个新入口互相证明。新增数组NULL用例先失败，补DATE参数数组守卫后通过。其他失败全部保留：最初原native FSP7 Display panic；两次在Main工作区尝试运行外部SDK测试的feature／dev-dependency错误；PB提取首次codec错误尚未转为common错误导致编译失败；新测试夹具调用不存在的Time API以及一次过宽替换碰到Chrono方法；SDK诊断夹具把codec与common错误前缀混为一谈。均已修正相关实现／夹具，不能描述为首次全部通过。早期修改脚本与日志一起留存，脚本只是尝试记录，不作为最终产品安装入口。

完整SDK make clippy本批未重跑，保留task43已知19项公告失败，不能声明完整workspace检查通过。未重跑全产品／全executor／session集，没有新性能矩阵；默认SQL共享路径、其他时间族、旧运行时退役和全范围10K目标仍未完成。

## 固定来源、复核及全部增删

两个Cargo图各有五个原owner的唯一身份；Main使用本地patch，独立SDK使用固定b6630d7bde。172份owner文件／资源及1421项SDK输入逐项匹配；独立检出无相邻TiDB、无Git alternates且干净。Main Cargo.lock本批无需变化，因为原本已使用相同本地包身份；SDK Cargo.lock五包来源已更新。

收据保留79份记录；最终所选输入5527项，SHA256为39b41e5b18b46b8b1e03e8cc55bf44f095420821434d6ca200bb445aabf7847c。

[验证收据](typed-date-validation.json)索引home-gentoo本agent工作区evidence/typed-date原始记录。工具链沿用nightly-2026-08-22，ENV及target路径由留存run脚本指定。所选后缀输入清单不是完整构建闭包，也没有在本批固定性能二进制。

```sh
# TiDB rust/：主要验证；具体全部命令与退出码见收据索引的json/log
cargo test -p tidb-expr --features shared-kernel --lib shared_kernel --locked --offline -- --nocapture
cargo test -p tidb-datatype --lib mysql_time::tests --locked --offline -- --nocapture
# 独立SDK根：全部表达式回归
cargo test -p tidb_query_expr --lib --locked -- --nocapture
# TiDB根：仅重算留存材料，不重跑产品
python3 rust/docs/shared-kernel/verify-typed-date.py --evidence /home/agent/.slock/agents/02fef0a9-76ce-4afe-9bce-73eebe82ff7d/evidence/typed-date --root /home/agent/tidb/expression-shared-kernel
python3 rust/docs/shared-kernel/count-typed-date.py /home/agent/tidb/expression-shared-kernel
```

<!-- DATE_COUNTS_START -->
| 本批分类 | 新增 | 删除 | 增删合计 |
|---|---:|---:|---:|
| 生产 | 293 | 33 | 326 |
| 测试Rust | 678 | 0 | 678 |
| Cargo构建 | 6 | 6 | 12 |
| 文档／核验／收据 | 509 | 0 | 509 |
本批全部+1486/−39，增删1525。相对原始TiDB364aef2bab及SDK548812e1ef，源码测试+9292/−324，增删9616；全部+30992/−427，增删31419。
<!-- DATE_COUNTS_END -->

按两个固定Git端点默认diff统计新增＋删除，移动、桥接和测试都计入；脚本分列生产、测试、Cargo构建及文档／核验／收据。18组总预算由Designer维护，本批只更新temporal值政策、Time恒等cast接纳、IF控制及实际engine／integration接口成本，没有给未退役运行时记删除信用，也不把有限DATE调用链外推成全部时间函数可迁移。
