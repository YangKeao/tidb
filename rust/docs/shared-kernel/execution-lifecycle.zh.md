# 共享表达式：一次执行的状态所有权

本批闭合已接纳实验表达式的执行边界：冻结程序可以重复使用，有种子RAND由每次执行独立持有；同一次执行换Chunk继续推进，新执行从种子重建。原共享eval_group／帧循环仍只有一份，没有接入默认SQL或跨EXECUTE缓存，也没有性能改善结论。依据Designer消息7db0fd17及固定设计ce74fd2b，task45提交限定复审。

固定源码／测试：TiDB [ae8a20aa49](https://github.com/YangKeao/tidb/commit/ae8a20aa4945a4da6ca9f15c5baf2caa3734c67c)，产品实现为853e0cceaa，ae8a20aa仅补同一程序两个节点的断言，SDK [c2ef52b8](https://github.com/YangKeao/tikv/commit/c2ef52b8eb2edba0447eaf605d4d29e218b0671d)。本批之前为TiDB854d47d675、SDK2781f677；原datatype来源88e183218a未改。SDK先保存8504984，再提交测试导入修正c2ef52b；失败尝试保留，不改写checkpoint。

## 实现与成本

`SharedExpression/SharedProjection::start_execution(effects)`固定借用程序和本次Columns；后续evaluate／evaluate_into_chunk不再接收可替换的effects。单表达式无上下文便利入口仍保留。SDK的RpnExecution保存借用的RPN程序和懒初始化HashMap，键是本程序指令序号，值直接使用已有MySqlRng；每根程序各自持有状态。同一根内不同节点及不同执行均不共用槽。原eval_group移到执行对象，原帧循环只增加状态参数，不增加另一条求值器或复制随机算法。

Columns新增声明式supports_seeded_rand，默认false，真实StmtContext及对应测试会话true；在start_execution读取，投影每次只读一次。没有调用RAND探测能力。支持时常量种子首次被需求才建流；动态种子每次重新初始化，NULL种子按0处理。NoColumns仍每次取首样本。共享InputColumns桥不再提供rand_seeded_next或传裸节点地址，原生旧路径的map和键保持原用途。

无种子RAND、用户变量、参数、模式及告警仍来自借用的真实上下文；建立、结束或报错不清空它们。错误清理本次临时栈，已发生的随机推进保留，下一合法批可继续；直接输出在成功验证全部根之前不重置旧目标。程序／执行依靠Rust借用和Drop管理寿命，没有全局编号、lease、锁或新取消机制；同步入口未新增外部取消注入测试。

x86_64实测RpnExecution为64字节、SharedExpressionExecution为88字节、SharedProjectionExecution为48字节，均不含动态容器。投影创建每根64字节元素的Vec，有种子节点首次需求会分配HashMap；纯表达式也有投影执行Vec成本，不能声称零额外分配。销毁执行即释放其状态，不依赖宿主清表。现有测试和测量程序已迁移；重复批次／测量闭包外持有执行对象，诊断evaluate切片也如此。更换真实上下文必须显式新建执行。

## 运行时见证与验证

旧API在同一程序、同一effects上用两个新EvalContext求值，第一组为0.9065021936842261／0.37600881361962185，第二组为0.16053751777907602／0.6746611787701595；要求新执行从头开始的断言实际exit101。before-witness.patch及原测试文件保留。这证明实验API缺少新执行隔离，不是新API不存在的编译失败，也不是默认SQL已复现错误。改后相同程序／effects的两个新执行返回相同首组。

| 检查 | 最终结果及范围 |
|---|---|
| shared_kernel | 47通过；交错独立执行、两根同种子、换Chunk、空／死分支、动态／NULL种子、乱序重复选择、Scalar／Vector、两种输出、报错后下一批；保留既有数值及诊断检查 |
| prepared参数 | 8通过；已有参数按需／转换诊断规则 |
| 独立SDK表达式库 | 430通过（原429＋1）；新增BENCHMARK同一节点两批共6次随机推进及无能力不留状态检查 |
| 真实StmtContext | 独立executor集成目标1通过；真实Arc参数／变量／会话RNG存储贯通两批和新执行，参数42参与原Decimal转换，变量实际写入 |
| 实际PREPARE／EXECUTE | session定向1通过；RAND(5)每次为0.40613597483014313，RAND()从0.028870999839968048继续到0.11641535266900002 |
| 限定Clippy | tidb-expr、tidb-executor、tidb_query_expr库退出0，有既有警告 |
| 格式／根lint | rustfmt、diff、Python语法、make lint退出0 |
| 测量调用迁移 | 最终程序constants／columns／phases三次最小调用通过，iterations=1及原预热；校验完整值，不作前后性能比较 |

真实StmtContext测试在已依赖expr的executor层，由可选shared-kernel feature转发，没有expr反向依赖executor。实际prepared SELECT的dispatch.rs创建新的statement_context；stmt_ctx.rs借用会话rand_session，StmtContext构造新的原生seeded map。这项SQL测试走原默认路径，不能当作共享RPN已进入默认SQL的证明；共享真实上下文用例另行验证。

所有失败保留：第一次迁移后43通过／1失败，原因是测试把连续两批写成两次新执行，改为持有对象后通过；独立SDK新增测试第一次导入FieldTypeBuilder路径错误，修正后430通过。executor初选lib test目标因7处未改动的TableResolver测试构造器缺clause_message而无法编译，两份相关源码与批前字节一致；没有修补无关夹具，改用独立集成目标。集成测试首次用了私有模块导入，随后根参数及整数静态类型超出已接纳Decimal参数入口；修正测试为已支持的Decimal乘法及正确静态类型，未扩展产品参数接纳。首次图核验遗漏Main shared-kernel feature而找不到SDK包，随后启用该feature取得正确图。上述记录不能列为首次全部成功。

完整SDK make clippy没有重跑，保留task43的exit2／19项公告错误，完整workspace检查仍未通过；公告库当时未固定revision。未重跑全session／executor单元集、全产品、Decimal性能矩阵或默认SQL共享性能。本批成功的限定集不能替代这些检查。

## 固定来源与复核

两个Cargo依赖图各只有原datatype等五包的一个身份；Main共享本地owner，独立SDK使用固定Git88e183。172份owner文件／资源及1421项SDK输入逐一吻合；独立检出无相邻TiDB、无Git alternates且干净。最终测量程序、源码及12份直接库核对后复制保存；构建及三次调用只验证API迁移，不沿用旧性能数字。选择后缀输入5527项，SHA256为3f18a36b79cb03db493c4122cfcc19ff70d62dd3ad0317b7366e4d2e0ee6b7b9；该清单和直接库都不是完整构建闭包。

[验证收据](execution-lifecycle-validation.json)逐项索引留存证据。原始材料在home-gentoo本agent工作区evidence/execution-lifecycle，未覆盖task43／44原数据。执行：

```sh
python3 rust/docs/shared-kernel/verify-execution-lifecycle.py --evidence /home/agent/.slock/agents/02fef0a9-76ce-4afe-9bce-73eebe82ff7d/evidence/execution-lifecycle --root /home/agent/tidb/expression-shared-kernel
python3 rust/docs/shared-kernel/count-execution-lifecycle.py /home/agent/tidb/expression-shared-kernel
```

## 全部增删

本批生产+262/−117，测试／测量Rust+608/−172，构建+8/−0，文档／核验／收据+589/−0；合计+1467/−289，全部增删1756。相对原始TiDB364aef2bab及SDK548812e1ef，源码测试+8325/−295，增删8620；全部+29516/−398，增删29914。

按两个固定Git端点默认diff计数，测试模块及测试／测量文件分开，移动和接口迁移同样计入增删。完整18组迁移预算由Designer维护；本批只闭合已接纳程序的状态所有权，不代表245族迁移、旧运行时退役或全范围10K目标完成。DATE仍只是下一候选，未在本批实施。
