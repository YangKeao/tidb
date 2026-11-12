# 剩余逻辑复查：数值恢复收尾

本次静态复查固定在84df525b257f0cab76e4883a65be183095ac1a4f。审查范围是宿主恢复分派、其适配函数、立即调用闭包、状态消费和剩余dead_code标记，并非整个TiDB的无冗余证明。实现仅删除两处单次调用包装，生产净减6行；测试、接口和状态协议未改。

| 位置 | 结论与理由 |
| --- | --- |
| batch_numeric_parent.rs:530、batch_numeric_window.rs:250 | 已替换为直接调用。旧闭包只调用drive函数再返回同一结果，没有局部?或return；调用后的认证仍先于transition?，错误顺序不变。 |
| batch_hosts.rs:3062、3297及21个仅有一处文本调用的适配方法 | 保留方法内部task查找。上层认证确有可复用信息，但方法还取slot、row、阶段或参数位置；统一传递这些信息需要逐个扩大接口或新增承载结构。当前收益仅省一次查找，不据此引入跨35个方法的改造。 |
| batch_hosts.rs:3017、batch_ast_host.rs:50、batch_diagnostic.rs:311 | 35个分派方法中14个具有多处文本调用，其中resume_arguments已经显式接收已认证信息；其余13个还覆盖无参数启动、AST启动或诊断入口，不能只凭普通resume_sql路径删除校验。完整文本调用清单见adapter-call-inventory.json；该清单不是动态调用图证明。 |
| batch_hosts.rs:1042及各store.get/authenticate | task验证代次和存活状态，值验证还约束node、row和角色；两者不是重复验证同一个事实。回调可重置值存储，回调前后的验证不能合并。 |
| batch_stateful.rs:133、batch_json_schema.rs:106、batch_final_state.rs:131/192 | 闭包用局部EvalError处理与提前返回隔开最终认证、租约释放、状态恢复和native_failure。直接将?移到外层会跳过这些步骤或改变错误域。保留。 |
| batch_typed_context.rs:292、batch_raw_values.rs:161、batch_regexp_values.rs:137、batch_context_value.rs:81 | 转换可调用上下文；先保存转换结果，之后复核原能力再传播错误。不是可直接删去的单次调用包装。保留现有控制流，避免仅为行数改成组合器链。 |
| batch_cached_list.rs:101/161 | 初始化分支的局部return区分构造失败与提交缓存后的查找失败，且后面仍认证输入。命中分支可单独展开，但会让两分支的LocalResult<Result<Datum,EvalError>>表达方式不一致，没有减少共享状态或校验；不为少量排版行数改写。 |
| batch_diagnostic.rs:630 | 闭包将缺少pending_constant转换成EvalError，再走native_failure；展开需重新安排错误传播而非直接调用，保留。 |
| batch_arguments.rs:305及Consumed状态 | 公共参数路径已经复用slot/row/next/tokens。next、token是否存在和TimestampDiff的NULL停止控制真实需求；消费状态阻止重放，不能当防御性噪声删除。 |
| rewriter.rs:1030、new_function.rs:277 | 两个项目级dead_code豁免属于结构准备实验入口，仍有真实准备测试调用；删入口需同时改变实验接口和覆盖范围。保留。 |
| batch_values.rs:803 | cfg(test)独立append-only值存储是认证比较的参考实现，不进入生产回退；保留独立测试基准，不能用生产实现替代它来减少行数。 |

上述位置均可在[固定源码](https://github.com/YangKeao/tidb/blob/84df525b257f0cab76e4883a65be183095ac1a4f/rust/crates/tidb-expr/src/evaluator/batch_hosts.rs)中复核。未对性能作静态推断；功能和性能结果必须使用此最终源码自己的实际回执。SDK、全量2441项测试及分配失败注入不因这次静态复查增加覆盖声明。
