# TiDB / TiKV 表达式实现统一：架构、迁移与评估报告

> 报告基线：TiDB `364aef2bab5cc633ecb76a775ae8f36f86a6687d`、TiKV `548812e1ef57aef077a2062a9cc356640a6347f5`
> 当前实现：TiDB `a3cf8821f63bcb39460d4d7fc80db336b63fc3f9`、TiKV `196f8025f03f250bd70fe39553bb3c5e4ff74e5a`
> 报告检查点：`architecture-performance-report-217`

## 1. 摘要

这次实验的目标不是再写一个跨仓库表达式解释器，而是消除 TiDB 与 TiKV 之间重复的表达式算法实现：**TiKV 成为值算法和 RPN 执行内核的唯一 owner，TiDB 保留 SQL 宿主必须掌握的类型元信息、求值顺序、session/context、warning/error 投影和生命周期。**

总体依赖顺序和目标是自底向上：先统一 collation、Decimal、temporal 等值与类型基础，再建立 TiKV local RPN facade、严格控制流和有界资源协议，最后逐函数族切换 AST、typed scalar、PB、vector、Unistore 等入口，并在切换成功后删除 TiDB 内对应的 native 算法。实际 checkpoint 为加速 Demo 交错推进：runtime/control 与 Decimal 后续工作有并行重叠，M0–M6 是概念 workstream 和依赖关系，不是严格串行的提交阶段。禁止在 TiKV 拒绝或失败时回退到旧 TiDB 算法，否则同一语义仍会有两个 owner。

冻结分母为 245 个已经实现的 pure/contextual expression family；当前有 240 个完成了功能性 TiKV-only 接管和 native 算法删除，即 **240/245（97.96%）**。五个兼容成本过高的 family 暂时保留在 TiDB，但已收窄到一个闭合 `host_compat` 入口，不再依赖完整的 generic native evaluator。严格逐 family 最终审计计数仍是 0，`pr_ready=false`；本报告不把 Demo 完成表述成 release ready 或完整 Go package transcreation。

## 2. 总体设计

### 2.1 所有权划分

| 层次 | TiKV 负责 | TiDB 负责 |
|---|---|---|
| 值与算法 | collation/key/LIKE、Decimal 运算、temporal/JSON/vector primitive、数学/字符串/crypto kernel | 不再保留已迁移算法的第二份实现 |
| 表达式执行 | official RPN wrapper、local compiler、严格 selector、fixed recipe、batch/selection driver | AST/PB/typed expression 的接入、argument demand、结果投影 |
| 类型 | kernel 所需的 `FieldType`/`ScalarValue`/`VectorValue` 表示与检查 | 完整 SQL `FieldType`：flags、flen、decimal、charset/collation、ENUM/SET、array 等 |
| 上下文与副作用 | 接收显式传入的时区、precision、cache invocation、typed host request | SQL mode、statement clock、packet limit、warning sink、sysvar、identity、参数和相关列 |
| 生命周期 | worker/program/frame 的创建、执行、清理和资源限制 | session/request 级 pool、独立 statement execution、Drop/异常关闭 |
| 错误 | 结构化 admission/runtime/resource 错误及真实 TiKV cause | 转换成 TiDB SQL error/warning，保留错误时机和 warning 顺序 |

这一边界刻意区分“算法”和“宿主 effect”。例如字符串转整数前的 SQL coercion、TIMESTAMP 的 session timezone、deprecated JSON warning、未访问分支是否求值，都不是一个纯 kernel 能自行推断的；它们必须由 TiDB 明确选择或传入。相反，完成 coercion 后的比较、hash、Decimal 算术或 JSON primitive 不应在 TiDB 再实现一次。

### 2.2 三条核心约束

1. **唯一实现**：已迁移 family 的最终 kernel 只在 TiKV；TiDB adapter 不得重新实现该 kernel，但可以执行显式 child demand、SQL coercion、typed staged-host orchestration、context/effect 投影和表示转换，并须证明其中调用的共享 comparison/cast 等不会形成 native fallback。
2. **没有 native fallback**：admission、资源、桥接或执行失败直接传播；不能失败后再运行 TiDB 旧算法。
3. **兼容性不只看返回值**：NULL、unsigned、scale/FSP、collation、错误/warning 时机、child demand、selection 顺序、cache/rebind 和生命周期都是契约的一部分。

### 2.3 简化调用图

```text
SQL AST / typed row / PB / Unistore DAG
                  │
                  ▼
TiDB frontend: signature + FieldType + demand + coercion + effects
                  │
                  ▼
TiDB glue: checked value bridge / fixed recipe / execution scope
                  │
                  ▼
TiKV local compiler + official RPN wrapper + shared algorithm
                  │
                  ▼
ComputedValue / structured error / warnings
                  │
                  ▼
TiDB Datum + declared SQL metadata + SQL error/warning projection
```

对于重叠的真实 PB signature，wire builder 与 local facade 共享 `prepare_selected_call`、RPN metadata 和 kernel；wire path 不经过 TiDB。local-only operation 则走闭合 local selector，不扩大 wire admission。local facade 的意义是让 TiDB 在同进程内复用同一套 TiKV 实现，而不是复制 wire evaluator。

## 3. 接口设计和 glue 代码

### 3.1 跨仓库依赖和类型边界

TiDB 在 `rust/Cargo.toml` 通过相邻 worktree path 直接依赖 `tidb_query_crypto`、`tidb_query_datatype` 和 `tidb_query_expr`。这样共享的是 Rust 类型与函数调用，不引入 RPC，也不为 local-only 函数伪造 protobuf signature。

`rust/crates/tidb-datatype/src/tikv_compat/` 是 checked value boundary。它只做精确表示运输：不执行 SQL cast，不根据值猜丢失的 schema，不提前转换尚未 demand 的分支，也不把 Decimal/temporal/JSON 通过 string 或 `f64` 中转。完整 SQL metadata 仍留在 TiDB。

TiKV 使用 `types/function.rs::FunctionRef` 区分：

- `FunctionRef::TiPb(ScalarFuncSig)`：真实 wire signature；
- `FunctionRef::Local(LocalFunctionId)`：TiKV 自有、无 wire ID 的闭合 local function。

这避免了为本地复用“借用”一个不存在或语义不等价的 PB 编号。

### 3.2 local expression、编译和运行接口

`components/tidb_query_expr/src/local/spec.rs::LocalExpr` 是不可变 typed construction input，包含：

- `Constant`；
- `InputSlot`；
- `Call { function, args, return_type, metadata }`；
- `HostCall { slot, args, return_type }`。

它不持有 session、row 或 native expression callback。`LocalCompileContext` 和 `CompileLimits` 限制节点数与深度；过深树的析构使用迭代方式，避免被拒绝的输入在 drop 时再次栈溢出。

编译接口按可证明的语义域分开：

- `compile_local` / `compile_local_with_hosts`：普通 local program；
- `compile_local_profiled`：严格 profile；
- `compile_control_with_lineage`：控制流和结果 lineage；
- `compile_numeric_batch`：只接纳声明过的 numeric batch 图。

输出为 `LocalProgram`、`LocalControlProgram` 或 `LocalNumericBatchProgram`。不同 `ProgramEntry` 不互相冒充；空 batch 也需要验证 entry，而不是绕过 admission。

运行时的核心接口包括：

- `LocalBatch`：借用输入 columns、physical rows 和 selection；
- `LocalProgram` 调用内使用 width-one row scratch；`ExecutionLimits` 仅保存不可变执行上限；
- `LocalRuntimeServices::binding_schema/read_input/host_services`：只在实际 demand 时读取一个绑定值；
- `InputRow { occurrence, input_row }`：区分 selection 中的出现位置和物理行，保留重复/乱序 selection；
- `ExecutionLimits`：限制 steps、frame depth、active host tasks 和 retained bytes。

`LocalRuntimeServices` 返回的是值，不是 TiDB `Expr` 或递归 evaluator closure，因此 TiKV driver 不会暗中重新进入 TiDB expression evaluator。

### 3.3 fixed ready-value ABI

大量函数已经在 TiDB 完成了原有 child demand 与 coercion。为避免每个 family 自造协议，TiKV 提供闭合 ready-value ABI：

- `EvaluatedBytesOp`：有限 operation 集合；
- `EvaluatedArgs`：有限 typed shape，明确区分 nullable Int/Bytes、IEEE bits、Decimal、temporal、LIKE/regexp invocation 等；
- `prepare_evaluated_bytes` / `EvaluatedBytesWorker`：准备并复用 official RPN worker；
- `ComputedValue` / `EvaluatedBytesResult`：返回 owned typed result。

worker 在执行前校验 operation、shape、type 和 role；NULL 也进入真实 wrapper，不允许 TiDB 为 NULL 人工伪造一个“等价结果”而绕过错误、metadata 或资源时机。

### 3.4 TiDB glue

`rust/crates/tidb-expr/src/tikv/mod.rs` 汇总 crate-private adapter。通用 glue 位于历史命名的 `tikv/ready_value.rs`，它现在不只承载 ASCII：

- `ReadyValuePoolPolicy`：显式资源策略；
- `ReadyValuePoolOwner`：pool root；
- `ReadyValueExecution`：独立、可单独关闭的 statement/request execution；
- `ReadyValueScope`：一次词法调用的 affine scope；
- `evaluate_prepared_args_in` / `evaluate_args_in` / `evaluate_bytes_in`：准备参数、租借 worker、执行并投影结果。

`ScopedReadyValueColumns` 只覆盖 scope/execution capability，其余 `Columns` 方法全部转发给原 context，避免 adapter 丢掉时区、SQL mode、warning sink、参数、identity 等宿主信息。

family-specific glue 位于 `tidb-expr/src/tikv/{cast_*,date_arithmetic,interval,extremum,in_list,extract,...}.rs`。原 SQL frontend 仍在 `ops.rs`、`func.rs`、`scalar_function.rs`、`builtin_ext/`、`time_fn/` 中决定 demand、coercion、metadata 和 effect；最终 family kernel 进入 TiKV，TiDB glue 仍可按 TiKV staged request 执行宿主 coercion/effect，并通过共享 comparison/cast 路径完成所需子操作。

### 3.5 host protocol 与五个最终例外不是同一件事

TiKV local runtime 具有通用的 typed host protocol：

- `HostCatalog` / `HostSignature` / `HostSlot`：不可伪造的 catalog identity 和 typed slot；
- `PreparedHostCall`：编译器验证过的 call；
- `HostStep` 与 `LocalHostServices::{start,resume,cancel}`：宿主分阶段请求参数并返回结果。

该协议没有任意递归 callback；driver 释放借用后再计算被请求参数，task generation 防止错误复用。

当前五个延期 family **没有** 获得 TiKV/PB/Unistore admission，也不以这个 host protocol 冒充迁移完成。它们实际由 TiDB `rust/crates/tidb-expr/src/host_compat.rs` 管理：

- `eval(name, values, ctx)` 只匹配五个精确 name/arity；
- 未知名称返回 `None`，没有 generic fallback；
- `eval_json_schema` 是唯一 expression-level adapter，用于保留 schema cache 和 schema 为 NULL 时不求 document、不发生 I/O。

generic `builtin_ext::{json,info,crypto}` 的生产 match 不再接纳这五项，因此保留例外不等于保留一个完整 native expression evaluator。

### 3.6 错误和诊断接口

TiDB 的 `tikv/runtime_failure.rs` 与 `tikv/adapter_failure.rs` 分别保存 runtime 与 bridge/admission 错误类别、阶段及原始 cause。错误不能靠字符串匹配后重跑 native；TiDB executor 最终把结构化错误映射到 SQL error/warning。

这项设计同时解决两个问题：一是保留错误发生在 prepare、bind、execute 还是 cleanup 的信息；二是防止 adapter failure 被误认为某个 SQL 值域错误并静默走另一套算法。

## 4. expression 与其他模块的关系

| 模块 | 与 expression 的关系 | 本次改动后的边界 |
|---|---|---|
| `tidb-parser` | 产生 AST；SQL digest 例外依赖 lexer/normalizer | parser 语法与 normalization 仍属 TiDB；普通值计算不在 parser |
| `tidb-datatype` | `Datum`、完整 `FieldType`、collation/Decimal/time/json facade | SQL metadata 留 TiDB；底层表示与算法尽量复用 `tidb_query_datatype` |
| `tidb-expr` | AST/value/typed/PB dispatcher、coercion、metadata、adapter | 不再是已迁移 family 的第二算法 owner |
| `tidb-session` | statement 生命周期、参数、sysvar、clock、identity | `SessionReadyValueRuntime` 可显式安装实验 pool/execution，dispatch/record-set 负责关闭；默认 policy 为 `None` |
| `tidb-executor` | selection/projection/default/DML、statement context、错误投影 | 传递真实 execution/context；保留调度而非重复 kernel |
| `tidb-unistore` | PB/DAG、request flags/TZ/div precision、warning sink | 每 request 建 local owner；已迁移 signature 复用同一 TiKV program |
| `tidb-util` | AES facade、plan codec、password policy 等 | 已迁移 utility 退化为 facade；plan/password 两类仍是明确例外 |
| `tidb_query_datatype` | shared value、collation、Decimal、temporal/vector primitive | 值算法与底层表示的主要 owner |
| `tidb_query_expr` | official RPN、function metadata、local compiler/runtime、kernel | 表达式计算唯一 owner |
| `tidb_query_crypto` | AES 等 crypto primitive | TiDB 只保留 SQL mode/IV/warning glue |
| `tidb_query_aggr` / `tidb_query_executors` | TiKV 生产 DAG 的 aggregate/selection/projection/TopN 等 | 使用 strict builder，深层控制不退化到 eager |
| `tipb` / protobuf | wire signature 和完整 wire `FieldType` | PB origin 保留，不经 SQL 名称重写，不伪造 local ID |

### 4.1 生命周期关系

`tidb-session/src/ready_value_runtime.rs::SessionReadyValueRuntime` 只有在显式安装实验 policy 后才持有 session pool root；当前默认 policy 是 `None`，也没有在 production caller 中自动启用。启用后，每个外层 statement call 开始一个独立 execution，嵌套 call 不创建第二个 execution；captured closer 随 record set 转移，并在正常、异常或 Drop 路径只关闭实际 captured execution。Session Drop 关闭仍存活的全部 attached/detached executions。无 capability 的 AST/value 调用会为每次调用创建 one-shot owner/execution。

Unistore 不跨进程借用 session token。`tidb-unistore/src/cophandler/eval_context.rs::RequestEvalContext` 根据真实 DAG flags、timezone、division precision、column types 和 warning sink 创建 request owner，并在 Drop 时关闭。独立、ownerless 的 helper API 保持旧契约并按调用创建 one-shot owner；当前 session 默认也未安装 pool policy。不同 production caller 的实际调用频率和缓存层次没有在本次 microbenchmark 中建模。

### 4.2 metadata 与 PB 关系

TiDB lowering 是 value-free 的：动态 column 只产生 binding 描述，不读取值。sidecar metadata 保存完整 SQL `FieldType`、collation snapshot、source identity 和 PB origin，但不保存 executable child/native evaluator/cache。

PB path 由真实 `ScalarFuncSig` 选择实现，显示名只用于诊断。重写后 type 或 origin 不匹配时拒绝，而不是从 `Datum` 猜回 schema。迁移也没有为了提高覆盖率而新增原本不存在的 PB/Unistore signature。

## 5. 如何逐步替换

### 5.1 M0–M6

以下是设计上的 workstream、依赖和收口顺序，不是实际 commit 的严格串行时间线。为缩短 Demo 周期，不冲突的 datatype、runtime、lowering、family 和审计 checkpoint 曾并行或交错推进；每项只有达到自己的证据门槛后才收口。

1. **M0：冻结基线和分母**
   归并 aliases、operators、synthetic CAST、AST/PB/Unistore/helper 入口，冻结 245 family 分母，并记录每项的类型域、上下文、源实现、测试和最终 owner。
2. **M1：collation / LIKE**
   先迁最底层 compare/key/pattern 语义，区分 PAD SPACE、signed wire ID、byte/rune/collator policy、GB/UCA 特例，再删除 TiDB 权重表和 matcher 重复实现。
3. **M2：类型和值**
   先接 NULL/int/real/bytes，再接 Decimal、temporal、JSON、vector；禁止 string/f64 中转。Decimal 同时覆盖算术、比较、hash/group key、assignment 和 codec consumer。
4. **M3：local runtime 与严格语义**
   基于 official RPN 建 local facade；实现 lazy IF/CASE/COALESCE/NULLIF、深层 AND/OR、runtime-bound regexp、IN prepare/rebind、资源限制和结构化 diagnostics。
5. **M4：接通所有入口**
   AST/value、typed row、PB、vector/selection、fold/default/DML、executor、Unistore/TopN/aggregate 都指向同一 TiKV implementation；入口保留各自 metadata/effect。
6. **M5：按 family 迁移并同步删除**
   每项都完成“旧实现 → TiKV kernel → TiDB adapter → 全入口测试 → 删除 native body”；真正困难项进入显式 deferred 清单，而不是隐藏 fallback。
7. **M6：集成、成本和独立复核**
   冻结覆盖率，检查值/metadata/warning/error/cache/parallel/index bytes/执行来源，运行 TiDB lint、TiKV workspace check/clippy、core tests，记录编译与 width-one/batch 成本，并由独立 reviewer 检查。

### 5.2 单个 family 的替换模板

每个 family 实际按以下闭环推进：

1. 枚举所有入口及 overload，冻结 Go/source vectors 和当前行为；
2. 判断 TiKV 是否已有可复用 kernel；没有则只在 TiKV 实现一次；
3. 为真实输入域增加 `FunctionRef`/fixed recipe 和 typed carrier，不扩大 wire admission；
4. TiDB adapter 保留原 child demand、coercion、context、metadata 和 warning/error 投影；
5. 接通 AST/value、typed scalar、PB、vector、Unistore 和 helper 中实际存在的入口；
6. 对 NULL、边界值、selection、lazy/error/warning/lifecycle 做针对性测试；
7. 删除 TiDB native 算法和旁路；
8. 只有源码与调用图确认唯一 owner 后，才增加 family credit。

这种顺序避免了“先加一层 wrapper，但旧算法仍可达”的假迁移，也避免 formatter、leaf helper 或 test-only 共用被误计成整个 family 完成。

## 6. 遇到的问题及处理

### 6.1 SQL 兼容性不是纯函数等价

相同返回值仍可能在 warning 次数、错误时机、未访问分支、副作用次数或 metadata 上不兼容。IF/CASE、REGEXP、IN、temporal、JSON 和 packet-limited string 尤其明显。

处理方式是把 demand 与 effect 作为接口的一部分：selector 返回下一步需要的参数，`Columns` 提供真实 statement context，warning 仍写入原 sink；dead branch 不 cast、不 compile regexp、不访问 host。

### 6.2 TiDB 与 TiKV 的类型模型不完全相同

完整 SQL `FieldType` 不能压缩成 kernel enum；Decimal 的 visible scale、内部精度和 declared shape 不能通过字符串来回；TIMESTAMP 与 DATETIME 也不能只按结构体布局强转。

处理方式是建立 checked projection 和 exact carrier；schema metadata 留 TiDB，数值表示与算法进 TiKV。无法无损表示的 domain 明确拒绝，而不是静默截断。

### 6.3 row-major 与 node-major 的顺序差异

TiDB 原路径按行和 source order 产生 warning/error/volatile effect，RPN 容易按 node/batch 求值。简单扩大 batch 会改变第一个错误或 warning 顺序。

处理方式是只对证明安全的图启用 numeric batch；其他路径用 width-one 调用同一个已编译 RPN。selection occurrence 与 physical row 分开记录，重复/乱序 selection 不被重新排序。

### 6.4 lazy control 和深度退化

早期 production PB builder 在深度超过 32 时可能退回 eager；常量 REGEXP 也曾在 dead branch 中过早编译，IN metadata 可能重复 prepare。

处理方式是 production 构造点改用 strict builder，验证深度 33/256；CASE 以有界迭代 demand 运行；REGEXP 延迟到第一次实际调用，IN prepare once 并在 rebind 时失效。

### 6.5 lifecycle、缓存和并发

worker/cache 如果按行创建会带来明显成本；如果跨 statement 错误复用，又会污染 context。并发 pool 还出现过 cached poison 与 epoch/debt torn observation。

处理方式是 session/request owner + 独立 statement execution + lexical scope；nested call 复用当前 execution，另一个 live/detached statement 不会被新 admission 或 peer close 失效，clone/reset/failure retry 有独立测试。pool 状态使用一致的同步和 sticky poison，cleanup 失败不会让有问题的 worker 回池。

### 6.6 error provenance 与 fallback 风险

如果 adapter 只返回字符串错误，调用方可能把 infrastructure failure 当成 SQL domain error，或者为了“兼容”重跑 native。

处理方式是保留 failure class、phase、operation/profile 和原始 cause；no-fallback 是硬边界。只有真实 operation/profile 和本次输入 witness 才能投影成对应 SQL error。

### 6.7 PB、macro ABI 和跨 crate 可见性

strict builder 接入 executor 后，`rpn_fn` 宏展开曾因 `CallShape`、`CallArg`、`CallBuild` 等 private 项出现 18 个编译错误。

修复只开放宏跨 crate 必需的最窄 `#[doc(hidden)] pub` ABI；metadata mutation 与内部 registry 仍保持私有。随后 executor full lib 120 tests、aggregate 40 tests 通过。

### 6.8 native toolchain 和 workspace clippy

完整 TiKV clippy 最初被 CMake 4 删除旧 policy compatibility、GCC 16 与旧 RocksDB/Abseil include 假设阻塞。兼容环境使用 GCC 14、`-include cstdint`，以及只在 configure 阶段追加 `CMAKE_POLICY_VERSION_MINIMUM=3.5` 的 wrapper，没有修改 vendored dependency。

clippy 进一步发现 flate `find_match` 的 `tries` 未递减，这是实际转写 bug。移除 decrement 的 cyclic-chain regression 会超时，恢复 `tries -= 1` 后通过；这不是单纯 lint suppress。

### 6.9 既有红测和基线噪声

完整 `tidb-expr` 仍有 4 个历史失败，完整 Unistore 有 1 个历史失败；它们在冻结/恢复局部改动后仍可复现，因此没有用本次迁移偷偷修补或隐藏。报告兼容性时保留这些失败，不把 targeted green 升级为“全套测试全绿”。

### 6.10 五个难迁 family

- `JSON_SCHEMA_VALID`：validator、statement cache、file/HTTP `$ref` 和 no-I/O-on-NULL；
- `TIDB_DECODE_PLAN`：完整 plan codec/tree/text renderer 与 malformed raw fallback；
- `TIDB_DECODE_BINARY_PLAN`：Explain protobuf、递归 tree、warning/panic policy；
- `TIDB_ENCODE_SQL_DIGEST`：真正算法是 parser lexer normalization，不只是 SHA-256；
- `VALIDATE_PASSWORD_STRENGTH`：identity、七次有序 live GLOBAL 读取和 byte/Go-rune/Unicode policy。

目前保留这些算法，但已用闭合 `host_compat` 代替 generic native evaluator。未来必须先把 resource retrieval、codec error、parser bytes、identity/sysvar reads 设计成 typed staged protocol，再迁真正 kernel；传预计算答案或只包一层 adapter 不能获得 credit。

## 7. 兼容性评估

### 7.1 已执行的代表性 / targeted 验证维度

以下维度由不同的 focused suite 覆盖，不表示 240 个 family 每个都在每个入口和维度上完成了矩阵验证；严格逐 family final audit 仍为 0。

- 冻结 245 family 分母，240 个功能性 TiKV-only family；
- Go/source-derived vectors 和原有 immutable fixtures；
- AST/value、typed row、PB、vector/selection、Unistore 等真实入口中的已实现域；
- NULL、signed/unsigned、Decimal scale/FSP、collation、timezone、SQL mode；
- warning/error 次数与时机、lazy child demand、cache/rebind/clone/reset；
- selection 宽度 0/1/1024/1025、重复/乱序 selection；
- strict depth 33/256、CASE 1024 pairs；
- session/request pool lifecycle 和异常/Drop 关闭；
- TiDB `make lint`、TiKV changed-crate locked check 与兼容环境 full `make clippy`；
- executor 120 tests、aggregate 40 tests 及多个 family focused suites。

### 7.2 当前结论

在 accelerated Demo 的 targeted receipts 中，除下面的 MD5 release 问题外没有发现新的已确认差异：功能覆盖率达到 97.96%，没有为失败路径保留隐式 native fallback，五个例外显式列出且不计 credit。metadata、effect 和 lifecycle 被当作一等契约，而不是只比较结果值。这个结论是 focused evidence 的汇总，不是量化的全域兼容率。

但这个结论不是“全域等价”：strict per-family final audit 仍为 0；现有 focused tests 不能覆盖所有 SQL type/collation/context cross-product，也不能证明任意 PB tree 都可接纳。

本报告的 release 性能探针还发现了一个新的兼容性问题：同名、使用同一 MD5 row table 的 `test_md5_hash` 在 frozen release test binary 中通过，而 current release test binary 在 TiKV prepare 阶段返回 `ExpressionRuntimeFailure { class: InvalidSpecification }`。两个 binary 和测试 helper 源码并非字节相同；receipt 证明的是相同 MD5 行集合在两个 revision 上的结果分叉。该问题在此前 debug-focused gates 中没有暴露。因而 MD5 没有 current 性能数值，并且“release 环境未验证”已经从理论风险变成一个有实际 receipt 的问题；在修复并补充 release regression 前，不能把 **MD5 family/path** 表述为 release-compatible，这一 receipt 不单独判定其他 crypto family。

### 7.3 已知未验证和风险

- exhaustive differential、完整 release/TiFlash/FIPS 环境；
- allocator physical peak、OOM threshold 和故障恢复；
- 每个 family 的 strict final audit；
- 完整 Go package transcreation；
- 五个 deferred family 的 TiKV ownership；
- production server/sysbench 级端到端吞吐与长尾延迟。

因此当前状态保持 `pr_ready=false`。完整 `tidb-expr` 的 4 个历史失败和 Unistore 的 1 个历史失败也继续披露。

## 8. 性能测量

### 8.1 方法

本次新增了同机、同工具链的 frozen-before / current-after microbenchmark。测量边界是稳定的 TiDB `eval_in` AST/value API：SQL 只解析一次，解析不计入循环；每个 workload 先 warm up 2,000 次，再进行 9 个 sample。baseline 使用旧 native 实现；current 主结果使用 public AST/value 默认的 ownerless context：由于当前 Session 默认 policy 为 `None`，每次调用创建 one-shot owner/execution。另测一组显式注入可复用 `ReadyValueExecution` 的 pooled warm path，作为生命周期设计候选的 best case；它不是当前 production/default 路径。

最初的 one-shot probe 在两棵树中源码字节相同（SHA-256 `fdd542d9…c187e`）；最终可继续执行的 probe 只增加了“记录 MD5 错误后继续”的 harness 控制，不改变其他 workload 的表达式、循环次数、black-box 和输出检查。pooled 变体额外提供 `ReadyValuePoolOwner/ReadyValueExecution` context；baseline 没有这一 capability。各 revision 分别编译到独立 target，最终 test binary 在 CPU 2 上交错运行。临时 probe 和 detached baseline worktree在测量后删除，没有进入提交。

环境：AMD Ryzen 9 9900X（12 cores / 24 threads）、46 GiB RAM、Linux 7.1.8；TiDB nightly-2026-08-22，release profile，locked dependencies。最终对比运行固定在 CPU 2，但 CPU boost/scaling 保持启用。结果单位是每次表达式求值的 ns；它不是数据库 QPS，也不包括 parser/planner/storage/network。

正确性前置条件是 before/after 的 `Datum::label()` 一致。workload 覆盖整数加法、lazy IF、字符串比较、Decimal 加法、MD5、JSON_TYPE 和 REGEXP_LIKE。

三次独立进程按 before/after/after/before/before/after 交错运行；每个进程每项有 9 个连续 sample，因此每个成功项共 27 个观测，但真正独立的 process-level replicate 只有 3 个，进程内 sample 相关。下表给出全体 sample 中位数和全范围；全范围不是 confidence interval，本次没有计算置信区间或统计显著性。

**当前默认 one-shot 路径（主结果）**

| workload | frozen before (ns/eval) | current default (ns/eval) | current / before | 结果 |
|---|---:|---:|---:|---|
| integer add | 34.072（33.331–34.969） | 2,259.850（2,252.348–2,360.737） | **66.33× / +6532.6%** | `INT:3` 一致 |
| lazy IF | 94.867（94.532–95.944） | 4,334.148（4,303.269–4,383.556） | **45.69× / +4468.7%** | `INT:2` 一致 |
| STRCMP | 165.926（164.619–167.422） | 2,872.959（2,850.228–2,908.208） | **17.32× / +1631.5%** | `INT:-1` 一致 |
| Decimal add | 549.241（546.452–563.021） | 4,006.884（3,992.414–4,053.350） | **7.30× / +629.5%** | `DEC:124.00` 一致 |
| MD5 | 281.331（279.155–283.846） | **无数值：Prepare/InvalidSpecification** | 不可比较 | frozen `STR:b1b5...ddd9`；current release 失败 |
| JSON_TYPE | 237.289（234.354–238.858） | 2,172.428（2,157.067–2,803.819） | **9.16× / +815.5%** | `STR:OBJECT` 一致 |
| REGEXP_LIKE | 8,994.609（8,959.193–9,049.748） | 12,099.094（11,915.070–12,175.097） | **1.35× / +34.5%** | `INT:1` 一致 |

**显式 pooled execution（候选 best case，非当前默认）**

该组另以三进程 ABBA 运行。它与 native baseline 有意不对称，只用于估计显式复用 execution 后的候选下界：integer add 1,284.414 ns（37.50×）、lazy IF 3,360.578 ns（35.53×）、STRCMP 1,670.081 ns（10.13×）、Decimal add 2,818.449 ns（5.17×）、JSON_TYPE 1,393.691 ns（5.92×）、REGEXP_LIKE 10,695.672 ns（1.22×）；MD5 同样在 Prepare 阶段失败。该差值不能归因到单个 adapter、kernel 或 pool 组件。

结果明确显示：在所测 public AST/value default warm path 上，当前实现不是性能中性替换。越简单的表达式，固定 adapter/runtime 成本占比越高；REGEXP 这类本身较重的算法默认路径回退约 35%，而整数和 lazy control 的相对回退达到 46–66 倍。显式 pool 可显著降低固定成本，但仍没有接近旧 native 路径。MD5 current release 的问题属于正确性失败，不能用性能数字掩盖。

### 8.2 编译和内存成本

在两个独立空 target 中构建同一个 release `tidb-expr` libtest，均使用 4 个 Cargo job：

| 指标 | frozen before | current | 变化 |
|---|---:|---:|---:|
| Cargo `Finished release` | 2m30s | 5m30s | 约 2.20× |
| `/usr/bin/time` wall | 151.62s | 341.13s | 约 2.25× |
| 最大 RSS | 2,679,980 KiB | 3,252,576 KiB | +21.4% |

current build 的 Rust/C++ transitive closure 明显更大，这与引入 TiKV direct dependency closure 一致；但两个 revision 还包含其他源码、lock 和 dependency-graph 变化，这组 before/after 不能把增长单独归因于 direct TiKV dependencies。Cargo `Finished` 是较干净的 compile-only 标记；`/usr/bin/time` wall/RSS 还包含编译后的测试执行（baseline 约 1.35s，current 在约 10.60s 后因 MD5 返回 101）。该数据是本机 cold-target 记录，不是 CI 保证，也不能解释为 evaluator runtime。另一个较窄的 M6 dev check（四个 TiKV production crates）记录为 10.19s wall / 1,472,560 KiB RSS，两者范围不同，不能直接互换。

已有 M6 current-only 诊断复用一个 compiled Plus program 和 `EvalContext`，并逐调用复制同一 `ExecutionLimits`、创建新 budget/row scratch；10,000 次 width-one 执行约 1,915 ns/eval。retained output 16 B，input payload copy 0，逻辑上每次有 output collector + temporary result 两个 owned vectors 和一次 append。这个数字没有 frozen-before 对照，也不是 allocator-call/peak-memory 测量，因此不用于宣称改进。

### 8.3 prepared worker 构造成本与 statement 生命周期选择

在当前 TiKV revision 的 release profile 上，固定 CPU2、500 次 warmup、每个样本 20,000 次 `prepare_evaluated_bytes + operation + retained_storage + drop`、11 个样本，测得 steady-state 构造/销毁中位数：ASCII 543.153 ns、integer add 687.988 ns、STRCMP 844.112 ns、Decimal add 851.984 ns、JSON_TYPE 530.752 ns、REGEXP_LIKE 851.114 ns；各 family 的 11-sample min/max 分别为 539.806–544.024、686.950–695.065、842.857–854.526、850.536–881.954、525.982–556.141、842.361–853.710 ns。原始记录在 `logs/worker-prepare-probe-build-run.log`。

这组数字包含 worker drop，不包含 TiDB pool owner/slot/Arc/Mutex 和 public Datum glue；warm allocator/thread cache 也意味着它不是 cold-process 或物理 allocation peak。尽管如此，0.53–0.85 µs 的构造成本相对默认 public one-shot 路径的 2.17–12.10 µs 较小，支持更简单的 statement-owned 策略：worker 只在同一 statement execution 内复用；statement close 只退休自己的 idle worker；不同 live/detached statement 不再通过 rotating global epoch 相互失效。该选择明确放弃跨 statement worker 复用，从而避免在当前 metadata 仍含 per-invocation mutable binding 时引入错误共享。

### 8.4 性能解释

显式复用 execution 很重要，但还不足以消除固定成本。integer add、lazy IF、STRCMP、Decimal add、JSON_TYPE、REGEXP_LIKE 从默认 one-shot 的约 2,260、4,334、2,873、4,007、2,172、12,099 ns/eval 降到 pooled 的约 1,284、3,361、1,670、2,818、1,394、10,696 ns/eval，说明实验 pool/execution policy 有能力避免每次新建 owner 的一部分开销；当前 Session 默认尚未安装该 policy。即便显式启用后，operation/shape 检查、typed carrier 建立、worker lease/guard、width-one `VectorValue` materialization 和 TiDB `Datum` 回投仍形成较大的每调用固定成本。

这些数字只代表 AST/value、width-one、warm execution 路径。它们不包含 SQL parser/planner/storage/network，也没有测 typed batch 或真实 server；不能直接换算为 TiDB QPS。相反，它们适合作为一个明确的优化信号：当前架构实现了唯一 owner 和兼容边界，但 hot-path glue 仍未达到生产性能要求。更复杂表达式能够摊薄固定成本，而简单 arithmetic/control 是最需要减少层次和临时 vector 的场景。

MD5 的 release-only failure 更优先于性能优化。current debug-profile 的同名测试仍通过，但 current release path 在 Prepare 阶段返回 `InvalidSpecification`；根因尚未定位，应先建立 release regression 并完成 root-cause analysis，不能提前归因于 admission、metadata 或其他具体组件。

性能不足不能通过重新引入 TiDB native 快路径解决。后续优化应继续作用于同一个 TiKV kernel，例如减少 width-one materialization、按 operation 复用 prepared worker、扩大经过证明的安全 batch 域，以及为 ownerless helper 提供显式可复用 execution，而不是恢复第二套算法。

## 9. 结论与后续工作

这次 accelerated Demo 的 targeted evidence 表明：在已覆盖域内，TiDB 可以保留所测 SQL 宿主语义，同时把绝大多数 expression value algorithm 统一到 TiKV；这不构成全域等价或逐 family 完整语义证明。关键不在于增加一个跨仓库函数调用，而在于把类型、demand、metadata、effect、lifecycle、错误来源和资源边界全部显式化。

下一阶段应优先：

1. 对高频 family 建立稳定的 release benchmark 和回归预算，分别测 cold/first/warm、width-one/batch；
2. 减少 local adapter 的固定调度与 materialization 成本；
3. 运行真实 session/Unistore/server workload，补 p95/p99 和 allocator 数据；
4. 按 effect protocol 难度逐一迁移五个 deferred family；
5. 开始严格逐 family final audit，而不是把 240/245 功能计数升级成更强结论。

## 10. 证据索引

- 总计划：`EXPRESSION_UNIFICATION_PLAN.md`
- 当前状态：`docs/expression-unification/README.md`
- 冻结分母：`docs/expression-unification/evidence/coverage-baseline-notes.md`
- 完成检查点：`docs/expression-unification/evidence/m0-m6-accelerated-complete-checkpoint.md`
- 五个例外：`docs/expression-unification/evidence/final-five-exceptions.md`
- host adapter 收窄：`docs/expression-unification/evidence/five-host-adapters-checkpoint.md`
- local runtime 合同：`docs/expression-unification/evidence/runtime-contract.md`
- lowering 合同：`docs/expression-unification/evidence/lowering-contract.md`
- M6 成本：`docs/expression-unification/evidence/m6-cost-record-checkpoint.md`
- before/after 性能 receipt：`docs/expression-unification/logs/performance-before-after-summary.txt`
- 独立报告审阅：`docs/expression-unification/logs/architecture-performance-report-review.txt`
- full clippy：`docs/expression-unification/evidence/m6-full-clippy-checkpoint.md`
