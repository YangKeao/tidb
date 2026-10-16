import json,statistics,os
from pathlib import Path
r=Path(__file__).resolve().parent
root=Path(os.environ.get('REPORT_DOC_ROOT', str(r.parent.parent) if r.parent.name=='logs' else '/home/agent/tidb/expression-unification/tidb/docs/expression-unification'))
d=json.loads((r/'summary.json').read_text());b={(x['workload'],x['batch_size']):x for x in d['batch']};w={(x['tier'],x['workload']):x for x in d['width']}
core=['integer_add','integer_add_literal','abs','lazy_if','coalesce_nullif','strcmp','lower','upper','concat','substring','decimal_add','json_type','regexp_like']
small=['integer_add','lazy_if','strcmp','decimal_add','json_type','regexp_like']
comp=json.loads((r/'initial-comparison-summary.json').read_text())
f=lambda x:f'{x:,.3f}'
s='''# Expression unification：调优历程与最终性能对照

测量日期：2026-10-16。生产代码停在 M30：TiDB `c06f259300553625416981dafbb86ce5fa2dd991` / TiKV `d32cebb3ebc353748cb225fe254d6893e62c8bdc`。本轮只扩展测试探针和报告，没有继续修改生产算法。

**结论：大 batch 的已准入运算获得明显收益，但单行和若干 fallback workload 仍然很慢，Decimal PLUS 也仍落后于原始 native。** 所有结果、包括大于1的慢项，都列在表中；不能用 PI 或少数热点代表全部函数。

## 三种版本与读表方法

- **frozen native**：统一前的原始 evaluator，TiDB `364aef2bab5cc633ecb76a775ae8f36f86a6687d` / TiKV `548812e1ef57aef077a2062a9cc356640a6347f5`。只加测试探针，生产源码不改。
- **未做 M1–M30 调优的统一版**：真实历史 TiDB `f388b337dfad156233904d5407230f8439e0150e` / TiKV `608bbfc9c63e8b50fa2be757533eba572ef9b874`，已有前期 lane cache，尚无后续 batch 优化。本轮保留原二进制作PLUS控制，另按同一SQL重建仅改测试探针的版本作13项直接对照；其输出 route 名 `tikv_numeric` 是当时的名称。
- **最终 unified**：M30 生产代码，统一 batch 入口；不准入的 workload 进入当前唯一 canonical fallback。它不是 frozen native，也不能冒充历史统一版。

所有最终倍率都按截图方向 **unified / frozen native**；**小于1更快，大于1更慢**。单位为 ns/eval 或 ns/selected-row，延迟越低越好。历史逐步表中另外明确写出的“加速x”则是 before/after，不能混用。`batch=1` 与直接 AST/value 单次 eval 是不同入口。

'''
s+=f"本轮64个dense1024测例中57个更快、7个更慢（只是该测例集，不能当作SQL覆盖率）。Int PLUS为{b['integer_add',1024]['current_median']:.3f}ns/row，对native的配对耗时倍率{b['integer_add',1024]['ratio']:.3f}x；另组真实历史统一版对照中，Int PLUS为{next(z for z in comp if z['workload']=='integer_add')['initial_median']:.3f}→{next(z for z in comp if z['workload']=='integer_add')['current_median']:.3f}ns/row，配对加速{1/next(z for z in comp if z['workload']=='integer_add')['ratio']:.2f}x。\n\n"
s+=(r/'history.md').read_text()+'\n'
s+='''## For 1 row：直接 AST/value 单次 eval

与截图的 lane-cache 口径对应，`unified` 使用执行 lane 持有的 ReadyValueCache；parse/rewrite 在计时外。**这不是无 cache owner 的默认 one-shot 入口**，后面单列其真实结果。

| workload | frozen native (ns/eval) | unified：复用 lane (ns/eval) | unified / frozen |
|---|---:|---:|---:|
'''
for n in small:
 x=w['ast',n];s+=f"| `{n}` | {f(x['frozen_native_median'])} | {f(x['lane_cache_median'])} | **{x['lane_cache_ratio']:.3f}x** |\n"
s+='\n默认 one-shot 入口（每次调用没有外部持有的 cache owner），不能用上面的 lane 数字代替：\n\n| workload | frozen native (ns/eval) | unified：one-shot (ns/eval) | unified / frozen |\n|---|---:|---:|---:|\n'
for n in small:
 x=w['ast',n];s+=f"| `{n}` | {f(x['frozen_native_median'])} | {f(x['one_shot_median'])} | {x['one_shot_ratio']:.3f}x |\n"
s+='''
## For batched multiple rows：unified / frozen native

真实 Chunk、相同 SQL/输入生成器、dense selection，计时包含 output reset、production evaluator 和结果 materialization。每个格子是三个独立进程对的比值中位数；**不是每 batch 的总耗时倍率与每行倍率混用**。

| workload | 1 | 8 | 64 | 256 | 1024 |
|---|---:|---:|---:|---:|---:|
'''
for n in core:s+='| `'+n+'` | '+' | '.join(f"{b[n,size]['ratio']:.3f}x" for size in [1,8,64,256,1024])+' |\n'
s+='\n1024行的绝对耗时（上述主表同一组采样），避免只看倍率：\n\n| workload | frozen native (ns/row) | final unified (ns/row) | unified / frozen | route |\n|---|---:|---:|---:|---|\n'
for n in core:
 x=b[n,1024];s+=f"| `{n}` | {f(x['native_median'])} | {f(x['current_median'])} | {x['ratio']:.3f}x | {x['route']} |\n"
s+='''
## 真正未调优统一版 → 最终统一版

以下是额外三轮真实历史二进制对照，batch1024；不是同一当前版本强制 scalar/单行的消融。SQL、行数和被选列的值相同；历史探针 schema 有6列，最终有12列（新增的6列在这些 SQL 中未引用）。这是两个真实系统版本的比较，不是只改变一行源码、完全固定所有 metadata 的因果实验。

| workload | 未做M1–M30调优的统一版 (ns/row) | final unified (ns/row) | final / initial unified | 耗时变化 |
|---|---:|---:|---:|---:|
'''
for x in comp:s+=f"| `{x['workload']}` | {f(x['initial_median'])} | {f(x['current_median'])} | {x['ratio']:.3f}x | {(x['ratio']-1)*100:+.1f}% |\n"
s+='''
注意：最早 M2 留存的调优前 Int 是285.179ns/row，M8另一轮真实历史版本复测是253.812；两者的探针/构建和采样时间不同。不能把这些旧数值串成一次严格配对实验；上表使用本轮重新采样的真实历史二进制。

更早、尚未引入 lane cache 的 AST/value 统一版 `a3cf8821`，历史 one-shot 记录为 Int2259.850、IF4334.148、STRCMP2872.959、Decimal4006.884、JSON_TYPE2172.428、REGEXP12099.094ns/eval；这是历史档案，不是本轮同条件复测，也不能拿 AST ns/eval 与 batch ns/row 相除。[原始口径与记录](logs/performance-before-after-summary.txt)。

## 附表：全部64个当前 benchmark workload，dense1024

覆盖58个走batch和6个走canonical fallback的 workload；**64个 workload 不等于68个准入签名，更不等于全部 SQL 函数**。本表一并报告已优化和未优化部分。

| workload | frozen native (ns/row) | final unified (ns/row) | unified / frozen | route |
|---|---:|---:|---:|---|
'''
for (n,size),x in b.items():
 if size==1024:s+=f"| `{n}` | {f(x['native_median'])} | {f(x['current_median'])} | {x['ratio']:.3f}x | {x['route']} |\n"
s+='\n## 附表：standalone rewritten Chunk 单次 eval\n\n这也不是整批执行。两个版本都用已 rewrite 表达式在同一虚拟行上求值。\n\n| workload | frozen native | unified one-shot | unified lane | lane / native |\n|---|---:|---:|---:|---:|\n'
for n in small:
 x=w['chunk',n];s+=f"| `{n}` | {f(x['frozen_native_median'])} | {f(x['one_shot_median'])} | {f(x['lane_cache_median'])} | {x['lane_cache_ratio']:.3f}x |\n"
s+='''
## 复现与边界

- AMD Ryzen9 9900X，release libtest，固定CPU2、单测试线程；boost/SMT和宿主机活动未隔离，不将三个进程对当作总体置信区间。
- 主 batch 表116个 workload/size cells，三个交错进程对，每格7个约75ms样本：4872条计时、348组跨版本 checksum 全一致。width-one 两个 tier、6个 workload，每进程9个样本，共972条计时、36组结果标签一致。另保存105条真实初始PLUS复测，以及798条13workload的初始/最终额外对照（其中546条dense、39组dense checksum一致）。
- 时延是三个进程中位数的中位数；倍率是对应进程对倍率的中位数，因此可能与展示的两个总体中位数直接相除略有差别。每个进程的中位数、所有慢项和原始样本均保留，不挑最快组。
- ASIN/ACOS 使用相同正常/越界混合数据；两项 UInt identity 在两个版本均设置相同 unsigned schema 标志，其余仍为原 signed LongLong/原 Real 数据。不同 batch size 使用同生成器的不同长度 prefix，并非固定完全相同的值分布。
- 初始一次未完成的主表采样因为 CONFIG 标题仍打印旧size列表而中止；实际physical_rows正确，但仍整轮剔除、修正标题重建后从头测。额外历史对照的首次尝试还发现旧decimal_add是d+0.55、最终是d+d，因此整轮剔除，只修正旧版测试SQL、重建并重跑全部13项。两次排除记录单独保存，均未混入正式表。
- M30的生产测试结果沿用明确范围：batch34、完整TiDB1386通过/原有4失败，TiKV1032通过/原有6失败。本轮新增的是测试探针和测量断言，make lint/格式/diff及数据校验；没有声称重新执行server/PD-TiKV/FIPS或修复所有基线失败。
- 这是 warm 内存中 expression evaluator microbenchmark，不含parse/plan/storage/RPC/网络并发，不代表SQL server QPS、冷启动、P99或所有类型/字符集/SQL-mode域。

全部脚本、两个 native frozen 探针及两个历史统一版探针、当前 probe-only diff、源/二进制hash、每个进程值和原始输出见 [最终复测证据目录](logs/performance-final-evolution/)。机器可读主表为 [summary.json](logs/performance-final-evolution/summary.json)，初始/最终额外对照为 [initial-comparison-summary.json](logs/performance-final-evolution/initial-comparison-summary.json)。运行顺序与精确命令见 [validation.md](logs/performance-final-evolution/validation.md)。
'''
(root/'PERFORMANCE_EVOLUTION_REPORT.md').write_text(s)
# Small rendered tables for the final Raft reply, preserving the requested layout.
chat='For 1 row（AST，复用 lane；ns/eval）：\n\n| workload | frozen native | unified | unified / frozen |\n|---|---:|---:|---:|\n'
for n in small:
 x=w['ast',n];chat+=f"| {n} | {f(x['frozen_native_median'])} | {f(x['lane_cache_median'])} | {x['lane_cache_ratio']:.3f}x |\n"
chat+='\nFor batched multiple rows（unified / frozen native）：\n\n| workload | 1 | 8 | 64 | 256 | 1024 |\n|---|---:|---:|---:|---:|---:|\n'
for n in core:chat+='| '+n+' | '+' | '.join(f"{b[n,z]['ratio']:.3f}x" for z in [1,8,64,256,1024])+' |\n'
(r/'chat-tables.md').write_text(chat)
