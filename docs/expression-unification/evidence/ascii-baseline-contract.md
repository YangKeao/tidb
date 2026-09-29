# ASCII native baseline contract — AB-r1

## 1. Status and authority

This is a **realized OLD native baseline setup**, not M6 acceptance, new C4 performance evidence, an execution-scope proof, or a family-migration increment. It is evidence under the sole `EXPRESSION_UNIFICATION_PLAN.md`, not another ExecPlan.

- E authored `probes/ascii-baseline/source.rs` under the earlier single-source grant. Its frozen SHA256 is `8326eafe34d16cef415b37df227f2d0181d8ee3792b858557cacac7304c14440`.
- Parent subsequently compiled/linked the **unchanged probe**, using existing library artifacts. Parent reports **no product libraries recompiled**. The successful paired-artifact link was parent job 101; the run was parent job 102, exit 0.
- The recorded invocation was `probes/ascii-baseline/native-baseline old-native split-paired-dev-347ed91b09f692de` from the experiment root. It produced `logs/ascii-native-baseline.tsv`.
- E's present grant is **this document only**. E read the source/results/receipts, calculated descriptive statistics, and checked manifest structure. E did not rebuild, relink, rerun, instrument, or edit the immutable probe or binary.
- The source's historical “SOURCE ONLY”/candidate-command comments remain unchanged. This companion receipt records what parent actually did, including the necessary link-command correction.

`old-native`, `split-paired-dev-347ed91b09f692de`, and route names are supplied labels, **not runtime execution-origin or factory counters**. Successful public API evaluation does not prove an official TiKV factory invocation. This receipt contributes **0** migrated/shared-runtime-verified families; denominator **245** and target **221** are unchanged. No numeric performance threshold is introduced.

## 2. Frozen evidence and artifact cohort

Paths in this document are relative to `/home/agent/tidb/expression-unification` unless explicitly absolute.

### 2.1 Primary receipts

| Artifact | SHA256 | Receipt role |
| --- | --- | --- |
| `probes/ascii-baseline/source.rs` | `8326eafe34d16cef415b37df227f2d0181d8ee3792b858557cacac7304c14440` | Immutable 469-line harness; E rechecked hash during doc work |
| `probes/ascii-baseline/native-baseline` | `ae1ca9de709a07ff523569d0108b8ee94376b48859a0210b5997f60f75fb61e8` | Actual linked executable; hash from parent's `logs/ascii-native-baseline-binary.sha256` |
| `probes/ascii-baseline/native-baseline.d` | `1017d24d5473e44906dac79307c86057db85699d249d28df60e1083abc9014af` | Compiler-emitted binary dependency information |
| `logs/ascii-native-baseline.tsv` | `9ba83e219e53ccbd595c447a19fd68aabb297fbed994a41a6ed0c6653f4ae259` | Actual 159-line output, including 150 measurement rows |
| `logs/ascii-baseline-link-paired-command.txt` | `4c460cb9018ad96cd7821bbde7b13cfe2bdf483cea7fb0c601bdb08ff21aa493` | Exact successful shell-escaped argv, including all search directories |
| `logs/ascii-native-baseline-dependencies.sha256` | `77870994cafdd79553f62ab4103c90453f521ec6d1800c7707e7acc915d5874a` | Parent's hashes for all 812 compiler-reported prerequisite paths |

E rehashed the source and the four small receipt files in the last four table rows, not the complete library closure or executable. The binary hash and individual dependency hashes are parent's receipts; E checked their structure and the direct-cohort entries.

### 2.2 Direct dependencies: metadata AND code

Let `B=/home/agent/tidb/expression-unification/target-tidb/debug/build`. Each base below names **two** files: append `.rmeta` and `.rlib`.

| Crate | Base path below B | `.rmeta` SHA256 | `.rlib` SHA256 |
| --- | --- | --- | --- |
| `tidb_expr` | `tidb-expr/347ed91b09f692de/out/libtidb_expr-347ed91b09f692de` | `7530ae8e8e2b20596603a8329d87f5b4aba412bd4ee4624005f76cf28fbb6f30` | `77b637ca3ba0f164aa46b88fa3cc58ccd9553f879142f8a5023248ba9a6b8ede` |
| `tidb_datatype` | `tidb-datatype/3b7c7d097b47cd17/out/libtidb_datatype-3b7c7d097b47cd17` | `6db7f479916fb3695fccf7187d70f67fbfa07d8d30ddfec10fd19d4578096adc` | `21544a90c485666a2739ee37b32890d966da7a9b8c71244dab9f57106a927f3b` |
| `tidb_chunk` | `tidb-chunk/f662d4bd6a26915a/out/libtidb_chunk-f662d4bd6a26915a` | `7f9078b8b43c6e366bc6ab69fa504d097cb26237f57409ac923f8ca4402a1dbb` | `2b4821471853b2062942a47e39117a764a7e39bbf422ffccfb17689f0411ad7b` |
| `tidb_ast` | `tidb-ast/1103bc6200e1af08/out/libtidb_ast-1103bc6200e1af08` | `ca0a21ac6d0ac6f08ebc1125b2236f0bd1c58cb10d13c7b9d5272ff6a01ceb42` | `bc9d30976baeb0e6bce2ceaa17f9ccc27b23c529db53dcc318953280545f04f8` |

The four rlib hashes match the pre-link source receipt. The source records Cargo profile hash `4734766564810459948`, compiler fingerprint `11842659644861950951`, and the dependency-fingerprint matching that selected this cohort. These are existing **DEV/debug** artifacts, **not the later D5 test/aggr graph** and not a fresh build of the current working tree. Filenames/mtime alone are not provenance.

The private wrapper was compiled with `-Copt-level=3 -Ccodegen-units=1 -Clto=off -Cpanic=unwind`; that does not rebuild the precompiled DEV library bodies as release code. Generic/inlined code generated in the wrapper still follows its flags. Treat this as the recorded mixed wrapper/library build, not a release-performance result.

### 2.3 Dependency manifest: exact scope

E read the complete `.d` prerequisite list and checked both long target rules against its per-path empty rules. They agree on **812 distinct paths**:

- 390 `.rlib`, 390 `.rmeta`, 31 `.so`, 1 `.rs`;
- 772 paths under the experiment's `target-tidb`, 39 installed pinned-toolchain library paths, and the probe source;
- the hash manifest has **812 SHA256 records plus one comment footer**, not 813 dependency records; its path set exactly equals the `.d` prerequisite set.

The 31 `.so` entries include compile-time proc-macro artifacts and the toolchain's `libstd.so`. This is **compiler-reported dependency information**, not a claim that every listed file was linked into the final executable or loaded during the measured calls.

The manifest does **not** hash all system linker inputs, the compiler binary, all source inputs that previously produced the rlibs, or the runtime machine/environment. The separate linker trace in `logs/ascii-baseline-link-paired.log` names additional CRT/system inputs, including libssl/libcrypto/libstdc++ and libc/libm/loader paths. Their presence in that trace is not coverage by the 812-path hash manifest. Reading the current source tree cannot replace the frozen artifact cohort.

## 3. Actual standalone-link resolution

| Attempt | Exact argv receipt | Diagnostic receipt | Outcome |
| --- | --- | --- | --- |
| rlib-only direct externs | `logs/ascii-baseline-link-command.txt` | `logs/ascii-baseline-link-first.log` | Four “only metadata stub found” errors: `tidb_ast`, `tidb_chunk`, `tidb_datatype`, `tidb_expr` |
| rmeta-only direct externs | `logs/ascii-baseline-link-metadata-command.txt` | `logs/ascii-baseline-link-metadata.log` | API/type resolution advanced; parent reports full typecheck, then three missing-rlib errors: `tidb_ast`, `tidb_chunk`, `tidb_expr` |
| paired same-crate externs | `logs/ascii-baseline-link-paired-command.txt` | `logs/ascii-baseline-link-paired.log` | Parent job 101 succeeded; binary and `.d` produced; no source edit/product rebuild |

For this compiler/artifact layout, the successful interface supplies **both split outputs for every direct crate**: full metadata via `.rmeta` and linkable code via `.rlib`. It repeats the **same** extern crate name, not an alias or a comma-separated pseudo-path. This is the observed working form; the two failed attempts above retain their exact, different error sets. Example extracted from the successful command:

```text
--extern tidb_expr=/home/agent/tidb/expression-unification/target-tidb/debug/build/tidb-expr/347ed91b09f692de/out/libtidb_expr-347ed91b09f692de.rmeta
--extern tidb_expr=/home/agent/tidb/expression-unification/target-tidb/debug/build/tidb-expr/347ed91b09f692de/out/libtidb_expr-347ed91b09f692de.rlib
```

There are eight direct `--extern` arguments: `.rmeta` then `.rlib` for expression, datatype, chunk, AST. No extra `tidb_model` or parser direct extern was needed: `CiString` is public through `tidb_ast`, and the AST is constructed through public enum variants.

The exact successful command uses:

```text
/home/agent/tidb/expression-reuse/rustup-home/toolchains/nightly-2026-08-22-x86_64-unknown-linux-gnu/bin/rustc
--edition=2021 --crate-name ascii_baseline
-Zthreads=8 -Zbinary-dep-depinfo --emit=link,dep-info
-Copt-level=3 -Ccodegen-units=1 -Clto=off -Cpanic=unwind
-Clinker=/usr/bin/gcc-14 -Clink-arg=-Wl,--trace
```

This is an **excerpt**, not a replacement command. The authoritative argv receipt has 4,928 shell tokens, including 2,448 `-L` search-directory arguments, all eight extern arguments, source and output. The two warnings in the successful log concern adapted output filenames for multiple output types and emitted linker stdout. They are not the earlier missing-artifact errors.

Do not rerun the source's historical rlib-only candidate, overwrite the native binary, or relink against a “newest” independent dependency and call it the same baseline. A future rebuild needs a new coherent artifact receipt. The old binary and this run remain immutable.

## 4. Protocol and data boundaries

### 4.1 Public frontends, not isolated kernels

`source.rs:230–266,449–466` defines the two independent routes:

- **`typed_row_manual`**: `Column::new(1, input_type.clone())`, index 0, inside `ScalarFunction::new(CiString::new("ascii"), result_type, [Column])`; evaluated through `Expression::eval(context, row)`. The result descriptor is `FieldType::new(LongLong).with_flen(3)`. Shape assertions check the complete descriptor and `pb_signature() == None` before/after measured evaluation. This is a **manually constructed node**, not SQL factory/rewriter output.
- **`public_ast`**: public `Expr::Func { name: "ASCII", args: [Expr::Column(["c"])], origin_position: 0 }`, evaluated through `tidb_expr::eval_in`. No parser or SQL rewrite is timed.

A fixture supplies one stable `InputContext` with public `Columns` defaults and an explicit column resolver. It is not reconstructed per evaluation. The typed route materializes its column from a real one-row `Chunk`; the AST resolver clones its supplied datum. Native argument scheduling, coercion, AST charset handling and typed result coercion remain in the invoked public APIs. The probe neither calls private `string_fn::ascii` nor implements an alternate ASCII operation.

### 4.2 Supplied value is not a decoded-kind receipt

| Boundary | What is established | What is not established |
| --- | --- | --- |
| Fixture construction | NULL, or `Datum::new_collation_string(vec![0x41; n], Collation::Binary)`; VarString input descriptor, binary collation, declared flen 4096 | `bytes_1`/`bytes_64`/`bytes_4096` names are not `DatumKind::Bytes` observations |
| Public AST input | `InputContext::get(["c"])` clones the constructor-supplied value | No emitted per-call input-kind trace |
| Physical chunk | `append_datum(0, &value)` and one row; `get_datum(0, &input_type)` is compared with the supplied value during setup | The probe emits no independent decoded `.kind()` receipt; do not turn a descriptor/label/source comment into such a measurement |
| Runtime results | Per-Datum matching accepts only `Null` for NULL or the exact expected `Datum::Int` | Result kind/value does not establish the input's historical kind or execution factory |

**Actual decoded input kind is not separately measured in this frozen run.** Parent confirms there is no additional kind receipt for this probe. The setup equality assertion passed, but this contract does not promote source comments such as “StringDatum(binary) in BOTH routes” into independent runtime kind instrumentation or recovery of a chunk's original tag. The source and binary are not modified to add it.

### 4.3 Input matrix and timing definitions

| Case | Payload bytes | Recipe | Expected result in both timed routes |
| --- | --- | --- | --- |
| `null` | `NULL` | `Datum::Null` | NULL |
| `empty` | 0 | Empty binary-collation string recipe | `Int(0)` |
| `bytes_1` | 1 | Byte `0x41` repeated once | `Int(65)` |
| `bytes_64` | 64 | Byte `0x41` repeated 64 times | `Int(65)` |
| `bytes_4096` | 4096 | Byte `0x41` repeated 4096 times | `Int(65)` |

Every evaluation has **one physical/logical row**. Empty payload is not zero rows/empty selection. One fixed fixture is reused; these are not tests of input rebinding or varied row contents.

Each route/case has five samples, in fixed source order (case order above, typed then AST, samples 0–4):

| Phase | Operations per sample | Timed region | Outside its timer |
| --- | ---: | --- | --- |
| `cold_frontend_construct` | 1,024 | Fresh frontend construction, its internal allocations and storage into a preallocated outer vector | Fixture/chunk construction, parsing/rewriting, evaluation, shape checks and drops |
| `fresh_tree_first_eval` | 1,024 | One public eval per fresh tree, successful Result extraction, raw Datum output writes | Output-buffer allocation, shape/result checks, checksum, printing, final drops |
| `warm_public_eval` | 65,536 | Repeated public eval on one reused frontend/row/context, Result extraction and raw Datum writes | Frontend construction, 1,024 warmup calls, output-buffer allocation, checks/checksum/printing/drops |

`black_box` protects prepared inputs/context/row/evaluation results; no empty-loop subtraction is made. Frontend constructors themselves allocate within their timer; saying “buffer allocation excluded” does not exclude allocations made by the real frontend/evaluator. The source retains timed raw outputs and validates them **after** stopping the clock.

“Cold” means fresh frontend objects, not a fresh process, cold CPU caches, a new context for every call, or isolated backend compilation. `warm_public_eval` is **not** a warmed reusable TiKV execution scope. A future `warm_explicit_scope` must be separate, with its approved scope prepared outside the repeated-eval timer and a separate cold scope/preparation measurement, through the same frontend/input contract.

## 5. Executed result coverage and checksum limits

`logs/ascii-native-baseline.tsv` contains **8 comment/control lines + 1 header + 150 measurement rows**. The checked Cartesian product is:

`2 routes × 5 cases × 3 phases × 5 sample indices = 150`, with 150 unique keys and no missing/extra combination. All iteration counts, payload labels, three-decimal rate rounding and `alloc_calls=unmeasured` cells match the protocol.

There are 30 groups with five samples each. Fifty `fresh_tree_first_eval` rows cover **51,200** checked outputs; fifty warm rows cover **3,276,800** checked outputs: **3,328,000 timed results** in total. The fifty construction rows reuse their paired first-eval checksum; they do **not** add another 51,200 evaluated results. The **51,200 warmup evaluations** check successful `Result` extraction, not each returned value. Four additional untimed control results are individually validated.

The result checks are `checked_checksum`'s per-Datum match against the hard-coded expected value (`source.rs:270–280`), executed outside timing. Successful termination also passed the frontend-shape and zero-warning-count assertions. Warning detail storage/pool health is not tested by this counter-only context.

### 5.1 Observed checksum groups

| Results represented | Count of TSV rows | Checksum |
| --- | ---: | --- |
| NULL, 1,024 outputs; construction and first-eval rows | 20 | `427a1514ee182c0e` |
| NULL, 65,536 outputs; warm rows | 10 | `d559d23cb1e32e23` |
| ALL non-NULL cases, ALL phases, both iteration counts | 120 | `cbf29ce484222325` |

**The checksum is degenerate for these repeated non-NULL patterns/counts**: empty's zero and the three 65-valued cases all end at the initial checksum seed. It is neither a distinct value oracle nor a cryptographic commitment. Matching checksum strings alone would not establish result correctness. The evidence is the preceding per-Datum validation in the frozen source together with the completed exit-0 run. This limitation is preserved, not silently repaired in the immutable harness.

### 5.2 Untimed conversion controls

| Control | Manual typed expectation/hash | Public AST expectation/hash |
| --- | --- | --- |
| `numeric_2` | `Int(50)` / `f94e724211119417` | `Int(50)` / `f94e724211119417` |
| `gbk_manual_vs_ast` | `Int(228)` / `f94e7242111194c9` | `Int(214)` / `f94e7242111194bb` |

The numeric control retains real string coercion. The GBK control supplies UTF-8 storage for `中文` with GBK collation: the manual typed call has no rewriter-inserted `to_binary` child, while the public AST's own BinAware conversion produces GBK bytes. These distinct expectations passed. They are **not timing samples** and do not establish parity between a manually assembled node and rewritten SQL.

## 6. Descriptive timing receipt — not a performance gate

All entries below are **nanoseconds per operation**, calculated from exact `elapsed_ns / iterations`. Each cell is **median [minimum, maximum] across the five samples**; no sample/outlier was removed. Construction's operation is a tree construction; the other columns' operation is one public evaluation.

| Route | Case | Cold frontend construction | Fresh-tree first eval | Warm public eval |
| --- | --- | --- | --- | --- |
| typed_row_manual | null | 268.652 [259.209, 458.477] | 1106.387 [1104.053, 1123.192] | 1121.074 [1114.293, 1148.595] |
| typed_row_manual | empty | 283.652 [261.465, 389.678] | 1229.648 [1217.920, 1272.334] | 1236.369 [1225.084, 1239.129] |
| typed_row_manual | bytes_1 | 259.473 [257.529, 265.684] | 1303.438 [1302.158, 1353.301] | 1301.959 [1299.689, 1302.505] |
| typed_row_manual | bytes_64 | 256.055 [252.725, 262.188] | 1297.969 [1294.170, 1324.775] | 1297.286 [1296.606, 1298.313] |
| typed_row_manual | bytes_4096 | 261.592 [256.660, 296.504] | 1389.131 [1381.271, 1410.205] | 1398.333 [1395.378, 1400.724] |
| public_ast | null | 59.033 [57.949, 113.604] | 962.969 [959.883, 1028.662] | 967.899 [964.394, 975.297] |
| public_ast | empty | 57.646 [57.363, 62.432] | 1073.944 [1068.525, 1110.566] | 1070.045 [1067.462, 1096.506] |
| public_ast | bytes_1 | 58.428 [56.729, 59.385] | 1166.768 [1155.449, 1197.412] | 1165.828 [1161.421, 1171.848] |
| public_ast | bytes_64 | 60.049 [57.832, 110.020] | 1178.311 [1135.450, 1312.119] | 1159.663 [1153.386, 1171.541] |
| public_ast | bytes_4096 | 57.803 [57.314, 66.738] | 1352.724 [1345.547, 1374.092] | 1353.596 [1343.210, 1360.542] |

Ranges of the **five case medians**, not confidence intervals:

| Route | Construction | First eval | Warm public eval |
| --- | --- | --- | --- |
| typed_row_manual | 256.055–283.652 ns | 1106.387–1389.131 ns | 1121.074–1398.333 ns |
| public_ast | 57.646–60.049 ns | 962.969–1352.724 ns | 967.899–1353.596 ns |

These are one process/run's five sequential samples per group, not five independent host trials. The receipts here do not establish CPU affinity, frequency/governor, background load, hardware normalization, allocator-call counts, release throughput or statistical confidence. There is no new-backend measurement or ratio. Do not compare different frontend routes as if they were the same workload, or treat these numbers as isolated ASCII kernel cost.

## 7. Validation performed for this document

E used read/grep tools to inspect the complete probe, TSV, failure logs, successful command/trace and all 812 `.d` prerequisite entries. Read-only `python3 -B` here-doc analysis checked the Cartesian matrix, counts, expected checksum calculation, rate rounding, medians/ranges, paired externs and dependency-set equality. The hash-manifest footer was explicitly excluded; treating its 813 text lines as 813 paths would be incorrect. No analysis script/output file was created.

Exact small-receipt hash command executed from `/home/agent/tidb`:

```bash
sha256sum expression-unification/probes/ascii-baseline/source.rs expression-unification/probes/ascii-baseline/native-baseline.d expression-unification/logs/ascii-native-baseline.tsv expression-unification/logs/ascii-baseline-link-paired-command.txt expression-unification/logs/ascii-native-baseline-dependencies.sha256
```

The following read-only calculation reproduces the matrix/statistics/manifest checks without executing the probe or changing any artifact:

```bash
python3 -B - <<'PY'
import csv, itertools, pathlib, re, shlex, statistics
from collections import Counter
root = pathlib.Path('/home/agent/tidb/expression-unification')
lines = (root/'logs/ascii-native-baseline.tsv').read_text().splitlines()
rows = list(csv.DictReader((x for x in lines if not x.startswith('#')), delimiter='\t'))
routes = ['typed_row_manual', 'public_ast']
cases = {'null': ('NULL', None), 'empty': ('0', 0), 'bytes_1': ('1', 65),
         'bytes_64': ('64', 65), 'bytes_4096': ('4096', 65)}
phases = {'cold_frontend_construct': 1024, 'fresh_tree_first_eval': 1024,
          'warm_public_eval': 65536}
keys = [(r['route'], r['case'], r['phase'], int(r['sample'])) for r in rows]
assert len(lines) == 159 and len(rows) == len(set(keys)) == 150
assert set(keys) == set(itertools.product(routes, cases, phases, range(5)))
checks = {}
for value, count in itertools.product([None, 0, 65], [1024, 65536]):
    state = 0xcbf29ce484222325
    token = 0x9e3779b97f4a7c15 if value is None else value ^ 0x100
    for _ in range(count):
        state = (((state << 7) | (state >> 57)) + token) & ((1 << 64) - 1)
    checks[value, count] = f'{state:016x}'
for r in rows:
    n = int(r['iterations'])
    assert n == phases[r['phase']]
    assert r['payload_bytes'] == cases[r['case']][0]
    assert r['alloc_calls'] == 'unmeasured'
    assert r['result_checksum'] == checks[cases[r['case']][1], n]
    assert abs(int(r['elapsed_ns']) / n - float(r['ns_per_op'])) <= .000501
print('rows', len(rows), 'checksum groups', Counter(r['result_checksum'] for r in rows))
for route in routes:
    for case in cases:
        cells = []
        for phase in phases:
            v = [int(r['elapsed_ns']) / int(r['iterations']) for r in rows
                 if (r['route'], r['case'], r['phase']) == (route, case, phase)]
            cells.append(f'{statistics.median(v):.3f} [{min(v):.3f}, {max(v):.3f}]')
        print(route, case, *cells, sep=' | ')
d = (root/'probes/ascii-baseline/native-baseline.d').read_text().splitlines()
deps = shlex.split(d[0].split(': ', 1)[1])
assert deps == shlex.split(d[2].split(': ', 1)[1])
assert set(deps) == {x[:-1] for x in d[4:] if x}
h = (root/'logs/ascii-native-baseline-dependencies.sha256').read_text().splitlines()
records = [x.split(maxsplit=1) for x in h if x and not x.startswith('#')]
assert all(re.fullmatch('[0-9a-f]{64}', digest) for digest, path in records)
assert len(deps) == len(set(deps)) == len(records) == 812
assert set(deps) == {path for digest, path in records}
print('dependency extensions', Counter(pathlib.Path(p).suffix for p in deps))
argv = shlex.split((root/'logs/ascii-baseline-link-paired-command.txt').read_text())
externs = [argv[i+1] for i, x in enumerate(argv) if x == '--extern']
assert len(externs) == 8
for crate in ['tidb_expr', 'tidb_datatype', 'tidb_chunk', 'tidb_ast']:
    paths = [pathlib.Path(x.split('=', 1)[1]) for x in externs
             if x.split('=', 1)[0] == crate]
    assert len(paths) == 2 and {p.suffix for p in paths} == {'.rmeta', '.rlib'}
    assert len({str(p.with_suffix('')) for p in paths}) == 1
print('paired externs verified; no benchmark executed')
PY
```

## 8. Preservation and future comparison boundary

1. Preserve the frozen source, binary, TSV, successful argv, dependency information and hash receipts. A pathname surviving a Cargo rebuild does not preserve its old contents; hashes identify this cohort, and hashes alone do not archive overwritten libraries.
2. Future backend measurements must keep the same frontend route, supplied input/FieldType recipe, sample/iteration counts, public evaluation boundary and timed-buffer policy. Record a new coherent library cohort and matching compiler/profile/wrapper flags. Do not overwrite the native executable or this TSV.
3. Any future explicit reusable scope has its own cold preparation and warmed measurement; do not rename `warm_public_eval` into scope proof or hide one-shot preparation with an unapproved cache.
4. Kernel/factory provenance, all implemented domains/routes, warning/error behavior, context lifetime/reuse, empty selection, larger batching, parallel execution, SQL-factory inference, PB/unistore behavior, native deletion and product performance gates remain outside this baseline receipt.
5. The only output of this doc turn is `evidence/ascii-baseline-contract.md`. No producer source/binary/log, product, Cargo file, ledger, plan, or evaluated-value contract was changed.
