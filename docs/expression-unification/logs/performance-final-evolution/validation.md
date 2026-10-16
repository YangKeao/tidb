# Final performance evolution report: reproduction and scope

## Production revisions

Final TiDB c06f259300553625416981dafbb86ce5fa2dd991 /
TiKV d32cebb3ebc353748cb225fe254d6893e62c8bdc (M30).
Frozen native TiDB364aef2bab5cc633ecb76a775ae8f36f86a6687d /
TiKV548812e1ef57aef077a2062a9cc356640a6347f5.
Historical unified before M1–M30: TiDBf388b337dfad156233904d5407230f8439e0150e /
TiKV608bbfc9c63e8b50fa2be757533eba572ef9b874. It already has the earlier lane cache.

No production algorithms are changed in this reporting task. Current changes
only extend the ignored test's sizes to1/8/64/256/1024 and add an optional dense
scenario filter. Frozen native adds two test probes/registrations. Historical
unified only changes its ignored decimal_add SQL from d+CAST(0.55 AS DECIMAL(10,2))
to d+d for the supplemental matched-query comparison. The original historical
binary401b5aa8 remains separately pinned for the initial PLUS-only control.

## Probe sources and binaries

binary-hashes.txt pins current-bin, native-bin, initial-bin and initial-matched-bin.
The first initial binary is the original retained401b5aa8; the matched binary is
freshly rebuilt with the one query correction and the standard environment.
No ELF files are committed. probe-hashes.txt pins four retained probe sources in
../../evidence/: original-native-final-batch-performance.rs,
original-native-final-width-one-performance.rs,
original-unified-initial-plus-probe.rs (original), and
original-unified-final-comparison-probe.rs (matched query).
current-probe.patch and initial-probe.patch describe the exact test-only diffs.

Build in each TiDB rust/ directory, with its paired TiKV sibling checkout:

    export RUSTC_BOOTSTRAP=1 RUSTFLAGS='' CC=gcc-14 CXX=g++-14 CXXFLAGS='-include cstdint' CMAKE_POLICY_VERSION_MINIMUM=3.5 CARGO_HOME=/home/agent/tidb/expression-unification/cargo-home
    CARGO_TARGET_DIR=/home/agent/tidb/expression-unification/target-tidb cargo test --release --locked -p tidb-expr --lib --no-run
    CARGO_TARGET_DIR=/home/agent/tidb/bench-original-native/target cargo test --release --locked -p tidb-expr --lib --no-run
    CARGO_TARGET_DIR=/home/agent/tidb/bench-original-tikv/target cargo test --release --locked -p tidb-expr --lib --no-run

Run each command only in the corresponding checkout. Copy the release libtest
outputs to the pinned names. To reproduce frozen native, copy/register its two
retained probes; to reproduce matched historical unified, use its retained
15workload probe. Production source stays at the stated revisions. After measurement, the historical unified test-only query change is reverse-applied so that checkout is clean again; its matched probe and binary remain pinned here.
Compiler/toolchain and same-host machine snapshot are in toolchain.txt/machine.txt.
Build/compilation does not overlap any formal timing collection.

## Collect and parse

From this receipt directory, with binaries present:

    python3 collect.py > collect.log 2>&1
    python3 summarize.py > parsed-summary.txt
    python3 collect-initial-comparison.py > initial-comparison-collect.log 2>&1
    python3 summarize-initial-comparison.py > initial-comparison-parsed.txt
    python3 make-report.py

The archived make-report.py defaults to its parent expression-unification doc
root; REPORT_DOC_ROOT can override the generated report destination.
The first collector has116batch cells:13requested rows×5sizes plus the other51
workloads×1024. Three process pairs use current/native then native/current then
current/native, interleaved at each cell. Each cell7samples target75ms, warm32,
output reset and materialization included. Native fixture reproduces current's
12column input generator and all64SQL definitions exactly. Two UInt identities
set identical flags; ASIN/ACOS alone use the inverse-trig valid/invalid mixed
input domain. Other controls retain their original data/flags.

4872batch timing records/348checksum groups must agree; each process has812
records. Current58batch/6fallback workload routes are explicit. Prepared batch
records must have positive live_prepared and zero fallback/refusal; fallback
records must report the actual production_fallback and refusal, not be relabeled
as native. Native size is recorded per invocation, current physical_rows per
record. The parser checks both and the complete7sample cardinality.

Width-one uses unchanged unfolded constant expressions, both AST/value and
rewritten Chunk.9samples for6workloads; current one-shot/lane-cache order is
balanced ABBA/BABA. Native iterations×16 avoid short native samples. Three pairs
produce972timings and36result-label groups, all exact. Primary screenshot-style
width-one table explicitly uses lane-held cache; default one-shot is separate.

The original retained historical binary adds105PLUS-only timings (five shapes,
three processes, seven samples; dense agrees with main current/native checksum).
Supplemental13workload old/final comparison adds798timings, including546dense
and39matched checksum groups. Old probe has6schema columns, final12; SQL and
selected data are identical, the six added final columns are unused. This is
an actual whole-version comparison, not a fully fixed-metadata one-line A/B.

Latencies are medians of3process medians, ratios medians of3paired ratios.
No samples or slower cells are selected away. Process values/ranges remain in
JSON. CPU2 pinning does not isolate boost/SMT/scheduler/host activity; n=3 does
not establish a population confidence interval or application/server QPS.

## Excluded attempts

excluded-stale-size-header/: incomplete first main attempt had correct actual
row sizes but a stale CONFIG dense_sizes label. Stopped; header corrected,
current binary rebuilt, and all formal pairs restarted. None contributes.

excluded-different-decimal-query/: initial supplemental attempt had12matched
SQLs but initial decimal_add was d+0.55 versus current d+d. Its three Decimal
checksums differed as expected from different SQL. No ratio is used. Correct
only the old test probe, rebuild, and rerun all thirteen supplemental pairs.

## Validation scope

Current ignored-probe smoke emitted all64workloads and validates output against
scalar checksum; integer PLUS and PI also retain independent formula/bit checks.
Fresh make lint, scoped rustfmt and git diff --check apply to the small test-only
change. All formal samples have per-run correctness assertions and parser checks.
Production tests remain the explicitly reused M30 results: batch34pass;
TiDB1386pass/same4failures; TiKV1032pass/same6. No fresh whole-server, PD/TiKV,
FIPS or workspace-clippy claim is made, and existing failures are not declared
fixed. No Go/Bazel inputs changed, hence no bazel_prepare.
