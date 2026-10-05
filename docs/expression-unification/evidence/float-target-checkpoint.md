# Shared float foundations and target fitting

**float-target-135 / R140**, after [numeric completion](numeric-completion-checkpoint.md). Functional238/245, strict0 and remaining7 unchanged; no whole CAST/M2/package credit.

New datatype owner `codec/native_float_convert.rs` holds the native round-to-even, decimal shift, truncate/max/overflow foundations and ProduceFloat target controller. Wire `truncate_f64` uses a different rounding/input domain and is deliberately not substituted. Existing canonical formatter/type-name primitives are reused.

NaN/Inf return early; unsigned handling follows decimal fitting, and decimal fitting errors precede FLOAT storage-range checks. Only actual Known FLOAT takes the range branch. Diagnostic text remains lazy through an SDK descriptor, while native constructs typed errors. Rust event subjects and Go-formatted diagnostic subjects stay distinct. Original branch-specific order is retained: ordinary overflow reports before event formatting; truncation creates its event first, then subsequent unsigned/report handling.

Native numeric helpers become facades and alias the shared overflow type with original Display/Debug. Native ProduceFloat projects actual metadata, diagnostics and result/event storage. Fixed-shortest rendering, integer text parsing, source conversion/event merging, other datatype controllers and expression rendering remain explicit dependencies.

## Validation

Five matched gates GREEN without failure/retry: SDK2, full native datatype478, native numeric consumers5 and existing SQL2. [Exact commands/counts/hashes](../logs/float-target-summary.txt). Three new tests; all30 old native touched-file test bodies unchanged (SDK touched files had no old tests). One new SDK file, no new native files. Existing fixed-shortest/integer parser/composition/event-merge helpers are byte-identical; expression/SQL files unchanged. No new SQL fixture/probe credit. Broader M2/root/liveDAG/final acceptance remain. Full expression/unistore/workspace/lint/release/exhaustive/performance/physical memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/package/PR readiness unverified. Historical R100 expression4/unistore1 failures and prior RED receipts retained.
