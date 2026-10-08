# M6 workspace and diagnostic metrics

`m6-workspace-metrics-212` turns the remaining broad labels into current receipts.

Full TiKV `make clippy` was rerun at the current HEAD with the pinned Rust toolchain. Repository policy/format-adjacent gates through cargo-deny completed, then native dependencies failed before complete Rust clippy: CMake 4.3 rejects grpcio's vendored c-ares old minimum policy, while GCC 16 rejects the old RocksDB source's missing integer declarations and dependent members. This is a current environment/dependency result, not the earlier Abseil wording and not a passing clippy receipt.

For a bounded diagnostic record, the already-built test binaries were timed directly. The 33/256-depth strict-wire differential used 0.22 s wall and 36,228 KB maximum RSS; the 1,024-pair TiDB CASE gate used 0.01 s and 32,988 KB. Cargo-harness runs were also retained, but especially TiDB's 2.2 GB peak includes debug compilation/linking and is not an evaluator allocation measurement. There is no baseline, threshold or performance-improvement claim.

Performance recording is now present and the workspace gate is attempted/classified. Canonical completion remains ACTIVE pending a green full native toolchain or an explicit waiver for this environment workspace gate and final performance threshold. [Receipts](../logs/m6-workspace-metrics-summary.txt).
